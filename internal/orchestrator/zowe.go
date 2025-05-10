package orchestrator

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/template"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/executor"
	"github.com/graceinfra/grace/internal/jcl"
	"github.com/graceinfra/grace/internal/jobhandler"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/paths"
	"github.com/graceinfra/grace/internal/resolver"
	grctemplate "github.com/graceinfra/grace/internal/templates"
	"github.com/graceinfra/grace/internal/utils"
	"github.com/graceinfra/grace/internal/zowe"
)

type zoweOrchestrator struct{} // No fields, state comes from ExecutionContext

// NewZoweOrchestrator creates a new Zowe-based orchestrator.
func NewZoweOrchestrator() Orchestrator {
	return &zoweOrchestrator{}
}

// DeckAndUpload implements the JCL generation and upload logic
func (o *zoweOrchestrator) DeckAndUpload(ctx *context.ExecutionContext, registry *jobhandler.HandlerRegistry, noCompile, noUpload bool) error {
	graceCfg := ctx.Config

	// Ensure .grace/deck exists
	deckDir := filepath.Join(".grace", "deck")
	if err := os.MkdirAll(deckDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create deck directory %s: %w", deckDir, err)
	}

	// Check if loadlib exists
	// Assumes ValidateGraceConfig is run and loadlib field is valid and populated
	exists, err := zowe.CheckPDSExists(ctx, ctx.Config.Datasets.LoadLib)
	if err != nil {
		return fmt.Errorf("failed to check if loadlib %q exists: %w", ctx.Config.Datasets.LoadLib, err)
	}
	if !exists {
		return fmt.Errorf("loadlib %q does not exist", ctx.Config.Datasets.LoadLib)
	}

	// --- Preresolve paths needed for JCL generation ---

	log.Debug().Str("workflow_id", ctx.WorkflowId.String()).Msg("Preresolving output paths for decking...")
	resolvedPathMap, resolveErr := paths.PreresolveOutputPaths(graceCfg)
	if resolveErr != nil {
		return fmt.Errorf("decking failed: could not resolve output paths: %w", resolveErr)
	}

	// Initialize context with resolved paths
	ctx.PathMutex.Lock()
	if ctx.ResolvedPaths == nil {
		ctx.ResolvedPaths = make(map[string]string)
	}
	maps.Copy(ctx.ResolvedPaths, resolvedPathMap)
	ctx.PathMutex.Unlock()

	for _, job := range graceCfg.Jobs {
		if len(ctx.SubmitOnly) > 0 && !slices.Contains(ctx.SubmitOnly, job.Name) {
			log.Debug().Str("job_name", job.Name).Msg("Skipping deck/upload due to --only filter")
			continue
		}

		jobNameUpper := strings.ToUpper(job.Name)
		jclFileName := fmt.Sprintf("%s.jcl", job.Name)
		jclOutPath := filepath.Join(deckDir, jclFileName)

		// Initialize contextual logger
		logCtx := log.Logger
		if ctx.WorkflowId != uuid.Nil {
			logCtx = log.With().Str("job_name", job.Name).Str("workflow_id", ctx.WorkflowId.String()).Logger()
		} else {
			logCtx = log.With().Str("job_name", job.Name).Logger()
		}

		isJCLJobType := job.Type == "compile" || job.Type == "linkedit" || job.Type == "execute"

		// --- Compile JCL (conditional) ---
		if isJCLJobType {
			if !noCompile {
				logCtx.Info().Msgf("Generating JCL -> %s", jclOutPath)

				// --- Determine template ---

				var templatePath string
				jobType := job.Type

				if job.Template != "" {
					templatePath = job.Template
				} else if jobType != "" {
					switch jobType {
					case "compile":
						templatePath = "files/compile.jcl.tmpl"
					case "linkedit":
						templatePath = "files/linkedit.jcl.tmpl"
					case "execute":
						templatePath = "files/execute.jcl.tmpl"
					default:
						logCtx.Error().Str("type", jobType).Msg("Unsupported job type")
						return fmt.Errorf("unsupported job type %q for job %q", jobType, job.Name)
					}
				} else {
					logCtx.Error().Str("type", jobType).Msg("No job type or template defined")
					return fmt.Errorf("job %q has no type or template defined", job.Name)
				}

				// --- Generate DD statements ---

				ddStatements, err := jcl.GenerateDDStatements(job, ctx)
				if err != nil {
					logCtx.Error().Err(err).Msg("Failed to generate DD statements")
					return err // Stop decking process if DDs fail
				}

				// --- Prepare template data ---
				programName := resolver.ResolveProgramName(job, graceCfg) // Used for PGM= or member name
				compilerPgm := resolver.ResolveCompilerPgm(job, graceCfg)
				compilerParms := resolver.ResolveCompilerParms(job, graceCfg)
				compilerSteplib := resolver.ResolveCompilerSteplib(job, graceCfg)
				linkerPgm := resolver.ResolveLinkerPgm(job, graceCfg)
				linkerParms := resolver.ResolveLinkerParms(job, graceCfg)
				linkerSteplib := resolver.ResolveLinkerSteplib(job, graceCfg)
				loadLib := resolver.ResolveLoadLib(job, graceCfg) // Get potentially overridden loadlib

				data := map[string]string{
					"JobName":         job.Name,
					"WorkflowId":      "DECK-RUN",
					"ProgramName":     programName, // Resolved program name
					"LoadLib":         loadLib,     // Resolved load library
					"CompilerPgm":     compilerPgm,
					"CompilerParms":   compilerParms,
					"CompilerSteplib": compilerSteplib, // Pass resolved value (might be "")
					"LinkerPgm":       linkerPgm,
					"LinkerParms":     linkerParms,
					"LinkerSteplib":   linkerSteplib, // Pass resolved value (might be "")
					"DDStatements":    ddStatements,
				}

				funcMap := template.FuncMap{
					"ToUpper": strings.ToUpper,
				}

				err = grctemplate.WriteTplWithFuncs(templatePath, jclOutPath, data, funcMap)
				if err != nil {
					logCtx.Error().Err(err).Str("template", templatePath).Msg("Failed to write JCL template")
					return fmt.Errorf("failed to write %s: %w", jclFileName, err)
				}
				logCtx.Info().Str("output_path", jclOutPath).Msg("✓ JCL generated")
			} else {
				logCtx.Info().Msgf("Skipping JCL compilation (--no-compile).")
			}

			// --- Upload files (conditional) ---
			if !noUpload {
				// Ensure PDS exist for JCL and COBOL
				if err := zowe.EnsurePDSExists(ctx, graceCfg.Datasets.JCL); err != nil {
					return err
				}

				// --- Resolve and upload COBOL ---
				log.Info().Msg("Scanning for and uploading required source files...")
				uploadedSources := make(map[string]bool) // Track unique local paths uploaded: localPath -> true

				for _, job := range graceCfg.Jobs {
					jobLogCtx := log.With().Str("job", job.Name).Logger()

					targetSrcPDS := resolver.ResolveSRCDataset(job, graceCfg)
					if targetSrcPDS == "" {
						continue
					}

					// Ensure the target SRC PDS exists (do this once per needed PDS)
					// TODO: Optimize EnsurePDSExists check - maybe track checked PDSs? For now, check each time.
					if err := zowe.EnsurePDSExists(ctx, targetSrcPDS); err != nil {
						jobLogCtx.Error().Err(err).Str("dataset", targetSrcPDS).Msg("Failed ensuring SRC PDS exists")
						return err // Fail early
					}

					// Check inputs for src:// paths for this job
					for _, inputSpec := range job.Inputs {
						if strings.HasPrefix(inputSpec.Path, "src://") {
							resource := strings.TrimPrefix(inputSpec.Path, "src://")
							localPath := filepath.Join("src", resource)

							if _, err := os.Stat(localPath); err != nil {
								jobLogCtx.Error().Err(err).Str("local_path", localPath).Str("virtual_path", inputSpec.Path).Msg("Source file required by input not found locally")
								return fmt.Errorf("required source file %q (for %q in job %q) not found at %s", resource, inputSpec.Path, job.Name, localPath)
							}

							if uploadedSources[localPath] {
								jobLogCtx.Debug().Str("local_path", localPath).Msg("Source file already uploaded in this deck run.")
								continue
							}

							memberName := strings.ToUpper(strings.TrimSuffix(filepath.Base(resource), filepath.Ext(resource)))
							if err := utils.ValidatePDSMemberName(memberName); err != nil {
								jobLogCtx.Error().Err(err).Str("virtual_path", inputSpec.Path).Str("derived_member", memberName).Msg("Invalid member name derived from src:// path")
								return fmt.Errorf("invalid member name %q derived from path %q", memberName, inputSpec.Path)
							}

							targetMember := fmt.Sprintf("%s(%s)", targetSrcPDS, memberName)
							jobLogCtx.Info().Str("local_path", localPath).Str("target", targetMember).Msgf("Uploading source file for DD %q...", inputSpec.Name)

							uploadRes, err := zowe.UploadFileToDataset(ctx, localPath, targetMember)
							if err != nil {
								jobLogCtx.Error().Err(err).Str("target", targetMember).Msg("Source file upload failed")
								return fmt.Errorf("failed to upload source %s to %s: %w", localPath, targetMember, err)
							} // error from UploadFileToDataset
							jobLogCtx.Info().Str("target", targetMember).Msg("✓ Source file uploaded")
							if uploadRes != nil && uploadRes.Data.Success && len(uploadRes.Data.APIResponse) > 0 {
								jobLogCtx.Debug().Str("local_path", localPath).Str("target", targetMember).Msg("Successfully uploaded COBOL source")
							}

							uploadedSources[localPath] = true
						}
					}
				}
			}

			// --- Upload JCL ---

			if isJCLJobType {
				if _, err := os.Stat(jclOutPath); err != nil {
					logCtx.Error().Err(err).Str("path", jclOutPath).Msg("JCL file not found for upload")
					if noCompile {
						return fmt.Errorf("cannot upload JCL for job %q: file %s does not exist and --no-compile was specified", job.Name, jclOutPath)
					} else {
						return fmt.Errorf("internal error: JCL file %s not found for job %q after compilation attempt", jclOutPath, job.Name)
					}
				}

				logCtx.Info().Str("target_member", jobNameUpper).Msg("Uploading JCL deck...")

				targetJCL := fmt.Sprintf("%s(%s)", ctx.Config.Datasets.JCL, jobNameUpper)
				jclUploadRes, err := zowe.UploadFileToDataset(ctx, jclOutPath, targetJCL)
				if err != nil {
					return fmt.Errorf("failed to upload JCL %s to %s: %w", jclOutPath, targetJCL, err)
				}

				logCtx.Info().Str("target", targetJCL).Msg("✓ JCL deck uploaded")
				if jclUploadRes != nil && jclUploadRes.Data.Success && len(jclUploadRes.Data.APIResponse) > 0 {
					logCtx.Debug().Str("from", jclUploadRes.Data.APIResponse[0].From).Str("to", jclUploadRes.Data.APIResponse[0].To).Msg("JCL upload details")
				}
			} else {
				logCtx.Info().Msg("Skipping uploads (--no-upload).")
			}
		}

	}

	log.Info().Msg("✓ Deck and upload process completed.")
	return nil
}

// Run implements the DAG job execution and monitoring logic using the executor.
func (o *zoweOrchestrator) Run(ctx *context.ExecutionContext, registry *jobhandler.HandlerRegistry) ([]models.JobExecutionRecord, error) {
	// Configure contextual logger
	runLogger := log.With().Str("workflow_id", ctx.WorkflowId.String()).Logger()

	// --- Pre-Loop Validations / Setup ---

	hlq := ""
	if ctx.Config.Datasets.JCL != "" {
		parts := strings.Split(ctx.Config.Datasets.JCL, ".")
		if len(parts) > 0 {
			hlq = parts[0]
		}
	}
	if hlq == "" {
		return nil, fmt.Errorf("orchestration failed: invalid HLQ derived from datasets.jcl (%q). Ensure the field exists and is valid in grace.yml", ctx.Config.Datasets.JCL)
	}
	runLogger.Debug().Msgf("Using HLQ: %s for initiator info", hlq)

	// --- Preresolve output paths ---

	log.Debug().Msg("Preresolving output paths...")
	resolvedPathMap, resolveErr := paths.PreresolveOutputPaths(ctx.Config)
	if resolveErr != nil {
		return nil, fmt.Errorf("orchestration failed: could not resolve output paths: %v", resolveErr)
	}

	// Initialize context with resolved paths
	ctx.PathMutex.Lock()
	if ctx.ResolvedPaths == nil {
		ctx.ResolvedPaths = make(map[string]string)
	}
	maps.Copy(ctx.ResolvedPaths, resolvedPathMap)
	ctx.PathMutex.Unlock()

	log.Debug().Msgf("Resolved %d output paths.", len(resolvedPathMap))

	// --- Build job graph ---

	// Assumes ValidateGraceConfig (including cycle check)
	log.Debug().Msg("Building job graph from configuration...")
	jobGraph, graphErr := config.BuildJobGraph(ctx.Config)
	if graphErr != nil {
		return nil, fmt.Errorf("orchestration failed: could not build job graph: %w", graphErr)
	}

	// Handle case where config is valid but has no jobs
	if len(jobGraph) == 0 {
		runLogger.Info().Msg("No jobs defined in the configuration. Workflow finished.")
		return []models.JobExecutionRecord{}, nil
	}
	runLogger.Debug().Msgf("Job graph built successfully with %d nodes.", len(jobGraph))

	// --- Create and run executor ---

	// We pass the max concurrency setting from grace.yml here
	exec := executor.NewExecutor(ctx, jobGraph, ctx.Config.Config.Concurrency, registry)

	runLogger.Debug().Msg("Invoking executor...")

	jobExecutionRecords, execErr := exec.ExecuteAndWait()
	// NOTE: execErr represents errors from the executor's own logic (e.g. deadlock)
	// not individual job failures. Those are captured in the jobExecutionRecords.

	if execErr != nil {
		runLogger.Error().Err(execErr).Msgf("Executor encountered an error")

		// Return the records collected so far anyways so that the caller can still generate
		// a partial summary
		return jobExecutionRecords, fmt.Errorf("DAG execution failed: %w", execErr)
	}

	runLogger.Info().Msg("✓ Executor finished successfully.")

	// --- Perform cleanup ---
	runCleanup := false
	workflowActuallyFailed := execErr != nil // Executor-level failure

	if !workflowActuallyFailed { // No executor error, check job records for any actual failures
		for _, record := range jobExecutionRecords {
			jobFailed := false
			if record.JobID == "SUBMIT_FAILED" {
				jobFailed = true
			} else if record.JobID != "SKIPPED" {
				if record.FinalResponse != nil && record.FinalResponse.Data != nil {
					status := record.FinalResponse.Data.Status
					rc := record.FinalResponse.Data.RetCode

					isJobStatusFailure := status == "ABEND" ||
						status == "JCL ERROR" ||
						status == "SEC ERROR" ||
						status == "SYSTEM FAILURE" ||
						status == "CANCELED" ||
						(status == "OUTPUT" && rc != nil && *rc != "CC 0000" && *rc != "CC 0004")

					if isJobStatusFailure {
						jobFailed = true
					}
				} else if record.FinalResponse == nil {
					// Job attempted to run (not SKIPPED) but has no final response
					jobFailed = true
					runLogger.Warn().Str("job_name", record.JobName).Msg("Job ran but has no final response, considering workflow as failed for cleanup decision.")
				}
			}

			if jobFailed {
				workflowActuallyFailed = true
				break
			}
		}
	}

	cfgCleanup := ctx.Config.Config.Cleanup

	effectiveCleanupOnSuccess := true
	if cfgCleanup.OnSuccess != nil {
		effectiveCleanupOnSuccess = *cfgCleanup.OnSuccess
	}

	effectiveCleanupOnFailure := false
	if cfgCleanup.OnFailure != nil {
		effectiveCleanupOnFailure = *cfgCleanup.OnFailure
	}

	if !workflowActuallyFailed && effectiveCleanupOnSuccess {
		runCleanup = true
		runLogger.Info().Msg("Workflow successful, proceeding with cleanup of temporary datasets.")
	} else if workflowActuallyFailed && effectiveCleanupOnFailure {
		runCleanup = true
		runLogger.Info().Msg("Workflow failed, proceeding with cleanup of temporary datasets as configured.")
	} else {
		if workflowActuallyFailed {
			runLogger.Info().Msgf("Workflow failed and cleanup.on_failure is not enabled. Skipping cleanup of temporary datasets.")
		} else {
			// Workflow succeeded but cleanup.on_success is not enabled
			runLogger.Info().Msgf("Workflow successful but cleanup.on_success is not enabled. Skipping cleanup of temporary datasets.")
		}
	}

	if runCleanup {
		o.cleanupTemporaryDatasets(ctx, runLogger)
	}

	return jobExecutionRecords, nil
}

func (o *zoweOrchestrator) cleanupTemporaryDatasets(ctx *context.ExecutionContext, logger zerolog.Logger) {
	logger.Info().Msg("Cleaning intermediate datasets...")
	cleanedCount := 0
	failedCleanupCount := 0

	for _, job := range ctx.Config.Jobs {
		for _, outputSpec := range job.Outputs {
			logger.Debug().
				Str("job_cleanup_scan", job.Name).
				Str("output_dd", outputSpec.Name).
				Str("output_path", outputSpec.Path).
				Bool("output_keep", outputSpec.Keep).
				Msg("Scanning output for cleanup eligibility")

			isZosTemp := strings.HasPrefix(outputSpec.Path, "zos-temp://")
			isLocalTemp := strings.HasPrefix(outputSpec.Path, "local-temp://")

			if (isZosTemp || isLocalTemp) && !outputSpec.Keep {
				resolvedIdentifier, exists := ctx.ResolvedPaths[outputSpec.Path]
				if !exists {
					logger.Warn().Str("job", job.Name).Str("virtual_path", outputSpec.Path).Msg("Temporary output path not found in resolved paths, cannot clean up.")
					continue
				}

				if isZosTemp {
					dsnToDelete := resolvedIdentifier
					logger.Info().Str("dsn", dsnToDelete).Msgf("Attempting to clean zos-temporary dataset for output '%s' ('%s') of job '%s'", outputSpec.Name, outputSpec.Path, job.Name)
					if err := zowe.DeleteDatasetIfExists(ctx, dsnToDelete); err != nil {
						logger.Error().Err(err).Str("dsn", dsnToDelete).Msg("Failed to delete zos-temporary dataset.")
						failedCleanupCount++
					} else {
						logger.Info().Str("dsn", dsnToDelete).Msg("Successfully cleaned zos-temporary dataset.")
						cleanedCount++
					}
				} else {
					// resolvedIdentifier for local-temp is the filename part
					localPathToDelete := filepath.Join(ctx.LocalStageDir, resolvedIdentifier)
					logger.Info().Str("local_path", localPathToDelete).Msgf("Attempting to clean local-temporary file for output '%s' ('%s') of job '%s'", outputSpec.Name, outputSpec.Path, job.Name)

					if err := os.Remove(localPathToDelete); err != nil && !os.IsNotExist(err) {
						logger.Error().Err(err).Str("local_path", localPathToDelete).Msg("Failed to delete local-temporary file.")
						failedCleanupCount++
					} else {
						if os.IsNotExist(err) {
							logger.Info().Str("local_path", localPathToDelete).Msg("Local-temporary file already deleted or never existed.")
						} else {
							logger.Info().Str("local_path", localPathToDelete).Msg("Successfully cleaned local-temporary file.")
						}
						cleanedCount++
					}
				}
			} else if (isZosTemp || isLocalTemp) && outputSpec.Keep {
				resolvedIdentifierToKeep, exists := ctx.ResolvedPaths[outputSpec.Path]
				if exists {
					pathType := "dataset"
					if isLocalTemp {
						pathType = "file"
						resolvedIdentifierToKeep = filepath.Join(ctx.LocalStageDir, resolvedIdentifierToKeep)
					}
					logger.Info().Str(pathType, resolvedIdentifierToKeep).Msgf("Skipping cleanup of temporary %s due to 'keep: true' flag for output '%s' of job '%s'.", pathType, outputSpec.Name, job.Name)
				}
			}
		}
	}

	logger.Info().Msgf("Temporary dataset cleanup summary: %d item(s) processed for deletion, %d failure(s).", cleanedCount, failedCleanupCount)
}
