package orchestrator

import (
	"bytes"
	"fmt"
	"github.com/graceinfra/grace/types"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/executor"
	"github.com/graceinfra/grace/internal/jobhandler"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/paths"
	"github.com/graceinfra/grace/internal/resolver"
	grctemplate "github.com/graceinfra/grace/internal/templates"
	"github.com/graceinfra/grace/internal/utils"
	"github.com/graceinfra/grace/internal/zowe"
)

type zoweOrchestrator struct{}

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
	jobMap := make(map[string]*types.Job)
	for _, job := range graceCfg.Jobs {
		jobMap[job.Name] = job
	}
	resolvedPathMap, resolveErr := paths.PreresolveOutputAndLocalTempDataSources(graceCfg)
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

		jclLocalOutPath := filepath.Join(deckDir, fmt.Sprintf("%s.jcl", job.Name))

		// Initialize contextual logger
		logCtx := log.Logger
		if ctx.WorkflowId != uuid.Nil && !bytes.Equal(ctx.WorkflowId[:], make([]byte, 16)) {
			logCtx = log.With().Str("job_name", job.Name).Str("workflow_id", ctx.WorkflowId.String()).Logger()
		} else {
			logCtx = log.With().Str("job_name", job.Name).Logger()
		}

		isZosJobType := job.Type == "compile" || job.Type == "linkedit" || job.Type == "execute"

		if !isZosJobType {
			logCtx.Debug().Str("job_type", job.Type).Msg("Skipping JCL generation for non-ZOS job type")
			continue // Skip JCL processing for non-ZOS job types like "shell"
		}

		var renderedJCLContent string
		var err error
		var skipJCLProcessingAndUpload bool

		// --- Determine JCL source and render/prepare ---

		if job.JCL != "" && strings.HasPrefix(job.JCL, "zos://") {
			// Case 1: use existing JCL on z/OS
			logCtx.Info().Str("jcl_source", job.JCL).Msg("Using existing JCL from mainframe. No local JCL generation or upload for this job's JCL.")
			skipJCLProcessingAndUpload = true
		} else if job.JCL != "" && strings.HasPrefix(job.JCL, "file://") {
			// Case 2: user provided local JCL file (static or template)
			if noCompile {
				logCtx.Info().Str("jcl_source", job.JCL).Msg("Skipping compilation of user-provided JCL (--no-compile). Will attempt to use existing local file if uploading.")
			} else {
				logCtx.Info().Str("jcl_source", job.JCL).Msgf("Processing user-provided JCL file -> %s", jclLocalOutPath)
			}

			userJCLPath := strings.TrimPrefix(job.JCL, "file://")
			if !filepath.IsAbs(userJCLPath) {
				userJCLPath = filepath.Join(ctx.ConfigDir, userJCLPath)
			}

			userJCLFileContentBytes, readErr := os.ReadFile(userJCLPath)
			if readErr != nil {
				return fmt.Errorf("job %s: failed to read user-provided JCL file %s (from %s): %w", job.Name, userJCLPath, job.JCL, readErr)
			}
			userJCLFileContent := string(userJCLFileContentBytes)

			if !noCompile {
				templateData, prepErr := grctemplate.PrepareJCLTemplateData(ctx, job)
				if prepErr != nil {
					return fmt.Errorf("job %s: failed to prepare data for user JCL template: %w", job.Name, prepErr)
				}
				renderedJCLContent, err = grctemplate.RenderJCL(userJCLPath, userJCLFileContent, templateData)
				if err != nil {
					return fmt.Errorf("job %s: failed to render user-provided JCL template %s: %w", job.Name, userJCLPath, err)
				}
			} else {
				renderedJCLContent = userJCLFileContent
			}
		} else {
			// Case 3: Grace generates JCL using internal default templates
			if noCompile {
				logCtx.Info().Msg("Skipping Grace's JCL generation (--no-compile). Will attempt to use existing local file if uploading.")
			} else {
				logCtx.Info().Msgf("Generating JCL using Grace internal template -> %s", jclLocalOutPath)

				var internalTemplatePath string
				switch job.Type {
				case "compile":
					internalTemplatePath = "files/compile.jcl.tmpl"
				case "linkedit":
					internalTemplatePath = "files/linkedit.jcl.tmpl"
				case "execute":
					internalTemplatePath = "files/execute.jcl.tmpl"
				default:
					return fmt.Errorf("job %s: unsupported job type %q for internal JCL generation", job.Name, job.Type)
				}

				internalTemplateBytes, readErr := grctemplate.TplFS.ReadFile(internalTemplatePath)
				if readErr != nil {
					return fmt.Errorf("job %s: failed to read internal JCL template %s: %w", job.Name, internalTemplatePath, readErr)
				}
				internalTemplateContent := string(internalTemplateBytes)

				templateData, prepErr := grctemplate.PrepareJCLTemplateData(ctx, job)
				if prepErr != nil {
					return fmt.Errorf("job %s: failed to prepare data for internal JCL template: %w", job.Name, prepErr)
				}

				renderedJCLContent, err = grctemplate.RenderJCL(internalTemplatePath, internalTemplateContent, templateData)
				if err != nil {
					return fmt.Errorf("job %s: failed to render internal JCL template %s: %w", job.Name, internalTemplatePath, err)
				}
			}
		}

		// --- Write rendered JCL to .grace/deck/ ---

		// This happens if JCL was generated by Grace or came from a user's file://
		// and !noCompile. If noCompile, we only write if we actually read and "rendered" (even if static/no template vars)

		if !skipJCLProcessingAndUpload {
			if renderedJCLContent != "" && (!noCompile || (job.JCL != "" && strings.HasPrefix(job.JCL, "file://"))) {
				// Write if:
				// 1. We have content and compilation was not skipped
				// or
				// 2. We have content and it's a user file (even if noCompile, we read it for potential upload)
				writeErr := os.WriteFile(jclLocalOutPath, []byte(renderedJCLContent), 0644)
				if writeErr != nil {
					return fmt.Errorf("job %s: failed to write rendered JCL to %s: %w", job.Name, jclLocalOutPath, writeErr)
				}
				if !noCompile {
					logCtx.Info().Str("output_path", jclLocalOutPath).Msg("✓ JCL processed and written locally.")
				} else if job.JCL != "" && strings.HasPrefix(job.JCL, "file://") {
					logCtx.Info().Str("output_path", jclLocalOutPath).Str("source_file", job.JCL).Msg("✓ User JCL file copied locally (compilation skipped).")
				}
			} else if renderedJCLContent == "" && !noCompile {
				logCtx.Warn().Msg("No JCL content was rendered or prepared for writing and compilation not skipped. This might be an issue.")
			}
		}

		// --- Upload JCL ---

		jobNameUpper := strings.ToUpper(job.Name) // Already have this, but ensure it's available here
		if !skipJCLProcessingAndUpload {          // Only if not using zos:// JCL
			if !noUpload { // Only if uploads are generally enabled
				if renderedJCLContent != "" || (noCompile && (job.JCL == "" || strings.HasPrefix(job.JCL, "file://"))) {
					// Condition to attempt upload:
					// 1. We have renderedJCLContent (meaning it was generated or read from user file and not --no-compile)
					// OR
					// 2. --no-compile is true AND (it's default JCL OR it's a file:// JCL)
					//    In this case, we rely on jclLocalOutPath possibly existing from a previous run or being the user's file itself.

					// Ensure local JCL file exists before attempting to upload
					if _, statErr := os.Stat(jclLocalOutPath); os.IsNotExist(statErr) {
						// If noCompile was true for default JCL, and the file doesn't exist, we can't upload.
						// If noCompile was true for file:// JCL, and we couldn't read it earlier, we would have errored out.
						// If renderedJCLContent is empty AND noCompile is true, it means we skipped generation.
						if noCompile && renderedJCLContent == "" {
							logCtx.Info().Str("jcl_local_path", jclLocalOutPath).Msg("JCL generation skipped (--no-compile) and no existing local JCL file to upload.")
						} else if renderedJCLContent != "" {
							return fmt.Errorf("job %s: internal error, JCL file %s expected but not found for upload after processing", job.Name, jclLocalOutPath)
						}
					} else { // File exists, proceed with upload
						targetJCLPDS := resolver.ResolveJCLDataset(job, graceCfg)
						if targetJCLPDS == "" {
							return fmt.Errorf("job %s: cannot determine target JCL PDS for JCL upload", job.Name)
						}
						if err := zowe.EnsurePDSExists(ctx, targetJCLPDS); err != nil {
							return fmt.Errorf("job %s: failed to ensure JCL PDS %s exists for upload: %w", job.Name, targetJCLPDS, err)
						}
						targetJCLMember := fmt.Sprintf("%s(%s)", targetJCLPDS, jobNameUpper)

						logCtx.Info().Str("local_jcl_path", jclLocalOutPath).Str("target_member", targetJCLMember).Msg("Uploading JCL deck...")
						_, uploadErr := zowe.UploadFileToDataset(ctx, jclLocalOutPath, targetJCLMember, "text")
						if uploadErr != nil {
							return fmt.Errorf("job %s: failed to upload JCL %s to %s: %w", job.Name, jclLocalOutPath, targetJCLMember, uploadErr)
						}
						logCtx.Info().Str("target", targetJCLMember).Msg("✓ JCL deck uploaded.")
					}
				} else {
					// This case means renderedJCLContent is "" and it's not a --no-compile scenario for default/file JCL
					// or it's --no-compile and renderedJCLContent is still empty (e.g. default jcl with --no-compile and no pre-existing file)
					logCtx.Info().Msg("No JCL content available or generated; JCL upload skipped.")
				}
			} else { // noUpload is true
				logCtx.Info().Msg("JCL Upload globally skipped (--no-upload).")
			}
		}

		// --- Upload source files (src:// inputs) ---

		if !noUpload {
			effectiveSrcPDS := resolver.ResolveSRCDataset(job, graceCfg)
			if effectiveSrcPDS != "" {
				if err := zowe.EnsurePDSExists(ctx, effectiveSrcPDS); err != nil {
					return fmt.Errorf("job %s: failed to ensure SRC PDS %s exists: %w", job.Name, effectiveSrcPDS, err)
				}

				// Check inputs for src:// paths for this job
				for _, inputSpec := range job.Inputs {
					if strings.HasPrefix(inputSpec.Path, "src://") {
						resource := strings.TrimPrefix(inputSpec.Path, "src://")
						localPath := filepath.Join(ctx.ConfigDir, "src", resource) // src relative to grace.yml

						if _, statErr := os.Stat(localPath); statErr != nil {
							return fmt.Errorf("job %s: source file %s (for input %s, DD %s) not found at %s: %w", job.Name, resource, inputSpec.Path, inputSpec.Name, localPath, statErr)
						}

						memberName := strings.ToUpper(strings.TrimSuffix(filepath.Base(resource), filepath.Ext(resource)))
						if err := utils.ValidatePDSMemberName(memberName); err != nil {
							return fmt.Errorf("job %s: invalid PDS member name '%s' derived from src path '%s': %w", job.Name, memberName, inputSpec.Path, err)
						}

						targetSrcMember := fmt.Sprintf("%s(%s)", effectiveSrcPDS, memberName)
						logCtx.Info().Str("local_path", localPath).Str("target", targetSrcMember).Msgf("Uploading source file for DD %s...", inputSpec.Name)
						_, uploadErr := zowe.UploadFileToDataset(ctx, localPath, targetSrcMember, "text")
						if uploadErr != nil {
							return fmt.Errorf("job %s: failed to upload source %s to %s: %w", job.Name, localPath, targetSrcMember, uploadErr)
						}
						logCtx.Info().Str("target", targetSrcMember).Msg("✓ Source file uploaded.")
					}
				}
			} else {
				// Check if any src:// inputs exist when no effectiveSrcPDS is defined
				for _, inputSpec := range job.Inputs {
					if strings.HasPrefix(inputSpec.Path, "src://") {
						logCtx.Warn().Str("input_path", inputSpec.Path).Msg("Job has 'src://' input but no effective 'datasets.src' is defined. Source file cannot be uploaded.")
						// This could be an error depending on strictness. For now, a warning.
					}
				}
			}
		} else { // noUpload is true
			logCtx.Info().Msg("Source file uploads skipped (--no-upload).")
			// Check if any src:// inputs exist, as they won't be uploaded.
			for _, inputSpec := range job.Inputs {
				if strings.HasPrefix(inputSpec.Path, "src://") {
					logCtx.Warn().Str("input_path", inputSpec.Path).Msgf("Job has 'src://' input '%s', but uploads are disabled (--no-upload). This source will not be available on the mainframe unless already present.", inputSpec.Path)
				}
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

	log.Debug().Msg("Preresolving output paths for run...")
	jobMap := make(map[string]*types.Job)
	for _, job := range ctx.Config.Jobs {
		jobMap[job.Name] = job
	}
	resolvedPathMap, resolveErr := paths.PreresolveOutputAndLocalTempDataSources(ctx.Config)
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

			isZosJobType := job.Type == "compile" || job.Type == "linkedit" || job.Type == "execute"

			createdTempZosDataset := strings.HasPrefix(outputSpec.Path, "zos-temp://") ||
				(isZosJobType && (strings.HasPrefix(outputSpec.Path, "file://") ||
					strings.HasPrefix(outputSpec.Path, "local-temp://")))

			if createdTempZosDataset && !outputSpec.Keep {
				resolvedIdentifier, exists := ctx.ResolvedPaths[outputSpec.Path]
				if !exists {
					logger.Warn().Str("job", job.Name).Str("virtual_path", outputSpec.Path).Msg("Temporary output path not found in resolved paths, cannot clean up.")
					continue
				}

				// For zos-temp://, file://, local-temp:// (from a z/OS job), resolvedIdentifier is the temp z/OS DSN
				isGraceTempDSN := strings.HasPrefix(resolvedIdentifier, ctx.Config.Datasets.JCL[:strings.Index(ctx.Config.Datasets.JCL, ".")]) &&
					strings.Contains(resolvedIdentifier, ".GRC.H")

				if isGraceTempDSN {
					dsnToDelete := resolvedIdentifier
					logger.Info().Str("dsn", dsnToDelete).Msgf("Attempting to clean zos-temporary dataset for output '%s' ('%s') of job '%s'", outputSpec.Name, outputSpec.Path, job.Name)
					if err := zowe.DeleteDatasetIfExists(ctx, dsnToDelete); err != nil {
						logger.Error().Err(err).Str("dsn", dsnToDelete).Msg("Failed to delete zos-temporary dataset.")
						failedCleanupCount++
					} else {
						logger.Info().Str("dsn", dsnToDelete).Msg("Successfully cleaned zos-temporary dataset.")
						cleanedCount++
					}
				} else if strings.HasPrefix(outputSpec.Path, "local-temp://") && !isZosJobType {
					// This handles local-temp:// outputs from shell jobs (local file cleanup)
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
			} else if createdTempZosDataset && outputSpec.Keep {
				resolvedIdentifierToKeep, exists := ctx.ResolvedPaths[outputSpec.Path]
				if exists {
					pathType := "dataset"
					if strings.HasPrefix(outputSpec.Path, "local-temp://") && !isZosJobType {
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
