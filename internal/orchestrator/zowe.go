package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/executor"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/templates"
	"github.com/graceinfra/grace/internal/zowe"
)

type zoweOrchestrator struct{} // No fields, state comes from ExecutionContext

// NewZoweOrchestrator creates a new Zowe-based orchestrator.
func NewZoweOrchestrator() Orchestrator {
	return &zoweOrchestrator{}
}

// DeckAndUpload implements the JCL generation and upload logic
func (o *zoweOrchestrator) DeckAndUpload(ctx *context.ExecutionContext, noCompile, noUpload bool) error {
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

	for _, job := range graceCfg.Jobs {
		if len(ctx.SubmitOnly) > 0 && !slices.Contains(ctx.SubmitOnly, job.Name) {
			log.Debug().Str("job_name", job.Name).Msg("Skipping deck/upload due to --only filter")
			continue
		}

		jobNameUpper := strings.ToUpper(job.Name)
		jclFileName := fmt.Sprintf("%s.jcl", job.Name)
		jclOutPath := filepath.Join(deckDir, jclFileName)

		// Initialize contextual logger
		deckJobLogger := log.With().Str("job_name", job.Name).Logger()

		fmt.Println() // Newline

		// --- Compile JCL (conditional) ---
		if !noCompile {
			deckJobLogger.Info().Msgf("Generating JCL -> %s", jclOutPath)

			var templatePath string

			step := job.Step

			if job.Template != "" {
				templatePath = job.Template
			} else if step != "" {
				switch step {
				case "execute":
					templatePath = "files/execute.jcl.tmpl"
				default:
					return fmt.Errorf("unsupported step: %s", step)
				}
			} else {
				return fmt.Errorf("no template found for step %s", step)
			}

			cobolDsn := graceCfg.Datasets.SRC + "(" + jobNameUpper + ")"

			data := map[string]string{
				"JobName":  jobNameUpper,
				"CobolDSN": cobolDsn,
				"LoadLib":  graceCfg.Datasets.LoadLib,
			}

			err := templates.WriteTpl(templatePath, jclOutPath, data)
			if err != nil {
				return fmt.Errorf("failed to write %s: %w", jclFileName, err)
			}
			deckJobLogger.Info().Msgf("✓ JCL generated at %s", jclOutPath)
		} else {
			deckJobLogger.Info().Msgf("Skipping JCL compilation (--no-compile).")
		}

		// --- Upload files (conditional) ---
		if !noUpload {
			// Ensure PDS exist for JCL and COBOL
			if err := zowe.EnsurePDSExists(ctx, graceCfg.Datasets.JCL); err != nil {
				return err
			}
			if err := zowe.EnsurePDSExists(ctx, graceCfg.Datasets.SRC); err != nil {
				return err
			}

			// --- Upload JCL ---
			if _, err := os.Stat(jclOutPath); err != nil {
				if noCompile {
					return fmt.Errorf("cannot upload JCL for job %q: file %s does not exist and --no-compile was specified", job.Name, jclOutPath)
				} else {
					return fmt.Errorf("internal error: JCL file %s not found for job %q after compilation attempt", jclOutPath, job.Name)
				}
			}

			target := fmt.Sprintf("%s(%s)", ctx.Config.Datasets.JCL, jobNameUpper)
			jclUploadRes, err := zowe.UploadFileToDataset(ctx, jclOutPath, target)
			if err != nil {
				return fmt.Errorf("failed to upload JCL %s to %s: %w", jclOutPath, target, err)
			}

			log.Info().Str("job", job.Name).Msg("✓ JCL deck uploaded")
			if jclUploadRes != nil && jclUploadRes.Data.Success && len(jclUploadRes.Data.APIResponse) > 0 {
				deckJobLogger.Debug().Msgf("  From: %s", jclUploadRes.Data.APIResponse[0].From)
				deckJobLogger.Debug().Msgf("  To:   %s", jclUploadRes.Data.APIResponse[0].To)
			}

			// --- Upload COBOL source ---
			if job.Source != "" {

				// Construct path to COBOL file
				srcMember := fmt.Sprintf("%s(%s)", graceCfg.Datasets.SRC, jobNameUpper)

				srcPath := filepath.Join("src", job.Source)
				_, err = os.Stat(srcPath)
				if err != nil {
					return fmt.Errorf("unable to resolve COBOL source file %s for job %s", srcPath, job.Name)
				}

				cobolUploadRes, err := zowe.UploadFileToDataset(ctx, srcPath, srcMember)
				if err != nil {
					return fmt.Errorf("COBOL source upload to %s failed for job %s: %w\n", srcMember, job.Name, err)
				}

				log.Info().Str("job_name", job.Name).Msgf("✓ COBOL data set %s submitted", job.Source)
				if cobolUploadRes != nil && cobolUploadRes.Data.Success && len(cobolUploadRes.Data.APIResponse) > 0 {
					deckJobLogger.Debug().Msgf("  From: %s", jclUploadRes.Data.APIResponse[0].From)
					deckJobLogger.Debug().Msgf("  To:   %s", jclUploadRes.Data.APIResponse[0].To)
				}
			} else {
				deckJobLogger.Info().Msg("Skipping COBOL source upload (no source defined).")
			}
		} else {
			deckJobLogger.Info().Msg("Skipping uploads (--no-upload).")
		}

	}
	return nil
}

// Run implements the DAG job execution and monitoring logic using the executor.
func (o *zoweOrchestrator) Run(ctx *context.ExecutionContext) ([]models.JobExecutionRecord, error) {
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

	// We pass the max concurrency value from grace.yml here
	exec := executor.NewExecutor(ctx, jobGraph, ctx.Config.Config.Concurrency)

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
	return jobExecutionRecords, nil
}
