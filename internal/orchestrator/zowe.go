package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/executor"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/templates"
	"github.com/graceinfra/grace/internal/zowe"
	"github.com/graceinfra/grace/types"
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

	for _, job := range graceCfg.Jobs {
		if len(ctx.SubmitOnly) > 0 && !slices.Contains(ctx.SubmitOnly, job.Name) {
			ctx.Logger.Verbose("Skipping deck/upload for job %q due to --only filter", job.Name)
			continue
		}

		jobNameUpper := strings.ToUpper(job.Name)
		jclFileName := fmt.Sprintf("%s.jcl", job.Name)
		jclOutPath := filepath.Join(deckDir, jclFileName)

		fmt.Println() // Newline

		// --- Compile JCL (conditional) ---
		if !noCompile {
			ctx.Logger.Info("Generating JCL for job %q -> %s", job.Name, jclOutPath)

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
			ctx.Logger.Info("✓ JCL for job %q generated at %s", job.Name, jclOutPath)
		} else {
			ctx.Logger.Info("Skipping JCL compilation for job %q (--no-compile).", job.Name)
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

			spinnerTextJCL := fmt.Sprintf("Uploading JCL deck %s ...", jobNameUpper)
			ctx.Logger.StartSpinner(spinnerTextJCL)

			target := fmt.Sprintf("%s(%s)", ctx.Config.Datasets.JCL, jobNameUpper)
			jclUploadRes, err := zowe.UploadFileToDataset(ctx, jclOutPath, target)
			if err != nil {
				ctx.Logger.StopSpinner()
				return fmt.Errorf("failed to upload JCL %s to %s: %w", jclOutPath, target, err)
			}

			ctx.Logger.StopSpinner()

			ctx.Logger.Info("✓ JCL deck for job %q uploaded", job.Name)
			if jclUploadRes != nil && jclUploadRes.Data.Success && len(jclUploadRes.Data.APIResponse) > 0 {
				ctx.Logger.Verbose("  From: %s", jclUploadRes.Data.APIResponse[0].From)
				ctx.Logger.Verbose("  To:   %s", jclUploadRes.Data.APIResponse[0].To)
			}

			// --- Upload COBOL source ---
			if job.Source != "" {
				spinnerTextCOBOL := fmt.Sprintf("Uploading COBOL source %s ...", job.Source)
				ctx.Logger.StartSpinner(spinnerTextCOBOL)

				// Construct path to COBOL file
				srcMember := fmt.Sprintf("%s(%s)", graceCfg.Datasets.SRC, jobNameUpper)

				srcPath := filepath.Join("src", job.Source)
				_, err = os.Stat(srcPath)
				if err != nil {
					ctx.Logger.StopSpinner()
					return fmt.Errorf("unable to resolve COBOL source file %s for job %s", srcPath, job.Name)
				}

				cobolUploadRes, err := zowe.UploadFileToDataset(ctx, srcPath, srcMember)
				if err != nil {
					ctx.Logger.StopSpinner()
					return fmt.Errorf("COBOL source upload to %s failed for job %s: %w\n", srcMember, job.Name, err)
				}

				ctx.Logger.StopSpinner()

				ctx.Logger.Info(fmt.Sprintf("✓ COBOL data set %s submitted for job %q", job.Source, job.Name))
				if cobolUploadRes != nil && cobolUploadRes.Data.Success && len(cobolUploadRes.Data.APIResponse) > 0 {
					ctx.Logger.Verbose("  From: %s", cobolUploadRes.Data.APIResponse[0].From)
					ctx.Logger.Verbose("  To:   %s", cobolUploadRes.Data.APIResponse[0].To)
				}
			} else {
				ctx.Logger.Info("Skipping COBOL source upload for job %q (no source defined).", job.Name)
			}
		} else {
			ctx.Logger.Info("Skipping uploads for job %q (--no-upload).", job.Name)
		}

	}
	return nil
}

// Run implements the DAG job execution and monitoring logic using the executor.
func (o *zoweOrchestrator) Run(ctx *context.ExecutionContext) ([]models.JobExecutionRecord, error) {
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
	ctx.Logger.Verbose("Using HLQ: %s for initiator info", hlq)

	// --- Build job graph ---

	// Assumes ValidateGraceConfig (including cycle check)
	ctx.Logger.Verbose("Building job graph from configuration...")
	jobGraph, graphErr := config.BuildJobGraph(ctx.Config)
	if graphErr != nil {
		return nil, fmt.Errorf("orchestration failed: could not build job graph: %w", graphErr)
	}

	// Handle case where config is valid but has no jobs
	if len(jobGraph) == 0 {
		ctx.Logger.Info("No jobs defined in the configuration. Workflow finished.")
		return []models.JobExecutionRecord{}, nil
	}
	ctx.Logger.Verbose("Job graph built successfully with %d nodes.", len(jobGraph))

	// --- Create and run executor ---

	// We pass the max concurrency value from grace.yml here
	exec := executor.NewExecutor(ctx, jobGraph, ctx.Config.Config.Concurrency)

	ctx.Logger.Verbose("Invoking executor...")
	jobExecutionRecords, execErr := exec.ExecuteAndWait()
	// NOTE: execErr represents errors from the executor's own logic (e.g. deadlock)
	// not individual job failures. Those are captured in the jobExecutionRecords.

	if execErr != nil {
		ctx.Logger.Error("Executor encountered and error: %v", execErr)

		// Return the records collected so far anyways so that the caller can still generate
		// a partial summary
		return jobExecutionRecords, fmt.Errorf("DAG execution failed: %w", execErr)
	}

	ctx.Logger.Info("✓ Executor finished successfully.")
	return jobExecutionRecords, nil
}

func (o *zoweOrchestrator) Submit(ctx *context.ExecutionContext) ([]*types.ZoweRfj, error) {
	return nil, nil
}
