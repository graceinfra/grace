package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
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

// Run implements the job execution and monitoring logic.
func (o *zoweOrchestrator) Run(ctx *context.ExecutionContext) ([]models.JobExecutionRecord, error) {
	var jobExecutions []models.JobExecutionRecord

	// --- Pre-Loop Validations / Setup ---

	host, err := os.Hostname()
	if err != nil {
		ctx.Logger.Error("⚠️ Failed to get hostname: %v. Using default.", err)
		host = "unknown"
	}

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

	for _, job := range ctx.Config.Jobs {
		fmt.Println() // Newline

		// Skip based on --only
		if len(ctx.SubmitOnly) > 0 && !slices.Contains(ctx.SubmitOnly, job.Name) {
			ctx.Logger.Verbose("Skipping job %q due to --only filter", job.Name)
			continue
		}

		startTime := time.Now()

		record := models.JobExecutionRecord{
			JobName:     job.Name,
			JobID:       "PENDING", // Initial state before submit attempt
			Step:        job.Step,
			Source:      job.Source,
			RetryIndex:  0,
			GraceCmd:    ctx.GraceCmd,
			ZoweProfile: ctx.Config.Config.Profile,
			HLQ:         hlq,
			Initiator: types.Initiator{
				Type:   "user",
				Id:     os.Getenv("USER"),
				Tenant: host,
			},
			WorkflowId:     ctx.WorkflowId,
			SubmitTime:     startTime.Format(time.RFC3339),
			SubmitResponse: nil,
			FinalResponse:  nil,
			// FinishTime and DurationMs set later
		}

		// --- Submit job ---

		spinnerMsg := fmt.Sprintf("Submitting job %s ...", strings.ToUpper(job.Name))
		ctx.Logger.StartSpinner(spinnerMsg)

		submitResult, submitErr := zowe.SubmitJob(ctx, job)
		record.SubmitResponse = submitResult

		ctx.Logger.StopSpinner()

		// --- Handle submit outcome ---

		if submitErr != nil {
			// Error could be process error or Zowe logical error from SubmitJob
			record.JobID = "SUBMIT_FAILED"
			record.FinishTime = time.Now().Format(time.RFC3339)
			record.DurationMs = time.Since(startTime).Milliseconds()
			ctx.Logger.Error("⚠️ Job %s submission failed: %v", record.JobName, submitErr)

			// Log the record even on failure
			_ = log.SaveJobExecutionRecord(ctx.LogDir, record)
			jobExecutions = append(jobExecutions, record)

			// Move to the next job
			continue
		}

		record.JobID = submitResult.Data.JobID
		ctx.Logger.Info("✓ Job %s submitted with ID %s (status: %s)", record.JobName, record.JobID, submitResult.Data.Status)

		// --- Wait for job completion ---

		// WaitForJobCompletion handles its own spinner
		finalResult, waitErr := zowe.WaitForJobCompletion(ctx, record.JobID)
		record.FinalResponse = finalResult

		finishTime := time.Now()
		record.FinishTime = finishTime.Format(time.RFC3339)
		record.DurationMs = int64(finishTime.Sub(startTime).Milliseconds())

		// --- Handle completion outcome ---

		if waitErr != nil {
			ctx.Logger.Error("⚠️ Failed to get final status for job %s (%s): %v", record.JobName, record.JobID, waitErr)
		} else if finalResult != nil && finalResult.Data != nil {
			retCode := "null"
			if finalResult.Data.RetCode != nil {
				retCode = *finalResult.Data.RetCode
			}
			ctx.Logger.Info("✓ Job %s (%s) completed: Status %s, RC %s", record.JobName, record.JobID, finalResult.Data.Status, retCode)
		} else {
			ctx.Logger.Error("⚠️ Polling for job %s (%s) finished, but final status data is incomplete.", record.JobName, record.JobID)
		}

		ctx.Logger.Json(record)

		// --- Save detailed log ---

		saveErr := log.SaveJobExecutionRecord(ctx.LogDir, record)
		if saveErr != nil {
			ctx.Logger.Error("⚠️ Failed to save detailed log for job %s (%s): %v", record.JobName, record.JobID, saveErr)
		}

		jobExecutions = append(jobExecutions, record)

		// TODO: Add logic for overall workflow failure (e.g. stop processing if a job ABENDs?)
	}

	return jobExecutions, nil
}

func (o *zoweOrchestrator) Submit(ctx *context.ExecutionContext) ([]*types.ZoweRfj, error) {
	return nil, nil
}
