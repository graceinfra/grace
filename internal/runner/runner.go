package runner

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/zowe"
	"github.com/graceinfra/grace/types"
)

// RunWorkflow runs a workflow end-to-end. It covers submitting + watching job executions.
func RunWorkflow(ctx *context.ExecutionContext) []models.JobExecutionRecord {
	var jobExecutions []models.JobExecutionRecord

	host, _ := os.Hostname()

	for _, job := range ctx.Config.Jobs {
		if shouldSkip(job.Name, ctx.SubmitOnly) {
			ctx.Logger.Verbose("Skipping job %q due to --only filter", job.Name)
			continue
		}

		startTime := time.Now()
		hlq := strings.Split(ctx.Config.Datasets.JCL, ".")[0]

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
			continue // Move to the next job
		}

		// --- Submission succeeded ---

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

	return jobExecutions
}

// Helper to determine if a job should be skipped based on if its name
// was included in the --only flag during [grace ...] command invocation.
func shouldSkip(jobName string, only []string) bool {
	return len(only) > 0 && !slices.Contains(only, jobName)
}
