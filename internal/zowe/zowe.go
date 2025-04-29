package zowe

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/types"
	"github.com/spf13/cobra"
)

func UploadJCL(ctx *context.ExecutionContext, job *types.Job) error {
	jclPath := filepath.Join(".grace", "deck", job.Name+".jcl")
	_, err := os.Stat(jclPath)
	if err != nil {
		return fmt.Errorf("unable to resolve %s. Did you run [grace deck]?", jclPath)
	}

	target := fmt.Sprintf("%s(%s)", ctx.Config.Datasets.JCL, strings.ToUpper(job.Name))

	res, err := UploadFileToDataset(ctx, jclPath, target)
	if err != nil {
		return err
	}

	ctx.Logger.Info(fmt.Sprintf("✓ JCL data set submitted for job %s\n", job.Name))
	ctx.Logger.Verbose(fmt.Sprintf("From: %s\nTo: %s\n", res.Data.APIResponse[0].From, res.Data.APIResponse[0].To))
	return nil
}

// SubmitJobAndWatch submits a job, waits for completion, and returns a detailed JobExecutionRecord
func SubmitJobAndWatch(ctx *context.ExecutionContext, job *types.Job) models.JobExecutionRecord {
	startTime := time.Now()

	host, _ := os.Hostname()
	hlq := strings.Split(ctx.Config.Datasets.JCL, ".")[0]
	if hlq == "" {
		cobra.CheckErr(fmt.Errorf("unable to resolve JCL dataset identifier. Is the 'jcl' field filled out under 'datasets' in grace.yml?"))
	}

	record := models.JobExecutionRecord{
		JobName:     job.Name,
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
	}

	// --- Submit job via Zowe ---

	spinnerMsg := fmt.Sprintf("Submitting job %s ...", strings.ToUpper(job.Name))
	ctx.Logger.StartSpinner(spinnerMsg)

	qualifier := fmt.Sprintf("%s(%s)", ctx.Config.Datasets.JCL, strings.ToUpper(job.Name))
	rawSubmit, err := runZowe(ctx, "zos-jobs", "submit", "data-set", qualifier, "--rfj")
	ctx.Logger.StopSpinner()

	// --- Process submit response ---

	var submitResult types.ZoweRfj
	unmarshalErr := json.Unmarshal(rawSubmit, &submitResult)
	if unmarshalErr != nil {
		// If unmarshalling fails, create a synthetic error response
		record.JobID = "SUBMIT_UNMARSHAL_ERROR"
		errMsg := fmt.Sprintf("Failed to run/parse submit command: %v, Unmarshal error: %v, Raw: %s", err, unmarshalErr, string(rawSubmit))
		record.SubmitResponse = &types.ZoweRfj{ // Store some error info
			Success: false,
			Message: errMsg,
			Error:   &types.ZoweRfjError{Msg: errMsg},
		}
		ctx.Logger.Error("⚠️ Job %s submission failed: %s", record.JobName, errMsg)
	} else {
		// Store the actual submit response
		record.SubmitResponse = &submitResult
	}

	// Handle Zowe command execution error OR Zowe logical failure OR missing data
	if err != nil || !submitResult.Success || submitResult.Data == nil {
		errMsg := "Unknown submission error"
		if err != nil {
			errMsg = fmt.Sprintf("Command execution failed: %v", err)
		} else if !submitResult.Success {
			errMsg = fmt.Sprintf("Zowe submission failed: %s", submitResult.GetError())
		} else { // submitResult.Data == nil
			errMsg = "Zowe submission response missing 'data' field."
		}

		// Update record for failure case
		if record.JobID == "" { // Avoid overwriting specific unmarshal error ID
			record.JobID = "SUBMIT_FAILED"
		}
		record.FinishTime = time.Now().Format(time.RFC3339)
		record.DurationMs = time.Since(startTime).Milliseconds()

		ctx.Logger.Error("⚠️ Job %s submission failed: %s", record.JobName, errMsg)

		// Save the incomplete record and return
		_ = saveJobExecutionRecord(ctx.LogDir, record) // Log the failure attempt
		return record
	}

	// --- Submission succeeded logically ---

	record.JobID = submitResult.Data.JobID
	ctx.Logger.Info("✓ Job %s submitted with ID %s (status: %s)", record.JobName, record.JobID, submitResult.Data.Status)

	// --- Poll for job completion ---

	finalResult, pollErr := pollJobStatus(ctx, record.JobID)

	finishTime := time.Now()
	record.FinishTime = finishTime.Format(time.RFC3339)
	record.DurationMs = int64(finishTime.Sub(startTime).Milliseconds())
	record.FinalResponse = finalResult

	// --- Handle polling outcome ---

	if pollErr != nil {
		// Polling failed, log it, but we still have the submit data and maybe partial final data
		ctx.Logger.Error("⚠️ Failed to poll final status for job %s (%s): %v", record.JobName, record.JobID, pollErr)
		// The record already contains SubmitResponse and potentially FinalResponse with error details from Zowe
	} else if finalResult != nil && finalResult.Data != nil {
		// Polling succeeded, log final status and RC
		ret := "null"
		if finalResult.Data.RetCode != nil {
			ret = *finalResult.Data.RetCode
		}
		ctx.Logger.Info("✓ Job %s (%s) completed: Status %s, RC %s", record.JobName, record.JobID, finalResult.Data.Status, ret)
	} else {
		// Polling finished without error, but finalResult or its data is nil (unexpected)
		ctx.Logger.Error("⚠️ Polling for job %s (%s) finished, but final status data is incomplete.", record.JobName, record.JobID)
	}

	ctx.Logger.Json(record)

	// --- Save individual job log ---

	// Save the complete record to JOBID_JOBNAME.json
	saveErr := saveJobExecutionRecord(ctx.LogDir, record)
	if saveErr != nil {
		ctx.Logger.Error("⚠️ Failed to save detailed log for job %s (%s): %v", record.JobName, record.JobID, saveErr)
		// Continue execution, but log the error
	}

	// Return the full execution record
	return record
}
