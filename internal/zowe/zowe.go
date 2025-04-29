package zowe

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/types"
)

// SubmitJob submits a job via Zowe based on a dataset member.
// Returns the initial Zowe response or an error. Does NOT wait for completion.
func SubmitJob(ctx *context.ExecutionContext, job *types.Job) (*types.ZoweRfj, error) {
	qualifier := fmt.Sprintf("%s(%s)", ctx.Config.Datasets.JCL, strings.ToUpper(job.Name))

	// Note: No spinner managed here; caller should manage submit spinner
	rawSubmit, err := runZowe(ctx, "zos-jobs", "submit", "data-set", qualifier, "--rfj")
	if err != nil {
		// Return rawSubmit even on error, as it might contain partial info
		var submitResult types.ZoweRfj
		// Attempt to unmarshal even if runZowe failed, might have partial JSON
		_ = json.Unmarshal(rawSubmit, &submitResult)
		// Prepend context to the error from runZowe
		return &submitResult, fmt.Errorf("zowe submit process failed for job %s: %w", job.Name, err)
	}

	var submitResult types.ZoweRfj
	unmarshalErr := json.Unmarshal(rawSubmit, &submitResult)
	if unmarshalErr != nil {
		errMsg := fmt.Sprintf("failed to parse zowe submit response for job %s: %v, raw: %s", job.Name, unmarshalErr, string(rawSubmit))
		submitResult.Success = false
		submitResult.Message = errMsg
		submitResult.Error = &types.ZoweRfjError{Msg: errMsg}
		return &submitResult, fmt.Errorf(errMsg)
	}

	// Check for logical failure reported by Zowe within the JSON
	if !submitResult.Success || submitResult.Data == nil {
		errMsg := "unknown zowe submission error"
		if !submitResult.Success && submitResult.Error != nil {
			errMsg = fmt.Sprintf("zowe submission failed for job %s: %s", job.Name, submitResult.GetError())
		} else if submitResult.Data == nil {
			errMsg = fmt.Sprintf("zowe submission response for job %s missing 'data' field", job.Name)
		} else {
			errMsg = fmt.Sprintf("zowe submission failed for job %s with unknown reason", job.Name)
		}
		// Return the result structure containing the error details, but also return an error for flow control
		return &submitResult, errors.New(errMsg)
	}

	return &submitResult, nil
}

// GetJobStatus retrieves the current status of a job by its ID.
func GetJobStatus(ctx *context.ExecutionContext, jobId string) (*types.ZoweRfj, error) {
	rawStatus, err := runZowe(ctx, "zos-jobs", "view", "job-status-by-jobid", jobId, "--rfj")
	if err != nil {
		var statusResult types.ZoweRfj
		_ = json.Unmarshal(rawStatus, &statusResult)
		return &statusResult, fmt.Errorf("zowe view status process failed for job %s: %w", jobId, err)
	}

	var statusResult types.ZoweRfj
	unmarshalErr := json.Unmarshal(rawStatus, &statusResult)
	if unmarshalErr != nil {
		errMsg := fmt.Sprintf("failed to parse zowe status response for job %s: %v, raw: %s", jobId, unmarshalErr, string(rawStatus))
		statusResult.Success = false
		statusResult.Message = errMsg
		statusResult.Error = &types.ZoweRfjError{Msg: errMsg}
		return &statusResult, fmt.Errorf(errMsg)
	}

	if !statusResult.Success || statusResult.Data == nil {
		errMsg := "unknown zowe view status error"
		if !statusResult.Success && statusResult.Error != nil {
			errMsg = fmt.Sprintf("zowe view status failed for job %s: %s", jobId, statusResult.GetError())
		} else if statusResult.Data == nil {
			errMsg = fmt.Sprintf("zowe view status response for job %s missing 'data' field", jobId)
		} else {
			errMsg = fmt.Sprintf("zowe view status failed for job %s with unknown reason", jobId)
		}
		return &statusResult, errors.New(errMsg)
	}

	return &statusResult, nil
}

// pollJobStatus checks job status repeatedly until a terminal state is reached.
// Internal helper for WaitForJobCompletion.
func pollJobStatus(ctx *context.ExecutionContext, jobId string) (*types.ZoweRfj, error) {
	// Spinner is managed WITHIN WaitForJobCompletion's loop
	for {
		time.Sleep(2 * time.Second)

		statusResult, err := GetJobStatus(ctx, jobId)
		if err != nil {
			ctx.Logger.Verbose("Polling error for %s: %v", jobId, err)
			// Decide if we should retry or fail hard. For now, let's retry a few times implicitly.
			// A more robust implementation would have explicit retry counts/backoff.
			// If Zowe itself fails repeatedly, we should probably bail out.
			// Let's return the error to let WaitForJobCompletion decide.
			return statusResult, fmt.Errorf("failed to get job status during polling for %s: %w", jobId, err)
		}

		// Log current status for verbose users
		statusText := "UNKNOWN"
		if statusResult != nil && statusResult.Data != nil {
			statusText = statusResult.Data.Status
		}
		spinnerText := fmt.Sprintf("Polling %s ... (status: %s)", jobId, statusText) // Removed \n
		if ctx.Logger.Spinner != nil {
			ctx.Logger.Spinner.Suffix = " " + spinnerText
		}

		// Check for terminal states (Success/Failure)
		if statusResult.Data != nil {
			switch statusResult.Data.Status {
			case "OUTPUT", "ABEND", "JCL ERROR", "SEC ERROR", "SYSTEM FAILURE", "CANCELED":
				return statusResult, nil
			case "INPUT", "ACTIVE", "WAITING":
			default:
				ctx.Logger.Info("⚠️ Job %s polling: Received unknown status '%s'", jobId, statusResult.Data.Status)
			}
		} else {
			// Should not happen if GetJobStatus error handling is correct, but safeguard
			ctx.Logger.Error("⚠️ Polling for job %s: GetJobStatus returned success but no data", jobId)
			// Continue polling cautiously? Or return error?
			return statusResult, fmt.Errorf("polling for job %s received inconsistent status (success but no data)", jobId)
		}

		// TODO: Add a timeout mechanism to prevent infinite polling
	}
}

// WaitForJobCompletion polls a job until it reaches a terminal state.
// Manages its own polling spinner.
func WaitForJobCompletion(ctx *context.ExecutionContext, jobId string) (*types.ZoweRfj, error) {
	spinnerText := fmt.Sprintf("Waiting for job %s to complete...", jobId)
	ctx.Logger.StartSpinner(spinnerText)
	defer ctx.Logger.StopSpinner()

	finalStatus, err := pollJobStatus(ctx, jobId)
	if err != nil {
		// Error occurred during polling (e.g., repeated Zowe failures)
		// finalStatus might contain the last known info or error details from GetJobStatus
		return finalStatus, fmt.Errorf("error waiting for job %s completion: %w", jobId, err)
	}

	// Polling completed successfully, return the final status object
	return finalStatus, nil
}

// UploadJCL uploads the pre-generated JCL file for a job.
// Spinner should be managed by the caller (e.g., cmd/deck.go).
func UploadJCL(ctx *context.ExecutionContext, job *types.Job) (*uploadRes, error) { // Changed to return uploadRes
	jclFileName := job.Name + ".jcl"
	jclPath := filepath.Join(".grace", "deck", jclFileName)
	_, err := os.Stat(jclPath)
	if err != nil {
		return nil, fmt.Errorf("JCL file %s not found. Did you run [grace deck]?", jclPath)
	}

	target := fmt.Sprintf("%s(%s)", ctx.Config.Datasets.JCL, strings.ToUpper(job.Name))

	res, err := UploadFileToDataset(ctx, jclPath, target)
	if err != nil {
		// Error could be from runZowe process error or Zowe logical error from UploadFileToDataset
		return res, fmt.Errorf("failed to upload JCL %s to %s: %w", jclPath, target, err)
	}

	// Return the result structure on success
	return res, nil
}
