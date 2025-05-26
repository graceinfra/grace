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
	"github.com/graceinfra/grace/internal/resolver"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog/log"
)

// SubmitJob submits a job via Zowe. It determines which JCL to submit based on job.JCL.
// If job.JCL starts with "zos://", it submits that DSN.
// Otherwise, it assumes the JCL is in the Grace-managed JCL PDS (datasets.jcl(JOBNAME)).
// Returns the initial Zowe response or an error. Does NOT wait for completion.
func SubmitJob(ctx *context.ExecutionContext, job *types.Job) (*types.ZoweRfj, error) {
	var jclToSubmitDSN string
	var jclSourceDescription string

	if strings.HasPrefix(job.JCL, "zos://") {
		jclToSubmitDSN = strings.TrimPrefix(job.JCL, "zos://")
		jclSourceDescription = fmt.Sprintf("existing mainframe JCL ('%s')", job.JCL)
	} else {
		// Default: Grace manages the JCL (either its own default template or user's file:// template,
		// which would have been uploaded to the job's standard JCL PDS member by DeckAndUpload)
		jclPDS := resolver.ResolveJCLDataset(job, ctx.Config)
		if jclPDS == "" {
			errMsg := fmt.Sprintf("JCL PDS not defined in datasets configuration for job %s", job.Name)
			log.Error().Str("job_name", job.Name).Msg(errMsg)
			return &types.ZoweRfj{
				Success: false,
				Error: &types.ZoweRfjError{
					Msg: errMsg,
				},
			}, fmt.Errorf(errMsg)
		}

		jclToSubmitDSN = fmt.Sprintf("%s(%s)", jclPDS, strings.ToUpper(job.Name))

		if strings.HasPrefix(job.JCL, "file://") {
			jclSourceDescription = fmt.Sprintf("user-provided local JCL ('%s', submitted from %s)", job.JCL, jclToSubmitDSN)
		} else {
			jclSourceDescription = fmt.Sprintf("Grace-generated JCL (submitted from %s)", jclToSubmitDSN)
		}
	}

	logger := log.With().
		Str("workflow_id", ctx.WorkflowId.String()).
		Str("job_name", job.Name).
		Str("jcl_dsn_to_submit", jclToSubmitDSN).
		Logger()

	logger.Debug().Msgf("Preparing to submit JCL via Zowe, using %s.", jclSourceDescription)

	rawSubmit, err := runZowe(ctx, "zos-jobs", "submit", "data-set", jclToSubmitDSN, "--rfj")
	if err != nil {
		// Return rawSubmit even on error, as it might contain partial info
		var submitResult types.ZoweRfj
		_ = json.Unmarshal(rawSubmit, &submitResult)

		logger.Error().Err(err).Msg("Zowe submit process execution failed.")

		return &submitResult, fmt.Errorf("zowe submit process failed for job %s: %w", job.Name, err)
	}

	var submitResult types.ZoweRfj
	unmarshalErr := json.Unmarshal(rawSubmit, &submitResult)
	if unmarshalErr != nil {
		errMsg := fmt.Sprintf("failed to parse zowe submit response for job %s: %v, raw: %s", job.Name, unmarshalErr, string(rawSubmit))

		submitResult.Success = false
		submitResult.Message = errMsg
		submitResult.Error = &types.ZoweRfjError{Msg: errMsg}

		logger.Error().Str("raw_output", string(rawSubmit)).Err(unmarshalErr).Msg("Failed to parse Zowe submit response.")
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
		logger.Warn().Msg(errMsg)
		return &submitResult, errors.New(errMsg)
	}

	logger.Debug().Str("returned_jobid", submitResult.Data.JobID).Msg("Zowe submit call successful.")
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
func pollJobStatus(ctx *context.ExecutionContext, jobId, jobName string) (*types.ZoweRfj, error) {
	logger := log.With().
		Str("workflow_id", ctx.WorkflowId.String()).
		Str("job_name", jobName).
		Str("job_id", jobId).
		Logger()

	for {
		time.Sleep(2 * time.Second)

		statusResult, err := GetJobStatus(ctx, jobId)
		if err != nil {
			logger.Debug().Err(err).Msg("Polling error")
			// Decide if we should retry or fail hard. For now, let's retry a few times implicitly.
			// A more robust implementation would have explicit retry counts/backoff.
			// If Zowe itself fails repeatedly, we should probably bail out.
			// Let's return the error to let WaitForJobCompletion decide.
			return statusResult, fmt.Errorf("failed to get job status during polling for %s: %w", jobId, err)
		}

		statusText := "UNKNOWN"
		if statusResult != nil && statusResult.Data != nil {
			statusText = statusResult.Data.Status
		}
		logger.Info().Msgf("Polling ... (status: %s)", statusText)
		// Check for terminal states (Success/Failure)
		if statusResult.Data != nil {
			switch statusResult.Data.Status {
			case "OUTPUT", "ABEND", "JCL ERROR", "SEC ERROR", "SYSTEM FAILURE", "CANCELED":
				return statusResult, nil
			case "INPUT", "ACTIVE", "WAITING":
			default:
				logger.Info().Msgf("Received unknown status '%s'", statusResult.Data.Status)
			}
		} else {
			// Should not happen if GetJobStatus error handling is correct, but safeguard
			logger.Error().Msg("Polling: GetJobStatus returned success but no data")
			// Continue polling cautiously? Or return error?
			return statusResult, fmt.Errorf("polling for job %s received inconsistent status (success but no data)", jobId)
		}

		// TODO: Add a timeout mechanism to prevent infinite polling
	}
}

// WaitForJobCompletion polls a job until it reaches a terminal state.
func WaitForJobCompletion(ctx *context.ExecutionContext, jobId, jobName string) (*types.ZoweRfj, error) {
	finalStatus, err := pollJobStatus(ctx, jobId, jobName)
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
func UploadJCL(ctx *context.ExecutionContext, job *types.Job) (*UploadRes, error) { // Changed to return uploadRes
	jclFileName := job.Name + ".jcl"
	jclPath := filepath.Join(".grace", "deck", jclFileName)
	_, err := os.Stat(jclPath)
	if err != nil {
		return nil, fmt.Errorf("JCL file %s not found. Did you run [grace deck]?", jclPath)
	}

	target := fmt.Sprintf("%s(%s)", ctx.Config.Datasets.JCL, strings.ToUpper(job.Name))

	res, err := UploadFileToDataset(ctx, jclPath, target, "text")
	if err != nil {
		// Error could be from runZowe process error or Zowe logical error from UploadFileToDataset
		return res, fmt.Errorf("failed to upload JCL %s to %s: %w", jclPath, target, err)
	}

	// Return the result structure on success
	return res, nil
}
