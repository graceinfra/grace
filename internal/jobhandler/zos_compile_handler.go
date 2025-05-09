package jobhandler

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/paths"
	"github.com/graceinfra/grace/internal/zowe"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ZosCompileHandler struct{}

func (h *ZosCompileHandler) Type() string {
	return "compile"
}

func (h *ZosCompileHandler) Validate(job *types.Job, cfg *types.GraceConfig) []string {
	var errs []string

	jobCtx := fmt.Sprintf("job[%s] (type: %s)", job.Name, h.Type())

	if len(job.Inputs) == 0 {
		errs = append(errs, fmt.Sprintf("%s: requires at least one input (e.g. SYSIN)", jobCtx))
	}

	if len(job.Outputs) == 0 {
		errs = append(errs, fmt.Sprintf("%s: requires at least one output (e.g. SYSLIN)", jobCtx))
	}

	if job.Program != nil && *job.Program != "" {
		errs = append(errs, fmt.Sprintf("%s: 'program', field must not be set for 'compile' jobs", jobCtx))
	}

	return errs
}

func (h *ZosCompileHandler) Prepare(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) error {
	logger.Debug().Msg("Prepare step for ZosCompileHandler")
	for _, outputSpec := range job.Outputs {
		if strings.HasPrefix(outputSpec.Path, "zos-temp://") && !outputSpec.Keep {
			physicalDSN, err := paths.ResolvePath(ctx, job, outputSpec.Path)
			if err != nil {
				logger.Error().Err(err).Str("virtual_path", outputSpec.Path).Msg("Failed to resolve temporary output path for pre-deletion.")
				// This could be an error preventing the job from running, as the output DSN is unknown
				return fmt.Errorf("failed to resolve temporary output path %s for pre-deletion: %w", outputSpec.Path, err)
			}
			logger.Info().Str("dsn", physicalDSN).Msgf("Pre-deleting temporary dataset for output '%s'", outputSpec.Name)

			if err := zowe.DeleteDatasetIfExists(ctx, physicalDSN); err != nil {
				logger.Error().Err(err).Str("dsn", physicalDSN).Msg("Failed to pre-delete temporary dataset during Prepare phase. This might cause the job to fail if the dataset cannot be created/overwritten.")
				// TODO: consider making this fatal if it fucks up downstream
			}
		}
	}

	return nil
}

func (h *ZosCompileHandler) Execute(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) *models.JobExecutionRecord {
	logger.Info().Msg("Executing ZosCompileHandler")
	startTime := time.Now()

	hlq := ""
	if ctx.Config.Datasets.JCL != "" {
		parts := strings.Split(ctx.Config.Datasets.JCL, ".")
		if len(parts) > 0 {
			hlq = parts[0]
		}
	}

	host, _ := os.Hostname()

	record := &models.JobExecutionRecord{
		JobName:     job.Name,
		JobID:       "PENDING_SUBMIT",
		Type:        h.Type(),
		GraceCmd:    ctx.GraceCmd,
		ZoweProfile: ctx.Config.Config.Profile,
		HLQ:         hlq,
		Initiator: types.Initiator{
			Type:   "user",
			Id:     os.Getenv("USER"),
			Tenant: host,
		},
		WorkflowId: ctx.WorkflowId,
		SubmitTime: startTime.Format(time.RFC3339),
	}

	logger.Info().Msg("Submitting JCL for compile job...")
	submitResult, submitErr := zowe.SubmitJob(ctx, job)
	record.SubmitResponse = submitResult

	if submitErr != nil {
		log.Error().Err(submitErr).Msg("Compile job submission failed")
		record.JobID = "SUBMIT_FAILED"
		record.FinishTime = time.Now().Format(time.RFC3339)
		record.DurationMs = time.Since(startTime).Milliseconds()
		return record
	}

	if submitResult.Data == nil || submitResult.Data.JobID == "" {
		logger.Error().Msg("Compile job submission seemed to succeed but no JobID was returned.")
		record.JobID = "SUBMIT_NO_JOBID"
		record.FinishTime = time.Now().Format(time.RFC3339)
		record.DurationMs = time.Since(startTime).Milliseconds()
		return record
	}

	record.JobID = submitResult.Data.JobID
	jobIdLogger := logger.With().Str("job_id", record.JobID).Logger()
	jobIdLogger.Info().Msgf("✓ Compile job submitted (ID: %s). Waiting for completion...", record.JobID)

	finalResult, waitErr := zowe.WaitForJobCompletion(ctx, record.JobID, job.Name)
	record.FinalResponse = finalResult

	finishTime := time.Now()
	record.FinishTime = finishTime.Format(time.RFC3339)
	record.DurationMs = int64(finishTime.Sub(startTime).Milliseconds())

	if waitErr != nil {
		jobIdLogger.Error().Err(waitErr).Msg("Failed to get final status after waiting for compile job")
	} else if finalResult == nil || finalResult.Data == nil {
		jobIdLogger.Error().Msg("Polling for compile job finished, but final status data is incomplete.")
	} else {
		status := finalResult.Data.Status
		retCodeStr := "null"

		if finalResult.Data.RetCode != nil {
			retCodeStr = *finalResult.Data.RetCode
		}

		// Generic success/failure logging. The executor will set JobState based on this record.
		if status == "OUTPUT" && (retCodeStr == "CC 0000" || retCodeStr == "CC 0004") {
			jobIdLogger.Info().Msgf("Compile job finished. Status: %s, RC: %s", status, retCodeStr)
		} else {
			jobIdLogger.Warn().Msgf("Compile job finished with potentially non-successful outcome. Status: %s, RC: %s", status, retCodeStr)
		}
	}

	return record
}

func (h *ZosCompileHandler) Cleanup(ctx *context.ExecutionContext, job *types.Job, execRecord *models.JobExecutionRecord, logger zerolog.Logger) error {
	logger.Debug().Msg("Cleanup step for ZosCompileHandler (no-op)")
	return nil
}
