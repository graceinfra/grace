package jobhandler

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/paths"
	"github.com/graceinfra/grace/internal/resolver"
	"github.com/graceinfra/grace/internal/utils"
	"github.com/graceinfra/grace/internal/zowe"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog"
)

type ZosExecuteHandler struct{}

func (h *ZosExecuteHandler) Type() string {
	return "execute"
}

func (h *ZosExecuteHandler) Validate(job *types.Job, cfg *types.GraceConfig) []string {
	var errs []string

	jobCtx := fmt.Sprintf("job[%s] (type: %s)", job.Name, h.Type())

	if job.Program == nil || *job.Program == "" {
		errs = append(errs, fmt.Sprintf("%s: 'program' field (program to execute) is required and cannot be empty", jobCtx))
	} else {
		if err := utils.ValidatePDSMemberName(*job.Program); err != nil {
			errs = append(errs, fmt.Sprintf("%s: invalid 'program' field (program to execute): %v", jobCtx, err))
		}
	}

	loadLib := resolver.ResolveLoadLib(job, cfg)
	if loadLib == "" {
		errs = append(errs, fmt.Sprintf("%s: a load library must be defined for STEPLIB either globally (datasets.loadlib) or locally (job.datasets.loadlib)", jobCtx))
	}
	// Further validation of the loadLib DSN itself is handled by generic dataset validation in config.go

	return errs
}

func (h *ZosExecuteHandler) Prepare(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) error {
	logger.Debug().Msg("Prepare step for ZosExecuteHandler")

	for _, outputSpec := range job.Outputs {
		if strings.HasPrefix(outputSpec.Path, "zos-temp://") && !outputSpec.Keep {
			physicalDSN, err := paths.ResolvePath(ctx, job, outputSpec.Path)
			if err != nil {
				logger.Error().Err(err).Str("virtual_path", outputSpec.Path).Msg("Failed to resolve temporary output path for pre-deletion.")
				return fmt.Errorf("failed to resolve temporary output path %s for pre-deletion: %w", outputSpec.Path, err)
			}

			logger.Info().Str("dsn", physicalDSN).Msgf("Pre-deleting temporary dataset for output '%s'", outputSpec.Name)
			if err := zowe.DeleteDatasetIfExists(ctx, physicalDSN); err != nil {
				logger.Warn().Err(err).Str("dsn", physicalDSN).Msg("Failed to pre-delete temporary dataset during Prepare phase.")
			}
		}
	}

	return nil
}

func (h *ZosExecuteHandler) Execute(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) *models.JobExecutionRecord {
	logger.Info().Msg("Executing ZosExecuteHandler")
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

	logger.Info().Msg("Submitting JCL for execute job...")
	submitResult, submitErr := zowe.SubmitJob(ctx, job)
	record.SubmitResponse = submitResult

	if submitErr != nil {
		logger.Error().Err(submitErr).Msg("Execute job submission failed")
		record.JobID = "SUBMIT_FAILED"
		record.FinishTime = time.Now().Format(time.RFC3339)
		record.DurationMs = time.Since(startTime).Milliseconds()
		return record
	}

	if submitResult.Data == nil || submitResult.Data.JobID == "" {
		logger.Error().Msg("Execute job submission seemed to succeed but no JobID was returned.")
		record.JobID = "SUBMIT_NO_JOBID"
		record.FinishTime = time.Now().Format(time.RFC3339)
		record.DurationMs = time.Since(startTime).Milliseconds()
		return record
	}
	record.JobID = submitResult.Data.JobID
	jobIdLogger := logger.With().Str("job_id", record.JobID).Logger()
	jobIdLogger.Info().Msgf("✓ Execute job submitted (ID: %s). Waiting for completion...", record.JobID)

	finalResult, waitErr := zowe.WaitForJobCompletion(ctx, record.JobID, job.Name)
	record.FinalResponse = finalResult

	finishTime := time.Now()
	record.FinishTime = finishTime.Format(time.RFC3339)
	record.DurationMs = int64(finishTime.Sub(startTime).Milliseconds())

	if waitErr != nil {
		jobIdLogger.Error().Err(waitErr).Msg("Failed to get final status after waiting for execute job")
	} else if finalResult == nil || finalResult.Data == nil {
		jobIdLogger.Error().Msg("Polling for execute job finished, but final status data is incomplete.")
	} else {
		status := finalResult.Data.Status
		retCodeStr := "null"
		if finalResult.Data.RetCode != nil {
			retCodeStr = *finalResult.Data.RetCode
		}
		// For execute, success is typically CC 0000, but CC 0004 might also be acceptable depending on context.
		// The executor will make the final Succeeded/Failed decision for the JobState.
		if status == "OUTPUT" && (retCodeStr == "CC 0000" || retCodeStr == "CC 0004") {
			jobIdLogger.Info().Msgf("Execute job finished. Status: %s, RC: %s", status, retCodeStr)
		} else {
			jobIdLogger.Warn().Msgf("Execute job finished with potentially non-successful outcome. Status: %s, RC: %s", status, retCodeStr)
		}
	}
	return record
}

func (h *ZosExecuteHandler) Cleanup(ctx *context.ExecutionContext, job *types.Job, execRecord *models.JobExecutionRecord, logger zerolog.Logger) error {
	logger.Debug().Msg("Cleanup step for ZosExecuteHandler (no-op)")
	return nil
}
