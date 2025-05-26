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

type ZosLinkeditHandler struct{}

func (h *ZosLinkeditHandler) Type() string {
	return "linkedit"
}

func (h *ZosLinkeditHandler) Validate(job *types.Job, cfg *types.GraceConfig) []string {
	var errs []string

	jobCtx := fmt.Sprintf("job[%s] (type: %s)", job.Name, h.Type())

	if len(job.Inputs) == 0 {
		errs = append(errs, fmt.Sprintf("%s: requires at least one input (e.g. SYSIN)", jobCtx))
	}

	if job.Program == nil && *job.Program == "" {
		errs = append(errs, fmt.Sprintf("%s: 'program' field (output member name) is required and cannot be empty", jobCtx))
	} else {
		if err := utils.ValidatePDSMemberName(*job.Program); err != nil {
			errs = append(errs, fmt.Sprintf("%s: invalid 'program' field (output member name): %v", jobCtx, err))
		}
	}

	// Check if loadlib is defined either globally or locally (in the job)
	loadLib := resolver.ResolveLoadLib(job, cfg)
	if loadLib == "" {
		errs = append(errs, fmt.Sprintf("%s: a load library must be defined either globally (datasets.loadlib) or locally (job.datasets.loadlib)", jobCtx))
	}

	return errs
}

func (h *ZosLinkeditHandler) Prepare(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) error {
	logger.Debug().Msg("Prepare step for ZosLinkeditHandler")

	if err := prepareZosJobInputs(ctx, job, logger); err != nil {
		return fmt.Errorf("failed to prepare inputs for linkedit job %s: %w", job.Name, err)
	}

	// For linkedit, the primary output is the member in the load library.
	// We don't typically pre-delete the load library member itself, as linkedit replaces it.
	// However, if there were other temp:// outputs defined for a linkedit job (e.g., SYSPRINT to a temp dataset),
	// those would be handled here.

	for _, outputSpec := range job.Outputs {
		if strings.HasPrefix(outputSpec.Path, "zos-temp://") && !outputSpec.Keep {
			physicalDSN, err := paths.ResolvePath(ctx, job, outputSpec.Path, nil)
			if err != nil {
				logger.Error().Err(err).Str("virtual_path", outputSpec.Path).Msg("Failed to resolve temporary output path for pre-deletion.")
				return fmt.Errorf("failed to resolve temporary output path %s for pre-deletion: %w", outputSpec.Path, err)
			}

			logger.Info().Str("dsn", physicalDSN).Msgf("Pre-deleting temporary dataset for output '%s'", outputSpec.Name)

			if err := zowe.DeleteDatasetIfExists(ctx, physicalDSN); err != nil {
				logger.Warn().Err(err).Str("dsn", physicalDSN).Msg("Failed to pre-delete temporary dataset during Prepare phase.")
				// Non-fatal for now, link step might still work if it can overwrite.
			}
		}
	}

	return nil
}

func (h *ZosLinkeditHandler) Execute(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) *models.JobExecutionRecord {
	logger.Info().Msg("Executing ZosLinkeditHandler")
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

	logger.Info().Msg("Submitting JCL for linkedit job...")
	submitResult, submitErr := zowe.SubmitJob(ctx, job)
	record.SubmitResponse = submitResult

	if submitErr != nil {
		logger.Error().Err(submitErr).Msg("Linkedit job submission failed")
		record.JobID = "SUBMIT_FAILED"
		record.FinishTime = time.Now().Format(time.RFC3339)
		record.DurationMs = time.Since(startTime).Milliseconds()
		return record
	}

	if submitResult.Data == nil || submitResult.Data.JobID == "" {
		logger.Error().Msg("Linkedit job submission seemed to succeed but no JobID was returned.")
		record.JobID = "SUBMIT_NO_JOBID"
		record.FinishTime = time.Now().Format(time.RFC3339)
		record.DurationMs = time.Since(startTime).Milliseconds()
		return record
	}

	record.JobID = submitResult.Data.JobID
	jobIdLogger := logger.With().Str("job_id", record.JobID).Logger()
	jobIdLogger.Info().Msgf("✓ Linkedit job submitted (ID: %s). Waiting for completion...", record.JobID)

	finalResult, waitErr := zowe.WaitForJobCompletion(ctx, record.JobID, job.Name)
	record.FinalResponse = finalResult

	finishTime := time.Now()
	record.FinishTime = finishTime.Format(time.RFC3339)
	record.DurationMs = int64(finishTime.Sub(startTime).Milliseconds())

	if waitErr != nil {
		jobIdLogger.Error().Err(waitErr).Msg("Failed to get final status after waiting for linkedit job")
	} else if finalResult == nil || finalResult.Data == nil {
		jobIdLogger.Error().Msg("Polling for linkedit job finished, but final status data is incomplete.")
	} else {
		status := finalResult.Data.Status
		retCodeStr := "null"
		if finalResult.Data.RetCode != nil {
			retCodeStr = *finalResult.Data.RetCode
		}
		if status == "OUTPUT" && (retCodeStr == "CC 0000" || retCodeStr == "CC 0004") { // Linkedit often RC 0 or 4
			jobIdLogger.Info().Msgf("Linkedit job finished. Status: %s, RC: %s", status, retCodeStr)
		} else {
			jobIdLogger.Warn().Msgf("Linkedit job finished with potentially non-successful outcome. Status: %s, RC: %s", status, retCodeStr)
		}
	}

	return record
}

func (h *ZosLinkeditHandler) Cleanup(ctx *context.ExecutionContext, job *types.Job, execRecord *models.JobExecutionRecord, logger zerolog.Logger) error {
	logger.Debug().Msg("Cleanup step for ZosLinkeditHandler")
	if err := cleanupZosJobOutputs(ctx, job, execRecord, logger); err != nil {
		logger.Error().Err(err).Msg("Error during ZosLinkeditHandler output cleanup.")
	}
	return nil
}
