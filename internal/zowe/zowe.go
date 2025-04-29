package zowe

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
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

	ctx.Logger.Info(fmt.Sprintf("\n✓ JCL data set submitted for job %s\nFrom: %s\nTo: %s\n", job.Name, res.Data.APIResponse[0].From, res.Data.APIResponse[0].To))
	return nil
}

func SubmitJobAndWatch(ctx *context.ExecutionContext, job *types.Job) models.JobExecution {
	// --- Submit job via Zowe ---

	spinnerMsg := fmt.Sprintf("Submitting job %s ...", strings.ToUpper(job.Name))
	ctx.Logger.StartSpinner(spinnerMsg)

	qualifier := fmt.Sprintf("%s(%s)", ctx.Config.Datasets.JCL, strings.ToUpper(job.Name))
	raw, err := runZowe(ctx, "zos-jobs", "submit", "data-set", qualifier, "--rfj")
	cobra.CheckErr(err)

	ctx.Logger.StopSpinner()

	var jobExecution models.JobExecution

	// We store JSON output in result and use it to construct the final JobExecution struct
	// to be displayed at the end of the job's runtime.
	// TODO: consolidate JobExecution as a subset of LogContext
	var result types.ZoweRfj
	err = json.Unmarshal(raw, &result)
	cobra.CheckErr(err)

	if !result.Success {
		cobra.CheckErr(fmt.Errorf("Zowe CLI submission failed: %s", result.GetError()))
	}

	ctx.Logger.Info("✓ Job %s submitted with ID %s (status: %s)", result.GetJobName(), result.GetJobID(), result.GetStatus())

	jobExecution.Job = models.JobInJobExecution{
		Name:       result.Data.JobName,
		ID:         result.Data.JobID,
		Step:       job.Step,
		RetryIndex: 0,
	}

	jobExecution.Submit = models.ZoweJobData{
		Status:  result.Data.Status,
		Retcode: result.Data.RetCode,
		Time:    time.Now().Format(time.RFC3339),
	}

	waited, err := pollJobStatus(ctx, result.Data.JobID)
	cobra.CheckErr(err)

	jobExecution.Result = models.ZoweJobData{
		Status:  waited.Data.Status,
		Retcode: waited.Data.RetCode,
		Time:    time.Now().Format(time.RFC3339),
	}

	ret := "null"
	if waited.Data.RetCode != nil {
		ret = *waited.Data.RetCode
	}
	ctx.Logger.Info("✓ Job %s completed: %s\n", waited.Data.JobName, ret)

	ctx.Logger.Json(waited)

	err = saveZoweLog(ctx.LogDir, log.NewLogContext(job, result.Data.JobID, result.Data.JobName, ctx.GraceCmd, ctx.Config), waited)
	cobra.CheckErr(err)

	return jobExecution
}
