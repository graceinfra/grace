package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/graceinfra/grace/types"
	"github.com/spf13/cobra"
)

func RunWorkflow(graceCfg types.GraceConfig, logDir string, wantSpool, wantJSON, verbose, quiet, useSpinner bool, submitOnly []string) []types.JobExecution {
	var jobExecutions []types.JobExecution
	for _, job := range graceCfg.Jobs {
		if shouldSkip(job, submitOnly) {
			continue
		}

		uploadJCL(job, graceCfg, verbose, quiet)
	}

	for _, job := range graceCfg.Jobs {
		if shouldSkip(job, submitOnly) {
			continue
		}

		jobExecution := submitAndWatch(job, graceCfg, logDir, wantSpool, wantJSON, quiet, verbose, useSpinner)
		jobExecutions = append(jobExecutions, jobExecution)
	}

	return jobExecutions
}

func shouldSkip(job types.Job, only []string) bool {
	return len(only) > 0 && !slices.Contains(only, job.Name)
}

func uploadJCL(job types.Job, cfg types.GraceConfig, verbose, quiet bool) {
	jclPath := filepath.Join(".grace", "deck", job.Name+".jcl")
	_, err := os.Stat(jclPath)
	if err != nil {
		cobra.CheckErr(fmt.Errorf("unable to resolve %s. Did you run [grace deck]?", jclPath))
	}

	qualifier := fmt.Sprintf("%s(%s)", cfg.Datasets.JCL, strings.ToUpper(job.Name))

	VerboseLog(verbose, "Uploading job %s to %s", job.Name, qualifier)
	_, err = RunZowe(verbose, quiet, "zos-files", "upload", "file-to-data-set", jclPath, qualifier)
	cobra.CheckErr(err)
	VerboseLog(verbose, "✓ Upload complete")
}

func submitAndWatch(job types.Job, cfg types.GraceConfig, logDir string, wantSpool, wantJSON, quiet, verbose, useSpinner bool) types.JobExecution {
	qualifier := fmt.Sprintf("%s(%s)", cfg.Datasets.JCL, strings.ToUpper(job.Name))

	zArgs := []string{"zos-jobs", "submit", "data-set", qualifier}
	if wantSpool {
		zArgs = append(zArgs, "--vasc")
	} else {
		zArgs = append(zArgs, "--rfj")
	}

	VerboseLog(!quiet, fmt.Sprintf("Submitting job %s ...", strings.ToUpper(job.Name)))

	// Pass 'quiet' param as true so we don't immediately print JSON response.
	// If --json,  ParseAndPrintJobResult will print JSON res.
	// If --spool, spool output will be printed in the if spool { ... }  block
	out, err := RunZowe(verbose, true, zArgs...)
	cobra.CheckErr(err)

	if wantSpool {
		var jobExecution types.JobExecution

		jobId, jobName := ParseSpoolMeta(out)
		err := os.WriteFile(filepath.Join(logDir, jobId+"_"+strings.ToUpper(jobName)+".spool.log"), out, 0644)
		if err != nil {
			VerboseLog(true, "⚠️ Failed to write spool log: %v", err)
		}
		_ = SaveZoweLog(logDir, NewLogContext(job, jobId, jobName, "run", cfg), "spooled")

		jobExecution.Job = types.JobInJobExecution{
			Name:       job.Name,
			ID:         jobId,
			Step:       job.Step,
			RetryIndex: 0,
			Spooled:    true,
		}

		jobExecution.Submit = types.ZoweJobData{
			Status:  "",
			Retcode: nil,
			Time:    time.Now().Format(time.RFC3339),
		}

		waited, err := waitAndPoll(jobId, false, useSpinner)
		cobra.CheckErr(err)

		jobExecution.Result = types.ZoweJobData{
			Status:  waited.Data.Status,
			Retcode: waited.Data.RetCode,
			Time:    time.Now().Format(time.RFC3339),
		}

		fmt.Fprintf(os.Stderr, string(out)+"\n")

		return jobExecution
	}

	var jobExecution types.JobExecution

	// Set wantJSON and quiet params so this does not print.
	// We store JSON output in result and construct final JobExecution struct
	// to be displayed at the end of the job's runtime.
	result, err := ParseAndPrintJobResult(out, false, true)
	cobra.CheckErr(err)

	jobExecution.Job = types.JobInJobExecution{
		Name:       result.Data.JobName,
		ID:         result.Data.JobID,
		Step:       job.Step,
		RetryIndex: 0,
		Spooled:    false,
	}

	jobExecution.Submit = types.ZoweJobData{
		Status:  result.Data.Status,
		Retcode: result.Data.RetCode,
		Time:    time.Now().Format(time.RFC3339),
	}

	waited, err := waitAndPoll(result.Data.JobID, verbose, useSpinner)
	cobra.CheckErr(err)

	jobExecution.Result = types.ZoweJobData{
		Status:  waited.Data.Status,
		Retcode: waited.Data.RetCode,
		Time:    time.Now().Format(time.RFC3339),
	}

	// Always display this unless --json
	ret := "null"
	if waited.Data.RetCode != nil {
		ret = *waited.Data.RetCode
	}
	VerboseLog(!wantJSON, "✓ Job %s completed: %s\n", waited.Data.JobName, ret)

	if wantJSON {
		b, err := json.MarshalIndent(jobExecution, "", "  ")
		cobra.CheckErr(err)
		VerboseLog(wantJSON, string(b))
	}

	err = SaveZoweLog(logDir, NewLogContext(job, result.Data.JobID, result.Data.JobName, "run", cfg), waited)
	cobra.CheckErr(err)

	return jobExecution
}

func waitAndPoll(jobId string, verbose, useSpinner bool) (*types.ZoweRfj, error) {
	args := []string{"zos-jobs", "view", "job-status-by-jobid", jobId, "--rfj"}

	s := spinner.New(spinner.CharSets[43], 100*time.Millisecond)
	if useSpinner {
		s.Start()
	}

	for {
		time.Sleep(2 * time.Second)
		out, err := RunZowe(verbose, true, args...)
		if err != nil {
			return nil, err
		}

		var status types.ZoweRfj
		if err := json.Unmarshal(out, &status); err != nil {
			VerboseLog(true, "⚠️ Failed to parse job status: %v", err)
			continue
		}

		if status.Data.Status == "OUTPUT" || status.Data.RetCode != nil {
			if useSpinner {
				s.Stop()
			}
			return &status, nil
		}

		VerboseLog(verbose, "Polling %s... (status: %s)", jobId, status.Data.Status)
	}
}

func PrintJobResult(result types.JobResult, raw []byte, jsonMode, quiet bool) {
	if jsonMode {
		_, _ = os.Stdout.Write(raw)
		return
	}
	if quiet {
		return
	}
	fmt.Printf("\n\u2713 Job %s submitted as %s (status: %s)\n", result.GetJobName(), result.GetJobID(), result.GetStatus())
}

func ParseAndPrintJobResult(raw []byte, jsonMode, quiet bool) (types.ZoweRfj, error) {
	var result types.ZoweRfj
	err := json.Unmarshal(raw, &result)
	if err != nil {
		return result, fmt.Errorf("invalid Zowe JSON: %w", err)
	}
	if !result.Success {
		return result, fmt.Errorf("Zowe CLI submission failed")
	}
	PrintJobResult(result, raw, jsonMode, quiet)
	return result, nil
}

// NewLogContext builds a reusable log context
func NewLogContext(job types.Job, jobId, jobName, graceCmd string, cfg types.GraceConfig) types.LogContext {
	hlq := strings.Split(cfg.Datasets.JCL, ".")[0]
	host, _ := os.Hostname()

	return types.LogContext{
		JobID:       jobId,
		JobName:     jobName,
		Step:        job.Step,
		RetryIndex:  0,
		GraceCmd:    graceCmd,
		ZoweProfile: cfg.Config.Profile,
		HLQ:         hlq,
		Timestamp:   time.Now().Format(time.RFC3339),
		Initiator: types.Initiator{
			Type:   "user",
			Id:     os.Getenv("USER"),
			Tenant: host,
		},
	}
}
