package runner

import (
	"slices"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/zowe"
)

// RunWorkflow runs a workflow end-to-end. It covers submitting + watching job executions.
func RunWorkflow(ctx *context.ExecutionContext) []models.JobExecution {
	var jobExecutions []models.JobExecution

	for _, job := range ctx.Config.Jobs {
		if shouldSkip(job.Name, ctx.SubmitOnly) {
			continue
		}

		jobExecution := zowe.SubmitJobAndWatch(ctx, job)
		jobExecutions = append(jobExecutions, jobExecution)
	}

	return jobExecutions
}

// Helper to determine if a job should be skipped based on if its name
// was included in the --only flag during [grace ...] command invocation.
func shouldSkip(jobName string, only []string) bool {
	return len(only) > 0 && !slices.Contains(only, jobName)
}
