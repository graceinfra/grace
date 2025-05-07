package models

import (
	"github.com/google/uuid"
	"github.com/graceinfra/grace/types"
)

// ExecutionSummary holds the overall results of a workflow run.
type ExecutionSummary struct {
	WorkflowId        uuid.UUID       `json:"workflow_id"`
	WorkflowStartTime string          `json:"workflow_start_time"`
	GraceCmd          string          `json:"grace_cmd"`
	ZoweProfile       string          `json:"zowe_profile"`
	HLQ               string          `json:"hlq"`
	Initiator         types.Initiator `json:"initiator"`
	Jobs              []JobSummary    `json:"jobs"`
	OverallStatus     string          `json:"overall_status"` // e.g., "Success", "Failed", "Partial"
	TotalDurationMs   int64           `json:"total_duration_ms"`
	JobsSucceeded     int             `json:"jobs_succeeded"`
	JobsFailed        int             `json:"jobs_failed"`
	FirstFailure      *JobSummary     `json:"first_failure,omitempty"` // Pointer to the first job that failed
}

// JobSummary provides a concise overview of a single job's execution for the summary file.
type JobSummary struct {
	JobName    string  `json:"job_name"`
	JobID      string  `json:"job_id"`
	Type       string  `json:"type"`
	Source     string  `json:"source"`
	Status     string  `json:"status"`      // Final status (e.g., "OUTPUT", "ABEND", "INPUT", "SUBMIT_FAILED")
	ReturnCode *string `json:"return_code"` // Final return code
	SubmitTime string  `json:"submit_time"` // RFC3339
	FinishTime string  `json:"finish_time"` // RFC3339
	DurationMs int64   `json:"duration_ms"`
	LogFile    string  `json:"log_file"` // Relative path to the detailed log file
}

// JobExecutionRecord contains ALL information about a single job's execution.
// This is saved to the individual job log file (e.g., JOBID_JOBNAME.json).
type JobExecutionRecord struct {
	JobName     string          `json:"job_name"`
	JobID       string          `json:"job_id"`
	Type        string          `json:"type"`
	Source      string          `json:"source"`
	RetryIndex  int             `json:"retry_index"`
	GraceCmd    string          `json:"grace_cmd"`
	ZoweProfile string          `json:"zowe_profile"`
	HLQ         string          `json:"hlq"`
	Initiator   types.Initiator `json:"initiator"`
	WorkflowId  uuid.UUID       `json:"workflow_id"`

	// Execution Timing
	SubmitTime string `json:"submit_time"`
	FinishTime string `json:"finish_time"`
	DurationMs int64  `json:"duration_ms"`

	// Zowe Responses (FULL DETAILS HERE)
	SubmitResponse *types.ZoweRfj `json:"submit_response"`
	FinalResponse  *types.ZoweRfj `json:"final_response"`
}
