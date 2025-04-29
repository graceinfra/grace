package models

import "github.com/graceinfra/grace/types"

type ExecutionSummary struct {
	Timestamp   string          `json:"timestamp"`
	GraceCmd    string          `json:"grace_cmd"`
	ZoweProfile string          `json:"zowe_profile"`
	HLQ         string          `json:"hlq"`
	Initiator   types.Initiator `json:"initiator"`
	Jobs        []JobExecution  `json:"jobs"`
}

type JobExecution struct {
	Job    JobInJobExecution `json:"job"`
	Submit ZoweJobData       `json:"submit"`
	Result ZoweJobData       `json:"result"`
}

type JobInJobExecution struct {
	Name       string `json:"name"`
	ID         string `json:"id"`
	Step       string `json:"step"`
	RetryIndex int    `json:"retry_index"`
}

type ZoweJobData struct {
	Status  string  `json:"status"`
	Retcode *string `json:"retcode"` // can be null
	Time    string  `json:"time"`
}
