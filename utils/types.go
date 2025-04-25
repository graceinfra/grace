package utils

type GraceConfig struct {
	Config struct {
		Profile string `yaml:"profile"`
	} `yaml:"config"`

	Datasets struct {
		PDS     string `yaml:"pds"`
		JCL     string `yaml:"jcl"`
		SRC     string `yaml:"src"`
		LoadLib string `yaml:"loadlib"`
	} `yaml:"datasets"`

	Jobs []Job `yaml:"jobs"`
}

type Job struct {
	Name     string `yaml:"name"`
	Step     string `yaml:"step"`
	Source   string `yaml:"source"`
	Template string `yaml:"template,omitempty"`
	Wait     bool   `yaml:"wait,omitempty"`
	View     string `yaml:"view"`
	Retries  int    `yaml:"retries"`
}

type JobResult interface {
	GetJobName() string
	GetJobID() string
	GetStatus() string
}

type ZoweConfig struct {
	Schema    string                 `json:"$schema"`
	Profiles  map[string]ZoweProfile `json:"profiles"`
	Defaults  map[string]string      `json:"defaults"`
	AutoStore bool                   `json:"autoStore"`
}

type ZoweProfile struct {
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
	Secure     []string       `json:"secure"`
}

type ZoweRfj struct {
	Success  bool          `json:"success"`
	ExitCode int           `json:"exitCode"`
	Message  string        `json:"message"`
	Stdout   string        `json:"stdout"`
	Stderr   string        `json:"stderr"`
	Data     ZoweRfjData   `json:"data,omitempty"`
	Error    *ZoweRfjError `json:"error,omitempty"`
}

func (r ZoweRfj) GetJobName() string { return r.Data.JobName }
func (r ZoweRfj) GetJobID() string   { return r.Data.JobID }
func (r ZoweRfj) GetStatus() string  { return r.Data.Status }

type ZoweRfjData struct {
	JobID     string  `json:"jobid"`
	JobName   string  `json:"jobname"`
	Status    string  `json:"status"`
	Class     string  `json:"class"`
	Phase     int     `json:"phase"`
	PhaseName string  `json:"phase-name"`
	Subsystem string  `json:"subsystem"`
	Owner     string  `json:"owner"`
	Type      string  `json:"type"`
	URL       string  `json:"url"`
	FilesURL  string  `json:"files-url"`
	RetCode   *string `json:"retcode"` // can be null
}

type ZoweRfjError struct {
	Msg         string           `json:"msg"`
	CauseErrors string           `json:"causeErrors"` // JSON string, we could parse this further
	Source      string           `json:"source"`
	ErrorCode   int              `json:"errorCode"`
	Protocol    string           `json:"protocol"`
	Port        int              `json:"port"`
	Host        string           `json:"host"`
	BasePath    string           `json:"basePath"`
	HTTPStatus  int              `json:"httpStatus"`
	Payload     map[string]any   `json:"payload"`
	Headers     []map[string]any `json:"headers"`
	Resource    string           `json:"resource"`
	Request     string           `json:"request"`
	Additional  string           `json:"additionalDetails"`
}

type LogContext struct {
	JobID       string    `json:"job_id"`   // "JOB02848"
	JobName     string    `json:"job_name"` // "HELLO"
	Step        string    `json:"step"`     // "execute", "compile"
	RetryIndex  int       `json:"retry_index"`
	GraceCmd    string    `json:"grace_cmd"`    // submit", "run"
	ZoweProfile string    `json:"zowe_profile"` // "zosmf", "ssh", "tso"
	HLQ         string    `json:"hlq"`          // "IBMUSER.GRC"
	Timestamp   string    `json:"timestamp"`    // RFC3339 format
	Initiator   Initiator `json:"initiator"`
}

type Initiator struct {
	Type   string `json:"type"`   // "user", "service", "ci"
	Id     string `json:"id"`     // "arnav", "grace-runner-01234"
	Tenant string `json:"tenant"` // "ca-dmv"
}

type GraceJobLog struct {
	LogContext
	Result any `json:"result"`
}

type ExecutionSummary struct {
	Timestamp   string         `json:"timestamp"`
	GraceCmd    string         `json:"grace_cmd"`
	ZoweProfile string         `json:"zowe_profile"`
	HLQ         string         `json:"hlq"`
	Initiator   Initiator      `json:"initiator"`
	Jobs        []JobExecution `json:"jobs"`
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
	Spooled    bool   `json:"spooled"`
}

type ZoweJobData struct {
	Status  string  `json:"status"`
	Retcode *string `json:"retcode"` // can be null
	Time    string  `json:"time"`
}
