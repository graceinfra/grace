package types

type Job struct {
	Name     string `yaml:"name"`
	Step     string `yaml:"step"`
	Source   string `yaml:"source"`
	Template string `yaml:"template,omitempty"`

	DependsOn []string `yaml:"depends_on,omitempty"`

	Wait    bool   `yaml:"wait,omitempty"`
	View    string `yaml:"view"`
	Retries int    `yaml:"retries"`
}

type JobResult interface {
	GetJobName() string
	GetJobID() string
	GetStatus() string
}

type ZoweRfj struct {
	Success  bool          `json:"success"`
	ExitCode int           `json:"exitCode"`
	Message  string        `json:"message"`
	Stdout   string        `json:"stdout"`
	Stderr   string        `json:"stderr"`
	Data     *ZoweRfjData  `json:"data,omitempty"`
	Error    *ZoweRfjError `json:"error,omitempty"`
}

func (r ZoweRfj) GetJobName() string { return r.Data.JobName }
func (r ZoweRfj) GetJobID() string   { return r.Data.JobID }
func (r ZoweRfj) GetStatus() string  { return r.Data.Status }
func (r ZoweRfj) GetError() string   { return r.Error.Msg }

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

// Initiator stores information about who initiated a workflow - whether
// it's a user, service account, or part of a CI pipeline
type Initiator struct {
	Type   string `json:"type"`   // "user", "service", "ci"
	Id     string `json:"id"`     // "arnav", "grace-runner-01234"
	Tenant string `json:"tenant"` // "ca-dmv"
}
