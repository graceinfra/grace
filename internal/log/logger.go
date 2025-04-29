package log

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/graceinfra/grace/types"
)

type GraceJobLog struct {
	LogContext
	Result any `json:"result"`
}

type LogContext struct {
	JobID       string          `json:"job_id"`   // "JOB02848"
	JobName     string          `json:"job_name"` // "HELLO"
	Step        string          `json:"step"`     // "execute", "compile"
	RetryIndex  int             `json:"retry_index"`
	GraceCmd    string          `json:"grace_cmd"`    // submit", "run"
	ZoweProfile string          `json:"zowe_profile"` // "zosmf", "ssh", "tso"
	HLQ         string          `json:"hlq"`          // "IBMUSER"
	Timestamp   string          `json:"timestamp"`    // RFC3339 format
	Initiator   types.Initiator `json:"initiator"`
}

// NewLogContext builds a reusable log context
func NewLogContext(job *types.Job, jobId, jobName, graceCmd string, cfg *types.GraceConfig) LogContext {
	hlq := strings.Split(cfg.Datasets.JCL, ".")[0]
	host, _ := os.Hostname()

	return LogContext{
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

type GraceLogger struct {
	OutputStyle types.OutputStyle
	Spinner     *spinner.Spinner
}

func NewLogger(style types.OutputStyle) *GraceLogger {
	return &GraceLogger{
		OutputStyle: style,
		Spinner: spinner.New(
			spinner.CharSets[11], // Default ⣾ style spinner, can modify this at the call site
			100*time.Millisecond,
			spinner.WithHiddenCursor(true)),
	}
}

func (l *GraceLogger) Info(msg string, args ...any) {
	if l.OutputStyle == types.StyleHuman || l.OutputStyle == types.StyleHumanVerbose {
		fmt.Printf(msg+"\n", args...)
	}
	// Silent for machine modes
}

func (l *GraceLogger) Verbose(msg string, args ...any) {
	if l.OutputStyle == types.StyleHumanVerbose {
		fmt.Printf(msg+"\n", args...)
	}
	// Silent for normal human and machine modes
}

func (l *GraceLogger) Json(data any) {
	if l.OutputStyle == types.StyleMachineJSON {
		encoded, _ := json.MarshalIndent(data, "", "  ")
		fmt.Println(string(encoded))
	}
}

// StartSpinner starts the logger spinner. you can pass optionalCharset
// to override the default spinner. It is a variadic parameter but only
// the first argument will be used.
func (l *GraceLogger) StartSpinner(text string, optionalCharset ...[]string) {
	if l.OutputStyle == types.StyleHuman || l.OutputStyle == types.StyleHumanVerbose {
		l.Spinner.Suffix = " " + text
		if len(optionalCharset) > 0 {
			l.Spinner.UpdateCharSet(optionalCharset[0])
		}
		l.Spinner.Start()
	}
}

func (l *GraceLogger) StopSpinner() {
	if l.OutputStyle == types.StyleHuman || l.OutputStyle == types.StyleHumanVerbose {
		l.Spinner.Stop()
	}
}

// CreateLogDir returns a full path like
// ".grace/logs/20250423T213245_submit"
func CreateLogDir(graceCmd string) (string, string, error) {
	timestamp := time.Now().Format("20060102T150405")
	dirName := fmt.Sprintf("%s_%s", timestamp, graceCmd)
	fullPath := filepath.Join(".grace", "logs", dirName)

	err := os.MkdirAll(fullPath, os.ModePerm)
	if err != nil {
		return "", "", err
	}
	return fullPath, timestamp, nil
}
