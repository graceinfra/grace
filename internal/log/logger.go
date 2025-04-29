package log

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/types"
)

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

func (l *GraceLogger) Error(msg string, args ...any) {
	if l.OutputStyle == types.StyleHuman || l.OutputStyle == types.StyleHumanVerbose {
		fmt.Fprintf(os.Stderr, "Error: "+msg+"\n", args...)
	}
	// Silent for machine modes
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
// ".grace/logs/20250423T213245_submit_3c43e9f4-9026-4d04-ba06-054e8903e80a"
func CreateLogDir(workflowId uuid.UUID, workflowStartTime time.Time, graceCmd string) (string, error) {
	timestampStr := workflowStartTime.Format("20060102T150405")

	dirName := fmt.Sprintf("%s_%s_%s", timestampStr, graceCmd, workflowId)
	fullPath := filepath.Join(".grace", "logs", dirName)

	err := os.MkdirAll(fullPath, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("failed to create log directory '%s': %w", fullPath, err)
	}
	return fullPath, nil
}

// SaveJobExecutionRecord stores the detailed record for a single job.
// Filename: JOBID_JOBNAME.json (e.g., JOB02929_HELLO.json)
func SaveJobExecutionRecord(logDir string, record models.JobExecutionRecord) error {
	jobId := record.JobID
	// Handle cases where JobID might be missing or indicate failure
	if jobId == "" || jobId == "SUBMIT_FAILED" || jobId == "SUBMIT_UNMARSHAL_ERROR" {
		// Create a more informative filename for failures before JobID assignment
		timestamp := time.Now().Format("150405") // Add time to distinguish potentially same-named failures
		jobId = fmt.Sprintf("FAILED_%s_%s", record.JobName, timestamp)
	}
	fileName := fmt.Sprintf("%s_%s.json", jobId, strings.ToUpper(record.JobName))
	filePath := filepath.Join(logDir, fileName)

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create job log file %s: %w", filePath, err)
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(record); err != nil {
		return fmt.Errorf("failed to encode job log record to %s: %w", filePath, err)
	}
	return nil
}
