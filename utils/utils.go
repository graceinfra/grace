package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/graceinfra/grace/internal/templates"
	"github.com/spf13/cobra"
)

// WriteTpl loads tplName from tplFS, executes it with data, and writes to outPath
func WriteTpl(tplName, outPath string, data any) error {
	t, err := template.ParseFS(templates.TplFS, tplName)
	if err != nil {
		return err
	}

	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	return t.Execute(f, data)
}

// Helper to create directory structure
func MkDir(targetDir string, parts ...string) {
	path := filepath.Join(append([]string{targetDir}, parts...)...)
	err := os.MkdirAll(path, 0755)
	cobra.CheckErr(err)
}

// Helper to check if a dir already exists
func MustNotExist(path string) {
	if _, err := os.Stat(path); err == nil {
		cobra.CheckErr(fmt.Errorf("refusing to overwrite existing file or directory: %s", path))
	}
}

// Helper to validate data set name (max qualifiers, max qualifier length, invalid chars in qualifier)
func IsValidDataSetName(name string) error {
	parts := strings.Split(name, ".")
	if len(parts) > 22 {
		return fmt.Errorf("dataset name has too many qualifiers (max 22): %d", len(parts))
	}

	for _, part := range parts {
		if len(part) == 0 {
			return fmt.Errorf("dataset name contains an empty qualifier")
		}
		if len(part) > 8 {
			return fmt.Errorf("qualifier %q exceeds 8 characters", part)
		}
		if matched, _ := regexp.MatchString(`^[A-Z0-9#$@]+$`, part); !matched {
			return fmt.Errorf("qualifier %q contains invalid characters; only A-Z, 0-9, $, #, @ allowed", part)
		}
	}
	return nil
}

// Helper to parse job id and name from spool output bc no JSON
func ParseSpoolMeta(out []byte) (jobId string, jobName string) {
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if jobId == "" {
			if fields := strings.Fields(line); len(fields) > 0 {
				for _, field := range fields {
					if strings.HasPrefix(field, "JOB") && len(field) >= 7 {
						jobId = field
						break
					}
				}
			}
		}
		if jobName == "" && strings.Contains(line, "IEF453I ") {
			parts := strings.Split(line, "IEF453I ")
			if len(parts) > 1 {
				right := strings.TrimSpace(parts[1])
				tokens := strings.Split(right, " ")
				if len(tokens) > 0 {
					jobName = tokens[0]
				}
			}
		}
		if jobName == "" && strings.Contains(line, "NAME-") {
			idx := strings.Index(line, "NAME-")
			if idx != -1 {
				after := strings.TrimSpace(line[idx+5:])
				fields := strings.Fields(after)
				if len(fields) > 0 {
					jobName = fields[0]
				}
			}
		}
		if jobId != "" && jobName != "" {
			break
		}
	}
	return
}

type JobResult interface {
	GetJobName() string
	GetJobID() string
	GetStatus() string
}

func (r ZoweRfj) GetJobName() string { return r.Data.JobName }
func (r ZoweRfj) GetJobID() string   { return r.Data.JobID }
func (r ZoweRfj) GetStatus() string  { return r.Data.Status }

func PrintJobResult(result JobResult, raw []byte, jsonMode, quiet bool) {
	if jsonMode {
		_, _ = os.Stdout.Write(raw)
		return
	}
	if quiet {
		return
	}
	fmt.Printf("\n\u2713 Job %s submitted as %s (status: %s)\n", result.GetJobName(), result.GetJobID(), result.GetStatus())
}

func ParseAndPrintJobResult(raw []byte, jsonMode, quiet bool) (ZoweRfj, error) {
	var result ZoweRfj
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
func NewLogContext(job Job, jobId, jobName string, cfg GraceConfig) LogContext {
	return LogContext{
		JobID:       jobId,
		JobName:     jobName,
		Step:        job.Step,
		RetryIndex:  0,
		GraceCmd:    "submit",
		ZoweProfile: cfg.Config.Profile,
		HLQ:         cfg.Datasets.Prefix,
		Timestamp:   time.Now().Format(time.RFC3339),
		Initiator: Initiator{
			Type:   "user",
			Id:     os.Getenv("USER"),
			Tenant: "nara",
		},
	}
}
