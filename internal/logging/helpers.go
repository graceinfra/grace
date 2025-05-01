package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/models"
)

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
