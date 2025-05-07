package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/types"
)

// generateExecutionSummary calculates the summary based on job records and context.
// Takes submitOnly filter used during the run to accurately calculate Attempted/Skipped counts.
func generateExecutionSummary(
	records []models.JobExecutionRecord,
	workflowId uuid.UUID,
	startTime time.Time,
	cfg *types.GraceConfig,
	cmdName string,
	submitOnlyFilter []string,
) models.ExecutionSummary {
	host, _ := os.Hostname()
	hlq := ""
	if cfg.Datasets.JCL != "" {
		parts := strings.Split(cfg.Datasets.JCL, ".")
		if len(parts) > 0 {
			hlq = parts[0]
		}
	}

	jobSummaries := make([]models.JobSummary, 0, len(records))
	jobsSucceeded := 0
	jobsFailed := 0
	jobsSkipped := 0
	var firstFailure *models.JobSummary = nil
	overallStatus := "Success"

	logDirBaseName := fmt.Sprintf("%s_%s_%s", startTime.Format("20060102T150405"), cmdName, workflowId)
	if cmdName == "submit-bg" {
		logDirBaseName = fmt.Sprintf("%s_%s_%s", startTime.Format("20060102T150405"), "submit", workflowId)
	}

	for _, record := range records {
		finalStatus := "UNKNOWN"
		var finalRetCode *string = nil

		// Determine final status and RC from the available responses
		// Special case for explicitly skipped jobs first
		if record.JobID == "SKIPPED" {
			finalStatus = "SKIPPED"
		} else if record.JobID == "SUBMIT_FAILED" {
			finalStatus = "SUBMIT_FAILED"
		} else if record.FinalResponse != nil && record.FinalResponse.Data != nil {
			finalStatus = record.FinalResponse.Data.Status
			finalRetCode = record.FinalResponse.Data.RetCode
		} else if record.SubmitResponse != nil && record.SubmitResponse.Data != nil {
			finalStatus = record.SubmitResponse.Data.Status
			finalRetCode = record.SubmitResponse.Data.RetCode
		}

		logFileName := fmt.Sprintf("%s_%s.json", record.JobID, strings.ToUpper(record.JobName))
		if record.JobID == "SKIPPED" {
			timestamp := time.Now().Format("150405")
			jobIdForLog := fmt.Sprintf("SKIPPED_%s_%s", record.JobName, timestamp)
			logFileName = fmt.Sprintf("%s_%s.json", jobIdForLog, strings.ToUpper(record.JobName))
		}

		relativeLogPath := filepath.Join(logDirBaseName, logFileName)

		summary := models.JobSummary{
			JobName:    record.JobName,
			JobID:      record.JobID,
			Type:       record.Type,
			Source:     record.Source,
			Status:     finalStatus,
			ReturnCode: finalRetCode,
			SubmitTime: record.SubmitTime,
			FinishTime: record.FinishTime,
			DurationMs: record.DurationMs,
			LogFile:    relativeLogPath,
		}
		jobSummaries = append(jobSummaries, summary)

		// Update overall stats based on the final determined status for the summary
		// Define failure conditions for summary status calculation
		isFailed := finalStatus == "SUBMIT_FAILED" ||
			finalStatus == "ABEND" ||
			finalStatus == "JCL ERROR" ||
			finalStatus == "SEC ERROR" ||
			finalStatus == "SYSTEM FAILURE" ||
			finalStatus == "CANCELED" ||
			// Also consider a non-null, non-zero/non-4 return code with OUTPUT status as failure
			(finalStatus == "OUTPUT" && finalRetCode != nil && *finalRetCode != "CC 0000" && *finalRetCode != "CC 0004") || // Example: Treat > CC 0004 as failure
			(finalStatus != "OUTPUT" && finalStatus != "SKIPPED" && record.JobID != "PENDING" && record.JobID != "SKIPPED")

		if isFailed {
			jobsFailed++
			if firstFailure == nil {
				firstFailure = &jobSummaries[len(jobSummaries)-1]
			}
		} else if finalStatus == "OUTPUT" {
			jobsSucceeded++
		} else if finalStatus == "SKIPPED" {
			jobsSkipped++
		}
	}

	totalJobsDefined := len(cfg.Jobs)
	totalJobsAttempted := 0
	for _, job := range cfg.Jobs {
		if len(submitOnlyFilter) == 0 || slices.Contains(submitOnlyFilter, job.Name) {
			totalJobsAttempted++
		}
	}

	if jobsFailed > 0 {
		overallStatus = "Failed"
	} else if jobsSucceeded == totalJobsAttempted && totalJobsAttempted > 0 {
		overallStatus = "Success"
	} else if totalJobsAttempted == 0 && totalJobsDefined > 0 {
		overallStatus = "Skipped"
	} else if jobsSucceeded+jobsFailed+jobsSkipped == totalJobsAttempted {
		overallStatus = "Success"
	} else {
		// Counts don't add up - indicates unexpected statuses or logic error
		overallStatus = "Partial"
	}

	// Determine initiator type based on command
	initiatorType := "user"
	if cmdName == "submit" || cmdName == "submit-bg" {
		initiatorType = "grace-submit"
	}

	execSummary := models.ExecutionSummary{
		WorkflowId:        workflowId,
		WorkflowStartTime: startTime.Format(time.RFC3339),
		GraceCmd:          cmdName,
		ZoweProfile:       cfg.Config.Profile,
		HLQ:               hlq,
		Initiator: types.Initiator{
			Type:   initiatorType,
			Id:     os.Getenv("USER"),
			Tenant: host,
		},
		Jobs:            jobSummaries,
		OverallStatus:   overallStatus,
		TotalDurationMs: time.Since(startTime).Milliseconds(),
		JobsSucceeded:   jobsSucceeded,
		JobsFailed:      jobsFailed,
		FirstFailure:    firstFailure,
	}

	return execSummary
}

// writeSummary writes the execution summary to summary.json in the log directory.
// Returns an error if file operations fail.
func writeSummary(summary models.ExecutionSummary, logDir string) error {
	formatted, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		// This is an internal error, should be logged by caller
		return fmt.Errorf("failed to marshal execution summary: %w", err)
	}

	summaryPath := filepath.Join(logDir, "summary.json")
	f, err := os.Create(summaryPath)
	if err != nil {
		return fmt.Errorf("failed to create summary file %s: %w", summaryPath, err)
	}
	defer f.Close()

	_, err = f.Write(formatted)
	if err != nil {
		return fmt.Errorf("failed to write summary file %s: %w", summaryPath, err)
	}

	return nil
}
