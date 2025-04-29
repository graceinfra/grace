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
	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/orchestrator"
	"github.com/graceinfra/grace/types"
	"github.com/spf13/cobra"
)

var (
	wantJSON   bool
	submitOnly []string
)

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().BoolVar(&wantJSON, "json", false, "Return structured JSON data about each job")
	runCmd.Flags().StringSliceVar(&submitOnly, "only", nil, "Submit only specified job(s)")
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Execute and monitor mainframe jobs defined in grace.yml",
	Long: `Run orchestrates the execution of mainframe jobs defined in grace.yml, submitting them in sequence and monitoring their execution until completion.

It works with resources already available on the mainframe (previously uploaded via [grace deck]) and provides real-time status updates as jobs progress. Each job execution is tracked, with results and logs collected for review.

Run creates a timestamped log directory containing job output and a summary.json file with execution details.

Use '--only' to selectively run specific jobs, or '--json' for machine-readable structured output.`,
	Run: func(cmd *cobra.Command, args []string) {
		var outputStyle types.OutputStyle
		switch {
		case wantJSON:
			outputStyle = types.StyleMachineJSON
		case Verbose:
			outputStyle = types.StyleHumanVerbose
		default:
			outputStyle = types.StyleHuman
		}

		// --- Load and validate grace.yml ---

		graceCfg, err := config.LoadGraceConfig("grace.yml")
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to load grace configuration: %w", err))
		}

		// --- Create log directory ---

		workflowStartTime := time.Now()
		workflowId := uuid.New()

		logDir, err := log.CreateLogDir(workflowId, workflowStartTime, "run")
		cobra.CheckErr(err)

		// --- Prepare ExecutionContext ---

		logger := log.NewLogger(outputStyle)

		ctx := &context.ExecutionContext{
			WorkflowId:  workflowId,
			Config:      graceCfg,
			Logger:      logger,
			LogDir:      logDir,
			OutputStyle: outputStyle,
			SubmitOnly:  submitOnly,
			GraceCmd:    "run",
		}

		// --- Instantiate and run orchestrator ---

		orch := orchestrator.NewZoweOrchestrator()
		logger.Info("Starting orchestrator workflow run...")

		jobExecutionRecords, err := orch.Run(ctx)
		cobra.CheckErr(err)

		// --- Construct workflow summary ---

		host, _ := os.Hostname()
		hlq := ""
		if graceCfg.Datasets.JCL != "" {
			parts := strings.Split(graceCfg.Datasets.JCL, ".")
			if len(parts) > 0 {
				hlq = parts[0]
			}
		}
		// If HLQ is still empty here, summary will just show empty HLQ, which is fine.
		// Critical check happened in the orchestrator.

		jobSummaries := make([]models.JobSummary, 0, len(jobExecutionRecords))
		jobsSucceeded := 0
		jobsFailed := 0
		var firstFailure *models.JobSummary = nil
		overallStatus := "Success" // Assume success initially

		for _, record := range jobExecutionRecords {
			finalStatus := "UNKNOWN"
			var finalRetCode *string = nil

			// Determine final status and RC from the available responses
			if record.JobID == "SUBMIT_FAILED" {
				finalStatus = "SUBMIT_FAILED"
			} else if record.FinalResponse != nil && record.FinalResponse.Data != nil {
				finalStatus = record.FinalResponse.Data.Status
				finalRetCode = record.FinalResponse.Data.RetCode
			} else if record.SubmitResponse != nil && record.SubmitResponse.Data != nil {
				// Use submit status if final isn't available (e.g., submit failed but got JobID)
				finalStatus = record.SubmitResponse.Data.Status
				finalRetCode = record.SubmitResponse.Data.RetCode // Usually null on submit
			}

			// Generate relative log file path
			logFileName := fmt.Sprintf("%s_%s.json", record.JobID, strings.ToUpper(record.JobName))
			relativeLogPath := filepath.Join(filepath.Base(logDir), logFileName) // e.g., "20230101T120000_run/JOB12345_MYJOB.json"

			summary := models.JobSummary{
				JobName:    record.JobName,
				JobID:      record.JobID,
				Step:       record.Step,
				Source:     record.Source,
				Status:     finalStatus,
				ReturnCode: finalRetCode,
				SubmitTime: record.SubmitTime,
				FinishTime: record.FinishTime,
				DurationMs: record.DurationMs,
				LogFile:    relativeLogPath,
			}
			jobSummaries = append(jobSummaries, summary)

			// Update overall stats
			isFailed := finalStatus == "SUBMIT_FAILED" ||
				finalStatus == "ABEND" ||
				finalStatus == "JCL ERROR" ||
				finalStatus == "SEC ERROR" ||
				finalStatus == "SYSTEM FAILURE" ||
				finalStatus == "CANCELED" ||
				(finalStatus != "OUTPUT" && finalStatus != "SUBMITTED" && record.JobID != "PENDING" && record.JobID != "SUBMIT_FAILED")

			// More precise RC check (optional, depends on exact success definition)
			// isFailed := finalStatus == "SUBMIT_FAILED" ||
			// 	(finalStatus != "OUTPUT" && record.JobID != "PENDING") || // Any non-output terminal state
			// 	(finalStatus == "OUTPUT" && finalRetCode != nil && *finalRetCode != "CC 0000" && *finalRetCode != "CC 0004") // Example: Allow CC 0004

			if isFailed {
				jobsFailed++
				if firstFailure == nil {
					firstFailure = &jobSummaries[len(jobSummaries)-1] // Point to the summary just added
					overallStatus = "Failed"
				}
			} else if finalStatus == "OUTPUT" {
				jobsSucceeded++
			}
		}

		// Refine overall status calculation
		totalJobsDefined := len(graceCfg.Jobs)
		totalJobsAttempted := 0
		for _, job := range graceCfg.Jobs {
			if len(submitOnly) == 0 || slices.Contains(submitOnly, job.Name) {
				totalJobsAttempted++
			}
		}

		if overallStatus == "Success" && (jobsSucceeded+jobsFailed != totalJobsAttempted) {
			// If we didn't fail, but the counts don't add up (e.g., unexpected status, polling error), mark as Partial
			overallStatus = "Partial"
		} else if jobsFailed > 0 {
			overallStatus = "Failed"
		} else if totalJobsAttempted == 0 && totalJobsDefined > 0 {
			overallStatus = "Skipped"
		} else if jobsSucceeded == totalJobsAttempted && totalJobsAttempted > 0 {
			overallStatus = "Success"
		}
		// If jobsSucceeded + jobsFailed == totalJobsAttempted, and jobsFailed == 0, it's Success
		// If jobsSucceeded + jobsFailed == totalJobsAttempted, and jobsFailed > 0, it's Failed

		summary := models.ExecutionSummary{
			WorkflowId:        workflowId,
			WorkflowStartTime: workflowStartTime.Format(time.RFC3339),
			GraceCmd:          "run",
			ZoweProfile:       graceCfg.Config.Profile,
			HLQ:               hlq,
			Initiator: types.Initiator{
				Type:   "user",
				Id:     os.Getenv("USER"),
				Tenant: host,
			},
			Jobs:            jobSummaries,
			OverallStatus:   overallStatus,
			TotalDurationMs: time.Since(workflowStartTime).Milliseconds(),
			JobsSucceeded:   jobsSucceeded,
			JobsFailed:      jobsFailed,
			FirstFailure:    firstFailure,
		}

		// --- Write workflow summary to summary.json ---

		formatted, err := json.MarshalIndent(summary, "", "  ")
		cobra.CheckErr(err)

		summaryPath := filepath.Join(logDir, "summary.json")
		f, err := os.Create(summaryPath)
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to create summary file %s: %w", summaryPath, err))
		}
		defer f.Close()

		_, err = f.Write(formatted)
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to write summary file %s: %w", summaryPath, err))
		}

		fmt.Println() // Newline
		logger.Info("✓ Workflow complete, logs saved to: %s", logDir)
	},
}
