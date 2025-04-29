package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/runner"
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
		// --- Decide output style ---
		// Are we user facing? Part of a pipeline? This will let the logger
		// know whether to show animations, verbose text, etc.)

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

		// --- Run workflow ---

		logger := log.NewLogger(outputStyle)

		jobExecutionRecords := runner.RunWorkflow(&context.ExecutionContext{
			WorkflowId:  workflowId,
			Config:      graceCfg,
			Logger:      logger,
			LogDir:      logDir,
			OutputStyle: outputStyle,
			SubmitOnly:  submitOnly,
			GraceCmd:    "run",
		})

		// --- Construct workflow summary ---

		host, _ := os.Hostname()
		hlq := strings.Split(graceCfg.Datasets.JCL, ".")[0]
		if hlq == "" {
			cobra.CheckErr(fmt.Errorf("unable to resolve JCL dataset identifier. Is the 'jcl' field filled out under 'datasets' in grace.yml?"))
		}

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
			// Define "failure" condition more robustly
			isFailed := finalStatus == "SUBMIT_FAILED" ||
				finalStatus == "ABEND" ||
				finalStatus == "JCL ERROR" ||
				finalStatus == "SEC ERROR" ||
				(finalRetCode != nil && *finalRetCode != "CC 0000") // Adjust non-zero RC logic as needed

			if isFailed {
				jobsFailed++
				if firstFailure == nil {
					// Capture the pointer to the summary we just created
					firstFailure = &jobSummaries[len(jobSummaries)-1]
					overallStatus = "Failed" // Mark overall as failed
				}
			} else if finalStatus == "OUTPUT" { // Or your definition of success
				jobsSucceeded++
			} else {
				// Handle other statuses (INPUT, ACTIVE etc.) if the workflow stops unexpectedly
				if overallStatus != "Failed" { // Don't overwrite Failed status
					overallStatus = "Partial" // Or "Unknown", "Incomplete"
				}
			}
		}
		// If loop finished and nothing failed, but counts don't match total jobs, it might be partial
		if overallStatus == "Success" && (jobsSucceeded+jobsFailed != len(graceCfg.Jobs)-len(submitOnly)) {
			overallStatus = "Partial" // Or Incomplete
		}

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
			Jobs:            jobSummaries, // Assign the mapped summaries
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
		cobra.CheckErr(err)
		defer f.Close()

		_, err = f.Write(formatted)
		cobra.CheckErr(err)

		logger.Info("✓ Workflow complete, logs saved to: %s", logDir)
	},
}
