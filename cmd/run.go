package cmd

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
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
		logger.Info("Starting workflow run...")

		jobExecutionRecords, err := orch.Run(ctx)
		cobra.CheckErr(err)

		// --- Construct workflow summary ---

		logger.Verbose("Generating execution summary...")

		summary := generateExecutionSummary(jobExecutionRecords, workflowId, workflowStartTime, graceCfg, "run", submitOnly)

		// --- Write workflow summary to summary.json ---

		if err = writeSummary(summary, logDir); err != nil {
			logger.Error("Failed to write summary.json to %s", logDir)
		}

		fmt.Println() // Newline
		logger.Info("✓ Workflow complete, logs saved to: %s", logDir)
	},
}
