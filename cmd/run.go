package cmd

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/logging"
	"github.com/graceinfra/grace/internal/orchestrator"
	"github.com/spf13/cobra"
)

var submitOnly []string

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().StringSliceVar(&submitOnly, "only", nil, "Submit only specified job(s)")
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Execute and monitor mainframe jobs defined in grace.yml",
	Long: `Run orchestrates the execution of mainframe jobs defined in grace.yml, submitting them in sequence and monitoring their execution until completion.

It works with resources already available on the mainframe (previously uploaded via [grace deck]) and provides real-time status updates as jobs progress. Each job execution is tracked, with results and logs collected for review.

Run creates a timestamped log directory containing job output and a summary.json file with execution details.

Use '--only' to selectively run specific jobs.`,
	Run: func(cmd *cobra.Command, args []string) {
		// --- Load and validate grace.yml ---

		registry := GetDependencies().HandlerRegistry

		graceCfg, configDir, err := config.LoadGraceConfig("grace.yml")
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to load grace configuration: %w", err))
		}

		// --- Perform full validation using the registry ---

		if err := config.ValidateGraceConfig(graceCfg, registry); err != nil {
			cobra.CheckErr(fmt.Errorf("grace configuration validation failed: %w", err))
		}

		// --- Create log directory ---

		workflowStartTime := time.Now()
		workflowId := uuid.New()

		logDir, err := logging.CreateLogDir(workflowId, workflowStartTime, "run")
		cobra.CheckErr(err)

		// --- Prepare ExecutionContext ---

		localStageDir := filepath.Join(logDir, ".local-staging")
		ctx := &context.ExecutionContext{
			WorkflowId:    workflowId,
			Config:        graceCfg,
			ConfigDir:     configDir,
			LogDir:        logDir,
			LocalStageDir: localStageDir,
			SubmitOnly:    submitOnly,
			GraceCmd:      "run",
		}

		// --- Instantiate and run orchestrator ---

		orch := orchestrator.NewZoweOrchestrator()
		log.Info().Str("workflow_id", workflowId.String()).Msg("Starting workflow run...")

		jobExecutionRecords, err := orch.Run(ctx, registry)
		cobra.CheckErr(err)

		// --- Construct workflow summary ---

		log.Debug().Str("workflow_id", workflowId.String()).Msg("Generating execution summary...")

		summary := generateExecutionSummary(jobExecutionRecords, workflowId, workflowStartTime, graceCfg, "run", submitOnly)

		// --- Write workflow summary to summary.json ---

		if err = writeSummary(summary, logDir); err != nil {
			log.Error().Str("workflow_id", workflowId.String()).Msgf("Failed to write summary.json to %s", logDir)
		}

		fmt.Println() // Newline
		log.Info().Str("workflow_id", workflowId.String()).Msgf("✓ Workflow complete, logs saved to: %s", logDir)
	},
}
