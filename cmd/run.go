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
var isVerbose bool

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().StringSliceVar(&submitOnly, "only", nil, "Submit only specified job(s)")
	runCmd.Flags().BoolVarP(&isVerbose, "verbose", "v", false, "Enable verbose logging")
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a workflow and wait for completion",
	Long: `Run executes a workflow defined in grace.yml synchronously.

Grace will execute the full workflow orchestration, including dependencies
and concurrency, and wait for completion. Progress will be shown in the
terminal and logs will be written to '.grace/logs/'.

Use 'grace submit' instead for asynchronous execution that returns immediately.

Assumes 'grace deck' has been run previously to prepare JCL and source files.`,
	Run: func(cmd *cobra.Command, args []string) {
		// --- Load and validate grace.yml ---

		registry := GetDependencies().HandlerRegistry

		// TODO: could implement this as a flag
		configPath := "grace.yml"
		graceCfg, configDir, err := config.LoadGraceConfig(configPath)
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to load %q: %w", configPath, err))
		}

		err = config.ValidateGraceConfig(graceCfg, registry)
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to validate %q: %w", configPath, err))
		}

		log.Info().Msgf("✓ Configuration %q loaded and validated.", configPath)

		// --- Initialize workflow context and logging ---
		workflowId := uuid.New()
		workflowStartTime := time.Now()

		logDir, err := logging.CreateLogDir(workflowId, workflowStartTime, "run")
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to create log directory for workflow %s: %w", workflowId.String(), err))
		}

		// Configure file logging
		logFilePath := filepath.Join(logDir, "workflow.log")
		err = logging.ConfigureGlobalLogger(isVerbose, logFilePath)
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to initialize logging: %w", err))
		}

		// Initialize contextual logger
		logCtx := log.With().Str("workflow_id", workflowId.String()).Logger()
		logCtx.Info().Msgf("Logs will be stored in: %s", logDir)

		// --- Set up execution context ---

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
		logCtx.Info().Msg("Starting workflow run...")

		jobExecutionRecords, err := orch.Run(ctx, registry)
		if err != nil {
			logCtx.Error().Err(err).Msg("Orchestration failed")
			cobra.CheckErr(err)
		}

		// --- Construct and write workflow summary ---

		logCtx.Debug().Msg("Generating execution summary...")

		summary := generateExecutionSummary(jobExecutionRecords, workflowId, workflowStartTime, graceCfg, "run", submitOnly)

		if err = writeSummary(summary, logDir); err != nil {
			logCtx.Error().Err(err).Msg("Failed to write summary.json")
		}

		fmt.Println() // Visual spacing
		logCtx.Info().Msgf("✓ Workflow complete, logs saved to: %s", logDir)
	},
}
