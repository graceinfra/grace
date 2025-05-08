package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/jobhandler"
	"github.com/graceinfra/grace/internal/orchestrator"
	"github.com/rs/zerolog/log"
)

// RunBackgroundWorkflow is executed when 'grace' is launched with internal flags.
// It runs the full orchestration logic and logs to files.
func RunBackgroundWorkflow(workflowIdStr, configPath, logDir string, onlyFilter []string) {
	bgWorkflowLogger := log.With().Str("workflow_id", workflowIdStr).Logger()
	workflowId, err := uuid.Parse(workflowIdStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Background Error: Invalid workflow ID %q: %v\n", workflowIdStr, err)
		os.Exit(1)
	}

	if logDir == "" {
		fmt.Fprintf(os.Stderr, "Background Error: Log directory path not provided.\n")
		os.Exit(1)
	}

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Background Error: Unable to resolve log directory %s", logDir)
	}

	// --- Configure file logging ---

	bgWorkflowLogger.Info().Msg("Starting execution.")
	bgWorkflowLogger.Info().Msgf("Using config: %s", configPath)
	bgWorkflowLogger.Info().Msgf("Using log directory: %s", logDir)

	// --- Initialize registry ---

	registry := jobhandler.NewRegistry()
	registry.Register(&jobhandler.ZosCompileHandler{})
	registry.Register(&jobhandler.ZosLinkeditHandler{})
	registry.Register(&jobhandler.ZosExecuteHandler{})
	registry.Register(&jobhandler.ShellHandler{})

	// --- Load grace.yml ---

	graceCfg, configDir, err := config.LoadGraceConfig(configPath)
	if err != nil {
		log.Error().Str("workflow", workflowIdStr).Msgf("Failed to load configuration: %v", err)
		os.Exit(1)
	}

	if err := config.ValidateGraceConfig(graceCfg, registry); err != nil {
		log.Error().Err(err).Str("workflow_id", workflowIdStr).Msg("Configuration validation failed")
		os.Exit(1)
	}

	// --- Create context ---
	localStageDir := filepath.Join(logDir, ".local-staging")
	ctx := &context.ExecutionContext{
		WorkflowId:    workflowId,
		Config:        graceCfg,
		ConfigDir:     configDir,
		LogDir:        logDir,
		LocalStageDir: localStageDir,
		SubmitOnly:    onlyFilter,
		GraceCmd:      "submit-bg",
	}

	// --- Instantiate and run orchestrator ---

	orch := orchestrator.NewZoweOrchestrator()
	bgWorkflowLogger.Debug().Msg("Invoking DAG executor...")
	workflowStartTimeForSummary := time.Now()
	jobExecutionRecords, execErr := orch.Run(ctx, registry)

	// --- Process results & write summary

	if execErr != nil {
		bgWorkflowLogger.Error().Err(execErr).Msg("Orchestration failed")
	} else {
		bgWorkflowLogger.Info().Msg("Orchestration finished. Processing results...")
	}

	// Generate summary regardless of execErr, using potentially partial records
	summary := generateExecutionSummary(jobExecutionRecords, workflowId, workflowStartTimeForSummary, graceCfg, "submit-bg", onlyFilter)

	// Attempt to write summary
	err = writeSummary(summary, logDir)
	if err != nil {
		bgWorkflowLogger.Error().Err(err).Msgf("Failed to write summary")
	} else {
		bgWorkflowLogger.Info().Msgf("Workflow summary written to %s", filepath.Join(logDir, "summary.json"))
	}

	bgWorkflowLogger.Info().Msg("Execution finished.")

	if execErr != nil {
		os.Exit(1) // Exit with error code if orchestration itself failed
	}

	os.Exit(0) // Exit successfully
}
