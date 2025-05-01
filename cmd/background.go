package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/orchestrator"
	"github.com/rs/zerolog/log"
)

// RunBackgroundWorkflow is executed when 'grace' is launched with internal flags.
// It runs the full orchestration logic and logs to files.
func RunBackgroundWorkflow(workflowIdStr, configPath, logDir string, onlyFilter []string, verbose bool) {
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

	log.Info().Str("workflow", workflowIdStr).Msg("Starting execution.")
	log.Info().Str("workflow", workflowIdStr).Msgf("Using config: %s", configPath)
	log.Info().Str("workflow", workflowIdStr).Msgf("Using log directory: %s", logDir)

	// --- Load grace.yml ---

	graceCfg, err := config.LoadGraceConfig(configPath)
	if err != nil {
		log.Error().Str("workflow", workflowIdStr).Msgf("Failed to load configuration: %v", err)
		os.Exit(1)
	}

	// --- Create context ---
	ctx := &context.ExecutionContext{
		WorkflowId: workflowId,
		Config:     graceCfg,
		LogDir:     logDir,
		SubmitOnly: onlyFilter,
		GraceCmd:   "submit-bg",
	}

	// --- Instantiate and run orchestrator ---

	orch := orchestrator.NewZoweOrchestrator()
	log.Debug().Str("workflow", workflowIdStr).Msg("Invoking DAG executor...")
	startTimeForSummary := time.Now()
	jobExecutionRecords, execErr := orch.Run(ctx)

	// --- Process results & write summary

	if execErr != nil {
		log.Error().Str("workflow", workflowIdStr).Msgf("Orchestration failed: %v", execErr)
	} else {
		log.Info().Str("workflow", workflowIdStr).Msg("Orchestration finished. Processing results...")
	}

	// Generate summary regardless of execErr, using potentially partial records
	summary := generateExecutionSummary(jobExecutionRecords, workflowId, startTimeForSummary, graceCfg, "submit-bg", onlyFilter)

	// Attempt to write summary
	err = writeSummary(summary, logDir)
	if err != nil {
		log.Error().Str("workflow", workflowIdStr).Msgf("Failed to write summary: %v", err)
	} else {
		log.Info().Str("workflow", workflowIdStr).Msgf("Workflow summary written to %s", filepath.Join(logDir, "summary.json"))
	}

	log.Info().Str("workflow", workflowIdStr).Msg("Execution finished.")

	if execErr != nil {
		os.Exit(1) // Exit with error code if orchestration itself failed
	}

	os.Exit(0) // Exit successfully
}
