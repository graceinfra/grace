package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
	"github.com/graceinfra/grace/internal/orchestrator"
	"github.com/graceinfra/grace/types"
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

	// IMPORTANT: TODO
	// Need to refactor logger to provide a NewFileLogger that logs to a file

	// Placeholder using existing logger (output will just be lost for now)

	outputStyle := types.StyleHuman
	if verbose {
		outputStyle = types.StyleHumanVerbose
	}

	logger := log.NewLogger(outputStyle)
	logger.Info("[Background:%s] Starting execution.", workflowIdStr)
	logger.Info("[Background:%s] Using config: %s", workflowIdStr, configPath)
	logger.Info("[Background:%s] Using log directory: %s", workflowIdStr, logDir)

	// --- Load grace.yml ---

	graceCfg, err := config.LoadGraceConfig(configPath)
	if err != nil {
		logger.Error("[Background:%s] Failed to load configuration: %v", workflowIdStr, err)
		os.Exit(1)
	}

	// --- Create context ---
	ctx := &context.ExecutionContext{
		WorkflowId:  workflowId,
		Config:      graceCfg,
		Logger:      logger,
		LogDir:      logDir,
		OutputStyle: outputStyle,
		SubmitOnly:  onlyFilter,
		GraceCmd:    "submit-bg",
	}

	// --- Instantiate and run orchestrator ---

	orch := orchestrator.NewZoweOrchestrator()
	logger.Info("[Background:%s] Invoking DAG executor...", workflowIdStr)
	startTimeForSummary := time.Now()
	jobExecutionRecords, execErr := orch.Run(ctx)

	// --- Process results & write summary

	if execErr != nil {
		logger.Error("[Background:%s] Orchestration failed: %v", workflowIdStr, execErr)
	} else {
		logger.Verbose("[Background:%s] Orchestration finished. Processing results...", workflowIdStr)
	}

	// Generate summary regardless of execErr, using potentially partial records
	summary := generateExecutionSummary(jobExecutionRecords, workflowId, startTimeForSummary, graceCfg, "submit-bg", onlyFilter)

	// Attempt to write summary
	err = writeSummary(summary, logDir)
	if err != nil {
		logger.Error("[Background:%s] Failed to write summary: %v", workflowIdStr, err)
	} else {
		logger.Info("[Background:%s] Workflow summary written to %s", workflowIdStr, filepath.Join(logDir, "summary.json"))
	}

	logger.Info("[Background:%s] Execution finished.", workflowIdStr)

	if execErr != nil {
		os.Exit(1) // Exit with error code if orchestration itself failed
	}

	os.Exit(0) // Exit successfully
}
