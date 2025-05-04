package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/graceinfra/grace/cmd"
	"github.com/graceinfra/grace/internal/logging"
	"github.com/rs/zerolog/log"
)

func main() {
	// Check if launched in internal background mode before executing normal commands
	isInternalRun := false
	targetWorkflowId := ""
	targetCfgPath := ""
	targetOnlyFilter := []string{}
	targetLogDir := ""
	isVerbose := false

	for i, arg := range os.Args {
		if arg == "--internal-run" {
			isInternalRun = true
		}
		if arg == "--workflow-id" && i+1 < len(os.Args) {
			targetWorkflowId = os.Args[i+1]
		}
		if arg == "--cfg-path" && i+1 < len(os.Args) {
			targetCfgPath = os.Args[i+1]
		}
		// Collect all --only arguments
		if arg == "--only" && i+1 < len(os.Args) {
			targetOnlyFilter = append(targetOnlyFilter, os.Args[i+1])
		}
		if arg == "--log-dir" && i+1 < len(os.Args) {
			targetLogDir = os.Args[i+1]
		}
		if arg == "--verbose" || arg == "-v" {
			isVerbose = true
		}
	}

	logFilePath := "" // Default to terminal logging

	if isInternalRun {
		if targetLogDir == "" {
			fmt.Fprintln(os.Stderr, "Background Error: Log directory must be provided via --log-dir for internal run.")
			os.Exit(1)
		}

		logFilePath = filepath.Join(targetLogDir, "workflow.log")
	}

	err := logging.ConfigureGlobalLogger(isVerbose, logFilePath)
	if err != nil {
		// Fallback to basic stderr if logger setup fails
		fmt.Fprintf(os.Stderr, "FATAL: Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}

	// --- Execute command ---

	if isInternalRun {
		log.Info().Msgf("[Background Startup] Running background workflow %s", targetWorkflowId)
		cmd.RunBackgroundWorkflow(targetWorkflowId, targetCfgPath, targetLogDir, targetOnlyFilter)
	} else {
		log.Debug().Msg("Starting Grace CLI command execution")
		cmd.Execute()
	}
}
