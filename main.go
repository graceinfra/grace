package main

import (
	"fmt"
	"os"

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
	}

	err := logging.SetupLogging(isVerbose, logFilePath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize logging")
	}

	// --- Execute command ---

	if isInternalRun {
		// Execute the background task directly
		cmd.RunBackgroundWorkflow(targetWorkflowId, targetCfgPath, targetLogDir, targetOnlyFilter, isVerbose)
	} else {
		// Normal CLI execution
		cmd.Execute()
	}
}
