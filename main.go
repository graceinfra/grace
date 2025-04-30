package main

import (
	"os"

	"github.com/graceinfra/grace/cmd"
)

func main() {
	// Check if launched in internal background mode before executing normal commands
	isInternalRun := false
	targetWorkflowId := ""
	targetCfgPath := ""
	targetOnlyFilter := []string{}
	logDir := ""
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
			logDir = os.Args[i+1]
		}
		if arg == "--verbose" || arg == "-v" {
			isVerbose = true
		}
	}

	if isInternalRun {
		// Execute the background task directly
		cmd.RunBackgroundWorkflow(targetWorkflowId, targetCfgPath, logDir, targetOnlyFilter, isVerbose)
	} else {
		// Normal CLI execution
		cmd.Execute()
	}
}
