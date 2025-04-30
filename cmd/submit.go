package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/log"
	"github.com/graceinfra/grace/types"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(submitCmd)

	submitCmd.Flags().BoolVar(&wantJSON, "json", false, "Return structured JSON data about each job")
	submitCmd.Flags().StringSliceVar(&submitOnly, "only", nil, "Submit only specified job(s)")
}

var submitCmd = &cobra.Command{
	Use:   "submit",
	Short: "Upload datasets and submit jobs to the mainframe",
	Long: `Submit initiates the asynchronous execution of a workflow defined in grace.yml.

Grace will launch a detached background process to manage the full DAG execution,
including dependencies and concurrency, identical to 'grace run'. This command
returns immediately after successfully launching the background process.

Logs and the final summary for the workflow run will be written to a timestamped
directory within '.grace/logs/'. Use 'grace status <workflow-id>' (TBD) or
check the log files directly to monitor progress.

Assumes 'grace deck' has been run previously to prepare JCL and source files.`,
	Run: func(cmd *cobra.Command, args []string) {
		outputStyle := types.StyleHuman
		if Verbose {
			outputStyle = types.StyleHumanVerbose
		}

		logger := log.NewLogger(outputStyle)

		// --- Load and validate grace.yml ---

		configPath := "grace.yml"
		_, err := config.LoadGraceConfig(configPath)
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to load/validate %q: %w", configPath, err))
		}
		logger.Info("✓ Configuration %q loaded and validated.", configPath)

		// --- Prepare for background execution ---

		workflowId := uuid.New()
		workflowStartTime := time.Now()

		logDir, err := log.CreateLogDir(workflowId, workflowStartTime, "submit")
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to create log directory for workflow %s: %w", workflowId.String(), err))
		}
		logger.Info("Logs for workflow %s will be stored in: %s", workflowId.String(), logDir)

		// Find the currently running grace executable
		executablePath, err := os.Executable()
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to determine grace executable path: %w", err))
		}
		logger.Verbose("Found grace executable at: %s", executablePath)

		// Get absolute path to grace.yml for the background process
		absConfigPath, err := filepath.Abs(configPath)
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to get absolute path for config %q: %w", configPath, err))
		}

		// --- Prepare args for background process ---

		bgArgs := []string{
			"--internal-run",
			"--workflow-id", workflowId.String(),
			"--cfg-path", absConfigPath,
			"--cfg-path", absConfigPath,
			"--log-dir", logDir, // this includes '.grace/logs/' in the path
		}

		if len(submitOnly) > 0 {
			for _, jobName := range submitOnly {
				bgArgs = append(bgArgs, "--only", jobName)
			}
		}

		if Verbose {
			bgArgs = append(bgArgs, "--verbose")
		}

		// --- Create the command for background execution ---

		bgCmd := exec.Command(executablePath, bgArgs...)

		// Prevent inheriting std streams, this is crucial for detachment
		bgCmd.Stdin = nil
		bgCmd.Stdout = nil
		bgCmd.Stderr = nil

		// --- Set detachment attributes (Unix specific for now FUCK windows) ---

		// Creates a new session and detaches from the controlling terminal
		bgCmd.SysProcAttr = &syscall.SysProcAttr{
			Setsid: true,
		}
		// TODO: add //go:build windows section here later for Windows detachment flags

		logger.Info("Launching background process for workflow %s ...", workflowId.String())

		// --- Start the background process ---

		err = bgCmd.Start()
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to start background Grace process: %w", err))
		}

		logger.Info("✓ Workflow %s submitted successfully.", workflowId.String())
		logger.Info("  Logs will be written to: %s", logDir)
		logger.Info("  Use 'grace status %s' or 'grace dash' to check progress.", workflowId.String())
	},
}
