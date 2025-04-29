package cmd

import (
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
	Long: `Submit reads generated JCL decks from .grace/deck/ and handles all the steps required to run them on a remote mainframe environment.

This includes uploading COBOL source, input files, or other required datasets to the target system via Zowe CLI, and submitting each job through zos-jobs.

By default, it prints a summary of each job submission, including the job name, ID, and status. Use the --json flag to retrieve raw structured output.

NOTE: This command is fire-and-forget. It runs asynchronously and DOES NOT wait for jobs to complete. Use [grace run] to wait for job execution and manually monitor results.`,
	Run: func(cmd *cobra.Command, args []string) {
		return
	},
}
