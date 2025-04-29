package cmd

import (
	"fmt"
	"os"

	"github.com/graceinfra/grace/internal/config"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(lintCmd)
}

var lintCmd = &cobra.Command{
	Use:   "lint",
	Short: "Validate the syntax and structure of a grace.yml file",
	Long: `Lint checks a grace.yml file for correctness according to Grace's schema and rules.
It validates required fields, data types, dataset naming conventions, PDS member name formats,
known step types, and other structural requirements without executing any part of the workflow.

Use this command to check your configuration file before running 'deck', 'run', or 'submit'.`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		lintFile := "grace.yml"

		if len(args) > 0 {
			lintFile = args[0]
		}

		fmt.Printf("Linting file: %s\n", lintFile)

		_, err := config.LoadGraceConfig(lintFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "✖ Validation failed: %v\n", err)
			os.Exit(1)
		} else {
			fmt.Printf("✓ %s is valid!\n", lintFile)
		}
	},
}
