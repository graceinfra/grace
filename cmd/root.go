package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var Verbose bool

var rootCmd = &cobra.Command{
	Use:   "grace",
	Short: "Grace is a modern IaC and COBOL developer experience",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Grace CLI: mainframe automation, reborn.")
	},
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Enable verbose logs to stderr")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
