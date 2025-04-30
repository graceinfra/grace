package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	Verbose bool

	// Internal flags, hidden from user help
	internalRun  bool
	workflowId   string
	cfgPath      string
	internalOnly []string // For --only flag passed to bg process
)

var rootCmd = &cobra.Command{
	Use:   "grace",
	Short: "Grace is a modern IaC and COBOL developer experience",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Grace CLI: mainframe automation, reborn.")
	},
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Enable verbose logs to stderr")

	rootCmd.PersistentFlags().BoolVar(&internalRun, "internal-run", false, "Internal flag to trigger background workflow execution")
	rootCmd.PersistentFlags().StringVar(&workflowId, "workflow-id", "", "Internal flag for background workflow ID")
	rootCmd.PersistentFlags().StringVar(&cfgPath, "cfg-path", "grace.yml", "Internal flag for background workflow config path")
	rootCmd.PersistentFlags().StringVar(&cfgPath, "log-dir", "", "Internal flag for background workflow log directory path")
	rootCmd.PersistentFlags().StringSliceVar(&internalOnly, "only", nil, "Internal flag mirroring --only for background process") // Re-define --only for internal use

	// Hide internal flags from help output
	_ = rootCmd.PersistentFlags().MarkHidden("internal-run")
	_ = rootCmd.PersistentFlags().MarkHidden("workflow-id")
	_ = rootCmd.PersistentFlags().MarkHidden("cfg-path")
	_ = rootCmd.PersistentFlags().MarkHidden("log-dir")
	_ = rootCmd.PersistentFlags().MarkHidden("only")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
