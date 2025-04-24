package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/graceinfra/grace/utils"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(initCmd)
}

var initCmd = &cobra.Command{
	Use:   "init [workspace-name]",
	Args:  cobra.MaximumNArgs(1),
	Short: "Bootstrap a new Grace workspace",
	Long: `Bootstrap a new Grace workspace with:
    - A starter grace.yml
    - ".grace/" output directory

This command is used to scaffold mainframe infrastructure-as-code pipelines
using Grace's declarative job format.`,
	Run: func(cmd *cobra.Command, args []string) {
		var hlq, profile, workspaceName string
		var canceled bool

		if len(args) > 0 {
			hlq, profile, workspaceName, canceled = RunInitTUI(args[0])
		} else {
			hlq, profile, workspaceName, canceled = RunInitTUI("")
		}

		if canceled {
			fmt.Println("✖ Grace init canceled.")
			return
		}

		targetDir := workspaceName
		jobName := workspaceName

		// If current directory (default) selected, set jobName to cwd
		if jobName == "." {
			cwd, _ := os.Getwd()
			jobName = filepath.Base(cwd)
		}

		// If we are making a new subdirectory, ensure it doesn't already exist
		if targetDir != "." {
			utils.MustNotExist(targetDir)
			err := os.MkdirAll(targetDir, 0755)
			cobra.CheckErr(err)
		}

		fmt.Printf("↪ scaffolding new workspace %q ...\n", jobName)

		// Ensure .grace or src directory does not exist
		utils.MustNotExist(filepath.Join(targetDir, ".grace"))
		utils.MustNotExist(filepath.Join(targetDir, "src"))

		// Create directory structure
		utils.MkDir(targetDir, "src")
		utils.MkDir(targetDir, ".grace")
		utils.MkDir(targetDir, ".grace", "deck")
		utils.MkDir(targetDir, ".grace", "logs")

		// Copy each template to destination with template data
		data := map[string]string{
			"HLQ":     hlq,
			"Profile": profile,
			"JobName": jobName,
		}

		files := map[string]string{
			"files/grace.yml.tpl": "grace.yml",
		}

		for tplPath, outName := range files {
			outPath := filepath.Join(targetDir, outName)
			utils.MustNotExist(outPath)
			utils.WriteTpl(tplPath, outPath, data)
		}

		fmt.Printf("✓ workspace %q initialized!\n", jobName)
	},
}
