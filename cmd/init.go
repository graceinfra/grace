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
		var (
			targetDir string
			jobName   string
		)

		// Determine where to write files
		// targetDir is where files go, jobName is for templating
		if len(args) == 1 {
			targetDir = args[0]
			jobName = args[0]
		} else {
			targetDir = "." // scatter init files in cwd
			cwd, err := os.Getwd()
			cobra.CheckErr(err)
			jobName = filepath.Base(cwd) // use cwd name as job name
		}

		// If we are making a new subdirectory, ensure it doesn't already exist
		if targetDir != "." {
			utils.MustNotExist(targetDir)
			err := os.MkdirAll(targetDir, 0755)
			cobra.CheckErr(err)
		}

		fmt.Printf("↪ scaffolding new workspace %q ...\n", jobName)

		// Ensure .grace/ directory does not exist
		utils.MustNotExist(filepath.Join(targetDir, ".grace"))
		utils.MkDir(targetDir, ".grace")
		utils.MkDir(targetDir, ".grace", "deck")
		utils.MkDir(targetDir, ".grace", "logs")

		// Copy each template to destination with template data
		data := map[string]string{"JobName": jobName}

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
