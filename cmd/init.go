package cmd

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/spf13/cobra"
)

//go:embed templates/*
var tplFS embed.FS

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
			targetDir = "."        // scatter in cwd
			cwd, err := os.Getwd() // use base name for templating
			cobra.CheckErr(err)
			jobName = filepath.Base(cwd)
		}

		// If we are making a new subdirectory, ensure it doesn't already exist
		if targetDir != "." {
			mustNotExist(targetDir)
			err := os.MkdirAll(targetDir, 0755)
			cobra.CheckErr(err)
		}

		fmt.Printf("↪ scaffolding new workspace %q ...\n", jobName)

		// Ensure .grace/ directory does not exist
		mustNotExist(filepath.Join(targetDir, ".grace"))
		mkdir(targetDir, ".grace")
		mkdir(targetDir, ".grace", "deck")
		mkdir(targetDir, ".grace", "logs")

		// Copy each template to destination with template data
		data := map[string]string{"JobName": jobName}

		files := map[string]string{
			"templates/grace.yml.tpl": "grace.yml",
		}

		for tplPath, outName := range files {
			outPath := filepath.Join(targetDir, outName)
			mustNotExist(outPath)
			writeTpl(tplPath, outPath, data)
		}

		fmt.Printf("✓ workspace %q initialized!\n", jobName)
	},
}

// writeTpl loads tplName from tplFS, executes it with data, and writes to outPath
func writeTpl(tplName, outPath string, data any) {
	t, err := template.ParseFS(tplFS, tplName)
	cobra.CheckErr(err)

	f, err := os.Create(outPath)
	cobra.CheckErr(err)
	defer f.Close()

	err = t.Execute(f, data)
	cobra.CheckErr(err)
}

// Helper to create directory structure
func mkdir(targetDir string, parts ...string) {
	path := filepath.Join(append([]string{targetDir}, parts...)...)
	err := os.MkdirAll(path, 0755)
	cobra.CheckErr(err)
}

// Helper to check if a dir already exists
func mustNotExist(path string) {
	if _, err := os.Stat(path); err == nil {
		cobra.CheckErr(fmt.Errorf("refusing to overwrite existing file or directory: %s", path))
	}
}
