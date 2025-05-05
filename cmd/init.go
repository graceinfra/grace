package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/graceinfra/grace/internal/templates"
	"github.com/graceinfra/grace/internal/utils"
	"github.com/spf13/cobra"
)

var wantTutorial bool = false

func init() {
	rootCmd.AddCommand(initCmd)

	initCmd.Flags().BoolVar(&wantTutorial, "tutorial", false, `Initialize a 'Hello, Grace' tutorial workflow`)
}

var initCmd = &cobra.Command{
	Use:   "init [workspace-name]",
	Args:  cobra.MaximumNArgs(1),
	Short: "Scaffold a new Grace workflow",
	Long: `Initialize a new Grace workflow by scaffolding the required structure:
  - A starter grace.yml configuration file
  - A .grace/ directory for logs and deck output
  - A src/ directory for COBOL or GraceLang source files

This command can be used with an optional [workflow-name] argument, and will launch an interactive prompt to populate your HLQ, Zowe profile, and workflow name in grace.yml.

Use init to start building Grace workflows declaratively, with ready-to-run JCL templates and a clean IaC layout.`,
	Run: func(cmd *cobra.Command, args []string) {
		var hlq, profile, workflowName string
		var canceled bool

		if len(args) > 0 {
			hlq, profile, workflowName, canceled = RunInitTUI(args[0])
		} else {
			hlq, profile, workflowName, canceled = RunInitTUI("")
		}

		if canceled {
			fmt.Println("✖ Grace init canceled.")
			return
		}

		targetDir := workflowName
		jobName := workflowName

		fmt.Println("targetDir: ", targetDir)

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

		fmt.Printf("↪ Scaffolding new workflow %q ...\n", jobName)

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
			"WorkflowName": sanitizeWorkflowName(workflowName),
			"HLQ":          hlq,
			"Profile":      profile,
			"JobName":      jobName,
		}

		if wantTutorial {
			files := map[string]string{
				"files/tutorial/graceTutorial.yml": "grace.yml",
				"files/tutorial/hello.cbl":         "src/hello.cbl",
			}

			for srcPath, outPath := range files {
				sourceData, err := templates.TplFS.ReadFile(srcPath)
				if err != nil {
					cobra.CheckErr(err)
				}

				destPath := filepath.Join(targetDir, outPath)
				destFile, err := os.Create(destPath)
				if err != nil {
					cobra.CheckErr(err)
				}
				defer destFile.Close()

				err = os.WriteFile(destPath, sourceData, 0644)
				cobra.CheckErr(err)
			}

			fmt.Printf("✓ Workflow %q initialized!\n", workflowName)
			return
		}

		files := map[string]string{
			"files/grace.yml.tmpl": "grace.yml",
		}

		for tplPath, outName := range files {
			outPath := filepath.Join(targetDir, outName)
			utils.MustNotExist(outPath)
			templates.WriteTpl(tplPath, outPath, data)
		}

		fmt.Printf("✓ Workflow %q initialized!\n", workflowName)
	},
}

func sanitizeWorkflowName(name string) string {
	upper := strings.ToUpper(name)

	var sanitized []rune
	for _, r := range upper {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '#' || r == '@' || r == '$' {
			sanitized = append(sanitized, r)
		}
	}

	// Ensure it starts with a letter
	if len(sanitized) == 0 || !unicode.IsLetter(sanitized[0]) {
		sanitized = append([]rune{'A'}, sanitized...)
	}

	// Truncate to 8 characters
	if len(sanitized) > 8 {
		sanitized = sanitized[:8]
	}

	return string(sanitized)
}
