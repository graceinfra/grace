package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/graceinfra/grace/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func init() {
	rootCmd.AddCommand(deckCmd)
}

var deckCmd = &cobra.Command{
	Use:   "deck",
	Short: "Generate JCL job scripts from grace.yml and COBOL source files",
	Long: `Deck reads a grace.yml configuration file and generates JCL job scripts into the .grace/deck/ directory.

Each job defined in grace.yml is rendered into a standalone .jcl file. These files are ready for inspection, testing, or submission to a mainframe.

Use deck to define and compile multi-step mainframe workflows in YAML - including GraceLang compilation, COBOL execution, and job control logic - without performing any network operations or file uploads.`,
	Run: func(cmd *cobra.Command, args []string) {
		os.MkdirAll(filepath.Join(".grace", "deck"), os.ModePerm)

		// Read grace.yml
		ymlData, err := os.ReadFile("grace.yml")
		if err != nil {
			cobra.CheckErr(err)
		}

		var graceCfg utils.GraceConfig
		err = yaml.Unmarshal(ymlData, &graceCfg)
		if err != nil {
			cobra.CheckErr(fmt.Errorf("Failed to read grace.yml: %w", err))
		}

		for _, job := range graceCfg.Jobs {
			jobName := job.Name
			step := job.Step

			// Construct path to COBOL file
			sourcePath := filepath.Join(".grace", "src", job.Source)

			// Read COBOL file
			src, err := os.ReadFile(sourcePath)
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Failed to read COBOL source %s: %w", sourcePath, err))
			}

			var templatePath string

			if job.Template != "" {
				templatePath = job.Template
			} else if step != "" {
				switch step {
				case "execute":
					templatePath = "files/execute.jcl.tpl"
				default:
					cobra.CheckErr(fmt.Errorf("Unsupported step: %s", step))
				}
			}

			// Render JCL
			data := map[string]string{
				"JobName":       strings.ToUpper(jobName),
				"CobolSource":   string(src),
				"DatasetPrefix": graceCfg.Datasets.Prefix,
			}

			outPath := filepath.Join(".grace", "deck", jobName+".jcl")

			err = utils.WriteTpl(templatePath, outPath, data)
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Failed to write %s: %w", jobName+".jcl", err))
			}

			fmt.Printf("✓ JCL for job %q generated at %s\n", jobName, outPath)
		}
	},
}
