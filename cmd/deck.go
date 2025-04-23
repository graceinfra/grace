package cmd

import (
	"fmt"
	"os"
	"os/exec"
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
	Use:   "deck [COBOL-source.cbl]",
	Args:  cobra.ExactArgs(1),
	Short: "Generate JCL job decks from grace.yml",
	Long: `Deck takes a grace.yml file and COBOL source file and generates JCL job scripts into the .grace/deck/ directory.
Each job defined in grace.yml is translated into a standalone .jcl file, ready for submission to a mainframe or emulator.
Use this to define multi-step mainframe pipelines in YAML - including Grace source compilation, COBOL emission, and JCL execution with parameter injection.`,
	Run: func(cmd *cobra.Command, args []string) {
		sourcePath := args[0]

		// Read COBOL file
		src, err := os.ReadFile(sourcePath)
		cobra.CheckErr(err)

		// Read grace.yml
		ymlData, err := os.ReadFile("grace.yml")
		if err != nil {
			cobra.CheckErr(err)
		}

		var cfg utils.GraceConfig
		err = yaml.Unmarshal(ymlData, &cfg)
		if err != nil {
			cobra.CheckErr(err)
		}

		jobName := filepath.Base(sourcePath)
		jobName = strings.TrimSuffix(jobName, filepath.Ext(jobName)) // strip .cbl

		data := map[string]string{
			"JobName":     strings.ToUpper(jobName),
			"CobolSource": string(src),
		}

		// Emit JCL
		outPath := filepath.Join(".grace", "deck", jobName+".jcl")
		utils.WriteTpl("files/job.jcl.tpl", outPath, data)

		fmt.Printf("✓ JCL for %q generated at %s\n", jobName, outPath)

		// Upload generated JCL to a dataset
		// zowe zos-files upload file-to-data-set ".grace/deck/hello.jcl" "Z71041.GRC.HELLO.JCL"
		qualifier := fmt.Sprintf("%s.%s", cfg.Datasets.Prefix, strings.ToUpper(cfg.Jobs[0].JCL))
		zoweCmd := exec.Command("zowe", "zos-files", "upload", "file-to-data-set", outPath, qualifier)
		zoweCmd.Stdout = os.Stdout
		zoweCmd.Stderr = os.Stderr
		zoweCmd.Stdin = os.Stdin
		err = zoweCmd.Run()
	},
}
