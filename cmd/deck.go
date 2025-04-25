package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/graceinfra/grace/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var deckJobs []string

func init() {
	rootCmd.AddCommand(deckCmd)

	deckCmd.Flags().StringSliceVar(&deckJobs, "only", nil, "Deck only specified job(s)")
}

var deckCmd = &cobra.Command{
	Use:   "deck",
	Short: "Generate JCL job scripts from grace.yml and COBOL source files",
	Long: `Deck reads a grace.yml configuration file and generates JCL job scripts into the .grace/deck/ directory.

Each job defined in grace.yml is rendered into a standalone .jcl file. These files are ready for inspection, testing, or submission to a mainframe.

Use deck to define and compile multi-step mainframe workflows in YAML - including GraceLang compilation, COBOL execution, and job control logic - without performing any network operations or file uploads.`,
	Run: func(cmd *cobra.Command, args []string) {
		os.MkdirAll(filepath.Join(".grace", "deck"), os.ModePerm)

		wantVerbose := Verbose

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

			// If --only flag is set, deck the current job only if it is included in the args
			if len(deckJobs) > 0 && !slices.Contains(deckJobs, jobName) {
				continue
			}

			// Construct path to COBOL file
			sourcePath := filepath.Join("src", job.Source)

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
			} else {
				cobra.CheckErr(fmt.Errorf("No template found for step %s", step))
			}

			// Render JCL
			data := map[string]string{
				"JobName":     strings.ToUpper(jobName),
				"CobolSource": string(src),
				"LoadLib":     graceCfg.Datasets.LoadLib,
			}

			jclFileName := fmt.Sprintf(jobName + ".jcl")

			outPath := filepath.Join(".grace", "deck", jclFileName)

			err = utils.WriteTpl(templatePath, outPath, data)
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Failed to write %s: %w", jclFileName, err))
			}

			fmt.Printf("✓ JCL for job %q generated at %s\n", jobName, outPath)

			// --- JCL upload to mainframe ---

			utils.VerboseLog(wantVerbose, "Checking allocation on %s ...", "mainframe") // TODO: read zowe config and determine target mainframe ip/hostname

			cfgJcl := graceCfg.Datasets.JCL
			if cfgJcl == "" {
				cobra.CheckErr(fmt.Errorf("Error resolving JCL data set. Is the jcl field set in grace.yml?"))
			}

			jclPath := filepath.Join(".grace", "deck", jclFileName)
			_, err = os.Stat(jclPath)
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Error resolving " + jclFileName + ". Does it exist at " + jclPath + "?"))
			}

			err = utils.ValidateDataSetQualifiers(cfgJcl)
			if err != nil {
				cobra.CheckErr(err)
			}

			err = utils.EnsurePDSExists(cfgJcl, wantVerbose)
			if err != nil {
				cobra.CheckErr(err)
			}

			jclMember := fmt.Sprintf("\"%s(%s)\"", cfgJcl, strings.ToUpper(jobName))

			var uploadRes struct {
				Data struct {
					Success     bool `json:"success"`
					APIResponse []struct {
						Success bool   `json:"success"`
						From    string `json:"from"`
						To      string `json:"to"`
					} `json:"apiResponse"`
					Error struct {
						Msg string `json:"msg,omitempty"`
					} `json:"error,omitempty"`
				} `json:"data"`
			}

			utils.VerboseLog(!wantVerbose, fmt.Sprintf("Uploading deck %s ...", strings.ToUpper(job.Name)))
			s := spinner.New(spinner.CharSets[43], 100*time.Millisecond)
			s.Start()

			out, err := utils.RunZowe(false, true, "zos-files", "upload", "file-to-data-set", jclPath, jclMember, "--rfj")
			if err != nil {
				cobra.CheckErr(err)
			}

			s.Stop()

			err = json.Unmarshal(out, &uploadRes)
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Unexpected API response structure"))
			}

			if !uploadRes.Data.Success {
				cobra.CheckErr(fmt.Errorf("JCL upload to data set failed: %s\n", uploadRes.Data.Error.Msg))
			}

			utils.VerboseLog(true, fmt.Sprintf("\n✓ JCL data set submitted for job %s\nFrom: %s\nTo: %s\n", jobName, uploadRes.Data.APIResponse[0].From, uploadRes.Data.APIResponse[0].To))
		}
	},
}
