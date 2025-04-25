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
	"github.com/graceinfra/grace/types"
	"github.com/graceinfra/grace/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var deckJobs []string

func init() {
	rootCmd.AddCommand(deckCmd)

	deckCmd.Flags().StringSliceVar(&deckJobs, "only", nil, "Only deck specified job(s)")
}

var deckCmd = &cobra.Command{
	Use:   "deck",
	Short: "Generate and upload JCL and COBOL source files from a grace.yml workflow definition",
	Long: `Deck processes a grace.yml workflow file and generates JCL job scripts based on each job's defined source and template. 
For each job, it renders a standalone .jcl file in the .grace/deck/ directory and uploads both the JCL and COBOL source files to the appropriate data sets on the mainframe.

This prepares all required inputs for batch job submission via Grace, ensuring both JCL and COBOL source members are available in your configured PDS libraries. 
Deck supports templated compilation, custom templates, and selective job targeting via the --only flag.

Use deck to prepare and stage mainframe batch jobs before invoking [grace run] or [grace submit].`,
	Run: func(cmd *cobra.Command, args []string) {
		os.MkdirAll(filepath.Join(".grace", "deck"), os.ModePerm)

		wantVerbose := Verbose

		// Read grace.yml
		ymlData, err := os.ReadFile("grace.yml")
		if err != nil {
			cobra.CheckErr(err)
		}

		var graceCfg types.GraceConfig
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
			srcPath := filepath.Join("src", job.Source)

			_, err = os.Stat(srcPath)
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Error resolving " + job.Source + ". Does it exist at " + srcPath + "?"))
			}

			// Read COBOL file
			src, err := os.ReadFile(srcPath)
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Failed to read COBOL source %s: %w", srcPath, err))
			}

			var templatePath string

			if job.Template != "" {
				templatePath = job.Template
			} else if step != "" {
				switch step {
				case "execute":
					templatePath = "files/execute.jcl.tmpl"
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

			// --- JCL and COBOL upload to mainframe ---

			cfgJcl := graceCfg.Datasets.JCL
			if cfgJcl == "" {
				cobra.CheckErr(fmt.Errorf("Error resolving target JCL data set. Is the jcl field set in grace.yml?"))
			}

			cfgSrc := graceCfg.Datasets.SRC
			if cfgSrc == "" {
				cobra.CheckErr(fmt.Errorf("Error resolving target COBOL source data set. Is the src field set in grace.yml?"))
			}

			jclPath := filepath.Join(".grace", "deck", jclFileName)
			_, err = os.Stat(jclPath)
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Error resolving " + jclFileName + ". Does it exist at " + jclPath + "?"))
			}

			utils.VerboseLog(wantVerbose, "Allocating on %s ...", "mainframe") // TODO: read zowe config and determine target mainframe ip/hostname

			err = utils.ValidateDataSetQualifiers(cfgJcl)
			if err != nil {
				cobra.CheckErr(err)
			}

			err = utils.ValidateDataSetQualifiers(cfgSrc)
			if err != nil {
				cobra.CheckErr(err)
			}

			err = utils.EnsurePDSExists(cfgJcl, wantVerbose)
			if err != nil {
				cobra.CheckErr(err)
			}

			err = utils.EnsurePDSExists(cfgSrc, wantVerbose)
			if err != nil {
				cobra.CheckErr(err)
			}

			jclMember := fmt.Sprintf("\"%s(%s)\"", cfgJcl, strings.ToUpper(jobName))
			srcMember := fmt.Sprintf("\"%s(%s)\"", cfgSrc, strings.ToUpper(jobName))

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

			utils.VerboseLog(!wantVerbose, fmt.Sprintf("Uploading source %s ...", job.Source))
			s.Start()

			out, err = utils.RunZowe(false, true, "zos-files", "upload", "file-to-data-set", srcPath, srcMember, "--rfj")
			if err != nil {
				cobra.CheckErr(err)
			}

			s.Stop()

			err = json.Unmarshal(out, &uploadRes)
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Unexpected API response structure"))
			}

			if !uploadRes.Data.Success {
				cobra.CheckErr(fmt.Errorf("COBOL source upload to data set failed: %s\n", uploadRes.Data.Error.Msg))
			}

			utils.VerboseLog(true, fmt.Sprintf("\n✓ COBOL data set submitted for job %s\nFrom: %s\nTo: %s\n", jobName, uploadRes.Data.APIResponse[0].From, uploadRes.Data.APIResponse[0].To))
		}
	},
}
