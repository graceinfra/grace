package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
	"github.com/graceinfra/grace/internal/templates"
	"github.com/graceinfra/grace/internal/zowe"
	"github.com/graceinfra/grace/types"
	"github.com/spf13/cobra"
)

var (
	deckJobs  []string
	noUpload  bool
	noCompile bool
)

func init() {
	rootCmd.AddCommand(deckCmd)

	deckCmd.Flags().StringSliceVar(&deckJobs, "only", nil, "Only deck specified job(s)")
	deckCmd.Flags().BoolVar(&noCompile, "no-compile", false, "Upload JCL or COBOL to remote mainframe data sets without recompiling JCL")
	deckCmd.Flags().BoolVar(&noUpload, "no-upload", false, "Only compile JCL, but don't upload JCL or COBOL to remote mainframe data sets")
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
		// Ensure .grace/deck/ exists
		err := os.MkdirAll(filepath.Join(".grace", "deck"), os.ModePerm)
		cobra.CheckErr(err)

		var outputStyle types.OutputStyle
		switch {
		case Verbose:
			outputStyle = types.StyleHumanVerbose
		default:
			outputStyle = types.StyleHuman
		}

		// --- Load and validate grace.yml ---

		graceCfg, err := config.LoadGraceConfig("grace.yml")
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to load grace configuration: %w", err))
		}

		// --- Initialize logger ---

		logger := log.NewLogger(outputStyle)

		// --- Initialize ExecutionContext ---

		ctx := &context.ExecutionContext{
			Config:      graceCfg,
			Logger:      logger,
			OutputStyle: outputStyle,
			SubmitOnly:  submitOnly,
			GraceCmd:    "deck",
		}

		// --- Run decking logic ---

		for _, job := range graceCfg.Jobs {
			jobName := job.Name
			step := job.Step

			// If --only flag is set, deck the current job only if it is included in the args
			if len(deckJobs) > 0 && !slices.Contains(deckJobs, jobName) {
				continue
			}

			jclFileName := fmt.Sprintf(jobName + ".jcl")

			if !noCompile {
				// --- Render JCL ---
				// Skip this step if --no-compile flag is set

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

				cobolDsn := graceCfg.Datasets.SRC + "(" + strings.ToUpper(jobName) + ")"

				data := map[string]string{
					"JobName":  strings.ToUpper(jobName),
					"CobolDSN": cobolDsn,
					"LoadLib":  graceCfg.Datasets.LoadLib,
				}

				outPath := filepath.Join(".grace", "deck", jclFileName)

				err = templates.WriteTpl(templatePath, outPath, data)
				if err != nil {
					cobra.CheckErr(fmt.Errorf("Failed to write %s: %w", jclFileName, err))
				}

				logger.Info("✓ JCL for job %q generated at %s", jobName, outPath)
			}

			// --- Skip upload step if --no-upload ---

			if noUpload {
				continue
			}

			// --- JCL and COBOL upload to target data sets ---

			// Resolve JCL target
			cfgJcl := graceCfg.Datasets.JCL
			if cfgJcl == "" {
				cobra.CheckErr(fmt.Errorf("Error resolving target JCL data set. Is the jcl field set in grace.yml?"))
			}

			// Resolve COBOL source target
			cfgSrc := graceCfg.Datasets.SRC
			if cfgSrc == "" {
				cobra.CheckErr(fmt.Errorf("Error resolving target COBOL source data set. Is the src field set in grace.yml?"))
			}

			// Construct JCL path
			jclPath := filepath.Join(".grace", "deck", jclFileName)
			_, err = os.Stat(jclPath)
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Error resolving " + jclFileName + ". Does it exist at " + jclPath + "?"))
			}

			logger.Info("Allocating on %s ...", "mainframe") // TODO: read zowe config and determine target mainframe ip/hostname

			// Ensure target PDS exist
			err = zowe.EnsurePDSExists(ctx, cfgJcl)
			cobra.CheckErr(err)

			err = zowe.EnsurePDSExists(ctx, cfgSrc)
			cobra.CheckErr(err)

			srcMember := fmt.Sprintf("%s(%s)", cfgSrc, strings.ToUpper(jobName))

			// --- Upload JCL to target data set ---

			// UploadJCL has its own spinner
			err = zowe.UploadJCL(ctx, job)
			if err != nil {
				cobra.CheckErr(err)
			}

			// --- Upload COBOL source to target data set ---
			// LoadGraceConfig ensures job.Source is present for 'execute' step.
			if job.Source == "" {
				// If other steps might not have source, skip upload
				logger.Verbose("Skipping COBOL source upload for job %q (no source defined in grace.yml)", jobName)
				continue
			}

			spinnerText := fmt.Sprintf("Uploading COBOL source %s ...", strings.ToUpper(job.Source))
			ctx.Logger.StartSpinner(spinnerText)

			// Construct path to COBOL file
			srcPath := filepath.Join("src", job.Source)
			_, err := os.Stat(srcPath)
			if err != nil {
				ctx.Logger.StopSpinner()
				cobra.CheckErr(fmt.Errorf("unable to resolve %s", srcPath))
			}

			res, err := zowe.UploadFileToDataset(ctx, srcPath, srcMember)
			if err != nil {
				ctx.Logger.StopSpinner()
				cobra.CheckErr(fmt.Errorf("COBOL source upload to data set failed: %w\n", err))
			}

			ctx.Logger.StopSpinner()

			ctx.Logger.Info(fmt.Sprintf("✓ COBOL data set %s submitted for job %q\n", job.Source, jobName))
			ctx.Logger.Verbose(fmt.Sprintf("  From: %s", res.Data.APIResponse[0].From))
			ctx.Logger.Verbose(fmt.Sprintf("  To:   %s", res.Data.APIResponse[0].To))
		}
	},
}
