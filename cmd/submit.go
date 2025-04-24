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
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

var (
	jsn        bool
	spool      bool
	submitOnly []string
)

func init() {
	rootCmd.AddCommand(submitCmd)

	submitCmd.Flags().BoolVar(&jsn, "json", false, "Return structured JSON data about each job")
	submitCmd.Flags().BoolVar(&spool, "spool", false, "Return full spool content for each job")
	submitCmd.Flags().StringSliceVar(&submitOnly, "only", nil, "Submit only specified job(s)")
}

var submitCmd = &cobra.Command{
	Use:   "submit",
	Short: "Upload datasets and submit jobs to the mainframe",
	Long: `Submit reads generated JCL decks from .grace/deck/ and handles all the steps required to run them on a remote mainframe environment.

This includes uploading COBOL source, input files, or other required datasets to the target system via Zowe CLI, and submitting each job through zos-jobs.

By default, it prints a summary of each job submission, including the job name, ID, and status. Use the --json or --spool flags to retrieve raw structured output.

NOTE: This command is fire-and-forget. It does not wait for jobs to complete. Use [grace run] to orchestrate job execution and monitor results.`,
	Run: func(cmd *cobra.Command, args []string) {
		if jsn && spool {
			cobra.CheckErr("--json and --spool cannot be used together")
		}

		quiet := jsn || spool
		verbose := Verbose
		useSpinner := term.IsTerminal(int(os.Stdout.Fd())) && !quiet

		ymlData, err := os.ReadFile("grace.yml")
		cobra.CheckErr(err)

		var graceCfg utils.GraceConfig
		err = yaml.Unmarshal(ymlData, &graceCfg)
		cobra.CheckErr(err)

		logDir, _, err := utils.CreateLogDir("submit")
		cobra.CheckErr(err)

		summaryJobs := []utils.JobSummaryEntry{}

		for _, job := range graceCfg.Jobs {
			if len(submitOnly) > 0 && !slices.Contains(submitOnly, job.Name) {
				continue
			}

			jclPath := filepath.Join(".grace", "deck", job.Name+".jcl")
			qualifier := graceCfg.Datasets.Prefix + "." + strings.ToUpper(job.Name+".jcl")

			// --- zowe CLI upload file to data set ---

			utils.VerboseLog(verbose, "Uploading job %s to %s", job.Name, qualifier)
			_, err = utils.RunZowe(verbose, quiet, "zos-files", "upload", "file-to-data-set", jclPath, qualifier)
			cobra.CheckErr(err)
			utils.VerboseLog(verbose, "✓ Upload complete")

			// --- zowe CLI submit data set ---

			zArgs := []string{"zos-jobs", "submit", "data-set", qualifier}
			if spool {
				zArgs = append(zArgs, "--vasc")
			} else {
				zArgs = append(zArgs, "--rfj")
			}

			if !quiet && useSpinner {
				s := spinner.New(spinner.CharSets[43], 100*time.Millisecond)
				s.Start()
				defer s.Stop()
			}

			// Pass 'quiet' param as true so we don't immediately print JSON response.
			// If --json,  ParseAndPrintJobResult will print JSON res.
			// If --spool, spool output will be printed in the if spool { ... }  block
			out, err := utils.RunZowe(verbose, true, zArgs...)
			cobra.CheckErr(err)

			if spool {
				jobId, jobName := utils.ParseSpoolMeta(out)
				err := os.WriteFile(filepath.Join(logDir, jobId+"_"+strings.ToUpper(jobName)+".spool.log"), out, 0644)
				if err != nil {
					utils.VerboseLog(true, "⚠️ Failed to write spool log: %v", err)
				}
				_ = utils.SaveZoweLog(logDir, utils.NewLogContext(job, jobId, jobName, graceCfg), "spooled")
				summaryJobs = append(summaryJobs, utils.JobSummaryEntry{
					Name:    job.Name,
					ID:      jobId,
					Status:  "SPOOLED",
					Step:    job.Step,
					Spooled: true,
				})
				fmt.Fprintf(os.Stderr, string(out)+"\n")
				continue
			}

			result, err := utils.ParseAndPrintJobResult(out, jsn, quiet)
			cobra.CheckErr(err)
			_ = utils.SaveZoweLog(logDir, utils.NewLogContext(job, result.Data.JobID, result.Data.JobName, graceCfg), result)
			summaryJobs = append(summaryJobs, utils.JobSummaryEntry{
				Name:   job.Name,
				ID:     result.Data.JobID,
				Status: result.Data.Status,
				Step:   job.Step,
			})
		}

		summary := utils.SubmitSummary{
			Timestamp:   time.Now().Format(time.RFC3339),
			GraceCmd:    "submit",
			ZoweProfile: graceCfg.Config.Profile,
			HLQ:         graceCfg.Datasets.Prefix,
			Initiator: utils.Initiator{
				Type:   "user",
				Id:     os.Getenv("USER"),
				Tenant: "nara",
			},
			Jobs: summaryJobs,
		}

		f, err := os.Create(filepath.Join(logDir, "summary.json"))
		cobra.CheckErr(err)
		defer f.Close()
		err = json.NewEncoder(f).Encode(summary)
		cobra.CheckErr(err)
	},
}
