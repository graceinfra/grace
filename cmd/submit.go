package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
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
	submitJobs []string
)

func init() {
	rootCmd.AddCommand(submitCmd)

	submitCmd.Flags().BoolVar(&jsn, "json", false, "Return structured JSON data about each job")
	submitCmd.Flags().BoolVar(&spool, "spool", false, "Return full spool content for each job")
	submitCmd.Flags().StringSliceVar(&submitJobs, "only", nil, "Submit only specified job(s)")
}

var submitCmd = &cobra.Command{
	Use:   "submit",
	Short: "Upload datasets and submit jobs to the mainframe",
	Long: `Submit reads generated JCL decks from .grace/deck/ and handles all the steps required to run them on a remote mainframe environment.

This includes uploading COBOL source, input files, or other required datasets to the target system via Zowe CLI, and submitting each job through zos-jobs.

By default, it prints a summary of each job submission, including the job name, ID, and status. Use the --json or --spool flags to retrieve raw structured output.

NOTE: This command is fire-and-forget. It does not wait for jobs to complete. Use [grace run] to orchestrate job execution and monitor results.`,
	Run: func(cmd *cobra.Command, args []string) {
		if spool && jsn {
			cobra.CheckErr("--spool and --json cannot be used together")
		}

		summaryJobs := []utils.JobSummaryEntry{}

		// Read grace.yml
		ymlData, err := os.ReadFile("grace.yml")
		if err != nil {
			cobra.CheckErr(err)
		}

		var graceCfg utils.GraceConfig
		err = yaml.Unmarshal(ymlData, &graceCfg)
		if err != nil {
			cobra.CheckErr(err)
		}

		// Create and store log dir and timestamp
		logDir, _, err := utils.CreateLogDir("submit")

		for _, job := range graceCfg.Jobs {
			// If --only flag is set, submit the current job only if it is included in the args
			if len(submitJobs) > 0 && !slices.Contains(submitJobs, job.Name) {
				continue
			}

			jclFile := job.Name + ".jcl"
			jclPath := filepath.Join(".grace", "deck", jclFile)

			if _, err := os.Stat(jclPath); os.IsNotExist(err) {
				cobra.CheckErr(fmt.Errorf("JCL source file not found. Did you run [grace deck]?"))
			}

			qualifier := fmt.Sprintf("%s.%s", graceCfg.Datasets.Prefix, strings.ToUpper(jclFile)) // IBMUSER.GRC.JOBNAME.JCL

			// --- Upload generated JCL as a dataset ---

			// zowe zos-files upload file-to-data-set ".grace/deck/jobname.jcl" "IBMUSER.GRC.JOBNAME.JCL"
			uploadCmd := exec.Command("zowe", "zos-files", "upload", "file-to-data-set", jclPath, qualifier)

			uploadCmd.Stdout = os.Stdout
			uploadCmd.Stderr = os.Stderr
			uploadCmd.Stdin = os.Stdin
			err = uploadCmd.Run()
			if err != nil {
				cobra.CheckErr(fmt.Errorf("Failed to upload %s to %s: %w", job.Name, qualifier, err))
			}

			fmt.Printf("✓ JCL for job %q uploaded at %s\n", job.Name, qualifier)

			fmt.Println("Submitting job ...")

			// Show spinner only if stdout is a terminal
			s := spinner.New(spinner.CharSets[43], 100*time.Millisecond)
			if term.IsTerminal(int(os.Stdout.Fd())) {
				s.Start()
			}

			// --- Run the job ---

			// zowe zos-jobs submit data-set "IBMUSER.GRC.JOBNAME.JCL" --vasc --rfj
			args := []string{"zos-jobs", "submit", "data-set", qualifier}

			switch {
			case spool:
				args = append(args, "--vasc")
			case jsn:
				args = append(args, "--rfj")
			default:
				args = append(args, "--rfj")
			}

			runCmd := exec.Command("zowe", args...)

			var stdoutBuf, stderrBuf bytes.Buffer
			runCmd.Stdout = &stdoutBuf
			runCmd.Stderr = &stderrBuf
			runCmd.Stdin = os.Stdin

			runErr := runCmd.Run()

			if spool {
				jobId, jobName := parseSpoolMeta(stdoutBuf.Bytes())

				// Save raw spool output
				spoolPath := filepath.Join(logDir, fmt.Sprintf("%s_%s.spool.log", jobId, strings.ToUpper(jobName)))
				err := os.WriteFile(spoolPath, stdoutBuf.Bytes(), 0644)
				if err != nil {
					fmt.Fprintf(os.Stderr, "⚠️ Failed to write spool log: %v\n", err)
				}

				// Log Grace metadata (no result field)
				logCtx := utils.LogContext{
					JobID:       jobId,
					JobName:     jobName,
					Step:        job.Step,
					RetryIndex:  0,
					GraceCmd:    "submit",
					ZoweProfile: graceCfg.Config.Profile,
					HLQ:         graceCfg.Datasets.Prefix,
					Timestamp:   time.Now().Format(time.RFC3339),
					Initiator: utils.Initiator{
						Type:   "user",
						Id:     os.Getenv("USER"),
						Tenant: "nara",
					},
				}

				if term.IsTerminal(int(os.Stdout.Fd())) {
					s.Stop()
					fmt.Println()
				}

				err = utils.SaveZoweLog(logDir, logCtx, "spooled")
				if err != nil {
					fmt.Fprintf(os.Stderr, "⚠️ Failed to write metadata log: %v\n", err)
				}

				summaryJobs = append(summaryJobs, utils.JobSummaryEntry{
					Name:       job.Name,
					ID:         jobId,
					Status:     "SPOOLED",
					Step:       job.Step,
					RetryIndex: 0,
					Spooled:    spool, // true if submitted with --spool
				})

				// Print spool output
				fmt.Print(stdoutBuf.String())
				continue
			}

			// --- Catch JSON response ---

			// Parse JSON response into result
			var result utils.ZoweRfj
			jsonErr := json.Unmarshal(stdoutBuf.Bytes(), &result)
			if jsonErr != nil {
				cobra.CheckErr(fmt.Errorf("Invalid Zowe response: %w", jsonErr))
			}

			// Create log JSON structure
			logCtx := utils.LogContext{
				JobID:       result.Data.JobID,
				JobName:     result.Data.JobName,
				Step:        job.Step,
				RetryIndex:  0,
				GraceCmd:    "submit",
				ZoweProfile: graceCfg.Config.Profile,
				HLQ:         graceCfg.Datasets.Prefix,
				Timestamp:   time.Now().Format(time.RFC3339),
				Initiator: utils.Initiator{
					Type:   "user",
					Id:     os.Getenv("USER"),
					Tenant: "nara",
				},
			}

			if term.IsTerminal(int(os.Stdout.Fd())) {
				s.Stop()
				fmt.Println()
			}

			// Write log JSON for this job
			logErr := utils.SaveZoweLog(logDir, logCtx, result)
			if logErr != nil {
				cobra.CheckErr(fmt.Errorf("Error writing to %s: %w", logDir, logErr))
			}

			summaryJobs = append(summaryJobs, utils.JobSummaryEntry{
				Name:       job.Name,
				ID:         result.Data.JobID,
				Status:     result.Data.Status,
				Step:       job.Step,
				RetryIndex: 0,
				Spooled:    spool, // true if submitted with --spool
			})

			if !result.Success {
				if result.Error != nil {
					cobra.CheckErr(fmt.Errorf(
						"✗ Zowe job submission failed (rc=%d, reason=%d): %s",
						extractRC(result.Error.CauseErrors),
						extractReason(result.Error.CauseErrors),
						result.Error.Msg,
					))
				}
				cobra.CheckErr("Zowe CLI reported failure, but no detailed error was returned")
			}

			if runErr != nil {
				fmt.Fprintf(os.Stderr, "⚠️ Zowe CLI exited non-zero but job appears to have been submitted: %v\n", runErr)
			}

			if jsn {
				// Show raw JSON
				fmt.Print(stdoutBuf.String())
			} else {
				fmt.Printf("✓ Job %s submitted as %s (status: %s)\n",
					result.Data.JobName, result.Data.JobID, result.Data.Status)
			}
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

		summaryPath := filepath.Join(logDir, "summary.json")
		file, err := os.Create(summaryPath)
		if err != nil {
			cobra.CheckErr(fmt.Errorf("Error writing %s: %w", summaryPath, err))
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", " ")
		err = encoder.Encode(summary)
		if err != nil {
			cobra.CheckErr(fmt.Errorf("Error writing %s: %w", summaryPath, err))
		}
	},
}

func extractRC(jsonStr string) int {
	var parsed map[string]any
	_ = json.Unmarshal([]byte(jsonStr), &parsed)
	if rc, ok := parsed["rc"].(float64); ok {
		return int(rc)
	}
	return -1
}

func extractReason(jsonStr string) int {
	var parsed map[string]any
	_ = json.Unmarshal([]byte(jsonStr), &parsed)
	if r, ok := parsed["reason"].(float64); ok {
		return int(r)
	}
	return -1
}

// Helper to parse job id and name from spool output bc no JSON
func parseSpoolMeta(out []byte) (jobId string, jobName string) {
	lines := strings.Split(string(out), "\n")

	for _, line := range lines {
		// Extract job ID like JOB03746
		if jobId == "" {
			if fields := strings.Fields(line); len(fields) > 0 {
				for _, field := range fields {
					if strings.HasPrefix(field, "JOB") && len(field) >= 7 {
						jobId = field
						break
					}
				}
			}
		}

		// Extract job name from lines like:
		// 03.41.02 JOB03699  IEF453I HELLO - JOB FAILED - JCL ERROR
		if jobName == "" && strings.Contains(line, "IEF453I ") {
			parts := strings.Split(line, "IEF453I ")
			if len(parts) > 1 {
				right := strings.TrimSpace(parts[1])
				tokens := strings.Split(right, " ")
				if len(tokens) > 0 {
					jobName = tokens[0]
				}
			}
		}

		// Fallback: find NAME-{JOBNAME} pattern
		if jobName == "" && strings.Contains(line, "NAME-") {
			idx := strings.Index(line, "NAME-")
			if idx != -1 {
				after := strings.TrimSpace(line[idx+5:])
				fields := strings.Fields(after)
				if len(fields) > 0 {
					jobName = fields[0]
				}
			}
		}

		if jobId != "" && jobName != "" {
			break
		}
	}

	return
}
