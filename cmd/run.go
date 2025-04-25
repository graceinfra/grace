package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/graceinfra/grace/utils"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

var (
	wantJSON   bool
	wantSpool  bool
	submitOnly []string
)

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().BoolVar(&wantJSON, "json", false, "Return structured JSON data about each job")
	runCmd.Flags().BoolVar(&wantSpool, "spool", false, "Return full spool content for each job")
	runCmd.Flags().StringSliceVar(&submitOnly, "only", nil, "Submit only specified job(s)")
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a Grace workflow end-to-end, waiting for each job to complete",
	Long:  `Run reads grace.yml and orchestrates jobs in order, submitting one at a time and waiting for results.`,
	Run: func(cmd *cobra.Command, args []string) {
		if wantJSON && wantSpool {
			cobra.CheckErr("--json and --spool cannot be used together")
		}

		quiet := wantJSON || wantSpool
		wantVerbose := Verbose
		useSpinner := term.IsTerminal(int(os.Stdout.Fd())) && !quiet

		ymlData, err := os.ReadFile("grace.yml")
		cobra.CheckErr(err)

		var graceCfg utils.GraceConfig
		err = yaml.Unmarshal(ymlData, &graceCfg)
		cobra.CheckErr(err)

		logDir, _, err := utils.CreateLogDir("run")
		cobra.CheckErr(err)

		jobExecutions := utils.RunWorkflow(graceCfg, logDir, wantSpool, wantJSON, wantVerbose, quiet, useSpinner, submitOnly)

		host, _ := os.Hostname()

		summary := utils.ExecutionSummary{
			Timestamp:   time.Now().Format(time.RFC3339),
			GraceCmd:    "run",
			ZoweProfile: graceCfg.Config.Profile,
			HLQ:         strings.Split(graceCfg.Datasets.JCL, ".")[0],
			Initiator: utils.Initiator{
				Type:   "user",
				Id:     os.Getenv("USER"),
				Tenant: host,
			},
			Jobs: jobExecutions,
		}

		formatted, err := json.MarshalIndent(summary, "", "  ")
		cobra.CheckErr(err)

		f, err := os.Create(filepath.Join(logDir, "summary.json"))
		cobra.CheckErr(err)
		defer f.Close()

		_, err = f.Write(formatted)
		cobra.CheckErr(err)
	},
}
