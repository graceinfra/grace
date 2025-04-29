package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/runner"
	"github.com/graceinfra/grace/types"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	wantJSON   bool
	submitOnly []string
)

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().BoolVar(&wantJSON, "json", false, "Return structured JSON data about each job")
	runCmd.Flags().StringSliceVar(&submitOnly, "only", nil, "Submit only specified job(s)")
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a Grace workflow end-to-end, waiting for each job to complete",
	Long:  `Run reads grace.yml and orchestrates jobs in order, submitting one at a time and waiting for results.`,
	Run: func(cmd *cobra.Command, args []string) {
		// --- Decide output style ---
		// Are we user facing? Part of a pipeline? This will let the logger
		// know whether to show animations, verbose text, etc.)

		var outputStyle types.OutputStyle
		switch {
		case wantJSON:
			outputStyle = types.StyleMachineJSON
		case Verbose:
			outputStyle = types.StyleHumanVerbose
		default:
			outputStyle = types.StyleHuman
		}

		// --- Read grace.yml and construct GraceConfig ---

		ymlData, err := os.ReadFile("grace.yml")
		cobra.CheckErr(err)

		var graceCfg types.GraceConfig
		err = yaml.Unmarshal(ymlData, &graceCfg)
		cobra.CheckErr(err)

		// --- Create log directory ---

		logDir, _, err := log.CreateLogDir("run")
		cobra.CheckErr(err)

		// --- Run workflow ---

		logger := log.NewLogger(outputStyle)

		jobExecutions := runner.RunWorkflow(&context.ExecutionContext{
			Config:      &graceCfg,
			Logger:      logger,
			LogDir:      logDir,
			OutputStyle: outputStyle,
			SubmitOnly:  submitOnly,
			GraceCmd:    "run",
		})

		// --- Construct workflow summary ---

		host, _ := os.Hostname()

		summary := models.ExecutionSummary{
			Timestamp:   time.Now().Format(time.RFC3339),
			GraceCmd:    "run",
			ZoweProfile: graceCfg.Config.Profile,
			HLQ:         strings.Split(graceCfg.Datasets.JCL, ".")[0],
			Initiator: types.Initiator{
				Type:   "user",
				Id:     os.Getenv("USER"),
				Tenant: host,
			},
			Jobs: jobExecutions,
		}

		// --- Write workflow summary to summary.json ---

		formatted, err := json.MarshalIndent(summary, "", "  ")
		cobra.CheckErr(err)

		f, err := os.Create(filepath.Join(logDir, "summary.json"))
		cobra.CheckErr(err)
		defer f.Close()

		_, err = f.Write(formatted)
		cobra.CheckErr(err)
	},
}
