package cmd

import (
	"fmt"

	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/orchestrator"
	"github.com/rs/zerolog/log"
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
		// --- Get HandlerRegistry from dependencies ---

		registry := GetDependencies().HandlerRegistry

		// --- Load and validate grace.yml ---

		// LoadGraceConfig performs initial validation
		graceCfg, configDir, err := config.LoadGraceConfig("grace.yml")
		if err != nil {
			cobra.CheckErr(fmt.Errorf("failed to load grace configuration: %w", err))
		}

		// Perform full validation using the registry. This
		// this validates job types at the handler level
		if err := config.ValidateGraceConfig(graceCfg, registry); err != nil {
			cobra.CheckErr(fmt.Errorf("grace configuration validation failed: %w", err))
		}

		ctx := &context.ExecutionContext{
			Config:     graceCfg,
			ConfigDir:  configDir,
			SubmitOnly: deckJobs,
			GraceCmd:   "deck",
		}

		// Instantiate orchestrator
		orch := orchestrator.NewZoweOrchestrator()

		log.Info().Msg("Starting deck and upload process...")

		err = orch.DeckAndUpload(ctx, registry, noCompile, noUpload)
		cobra.CheckErr(err)
	},
}
