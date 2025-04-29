package config

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/graceinfra/grace/internal/utils"
	"github.com/graceinfra/grace/types"
	"gopkg.in/yaml.v3"
)

var allowedSteps = map[string]bool{
	"execute": true,
	// TODO: Add other steps like "compile", "link" here as they become supported
}

func LoadGraceConfig(filename string) (*types.GraceConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filename, err)
	}

	var graceCfg types.GraceConfig
	err = yaml.Unmarshal(data, &graceCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML in %s: %w", filename, err)
	}

	// Validate the loaded configuration
	if err := ValidateGraceConfig(&graceCfg); err != nil {
		return nil, fmt.Errorf("validation error in %s: %w", filename, err)
	}

	return &graceCfg, nil
}

// Helper to get sorted keys from the allowedSteps map for error messages
func getAllowedStepKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys) // For consistent error messages
	return keys
}

func ValidateGraceConfig(cfg *types.GraceConfig) error {
	var errs []string // Slice to collect error messages

	// --- Validate top-level 'config' section ---
	if cfg.Config.Profile == "" {
		errs = append(errs, "field 'config.profile' is required")
	}
	// Potential future validation: check if profile exists in Zowe config

	// --- Validate 'datasets' section ---
	if cfg.Datasets.JCL == "" {
		errs = append(errs, "field 'datasets.jcl' is required")
	} else {
		if err := utils.ValidateDataSetQualifiers(cfg.Datasets.JCL); err != nil {
			errs = append(errs, fmt.Sprintf("datasets.jcl: %v", err))
		}
	}

	if cfg.Datasets.SRC == "" {
		errs = append(errs, "field 'datasets.src' is required")
	} else {
		if err := utils.ValidateDataSetQualifiers(cfg.Datasets.SRC); err != nil {
			errs = append(errs, fmt.Sprintf("datasets.src: %v", err))
		}
	}

	if cfg.Datasets.LoadLib == "" {
		errs = append(errs, "field 'datasets.loadlib' is required")
	} else {
		if err := utils.ValidateDataSetQualifiers(cfg.Datasets.LoadLib); err != nil {
			errs = append(errs, fmt.Sprintf("datasets.loadlib: %v", err))
		}
	}

	// --- Validate 'jobs' section ---
	if len(cfg.Jobs) == 0 {
		errs = append(errs, "at least one job must be defined under the 'jobs' list")
	}

	jobNames := make(map[string]bool)

	for i, job := range cfg.Jobs {
		jobCtx := fmt.Sprintf("job[%d]", i)
		if job.Name != "" {
			jobCtx = fmt.Sprintf("job[%d] (name: %q)", i, job.Name)
		}

		// Validate job.Name
		if job.Name == "" {
			errs = append(errs, fmt.Sprintf("job[%d]: field 'name' is required", i))
		} else {
			if err := utils.ValidatePDSMemberName(job.Name); err != nil {
				errs = append(errs, fmt.Sprintf("%s: invalid name for use as PDS member: %v", jobCtx, err))
			}

			upperName := strings.ToUpper(job.Name)

			// Check for duplicate job names (case-insensitive)
			if jobNames[upperName] {
				errs = append(errs, fmt.Sprintf("%s: duplicate job name found", jobCtx))
			}
			jobNames[upperName] = true
		}

		// Validate job.Step
		if job.Step == "" {
			errs = append(errs, fmt.Sprintf("%s: field 'step' is required", jobCtx))
		} else if !allowedSteps[job.Step] { // Check against allowed steps
			allowed := getAllowedStepKeys(allowedSteps)
			errs = append(errs, fmt.Sprintf("%s: invalid step %q; allowed steps are: %v", jobCtx, job.Step, allowed))
		}

		// Validate job.Source (required for 'execute' step)
		// Add conditions here if other steps don't require 'source'
		if job.Step == "execute" && job.Source == "" {
			errs = append(errs, fmt.Sprintf("%s: field 'source' is required for step 'execute'", jobCtx))
		}
		// Potential future validation: check if source file exists relative to grace.yml

		// Validate job.Template (optional)
		// if job.Template != "" {
		//    // Potential future validation: check if template file exists
		// }
	}

	if len(errs) > 0 {
		// Join errors with a newline for better readability in output
		return errors.New("Grace configuration validation failed:\n- " + strings.Join(errs, "\n- "))
	}

	return nil // No errors found
}
