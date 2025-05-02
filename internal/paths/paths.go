package paths

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/utils"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog/log"
)

// PreresolveOutputPaths generates unique DSNs for all declared output virtual paths.
// It uses a naming convention incorporating HLQ, workflow ID, and path hash/name.
// Assumes cfg has been validated (grace.yml).
func PreresolveOutputPaths(workflowId uuid.UUID, cfg *types.GraceConfig) (map[string]string, error) {
	resolvedPaths := make(map[string]string)
	hlq := ""
	if cfg.Datasets.JCL != "" {
		parts := strings.Split(cfg.Datasets.JCL, ".")
		if len(parts) > 0 {
			hlq = parts[0]
		}
	}
	if hlq == "" {
		// This should be caught by validation, sanity check
		return nil, fmt.Errorf("cannot determine HLQ from datasets > jcl in grace.yml")
	}

	wfShortId := strings.ToUpper(workflowId.String()[:8])

	generatedDSNs := make(map[string]string) // dsn -> virtual path

	for _, job := range cfg.Jobs {
		for _, outputSpec := range job.Outputs {
			if !strings.HasPrefix(outputSpec.Path, "temp://") {
				log.Warn().Str("path", outputSpec.Path).Msg("Skipping resolution for non temp:// output path")
				continue
			}

			if _, exists := resolvedPaths[outputSpec.Path]; exists {
				continue
			}

			// Generate DSN based on convention
			dsn, err := generateTempDSN(hlq, wfShortId, outputSpec)
			if err != nil {
				return nil, fmt.Errorf("failed to generate DSN for job %q output %q (%s): %w", job.Name, outputSpec.Name, outputSpec.Path, err)
			}

			// Check for collision in generated DSNs
			if existingVP, collision := generatedDSNs[dsn]; collision {
				// Unlikely but possible
				// I don't want to think abt this right now
				return nil, fmt.Errorf("DSN collision detected: %q generated for both %q and %q", dsn, existingVP, outputSpec.Path)
			}

			// Store the mapping
			resolvedPaths[outputSpec.Path] = dsn
			generatedDSNs[dsn] = outputSpec.Path
			log.Debug().Str("virtual_path", outputSpec.Path).Str("resolved_dsn", dsn).Msg("Preresolved output path")
		}
	}

	return resolvedPaths, nil
}

// generateTempDSN creates a unique DSN for a temporary virtual path.
// <HLQ>.GRC.W<WFID_short>.H<PathHash>.<DDNAME>
// Needs careful handling of length limits and valid characters.
func generateTempDSN(hlq, wfShortId string, spec types.FileSpec) (string, error) {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(spec.Path))
	pathHash := strconv.FormatUint(uint64(hasher.Sum32()), 36)

	ddName := strings.ToUpper(spec.Name)
	if len(ddName) > 8 {
		ddName = ddName[:8]
	}

	fixedQualifier := "GRC"
	wfQualifier := "W" + wfShortId
	hashQualifier := "H" + pathHash
	// DDName itself is the final qualifier

	dsnParts := []string{hlq, fixedQualifier, wfQualifier, hashQualifier, ddName}
	dsn := strings.Join(dsnParts, ".")

	if err := utils.ValidateDataSetQualifiers(dsn); err != nil {
		log.Error().Err(err).Str("generated_dsn", dsn).Str("virtual_path", spec.Path).Msg("Generated DSN failed validation")

		// Potentially try alternative shortening/hashing if validation fails?
		// For now, return error. User might need shorter HLQ or workflow might generate too many parts.
		return "", fmt.Errorf("generated DSN %q failed validation: %w", dsn, err)
	}

	return dsn, nil
}
