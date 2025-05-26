package paths

import (
	"fmt"
	"hash/fnv"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/resolver"
	"github.com/graceinfra/grace/internal/utils"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog/log"
)

// PreresolveOutputAndLocalTempDataSources primarily populates ctx.ResolvedPaths for:
// 1. Outputs that will be temporary z/OS DSNs (zos-temp://, or file://, local-temp:// for ZOS jobs).
// 2. Outputs that are local identifiers (file://, local-temp:// for SHELL jobs).
// It does not generate temporary ZOS DSNs for INPUTS; ResolvePath will do that dynamically.
func PreresolveOutputAndLocalTempDataSources(cfg *types.GraceConfig) (map[string]string, error) {
	resolvedPaths := make(map[string]string)
	hlq := ""
	if cfg.Datasets.JCL != "" {
		parts := strings.Split(cfg.Datasets.JCL, ".")
		if len(parts) > 0 {
			hlq = parts[0]
		}
	}
	if hlq == "" {
		return nil, fmt.Errorf("cannot determine HLQ from datasets.jcl for DSN generation")
	}

	generatedDSNs := make(map[string]string)

	for _, job := range cfg.Jobs {
		isZosJobType := job.Type == "compile" || job.Type == "linkedit" || job.Type == "execute"

		// Process ONLY outputs here
		for _, outputSpec := range job.Outputs {
			log.Debug().
				Str("job_name_preresolve", job.Name).
				Str("output_dd_preresolve", outputSpec.Name).
				Str("output_path_preresolve", outputSpec.Path).
				Bool("is_zos_job_type_preresolve", isZosJobType).
				Msg("Preresolve: Processing outputSpec")

			// Determine if this output needs a temporary z/OS DSN
			needsTempZosDsnForOutput := strings.HasPrefix(outputSpec.Path, "zos-temp://") ||
				(isZosJobType && (strings.HasPrefix(outputSpec.Path, "file://") || strings.HasPrefix(outputSpec.Path, "local-temp://")))

			if needsTempZosDsnForOutput {
				if _, exists := resolvedPaths[outputSpec.Path]; exists {
					log.Debug().Str("virtual_path", outputSpec.Path).Msg("Preresolve Output: Path already has a DSN (e.g. from another job's identical output spec). Skipping DSN re-generation.")
					continue
				}
				dsn, err := generateZosTempDSN_Idempotent(hlq, outputSpec)
				if err != nil {
					return nil, fmt.Errorf("failed to generate temporary DSN for job %q output %q (%s): %w", job.Name, outputSpec.Name, outputSpec.Path, err)
				}
				if existingVP, collision := generatedDSNs[dsn]; collision {
					return nil, fmt.Errorf("DSN hash collision detected: %q generated for both %q and %q", dsn, existingVP, outputSpec.Path)
				}
				resolvedPaths[outputSpec.Path] = dsn
				generatedDSNs[dsn] = outputSpec.Path
				log.Debug().Str("virtual_path", outputSpec.Path).Str("resolved_temp_dsn", dsn).Msg("Preresolved output path to temporary z/OS DSN")
			} else if strings.HasPrefix(outputSpec.Path, "file://") || strings.HasPrefix(outputSpec.Path, "local-temp://") {
				// This is for file:// or local-temp:// outputs of NON-ZOS (shell) jobs
				if _, exists := resolvedPaths[outputSpec.Path]; !exists {
					resource := ""
					if strings.HasPrefix(outputSpec.Path, "file://") {
						resource = strings.TrimPrefix(outputSpec.Path, "file://")
					} else { // local-temp://
						resource = strings.TrimPrefix(outputSpec.Path, "local-temp://")
					}
					if resource == "" {
						return nil, fmt.Errorf("output path for job %q (%s) scheme %s cannot have an empty resource part", job.Name, outputSpec.Path, outputSpec.Path[:strings.Index(outputSpec.Path, "://")])
					}
					resolvedPaths[outputSpec.Path] = resource // Store the local identifier
					log.Debug().Str("virtual_path", outputSpec.Path).Str("local_identifier", resource).Msg("Preresolved output path (local identifier for shell job)")
				} else {
					log.Debug().Str("virtual_path", outputSpec.Path).Msg("Preresolve Output: Path already has a local identifier. Skipping.")
				}
			}
			// Other output path types (e.g., zos://) are not pre-resolved here.
		}
	}
	return resolvedPaths, nil
}

// generateZosTempDSN_Idempotent creates a DSN without runtime IDs.
// Convention: <HLQ>.GRC.H<PathHash>.<DDNAME>
func generateZosTempDSN_Idempotent(hlq string, spec types.FileSpec) (string, error) {
	// Hash the virtual path
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(spec.Path))
	pathHash := strconv.FormatUint(uint64(hasher.Sum32()), 36)
	if len(pathHash) > 6 {
		pathHash = pathHash[:6]
	}
	pathHash = strings.ToUpper(pathHash)

	// Get DDName, ensure it's valid as a DSN qualifier part
	ddName := strings.ToUpper(spec.Name)
	if len(ddName) > 8 {
		ddName = ddName[:8]
	}

	// Construct DSN parts (No WFID qualifier)
	fixedQualifier := "GRC"
	hashQualifier := "H" + pathHash

	dsnParts := []string{hlq, fixedQualifier, hashQualifier, ddName}
	dsn := strings.Join(dsnParts, ".")

	// Validate
	if err := utils.ValidateDataSetQualifiers(dsn); err != nil {
		log.Error().Err(err).Str("generated_dsn", dsn).Str("virtual_path", spec.Path).Msg("Generated idempotent DSN failed validation")
		return "", fmt.Errorf("generated idempotent DSN %q failed validation: %w", dsn, err)
	}
	return dsn, nil
}

// ResolvePath retrieves the physical DSN/path for a given virtual path based on its scheme.
// For z/OS job types, file:// and local-temp:// will resolve to their temporary z/OS DSN.
// For shell job types, file:// and local-temp:// will resolve to local paths.
// Assumes temporary paths requiring z/OS DSNs were pre-resolved and stored in ctx.resolvedPaths.
func ResolvePath(ctx *context.ExecutionContext, job *types.Job, virtualPath string, fileSpecForPath *types.FileSpec) (string, error) {
	schemeEnd := strings.Index(virtualPath, "://")
	if schemeEnd == -1 {
		return "", fmt.Errorf("invalid virtual path format: missing '://' scheme separator in %q", virtualPath)
	}
	scheme := virtualPath[:schemeEnd]
	resource := virtualPath[schemeEnd+3:] // Path part after scheme://

	isZosJobType := job.Type == "compile" || job.Type == "linkedit" || job.Type == "execute"

	log.Debug().Str("job_name", job.Name).Str("job_type", job.Type).Str("virtual_path", virtualPath).Bool("is_zos_job", isZosJobType).
		Bool("is_resolving_for_filespec", fileSpecForPath != nil).Msg("Resolving path")

	// If fileSpecForPath is provided, it means we are resolving an input for a ZOS job
	// that needs a dynamic temporary DSN.
	if fileSpecForPath != nil && isZosJobType && (scheme == "file" || scheme == "local-temp") {
		hlq := ""                          // Determine HLQ (e.g., from ctx.Config.Datasets.JCL)
		if ctx.Config.Datasets.JCL != "" { // Prefer job-specific JCL dataset HLQ if overridden
			jobJCLdsn := resolver.ResolveJCLDataset(job, ctx.Config)
			parts := strings.Split(jobJCLdsn, ".")
			if len(parts) > 0 {
				hlq = parts[0]
			}
		}
		if hlq == "" && ctx.Config.Datasets.JCL != "" { // Fallback to global if job-specific didn't yield HLQ
			parts := strings.Split(ctx.Config.Datasets.JCL, ".")
			if len(parts) > 0 {
				hlq = parts[0]
			}
		}
		if hlq == "" {
			return "", fmt.Errorf("cannot determine HLQ for generating temp DSN for input %s", virtualPath)
		}
		dsn, err := generateZosTempDSN_Idempotent(hlq, *fileSpecForPath)
		if err != nil {
			return "", fmt.Errorf("failed to dynamically generate temp DSN for input %s (DD: %s): %w", virtualPath, fileSpecForPath.Name, err)
		}
		log.Debug().Str("virtual_path", virtualPath).Str("dd_name", fileSpecForPath.Name).Str("dynamic_temp_dsn", dsn).Msgf("Dynamically resolved %s input for z/OS job to temporary DSN", scheme)
		return dsn, nil
	}

	switch scheme {
	case "zos-temp":
		ctx.PathMutex.RLock()
		dsn, exists := ctx.ResolvedPaths[virtualPath]
		ctx.PathMutex.RUnlock()
		if !exists {
			return "", fmt.Errorf("zos-temp:// path %q not found in resolved paths map (was it an output of a previous job?)", virtualPath)
		}
		log.Debug().Str("virtual_path", virtualPath).Str("scheme", scheme).Str("resolved_dsn_from_map", dsn).Msg("Resolved zos-temp:// path from map")
		return dsn, nil

	case "file", "local-temp":
		if isZosJobType {
			// This is for an OUTPUT of a ZOS job (file:// or local-temp://)
			// Inputs for ZOS jobs using these schemes are handled by the dynamic DSN generation block above.
			ctx.PathMutex.RLock()
			dsnForZosOutput, exists := ctx.ResolvedPaths[virtualPath]
			ctx.PathMutex.RUnlock()
			if !exists {
				return "", fmt.Errorf("%s output path %q for z/OS job %q not found in resolved DSNs map (should have been pre-resolved)", scheme, virtualPath, job.Name)
			}
			log.Debug().Str("virtual_path", virtualPath).Str("resolved_dsn_for_zos_output", dsnForZosOutput).Msgf("Resolved %s output for z/OS job from map (is a DSN)", scheme)
			return dsnForZosOutput, nil
		} else {
			// This is for a NON-ZOS job (SHELL job), for either an input or an output.
			// The physical path is always based on the 'resource' part of the virtualPath.
			if scheme == "local-temp" {
				if ctx.LocalStageDir == "" {
					return "", fmt.Errorf("LocalStageDir not set in context for job %s, cannot resolve %s", job.Name, virtualPath)
				}
				finalLocalPath := filepath.Join(ctx.LocalStageDir, resource) // resource is like "initial-data.txt"
				log.Debug().Str("virtual_path", virtualPath).Str("local_path", finalLocalPath).Msg("Resolved local-temp:// path for shell job (using resource part)")
				return finalLocalPath, nil
			}

			// Scheme is "file" for shell job
			if filepath.IsAbs(resource) {
				log.Debug().Str("virtual_path", virtualPath).Str("abs_path", resource).Msg("Resolved file:// path for shell job (absolute, using resource part)")
				return resource, nil
			}
			finalLocalPath := filepath.Join(ctx.ConfigDir, resource)
			log.Debug().Str("virtual_path", virtualPath).Str("rel_path", finalLocalPath).Msg("Resolved file:// path for shell job (relative to config dir, using resource part)")
			return finalLocalPath, nil
		}

	case "src":
		srcPDS := resolver.ResolveSRCDataset(job, ctx.Config)
		if srcPDS == "" {
			return "", fmt.Errorf("cannot resolve src:// path %q: datasets.src is not defined", virtualPath)
		}
		baseName := filepath.Base(resource)
		memberName := strings.ToUpper(strings.TrimSuffix(baseName, filepath.Ext(baseName)))
		if err := utils.ValidatePDSMemberName(memberName); err != nil {
			return "", fmt.Errorf("invalid member name %q from src path %q: %w", memberName, virtualPath, err)
		}
		resolvedDSN := fmt.Sprintf("%s(%s)", srcPDS, memberName)
		if err := utils.ValidateDataSetQualifiers(srcPDS); err != nil {
			return "", fmt.Errorf("invalid datasets.src %q for src:// path %q: %w", srcPDS, virtualPath, err)
		}
		log.Debug().Str("virtual_path", virtualPath).Str("scheme", scheme).Str("resolved_dsn", resolvedDSN).Msg("Resolved src:// path")
		return resolvedDSN, nil

	case "zos":
		dsn := strings.ToUpper(resource)
		if strings.Contains(dsn, "(") {
			if !strings.HasSuffix(dsn, ")") {
				return "", fmt.Errorf("invalid zos:// path %q: unbalanced parenthesis", virtualPath)
			}
			parts := strings.SplitN(dsn, "(", 2)
			baseDsn := parts[0]
			member := strings.TrimSuffix(parts[1], ")")
			if err := utils.ValidateDataSetQualifiers(baseDsn); err != nil {
				return "", fmt.Errorf("invalid base DSN in zos:// path %q: %w", virtualPath, err)
			}
			if err := utils.ValidatePDSMemberName(member); err != nil {
				return "", fmt.Errorf("invalid member name in zos:// path %q: %w", virtualPath, err)
			}
		} else {
			if err := utils.ValidateDataSetQualifiers(dsn); err != nil {
				return "", fmt.Errorf("invalid DSN in zos:// path %q: %w", virtualPath, err)
			}
		}
		log.Debug().Str("virtual_path", virtualPath).Str("scheme", scheme).Str("resolved_dsn", dsn).Msg("Resolved zos:// path")
		return dsn, nil

	default:
		return "", fmt.Errorf("unsupported scheme %q in path %q", scheme, virtualPath)
	}
}
