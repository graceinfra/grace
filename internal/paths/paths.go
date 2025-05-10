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

// PreresolveOutputPaths generates DSNs for declared output virtual paths.
// Convention for deck/run: <HLQ>.GRC.H<PathHash>.<DDNAME>
// Assumes cfg has been validated.
func PreresolveOutputPaths(cfg *types.GraceConfig) (map[string]string, error) {
	resolvedPaths := make(map[string]string)
	hlq := ""
	if cfg.Datasets.JCL != "" { // Use JCL dataset's HLQ as the base
		parts := strings.Split(cfg.Datasets.JCL, ".") // TODO: Use resolver.GetHLQ or separate HLQ field in grace.yml if JCL can be overriden for hlq assignments THROUGHOUT the codebase
		if len(parts) > 0 {
			hlq = parts[0]
		}
	}
	if hlq == "" {
		return nil, fmt.Errorf("cannot determine HLQ from datasets.jcl")
	}

	generatedDSNs := make(map[string]string)

	for _, job := range cfg.Jobs {
		for _, outputSpec := range job.Outputs {
			if !(strings.HasPrefix(outputSpec.Path, "zos-temp://") || strings.HasPrefix(outputSpec.Path, "local-temp://") || strings.HasPrefix(outputSpec.Path, "file://")) {
				continue
			}
			if _, exists := resolvedPaths[outputSpec.Path]; exists {
				continue
			}

			if strings.HasPrefix(outputSpec.Path, "zos-temp://") {
				dsn, err := generateZosTempDSN_Idempotent(hlq, outputSpec)
				if err != nil {
					return nil, fmt.Errorf("failed to generate DSN for job %q output %q (%s): %w", job.Name, outputSpec.Name, outputSpec.Path, err)
				}
				if existingVP, collision := generatedDSNs[dsn]; collision {
					// Still check for hash collisions, though less likely
					return nil, fmt.Errorf("DSN hash collision detected: %q generated for both %q and %q", dsn, existingVP, outputSpec.Path)
				}
				resolvedPaths[outputSpec.Path] = dsn
				generatedDSNs[dsn] = outputSpec.Path
				log.Debug().Str("virtual_path", outputSpec.Path).Str("resolved_dsn", dsn).Msg("Preresolved output path (idempotent)")
			} else if strings.HasPrefix(outputSpec.Path, "local-temp://") {
				resource := strings.TrimPrefix(outputSpec.Path, "local-temp://")
				if resource == "" {
					return nil, fmt.Errorf("local-temp:// path for job %q output %q (%s) cannot be empty", job.Name, outputSpec.Name, outputSpec.Path)
				}

				resolvedPaths[outputSpec.Path] = resource
				log.Debug().Str("virtual_path", outputSpec.Path).Str("local_indentifier", resource).Msg("Preresolved local-temp:// output path (identifier)")
			} else if strings.HasPrefix(outputSpec.Path, "file://") {
				resource := strings.TrimPrefix(outputSpec.Path, "file://")
				if resource == "" {
					return nil, fmt.Errorf("file:// path of job %q output %q (%s) cannot be empty", job.Name, outputSpec.Name, outputSpec.Path)
				}

				resolvedPaths[outputSpec.Path] = resource
				log.Debug().Str("virtual_path", outputSpec.Path).Str("file_path_identifier", resource).Msg("Preresolved file:// output path (identifier)")
			}
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

	// Get DDName
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
// Assumes temp:// paths were pre-resolved and stored via InitializePaths.
// Requires ExecutionContext to access configured datasets (like datasets.src).
func ResolvePath(ctx *context.ExecutionContext, job *types.Job, virtualPath string) (string, error) {
	schemeEnd := strings.Index(virtualPath, "://")
	if schemeEnd == -1 {
		return "", fmt.Errorf("invalid virtual path format: missing '://' scheme separator in %q", virtualPath)
	}
	scheme := virtualPath[:schemeEnd]
	resource := virtualPath[schemeEnd+3:] // Path part after scheme://

	log.Debug().Str("virtual_path", virtualPath).Str("scheme", scheme).Str("resource", resource).Msg("Resolving path")

	switch scheme {
	case "zos-temp":
		// Retrieve pre-resolved DSN from the context's map
		ctx.PathMutex.RLock()
		dsn, exists := ctx.ResolvedPaths[virtualPath]
		ctx.PathMutex.RUnlock()

		if !exists {
			log.Error().Str("virtual_path", virtualPath).Msg("Attempted to resolve an unknown/unresolved zos-temp:// path")
			return "", fmt.Errorf("temporary path %q not found in resolved paths map (was it produced by an earlier job?)", virtualPath)
		}

		log.Debug().Str("virtual_path", virtualPath).Str("scheme", scheme).Str("resolved_dsn", dsn).Msg("Resolved temp:// path")
		return dsn, nil

	case "local-temp":
		localIdentifier, exists := ctx.ResolvedPaths[virtualPath]
		if !exists || ctx.LocalStageDir == "" {
			return "", fmt.Errorf("local-temp path %q not resolved or LocalStageDir not set", virtualPath)
		}

		resolvedLocalPath := filepath.Join(ctx.LocalStageDir, localIdentifier)

		log.Debug().Str("virtual_path", virtualPath).Str("scheme", scheme).Str("resolved_local_path", resolvedLocalPath).Msg("Resolved local-temp:// path")
		return resolvedLocalPath, nil

	case "src":
		// Resolve relative to the configured 'datasets.src' PDS
		srcPDS := resolver.ResolveSRCDataset(job, ctx.Config)
		if srcPDS == "" {
			return "", fmt.Errorf("cannot resolve src:// path %q: datasets.src is not defined in grace.yml", virtualPath)
		}

		// Resource is the member name (potentially with extension)
		// Convert filename to member name (uppercase, remove extension)
		baseName := filepath.Base(resource)
		memberName := strings.ToUpper(strings.TrimSuffix(baseName, filepath.Ext(baseName)))

		// Validate the derived member name
		if err := utils.ValidatePDSMemberName(memberName); err != nil {
			log.Error().Err(err).Str("virtual_path", virtualPath).Str("derived_member", memberName).Msg("Invalid PDS member name derived from src:// path")
			return "", fmt.Errorf("invalid member name %q derived from src path %q: %w", memberName, virtualPath, err)
		}

		resolvedDSN := fmt.Sprintf("%s(%s)", srcPDS, memberName)

		// Validate the final DSN format too
		if err := utils.ValidateDataSetQualifiers(srcPDS); err != nil { // Validate base PDS name
			return "", fmt.Errorf("invalid datasets.src %q used for src:// path %q: %w", srcPDS, virtualPath, err)
		}

		log.Debug().Str("virtual_path", virtualPath).Str("scheme", scheme).Str("resolved_dsn", resolvedDSN).Msg("Resolved src:// path")
		return resolvedDSN, nil

	case "zos":
		// Resolve to an existing absolute DSN (potentially with member)
		// Resource path is the DSN(+member)
		dsn := strings.ToUpper(resource)

		if strings.Contains(dsn, "(") {
			if !strings.HasSuffix(dsn, ")") {
				return "", fmt.Errorf("invalid zos:// path %q: unbalanced parenthesis for member name", virtualPath)
			}

			parts := strings.SplitN(dsn, "(", 2)
			baseDsn := parts[0]
			member := strings.TrimSuffix(parts[1], ")")

			if err := utils.ValidateDataSetQualifiers(baseDsn); err != nil {
				return "", fmt.Errorf("invalid base dataset name in zos:// path %q: %w", virtualPath, err)
			}

			if err := utils.ValidatePDSMemberName(member); err != nil {
				return "", fmt.Errorf("invalid member name in zos:// path %q: %w", virtualPath, err)
			}
		} else {
			// Assume sequential dataset
			if err := utils.ValidateDataSetQualifiers(dsn); err != nil {
				return "", fmt.Errorf("invalid dataset name in zos:// path %q: %w", virtualPath, err)
			}
		}

		log.Debug().Str("virtual_path", virtualPath).Str("scheme", scheme).Str("resolved_dsn", dsn).Msg("Resolved zos:// path")
		return dsn, nil

	case "file":
		// The resource is the path relative to ConfigDir or an absolute path if specified.
		// Here, we return an absolute path for clarity downstream.
		if filepath.IsAbs(resource) {
			log.Debug().Str("virtual_path", virtualPath).Str("scheme", scheme).Str("resolved_abs_path", resource).Msg("Resolved file:// path (absolute)")
			return resource, nil
		}

		resolvedFilePath := filepath.Join(ctx.ConfigDir, resource)
		log.Debug().Str("virtual_path", virtualPath).Str("scheme", scheme).Str("resovled_file_path", resolvedFilePath).Msg("Resolved file:// path (relative to grace.yml directory)")
		return resolvedFilePath, nil

	default:
		log.Error().Str("virtual_path", virtualPath).Str("scheme", scheme).Msg("Unsupported virtual path scheme")
		return "", fmt.Errorf("unsupported virtual path scheme %q in path %q", scheme, virtualPath)
	}
}
