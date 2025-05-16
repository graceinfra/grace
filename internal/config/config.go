package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/graceinfra/grace/internal/jobhandler"
	"github.com/graceinfra/grace/internal/utils"
	"github.com/graceinfra/grace/types"
	"gopkg.in/yaml.v3"
)

var allowedTypes = map[string]bool{
	"execute":  true,
	"compile":  true,
	"linkedit": true,
	"shell":    true,
}

// --- Define validation rules for DD names and virtual paths ---

// Basic DDName validation (1-8 chars, starts non-numeric, common chars)
var ddNameRegex = regexp.MustCompile(`^[A-Z#$@][A-Z0-9#$@]{0,7}$`)

// Basic virtual path check (e.g., scheme://resource)
var virtualPathRegex = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-]*://.+`)

func LoadGraceConfig(configPath string) (*types.GraceConfig, string, error) {
	absPath, err := filepath.Abs(configPath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get absolute path for config %s: %w", configPath, err)
	}

	configDir := filepath.Dir(absPath)

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read config file %s: %w", absPath, err)
	}

	var graceCfg types.GraceConfig
	err = yaml.Unmarshal(data, &graceCfg)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse YAML in %s: %w", absPath, err)
	}

	// Perform initial validation (syntax, structure), passing nil for registry.
	// Full handler-based validation will occur later when the handler registry is available
	if err := ValidateGraceConfig(&graceCfg, nil); err != nil {
		return nil, "", fmt.Errorf("validation error in %s: %w", absPath, err)
	}

	if graceCfg.Config.Cleanup.OnSuccess == nil {
		defaultTrue := true
		graceCfg.Config.Cleanup.OnSuccess = &defaultTrue
	}

	return &graceCfg, configDir, nil
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

func ValidateGraceConfig(cfg *types.GraceConfig, registry *jobhandler.HandlerRegistry) error {
	// Basic syntax and field validation
	syntaxErrs := validateSyntax(cfg, registry)

	// --- Build job map & prepare graph ---

	jobMap := make(map[string]*types.Job, len(cfg.Jobs))
	jobGraph := make(map[string]*JobNode, len(cfg.Jobs))
	producedPaths := make(map[string]string) // virtual path -> name of producing job

	if len(syntaxErrs) != 0 {
		return errors.New(strings.Join(syntaxErrs, "\n- "))
	}

	for _, job := range cfg.Jobs {
		jobMap[job.Name] = job
		jobGraph[job.Name] = &JobNode{Job: job}

		for _, outputSpec := range job.Outputs {
			if producerJob, exists := producedPaths[outputSpec.Path]; exists {
				// Error - same virtual path produced by multiple jobs
				syntaxErrs = append(syntaxErrs, fmt.Sprintf("job[%q] and job[%q] both produce the same virtual path %q", producerJob, job.Name, outputSpec.Path))
			} else {
				producedPaths[outputSpec.Path] = job.Name
			}
		}
	}

	if len(syntaxErrs) != 0 {
		return errors.New(strings.Join(syntaxErrs, "\n- "))
	}

	// --- Validate dependencies & build graph links ---

	depErrs := validateDependenciesAndBuildGraph(cfg, jobMap, jobGraph, producedPaths)

	// --- Cycle detection ---

	var cycleErrs []string
	if len(syntaxErrs) == 0 && len(depErrs) == 0 {
		if cyclePath := detectCycle(jobGraph); cyclePath != nil {
			cycleErrs = append(cycleErrs, fmt.Sprintf("dependency cycle detected: %s", strings.Join(cyclePath, " -> ")))
		}
	}

	// --- Combine errors and final reporting ---

	allErrs := append(append(syntaxErrs, depErrs...), cycleErrs...)
	if len(allErrs) > 0 {
		return errors.New(strings.Join(allErrs, "\n- "))
	}

	return nil
}

func validateSyntax(cfg *types.GraceConfig, registry *jobhandler.HandlerRegistry) []string {
	var errs []string

	// --- Validate top-level 'config' section ---
	if cfg.Config.Profile == "" {
		errs = append(errs, "field 'config.profile' is required")
	}
	// Potential future validation: check if profile exists in Zowe config

	if cfg.Config.Concurrency < 0 {
		errs = append(errs, "field 'concurrency' cannot be negative")
	}

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

		// Validate job.Type
		if job.Type == "" {
			errs = append(errs, fmt.Sprintf("%s: field 'type' is required", jobCtx))
		} else if registry != nil { // Check against job handler registry
			handler, exists := registry.Get(job.Type)
			if !exists {
				allowed := registry.GetRegisteredTypes()
				errs = append(errs, fmt.Sprintf("%s: invalid type %q; known types are: %v", jobCtx, job.Type, allowed))
			} else {
				handlerErrs := handler.Validate(job, cfg)
				if len(handlerErrs) > 0 {
					for _, handlerErr := range handlerErrs {
						errs = append(errs, fmt.Sprintf("%s: %s", jobCtx, handlerErr))
					}
				}
			}
		} else if !allowedTypes[job.Type] {
			allowed := getAllowedStepKeys(allowedTypes)
			errs = append(errs, fmt.Sprintf("%s: invalid type %q (handler validation skipped); allowed built-in types are: %v", jobCtx, job.Type, allowed))
		}

		// Validate job.JCL
		if job.JCL != "" {
			isZOSJobType := job.Type == "compile" || job.Type == "linkedit" || job.Type == "execute"
			if !isZOSJobType {
				errs = append(errs, fmt.Sprintf("%s: 'jcl' field is only applicable to z/OS job types (compile, linkedit, execute), not for type %q", jobCtx, job.Type))
			} else {
				if !(strings.HasPrefix(job.JCL, "file://") || strings.HasPrefix(job.JCL, "zos://")) {
					errs = append(errs, fmt.Sprintf("%s: 'jcl' field must start with 'file://' or 'zos://'", jobCtx))
				} else if strings.HasPrefix(job.JCL, "zos://") {
					dsnWithMember := strings.TrimPrefix(job.JCL, "zos://")
					if dsnWithMember == "" {
						errs = append(errs, fmt.Sprintf("%s: 'jcl' field 'zos://' path cannot be empty", jobCtx))
					} else {
						parts := strings.SplitN(dsnWithMember, "(", 2)
						if len(parts) == 1 {
							errs = append(errs, fmt.Sprintf("%s: 'jcl' field 'zos://%s' must specify a PDS member like 'PDS.NAME(MEMBER)'", jobCtx, dsnWithMember))
							// If just PDS name is valid for some operations in future, adjust this.
							// If it's a sequential dataset, Zowe submit might handle it, but our primary use case is PDS members for JCL.
						} else if len(parts) == 2 {
							baseDsn := parts[0]
							memberWithSuffix := parts[1]
							if !strings.HasSuffix(memberWithSuffix, ")") || len(memberWithSuffix) < 2 {
								errs = append(errs, fmt.Sprintf("%s: 'jcl' field 'zos://%s' has malformed member part", jobCtx, dsnWithMember))
							} else {
								member := memberWithSuffix[:len(memberWithSuffix)-1]
								if err := utils.ValidateDataSetQualifiers(baseDsn); err != nil {
									errs = append(errs, fmt.Sprintf("%s: invalid base dataset name in 'jcl' field 'zos://%s': %v", jobCtx, dsnWithMember, err))
								}
								if err := utils.ValidatePDSMemberName(member); err != nil {
									errs = append(errs, fmt.Sprintf("%s: invalid member name in 'jcl' field 'zos://%s': %v", jobCtx, dsnWithMember, err))
								}
							}
						}
					}
				} else if strings.HasPrefix(job.JCL, "file://") {
					filePath := strings.TrimPrefix(job.JCL, "file://")
					if filePath == "" {
						errs = append(errs, fmt.Sprintf("%s: 'jcl' field 'file://' path cannot be empty", jobCtx))
					}
					// Actual file existence will be checked later (e.g., in DeckAndUpload or handler Prepare)
					// as it's relative to the grace.yml config file directory.
				}
			}
		}

		// Validate job.Datasets if they exist. This applies if
		// the job block in grace.yml contains job-specific dataset overrides
		// for the global workspace level config.datasets block
		if job.Datasets != nil {
			dsCtx := fmt.Sprintf("%s datasets override:", jobCtx)
			if job.Datasets.JCL != "" {
				if err := utils.ValidateDataSetQualifiers(job.Datasets.JCL); err != nil {
					errs = append(errs, fmt.Sprintf("%s jcl: %v", dsCtx, err))
				}
			}
			if job.Datasets.SRC != "" {
				if err := utils.ValidateDataSetQualifiers(job.Datasets.SRC); err != nil {
					errs = append(errs, fmt.Sprintf("%s src: %v", dsCtx, err))
				}
			}
			if job.Datasets.LoadLib != "" {
				if err := utils.ValidateDataSetQualifiers(job.Datasets.LoadLib); err != nil {
					errs = append(errs, fmt.Sprintf("%s loadlib: %v", dsCtx, err))
				}
			}
		}

		// Validate toolchain overrides (if present)
		overrideCtx := fmt.Sprintf("%s overrides:", jobCtx)
		if job.Overrides.Compiler.Pgm != nil {
			if *job.Overrides.Compiler.Pgm == "" {
				errs = append(errs, fmt.Sprintf("%s compiler.pgm cannot be empty if provided", overrideCtx))
			} else if err := utils.ValidateDataSetQualifiers(*job.Overrides.Compiler.Pgm); err != nil {
				errs = append(errs, fmt.Sprintf("%s compiler.pgm: %v", overrideCtx, err))
			}
		}
		if job.Overrides.Compiler.Parms != nil {
			if *job.Overrides.Compiler.Parms == "" {
				errs = append(errs, fmt.Sprintf("%s compiler.parms cannot be empty if provided", overrideCtx))
			} else if err := utils.ValidateDataSetQualifiers(*job.Overrides.Compiler.Parms); err != nil {
				errs = append(errs, fmt.Sprintf("%s compiler.parms: %v", overrideCtx, err))
			}
		}
		if job.Overrides.Compiler.StepLib != nil {
			if *job.Overrides.Compiler.StepLib == "" {
				errs = append(errs, fmt.Sprintf("%s compiler.steplib cannot be empty if provided", overrideCtx))
			} else if err := utils.ValidateDataSetQualifiers(*job.Overrides.Compiler.StepLib); err != nil {
				errs = append(errs, fmt.Sprintf("%s compiler.steplib: %v", overrideCtx, err))
			}
		}
		if job.Overrides.Linker.Pgm != nil {
			if *job.Overrides.Linker.Pgm == "" {
				errs = append(errs, fmt.Sprintf("%s linker.pgm cannot be empty if provided", overrideCtx))
			} else if err := utils.ValidateDataSetQualifiers(*job.Overrides.Linker.Pgm); err != nil {
				errs = append(errs, fmt.Sprintf("%s linker.pgm: %v", overrideCtx, err))
			}
		}
		if job.Overrides.Linker.Parms != nil {
			if *job.Overrides.Linker.Parms == "" {
				errs = append(errs, fmt.Sprintf("%s linker.parms cannot be empty if provided", overrideCtx))
			} else if err := utils.ValidateDataSetQualifiers(*job.Overrides.Linker.Parms); err != nil {
				errs = append(errs, fmt.Sprintf("%s linker.parms: %v", overrideCtx, err))
			}
		}
		if job.Overrides.Linker.StepLib != nil {
			if *job.Overrides.Linker.StepLib == "" {
				errs = append(errs, fmt.Sprintf("%s linker.steplib cannot be empty if provided", overrideCtx))
			} else if err := utils.ValidateDataSetQualifiers(*job.Overrides.Linker.StepLib); err != nil {
				errs = append(errs, fmt.Sprintf("%s linker.steplib: %v", overrideCtx, err))
			}
		}

		// Validate job inputs
		inputDDNames := make(map[string]bool)
		for j, inputSpec := range job.Inputs {
			inputCtx := fmt.Sprintf("%s input[%d]", jobCtx, j)

			if inputSpec.Name == "" {
				errs = append(errs, fmt.Sprintf("%s: field 'name' (DDName) is required", inputCtx))
			} else if !ddNameRegex.MatchString(strings.ToUpper(inputSpec.Name)) {
				errs = append(errs, fmt.Sprintf("%s: invalid 'name' (DDName) %q", inputCtx, inputSpec.Name))
			} else {
				// Check for duplicate DDNames within the same job's inputs
				if inputDDNames[inputSpec.Name] {
					errs = append(errs, fmt.Sprintf("%s: duplicate input DDName %q found", jobCtx, inputSpec.Name))
				}

				inputDDNames[inputSpec.Name] = true
			}

			if inputSpec.Path == "" {
				errs = append(errs, fmt.Sprintf("%s: field 'path' (virtual path) is required", inputCtx))
			} else if !virtualPathRegex.MatchString(inputSpec.Path) {
				errs = append(errs, fmt.Sprintf("%s: invalid 'path' format %q (must be scheme://resource)", inputCtx, inputSpec.Path))
			} else {
				isZosTemp := strings.HasPrefix(inputSpec.Path, "zos-temp://")
				isLocalTemp := strings.HasPrefix(inputSpec.Path, "local-temp://")
				isSrc := strings.HasPrefix(inputSpec.Path, "src://")
				isZos := strings.HasPrefix(inputSpec.Path, "zos://")
				isFile := strings.HasPrefix(inputSpec.Path, "file://")

				if !isZosTemp && !isLocalTemp && !isSrc && !isZos && !isFile {
					errs = append(errs, fmt.Sprintf("%s: unsupported scheme in path %q", inputCtx, inputSpec.Path))
				}
			}

			if inputSpec.Encoding != "" && inputSpec.Encoding != "binary" && inputSpec.Encoding != "text" {
				errs = append(errs, fmt.Sprintf("%s: invalid 'encoding' value %q; allowed values are 'binary', 'text', or omitted (defaults to binary)", inputCtx, inputSpec.Encoding))
			}
		}

		// Validate job outputs
		outputDDNames := make(map[string]bool)
		for k, outputSpec := range job.Outputs {
			outputCtx := fmt.Sprintf("%s output[%d]", jobCtx, k)
			if outputSpec.Name == "" {
				errs = append(errs, fmt.Sprintf("%s: field 'name' (DDName) is required", outputCtx))
			} else if !ddNameRegex.MatchString(outputSpec.Name) {
				errs = append(errs, fmt.Sprintf("%s: invalid 'name' (DDName) %q", outputCtx, outputSpec.Name))
			} else {
				// Check for duplicate DDNames within the same job's outputs
				if outputDDNames[outputSpec.Name] {
					errs = append(errs, fmt.Sprintf("%s: duplicate output DDName %q found", jobCtx, outputSpec.Name))
				}
				outputDDNames[outputSpec.Name] = true
				// Check if an output DDName conflicts with an input DDName
				if inputDDNames[outputSpec.Name] {
					errs = append(errs, fmt.Sprintf("%s: output DDName %q conflicts with an input DDName in the same job", jobCtx, outputSpec.Name))
				}
			}

			if outputSpec.Path == "" {
				errs = append(errs, fmt.Sprintf("%s: field 'path' (virtual path) is required", outputCtx))
			} else if !virtualPathRegex.MatchString(outputSpec.Path) {
				errs = append(errs, fmt.Sprintf("%s: invalid 'path' format %q (must be scheme://resource)", outputCtx, outputSpec.Path))
			} else {
				isZosTemp := strings.HasPrefix(outputSpec.Path, "zos-temp://")
				isLocalTemp := strings.HasPrefix(outputSpec.Path, "local-temp://")
				isSrc := strings.HasPrefix(outputSpec.Path, "src://")
				isZos := strings.HasPrefix(outputSpec.Path, "zos://")
				isFile := strings.HasPrefix(outputSpec.Path, "file://")

				if !isZosTemp && !isLocalTemp && !isSrc && !isZos && !isFile {
					errs = append(errs, fmt.Sprintf("%s: unsupported scheme in path %q", outputCtx, outputSpec.Path))
				}
			}
		}
	}

	return errs
}

// validateDependenciesAndBuildGraph checks explicit dependencies and input path availability.
// It populates the graph structure used for cycle detection.
func validateDependenciesAndBuildGraph(cfg *types.GraceConfig, jobMap map[string]*types.Job, jobGraph map[string]*JobNode, producedPaths map[string]string) []string {
	var errs []string

	// --- Build the graph links based on explicit depends_on ---
	for _, currentJob := range cfg.Jobs {
		jobCtx := fmt.Sprintf("job (name: %q)", currentJob.Name)
		currentNode := jobGraph[currentJob.Name]
		if currentNode == nil { // Should not happen
			errs = append(errs, fmt.Sprintf("%s: internal error - node not found in graph map", jobCtx))
			continue
		}

		for _, depName := range currentJob.DependsOn {
			dependencyNode, exists := jobGraph[depName]
			if !exists {
				errs = append(errs, fmt.Sprintf("%s: dependency %q not found", jobCtx, depName))
				continue
			}
			if depName == currentJob.Name {
				errs = append(errs, fmt.Sprintf("%s: job cannot depend on itself", jobCtx))
				continue
			}

			// Add edge: dependencyNode -> currentNode
			currentNode.Dependencies = append(currentNode.Dependencies, dependencyNode)
			dependencyNode.Dependents = append(dependencyNode.Dependents, currentNode)
		}
	} // End depends_on link building loop

	// --- Validate inputs availability (separate loop for clarity) ---
	for _, currentJob := range cfg.Jobs {
		jobCtx := fmt.Sprintf("job (name: %q)", currentJob.Name)
		for j, inputSpec := range currentJob.Inputs {
			inputCtx := fmt.Sprintf("%s input[%d]", jobCtx, j)
			producerJobName, produced := producedPaths[inputSpec.Path]

			if !produced && (strings.HasPrefix(inputSpec.Path, "zos-temp://") || strings.HasPrefix(inputSpec.Path, "local-temp://")) {
				errs = append(errs, fmt.Sprintf("%s: input path %q is not produced by any job", inputCtx, inputSpec.Path))
				continue
			} else if produced {
				_ = producerJobName
			}
		}
	}

	return errs
}

// detectCycle performs DFS to find cycles in the job graph.
// Returns a slice of job names representing the cycle path if found, otherwise nil.
func detectCycle(graph map[string]*JobNode) []string {
	path := []string{}
	visited := make(map[string]bool)
	visiting := make(map[string]bool)

	var dfs func(nodeName string) []string // Returns the cycle path if found

	dfs = func(nodeName string) []string {
		visited[nodeName] = true
		visiting[nodeName] = true
		path = append(path, nodeName)

		node := graph[nodeName]
		for _, dep := range node.Dependents {
			depName := dep.Job.Name

			if visiting[depName] {
				// Cycle detected
				// Encountered a node already in the current recursion stack
				cycleStartIndex := -1
				for i, nameInPath := range path {
					if nameInPath == depName {
						cycleStartIndex = i
						break
					}
				}

				if cycleStartIndex != -1 {
					return append(path[cycleStartIndex:], depName)
				} else {
					fmt.Fprintf(os.Stderr, "DEBUG: Cycle detected but start node %s not in path %v\n", depName, path)
					return append(path, depName)
				}
			}

			if !visited[depName] {
				// If a dependent hasn't been visited at all, recurse
				if cycleResult := dfs(depName); cycleResult != nil {
					// If a cycle was found deeper down, propagate up immediately
					return cycleResult
				}
			}
		}

		path = path[:len(path)-1]
		visiting[nodeName] = false
		return nil
	}

	for nodeName := range graph {
		if !visited[nodeName] {
			if cycle := dfs(nodeName); cycle != nil {
				return cycle
			}
		}
	}

	return nil
}
