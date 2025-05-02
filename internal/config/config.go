package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/graceinfra/grace/internal/utils"
	"github.com/graceinfra/grace/types"
	"gopkg.in/yaml.v3"
)

var allowedSteps = map[string]bool{
	"execute":  true,
	"compile":  true,
	"linkedit": true,
}

// --- Define validation rules for DD names and virtual paths ---

// Basic DDName validation (1-8 chars, starts non-numeric, common chars)
var ddNameRegex = regexp.MustCompile(`^[A-Z#$@][A-Z0-9#$@]{0,7}$`)

// Basic virtual path check (e.g., scheme://path)
var virtualPathRegex = regexp.MustCompile(`^[a-zA-Z]+://.+`)

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
	// Basic syntax and field validation
	syntaxErrs := validateSyntax(cfg)

	// --- Build job map & prepare graph ---

	jobMap := make(map[string]*types.Job, len(cfg.Jobs))
	jobGraph := make(map[string]*JobNode, len(cfg.Jobs))
	producedPaths := make(map[string]string) // virtual path -> name of producing job

	if len(syntaxErrs) != 0 {
		return errors.New("Grace configuration validation failed:\n- " + strings.Join(syntaxErrs, "\n- "))
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
		return errors.New("Grace configuration validation failed:\n- " + strings.Join(syntaxErrs, "\n- "))
	}

	// --- Validate dependencies & build graph links ---

	depErrs := validateDependenciesAndBuildGraph(cfg, jobMap, jobGraph, producedPaths)

	// --- *** ADD DEBUG PRINTING HERE *** ---
	// fmt.Println("--- DEBUG: Graph Structure before Cycle Check ---")
	// for name, node := range jobGraph {
	// 	fmt.Printf("Node: %s\n", name)
	// 	depNames := []string{}
	// 	for _, dep := range node.Dependencies {
	// 		depNames = append(depNames, dep.Job.Name)
	// 	}
	// 	fmt.Printf("  Dependencies: %v\n", depNames)
	// 	dependentNames := []string{}
	// 	for _, dpt := range node.Dependents {
	// 		dependentNames = append(dependentNames, dpt.Job.Name)
	// 	}
	// 	fmt.Printf("  Dependents: %v\n", dependentNames)
	// }
	// fmt.Println("--- END DEBUG ---")
	// --- *** END DEBUG PRINTING *** ---

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
		return errors.New("Grace configuration validation failed:\n- " + strings.Join(allErrs, "\n- "))
	}

	return nil
}

func validateSyntax(cfg *types.GraceConfig) []string {
	var errs []string

	// --- Validate top-level 'config' section ---
	if cfg.Config.Profile == "" {
		errs = append(errs, "field 'config.profile' is required")
	}
	// Potential future validation: check if profile exists in Zowe config

	if cfg.Config.Concurrency < 0 {
		errs = append(errs, fmt.Sprintf("field 'concurrency' cannot be negative"))
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

		// Validate job.Step
		if job.Step == "" {
			errs = append(errs, fmt.Sprintf("%s: field 'step' is required", jobCtx))
		} else if !allowedSteps[job.Step] { // Check against allowed steps
			allowed := getAllowedStepKeys(allowedSteps)
			errs = append(errs, fmt.Sprintf("%s: invalid step %q; allowed steps are: %v", jobCtx, job.Step, allowed))
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
				isTemp := strings.HasPrefix(inputSpec.Path, "temp://")
				isSrc := strings.HasPrefix(inputSpec.Path, "src://")

				if !isTemp && !isSrc {
					errs = append(errs, fmt.Sprintf("%s: unsupported scheme in path %q (only temp:// allowed for now)", inputCtx, inputSpec.Path))
				}
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
				isTemp := strings.HasPrefix(outputSpec.Path, "temp://")
				isSrc := strings.HasPrefix(outputSpec.Path, "src://")

				if !isTemp && !isSrc {
					errs = append(errs, fmt.Sprintf("%s: unsupported scheme in path %q (only temp:// allowed for now)", outputCtx, outputSpec.Path))
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

			if !produced && strings.HasPrefix(inputSpec.Path, "temp://") {
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
