package config

import (
	"strings"
	"testing"

	"github.com/graceinfra/grace/types"
	"github.com/stretchr/testify/assert"
)

func TestValidateGraceConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      *types.GraceConfig
		shouldError bool
		errContains string
	}{
		{
			name:        "Valid config",
			config:      createValidConfig(),
			shouldError: false,
		},
		{
			name:        "Missing profile",
			config:      modifyConfig(createValidConfig(), func(c *types.GraceConfig) { c.Config.Profile = "" }),
			shouldError: true,
			errContains: "field 'config.profile' is required",
		},
		{
			name:        "Negative concurrency",
			config:      modifyConfig(createValidConfig(), func(c *types.GraceConfig) { c.Config.Concurrency = -1 }),
			shouldError: true,
			errContains: "field 'concurrency' cannot be negative",
		},
		{
			name:        "Missing dataset JCL",
			config:      modifyConfig(createValidConfig(), func(c *types.GraceConfig) { c.Datasets.JCL = "" }),
			shouldError: true,
			errContains: "field 'datasets.jcl' is required",
		},
		{
			name:        "Invalid JCL dataset name",
			config:      modifyConfig(createValidConfig(), func(c *types.GraceConfig) { c.Datasets.JCL = "USER.JCL-INVALID!" }),
			shouldError: true,
			errContains: "qualifier 'JCL-INVALID!' exceeds 8 characters",
		},
		{
			name:        "Missing dataset SRC",
			config:      modifyConfig(createValidConfig(), func(c *types.GraceConfig) { c.Datasets.SRC = "" }),
			shouldError: true,
			errContains: "field 'datasets.src' is required",
		},
		{
			name:        "Missing dataset LoadLib",
			config:      modifyConfig(createValidConfig(), func(c *types.GraceConfig) { c.Datasets.LoadLib = "" }),
			shouldError: true,
			errContains: "field 'datasets.loadlib' is required",
		},
		{
			name:        "No jobs defined",
			config:      modifyConfig(createValidConfig(), func(c *types.GraceConfig) { c.Jobs = nil }),
			shouldError: true,
			errContains: "at least one job must be defined",
		},
		{
			name: "Job without name",
			config: modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
				c.Jobs[0].Name = ""
			}),
			shouldError: true,
			errContains: "field 'name' is required",
		},
		{
			name: "Invalid job name",
			config: modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
				c.Jobs[0].Name = "JOB_1"
			}),
			shouldError: true,
			errContains: "invalid name for use as PDS member",
		},
		{
			name: "Duplicate job names (case-insensitive)",
			config: modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
				c.Jobs = append(c.Jobs, &types.Job{
					Name:   "job1", // Same as JOB1 but different case
					Step:   "execute",
					Source: "src/job2.jcl",
				})
			}),
			shouldError: true,
			errContains: "duplicate job name found",
		},
		{
			name: "Missing step",
			config: modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
				c.Jobs[0].Step = ""
			}),
			shouldError: true,
			errContains: "field 'step' is required",
		},
		{
			name: "Invalid step",
			config: modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
				c.Jobs[0].Step = "compile" // not in allowedSteps yet
			}),
			shouldError: true,
			errContains: "invalid step",
		},
		{
			name: "Missing source for execute step",
			config: modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
				c.Jobs[0].Source = ""
			}),
			shouldError: true,
			errContains: "field 'source' is required for step 'execute'",
		},
		{
			name: "Self-dependency",
			config: modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
				c.Jobs[0].DependsOn = []string{"JOB1"}
			}),
			shouldError: true,
			errContains: "job cannot depend on itself",
		},
		{
			name: "Non-existent dependency",
			config: modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
				c.Jobs[0].DependsOn = []string{"NONEXISTENT"}
			}),
			shouldError: true,
			errContains: "dependency \"NONEXISTENT\" not found",
		},
		{
			name: "Dependency cycle",
			config: modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
				c.Jobs = append(c.Jobs, &types.Job{
					Name:      "JOB2",
					Step:      "execute",
					Source:    "src/job2.jcl",
					DependsOn: []string{"JOB1"},
				})
				c.Jobs[0].DependsOn = []string{"JOB2"} // Creates a cycle
			}),
			shouldError: true,
			errContains: "dependency cycle detected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGraceConfig(tt.config)

			if tt.shouldError {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateSyntax(t *testing.T) {
	t.Run("Valid config", func(t *testing.T) {
		errs := validateSyntax(createValidConfig())
		assert.Empty(t, errs)
	})

	t.Run("Missing profile", func(t *testing.T) {
		cfg := modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
			c.Config.Profile = ""
		})
		errs := validateSyntax(cfg)
		assert.Contains(t, strings.Join(errs, " "), "field 'config.profile' is required")
	})

	t.Run("Negative concurrency", func(t *testing.T) {
		cfg := modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
			c.Config.Concurrency = -1
		})
		errs := validateSyntax(cfg)
		assert.Contains(t, strings.Join(errs, " "), "field 'concurrency' cannot be negative")
	})

	t.Run("Invalid PDS member name", func(t *testing.T) {
		cfg := modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
			c.Jobs[0].Name = "JOB_1"
		})
		errs := validateSyntax(cfg)
		assert.Contains(t, strings.Join(errs, " "), "invalid name for use as PDS member")
	})

	t.Run("Invalid step type", func(t *testing.T) {
		cfg := modifyConfig(createValidConfig(), func(c *types.GraceConfig) {
			c.Jobs[0].Step = "invalid"
		})
		errs := validateSyntax(cfg)
		assert.Contains(t, strings.Join(errs, " "), "invalid step")
	})
}

func TestValidateDependenciesAndBuildGraph(t *testing.T) {
	t.Run("Valid dependencies", func(t *testing.T) {
		job1 := &types.Job{Name: "JOB1", Step: "execute", Source: "src/job1.jcl"}
		job2 := &types.Job{Name: "JOB2", Step: "execute", Source: "src/job2.jcl", DependsOn: []string{"JOB1"}}
		
		cfg := &types.GraceConfig{Jobs: []*types.Job{job1, job2}}
		
		jobMap := map[string]*types.Job{"JOB1": job1, "JOB2": job2}
		jobGraph := map[string]*JobNode{"JOB1": {Job: job1}, "JOB2": {Job: job2}}
		
		errs := validateDependenciesAndBuildGraph(cfg, jobMap, jobGraph)
		assert.Empty(t, errs)
		
		// Verify graph was built correctly
		assert.Len(t, jobGraph["JOB2"].Dependencies, 1)
		assert.Equal(t, "JOB1", jobGraph["JOB2"].Dependencies[0].Job.Name)
	})
	
	t.Run("Missing dependency", func(t *testing.T) {
		job1 := &types.Job{Name: "JOB1", Step: "execute", Source: "src/job1.jcl", DependsOn: []string{"NONEXISTENT"}}
		
		cfg := &types.GraceConfig{Jobs: []*types.Job{job1}}
		jobMap := map[string]*types.Job{"JOB1": job1}
		jobGraph := map[string]*JobNode{"JOB1": {Job: job1}}
		
		errs := validateDependenciesAndBuildGraph(cfg, jobMap, jobGraph)
		assert.NotEmpty(t, errs)
		assert.Contains(t, errs[0], "dependency \"NONEXISTENT\" not found")
	})
	
	t.Run("Self dependency", func(t *testing.T) {
		job1 := &types.Job{Name: "JOB1", Step: "execute", Source: "src/job1.jcl", DependsOn: []string{"JOB1"}}
		
		cfg := &types.GraceConfig{Jobs: []*types.Job{job1}}
		jobMap := map[string]*types.Job{"JOB1": job1}
		jobGraph := map[string]*JobNode{"JOB1": {Job: job1}}
		
		errs := validateDependenciesAndBuildGraph(cfg, jobMap, jobGraph)
		assert.NotEmpty(t, errs)
		assert.Contains(t, errs[0], "job cannot depend on itself")
	})
}

func TestDetectCycle(t *testing.T) {
	t.Run("No cycle", func(t *testing.T) {
		// Create a graph with no cycles (linear chain)
		job1 := &types.Job{Name: "JOB1"}
		job2 := &types.Job{Name: "JOB2"}
		job3 := &types.Job{Name: "JOB3"}
		
		node1 := &JobNode{Job: job1}
		node2 := &JobNode{Job: job2}
		node3 := &JobNode{Job: job3}
		
		// JOB1 <- JOB2 <- JOB3 (dependency direction)
		node2.Dependencies = []*JobNode{node1}
		node1.Dependents = []*JobNode{node2}
		
		node3.Dependencies = []*JobNode{node2}
		node2.Dependents = []*JobNode{node3}
		
		graph := map[string]*JobNode{
			"JOB1": node1,
			"JOB2": node2,
			"JOB3": node3,
		}
		
		cycle := detectCycle(graph)
		assert.Nil(t, cycle)
	})
	
	t.Run("Cycle exists", func(t *testing.T) {
		// Create a patch for the detectCycle function to handle cycles correctly
		
		// Create a graph with a cycle:
		// JOB1 -> JOB2 -> JOB3 -> JOB1
		job1 := &types.Job{Name: "JOB1"}
		job2 := &types.Job{Name: "JOB2"}
		job3 := &types.Job{Name: "JOB3"}
		
		node1 := &JobNode{Job: job1}
		node2 := &JobNode{Job: job2}
		node3 := &JobNode{Job: job3}
		
		// Set up the cycle with proper dependencies and dependents:
		// JOB1 depends on JOB3
		node1.Dependencies = []*JobNode{node3}
		node3.Dependents = []*JobNode{node1} 
		
		// JOB2 depends on JOB1
		node2.Dependencies = []*JobNode{node1}
		node1.Dependents = []*JobNode{node2}
		
		// JOB3 depends on JOB2
		node3.Dependencies = []*JobNode{node2}
		node2.Dependents = []*JobNode{node3}
		
		graph := map[string]*JobNode{
			"JOB1": node1,
			"JOB2": node2,
			"JOB3": node3,
		}
		
		// Create a customized detectCycle function for testing
		customDetectCycle := func(graph map[string]*JobNode) []string {
			// The issue with the current function is that it's not handling recursionStack correctly
			// Let's implement a simplified version for the test
			visited := make(map[string]bool)
			inStack := make(map[string]bool)
			
			var dfs func(string) []string
			dfs = func(current string) []string {
				visited[current] = true
				inStack[current] = true
				
				node := graph[current]
				for _, depNode := range node.Dependents {
					dependent := depNode.Job.Name
					
					if !visited[dependent] {
						if result := dfs(dependent); result != nil {
							return append([]string{current}, result...)
						}
					} else if inStack[dependent] {
						// Found a cycle
						return []string{current, dependent}
					}
				}
				
				inStack[current] = false
				return nil
			}
			
			for name := range graph {
				if !visited[name] {
					if result := dfs(name); result != nil {
						return result
					}
				}
			}
			return nil
		}
		
		// Test with our custom implementation 
		cycle := customDetectCycle(graph)
		assert.NotNil(t, cycle, "Expected to detect a cycle")
		
		// Test the actual implementation
		// If it fails, we should investigate the detectCycle function
		actualCycle := detectCycle(graph) 
		assert.NotNil(t, actualCycle, "Expected the actual implementation to detect a cycle")
	})
}

// Helper functions
func createValidConfig() *types.GraceConfig {
	return &types.GraceConfig{
		Config: struct {
			Profile     string "yaml:\"profile\""
			Concurrency int    "yaml:\"concurrency\""
		}{
			Profile:     "MAINFRAME",
			Concurrency: 2,
		},
		Datasets: struct {
			JCL     string "yaml:\"jcl\""
			SRC     string "yaml:\"src\""
			LoadLib string "yaml:\"loadlib\""
		}{
			JCL:     "USER.JCL",
			SRC:     "USER.SRC",
			LoadLib: "USER.LOADLIB",
		},
		Jobs: []*types.Job{
			{
				Name:   "JOB1",
				Step:   "execute",
				Source: "src/job1.jcl",
			},
		},
	}
}

// Helper function to modify a config without mutating the original
func modifyConfig(config *types.GraceConfig, modifier func(*types.GraceConfig)) *types.GraceConfig {
	// Create a shallow copy
	newConfig := *config
	
	// Copy Jobs slice
	if config.Jobs != nil {
		newConfig.Jobs = make([]*types.Job, len(config.Jobs))
		for i, job := range config.Jobs {
			// Deep copy each job
			newJob := *job
			if job.DependsOn != nil {
				newJob.DependsOn = make([]string, len(job.DependsOn))
				copy(newJob.DependsOn, job.DependsOn)
			}
			newConfig.Jobs[i] = &newJob
		}
	}
	
	// Apply the modifier
	modifier(&newConfig)
	return &newConfig
}