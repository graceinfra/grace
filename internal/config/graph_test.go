package config

import (
	"testing"

	"github.com/graceinfra/grace/types"
	"github.com/stretchr/testify/assert"
)

func TestBuildJobGraph(t *testing.T) {
	t.Run("Empty job list", func(t *testing.T) {
		cfg := &types.GraceConfig{
			Jobs: []*types.Job{},
		}

		graph, err := BuildJobGraph(cfg)
		assert.NoError(t, err)
		assert.Empty(t, graph)
	})

	t.Run("Single job without dependencies", func(t *testing.T) {
		job := &types.Job{
			Name:   "JOB1",
			Step:   "execute",
			Source: "src/job1.jcl",
		}

		cfg := &types.GraceConfig{
			Jobs: []*types.Job{job},
		}

		graph, err := BuildJobGraph(cfg)
		assert.NoError(t, err)
		assert.Len(t, graph, 1)
		assert.Contains(t, graph, "JOB1")
		assert.Equal(t, job, graph["JOB1"].Job)
		assert.Empty(t, graph["JOB1"].Dependencies)
		assert.Empty(t, graph["JOB1"].Dependents)
	})

	t.Run("Linear dependency chain", func(t *testing.T) {
		job1 := &types.Job{
			Name:   "JOB1",
			Step:   "execute",
			Source: "src/job1.jcl",
		}
		job2 := &types.Job{
			Name:      "JOB2",
			Step:      "execute",
			Source:    "src/job2.jcl",
			DependsOn: []string{"JOB1"},
		}
		job3 := &types.Job{
			Name:      "JOB3",
			Step:      "execute",
			Source:    "src/job3.jcl",
			DependsOn: []string{"JOB2"},
		}

		cfg := &types.GraceConfig{
			Jobs: []*types.Job{job1, job2, job3},
		}

		graph, err := BuildJobGraph(cfg)
		assert.NoError(t, err)
		assert.Len(t, graph, 3)

		// Check JOB1 connections
		assert.Empty(t, graph["JOB1"].Dependencies)
		assert.Len(t, graph["JOB1"].Dependents, 1)
		assert.Equal(t, "JOB2", graph["JOB1"].Dependents[0].Job.Name)

		// Check JOB2 connections
		assert.Len(t, graph["JOB2"].Dependencies, 1)
		assert.Equal(t, "JOB1", graph["JOB2"].Dependencies[0].Job.Name)
		assert.Len(t, graph["JOB2"].Dependents, 1)
		assert.Equal(t, "JOB3", graph["JOB2"].Dependents[0].Job.Name)

		// Check JOB3 connections
		assert.Len(t, graph["JOB3"].Dependencies, 1)
		assert.Equal(t, "JOB2", graph["JOB3"].Dependencies[0].Job.Name)
		assert.Empty(t, graph["JOB3"].Dependents)
	})

	t.Run("Multiple dependencies", func(t *testing.T) {
		job1 := &types.Job{
			Name:   "JOB1",
			Step:   "execute",
			Source: "src/job1.jcl",
		}
		job2 := &types.Job{
			Name:   "JOB2",
			Step:   "execute",
			Source: "src/job2.jcl",
		}
		job3 := &types.Job{
			Name:      "JOB3",
			Step:      "execute",
			Source:    "src/job3.jcl",
			DependsOn: []string{"JOB1", "JOB2"},
		}

		cfg := &types.GraceConfig{
			Jobs: []*types.Job{job1, job2, job3},
		}

		graph, err := BuildJobGraph(cfg)
		assert.NoError(t, err)
		assert.Len(t, graph, 3)

		// Check JOB3 has two dependencies
		assert.Len(t, graph["JOB3"].Dependencies, 2)
		depNames := []string{
			graph["JOB3"].Dependencies[0].Job.Name,
			graph["JOB3"].Dependencies[1].Job.Name,
		}
		assert.Contains(t, depNames, "JOB1")
		assert.Contains(t, depNames, "JOB2")

		// Check JOB1 and JOB2 both have JOB3 as dependent
		assert.Len(t, graph["JOB1"].Dependents, 1)
		assert.Equal(t, "JOB3", graph["JOB1"].Dependents[0].Job.Name)
		assert.Len(t, graph["JOB2"].Dependents, 1)
		assert.Equal(t, "JOB3", graph["JOB2"].Dependents[0].Job.Name)
	})

	t.Run("Complex graph", func(t *testing.T) {
		// Create a diamond dependency pattern:
		// JOB1 <- JOB2 <- JOB4
		//   ^       ^
		//   |       |
		//   +- JOB3 -+
		job1 := &types.Job{Name: "JOB1", Step: "execute", Source: "src/job1.jcl"}
		job2 := &types.Job{Name: "JOB2", Step: "execute", Source: "src/job2.jcl", DependsOn: []string{"JOB1"}}
		job3 := &types.Job{Name: "JOB3", Step: "execute", Source: "src/job3.jcl", DependsOn: []string{"JOB1"}}
		job4 := &types.Job{Name: "JOB4", Step: "execute", Source: "src/job4.jcl", DependsOn: []string{"JOB2", "JOB3"}}

		cfg := &types.GraceConfig{
			Jobs: []*types.Job{job1, job2, job3, job4},
		}

		graph, err := BuildJobGraph(cfg)
		assert.NoError(t, err)
		assert.Len(t, graph, 4)

		// Verify the structure
		assert.Empty(t, graph["JOB1"].Dependencies)
		assert.Len(t, graph["JOB1"].Dependents, 2)

		assert.Len(t, graph["JOB2"].Dependencies, 1)
		assert.Equal(t, "JOB1", graph["JOB2"].Dependencies[0].Job.Name)
		assert.Len(t, graph["JOB2"].Dependents, 1)
		assert.Equal(t, "JOB4", graph["JOB2"].Dependents[0].Job.Name)

		assert.Len(t, graph["JOB3"].Dependencies, 1)
		assert.Equal(t, "JOB1", graph["JOB3"].Dependencies[0].Job.Name)
		assert.Len(t, graph["JOB3"].Dependents, 1)
		assert.Equal(t, "JOB4", graph["JOB3"].Dependents[0].Job.Name)

		assert.Len(t, graph["JOB4"].Dependencies, 2)
		assert.Empty(t, graph["JOB4"].Dependents)
	})
}