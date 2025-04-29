package config

import (
	"fmt"

	"github.com/graceinfra/grace/types"
)

type JobNode struct {
	Job          *types.Job
	Dependencies []*JobNode
	Dependents   []*JobNode
}

func BuildJobGraph(cfg *types.GraceConfig) (map[string]*JobNode, error) {
	jobMap := make(map[string]*types.Job, len(cfg.Jobs))
	jobGraph := make(map[string]*JobNode, len(cfg.Jobs))

	// First pass: create nodes
	for _, job := range cfg.Jobs {
		jobMap[job.Name] = job
		jobGraph[job.Name] = &JobNode{Job: job}
	}

	// Second pass: link dependencies
	for _, job := range cfg.Jobs {
		node := jobGraph[job.Name]
		for _, depName := range job.DependsOn {
			depNode, exists := jobGraph[depName]

			if !exists || depNode == nil {
				return nil, fmt.Errorf("internal error: dependency %q for job %q not found during graph build", depName, job.Name)
			}

			node.Dependencies = append(node.Dependencies, depNode)
			depNode.Dependents = append(depNode.Dependents, node)
		}
	}

	return jobGraph, nil
}
