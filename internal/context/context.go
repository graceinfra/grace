package context

import (
	"sync"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/types"
)

type ExecutionContext struct {
	WorkflowId    uuid.UUID
	Config        *types.GraceConfig
	ConfigDir     string // Directory that holds grace.yml
	LogDir        string
	LocalStageDir string   // Directory for local staging files ([logDir]/.local-staging/)
	SubmitOnly    []string // Selective job runs
	GraceCmd      string   // "submit", "run"

	ResolvedPaths map[string]string
	PathMutex     sync.RWMutex
}
