package context

import (
	"github.com/google/uuid"
	"github.com/graceinfra/grace/types"
)

type ExecutionContext struct {
	WorkflowId uuid.UUID
	Config     *types.GraceConfig
	LogDir     string
	SubmitOnly []string // Selective job runs
	GraceCmd   string   // "submit", "run"
}
