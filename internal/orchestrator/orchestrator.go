package orchestrator

import (
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/types"
)

type Orchestrator interface {
	Run(ctx *context.ExecutionContext) ([]models.JobExecutionRecord, error)
	Submit(ctx *context.ExecutionContext) ([]*types.ZoweRfj, error)
	DeckAndUpload(ctx *context.ExecutionContext, noCompile, noUpload bool) error
}
