package orchestrator

import (
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/models"
)

type Orchestrator interface {
	Run(ctx *context.ExecutionContext) ([]models.JobExecutionRecord, error)
	DeckAndUpload(ctx *context.ExecutionContext, noCompile, noUpload bool) error
}
