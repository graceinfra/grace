package orchestrator

import (
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/jobhandler"
	"github.com/graceinfra/grace/internal/models"
)

type Orchestrator interface {
	Run(ctx *context.ExecutionContext, registry *jobhandler.HandlerRegistry) ([]models.JobExecutionRecord, error)
	DeckAndUpload(ctx *context.ExecutionContext, noCompile, noUpload bool) error
}
