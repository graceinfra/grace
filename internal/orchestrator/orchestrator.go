package orchestrator

import (
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/jobhandler"
	"github.com/graceinfra/grace/internal/models"
)

type Orchestrator interface {
	DeckAndUpload(ctx *context.ExecutionContext, registry *jobhandler.HandlerRegistry, noCompile, noUpload bool) error
	Run(ctx *context.ExecutionContext, registry *jobhandler.HandlerRegistry) ([]models.JobExecutionRecord, error)
}
