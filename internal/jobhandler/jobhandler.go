package jobhandler

import (
	"fmt"
	"sort"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// JobHandler defines the standard interface for all job type implementations
type JobHandler interface {
	// Type returns the canonical string identifier for this handler
	// (e.g., "graceinfra/compile@v0.1", "shell"). This should match the 'type' field in grace.yml.
	Type() string

	// Validate checks the job-specific configuration fields within the types.Job struct
	// (e.g., the 'with' block for shell, 'program' for execute) against the global config.
	// It should return a slice of error messages if validation fails.
	Validate(job *types.Job, cfg *types.GraceConfig) []string

	// Prepare performs any setup needed before the Execute step.
	// This might involve pre-checking resources, allocating local staging, etc.
	// It can return an error if preparation fails, preventing execution.
	Prepare(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) error

	// Execute runs the main logic of the job type.
	// It receives the ExecutionContext and the specific job definition.
	// It MUST return a JobExecutionRecord, populated with the results (success/failure,
	// stdout/stderr if applicable, timing, final status, etc.).
	Execute(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) *models.JobExecutionRecord

	// Cleanup performs any necessary cleanup after the job's Execute step,
	// regardless of success or failure (unless Prepare failed).
	// This is for resources managed by the handler itself, separate from the
	// orchestrator's general temp resource cleanup.
	Cleanup(ctx *context.ExecutionContext, job *types.Job, execRecord *models.JobExecutionRecord, logger zerolog.Logger) error
}

// HandlerRegistry holds registered job handlers
type HandlerRegistry struct {
	handlers map[string]JobHandler
}

// NewRegistry creates a new, empty handler registry
func NewRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]JobHandler),
	}
}

// Register adds a JobHandler to the registry. It will panic if a handler
// with the same type is already registered (indicating an initialization error)
func (r *HandlerRegistry) Register(handler JobHandler) {
	typeName := handler.Type()
	if _, exists := r.handlers[typeName]; exists {
		panic(fmt.Sprintf("handler for type %q already registered", typeName))
	}
	r.handlers[typeName] = handler
	log.Debug().Str("handler_type", typeName).Msg("Registered job handler")
}

// Get retrieves a handler by its type name. Returns the handler and true if found,
// otherwise nil and false
func (r *HandlerRegistry) Get(typeName string) (JobHandler, bool) {
	// TODO: add handling for canonical names vs aliases if implemented later
	// e.g. "compile", look up "graceinfra/compile@latest"
	handler, exists := r.handlers[typeName]
	return handler, exists
}

// MustGet retrieves a handler by its type name. Panics if the handler is not found.
// Useful for internal logic where a handler is expected to exist after validation.
func (r *HandlerRegistry) MustGet(typeName string) JobHandler {
	handler, exists := r.Get(typeName)
	if !exists {
		panic(fmt.Sprintf("critical error: no handler registered for type %q", typeName))
	}
	return handler
}

// IsKnownType checks if a handler is registered for the given type name
func (r *HandlerRegistry) IsKnownType(typeName string) bool {
	_, exists := r.Get(typeName)
	return exists
}

// GetRegisteredTypes returns a sorted list of known handler type names
func (r *HandlerRegistry) GetRegisteredTypes() []string {
	keys := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	return keys
}
