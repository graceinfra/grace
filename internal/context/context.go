package context

import (
	"fmt"
	"maps"
	"sync"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog/log"
)

type ExecutionContext struct {
	WorkflowId uuid.UUID
	Config     *types.GraceConfig
	LogDir     string
	SubmitOnly []string // Selective job runs
	GraceCmd   string   // "submit", "run"

	resolvedPaths map[string]string
	pathMutex     sync.RWMutex
}

// InitializePaths stores the pre-resolved path map in the context.
// Should be called once after paths.PreresolveOutputPaths.
func (ctx *ExecutionContext) InitializePaths(resolvedMap map[string]string) {
	ctx.pathMutex.Lock()
	defer ctx.pathMutex.Unlock()

	// Initialize map if it hasn't been already
	if ctx.resolvedPaths == nil {
		ctx.resolvedPaths = make(map[string]string)
	}

	// Copy values from resolvedMap
	maps.Copy(ctx.resolvedPaths, resolvedMap)
}

// ResolvePath retrieves the physical DSN for a given virtual path.
// Assumes paths were pre-resolved and stored via InitializePaths.
func (ctx *ExecutionContext) ResolvePath(virtualPath string) (string, error) {
	ctx.pathMutex.RLock()
	dsn, exists := ctx.resolvedPaths[virtualPath]
	ctx.pathMutex.RUnlock()

	if !exists {
		// This indicates a logic error - either validation missed something
		// or resolution wasn't called/completed correctly.
		log.Error().Str("virtual_path", virtualPath).Msg("Attempted to resolve an unknown/unresolved virtual path")
		return "", fmt.Errorf("virtual path %q not found in resolved paths map", virtualPath)
	}
	return dsn, nil
}
