package cmd

import "github.com/graceinfra/grace/internal/jobhandler"

type AppDependencies struct {
	HandlerRegistry *jobhandler.HandlerRegistry
}

var appDependencies *AppDependencies

// SetDependencies allows for injecting application dependencies
func SetDependencies(deps *AppDependencies) {
	if deps == nil || deps.HandlerRegistry == nil {
		panic("critical error: attempted to set nil dependencies or registry")
	}
	appDependencies = deps
}

// GetDependencies provides access to the dependencies.
// Panics if dependencies haven't been set (indicates setup error).
func GetDependencies() *AppDependencies {
	if appDependencies == nil {
		panic("critical error: application dependencies not set before access")
	}
	return appDependencies
}
