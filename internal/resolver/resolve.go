package resolver

import (
	"github.com/graceinfra/grace/types"
)

// Helper function to dereference string pointer or return fallback
func resolveString(override *string, fallback string) string {
	if override != nil {
		return *override
	}
	return fallback
}

// --- Resolver functions ---

func ResolveCompilerPgm(job *types.Job, cfg *types.GraceConfig) string {
	hardcodedDefault := "IGYCRCTL"
	defaultVal := cfg.Config.Defaults.Compiler.Pgm
	if defaultVal == "" {
		defaultVal = hardcodedDefault
	}
	return resolveString(job.Overrides.Compiler.Pgm, defaultVal)
}

func ResolveCompilerParms(job *types.Job, cfg *types.GraceConfig) string {
	hardcodedDefault := "OBJECT,NODECK,LIB"
	defaultVal := cfg.Config.Defaults.Compiler.Parms
	if defaultVal == "" {
		defaultVal = hardcodedDefault
	}
	return resolveString(job.Overrides.Compiler.Parms, defaultVal)
}

func ResolveCompilerSteplib(job *types.Job, cfg *types.GraceConfig) string {
	defaultVal := cfg.Config.Defaults.Compiler.StepLib
	return resolveString(job.Overrides.Compiler.StepLib, defaultVal)
}

func ResolveLinkerPgm(job *types.Job, cfg *types.GraceConfig) string {
	hardcodedDefault := "IEWL"
	defaultVal := cfg.Config.Defaults.Linker.Pgm
	if defaultVal == "" {
		defaultVal = hardcodedDefault
	}
	return resolveString(job.Overrides.Linker.Pgm, defaultVal)
}

func ResolveLinkerParms(job *types.Job, cfg *types.GraceConfig) string {
	hardcodedDefault := "LIST,MAP,XREF"
	defaultVal := cfg.Config.Defaults.Linker.Parms
	if defaultVal == "" {
		defaultVal = hardcodedDefault
	}
	return resolveString(job.Overrides.Linker.Parms, defaultVal)
}

func ResolveLinkerSteplib(job *types.Job, cfg *types.GraceConfig) string {
	defaultVal := cfg.Config.Defaults.Linker.StepLib
	return resolveString(job.Overrides.Linker.StepLib, defaultVal)
}

// ResolveProgramName determines the PGM= value for execute or SYSLMOD member name for linkedit
func ResolveProgramName(job *types.Job, cfg *types.GraceConfig) string {
	if job.Program != nil {
		return *job.Program
	}
	return ""
}

// --- Dataset Resolvers ---

func ResolveJCLDataset(job *types.Job, cfg *types.GraceConfig) string {
	if job.Datasets != nil && job.Datasets.JCL != "" {
		return job.Datasets.JCL
	}
	return cfg.Datasets.JCL
}

func ResolveSRCDataset(job *types.Job, cfg *types.GraceConfig) string {
	if job.Datasets != nil && job.Datasets.SRC != "" {
		return job.Datasets.SRC
	}
	return cfg.Datasets.SRC
}

func ResolveLoadLib(job *types.Job, cfg *types.GraceConfig) string {
	if job.Datasets != nil && job.Datasets.LoadLib != "" {
		return job.Datasets.LoadLib
	}
	return cfg.Datasets.LoadLib
}
