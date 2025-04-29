package context

import (
	"github.com/graceinfra/grace/internal/log"
	"github.com/graceinfra/grace/types"
)

type ExecutionContext struct {
	Config      *types.GraceConfig
	Logger      *log.GraceLogger
	LogDir      string
	OutputStyle types.OutputStyle // Human / Human verbose / JSON
	SubmitOnly  []string          // Selective job runs
	GraceCmd    string            // "submit", "run"
}
