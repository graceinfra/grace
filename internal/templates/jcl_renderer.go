package templates

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/jcl"
	"github.com/graceinfra/grace/internal/paths"
	"github.com/graceinfra/grace/internal/resolver"
	"github.com/graceinfra/grace/types"
)

type JCLTemplateData struct {
	JobName         string
	WorkflowId      string // Can be empty if not in a full workflow context (e.g., just 'deck')
	Type            string // compile, linkedit, execute
	ProgramName     string // PGM= for execute, member name for linkedit output
	LoadLib         string
	CompilerPgm     string
	CompilerParms   string
	CompilerSteplib string
	LinkerPgm       string
	LinkerParms     string
	LinkerSteplib   string

	// Option 1: Pre-generated block of DDs
	DDStatements string

	// Option 2: Granular DD info
	Inputs  []ResolvedDD
	Outputs []ResolvedDD
}

type ResolvedDD struct {
	Name string // DDName
	DSN  string // Resolved DSN
	DISP string // Calculated DISP

	// For Outputs, add Space, DCB, etc.
	Space    string
	DCB      string
	IsOutput bool
}

// PrepareJCLTemplateData gathers all necessary data for rendering a JCL template.
func PrepareJCLTemplateData(ctx *context.ExecutionContext, job *types.Job) (*JCLTemplateData, error) {
	programName := resolver.ResolveProgramName(job, ctx.Config)
	loadLib := resolver.ResolveLoadLib(job, ctx.Config)
	compilerPgm := resolver.ResolveCompilerPgm(job, ctx.Config)
	compilerParms := resolver.ResolveCompilerParms(job, ctx.Config)
	compilerSteplib := resolver.ResolveCompilerSteplib(job, ctx.Config)
	linkerPgm := resolver.ResolveLinkerPgm(job, ctx.Config)
	linkerParms := resolver.ResolveLinkerParms(job, ctx.Config)
	linkerSteplib := resolver.ResolveLinkerSteplib(job, ctx.Config)

	// Generate DDStatements block
	ddStatementsBlock, err := jcl.GenerateDDStatements(job, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to generate DD statements for job %s template: %w", job.Name, err)
	}

	// --- Populate granular DD info ---

	var templateInputs []ResolvedDD
	var templateOutputs []ResolvedDD

	// Process inputs
	for _, inputSpec := range job.Inputs {
		var fileSpecForZosInput *types.FileSpec = nil
		isZosJobTypeForInput := job.Type == "compile" || job.Type == "linkedit" || job.Type == "execute"
		if isZosJobTypeForInput && (strings.HasPrefix(inputSpec.Path, "file://") || strings.HasPrefix(inputSpec.Path, "local-temp://")) {
			fileSpecForZosInput = &inputSpec
		}
		resolvedDsn, err := paths.ResolvePath(ctx, job, inputSpec.Path, fileSpecForZosInput)
		if err != nil {
			return nil, fmt.Errorf("job %s: failed to resolve input path %s for DD %s during template data preparation: %w", job.Name, inputSpec.Path, inputSpec.Name, err)
		}

		disp := jcl.DefaultInputDISP
		if inputSpec.Disp != "" {
			disp = inputSpec.Disp
		}

		templateInputs = append(templateInputs, ResolvedDD{
			Name: strings.ToUpper(inputSpec.Name),
			DSN:  resolvedDsn,
			DISP: disp,
		})
	}

	// Process outputs
	for _, outputSpec := range job.Outputs {
		resolvedDsn, err := paths.ResolvePath(ctx, job, outputSpec.Path, nil)
		if err != nil {
			// This error indicates a problem with PreresolveOutputPaths or an unexpected output path.
			return nil, fmt.Errorf("job %s: failed to resolve output path %s for DD %s during template data preparation: %w", job.Name, outputSpec.Path, outputSpec.Name, err)
		}

		disp := jcl.DefaultOutputDISP
		space := jcl.DefaultOutputSpace
		dcb := jcl.DefaultOutputDCB

		if outputSpec.Space != "" {
			space = outputSpec.Space
		}
		if outputSpec.DCB != "" {
			dcb = outputSpec.DCB
		}
		if outputSpec.Keep {
			disp = "(NEW,CATLG,CATLG)"
		}

		templateOutputs = append(templateOutputs, ResolvedDD{
			Name:     strings.ToUpper(outputSpec.Name),
			DSN:      resolvedDsn,
			DISP:     disp,
			Space:    space,
			DCB:      dcb,
			IsOutput: true,
		})
	}

	workflowIdStr := ""
	if ctx.WorkflowId != uuid.Nil && !bytes.Equal(ctx.WorkflowId[:], make([]byte, 16)) { // Check for nil or zero UUID
		workflowIdStr = ctx.WorkflowId.String()
	}

	data := &JCLTemplateData{
		JobName:         job.Name,
		WorkflowId:      workflowIdStr,
		Type:            job.Type,
		ProgramName:     programName,
		LoadLib:         loadLib,
		CompilerPgm:     compilerPgm,
		CompilerParms:   compilerParms,
		CompilerSteplib: compilerSteplib,
		LinkerPgm:       linkerPgm,
		LinkerParms:     linkerParms,
		LinkerSteplib:   linkerSteplib,
		DDStatements:    ddStatementsBlock,
		Inputs:          templateInputs,
		Outputs:         templateOutputs,
	}
	return data, nil
}

// RenderJCL takes template content (either from Grace's FS or user file)
// and data, then renders it.
func RenderJCL(templateNameForErrorMsg string, templateContent string, data *JCLTemplateData) (string, error) {
	tpl := template.New(templateNameForErrorMsg) // Use a descriptive name for errors
	tpl = tpl.Funcs(template.FuncMap{
		"ToUpper": strings.ToUpper,
		"Default": func(defVal interface{}, givenVal ...interface{}) interface{} {
			if len(givenVal) > 0 && givenVal[0] != nil {
				// Check if it's an empty string, if so, still use default
				s, ok := givenVal[0].(string)
				if ok && s != "" {
					return givenVal[0]
				}
				if !ok { // If not a string, and not nil, consider it "given"
					return givenVal[0]
				}
			}
			return defVal
		},
	})

	parsedTpl, err := tpl.Parse(templateContent)
	if err != nil {
		return "", fmt.Errorf("failed to parse JCL template '%s': %w", templateNameForErrorMsg, err)
	}

	var renderedJCL bytes.Buffer
	if err := parsedTpl.Execute(&renderedJCL, data); err != nil {
		return "", fmt.Errorf("failed to execute JCL template '%s': %w", templateNameForErrorMsg, err)
	}

	return renderedJCL.String(), nil
}
