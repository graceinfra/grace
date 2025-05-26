package jcl

import (
	"fmt"
	"strings"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/paths"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog/log"
)

const (
	DefaultOutputDISP  = "(NEW,CATLG,DELETE)"  // Create, Catalog if OK, Delete if ABEND
	DefaultOutputSpace = "(TRK,(5,5),RLSE)"    // Default space allocation
	DefaultOutputDCB   = "(RECFM=FB,LRECL=80)" // Minimal DCB, blksize determined by system typically
	DefaultInputDISP   = "SHR"                 // Assume input datasets exist
)

// GenerateDDStatements creates the JCL DD statement lines for a job's inputs and outputs
func GenerateDDStatements(job *types.Job, ctx *context.ExecutionContext) (string, error) {
	var ddLines []string
	logCtx := log.With().Str("job", job.Name).Str("workflow_id", ctx.WorkflowId.String()).Logger()

	logCtx.Debug().Msg("Generating DD statements...")

	// --- Process inputs ---

	for _, inputSpec := range job.Inputs {
		logCtx.Debug().Str("dd_name", inputSpec.Name).Str("virtual_path", inputSpec.Path).Msg("Processing input")

		var fileSpecForZosInput *types.FileSpec = nil
		isZosJobTypeForInput := job.Type == "compile" || job.Type == "linkedit" || job.Type == "execute"
		if isZosJobTypeForInput && (strings.HasPrefix(inputSpec.Path, "file://") || strings.HasPrefix(inputSpec.Path, "local-temp://")) {
			fileSpecForZosInput = &inputSpec
		}
		dsn, err := paths.ResolvePath(ctx, job, inputSpec.Path, fileSpecForZosInput)
		if err != nil {
			logCtx.Error().Err(err).Str("virtual_path", inputSpec.Path).Msg("Failed to resolve input path")
			return "", fmt.Errorf("job %q input %q (%s): %w", job.Name, inputSpec.Name, inputSpec.Path, err)
		}

		ddName := strings.ToUpper(inputSpec.Name) // Ensure uppercase DDName
		disp := DefaultInputDISP

		// If this input was originally a file:// or local-temp:// for a z/OS job,
		// it has been uploaded to a temporary DSN. Its DISP should be (OLD,DELETE) or (OLD,KEEP)
		isUploadedTempInput := (strings.HasPrefix(inputSpec.Path, "file://") || strings.HasPrefix(inputSpec.Path, "local-temp://")) &&
			(job.Type == "compile" || job.Type == "linkedit" || job.Type == "execute")

		if isUploadedTempInput {
			if inputSpec.Keep {
				disp = "(OLD,KEEP)"
			} else {
				disp = "(OLD,DELETE)"
			}
		}

		ddLine := fmt.Sprintf("//%-8s DD DSN=%s,DISP=%s", ddName, dsn, disp)
		ddLines = append(ddLines, ddLine)
		logCtx.Debug().Str("dd_name", ddName).Str("dsn", dsn).Msg("Generated input DD")
	}

	// --- Process outputs ---

	for _, outputSpec := range job.Outputs {
		logCtx.Debug().Str("dd_name", outputSpec.Name).Str("virtual_path", outputSpec.Path).Msg("Processing output")

		dsn, err := paths.ResolvePath(ctx, job, outputSpec.Path, nil)
		if err != nil {
			// This shouldn't happen if PreresolveOutputPaths worked, indicates internal error
			logCtx.Error().Err(err).Str("virtual_path", outputSpec.Path).Msg("Failed to resolve output path")
			return "", fmt.Errorf("job %q output %q (%s): %w", job.Name, outputSpec.Name, outputSpec.Path, err)
		}

		ddName := strings.ToUpper(outputSpec.Name)
		disp := DefaultOutputDISP
		space := DefaultOutputSpace
		dcb := DefaultOutputDCB
		// TODO: Handle different DsTypes (PDS, VSAM) later - needs different allocation params/methods

		// Check for user provided values
		if outputSpec.Space != "" {
			space = outputSpec.Space
		}
		if outputSpec.DCB != "" {
			dcb = outputSpec.DCB
		}
		// if outputSpec.Keep { // Modify DISP if Keep=true?
		//     disp = "(NEW,CATLG,CATLG)"
		// }

		// Construct DD statement with allocation parameters
		// Note: Formatting for continuation lines might be needed for very long DCB/SPACE
		line1 := fmt.Sprintf("//%-8s DD DSN=%s,DISP=%s,", ddName, dsn, disp)
		line1 = padLineToContinuation(line1)
		ddLines = append(ddLines, line1)

		line2 := fmt.Sprintf("//             SPACE=%s,", space)
		line2 = padLineToContinuation(line2)
		ddLines = append(ddLines, line2)

		line3 := fmt.Sprintf("//             DCB=%s", dcb)
		// No continuation needed
		ddLines = append(ddLines, line3)

		// Add UNIT=SYSDA or similar? Often defaults ok. Add if needed.
		// ddLine += ",\n//             UNIT=SYSDA"

		logCtx.Debug().Str("dd_name", ddName).Str("dsn", dsn).Msg("Generated output DD")
	}

	return strings.Join(ddLines, "\n"), nil
}

// padLineToContinuation adds spaces to reach column 71 and adds a continuation char in col 72
func padLineToContinuation(line string) string {
	const targetCol = 71
	const contChar = "*"

	currentLen := len(line)
	if currentLen < targetCol {
		padding := strings.Repeat(" ", targetCol-currentLen)
		return line + padding + contChar
	} else if currentLen == targetCol {
		return line + contChar
	} else {
		log.Warn().Str("line_content", line).Msg("JCL line exceeded column 71 before continuation mark could be added")
		return line + contChar
	}
}
