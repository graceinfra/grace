package jobhandler

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/paths"
	"github.com/graceinfra/grace/internal/zowe"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog"
)

type ShellHandler struct{}

func (h *ShellHandler) Type() string {
	return "shell"
}

func (h *ShellHandler) Validate(job *types.Job, cfg *types.GraceConfig) []string {
	var errs []string
	jobCtx := fmt.Sprintf("job[%s] (type: %s)", job.Name, h.Type())

	if job.ShellWith == nil {
		errs = append(errs, fmt.Sprintf("%s: 'with' block defining script/inline is required", jobCtx))
		return errs // Early return - other checks rely on ShellWith being non-nil
	}

	hasScript := job.ShellWith.Script != ""
	hasInlineScript := job.ShellWith.InlineScript != ""

	if !hasScript && !hasInlineScript {
		errs = append(errs, fmt.Sprintf("%s: either 'with.script' or 'with.inline' must be specified", jobCtx))
	} else if hasScript && hasInlineScript {
		errs = append(errs, fmt.Sprintf("%s: 'with.script' and 'with.inline' cannot both be specified", jobCtx))
	}

	if job.Program != nil && *job.Program != "" {
		errs = append(errs, fmt.Sprintf("%s: 'program' field must not be set for 'shell' jobs", jobCtx))
	}

	return errs
}

func (h *ShellHandler) Prepare(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) error {
	logger.Debug().Msg("Prepare step for ShellHandler")

	// Ensure local staging directory exists
	if ctx.LocalStageDir == "" {
		// This should ideally be set up by the executor/orchestrator before calling handlers
		return fmt.Errorf("LocalStageDir is not set in ExecutionContext for job %s", job.Name)
	}
	if err := os.MkdirAll(ctx.LocalStageDir, 0755); err != nil {
		return fmt.Errorf("failed to create local staging directory %s for job %s: %w", ctx.LocalStageDir, job.Name, err)
	}
	logger.Debug().Str("local_stage_dir", ctx.LocalStageDir).Msg("Ensured local staging directory exists.")

	for _, outputSpec := range job.Outputs {
		if strings.HasPrefix(outputSpec.Path, "local-temp://") && !outputSpec.Keep {
			localFileName := filepath.Base(outputSpec.Path)
			localStagePath := filepath.Join(ctx.LocalStageDir, localFileName)

			if _, err := os.Stat(localStagePath); err == nil {
				logger.Info().Str("local_path", localStagePath).Msgf("Pre-deleting existing local stage file for output '%s'", outputSpec.Name)
				if err := os.Remove(localStagePath); err != nil {
					logger.Warn().Err(err).Str("local_path", localStagePath).Msg("Failed to pre-delete local staged output file.")
				}
			}
		}
	}

	return nil
}

func (h *ShellHandler) Execute(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) *models.JobExecutionRecord {
	logger.Info().Msg("Execution ShellHandler")
	startTime := time.Now()

	// Unlike z/OS jobs, shell jobs don't have a mainframe JobID from submission.
	// We can use a UUID part
	shellJobId := fmt.Sprintf("shell-%s", uuid.New().String()[:8])

	hlq := ""
	if ctx.Config.Datasets.JCL != "" {
		parts := strings.Split(ctx.Config.Datasets.JCL, ".")
		if len(parts) > 0 {
			hlq = parts[0]
		}
	}

	host, _ := os.Hostname()

	record := &models.JobExecutionRecord{
		JobName:     job.Name,
		JobID:       shellJobId,
		Type:        h.Type(),
		GraceCmd:    ctx.GraceCmd,
		ZoweProfile: ctx.Config.Config.Profile,
		HLQ:         hlq,
		Initiator: types.Initiator{
			Type:   "user",
			Id:     os.Getenv("USER"),
			Tenant: host,
		},
		WorkflowId: ctx.WorkflowId,
		SubmitTime: startTime.Format(time.RFC3339),
	}

	// --- Data transfer (inputs) ---

	envVars := os.Environ()
	cleanupTempFiles := []string{}
	defer func() {
		for _, f := range cleanupTempFiles {
			os.Remove(f)
		}
	}()

	for _, inputSpec := range job.Inputs {
		virtualPath := inputSpec.Path
		ddName := strings.ToUpper(inputSpec.Name)
		encoding := inputSpec.Encoding

		physicalPath, err := paths.ResolvePath(ctx, job, inputSpec.Path)
		if err != nil {
			logger.Error().Err(err).Str("virtual_path", inputSpec.Path).Msg("Failed to resolve input path for shell job.")
			record.FinishTime = time.Now().Format(time.RFC3339)
			record.DurationMs = time.Since(startTime).Milliseconds()
			// Simulate a Zowe-like error structure for consistency in summary
			// TODO: eventually interface or more general struct in summaries
			record.SubmitResponse = &types.ZoweRfj{Success: false, Error: &types.ZoweRfjError{Msg: fmt.Sprintf("Input path resolution failed: %s", err.Error())}}
			return record
		}

		localStagePathForEnv := physicalPath // Default for local-temp:// or file://

		if strings.HasPrefix(virtualPath, "zos://") {
			resource := strings.TrimPrefix(virtualPath, "zos://") // e.g., "IBMUSER.DATA(MEMBER)" or "IBMUSER.PSFILE"
			dsnNameParts := strings.Split(resource, "(")
			var derivedLocalName string
			if len(dsnNameParts) > 1 { // DSN(MEMBER) format
				derivedLocalName = strings.TrimSuffix(dsnNameParts[1], ")")
			} else { // PS or PDS root
				qualifiers := strings.Split(dsnNameParts[0], ".")
				derivedLocalName = qualifiers[len(qualifiers)-1]
			}
			localStagePathForEnv = filepath.Join(ctx.LocalStageDir, derivedLocalName) // Actual download target

			logger.Info().Str("dsn", physicalPath).Str("local_path", localStagePathForEnv).Msgf("Downloading input %s for shell job", ddName)
			if dlErr := zowe.DownloadFile(ctx, localStagePathForEnv, physicalPath, encoding); dlErr != nil {
				logger.Error().Err(dlErr).Msg("Failed to download input")
				record.FinishTime = time.Now().Format(time.RFC3339)
				record.DurationMs = time.Since(startTime).Milliseconds()
				return record
			}
			logger.Debug().Str("dsn", physicalPath).Str("local_path", localStagePathForEnv).Msg("Successfully downloaded zos:// input")

		} else if strings.HasPrefix(virtualPath, "zos-temp://") {
			// For zos-temp, physicalPath is the DSN. Using DDNAME for local file name for simplicity and uniqueness in stage
			derivedLocalName := ddName
			localStagePathForEnv = filepath.Join(ctx.LocalStageDir, derivedLocalName) // Actual download target

			logger.Info().Str("dsn", physicalPath).Str("local_path", localStagePathForEnv).Msgf("Downloading input %s for shell job", ddName)
			if dlErr := zowe.DownloadFile(ctx, localStagePathForEnv, physicalPath, encoding); dlErr != nil {
				logger.Error().Err(dlErr).Msg("Failed to download input")
				record.FinishTime = time.Now().Format(time.RFC3339)
				record.DurationMs = time.Since(startTime).Milliseconds()
				return record
			}
			logger.Debug().Str("dsn", physicalPath).Str("local_path", localStagePathForEnv).Msg("Successfully downloaded zos-temp:// input")

		} else if strings.HasPrefix(virtualPath, "local-temp://") {
			logger.Debug().Str("local_path", localStagePathForEnv).Msgf("Using local-temp input %s", ddName)
		}

		envVarName := fmt.Sprintf("GRACE_INPUT_%s", strings.ToUpper(inputSpec.Name))
		envVars = append(envVars, fmt.Sprintf("%s=%s", envVarName, localStagePathForEnv))
		logger.Debug().Str("env_var", envVarName).Str("value", localStagePathForEnv).Msg("Set input environment variable")
	}

	for _, outputSpec := range job.Outputs {
		virtualPath := outputSpec.Path
		ddName := strings.ToUpper(outputSpec.Name)
		var localPathForEnv string

		// Resolve the path first to know its absolute/final form
		resolvedPath, err := paths.ResolvePath(ctx, job, virtualPath)
		if err != nil {
			logger.Error().Err(err).Str("virtual_path", virtualPath).Msg("Failed to resolve output path for setting env var.")

			// This is a pre-script-execution setup failure, we should fail the job
			record.FinishTime = time.Now().Format(time.RFC3339)
			record.DurationMs = time.Since(startTime).Milliseconds()
			record.SubmitResponse = &types.ZoweRfj{Success: false, Error: &types.ZoweRfjError{Msg: fmt.Sprintf("Output path resolution failed for env setup: %s", err.Error())}}
			return record
		}

		if strings.HasPrefix(virtualPath, "zos://") || strings.HasPrefix(virtualPath, "zos-temp://") || strings.HasPrefix(virtualPath, "local-temp://") {
			// These types are staged in LocalStageDir. Script writes to LocalStageDir/derivedLocalName
			var derivedLocalName string

			if strings.HasPrefix(virtualPath, "zos://") {
				resource := strings.TrimPrefix(virtualPath, "zos://")
				dsnNameParts := strings.Split(resource, "(")
				if len(dsnNameParts) > 1 {
					derivedLocalName = strings.TrimSuffix(dsnNameParts[1], ")")
				} else {
					qualifiers := strings.Split(dsnNameParts[0], ".")
					derivedLocalName = qualifiers[len(qualifiers)-1]
				}
			} else if strings.HasPrefix(virtualPath, "zos-temp://") {
				derivedLocalName = ddName
			} else { // local-temp://
				derivedLocalName = strings.TrimPrefix(virtualPath, "local-temp://")
			}
			localPathForEnv = filepath.Join(ctx.LocalStageDir, derivedLocalName)
		} else if strings.HasPrefix(virtualPath, "file://") {
			localPathForEnv = resolvedPath
		} else {
			logger.Warn().Str("virtual_path", virtualPath).Msgf("Shell output path %s has an unsupported scheme for GRACE_OUTPUT_ environment variable setting.", virtualPath)
			continue
		}

		envVarName := fmt.Sprintf("GRACE_OUTPUT_%s", ddName)
		envVars = append(envVars, fmt.Sprintf("%s=%s", envVarName, localPathForEnv))
		logger.Debug().Str("env_var", envVarName).Str("value", localPathForEnv).Msg("Set output environment variable")
	}

	// --- Script execution ---

	var scriptPath string
	var shellCmd *exec.Cmd

	shellToUse := job.ShellWith.Shell
	if shellToUse == "" {
		shellToUse = "sh" // Default shell
	}

	if job.ShellWith.InlineScript != "" {
		// Create a temporary file for the inline script
		tmpFile, err := os.CreateTemp(ctx.LocalStageDir, "grace-inline-script-*.sh")
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create temporary file for inline script.")
			record.FinishTime = time.Now().Format(time.RFC3339)
			record.DurationMs = time.Since(startTime).Milliseconds()
			record.SubmitResponse = &types.ZoweRfj{Success: false, Error: &types.ZoweRfjError{Msg: "Failed to create temp script file"}}
			return record
		}
		scriptPath = tmpFile.Name()
		if _, err := tmpFile.WriteString(job.ShellWith.InlineScript); err != nil {
			tmpFile.Close()
			os.Remove(scriptPath)
			logger.Error().Err(err).Msg("Failed to write to temporary inline script file.")
			record.FinishTime = time.Now().Format(time.RFC3339)
			record.DurationMs = time.Since(startTime).Milliseconds()
			record.SubmitResponse = &types.ZoweRfj{Success: false, Error: &types.ZoweRfjError{Msg: "Failed to write to temp script file"}}
			return record
		}
		tmpFile.Close()
		os.Chmod(scriptPath, 0755)
		cleanupTempFiles = append(cleanupTempFiles, scriptPath)
		shellCmd = exec.Command(shellToUse, scriptPath)
	} else {
		// job.ShellWith.Script is used
		var err error
		scriptPath, err = paths.ResolvePath(ctx, job, job.ShellWith.Script) // Resolve if it's src://path/to/script.sh
		if err != nil {
			logger.Error().Err(err).Str("script_path", job.ShellWith.Script).Msg("Failed to resolve script path.")
			record.FinishTime = time.Now().Format(time.RFC3339)
			record.DurationMs = time.Since(startTime).Milliseconds()
			record.SubmitResponse = &types.ZoweRfj{Success: false, Error: &types.ZoweRfjError{Msg: fmt.Sprintf("Failed to resolve script path: %s", err.Error())}}
			return record
		}

		// If scriptPath was src://, it needs to be downloaded first to localStageDir
		if strings.HasPrefix(job.ShellWith.Script, "src://") {
			localScriptFileName := filepath.Base(scriptPath) // This is actually the DSN member name from ResolvePath
			actualLocalScriptPath := filepath.Join(ctx.LocalStageDir, localScriptFileName)
			logger.Info().Str("dsn_member", scriptPath).Str("local_path", actualLocalScriptPath).Msg("Downloading script from src:// for shell job")

			// Scripts are almost always text, so we can default to text download encoding
			err := zowe.DownloadFile(ctx, actualLocalScriptPath, scriptPath, "text")
			if err != nil {
				logger.Error().Err(err).Str("remote_script_path", scriptPath).Msg("Failed to download script")
				record.FinishTime = time.Now().Format(time.RFC3339)
				record.DurationMs = time.Since(startTime).Milliseconds()
				return record
			}

			// Use the downloaded local path if it was downloaded to temp
			scriptPath = actualLocalScriptPath
			cleanupTempFiles = append(cleanupTempFiles, scriptPath)
		}
		shellCmd = exec.Command(shellToUse, scriptPath)
	}
	logger.Info().Str("script", scriptPath).Str("shell", shellToUse).Msg("Running shell script")

	var stdout, stderr strings.Builder
	shellCmd.Stdout = &stdout
	shellCmd.Stderr = &stderr
	shellCmd.Env = envVars
	shellCmd.Dir = ctx.ConfigDir // Run script from the directory of grace.yml

	err := shellCmd.Run()

	record.SubmitResponse = &types.ZoweRfj{
		Data: &types.ZoweRfjData{
			JobName: job.Name,
			JobID:   shellJobId,
		},
	}

	if err != nil {
		logger.Error().Err(err).Str("stderr", stderr.String()).Msg("Shell script execution failed")
		record.SubmitResponse.Success = false
		record.SubmitResponse.Error = &types.ZoweRfjError{Msg: fmt.Sprintf("Script error: %s. Stderr: %s", err.Error(), stderr.String())}
		record.SubmitResponse.Data.Status = "FAILED_SCRIPT"
		if exitErr, ok := err.(*exec.ExitError); ok {
			record.SubmitResponse.Data.RetCode = new(string)
			*record.SubmitResponse.Data.RetCode = fmt.Sprintf("EXIT %d", exitErr.ExitCode())
		}
	} else {
		logger.Info().Msg("Shell script executed successfully")
		record.SubmitResponse.Success = true
		record.SubmitResponse.Data.Status = "OUTPUT"
		record.SubmitResponse.Data.RetCode = new(string)
		*record.SubmitResponse.Data.RetCode = "CC 0000"
	}

	// Store stdout/stderr in the record, perhaps in a custom field if ZoweRfj isn't suitable
	// For now, let's put it in the general message if there's an error, or log it.
	// If we add specific fields to JobExecutionRecord for stdout/stderr, use those.
	// Let's assume FinalResponse can store this for shell jobs for now.
	record.FinalResponse = &types.ZoweRfj{ // Using ZoweRfj to store shell output for now
		Success: record.SubmitResponse.Success,
		Stdout:  stdout.String(),
		Stderr:  stderr.String(),
		Data:    record.SubmitResponse.Data,
		Error:   record.SubmitResponse.Error,
	}

	// --- Data transfer (outputs) ---
	if err == nil { // Only upload if script was successful
		for _, outputSpec := range job.Outputs {
			virtualPath := outputSpec.Path
			ddName := strings.ToUpper(outputSpec.Name)

			// Determine the local staged file path (where the script wrote the output)
			var localStagePath string
			var derivedLocalNameForUpload string

			if strings.HasPrefix(virtualPath, "zos://") {
				resource := strings.TrimPrefix(virtualPath, "zos://")
				dsnNameParts := strings.Split(resource, "(")
				if len(dsnNameParts) > 1 {
					derivedLocalNameForUpload = strings.TrimSuffix(dsnNameParts[1], ")")
				} else {
					qualifiers := strings.Split(dsnNameParts[0], ".")
					derivedLocalNameForUpload = qualifiers[len(qualifiers)-1]
				}
				localStagePath = filepath.Join(ctx.LocalStageDir, derivedLocalNameForUpload)
			} else if strings.HasPrefix(virtualPath, "zos-temp://") {
				derivedLocalNameForUpload = ddName
				localStagePath = filepath.Join(ctx.LocalStageDir, derivedLocalNameForUpload)
			} else if strings.HasPrefix(virtualPath, "local-temp://") {
				// No upload needed for local-temp://, it's already in its final (temporary) local place.
				logger.Debug().Str("virtual_path", virtualPath).Msg("Output is local-temp, no upload action by ShellHandler.")
				continue
			} else {
				logger.Warn().Str("virtual_path", virtualPath).Msgf("Shell output path %s has an unsupported scheme for upload by ShellHandler.", virtualPath)
				continue
			}

			// Check if the output file exists
			if _, statErr := os.Stat(localStagePath); os.IsNotExist(statErr) {
				logger.Warn().Str("local_path", localStagePath).Msgf("Output file for %s (expected at %s) not found locally after script execution. Skipping upload.", ddName, localStagePath)
				continue
			}

			// For zos:// and zos-temp://, upload the file
			if strings.HasPrefix(virtualPath, "zos://") || strings.HasPrefix(virtualPath, "zos-temp://") {
				// Resolve the target physical path on z/OS
				physicalPathToUpload, resolveErr := paths.ResolvePath(ctx, job, virtualPath)
				if resolveErr != nil {
					logger.Error().Err(resolveErr).Str("virtual_path", virtualPath).Msgf("Failed to resolve output path for upload. Job will be marked as failed.")
					record.SubmitResponse.Success = false
					record.SubmitResponse.Error = &types.ZoweRfjError{Msg: fmt.Sprintf("Output path resolution failed for upload: %s", resolveErr.Error())}
					record.SubmitResponse.Data.Status = "FAILED_UPLOAD_RESOLVE"
					record.FinalResponse = record.SubmitResponse
					break
				}

				logger.Info().Str("local_path", localStagePath).Str("dsn", physicalPathToUpload).Msgf("Uploading output %s for shell job", ddName)
				_, uploadErr := zowe.UploadFileToDataset(ctx, localStagePath, physicalPathToUpload)
				if uploadErr != nil {
					logger.Error().Err(uploadErr).Str("local_path", localStagePath).Str("dsn", physicalPathToUpload).Msgf("Failed to upload output %s. Job will be marked as failed.", ddName)
					record.SubmitResponse.Success = false
					record.SubmitResponse.Error = &types.ZoweRfjError{Msg: fmt.Sprintf("Output upload failed: %s", uploadErr.Error())}
					record.SubmitResponse.Data.Status = "FAILED_UPLOAD"
					record.FinalResponse = record.SubmitResponse
					break
				}
			}
		}
	}

	record.FinishTime = time.Now().Format(time.RFC3339)
	record.DurationMs = time.Since(startTime).Milliseconds()
	return record
}

func (h *ShellHandler) Cleanup(ctx *context.ExecutionContext, job *types.Job, execRecord *models.JobExecutionRecord, logger zerolog.Logger) error {
	logger.Debug().Msg("Cleanup step for ShellHandler")
	// Clean up temporary inline script file if one was created.
	// The defer in Execute handles this now.

	// Optionally, clean up local stage files for this job if not kept.
	// However, the orchestrator's generic cleanup of LocalStageDir might be sufficient,
	// or specific 'keep: false' logic for local files could be added here.
	// For now, let's assume LocalStageDir is cleaned globally or files are kept.
	return nil
}
