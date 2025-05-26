package jobhandler

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/paths"
	"github.com/graceinfra/grace/internal/zowe"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog"
)

// defaultDCBForUpload determines default DCB attributes for a temporary z/OS dataset
// based on the encoding hint.
func defaultDCBForUpload(encoding string) map[string]string {
	if encoding == "text" {
		return map[string]string{"RECFM": "FB", "LRECL": "80", "BLKSIZE": "27920"}
	}
	return map[string]string{"RECFM": "U", "LRECL": "0", "BLKSIZE": "32760"}
}

// defaultSpaceForTempDS provides default space allocation.
func defaultSpaceForTempDS() map[string]string {
	return map[string]string{"primary": "5"} // Provide primary, unit (TRK) is hardcoded in CreateTempSeqDataset for now
}

// prepareZosJobInputs handles the upload of file:// and local-temp:// inputs to temporary z/OS datasets.
func prepareZosJobInputs(ctx *context.ExecutionContext, job *types.Job, logger zerolog.Logger) error {
	for i := range job.Inputs {
		inputSpec := &job.Inputs[i] // Work with a pointer to the actual FileSpec

		isLocalFileForZosUpload := strings.HasPrefix(inputSpec.Path, "file://") || strings.HasPrefix(inputSpec.Path, "local-temp://")

		if !isLocalFileForZosUpload {
			continue
		}

		jobInputLogger := logger.With().Str("dd_name", inputSpec.Name).Str("virtual_path", inputSpec.Path).Logger()

		// 1. Resolve the local source path
		var localSourcePath string
		if strings.HasPrefix(inputSpec.Path, "file://") {
			resource := strings.TrimPrefix(inputSpec.Path, "file://")
			if filepath.IsAbs(resource) {
				localSourcePath = resource
			} else {
				localSourcePath = filepath.Join(ctx.ConfigDir, resource)
			}
		} else { // local-temp://
			resource := strings.TrimPrefix(inputSpec.Path, "local-temp://")
			if ctx.LocalStageDir == "" {
				return fmt.Errorf("LocalStageDir not set in context for job %s, cannot resolve input %s (%s)", job.Name, inputSpec.Name, inputSpec.Path)
			}
			localSourcePath = filepath.Join(ctx.LocalStageDir, resource)
		}

		// Check if local source file exists before proceeding
		if _, err := os.Stat(localSourcePath); os.IsNotExist(err) {
			jobInputLogger.Error().Err(err).Str("local_source_path", localSourcePath).Msg("Local source file for upload does not exist.")
			return fmt.Errorf("job %s input %s (%s): local source file %s not found for upload: %w", job.Name, inputSpec.Name, inputSpec.Path, localSourcePath, err)
		}
		jobInputLogger.Debug().Str("local_source_path", localSourcePath).Msg("Resolved local source path for upload.")

		// 2. Resolve the temporary z/OS target DSN (dynamically generated)
		tempZosDSN, err := paths.ResolvePath(ctx, job, inputSpec.Path, inputSpec) // Pass actual inputSpec
		if err != nil {
			jobInputLogger.Error().Err(err).Msg("Failed to resolve/generate temporary z/OS DSN for input.")
			return fmt.Errorf("job %s input %s (%s): failed to resolve/generate temporary z/OS DSN: %w", job.Name, inputSpec.Name, inputSpec.Path, err)
		}
		jobInputLogger.Debug().Str("temp_zos_dsn", tempZosDSN).Msg("Resolved temporary z/OS DSN for upload target.")

		// 3. Determine DCB and Space attributes for the temporary z/OS dataset
		dcbAttrs := defaultDCBForUpload(inputSpec.Encoding)
		// TODO: Enhance to parse inputSpec.DCB if provided and merge/override dcbAttrs.
		// For now, inputSpec.DCB on an input is ignored for temp DSN creation.
		if inputSpec.DCB != "" {
			jobInputLogger.Warn().Str("user_dcb_on_input", inputSpec.DCB).Msg("User-provided DCB on input FileSpec is currently ignored for temporary z/OS dataset creation during upload; using defaults based on encoding. This can be enhanced.")
		}
		spaceAttrs := defaultSpaceForTempDS()
		// TODO: Enhance to use inputSpec.Space for temp DSN if provided (e.g. inputSpec.TempSpace)

		// 4. Allocate this temporary z/OS DSN
		jobInputLogger.Info().Msgf("Allocating temporary z/OS dataset %s for input upload...", tempZosDSN)
		err = zowe.CreateTempSeqDataset(ctx, tempZosDSN, dcbAttrs, spaceAttrs)
		if err != nil {
			jobInputLogger.Error().Err(err).Msg("Failed to allocate temporary z/OS dataset.")
			return fmt.Errorf("job %s input %s (%s): failed to allocate temp DSN %s: %w", job.Name, inputSpec.Name, inputSpec.Path, tempZosDSN, err)
		}

		// Actively check for dataset catalog entry before proceeding
		maxRetries := 5
		retryDelay := 2 * time.Second
		foundInCatalog := false
		for r := 0; r < maxRetries; r++ {
			jobInputLogger.Debug().Msgf("Verifying dataset %s is cataloged (attempt %d/%d)...", tempZosDSN, r+1, maxRetries)
			exists, _ := zowe.CheckPDSExists(ctx, tempZosDSN) // CheckPDSExists uses 'list ds', good enough for PS too
			if exists {
				jobInputLogger.Info().Msgf("Dataset %s found in catalog.", tempZosDSN)
				foundInCatalog = true
				break
			}
			jobInputLogger.Debug().Msgf("Dataset %s not yet found in catalog, waiting %v...", tempZosDSN, retryDelay)
			time.Sleep(retryDelay)
		}
		if !foundInCatalog {
			jobInputLogger.Error().Msgf("Dataset %s was not found in catalog after %d retries. Upload might fail.", tempZosDSN, maxRetries)
			// Optionally, could return an error here, but let's see if upload fails with a clearer message.
		}

		// 5. Upload the local file
		jobInputLogger.Info().Msgf("Uploading local file %s to %s...", localSourcePath, tempZosDSN)
		effectiveEncoding := inputSpec.Encoding
		if effectiveEncoding == "" {
			effectiveEncoding = "binary"
		}
		_, uploadErr := zowe.UploadFileToDataset(ctx, localSourcePath, tempZosDSN, effectiveEncoding)
		if uploadErr != nil {
			jobInputLogger.Error().Err(uploadErr).Msg("Failed to upload local file to temporary z/OS dataset.")
			return fmt.Errorf("job %s input %s (%s): failed to upload %s to %s: %w", job.Name, inputSpec.Name, inputSpec.Path, localSourcePath, tempZosDSN, uploadErr)
		}
		jobInputLogger.Info().Msg("✓ Successfully uploaded input to temporary z/OS dataset.")
	}
	return nil
}

// cleanupZosJobOutputs handles the download of file:// and local-temp:// outputs
// from temporary z/OS datasets back to the local filesystem.
func cleanupZosJobOutputs(ctx *context.ExecutionContext, job *types.Job, execRecord *models.JobExecutionRecord, logger zerolog.Logger) error {
	jobSucceeded := false
	if execRecord != nil && execRecord.FinalResponse != nil && execRecord.FinalResponse.Data != nil {
		status := execRecord.FinalResponse.Data.Status
		rc := execRecord.FinalResponse.Data.RetCode
		if status == "OUTPUT" && (rc == nil || *rc == "CC 0000" || *rc == "CC 0004") {
			jobSucceeded = true
		}
	}

	if !jobSucceeded {
		jobId := "UNKNOWN_JOBID"
		if execRecord != nil {
			jobId = execRecord.JobID
		}
		logger.Warn().Str("job_id", jobId).Msg("Job did not succeed or final status unknown. Skipping download of file:// and local-temp:// outputs.")
		// TODO: Add a config option to download outputs even on failure in types.CleanupSettings?
		return nil
	}

	for i := range job.Outputs {
		outputSpec := &job.Outputs[i] // Work with a pointer

		isRemoteFileForZosDownload := strings.HasPrefix(outputSpec.Path, "file://") || strings.HasPrefix(outputSpec.Path, "local-temp://")

		if !isRemoteFileForZosDownload {
			continue
		}

		jobOutputLogger := logger.With().Str("dd_name", outputSpec.Name).Str("virtual_path", outputSpec.Path).Logger()

		// 1. Resolve the temporary z/OS source DSN
		// For outputs, ResolvePath (with nil for 4th arg) gets the DSN from ctx.ResolvedPaths
		tempZosDSN, err := paths.ResolvePath(ctx, job, outputSpec.Path, nil)
		if err != nil {
			jobOutputLogger.Error().Err(err).Msg("Failed to resolve temporary z/OS DSN for output download.")
			continue // Log and continue to other outputs
		}
		jobOutputLogger.Debug().Str("temp_zos_dsn", tempZosDSN).Msg("Resolved temporary z/OS DSN for download source.")

		// 2. Resolve the local target path
		var localTargetPath string
		if strings.HasPrefix(outputSpec.Path, "file://") {
			resource := strings.TrimPrefix(outputSpec.Path, "file://")
			if filepath.IsAbs(resource) {
				localTargetPath = resource
			} else {
				localTargetPath = filepath.Join(ctx.ConfigDir, resource)
			}
			if err := os.MkdirAll(filepath.Dir(localTargetPath), 0755); err != nil {
				jobOutputLogger.Error().Err(err).Str("local_target_path", localTargetPath).Msg("Failed to create parent directories for local target file.")
				continue
			}
		} else { // local-temp://
			resource := strings.TrimPrefix(outputSpec.Path, "local-temp://")
			if ctx.LocalStageDir == "" {
				jobOutputLogger.Error().Msg("LocalStageDir not set in context, cannot determine local target for local-temp:// output.")
				continue
			}
			localTargetPath = filepath.Join(ctx.LocalStageDir, resource)
		}
		jobOutputLogger.Debug().Str("local_target_path", localTargetPath).Msg("Resolved local target path for download.")

		// 3. Download from temporary z/OS DSN
		effectiveEncoding := outputSpec.Encoding
		if effectiveEncoding == "" { // Default to binary for download if not specified
			effectiveEncoding = "binary"
		}
		jobOutputLogger.Info().Str("encoding", effectiveEncoding).Msgf("Downloading temporary z/OS DSN %s to local file %s...", tempZosDSN, localTargetPath)
		err = zowe.DownloadFile(ctx, localTargetPath, tempZosDSN, effectiveEncoding)
		if err != nil {
			jobOutputLogger.Error().Err(err).Msg("Failed to download output from temporary z/OS dataset.")
			continue // Log and continue
		}
		jobOutputLogger.Info().Msg("✓ Successfully downloaded output to local file.")
	}
	return nil
}
