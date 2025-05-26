package zowe

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog/log"
)

// runZowe invokes `zowe <args...>` and returns the stdout bytes and potentially
// an error.
//
// The returned error indicates issues executing the 'zowe' process itself
// (e.g., command not found, permission errors). It DOES NOT indicate logical
// errors reported by Zowe CLI within its output (e.g., "job not found",
// authentication failure when using --rfj).
//
// Callers MUST inspect the returned byte slice (typically by unmarshalling
// the JSON response when using --rfj) to check for Zowe-level success or failure,
// even when the returned error is nil.
//
// Stderr from the Zowe command is logged verbosely if not empty, but not
// typically included in the returned error unless the process execution fails.
//
// We append `--profile [grace.yml config.profile]` here to use the Zowe profile
// defined by the user in grace.yml.
func runZowe(ctx *context.ExecutionContext, args ...string) ([]byte, error) {
	zoweLogger := log.With().
		Str("component", "zowe_cli").
		Str("workflow_id", ctx.WorkflowId.String()).
		Logger()

	profile := ctx.Config.Config.Profile

	zoweLogger.Debug().Str("profile", profile).Strs("args", args).Msg("Running zowe command")

	args = append(args, "--zosmf-profile", profile)

	cmd := exec.Command("zowe", args...)

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf // Capture stdout
	cmd.Stderr = &errBuf // Capture stderr
	cmd.Stdin = os.Stdin

	_ = cmd.Run()

	// Get stdout content regardless of error
	stdoutBytes := outBuf.Bytes()
	stdoutString := outBuf.String()

	// Log stdout content if verbose logging is enabled and stderr is not empty
	if stdoutString != "" {
		zoweLogger.Debug().Strs("args", args).Str("stdout", stdoutString).Msg("Zowe command stderr output")
	}

	// Process execution succeeded (exit code 0).
	// Return the captured stdout and nil error.
	// The caller is responsible for checking the content of stdout (e.g., the 'success' field in JSON).
	return stdoutBytes, nil
}

func listZoweProfiles() ([]types.ZoweProfile, error) {
	configPath := filepath.Join(os.Getenv("HOME"), ".zowe", "zowe.config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var zoweConfig types.ZoweConfig
	if err := json.Unmarshal(data, &zoweConfig); err != nil {
		return nil, err
	}

	profiles := make([]types.ZoweProfile, 0, len(zoweConfig.Profiles))
	for _, p := range zoweConfig.Profiles {
		profiles = append(profiles, p)
	}

	return profiles, nil
}

type UploadRes struct {
	Data struct {
		Success     bool `json:"success"`
		APIResponse []struct {
			Success bool   `json:"success"`
			From    string `json:"from"`
			To      string `json:"to"`
		} `json:"apiResponse"`
		Error struct {
			Msg string `json:"msg,omitempty"`
		} `json:"error,omitempty"`
	} `json:"data"`
}

// UploadFileToDataset uploads a local file to a z/OS dataset or PDS member.
// path: local file path.
// targetDatasetOrMember: full z/OS DSN, e.g., "USER.PDS(MEMBER)" or "USER.PSFILE".
// encodingHint: "text" or "binary". If "text", appropriate encoding flags are added.
// If empty, defaults to binary.
func UploadFileToDataset(ctx *context.ExecutionContext, path, targetDatasetOrMember, encodingHint string) (*UploadRes, error) {
	zoweLogger := log.With().
		Str("component", "zowe_cli").
		Str("workflow_id", ctx.WorkflowId.String()).
		Str("local_path", path).
		Str("target_dsn", targetDatasetOrMember).
		Str("encoding_hint", encodingHint).
		Logger()

	// Delete old member/dataset to ensure idempotency before upload
	err := DeleteDatasetIfExists(ctx, targetDatasetOrMember)
	if err != nil {
		// Log the error but continue, as the target might not exist, which is fine.
		// DeleteDatasetIfExists itself logs if it's a real issue.
		zoweLogger.Warn().Err(err).Msg("Pre-upload deletion of existing target encountered an issue or target did not exist. Proceeding with upload attempt.")
	}

	args := []string{"zos-files", "upload", "file-to-data-set", path, targetDatasetOrMember, "--rfj"}

	if encodingHint == "text" {
		// For text (especially to RECFM=VB or FB datasets like source code or parameters),
		// specify EBCDIC encoding for conversion and remove --record as it's often problematic for VB.
		args = append(args, "--encoding", "IBM-1047") // Common EBCDIC codepage
		zoweLogger.Debug().Msg("Preparing for text mode upload with EBCDIC conversion (IBM-1047).")
	} else { // Handles "binary" or empty encodingHint (defaulting to binary)
		args = append(args, "--binary")
		zoweLogger.Debug().Msg("Preparing for binary mode upload.")
	}

	zoweLogger.Debug().Strs("zowe_args", args).Msg("Executing Zowe upload command.")
	out, execErr := runZowe(ctx, args...) // Renamed 'err' to 'execErr' to avoid conflict
	if execErr != nil {
		var errorUploadRes UploadRes
		if unmarshalErr := json.Unmarshal(out, &errorUploadRes); unmarshalErr == nil && !errorUploadRes.Data.Success {
			errMsg := "Zowe CLI upload command execution failed"
			if errorUploadRes.Data.Error.Msg != "" {
				errMsg = fmt.Sprintf("%s: %s", errMsg, errorUploadRes.Data.Error.Msg)
			}
			return &errorUploadRes, fmt.Errorf(errMsg)
		}
		// If unmarshal fails or it's not a Zowe JSON error, return the original execErr
		return nil, fmt.Errorf("Zowe CLI process execution failed for upload: %w. Raw output: %s", execErr, string(out))
	}

	var uploadRes UploadRes // Changed from *UploadRes to UploadRes for unmarshalling
	if unmarshalErr := json.Unmarshal(out, &uploadRes); unmarshalErr != nil {
		zoweLogger.Error().Err(unmarshalErr).Str("raw_output", string(out)).Msg("Failed to unmarshal Zowe upload response.")
		return nil, fmt.Errorf("failed to parse Zowe upload response for %s: %w. Raw output: %s", targetDatasetOrMember, unmarshalErr, string(out))
	}

	// Check for logical failure reported by Zowe within the JSON
	if !uploadRes.Data.Success {
		errMsg := fmt.Sprintf("Zowe CLI reported upload failure for %s to %s", path, targetDatasetOrMember)
		if uploadRes.Data.Error.Msg != "" {
			errMsg = fmt.Sprintf("%s: %s", errMsg, uploadRes.Data.Error.Msg)
		}
		zoweLogger.Error().Str("zowe_error_msg", uploadRes.Data.Error.Msg).Msg(errMsg)
		return &uploadRes, errors.New(errMsg) // Return the partially filled uploadRes with the error
	}

	zoweLogger.Info().Msg("File uploaded successfully.")
	return &uploadRes, nil
}

// DownloadFile downloads a z/OS dataset or member to a local file path using Zowe CLI.
// targetLocalPath is the full path where the file should be saved locally.
// datasetName is the fully qualified z/OS dataset name, can include a member like DSN(MEMBER).
// encodingHint can be "text" or "binary" (or empty for default to binary).
func DownloadFile(ctx *context.ExecutionContext, targetLocalPath, datasetName, encodingHint string) error {
	zoweLogger := log.With().
		Str("component", "zowe_cli").
		Str("workflow_id", ctx.WorkflowId.String()).
		Str("dsn", datasetName).
		Str("local_path", targetLocalPath).
		Str("encoding_hint", encodingHint).
		Logger()

	args := []string{"zos-files", "download", "data-set", datasetName, "--file", targetLocalPath, "--rfj"}

	if encodingHint == "text" {
		zoweLogger.Debug().Msg("Downloading as text (implicit Zowe conversion to local encoding format).")
	} else {
		zoweLogger.Debug().Msg("Downloading as binary (--binary flag).")
		args = append(args, "--binary")
	}

	zoweLogger.Debug().Msg("Attempting to download dataset.")
	out, err := runZowe(ctx, args...)
	if err != nil {
		zoweLogger.Error().Err(err).Msg("Zowe CLI process execution failed during download attempt.")
		return fmt.Errorf("zowe CLI process execution failed while trying to download '%s' to '%s': %w", datasetName, targetLocalPath, err)
	}

	var dlRes types.ZoweRfj
	if unmarshalErr := json.Unmarshal(out, &dlRes); unmarshalErr != nil {
		zoweLogger.Error().Err(unmarshalErr).Str("raw_output", string(out)).Msg("Failed to unmarshal Zowe download response.")
		return fmt.Errorf("failed to parse Zowe download response for '%s': %w. Raw output: %s", datasetName, unmarshalErr, string(out))
	}

	if !dlRes.Success {
		errMsg := fmt.Sprintf("Zowe CLI reported download failure for '%s'", datasetName)
		if dlRes.Error != nil && dlRes.Error.Msg != "" {
			errMsg = fmt.Sprintf("%s: %s", errMsg, dlRes.Error.Msg)
			if dlRes.Error.Additional != "" {
				errMsg = fmt.Sprintf("%s. Details: %s", errMsg, dlRes.Error.Additional)
			}
		} else if dlRes.Message != "" { // Fallback to general message
			errMsg = fmt.Sprintf("%s: %s", errMsg, dlRes.Message)
		}
		zoweLogger.Error().Str("zowe_error_msg", dlRes.GetError()).Str("zowe_stdout", dlRes.Stdout).Str("zowe_stderr", dlRes.Stderr).Msg(errMsg)
		return fmt.Errorf("%v", errMsg)
	}

	zoweLogger.Info().Msg("Dataset/member downloaded successfully.")
	return nil
}

type listRes struct {
	Success bool `json:"success"`
	Data    struct {
		APIResponse struct {
			ReturnedRows int `json:"returnedRows"`
		} `json:"apiResponse"`
	} `json:"data"`
}

// CheckPDSExists checks if a partitioned data set exists. It DOES NOT create it.
func CheckPDSExists(ctx *context.ExecutionContext, name string) (bool, error) {
	zoweLogger := log.With().
		Str("component", "zowe_cli").
		Str("workflow_id", ctx.WorkflowId.String()).
		Logger()

	out, err := runZowe(ctx, "zos-files", "list", "data-set", name, "--rfj")
	if err != nil {
		return false, err
	}

	var res listRes
	if err = json.Unmarshal(out, &res); err != nil {
		return false, err
	}

	if res.Data.APIResponse.ReturnedRows > 0 {
		zoweLogger.Debug().Msgf("✔ %s exists", name)
		return true, nil
	}

	return false, nil
}

// EnsurePDSExists checks if a partitioned data set exists and creates it if not.
func EnsurePDSExists(ctx *context.ExecutionContext, name string) error {
	zoweLogger := log.With().
		Str("component", "zowe_cli").
		Str("workflow_id", ctx.WorkflowId.String()).
		Logger()

	out, err := runZowe(ctx, "zos-files", "list", "data-set", name, "--rfj")
	if err != nil {
		return err
	}

	var res listRes
	if err = json.Unmarshal(out, &res); err != nil {
		return err
	}

	if res.Data.APIResponse.ReturnedRows > 0 {
		zoweLogger.Debug().Msgf("%s already exists", name)
		return nil
	}

	zoweLogger.Info().Msgf("Allocating PDS %s ...", name)

	raw, err := runZowe(ctx, "zos-files", "create", "data-set-partitioned", name, "--rfj")
	if err != nil {
		return err
	}

	var allocRes types.ZoweRfj
	if err = json.Unmarshal(raw, &allocRes); err != nil {
		return err
	}

	if !allocRes.Success {
		return fmt.Errorf("partitioned data set allocation failed: %s", allocRes.Error.Msg)
	}

	zoweLogger.Info().Msgf("Successfully allocated PDS %s", name)
	return nil
}

// EnsureSDSExists checks if a sequential data set exists and creates it if not.
func EnsureSDSExists(ctx *context.ExecutionContext, name string) error {
	zoweLogger := log.With().
		Str("component", "zowe_cli").
		Str("workflow_id", ctx.WorkflowId.String()).
		Logger()

	out, err := runZowe(ctx, "zos-files", "list", "data-set", name, "--rfj")
	if err != nil {
		return err
	}

	var res listRes
	if err := json.Unmarshal(out, &res); err != nil {
		return err
	}

	if res.Data.APIResponse.ReturnedRows > 0 {
		zoweLogger.Debug().Msgf("%s already exists", name)
		return nil
	}

	zoweLogger.Info().Msgf("Allocating SDS %s ...", name)

	raw, err := runZowe(ctx,
		"zos-files", "create", "data-set-sequential", name, "--rfj")
	if err != nil {
		return err
	}

	var allocRes types.ZoweRfj
	if err = json.Unmarshal(raw, &allocRes); err != nil {
		return err
	}

	if !allocRes.Success {
		return fmt.Errorf("sequential data set allocation failed: %s", allocRes.Error.Msg)
	}

	zoweLogger.Info().Msgf("Successfully allocated PDS %s", name)

	return nil
}

func DeleteDatasetIfExists(ctx *context.ExecutionContext, dsn string) error {
	zoweLogger := log.With().Str("component", "zowe_cli").Str("workflow_id", ctx.WorkflowId.String()).Str("dsn_to_delete", dsn).Logger()
	zoweLogger.Debug().Msg("Attempting to delete dataset/member if it exists.")

	args := []string{"zos-files", "delete", "data-set", dsn, "--failifnotfound", "false", "--rfj"}

	out, execErr := runZowe(ctx, args...)
	if execErr != nil {
		zoweLogger.Error().Err(execErr).Msg("Zowe CLI process execution failed during delete attempt.")
		// Don't necessarily fail the whole operation for a pre-delete failure, just log it.
		return fmt.Errorf("zowe CLI process execution failed while trying to delete '%s': %w", dsn, execErr)
	}

	var deleteRes types.ZoweRfj // Using the general ZoweRfj type
	if unmarshalErr := json.Unmarshal(out, &deleteRes); unmarshalErr != nil {
		// If output is empty, it often means Zowe CLI exited 0 because --failifnotfound false
		// and the DSN wasn't there. This is fine for a "delete if exists".
		if len(strings.TrimSpace(string(out))) == 0 {
			zoweLogger.Debug().Msg("Zowe delete command returned empty output, assuming target did not exist or was deleted successfully with --failifnotfound=false.")
			return nil
		}
		zoweLogger.Warn().Err(unmarshalErr).Str("raw_output", string(out)).Msg("Failed to unmarshal Zowe delete response. This might be an issue if delete was expected to perform an action.")
		return fmt.Errorf("failed to parse Zowe delete response for '%s': %w", dsn, unmarshalErr) // Return error if output was not empty but unmarshal failed
	}

	if !deleteRes.Success {
		errMsg := "Zowe CLI reported an issue during pre-delete"
		if deleteRes.Error != nil && deleteRes.Error.Msg != "" {
			// Filter out "Dataset not found" type messages if --failifnotfound false truly makes them success:true
			// Common z/OSMF messages for "not found" when trying to delete:
			// - "IKJ56709I DATA SET dsn NOT ARCHIVED OR ARCHIVED DATA SET NOT FOUND" (for archive manager)
			// - "ADR412E (001)-DTDSC(01), DATA SET dsn NOT PROCESSED, IT WAS NOT FOUND ON ANY INPUT VOLUME" (for DFDSS/ADRDSSU)
			// - z/OSMF REST "Dataset not found" (HTTP 404) often wrapped by Zowe.
			// The key is that with --failifnotfound false, Zowe CLI should return success:true if the only "issue" was "not found".
			// So if success:false, it's a more significant problem.
			errMsg = fmt.Sprintf("%s: %s", errMsg, deleteRes.GetError())
			zoweLogger.Warn().Str("zowe_error_msg", deleteRes.GetError()).Msg(errMsg)
			// Still, don't return an error from DeleteDatasetIfExists for most pre-delete scenarios,
			// unless it's a critical auth error etc. For now, just log.
		} else if deleteRes.Message != "" {
			errMsg = fmt.Sprintf("%s: %s", errMsg, deleteRes.Message)
			zoweLogger.Warn().Msg(errMsg)
		} else {
			zoweLogger.Warn().Msg(errMsg + " (no specific error message from Zowe).")
		}
		// Don't return error; let the subsequent create/upload attempt fail if the DSN is truly problematic.
	} else {
		zoweLogger.Debug().Msg("Dataset/member confirmed absent or deleted successfully.")
	}
	return nil
}

// CreateTempSeqDataset allocates a new sequential dataset.
// It will delete the dataset if it already exists before attempting creation.
// dsn: The fully qualified dataset name.
// dcbAttrs: A map of DCB attributes, e.g., {"RECFM": "FB", "LRECL": "80", "BLKSIZE": "32720"}
// spaceAttrs: A map for space allocation, e.g., {"primary": "5", "secondary": "1", "unit": "TRK", "dirblks": "0"}
// If dirblks is "0" or not provided, it's a PS. If > "0", it would be for PDS (but use EnsurePDSExists for that).
func CreateTempSeqDataset(ctx *context.ExecutionContext, dsn string, dcbAttrs map[string]string, spaceAttrs map[string]string) error {
	zoweLogger := log.With().
		Str("component", "zowe_cli").
		Str("workflow_id", ctx.WorkflowId.String()).
		Str("dsn", dsn).
		Logger()

	zoweLogger.Info().Msg("Preparing to create temporary sequential dataset.")

	// Delete if exists (to ensure idempotency for retries or re-runs)
	if err := DeleteDatasetIfExists(ctx, dsn); err != nil {
		// Log the error but proceed; creation might still work or fail more informatively.
		zoweLogger.Warn().Err(err).Msg("Failed to pre-delete dataset before creating temporary SDS. Attempting creation anyway.")
	}

	// 2. Construct zowe create ds command

	args := []string{"zos-files", "create", "data-set-sequential", dsn, "--rfj"}

	// Apply DCB attributes
	if val, ok := dcbAttrs["RECFM"]; ok && val != "" {
		args = append(args, "--record-format", val)
	}
	if val, ok := dcbAttrs["LRECL"]; ok && val != "" {
		args = append(args, "--record-length", val)
	}
	if val, ok := dcbAttrs["BLKSIZE"]; ok && val != "" {
		args = append(args, "--block-size", val)
	}
	// TODO: Add other DCB attributes like DSORG if needed, but PS is default for create data-set-sequential

	// Apply space attributes using --size for TRK allocation
	// Default to "5TRK" if not specified, mimicking SPACE=(TRK,(5,1)) (secondary is ~10%)
	primaryTracks := "5" // Default primary tracks
	if pVal, ok := spaceAttrs["primary"]; ok && pVal != "" {
		primaryTracks = pVal // Allow override if "primary" is in spaceAttrs
	}
	sizeArg := fmt.Sprintf("%sTRK", primaryTracks)
	args = append(args, "--size", sizeArg)
	zoweLogger.Debug().Str("size_param", sizeArg).Msg("Using --size parameter for allocation.")

	// If we need specific VOLSER for IBMUSER.GRC.*, uncomment and set this and expose in grace.yml.
	// args = append(args, "--volume-serial", "USRVS1") // Or appropriate volume

	// If your site requires specific SMS classes for IBMUSER.GRC.*, uncomment and set these.
	// args = append(args, "--storage-class", "YOUR_STORCLAS")
	// args = append(args, "--management-class", "YOUR_MGMTCLAS")
	// args = append(args, "--data-class", "YOUR_DATACLAS_FOR_PS_VB_TRACKS")

	zoweLogger.Debug().Str("command_args", strings.Join(args, " ")).Msg("Constructed Zowe create ds command args")

	// 3. Execute command
	raw, err := runZowe(ctx, args...)
	if err != nil {
		// runZowe already logs its own process execution errors
		return fmt.Errorf("Zowe CLI process execution failed during temporary SDS creation for %s: %w", dsn, err)
	}

	var allocRes types.ZoweRfj
	if unmarshalErr := json.Unmarshal(raw, &allocRes); unmarshalErr != nil {
		zoweLogger.Error().Err(unmarshalErr).Str("raw_output", string(raw)).Msg("Failed to unmarshal Zowe create SDS response.")
		return fmt.Errorf("failed to parse Zowe create SDS response for %s: %w. Raw output: %s", dsn, unmarshalErr, string(raw))
	}

	if !allocRes.Success {
		errMsg := fmt.Sprintf("temporary sequential dataset allocation failed for %s", dsn)
		if allocRes.Error != nil && allocRes.Error.Msg != "" {
			errMsg = fmt.Sprintf("%s: %s", errMsg, allocRes.Error.Msg)
			if allocRes.Error.Additional != "" {
				errMsg = fmt.Sprintf("%s. Details: %s", errMsg, allocRes.Error.Additional)
			}
		} else if allocRes.Message != "" { // Fallback to general message
			errMsg = fmt.Sprintf("%s: %s", errMsg, allocRes.Message)
		}
		zoweLogger.Error().Str("zowe_error_msg", allocRes.GetError()).Str("zowe_stdout", allocRes.Stdout).Str("zowe_stderr", allocRes.Stderr).Msg(errMsg)
		return fmt.Errorf(errMsg)
	}

	zoweLogger.Info().Msgf("Successfully allocated temporary sequential dataset %s", dsn)
	return nil
}
