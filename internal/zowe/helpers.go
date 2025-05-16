package zowe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

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
func runZowe(ctx *context.ExecutionContext, args ...string) ([]byte, error) {
	zoweLogger := log.With().
		Str("component", "zowe_cli").
		Str("workflow_id", ctx.WorkflowId.String()).
		Logger()

	zoweLogger.Debug().Strs("args", args).Msg("Running zowe command")

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

type uploadRes struct {
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

func UploadFileToDataset(ctx *context.ExecutionContext, path, member string) (*uploadRes, error) {
	// Delete old member to ensure idempotency
	// 'member' here is the full DSN string, potentially DSN(MEMBER)
	err := DeleteDatasetIfExists(ctx, member)
	if err != nil {
		log.Error().Err(err).Str("component", "zowe_cli").Str("target_dataset", member).Msg("Pre-upload deletion of existing target failed.")
		return nil, fmt.Errorf("failed to delete existing target %s before upload: %w", member, err)
	}

	// Regular runZowe call for uploading file
	out, err := runZowe(ctx, "zos-files", "upload", "file-to-data-set", path, member, "--rfj")
	if err != nil {
		return nil, err
	}

	var uploadRes *uploadRes

	err = json.Unmarshal(out, &uploadRes)
	if err != nil {
		return nil, fmt.Errorf("unexpected API response structure")
	}

	if !uploadRes.Data.Success {
		return nil, fmt.Errorf(uploadRes.Data.Error.Msg)
	}

	return uploadRes, nil
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
		return fmt.Errorf(errMsg)
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
	zoweLogger := log.With().Str("component", "zowe_cli").Str("workflow_id", ctx.WorkflowId.String()).Logger()

	out, _ := runZowe(ctx, "zos-files", "delete", "data-set", dsn, "-fi", "--rfj")

	var deleteRes types.ZoweRfj
	if unmarshalErr := json.Unmarshal(out, &deleteRes); unmarshalErr != nil {
		zoweLogger.Error().Err(unmarshalErr).Str("dsn", dsn).Str("raw_output", string(out)).Msg("Failed to unmarshal Zowe delete response.")
		return fmt.Errorf("failed to parse Zowe delete response for '%s': %w", dsn, unmarshalErr)
	}

	zoweLogger.Debug().Str("dsn", dsn).Msg("Dataset/member deleted successfully or did not exist.")
	return nil
}
