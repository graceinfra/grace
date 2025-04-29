package zowe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/types"
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
	cmd := exec.Command("zowe", args...)

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf // Capture stdout
	cmd.Stderr = &errBuf // Capture stderr
	cmd.Stdin = os.Stdin

	ctx.Logger.Verbose("Running: zowe %v", strings.Join(args, " "))

	err := cmd.Run()

	// Get stdout and stderr content regardless of error
	stdoutBytes := outBuf.Bytes()
	stderrString := errBuf.String()

	// Log stderr content if verbose logging is enabled and stderr is not empty
	if stderrString != "" {
		ctx.Logger.Verbose("stderr from 'zowe %s':\n%s", strings.Join(args, " "), stderrString)
	}

	// Handle process execution errors (e.g., command not found, non-zero exit code)
	if err != nil {
		// Process execution failed. Return stdout (it might still contain partial JSON/info)
		// and a comprehensive error including the original error and stderr.
		return stdoutBytes, fmt.Errorf("zowe process execution failed for 'zowe %s': %w\nstderr:\n%s",
			strings.Join(args, " "),
			err,
			stderrString) // Include stderr in the error message only on process failure
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
	// Delete old member
	// We manually call os/exec here instead of using the runZowe helper
	// so that we can ignore dataset deletion error if the member doesn't exist.
	//
	// We do this because there is no easy way to simply overwrite a data set if it exists,
	// so we delete and reupload to maintain idempotency.
	quotedMember := `"` + member + `"`
	ctx.Logger.Verbose("Running: zowe zos-files delete data-set %s -f --rfj", quotedMember)

	var silencedBuf bytes.Buffer
	cmd := exec.Command("zowe", "zos-files", "delete", "data-set", quotedMember, "-f", "--rfj")
	cmd.Stdout = &silencedBuf
	cmd.Stderr = &silencedBuf
	_ = cmd.Run()

	// Regular runZowe call for uploading file
	out, err := runZowe(ctx, "zos-files", "upload", "file-to-data-set", path, member, "--rfj")
	if err != nil {
		return nil, err
	}

	var uploadRes *uploadRes

	err = json.Unmarshal(out, &uploadRes)
	if err != nil {
		return nil, fmt.Errorf("Unexpected API response structure")
	}

	if !uploadRes.Data.Success {
		return nil, fmt.Errorf(uploadRes.Data.Error.Msg)
	}

	return uploadRes, nil
}

type listRes struct {
	Success bool `json:"success"`
	Data    struct {
		APIResponse struct {
			ReturnedRows int `json:"returnedRows"`
		} `json:"apiResponse"`
	} `json:"data"`
}

// EnsurePDSExists checks if a partitioned data set exists and creates it if not.
func EnsurePDSExists(ctx *context.ExecutionContext, name string) error {
	quotedName := `"` + name + `"`

	out, err := runZowe(ctx, "zos-files", "list", "data-set", quotedName, "--rfj")
	if err != nil {
		return err
	}

	var res listRes
	if err = json.Unmarshal(out, &res); err != nil {
		return err
	}

	if res.Data.APIResponse.ReturnedRows > 0 {
		ctx.Logger.Verbose("%s already exists", name)
		return nil
	}

	ctx.Logger.Info("Allocating PDS %s ...", name)

	raw, err := runZowe(ctx, "zos-files", "create", "data-set-partitioned", name, "--rfj")
	if err != nil {
		return err
	}

	var allocRes types.ZoweRfj
	if err = json.Unmarshal(raw, &allocRes); err != nil {
		return err
	}

	if !allocRes.Success {
		return fmt.Errorf("Partitioned data set allocation failed: %s", allocRes.Error.Msg)
	}

	ctx.Logger.Info("Successfully allocated PDS %s", name)
	return nil
}

// EnsureSDSExists checks if a sequential data set exists and creates it if not.
func EnsureSDSExists(ctx *context.ExecutionContext, name string) error {
	quotedName := `"` + name + `"`

	out, err := runZowe(ctx, "zos-files", "list", "data-set", quotedName, "--rfj")
	if err != nil {
		return err
	}

	var res listRes
	if err := json.Unmarshal(out, &res); err != nil {
		return err
	}

	if res.Data.APIResponse.ReturnedRows > 0 {
		ctx.Logger.Verbose("%s already exists", name)
		return nil
	}

	ctx.Logger.Info("Allocating SDS %s ...", name)

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
		return fmt.Errorf("Partitioned data set allocation failed: %s", allocRes.Error.Msg)
	}

	ctx.Logger.Info("Successfully allocated PDS %s", name)

	return nil
}
