package zowe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
	"github.com/graceinfra/grace/types"
)

// runZowe invokes `zowe <args...>` under the hood and
// returns stdout bytes (for parsing) or an error.
func runZowe(ctx *context.ExecutionContext, args ...string) ([]byte, error) {
	cmd := exec.Command("zowe", args...)

	var outBuf, errBuf bytes.Buffer
	cmd.Stdin = os.Stdin

	ctx.Logger.Verbose("Running: zowe %v", args)

	// Pipe stdout/stderr to a buffer instead of immediately displaying it
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err := cmd.Run()
	if err != nil {
		return outBuf.Bytes(), fmt.Errorf("\nzowe %v failed: %w\n%s\n%s", args, err, errBuf.String(), outBuf.String())
	}
	return outBuf.Bytes(), err
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
	// so that we can ignore dataset deletion error if the member doesn't exist
	//
	// We do this because there is no easy way to simply overwrite a data set if it exists,
	// so we delete and upload.
	ctx.Logger.Verbose("Running: zowe zos-files delete data-set %s -f --rfj", member)

	var silencedBuf bytes.Buffer
	cmd := exec.Command("zowe", "zos-files", "delete", "data-set", `"`+member+`"`, "-f", "--rfj")
	cmd.Stdout = &silencedBuf
	cmd.Stderr = &silencedBuf
	_ = cmd.Run()

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

// Helper to validate data set (max qualifiers, max qualifier length, invalid chars in qualifier)
func ValidateDataSetQualifiers(name string) error {
	parts := strings.Split(name, ".")
	if len(parts) > 22 {
		return fmt.Errorf("dataset name has too many qualifiers (max 22): %d", len(parts))
	}

	for _, part := range parts {
		if len(part) == 0 {
			return fmt.Errorf("dataset name contains an empty qualifier")
		}
		if len(part) > 8 {
			return fmt.Errorf("qualifier %q exceeds 8 characters", part)
		}
		if matched, _ := regexp.MatchString(`^[A-Z0-9#$@]+$`, part); !matched {
			return fmt.Errorf("qualifier %q contains invalid characters; only A-Z, 0-9, $, #, @ allowed", part)
		}
	}
	return nil
}

// pollJobStatus runs zowe view job status by job ID and returns the JSON response on status 'OUTPUT'
// or non-nil return code
func pollJobStatus(ctx *context.ExecutionContext, jobId string) (*types.ZoweRfj, error) {
	spinnerText := fmt.Sprintf("Polling %s ...", jobId)
	ctx.Logger.StartSpinner(spinnerText)
	defer ctx.Logger.StopSpinner()

	for {
		time.Sleep(2 * time.Second)
		out, err := runZowe(ctx, "zos-jobs", "view", "job-status-by-jobid", jobId, "--rfj")
		if err != nil {
			return nil, err
		}

		var status types.ZoweRfj
		if err := json.Unmarshal(out, &status); err != nil {
			ctx.Logger.Info("⚠️ Failed to parse job status")
			return nil, err
		}

		spinnerText = fmt.Sprintf("Polling %s... (status: %s)", jobId, status.Data.Status)
		ctx.Logger.Spinner.Suffix = " " + spinnerText

		if status.Data.Status == "OUTPUT" || status.Data.RetCode != nil {
			return &status, nil
		}
	}
}

// saveZoweLog stores a parsed Zowe result to
// .grace/logs/20250423T213245_submit/JOB02848_HELLO.json
// (example log dir and file name)
func saveZoweLog(logDir string, ctx log.LogContext, payload any) error {
	// Filename: JOB02848_HELLO.json
	fileName := fmt.Sprintf("%s_%s.json", ctx.JobID, strings.ToUpper(ctx.JobName))
	filePath := filepath.Join(logDir, fileName)

	// Wrap full log context
	logObj := log.GraceJobLog{
		LogContext: ctx,
		Result:     payload,
	}

	// Write to disk
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	return encoder.Encode(logObj)
}
