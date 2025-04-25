package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/graceinfra/grace/types"
)

// RunZowe invokes `zowe <args...>` under the hood, respecting JsonMode/SpoolMode/Verbose.
// Returns stdout bytes (for parsing) or an error.
func RunZowe(verbose, quiet bool, args ...string) ([]byte, error) {
	cmd := exec.Command("zowe", args...)

	var outBuf, errBuf bytes.Buffer
	cmd.Stdin = os.Stdin

	if verbose {
		fmt.Fprintf(os.Stderr, "Running: zowe %v\n", args)
	}

	if quiet {
		// In --json or --spool mode: capture silently
		cmd.Stdout = &outBuf
		cmd.Stderr = &errBuf
	} else if verbose {
		// Stream to terminal and capture
		cmd.Stdout = io.MultiWriter(os.Stdout, &outBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &errBuf)
	} else {
		cmd.Stdout = io.MultiWriter(os.Stdout, &outBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &errBuf)
	}

	err := cmd.Run()
	// Always return captured output
	if err != nil {
		return outBuf.Bytes(), fmt.Errorf("zowe %v failed: %w\n%s\n%s", args, err, errBuf.String(), outBuf.String())
	}
	return outBuf.Bytes(), nil
}

func ListZoweProfiles() ([]types.ZoweProfile, error) {
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

type listRes struct {
	Success bool `json:"success"`
	Data    struct {
		APIResponse struct {
			ReturnedRows int `json:"returnedRows"`
		} `json:"apiResponse"`
	} `json:"data"`
}

// EnsurePDSExists checks if a partitioned data set exists and creates it if not.
func EnsurePDSExists(name string, verbose bool) error {
	quotedName := `"` + name + `"`
	out, err := RunZowe(verbose, true, "zos-files", "list", "data-set", quotedName, "--rfj")
	if err != nil {
		return err
	}

	var res listRes

	if err = json.Unmarshal(out, &res); err != nil {
		return err
	}

	if res.Data.APIResponse.ReturnedRows > 0 {
		VerboseLog(verbose, "%s already exists", name)
		return nil
	}

	if res.Data.APIResponse.ReturnedRows == 0 {
		VerboseLog(verbose, "Allocating PDS %s ...", name)

		raw, err := RunZowe(verbose, true,
			"zos-files", "create", "data-set-partitioned", name, "--rfj")
		if err != nil {
			return err
		}

		var res types.ZoweRfj
		if err = json.Unmarshal(raw, &res); err != nil {
			return err
		}

		if !res.Success {
			return fmt.Errorf("Partitioned data set allocation failed: %s", res.Error.Msg)
		}

		VerboseLog(verbose, "Successfully allocated PDS %s", name)
	}

	return nil
}

// EnsureSDSExists checks if a sequential data set exists and creates it if not.
func EnsureSDSExists(name string, verbose bool) error {
	quotedName := `"` + name + `"`
	out, err := RunZowe(verbose, true, "zos-files", "list", "data-set", quotedName, "--rfj")
	if err != nil {
		return err
	}

	var res listRes

	if err := json.Unmarshal(out, &res); err != nil {
		return err
	}
	if res.Data.APIResponse.ReturnedRows > 0 {
		VerboseLog(verbose, "%s already exists", name)
		return nil
	}

	if res.Data.APIResponse.ReturnedRows == 0 {
		VerboseLog(verbose, "Allocating SDS %s ...", name)
		raw, err := RunZowe(verbose, false,
			"zos-files", "create", "data-set-sequential", name, "--rfj")
		if err != nil {
			return err
		}

		var res types.ZoweRfj
		if err = json.Unmarshal(raw, &res); err != nil {
			return err
		}

		if !res.Success {
			return fmt.Errorf("Sequential data set allocation failed: %s", res.Error.Msg)
		}
	}

	return nil
}
