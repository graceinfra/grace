package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
		return outBuf.Bytes(), fmt.Errorf("zowe %v failed: %w\n%s", args, err, errBuf.String())
	}
	return outBuf.Bytes(), nil
}

func ListZoweProfiles() ([]ZoweProfile, error) {
	configPath := filepath.Join(os.Getenv("HOME"), ".zowe", "zowe.config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var zoweConfig ZoweConfig
	if err := json.Unmarshal(data, &zoweConfig); err != nil {
		return nil, err
	}

	profiles := make([]ZoweProfile, 0, len(zoweConfig.Profiles))
	for _, p := range zoweConfig.Profiles {
		profiles = append(profiles, p)
	}

	return profiles, nil
}

type listRes struct {
	Data struct {
		ReturnedRows int `json:"returnedRows"`
	} `json:"data"`
}

// EnsurePDSExists checks if a partitioned data set exists and creates it if not.
func EnsurePDSExists(name string, verbose bool) error {
	out, err := RunZowe(verbose, true, "zos-files", "list", "data-set", name, "--rfj")
	if err != nil {
		return err
	}

	var res listRes

	if err = json.Unmarshal(out, &res); err != nil {
		return err
	}

	if res.Data.ReturnedRows > 0 {
		VerboseLog(verbose, "%s already exists", name)
		return nil
	}

	if res.Data.ReturnedRows == 0 {
		VerboseLog(verbose, "Allocating PDS %s ...", name)

		raw, err := RunZowe(verbose, false,
			"zos-files", "create", "data-set-partitioned", name,
			"--record-length", "80", "--block-size", "800", "--primary", "5", "--secondary", "5",
			"--dsorg", "PO", "--recfm", "FB")
		if err != nil {
			return err
		}

		var res ZoweRfj
		if err = json.Unmarshal(raw, &res); err != nil {
			return err
		}

		if !res.Success {
			return fmt.Errorf("Partitioned data set allocation failed: %s", res.Error.Msg)
		}
	}

	return nil
}

// EnsureSDSExists checks if a sequential data set exists and creates it if not.
func EnsureSDSExists(name string, verbose bool) error {
	out, err := RunZowe(verbose, true, "zos-files", "list", "data-set", name, "--rfj")
	if err != nil {
		return err
	}

	var res listRes

	if err := json.Unmarshal(out, &res); err != nil {
		return err
	}
	if res.Data.ReturnedRows > 0 {
		VerboseLog(verbose, "%s already exists", name)
		return nil
	}

	if res.Data.ReturnedRows == 0 {
		VerboseLog(verbose, "Allocating SDS %s ...", name)
		raw, err := RunZowe(verbose, false,
			"zos-files", "create", "data-set-sequential", name,
			"--record-length", "80", "--block-size", "800", "--primary", "5", "--secondary", "5",
			"--dsorg", "PS", "--recfm", "FB", "--binary")
		if err != nil {
			return err
		}

		var res ZoweRfj
		if err = json.Unmarshal(raw, &res); err != nil {
			return err
		}

		if !res.Success {
			return fmt.Errorf("Sequential data set allocation failed: %s", res.Error.Msg)
		}
	}

	return nil
}
