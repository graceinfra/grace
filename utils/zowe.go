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
