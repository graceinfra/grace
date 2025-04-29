package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

// Helper to create directory structure
func MkDir(targetDir string, parts ...string) {
	path := filepath.Join(append([]string{targetDir}, parts...)...)
	err := os.MkdirAll(path, 0755)
	cobra.CheckErr(err)
}

// Helper to check if a dir already exists
func MustNotExist(path string) {
	if _, err := os.Stat(path); err == nil {
		cobra.CheckErr(fmt.Errorf("refusing to overwrite existing file or directory: %s", path))
	}
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
