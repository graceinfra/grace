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

// Helper to avoid overwriting a file or directory
func MustNotExist(path string) {
	if _, err := os.Stat(path); err == nil {
		cobra.CheckErr(fmt.Errorf("refusing to overwrite existing file or directory: %s", path))
	}
}

// Helper to validate data set (max qualifiers, max qualifier length, invalid chars in qualifier)
func ValidateDataSetQualifiers(name string) error {
	upperName := strings.ToUpper(name) // Work with uppercase
	parts := strings.Split(upperName, ".")
	if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
		return fmt.Errorf("dataset name cannot be empty")
	}
	if len(parts) > 22 {
		return fmt.Errorf("dataset name '%s' has too many qualifiers (%d), max 22", name, len(parts))
	}

	for _, part := range parts {
		if len(part) == 0 {
			// This case means double dots ".." or leading/trailing dot.
			return fmt.Errorf("dataset name '%s' contains an empty qualifier (adjacent or leading/trailing dots)", name)
		}
		if len(part) > 8 {
			return fmt.Errorf("in dataset '%s', qualifier '%s' exceeds 8 characters", name, part)
		}
		// Mainframe standard: First char A-Z, #, $, @. Subsequent A-Z, 0-9, #, $, @, -
		if matched, _ := regexp.MatchString(`^[A-Z0-9#$@]+$`, part); !matched {
			return fmt.Errorf("in dataset '%s', qualifier '%s' contains invalid characters (only A-Z, 0-9, $, #, @ allowed)", name, part)
		}
		// More precise regex (if needed later): ^[A-Z#$@][A-Z0-9#$@-]*$
	}
	return nil
}

// ValidatePDSMemberName checks if a string conforms to standard PDS member naming rules.
// Rules: 1-8 characters, uppercase A-Z, 0-9, national chars (#, $, @). First char cannot be numeric.
// Ensures name is uppercase before validation.
func ValidatePDSMemberName(name string) error {
	if name == "" {
		return fmt.Errorf("member name cannot be empty")
	}

	upperName := strings.ToUpper(name)

	if len(upperName) > 8 {
		return fmt.Errorf("member name '%s' exceeds 8 characters", name)
	}

	// Regex breakdown:
	// ^                  Start of string
	// [A-Z#$@]          First character must be A-Z, #, $, or @
	// [A-Z0-9#$@]{0,7}   Followed by 0 to 7 characters that are A-Z, 0-9, #, $, or @
	// $                  End of string
	// Note: This regex implicitly handles the 1-8 character length combined with the check above.
	// A simpler regex `^[A-Z#$@][A-Z0-9#$@]*$` would also work if combined with the length check.
	matched, _ := regexp.MatchString(`^[A-Z#$@][A-Z0-9#$@]{0,7}$`, upperName)
	if !matched {
		if len(upperName) > 0 && (upperName[0] >= '0' && upperName[0] <= '9') {
			return fmt.Errorf("member name '%s' is invalid: first character cannot be numeric", name)
		}
		return fmt.Errorf("member name '%s' contains invalid characters (allowed: A-Z, 0-9, #, $, @; first char must be non-numeric)", name)
	}

	return nil
}
