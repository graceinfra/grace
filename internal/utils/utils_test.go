package utils

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateDataSetQualifiers(t *testing.T) {
	tests := []struct {
		name        string
		dataset     string
		shouldError bool
		errContains string
	}{
		{
			name:        "Valid dataset name",
			dataset:     "USER.JCL",
			shouldError: false,
		},
		{
			name:        "Valid dataset with multiple qualifiers",
			dataset:     "SYS1.USER.JCL.DATA",
			shouldError: false,
		},
		{
			name:        "Valid dataset with special chars",
			dataset:     "A$B#C@D.DATA",
			shouldError: false,
		},
		{
			name:        "Empty dataset name",
			dataset:     "",
			shouldError: true,
			errContains: "dataset name cannot be empty",
		},
		{
			name:        "Too many qualifiers",
			dataset:     createLongDatasetName(),
			shouldError: true,
			errContains: "has too many qualifiers",
		},
		{
			name:        "Empty qualifier (double dot)",
			dataset:     "USER..JCL",
			shouldError: true,
			errContains: "contains an empty qualifier",
		},
		{
			name:        "Empty qualifier (trailing dot)",
			dataset:     "USER.JCL.",
			shouldError: true,
			errContains: "contains an empty qualifier",
		},
		{
			name:        "Empty qualifier (leading dot)",
			dataset:     ".USER.JCL",
			shouldError: true,
			errContains: "contains an empty qualifier",
		},
		{
			name:        "Qualifier too long",
			dataset:     "USER.JCLLIBRARY", // JCLLIBRARY is 10 chars
			shouldError: true,
			errContains: "exceeds 8 characters",
		},
		{
			name:        "Invalid characters",
			dataset:     "USER.JCL-LIB",
			shouldError: true,
			errContains: "contains invalid characters",
		},
		{
			name:        "Lowercase converted to uppercase",
			dataset:     "user.jcl",
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDataSetQualifiers(tt.dataset)

			if tt.shouldError {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidatePDSMemberName(t *testing.T) {
	tests := []struct {
		name        string
		memberName  string
		shouldError bool
		errContains string
	}{
		{
			name:        "Valid member name",
			memberName:  "MEMBER1",
			shouldError: false,
		},
		{
			name:        "Valid member name with special chars",
			memberName:  "M$B#C@D",
			shouldError: false,
		},
		{
			name:        "Empty member name",
			memberName:  "",
			shouldError: true,
			errContains: "member name cannot be empty",
		},
		{
			name:        "Member name too long",
			memberName:  "TOOLONGMN",
			shouldError: true,
			errContains: "exceeds 8 characters",
		},
		{
			name:        "Invalid first character (numeric)",
			memberName:  "1MEMBER",
			shouldError: true,
			errContains: "first character cannot be numeric",
		},
		{
			name:        "Invalid characters",
			memberName:  "MEM-BER",
			shouldError: true,
			errContains: "contains invalid characters",
		},
		{
			name:        "Lowercase converted to uppercase",
			memberName:  "member",
			shouldError: false,
		},
		{
			name:        "Invalid characters (lowercase)",
			memberName:  "mem-ber",
			shouldError: true,
			errContains: "contains invalid characters",
		},
		{
			name:        "Maximum length (8 chars)",
			memberName:  "ABCDEFGH",
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePDSMemberName(tt.memberName)

			if tt.shouldError {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Helper function to create long dataset name for tests
func createLongDatasetName() string {
	return strings.Repeat("A.", 22) + "A" // 23 qualifiers
}