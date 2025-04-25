package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/graceinfra/grace/internal/templates"
	"github.com/spf13/cobra"
)

// WriteTpl loads tplName from tplFS, executes it with data, and writes to outPath
func WriteTpl(tplName, outPath string, data any) error {
	t, err := template.ParseFS(templates.TplFS, tplName)
	if err != nil {
		return err
	}

	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	return t.Execute(f, data)
}

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

// Helper to validate data set name (max qualifiers, max qualifier length, invalid chars in qualifier)
func IsValidDataSetName(name string) error {
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

// Helper to parse job id and name from spool output bc no JSON
func ParseSpoolMeta(out []byte) (jobId string, jobName string) {
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if jobId == "" {
			if fields := strings.Fields(line); len(fields) > 0 {
				for _, field := range fields {
					if strings.HasPrefix(field, "JOB") && len(field) >= 7 {
						jobId = field
						break
					}
				}
			}
		}
		if jobName == "" && strings.Contains(line, "IEF453I ") {
			parts := strings.Split(line, "IEF453I ")
			if len(parts) > 1 {
				right := strings.TrimSpace(parts[1])
				tokens := strings.Split(right, " ")
				if len(tokens) > 0 {
					jobName = tokens[0]
				}
			}
		}
		if jobName == "" && strings.Contains(line, "NAME-") {
			idx := strings.Index(line, "NAME-")
			if idx != -1 {
				after := strings.TrimSpace(line[idx+5:])
				fields := strings.Fields(after)
				if len(fields) > 0 {
					jobName = fields[0]
				}
			}
		}
		if jobId != "" && jobName != "" {
			break
		}
	}
	return
}
