package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/graceinfra/grace/internal/templates"
	"github.com/spf13/cobra"
)

// WriteTpl loads tplName from tplFS, executes it with data, and writes to outPath
func WriteTpl(tplName, outPath string, data any) {
	t, err := template.ParseFS(templates.TplFS, tplName)
	cobra.CheckErr(err)

	f, err := os.Create(outPath)
	cobra.CheckErr(err)
	defer f.Close()

	err = t.Execute(f, data)
	cobra.CheckErr(err)
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
