package templates

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
)

//go:embed files/*
var TplFS embed.FS

// WriteTpl loads tplName from tplFS, executes it with data, and writes to outPath
func WriteTpl(tplName, outPath string, data any) error {
	return WriteTplWithFuncs(tplName, outPath, data, nil)
}

// WriteTplWithFuncs loads tplName, adds funcs to the template, executes, and writes.
func WriteTplWithFuncs(tplName, outPath string, data any, funcMap template.FuncMap) error {
	// Create a new template and parse the base file
	t := template.New(filepath.Base(tplName)) // Use filename as template name

	// Add custom functions IF provided
	if funcMap != nil {
		t = t.Funcs(funcMap)
	}

	// Parse the template file content from the embedded FS
	t, err := t.ParseFS(TplFS, tplName)
	if err != nil {
		return fmt.Errorf("failed to parse template %s: %w", tplName, err)
	}

	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", outPath, err)
	}
	defer f.Close()

	// Execute the template
	return t.Execute(f, data)
}
