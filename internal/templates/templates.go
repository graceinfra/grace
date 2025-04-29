package templates

import (
	"embed"
	"os"
	"text/template"
)

//go:embed files/*
var TplFS embed.FS

// WriteTpl loads tplName from tplFS, executes it with data, and writes to outPath
func WriteTpl(tplName, outPath string, data any) error {
	t, err := template.ParseFS(TplFS, tplName)
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

