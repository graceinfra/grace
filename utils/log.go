package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	Verbose    = false
	JsonMode   = false
	SpoolMode  = false
	UseSpinner = false
)

// Info logs to stderr
func Info(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
}

// VerboseLog logs if --verbose
func VerboseLog(enabled bool, msg string, args ...any) {
	if enabled {
		fmt.Fprintf(os.Stderr, msg+"\n", args...)
	}
}

// Quiet returns true if we should suppress all UI (for use in commands)
func Quiet() bool {
	return JsonMode || SpoolMode
}

// SaveZoweLog stores a parsed Zowe result to
// .grace/logs/20250423T213245_submit/JOB02848_HELLO.json
// (example log dir and file name)
func SaveZoweLog(logDir string, ctx LogContext, payload any) error {
	// Filename: JOB02848_HELLO.json
	fileName := fmt.Sprintf("%s_%s.json", ctx.JobID, strings.ToUpper(ctx.JobName))
	filePath := filepath.Join(logDir, fileName)

	// Wrap full log context
	logObj := GraceJobLog{
		LogContext: ctx,
		Result:     payload,
	}

	// Write to disk
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	return encoder.Encode(logObj)
}

// CreateLogDir returns a full path like
// ".grace/logs/20250423T213245_submit"
// as well as YYYYMMDDTHHMMSS timestamp
func CreateLogDir(graceCmd string) (string, string, error) {
	timestamp := time.Now().Format("20060102T150405")
	dirName := fmt.Sprintf("%s_%s", timestamp, graceCmd)
	fullPath := filepath.Join(".grace", "logs", dirName)

	err := os.MkdirAll(fullPath, os.ModePerm)
	if err != nil {
		return "", "", err
	}
	return fullPath, timestamp, nil
}
