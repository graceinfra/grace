package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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
