package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	globallog "github.com/rs/zerolog/log"
)

// ConfigureGlobalLogger sets up the zerolog global logger instance.
// Call this once at the start of the application (e.g., in main.go).
// logFilePath should be empty for terminal logging (uses ConsoleWriter to stderr).
// If logFilePath is provided, logs in JSON format to that file and to terminal.
func ConfigureGlobalLogger(isVerbose bool, logFilePath string) error {
	logLevel := zerolog.InfoLevel
	if isVerbose {
		logLevel = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(logLevel)
	zerolog.TimeFieldFormat = time.RFC3339

	// Create console writer for terminal output
	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
		NoColor:    false,
		FormatLevel: func(i any) string {
			if level, ok := i.(string); ok {
				return strings.ToUpper(fmt.Sprintf("[%s]", level))
			}
			return fmt.Sprintf("[%v]", i)
		},
		FormatMessage: func(i any) string {
			// Prevent extra quotes around simple messages in console
			if msg, ok := i.(string); ok {
				return msg
			}
			return fmt.Sprintf("%v", i)
		},
	}

	var writers []io.Writer
	writers = append(writers, consoleWriter)

	if logFilePath != "" {
		// Set up file logging in addition to console
		dir := filepath.Dir(logFilePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create log directory %q: %w", dir, err)
		}

		fileHandle, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file %q: %w", logFilePath, err)
		}
		writers = append(writers, fileHandle)
	}

	// Create multi-writer if we have both console and file
	outputWriter := io.MultiWriter(writers...)
	globallog.Logger = zerolog.New(outputWriter).With().Timestamp().Logger()

	// --- Log confirmation ---
	if logFilePath != "" {
		globallog.Debug().Msgf("Configured logging to file (JSON) and console: %s", logFilePath)
	} else {
		globallog.Debug().Msg("Configured console-only logging")
	}
	globallog.Debug().Msgf("Log level set to: %s", logLevel)

	return nil
}
