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
// If logFilePath is provided, logs in JSON format to that file.
func ConfigureGlobalLogger(isVerbose bool, logFilePath string) error {
	logLevel := zerolog.InfoLevel
	zerolog.SetGlobalLevel(logLevel)

	var outputWriter io.Writer
	isLoggingToFile := false

	if logFilePath != "" {
		// --- File logging ---
		isLoggingToFile = true
		dir := filepath.Dir(logFilePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create log directory %q: %w", dir, err)
		}

		fileHandle, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file %q: %w", logFilePath, err)
		}

		outputWriter = fileHandle

		// Set level to DEBUG regardless of isVerbose flag.
		// workflow.log file should contain all log levels.
		globallog.Logger = zerolog.New(outputWriter).With().Timestamp().Logger()
		logLevel = zerolog.DebugLevel
	} else {
		// --- Terminal logging ---
		outputWriter = zerolog.ConsoleWriter{
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
		globallog.Logger = zerolog.New(outputWriter).With().Timestamp().Logger()
	}

	zerolog.SetGlobalLevel(logLevel)
	zerolog.TimeFieldFormat = time.RFC3339

	// --- Log confirmation ---
	if isLoggingToFile {
		globallog.Debug().Msgf("Configured file logging (JSON format) to: %s", logFilePath)
		globallog.Debug().Msgf("File log level set to: %s", logLevel) // Reflects Debug
	} else {
		globallog.Debug().Msg("Configured console logging.")
		globallog.Debug().Msgf("Console log level set to: %s", logLevel) // Reflects Info or Debug
	}
	return nil
}
