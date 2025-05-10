package executor

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/jobhandler"
	"github.com/graceinfra/grace/internal/logging"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type JobState string

const (
	// Initial state, dependencies not checked or not met.
	StatePending JobState = "PENDING"

	// Dependencies checked and met, ready for submission queue.
	StateReady JobState = "READY"

	StateDispatching JobState = "DISPATCHING"

	// Terminal state: Job completed successfully (e.g., RC 0).
	StateSucceeded JobState = "SUCCEEDED"

	// Terminal state: Job failed (submit error, ABEND, JCL Error, non-zero RC, etc.).
	StateFailed JobState = "FAILED"

	// Terminal state: Job was not executed because an upstream dependency failed.
	StateSkipped JobState = "SKIPPED"
)

type Executor struct {
	ctx               *context.ExecutionContext
	jobGraph          map[string]*config.JobNode
	jobStates         map[string]JobState
	stateMutex        sync.RWMutex
	results           map[string]*models.JobExecutionRecord
	resultsMutex      sync.RWMutex
	wg                sync.WaitGroup // Waits for all launched job goroutines
	maxConcurrency    int
	concurrencyChan   chan struct{} // Semaphore to limit concurrency
	jobCompletionChan chan struct{} // Channel to signal job completion
	errChan           chan error    // For fatal errors (not currently used) TODO (?)
	registry          *jobhandler.HandlerRegistry
	logger            zerolog.Logger
}

const DefaultConcurrency = 5

func NewExecutor(ctx *context.ExecutionContext, graph map[string]*config.JobNode, concurrency int, registry *jobhandler.HandlerRegistry) *Executor {
	instanceLogger := log.With().
		Str("component", "executor").
		Str("workflow_id", ctx.WorkflowId.String()).
		Logger()

	if concurrency <= 0 {
		concurrency = DefaultConcurrency
		instanceLogger.Debug().Msgf("Using default concurrency: %d", concurrency)
	}

	return &Executor{
		ctx:               ctx,
		jobGraph:          graph,
		jobStates:         make(map[string]JobState, len(graph)),
		results:           make(map[string]*models.JobExecutionRecord),
		maxConcurrency:    concurrency,
		concurrencyChan:   make(chan struct{}, concurrency), // Buffered channel acts as a semaphore
		jobCompletionChan: make(chan struct{}, len(graph)),
		errChan:           make(chan error, 1),
		registry:          registry,
		logger:            instanceLogger,
	}
}

func (e *Executor) ExecuteAndWait() ([]models.JobExecutionRecord, error) {
	e.logger.Debug().Msg("Initializing DAG execution states...")

	// --- Initialize all job states ---

	e.stateMutex.Lock()

	for jobName, node := range e.jobGraph {
		e.results[jobName] = nil // Indicates not yet run/finished

		if len(node.Dependencies) == 0 {
			e.jobStates[jobName] = StateReady
			e.logger.Debug().Str("job", jobName).Msgf("Initial state: %s (no dependencies)", StateReady)
		} else {
			depNames := make([]string, len(node.Dependencies))
			validDeps := true
			for i, depNode := range node.Dependencies {
				depNames[i] = depNode.Job.Name
				if _, exists := e.jobGraph[depNode.Job.Name]; !exists {
					validDeps = false
				}
			}

			if !validDeps {
				// This shouldn't happen with a validated graph - check for bugs
				e.stateMutex.Unlock()
				return nil, fmt.Errorf("internal error: inconsistency found for job %q dependencies while initializing states", jobName)
			}

			e.jobStates[jobName] = StatePending
			e.logger.Debug().Str("job", jobName).Msgf("Initial state: %s (depends on: %v)", StatePending, depNames)
		}
	}

	e.stateMutex.Unlock()

	e.logger.Debug().Msg("Starting DAG execution loop...")

	// --- Core execution loop ---

	activeGoroutines := 0

	for {
		if e.allJobsDone() {
			break
		}

		// Find jobs that are ready to run (PENDING -> READY)
		e.checkAndReadyJobs()

		// Launch ready jobs concurrently
		launchedCount := e.launchReadyJobs(&activeGoroutines)

		if activeGoroutines == 0 && launchedCount == 0 && !e.allJobsDone() {
			e.logger.Error().Msg("Deadlock detected or internal scheduling error. No jobs running or ready.")
			return e.collectFinalResults(), fmt.Errorf("executor deadlock: no jobs running or ready, but not all jobs are finished")
		}

		if launchedCount == 0 && activeGoroutines > 0 {
			e.logger.Debug().Msg("Waiting for any running job to complete...")
			select {
			case <-e.jobCompletionChan:
				e.logger.Debug().Msg("Received job completion signal.")
				activeGoroutines--
			case err := <-e.errChan:
				e.logger.Error().Err(err).Msg("Fatal error received from job goroutine")
				// TODO: Implement cancellation logic here? (signal other goroutines to stop)
				return e.collectFinalResults(), err
			case <-time.After(60 * time.Second):
				e.logger.Warn().Msg("Timeout waiting for job completion signal.")
			}
		}
	}

	// --- Wait for any remaining jobs and collect results ---

	e.logger.Debug().Msg("Waiting for final job completions...")
	e.wg.Wait()
	e.logger.Debug().Msg("All job goroutines completed.")

	// --- Handle skipped jobs and finalize results ---

	e.markSkippedJobs()
	finalResults := e.collectFinalResults()

	e.logger.Debug().Msgf("Collected %d final job execution records.", len(finalResults))

	return finalResults, nil
}

// Helper to get job state safely
func (e *Executor) getJobState(jobName string) JobState {
	e.stateMutex.RLock()
	defer e.stateMutex.RUnlock()
	return e.jobStates[jobName]
}

// Helper to set job state safely
func (e *Executor) setJobState(jobName string, state JobState) {
	e.stateMutex.Lock()
	defer e.stateMutex.Unlock()

	e.jobStates[jobName] = state

	e.logger.Debug().
		Str("job_name", jobName).
		Msgf("State changed to %s", state)
}

// Helper to add result safely
func (e *Executor) addResult(jobName string, record *models.JobExecutionRecord) {
	e.resultsMutex.Lock()
	defer e.resultsMutex.Unlock()

	e.results[jobName] = record
}

// allJobsDone checks if all jobs in the graph are in a terminal state
func (e *Executor) allJobsDone() bool {
	e.stateMutex.RLock()
	defer e.stateMutex.RUnlock()

	for _, state := range e.jobStates {
		switch state {
		case StateSucceeded, StateFailed, StateSkipped:
			continue
		default:
			return false // Found a job not in a terminal state
		}
	}
	return true // All jobs are in a terminal state
}

// checkAndReadyJobs transitions PENDING jobs to READY if dependencies are met, otherwise PENDING
func (e *Executor) checkAndReadyJobs() {
	e.stateMutex.Lock()
	defer e.stateMutex.Unlock()

	for jobName, state := range e.jobStates {
		if state == StatePending {
			node := e.jobGraph[jobName]
			depsMet := true

			for _, depNode := range node.Dependencies {
				depState := e.jobStates[depNode.Job.Name]
				if depState != StateSucceeded {
					depsMet = false

					if depState == StateFailed || depState == StateSkipped {
						e.logger.Debug().Str("job_name", jobName).Msgf("Skipping job because dependency %q failed or was skipped.", depNode.Job.Name)
						e.jobStates[jobName] = StateSkipped
						depsMet = false
						break // No need to check other dependencies if one failed/skipped
					}

					break
				}
			}

			if depsMet && e.jobStates[jobName] == StatePending { // Re-check state in case it became SKIPPED
				e.jobStates[jobName] = StateReady
				e.logger.Debug().Str("job_name", jobName).Msg("Job is now READY (dependencies met).")
			}
		}
	}
}

// launchReadyJobs finds jobs in READY state and launches goroutines for them, respecting concurrency limits
func (e *Executor) launchReadyJobs(activeGoroutines *int) int {
	launchedCount := 0

	// Create a list of jobs that are currently StateReady.
	// This snapshot is important to avoid issues with map iteration while modifying.
	var jobsToConsider []string
	e.stateMutex.RLock()
	for name, st := range e.jobStates {
		if st == StateReady {
			jobsToConsider = append(jobsToConsider, name)
		}
	}
	e.stateMutex.RUnlock()

	if len(jobsToConsider) > 0 {
		e.logger.Debug().Msgf("Considering %d READY jobs for dispatch: %v", len(jobsToConsider), jobsToConsider)
	}

	for _, jobName := range jobsToConsider {
		jobLogger := e.logger.With().Str("job_name", jobName).Logger()

		// Lock to check and change state to DISPATCHING
		e.stateMutex.Lock()
		if e.jobStates[jobName] != StateReady {
			e.stateMutex.Unlock()
			jobLogger.Debug().Msgf("Job no longer READY (now %s), skipping dispatch attempt.", e.jobStates[jobName])
			continue
		}

		e.jobStates[jobName] = StateDispatching
		e.stateMutex.Unlock()

		jobLogger.Debug().Msg("Job marked DISPATCHING. Attempting to acquire concurrency slot...")
		e.concurrencyChan <- struct{}{} // Acquire slot (blocks if full)
		jobLogger.Debug().Msg("Concurrency slot acquired.")

		// Re-check state after acquiring semaphore.
		// It should still be DISPATCHING. If it changed (e.g., to SKIPPED by a very fast dependency failure propagated by checkAndReadyJobs,
		// or if another part of the code incorrectly modified it), then don't launch.
		e.stateMutex.Lock()
		currentState := e.jobStates[jobName]
		if currentState != StateDispatching {
			e.stateMutex.Unlock()
			jobLogger.Warn().Msgf("Job state changed from DISPATCHING to %s while waiting for/after acquiring slot. Releasing slot, not launching.", currentState)
			<-e.concurrencyChan // Release the acquired slot
			// Do NOT change its state back here; its current state (e.g., SKIPPED) is now the truth.
			continue
		}

		e.stateMutex.Unlock()

		jobLogger.Info().Msg("🚀 Launching job")
		*activeGoroutines++
		e.wg.Add(1)
		launchedCount++
		go e.executeJob(jobName) // executeJob will handle a DISPATCHING job
	}
	return launchedCount
}

// determineJobSuccess inspects the JobExecutionRecord to determine if the job succeeded.
// This encapsulates the logic for checking Zowe return codes or Shell exit statuses.
func determineJobSuccess(record *models.JobExecutionRecord, jobType string) bool {
	if record == nil {
		return false // Should not happen if handler.Execute always returns a record
	}

	// For Zowe-based jobs (compile, linkedit, execute)
	if jobType == "compile" || jobType == "linkedit" || jobType == "execute" {
		if record.SubmitResponse != nil && !record.SubmitResponse.Success {
			return false // Submission itself failed
		}
		if record.FinalResponse == nil || record.FinalResponse.Data == nil {
			return false // No final response or data means we can't determine success
		}
		if !record.FinalResponse.Success { // Zowe reported logical failure in final response
			return false
		}

		status := record.FinalResponse.Data.Status
		retCode := record.FinalResponse.Data.RetCode

		if status == "OUTPUT" {
			if retCode == nil { // Consider OUTPUT with null retcode as success (e.g. some utilities)
				return true
			}
			// Common success codes
			return *retCode == "CC 0000" || *retCode == "CC 0004"
		}
		return false // Not in OUTPUT status
	}

	if jobType == "shell" {
		// ShellHandler's Execute method populates SubmitResponse/FinalResponse to indicate script outcome
		if record.FinalResponse != nil { // FinalResponse holds shell stdout/stderr/exit
			return record.FinalResponse.Success // ShellHandler sets this based on script exit code
		}
		return false // No final response from shell handler implies an issue
	}

	return false
}

// executeJob is the goroutine function that handles the lifecycle of a single job
func (e *Executor) executeJob(jobName string) {
	jobLogger := e.logger.With().Str("job_name", jobName).Logger()

	defer e.wg.Done()                      // Decrement WaitGroup counter when goroutine finishes
	defer func() { <-e.concurrencyChan }() // Release semaphore when goroutine finishes
	defer func() {
		e.jobCompletionChan <- struct{}{}
		jobLogger.Debug().Msg("Job signaled completion.")
	}()

	host, _ := os.Hostname()
	hlq := ""
	if e.ctx.Config.Datasets.JCL != "" {
		parts := strings.Split(e.ctx.Config.Datasets.JCL, ".")
		if len(parts) > 0 {
			hlq = parts[0]
		}
	}

	node := e.jobGraph[jobName]
	job := node.Job
	startTime := time.Now()

	// --- Get the appropriate handler for the job type ---

	handler, exists := e.registry.Get(job.Type)
	if !exists {
		jobLogger.Error().Str("job_type", job.Type).Msg("Critical: No handler found for job type during execution")
		e.setJobState(jobName, StateFailed)

		errorRecord := &models.JobExecutionRecord{
			JobName:     job.Name,
			JobID:       "NO_HANDLER_FOUND",
			Type:        job.Type,
			RetryIndex:  0,
			GraceCmd:    e.ctx.GraceCmd,
			ZoweProfile: e.ctx.Config.Config.Profile,
			HLQ:         hlq,
			Initiator: types.Initiator{
				Type:   "system",
				Id:     "grace-executor",
				Tenant: host,
			},
			WorkflowId: e.ctx.WorkflowId,
			SubmitTime: startTime.Format(time.RFC3339),
			FinishTime: time.Now().Format(time.RFC3339),
			DurationMs: time.Since(startTime).Milliseconds(),
			SubmitResponse: &types.ZoweRfj{
				Success: false,
				Error: &types.ZoweRfjError{
					Msg: "No handler found for job type",
				},
			},
		}

		e.setJobState(jobName, StateFailed)
		e.addResult(jobName, errorRecord)

		if err := logging.SaveJobExecutionRecord(e.ctx.LogDir, *errorRecord); err != nil {
			jobLogger.Error().Err(err).Msg("Failed to save error job execution record for NO_HANDLER")
		}
		return
	}

	jobLogger = jobLogger.With().Str("handler_type", handler.Type()).Logger()

	// --- Call handler Prepare ---

	jobLogger.Info().Msg("Preparing job...")

	if err := handler.Prepare(e.ctx, job, jobLogger); err != nil {
		jobLogger.Error().Err(err).Msg("Job preparation failed.")

		prepFailRecord := models.JobExecutionRecord{
			JobName:     jobName,
			JobID:       "PREPARE_FAILED",
			Type:        job.Type,
			GraceCmd:    e.ctx.GraceCmd,
			ZoweProfile: e.ctx.Config.Config.Profile,
			HLQ:         strings.Split(e.ctx.Config.Datasets.JCL, ".")[0],
			Initiator: types.Initiator{
				Type: "system",
				Id:   "grace-executor",
			},
			WorkflowId: e.ctx.WorkflowId,
			SubmitTime: startTime.Format(time.RFC3339),
			FinishTime: time.Now().Format(time.RFC3339),
			DurationMs: time.Since(startTime).Milliseconds(),
			SubmitResponse: &types.ZoweRfj{
				Success: false,
				Error: &types.ZoweRfjError{
					Msg: fmt.Sprintf("Preparation failed: %s", err.Error()),
				},
			},
		}
		e.setJobState(jobName, StateFailed)
		e.addResult(jobName, &prepFailRecord)
		if logErr := logging.SaveJobExecutionRecord(e.ctx.LogDir, prepFailRecord); logErr != nil {
			jobLogger.Error().Err(logErr).Msg("Failed to save job execution record for PREPARE_FAILED")
		}
		return // Do not proceed to Execute or Cleanup if Prepare fails
	}

	jobLogger.Info().Msg("✓ Job preparation complete.")

	// --- Call handler Execute ---

	jobLogger.Info().Msg("Executing job...")
	execRecord := handler.Execute(e.ctx, job, jobLogger)

	if execRecord.FinishTime == "" {
		execRecord.FinishTime = time.Now().Format(time.RFC3339)
	}
	if execRecord.DurationMs == 0 {
		parsedSubmitTime, pErr := time.Parse(time.RFC3339, execRecord.SubmitTime)
		if pErr != nil { // Fallback if SubmitTime in record is bad
			parsedSubmitTime = startTime
		}
		parsedFinishTime, _ := time.Parse(time.RFC3339, execRecord.FinishTime)
		execRecord.DurationMs = parsedFinishTime.Sub(parsedSubmitTime).Milliseconds()
	}

	// --- Determine overall job success from the record ---

	isSuccess := determineJobSuccess(execRecord, job.Type)

	if isSuccess {
		jobLogger.Info().Str("job_id", execRecord.JobID).Msgf("✅ Job execution SUCCEEDED. Final Status: %s, RC: %s", execRecord.FinalResponse.GetStatus(), derefString(execRecord.FinalResponse.Data.RetCode))
		e.setJobState(jobName, StateSucceeded)
	} else {
		errMsg := "Job execution FAILED."
		if execRecord.FinalResponse != nil && execRecord.FinalResponse.Error != nil {
			errMsg = fmt.Sprintf("❌ Job execution FAILED: %s. Final Status: %s, RC: %s", execRecord.FinalResponse.Error.Msg, execRecord.FinalResponse.GetStatus(), derefString(execRecord.FinalResponse.Data.RetCode))
		} else if execRecord.SubmitResponse != nil && execRecord.SubmitResponse.Error != nil {
			errMsg = fmt.Sprintf("❌ Job submission FAILED: %s", execRecord.SubmitResponse.Error.Msg)
		}
		jobLogger.Error().Str("job_id", execRecord.JobID).Msg(errMsg)
		e.setJobState(jobName, StateFailed)
	}

	// --- Add result and save log ---

	e.addResult(jobName, execRecord)
	if err := logging.SaveJobExecutionRecord(e.ctx.LogDir, *execRecord); err != nil {
		jobLogger.Error().Err(err).Str("log_dir", e.ctx.LogDir).Msg("Failed to save job execution record")
	}

	// --- Call handler Cleanup ----

	jobLogger.Info().Msg("Cleaning up job resources...")

	if err := handler.Cleanup(e.ctx, job, execRecord, jobLogger); err != nil {
		jobLogger.Error().Err(err).Msg("Job cleanup failed.")
	}

	jobLogger.Info().Msg("✓ Job cleanup complete.")
	jobLogger.Info().Msgf("🏁 Finished job execution. Final state: %s", e.getJobState(jobName))
}

// Helper to dereference string pointer for logging, returning "null" if nil
func derefString(s *string) string {
	if s == nil {
		return "null"
	}
	return *s
}

// markSkippedJobs iterates through jobs that are still PENDING after the main loop
// and marks them as SKIPPED if any of their direct or indirect dependencies FAILED or were SKIPPED
func (e *Executor) markSkippedJobs() {
	e.stateMutex.Lock()
	defer e.stateMutex.Unlock()

	// Need to potentially iterate multiple times or use recursion/DFS
	// because skipping one job might enable another to be skipped.
	// Simpler iterative approach first. Repeat until no more changes.
	madeChanges := true
	for madeChanges {
		madeChanges = false
		for jobName, state := range e.jobStates {
			if state == StatePending { // Only consider jobs that never started
				node := e.jobGraph[jobName]
				shouldSkip := false
				for _, depNode := range node.Dependencies {
					depState := e.jobStates[depNode.Job.Name]
					if depState == StateFailed || depState == StateSkipped {
						shouldSkip = true
						break
					}
				}

				if shouldSkip {
					e.jobStates[jobName] = StateSkipped
					e.logger.Info().Str("job_name", jobName).Msgf("Marking job as %s due to upstream failure/skip.", StateSkipped)
					madeChanges = true
				}
			}
		}
	}
}

// collectFinalResults returns the final JobExecutionRecords from each job run
func (e *Executor) collectFinalResults() []models.JobExecutionRecord {
	finalResults := make([]models.JobExecutionRecord, 0, len(e.jobGraph))
	e.resultsMutex.RLock()
	defer e.resultsMutex.RUnlock()

	for jobName, record := range e.results {
		if record != nil {
			finalResults = append(finalResults, *record)
		} else {
			// If a record is still nil, that means it was probably skipped.
			state := e.getJobState(jobName)

			if state == StateSkipped {
				e.logger.Debug().Str("job_name", jobName).Msg("Creating SKIPPED record")

				node := e.jobGraph[jobName]

				hlq := strings.Split(e.ctx.Config.Datasets.JCL, ".")[0]
				host, _ := os.Hostname()

				skippedRecord := models.JobExecutionRecord{
					JobName:     jobName,
					JobID:       "SKIPPED",
					Type:        node.Job.Type,
					GraceCmd:    e.ctx.GraceCmd,
					ZoweProfile: e.ctx.Config.Config.Profile,
					HLQ:         hlq,
					Initiator: types.Initiator{
						Type:   "system",
						Id:     "grace-executor",
						Tenant: host,
					},
					WorkflowId: e.ctx.WorkflowId,
				}

				finalResults = append(finalResults, skippedRecord)

			} else if state != StateSucceeded && state != StateFailed {
				// Shouldn't happen if loop/wait logic is correct but log this in case
				e.logger.Error().Str("job_name", jobName).Msgf("Job finished in unexpected non-terminal state: %s", state)
			}
		}
	}
	return finalResults
}
