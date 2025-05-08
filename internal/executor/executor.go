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
	"github.com/graceinfra/grace/internal/zowe"
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

	// Picked up by the scheduler, attempting to submit via Zowe.
	StateSubmitting JobState = "SUBMITTING"

	// Submitted successfully via Zowe, polling for completion.
	StateRunning JobState = "RUNNING"

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

	// --- Handle skipped jobs and finalize results

	e.markSkippedJobs()
	finalResults := e.collectFinalResults()

	e.logger.Debug().Msgf("Collected %d final job execution records.", len(finalResults))

	// TODO: Error reporting - did the executor itself fail?
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

			if depsMet && e.jobStates[jobName] == StatePending {
				e.jobStates[jobName] = StateReady
				e.logger.Debug().Str("job_name", jobName).Msg("Job is now READY (dependencies met).")
			} else if !depsMet && e.jobStates[jobName] == StatePending {
				// If deps are not met and state didn't change to SKIPPED, ensure it's WAITING_DEPS or keep PENDING
				// e.jobStates[jobName] = StateWaitingDeps // Optional explicit state but we can leave it as PENDING for now
			}
		}
	}
}

// launchReadyJobs finds jobs in READY state and launches goroutines for them, respecting concurrency limits
func (e *Executor) launchReadyJobs(activeGoroutines *int) int {
	launchedCount := 0

	// Collect ready jobs first to avoid holding the lock while waiting for semaphore
	readyJobs := []string{}
	e.stateMutex.RLock()
	for jobName, state := range e.jobStates {
		if state == StateReady {
			readyJobs = append(readyJobs, jobName)
		}
	}
	e.stateMutex.RUnlock()

	if len(readyJobs) > 0 {
		e.logger.Debug().Msgf("Found %d READY jobs: %v", len(readyJobs), readyJobs)
	}

	for _, jobName := range readyJobs {
		jobLogger := e.logger.With().Str("job_name", jobName).Logger()
		jobLogger.Debug().Msg("Attempting to acquire concurrency slot...")
		e.concurrencyChan <- struct{}{}
		jobLogger.Debug().Msg("Concurrency slot acquired.")

		// Re-check state after acquiring semaphore, before launching goroutine
		e.stateMutex.Lock()
		currentState := e.jobStates[jobName]
		if currentState != StateReady {
			e.stateMutex.Unlock()

			// State changed while waiting for semaphore (e.g. marked SKIPPED)
			jobLogger.Debug().Msgf("Job state changed to %s before launch, releasing slot.", currentState)
			<-e.concurrencyChan
			continue
		}

		e.jobStates[jobName] = StateSubmitting
		e.stateMutex.Unlock()

		jobLogger.Info().Msg("🚀 Launching job")

		*activeGoroutines++

		e.wg.Add(1)
		launchedCount++

		go e.executeJob(jobName)
	}

	return launchedCount
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

	startTime := time.Now()

	// --- Get the appropriate handler for the job type ---
	node := e.jobGraph[jobName]
	job := node.Job
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
				Type:   "user",
				Id:     os.Getenv("USER"),
				Tenant: host,
			},
			WorkflowId: e.ctx.WorkflowId,
			SubmitTime: startTime.Format(time.RFC3339),
		}

		e.addResult(jobName, errorRecord)
		return
	}

	jobLogger = jobLogger.With().Str("handler", handler.Type()).Logger()

	record := &models.JobExecutionRecord{
		JobName:     job.Name,
		JobID:       "PENDING_SUBMIT",
		Type:        job.Type,
		GraceCmd:    e.ctx.GraceCmd,
		ZoweProfile: e.ctx.Config.Config.Profile,
		HLQ:         hlq,
		Initiator: types.Initiator{
			Type:   "user",
			Id:     os.Getenv("USER"),
			Tenant: host,
		},
		WorkflowId: e.ctx.WorkflowId,
		SubmitTime: startTime.Format(time.RFC3339),
	}

	jobLogger.Info().Msg("Cleaning datasets...")

	handler.Prepare(e.ctx, job, jobLogger)

	// --- Submit job ---

	jobLogger.Info().Msg("Submitting job ...")

	submitResult, submitErr := zowe.SubmitJob(e.ctx, job)
	record.SubmitResponse = submitResult

	if submitErr != nil {
		jobLogger.Error().Err(submitErr).Msgf("Job submission failed")

		e.setJobState(jobName, StateFailed)

		record.JobID = "SUBMIT_FAILED"
		record.FinishTime = time.Now().Format(time.RFC3339)
		record.DurationMs = time.Since(startTime).Milliseconds()

		e.addResult(jobName, record)

		err := logging.SaveJobExecutionRecord(e.ctx.LogDir, *record)
		if err != nil {
			jobLogger.Error().
				Err(err).
				Str("log_dir", e.ctx.LogDir).
				Msg("Failed to save job execution record")
		}

		return
	}

	// --- Submission succeeded ---

	record.JobID = submitResult.Data.JobID

	// Initialize a new logger with job_id
	jobIdLogger := jobLogger.With().Str("job_id", record.JobID).Logger()

	jobIdLogger.Info().Msgf("✓ Job submitted (ID: %s). Waiting for completion...", record.JobID)
	e.setJobState(jobName, StateRunning)

	// --- Wait for completion ---

	finalResult, waitErr := zowe.WaitForJobCompletion(e.ctx, record.JobID, jobName)
	record.FinalResponse = finalResult

	finishTime := time.Now()
	record.FinishTime = finishTime.Format(time.RFC3339)
	record.DurationMs = int64(finishTime.Sub(startTime).Milliseconds())

	// --- Determine final state AND success ---

	finalState := StateFailed
	isSuccess := false

	if waitErr != nil {
		jobIdLogger.Error().Err(waitErr).Msg("Failed to get final status after waiting")
	} else if finalResult == nil || finalResult.Data == nil {
		jobIdLogger.Error().Msg("Polling for job finished, but final status data is incomplete.")
	} else {
		status := finalResult.Data.Status
		retCodeStr := "null"
		isRcFailure := false

		if finalResult.Data.RetCode != nil {
			retCodeStr = *finalResult.Data.RetCode

			// Check if the RC string itself indicates a failure type
			if retCodeStr == "JCL ERROR" || // Explicitly check for JCL ERROR RC string
				retCodeStr == "ABEND" || // Check for ABEND RC string (might include Sxxx/Uxxx)
				retCodeStr == "FLUSHED" || // Jobs can be flushed

				// Add other RC strings that always mean failure?
				(retCodeStr != "CC 0000" && retCodeStr != "CC 0004") { // Check for non-success CCs
				// Note: This condition might double-count "JCL ERROR", which is fine.
				isRcFailure = true
			}
		}

		// Determine success: Status must be OUTPUT and RC must NOT indicate failure
		if status == "OUTPUT" && !isRcFailure {
			finalState = StateSucceeded
			isSuccess = true
			jobIdLogger.Info().Msgf("✓ Job completed: Status %s, RC %s", status, retCodeStr)
		} else {
			// Any other status OR OUTPUT with a failure RC is treated as failure for state machine
			finalState = StateFailed
			isSuccess = false
			if status != "OUTPUT" {
				jobIdLogger.Warn().Msgf("Job finished with non-OUTPUT status: %s", status)
			} else { // Status was OUTPUT, so RC must have been bad
				jobIdLogger.Warn().Msgf("Job finished with OUTPUT status but failure RC: %s", retCodeStr)
			}
		}
	}

	// --- Final update ---

	e.setJobState(jobName, finalState)
	e.addResult(jobName, record)
	err := logging.SaveJobExecutionRecord(e.ctx.LogDir, *record)
	if err != nil {
		jobIdLogger.Error().Err(err).Str("log_dir", e.ctx.LogDir).Msg("Failed to save job execution record")
	}

	if isSuccess {
		jobIdLogger.Info().Msgf("✅ Finished job execution. Final state: %s", finalState)
	} else {
		jobIdLogger.Error().Msgf("❌ Finished job execution. Final state: %s", finalState)
	}
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
						Type:   "user",
						Id:     os.Getenv("USER"),
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
