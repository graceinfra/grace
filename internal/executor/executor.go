package executor

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/graceinfra/grace/internal/config"
	"github.com/graceinfra/grace/internal/context"
	"github.com/graceinfra/grace/internal/log"
	"github.com/graceinfra/grace/internal/models"
	"github.com/graceinfra/grace/internal/zowe"
	"github.com/graceinfra/grace/types"
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
}

const DefaultConcurrency = 5

func NewExecutor(ctx *context.ExecutionContext, graph map[string]*config.JobNode, concurrency int) *Executor {
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
		ctx.Logger.Verbose("Using default concurrency: %d", concurrency)
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
	}
}

func (e *Executor) ExecuteAndWait() ([]models.JobExecutionRecord, error) {
	e.ctx.Logger.Verbose("Initializing DAG execution states...")

	// --- Initialize all job states ---

	e.stateMutex.Lock()

	for jobName, node := range e.jobGraph {
		e.results[jobName] = nil // Indicates not yet run/finished

		if len(node.Dependencies) == 0 {
			e.jobStates[jobName] = StateReady
			e.ctx.Logger.Verbose("Job %q initial state: %s (no dependencies)", jobName, StateReady)
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
			e.ctx.Logger.Verbose("Job %q initial state: %s (depends on: %v)", jobName, StatePending, depNames)
		}
	}

	e.stateMutex.Unlock()

	e.ctx.Logger.Verbose("Starting DAG execution loop...")

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
			e.ctx.Logger.Error("deadlock detected or internal scheduling error. No jobs running or ready.")
			return e.collectFinalResults(), fmt.Errorf("executor deadlick: no jobs running or ready, but not all jobs are finished")
		}

		if launchedCount == 0 && activeGoroutines > 0 {
			e.ctx.Logger.Verbose("Waiting for any running job to complete...")
			select {
			case <-e.jobCompletionChan:
				e.ctx.Logger.Verbose("Received job completion signal.")
				activeGoroutines--
			case err := <-e.errChan:
				e.ctx.Logger.Error("Fatal error received from job goroutine: %v", err)
				// TODO: Implement cancellation logic here? (signal other goroutines to stop)
				return e.collectFinalResults(), err
			case <-time.After(60 * time.Second):
				e.ctx.Logger.Warn("Timeout waiting for job completion signal.")
			}
		}
	}

	// --- Wait for any remaining jobs and collect results ---

	e.ctx.Logger.Verbose("Waiting for final job completions...")
	e.wg.Wait()
	e.ctx.Logger.Verbose("All job goroutines completed.")

	// --- Handle skipped jobs and finalize results

	e.markSkippedJobs()
	finalResults := e.collectFinalResults()

	e.ctx.Logger.Verbose("Collected %d final job execution records.", len(finalResults))

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
	e.ctx.Logger.Verbose("Job %q state changed to %s", jobName, state)
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
						e.ctx.Logger.Verbose("Skipping job %q because dependency %q failed or was skipped.", jobName, depNode.Job.Name)
						e.jobStates[jobName] = StateSkipped
						depsMet = false
						break // No need to check other dependencies if one failed/skipped
					}

					break
				}
			}

			if depsMet && e.jobStates[jobName] == StatePending {
				e.jobStates[jobName] = StateReady
				e.ctx.Logger.Verbose("Job %q is now READY (dependencies met).", jobName)
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
		e.ctx.Logger.Verbose("Found %d READY jobs: %v", len(readyJobs), readyJobs)
	}

	for _, jobName := range readyJobs {
		e.ctx.Logger.Verbose("Attempting to acquire concurrency slot for job %q...", jobName)
		e.concurrencyChan <- struct{}{}
		e.ctx.Logger.Verbose("Concurrency slot acquired for job %q.", jobName)

		// Re-check state after acquiring semaphore, before launching goroutine
		e.stateMutex.Lock()
		currentState := e.jobStates[jobName]
		if currentState != StateReady {
			e.stateMutex.Unlock()

			// State changed while waiting for semaphore (e.g. marked SKIPPED)
			e.ctx.Logger.Verbose("Job %q changed to %s before launch, releasing slot.", jobName, currentState)
			<-e.concurrencyChan
			continue
		}

		e.jobStates[jobName] = StateSubmitting
		e.stateMutex.Unlock()

		e.ctx.Logger.Info("🚀 Launching job %q", jobName)

		*activeGoroutines++

		e.wg.Add(1)
		launchedCount++

		go e.executeJob(jobName)
	}

	return launchedCount
}

// executeJob is the goroutine function that handles the lifecycle of a single job
func (e *Executor) executeJob(jobName string) {
	defer e.wg.Done()                      // Decrement WaitGroup counter when goroutine finishes
	defer func() { <-e.concurrencyChan }() // Release semaphore when goroutine finishes
	defer func() {
		e.jobCompletionChan <- struct{}{}
		e.ctx.Logger.Verbose("Job %q signaled completion.", jobName)
	}()

	node := e.jobGraph[jobName]
	job := node.Job
	startTime := time.Now()

	host, _ := os.Hostname()
	hlq := ""
	if e.ctx.Config.Datasets.JCL != "" {
		parts := strings.Split(e.ctx.Config.Datasets.JCL, ".")
		if len(parts) > 0 {
			hlq = parts[0]
		}
	}

	record := &models.JobExecutionRecord{
		JobName:     job.Name,
		JobID:       "SKIPPED",
		Step:        job.Step,
		Source:      job.Source,
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

	// --- Submit job ---

	e.ctx.Logger.Info("Submitting job %q...", jobName)

	submitResult, submitErr := zowe.SubmitJob(e.ctx, job)
	record.SubmitResponse = submitResult

	if submitErr != nil {
		e.ctx.Logger.Error("⚠️ Job %s submission failed: %v", record.JobName, submitErr)

		e.setJobState(jobName, StateFailed)

		record.JobID = "SUBMIT_FAILED"
		record.FinishTime = time.Now().Format(time.RFC3339)
		record.DurationMs = time.Since(startTime).Milliseconds()

		e.addResult(jobName, record)

		_ = log.SaveJobExecutionRecord(e.ctx.LogDir, *record)

		return
	}

	// --- Submission succeeded ---

	record.JobID = submitResult.Data.JobID
	e.ctx.Logger.Info("✓ Job %q submitted (ID: %s). Waiting for completion...", jobName, record.JobID)
	e.setJobState(jobName, StateRunning)

	// --- Wait for completion ---

	finalResult, waitErr := zowe.WaitForJobCompletion(e.ctx, record.JobID)
	record.FinalResponse = finalResult

	finishTime := time.Now()
	record.FinishTime = finishTime.Format(time.RFC3339)
	record.DurationMs = int64(finishTime.Sub(startTime).Milliseconds())

	// --- Determine final state ---

	finalState := StateFailed
	if waitErr != nil {
		e.ctx.Logger.Error("⚠️ Failed to getfinal status for job %q (%s): %v", jobName, record.JobID, waitErr)
	} else if finalResult != nil && finalResult.Data != nil {
		if finalResult.Data.Status == "OUTPUT" {
			finalState = StateSucceeded
			retCode := "null"
			if finalResult.Data.RetCode != nil {
				retCode = *finalResult.Data.RetCode
			}
			e.ctx.Logger.Info("✓ Job %s (%s) completed: Status %s, RC %s", record.JobName, record.JobID, finalResult.Data.Status, retCode)
		} else {
			e.ctx.Logger.Error("✓ Job %q (%s) finished with non-OUTPUT status: %s", jobName, record.JobID, finalResult.Data.Status)
			// State remains FAILED
		}
	} else {
		e.ctx.Logger.Error("⚠️ Polling for job %s (%s) finished, but final status data is incomplete.", record.JobName, record.JobID)
	}

	// --- Final update ---

	e.setJobState(jobName, finalState)
	e.addResult(jobName, record)
	_ = log.SaveJobExecutionRecord(e.ctx.LogDir, *record)

	e.ctx.Logger.Info("💫 Finished job %q execution. Final state: %s", jobName, finalState)
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
					e.ctx.Logger.Info("Marking job %q as %s due to upstream failure/skip.", jobName, StateSkipped)
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
				e.ctx.Logger.Verbose("Creating SKIPPED record for job %q", jobName)

				node := e.jobGraph[jobName]

				hlq := strings.Split(e.ctx.Config.Datasets.JCL, ".")[0]
				host, _ := os.Hostname()

				skippedRecord := models.JobExecutionRecord{
					JobName:     jobName,
					JobID:       "SKIPPED",
					Step:        node.Job.Step,
					Source:      node.Job.Source,
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
				e.ctx.Logger.Error("⚠️ Job %q finished in unexpected non-terminal state: %s", jobName, state)
			}
		}
	}
	return finalResults
}
