package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestProblem3Review5ImmediateReconciledEmptyRunDoesNotWake(t *testing.T) {
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.SetRecheckReconciledWake(nil)
	storeWriterCommit := func(realCommit func() error) error {
		if err := realCommit(); err != nil {
			return err
		}
		return errors.New("review5 immediate commit error")
	}
	storeWriterLookup := func(context.Context, string, string) (bool, error) { return true, nil }
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		Commit:             storeWriterCommit,
		WriterCommitLookup: storeWriterLookup,
	})
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckReconciledWake(scheduler.SignalReconciled)
	runnerReturned := make(chan int, 2)
	headReturned := make(chan struct{}, 2)
	processFinished := make(chan struct{}, 2)
	productionRunner := scheduler.runner
	call := 0
	scheduler.runner = func(ctx context.Context, batchSize int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		result, runErr := productionRunner(ctx, batchSize)
		runnerReturned <- call
		return result, runErr
	}
	productionHead := scheduler.head
	scheduler.head = func(ctx context.Context) (alertlifecycle.RecheckQueueHead, error) {
		head, headErr := productionHead(ctx)
		headReturned <- struct{}{}
		return head, headErr
	}
	releaseAfterProcess := make(chan struct{})
	scheduler.afterProcess = func() {
		processFinished <- struct{}{}
		<-releaseAfterProcess
	}
	scheduler.Start()
	if got := <-runnerReturned; got != 1 {
		t.Fatalf("first runner call=%d", got)
	}
	<-headReturned
	<-processFinished
	scheduler.stateMu.Lock()
	wakePending, reconciledWakePending, wakeLength := scheduler.wakePending, scheduler.reconciledWakePending, len(scheduler.wake)
	scheduler.stateMu.Unlock()
	if wakePending || reconciledWakePending || wakeLength != 0 {
		t.Fatalf("immediate empty run left a pending wake: wakePending=%v reconciledWakePending=%v channel=%d", wakePending, reconciledWakePending, wakeLength)
	}
	idleProbe := armRecheckSchedulerIdleLoopProbe(scheduler)
	close(releaseAfterProcess)
	<-idleProbe
	select {
	case got := <-runnerReturned:
		t.Fatalf("immediate reconciled empty run self-woke call=%d", got)
	default:
	}
	scheduler.Stop()
}

func TestProblem3Review5DurableFailedHeadKeepsSchedulerAlive(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	scheduler := newRecheckScheduler(nil, clock)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, alertlifecycle.NewRecheckDurableFailureError(17, errors.New("terminal candidate failure"))
	}
	scheduler.Start()
	select {
	case <-scheduler.done:
		t.Fatal("durable failed head terminated scheduler")
	default:
	}
	scheduler.Stop()
}

func TestProblem3Review5TerminalBookkeepingErrorsDoNotBackoff(t *testing.T) {
	for name, failure := range map[string]error{
		"invariant":    alertlifecycle.ErrWriterOperationInvariant,
		"token":        alertlifecycle.ErrWriterOperationToken,
		"terminal_sql": problem3Review5SQLiteError{code: 13},
	} {
		t.Run(name, func(t *testing.T) {
			clock := &reviewSchedulerClock{now: time.Now().UTC()}
			scheduler := newRecheckScheduler(nil, clock)
			scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
				return alertlifecycle.RecheckBatchResult{}, errors.Join(alertlifecycle.ErrRecheckFailurePersistence, failure)
			}
			scheduler.process(clock.NewTimer(time.Second), nil)
			if len(clock.timer.resets) != 0 {
				t.Fatalf("terminal bookkeeping error armed backoff=%v", clock.timer.resets)
			}
		})
	}
}

type problem3Review5SQLiteError struct{ code int }

func (e problem3Review5SQLiteError) Error() string { return "review5 sqlite error" }

func (e problem3Review5SQLiteError) SQLiteCode() int { return e.code }

func TestProblem3Review5UnrelatedReconciledWriterDoesNotResetPreAttempt(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	scheduler := newRecheckScheduler(nil, clock)
	scheduler.backoffStep[recheckBackoffPreAttempt] = 3
	scheduler.reconciledWakePending = true
	runs := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		runs++
		if runs == 1 {
			return alertlifecycle.RecheckBatchResult{}, alertlifecycle.NewRecheckWriterCommitReconciledError("poll")
		}
		return alertlifecycle.RecheckBatchResult{}, schedulerPreAttemptError{}
	}
	scheduler.process(clock.NewTimer(time.Second), nil)
	if got := scheduler.backoffStep[recheckBackoffPreAttempt]; got != 4 {
		t.Fatalf("unrelated reconciliation changed pre-attempt step=%d want=4", got)
	}
	if !scheduler.reconciledWakePending {
		t.Fatal("unrelated reconciliation discarded a pending recheck wake")
	}
}

func TestProblem3Review5UnrelatedPollReconciliationPreservesPreAttemptOwner(t *testing.T) {
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	commitCalls := 0
	lookupCalls := 0
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		Commit: func(realCommit func() error) error {
			commitCalls++
			if err := realCommit(); err != nil {
				return err
			}
			if commitCalls == 1 {
				return errors.New("review5 poll commit outcome unknown")
			}
			return nil
		},
		WriterCommitLookup: func(context.Context, string, string) (bool, error) {
			lookupCalls++
			if lookupCalls == 1 {
				return false, errors.New("review5 poll lookup delayed")
			}
			return true, nil
		},
	})
	if _, err := store.ApplyPollCycle(alertlifecycle.PollCycleInput{Timestamp: time.Now().UTC()}); !errors.Is(err, alertlifecycle.ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("poll outcome=%v", err)
	}
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	scheduler := newRecheckScheduler(store, clock)
	scheduler.backoffStep[recheckBackoffPreAttempt] = 3
	productionRunner := scheduler.runner
	runs := 0
	scheduler.runner = func(ctx context.Context, batchSize int) (alertlifecycle.RecheckBatchResult, error) {
		runs++
		if runs == 1 {
			return productionRunner(ctx, batchSize)
		}
		return alertlifecycle.RecheckBatchResult{}, schedulerPreAttemptError{}
	}
	scheduler.process(clock.NewTimer(time.Second), nil)
	if got := scheduler.backoffStep[recheckBackoffPreAttempt]; got != 4 {
		t.Fatalf("unrelated poll reconciliation changed pre-attempt step=%d want=4", got)
	}
}

func TestProblem3Review5FailedHeadRetryResumesSameJobThroughLiveScheduler(t *testing.T) {
	store, db, now := seedProblem3Review3SchedulerStore(t)
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		RecheckFault: func(stage string) error {
			if stage == "persist_candidate" {
				return errors.New("review5 terminal candidate")
			}
			return nil
		},
	})
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(scheduler.Signal)
	store.SetRecheckReconciledWake(scheduler.SignalReconciled)
	runs, heads := observeProblem3Review3Scheduler(scheduler)
	processFinished := make(chan struct{}, 4)
	scheduler.afterProcess = func() { processFinished <- struct{}{} }
	scheduler.Start()
	first := <-runs
	if !alertlifecycle.IsRecheckDurableFailure(first.err) {
		t.Fatalf("first failure=%+v", first)
	}
	<-processFinished
	select {
	case <-scheduler.done:
		t.Fatal("scheduler stopped at durable failed head")
	default:
	}
	store.SetRecheckIntegrationTestHooks(nil)
	actor := "review5-retry"
	if _, err := store.RetryRecheckJobContext(context.Background(), 1, "review5-retry", "retry failed head", &actor, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	second := <-runs
	if second.err != nil {
		t.Fatalf("retry result=%+v", second)
	}
	<-heads
	<-processFinished
	scheduler.Stop()
	var status string
	var attempts, jobs int
	if err := db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || attempts != 2 || jobs != 1 {
		t.Fatalf("retry recovery status=%s attempts=%d jobs=%d", status, attempts, jobs)
	}
}

func TestProblem3Review5FailedHeadAbandonReleasesLiveSchedulerSuccessor(t *testing.T) {
	store, db, now := seedProblem3Review3SchedulerStore(t)
	nowRaw := now.Add(time.Second).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,total_candidates,processed_candidates,created_at) VALUES(?,?,?,?,?,?,?,?)`, "review5-successor-context", "sha256:review5-successor", `{"formatVersion":1,"sources":[],"clusters":[]}`, "source_context_changed", "queued", 0, 0, nowRaw); err != nil {
		t.Fatal(err)
	}
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		RecheckFault: func(stage string) error {
			if stage == "persist_candidate" {
				return errors.New("review5 terminal candidate")
			}
			return nil
		},
	})
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(scheduler.Signal)
	store.SetRecheckReconciledWake(scheduler.SignalReconciled)
	runs, heads := observeProblem3Review3Scheduler(scheduler)
	processFinished := make(chan struct{}, 4)
	scheduler.afterProcess = func() { processFinished <- struct{}{} }
	scheduler.Start()
	first := <-runs
	if !alertlifecycle.IsRecheckDurableFailure(first.err) {
		t.Fatalf("first failure=%+v", first)
	}
	<-processFinished
	store.SetRecheckIntegrationTestHooks(nil)
	actor := "review5-abandon"
	if _, err := store.AbandonRecheckJobContext(context.Background(), 1, "review5-abandon", "release failed head", &actor, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	second := <-runs
	if second.err != nil {
		t.Fatalf("successor result=%+v", second)
	}
	<-heads
	<-processFinished
	scheduler.Stop()
	var firstStatus, secondStatus string
	if err := db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&firstStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=2`).Scan(&secondStatus); err != nil {
		t.Fatal(err)
	}
	if firstStatus != "abandoned" || secondStatus != "completed" {
		t.Fatalf("abandon recovery statuses=%s,%s", firstStatus, secondStatus)
	}
}

func TestProblem3Review5SixthRetryableFailureLeavesLiveFailedHead(t *testing.T) {
	store, db, now := seedProblem3Review3SchedulerStore(t)
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		RecheckFault: func(stage string) error {
			if stage == "persist_candidate" {
				return problem3Review5SQLiteError{code: 5}
			}
			return nil
		},
	})
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(scheduler.Signal)
	store.SetRecheckReconciledWake(scheduler.SignalReconciled)
	runs := make(chan review3SchedulerObservation, 8)
	productionRunner := scheduler.runner
	scheduler.runner = func(ctx context.Context, batchSize int) (alertlifecycle.RecheckBatchResult, error) {
		result, runErr := productionRunner(ctx, batchSize)
		runs <- review3SchedulerObservation{result: result, err: runErr}
		return result, runErr
	}
	processFinished := make(chan struct{}, 8)
	scheduler.afterProcess = func() { processFinished <- struct{}{} }
	scheduler.Start()
	for attempt := 1; attempt <= 6; attempt++ {
		result := <-runs
		if attempt < 6 {
			var retry interface{ NextTime() time.Time }
			if !errors.As(result.err, &retry) {
				t.Fatalf("attempt %d result=%+v", attempt, result)
			}
			if got := <-clock.timer.resetEvents; got == 0 {
				t.Fatalf("attempt %d armed zero retry delay", attempt)
			}
			if _, err := db.Exec(`UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=1`, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			clock.timer.ch <- now
			continue
		}
		if !alertlifecycle.IsRecheckDurableFailure(result.err) {
			t.Fatalf("sixth attempt result=%+v", result)
		}
	}
	<-processFinished
	select {
	case <-scheduler.done:
		t.Fatal("scheduler stopped after durable sixth failure")
	default:
	}
	scheduler.Stop()
	var status string
	var attempts, attemptCount int
	if err := db.QueryRow(`SELECT status,attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attemptCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || attempts != 6 || attemptCount != attempts {
		t.Fatalf("sixth failure status=%s attempts=%d parent=%d", status, attempts, attemptCount)
	}
}

func TestProblem3Review5QueueBlockedAndSuccessfulHeadResetQueueRead(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	scheduler := newRecheckScheduler(nil, clock)
	scheduler.backoffStep[recheckBackoffQueueRead] = 2
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, &alertlifecycleTestQueueBlockedError{status: "failed"}
	}
	scheduler.process(clock.NewTimer(time.Second), nil)
	if got := scheduler.backoffStep[recheckBackoffQueueRead]; got != 2 {
		t.Fatalf("queue-blocked reset queue-read step=%d want=2", got)
	}
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, nil
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		return alertlifecycle.RecheckQueueHead{}, nil
	}
	scheduler.process(clock.NewTimer(time.Second), nil)
	if _, ok := scheduler.backoffStep[recheckBackoffQueueRead]; ok {
		t.Fatalf("successful queue read retained queue-read backoff=%v", scheduler.backoffStep)
	}
}

type alertlifecycleTestQueueBlockedError struct{ status string }

func (e *alertlifecycleTestQueueBlockedError) Error() string { return "queue blocked" }

func (e *alertlifecycleTestQueueBlockedError) QueueBlocked() (string, *time.Time) {
	return e.status, nil
}
