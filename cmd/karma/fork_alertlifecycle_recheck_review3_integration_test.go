package main

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

type review3SchedulerObservation struct {
	promoted int
	err      error
}

func seedProblem3Review3SchedulerStore(t *testing.T) (*alertlifecycle.Store, *sql.DB, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := time.Date(2026, time.August, 19, 11, 0, 0, 0, time.UTC)
	labels := `{"alertname":"Review3","service":"review3"}`
	if err = store.UpsertCurrentAlert(alertlifecycle.AlertCurrent{
		AlertID:           "review3-scheduler-alert",
		State:             alertlifecycle.StateInactive,
		RawState:          alertlifecycle.StateActive,
		Receiver:          "review3-team",
		GroupID:           "review3-group",
		GroupLabelsJSON:   `{"alertname":"Review3"}`,
		LabelsJSON:        labels,
		MatchLabelsJSON:   &labels,
		AnnotationsJSON:   `{}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["review3-am"]`,
		RelevantSources:   `[{"name":"review3-am","cluster":"review3-cluster"}]`,
		UpdatedAt:         now,
	}); err != nil {
		t.Fatal(err)
	}
	result, err := store.ApplyPollCycle(alertlifecycle.PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"review3-am": true},
		SilenceSources: []alertlifecycle.SourceSilenceState{{
			SourceName: "review3-am", ClusterID: "review3-cluster", SyncStatus: "success",
			Silences: []alertlifecycle.SourceSilenceSnapshot{{
				SilenceID: "review3-scheduler-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "service", Value: "review3", IsEqual: true}},
			}},
		}},
		Timestamp: now,
	})
	if err != nil || result.RecheckJobsQueued != 1 {
		t.Fatalf("candidate job result=%+v err=%v", result, err)
	}
	var total int
	if err = db.QueryRow(`SELECT total_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("candidate total=%d want=1", total)
	}
	return store, db, now
}

func observeProblem3Review3Scheduler(scheduler *recheckScheduler) (<-chan review3SchedulerObservation, <-chan struct{}) {
	observations := make(chan review3SchedulerObservation, 16)
	headReads := make(chan struct{}, 4)
	runner := scheduler.runner
	head := scheduler.head
	scheduler.runner = func(ctx context.Context, batchSize int) (int, error) {
		promoted, err := runner(ctx, batchSize)
		observations <- review3SchedulerObservation{promoted: promoted, err: err}
		return promoted, err
	}
	scheduler.head = func(ctx context.Context) (alertlifecycle.RecheckQueueHead, error) {
		result, err := head(ctx)
		headReads <- struct{}{}
		return result, err
	}
	return observations, headReads
}

func TestProblem3Review3SchedulerUsesRealStoreReconciliation(t *testing.T) {
	for _, test := range []struct {
		name  string
		hooks func() *alertlifecycle.RecheckIntegrationTestHooks
	}{
		{name: "generic lookup", hooks: func() *alertlifecycle.RecheckIntegrationTestHooks {
			return &alertlifecycle.RecheckIntegrationTestHooks{
				RecheckFault: func(stage string) error {
					if stage == "persist_candidate" {
						return errors.New("review3 candidate failure")
					}
					return nil
				},
				Commit: func(realCommit func() error) error {
					if err := realCommit(); err != nil {
						return err
					}
					return schedulerSQLiteError(5)
				},
				WriterCommitLookup: func(context.Context, string, string) (bool, error) { return false, context.DeadlineExceeded },
			}
		}},
		{name: "attempt tuple lookup", hooks: func() *alertlifecycle.RecheckIntegrationTestHooks {
			return &alertlifecycle.RecheckIntegrationTestHooks{
				RecheckFault: func(stage string) error {
					if stage == "persist_candidate" {
						return errors.New("review3 candidate failure")
					}
					return nil
				},
				Commit: func(realCommit func() error) error {
					if err := realCommit(); err != nil {
						return err
					}
					return schedulerSQLiteError(5)
				},
				WriterCommitLookup: func(context.Context, string, string) (bool, error) { return true, nil },
				AttemptLookup:      func(context.Context, string) (int64, int, string, error) { return 0, 0, "", context.DeadlineExceeded },
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, db, now := seedProblem3Review3SchedulerStore(t)
			clock := &reviewSchedulerClock{now: now}
			scheduler := newRecheckScheduler(store, clock)
			store.SetRecheckCommitWake(scheduler.Signal)
			store.SetRecheckReconciledWake(scheduler.SignalReconciled)
			processFinished := make(chan struct{}, 4)
			finalProcessRelease := make(chan struct{})
			var processMu sync.Mutex
			blockFinalProcess := false
			scheduler.afterProcess = func() {
				processFinished <- struct{}{}
				processMu.Lock()
				shouldBlock := blockFinalProcess
				processMu.Unlock()
				if shouldBlock {
					<-finalProcessRelease
				}
			}
			store.SetRecheckIntegrationTestHooks(test.hooks())
			observations, headReads := observeProblem3Review3Scheduler(scheduler)
			scheduler.Start()
			first := <-observations
			if !errors.Is(first.err, alertlifecycle.ErrWriterCommitOutcomeUnknown) {
				t.Fatalf("first result=%+v", first)
			}
			if got := <-clock.timer.resetEvents; got != time.Second {
				t.Fatalf("reconciliation backoff=%s want=1s", got)
			}
			<-processFinished
			if _, stale := scheduler.backoffStep[recheckBackoffPreAttempt]; stale {
				t.Fatalf("selected recheck commit uncertainty retained pre-attempt owner: %v", scheduler.backoffStep)
			}
			var attemptsBefore int
			if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attemptsBefore); err != nil {
				t.Fatal(err)
			}
			if attemptsBefore != 1 {
				t.Fatalf("attempts before reconciliation=%d", attemptsBefore)
			}
			processMu.Lock()
			blockFinalProcess = true
			processMu.Unlock()
			store.SetRecheckIntegrationTestHooks(nil)
			clock.timer.ch <- now.Add(time.Second)
			second := <-observations
			if !errors.Is(second.err, alertlifecycle.ErrWriterCommitReconciled) {
				t.Fatalf("reconciled result=%+v", second)
			}
			third := <-observations
			thirdIsDurableFailure := alertlifecycle.IsRecheckDurableFailure(third.err)
			if third.err != nil && !thirdIsDurableFailure {
				t.Fatalf("post-reconciliation result=%+v", third)
			}
			if !thirdIsDurableFailure {
				<-headReads
			}
			<-processFinished
			scheduler.stateMu.Lock()
			wakePending, reconciledWakePending, wakeLength := scheduler.wakePending, scheduler.reconciledWakePending, len(scheduler.wake)
			scheduler.stateMu.Unlock()
			if wakePending || reconciledWakePending || wakeLength != 0 {
				t.Fatalf("reconciled selected batch left a pending wake: wakePending=%v reconciledWakePending=%v channel=%d", wakePending, reconciledWakePending, wakeLength)
			}
			idleProbe := armRecheckSchedulerIdleLoopProbe(scheduler)
			close(finalProcessRelease)
			<-idleProbe
			select {
			case duplicate := <-observations:
				t.Fatalf("reconciled selected batch ran duplicate execution=%+v", duplicate)
			default:
			}
			scheduler.Stop()
			var parentCount, ledgerCount int
			if err := db.QueryRow(`SELECT attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&parentCount); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&ledgerCount); err != nil {
				t.Fatal(err)
			}
			if parentCount != 1 || ledgerCount != 1 {
				t.Fatalf("reconciliation attempts parent=%d ledger=%d", parentCount, ledgerCount)
			}
		})
	}
}

func TestProblem3Review3JoinedBookkeepingUnknownUsesRealStoreReconciliation(t *testing.T) {
	store, db, now := seedProblem3Review3SchedulerStore(t)
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		RecheckFault: func(stage string) error {
			if stage == "persist_candidate" {
				return errors.New("review3 candidate failure")
			}
			return nil
		},
		Commit: func(realCommit func() error) error {
			if err := realCommit(); err != nil {
				return err
			}
			return schedulerSQLiteError(5)
		},
		WriterCommitLookup: func(context.Context, string, string) (bool, error) { return false, context.DeadlineExceeded },
	})
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(scheduler.Signal)
	store.SetRecheckReconciledWake(scheduler.SignalReconciled)
	observations, _ := observeProblem3Review3Scheduler(scheduler)
	scheduler.Start()
	first := <-observations
	if !errors.Is(first.err, alertlifecycle.ErrRecheckFailurePersistence) || !errors.Is(first.err, alertlifecycle.ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("joined bookkeeping result=%+v", first)
	}
	if got := <-clock.timer.resetEvents; got != time.Second {
		t.Fatalf("joined reconciliation backoff=%s want=1s", got)
	}
	if scheduler.backoffStep[recheckBackoffReconciliation] != 1 || scheduler.backoffStep[recheckBackoffBookkeeping] != 0 {
		t.Fatalf("joined owner state=%v", scheduler.backoffStep)
	}
	store.SetRecheckIntegrationTestHooks(nil)
	clock.timer.ch <- now.Add(time.Second)
	second := <-observations
	if !errors.Is(second.err, alertlifecycle.ErrWriterCommitReconciled) {
		t.Fatalf("joined reconciliation=%+v", second)
	}
	third := <-observations
	if !alertlifecycle.IsRecheckDurableFailure(third.err) {
		t.Fatalf("joined completion=%+v", third)
	}
	scheduler.Stop()
	var attempts, attemptCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&attemptCount); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || attemptCount != 1 {
		t.Fatalf("joined attempts=%d parent=%d", attempts, attemptCount)
	}
}

func TestProblem3Review3SchedulerCancellationJoinsActiveStoreTransaction(t *testing.T) {
	store, db, now := seedProblem3Review3SchedulerStore(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var barrier sync.Once
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		TransactionFinished: func(string) {
			barrier.Do(func() {
				close(entered)
				<-release
			})
		},
	})
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	stopWaiting := make(chan struct{})
	scheduler.stopWaitBoundary = func() { close(stopWaiting) }
	scheduler.Start()
	<-entered
	stopped := make(chan struct{})
	go func() {
		scheduler.Stop()
		close(stopped)
	}()
	<-stopWaiting
	select {
	case <-stopped:
		t.Fatal("scheduler Stop returned before the active Store transaction was released")
	default:
	}
	close(release)
	<-stopped
	store.SetRecheckIntegrationTestHooks(nil)
	var attempts, attemptCount, processed int
	var outcome sql.NullString
	if err := db.QueryRow(`SELECT attempt_count,processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&attemptCount, &processed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates WHERE job_id=1`).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || attemptCount != 0 || processed != 0 || outcome.Valid {
		t.Fatalf("canceled active transaction attempts=%d parent=%d processed=%d outcome=%v", attempts, attemptCount, processed, outcome)
	}
}
