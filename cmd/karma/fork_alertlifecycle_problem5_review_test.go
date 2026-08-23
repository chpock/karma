package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestProblem5CompleteDeterministicLifecycleSequence(t *testing.T) {
	store, db, _, path := seedProblem5SchedulerFollower(t)
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	failureScript := &problem5FailureScript{}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(scheduler.Signal)
	store.SetRecheckReconciledWake(scheduler.SignalReconciled)
	calls := make(chan int, 16)
	processed := make(chan struct{}, 16)
	productionRunner := scheduler.runner
	call := 0
	scheduler.runner = func(ctx context.Context, _ int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		failureScript.arm(problem5FailureForCall(call))
		calls <- call
		return productionRunner(ctx, 20)
	}
	scheduler.afterProcess = func() { processed <- struct{}{} }
	previousLogger := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previousLogger)
	installProblem5FailureScript(store, clock, failureScript)
	scheduler.Start()
	waitProblem5Call(t, calls, 1)
	waitProblem5Call(t, calls, 2)
	waitProblem5Process(t, processed)
	var processedCandidates int
	if err := db.QueryRow(`SELECT processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&processedCandidates); err != nil {
		t.Fatal(err)
	}
	if processedCandidates != 20 || problem5JobStatus(t, db, 1) != "retry_wait" {
		t.Fatalf("partial retry batch status=%s processed=%d want retry_wait/20", problem5JobStatus(t, db, 1), processedCandidates)
	}
	if err := assertProblem5SchedulerPhase(db, 1, "retry_wait", 20, 2, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	if fired := failureScript.firedSnapshot(); len(fired) != 1 || fired[0] != "retryable" {
		t.Fatalf("retry scheduling fault script=%v want [retryable]", fired)
	}
	if err := assertProblem5FailureRecords(failureScript.recordsSnapshot(), []problem5FailureRecord{{Mode: "retryable", Stage: "persist_candidate", JobID: 1, Attempt: 2, Candidate: "problem5-fifo-head-20"}}); err != nil {
		t.Fatal(err)
	}
	var nextAttemptRaw string
	if err := db.QueryRow(`SELECT next_attempt_at FROM silence_recheck_jobs WHERE job_id=1`).Scan(&nextAttemptRaw); err != nil {
		t.Fatal(err)
	}
	nextAttemptAt, err := time.Parse(time.RFC3339Nano, nextAttemptRaw)
	if err != nil {
		t.Fatal(err)
	}
	scheduler.Stop()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, db = reopenProblem5SchedulerStore(t, path, clock, failureScript)
	clock.now = nextAttemptAt
	resume := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(resume.Signal)
	store.SetRecheckReconciledWake(resume.SignalReconciled)
	resumeProcessed := make(chan struct{}, 8)
	resume.afterProcess = func() { resumeProcessed <- struct{}{} }
	resumeFirstBatchDone := make(chan struct{})
	resumeFirstBatchContinue := make(chan struct{})
	productionRunner = resume.runner
	resume.runner = func(ctx context.Context, _ int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		failureScript.arm(problem5FailureForCall(call))
		calls <- call
		result, runErr := productionRunner(ctx, 20)
		if call == 3 {
			close(resumeFirstBatchDone)
			<-resumeFirstBatchContinue
		}
		return result, runErr
	}
	resume.Start()
	waitProblem5Call(t, calls, 3)
	waitProblem5Signal(t, resumeFirstBatchDone)
	if err := assertProblem5SchedulerPhase(db, 1, "running", 40, 3, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	close(resumeFirstBatchContinue)
	waitProblem5Call(t, calls, 4)
	waitProblem5Process(t, resumeProcessed)
	if err := db.QueryRow(`SELECT processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&processedCandidates); err != nil {
		t.Fatal(err)
	}
	if processedCandidates != 40 || problem5JobStatus(t, db, 1) != "failed" {
		t.Fatalf("restart/resume state=%s processed=%d want failed/40", problem5JobStatus(t, db, 1), processedCandidates)
	}
	if err := assertProblem5SchedulerPhase(db, 1, "failed", 40, 4, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	if fired := failureScript.firedSnapshot(); len(fired) != 2 || fired[1] != "terminal" {
		t.Fatalf("terminal persistence fault script=%v want [retryable terminal]", fired)
	}
	if err := assertProblem5FailureRecords(failureScript.recordsSnapshot(), []problem5FailureRecord{
		{Mode: "retryable", Stage: "persist_candidate", JobID: 1, Attempt: 2, Candidate: "problem5-fifo-head-20"},
		{Mode: "terminal", Stage: "persist_candidate", JobID: 1, Attempt: 4, Candidate: "problem5-fifo-head-40"},
	}); err != nil {
		t.Fatal(err)
	}

	for callNumber := 5; callNumber <= 7; callNumber++ {
		resume.Signal()
		waitProblem5Call(t, calls, callNumber)
		waitProblem5Process(t, resumeProcessed)
	}
	if problem5HasEventForJob(logs.String(), "job_started", 2) {
		t.Fatal("FIFO successor started while failed head was blocked")
	}
	assertProblem5BlockedLogSequence(t, logs.String(), 1)

	resume.Stop()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, db = reopenProblem5SchedulerStore(t, path, clock, failureScript)
	resume = newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(resume.Signal)
	store.SetRecheckReconciledWake(resume.SignalReconciled)
	resume.afterProcess = func() { resumeProcessed <- struct{}{} }
	productionRunner = resume.runner
	retryRunRelease := make(chan struct{})
	resume.runner = func(ctx context.Context, _ int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		failureScript.arm(problem5FailureForCall(call))
		calls <- call
		if call == 9 {
			select {
			case <-retryRunRelease:
			case <-ctx.Done():
				return alertlifecycle.RecheckBatchResult{}, ctx.Err()
			}
		}
		return productionRunner(ctx, 20)
	}
	// Reopen first and prove the failed head still blocks the FIFO queue.
	resume.Start()
	waitProblem5Call(t, calls, 8)
	waitProblem5Process(t, resumeProcessed)
	if err := assertProblem5SchedulerPhase(db, 1, "failed", 40, 4, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	retryAction, retryErr := store.RetryRecheckJobContext(context.Background(), 1, "problem5-review-retry", "review retry", nil, clock.now.Add(time.Minute))
	if retryErr != nil || retryAction.JobID != 1 || retryAction.CurrentStatus != "queued" {
		t.Fatalf("retry action=%+v err=%v", retryAction, retryErr)
	}
	waitProblem5Call(t, calls, 9)
	if err := assertProblem5SchedulerPhase(db, 1, "queued", 40, 4, 1, 0, 1); err != nil {
		t.Fatal(err)
	}
	// The reopened scheduler consumes the operator wake and the stateful script's third fault.
	close(retryRunRelease)
	waitProblem5Process(t, resumeProcessed)
	if problem5JobStatus(t, db, 1) != "failed" || problem5HasEventForJob(logs.String(), "job_started", 2) {
		t.Fatalf("retry did not produce the same blocked head: status=%s logs=%s", problem5JobStatus(t, db, 1), logs.String())
	}
	var attempts int
	if err := db.QueryRow(`SELECT attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 5 {
		t.Fatalf("second terminal head attempt_count=%d want 5", attempts)
	}
	if err := assertProblem5SchedulerPhase(db, 1, "failed", 40, 5, 1, 0, 1); err != nil {
		t.Fatal(err)
	}
	if fired := failureScript.firedSnapshot(); len(fired) != 3 || fired[2] != "terminal" {
		t.Fatalf("retry-after-reopen fault script=%v want [retryable terminal terminal]", fired)
	}
	if err := assertProblem5FailureRecords(failureScript.recordsSnapshot(), []problem5FailureRecord{
		{Mode: "retryable", Stage: "persist_candidate", JobID: 1, Attempt: 2, Candidate: "problem5-fifo-head-20"},
		{Mode: "terminal", Stage: "persist_candidate", JobID: 1, Attempt: 4, Candidate: "problem5-fifo-head-40"},
		{Mode: "terminal", Stage: "persist_candidate", JobID: 1, Attempt: 5, Candidate: "problem5-fifo-head-40"},
	}); err != nil {
		t.Fatal(err)
	}

	abandonAction, abandonErr := store.AbandonRecheckJobContext(context.Background(), 1, "problem5-review-abandon", "review abandon", nil, clock.now.Add(2*time.Minute))
	if abandonErr != nil || abandonAction.JobID != 1 || abandonAction.CurrentStatus != "abandoned" {
		t.Fatalf("abandon action=%+v err=%v", abandonAction, abandonErr)
	}
	resume.Stop()
	successorScheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(successorScheduler.Signal)
	store.SetRecheckReconciledWake(successorScheduler.SignalReconciled)
	successorProcessed := make(chan struct{}, 4)
	successorScheduler.afterProcess = func() { successorProcessed <- struct{}{} }
	successorScheduler.Start()
	waitProblem5Process(t, successorProcessed)
	if problem5JobStatus(t, db, 1) != "abandoned" || problem5JobStatus(t, db, 2) != "completed" || !problem5HasEventForJob(logs.String(), "job_started", 2) {
		t.Fatalf("abandon did not release FIFO successor: head=%s successor=%s logs=%s", problem5JobStatus(t, db, 1), problem5JobStatus(t, db, 2), logs.String())
	}
	if err := assertProblem5SchedulerPhase(db, 1, "abandoned", 40, 5, 1, 0, 2); err != nil {
		t.Fatal(err)
	}
	for _, action := range []struct{ requestID, name string }{{"problem5-review-retry", "retry"}, {"problem5-review-abandon", "abandon"}} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions WHERE job_id=1 AND request_id=? AND action=? AND applied_result<>''`, action.requestID, action.name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("operator action request=%s action=%s count=%d", action.requestID, action.name, count)
		}
	}
	successorScheduler.Stop()
}

type problem5FailureScript struct {
	mu     sync.Mutex
	active problem5FailureRecord
	fired  []problem5FailureRecord
}

type problem5FailureRecord struct {
	Mode      string
	Stage     string
	JobID     int
	Attempt   int
	Candidate string
}

func (s *problem5FailureScript) fault(stage string) error {
	if stage != "persist_candidate" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active.Mode == "" {
		return nil
	}
	mode := s.active
	s.active = problem5FailureRecord{}
	mode.Stage = stage
	s.fired = append(s.fired, mode)
	switch mode.Mode {
	case "retryable":
		return schedulerSQLiteError(5)
	case "terminal":
		return errors.New("problem5 scheduler terminal failure")
	default:
		return fmt.Errorf("unknown Problem 5 failure mode %q", mode.Mode)
	}
}

func (s *problem5FailureScript) arm(record problem5FailureRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = record
}

func (s *problem5FailureScript) firedSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]string, 0, len(s.fired))
	for _, record := range s.fired {
		result = append(result, record.Mode)
	}
	return result
}

func (s *problem5FailureScript) recordsSnapshot() []problem5FailureRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]problem5FailureRecord(nil), s.fired...)
}

func assertProblem5FailureRecords(actual, expected []problem5FailureRecord) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("failure script records=%d want=%d: %v", len(actual), len(expected), actual)
	}
	for index := range expected {
		if actual[index] != expected[index] {
			return fmt.Errorf("failure script record[%d]=%+v want %+v", index, actual[index], expected[index])
		}
	}
	return nil
}

func problem5FailureForCall(call int) problem5FailureRecord {
	switch call {
	case 2:
		return problem5FailureRecord{Mode: "retryable", JobID: 1, Attempt: 2, Candidate: "problem5-fifo-head-20"}
	case 4:
		return problem5FailureRecord{Mode: "terminal", JobID: 1, Attempt: 4, Candidate: "problem5-fifo-head-40"}
	case 9:
		return problem5FailureRecord{Mode: "terminal", JobID: 1, Attempt: 5, Candidate: "problem5-fifo-head-40"}
	default:
		return problem5FailureRecord{}
	}
}

func installProblem5FailureScript(store *alertlifecycle.Store, clock *reviewSchedulerClock, script *problem5FailureScript) {
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		RecheckFault: script.fault,
		Now:          func() time.Time { return clock.now },
	})
}

func reopenProblem5SchedulerStore(t *testing.T, path string, clock *reviewSchedulerClock, script *problem5FailureScript) (*alertlifecycle.Store, *sql.DB) {
	t.Helper()
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Cleanup(func() { _ = store.Close() })
	installProblem5FailureScript(store, clock, script)
	return store, db
}

func TestProblem5ReviewSchedulerRetryWaitSurvivesRestart(t *testing.T) {
	store, db, _, _ := seedProblem5SchedulerFollower(t)
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	first := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(first.Signal)
	store.SetRecheckReconciledWake(first.SignalReconciled)
	faultEnabled := true
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{RecheckFault: func(stage string) error {
		if stage == "persist_candidate" && faultEnabled {
			return schedulerSQLiteError(5)
		}
		return nil
	}, Now: func() time.Time { return clock.now }})
	calls := make(chan int, 4)
	productionRunner := first.runner
	first.runner = func(ctx context.Context, batchSize int) (alertlifecycle.RecheckBatchResult, error) {
		calls <- 1
		return productionRunner(ctx, batchSize)
	}
	first.Start()
	waitProblem5Call(t, calls, 1)
	if got := <-clock.timer.resetEvents; got < time.Second || got >= 2*time.Second {
		t.Fatalf("retry_wait timer=%s want approximately 1s", got)
	}
	if status := problem5JobStatus(t, db, 1); status != "retry_wait" {
		t.Fatalf("persisted retry status=%s", status)
	}
	first.Stop()
	faultEnabled = false

	var nextAttemptRaw string
	if err := db.QueryRow(`SELECT next_attempt_at FROM silence_recheck_jobs WHERE job_id=1`).Scan(&nextAttemptRaw); err != nil {
		t.Fatal(err)
	}
	nextAttemptAt, err := time.Parse(time.RFC3339Nano, nextAttemptRaw)
	if err != nil {
		t.Fatal(err)
	}
	clock.now = nextAttemptAt
	second := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(second.Signal)
	store.SetRecheckReconciledWake(second.SignalReconciled)
	secondDone := make(chan struct{}, 2)
	second.afterProcess = func() { secondDone <- struct{}{} }
	secondCalls := make(chan int, 4)
	productionRunner = second.runner
	second.runner = func(ctx context.Context, batchSize int) (alertlifecycle.RecheckBatchResult, error) {
		secondCalls <- 1
		return productionRunner(ctx, batchSize)
	}
	second.Start()
	go func() {
		select {
		case clock.timer.ch <- clock.now:
		case <-time.After(5 * time.Second):
		}
	}()
	waitProblem5Call(t, secondCalls, 1)
	waitProblem5Process(t, secondDone)
	if status := problem5JobStatus(t, db, 1); status != "completed" {
		t.Fatalf("restarted retry_wait status=%s", status)
	}
	second.Stop()
}

func TestProblem5ReviewRegisteredRouterOperatorActions(t *testing.T) {
	for _, action := range []string{"retry", "abandon"} {
		t.Run(action, func(t *testing.T) {
			store, db, now, _ := seedProblem5SchedulerFollower(t)
			faultEnabled := true
			store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{RecheckFault: func(stage string) error {
				if stage == "persist_candidate" && faultEnabled {
					return errors.New("problem5 router terminal failure")
				}
				return nil
			}})
			previousStore, previousConfig, previousScheduler := lifecycleStore, config.Config, lifecycleRecheckScheduler
			defer func() {
				store.SetRecheckIntegrationTestHooks(nil)
				lifecycleStore, config.Config, lifecycleRecheckScheduler = previousStore, previousConfig, previousScheduler
			}()
			lifecycleStore = store
			config.Config.Listen.Prefix = "/problem5"
			config.Config.Authentication.Enabled = true
			config.Config.Authentication.Header.Name = "X-Problem5-User"
			config.Config.Authentication.Header.ValueRegex = "(.+)"
			clock := &reviewSchedulerClock{now: now}
			scheduler := newRecheckScheduler(store, clock)
			lifecycleRecheckScheduler = scheduler
			store.SetRecheckCommitWake(scheduler.Signal)
			store.SetRecheckReconciledWake(scheduler.SignalReconciled)
			processed := make(chan struct{}, 4)
			scheduler.afterProcess = func() { processed <- struct{}{} }
			scheduler.Start()
			waitProblem5Process(t, processed)
			faultEnabled = false
			scheduler.Stop()

			router := chi.NewRouter()
			setupRouter(router, nil)
			requestID := "problem5-router-" + action
			body := fmt.Sprintf(`{"requestId":%q}`, requestID)
			if action == "abandon" {
				body = fmt.Sprintf(`{"requestId":%q,"reason":"router abandon"}`, requestID)
			}
			request := httptest.NewRequest(http.MethodPost, "/problem5/alerts-history/recheck/1/"+action, strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(recheckOperatorRequestHeader, "recheck-operator")
			request.Header.Set("X-Problem5-User", "alice")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("router action=%s status=%d body=%s", action, response.Code, response.Body.String())
			}
			var payload map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			wantStatus := "queued"
			if action == "abandon" {
				wantStatus = "abandoned"
			}
			if payload["status"] != "success" || payload["jobId"] != float64(1) || payload["action"] != action || payload["requestId"] != requestID || payload["applied"] != true || payload["currentStatus"] != wantStatus {
				t.Fatalf("router action payload=%v", payload)
			}
			resume := newRecheckScheduler(store, clock)
			lifecycleRecheckScheduler = resume
			store.SetRecheckCommitWake(resume.Signal)
			store.SetRecheckReconciledWake(resume.SignalReconciled)
			resumeProcessed := make(chan struct{}, 4)
			resume.afterProcess = func() { resumeProcessed <- struct{}{} }
			resume.Start()
			waitProblem5Process(t, resumeProcessed)
			if action == "retry" {
				if status := problem5JobStatus(t, db, 1); status != "completed" {
					t.Fatalf("router retry final status=%s", status)
				}
			} else if status := problem5JobStatus(t, db, 1); status != "abandoned" {
				t.Fatalf("router abandon final status=%s", status)
			}
			if problem5JobStatus(t, db, 2) != "completed" {
				waitProblem5Process(t, resumeProcessed)
			}
			if status := problem5JobStatus(t, db, 2); status != "completed" {
				t.Fatalf("router %s successor final status=%s", action, status)
			}
			var actor, storedAction string
			if err := db.QueryRow(`SELECT actor,action FROM silence_recheck_job_operator_actions WHERE request_id=?`, requestID).Scan(&actor, &storedAction); err != nil {
				t.Fatal(err)
			}
			if actor != "alice" || storedAction != action {
				t.Fatalf("router operator row actor=%q action=%q", actor, storedAction)
			}
			resume.Stop()
		})
	}
}

func seedProblem5SchedulerFollower(t *testing.T) (*alertlifecycle.Store, *sql.DB, time.Time, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "problem5-scheduler.sqlite")
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
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	headLabels := `{"alertname":"Problem5Head","service":"review3"}`
	for i := 0; i < 41; i++ {
		alertID := "problem5-fifo-head"
		if i > 0 {
			alertID = fmt.Sprintf("problem5-fifo-head-%02d", i)
		}
		if err = store.UpsertCurrentAlert(alertlifecycle.AlertCurrent{
			AlertID: alertID, State: alertlifecycle.StateInactive, RawState: alertlifecycle.StateActive,
			Receiver: "review3-team", GroupID: "review3-group", GroupLabelsJSON: `{"alertname":"Problem5"}`,
			LabelsJSON: headLabels, MatchLabelsJSON: &headLabels, AnnotationsJSON: `{}`,
			StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
			RelevantUpstreams: `["review3-am"]`, RelevantSources: `[{"name":"review3-am","cluster":"review3-cluster"}]`, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	labels := `{"alertname":"Review3Follower","service":"review3"}`
	follower := alertlifecycle.LiveAlertSnapshot{
		AlertID: "problem5-fifo-successor", State: alertlifecycle.StateActive, Receiver: "review3-team", GroupID: "review3-group",
		GroupLabelsJSON: `{"alertname":"Review3"}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
		StartsAt: now.Add(-time.Hour), LastSeenAt: now.Add(time.Minute), RelevantUpstreams: []string{"review3-am"},
		RelevantSources: []alertlifecycle.SourceRef{{Name: "review3-am", Cluster: "review3-cluster"}},
	}
	sources := []alertlifecycle.SourceSilenceState{{
		SourceName: "review3-am", ClusterID: "review3-cluster", SyncStatus: "success",
		Silences: []alertlifecycle.SourceSilenceSnapshot{{
			SilenceID: "review3-scheduler-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
			Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "service", Value: "review3", IsEqual: true}},
		}},
	}}
	creation, err := store.ApplyPollCycle(alertlifecycle.PollCycleInput{LiveAlerts: []alertlifecycle.LiveAlertSnapshot{follower}, SuccessfulUpstreams: map[string]bool{"review3-am": true}, SilenceSources: sources, Timestamp: now.Add(time.Minute)})
	if err != nil || creation.RecheckJobsQueued != 1 {
		t.Fatalf("production head creation=%+v err=%v", creation, err)
	}
	creation, err = store.ApplyPollCycle(alertlifecycle.PollCycleInput{SuccessfulUpstreams: map[string]bool{"review3-am": true}, SilenceSources: sources, Timestamp: now.Add(2 * time.Minute)})
	if err != nil || creation.RecheckJobsAlertBecameEligible != 1 {
		t.Fatalf("FIFO successor creation=%+v err=%v", creation, err)
	}
	return store, db, now, path
}

func waitProblem5Call(t *testing.T, calls <-chan int, want int) {
	t.Helper()
	select {
	case got := <-calls:
		if got != want {
			t.Fatalf("runner call=%d want %d", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for runner call %d", want)
	}
}

func waitProblem5Process(t *testing.T, processed <-chan struct{}) {
	t.Helper()
	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scheduler process completion")
	}
}

func waitProblem5Signal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for lifecycle phase barrier")
	}
}

func problem5JobStatus(t *testing.T, db *sql.DB, jobID int) string {
	t.Helper()
	var status string
	if err := db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func assertProblem5SchedulerPhase(db *sql.DB, jobID int, wantStatus string, wantProcessed, wantAttempts, wantRetry, wantConsecutive, wantActions int) error {
	for _, tableQuery := range []string{
		`SELECT * FROM silence_recheck_jobs WHERE job_id=?`,
		`SELECT * FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`,
		`SELECT * FROM silence_recheck_job_attempts WHERE job_id=? ORDER BY attempt_number`,
	} {
		rows, err := captureReplayRows(db, tableQuery, jobID)
		if err != nil {
			return err
		}
		if rows == "" {
			return fmt.Errorf("phase %s missing complete row capture for %q", wantStatus, tableQuery)
		}
	}
	if _, err := captureReplayRows(db, `SELECT * FROM silence_recheck_job_operator_actions WHERE job_id=? ORDER BY action_id`, jobID); err != nil {
		return err
	}
	for _, tableQuery := range []string{
		`SELECT * FROM alert_events ORDER BY id`,
		`SELECT * FROM lifecycle_writer_commits ORDER BY operation_token`,
	} {
		if _, err := captureReplayRows(db, tableQuery); err != nil {
			return err
		}
	}
	var status string
	var processed, attempts, retry, consecutive int
	if err := db.QueryRow(`SELECT status,processed_candidates,attempt_count,retry_count,consecutive_retry_count FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&status, &processed, &attempts, &retry, &consecutive); err != nil {
		return err
	}
	if status != wantStatus || processed != wantProcessed || attempts != wantAttempts || retry != wantRetry || consecutive != wantConsecutive {
		return fmt.Errorf("phase job=%d status=%s/%s processed=%d/%d attempts=%d/%d retry=%d/%d consecutive=%d/%d", jobID, status, wantStatus, processed, wantProcessed, attempts, wantAttempts, retry, wantRetry, consecutive, wantConsecutive)
	}
	var total, promoted, pending int
	if err := db.QueryRow(`SELECT COUNT(*),COALESCE(SUM(outcome='promoted'),0),COALESCE(SUM(outcome IS NULL),0) FROM silence_recheck_job_candidates WHERE job_id=?`, jobID).Scan(&total, &promoted, &pending); err != nil {
		return err
	}
	if total != 41 || promoted != wantProcessed || pending != total-wantProcessed {
		return fmt.Errorf("phase candidate partition total=%d promoted=%d pending=%d want=%d/%d/%d", total, promoted, pending, 41, wantProcessed, 41-wantProcessed)
	}
	var actions int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions WHERE job_id=?`, jobID).Scan(&actions); err != nil {
		return err
	}
	if actions != wantActions {
		return fmt.Errorf("phase operator actions=%d want=%d", actions, wantActions)
	}
	return nil
}

func assertProblem5BlockedLogSequence(t *testing.T, raw string, jobID int64) {
	t.Helper()
	levels := []string{}
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil || record["event"] != "failed_job_blocks_queue" {
			continue
		}
		if record["job_id"] != float64(jobID) || record["status"] != "failed" {
			t.Fatalf("blocked event=%v", record)
		}
		levels = append(levels, fmt.Sprint(record["level"]))
	}
	if len(levels) != 3 || levels[0] != "WARN" || levels[1] != "DEBUG" || levels[2] != "DEBUG" {
		t.Fatalf("blocked event levels=%v want [WARN DEBUG DEBUG]", levels)
	}
}

func problem5HasEventForJob(raw, event string, jobID int64) bool {
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil || record["event"] != event {
			continue
		}
		if record["job_id"] == float64(jobID) {
			return true
		}
	}
	return false
}
