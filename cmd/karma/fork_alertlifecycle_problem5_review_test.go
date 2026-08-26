package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestProblem5CompleteDeterministicLifecycleSequence(t *testing.T) {
	store, db, now, path := seedProblem5SchedulerFollower(t)
	if err := assertProblem5DeterministicCreation(db, now); err != nil {
		t.Fatal(err)
	}
	clock := &reviewSchedulerClock{now: now}
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
	if err := assertProblem5DeterministicImmutableInput(db, now); err != nil {
		t.Fatal(err)
	}
	if fired := failureScript.firedSnapshot(); len(fired) != 1 || fired[0] != "retryable" {
		t.Fatalf("retry scheduling fault script=%v want [retryable]", fired)
	}
	if err := assertProblem5FailureRecords(failureScript.recordsSnapshot(), []problem5FailureRecord{{Mode: "retryable", Stage: "persist_candidate", JobID: 1, Attempt: 2, Candidate: "problem5-fifo-head-20"}}); err != nil {
		t.Fatal(err)
	}
	if err := createProblem5SchedulerSuccessor(store, now); err != nil {
		t.Fatal(err)
	}
	if err := assertProblem5DeterministicSuccessor(db); err != nil {
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
	if err = assertProblem5DeterministicImmutableInput(db, now); err != nil {
		t.Fatal(err)
	}
	inputBeforeRestart, err := captureReplayInputRows(db, 1)
	if err != nil {
		t.Fatal(err)
	}
	scheduler.Stop()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, db = reopenProblem5SchedulerStore(t, path, clock, failureScript)
	inputAfterRestart, err := captureReplayInputRows(db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if inputAfterRestart != inputBeforeRestart {
		t.Fatal("deterministic pending immutable input changed across Store restart")
	}
	if err = assertProblem5DeterministicImmutableInput(db, now); err != nil {
		t.Fatal(err)
	}
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
	if err = assertProblem5SchedulerPhase(db, 1, "running", 40, 3, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	close(resumeFirstBatchContinue)
	waitProblem5Call(t, calls, 4)
	waitProblem5Process(t, resumeProcessed)
	if err = db.QueryRow(`SELECT processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&processedCandidates); err != nil {
		t.Fatal(err)
	}
	if processedCandidates != 40 || problem5JobStatus(t, db, 1) != "failed" {
		t.Fatalf("restart/resume state=%s processed=%d want failed/40", problem5JobStatus(t, db, 1), processedCandidates)
	}
	if err = assertProblem5SchedulerPhase(db, 1, "failed", 40, 4, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	if fired := failureScript.firedSnapshot(); len(fired) != 2 || fired[1] != "terminal" {
		t.Fatalf("terminal persistence fault script=%v want [retryable terminal]", fired)
	}
	if err = assertProblem5FailureRecords(failureScript.recordsSnapshot(), []problem5FailureRecord{
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
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, db = reopenProblem5SchedulerStore(t, path, clock, failureScript)
	resume = newRecheckScheduler(store, clock)
	var wakeMu sync.Mutex
	wakeCount := 0
	phaseEvents := make([]string, 0, 32)
	recordPhase := func(event string) {
		wakeMu.Lock()
		phaseEvents = append(phaseEvents, event)
		wakeMu.Unlock()
	}
	phaseSnapshot := func() string {
		wakeMu.Lock()
		defer wakeMu.Unlock()
		return strings.Join(phaseEvents, "\n")
	}
	signalCommitWake := func() {
		wakeMu.Lock()
		wakeCount++
		phaseEvents = append(phaseEvents, "wake:commit")
		wakeMu.Unlock()
		resume.Signal()
	}
	signalReconciledWake := func() {
		wakeMu.Lock()
		wakeCount++
		phaseEvents = append(phaseEvents, "wake:reconciled")
		wakeMu.Unlock()
		resume.SignalReconciled()
	}
	wakeSnapshot := func() int {
		wakeMu.Lock()
		defer wakeMu.Unlock()
		return wakeCount
	}
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		RecheckFault:      failureScript.fault,
		Now:               func() time.Time { return clock.now },
		WorkerBarrier:     func(stage string) { recordPhase("worker:" + stage) },
		OperatorFinalizer: func() { recordPhase("operator_finalizer") },
		FailureFinalizer:  func() { recordPhase("failure_finalizer") },
	})
	store.SetRecheckCommitWake(signalCommitWake)
	store.SetRecheckReconciledWake(signalReconciledWake)
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
	if err = assertProblem5SchedulerPhase(db, 1, "failed", 40, 4, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	previousStore, previousConfig, previousScheduler := lifecycleStore, config.Config, lifecycleRecheckScheduler
	t.Cleanup(func() {
		lifecycleStore, config.Config, lifecycleRecheckScheduler = previousStore, previousConfig, previousScheduler
	})
	problem5ConfigureRouter(t, store, resume)
	beforeRetry, err := captureProblem5OperatorSnapshot(db)
	if err != nil {
		t.Fatal(err)
	}
	beforeRetry.Phase = phaseSnapshot()
	wakesBeforeRetry := wakeSnapshot()
	firstRetryBody := problem5ServeOperatorAction(t, "retry", "problem5-review-retry", "review retry", "alice")
	afterRetry, err := captureProblem5OperatorSnapshot(db)
	if err != nil {
		t.Fatal(err)
	}
	afterRetry.Phase = phaseSnapshot()
	replayedRetryBody := problem5ServeOperatorAction(t, "retry", "problem5-review-retry", "review retry", "alice")
	afterRetryReplay, err := captureProblem5OperatorSnapshot(db)
	if err != nil {
		t.Fatal(err)
	}
	afterRetryReplay.Phase = phaseSnapshot()
	if !bytes.Equal(firstRetryBody, replayedRetryBody) {
		t.Fatalf("idempotent retry response changed: first=%s replay=%s", firstRetryBody, replayedRetryBody)
	}
	if err = assertProblem5OperatorReplay(afterRetry, afterRetryReplay); err != nil {
		t.Fatal(err)
	}
	if wakeSnapshot() != wakesBeforeRetry+1 {
		t.Fatalf("idempotent retry wake count=%d want=%d", wakeSnapshot(), wakesBeforeRetry+1)
	}
	if beforeRetry.Semantic == afterRetry.Semantic {
		t.Fatal("initial retry operator action had no durable semantic transition")
	}
	if err = assertProblem5OperatorActionSnapshot(beforeRetry, afterRetry, "problem5-review-retry", "retry"); err != nil {
		t.Fatal(err)
	}
	if wakeSnapshot() != wakesBeforeRetry+1 {
		t.Fatalf("retry wake count=%d want=%d", wakeSnapshot(), wakesBeforeRetry+1)
	}
	waitProblem5Call(t, calls, 9)
	if err = assertProblem5SchedulerPhase(db, 1, "queued", 40, 4, 1, 0, 1); err != nil {
		t.Fatal(err)
	}
	// The reopened scheduler consumes the operator wake and the stateful script's third fault.
	close(retryRunRelease)
	waitProblem5Process(t, resumeProcessed)
	if problem5JobStatus(t, db, 1) != "failed" || problem5HasEventForJob(logs.String(), "job_started", 2) {
		t.Fatalf("retry did not produce the same blocked head: status=%s logs=%s", problem5JobStatus(t, db, 1), logs.String())
	}
	var attempts int
	if err = db.QueryRow(`SELECT attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 5 {
		t.Fatalf("second terminal head attempt_count=%d want 5", attempts)
	}
	if err = assertProblem5SchedulerPhase(db, 1, "failed", 40, 5, 1, 0, 1); err != nil {
		t.Fatal(err)
	}
	if fired := failureScript.firedSnapshot(); len(fired) != 3 || fired[2] != "terminal" {
		t.Fatalf("retry-after-reopen fault script=%v want [retryable terminal terminal]", fired)
	}
	if err = assertProblem5FailureRecords(failureScript.recordsSnapshot(), []problem5FailureRecord{
		{Mode: "retryable", Stage: "persist_candidate", JobID: 1, Attempt: 2, Candidate: "problem5-fifo-head-20"},
		{Mode: "terminal", Stage: "persist_candidate", JobID: 1, Attempt: 4, Candidate: "problem5-fifo-head-40"},
		{Mode: "terminal", Stage: "persist_candidate", JobID: 1, Attempt: 5, Candidate: "problem5-fifo-head-40"},
	}); err != nil {
		t.Fatal(err)
	}

	resume.Stop()
	beforeAbandon, err := captureProblem5OperatorSnapshot(db)
	if err != nil {
		t.Fatal(err)
	}
	beforeAbandon.Phase = phaseSnapshot()
	wakesBeforeAbandon := wakeSnapshot()
	problem5ServeOperatorAction(t, "abandon", "problem5-review-abandon", "review abandon", "alice")
	afterAbandon, err := captureProblem5OperatorSnapshot(db)
	if err != nil {
		t.Fatal(err)
	}
	afterAbandon.Phase = phaseSnapshot()
	if err = assertProblem5OperatorActionSnapshot(beforeAbandon, afterAbandon, "problem5-review-abandon", "abandon"); err != nil {
		t.Fatal(err)
	}
	if wakeSnapshot() != wakesBeforeAbandon+1 {
		t.Fatalf("abandon wake count=%d want=%d", wakeSnapshot(), wakesBeforeAbandon+1)
	}
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
	if err = assertProblem5SchedulerPhase(db, 1, "abandoned", 40, 5, 1, 0, 2); err != nil {
		t.Fatal(err)
	}
	for _, action := range []struct{ requestID, name string }{{"problem5-review-retry", "retry"}, {"problem5-review-abandon", "abandon"}} {
		var count int
		if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions WHERE job_id=1 AND request_id=? AND action=? AND applied_result<>''`, action.requestID, action.name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("operator action request=%s action=%s count=%d", action.requestID, action.name, count)
		}
	}
	if err = assertProblem5DeterministicFinalRows(db, now, nextAttemptAt); err != nil {
		t.Fatal(err)
	}
	if err = assertProblem5DeterministicRecheckSuppressionEvidence(db, now, nextAttemptAt); err != nil {
		t.Fatal(err)
	}
	lifecycleStore, config.Config.AlertLifecycle.Enabled, lifecycleRecheckScheduler = store, true, successorScheduler
	if merged := mergeLifecycleStoredAlertsContext(context.Background(), nil, true); len(merged) == 0 || len(merged[0].Alerts) == 0 {
		t.Fatal("deterministic lifecycle merge returned no alerts")
	}
	beforeReadOnly, err := captureProblem5OperatorSnapshot(db)
	if err != nil {
		t.Fatal(err)
	}
	statsBody := problem5ServeReadOnlyEndpoint(t, "/problem5/alerts-history/stats")
	warningBody := problem5ServeReadOnlyEndpoint(t, "/problem5/alerts-history/warning")
	afterReadOnly, err := captureProblem5OperatorSnapshot(db)
	if err != nil {
		t.Fatal(err)
	}
	if beforeReadOnly != afterReadOnly {
		t.Fatal("stats/warning reads changed durable lifecycle rows or writer ledger")
	}
	expectedStats, statsErr := problem5DeterministicStats(db, path)
	if statsErr != nil {
		t.Fatal(statsErr)
	}
	if err := assertProblem5ExactStatsWarningResponses(statsBody, warningBody, expectedStats); err != nil {
		t.Fatal(err)
	}
	successorScheduler.Stop()
}

func problem5ConfigureRouter(t *testing.T, store *alertlifecycle.Store, scheduler *recheckScheduler) {
	t.Helper()
	lifecycleStore = store
	lifecycleRecheckScheduler = scheduler
	config.Config.Listen.Prefix = "/problem5"
	config.Config.Authentication.Enabled = true
	config.Config.Authentication.Header.Name = "X-Problem5-User"
	config.Config.Authentication.Header.ValueRegex = "(.+)"
}

func problem5ServeOperatorAction(t *testing.T, action, requestID, reason, actor string) []byte {
	t.Helper()
	router := chi.NewRouter()
	setupRouter(router, nil)
	body := fmt.Sprintf(`{"requestId":%q,"reason":%q}`, requestID, reason)
	request := httptest.NewRequest(http.MethodPost, "/problem5/alerts-history/recheck/1/"+action, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(recheckOperatorRequestHeader, "recheck-operator")
	request.Header.Set("X-Problem5-User", actor)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("registered operator action=%s status=%d body=%s", action, response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	wantStatus := "queued"
	if action == "abandon" {
		wantStatus = "abandoned"
	}
	if len(payload) != 6 || payload["status"] != "success" || payload["jobId"] != float64(1) || payload["action"] != action || payload["requestId"] != requestID || payload["applied"] != true || payload["currentStatus"] != wantStatus {
		t.Fatalf("registered operator action payload=%v", payload)
	}
	expectedBody, err := json.Marshal(struct {
		Status        string `json:"status"`
		JobID         int64  `json:"jobId"`
		Action        string `json:"action"`
		RequestID     string `json:"requestId"`
		Applied       bool   `json:"applied"`
		CurrentStatus string `json:"currentStatus"`
	}{Status: "success", JobID: 1, Action: action, RequestID: requestID, Applied: true, CurrentStatus: wantStatus})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response.Body.Bytes(), expectedBody) {
		t.Fatalf("registered operator action body=%s want=%s", response.Body.Bytes(), expectedBody)
	}
	if response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Cache-Control") != "no-cache, no-store, must-revalidate" {
		t.Fatalf("registered operator action headers content-type=%q cache-control=%q", response.Header().Get("Content-Type"), response.Header().Get("Cache-Control"))
	}
	return append([]byte(nil), response.Body.Bytes()...)
}

func problem5ServeReadOnlyEndpoint(t *testing.T, path string) []byte {
	t.Helper()
	router := chi.NewRouter()
	setupRouter(router, nil)
	request := httptest.NewRequest(http.MethodGet, path, http.NoBody)
	request.Header.Set("X-Problem5-User", "alice")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("registered endpoint=%s status=%d body=%s", path, response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Cache-Control") != "no-cache, no-store, must-revalidate" {
		t.Fatalf("registered endpoint=%s headers content-type=%q cache-control=%q", path, response.Header().Get("Content-Type"), response.Header().Get("Cache-Control"))
	}
	return response.Body.Bytes()
}

type problem5OperatorSnapshot struct {
	Semantic string
	Ledger   string
	Phase    string
}

func captureProblem5OperatorSnapshot(db *sql.DB) (problem5OperatorSnapshot, error) {
	tables := []string{
		"alert_states", "alert_target_states", "alert_events", "silence_recheck_jobs",
		"silence_recheck_job_rules", "silence_recheck_job_candidates", "silence_recheck_job_attempts",
		"silence_recheck_job_operator_actions", "alert_suppression_cause_history", "alert_suppression_cause_members",
	}
	var semantic strings.Builder
	for _, table := range tables {
		rows, err := captureReplayRows(db, `SELECT * FROM `+table+` ORDER BY rowid`)
		if err != nil {
			return problem5OperatorSnapshot{}, err
		}
		fmt.Fprintf(&semantic, "--%s--\n%s\n", table, rows)
	}
	ledger, err := captureReplayRows(db, `SELECT * FROM lifecycle_writer_commits ORDER BY rowid`)
	if err != nil {
		return problem5OperatorSnapshot{}, err
	}
	return problem5OperatorSnapshot{Semantic: semantic.String(), Ledger: ledger}, nil
}

func assertProblem5OperatorReplay(before, after problem5OperatorSnapshot) error {
	if before.Semantic != after.Semantic {
		return errors.New("idempotent operator replay changed semantic durable rows")
	}
	if before.Phase != after.Phase {
		return errors.New("idempotent operator replay changed volatile phase state")
	}
	if before.Ledger == after.Ledger {
		return errors.New("idempotent operator replay did not record its operational writer ledger row")
	}
	if !strings.HasPrefix(after.Ledger, before.Ledger) {
		return fmt.Errorf("idempotent operator replay changed previous writer ledger rows: before=%q after=%q", before.Ledger, after.Ledger)
	}
	added := strings.TrimPrefix(after.Ledger, before.Ledger)
	if strings.Count(added, "recheck_operator_action") != 1 {
		return fmt.Errorf("idempotent operator replay ledger addition=%q", added)
	}
	return nil
}

func assertProblem5OperatorActionSnapshot(before, after problem5OperatorSnapshot, requestID, action string) error {
	if before.Semantic == after.Semantic {
		return fmt.Errorf("operator %s request=%q did not change the durable semantic snapshot", action, requestID)
	}
	if !strings.HasPrefix(after.Ledger, before.Ledger) || strings.Count(strings.TrimPrefix(after.Ledger, before.Ledger), "recheck_operator_action") != 1 {
		return fmt.Errorf("operator %s request=%q changed the writer ledger unexpectedly", action, requestID)
	}
	if strings.Count(after.Semantic, requestID) != 1 {
		return fmt.Errorf("operator %s request=%q lacks exactly one applied action delta", action, requestID)
	}
	delta, ok := strings.CutPrefix(after.Phase, before.Phase)
	if !ok || strings.Count(delta, "operator_finalizer") != 1 || strings.Count(delta, "wake:commit") != 1 || strings.Count(delta, "wake:reconciled") != 0 || strings.Index(delta, "operator_finalizer") > strings.Index(delta, "wake:commit") {
		return fmt.Errorf("operator %s request=%q phase delta=%q", action, requestID, delta)
	}
	return nil
}

func assertProblem5DeterministicCreation(db *sql.DB, now time.Time) error {
	expectedContext, expectedSourceFingerprint, expectedJobFingerprint, expectedCandidates, expectedRule, err := problem5DeterministicExpectedInput(now)
	if err != nil {
		return err
	}
	var jobID, total, processed, promoted, ineligible, missingLabels, incomplete, stale, attempts, retries, consecutive, recovery int
	var actualSourceFingerprint, actualJobFingerprint, trigger, status, contextJSON, cursor string
	var nextAttempt, firstMessage, firstClass, firstStage, firstAt, lastMessage, lastClass, lastStage, lastAt, abandonedAt, abandonedBy, abandonReason, createdAt, startedAt, finishedAt sql.NullString
	if err = db.QueryRow(`SELECT job_id,source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,COALESCE(cursor_alert_id,''),total_candidates,processed_candidates,promoted_candidates,skipped_ineligible,skipped_missing_labels,skipped_incomplete_sources,stale_conflicts,attempt_count,retry_count,consecutive_retry_count,next_attempt_at,first_error_message,first_error_class,first_error_stage,first_error_at,last_error_message,last_error_class,last_error_stage,last_error_at,recovery_count,abandoned_at,abandoned_by,abandon_reason,created_at,started_at,finished_at FROM silence_recheck_jobs WHERE job_id=1`).Scan(&jobID, &actualSourceFingerprint, &actualJobFingerprint, &contextJSON, &trigger, &status, &cursor, &total, &processed, &promoted, &ineligible, &missingLabels, &incomplete, &stale, &attempts, &retries, &consecutive, &nextAttempt, &firstMessage, &firstClass, &firstStage, &firstAt, &lastMessage, &lastClass, &lastStage, &lastAt, &recovery, &abandonedAt, &abandonedBy, &abandonReason, &createdAt, &startedAt, &finishedAt); err != nil {
		return err
	}
	createdWant := now.Add(time.Minute).UTC().Format(time.RFC3339Nano)
	if jobID != 1 || actualSourceFingerprint != expectedSourceFingerprint || actualJobFingerprint != expectedJobFingerprint || contextJSON != expectedContext || trigger != "source_context_changed" || status != "queued" || cursor != "" || total != len(expectedCandidates) || processed != 0 || promoted != 0 || ineligible != 0 || missingLabels != 0 || incomplete != 0 || stale != 0 || attempts != 0 || retries != 0 || consecutive != 0 || recovery != 0 || nextAttempt.Valid || firstMessage.Valid || firstClass.Valid || firstStage.Valid || firstAt.Valid || lastMessage.Valid || lastClass.Valid || lastStage.Valid || lastAt.Valid || abandonedAt.Valid || abandonedBy.Valid || abandonReason.Valid || createdAt.String != createdWant || startedAt.Valid || finishedAt.Valid {
		return fmt.Errorf("deterministic creator complete job id=%d source=%q/%q fingerprint=%q/%q trigger=%q status=%q total=%d/%d processed=%d counters=%d/%d/%d/%d/%d attempts=%d timestamps=%q/%q/%q", jobID, actualSourceFingerprint, expectedSourceFingerprint, actualJobFingerprint, expectedJobFingerprint, trigger, status, total, len(expectedCandidates), processed, promoted, ineligible, missingLabels, incomplete, stale, attempts, createdAt.String, startedAt.String, finishedAt.String)
	}
	if err = assertProblem5DeterministicRuleRow(db, expectedRule); err != nil {
		return err
	}
	rows, err := db.Query(`SELECT job_id,alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json,processed_at,outcome FROM silence_recheck_job_candidates WHERE job_id=1 ORDER BY alert_id,target_key`)
	if err != nil {
		return err
	}
	defer rows.Close()
	expectedByKey := expectedCandidatesByKey(expectedCandidates)
	seenCandidates := make(map[string]struct{}, len(expectedByKey))
	for rows.Next() {
		var actualJobID int64
		var alertID, targetKey string
		var actualAlert, actualTarget, actualLabels, actualRelevant, actualMembers, actualProcessed, actualOutcome sql.NullString
		if err = rows.Scan(&actualJobID, &alertID, &targetKey, &actualAlert, &actualTarget, &actualLabels, &actualRelevant, &actualMembers, &actualProcessed, &actualOutcome); err != nil {
			return err
		}
		key := alertID + "\x00" + targetKey
		want, ok := expectedByKey[key]
		if !ok || actualJobID != 1 || !problem5NullableEqual(actualAlert, want.ExpectedAlertLastSeenAt) || !problem5NullableEqual(actualTarget, want.ExpectedTargetLastSeenAt) || !problem5NullableEqual(actualLabels, want.MatchLabelsJSON) || actualRelevant.String != want.RelevantSourcesJSON || !problem5NullableEqual(actualMembers, want.TargetSourceMembersJSON) || actualProcessed.Valid || actualOutcome.Valid {
			return fmt.Errorf("deterministic creator candidate key=%q job=%d alert=%q target=%q labels=%q relevant=%q members=%q processed=%q outcome=%q", key, actualJobID, actualAlert.String, actualTarget.String, actualLabels.String, actualRelevant.String, actualMembers.String, actualProcessed.String, actualOutcome.String)
		}
		if _, duplicate := seenCandidates[key]; duplicate {
			return fmt.Errorf("deterministic creator duplicate candidate=%q", key)
		}
		seenCandidates[key] = struct{}{}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(seenCandidates) != len(expectedByKey) {
		return fmt.Errorf("deterministic creator candidate IDs=%d want 41", len(seenCandidates))
	}
	return nil
}

func assertProblem5DeterministicImmutableInput(db *sql.DB, now time.Time) error {
	expectedContext, expectedSourceFingerprint, expectedJobFingerprint, expectedCandidates, expectedRule, err := problem5DeterministicExpectedInput(now)
	if err != nil {
		return err
	}
	var actualSourceFingerprint, actualJobFingerprint, actualContext string
	if err = db.QueryRow(`SELECT source_context_fingerprint,job_fingerprint,source_context_json FROM silence_recheck_jobs WHERE job_id=1`).Scan(&actualSourceFingerprint, &actualJobFingerprint, &actualContext); err != nil {
		return err
	}
	if actualSourceFingerprint != expectedSourceFingerprint || actualJobFingerprint != expectedJobFingerprint || actualContext != expectedContext {
		return fmt.Errorf("deterministic immutable job input source=%q/%q fingerprint=%q/%q context=%q/%q", actualSourceFingerprint, expectedSourceFingerprint, actualJobFingerprint, expectedJobFingerprint, actualContext, expectedContext)
	}
	if err = assertProblem5DeterministicRuleRow(db, expectedRule); err != nil {
		return err
	}
	expectedByKey := expectedCandidatesByKey(expectedCandidates)
	rows, err := db.Query(`SELECT job_id,alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json FROM silence_recheck_job_candidates WHERE job_id=1 ORDER BY alert_id,target_key`)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := make(map[string]struct{}, len(expectedByKey))
	for rows.Next() {
		var jobID int64
		var alertID, targetKey string
		var actualAlert, actualTarget, actualLabels, actualRelevant, actualMembers sql.NullString
		if err = rows.Scan(&jobID, &alertID, &targetKey, &actualAlert, &actualTarget, &actualLabels, &actualRelevant, &actualMembers); err != nil {
			return err
		}
		key := alertID + "\x00" + targetKey
		want, ok := expectedByKey[key]
		if !ok || jobID != 1 || !problem5NullableEqual(actualAlert, want.ExpectedAlertLastSeenAt) || !problem5NullableEqual(actualTarget, want.ExpectedTargetLastSeenAt) || !problem5NullableEqual(actualLabels, want.MatchLabelsJSON) || actualRelevant.String != want.RelevantSourcesJSON || !problem5NullableEqual(actualMembers, want.TargetSourceMembersJSON) {
			return fmt.Errorf("deterministic immutable candidate key=%q job=%d alert=%q target=%q labels=%q relevant=%q members=%q", key, jobID, actualAlert.String, actualTarget.String, actualLabels.String, actualRelevant.String, actualMembers.String)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("deterministic immutable candidate duplicate=%q", key)
		}
		seen[key] = struct{}{}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(seen) != len(expectedByKey) {
		return fmt.Errorf("deterministic immutable candidate coverage=%d want=%d", len(seen), len(expectedByKey))
	}
	return nil
}

func assertProblem5DeterministicSuccessor(db *sql.DB) error {
	var predecessorContext, successorContext, successorFingerprint string
	var predecessorFingerprint string
	if err := db.QueryRow(`SELECT job_fingerprint,source_context_json FROM silence_recheck_jobs WHERE job_id=1`).Scan(&predecessorFingerprint, &predecessorContext); err != nil {
		return err
	}
	var successorTrigger, successorStatus string
	var successorTotal, successorProcessed int
	if err := db.QueryRow(`SELECT trigger_type,status,total_candidates,processed_candidates,job_fingerprint,source_context_json FROM silence_recheck_jobs WHERE job_id=2`).Scan(&successorTrigger, &successorStatus, &successorTotal, &successorProcessed, &successorFingerprint, &successorContext); err != nil {
		return err
	}
	if successorTrigger != "alert_became_eligible" || successorStatus != "queued" || successorTotal != 1 || successorProcessed != 0 || successorFingerprint == predecessorFingerprint || successorContext != predecessorContext {
		return fmt.Errorf("deterministic successor header trigger=%q status=%q total=%d processed=%d fingerprint=%q predecessor=%q contextEqual=%t", successorTrigger, successorStatus, successorTotal, successorProcessed, successorFingerprint, predecessorFingerprint, successorContext == predecessorContext)
	}
	var successorAlert, successorTarget, successorSources string
	if err := db.QueryRow(`SELECT alert_id,target_key,relevant_sources_json FROM silence_recheck_job_candidates WHERE job_id=2`).Scan(&successorAlert, &successorTarget, &successorSources); err != nil {
		return err
	}
	if successorAlert != "problem5-fifo-successor" || successorTarget != "review3-cluster" || successorSources != `[{"name":"review3-am","cluster":"review3-cluster"}]` {
		return fmt.Errorf("deterministic successor candidate alert=%q target=%q sources=%q", successorAlert, successorTarget, successorSources)
	}
	return nil
}

func assertProblem5DeterministicRecheckSuppressionEvidence(db *sql.DB, firstBatchAt, resumedBatchAt time.Time) error {
	var promoted int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=1 AND outcome='promoted'`).Scan(&promoted); err != nil {
		return err
	}
	if promoted != 40 {
		return fmt.Errorf("deterministic recheck promotions=%d want 40", promoted)
	}
	var targetHistoryCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM alert_target_suppression_cause_history WHERE recheck_job_id=1 AND source='silence_recheck' AND cause_status='known'`).Scan(&targetHistoryCount); err != nil {
		return err
	}
	if targetHistoryCount != 40 {
		return fmt.Errorf("deterministic target recheck suppression histories=%d want 40", targetHistoryCount)
	}
	wantFingerprint := problem5CauseFingerprint(1, 1)
	historyRows, err := db.Query(`SELECT history_id,alert_id,target,target_transition_event_id,recheck_job_id,source,cause_status,cause_fingerprint,observed_at,last_confirmed_at FROM alert_target_suppression_cause_history WHERE recheck_job_id=1 AND source='silence_recheck' ORDER BY alert_id`)
	if err != nil {
		return err
	}
	seenHistory := make(map[string]struct{}, 40)
	for historyRows.Next() {
		var historyID, transitionEventID, jobID int64
		var alertID, target, source, causeStatus, fingerprint, observedAt, lastConfirmedAt string
		if err = historyRows.Scan(&historyID, &alertID, &target, &transitionEventID, &jobID, &source, &causeStatus, &fingerprint, &observedAt, &lastConfirmedAt); err != nil {
			historyRows.Close()
			return err
		}
		wantObservedAt := firstBatchAt
		if index, parseErr := strconv.Atoi(strings.TrimPrefix(alertID, "problem5-fifo-head-")); parseErr == nil && index >= 20 {
			wantObservedAt = resumedBatchAt
		}
		if historyID == 0 || transitionEventID == 0 || jobID != 1 || target != "review3-cluster" || source != "silence_recheck" || causeStatus != "known" || fingerprint != wantFingerprint || observedAt != wantObservedAt.UTC().Format(time.RFC3339Nano) || lastConfirmedAt != observedAt {
			historyRows.Close()
			return fmt.Errorf("deterministic complete target history alert=%q history=%d target=%q event=%d job=%d source=%q status=%q fingerprint=%q observed=%q confirmed=%q", alertID, historyID, target, transitionEventID, jobID, source, causeStatus, fingerprint, observedAt, lastConfirmedAt)
		}
		if _, duplicate := seenHistory[alertID]; duplicate {
			historyRows.Close()
			return fmt.Errorf("deterministic duplicate target history alert=%q", alertID)
		}
		seenHistory[alertID] = struct{}{}
	}
	if err = historyRows.Err(); err != nil {
		historyRows.Close()
		return err
	}
	historyRows.Close()
	if len(seenHistory) != 40 {
		return fmt.Errorf("deterministic complete target history rows=%d want 40", len(seenHistory))
	}
	for index := 0; index < 40; index++ {
		alertID := "problem5-fifo-head"
		if index > 0 {
			alertID = fmt.Sprintf("problem5-fifo-head-%02d", index)
		}
		var targetHistoryID, targetJobID, targetEventID int64
		var target, targetSource, targetStatus, targetFingerprint string
		if err := db.QueryRow(`SELECT history_id,target,recheck_job_id,target_transition_event_id,source,cause_status,cause_fingerprint FROM alert_target_suppression_cause_history WHERE alert_id=? AND target=? AND source='silence_recheck'`, alertID, "review3-cluster").Scan(&targetHistoryID, &target, &targetJobID, &targetEventID, &targetSource, &targetStatus, &targetFingerprint); err != nil {
			return fmt.Errorf("deterministic target recheck suppression history alert=%s: %w", alertID, err)
		}
		if target != "review3-cluster" || targetJobID != 1 || targetEventID == 0 || targetSource != "silence_recheck" || targetStatus != "known" || targetFingerprint != wantFingerprint {
			return fmt.Errorf("deterministic target recheck suppression history alert=%s id=%d target=%q job=%d event=%d source=%q status=%q fingerprint=%q want=%q", alertID, targetHistoryID, target, targetJobID, targetEventID, targetSource, targetStatus, targetFingerprint, wantFingerprint)
		}
		var memberCount int
		if err := db.QueryRow(`SELECT COUNT(*) FROM alert_target_suppression_cause_members WHERE history_id=?`, targetHistoryID).Scan(&memberCount); err != nil {
			return err
		}
		if memberCount != 1 {
			return fmt.Errorf("deterministic target recheck suppression history=%d members=%d want 1", targetHistoryID, memberCount)
		}
		var memberID int64
		var memberSource, clusterID, silenceID, resolution string
		var ruleID, revisionID int64
		if err := db.QueryRow(`
SELECT m.member_id,m.source_name,r.cluster_id,r.silence_id,m.rule_id,m.revision_id,m.resolution_status
FROM alert_target_suppression_cause_members m
JOIN silence_rules r ON r.rule_id=m.rule_id
JOIN silence_revisions v ON v.revision_id=m.revision_id AND v.rule_id=m.rule_id
		WHERE m.history_id=?`, targetHistoryID).Scan(&memberID, &memberSource, &clusterID, &silenceID, &ruleID, &revisionID, &resolution); err != nil {
			return err
		}
		if memberID == 0 || memberSource != "review3-am" || clusterID != "review3-cluster" || silenceID != "review3-scheduler-silence" || ruleID != 1 || revisionID != 1 || resolution != "known" {
			return fmt.Errorf("deterministic recheck suppression member alert=%s source=%q cluster=%q silence=%q rule=%d revision=%d status=%q", alertID, memberSource, clusterID, silenceID, ruleID, revisionID, resolution)
		}
		var eventAlert, oldState, newState, oldRaw, newRaw, changedAt, reason string
		var oldLive, newLive, oldSuppressed, newSuppressed, oldResolved, newResolved int
		if err := db.QueryRow(`SELECT alert_id,old_state,new_state,old_raw_state,new_raw_state,old_is_live,new_is_live,old_is_suppressed,new_is_suppressed,old_is_resolved,new_is_resolved,changed_at,reason_code FROM alert_target_events WHERE id=?`, targetEventID).Scan(&eventAlert, &oldState, &newState, &oldRaw, &newRaw, &oldLive, &newLive, &oldSuppressed, &newSuppressed, &oldResolved, &newResolved, &changedAt, &reason); err != nil {
			return err
		}
		if eventAlert != alertID || oldState != "inactive" || newState != "suppressed" || oldRaw != "active" || newRaw != "active" || oldLive != 0 || newLive != 0 || oldSuppressed != 0 || newSuppressed != 1 || oldResolved != 0 || newResolved != 0 || reason != "silence_match_promoted" {
			return fmt.Errorf("deterministic target transition alert=%s event=%d old=%q/%q/%d/%d/%d new=%q/%q/%d/%d/%d reason=%q", alertID, targetEventID, oldState, oldRaw, oldLive, oldSuppressed, oldResolved, newState, newRaw, newLive, newSuppressed, newResolved, reason)
		}
		parsed, parseErr := time.Parse(time.RFC3339Nano, changedAt)
		if parseErr != nil || parsed.Location() != time.UTC {
			return fmt.Errorf("deterministic target transition timestamp alert=%s value=%q", alertID, changedAt)
		}
	}
	return nil
}

func problem5CauseFingerprint(ruleID, revisionID int64) string {
	keys := []string{fmt.Sprintf("review3-am:%d:%d", ruleID, revisionID)}
	raw, _ := json.Marshal(keys)
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum)
}

func assertProblem5DeterministicFinalRows(db *sql.DB, firstBatchAt, resumedBatchAt time.Time) error {
	var sourceContextFingerprint, jobFingerprint, contextJSON, trigger, status, cursor string
	var total, processed, promoted, skippedIneligible, skippedMissingLabels, skippedIncompleteSources, staleConflicts, attempts, retries, consecutive, recovery int
	var nextAttempt, firstMessage, firstClass, firstStage, firstAt, lastMessage, lastClass, lastStage, lastAt, abandonedAt, abandonedBy, abandonReason, createdAt, startedAt, finishedAt sql.NullString
	if err := db.QueryRow(`
SELECT source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,COALESCE(cursor_alert_id,''),
 total_candidates,processed_candidates,promoted_candidates,skipped_ineligible,skipped_missing_labels,skipped_incomplete_sources,stale_conflicts,
 attempt_count,retry_count,consecutive_retry_count,next_attempt_at,first_error_message,first_error_class,first_error_stage,first_error_at,
 last_error_message,last_error_class,last_error_stage,last_error_at,recovery_count,abandoned_at,abandoned_by,abandon_reason,created_at,started_at,finished_at
FROM silence_recheck_jobs WHERE job_id=1`).Scan(
		&sourceContextFingerprint, &jobFingerprint, &contextJSON, &trigger, &status, &cursor,
		&total, &processed, &promoted, &skippedIneligible, &skippedMissingLabels, &skippedIncompleteSources, &staleConflicts,
		&attempts, &retries, &consecutive, &nextAttempt, &firstMessage, &firstClass, &firstStage, &firstAt,
		&lastMessage, &lastClass, &lastStage, &lastAt, &recovery, &abandonedAt, &abandonedBy, &abandonReason, &createdAt, &startedAt, &finishedAt,
	); err != nil {
		return err
	}
	expectedContext, expectedSourceFingerprint, expectedJobFingerprint, expectedCandidates, expectedRule, err := problem5DeterministicExpectedInput(firstBatchAt)
	if err != nil {
		return err
	}
	if sourceContextFingerprint != expectedSourceFingerprint || jobFingerprint != expectedJobFingerprint || contextJSON != expectedContext || trigger != "source_context_changed" || status != "abandoned" || cursor != "problem5-fifo-head-39" || total != 41 || processed != 40 || promoted != 40 || skippedIneligible != 0 || skippedMissingLabels != 0 || skippedIncompleteSources != 0 || staleConflicts != 0 || attempts != 5 || retries != 1 || consecutive != 0 || nextAttempt.Valid || firstClass.String != "retryable" || firstStage.String != "persist_candidate" || lastClass.String != "terminal" || lastStage.String != "persist_candidate" || recovery != 1 || abandonedBy.String != "alice" || abandonReason.String != "review abandon" || !createdAt.Valid || !startedAt.Valid || !finishedAt.Valid {
		return fmt.Errorf("deterministic complete job row status=%q cursor=%q total=%d processed=%d promoted=%d attempts=%d retries=%d recovery=%d first=%q/%q last=%q/%q abandoned=%q/%q", status, cursor, total, processed, promoted, attempts, retries, recovery, firstClass.String, firstStage.String, lastClass.String, lastStage.String, abandonedBy.String, abandonReason.String)
	}
	for _, value := range []sql.NullString{createdAt, startedAt, finishedAt, firstAt, lastAt} {
		if !value.Valid {
			return errors.New("deterministic complete job row has missing timestamp")
		}
		parsed, parseErr := time.Parse(time.RFC3339Nano, value.String)
		if parseErr != nil || parsed.Location() != time.UTC {
			return fmt.Errorf("deterministic complete job timestamp=%q", value.String)
		}
	}
	if firstMessage.String != "persist_candidate: scheduler sqlite error" || lastMessage.String != "persist_candidate: problem5 scheduler terminal failure" || abandonReason.String != "review abandon" {
		return fmt.Errorf("deterministic complete job error messages first=%q last=%q", firstMessage.String, lastMessage.String)
	}
	if err = assertProblem5DeterministicRuleRow(db, expectedRule); err != nil {
		return err
	}
	rows, err := db.Query(`SELECT alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json,outcome,processed_at FROM silence_recheck_job_candidates WHERE job_id=1 ORDER BY alert_id,target_key`)
	if err != nil {
		return err
	}
	defer rows.Close()
	expectedByKey := expectedCandidatesByKey(expectedCandidates)
	seen := make(map[string]bool, len(expectedByKey))
	for rows.Next() {
		var alertID, targetKey, expectedAlertLastSeen, expectedTargetLastSeen, matchLabels, relevantSources string
		var targetMembers, outcome, processedAt sql.NullString
		if err = rows.Scan(&alertID, &targetKey, &expectedAlertLastSeen, &expectedTargetLastSeen, &matchLabels, &relevantSources, &targetMembers, &outcome, &processedAt); err != nil {
			return err
		}
		key := alertID + "\x00" + targetKey
		want, ok := expectedByKey[key]
		if !ok || seen[key] {
			return fmt.Errorf("deterministic head candidate duplicate or target alert=%q target=%q", alertID, targetKey)
		}
		if !problem5NullableEqual(sql.NullString{String: expectedAlertLastSeen, Valid: true}, want.ExpectedAlertLastSeenAt) || !problem5NullableEqual(sql.NullString{String: expectedTargetLastSeen, Valid: true}, want.ExpectedTargetLastSeenAt) || !problem5NullableEqual(sql.NullString{String: matchLabels, Valid: true}, want.MatchLabelsJSON) || relevantSources != want.RelevantSourcesJSON || !problem5NullableEqual(targetMembers, want.TargetSourceMembersJSON) {
			return fmt.Errorf("deterministic head candidate=%q target=%q last_seen=%q/%q match=%q sources=%q members=%q outcome=%q processed=%q", alertID, targetKey, expectedAlertLastSeen, expectedTargetLastSeen, matchLabels, relevantSources, targetMembers.String, outcome.String, processedAt.String)
		}
		wantOutcome := "promoted"
		if alertID == "problem5-fifo-head-40" {
			wantOutcome = ""
		}
		if outcome.String != wantOutcome || (wantOutcome == "" && processedAt.Valid) || (wantOutcome != "" && !processedAt.Valid) {
			return fmt.Errorf("deterministic head candidate=%q outcome=%q want=%q processed=%q", alertID, outcome.String, wantOutcome, processedAt.String)
		}
		if wantOutcome != "" {
			index := 0
			if alertID != "problem5-fifo-head" {
				_, _ = fmt.Sscanf(strings.TrimPrefix(alertID, "problem5-fifo-head-"), "%02d", &index)
			}
			wantProcessedAt := firstBatchAt
			if index >= 20 {
				wantProcessedAt = resumedBatchAt
			}
			if processedAt.String != wantProcessedAt.Format(time.RFC3339Nano) {
				return fmt.Errorf("deterministic head candidate=%q processed_at=%q want=%q", alertID, processedAt.String, wantProcessedAt.Format(time.RFC3339Nano))
			}
		}
		seen[key] = true
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(seen) != len(expectedByKey) {
		return fmt.Errorf("deterministic head candidates=%d want %d", len(seen), len(expectedByKey))
	}
	rows.Close()

	wantAttempts := []string{"committed", "retry_scheduled", "committed", "failed", "failed"}
	rows, err = db.Query(`SELECT attempt_number,attempt_token,outcome,failure_class,failure_stage,scheduled_retry_at FROM silence_recheck_job_attempts WHERE job_id=1 ORDER BY attempt_number`)
	if err != nil {
		return err
	}
	for expectedNumber, wantOutcome := range wantAttempts {
		if !rows.Next() {
			rows.Close()
			return fmt.Errorf("deterministic attempt %d is missing", expectedNumber+1)
		}
		var number int
		var token, outcome string
		var failureClass, failureStage, scheduledRetryAt sql.NullString
		if err = rows.Scan(&number, &token, &outcome, &failureClass, &failureStage, &scheduledRetryAt); err != nil {
			rows.Close()
			return err
		}
		if number != expectedNumber+1 || token == "" || outcome != wantOutcome {
			rows.Close()
			return fmt.Errorf("deterministic attempt=%d token=%q outcome=%q want=%q", number, token, outcome, wantOutcome)
		}
		wantClass, wantStage := "", ""
		switch wantOutcome {
		case "retry_scheduled":
			wantClass, wantStage = "retryable", "persist_candidate"
		case "failed":
			wantClass, wantStage = "terminal", "persist_candidate"
		}
		if (failureClass.String != wantClass || failureStage.String != wantStage) || (wantClass == "" && (failureClass.Valid || failureStage.Valid)) {
			rows.Close()
			return fmt.Errorf("deterministic attempt %d failure metadata class=%q/%q stage=%q/%q", number, failureClass.String, wantClass, failureStage.String, wantStage)
		}
		if wantOutcome == "committed" && scheduledRetryAt.Valid {
			rows.Close()
			return fmt.Errorf("committed attempt %d carries failure metadata", number)
		}
		if wantOutcome == "retry_scheduled" && !scheduledRetryAt.Valid {
			rows.Close()
			return fmt.Errorf("retry attempt %d lacks complete failure metadata", number)
		}
		if wantOutcome == "retry_scheduled" {
			parsed, parseErr := time.Parse(time.RFC3339Nano, scheduledRetryAt.String)
			if parseErr != nil || parsed.Location() != time.UTC || !parsed.Equal(firstBatchAt.Add(time.Second)) {
				rows.Close()
				return fmt.Errorf("retry attempt %d scheduled_at=%q want=%q", number, scheduledRetryAt.String, firstBatchAt.Add(time.Second).Format(time.RFC3339Nano))
			}
		}
		if wantOutcome == "failed" && scheduledRetryAt.Valid {
			rows.Close()
			return fmt.Errorf("failed attempt %d has invalid failure metadata", number)
		}
	}
	if rows.Next() {
		rows.Close()
		return errors.New("deterministic head has an extra attempt")
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	var successorAttemptCount int
	if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=2 AND attempt_number=1 AND outcome='committed' AND attempt_token<>''`).Scan(&successorAttemptCount); err != nil {
		return err
	}
	if successorAttemptCount != 1 {
		return fmt.Errorf("deterministic successor attempts=%d want 1 committed", successorAttemptCount)
	}

	var actionCount int
	if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions WHERE job_id=1`).Scan(&actionCount); err != nil {
		return err
	}
	if actionCount != 2 {
		return fmt.Errorf("deterministic operator actions=%d want 2", actionCount)
	}
	rows, err = db.Query(`SELECT action_id,job_id,request_id,action,COALESCE(actor,''),reason,request_fingerprint,requested_at,applied_at,applied_result,resulting_status FROM silence_recheck_job_operator_actions WHERE job_id=1 ORDER BY action_id`)
	if err != nil {
		return err
	}
	wantActions := []struct{ requestID, action, status string }{
		{requestID: "problem5-review-retry", action: "retry", status: "queued"},
		{requestID: "problem5-review-abandon", action: "abandon", status: "abandoned"},
	}
	for index, want := range wantActions {
		if !rows.Next() {
			rows.Close()
			return fmt.Errorf("missing deterministic operator action %s", want.requestID)
		}
		var actionID, jobID int
		var requestID, action, actor, reason, fingerprint, requestedAt, appliedAt, appliedResult, status string
		if err = rows.Scan(&actionID, &jobID, &requestID, &action, &actor, &reason, &fingerprint, &requestedAt, &appliedAt, &appliedResult, &status); err != nil {
			rows.Close()
			return err
		}
		if actionID != index+1 || jobID != 1 || requestID != want.requestID || action != want.action || actor != "alice" || reason != map[string]string{"problem5-review-retry": "review retry", "problem5-review-abandon": "review abandon"}[want.requestID] || status != want.status || fingerprint != problem4RouterFingerprint(action, reason, &actor) || requestedAt == "" || appliedAt != requestedAt || appliedResult != "applied" {
			rows.Close()
			return fmt.Errorf("operator action=%q/%q/%q actor=%q reason=%q fingerprint=%q requested=%q applied=%q result=%q want=%q/%q/%q", requestID, action, status, actor, reason, fingerprint, requestedAt, appliedAt, appliedResult, want.requestID, want.action, want.status)
		}
		if parsed, parseErr := time.Parse(time.RFC3339Nano, requestedAt); parseErr != nil || parsed.Location() != time.UTC {
			rows.Close()
			return fmt.Errorf("operator action timestamp=%q", requestedAt)
		}
	}
	if rows.Next() {
		rows.Close()
		return errors.New("deterministic operator action set has extra rows")
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if err = assertProblem5DeterministicAttemptRows(db, firstBatchAt, resumedBatchAt); err != nil {
		return err
	}
	if err = assertProblem5DeterministicFailureProjection(db, firstBatchAt, resumedBatchAt); err != nil {
		return err
	}
	var disappeared, promotedEvents int
	if err = db.QueryRow(`SELECT COUNT(*) FROM alert_events WHERE old_state='active' AND new_state='inactive' AND old_is_live=1 AND new_is_live=0 AND old_is_suppressed=0 AND new_is_suppressed=0`).Scan(&disappeared); err != nil {
		return err
	}
	if err = db.QueryRow(`SELECT COUNT(*) FROM alert_events WHERE old_state='inactive' AND new_state='suppressed' AND old_is_live=0 AND new_is_live=0 AND old_is_suppressed=0 AND new_is_suppressed=1`).Scan(&promotedEvents); err != nil {
		return err
	}
	if disappeared != 42 || promotedEvents != 41 {
		return fmt.Errorf("deterministic transition events disappeared=%d/42 promoted=%d/41", disappeared, promotedEvents)
	}
	eventRows, err := db.Query(`SELECT alert_id,COALESCE(old_state,''),new_state,COALESCE(old_raw_state,''),new_raw_state,COALESCE(old_is_live,0),new_is_live,COALESCE(old_is_suppressed,0),new_is_suppressed,COALESCE(old_is_resolved,0),new_is_resolved,changed_at,change_source,COALESCE(changed_by,''),COALESCE(reason_code,''),COALESCE(reason_text,'') FROM alert_events WHERE alert_id='problem5-fifo-successor' OR alert_id='problem5-fifo-head' OR alert_id LIKE 'problem5-fifo-head-%' ORDER BY alert_id,id`)
	if err != nil {
		return err
	}
	type transition struct {
		oldState, newState, oldRaw, newRaw, changedAt, changeSource, changedBy, reasonCode, reasonText string
		oldLive, newLive, oldSuppressed, newSuppressed, oldResolved, newResolved                       int
	}
	seenTransitions := make(map[string][]transition, 42)
	for eventRows.Next() {
		var alertID string
		var event transition
		if err = eventRows.Scan(&alertID, &event.oldState, &event.newState, &event.oldRaw, &event.newRaw, &event.oldLive, &event.newLive, &event.oldSuppressed, &event.newSuppressed, &event.oldResolved, &event.newResolved, &event.changedAt, &event.changeSource, &event.changedBy, &event.reasonCode, &event.reasonText); err != nil {
			eventRows.Close()
			return err
		}
		seenTransitions[alertID] = append(seenTransitions[alertID], event)
	}
	if err = eventRows.Err(); err != nil {
		eventRows.Close()
		return err
	}
	eventRows.Close()
	if len(seenTransitions) != 42 {
		return fmt.Errorf("deterministic event alert coverage=%d want 42", len(seenTransitions))
	}
	for alertID, transitions := range seenTransitions {
		wantPromoted := alertID == "problem5-fifo-successor"
		if alertID == "problem5-fifo-head" || (strings.HasPrefix(alertID, "problem5-fifo-head-") && alertID != "problem5-fifo-head-40") {
			wantPromoted = true
		}
		wantCount := 2
		if wantPromoted {
			wantCount = 3
		}
		firstSeenAt, disappearedAt := firstBatchAt, firstBatchAt.Add(time.Minute)
		if alertID == "problem5-fifo-successor" {
			firstSeenAt, disappearedAt = firstBatchAt.Add(time.Minute), firstBatchAt.Add(2*time.Minute)
		}
		wantTransitions := []transition{{oldState: "", newState: "active", oldRaw: "", newRaw: "active", oldLive: 0, newLive: 1, oldSuppressed: 0, newSuppressed: 0, oldResolved: 0, newResolved: 0, changedAt: firstSeenAt.Format(time.RFC3339Nano), changeSource: "poller", reasonCode: "poll_first_seen"}, {oldState: "active", newState: "inactive", oldRaw: "active", newRaw: "active", oldLive: 1, newLive: 0, oldSuppressed: 0, newSuppressed: 0, oldResolved: 0, newResolved: 0, changedAt: disappearedAt.Format(time.RFC3339Nano), changeSource: "poller", reasonCode: "poll_live_disappeared"}}
		if wantPromoted {
			promotedAt := firstBatchAt
			if alertID == "problem5-fifo-successor" {
				promotedAt = resumedBatchAt
			}
			if suffix, found := strings.CutPrefix(alertID, "problem5-fifo-head-"); found {
				var index int
				if _, scanErr := fmt.Sscanf(suffix, "%02d", &index); scanErr == nil && index >= 20 {
					promotedAt = resumedBatchAt
				}
			}
			wantTransitions = append(wantTransitions, transition{oldState: "inactive", newState: "suppressed", oldRaw: "active", newRaw: "active", oldLive: 0, newLive: 0, oldSuppressed: 0, newSuppressed: 1, oldResolved: 0, newResolved: 0, changedAt: promotedAt.Format(time.RFC3339Nano), changeSource: "silence", reasonCode: "silence_match_promoted"})
		}
		if len(transitions) != wantCount || !reflect.DeepEqual(transitions, wantTransitions) {
			return fmt.Errorf("deterministic event alert=%q transitions=%+v wantPromoted=%t", alertID, transitions, wantPromoted)
		}
	}

	return nil
}

func assertProblem5DeterministicAttemptRows(db *sql.DB, firstBatchAt, resumedBatchAt time.Time) error {
	rows, err := db.Query(`SELECT attempt_id,job_id,attempt_token,attempt_number,started_at,finished_at,outcome,failure_class,failure_stage,scheduled_retry_at FROM silence_recheck_job_attempts WHERE job_id=1 ORDER BY attempt_number`)
	if err != nil {
		return err
	}
	defer rows.Close()
	expected := []struct {
		started, finished                time.Time
		outcome, class, stage, scheduled string
	}{
		{firstBatchAt, firstBatchAt, "committed", "", "", ""},
		{firstBatchAt, firstBatchAt, "retry_scheduled", "retryable", "persist_candidate", firstBatchAt.Add(time.Second).UTC().Format(time.RFC3339Nano)},
		{resumedBatchAt, resumedBatchAt, "committed", "", "", ""},
		{resumedBatchAt, resumedBatchAt, "failed", "terminal", "persist_candidate", ""},
		{resumedBatchAt, resumedBatchAt, "failed", "terminal", "persist_candidate", ""},
	}
	seenTokens := map[string]struct{}{}
	for index, want := range expected {
		if !rows.Next() {
			return fmt.Errorf("deterministic attempt row %d is missing", index+1)
		}
		var attemptID, jobID, number int
		var token, started, finished, outcome string
		var class, stage, scheduled sql.NullString
		if err = rows.Scan(&attemptID, &jobID, &token, &number, &started, &finished, &outcome, &class, &stage, &scheduled); err != nil {
			return err
		}
		if attemptID != index+1 || jobID != 1 || number != index+1 || token == "" || outcome != want.outcome || class.String != want.class || stage.String != want.stage || scheduled.String != want.scheduled || (want.scheduled == "" && scheduled.Valid) || started != want.started.UTC().Format(time.RFC3339Nano) || finished != want.finished.UTC().Format(time.RFC3339Nano) {
			return fmt.Errorf("deterministic complete attempt row=%d id=%d/%d job=%d number=%d token=%q started=%q finished=%q outcome=%q class=%q stage=%q scheduled=%q", index+1, attemptID, index+1, jobID, number, token, started, finished, outcome, class.String, stage.String, scheduled.String)
		}
		if _, duplicate := seenTokens[token]; duplicate {
			return fmt.Errorf("deterministic attempt token reused=%q", token)
		}
		seenTokens[token] = struct{}{}
	}
	if rows.Next() {
		return errors.New("deterministic attempt table has extra rows")
	}
	return rows.Err()
}

func assertProblem5DeterministicFailureProjection(db *sql.DB, firstFailureAt, terminalFailureAt time.Time) error {
	var firstMessage, firstClass, firstStage, firstAt sql.NullString
	var lastMessage, lastClass, lastStage, lastAt sql.NullString
	if err := db.QueryRow(`
SELECT first_error_message,first_error_class,first_error_stage,first_error_at,
       last_error_message,last_error_class,last_error_stage,last_error_at
FROM silence_recheck_jobs WHERE job_id=1`).Scan(
		&firstMessage, &firstClass, &firstStage, &firstAt,
		&lastMessage, &lastClass, &lastStage, &lastAt,
	); err != nil {
		return err
	}
	wantFirstAt := firstFailureAt.UTC().Format(time.RFC3339Nano)
	wantLastAt := terminalFailureAt.UTC().Format(time.RFC3339Nano)
	wantFirstMessage := "persist_candidate: " + schedulerSQLiteError(5).Error()
	wantLastMessage := "persist_candidate: problem5 scheduler terminal failure"
	if !problem5ExactNullableEqual(firstMessage, wantFirstMessage) || !problem5ExactNullableEqual(firstClass, "retryable") || !problem5ExactNullableEqual(firstStage, "persist_candidate") || !problem5ExactNullableEqual(firstAt, wantFirstAt) || !problem5ExactNullableEqual(lastMessage, wantLastMessage) || !problem5ExactNullableEqual(lastClass, "terminal") || !problem5ExactNullableEqual(lastStage, "persist_candidate") || !problem5ExactNullableEqual(lastAt, wantLastAt) {
		return fmt.Errorf("deterministic failure projection first=(%q,%q,%q,%q) last=(%q,%q,%q,%q)", firstMessage.String, firstClass.String, firstStage.String, firstAt.String, lastMessage.String, lastClass.String, lastStage.String, lastAt.String)
	}
	for name, value := range map[string]string{"first_error_at": firstAt.String, "last_error_at": lastAt.String} {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil || parsed.Location() != time.UTC {
			return fmt.Errorf("deterministic %s is not exact UTC RFC3339Nano: %q", name, value)
		}
	}
	return nil
}

func problem5ExactNullableEqual(actual sql.NullString, expected string) bool {
	return actual.Valid && actual.String == expected
}

func problem5DeterministicExpectedInput(now time.Time) (string, string, string, []problem5IndependentCandidateInput, problem5ControlledRule, error) {
	sources := problem5SchedulerSourceState(now)
	contextJSON, sourceFingerprint, err := problem5ExpectedSourceContext(sources, 1)
	if err != nil {
		return "", "", "", nil, problem5ControlledRule{}, err
	}
	silence := sources[0].Silences[0]
	contentFingerprint, evaluationFingerprint, matchersJSON := problem5SilenceRevisionFingerprints(silence)
	rule := problem5ControlledRule{
		SourceName: "review3-am", ClusterID: "review3-cluster", TargetKey: "review3-cluster",
		SilenceID: silence.SilenceID, RuleID: 1, RevisionID: 1, EpisodeID: 1, RevisionNumber: 1,
		ContentFingerprint: contentFingerprint, EvaluationFingerprint: evaluationFingerprint,
		MatchersJSON: matchersJSON, ObservedState: silence.Status,
		StartsAt: silence.StartsAt.UTC().Format(time.RFC3339Nano), EndsAt: silence.EndsAt.UTC().Format(time.RFC3339Nano),
		AnnotationsJSON: "{}",
	}
	labels := `{"alertname":"Problem5Head","service":"review3"}`
	relevantSources := `[{"name":"review3-am","cluster":"review3-cluster"}]`
	targetMembers := `["review3-am"]`
	lastSeen := now.UTC().Format(time.RFC3339Nano)
	expectedCandidates := make([]problem5IndependentCandidateInput, 0, 41)
	for index := 0; index < 41; index++ {
		alertID := "problem5-fifo-head"
		if index > 0 {
			alertID = fmt.Sprintf("problem5-fifo-head-%02d", index)
		}
		alertLastSeen, targetLastSeen, matchLabels, members := lastSeen, lastSeen, labels, targetMembers
		expectedCandidates = append(expectedCandidates, problem5IndependentCandidateInput{
			AlertID: alertID, TargetKey: "review3-cluster",
			ExpectedAlertLastSeenAt: &alertLastSeen, ExpectedTargetLastSeenAt: &targetLastSeen,
			MatchLabelsJSON: &matchLabels, RelevantSourcesJSON: relevantSources, TargetSourceMembersJSON: &members,
		})
	}
	jobFingerprint, err := problem5ExpectedJobFingerprint(contextJSON, sourceFingerprint, []problem5ControlledRule{rule}, expectedCandidates)
	if err != nil {
		return "", "", "", nil, problem5ControlledRule{}, err
	}
	return contextJSON, sourceFingerprint, jobFingerprint, expectedCandidates, rule, nil
}

func problem5DeterministicStats(db *sql.DB, sqlitePath string) (alertlifecycle.Stats, error) {
	stats := alertlifecycle.Stats{CurrentRows: 42, HistoryRows: 125, StateCounts: map[string]int{string(alertlifecycle.StateInactive): 1, string(alertlifecycle.StateSuppressed): 41}, RegroupStatus: "ready"}
	stats.SuppressedBreakdownPast = 41
	for query, destination := range map[string]any{`PRAGMA page_count`: &stats.PageCount, `PRAGMA page_size`: &stats.PageSizeBytes, `PRAGMA freelist_count`: &stats.FreelistCount} {
		if err := db.QueryRow(query).Scan(destination); err != nil {
			return alertlifecycle.Stats{}, err
		}
	}
	if fileInfo, err := os.Stat(sqlitePath); err == nil {
		stats.FileSizeBytes = fileInfo.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return alertlifecycle.Stats{}, err
	}
	return stats, nil
}

func assertProblem5DeterministicRuleRow(db *sql.DB, expected problem5ControlledRule) error {
	var jobID, ruleID, revisionID int64
	var sourceName, clusterID, targetKey, silenceID, contentFingerprint, observedState, evaluationFingerprint, matchersJSON string
	if err := db.QueryRow(`
SELECT job_id,source_name,cluster_id,target_key,rule_id,revision_id,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json
FROM silence_recheck_job_rules WHERE job_id=1`).Scan(
		&jobID, &sourceName, &clusterID, &targetKey, &ruleID, &revisionID, &silenceID, &contentFingerprint, &observedState, &evaluationFingerprint, &matchersJSON,
	); err != nil {
		return err
	}
	if jobID != 1 || sourceName != expected.SourceName || clusterID != expected.ClusterID || targetKey != expected.TargetKey || ruleID != expected.RuleID || revisionID != expected.RevisionID || silenceID != expected.SilenceID || contentFingerprint != expected.ContentFingerprint || observedState != expected.ObservedState || evaluationFingerprint != expected.EvaluationFingerprint || matchersJSON != expected.MatchersJSON {
		return fmt.Errorf("deterministic rule row job=%d source=%q/%q target=%q rule=%d/%d revision=%d/%d silence=%q/%q content=%q/%q state=%q/%q evaluation=%q/%q matchers=%q/%q", jobID, sourceName, expected.SourceName, targetKey, ruleID, expected.RuleID, revisionID, expected.RevisionID, silenceID, expected.SilenceID, contentFingerprint, expected.ContentFingerprint, observedState, expected.ObservedState, evaluationFingerprint, expected.EvaluationFingerprint, matchersJSON, expected.MatchersJSON)
	}
	var extra int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_rules WHERE job_id=1`).Scan(&extra); err != nil {
		return err
	}
	if extra != 1 {
		return fmt.Errorf("deterministic rule rows=%d want 1", extra)
	}
	return nil
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
			if err := createProblem5SchedulerSuccessor(store, now); err != nil {
				t.Fatal(err)
			}
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
	headSnapshots := make([]alertlifecycle.LiveAlertSnapshot, 0, 41)
	for i := 0; i < 41; i++ {
		alertID := "problem5-fifo-head"
		if i > 0 {
			alertID = fmt.Sprintf("problem5-fifo-head-%02d", i)
		}
		headSnapshots = append(headSnapshots, alertlifecycle.LiveAlertSnapshot{
			AlertID: alertID, State: alertlifecycle.StateActive, Receiver: "review3-team", GroupID: "review3-group",
			GroupLabelsJSON: `{"alertname":"Problem5"}`, LabelsJSON: headLabels, MatchLabelsJSON: &headLabels, AnnotationsJSON: `{}`,
			StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"review3-am"},
			RelevantSources: []alertlifecycle.SourceRef{{Name: "review3-am", Cluster: "review3-cluster"}},
		})
	}
	labels := `{"alertname":"Review3Follower","service":"review3"}`
	follower := alertlifecycle.LiveAlertSnapshot{
		AlertID: "problem5-fifo-successor", State: alertlifecycle.StateActive, Receiver: "review3-team", GroupID: "review3-group",
		GroupLabelsJSON: `{"alertname":"Review3"}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
		StartsAt: now.Add(-time.Hour), LastSeenAt: now.Add(time.Minute), RelevantUpstreams: []string{"review3-am"},
		RelevantSources: []alertlifecycle.SourceRef{{Name: "review3-am", Cluster: "review3-cluster"}},
	}
	sources := problem5SchedulerSourceState(now)
	creation, err := store.ApplyPollCycle(alertlifecycle.PollCycleInput{LiveAlerts: headSnapshots, SuccessfulUpstreams: map[string]bool{"review3-am": true}, Timestamp: now})
	if err != nil || creation.RecheckJobsQueued != 0 {
		t.Fatalf("production initial creation=%+v err=%v", creation, err)
	}
	creation, err = store.ApplyPollCycle(alertlifecycle.PollCycleInput{LiveAlerts: []alertlifecycle.LiveAlertSnapshot{follower}, SuccessfulUpstreams: map[string]bool{"review3-am": true}, SilenceSources: sources, Timestamp: now.Add(time.Minute)})
	if err != nil || creation.RecheckJobsQueued != 1 {
		t.Fatalf("production head creation=%+v err=%v", creation, err)
	}
	return store, db, now, path
}

func problem5SchedulerSourceState(now time.Time) []alertlifecycle.SourceSilenceState {
	return []alertlifecycle.SourceSilenceState{{
		SourceName: "review3-am", ClusterID: "review3-cluster", SyncStatus: "success",
		Silences: []alertlifecycle.SourceSilenceSnapshot{{
			SilenceID: "review3-scheduler-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
			Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "service", Value: "review3", IsEqual: true}},
		}},
	}}
}

func createProblem5SchedulerSuccessor(store *alertlifecycle.Store, now time.Time) error {
	creation, err := store.ApplyPollCycle(alertlifecycle.PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"review3-am": true},
		SilenceSources:      problem5SchedulerSourceState(now),
		Timestamp:           now.Add(2 * time.Minute),
	})
	if err != nil {
		return err
	}
	if creation.RecheckJobsQueued != 1 || creation.RecheckJobsAlertBecameEligible != 1 {
		return fmt.Errorf("production successor creation=%+v", creation)
	}
	return nil
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
