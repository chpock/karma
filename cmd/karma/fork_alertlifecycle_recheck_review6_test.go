package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func armRecheckSchedulerIdleLoopProbe(scheduler *recheckScheduler) <-chan struct{} {
	probe := make(chan struct{})
	scheduler.stateMu.Lock()
	scheduler.idleLoopProbe = probe
	scheduler.stateMu.Unlock()
	return probe
}

type review6RowsSnapshot struct {
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
}

func review6TableColumns(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return columns
}

func review6QuotedIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func review6Value(value any) string {
	switch typed := value.(type) {
	case nil:
		return "<NULL>"
	case []byte:
		return "bytes:" + hex.EncodeToString(typed)
	default:
		return fmt.Sprintf("%T:%v", value, value)
	}
}

func captureReview6Rows(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := review6RowsSnapshot{Columns: columns}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if scanErr := rows.Scan(pointers...); scanErr != nil {
			t.Fatal(scanErr)
		}
		encoded := make([]string, len(values))
		for index, value := range values {
			encoded[index] = review6Value(value)
		}
		snapshot.Rows = append(snapshot.Rows, encoded)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		t.Fatal(rowsErr)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func captureReview6JobTable(t *testing.T, db *sql.DB, table string, jobID int64, excluded ...string) string {
	t.Helper()
	excludedColumns := make(map[string]bool, len(excluded))
	for _, column := range excluded {
		excludedColumns[column] = true
	}
	columns := review6TableColumns(t, db, table)
	selected := make([]string, 0, len(columns))
	for _, column := range columns {
		if !excludedColumns[column] {
			selected = append(selected, review6QuotedIdentifier(column))
		}
	}
	if len(selected) == 0 {
		t.Fatalf("no columns selected from %s", table)
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE job_id=? ORDER BY rowid", strings.Join(selected, ","), review6QuotedIdentifier(table))
	return captureReview6Rows(t, db, query, jobID)
}

func captureReview6Attempt(t *testing.T, db *sql.DB, jobID int64, attemptNumber int) string {
	t.Helper()
	columns := review6TableColumns(t, db, "silence_recheck_job_attempts")
	selected := make([]string, len(columns))
	for index, column := range columns {
		selected[index] = review6QuotedIdentifier(column)
	}
	query := fmt.Sprintf("SELECT %s FROM silence_recheck_job_attempts WHERE job_id=? AND attempt_number=? ORDER BY rowid", strings.Join(selected, ","))
	return captureReview6Rows(t, db, query, jobID, attemptNumber)
}

func captureReview6FirstError(t *testing.T, db *sql.DB, jobID int64) string {
	return captureReview6Rows(t, db, `SELECT first_error_message,first_error_class,first_error_stage,first_error_at FROM silence_recheck_jobs WHERE job_id=?`, jobID)
}

var review6JobMutableColumns = []string{
	"status", "cursor_alert_id", "processed_candidates", "promoted_candidates", "stale_conflicts",
	"skipped_ineligible", "skipped_missing_labels", "skipped_incomplete_sources", "attempt_count",
	"retry_count", "consecutive_retry_count", "next_attempt_at", "started_at", "finished_at",
	"first_error_message", "first_error_class", "first_error_stage", "first_error_at",
	"last_error_message", "last_error_class", "last_error_stage", "last_error_at", "recovery_count", "abandoned_at",
	"abandoned_by", "abandon_reason",
}

func TestProblem3Review6RetryPreservesImmutableInputAndFailureHistory(t *testing.T) {
	store, db, now := seedProblem3Review3SchedulerStore(t)
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		RecheckFault: func(stage string) error {
			if stage == "persist_candidate" {
				return errors.New("review6 retry failure")
			}
			return nil
		},
	})
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(scheduler.Signal)
	store.SetRecheckReconciledWake(scheduler.SignalReconciled)
	runs, heads := observeProblem3Review3Scheduler(scheduler)
	scheduler.Start()
	first := <-runs
	if !alertlifecycle.IsRecheckDurableFailure(first.err) {
		t.Fatalf("initial durable failure=%+v", first)
	}
	store.SetRecheckIntegrationTestHooks(nil)
	beforeJob := captureReview6JobTable(t, db, "silence_recheck_jobs", 1, review6JobMutableColumns...)
	beforeRules := captureReview6JobTable(t, db, "silence_recheck_job_rules", 1)
	beforeCandidates := captureReview6JobTable(t, db, "silence_recheck_job_candidates", 1, "processed_at", "outcome")
	beforeAttempt := captureReview6Attempt(t, db, 1, 1)
	beforeFirstError := captureReview6FirstError(t, db, 1)

	actor := "review6-retry"
	if _, err := store.RetryRecheckJobContext(context.Background(), 1, "review6-retry", "retry failed head", &actor, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	second := <-runs
	if second.err != nil {
		t.Fatalf("retry result=%+v", second)
	}
	<-heads
	scheduler.Stop()

	if got := captureReview6JobTable(t, db, "silence_recheck_jobs", 1, review6JobMutableColumns...); got != beforeJob {
		t.Fatal("Retry changed immutable job input")
	}
	if got := captureReview6JobTable(t, db, "silence_recheck_job_rules", 1); got != beforeRules {
		t.Fatal("Retry changed pinned rule input")
	}
	if got := captureReview6JobTable(t, db, "silence_recheck_job_candidates", 1, "processed_at", "outcome"); got != beforeCandidates {
		t.Fatal("Retry changed pinned candidate input")
	}
	if got := captureReview6Attempt(t, db, 1, 1); got != beforeAttempt {
		t.Fatal("Retry changed the original attempt row")
	}
	if got := captureReview6FirstError(t, db, 1); got != beforeFirstError {
		t.Fatal("Retry changed first-error history")
	}
	var attempts, jobs, terminalCandidates int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=1 AND processed_at IS NOT NULL AND outcome IS NOT NULL`).Scan(&terminalCandidates); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || jobs != 1 || terminalCandidates != 1 {
		t.Fatalf("Retry durable result attempts=%d jobs=%d terminal_candidates=%d", attempts, jobs, terminalCandidates)
	}
}

func TestProblem3Review6AbandonPreservesFailedHeadHistoryAndReleasesOnlySuccessor(t *testing.T) {
	store, db, now := seedProblem3Review3SchedulerStore(t)
	nowRaw := now.Add(time.Second).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,total_candidates,processed_candidates,created_at) VALUES(?,?,?,?,?,?,?,?)`, "review6-successor-context", "sha256:review6-successor", `{"formatVersion":1,"sources":[],"clusters":[]}`, "source_context_changed", "queued", 0, 0, nowRaw); err != nil {
		t.Fatal(err)
	}
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		RecheckFault: func(stage string) error {
			if stage == "persist_candidate" {
				return errors.New("review6 abandon failure")
			}
			return nil
		},
	})
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(scheduler.Signal)
	store.SetRecheckReconciledWake(scheduler.SignalReconciled)
	runs, heads := observeProblem3Review3Scheduler(scheduler)
	scheduler.Start()
	first := <-runs
	if !alertlifecycle.IsRecheckDurableFailure(first.err) {
		t.Fatalf("initial durable failure=%+v", first)
	}
	store.SetRecheckIntegrationTestHooks(nil)
	beforeJob := captureReview6JobTable(t, db, "silence_recheck_jobs", 1, review6JobMutableColumns...)
	beforeRules := captureReview6JobTable(t, db, "silence_recheck_job_rules", 1)
	beforeCandidates := captureReview6JobTable(t, db, "silence_recheck_job_candidates", 1, "processed_at", "outcome")
	beforeAttempt := captureReview6Attempt(t, db, 1, 1)
	beforeFirstError := captureReview6FirstError(t, db, 1)

	actor := "review6-abandon"
	if _, err := store.AbandonRecheckJobContext(context.Background(), 1, "review6-abandon", "release failed head", &actor, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	second := <-runs
	if second.err != nil {
		t.Fatalf("successor result=%+v", second)
	}
	<-heads
	scheduler.Stop()

	if got := captureReview6JobTable(t, db, "silence_recheck_jobs", 1, review6JobMutableColumns...); got != beforeJob {
		t.Fatal("Abandon changed immutable failed-head input")
	}
	if got := captureReview6JobTable(t, db, "silence_recheck_job_rules", 1); got != beforeRules {
		t.Fatal("Abandon changed pinned rule input")
	}
	if got := captureReview6JobTable(t, db, "silence_recheck_job_candidates", 1, "processed_at", "outcome"); got != beforeCandidates {
		t.Fatal("Abandon changed pinned candidate input")
	}
	if got := captureReview6Attempt(t, db, 1, 1); got != beforeAttempt {
		t.Fatal("Abandon changed the original attempt row")
	}
	if got := captureReview6FirstError(t, db, 1); got != beforeFirstError {
		t.Fatal("Abandon changed first-error history")
	}
	var firstStatus, successorStatus string
	if err := db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&firstStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=2`).Scan(&successorStatus); err != nil {
		t.Fatal(err)
	}
	var jobs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if firstStatus != "abandoned" || successorStatus != "completed" || jobs != 2 {
		t.Fatalf("Abandon durable result statuses=%s,%s jobs=%d", firstStatus, successorStatus, jobs)
	}
}

func TestProblem3Review6UnknownFailureReconciliationResetsSelectionOwners(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 5, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	scheduler.backoffStep[recheckBackoffAcquisition] = 4
	scheduler.backoffStep[recheckBackoffPreAttempt] = 3
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, alertlifecycle.NewRecheckWriterCommitUnknownError(
			alertlifecycle.RecheckFailureWriterOperation,
			errors.New("review6 failure bookkeeping commit unknown"),
		)
	}

	scheduler.process(timer, nil)
	if _, ok := scheduler.backoffStep[recheckBackoffAcquisition]; ok {
		t.Fatalf("unknown recheck_failure kept acquisition backoff: %v", scheduler.backoffStep)
	}
	if _, ok := scheduler.backoffStep[recheckBackoffPreAttempt]; ok {
		t.Fatalf("unknown recheck_failure kept pre-attempt backoff: %v", scheduler.backoffStep)
	}
	if got := scheduler.backoffStep[recheckBackoffReconciliation]; got != 1 {
		t.Fatalf("unknown recheck_failure reconciliation step=%d want=1", got)
	}
	if len(clock.timer.resets) != 1 || clock.timer.resets[0] != time.Second {
		t.Fatalf("unknown recheck_failure timer=%v want=[1s]", clock.timer.resets)
	}
}

func TestProblem3Review6FailureBookkeepingAcquisitionUsesAcquisitionOwner(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 5, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	scheduler.backoffStep[recheckBackoffBookkeeping] = 3
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, alertlifecycle.NewRecheckFailurePersistenceError(
			errors.Join(alertlifecycle.ErrWriterAcquisition, schedulerSQLiteError(5)),
			true,
		)
	}

	scheduler.process(timer, nil)
	if got := scheduler.backoffStep[recheckBackoffAcquisition]; got != 1 {
		t.Fatalf("failure-bookkeeping acquisition step=%d want=1", got)
	}
	if got := scheduler.backoffStep[recheckBackoffBookkeeping]; got != 3 {
		t.Fatalf("failure-bookkeeping acquisition changed bookkeeping step=%d", got)
	}
	if len(clock.timer.resets) != 1 || clock.timer.resets[0] != time.Second {
		t.Fatalf("failure-bookkeeping acquisition timer=%v want=[1s]", clock.timer.resets)
	}
}

func TestProblem3Review6CommittedFailureReconciliationResetsAllCompletedOwners(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 5, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	scheduler.backoffStep[recheckBackoffAcquisition] = 4
	scheduler.backoffStep[recheckBackoffPreAttempt] = 3
	scheduler.backoffStep[recheckBackoffBookkeeping] = 2
	scheduler.backoffStep[recheckBackoffReconciliation] = 4
	runs := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		runs++
		if runs == 1 {
			return alertlifecycle.RecheckBatchResult{}, alertlifecycle.NewRecheckWriterCommitReconciledError(alertlifecycle.RecheckFailureWriterOperation)
		}
		return alertlifecycle.RecheckBatchResult{}, schedulerPreAttemptError{}
	}

	scheduler.process(timer, nil)
	if runs != 2 {
		t.Fatalf("runner calls=%d want=2", runs)
	}
	if _, ok := scheduler.backoffStep[recheckBackoffAcquisition]; ok {
		t.Fatalf("committed recheck_failure kept acquisition backoff: %v", scheduler.backoffStep)
	}
	if got := scheduler.backoffStep[recheckBackoffPreAttempt]; got != 1 {
		t.Fatalf("committed recheck_failure pre-attempt step=%d want=1", got)
	}
	if _, ok := scheduler.backoffStep[recheckBackoffBookkeeping]; ok {
		t.Fatalf("committed recheck_failure kept bookkeeping backoff: %v", scheduler.backoffStep)
	}
	if _, ok := scheduler.backoffStep[recheckBackoffReconciliation]; ok {
		t.Fatalf("committed recheck_failure kept reconciliation backoff: %v", scheduler.backoffStep)
	}
	if len(clock.timer.resets) != 1 || clock.timer.resets[0] != time.Second {
		t.Fatalf("committed recheck_failure timer=%v want=[1s]", clock.timer.resets)
	}
}

func TestProblem3Review6FailureReconciliationUnknownThenPresentThenUnknownUsesFreshBackoff(t *testing.T) {
	store, db, now := seedProblem3Review3SchedulerStore(t)
	lookupCalls := 0
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		RecheckFault: func(stage string) error {
			if stage == "persist_candidate" {
				return errors.New("review6 selected failure")
			}
			return nil
		},
		Commit: func(realCommit func() error) error {
			if err := realCommit(); err != nil {
				return err
			}
			return schedulerSQLiteError(5)
		},
		WriterCommitLookup: func(_ context.Context, _, operation string) (bool, error) {
			lookupCalls++
			if operation == "recheck_operator_action" {
				return true, nil
			}
			if lookupCalls == 2 {
				return true, nil
			}
			return false, context.DeadlineExceeded
		},
	})
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(scheduler.Signal)
	store.SetRecheckReconciledWake(scheduler.SignalReconciled)
	runs, _ := observeProblem3Review3Scheduler(scheduler)
	scheduler.Start()
	first := <-runs
	if !errors.Is(first.err, alertlifecycle.ErrWriterCommitOutcomeUnknown) || !errors.Is(first.err, alertlifecycle.ErrRecheckFailurePersistence) {
		t.Fatalf("first failure-bookkeeping result=%+v", first)
	}
	if got := <-clock.timer.resetEvents; got != time.Second {
		t.Fatalf("first reconciliation backoff=%s want=1s", got)
	}
	clock.timer.ch <- now.Add(time.Second)
	second := <-runs
	if !alertlifecycle.IsRecheckDurableFailure(second.err) || second.result.Disposition != alertlifecycle.RecheckBatchJobFailed {
		t.Fatalf("present reconciliation result=%+v", second)
	}
	actor := "review6-reconciliation"
	if _, err := store.RetryRecheckJobContext(context.Background(), 1, "review6-reconciliation-retry", "retry after durable failure", &actor, now.Add(time.Second)); err != nil {
		t.Fatalf("retry after present reconciliation=%v", err)
	}
	third := <-runs
	if !errors.Is(third.err, alertlifecycle.ErrWriterCommitOutcomeUnknown) || !errors.Is(third.err, alertlifecycle.ErrRecheckFailurePersistence) {
		t.Fatalf("second failure-bookkeeping result=%+v", third)
	}
	if got := <-clock.timer.resetEvents; got != time.Second {
		t.Fatalf("second reconciliation backoff=%s want=1s", got)
	}
	if got := scheduler.backoffStep[recheckBackoffReconciliation]; got != 1 {
		t.Fatalf("second reconciliation step=%d want=1", got)
	}
	if _, ok := scheduler.backoffStep[recheckBackoffAcquisition]; ok {
		t.Fatalf("reconciliation reset left acquisition owner: %v", scheduler.backoffStep)
	}
	if _, ok := scheduler.backoffStep[recheckBackoffPreAttempt]; ok {
		t.Fatalf("reconciliation reset left pre-attempt owner: %v", scheduler.backoffStep)
	}
	var attempts, attemptCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&attemptCount); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || attemptCount != 2 {
		t.Fatalf("reconciliation attempt accounting rows=%d parent=%d", attempts, attemptCount)
	}
	scheduler.Stop()
}
