package alertlifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestProblem3RetryDelay(t *testing.T) {
	tests := []struct {
		failures int
		want     time.Duration
		ok       bool
	}{
		{failures: 0, want: time.Second, ok: true},
		{failures: 1, want: 5 * time.Second, ok: true},
		{failures: 2, want: 30 * time.Second, ok: true},
		{failures: 3, want: 2 * time.Minute, ok: true},
		{failures: 4, want: 10 * time.Minute, ok: true},
		{failures: 5, ok: false},
		{failures: -1, ok: false},
	}
	for _, test := range tests {
		delay, ok := recheckRetryDelay(test.failures)
		if delay != test.want || ok != test.ok {
			t.Errorf("failures=%d delay=%s/%v want=%s/%v", test.failures, delay, ok, test.want, test.ok)
		}
	}
}

func TestProblem3QueueEligibility(t *testing.T) {
	now := time.Date(2026, time.August, 19, 1, 0, 0, 0, time.UTC)
	due := now.Add(-time.Second)
	notDue := now.Add(time.Second)
	for _, test := range []struct {
		name   string
		status string
		next   *time.Time
		want   bool
	}{
		{name: "queued", status: "queued", want: true},
		{name: "running", status: "running", want: true},
		{name: "retry due", status: "retry_wait", next: &due, want: true},
		{name: "retry not due", status: "retry_wait", next: &notDue, want: false},
		{name: "failed blocks", status: "failed", want: false},
		{name: "completed terminal", status: "completed", want: false},
		{name: "abandoned terminal", status: "abandoned", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := recheckQueueEligible(test.status, test.next, now); got != test.want {
				t.Fatalf("status=%s next=%v eligible=%v want=%v", test.status, test.next, got, test.want)
			}
		})
	}
}

func TestProblem3StateTransitions(t *testing.T) {
	now := time.Date(2026, time.August, 19, 1, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name    string
		state   recheckJobState
		event   recheckJobEvent
		want    recheckJobState
		wantErr bool
	}{
		{
			name:  "first selection",
			state: recheckJobState{Status: "queued"}, event: recheckJobSelected,
			want: recheckJobState{Status: "running"},
		},
		{
			name:  "resume selection",
			state: recheckJobState{Status: "running"}, event: recheckJobSelected,
			want: recheckJobState{Status: "running"},
		},
		{
			name:  "committed progress resets consecutive budget",
			state: recheckJobState{Status: "running", ConsecutiveRetryCount: 4, RetryCount: 4}, event: recheckJobBatchCommitted,
			want: recheckJobState{Status: "running", RetryCount: 4},
		},
		{
			name:  "committed final batch",
			state: recheckJobState{Status: "running", ConsecutiveRetryCount: 2}, event: recheckJobBatchCompleted,
			want: recheckJobState{Status: "completed", FinishedAt: &now},
		},
		{
			name:  "retryable failure schedules first retry",
			state: recheckJobState{Status: "running"}, event: recheckJobRetryableFailure,
			want: recheckJobState{Status: "retry_wait", RetryCount: 1, ConsecutiveRetryCount: 1, NextAttemptAt: timePtr(now.Add(time.Second))},
		},
		{
			name:  "sixth consecutive failure is terminal",
			state: recheckJobState{Status: "running", RetryCount: 5, ConsecutiveRetryCount: 5}, event: recheckJobRetryableFailure,
			want: recheckJobState{Status: "failed", RetryCount: 5, ConsecutiveRetryCount: 6, FinishedAt: &now},
		},
		{
			name:  "operator retry preserves job",
			state: recheckJobState{Status: "failed", RetryCount: 7, ConsecutiveRetryCount: 6, RecoveryCount: 1, FinishedAt: &now}, event: recheckJobOperatorRetry,
			want: recheckJobState{Status: "queued", RetryCount: 7, RecoveryCount: 2},
		},
		{
			name:  "operator abandon releases head",
			state: recheckJobState{Status: "failed", RetryCount: 5}, event: recheckJobOperatorAbandon,
			want: recheckJobState{Status: "abandoned", RetryCount: 5, FinishedAt: &now, AbandonedAt: &now},
		},
		{
			name: "completed cannot retry", state: recheckJobState{Status: "completed"}, event: recheckJobOperatorRetry, wantErr: true,
		},
		{
			name: "abandoned cannot retry", state: recheckJobState{Status: "abandoned"}, event: recheckJobOperatorRetry, wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := applyRecheckJobEvent(test.state, test.event, now)
			if test.wantErr {
				if err == nil {
					t.Fatal("transition unexpectedly succeeded")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !sameRecheckJobState(got, test.want) {
				t.Fatalf("state=%+v want=%+v", got, test.want)
			}
		})
	}
}

func TestProblem3RetryableErrorCodes(t *testing.T) {
	for _, code := range []int{5, 261, 517, 773} {
		if !isRetryableRecheckSQLiteCode(code) {
			t.Errorf("code %d was not classified as retryable", code)
		}
	}
	for _, code := range []int{6, 11, 13, 14, 275} {
		if isRetryableRecheckSQLiteCode(code) {
			t.Errorf("code %d was incorrectly classified as retryable", code)
		}
	}
	if classifyRecheckFailure(errors.New("x")) != recheckFailureTerminal {
		t.Fatal("unclassified errors must be terminal")
	}
}

func TestProblem3CommittedBatchWritesAttemptLedger(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	var status, outcome string
	var attemptCount, attempts int
	if err := store.db.QueryRow(`SELECT status,attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attemptCount); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*),COALESCE(MAX(outcome),'') FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts, &outcome); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || attemptCount != 1 || attempts != 1 || outcome != "committed" {
		t.Fatalf("job attempt state: status=%s attempt_count=%d attempts=%d outcome=%s", status, attemptCount, attempts, outcome)
	}
}

func TestProblem3TerminalFailurePersistsAttemptWithoutRetry(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("synthetic terminal failure")
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("terminal failure unexpectedly succeeded")
	}
	var status, outcome, failureClass, failureStage string
	var attempts int
	if err := store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT outcome,failure_class,failure_stage FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&outcome, &failureClass, &failureStage); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || outcome != "failed" || failureClass != "terminal" || failureStage != recheckStagePersistCandidate || attempts != 1 {
		t.Fatalf("terminal attempt state: status=%s outcome=%s class=%s stage=%s attempts=%d", status, outcome, failureClass, failureStage, attempts)
	}
}

type problem3SQLiteError int

func (e problem3SQLiteError) Error() string { return "synthetic sqlite error" }

func (e problem3SQLiteError) SQLiteCode() int { return int(e) }

func TestProblem3RetryableFailurePersistsAndResumes(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return problem3SQLiteError(5)
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.As(err, new(*recheckRetryScheduledError)) {
		t.Fatalf("retry scheduling error=%v", err)
	}
	var status, outcome string
	var retryCount, consecutive, attempts int
	if err := store.db.QueryRow(`SELECT status,retry_count,consecutive_retry_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &retryCount, &consecutive); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*),COALESCE(MAX(outcome),'') FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts, &outcome); err != nil {
		t.Fatal(err)
	}
	if status != "retry_wait" || retryCount != 1 || consecutive != 1 || attempts != 1 || outcome != "retry_scheduled" {
		t.Fatalf("retry state: status=%s retries=%d consecutive=%d attempts=%d outcome=%s", status, retryCount, consecutive, attempts, outcome)
	}
	store.lifecycleTestHooks = nil
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=1`, now.Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT status,consecutive_retry_count,attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &consecutive, &retryCount); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || consecutive != 0 || retryCount != 2 {
		t.Fatalf("resumed state: status=%s consecutive=%d attempt_count=%d", status, consecutive, retryCount)
	}
}

func TestProblem3OperatorRetryAbandonAndReplayCAS(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("synthetic terminal failure")
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("terminal failure unexpectedly succeeded")
	}
	var beforeFingerprint, beforeContext, beforeLabels string
	var beforeAttempts, beforeProcessed int
	if err := store.db.QueryRow(`SELECT job_fingerprint,source_context_json,attempt_count,processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&beforeFingerprint, &beforeContext, &beforeAttempts, &beforeProcessed); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT match_labels_json FROM silence_recheck_job_candidates WHERE job_id=1`).Scan(&beforeLabels); err != nil {
		t.Fatal(err)
	}
	actor := "alice"
	first, err := store.RetryRecheckJobContext(context.Background(), 1, "req-1", "try again", &actor, now)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Applied || first.CurrentStatus != "queued" {
		t.Fatalf("retry result: %+v", first)
	}
	var afterFingerprint, afterContext, afterLabels string
	var afterAttempts, afterProcessed int
	if queryErr := store.db.QueryRow(`SELECT job_fingerprint,source_context_json,attempt_count,processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&afterFingerprint, &afterContext, &afterAttempts, &afterProcessed); queryErr != nil {
		t.Fatal(queryErr)
	}
	if queryErr := store.db.QueryRow(`SELECT match_labels_json FROM silence_recheck_job_candidates WHERE job_id=1`).Scan(&afterLabels); queryErr != nil {
		t.Fatal(queryErr)
	}
	if beforeFingerprint != afterFingerprint || beforeContext != afterContext || beforeLabels != afterLabels || beforeAttempts != afterAttempts || beforeProcessed != afterProcessed {
		t.Fatal("operator retry changed immutable input or candidate progress")
	}
	replay, err := store.RetryRecheckJobContext(context.Background(), 1, "req-1", "try again", &actor, now.Add(time.Second))
	if err != nil || replay != first {
		t.Fatalf("retry replay: result=%+v err=%v want=%+v", replay, err, first)
	}
	otherActor := "bob"
	if _, err = store.RetryRecheckJobContext(context.Background(), 1, "req-1", "try again", &otherActor, now); !errors.Is(err, ErrRecheckOperatorConflict) {
		t.Fatalf("actor mismatch error=%v", err)
	}
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("second terminal failure unexpectedly succeeded")
	}
	abandoned, err := store.AbandonRecheckJobContext(context.Background(), 1, "req-2", "give up", &actor, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !abandoned.Applied || abandoned.CurrentStatus != "abandoned" {
		t.Fatalf("abandon result: %+v", abandoned)
	}
	var status string
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "abandoned" {
		t.Fatalf("final status=%s", status)
	}
}

func TestProblem3ConcurrentOperatorActionsUseSemanticCAS(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("terminal failure")
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("terminal failure unexpectedly succeeded")
	}
	actor := "concurrent-reviewer"
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, action := range []func() error{
		func() error {
			_, actionErr := store.RetryRecheckJobContext(context.Background(), 1, "concurrent-retry", "retry", &actor, now)
			return actionErr
		},
		func() error {
			_, actionErr := store.AbandonRecheckJobContext(context.Background(), 1, "concurrent-abandon", "abandon", &actor, now)
			return actionErr
		},
	} {
		wg.Add(1)
		go func(action func() error) { defer wg.Done(); results <- action() }(action)
	}
	wg.Wait()
	close(results)
	applied, conflicts := 0, 0
	for actionErr := range results {
		if actionErr == nil {
			applied++
		} else if errors.Is(actionErr, ErrRecheckJobStateConflict) {
			conflicts++
		}
	}
	if applied != 1 || conflicts != 1 {
		t.Fatalf("concurrent operator CAS results: applied=%d conflicts=%d", applied, conflicts)
	}
}

func timePtr(value time.Time) *time.Time { return &value }

func sameRecheckJobState(left, right recheckJobState) bool {
	return left.Status == right.Status &&
		left.RetryCount == right.RetryCount &&
		left.ConsecutiveRetryCount == right.ConsecutiveRetryCount &&
		left.RecoveryCount == right.RecoveryCount &&
		sameTimePtr(left.NextAttemptAt, right.NextAttemptAt) &&
		sameTimePtr(left.FinishedAt, right.FinishedAt) &&
		sameTimePtr(left.AbandonedAt, right.AbandonedAt)
}

func sameTimePtr(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}
