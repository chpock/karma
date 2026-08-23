package alertlifecycle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProblem8SelectedErrorPrecedenceDuringShutdown(t *testing.T) {
	tests := []struct {
		name        string
		cause       error
		wantStatus  string
		wantOutcome string
	}{
		{name: "retryable error first", cause: problem8SQLiteError(5), wantStatus: "retry_wait", wantOutcome: "retry_scheduled"},
		{name: "terminal error first", cause: errors.New("ordinary terminal candidate failure"), wantStatus: "failed", wantOutcome: "failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, db, _ := openProblem8Store(t)
			before, err := captureProblem8DurableSnapshot(store, db, 1)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reached := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			store.SetRecheckIntegrationTestHooks(&RecheckIntegrationTestHooks{
				RecheckFault: func(stage string) error {
					if stage == recheckStagePersistCandidate {
						return test.cause
					}
					return nil
				},
				WorkerBarrier: func(stage string) {
					if stage != "after_selected_error" {
						return
					}
					once.Do(func() { close(reached) })
					<-release
				},
			})
			resultCh := make(chan struct {
				result RecheckBatchResult
				err    error
			}, 1)
			go func() {
				result, err := store.RunSilenceRecheckBatchContext(ctx, 100)
				resultCh <- struct {
					result RecheckBatchResult
					err    error
				}{result, err}
			}()
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("ordinary selected error did not reach the precedence barrier")
			}
			cancel()
			close(release)
			outcome := <-resultCh
			after, snapshotErr := captureProblem8DurableSnapshot(store, db, 1)
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			retryError := &recheckRetryScheduledError{}
			hasExpectedError := IsRecheckDurableFailure(outcome.err) || (test.wantStatus == "retry_wait" && errors.As(outcome.err, &retryError))
			if outcome.err == nil || !hasExpectedError || outcome.result.Status != test.wantStatus || outcome.result.FailureClass == "worker_cancelled" {
				t.Fatalf("result=%+v err=%v", outcome.result, outcome.err)
			}
			var status, attemptOutcome string
			var attempts int
			if err := db.QueryRow(`SELECT status,attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attempts); err != nil {
				t.Fatal(err)
			}
			if status != test.wantStatus || attempts != 1 {
				t.Fatalf("job status=%s attempts=%d want=%s/1", status, attempts, test.wantStatus)
			}
			if err := db.QueryRow(`SELECT outcome FROM silence_recheck_job_attempts WHERE job_id=1 AND attempt_number=1`).Scan(&attemptOutcome); err != nil {
				t.Fatal(err)
			}
			if attemptOutcome != test.wantOutcome {
				t.Fatalf("attempt outcome=%s want=%s", attemptOutcome, test.wantOutcome)
			}
			expected, expectedErr := problem8ExpectedFailureSnapshot(before, test.wantStatus == "retry_wait", "persist_candidate", "persist_candidate: "+test.cause.Error())
			if expectedErr != nil {
				t.Fatal(expectedErr)
			}
			if got := canonicalProblem8Snapshot(after); got != expected {
				t.Fatalf("failure phase snapshot mismatch\nwant=%s\ngot=%s", expected, got)
			}
		})
	}
}

func TestProblem8CancellationFirstDoesNotCreateFailureHistory(t *testing.T) {
	store, db, _ := openProblem8Store(t)
	before, err := captureProblem8DurableSnapshot(store, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	store.SetRecheckIntegrationTestHooks(&RecheckIntegrationTestHooks{RecheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return context.Canceled
		}
		return nil
	}})
	result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if err == nil || !errors.Is(err, context.Canceled) || result.Disposition != RecheckBatchInfrastructureFailed || result.FailureClass != "worker_cancelled" {
		t.Fatalf("cancellation-first result=%+v err=%v", result, err)
	}
	var status string
	var attempts int
	if err = db.QueryRow(`SELECT status,attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "queued" || attempts != 0 {
		t.Fatalf("cancellation-first durable state=%s/%d", status, attempts)
	}
	var history int
	if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if history != 0 {
		t.Fatalf("cancellation-first created attempt history=%d", history)
	}
	after, err := captureProblem8DurableSnapshot(store, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if canonicalProblem8Snapshot(before) != canonicalProblem8Snapshot(after) {
		t.Fatalf("cancellation-first changed durable snapshot\nbefore=%s\nafter=%s", before, after)
	}
}

func TestProblem8UnknownCommitPrecedesLaterCancellation(t *testing.T) {
	store, db, _ := openProblem8Store(t)
	before, err := captureProblem8DurableSnapshot(store, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.SetRecheckIntegrationTestHooks(&RecheckIntegrationTestHooks{Commit: func(commit func() error) error {
		if commitErr := commit(); commitErr != nil {
			return commitErr
		}
		cancel()
		return errors.New("ordinary commit result became unknown")
	}, WriterCommitLookup: func(context.Context, string, string) (bool, error) {
		return false, context.DeadlineExceeded
	}})
	result, err := store.RunSilenceRecheckBatchContext(ctx, 100)
	if err == nil || !errors.Is(err, ErrWriterCommitOutcomeUnknown) || result.Disposition != RecheckBatchCommitOutcomeUnknown || result.FailureClass == "worker_cancelled" {
		t.Fatalf("unknown-commit result=%+v err=%v", result, err)
	}
	unknown, err := captureProblem8DurableSnapshot(store, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	expectedUnknown, expectedErr := problem8ExpectedCommittedSnapshot(before, true)
	if expectedErr != nil {
		t.Fatal(expectedErr)
	}
	if got, want := canonicalProblem8Snapshot(unknown), expectedUnknown; got != want {
		t.Fatalf("unknown-commit phase snapshot mismatch\nwant=%s\ngot=%s", want, got)
	}
	if before == unknown || store.pendingWriterCommit == nil {
		t.Fatalf("unknown-commit did not retain a distinct pending writer state: before=%s unknown=%s pending=%v", before, unknown, store.pendingWriterCommit != nil)
	}
	store.SetRecheckIntegrationTestHooks(&RecheckIntegrationTestHooks{WriterCommitLookup: func(context.Context, string, string) (bool, error) {
		return true, nil
	}})
	reconciled, reconcileErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if reconcileErr != nil || reconciled.ReconciledOperation != writerOperationRecheckBatch || store.pendingWriterCommit != nil {
		t.Fatalf("unknown-commit reconciliation result=%+v err=%v pending=%v", reconciled, reconcileErr, store.pendingWriterCommit != nil)
	}
	final, err := captureProblem8DurableSnapshot(store, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	expectedFinal, expectedErr := problem8ExpectedCommittedSnapshot(before, false)
	if expectedErr != nil {
		t.Fatal(expectedErr)
	}
	if got, want := canonicalProblem8Snapshot(final), expectedFinal; got != want {
		t.Fatalf("reconciled phase snapshot mismatch\nwant=%s\ngot=%s", want, got)
	}
	if final == unknown {
		t.Fatal("unknown-commit reconciliation did not change the durable snapshot")
	}
	var status, outcome string
	if err = db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT outcome FROM silence_recheck_job_attempts WHERE job_id=1 AND attempt_number=1`).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || outcome != "committed" {
		t.Fatalf("reconciled unknown commit state=%s outcome=%s", status, outcome)
	}
}

type problem8SnapshotEnvelope struct {
	Jobs       string `json:"jobs"`
	Candidates string `json:"candidates"`
	Attempts   string `json:"attempts"`
	Events     string `json:"events"`
	Ledger     string `json:"ledger"`
	Actions    string `json:"actions"`
	Pending    any    `json:"pending"`
}

func captureProblem8DurableSnapshot(store *Store, db *sql.DB, jobID int64) (string, error) {
	rows := func(query string, args ...any) (string, error) {
		result, err := db.Query(query, args...)
		if err != nil {
			return "", err
		}
		defer result.Close()
		columns, err := result.Columns()
		if err != nil {
			return "", err
		}
		values := make([]map[string]any, 0)
		for result.Next() {
			cells := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range cells {
				pointers[i] = &cells[i]
			}
			if err = result.Scan(pointers...); err != nil {
				return "", err
			}
			row := make(map[string]any, len(columns))
			for i, value := range cells {
				if bytes, ok := value.([]byte); ok {
					row[columns[i]] = string(bytes)
				} else {
					row[columns[i]] = value
				}
			}
			values = append(values, row)
		}
		if err = result.Err(); err != nil {
			return "", err
		}
		encoded, err := json.Marshal(values)
		return string(encoded), err
	}
	jobRows, err := rows(`SELECT * FROM silence_recheck_jobs WHERE job_id=?`, jobID)
	if err != nil {
		return "", err
	}
	candidateRows, err := rows(`SELECT * FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`, jobID)
	if err != nil {
		return "", err
	}
	attemptRows, err := rows(`SELECT * FROM silence_recheck_job_attempts WHERE job_id=? ORDER BY attempt_number`, jobID)
	if err != nil {
		return "", err
	}
	eventRows, err := rows(`SELECT * FROM alert_events ORDER BY id`)
	if err != nil {
		return "", err
	}
	ledgerRows, err := rows(`SELECT * FROM lifecycle_writer_commits ORDER BY operation_token`)
	if err != nil {
		return "", err
	}
	actionRows, err := rows(`SELECT * FROM silence_recheck_job_operator_actions WHERE job_id=? ORDER BY action_id`, jobID)
	if err != nil {
		return "", err
	}
	pending := map[string]any{"writer": nil, "failure": nil}
	if store.pendingWriterCommit != nil {
		pending["writer"] = map[string]any{"token": store.pendingWriterCommit.token, "kind": store.pendingWriterCommit.kind}
	}
	if failure := store.pendingRecheckFailureSnapshot(); failure != nil {
		pending["failure"] = map[string]any{"token": failure.Attempt.Token, "job_id": failure.Attempt.JobID, "attempt_number": failure.Attempt.Number, "operation": failure.Operation, "result_ready": failure.ResultReady, "status": failure.Result.Status, "disposition": failure.Result.Disposition}
	}
	encoded, err := json.Marshal(problem8SnapshotEnvelope{Jobs: jobRows, Candidates: candidateRows, Attempts: attemptRows, Events: eventRows, Ledger: ledgerRows, Actions: actionRows, Pending: pending})
	return string(encoded), err
}

func canonicalProblem8Snapshot(raw string) string {
	var envelope problem8SnapshotEnvelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return raw
	}
	for name, value := range map[string]*string{
		"jobs": &envelope.Jobs, "candidates": &envelope.Candidates, "attempts": &envelope.Attempts,
		"events": &envelope.Events, "ledger": &envelope.Ledger, "actions": &envelope.Actions,
	} {
		var rows []map[string]any
		if err := json.Unmarshal([]byte(*value), &rows); err != nil {
			return raw
		}
		for _, row := range rows {
			for field, cell := range row {
				text, ok := cell.(string)
				if !ok || text == "" || strings.HasPrefix(text, "<") {
					continue
				}
				switch field {
				case "operation_token":
					row[field] = "<operation-token>"
				case "attempt_token":
					row[field] = "<attempt-token>"
				case "started_at", "finished_at", "first_error_at", "last_error_at", "next_attempt_at", "scheduled_retry_at", "processed_at", "changed_at", "committed_at":
					row[field] = "<timestamp>"
				}
			}
		}
		sort.SliceStable(rows, func(i, j int) bool {
			return problem8SnapshotRowKey(name, rows[i]) < problem8SnapshotRowKey(name, rows[j])
		})
		encoded, err := json.Marshal(rows)
		if err != nil {
			return raw
		}
		*value = string(encoded)
	}
	problem8NormalizePending(envelope.Pending)
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return raw
	}
	return string(encoded)
}

func problem8SnapshotRowKey(table string, row map[string]any) string {
	fields := []string{"job_id", "alert_id", "target_key", "attempt_number", "id", "action_id", "operation_kind", "outcome"}
	parts := make([]string, 0, len(fields)+1)
	parts = append(parts, table)
	for _, field := range fields {
		if value, ok := row[field]; ok {
			parts = append(parts, fmt.Sprint(value))
		}
	}
	encoded, _ := json.Marshal(row)
	return strings.Join(parts, "\x00") + "\x00" + string(encoded)
}

func problem8NormalizePending(pending any) {
	state, ok := pending.(map[string]any)
	if !ok {
		return
	}
	if writer, ok := state["writer"].(map[string]any); ok {
		if _, exists := writer["token"]; exists {
			writer["token"] = "<operation-token>"
		}
	}
	if failure, ok := state["failure"].(map[string]any); ok {
		if _, exists := failure["token"]; exists {
			failure["token"] = "<attempt-token>"
		}
	}
}

func problem8ExpectedFailureSnapshot(before string, retryable bool, stage, message string) (string, error) {
	envelope, err := problem8DecodeSnapshot(before)
	if err != nil {
		return "", err
	}
	var jobs []map[string]any
	if err = json.Unmarshal([]byte(envelope.Jobs), &jobs); err != nil {
		return "", fmt.Errorf("problem8 expected failure jobs: %w", err)
	}
	if len(jobs) != 1 {
		return "", fmt.Errorf("problem8 expected one failure job, got %d", len(jobs))
	}
	job := jobs[0]
	class, status, outcome := "terminal", "failed", "failed"
	if retryable {
		class, status, outcome = "retryable", "retry_wait", "retry_scheduled"
	}
	job["status"], job["attempt_count"] = status, float64(1)
	job["retry_count"], job["consecutive_retry_count"] = float64(0), float64(0)
	if retryable {
		job["retry_count"], job["consecutive_retry_count"] = float64(1), float64(1)
	}
	job["started_at"], job["first_error_at"], job["last_error_at"] = "<timestamp>", "<timestamp>", "<timestamp>"
	job["first_error_class"], job["last_error_class"] = class, class
	job["first_error_stage"], job["last_error_stage"] = stage, stage
	job["first_error_message"], job["last_error_message"] = message, message
	job["finished_at"] = nil
	job["next_attempt_at"] = nil
	if !retryable {
		job["finished_at"] = "<timestamp>"
	}
	if retryable {
		job["next_attempt_at"] = "<timestamp>"
	}
	attempts := make([]map[string]any, 0, 1)
	if err = json.Unmarshal([]byte(envelope.Attempts), &attempts); err != nil {
		return "", err
	}
	attempts = append(attempts, map[string]any{
		"attempt_id": float64(1), "attempt_number": float64(1), "attempt_token": "<attempt-token>",
		"failure_class": class, "failure_stage": stage, "finished_at": "<timestamp>",
		"job_id": float64(1), "outcome": outcome, "scheduled_retry_at": nil, "started_at": "<timestamp>",
	})
	if retryable {
		attempts[len(attempts)-1]["scheduled_retry_at"] = "<timestamp>"
	}
	encodedJobs, _ := json.Marshal(jobs)
	encodedAttempts, _ := json.Marshal(attempts)
	envelope.Jobs = string(encodedJobs)
	envelope.Attempts = string(encodedAttempts)
	ledger := make([]map[string]any, 0, 1)
	if err = json.Unmarshal([]byte(envelope.Ledger), &ledger); err != nil {
		return "", err
	}
	ledger = append(ledger, map[string]any{"committed_at": "<timestamp>", "operation_kind": "recheck_failure", "operation_token": "<operation-token>"})
	encodedLedger, _ := json.Marshal(ledger)
	envelope.Ledger = string(encodedLedger)
	envelope.Pending = map[string]any{"failure": nil, "writer": nil}
	return canonicalProblem8Snapshot(problem8EncodeSnapshot(envelope)), nil
}

func problem8ExpectedCommittedSnapshot(before string, pending bool) (string, error) {
	envelope, err := problem8DecodeSnapshot(before)
	if err != nil {
		return "", err
	}
	var jobs, candidates []map[string]any
	ledger := make([]map[string]any, 0, 1)
	events := make([]map[string]any, 0, 1)
	attempts := make([]map[string]any, 0, 1)
	for raw, target := range map[*string]*[]map[string]any{
		&envelope.Jobs: &jobs, &envelope.Candidates: &candidates, &envelope.Attempts: &attempts,
		&envelope.Events: &events, &envelope.Ledger: &ledger,
	} {
		if err = json.Unmarshal([]byte(*raw), target); err != nil {
			return "", err
		}
	}
	if len(jobs) != 1 || len(candidates) != 1 {
		return "", errors.New("problem8 expected committed fixture shape changed")
	}
	job := jobs[0]
	job["status"], job["cursor_alert_id"] = "completed", "problem8-alert"
	job["attempt_count"], job["processed_candidates"], job["promoted_candidates"] = float64(1), float64(1), float64(1)
	job["started_at"], job["finished_at"] = "<timestamp>", "<timestamp>"
	candidates[0]["outcome"], candidates[0]["processed_at"] = "promoted", "<timestamp>"
	attempts = append(attempts, map[string]any{
		"attempt_id": float64(1), "attempt_number": float64(1), "attempt_token": "<attempt-token>",
		"failure_class": nil, "failure_stage": nil, "finished_at": "<timestamp>", "job_id": float64(1),
		"outcome": "committed", "scheduled_retry_at": nil, "started_at": "<timestamp>",
	})
	events = append(events, map[string]any{
		"alert_id": "problem8-alert", "change_source": "silence", "changed_at": "<timestamp>", "changed_by": nil,
		"id": float64(1), "new_is_live": float64(0), "new_is_resolved": float64(0), "new_is_suppressed": float64(1),
		"new_raw_state": "active", "new_state": "suppressed", "old_is_live": float64(0), "old_is_resolved": float64(0),
		"old_is_suppressed": float64(0), "old_raw_state": "active", "old_state": "inactive", "reason_code": "silence_match_promoted", "reason_text": nil,
	})
	ledger = append(ledger, map[string]any{"committed_at": "<timestamp>", "operation_kind": "recheck_batch", "operation_token": "<operation-token>"})
	var pendingState any
	if pending {
		pendingState = map[string]any{
			"failure": map[string]any{"attempt_number": float64(1), "disposition": "", "job_id": float64(1), "operation": "recheck_batch", "result_ready": true, "status": "completed", "token": "<attempt-token>"},
			"writer":  map[string]any{"kind": "recheck_batch", "token": "<operation-token>"},
		}
	} else {
		pendingState = map[string]any{"failure": nil, "writer": nil}
	}
	for raw, value := range map[*string]any{&envelope.Jobs: jobs, &envelope.Candidates: candidates, &envelope.Attempts: attempts, &envelope.Events: events, &envelope.Ledger: ledger} {
		encoded, encodeErr := json.Marshal(value)
		if encodeErr != nil {
			return "", encodeErr
		}
		*raw = string(encoded)
	}
	envelope.Pending = pendingState
	return canonicalProblem8Snapshot(problem8EncodeSnapshot(envelope)), nil
}

func problem8DecodeSnapshot(raw string) (problem8SnapshotEnvelope, error) {
	var envelope problem8SnapshotEnvelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return envelope, err
	}
	return envelope, nil
}

func problem8EncodeSnapshot(envelope problem8SnapshotEnvelope) string {
	encoded, _ := json.Marshal(envelope)
	return string(encoded)
}

func TestProblem8ConfirmedNotCommittedCausePrecedesLaterCancellation(t *testing.T) {
	for _, test := range []struct {
		name       string
		cause      error
		wantStatus string
		wantResult string
	}{
		{name: "retryable", cause: problem8SQLiteError(5), wantStatus: "retry_wait", wantResult: "retry_scheduled"},
		{name: "terminal", cause: problem8SQLiteError(13), wantStatus: "failed", wantResult: "failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, db, _ := openProblem8Store(t)
			before, err := captureProblem8DurableSnapshot(store, db, 1)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reached := make(chan struct{})
			release := make(chan struct{})
			lookupCalled := make(chan struct{}, 1)
			firstCommit := true
			store.SetRecheckIntegrationTestHooks(&RecheckIntegrationTestHooks{
				Commit: func(commit func() error) error {
					if firstCommit {
						firstCommit = false
						return test.cause
					}
					return commit()
				},
				WriterCommitLookup: func(context.Context, string, string) (bool, error) {
					lookupCalled <- struct{}{}
					return false, nil
				},
				WorkerBarrier: func(stage string) {
					if stage != "after_selected_error" {
						return
					}
					close(reached)
					<-release
				},
			})
			resultCh := make(chan struct {
				result RecheckBatchResult
				err    error
			}, 1)
			go func() {
				result, runErr := store.RunSilenceRecheckBatchContext(ctx, 100)
				resultCh <- struct {
					result RecheckBatchResult
					err    error
				}{result, runErr}
			}()
			select {
			case <-lookupCalled:
			case <-time.After(5 * time.Second):
				t.Fatal("not-committed path did not perform writer-token lookup")
			}
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("not-committed result did not reach causal barrier")
			}
			cancel()
			close(release)
			outcome := <-resultCh
			if outcome.err == nil || outcome.result.Status != test.wantStatus || outcome.result.FailureClass == "worker_cancelled" {
				t.Fatalf("not-committed result=%+v err=%v", outcome.result, outcome.err)
			}
			var status, attemptOutcome string
			var attempts int
			if queryErr := db.QueryRow(`SELECT status,attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attempts); queryErr != nil {
				t.Fatal(queryErr)
			}
			if status != test.wantStatus || attempts != 1 {
				t.Fatalf("not-committed durable state=%s/%d want=%s/1", status, attempts, test.wantStatus)
			}
			if queryErr := db.QueryRow(`SELECT outcome FROM silence_recheck_job_attempts WHERE job_id=1 AND attempt_number=1`).Scan(&attemptOutcome); queryErr != nil {
				t.Fatal(queryErr)
			}
			if attemptOutcome != test.wantResult {
				t.Fatalf("not-committed attempt outcome=%s want=%s", attemptOutcome, test.wantResult)
			}
			after, err := captureProblem8DurableSnapshot(store, db, 1)
			if err != nil {
				t.Fatal(err)
			}
			expected, expectedErr := problem8ExpectedFailureSnapshot(before, test.wantStatus == "retry_wait", "commit", test.cause.Error())
			if expectedErr != nil {
				t.Fatal(expectedErr)
			}
			if got := canonicalProblem8Snapshot(after); got != expected {
				t.Fatalf("not-committed phase snapshot mismatch\nwant=%s\ngot=%s", expected, got)
			}
		})
	}
}

type problem8SQLiteError int

func (e problem8SQLiteError) Error() string   { return fmt.Sprintf("controlled sqlite error %d", e) }
func (e problem8SQLiteError) SQLiteCode() int { return int(e) }

func openProblem8Store(t *testing.T) (*Store, *sql.DB, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "problem8.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
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
	labels := `{"alertname":"Problem8","service":"problem8"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "problem8-alert", State: StateInactive, RawState: StateActive, Receiver: "problem8", GroupID: "problem8", GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `["problem8-am"]`, RelevantSources: `[{"name":"problem8-am","cluster":"problem8-cluster"}]`, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	result, err := store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"problem8-am": true},
		SilenceSources: []SourceSilenceState{{
			SourceName: "problem8-am", ClusterID: "problem8-cluster", SyncStatus: "success",
			Silences: []SourceSilenceSnapshot{{
				SilenceID: "problem8-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "problem8", IsEqual: true}},
			}},
		}},
		Timestamp: now,
	})
	if err != nil || result.RecheckJobsQueued != 1 {
		t.Fatalf("problem8 fixture result=%+v err=%v", result, err)
	}
	return store, db, now
}
