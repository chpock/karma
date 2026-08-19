package alertlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestProblem3ReviewTerminalFailurePreservesRetryBudget(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("terminal review failure")
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("terminal failure unexpectedly succeeded")
	}
	var consecutive int
	if err := store.db.QueryRow(`SELECT consecutive_retry_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&consecutive); err != nil {
		t.Fatal(err)
	}
	if consecutive != 0 {
		t.Fatalf("terminal failure consumed retry budget: consecutive=%d", consecutive)
	}
}

func TestProblem3ReviewCompletionPreservesLastError(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return problem3SQLiteError(5)
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("retryable failure unexpectedly succeeded")
	}
	store.lifecycleTestHooks = nil
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=1`, now.Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, runErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000); runErr != nil {
		t.Fatal(runErr)
	}
	var lastError string
	if err := store.db.QueryRow(`SELECT COALESCE(last_error_message,'') FROM silence_recheck_jobs WHERE job_id=1`).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if lastError == "" {
		t.Fatal("successful completion erased the latest failure")
	}
}

func TestProblem3ReviewConfirmedNotCommittedUsesUnderlyingFailureClass(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	commitCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if commitCalls == 1 {
			return errors.New("disk full")
		}
		return tx.Commit()
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return false, nil }
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("confirmed terminal commit failure unexpectedly succeeded")
	}
	var status, outcome, failureClass string
	if err := store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT outcome,failure_class FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&outcome, &failureClass); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || outcome != "failed" || failureClass != "terminal" {
		t.Fatalf("commit failure was retried: status=%s outcome=%s class=%s", status, outcome, failureClass)
	}
}

func TestProblem3ReviewCommitCauseAllowlistProductionBoundary(t *testing.T) {
	for _, test := range []struct {
		name   string
		code   int
		status string
	}{
		{name: "busy", code: 5, status: "retry_wait"},
		{name: "busy extended", code: 261, status: "retry_wait"},
		{name: "busy snapshot", code: 517, status: "retry_wait"},
		{name: "busy recovery", code: 773, status: "retry_wait"},
		{name: "locked", code: 6, status: "failed"},
		{name: "io", code: 10, status: "failed"},
		{name: "corrupt", code: 11, status: "failed"},
		{name: "full", code: 13, status: "failed"},
		{name: "constraint", code: 275, status: "failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			commitCalls := 0
			store.writerCommit = func(tx *sql.Tx) error {
				commitCalls++
				if commitCalls == 1 {
					return problem3SQLiteError(test.code)
				}
				return tx.Commit()
			}
			store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return false, nil }
			if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
				t.Fatal("commit failure unexpectedly succeeded")
			}
			var status string
			if err := store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != test.status {
				t.Fatalf("code=%d status=%s want=%s", test.code, status, test.status)
			}
		})
	}
}

func TestProblem3ReviewFailureBookkeepingCommitReconciliation(t *testing.T) {
	for _, mode := range []string{"present", "absent", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			store, _, now := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			beforeDomain := captureProblem3Review2DomainState(t, store.db)
			attempt := recheckAttemptInfo{JobID: 1, Token: "bookkeeping-" + mode, Number: 1, StartedAt: now, FailureClass: recheckFailureTerminal, FailureStage: recheckStageEvaluate, FailureCause: errors.New("bookkeeping failure")}
			commitCalls := 0
			store.writerCommit = func(tx *sql.Tx) error {
				commitCalls++
				if commitCalls == 1 {
					if mode == "present" {
						if err := tx.Commit(); err != nil {
							return err
						}
					}
					return errors.New("bookkeeping commit error")
				}
				return tx.Commit()
			}
			lookupCalls := 0
			store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
				lookupCalls++
				switch {
				case mode == "present":
					return true, nil
				case mode == "unknown" && lookupCalls == 1:
					return false, errors.New("lookup unavailable")
				default:
					return false, nil
				}
			}
			result, err := store.persistRecheckAttemptFailure(context.Background(), attempt)
			switch mode {
			case "present":
				if err != nil || result.Status != "failed" {
					t.Fatalf("present reconciliation result=%+v err=%v", result, err)
				}
			case "absent":
				if !errors.Is(err, ErrRecheckFailurePersistence) {
					t.Fatalf("absent reconciliation error=%v", err)
				}
			default:
				if !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
					t.Fatalf("unknown reconciliation error=%v", err)
				}
			}
			if mode != "present" {
				store.writerCommit = func(tx *sql.Tx) error { return tx.Commit() }
				result, err = store.persistRecheckAttemptFailure(context.Background(), attempt)
				if err != nil || result.Status != "failed" {
					t.Fatalf("reconciled retry result=%+v err=%v", result, err)
				}
			}
			var attempts int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			if attempts != 1 {
				t.Fatalf("bookkeeping attempts=%d want=1", attempts)
			}
			if afterDomain := captureProblem3Review2DomainState(t, store.db); afterDomain != beforeDomain {
				t.Fatal("failure bookkeeping reconciliation changed candidate or suppression domain state")
			}
		})
	}
}

func TestProblem3ReviewPendingFailureBookkeepingDoesNotStartSecondAttempt(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("terminal candidate batch failure")
		}
		return nil
	}}
	commitCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if commitCalls == 1 {
			return errors.New("failure bookkeeping unavailable")
		}
		return tx.Commit()
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return false, nil }
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrRecheckFailurePersistence) {
		t.Fatalf("initial bookkeeping result=%v", err)
	}
	store.writerCommit = func(tx *sql.Tx) error { return tx.Commit() }
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !IsRecheckDurableFailure(err) {
		t.Fatalf("durable failed-head result=%v", err)
	}
	var attempts, attemptCount int
	var outcome, status string
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT attempt_count,status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&attemptCount, &status); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || attemptCount != 1 || outcome != "failed" || status != "failed" {
		t.Fatalf("pending bookkeeping started duplicate attempt: attempts=%d count=%d outcome=%s status=%s", attempts, attemptCount, outcome, status)
	}
}

func TestProblem3ReviewAttemptIdentityMustMatchFullTuple(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	attempt := recheckAttemptInfo{JobID: 999, Token: "review-token", Number: 1, StartedAt: now, FailureClass: recheckFailureTerminal, FailureStage: recheckStageEvaluate, FailureCause: errors.New("review")}
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome,failure_class,failure_stage) VALUES(?,?,?,?,?,?,?,?)`, 1, attempt.Token, 1, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), "failed", "terminal", recheckStageEvaluate); err != nil {
		t.Fatal(err)
	}
	if err := store.validateRecheckAttemptTupleLocked(context.Background(), attempt, "failed", false); !errors.Is(err, ErrWriterOperationInvariant) {
		t.Fatalf("wrong job identity was accepted: %v", err)
	}
	for _, test := range []struct {
		name          string
		token         string
		attemptNumber int
		outcome       string
		wantNumber    int
		wantOutcome   string
	}{
		{name: "wrong attempt number", token: "review-number", attemptNumber: 2, outcome: "failed", wantNumber: 1, wantOutcome: "failed"},
		{name: "wrong expected outcome", token: "review-outcome", attemptNumber: 3, outcome: "failed", wantNumber: 3, wantOutcome: "retry_scheduled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome,failure_class,failure_stage) VALUES(?,?,?,?,?,?,?,?)`, 1, test.token, test.attemptNumber, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), test.outcome, "terminal", recheckStageEvaluate); err != nil {
				t.Fatal(err)
			}
			candidate := recheckAttemptInfo{JobID: 1, Token: test.token, Number: test.wantNumber, StartedAt: now}
			if err := store.validateRecheckAttemptTupleLocked(context.Background(), candidate, test.wantOutcome, false); !errors.Is(err, ErrWriterOperationInvariant) {
				t.Fatalf("invalid tuple was accepted: %v", err)
			}
		})
	}
}

func TestProblem3ReviewAttemptRowsRequireCompleteValues(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,outcome) VALUES(?,?,?,?,?)`, 1, "missing-finished", 1, now.Format(time.RFC3339Nano), "failed"); err == nil {
		t.Fatal("attempt without finished_at was accepted")
	}
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,requested_at) VALUES(?,?,?,?,?)`, 1, "missing-applied", "retry", "fp", now.Format(time.RFC3339Nano)); err == nil {
		t.Fatal("operator action without applied fields was accepted")
	}
}

type reviewFailingTransactionStarter struct{ err error }

func (s reviewFailingTransactionStarter) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	return nil, s.err
}

func TestProblem3ReviewWriterAcquisitionIsTypedAndLeavesJobUnchanged(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.writerTransactionStarter = reviewFailingTransactionStarter{err: problem3SQLiteError(5)}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterAcquisition) || !IsRetryableRecheckWriterAcquisition(err) {
		t.Fatalf("acquisition error=%v, want typed retryable acquisition", err)
	}
	var status string
	var attempts int
	if err := store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if status != "queued" || attempts != 0 {
		t.Fatalf("acquisition changed durable state: status=%s attempts=%d", status, attempts)
	}
}

func TestProblem3ReviewAttemptCountMismatchIsInvariant(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET attempt_count=1 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterOperationInvariant) || !errors.Is(err, ErrRecheckPreAttempt) {
		t.Fatalf("count mismatch error=%v, want invariant pre-attempt error", err)
	}
}

func TestProblem3ReviewDelayedBatchReconciliationWakesScheduler(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	wake := make(chan struct{}, 1)
	wakeCalls := 0
	store.SetRecheckReconciledWake(func() { wakeCalls++; wake <- struct{}{} })
	lookupCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return errors.New("ambiguous commit")
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, errors.New("lookup delayed")
		}
		return true, nil
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("initial commit outcome=%v", err)
	}
	select {
	case <-wake:
		t.Fatal("scheduler woke before commit reconciliation")
	default:
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitReconciled) {
		t.Fatalf("reconciliation outcome=%v", err)
	}
	select {
	case <-wake:
	default:
		t.Fatal("scheduler wake was lost after delayed commit reconciliation")
	}
	if wakeCalls != 1 {
		t.Fatalf("delayed commit wake calls=%d want=1", wakeCalls)
	}
}

func TestProblem3ReviewOrdinaryBatchCommitDoesNotWakeScheduler(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	wake := make(chan struct{}, 1)
	store.SetRecheckCommitWake(func() { wake <- struct{}{} })
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
		t.Fatal("ordinary recheck batch commit self-woke scheduler")
	default:
	}
}

func TestProblem3ReviewDelayedOperatorReplayReturnsCommittedResult(t *testing.T) {
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
	store.lifecycleTestHooks = nil
	commitCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if err := tx.Commit(); err != nil {
			return err
		}
		if commitCalls == 1 {
			return errors.New("ambiguous operator commit")
		}
		return nil
	}
	lookupCalls := 0
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, errors.New("lookup delayed")
		}
		return true, nil
	}
	actor := "reviewer"
	if _, err := store.RetryRecheckJobContext(context.Background(), 1, "delayed-retry", "retry", &actor, now); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("initial operator outcome=%v", err)
	}
	replay, err := store.RetryRecheckJobContext(context.Background(), 1, "delayed-retry", "retry", &actor, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Applied || replay.CurrentStatus != "queued" {
		t.Fatalf("replayed operator result=%+v", replay)
	}
}

func TestProblem3ReviewProductionRetryBudgetAndErrorHistory(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return problem3SQLiteError(5)
		}
		return nil
	}}
	for attempt := 1; attempt <= 6; attempt++ {
		if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
			t.Fatalf("attempt %d unexpectedly succeeded", attempt)
		}
		var status, next string
		var retryCount, consecutive int
		if err := store.db.QueryRow(`SELECT status,retry_count,consecutive_retry_count,COALESCE(next_attempt_at,'') FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &retryCount, &consecutive, &next); err != nil {
			t.Fatal(err)
		}
		if attempt <= 5 {
			if status != "retry_wait" || retryCount != attempt || consecutive != attempt || next == "" {
				t.Fatalf("attempt %d state: status=%s retries=%d consecutive=%d next=%q", attempt, status, retryCount, consecutive, next)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=1`, now.Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if status != "failed" || retryCount != 5 || consecutive != 6 || next != "" {
			t.Fatalf("sixth failure state: status=%s retries=%d consecutive=%d next=%q", status, retryCount, consecutive, next)
		}
	}
	var firstError, lastError string
	if err := store.db.QueryRow(`SELECT COALESCE(first_error_message,''),COALESCE(last_error_message,'') FROM silence_recheck_jobs WHERE job_id=1`).Scan(&firstError, &lastError); err != nil {
		t.Fatal(err)
	}
	if firstError == "" || lastError == "" {
		t.Fatalf("error history missing: first=%q last=%q", firstError, lastError)
	}
}

func TestProblem3ReviewStrictFIFOAcrossPersistedStates(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: t.TempDir() + "/fifo.sqlite"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	insert := func(status string, next, finished, abandoned any) {
		jobID := 0
		if scanErr := store.db.QueryRow(`SELECT COALESCE(MAX(job_id),0)+1 FROM silence_recheck_jobs`).Scan(&jobID); scanErr != nil {
			t.Fatal(scanErr)
		}
		started := any(now.Format(time.RFC3339Nano))
		if status == "queued" {
			started = nil
		}
		if _, insertErr := store.db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,total_candidates,processed_candidates,created_at,started_at,next_attempt_at,finished_at,abandoned_at,abandoned_by,abandon_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, "fifo-context", fmt.Sprintf("fifo-job-%d", jobID), "{}", "source_context_changed", status, 0, 0, now.Format(time.RFC3339Nano), started, next, finished, abandoned, nil, nil); insertErr != nil {
			t.Fatal(insertErr)
		}
	}
	insert("completed", nil, now.Format(time.RFC3339Nano), nil)
	insert("abandoned", nil, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	insert("queued", nil, nil, nil)
	future := now.Add(time.Hour).Format(time.RFC3339Nano)
	insert("retry_wait", future, nil, nil)
	insert("failed", nil, now.Format(time.RFC3339Nano), nil)
	head, err := store.RecheckQueueHead(context.Background())
	if err != nil || head.JobID != 3 || head.Status != "queued" {
		t.Fatalf("initial FIFO head=%+v err=%v", head, err)
	}
	if _, runErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000); runErr != nil {
		t.Fatal(runErr)
	}
	head, err = store.RecheckQueueHead(context.Background())
	if err != nil || head.JobID != 4 || head.Status != "retry_wait" {
		t.Fatalf("not-due retry head=%+v err=%v", head, err)
	}
	if _, runErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.As(runErr, new(*recheckQueueBlockedError)) {
		t.Fatalf("not-due retry unexpectedly ran: %v", runErr)
	}
	if _, execErr := store.db.Exec(`UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=4`, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)); execErr != nil {
		t.Fatal(execErr)
	}
	if _, runErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000); runErr != nil {
		t.Fatal(runErr)
	}
	actor := "fifo-reviewer"
	if _, abandonErr := store.AbandonRecheckJobContext(context.Background(), 5, "fifo-abandon", "release", &actor, now); abandonErr != nil {
		t.Fatal(abandonErr)
	}
	head, err = store.RecheckQueueHead(context.Background())
	if err != nil || head.JobID != 0 {
		t.Fatalf("FIFO did not release terminal states: head=%+v err=%v", head, err)
	}
}

func TestProblem3ReviewProblem3SchemaConstraintMatrix(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	validStarted := now.Format(time.RFC3339Nano)
	validFinished := now.Add(time.Second).Format(time.RFC3339Nano)
	tests := []struct {
		name string
		stmt string
		args []any
	}{
		{name: "missing finished timestamp", stmt: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,outcome,failure_class,failure_stage) VALUES(?,?,?,?,?,?,?)`, args: []any{1, "schema-missing-finished", 1, validStarted, "failed", "terminal", recheckStageEvaluate}},
		{name: "non-positive attempt number", stmt: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome) VALUES(?,?,?,?,?,?)`, args: []any{1, "schema-negative-number", 0, validStarted, validFinished, "committed"}},
		{name: "committed failure fields", stmt: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome,failure_class,failure_stage) VALUES(?,?,?,?,?,?,?,?)`, args: []any{1, "schema-committed-failure", 1, validStarted, validFinished, "committed", "terminal", recheckStageEvaluate}},
		{name: "retry missing schedule", stmt: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome,failure_class,failure_stage) VALUES(?,?,?,?,?,?,?,?)`, args: []any{1, "schema-retry-no-schedule", 1, validStarted, validFinished, "retry_scheduled", "retryable", recheckStageCommit}},
		{name: "failed missing failure class", stmt: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome) VALUES(?,?,?,?,?,?)`, args: []any{1, "schema-failed-no-class", 1, validStarted, validFinished, "failed"}},
		{name: "operator missing applied fields", stmt: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,requested_at) VALUES(?,?,?,?,?)`, args: []any{1, "schema-missing-applied", "retry", "fp-missing", validStarted}},
		{name: "operator invalid resulting status", stmt: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,requested_at,applied_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?,?)`, args: []any{1, "schema-invalid-status", "retry", "fp-status", validStarted, validFinished, "applied", "failed"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var beforeAttempts, beforeActions int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts`).Scan(&beforeAttempts); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions`).Scan(&beforeActions); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(test.stmt, test.args...); err == nil {
				t.Fatal("invalid Problem 3 row was accepted")
			}
			var afterAttempts, afterActions int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts`).Scan(&afterAttempts); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions`).Scan(&afterActions); err != nil {
				t.Fatal(err)
			}
			if beforeAttempts != afterAttempts || beforeActions != afterActions {
				t.Fatalf("invalid row changed state: attempts=%d/%d actions=%d/%d", beforeAttempts, afterAttempts, beforeActions, afterActions)
			}
		})
	}
	validAttempt := `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome) VALUES(?,?,?,?,?,?)`
	if _, err := store.db.Exec(validAttempt, 1, "schema-duplicate-a", 1, validStarted, validFinished, "committed"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(validAttempt, 1, "schema-duplicate-b", 1, validStarted, validFinished, "committed"); err == nil {
		t.Fatal("duplicate job attempt number was accepted")
	}
	validOperator := `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,requested_at,applied_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?,?)`
	if _, err := store.db.Exec(validOperator, 1, "schema-duplicate-request", "retry", "fp-duplicate", validStarted, validFinished, "applied", "queued"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(validOperator, 1, "schema-duplicate-request", "retry", "fp-duplicate", validStarted, validFinished, "applied", "queued"); err == nil {
		t.Fatal("duplicate operator request identity was accepted")
	}
}

func TestProblem3ReviewRetryStateSurvivesStoreRestart(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	path := store.sqlitePath()
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return problem3SQLiteError(5)
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("retryable failure unexpectedly succeeded")
	}
	var beforeToken, beforeOutcome string
	if err := store.db.QueryRow(`SELECT attempt_token,outcome FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&beforeToken, &beforeOutcome); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	var status, afterToken, afterOutcome string
	if err := reopened.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := reopened.db.QueryRow(`SELECT attempt_token,outcome FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&afterToken, &afterOutcome); err != nil {
		t.Fatal(err)
	}
	if status != "retry_wait" || beforeToken != afterToken || beforeOutcome != afterOutcome {
		t.Fatalf("restart changed retry identity: status=%s token=%s/%s outcome=%s/%s", status, beforeToken, afterToken, beforeOutcome, afterOutcome)
	}
	if _, err := reopened.db.Exec(`UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=1`, now.Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	reopened.lifecycleTestHooks = nil
	if _, err := reopened.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	if err := reopened.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("restart did not resume retry job: %s", status)
	}
}

func TestProblem3ReviewFailedHeadSurvivesRestart(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	path := store.sqlitePath()
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("terminal failure")
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("terminal failure unexpectedly succeeded")
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopened, openErr := OpenStore(StoreOptions{Path: path})
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	head, err := reopened.RecheckQueueHead(context.Background())
	if err != nil || head.Status != "failed" || head.JobID != 1 {
		t.Fatalf("failed head after restart=%+v err=%v", head, err)
	}
	actor := "restart-reviewer"
	if _, actionErr := reopened.AbandonRecheckJobContext(context.Background(), 1, "restart-abandon", "release", &actor, now); actionErr != nil {
		t.Fatal(actionErr)
	}
	head, err = reopened.RecheckQueueHead(context.Background())
	if err != nil || head.JobID != 0 {
		t.Fatalf("failed head was not released after restart: %+v err=%v", head, err)
	}
}

func TestProblem3ReviewCancellationBeforeAndAfterSelectionLeavesNoAttempt(t *testing.T) {
	for _, afterSelection := range []bool{false, true} {
		t.Run(fmt.Sprintf("after_selection_%v", afterSelection), func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if afterSelection {
				store.writerTransactionFinished = func(kind string) {
					if kind == writerOperationRecheckBatch {
						cancel()
					}
				}
			} else {
				cancel()
			}
			if _, err := store.RunSilenceRecheckBatchContext(ctx, 1000); err == nil {
				t.Fatal("canceled recheck unexpectedly succeeded")
			}
			var status string
			var attempts, attemptCount, processed int
			if err := store.db.QueryRow(`SELECT status,attempt_count,processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attemptCount, &processed); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			if status != "queued" || attempts != 0 || attemptCount != 0 || processed != 0 {
				t.Fatalf("cancellation mutated job: status=%s attempts=%d count=%d processed=%d", status, attempts, attemptCount, processed)
			}
		})
	}
}

func TestProblem3ReviewCommittedPartialBatchResetsRetryBudget(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,target_key,match_labels_json,relevant_sources_json) VALUES(?,?,?,?,?)`, 1, "z-review-alert", "", `{}`, `[]`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=2 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	var status string
	var processed, consecutive int
	if err := store.db.QueryRow(`SELECT status,processed_candidates,consecutive_retry_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &processed, &consecutive); err != nil {
		t.Fatal(err)
	}
	if status != "running" || processed != 1 || consecutive != 0 {
		t.Fatalf("partial committed batch state: status=%s processed=%d consecutive=%d", status, processed, consecutive)
	}
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return problem3SQLiteError(5)
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1); err == nil {
		t.Fatal("second batch retryable failure unexpectedly succeeded")
	}
	store.lifecycleTestHooks = nil
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=1`, now.Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT status,consecutive_retry_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &consecutive); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || consecutive != 0 {
		t.Fatalf("committed batch did not reset budget: status=%s consecutive=%d", status, consecutive)
	}
	var outcomes string
	if err := store.db.QueryRow(`SELECT GROUP_CONCAT(outcome,',') FROM silence_recheck_job_attempts WHERE job_id=1 ORDER BY attempt_number`).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if outcomes != "committed,retry_scheduled,committed" {
		t.Fatalf("attempt outcomes=%s", outcomes)
	}
}
