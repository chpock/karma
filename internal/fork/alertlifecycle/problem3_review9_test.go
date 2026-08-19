package alertlifecycle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

func TestProblem3Review9RecoveryLinksRetainedAttemptAndCandidateLoop(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	candidateCalls := 0
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			candidateCalls++
			return problem3SQLiteError(5)
		}
		return nil
	}}
	starter := &review6SequencedTransactionStarter{
		inner:  store.writerTransactionStarter,
		failAt: 2,
		err:    driver.ErrBadConn,
	}
	store.writerTransactionStarter = starter
	store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) {
		return nil, errors.Join(driver.ErrBadConn, errors.New("review9 replacement unavailable"))
	}

	_, initialErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if initialErr == nil || !IsRetryableRecheckFailurePersistence(initialErr) {
		t.Fatalf("initial pending failure=%v", initialErr)
	}
	pendingBefore := store.pendingRecheckFailureSnapshot()
	if pendingBefore == nil {
		t.Fatal("initial failure did not retain pending attempt")
	}
	beforeState := captureProblem3Review2State(t, store.db)

	_, readinessErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if readinessErr == nil || !IsRetryableRecheckWriterAcquisition(readinessErr) {
		t.Fatalf("failed readiness=%v", readinessErr)
	}
	if afterState := captureProblem3Review2State(t, store.db); afterState != beforeState {
		t.Fatal("failed readiness changed durable state")
	}
	if pendingAfter := store.pendingRecheckFailureSnapshot(); pendingAfter == nil || !review8SameAttempt(pendingAfter.Attempt, pendingBefore.Attempt) {
		t.Fatalf("failed readiness changed pending attempt: before=%+v after=%+v", pendingBefore, store.pendingRecheckFailureSnapshot())
	}

	store.writerConnectionAcquirer = store.db.Conn
	_, recoveryErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	var retryScheduled *recheckRetryScheduledError
	if !errors.As(recoveryErr, &retryScheduled) {
		t.Fatalf("recovery result=%v", recoveryErr)
	}
	if store.pendingRecheckFailureSnapshot() != nil {
		t.Fatal("pending failure remained after recovery")
	}

	var attemptRows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attemptRows); err != nil {
		t.Fatal(err)
	}
	if attemptRows != 1 {
		t.Fatalf("attempt rows=%d want=1", attemptRows)
	}
	var attemptToken, outcome string
	var attemptNumber int
	if err := store.db.QueryRow(`SELECT attempt_token, outcome, attempt_number FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attemptToken, &outcome, &attemptNumber); err != nil {
		t.Fatal(err)
	}
	if attemptToken != pendingBefore.Attempt.Token || outcome != "retry_scheduled" || attemptNumber != pendingBefore.Attempt.Number {
		t.Fatalf("persisted attempt=(token=%q outcome=%q number=%d), retained=(token=%q number=%d)", attemptToken, outcome, attemptNumber, pendingBefore.Attempt.Token, pendingBefore.Attempt.Number)
	}
	var parentAttemptCount int
	if err := store.db.QueryRow(`SELECT attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&parentAttemptCount); err != nil {
		t.Fatal(err)
	}
	if parentAttemptCount != 1 {
		t.Fatalf("parent attempt_count=%d want=1", parentAttemptCount)
	}
	if candidateCalls != 1 {
		t.Fatalf("persist_candidate loop entries=%d want=1", candidateCalls)
	}
	if starter.calls != 2 {
		t.Fatalf("failed readiness or recovery entered the original starter unexpectedly: calls=%d", starter.calls)
	}
}
