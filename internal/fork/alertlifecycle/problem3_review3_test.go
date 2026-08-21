package alertlifecycle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

func seedProblem3Review3PendingCommittedAttempt(t *testing.T) *Store {
	t.Helper()
	store, _, _ := seedProblem2OpenJob(t)
	commitCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if err := tx.Commit(); err != nil {
			return err
		}
		return review2SQLiteError{code: 5, message: "review3 ambiguous committed batch"}
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		return false, context.DeadlineExceeded
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("initial ambiguous batch=%v", err)
	}
	store.writerCommit = nil
	if commitCalls != 1 || store.pendingWriterCommit == nil || store.pendingRecheckFailureSnapshot() == nil {
		t.Fatalf("pending setup commits=%d writer=%+v attempt=%+v", commitCalls, store.pendingWriterCommit, store.pendingRecheckFailureSnapshot())
	}
	var attempts int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("pending setup attempts=%d want=1", attempts)
	}
	return store
}

func review3DriverConnectionIdentity(t *testing.T, conn *sql.Conn) any {
	t.Helper()
	var identity any
	if err := conn.Raw(func(raw any) error {
		identity = raw
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return identity
}

func TestProblem3Review3InvalidGenericLookupConnectionIsReplaced(t *testing.T) {
	for _, test := range []struct {
		name            string
		requirePhysical bool
		invalidate      func(*testing.T, *Store, *sql.Conn)
	}{
		{name: "sql conn done", invalidate: func(t *testing.T, store *Store, conn *sql.Conn) {
			t.Helper()
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return false, sql.ErrConnDone }
		}},
		{name: "driver bad conn", requirePhysical: true, invalidate: func(t *testing.T, store *Store, conn *sql.Conn) {
			t.Helper()
			store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
				return false, driver.ErrBadConn
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := seedProblem3Review3PendingCommittedAttempt(t)
			t.Cleanup(func() { _ = store.Close() })
			oldConn := store.writerConn
			oldIdentity := review3DriverConnectionIdentity(t, oldConn)
			test.invalidate(t, store, oldConn)

			if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
				t.Fatalf("invalidated lookup=%v", err)
			}
			if store.writerConn == nil || store.writerConn == oldConn {
				t.Fatal("invalidated reconciliation connection was retained")
			}
			if newIdentity := review3DriverConnectionIdentity(t, store.writerConn); test.requirePhysical && newIdentity == oldIdentity {
				t.Fatalf("physical writer identity was reused: %T", newIdentity)
			}
			if store.pendingWriterCommit == nil || store.pendingRecheckFailureSnapshot() == nil {
				t.Fatal("connection replacement cleared pending reconciliation state")
			}

			store.writerCommitLookup = nil
			result, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
			if err != nil || result.Disposition != RecheckBatchCommitted {
				t.Fatalf("replacement reconciliation result=%+v err=%v", result, err)
			}
			if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
				t.Fatalf("replacement completion=%v", err)
			}
			var attempts int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			if attempts != 1 || store.pendingRecheckFailureSnapshot() != nil {
				t.Fatalf("replacement attempts=%d pending=%+v", attempts, store.pendingRecheckFailureSnapshot())
			}
		})
	}
}

func TestProblem3Review3InvalidAttemptTupleConnectionIsReplaced(t *testing.T) {
	for _, test := range []struct {
		name            string
		requirePhysical bool
		invalidate      func(*Store, *sql.Conn)
	}{
		{name: "sql conn done", invalidate: func(store *Store, conn *sql.Conn) {
			store.recheckAttemptLookup = func(context.Context, string) (int64, int, string, error) {
				_ = conn.Close()
				return 0, 0, "", sql.ErrConnDone
			}
		}},
		{name: "driver bad conn", requirePhysical: true, invalidate: func(store *Store, conn *sql.Conn) {
			store.recheckAttemptLookup = func(context.Context, string) (int64, int, string, error) {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
				return 0, 0, "", driver.ErrBadConn
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := seedProblem3Review3PendingCommittedAttempt(t)
			t.Cleanup(func() { _ = store.Close() })
			oldConn := store.writerConn
			oldIdentity := review3DriverConnectionIdentity(t, oldConn)
			test.invalidate(store, oldConn)
			store.writerCommitLookup = nil

			if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
				t.Fatalf("invalid attempt lookup=%v", err)
			}
			if store.writerConn == nil || store.writerConn == oldConn {
				t.Fatal("invalidated attempt lookup connection was retained")
			}
			if newIdentity := review3DriverConnectionIdentity(t, store.writerConn); test.requirePhysical && newIdentity == oldIdentity {
				t.Fatalf("attempt lookup physical identity was reused: %T", newIdentity)
			}
			store.recheckAttemptLookup = nil
			result, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
			if err != nil || result.Disposition != RecheckBatchCommitted {
				t.Fatalf("attempt replacement reconciliation result=%+v err=%v", result, err)
			}
		})
	}
}

func TestProblem3Review3ReplacementFailurePreservesPendingReconciliation(t *testing.T) {
	store := seedProblem3Review3PendingCommittedAttempt(t)
	t.Cleanup(func() { _ = store.Close() })
	oldPending := store.pendingWriterCommit
	if err := store.writerConn.Close(); err != nil {
		t.Fatal(err)
	}
	replacementErr := errors.New("review3 replacement unavailable")
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return false, sql.ErrConnDone }
	store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) { return nil, replacementErr }
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) || !errors.Is(err, ErrWriterUnavailable) || !errors.Is(err, replacementErr) {
		t.Fatalf("replacement failure=%v", err)
	}
	if store.pendingWriterCommit != oldPending || store.pendingRecheckFailureSnapshot() == nil {
		t.Fatal("replacement failure cleared pending state")
	}
	store.writerConnectionAcquirer = store.db.Conn
	store.writerCommitLookup = nil
	result, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if err != nil || result.Disposition != RecheckBatchCommitted {
		t.Fatalf("replacement retry result=%+v err=%v", result, err)
	}
}

func TestProblem3Review3GenericAbsentAttemptPresentIsInvariant(t *testing.T) {
	store := seedProblem3Review3PendingCommittedAttempt(t)
	t.Cleanup(func() { _ = store.Close() })
	before := captureProblem3Review2State(t, store.db)
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return false, nil }
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterOperationInvariant) {
		t.Fatalf("generic absent plus attempt present=%v", err)
	}
	if after := captureProblem3Review2State(t, store.db); after != before {
		t.Fatal("generic-ledger disagreement changed durable state")
	}
	if store.writerRecoveryState != writerRecoveryInvariant || store.pendingWriterCommit == nil {
		t.Fatalf("dual-ledger disagreement state=%v pending=%+v", store.writerRecoveryState, store.pendingWriterCommit)
	}
}

func TestProblem3Review3RepresentativeTupleMismatchPreservesCompleteState(t *testing.T) {
	store := seedProblem3Review3PendingCommittedAttempt(t)
	t.Cleanup(func() { _ = store.Close() })
	before := captureProblem3Review2State(t, store.db)
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return true, nil }
	store.recheckAttemptLookup = func(context.Context, string) (int64, int, string, error) {
		return 999, 77, "committed", nil
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterOperationInvariant) {
		t.Fatalf("mismatched attempt tuple=%v", err)
	}
	if after := captureProblem3Review2State(t, store.db); after != before {
		t.Fatal("mismatched attempt tuple changed durable state")
	}
}

func TestProblem3Review4DefinitiveReconciliationThenFailurePersistenceIsTyped(t *testing.T) {
	store := seedProblem3Review3PendingCommittedAttempt(t)
	t.Cleanup(func() { _ = store.Close() })
	pending := store.pendingRecheckFailureSnapshot()
	if pending == nil {
		t.Fatal("pending failure setup is missing")
	}
	if _, err := store.db.Exec(`DELETE FROM silence_recheck_job_attempts WHERE attempt_token=?`, pending.Attempt.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='running',attempt_count=0,finished_at=NULL WHERE job_id=?`, pending.Attempt.JobID); err != nil {
		t.Fatal(err)
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return false, nil }
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistFailure {
			return errors.New("review4 failure bookkeeping unavailable")
		}
		return nil
	}}
	_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if !errors.Is(err, ErrRecheckFailurePersistence) || !RecheckFailurePersistenceReconciliationResolved(err) {
		t.Fatalf("typed resolved bookkeeping error=%v", err)
	}
}
