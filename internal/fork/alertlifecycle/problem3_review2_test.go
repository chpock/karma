package alertlifecycle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	moderncsqlite "modernc.org/sqlite"
)

type review2SQLiteError struct {
	code    int
	message string
}

func (e review2SQLiteError) Error() string   { return e.message }
func (e review2SQLiteError) SQLiteCode() int { return e.code }

func TestProblem3Review2InternalReconciliationDeadlinePreservesAttempt(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.writerCommit = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return errors.New("ambiguous commit")
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		return false, context.DeadlineExceeded
	}

	ctx := context.Background()
	if _, err := store.RunSilenceRecheckBatchContext(ctx, 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("commit outcome=%v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("live worker context was canceled: %v", ctx.Err())
	}
	pending := store.pendingRecheckFailureSnapshot()
	if pending == nil || pending.Attempt.JobID != 1 || pending.Attempt.Token == "" || pending.Attempt.Number != 1 {
		t.Fatalf("selected attempt was lost after internal reconciliation timeout: %+v", pending)
	}
}

func TestProblem3Review2InternalDeadlineResolvesPresentAndAbsent(t *testing.T) {
	for _, present := range []bool{true, false} {
		t.Run(map[bool]string{true: "present", false: "absent"}[present], func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			commitCalls := 0
			store.writerCommit = func(tx *sql.Tx) error {
				commitCalls++
				if commitCalls == 1 {
					if present {
						if err := tx.Commit(); err != nil {
							return err
						}
					}
					return problem3SQLiteError(5)
				}
				return tx.Commit()
			}
			lookupCalls := 0
			store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
				lookupCalls++
				if lookupCalls == 1 {
					return false, context.DeadlineExceeded
				}
				return present, nil
			}

			beforeDomain := captureProblem3Review2DomainState(t, store.db)
			if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
				t.Fatalf("initial outcome=%v", err)
			}
			afterUnknownDomain := captureProblem3Review2DomainState(t, store.db)
			if !present && afterUnknownDomain != beforeDomain {
				t.Fatal("token-absent unknown branch committed domain effects")
			}
			pending := store.pendingRecheckFailureSnapshot()
			if pending == nil {
				t.Fatal("pending attempt was not retained")
			}
			token := pending.Attempt.Token
			secondResult, secondErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
			if present {
				if secondErr != nil || secondResult.Disposition != RecheckBatchCommitted || secondResult.Status != "completed" {
					t.Fatalf("present reconciliation result=%+v err=%v", secondResult, secondErr)
				}
				if _, thirdErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000); thirdErr != nil {
					t.Fatal(thirdErr)
				}
			} else if !errors.As(secondErr, new(*recheckRetryScheduledError)) {
				t.Fatalf("absent bookkeeping=%v", secondErr)
			}
			var status, storedToken, outcome string
			if err := store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRow(`SELECT attempt_token,outcome FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&storedToken, &outcome); err != nil {
				t.Fatal(err)
			}
			wantStatus, wantOutcome := "retry_wait", "retry_scheduled"
			if present {
				wantStatus, wantOutcome = "completed", "committed"
			}
			if status != wantStatus || outcome != wantOutcome || storedToken != token || store.pendingRecheckFailureSnapshot() != nil {
				t.Fatalf("resolved state: status=%s/%s outcome=%s/%s token=%s/%s pending=%+v", status, wantStatus, outcome, wantOutcome, storedToken, token, store.pendingRecheckFailureSnapshot())
			}
			if !present {
				var message, class, stage string
				if err := store.db.QueryRow(`SELECT last_error_message,last_error_class,last_error_stage FROM silence_recheck_jobs WHERE job_id=1`).Scan(&message, &class, &stage); err != nil {
					t.Fatal(err)
				}
				if message != "synthetic sqlite error" || class != "retryable" || stage != recheckStageCommit {
					t.Fatalf("absent original cause message=%q class=%s stage=%s", message, class, stage)
				}
			}
			finalDomain := captureProblem3Review2DomainState(t, store.db)
			if present && finalDomain != afterUnknownDomain {
				t.Fatal("token-present reconciliation duplicated domain effects")
			}
			if !present && finalDomain != beforeDomain {
				t.Fatal("failure bookkeeping changed candidate or suppression domain state")
			}
		})
	}
}

func TestProblem3Review2CrossOperationReconciliationRunsOriginalWake(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "review2-alert", State: StateInactive, RawState: StateActive, IsLive: false,
		Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
		RelevantUpstreams: ` ["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	wakeCalls := 0
	store.SetRecheckCommitWake(func() { wakeCalls++ })
	commitCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if commitErr := tx.Commit(); commitErr != nil {
			return commitErr
		}
		if commitCalls == 1 {
			return errors.New("ambiguous poll commit")
		}
		return nil
	}
	lookupCalls := 0
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, errors.New("lookup unavailable")
		}
		return true, nil
	}
	silence := SourceSilenceSnapshot{
		SilenceID: "review2-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}
	_, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}},
		Timestamp:           now,
	})
	if !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("initial poll outcome=%v", err)
	}
	if wakeCalls != 0 {
		t.Fatalf("wake ran before commit reconciliation: %d", wakeCalls)
	}
	_, err = withWriterTxValue(context.Background(), store, writerOperationHousekeeping, func(*sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	})
	if !errors.Is(err, ErrWriterCommitReconciled) {
		t.Fatalf("cross-operation reconciliation outcome=%v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("original poll wake calls=%d want=1", wakeCalls)
	}
}

func TestProblem3Review2ImmediatePollHasOneWakeOwner(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "review2-immediate", State: StateInactive, RawState: StateActive, IsLive: false, Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	wakes := 0
	store.SetRecheckCommitWake(func() { wakes++ })
	silence := SourceSilenceSnapshot{SilenceID: "review2-immediate-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}}}
	result, err := store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}}, Timestamp: now})
	if err != nil || result.RecheckJobsQueued != 1 || wakes != 1 {
		t.Fatalf("immediate poll result=%+v err=%v wakes=%d", result, err, wakes)
	}
}

func TestProblem3Review2CrossOperationOperatorReconciliationWakesOnce(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	failProblem3Review2Job(t, store)
	wakes := 0
	store.SetRecheckCommitWake(func() { wakes++ })
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
			return false, errors.New("operator lookup unavailable")
		}
		return true, nil
	}
	actor := "review2-cross-operation"
	if _, err := store.RetryRecheckJobContext(context.Background(), 1, "review2-cross-operation", "retry", &actor, now); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("initial operator outcome=%v", err)
	}
	if wakes != 0 {
		t.Fatalf("operator woke before reconciliation: %d", wakes)
	}
	if _, err := withWriterTxValue(context.Background(), store, writerOperationHousekeeping, func(*sql.Tx) (struct{}, error) { return struct{}{}, nil }); !errors.Is(err, ErrWriterCommitReconciled) {
		t.Fatalf("cross-operation reconciliation=%v", err)
	}
	if wakes != 1 {
		t.Fatalf("cross-operation operator wakes=%d", wakes)
	}
	replay, err := store.RetryRecheckJobContext(context.Background(), 1, "review2-cross-operation", "retry", &actor, now.Add(time.Second))
	if err != nil || !replay.Applied || replay.CurrentStatus != "queued" || wakes != 1 {
		t.Fatalf("operator replay=%+v err=%v wakes=%d", replay, err, wakes)
	}
}

func TestProblem3Review2RecoveredBadConnectionIsRetryableAcquisition(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.writerTransactionStarter = reviewFailingTransactionStarter{err: driver.ErrBadConn}

	_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if !IsRetryableRecheckWriterAcquisition(err) {
		t.Fatalf("recovered bad connection is not retryable: %v", err)
	}
	var attemptCount, attempts, processed int
	if queryErr := store.db.QueryRow(`SELECT attempt_count,processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&attemptCount, &processed); queryErr != nil {
		t.Fatal(queryErr)
	}
	if queryErr := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); queryErr != nil {
		t.Fatal(queryErr)
	}
	if attemptCount != 0 || attempts != 0 || processed != 0 {
		t.Fatalf("bad-connection acquisition mutated durable state: count=%d attempts=%d processed=%d", attemptCount, attempts, processed)
	}
}

func TestProblem3Review2ReplacementFailureIsWriterUnavailable(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.writerTransactionStarter = reviewFailingTransactionStarter{err: sql.ErrConnDone}
	store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) {
		return nil, errors.New("replacement unavailable")
	}

	_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if !errors.Is(err, ErrWriterUnavailable) || !IsRetryableRecheckWriterAcquisition(err) {
		t.Fatalf("replacement failure classification=%v", err)
	}
}

func TestProblem3Review2AttemptTupleTransientLookupRetries(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.writerCommit = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return errors.New("ambiguous commit")
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return true, nil }
	lookupCalls := 0
	store.recheckAttemptLookup = func(context.Context, string) (int64, int, string, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return 0, 0, "", context.DeadlineExceeded
		}
		return 1, 1, "committed", nil
	}

	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("initial tuple outcome=%v", err)
	}
	if store.writerRecoveryState != writerRecoveryCommitUnknown || store.pendingWriterCommit == nil || store.pendingWriterCommit.finalizer.reconcileDone {
		t.Fatalf("transient tuple lookup was finalized: state=%d pending=%+v", store.writerRecoveryState, store.pendingWriterCommit)
	}
	result, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if err != nil || result.Disposition != RecheckBatchCommitted || result.Status != "completed" {
		t.Fatalf("tuple retry result=%+v err=%v", result, err)
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	var attempts, attemptCount, processed int
	if err := store.db.QueryRow(`SELECT attempt_count,processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&attemptCount, &processed); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if lookupCalls != 2 || attempts != 1 || attemptCount != 1 || processed != 1 || store.pendingRecheckFailureSnapshot() != nil {
		t.Fatalf("tuple retry state: lookups=%d attempts=%d count=%d processed=%d pending=%+v", lookupCalls, attempts, attemptCount, processed, store.pendingRecheckFailureSnapshot())
	}
}

func TestProblem3Review2AttemptTupleTransientClassesRetryAtTransactionBoundary(t *testing.T) {
	for name, transientErr := range map[string]error{
		"deadline":          context.DeadlineExceeded,
		"busy":              problem3SQLiteError(5),
		"locked":            problem3SQLiteError(6),
		"busy recovery":     problem3SQLiteError(261),
		"busy snapshot":     problem3SQLiteError(517),
		"busy timeout":      problem3SQLiteError(773),
		"bad connection":    driver.ErrBadConn,
		"closed connection": sql.ErrConnDone,
	} {
		t.Run(name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			store.writerCommit = func(tx *sql.Tx) error {
				if err := tx.Commit(); err != nil {
					return err
				}
				return errors.New("ambiguous commit")
			}
			store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return true, nil }
			calls := 0
			store.recheckAttemptLookup = func(context.Context, string) (int64, int, string, error) {
				calls++
				if calls == 1 {
					return 0, 0, "", transientErr
				}
				return 1, 1, "committed", nil
			}
			if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) || errors.Is(err, ErrWriterOperationInvariant) {
				t.Fatalf("transient tuple error=%v", err)
			}
			result, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
			if err != nil || result.Disposition != RecheckBatchCommitted || result.Status != "completed" {
				t.Fatalf("tuple retry result=%+v err=%v", result, err)
			}
			if calls != 2 || store.writerRecoveryState != writerRecoveryHealthy {
				t.Fatalf("tuple calls=%d writer_state=%d", calls, store.writerRecoveryState)
			}
		})
	}
}

func TestProblem3Review2AttemptTupleDefinitiveFailuresPoisonWriter(t *testing.T) {
	for _, test := range []struct {
		name   string
		lookup func(context.Context, string) (int64, int, string, error)
	}{
		{name: "durable absence", lookup: func(context.Context, string) (int64, int, string, error) { return 0, 0, "", sql.ErrNoRows }},
		{name: "wrong job", lookup: func(context.Context, string) (int64, int, string, error) { return 2, 1, "committed", nil }},
		{name: "wrong number", lookup: func(context.Context, string) (int64, int, string, error) { return 1, 2, "committed", nil }},
		{name: "wrong outcome", lookup: func(context.Context, string) (int64, int, string, error) { return 1, 1, "failed", nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			store.writerCommit = func(tx *sql.Tx) error {
				if err := tx.Commit(); err != nil {
					return err
				}
				return errors.New("ambiguous commit")
			}
			store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return true, nil }
			store.recheckAttemptLookup = test.lookup

			if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterOperationInvariant) {
				t.Fatalf("tuple failure=%v", err)
			}
			if store.writerRecoveryState != writerRecoveryInvariant {
				t.Fatalf("writer state=%d want invariant", store.writerRecoveryState)
			}
		})
	}
}

func TestProblem3Review2FailureBookkeepingTupleTransientRetriesWithoutSecondAttempt(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("review2 terminal candidate failure")
		}
		return nil
	}}
	commitCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if err := tx.Commit(); err != nil {
			return err
		}
		if commitCalls == 1 {
			return errors.New("ambiguous bookkeeping commit")
		}
		return nil
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return true, nil }
	tupleCalls := 0
	store.recheckAttemptLookup = func(context.Context, string) (int64, int, string, error) {
		tupleCalls++
		if tupleCalls == 1 {
			return 0, 0, "", context.DeadlineExceeded
		}
		return 1, 1, "failed", nil
	}
	beforeDomain := captureProblem3Review2DomainState(t, store.db)
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) || !errors.Is(err, ErrRecheckFailurePersistence) {
		t.Fatalf("initial bookkeeping outcome=%v", err)
	}
	if pending := store.pendingRecheckFailureSnapshot(); pending == nil || store.pendingWriterCommit == nil || store.pendingWriterCommit.finalizer.reconcileDone {
		t.Fatalf("transient bookkeeping state pendingAttempt=%+v pendingWriter=%+v", pending, store.pendingWriterCommit)
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !IsRecheckDurableFailure(err) {
		t.Fatalf("durable failed-head result=%v", err)
	}
	var attempts, attemptCount int
	var status, outcome string
	if err := store.db.QueryRow(`SELECT status,attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attemptCount); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*),MAX(outcome) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts, &outcome); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || attempts != 1 || attemptCount != 1 || outcome != "failed" || tupleCalls != 2 || store.pendingRecheckFailureSnapshot() != nil {
		t.Fatalf("bookkeeping retry state=%s attempts=%d count=%d outcome=%s tupleCalls=%d pending=%+v", status, attempts, attemptCount, outcome, tupleCalls, store.pendingRecheckFailureSnapshot())
	}
	if afterDomain := captureProblem3Review2DomainState(t, store.db); afterDomain != beforeDomain {
		t.Fatal("failure bookkeeping tuple retry changed candidate or suppression domain state")
	}
}

func TestProblem3Review2CommittedRetryBookkeepingRestoresSchedule(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	for attemptNumber := 1; attemptNumber <= 4; attemptNumber++ {
		started := now.Add(time.Duration(attemptNumber) * time.Second)
		finished := started.Add(time.Millisecond)
		scheduled := finished.Add(time.Second)
		if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome,failure_class,failure_stage,scheduled_retry_at) VALUES(?,?,?,?,?,?,?,?,?)`, 1, fmt.Sprintf("review2-prior-%d", attemptNumber), attemptNumber, started.Format(time.RFC3339Nano), finished.Format(time.RFC3339Nano), "retry_scheduled", "retryable", recheckStagePersistCandidate, scheduled.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET attempt_count=4,retry_count=4,consecutive_retry_count=4 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return problem3SQLiteError(5)
		}
		return nil
	}}
	commitCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if err := tx.Commit(); err != nil {
			return err
		}
		if commitCalls == 1 {
			return errors.New("ambiguous retry bookkeeping commit")
		}
		return nil
	}
	lookupCalls := 0
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, errors.New("retry bookkeeping lookup unavailable")
		}
		return true, nil
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("initial retry bookkeeping=%v", err)
	}
	_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	var scheduled *recheckRetryScheduledError
	if !errors.As(err, &scheduled) {
		t.Fatalf("restored retry schedule=%v", err)
	}
	var persisted string
	if queryErr := store.db.QueryRow(`SELECT scheduled_retry_at FROM silence_recheck_job_attempts WHERE job_id=1 AND attempt_number=5`).Scan(&persisted); queryErr != nil {
		t.Fatal(queryErr)
	}
	persistedTime, err := time.Parse(time.RFC3339Nano, persisted)
	if err != nil {
		t.Fatal(err)
	}
	if !scheduled.NextAttemptAt.Equal(persistedTime) {
		t.Fatalf("restored schedule=%s persisted=%s", scheduled.NextAttemptAt, persistedTime)
	}
}

func TestProblem3Review2PreAttemptFailuresAreTypedAndNonMutating(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*Store)
		retryable bool
		reason    string
		tokenErr  bool
	}{
		{
			name: "selection busy", retryable: true, reason: "select_job",
			configure: func(store *Store) {
				store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
					if stage == recheckStageSelectJob {
						return problem3SQLiteError(5)
					}
					return nil
				}}
			},
		},
		{
			name: "invalid due time", reason: "invalid_next_attempt_at",
			configure: func(store *Store) {
				if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='retry_wait',started_at=?,next_attempt_at='not-a-time' WHERE job_id=1`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "writer token entropy", tokenErr: true,
			configure: func(store *Store) {
				store.writerOperationToken = func() (string, error) { return "", errors.New("entropy unavailable") }
			},
		},
		{
			name: "attempt token entropy", reason: "create_attempt",
			configure: func(store *Store) {
				calls := 0
				store.writerOperationToken = func() (string, error) {
					calls++
					if calls == 1 {
						return "review2-writer-token", nil
					}
					return "", errors.New("attempt entropy unavailable")
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			test.configure(store)
			_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
			if test.tokenErr {
				if !errors.Is(err, ErrWriterOperationToken) {
					t.Fatalf("writer token error=%v", err)
				}
			} else if !IsRecheckPreAttemptError(err) || IsRetryableRecheckPreAttempt(err) != test.retryable || RecheckPreAttemptReason(err) != test.reason {
				t.Fatalf("pre-attempt classification: err=%v retryable=%v reason=%q", err, IsRetryableRecheckPreAttempt(err), RecheckPreAttemptReason(err))
			}
			var attempts, attemptCount, processed int
			if queryErr := store.db.QueryRow(`SELECT attempt_count,processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&attemptCount, &processed); queryErr != nil {
				t.Fatal(queryErr)
			}
			if queryErr := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); queryErr != nil {
				t.Fatal(queryErr)
			}
			if attempts != 0 || attemptCount != 0 || processed != 0 {
				t.Fatalf("pre-attempt failure mutated state: attempts=%d count=%d processed=%d", attempts, attemptCount, processed)
			}
		})
	}
}

func TestProblem3Review2ExactRetryScheduleAndErrorHistory(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	failureNumber := 0
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage != recheckStagePersistCandidate {
			return nil
		}
		failureNumber++
		return review2SQLiteError{code: 5, message: fmt.Sprintf("review2 retry failure %d", failureNumber)}
	}}

	for attemptNumber := 1; attemptNumber <= 6; attemptNumber++ {
		if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
			t.Fatalf("attempt %d unexpectedly succeeded", attemptNumber)
		}
		var outcome, finishedRaw string
		var scheduledRaw sql.NullString
		if err := store.db.QueryRow(`SELECT outcome,finished_at,scheduled_retry_at FROM silence_recheck_job_attempts WHERE job_id=1 AND attempt_number=?`, attemptNumber).Scan(&outcome, &finishedRaw, &scheduledRaw); err != nil {
			t.Fatal(err)
		}
		finished, err := time.Parse(time.RFC3339Nano, finishedRaw)
		if err != nil {
			t.Fatal(err)
		}
		if attemptNumber <= 5 {
			delay, ok := recheckRetryDelay(attemptNumber - 1)
			if !ok || outcome != "retry_scheduled" || !scheduledRaw.Valid {
				t.Fatalf("attempt %d outcome=%s scheduled=%v", attemptNumber, outcome, scheduledRaw)
			}
			scheduled, parseErr := time.Parse(time.RFC3339Nano, scheduledRaw.String)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			if got := scheduled.Sub(finished); got != delay {
				t.Fatalf("attempt %d delay=%s want=%s", attemptNumber, got, delay)
			}
			if _, execErr := store.db.Exec(`UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=1`, now.Add(-time.Second).Format(time.RFC3339Nano)); execErr != nil {
				t.Fatal(execErr)
			}
		} else if outcome != "failed" || scheduledRaw.Valid {
			t.Fatalf("terminal attempt outcome=%s scheduled=%v", outcome, scheduledRaw)
		}
	}
	var firstError, lastError string
	if err := store.db.QueryRow(`SELECT first_error_message,last_error_message FROM silence_recheck_jobs WHERE job_id=1`).Scan(&firstError, &lastError); err != nil {
		t.Fatal(err)
	}
	if firstError != "persist_candidate: review2 retry failure 1" || lastError != "persist_candidate: review2 retry failure 6" {
		t.Fatalf("error history first=%q last=%q", firstError, lastError)
	}
	var parentAttemptCount, ledgerAttemptCount int
	if err := store.db.QueryRow(`SELECT attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&parentAttemptCount); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&ledgerAttemptCount); err != nil {
		t.Fatal(err)
	}
	if parentAttemptCount != 6 || ledgerAttemptCount != parentAttemptCount {
		t.Fatalf("final attempt accounting parent=%d ledger=%d", parentAttemptCount, ledgerAttemptCount)
	}
}

func TestProblem3Review2TerminalFailurePreservesNonzeroBudget(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET retry_count=3,consecutive_retry_count=3 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("review2 terminal failure")
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("terminal failure unexpectedly succeeded")
	}
	var status string
	var retryCount, consecutive int
	if err := store.db.QueryRow(`SELECT status,retry_count,consecutive_retry_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &retryCount, &consecutive); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || retryCount != 3 || consecutive != 3 {
		t.Fatalf("terminal state=%s retry=%d consecutive=%d", status, retryCount, consecutive)
	}
}

func TestProblem3Review2IsolatedFailuresAcrossCommittedBatchesResetBudget(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	for _, alertID := range []string{"review2-b", "review2-c"} {
		if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,target_key,match_labels_json,relevant_sources_json) VALUES(?,?,?,?,?)`, 1, alertID, "", `{}`, `[]`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=3 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	fail := true
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if fail && stage == recheckStagePersistCandidate {
			return problem3SQLiteError(5)
		}
		return nil
	}}
	for cycle := 1; cycle <= 3; cycle++ {
		fail = true
		if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1); err == nil {
			t.Fatalf("failure cycle %d unexpectedly succeeded", cycle)
		}
		var consecutive int
		if err := store.db.QueryRow(`SELECT consecutive_retry_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&consecutive); err != nil {
			t.Fatal(err)
		}
		if consecutive != 1 {
			t.Fatalf("failure cycle %d consecutive=%d want=1", cycle, consecutive)
		}
		if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=1`, now.Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		fail = false
		if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	var status string
	var retryCount, consecutive int
	if err := store.db.QueryRow(`SELECT status,retry_count,consecutive_retry_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &retryCount, &consecutive); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || retryCount != 3 || consecutive != 0 {
		t.Fatalf("final state=%s retry=%d consecutive=%d", status, retryCount, consecutive)
	}
}

func captureProblem3Review2State(t *testing.T, db *sql.DB) string {
	t.Helper()
	return captureProblem3Review2Tables(t, db, []string{
		"lifecycle_writer_commits",
		"silence_recheck_jobs", "silence_recheck_job_rules", "silence_recheck_job_candidates",
		"silence_recheck_job_attempts", "silence_recheck_job_operator_actions",
		"alert_states", "alert_target_states", "alert_events", "alert_target_events",
		"alert_suppression_cause_history", "alert_suppression_cause_members",
		"alert_target_suppression_cause_history", "alert_target_suppression_cause_members",
		"alert_target_aggregate_suppression_history", "alert_target_aggregate_suppression_links",
	})
}

func captureProblem3Review2DomainState(t *testing.T, db *sql.DB) string {
	t.Helper()
	return captureProblem3Review2Tables(t, db, []string{
		"silence_recheck_job_rules", "silence_recheck_job_candidates",
		"alert_states", "alert_target_states", "alert_events", "alert_target_events",
		"alert_suppression_cause_history", "alert_suppression_cause_members",
		"alert_target_suppression_cause_history", "alert_target_suppression_cause_members",
		"alert_target_aggregate_suppression_history", "alert_target_aggregate_suppression_links",
	})
}

func captureProblem3Review2Tables(t *testing.T, db *sql.DB, tables []string) string {
	t.Helper()
	parts := make([]string, 0, len(tables))
	for _, table := range tables {
		rows, err := db.Query(`SELECT * FROM ` + table + ` ORDER BY rowid`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		encodedRows := make([]string, 0)
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			encoded, marshalErr := json.Marshal(values)
			if marshalErr != nil {
				rows.Close()
				t.Fatal(marshalErr)
			}
			encodedRows = append(encodedRows, string(encoded))
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		parts = append(parts, table+":"+fmt.Sprint(encodedRows))
	}
	return fmt.Sprint(parts)
}

func TestProblem3Review2CancellationPreservesCompleteState(t *testing.T) {
	for _, afterSelection := range []bool{false, true} {
		t.Run(fmt.Sprintf("after_selection_%v", afterSelection), func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			before := captureProblem3Review2State(t, store.db)
			ctx, cancel := context.WithCancel(context.Background())
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
				t.Fatal("canceled batch unexpectedly succeeded")
			}
			if after := captureProblem3Review2State(t, store.db); after != before {
				t.Fatal("cancellation changed durable Problem 3 or domain state")
			}
		})
	}
}

func TestProblem3Review2RunningJobResumesAfterRestart(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,target_key,match_labels_json,relevant_sources_json) VALUES(?,?,?,?,?)`, 1, "review2-running", "", `{}`, `[]`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=2 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	path := store.sqlitePath()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	var status string
	var attempts, attemptCount, processed int
	if err = reopened.db.QueryRow(`SELECT status,attempt_count,processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attemptCount, &processed); err != nil {
		t.Fatal(err)
	}
	if err = reopened.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if status != "running" || attemptCount != 1 || attempts != 1 || processed != 1 {
		t.Fatalf("restart running state=%s count=%d attempts=%d processed=%d", status, attemptCount, attempts, processed)
	}
	if _, err = reopened.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	if err = reopened.db.QueryRow(`SELECT status,attempt_count,processed_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attemptCount, &processed); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || attemptCount != 2 || processed != 2 {
		t.Fatalf("resumed state=%s count=%d processed=%d", status, attemptCount, processed)
	}
}

func TestProblem3Review2CompleteFIFOAcrossRestartRetryAndAbandon(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fifo.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	nowRaw := now.Format(time.RFC3339Nano)
	futureRaw := now.Add(time.Hour).Format(time.RFC3339Nano)
	insert := func(status string, started, next, finished, abandoned any) {
		t.Helper()
		var jobID int
		if scanErr := store.db.QueryRow(`SELECT COALESCE(MAX(job_id),0)+1 FROM silence_recheck_jobs`).Scan(&jobID); scanErr != nil {
			t.Fatal(scanErr)
		}
		if _, insertErr := store.db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,total_candidates,processed_candidates,created_at,started_at,next_attempt_at,finished_at,abandoned_at,abandon_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, fmt.Sprintf("review2-fifo-context-%d", jobID), fmt.Sprintf("sha256:review2-fifo-%d", jobID), `{}`, "source_context_changed", status, 0, 0, nowRaw, started, next, finished, abandoned, problem3OptionalString(abandoned, "released")); insertErr != nil {
			t.Fatal(insertErr)
		}
	}
	insert("completed", nowRaw, nil, nowRaw, nil)
	insert("abandoned", nowRaw, nil, nowRaw, nowRaw)
	insert("running", nowRaw, nil, nil, nil)
	insert("queued", nil, nil, nil, nil)
	insert("retry_wait", nowRaw, futureRaw, nil, nil)
	insert("failed", nowRaw, nil, nowRaw, nil)
	insert("queued", nil, nil, nil, nil)
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	assertHead := func(jobID int64, status string) {
		t.Helper()
		head, headErr := store.RecheckQueueHead(context.Background())
		if headErr != nil || head.JobID != jobID || head.Status != status {
			t.Fatalf("head=%+v err=%v want=%d/%s", head, headErr, jobID, status)
		}
	}
	assertHead(3, "running")
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	assertHead(4, "queued")
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	assertHead(5, "retry_wait")
	blockedResult, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if err != nil || blockedResult.Disposition != RecheckBatchRetryDeferred {
		t.Fatalf("not-due retry did not return retry_deferred: result=%+v err=%v", blockedResult, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	assertHead(5, "retry_wait")
	if _, err = store.db.Exec(`UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=5`, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	assertHead(6, "failed")
	actor := "review2-fifo"
	if _, err = store.RetryRecheckJobContext(context.Background(), 6, "review2-fifo-retry", "retry", &actor, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertHead(6, "queued")
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	assertHead(7, "queued")
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	insert("failed", nowRaw, nil, nowRaw, nil)
	insert("queued", nil, nil, nil, nil)
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	assertHead(8, "failed")
	if _, err = store.AbandonRecheckJobContext(context.Background(), 8, "review2-fifo-abandon", "release", &actor, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertHead(9, "queued")
}

func problem3OptionalString(marker any, value string) any {
	if marker == nil {
		return nil
	}
	return value
}

func captureProblem3Review2OperatorPreserved(t *testing.T, store *Store) string {
	t.Helper()
	var fingerprint, contextJSON, trigger, firstMessage, firstClass, firstStage, firstAt, lastMessage, lastClass, lastStage, lastAt string
	var total, processed, promoted, stale, ineligible, missing, incomplete, attempts, retries int
	if err := store.db.QueryRow(`
SELECT job_fingerprint,source_context_json,trigger_type,total_candidates,processed_candidates,promoted_candidates,
       stale_conflicts,skipped_ineligible,skipped_missing_labels,skipped_incomplete_sources,attempt_count,retry_count,
       COALESCE(first_error_message,''),COALESCE(first_error_class,''),COALESCE(first_error_stage,''),COALESCE(first_error_at,''),
       COALESCE(last_error_message,''),COALESCE(last_error_class,''),COALESCE(last_error_stage,''),COALESCE(last_error_at,'')
FROM silence_recheck_jobs WHERE job_id=1
`).Scan(&fingerprint, &contextJSON, &trigger, &total, &processed, &promoted, &stale, &ineligible, &missing, &incomplete, &attempts, &retries, &firstMessage, &firstClass, &firstStage, &firstAt, &lastMessage, &lastClass, &lastStage, &lastAt); err != nil {
		t.Fatal(err)
	}
	parent := fmt.Sprintf("%q", []any{fingerprint, contextJSON, trigger, total, processed, promoted, stale, ineligible, missing, incomplete, attempts, retries, firstMessage, firstClass, firstStage, firstAt, lastMessage, lastClass, lastStage, lastAt})
	children := captureProblem3Review2Tables(t, store.db, []string{
		"silence_recheck_job_rules", "silence_recheck_job_candidates", "silence_recheck_job_attempts",
		"alert_states", "alert_target_states", "alert_events", "alert_target_events",
		"alert_suppression_cause_history", "alert_suppression_cause_members",
		"alert_target_suppression_cause_history", "alert_target_suppression_cause_members",
		"alert_target_aggregate_suppression_history", "alert_target_aggregate_suppression_links",
	})
	return parent + children
}

func failProblem3Review2Job(t *testing.T, store *Store) {
	t.Helper()
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("review2 terminal failure")
		}
		return nil
	}}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); err == nil {
		t.Fatal("terminal failure unexpectedly succeeded")
	}
	store.lifecycleTestHooks = nil
}

func TestProblem3Review2OperatorReplaySurvivesRestartAndPreservesInput(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	failProblem3Review2Job(t, store)
	before := captureProblem3Review2OperatorPreserved(t, store)
	actor := "review2-operator"
	result, err := store.RetryRecheckJobContext(context.Background(), 1, "review2-restart-retry", "retry once", &actor, now)
	if err != nil {
		t.Fatal(err)
	}
	if after := captureProblem3Review2OperatorPreserved(t, store); after != before {
		t.Fatal("Retry changed immutable input, progress, errors, or attempts")
	}
	path := store.sqlitePath()
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	replay, err := reopened.RetryRecheckJobContext(context.Background(), 1, "review2-restart-retry", "retry once", &actor, now.Add(time.Minute))
	if err != nil || replay != result {
		t.Fatalf("restart replay=%+v err=%v want=%+v", replay, err, result)
	}
	for _, conflict := range []struct {
		action string
		reason string
		actor  string
	}{
		{action: "retry", reason: "changed reason", actor: actor},
		{action: "abandon", reason: "retry once", actor: actor},
		{action: "retry", reason: "retry once", actor: "different actor"},
	} {
		var conflictErr error
		if conflict.action == "retry" {
			_, conflictErr = reopened.RetryRecheckJobContext(context.Background(), 1, "review2-restart-retry", conflict.reason, &conflict.actor, now)
		} else {
			_, conflictErr = reopened.AbandonRecheckJobContext(context.Background(), 1, "review2-restart-retry", conflict.reason, &conflict.actor, now)
		}
		if !errors.Is(conflictErr, ErrRecheckOperatorConflict) {
			t.Fatalf("changed %s/%q/%q conflict=%v", conflict.action, conflict.reason, conflict.actor, conflictErr)
		}
	}
}

func TestProblem3Review2AbandonReplaySurvivesRestartAndPreservesInput(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	failProblem3Review2Job(t, store)
	before := captureProblem3Review2OperatorPreserved(t, store)
	actor := "review2-abandon"
	result, err := store.AbandonRecheckJobContext(context.Background(), 1, "review2-restart-abandon", "release failed head", &actor, now)
	if err != nil {
		t.Fatal(err)
	}
	if after := captureProblem3Review2OperatorPreserved(t, store); after != before {
		t.Fatal("Abandon changed immutable input, progress, errors, or attempts")
	}
	path := store.sqlitePath()
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	replay, err := reopened.AbandonRecheckJobContext(context.Background(), 1, "review2-restart-abandon", "release failed head", &actor, now.Add(time.Minute))
	if err != nil || replay != result {
		t.Fatalf("restart abandon replay=%+v err=%v want=%+v", replay, err, result)
	}
	changedActor := "review2-other"
	if _, err = reopened.AbandonRecheckJobContext(context.Background(), 1, "review2-restart-abandon", "release failed head", &changedActor, now); !errors.Is(err, ErrRecheckOperatorConflict) {
		t.Fatalf("changed-actor abandon replay=%v", err)
	}
}

func TestProblem3Review2ConcurrentIdenticalOperatorReplayIsSingleAction(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	failProblem3Review2Job(t, store)
	actor := "review2-concurrent"
	const submissions = 8
	results := make(chan RecheckOperatorActionResult, submissions)
	errs := make(chan error, submissions)
	var wg sync.WaitGroup
	for range submissions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.RetryRecheckJobContext(context.Background(), 1, "review2-identical", "retry", &actor, now)
			results <- result
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for result := range results {
		if !result.Applied || result.CurrentStatus != "queued" || result.RequestID != "review2-identical" {
			t.Fatalf("concurrent replay result=%+v", result)
		}
	}
	var actions, recoveryCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions WHERE job_id=1`).Scan(&actions); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT recovery_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&recoveryCount); err != nil {
		t.Fatal(err)
	}
	if actions != 1 || recoveryCount != 1 {
		t.Fatalf("identical submissions actions=%d recovery_count=%d", actions, recoveryCount)
	}
}

func assertProblem3Review2SQLiteCode(t *testing.T, err error, want int) {
	t.Helper()
	var sqliteErr *moderncsqlite.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code() != want {
		t.Fatalf("SQLite rejection=%v code=%v want=%d", err, func() any {
			if sqliteErr == nil {
				return nil
			}
			return sqliteErr.Code()
		}(), want)
	}
}

func TestProblem3Review2CompleteSchemaRejectionMatrix(t *testing.T) {
	const (
		constraintCheck   = 275
		constraintForeign = 787
		constraintNotNull = 1299
		constraintUnique  = 2067
	)
	tests := []struct {
		name      string
		prepare   func(*testing.T, *Store, string, string)
		statement string
		args      func(string, string) []any
		code      int
	}{
		{name: "queued with retry time", statement: `UPDATE silence_recheck_jobs SET next_attempt_at=? WHERE job_id=1`, args: func(start, _ string) []any { return []any{start} }, code: constraintCheck},
		{name: "queued with finish", statement: `UPDATE silence_recheck_jobs SET finished_at=? WHERE job_id=1`, args: func(_, finish string) []any { return []any{finish} }, code: constraintCheck},
		{name: "running with retry time", statement: `UPDATE silence_recheck_jobs SET status='running',started_at=?,next_attempt_at=? WHERE job_id=1`, args: func(start, _ string) []any { return []any{start, start} }, code: constraintCheck},
		{name: "running without start", statement: `UPDATE silence_recheck_jobs SET status='running' WHERE job_id=1`, args: func(string, string) []any { return nil }, code: constraintCheck},
		{name: "retry wait without retry time", statement: `UPDATE silence_recheck_jobs SET status='retry_wait',started_at=? WHERE job_id=1`, args: func(start, _ string) []any { return []any{start} }, code: constraintCheck},
		{name: "retry wait without start", statement: `UPDATE silence_recheck_jobs SET status='retry_wait',next_attempt_at=? WHERE job_id=1`, args: func(start, _ string) []any { return []any{start} }, code: constraintCheck},
		{name: "retry wait with finish", statement: `UPDATE silence_recheck_jobs SET status='retry_wait',started_at=?,next_attempt_at=?,finished_at=? WHERE job_id=1`, args: func(start, finish string) []any { return []any{start, finish, finish} }, code: constraintCheck},
		{name: "failed without finish", statement: `UPDATE silence_recheck_jobs SET status='failed',started_at=? WHERE job_id=1`, args: func(start, _ string) []any { return []any{start} }, code: constraintCheck},
		{name: "failed without start", statement: `UPDATE silence_recheck_jobs SET status='failed',finished_at=? WHERE job_id=1`, args: func(_, finish string) []any { return []any{finish} }, code: constraintCheck},
		{name: "failed with retry time", statement: `UPDATE silence_recheck_jobs SET status='failed',started_at=?,finished_at=?,next_attempt_at=? WHERE job_id=1`, args: func(start, finish string) []any { return []any{start, finish, finish} }, code: constraintCheck},
		{name: "completed without finish", statement: `UPDATE silence_recheck_jobs SET status='completed',started_at=? WHERE job_id=1`, args: func(start, _ string) []any { return []any{start} }, code: constraintCheck},
		{name: "completed without start", statement: `UPDATE silence_recheck_jobs SET status='completed',finished_at=? WHERE job_id=1`, args: func(_, finish string) []any { return []any{finish} }, code: constraintCheck},
		{name: "abandoned without abandoned time", statement: `UPDATE silence_recheck_jobs SET status='abandoned',started_at=?,finished_at=? WHERE job_id=1`, args: func(start, finish string) []any { return []any{start, finish} }, code: constraintCheck},
		{name: "abandoned without start", statement: `UPDATE silence_recheck_jobs SET status='abandoned',finished_at=?,abandoned_at=?,abandon_reason='x' WHERE job_id=1`, args: func(_, finish string) []any { return []any{finish, finish} }, code: constraintCheck},
		{name: "abandoned time differs from finish", statement: `UPDATE silence_recheck_jobs SET status='abandoned',started_at=?,finished_at=?,abandoned_at=?,abandon_reason='x' WHERE job_id=1`, args: func(start, finish string) []any { return []any{start, finish, start} }, code: constraintCheck},
		{name: "non-abandoned has abandon metadata", statement: `UPDATE silence_recheck_jobs SET abandon_reason='x' WHERE job_id=1`, args: func(string, string) []any { return nil }, code: constraintCheck},
		{name: "attempt missing start", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,finished_at,outcome) VALUES(?,?,?,?,?)`, args: func(_, finish string) []any { return []any{1, "missing-start", 1, finish, "committed"} }, code: constraintNotNull},
		{name: "attempt missing finish", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,outcome) VALUES(?,?,?,?,?)`, args: func(start, _ string) []any { return []any{1, "missing-finish", 1, start, "committed"} }, code: constraintNotNull},
		{name: "attempt missing outcome", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at) VALUES(?,?,?,?,?)`, args: func(start, finish string) []any { return []any{1, "missing-outcome", 1, start, finish} }, code: constraintNotNull},
		{name: "attempt invalid outcome", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome) VALUES(?,?,?,?,?,?)`, args: func(start, finish string) []any { return []any{1, "invalid-outcome", 1, start, finish, "unknown"} }, code: constraintCheck},
		{name: "attempt missing job id", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome) VALUES(?,?,?,?,?,?)`, args: func(start, finish string) []any { return []any{nil, "missing-job-id", 1, start, finish, "committed"} }, code: constraintNotNull},
		{name: "attempt missing token", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome) VALUES(?,?,?,?,?,?)`, args: func(start, finish string) []any { return []any{1, nil, 1, start, finish, "committed"} }, code: constraintNotNull},
		{name: "attempt missing number", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome) VALUES(?,?,?,?,?,?)`, args: func(start, finish string) []any { return []any{1, "missing-number", nil, start, finish, "committed"} }, code: constraintNotNull},
		{name: "retry attempt missing class", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome,failure_stage,scheduled_retry_at) VALUES(?,?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{1, "retry-no-class", 1, start, finish, "retry_scheduled", "commit", finish}
		}, code: constraintCheck},
		{name: "retry attempt missing stage", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome,failure_class,scheduled_retry_at) VALUES(?,?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{1, "retry-no-stage", 1, start, finish, "retry_scheduled", "retryable", finish}
		}, code: constraintCheck},
		{name: "failed attempt has schedule", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome,failure_class,failure_stage,scheduled_retry_at) VALUES(?,?,?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{1, "failed-scheduled", 1, start, finish, "failed", "terminal", "evaluate", finish}
		}, code: constraintCheck},
		{name: "duplicate attempt token", prepare: func(t *testing.T, store *Store, start, finish string) {
			if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome) VALUES(?,?,?,?,?,?)`, 1, "duplicate-token", 1, start, finish, "committed"); err != nil {
				t.Fatal(err)
			}
		}, statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome) VALUES(?,?,?,?,?,?)`, args: func(start, finish string) []any { return []any{1, "duplicate-token", 2, start, finish, "committed"} }, code: constraintUnique},
		{name: "attempt missing parent", statement: `INSERT INTO silence_recheck_job_attempts(job_id,attempt_token,attempt_number,started_at,finished_at,outcome) VALUES(?,?,?,?,?,?)`, args: func(start, finish string) []any { return []any{999, "missing-parent", 1, start, finish, "committed"} }, code: constraintForeign},
		{name: "operator missing parent", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,reason,request_fingerprint,requested_at,applied_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{999, "missing-parent", "retry", "", "fp", start, finish, "applied", "queued"}
		}, code: constraintForeign},
		{name: "operator missing request", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,action,request_fingerprint,requested_at,applied_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?)`, args: func(start, finish string) []any { return []any{1, "retry", "fp", start, finish, "applied", "queued"} }, code: constraintNotNull},
		{name: "operator missing action", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,request_fingerprint,requested_at,applied_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{1, "missing-action", "fp", start, finish, "applied", "queued"}
		}, code: constraintNotNull},
		{name: "operator invalid action", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,requested_at,applied_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{1, "invalid-action", "unknown", "fp", start, finish, "applied", "queued"}
		}, code: constraintCheck},
		{name: "operator missing job id", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,requested_at,applied_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{nil, "missing-operator-job-id", "retry", "fp", start, finish, "applied", "queued"}
		}, code: constraintNotNull},
		{name: "operator missing fingerprint", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,requested_at,applied_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{1, "missing-fingerprint", "retry", start, finish, "applied", "queued"}
		}, code: constraintNotNull},
		{name: "operator missing request time", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,applied_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?)`, args: func(_, finish string) []any {
			return []any{1, "missing-requested", "retry", "fp", finish, "applied", "queued"}
		}, code: constraintNotNull},
		{name: "operator missing applied time", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,requested_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?)`, args: func(start, _ string) []any {
			return []any{1, "missing-applied", "retry", "fp", start, "applied", "queued"}
		}, code: constraintNotNull},
		{name: "operator empty result", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,requested_at,applied_at,applied_result,resulting_status) VALUES(?,?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{1, "empty-result", "retry", "fp", start, finish, "", "queued"}
		}, code: constraintCheck},
		{name: "operator missing applied result", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,requested_at,applied_at,resulting_status) VALUES(?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{1, "missing-applied-result", "retry", "fp", start, finish, "queued"}
		}, code: constraintNotNull},
		{name: "operator missing status", statement: `INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,request_fingerprint,requested_at,applied_at,applied_result) VALUES(?,?,?,?,?,?,?)`, args: func(start, finish string) []any {
			return []any{1, "missing-status", "retry", "fp", start, finish, "applied"}
		}, code: constraintNotNull},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, _, now := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			start, finish := now.Format(time.RFC3339Nano), now.Add(time.Second).Format(time.RFC3339Nano)
			if test.prepare != nil {
				test.prepare(t, store, start, finish)
			}
			before := captureProblem3Review2State(t, store.db)
			_, err := store.db.Exec(test.statement, test.args(start, finish)...)
			assertProblem3Review2SQLiteCode(t, err, test.code)
			if after := captureProblem3Review2State(t, store.db); after != before {
				t.Fatal("rejected schema mutation changed durable state")
			}
		})
	}
}

func TestProblem3Review2ParentLedgerMismatchPreservesCompleteState(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET attempt_count=1 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	before := captureProblem3Review2State(t, store.db)
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterOperationInvariant) {
		t.Fatalf("count mismatch error=%v", err)
	}
	if after := captureProblem3Review2State(t, store.db); after != before {
		t.Fatal("parent/ledger mismatch changed durable state")
	}
}
