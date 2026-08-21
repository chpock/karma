package alertlifecycle

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
	moderncsqlite "modernc.org/sqlite"
)

type writerTransactionStarterFunc func(context.Context, *sql.TxOptions) (*sql.Tx, error)

func (f writerTransactionStarterFunc) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return f(ctx, opts)
}

func openWriterLockTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(0)", filepath.Join(t.TempDir(), "lock.sqlite"))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err = db.Exec(`CREATE TABLE lock_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatalf("creating lock_probe: %v", err)
	}
	if _, err = db.Exec(`INSERT INTO lock_probe(id, value) VALUES(1, 'initial')`); err != nil {
		t.Fatalf("seeding lock_probe: %v", err)
	}
	return db
}

func sqliteErrorCode(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		t.Fatal("expected SQLite error, got nil")
	}
	var sqliteErr *moderncsqlite.Error
	if !errors.As(err, &sqliteErr) {
		t.Fatalf("expected modernc SQLite error, got %T: %v", err, err)
	}
	return sqliteErr.Code()
}

func beginDeferredAndRead(t *testing.T, conn *sql.Conn) *sql.Tx {
	t.Helper()
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx() error: %v", err)
	}
	var value string
	if err = tx.QueryRow(`SELECT value FROM lock_probe WHERE id=1`).Scan(&value); err != nil {
		_ = tx.Rollback()
		t.Fatalf("deferred SELECT error: %v", err)
	}
	if value != "initial" {
		_ = tx.Rollback()
		t.Fatalf("unexpected initial value: %q", value)
	}
	return tx
}

func beginImmediate(t *testing.T, conn *sql.Conn) {
	t.Helper()
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("BEGIN IMMEDIATE error: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
	})
}

func TestDeferredTransactionReturnsBusyCodeWhenWriterIsHeld(t *testing.T) {
	db := openWriterLockTestDB(t)
	ctx := context.Background()
	reader, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writer, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	tx := beginDeferredAndRead(t, reader)
	t.Cleanup(func() { _ = tx.Rollback() })
	beginImmediate(t, writer)

	if _, err = tx.ExecContext(ctx, `UPDATE lock_probe SET value='reader' WHERE id=1`); err == nil {
		t.Fatal("deferred read-to-write upgrade unexpectedly succeeded")
	} else if code := sqliteErrorCode(t, err); code != 5 {
		t.Fatalf("unexpected SQLite error code: got=%d want=5", code)
	}
}

func TestDeferredTransactionReturnsSnapshotCodeAfterWriterCommit(t *testing.T) {
	db := openWriterLockTestDB(t)
	ctx := context.Background()
	reader, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writer, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	tx := beginDeferredAndRead(t, reader)
	t.Cleanup(func() { _ = tx.Rollback() })
	beginImmediate(t, writer)
	if _, err = writer.ExecContext(ctx, `UPDATE lock_probe SET value='writer' WHERE id=1`); err != nil {
		t.Fatalf("writer UPDATE error: %v", err)
	}
	if _, err = writer.ExecContext(ctx, `COMMIT`); err != nil {
		t.Fatalf("writer COMMIT error: %v", err)
	}

	if _, err = tx.ExecContext(ctx, `UPDATE lock_probe SET value='reader' WHERE id=1`); err == nil {
		t.Fatal("stale deferred snapshot unexpectedly committed a write")
	} else if code := sqliteErrorCode(t, err); code != 517 {
		t.Fatalf("unexpected SQLite error code: got=%d want=517", code)
	}
}

func TestWALReaderContinuesWhileImmediateWriterIsOpen(t *testing.T) {
	db := openWriterLockTestDB(t)
	ctx := context.Background()
	writer, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	reader, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	beginImmediate(t, writer)
	var value string
	if err = reader.QueryRowContext(ctx, `SELECT value FROM lock_probe WHERE id=1`).Scan(&value); err != nil {
		t.Fatalf("WAL reader was blocked by immediate writer: %v", err)
	}
	if value != "initial" {
		t.Fatalf("unexpected reader value: %q", value)
	}
}

type writerStartBarrier struct {
	conn    *sql.Conn
	started chan<- struct{}
	proceed <-chan struct{}
	once    sync.Once
}

type writerStartObserver struct {
	inner   writerTransactionStarter
	started chan<- struct{}
	once    sync.Once
}

type writerStartFailure struct {
	started chan<- struct{}
	once    sync.Once
	err     error
}

type writerStartSequence struct {
	conn     *sql.Conn
	starts   []chan struct{}
	proceeds []<-chan struct{}
	mu       sync.Mutex
	index    int
}

func (s *writerStartSequence) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	s.mu.Lock()
	index := s.index
	s.index++
	s.mu.Unlock()
	if index >= len(s.starts) || index >= len(s.proceeds) {
		return nil, errors.New("unexpected writer transaction phase")
	}
	close(s.starts[index])
	<-s.proceeds[index]
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.conn.BeginTx(ctx, opts)
}

func (f *writerStartFailure) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	f.once.Do(func() { close(f.started) })
	return nil, f.err
}

func (o *writerStartObserver) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	o.once.Do(func() { close(o.started) })
	return o.inner.BeginTx(ctx, opts)
}

func seedQueuedWriterTestJob(t *testing.T, store *Store) {
	t.Helper()
	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.db.Exec(`
INSERT INTO silence_recheck_jobs(
  source_context_fingerprint, job_fingerprint, source_context_json,
  trigger_type, status, created_at
) VALUES('writer-test', 'sha256:writer-test', '{"formatVersion":1,"sources":[],"clusters":[]}', 'source_context_changed', 'queued', ?)
`, createdAt); err != nil {
		t.Fatalf("failed to seed recheck job: %v", err)
	}
}

type writerRecheckAggregateSnapshot struct {
	Job        writerRecheckJobSnapshot         `json:"job"`
	Rules      []writerRecheckRuleSnapshot      `json:"rules"`
	Candidates []writerRecheckCandidateSnapshot `json:"candidates"`
}

type writerRecheckJobSnapshot struct {
	JobID                    int64
	SourceContextFingerprint string
	JobFingerprint           string
	SourceContext            string
	TriggerType              string
	Status                   string
	CursorAlertID            sql.NullString
	TotalCandidates          int
	ProcessedCandidates      int
	PromotedCandidates       int
	SkippedIneligible        int
	SkippedMissingLabels     int
	SkippedIncomplete        int
	StaleConflicts           int
	RetryCount               int
	CreatedAt                string
	StartedAt                sql.NullString
	FinishedAt               sql.NullString
	LastErrorMessage         sql.NullString
}

type writerRecheckRuleSnapshot struct {
	JobID                 int64
	SourceName            string
	ClusterID             string
	TargetKey             string
	RuleID                int64
	RevisionID            int64
	ObservedState         string
	EvaluationFingerprint string
	MatchersJSON          string
}

type writerRecheckCandidateSnapshot struct {
	JobID                    int64
	AlertID                  string
	TargetKey                string
	ExpectedAlertLastSeenAt  sql.NullString
	ExpectedTargetLastSeenAt sql.NullString
	MatchLabelsJSON          sql.NullString
	RelevantSourcesJSON      string
	TargetSourceMembersJSON  sql.NullString
	ProcessedAt              sql.NullString
	Outcome                  sql.NullString
}

func captureWriterRecheckAggregate(t *testing.T, store *Store, jobID int64) string {
	t.Helper()
	snapshot := writerRecheckAggregateSnapshot{}
	var job writerRecheckJobSnapshot
	if err := store.db.QueryRow(`
SELECT job_id, source_context_fingerprint, job_fingerprint, source_context_json,
       trigger_type, status, cursor_alert_id, total_candidates, processed_candidates,
       promoted_candidates, skipped_ineligible, skipped_missing_labels,
       skipped_incomplete_sources, stale_conflicts, retry_count, created_at,
       started_at, finished_at, last_error_message
FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(
		&job.JobID, &job.SourceContextFingerprint, &job.JobFingerprint, &job.SourceContext,
		&job.TriggerType, &job.Status, &job.CursorAlertID, &job.TotalCandidates,
		&job.ProcessedCandidates, &job.PromotedCandidates, &job.SkippedIneligible,
		&job.SkippedMissingLabels, &job.SkippedIncomplete, &job.StaleConflicts,
		&job.RetryCount, &job.CreatedAt, &job.StartedAt, &job.FinishedAt, &job.LastErrorMessage,
	); err != nil {
		t.Fatal(err)
	}
	snapshot.Job = job
	ruleRows, err := store.db.Query(`
SELECT job_id, source_name, cluster_id, target_key, rule_id, revision_id, observed_state, evaluation_fingerprint, matchers_json
FROM silence_recheck_job_rules WHERE job_id=? ORDER BY job_id, source_name, rule_id`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer ruleRows.Close()
	for ruleRows.Next() {
		var rule writerRecheckRuleSnapshot
		if err = ruleRows.Scan(&rule.JobID, &rule.SourceName, &rule.ClusterID, &rule.TargetKey, &rule.RuleID, &rule.RevisionID, &rule.ObservedState, &rule.EvaluationFingerprint, &rule.MatchersJSON); err != nil {
			t.Fatal(err)
		}
		snapshot.Rules = append(snapshot.Rules, rule)
	}
	if err = ruleRows.Err(); err != nil {
		t.Fatal(err)
	}
	candidateRows, err := store.db.Query(`
SELECT job_id, alert_id, target_key, expected_alert_last_seen_at, expected_target_last_seen_at,
       match_labels_json, relevant_sources_json, target_source_members_json, processed_at, outcome
FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY job_id, alert_id, target_key`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer candidateRows.Close()
	for candidateRows.Next() {
		var candidate writerRecheckCandidateSnapshot
		if err = candidateRows.Scan(&candidate.JobID, &candidate.AlertID, &candidate.TargetKey, &candidate.ExpectedAlertLastSeenAt, &candidate.ExpectedTargetLastSeenAt, &candidate.MatchLabelsJSON, &candidate.RelevantSourcesJSON, &candidate.TargetSourceMembersJSON, &candidate.ProcessedAt, &candidate.Outcome); err != nil {
			t.Fatal(err)
		}
		snapshot.Candidates = append(snapshot.Candidates, candidate)
	}
	if err = candidateRows.Err(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func seedWriterRecheckAggregate(t *testing.T, store *Store) int64 {
	t.Helper()
	seedQueuedWriterTestJob(t, store)
	now := time.Now().UTC().Truncate(time.Millisecond)
	created := now.Add(-time.Hour).Format(time.RFC3339Nano)
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET source_context_fingerprint='aggregate-context', job_fingerprint='sha256:aggregate-context', source_context_json='{"formatVersion":1,"sources":[{"sourceName":"am-a","syncStatus":"success"}],"clusters":[]}' , trigger_type='source_context_changed', cursor_alert_id='cursor-before', total_candidates=2, processed_candidates=1, promoted_candidates=1, skipped_ineligible=0, skipped_missing_labels=0, skipped_incomplete_sources=0, stale_conflicts=0, retry_count=2, created_at=?, started_at=?, first_error_message='root-error', last_error_message='root-error' WHERE job_id=(SELECT MAX(job_id) FROM silence_recheck_jobs)`, created, created); err != nil {
		t.Fatal(err)
	}
	var jobID int64
	if err := store.db.QueryRow(`SELECT MAX(job_id) FROM silence_recheck_jobs`).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO alertmanager_sources(source_name,current_cluster_id,sync_status,first_configured_at,last_configured_at) VALUES('am-a','cluster-a','success',?,?)`, created, created); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"one", "two"} {
		if _, err := store.db.Exec(`INSERT INTO silence_rules(cluster_id,silence_id,first_observed_at,last_observed_at) VALUES('cluster-a',?,?,?)`, "rule-"+suffix, created, created); err != nil {
			t.Fatal(err)
		}
		var ruleID int64
		if err := store.db.QueryRow(`SELECT rule_id FROM silence_rules WHERE silence_id=?`, "rule-"+suffix).Scan(&ruleID); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec(`INSERT INTO silence_rule_episodes(rule_id,episode_number,first_observed_at,last_confirmed_at,open_marker) VALUES(?,1,?,?,1)`, ruleID, created, created); err != nil {
			t.Fatal(err)
		}
		var episodeID int64
		if err := store.db.QueryRow(`SELECT episode_id FROM silence_rule_episodes WHERE rule_id=?`, ruleID).Scan(&episodeID); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec(`INSERT INTO silence_revisions(episode_id,rule_id,revision_number,content_fingerprint,evaluation_fingerprint,matchers_json,starts_at,ends_at,created_by,comment,first_observed_at,last_observed_at) VALUES(?,?,1,?,?,?, ?,?,'writer','rule',?,?)`, episodeID, ruleID, "sha256:content-"+suffix, "sha256:evaluation-"+suffix, `[{"name":"alertname","value":"WriterProbe","isRegex":false,"isEqual":true}]`, created, now.Add(time.Hour).Format(time.RFC3339Nano), created, created); err != nil {
			t.Fatal(err)
		}
		var revisionID int64
		if err := store.db.QueryRow(`SELECT revision_id FROM silence_revisions WHERE rule_id=?`, ruleID).Scan(&revisionID); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_rules(job_id,source_name,cluster_id,rule_id,revision_id,observed_state) VALUES(?,?,?,?,?,'active')`, jobID, "am-a", "cluster-a", ruleID, revisionID); err != nil {
			t.Fatal(err)
		}
	}
	for index, id := range []string{"aggregate-alert-one", "aggregate-alert-two"} {
		matchLabels := `{"alertname":"WriterProbe","instance":"` + id + `"}`
		if err := store.UpsertCurrentAlert(AlertCurrent{AlertID: id, State: StateInactive, RawState: StateActive, Receiver: "team-a", GroupID: "group-a", LabelsJSON: matchLabels, MatchLabelsJSON: &matchLabels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `[]`}); err != nil {
			t.Fatal(err)
		}
		processedAt := any(nil)
		outcome := any(nil)
		if index == 0 {
			processedAt = created
			outcome = "promoted"
		}
		if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,expected_alert_last_seen_at,match_labels_json,relevant_sources_json,processed_at,outcome) VALUES(?,?,?,?,?,?,?)`, jobID, id, now.Format(time.RFC3339Nano), matchLabels, `[]`, processedAt, outcome); err != nil {
			t.Fatal(err)
		}
	}
	return jobID
}

func TestConfiguredStoreWriterTransactionsStartImmediately(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	tx, err := store.writerTransactionStarter.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })

	competitor, err := store.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = competitor.Close() })
	if _, err = competitor.ExecContext(context.Background(), `PRAGMA busy_timeout=0`); err != nil {
		t.Fatal(err)
	}
	if _, err = competitor.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err == nil {
		t.Fatal("a second immediate writer unexpectedly acquired the writer lock")
	} else if code := sqliteErrorCode(t, err); code != 5 {
		t.Fatalf("unexpected competing writer error code: got=%d want=5", code)
	}
}

func (b *writerStartBarrier) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	b.once.Do(func() { close(b.started) })
	select {
	case <-b.proceed:
		return b.conn.BeginTx(ctx, opts)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestRecheckCannotSelectJobBeforeWriterTransactionStarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	seedQueuedWriterTestJob(t, store)

	writerConn, err := store.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writerConn.Close() })

	started := make(chan struct{})
	proceed := make(chan struct{})
	store.writerTransactionStarter = &writerStartBarrier{
		conn:    writerConn,
		started: started,
		proceed: proceed,
	}

	workerDone := make(chan error, 1)
	go func() {
		_, runErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
		workerDone <- runErr
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach writer transaction acquisition")
	}

	var status string
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=(SELECT MAX(job_id) FROM silence_recheck_jobs)`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("job was modified before writer transaction acquisition: status=%q", status)
	}

	close(proceed)
	if err = <-workerDone; err != nil {
		t.Fatalf("worker returned error after writer acquisition: %v", err)
	}
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=(SELECT MAX(job_id) FROM silence_recheck_jobs)`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("job did not complete after writer acquisition: status=%q", status)
	}
}

func TestRecheckWaitsForWriterBeforeSelectingJob(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedQueuedWriterTestJob(t, store)

	competitor, err := store.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = competitor.Close() })
	if _, err = competitor.ExecContext(context.Background(), `PRAGMA busy_timeout=0`); err != nil {
		t.Fatal(err)
	}
	if _, err = competitor.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("failed to acquire competing writer lock: %v", err)
	}

	started := make(chan struct{})
	store.writerTransactionStarter = &writerStartObserver{
		inner:   store.writerTransactionStarter,
		started: started,
	}
	workerDone := make(chan error, 1)
	go func() {
		_, runErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
		workerDone <- runErr
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not attempt writer transaction acquisition")
	}

	var status string
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=(SELECT MAX(job_id) FROM silence_recheck_jobs)`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("job was selected before writer acquisition completed: status=%q", status)
	}
	if strings.Contains(logOutput.String(), `"event":"job_selected"`) {
		t.Fatalf("job_selected event was emitted before writer acquisition: %s", logOutput.String())
	}
	if _, err = competitor.ExecContext(context.Background(), `ROLLBACK`); err != nil {
		t.Fatalf("failed to release competing writer lock: %v", err)
	}

	if err = <-workerDone; err != nil {
		t.Fatalf("worker returned error after competing writer released: %v", err)
	}
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=(SELECT MAX(job_id) FROM silence_recheck_jobs)`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("job did not complete after writer acquisition: status=%q", status)
	}
	if !strings.Contains(logOutput.String(), `"event":"job_selected"`) {
		t.Fatalf("job_selected event was not emitted after writer acquisition: %s", logOutput.String())
	}
}

func TestRecheckWriterAcquisitionFailureLeavesQueuedJobUntouched(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	jobID := seedWriterRecheckAggregate(t, store)
	beforeAggregate := captureWriterRecheckAggregate(t, store, jobID)
	var beforeLedger int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits`).Scan(&beforeLedger); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	acquisitionErr := errors.New("controlled writer acquisition busy")
	store.writeMu.Lock()
	store.writerTransactionStarter = &writerStartFailure{
		started: started,
		err:     acquisitionErr,
	}
	store.writeMu.Unlock()
	workerDone := make(chan error, 1)
	go func() {
		_, runErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
		workerDone <- runErr
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not attempt writer transaction acquisition")
	}
	runErr := <-workerDone
	if runErr == nil {
		t.Fatal("writer acquisition failure was not returned")
	}
	var typedAcquisitionErr *writerAcquisitionError
	if !errors.As(runErr, &typedAcquisitionErr) || typedAcquisitionErr.operation != writerOperationRecheckBatch || !errors.Is(runErr, acquisitionErr) {
		t.Fatalf("recheck acquisition error identity: %v", runErr)
	}
	if afterAggregate := captureWriterRecheckAggregate(t, store, jobID); afterAggregate != beforeAggregate {
		t.Fatalf("writer acquisition failure changed job aggregate: before=%s after=%s", beforeAggregate, afterAggregate)
	}
	var afterLedger int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits`).Scan(&afterLedger); err != nil {
		t.Fatal(err)
	}
	if afterLedger != beforeLedger {
		t.Fatalf("writer acquisition inserted a commit token: before=%d after=%d", beforeLedger, afterLedger)
	}
	for _, event := range []string{"job_selected", "job_started", "job_resumed", "job_failed", "batch_completed", "job_completed"} {
		if strings.Contains(logOutput.String(), `"event":"`+event+`"`) {
			t.Fatalf("event %s emitted during failed acquisition: %s", event, logOutput.String())
		}
	}
}

func TestRegroupWriterAcquisitionFailureDoesNotPersistDomainFailure(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	beforeMetadata := captureRegroupMetadataSnapshot(t, store)
	busyErr := errors.New("controlled writer acquisition busy")
	startCalls := 0
	store.writerTransactionStarter = writerTransactionStarterFunc(func(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
		startCalls++
		if startCalls == 1 {
			return nil, busyErr
		}
		return store.writerConn.BeginTx(ctx, opts)
	})

	_, err = store.RunRegroupToConfig(context.Background(), GroupingConfig{LabelsKeep: []string{"alertname"}}, 1)
	var acquisitionErr *writerAcquisitionError
	if !errors.As(err, &acquisitionErr) {
		t.Fatalf("regroup acquisition error was not typed: %v", err)
	}
	if acquisitionErr.operation != writerOperationRegroupBegin || !errors.Is(err, busyErr) {
		t.Fatalf("regroup acquisition error identity: operation=%q error=%v", acquisitionErr.operation, err)
	}
	if startCalls != 1 {
		t.Fatalf("domain failure persistence acquired a second writer transaction: calls=%d", startCalls)
	}
	if afterMetadata := captureRegroupMetadataSnapshot(t, store); afterMetadata != beforeMetadata {
		t.Fatalf("regroup metadata changed after acquisition failure: before=%q after=%q", beforeMetadata, afterMetadata)
	}
	var failureLedger int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, writerOperationRegroupFailure).Scan(&failureLedger); err != nil {
		t.Fatal(err)
	}
	if failureLedger != 0 {
		t.Fatalf("regroup failure persistence ledger rows: got=%d want=0", failureLedger)
	}

	store.writerTransactionStarter = sqlConnWriterTransactionStarter{conn: store.writerConn}
	if _, err = store.RunRegroupToConfig(context.Background(), GroupingConfig{LabelsKeep: []string{"alertname"}}, 1); err != nil {
		t.Fatalf("regroup did not retry successfully after acquisition failure: %v", err)
	}
}

func TestWriterAcquisitionFailureReplacesBadConnectionWithoutDomainEntry(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	oldConn := store.writerConn
	var recoveryContextErr error
	store.writerConnectionAcquirer = func(ctx context.Context) (*sql.Conn, error) {
		if ctx.Err() != nil {
			recoveryContextErr = ctx.Err()
		}
		return store.db.Conn(ctx)
	}
	callbackCalls := 0
	store.writerTransactionStarter = writerTransactionStarterFunc(func(context.Context, *sql.TxOptions) (*sql.Tx, error) {
		callbackCalls++
		return nil, driver.ErrBadConn
	})

	_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
		callbackCalls++
		return struct{}{}, nil
	})
	var acquisitionErr *writerAcquisitionError
	if !errors.As(err, &acquisitionErr) || !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("bad connection acquisition error: %v", err)
	}
	if callbackCalls != 1 {
		t.Fatalf("domain callback ran after acquisition failure: calls=%d", callbackCalls)
	}
	if recoveryContextErr != nil {
		t.Fatalf("bad connection replacement used a canceled context: %v", recoveryContextErr)
	}
	if store.writerConn == nil || store.writerConn == oldConn {
		t.Fatal("bad writer connection was not replaced")
	}

	store.writerTransactionStarter = sqlConnWriterTransactionStarter{conn: store.writerConn}
	if err = store.withWriterTx(context.Background(), writerOperationPoll, func(tx *sql.Tx) error {
		_, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('after_bad_connection', 'ok')`)
		return execErr
	}); err != nil {
		t.Fatalf("healthy writer did not succeed after bad connection replacement: %v", err)
	}
}

func TestWriterAcquisitionSQLConnDoneReplacesConnection(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	oldConn := store.writerConn
	var recoveryContextErr error
	store.writerConnectionAcquirer = func(ctx context.Context) (*sql.Conn, error) {
		if ctx.Err() != nil {
			recoveryContextErr = ctx.Err()
		}
		return store.db.Conn(ctx)
	}
	store.writerTransactionStarter = writerTransactionStarterFunc(func(context.Context, *sql.TxOptions) (*sql.Tx, error) {
		return nil, sql.ErrConnDone
	})

	_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	})
	var acquisitionErr *writerAcquisitionError
	if !errors.As(err, &acquisitionErr) || !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("closed connection acquisition error: %v", err)
	}
	if recoveryContextErr != nil {
		t.Fatalf("closed connection replacement used a canceled context: %v", recoveryContextErr)
	}
	if store.writerConn == nil || store.writerConn == oldConn {
		t.Fatal("closed writer connection was not replaced")
	}
}

func TestWriterAcquisitionBadConnectionJoinsReplacementFailure(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	replacementErr := errors.New("controlled replacement failure")
	store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) {
		return nil, replacementErr
	}
	store.writerTransactionStarter = writerTransactionStarterFunc(func(context.Context, *sql.TxOptions) (*sql.Tx, error) {
		return nil, driver.ErrBadConn
	})

	_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	})
	var acquisitionErr *writerAcquisitionError
	if !errors.As(err, &acquisitionErr) || !errors.Is(err, driver.ErrBadConn) || !errors.Is(err, replacementErr) {
		t.Fatalf("bad connection replacement error: %v", err)
	}
	if store.writerRecoveryState != writerRecoveryUnavailable {
		t.Fatalf("writer recovery state after replacement failure: %v", store.writerRecoveryState)
	}
}

func TestWriterAcquisitionFailurePreservesCancellationAndDeadline(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			callbackCalls := 0
			store.writerTransactionStarter = writerTransactionStarterFunc(func(context.Context, *sql.TxOptions) (*sql.Tx, error) {
				return nil, test.err
			})
			_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
				callbackCalls++
				return struct{}{}, nil
			})
			var acquisitionErr *writerAcquisitionError
			if !errors.As(err, &acquisitionErr) || !errors.Is(err, test.err) {
				t.Fatalf("typed %s acquisition error: %v", test.name, err)
			}
			if acquisitionErr.operation != writerOperationPoll || callbackCalls != 0 {
				t.Fatalf("%s acquisition state: operation=%q callback_calls=%d", test.name, acquisitionErr.operation, callbackCalls)
			}
		})
	}
}

func TestWriterCancellationBeforeLedgerInsertWithoutHookDoesNotCommit(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	commitCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		return tx.Commit()
	}
	_, err = withWriterTxValue(ctx, store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		if _, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('no_hook_cancelled', 'must_rollback')`); execErr != nil {
			return struct{}{}, execErr
		}
		cancel()
		return struct{}{}, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("no-hook cancellation error: %v", err)
	}
	if commitCalls != 0 {
		t.Fatalf("commit was called after cancellation: calls=%d", commitCalls)
	}
	var domainRows, ledgerRows int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM metadata WHERE key='no_hook_cancelled'`).Scan(&domainRows); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, writerOperationPoll).Scan(&ledgerRows); err != nil {
		t.Fatal(err)
	}
	if domainRows != 0 || ledgerRows != 0 {
		t.Fatalf("canceled transaction persisted data: domain=%d ledger=%d", domainRows, ledgerRows)
	}
	store.writerCommit = nil
	if err = store.withWriterTx(context.Background(), writerOperationPoll, func(tx *sql.Tx) error {
		_, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('after_no_hook_cancelled', 'ok')`)
		return execErr
	}); err != nil {
		t.Fatalf("writer did not recover after no-hook cancellation: %v", err)
	}
}

func captureRegroupMetadataSnapshot(t *testing.T, store *Store) string {
	t.Helper()
	rows, err := store.db.Query(`SELECT key, value FROM metadata WHERE key LIKE 'regroup_%' ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var snapshot strings.Builder
	for rows.Next() {
		var key, value string
		if err = rows.Scan(&key, &value); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&snapshot, "%s=%s\n", key, value)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return snapshot.String()
}

func TestRecheckWriterInvariantAfterSelectionDoesNotFailJob(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	jobID := seedWriterRecheckAggregate(t, store)
	store.writerCommit = func(tx *sql.Tx) error {
		if commitCallErr := tx.Commit(); commitCallErr != nil {
			return commitCallErr
		}
		return errors.New("ambiguous recheck commit")
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		return false, ErrWriterOperationInvariant
	}
	_, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if !errors.Is(err, ErrWriterOperationInvariant) {
		t.Fatalf("recheck invariant error: got=%v", err)
	}
	var status string
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status == "failed" {
		t.Fatalf("writer invariant incorrectly failed selected job: status=%q", status)
	}
	var failureLedger int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, writerOperationRecheckFailure).Scan(&failureLedger); err != nil {
		t.Fatal(err)
	}
	if failureLedger != 0 {
		t.Fatalf("writer invariant created forbidden recheck_failure ledger row: %d", failureLedger)
	}
	if strings.Contains(logOutput.String(), `"event":"job_failed"`) {
		t.Fatalf("writer invariant emitted job_failed: %s", logOutput.String())
	}
}

func TestRecheckZeroCandidateJobEmitsStartThenCompletion(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedQueuedWriterTestJob(t, store)
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatalf("zero-candidate queued job failed: %v", err)
	}
	logText := logOutput.String()
	started := strings.Index(logText, `"event":"job_started"`)
	completed := strings.Index(logText, `"event":"job_completed"`)
	if started < 0 || completed < 0 || started > completed {
		t.Fatalf("zero-candidate event order: started=%d completed=%d logs=%s", started, completed, logText)
	}
	if strings.Contains(logText, `"event":"job_resumed"`) {
		t.Fatalf("queued zero-candidate job emitted resume: %s", logText)
	}
}

func TestRecheckZeroCandidateRunningJobEmitsResumeThenCompletion(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err = store.db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,created_at,started_at,total_candidates) VALUES('running-zero','sha256:running-zero','{"formatVersion":1,"sources":[],"clusters":[]}','source_context_changed','running',?,?,0)`, createdAt, createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatalf("zero-candidate running job failed: %v", err)
	}
	logText := logOutput.String()
	resumed := strings.Index(logText, `"event":"job_resumed"`)
	completed := strings.Index(logText, `"event":"job_completed"`)
	if resumed < 0 || completed < 0 || resumed > completed {
		t.Fatalf("zero-candidate resume event order: resumed=%d completed=%d logs=%s", resumed, completed, logText)
	}
}

func TestWriterConnectionReplacementReestablishesConnectionLocalSettings(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	oldConn := store.writerConn
	store.writeMu.Lock()
	err = store.replaceWriterConnectionLocked(context.Background())
	store.writeMu.Unlock()
	if err != nil {
		t.Fatalf("replaceWriterConnectionLocked() error: %v", err)
	}
	if store.writerConn == nil || store.writerConn == oldConn {
		t.Fatal("writer connection was not replaced")
	}
	if err = verifyWriterConnection(context.Background(), store.writerConn); err != nil {
		t.Fatalf("replacement writer connection verification failed: %v", err)
	}
}

func TestWriterTransactionRollsBackOnCallbackError(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	expectedErr := errors.New("controlled callback failure")
	if runErr := store.withWriterTx(context.Background(), writerOperationPoll, func(tx *sql.Tx) error {
		if _, err = tx.Exec(`INSERT INTO metadata(key, value) VALUES('writer_probe', 'must_rollback')`); err != nil {
			return err
		}
		return expectedErr
	}); !errors.Is(runErr, expectedErr) {
		t.Fatalf("withWriterTx() error: got=%v want=%v", runErr, expectedErr)
	}

	var count int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM metadata WHERE key='writer_probe'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("callback error committed writer transaction: count=%d", count)
	}
}

func TestWriterTransactionRollsBackOnPanic(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var rollbackCalls int
	store.writerRollback = func(tx *sql.Tx) error {
		rollbackCalls++
		return tx.Rollback()
	}

	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("withWriterTx() did not propagate callback panic")
			}
		}()
		_ = store.withWriterTx(context.Background(), writerOperationPoll, func(tx *sql.Tx) error {
			if _, err = tx.Exec(`INSERT INTO metadata(key, value) VALUES('writer_panic_probe', 'must_rollback')`); err != nil {
				return err
			}
			panic("controlled callback panic")
		})
	}()

	var count int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM metadata WHERE key='writer_panic_probe'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("callback panic committed writer transaction: count=%d", count)
	}
	if rollbackCalls != 1 {
		t.Fatalf("panic rollback count: got=%d want=1", rollbackCalls)
	}
}

func TestWriterCommitErrorWithDurableTokenReconcilesAsSuccess(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	commitErr := errors.New("ambiguous commit after durable commit")
	commitCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if commitCallErr := tx.Commit(); commitCallErr != nil {
			return commitCallErr
		}
		return commitErr
	}
	value, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (string, error) {
		if _, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('durable_commit_probe', 'committed')`); execErr != nil {
			return "", execErr
		}
		return "committed-result", nil
	})
	if err != nil {
		t.Fatalf("durable commit was not reconciled: %v", err)
	}
	if value != "committed-result" {
		t.Fatalf("unexpected callback result: %q", value)
	}
	if commitCalls != 1 {
		t.Fatalf("commit attempts: got=%d want=1", commitCalls)
	}

	var count int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM metadata WHERE key='durable_commit_probe'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("durable commit row missing after reconciliation: count=%d", count)
	}
}

func TestWriterDelayedCommitReconciliationRunsFinalizerOnceAndBlocksCallback(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	commitErr := errors.New("ambiguous commit")
	lookupErr := errors.New("ledger temporarily unavailable")
	lookupCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		if commitCallErr := tx.Commit(); commitCallErr != nil {
			return commitCallErr
		}
		return commitErr
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, lookupErr
		}
		return true, nil
	}
	finalizerCalls := 0
	callbackCalls := 0
	finalizer := &writerAfterCommitFinalizer{
		ctx:           context.Background(),
		operationKind: writerOperationPoll,
		run: func(reconciled bool) {
			if !reconciled {
				t.Fatal("delayed finalizer was not marked reconciled")
			}
			finalizerCalls++
		},
	}
	_, err = withWriterTxValueAfterCommit(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	}, finalizer)
	if !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("first commit outcome: got=%v want unknown", err)
	}
	_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		callbackCalls++
		return struct{}{}, nil
	})
	if !errors.Is(err, ErrWriterCommitReconciled) {
		t.Fatalf("delayed reconciliation result: got=%v want commit_reconciled", err)
	}
	if finalizerCalls != 1 {
		t.Fatalf("delayed finalizer calls: got=%d want=1", finalizerCalls)
	}
	if callbackCalls != 0 {
		t.Fatalf("callback ran during reconciliation: calls=%d", callbackCalls)
	}

	store.writeMu.Lock()
	store.writerCommit = nil
	store.writerCommitLookup = nil
	store.writeMu.Unlock()
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		callbackCalls++
		return struct{}{}, nil
	}); err != nil {
		t.Fatalf("explicit retry after reconciliation: %v", err)
	}
	if callbackCalls != 1 {
		t.Fatalf("explicit retry callback calls: got=%d want=1", callbackCalls)
	}
}

func TestWriterNotCommittedOutcomeDiscardsFinalizer(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.writerCommit = func(tx *sql.Tx) error {
		_ = tx.Rollback()
		return errors.New("not committed")
	}
	finalizerCalls := 0
	_, err = withWriterTxValueAfterCommit(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	}, &writerAfterCommitFinalizer{ctx: context.Background(), operationKind: writerOperationPoll, run: func(bool) { finalizerCalls++ }})
	if err == nil || finalizerCalls != 0 {
		t.Fatalf("not-committed outcome: err=%v finalizerCalls=%d", err, finalizerCalls)
	}
}

func TestWriterFinalizerPanicDoesNotPoisonWriter(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err = withWriterTxValueAfterCommit(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	}, &writerAfterCommitFinalizer{ctx: context.Background(), operationKind: writerOperationPoll, run: func(bool) { panic("finalizer failure") }}); err != nil {
		t.Fatalf("finalizer panic changed committed outcome: %v", err)
	}
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	}); err != nil {
		t.Fatalf("writer poisoned by finalizer panic: %v", err)
	}
}

func TestWriterOperationKindMustBeKnownBeforeBegin(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	beginCalls := 0
	store.writeMu.Lock()
	originalStarter := store.writerTransactionStarter
	store.writerTransactionStarter = writerTransactionStarterFunc(func(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
		beginCalls++
		return originalStarter.BeginTx(ctx, opts)
	})
	store.writeMu.Unlock()
	_, err = withWriterTxValue(context.Background(), store, "", func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	})
	if !errors.Is(err, ErrWriterOperationInvariant) {
		t.Fatalf("empty operation kind: got=%v", err)
	}
	if beginCalls != 0 {
		t.Fatalf("empty operation kind began transaction: calls=%d", beginCalls)
	}
	if _, err = withWriterTxValue(context.Background(), store, "unknown_operation", func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	}); !errors.Is(err, ErrWriterOperationInvariant) {
		t.Fatalf("unknown operation kind: got=%v", err)
	}
}

func TestWriterKindMismatchEntersTerminalInvariantState(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	token := "kind-mismatch-token"
	if _, err = store.db.Exec(`INSERT INTO lifecycle_writer_commits(operation_token, operation_kind, committed_at) VALUES(?, ?, ?)`, token, "wrong_kind", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	store.writeMu.Lock()
	store.pendingWriterCommit = &pendingWriterCommit{token: token, kind: writerOperationPoll}
	store.writerRecoveryState = writerRecoveryCommitUnknown
	store.writeMu.Unlock()
	_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		t.Fatal("mutation callback ran after ledger invariant failure")
		return struct{}{}, nil
	})
	if !errors.Is(err, ErrWriterOperationInvariant) {
		t.Fatalf("kind mismatch: got=%v", err)
	}
	lookupCalls := 0
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		return false, errors.New("must not retry invariant lookup")
	}
	_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	})
	if !errors.Is(err, ErrWriterOperationInvariant) || lookupCalls != 0 {
		t.Fatalf("terminal invariant was not retained: err=%v lookups=%d", err, lookupCalls)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("Close() from invariant state: %v", err)
	}
}

func TestStoreCloseZeroValueAndRepeatedCloseAfterErrorAreSafe(t *testing.T) {
	var zero Store
	if err := zero.Close(); err != nil {
		t.Fatalf("zero Store Close(): %v", err)
	}
	if err := zero.Close(); err != nil {
		t.Fatalf("repeated zero Store Close(): %v", err)
	}

	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("injected writer close failure")
	store.writerClose = func(conn *sql.Conn) error {
		_ = conn.Close()
		return closeErr
	}
	if err = store.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error: got=%v want=%v", err, closeErr)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("repeated Close() after error: %v", err)
	}
}

func TestWriterCommitErrorWithoutDurableTokenIsNotUnknown(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	commitErr := errors.New("commit failed before durable commit")
	store.writerCommit = func(tx *sql.Tx) error {
		_ = tx.Rollback()
		return commitErr
	}
	_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		if _, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('absent_commit_probe', 'must_rollback')`); execErr != nil {
			return struct{}{}, execErr
		}
		return struct{}{}, nil
	})
	if err == nil || errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("absent commit was classified incorrectly: %v", err)
	}

	var count int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM metadata WHERE key='absent_commit_probe'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("absent commit row unexpectedly exists: count=%d", count)
	}
}

func TestWriterRollbackErrorDiscardsConnection(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	oldConn := store.writerConn
	rollbackErr := errors.New("controlled rollback failure")
	store.writerRollback = func(tx *sql.Tx) error {
		_ = tx.Rollback()
		return rollbackErr
	}
	callbackErr := errors.New("domain failure")
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
		return struct{}{}, callbackErr
	}); !errors.Is(err, callbackErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback and callback errors were not preserved: %v", err)
	}
	if store.writerConn == nil || store.writerConn == oldConn {
		t.Fatal("rollback failure did not replace writer connection")
	}
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('after_rollback_recovery', 'ok')`)
		return struct{}{}, execErr
	}); err != nil {
		t.Fatalf("writer did not recover after rollback failure: %v", err)
	}
}

func TestWriterRollbackAndReplacementFailuresPreserveAllErrors(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	callbackErr := errors.New("callback failure")
	rollbackErr := errors.New("rollback failure")
	replacementErr := errors.New("replacement failure")
	store.writerRollback = func(tx *sql.Tx) error {
		_ = tx.Rollback()
		return rollbackErr
	}
	store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) { return nil, replacementErr }
	_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
		return struct{}{}, callbackErr
	})
	if !errors.Is(err, callbackErr) || !errors.Is(err, rollbackErr) || !errors.Is(err, ErrWriterUnavailable) || !errors.Is(err, replacementErr) {
		t.Fatalf("rollback/replacement errors were not preserved: %v", err)
	}
	store.writeMu.Lock()
	store.writerRollback = nil
	store.writerConnectionAcquirer = store.db.Conn
	store.writeMu.Unlock()
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	}); err != nil {
		t.Fatalf("writer did not recover after replacement failure was removed: %v", err)
	}
}

func TestWriterPanicCleanupPreservesPanicAndLogsRecoveryFailure(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rollbackErr := errors.New("panic rollback failure")
	replacementErr := errors.New("panic replacement failure")
	store.writerRollback = func(tx *sql.Tx) error {
		_ = tx.Rollback()
		return rollbackErr
	}
	store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) { return nil, replacementErr }
	func() {
		defer func() {
			if recovered := recover(); recovered != "original panic" {
				t.Fatalf("panic value changed: %v", recovered)
			}
		}()
		_, _ = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
			panic("original panic")
		})
	}()
	if !strings.Contains(logOutput.String(), `"event":"observability_collection_failed"`) {
		t.Fatalf("panic cleanup failure was not logged: %s", logOutput.String())
	}
	store.writeMu.Lock()
	store.writerRollback = nil
	store.writerConnectionAcquirer = store.db.Conn
	store.writeMu.Unlock()
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	}); err != nil {
		t.Fatalf("writer did not recover after panic cleanup failure was removed: %v", err)
	}
}

func TestWriterCommitRollbackErrorStillReconcilesDurableToken(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	commitErr := errors.New("ambiguous commit")
	rollbackErr := errors.New("rollback completion error")
	store.writerCommit = func(tx *sql.Tx) error {
		if commitCallErr := tx.Commit(); commitCallErr != nil {
			return commitCallErr
		}
		return commitErr
	}
	store.writerRollback = func(tx *sql.Tx) error {
		_ = tx.Rollback()
		return rollbackErr
	}
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('commit_rollback_reconcile', 'ok')`)
		return struct{}{}, execErr
	}); err != nil {
		t.Fatalf("durable commit was not reconciled after rollback completion error: %v", err)
	}
}

func TestWriterCommitRecoveryDoesNotUseCanceledOperationContext(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	commitErr := errors.New("ambiguous commit")
	oldConn := store.writerConn
	var recoveryContextErr error
	store.writerConnectionAcquirer = func(ctx context.Context) (*sql.Conn, error) {
		if ctx.Err() != nil {
			recoveryContextErr = ctx.Err()
		}
		return store.db.Conn(ctx)
	}
	operationCtx, cancel := context.WithCancel(context.Background())
	store.writerCommit = func(*sql.Tx) error {
		cancel()
		return commitErr
	}
	_, err = withWriterTxValue(operationCtx, store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	})
	if err == nil {
		t.Fatal("canceled commit unexpectedly succeeded")
	}
	var outcome *writerCommitOutcomeError
	if !errors.As(err, &outcome) || outcome.outcome != writerCommitOutcomeNotCommitted {
		t.Fatalf("canceled commit did not expose typed not-committed outcome: %T %+v", err, outcome)
	}
	if store.writerConn == nil || store.writerConn == oldConn {
		t.Fatal("canceled commit retained the old writer connection")
	}
	if recoveryContextErr != nil {
		t.Fatalf("writer replacement used canceled operation context: %v", recoveryContextErr)
	}
	if errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatal("absent commit should be recoverable, not permanently unknown")
	}

	store.writeMu.Lock()
	store.writerCommit = nil
	store.writeMu.Unlock()
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('after_canceled_commit', 'ok')`)
		return struct{}{}, execErr
	}); err != nil {
		t.Fatalf("writer did not recover after canceled commit: %v", err)
	}
}

func TestWriterCommitRecoveryCanRetryAfterInitialReplacementFailure(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	commitErr := errors.New("ambiguous commit")
	store.writerCommit = func(*sql.Tx) error { return commitErr }
	var acquireCalls int
	store.writerConnectionAcquirer = func(ctx context.Context) (*sql.Conn, error) {
		acquireCalls++
		if acquireCalls == 1 {
			return nil, errors.New("controlled replacement failure")
		}
		return store.db.Conn(ctx)
	}
	_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(_ *sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	})
	if !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("initial replacement failure was not explicit unknown outcome: %v", err)
	}

	store.writeMu.Lock()
	store.writerCommit = nil
	store.writeMu.Unlock()
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('after_replacement_retry', 'ok')`)
		return struct{}{}, execErr
	}); err != nil {
		t.Fatalf("writer did not recover after replacement retry: %v", err)
	}
	if acquireCalls < 2 {
		t.Fatalf("writer connection was not reacquired on the next mutation: calls=%d", acquireCalls)
	}
}

func TestWriterCommitLookupFailureBlocksMutationUntilLaterReconciliation(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	commitErr := errors.New("ambiguous commit")
	store.writerCommit = func(tx *sql.Tx) error {
		if commitCallErr := tx.Commit(); commitCallErr != nil {
			return commitCallErr
		}
		return commitErr
	}
	lookupErr := errors.New("temporary ledger read failure")
	var lookupCalls int
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, lookupErr
		}
		return true, nil
	}
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('lookup_retry_probe', 'committed')`)
		return struct{}{}, execErr
	}); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("initial ledger failure was not unknown outcome: %v", err)
	}
	var unknown *writerCommitOutcomeError
	if !errors.As(err, &unknown) || unknown.outcome != writerCommitOutcomeUnknown {
		t.Fatalf("unknown commit did not expose typed outcome: %T %+v", err, unknown)
	}

	callbackCalled := false
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
		callbackCalled = true
		return struct{}{}, nil
	}); !errors.Is(err, ErrWriterCommitReconciled) {
		t.Fatalf("later durable reconciliation did not block duplicate mutation: %v", err)
	}
	if callbackCalled {
		t.Fatal("mutation callback ran before unresolved commit was reconciled")
	}

	store.writeMu.Lock()
	store.writerCommit = nil
	store.writerCommitLookup = nil
	store.writeMu.Unlock()
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.Exec(`INSERT INTO metadata(key, value) VALUES('after_lookup_reconcile', 'ok')`)
		return struct{}{}, execErr
	}); err != nil {
		t.Fatalf("writer did not resume after reconciliation: %v", err)
	}
}

func TestRecheckUnknownCommitDoesNotPersistFalseFailedJob(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedQueuedWriterTestJob(t, store)

	commitErr := errors.New("ambiguous commit")
	store.writerCommit = func(tx *sql.Tx) error {
		if commitCallErr := tx.Commit(); commitCallErr != nil {
			return commitCallErr
		}
		return commitErr
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		return false, errors.New("ledger unavailable")
	}
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 10); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("expected unknown commit outcome, got %v", err)
	}

	var status string
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs LIMIT 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status == "failed" {
		t.Fatal("unknown commit outcome was persisted as a failed job")
	}
	if !strings.Contains(logOutput.String(), `"event":"job_commit_outcome_unknown"`) {
		t.Fatalf("missing structured unknown-commit event: %s", logOutput.String())
	}
}

func TestWriterCommitLedgerUsesExplicitHousekeepingRetention(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	oldAt := time.Now().UTC().Add(-writerCommitLedgerRetention - time.Hour).Format(time.RFC3339Nano)
	recentAt := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	if _, err = store.db.Exec(`
INSERT INTO lifecycle_writer_commits(operation_token, operation_kind, committed_at)
VALUES('old-token', 'test', ?), ('recent-token', 'test', ?)
`, oldAt, recentAt); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RunHousekeeping(24*time.Hour, 100000); err != nil {
		t.Fatalf("RunHousekeeping() error: %v", err)
	}
	var oldCount, recentCount int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_token='old-token'`).Scan(&oldCount); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_token='recent-token'`).Scan(&recentCount); err != nil {
		t.Fatal(err)
	}
	if oldCount != 0 || recentCount != 1 {
		t.Fatalf("unexpected writer ledger retention: old=%d recent=%d", oldCount, recentCount)
	}
}

func TestWriterEntrypointSmokeUsesImmediateTransactions(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	alertForWriter := func(id string) AlertCurrent {
		return AlertCurrent{
			AlertID:           id,
			State:             StateInactive,
			Receiver:          "team-a",
			GroupID:           "group-a",
			LabelsJSON:        `{"alertname":"WriterProbe"}`,
			AnnotationsJSON:   `{"summary":"writer probe"}`,
			StartsAt:          now.Add(-time.Hour),
			LastSeenAt:        &now,
			RelevantUpstreams: `[]`,
		}
	}
	type writerCase struct {
		name    string
		prepare func(*Store) error
		run     func(*Store) error
	}
	cases := []writerCase{
		{name: "upsert current alert", run: func(store *Store) error { return store.UpsertCurrentAlert(alertForWriter("writer-upsert")) }},
		{
			name:    "state transition",
			prepare: func(store *Store) error { return store.UpsertCurrentAlert(alertForWriter("writer-transition")) },
			run: func(store *Store) error {
				oldState := StateInactive
				return store.InsertStateTransition(AlertStateTransition{AlertID: "writer-transition", OldState: &oldState, NewState: StateResolved, ChangeSource: ChangeSourceManual})
			},
		},
		{
			name:    "resolve",
			prepare: func(store *Store) error { return store.UpsertCurrentAlert(alertForWriter("writer-resolve")) },
			run: func(store *Store) error {
				_, err := store.ResolveAlert("writer-resolve", nil, nil, now)
				return err
			},
		},
		{
			name:    "recheck batch",
			prepare: func(store *Store) error { seedQueuedWriterTestJob(t, store); return nil },
			run: func(store *Store) error {
				_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if tc.prepare != nil {
				if err = tc.prepare(store); err != nil {
					t.Fatalf("prepare: %v", err)
				}
			}

			started := make(chan struct{})
			proceed := make(chan struct{})
			store.writeMu.Lock()
			store.writerTransactionStarter = &writerStartBarrier{conn: store.writerConn, started: started, proceed: proceed}
			store.writeMu.Unlock()
			done := make(chan error, 1)
			go func() { done <- tc.run(store) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("writer entrypoint did not acquire the transaction barrier")
			}
			select {
			case runErr := <-done:
				t.Fatalf("writer entrypoint completed before barrier release: %v", runErr)
			default:
			}
			close(proceed)
			if runErr := <-done; runErr != nil {
				t.Fatalf("writer entrypoint returned error after barrier release: %v", runErr)
			}
		})
	}
}

func TestFreshBootstrapUsesWriterTransactionBeforeSchemaMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	started := make(chan struct{})
	proceed := make(chan struct{})
	var database *sql.DB
	deps := storeOpenDependencies{
		openDB: func(dsn string) (*sql.DB, error) {
			var err error
			database, err = sql.Open("sqlite", dsn)
			return database, err
		},
		bootstrap: func(store *Store) error {
			store.writerTransactionStarter = &writerStartBarrier{conn: store.writerConn, started: started, proceed: proceed}
			return store.bootstrapSchema()
		},
	}
	result := make(chan struct {
		store *Store
		err   error
	}, 1)
	go func() {
		store, err := openStoreWithDependencies(StoreOptions{Path: path}, deps)
		result <- struct {
			store *Store
			err   error
		}{store: store, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fresh bootstrap did not reach writer barrier")
	}
	var tableCount int
	if err := database.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE name='metadata'`).Scan(&tableCount); err != nil || tableCount != 0 {
		t.Fatalf("schema was mutated before bootstrap transaction release: err=%v tables=%d", err, tableCount)
	}
	close(proceed)
	opened := <-result
	if opened.err != nil {
		t.Fatalf("fresh bootstrap failed: %v", opened.err)
	}
	t.Cleanup(func() { _ = opened.store.Close() })
	var count int
	if err := opened.store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, writerOperationSchemaBootstrap).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("bootstrap commit ledger rows: got=%d want=1", count)
	}
}

func TestPollWriterPhasePersistsLiveTopologyAndCause(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	matcherJSON := `[ {"name":"alertname","value":"WriterProbe","isRegex":false,"isEqual":true} ]`
	result, err := store.ApplyPollCycle(PollCycleInput{
		Timestamp:           now,
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{{SilenceID: "silence-a", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), CreatedBy: "writer", Comment: "writer", Matchers: []SilenceMatcherSnapshot{{Name: "alertname", Value: "WriterProbe", IsEqual: true}}}}}},
		LiveAlerts: []LiveAlertSnapshot{{
			AlertID: "writer-poll-alert", State: StateSuppressed, Receiver: "team-a", GroupID: "group-a", GroupLabelsJSON: `{}`, LabelsJSON: `{"alertname":"WriterProbe"}`, MatchLabelsJSON: &matcherJSON, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-a"}}, SourceLinks: []AlertSourceLink{{Name: "am-a", Cluster: "cluster-a", URL: "http://prometheus/query"}}, Targets: []LiveTargetSnapshot{{Target: "cluster-a", State: StateSuppressed, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-a"}}}, SilencedBySources: []SourceSilenceRef{{SourceName: "am-a", ClusterID: "cluster-a", SilenceIDs: []string{"silence-a"}}},
		}},
	})
	if err != nil {
		t.Fatalf("poll phase: %v", err)
	}
	if result.Inserted != 1 {
		t.Fatalf("poll inserted alerts: got=%d want=1", result.Inserted)
	}
	var current, targets, events, sources, causes, queued, ledger int
	queries := []struct {
		name  string
		query string
		out   *int
	}{
		{"current", `SELECT COUNT(*) FROM alert_states WHERE alert_id='writer-poll-alert'`, &current},
		{"targets", `SELECT COUNT(*) FROM alert_target_states WHERE alert_id='writer-poll-alert'`, &targets},
		{"events", `SELECT COUNT(*) FROM alert_events WHERE alert_id='writer-poll-alert'`, &events},
		{"sources", `SELECT COUNT(*) FROM alertmanager_source_silences WHERE source_name='am-a'`, &sources},
		{"causes", `SELECT COUNT(*) FROM alert_suppression_cause_history WHERE alert_id='writer-poll-alert'`, &causes},
		{"queued", `SELECT COUNT(*) FROM silence_recheck_jobs`, &queued},
		{"ledger", `SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, &ledger},
	}
	for _, query := range queries {
		var scanErr error
		if query.name == "ledger" {
			scanErr = store.db.QueryRow(query.query, writerOperationPoll).Scan(query.out)
		} else {
			scanErr = store.db.QueryRow(query.query).Scan(query.out)
		}
		if scanErr != nil {
			t.Fatalf("%s query: %v", query.name, scanErr)
		}
	}
	if current != 1 || targets != 1 || events == 0 || sources != 1 || causes != 1 || ledger != 1 || queued == 0 {
		t.Fatalf("poll phase persisted incomplete aggregate: current=%d targets=%d events=%d sources=%d causes=%d queued=%d ledger=%d", current, targets, events, sources, causes, queued, ledger)
	}
}

func TestRecheckFailurePersistenceIsSeparateWriterPhase(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedQueuedWriterTestJob(t, store)
	if _, err = store.db.Exec(`UPDATE silence_recheck_jobs SET source_context_json='not-json' WHERE job_id=(SELECT MAX(job_id) FROM silence_recheck_jobs)`); err != nil {
		t.Fatal(err)
	}
	starts := []chan struct{}{make(chan struct{}), make(chan struct{})}
	proceeds := []chan struct{}{make(chan struct{}), make(chan struct{})}
	finished := make(chan string, 2)
	store.writerTransactionFinished = func(operationKind string) { finished <- operationKind }
	store.writerTransactionStarter = &writerStartSequence{conn: store.writerConn, starts: starts, proceeds: []<-chan struct{}{proceeds[0], proceeds[1]}}
	workerDone := make(chan error, 1)
	go func() {
		_, runErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
		workerDone <- runErr
	}()
	select {
	case <-starts[0]:
	case <-time.After(time.Second):
		t.Fatal("recheck batch did not reach first writer phase")
	}
	close(proceeds[0])
	select {
	case operationKind := <-finished:
		if operationKind != writerOperationRecheckBatch {
			t.Fatalf("first transaction finished as %q", operationKind)
		}
	case <-time.After(time.Second):
		t.Fatal("recheck batch did not finish")
	}
	select {
	case <-starts[1]:
	case <-time.After(time.Second):
		t.Fatal("recheck failure persistence did not reach second writer phase")
	}
	var status string
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs LIMIT 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status == "failed" {
		t.Fatal("failed status was persisted before recheck_failure barrier release")
	}
	close(proceeds[1])
	select {
	case operationKind := <-finished:
		if operationKind != writerOperationRecheckFailure {
			t.Fatalf("failure persistence finished as %q", operationKind)
		}
	case <-time.After(time.Second):
		t.Fatal("recheck failure persistence did not finish")
	}
	if runErr := <-workerDone; runErr == nil {
		t.Fatal("malformed recheck input unexpectedly succeeded")
	}
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs LIMIT 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("recheck failure status: got=%q want failed", status)
	}
	var ledgerCount int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, writerOperationRecheckFailure).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 1 {
		t.Fatalf("recheck failure ledger rows: got=%d want=1", ledgerCount)
	}
}

func TestRecheckCancellationBeforeLedgerInsertDoesNotCommit(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedQueuedWriterTestJob(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	store.beforeCommitLedgerInsert = func(_ context.Context, operationKind string) {
		if operationKind == writerOperationRecheckBatch {
			cancel()
		}
	}
	_, runErr := store.RunSilenceRecheckBatchContext(ctx, 1000)
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("recheck cancellation error: %v", runErr)
	}
	var status string
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs LIMIT 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("job status after pre-ledger cancellation: got=%q want queued", status)
	}
	var ledgerCount int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, writerOperationRecheckBatch).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 0 {
		t.Fatalf("recheck ledger rows after cancellation: got=%d want=0", ledgerCount)
	}
}

func TestRecheckDelayedCommitReplaysCommittedEventsExactlyOnce(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedWriterRecheckAggregate(t, store)
	commitErr := errors.New("ambiguous recheck commit")
	store.writerCommit = func(tx *sql.Tx) error {
		if commitCallErr := tx.Commit(); commitCallErr != nil {
			return commitCallErr
		}
		return commitErr
	}
	lookupCalls := 0
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, errors.New("delayed ledger lookup")
		}
		return true, nil
	}
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("first recheck outcome: %v", err)
	}
	result, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if err != nil || result.Disposition != RecheckBatchCommitted || result.Status != "completed" {
		t.Fatalf("delayed recheck result=%+v err=%v", result, err)
	}
	logged := logOutput.String()
	for _, event := range []string{"job_commit_reconciled", "job_started", "job_completed"} {
		if count := strings.Count(logged, `"event":"`+event+`"`); count != 1 {
			t.Fatalf("event %s count: got=%d log=%s", event, count, logged)
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(logged), "\n") {
		var record map[string]any
		if err = json.Unmarshal([]byte(line), &record); err != nil || record["event"] != "job_commit_reconciled" {
			continue
		}
		if record["level"] != "INFO" || record["writer_operation"] != writerOperationRecheckBatch || record["duration"] == nil {
			t.Fatalf("job_commit_reconciled fields: %v", record)
		}
	}
	if strings.Count(logged, `"event":"job_completed"`) > 1 {
		t.Fatalf("job_completed was replayed more than once: %s", logged)
	}
	if strings.Contains(logged, `"event":"job_failed"`) {
		t.Fatalf("delayed committed recheck emitted job_failed: %s", logged)
	}
	store.writeMu.Lock()
	store.writerCommit = nil
	store.writerCommitLookup = nil
	store.writeMu.Unlock()
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatalf("explicit retry after delayed reconciliation: %v", err)
	}
	if count := strings.Count(logOutput.String(), `"event":"job_started"`); count != 1 {
		t.Fatalf("reconciliation finalizer replayed after explicit retry: job_started=%d", count)
	}
}

func TestHousekeepingCleanupAndVacuumUseSeparateWriterTransactions(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	old := now.Add(-48 * time.Hour)
	if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "expired-writer-alert", State: StateInactive, RawState: StateActive, Receiver: "team-a", GroupID: "group-a", LabelsJSON: `{"alertname":"Expired"}`, AnnotationsJSON: `{}`, StartsAt: old, LastSeenAt: &old, RelevantUpstreams: `[]`}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`INSERT INTO lifecycle_writer_commits(operation_token,operation_kind,committed_at) VALUES('old-housekeeping-token',?,?)`, writerOperationPoll, old.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	starts := []chan struct{}{make(chan struct{}), make(chan struct{})}
	proceeds := []chan struct{}{make(chan struct{}), make(chan struct{})}
	store.writeMu.Lock()
	store.writerTransactionStarter = &writerStartSequence{conn: store.writerConn, starts: starts, proceeds: []<-chan struct{}{proceeds[0], proceeds[1]}}
	store.writeMu.Unlock()
	housekeepingDone := make(chan error, 1)
	go func() {
		_, runErr := store.RunHousekeepingContext(context.Background(), 24*time.Hour, 100)
		housekeepingDone <- runErr
	}()
	select {
	case <-starts[0]:
	case <-time.After(time.Second):
		t.Fatal("housekeeping cleanup did not reach writer transaction")
	}
	var currentCount int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_states WHERE alert_id='expired-writer-alert'`).Scan(&currentCount); err != nil {
		t.Fatal(err)
	}
	if currentCount != 1 {
		t.Fatalf("cleanup mutation occurred before writer release: count=%d", currentCount)
	}
	close(proceeds[0])
	select {
	case <-starts[1]:
	case <-time.After(time.Second):
		t.Fatal("housekeeping vacuum did not reach its separate writer transaction")
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_states WHERE alert_id='expired-writer-alert'`).Scan(&currentCount); err != nil {
		t.Fatal(err)
	}
	if currentCount != 0 {
		t.Fatalf("cleanup did not delete expired alert before vacuum: count=%d", currentCount)
	}
	close(proceeds[1])
	if err = <-housekeepingDone; err != nil {
		t.Fatalf("housekeeping failed: %v", err)
	}
	for _, kind := range []string{writerOperationHousekeeping, writerOperationVacuum} {
		var count int
		if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, kind).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%s ledger rows: got=%d want=1", kind, count)
		}
	}
}

func TestRegroupBeginBatchAndCompletionUseRealWriterPhases(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "regroup-writer-alert", State: StateInactive, RawState: StateActive, Receiver: "team-a", GroupID: "old-group", LabelsJSON: `{"alertname":"Regroup"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `[]`}); err != nil {
		t.Fatal(err)
	}
	starts := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	proceeds := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	store.writeMu.Lock()
	store.writerTransactionStarter = &writerStartSequence{conn: store.writerConn, starts: starts, proceeds: []<-chan struct{}{proceeds[0], proceeds[1], proceeds[2]}}
	store.writeMu.Unlock()
	regroupDone := make(chan error, 1)
	go func() {
		_, runErr := store.RunRegroupToConfig(context.Background(), GroupingConfig{LabelsKeep: []string{"alertname"}}, 1)
		regroupDone <- runErr
	}()
	select {
	case <-starts[0]:
	case <-time.After(time.Second):
		t.Fatal("regroup begin did not reach writer transaction")
	}
	var status string
	if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaRegroupStatus).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != regroupStatusReady {
		t.Fatalf("regroup began before writer release: status=%q", status)
	}
	close(proceeds[0])
	select {
	case <-starts[1]:
	case <-time.After(time.Second):
		t.Fatal("regroup batch did not reach writer transaction")
	}
	if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaRegroupStatus).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != regroupStatusRunning {
		t.Fatalf("regroup batch changed status before commit: status=%q", status)
	}
	close(proceeds[1])
	select {
	case <-starts[2]:
	case <-time.After(time.Second):
		t.Fatal("regroup completion did not reach its separate batch transaction")
	}
	var groupID string
	if err = store.db.QueryRow(`SELECT group_id FROM alert_states WHERE alert_id='regroup-writer-alert'`).Scan(&groupID); err != nil {
		t.Fatal(err)
	}
	if groupID == "old-group" {
		t.Fatal("regroup mutation was not committed before completion phase")
	}
	close(proceeds[2])
	if err = <-regroupDone; err != nil {
		t.Fatalf("regroup failed: %v", err)
	}
	if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaRegroupStatus).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != regroupStatusReady {
		t.Fatalf("regroup did not complete: status=%q", status)
	}
	var ledgerCount int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind IN (?, ?)`, writerOperationRegroupBegin, writerOperationRegroupBatch).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 3 {
		t.Fatalf("regroup writer ledger rows: got=%d want=3", ledgerCount)
	}
}

func TestRegroupResumesAfterDelayedBeginCommitReconciliation(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "delayed-regroup-begin", State: StateInactive, RawState: StateActive, Receiver: "team-a", GroupID: "old", LabelsJSON: `{"alertname":"Delayed"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `[]`}); err != nil {
		t.Fatal(err)
	}
	commitErr := errors.New("delayed begin commit lookup")
	commitCalls := 0
	lookupCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if commitCallErr := tx.Commit(); commitCallErr != nil {
			return commitCallErr
		}
		if commitCalls == 1 {
			return commitErr
		}
		return nil
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, errors.New("delayed ledger lookup")
		}
		return true, nil
	}
	result, err := store.RunRegroupToConfig(context.Background(), GroupingConfig{LabelsKeep: []string{"alertname"}}, 1)
	if err != nil {
		t.Fatalf("regroup did not resume after delayed begin commit: %v", err)
	}
	if result.Processed != 1 {
		t.Fatalf("regroup processed count: got=%d want=1", result.Processed)
	}
	if got := strings.Count(logOutput.String(), `"event":"regroup_started"`); got != 1 {
		t.Fatalf("regroup_started events: got=%d want=1 logs=%s", got, logOutput.String())
	}
	if strings.Contains(logOutput.String(), `"event":"regroup_failed"`) {
		t.Fatalf("delayed commit was reported as regroup failure: %s", logOutput.String())
	}
}

func TestRegroupResumesAfterDelayedBatchCommitReconciliation(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "delayed-regroup-batch", State: StateInactive, RawState: StateActive, Receiver: "team-a", GroupID: "old", LabelsJSON: `{"alertname":"Delayed"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `[]`}); err != nil {
		t.Fatal(err)
	}
	commitErr := errors.New("delayed batch commit lookup")
	commitCalls := 0
	lookupCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if commitCallErr := tx.Commit(); commitCallErr != nil {
			return commitCallErr
		}
		if commitCalls == 2 {
			return commitErr
		}
		return nil
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, errors.New("delayed ledger lookup")
		}
		return true, nil
	}
	result, err := store.RunRegroupToConfig(context.Background(), GroupingConfig{LabelsKeep: []string{"alertname"}}, 1)
	if err != nil {
		t.Fatalf("regroup did not resume after delayed batch commit: %v", err)
	}
	if result.Processed != 1 {
		t.Fatalf("regroup processed count: got=%d want=1", result.Processed)
	}
	if got := strings.Count(logOutput.String(), `"event":"regroup_batch_completed"`); got != 2 {
		t.Fatalf("regroup_batch_completed events: got=%d want=2 logs=%s", got, logOutput.String())
	}
	if strings.Contains(logOutput.String(), `"event":"regroup_failed"`) {
		t.Fatalf("delayed commit was reported as regroup failure: %s", logOutput.String())
	}
}

func TestPollEventsAreOwnedByTheirTransactionWhenTimestampsMatch(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	first := LiveAlertSnapshot{AlertID: "poll-owner-a", State: StateActive, Receiver: "team-a", GroupID: "group-a", LabelsJSON: `{"alertname":"A"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a"}}
	if _, err = store.ApplyPollCycle(PollCycleInput{LiveAlerts: []LiveAlertSnapshot{first}, SuccessfulUpstreams: map[string]bool{"am-a": true}, Timestamp: now}); err != nil {
		t.Fatalf("first poll failed: %v", err)
	}
	second := LiveAlertSnapshot{AlertID: "poll-owner-b", State: StateActive, Receiver: "team-a", GroupID: "group-a", LabelsJSON: `{"alertname":"B"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a"}}
	if _, err = store.ApplyPollCycle(PollCycleInput{LiveAlerts: []LiveAlertSnapshot{second}, SuccessfulUpstreams: map[string]bool{"am-a": true}, Timestamp: now}); err != nil {
		t.Fatalf("second poll failed: %v", err)
	}
	logText := logOutput.String()
	if got := strings.Count(logText, `"event":"alert_transition"`); got != 3 {
		t.Fatalf("transaction-owned transition count: got=%d want=3 logs=%s", got, logText)
	}
	if got := strings.Count(logText, `"event":"alert_transition","alert_id":"poll-owner-a"`); got != 2 {
		t.Fatalf("first poll transition replayed or lost: got=%d want=2 logs=%s", got, logText)
	}
	if got := strings.Count(logText, `"event":"alert_transition","alert_id":"poll-owner-b"`); got != 1 {
		t.Fatalf("second poll transition count: got=%d want=1 logs=%s", got, logText)
	}
}

func TestDelayedPollFinalizerKeepsEventsWithOriginalTransaction(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, time.January, 2, 12, 0, 0, 0, time.UTC)
	commitCalls := 0
	lookupCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if commitErr := tx.Commit(); commitErr != nil {
			return commitErr
		}
		if commitCalls == 1 {
			return errors.New("delayed poll commit lookup")
		}
		return nil
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, errors.New("delayed poll ledger lookup")
		}
		return true, nil
	}
	first := LiveAlertSnapshot{AlertID: "delayed-poll-a", State: StateActive, Receiver: "team-a", GroupID: "group-a", LabelsJSON: `{"alertname":"A"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a"}}
	second := LiveAlertSnapshot{AlertID: "delayed-poll-b", State: StateActive, Receiver: "team-a", GroupID: "group-b", LabelsJSON: `{"alertname":"B"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a"}}
	if _, err = store.ApplyPollCycle(PollCycleInput{LiveAlerts: []LiveAlertSnapshot{first}, SuccessfulUpstreams: map[string]bool{"am-a": true}, Timestamp: now}); !errors.Is(err, ErrWriterCommitOutcomeUnknown) {
		t.Fatalf("first poll error: got=%v want commit-unknown", err)
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{LiveAlerts: []LiveAlertSnapshot{second}, SuccessfulUpstreams: map[string]bool{"am-a": true}, Timestamp: now}); !errors.Is(err, ErrWriterCommitReconciled) {
		t.Fatalf("reconciliation poll error: got=%v want commit-reconciled", err)
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{LiveAlerts: []LiveAlertSnapshot{first, second}, SuccessfulUpstreams: map[string]bool{"am-a": true}, Timestamp: now}); err != nil {
		t.Fatalf("post-reconciliation poll failed: %v", err)
	}
	logText := logOutput.String()
	if got := strings.Count(logText, `"event":"alert_transition"`); got != 2 {
		t.Fatalf("delayed poll transition count: got=%d want=2 logs=%s", got, logText)
	}
	if strings.Count(logText, `"event":"alert_transition","alert_id":"delayed-poll-a"`) != 1 || strings.Count(logText, `"event":"alert_transition","alert_id":"delayed-poll-b"`) != 1 {
		t.Fatalf("delayed poll events were attributed or replayed incorrectly: %s", logText)
	}
}

func TestPollEventPayloadOrderingIsIndependentOfInputOrder(t *testing.T) {
	previousLogger := slog.Default()
	var logOutput bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	collect := func(order []string) []string {
		logOutput.Reset()
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		alerts := map[string]LiveAlertSnapshot{
			"ordered-a": {AlertID: "ordered-a", State: StateActive, Receiver: "team-a", GroupID: "group-a", LabelsJSON: `{"alertname":"A"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a"}},
			"ordered-b": {AlertID: "ordered-b", State: StateActive, Receiver: "team-a", GroupID: "group-b", LabelsJSON: `{"alertname":"B"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a"}},
		}
		live := []LiveAlertSnapshot{alerts[order[0]], alerts[order[1]]}
		if _, err = store.ApplyPollCycle(PollCycleInput{LiveAlerts: live, SuccessfulUpstreams: map[string]bool{"am-a": true}, Timestamp: now}); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		_ = store.Close()
		var normalized []string
		for _, line := range strings.Split(strings.TrimSpace(logOutput.String()), "\n") {
			var record map[string]any
			if err = json.Unmarshal([]byte(line), &record); err != nil || record["event"] == nil {
				continue
			}
			delete(record, "time")
			delete(record, "duration")
			encoded, marshalErr := json.Marshal(record)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			normalized = append(normalized, string(encoded))
		}
		return normalized
	}
	first := collect([]string{"ordered-a", "ordered-b"})
	second := collect([]string{"ordered-b", "ordered-a"})
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("poll event payload order depends on input order:\nfirst=%v\nsecond=%v", first, second)
	}
}

func TestResolveWriterPhaseUpdatesGlobalAndAllTargetRows(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "resolve-writer-alert", State: StateInactive, RawState: StateActive, Receiver: "team-a", GroupID: "group-a", LabelsJSON: `{"alertname":"Resolve"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `[]`}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`INSERT INTO alert_target_states(alert_id,target,state,raw_state,is_live,is_suppressed,is_resolved,starts_at,last_seen_at,source_members_json,match_labels_fingerprint,retired_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, "resolve-writer-alert", "cluster-a", "inactive", "active", 0, 0, 0, now.Add(-time.Hour).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), `[]`, "", nil, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	proceed := make(chan struct{})
	store.writeMu.Lock()
	store.writerTransactionStarter = &writerStartBarrier{conn: store.writerConn, started: started, proceed: proceed}
	store.writeMu.Unlock()
	resolveDone := make(chan error, 1)
	go func() {
		_, resolveErr := store.ResolveAlertContext(context.Background(), "resolve-writer-alert", nil, nil, now)
		resolveDone <- resolveErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("resolve did not reach writer transaction")
	}
	var globalResolved, targetResolved int
	if err = store.db.QueryRow(`SELECT is_resolved FROM alert_states WHERE alert_id=?`, "resolve-writer-alert").Scan(&globalResolved); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT is_resolved FROM alert_target_states WHERE alert_id=? AND target=?`, "resolve-writer-alert", "cluster-a").Scan(&targetResolved); err != nil {
		t.Fatal(err)
	}
	if globalResolved != 0 || targetResolved != 0 {
		t.Fatalf("resolve mutated state before writer release: global=%d target=%d", globalResolved, targetResolved)
	}
	close(proceed)
	if err = <-resolveDone; err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if err = store.db.QueryRow(`SELECT is_resolved FROM alert_states WHERE alert_id=?`, "resolve-writer-alert").Scan(&globalResolved); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT is_resolved FROM alert_target_states WHERE alert_id=? AND target=?`, "resolve-writer-alert", "cluster-a").Scan(&targetResolved); err != nil {
		t.Fatal(err)
	}
	if globalResolved != 1 || targetResolved != 1 {
		t.Fatalf("resolve did not update all projections: global=%d target=%d", globalResolved, targetResolved)
	}
	var targetEvents int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_target_events WHERE alert_id=? AND target=?`, "resolve-writer-alert", "cluster-a").Scan(&targetEvents); err != nil {
		t.Fatal(err)
	}
	if targetEvents != 1 {
		t.Fatalf("target resolve events: got=%d want=1", targetEvents)
	}
	var ledgerCount int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, writerOperationResolve).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 1 {
		t.Fatalf("resolve ledger rows: got=%d want=1", ledgerCount)
	}
}

func TestRegroupResumePreservesCursorAndProgress(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	target := BuildGroupingFingerprint(GroupingConfig{LabelsKeep: []string{"alertname"}})
	for key, value := range map[string]string{
		metaRegroupStatus:            regroupStatusRunning,
		metaRegroupTargetFingerprint: target,
		metaRegroupProcessed:         "7",
		metaRegroupCursorRowID:       "42",
		metaRegroupTotal:             "9",
	} {
		if _, err = store.db.Exec(`INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{})
	proceed := make(chan struct{})
	store.writeMu.Lock()
	store.writerTransactionStarter = &writerStartBarrier{conn: store.writerConn, started: started, proceed: proceed}
	store.writeMu.Unlock()
	result := make(chan struct {
		started bool
		resumed bool
		total   int
		err     error
	}, 1)
	go func() {
		started, resumed, total, beginErr := store.beginOrResumeRegroup(context.Background(), target)
		result <- struct {
			started bool
			resumed bool
			total   int
			err     error
		}{started: started, resumed: resumed, total: total, err: beginErr}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("regroup resume did not reach writer transaction")
	}
	var processed, cursor int
	if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaRegroupProcessed).Scan(&processed); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaRegroupCursorRowID).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if processed != 7 || cursor != 42 {
		t.Fatalf("regroup progress changed before writer release: processed=%d cursor=%d", processed, cursor)
	}
	close(proceed)
	res := <-result
	if res.err != nil || !res.started || !res.resumed {
		t.Fatalf("regroup resume result: %+v", res)
	}
	if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaRegroupProcessed).Scan(&processed); err != nil {
		t.Fatal(err)
	}
	if processed != 7 {
		t.Fatalf("regroup resume reset progress: got=%d want=7", processed)
	}
}

func TestRegroupFailureWritesErrorMetadataThroughWriter(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	target := BuildGroupingFingerprint(GroupingConfig{LabelsKeep: []string{"alertname"}})
	for key, value := range map[string]string{metaRegroupStatus: regroupStatusRunning, metaRegroupTargetFingerprint: target} {
		if _, err = store.db.Exec(`INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{})
	proceed := make(chan struct{})
	store.writeMu.Lock()
	store.writerTransactionStarter = &writerStartBarrier{conn: store.writerConn, started: started, proceed: proceed}
	store.writeMu.Unlock()
	failureDone := make(chan error, 1)
	go func() { failureDone <- store.failRegroup(context.Background(), target, "controlled regroup failure") }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("regroup failure did not reach writer transaction")
	}
	var status string
	if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaRegroupStatus).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != regroupStatusRunning {
		t.Fatalf("regroup failure mutated metadata before writer release: %q", status)
	}
	close(proceed)
	if err = <-failureDone; err != nil {
		t.Fatalf("regroup failure write failed: %v", err)
	}
	var failure string
	if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaRegroupStatus).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaRegroupError).Scan(&failure); err != nil {
		t.Fatal(err)
	}
	if status != regroupStatusError || failure != "controlled regroup failure" {
		t.Fatalf("regroup failure metadata: status=%q error=%q", status, failure)
	}
	var ledgerCount int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, writerOperationRegroupFailure).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 1 {
		t.Fatalf("regroup failure ledger rows: got=%d want=1", ledgerCount)
	}
}

func TestWriterRollbackIsOwnedByHelperExactlyOnce(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	var rollbackCalls int
	store.writerRollback = func(tx *sql.Tx) error {
		rollbackCalls++
		return tx.Rollback()
	}
	expectedErr := errors.New("callback failure")
	if runErr := store.withWriterTx(context.Background(), writerOperationPoll, func(*sql.Tx) error {
		return expectedErr
	}); !errors.Is(runErr, expectedErr) {
		t.Fatalf("unexpected callback error: %v", runErr)
	}
	if rollbackCalls != 1 {
		t.Fatalf("rollback count: got=%d want=1", rollbackCalls)
	}
}

func TestCloseWaitsForWriterAndRejectsLaterMutation(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	workerDone := make(chan error, 1)
	hooks := &storeLifecycleTestHooks{
		closeEntered:    make(chan struct{}),
		writerFinishing: make(chan struct{}),
		closingStateSet: make(chan struct{}),
	}
	store.writeMu.Lock()
	store.lifecycleTestHooks = hooks
	store.writeMu.Unlock()
	go func() {
		_, runErr := withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
			close(started)
			<-release
			return struct{}{}, nil
		})
		workerDone <- runErr
	}()
	<-started

	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	select {
	case <-hooks.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("Close did not reach the writer lock boundary")
	}
	close(release)
	select {
	case <-hooks.writerFinishing:
	case <-time.After(time.Second):
		t.Fatal("writer did not reach its finishing boundary")
	}
	select {
	case <-hooks.closingStateSet:
	case <-time.After(time.Second):
		t.Fatal("Close did not set the closing state")
	}
	if err = <-workerDone; err != nil {
		t.Fatalf("writer failed before Close: %v", err)
	}
	if err = <-closeDone; err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("repeated Close() error: %v", err)
	}
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
		return struct{}{}, nil
	}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("writer after Close() error: got=%v want ErrStoreClosed", err)
	}
}

func TestStoreWorkersCancellationAtWriterBoundaryDoesNotPersistFailure(t *testing.T) {
	t.Run("regroup", func(t *testing.T) {
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		now := time.Now().UTC()
		if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "cancel-regroup", State: StateInactive, RawState: StateActive, Receiver: "team-a", GroupID: "group-a", LabelsJSON: `{"alertname":"Cancel"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `[]`}); err != nil {
			t.Fatal(err)
		}
		started := make(chan struct{})
		proceed := make(chan struct{})
		store.writeMu.Lock()
		store.writerTransactionStarter = &writerStartSequence{conn: store.writerConn, starts: []chan struct{}{started}, proceeds: []<-chan struct{}{proceed}}
		store.writeMu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, runErr := store.RunRegroupToConfig(ctx, GroupingConfig{LabelsKeep: []string{"alertname"}}, 1)
			result <- runErr
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("regroup did not reach writer boundary")
		}
		cancel()
		close(proceed)
		select {
		case runErr := <-result:
			if !errors.Is(runErr, context.Canceled) {
				t.Fatalf("regroup cancellation error: %v", runErr)
			}
		case <-time.After(time.Second):
			t.Fatal("regroup cancellation did not finish")
		}
		var status string
		if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaRegroupStatus).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == regroupStatusError {
			t.Fatal("regroup cancellation persisted failure metadata")
		}
	})

	t.Run("housekeeping", func(t *testing.T) {
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		started := make(chan struct{})
		proceed := make(chan struct{})
		store.writeMu.Lock()
		store.writerTransactionStarter = &writerStartSequence{conn: store.writerConn, starts: []chan struct{}{started}, proceeds: []<-chan struct{}{proceed}}
		store.writeMu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, runErr := store.RunHousekeepingContext(ctx, time.Hour, 100)
			result <- runErr
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("housekeeping did not reach writer boundary")
		}
		cancel()
		close(proceed)
		select {
		case runErr := <-result:
			if !errors.Is(runErr, context.Canceled) {
				t.Fatalf("housekeeping cancellation error: %v", runErr)
			}
		case <-time.After(time.Second):
			t.Fatal("housekeeping cancellation did not finish")
		}
	})
}

func TestStoreCloseClosesWriterBeforeReadPool(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	var writerClosed, poolClosed bool
	store.writerClose = func(conn *sql.Conn) error {
		if poolClosed {
			t.Fatal("writer closed after read pool")
		}
		writerClosed = true
		return conn.Close()
	}
	store.dbClose = func(db *sql.DB) error {
		if !writerClosed {
			t.Fatal("read pool closed before writer")
		}
		poolClosed = true
		return db.Close()
	}
	if err = store.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if !writerClosed || !poolClosed {
		t.Fatalf("close hooks were not both called: writer=%t pool=%t", writerClosed, poolClosed)
	}
}

func TestStoreClosePreservesWriterAndPoolErrors(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	writerErr := errors.New("writer close failure")
	poolErr := errors.New("pool close failure")
	store.writerClose = func(conn *sql.Conn) error {
		_ = conn.Close()
		return writerErr
	}
	store.dbClose = func(db *sql.DB) error {
		_ = db.Close()
		return poolErr
	}
	if err = store.Close(); !errors.Is(err, writerErr) || !errors.Is(err, poolErr) {
		t.Fatalf("Close() did not preserve both close errors: %v", err)
	}
}

func TestProductionWriterCallbacksDoNotOwnTransactionCompletion(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() failed")
	}
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, filepath.Join(filepath.Dir(testFile), "store.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	helperNames := map[string]bool{
		"withWriterTx":                 true,
		"withWriterTxValue":            true,
		"withOwnedWriterTxValue":       true,
		"withWriterTxTupleAfterCommit": true,
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		var helperName string
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			helperName = fn.Name
		case *ast.SelectorExpr:
			helperName = fn.Sel.Name
		}
		if !helperNames[helperName] {
			return true
		}
		for _, arg := range call.Args {
			literal, ok := arg.(*ast.FuncLit)
			if !ok {
				continue
			}
			ast.Inspect(literal.Body, func(inner ast.Node) bool {
				selector, ok := inner.(*ast.SelectorExpr)
				if !ok || (selector.Sel.Name != "Commit" && selector.Sel.Name != "Rollback") {
					return true
				}
				if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "tx" {
					t.Fatalf("writer callback passed to %s calls tx.%s", helperName, selector.Sel.Name)
				}
				return true
			})
		}
		return true
	})
}
