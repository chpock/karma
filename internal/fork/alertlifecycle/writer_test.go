package alertlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
	moderncsqlite "modernc.org/sqlite"
)

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
  context_fingerprint, open_context_fingerprint, source_context_json,
  trigger_type, status, created_at
) VALUES('writer-test', 'writer-test', '{}', 'source_changed', 'queued', ?)
`, createdAt); err != nil {
		t.Fatalf("failed to seed recheck job: %v", err)
	}
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
}

func TestRecheckWriterAcquisitionFailureLeavesQueuedJobUntouched(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedQueuedWriterTestJob(t, store)

	started := make(chan struct{})
	store.writerTransactionStarter = &writerStartFailure{
		started: started,
		err:     errors.New("controlled writer acquisition failure"),
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
	if runErr := <-workerDone; runErr == nil {
		t.Fatal("writer acquisition failure was not returned")
	}

	var status string
	var lastError sql.NullString
	if err = store.db.QueryRow(`SELECT status, last_error FROM silence_recheck_jobs WHERE job_id=(SELECT MAX(job_id) FROM silence_recheck_jobs)`).Scan(&status, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != "queued" || lastError.Valid {
		t.Fatalf("writer acquisition failure changed queued job: status=%q lastError=%v", status, lastError)
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
	if runErr := store.withWriterTx(context.Background(), func(tx *sql.Tx) error {
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

	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("withWriterTx() did not propagate callback panic")
			}
		}()
		_ = store.withWriterTx(context.Background(), func(tx *sql.Tx) error {
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
}
