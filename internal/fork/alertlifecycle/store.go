package alertlifecycle

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	runtimeLog "github.com/prymitive/karma/internal/log"
	"github.com/prymitive/karma/internal/models"
	"github.com/prymitive/karma/internal/regex"
	"github.com/prymitive/karma/internal/transform"

	_ "modernc.org/sqlite" // Register the SQLite database/sql driver.
)

const schemaVersion = "1"

const (
	metaSchemaVersion            = "schema_version"
	metaSchemaLayoutFingerprint  = "schema_layout_fingerprint"
	metaLastHousekeepingAt       = "last_housekeeping_at"
	metaLastIncrementalVacuumAt  = "last_incremental_vacuum_at"
	metaGroupingFingerprint      = "grouping_fingerprint"
	metaRegroupStatus            = "regroup_status"
	metaRegroupError             = "regroup_error"
	metaRegroupProcessed         = "regroup_processed"
	metaRegroupTotal             = "regroup_total"
	metaRegroupStartedAt         = "regroup_started_at"
	metaRegroupFinishedAt        = "regroup_finished_at"
	metaRegroupTargetFingerprint = "regroup_target_fingerprint"
	metaRegroupCursorRowID       = "regroup_cursor_rowid"
	metaRecheckSourceContextGen  = "recheck_source_context_generation"
	metaSuggestionRevisionPrefix = "suggestion_revision/"
)

const expectedSchemaLayoutFingerprint = "sha256:08a58c07d0b27a1adcf69f0d6fc435012363a26bcea7099ed6e7b3963cacd732"

var (
	ErrStoreClosing               = errors.New("lifecycle store is closing")
	ErrStoreClosed                = errors.New("lifecycle store is closed")
	ErrWriterUnavailable          = errors.New("lifecycle writer is unavailable")
	ErrWriterAcquisition          = errors.New("lifecycle writer transaction acquisition failed")
	ErrWriterOperationToken       = errors.New("lifecycle writer operation token generation failed")
	ErrWriterCommitOutcomeUnknown = errors.New("lifecycle writer commit outcome is unknown")
	ErrWriterCommitReconciled     = errors.New("lifecycle writer commit was reconciled")
	ErrWriterOperationInvariant   = errors.New("lifecycle writer operation invariant failed")
)

const reasonCodeMaxLength = 64

const defaultSilenceHistoryRetentionTTL = 30 * 24 * time.Hour

const (
	regroupStatusReady   = "ready"
	regroupStatusRunning = "running"
	regroupStatusError   = "error"
)

type Store struct {
	db                         *sql.DB
	writerConn                 *sql.Conn
	writerTransactionStarter   writerTransactionStarter
	writerConnectionAcquirer   func(context.Context) (*sql.Conn, error)
	writerCommit               func(*sql.Tx) error
	writerRollback             func(*sql.Tx) error
	writerCommitLookup         func(context.Context, string, string) (bool, error)
	recheckAttemptLookup       func(context.Context, string) (int64, int, string, error)
	writerOperationToken       func() (string, error)
	writerTransactionFinished  func(string)
	beforeCommitLedgerInsert   func(context.Context, string)
	lifecycleTestHooks         *storeLifecycleTestHooks
	writerClose                func(*sql.Conn) error
	dbClose                    func(*sql.DB) error
	path                       string
	silenceHistoryRetentionTTL time.Duration
	writeMu                    sync.Mutex
	recheckCommitWake          func()
	recheckReconciledWake      func()
	recheckStateMu             sync.Mutex
	pendingRecheckFailure      *recheckPendingFailure
	pendingRecheckOperator     *recheckOperatorFinalization
	lifecycleState             lifecycleState
	writerRecoveryState        writerRecoveryState
	pendingWriterCommit        *pendingWriterCommit
}

type lifecycleState uint8

type storeLifecycleTestHooks struct {
	mu              sync.Mutex
	closeEntered    chan struct{}
	writerFinishing chan struct{}
	closingStateSet chan struct{}
	// postCASReadError is test-only fault injection for the Problem 2 evidence matrix.
	// It is nil in every production Store and has no effect on normal control flow.
	postCASReadError func(string) error
	// recheckFault is a test-only named-stage fault boundary for Problem 3.
	// Production Stores leave it nil and execute the normal SQL path.
	recheckFault func(string) error
	// observabilityFault is a test-only boundary for best-effort poll metadata.
	observabilityFault func(string, string) error
	// postCommitEventEmissionFault is a test-only post-commit logging boundary.
	postCommitEventEmissionFault func(string) error
	// statsSnapshotAfterCurrentRows is a test-only causal boundary for proving
	// all stats fields come from one read transaction.
	statsSnapshotAfterCurrentRows func()
	// recheckBatchDuration overrides only the emitted batch duration in tests.
	recheckBatchDuration func(time.Duration) time.Duration
	// recheckResultReadFault proves that confirmed reconciliation does not depend
	// on a later observability read. Production Stores leave it nil.
	recheckResultReadFault func() error
	// recheckNow fixes worker transition time only in deterministic evidence tests.
	recheckNow func() time.Time
	// regroupNow fixes regroup transition time only in deterministic evidence tests.
	regroupNow func() time.Time
	// recheckOperatorBarrier exposes the two operation-specific barrier checks to
	// deterministic concurrency tests. Production Stores leave it nil.
	recheckOperatorBarrier func(string)
	// recheckWorkerBarrier exposes operator-finalization checks to real scheduler tests.
	recheckWorkerBarrier func(string)
	// recheckOperatorFinalizer pauses the after-unlock operator event path in tests.
	recheckOperatorFinalizer func()
	// recheckFailureFinalizer pauses retained failure events after writer release in tests.
	recheckFailureFinalizer func()
}

func (s *Store) signalRecheckOperatorBarrier(stage string) {
	if s == nil || s.lifecycleTestHooks == nil {
		return
	}
	s.lifecycleTestHooks.mu.Lock()
	hook := s.lifecycleTestHooks.recheckOperatorBarrier
	s.lifecycleTestHooks.mu.Unlock()
	if hook != nil {
		hook(stage)
	}
}

func (s *Store) signalRecheckWorkerBarrier(stage string) {
	if s == nil || s.lifecycleTestHooks == nil {
		return
	}
	s.lifecycleTestHooks.mu.Lock()
	hook := s.lifecycleTestHooks.recheckWorkerBarrier
	s.lifecycleTestHooks.mu.Unlock()
	if hook != nil {
		hook(stage)
	}
}

func (s *Store) signalRecheckOperatorFinalizer() {
	if s == nil || s.lifecycleTestHooks == nil {
		return
	}
	s.lifecycleTestHooks.mu.Lock()
	hook := s.lifecycleTestHooks.recheckOperatorFinalizer
	s.lifecycleTestHooks.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (s *Store) signalRecheckFailureFinalizer() {
	if s == nil || s.lifecycleTestHooks == nil {
		return
	}
	s.lifecycleTestHooks.mu.Lock()
	hook := s.lifecycleTestHooks.recheckFailureFinalizer
	s.lifecycleTestHooks.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (s *Store) currentRecheckTime() time.Time {
	if s != nil && s.lifecycleTestHooks != nil {
		s.lifecycleTestHooks.mu.Lock()
		now := s.lifecycleTestHooks.recheckNow
		s.lifecycleTestHooks.mu.Unlock()
		if now != nil {
			return now().UTC()
		}
	}
	return time.Now().UTC()
}

func (s *Store) currentRegroupTime() time.Time {
	if s != nil && s.lifecycleTestHooks != nil {
		s.lifecycleTestHooks.mu.Lock()
		now := s.lifecycleTestHooks.regroupNow
		s.lifecycleTestHooks.mu.Unlock()
		if now != nil {
			return now().UTC()
		}
	}
	return time.Now().UTC()
}

func (s *Store) recheckPostCASReadError(stage string) error {
	if s == nil || s.lifecycleTestHooks == nil {
		return nil
	}
	s.lifecycleTestHooks.mu.Lock()
	hook := s.lifecycleTestHooks.postCASReadError
	s.lifecycleTestHooks.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(stage)
}

func (s *Store) recheckStageFault(stage string) error {
	if s == nil || s.lifecycleTestHooks == nil {
		return nil
	}
	s.lifecycleTestHooks.mu.Lock()
	hook := s.lifecycleTestHooks.recheckFault
	s.lifecycleTestHooks.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(stage)
}

func (s *Store) observabilityCollectionFault(stage, eventFamily string) error {
	if s == nil || s.lifecycleTestHooks == nil {
		return nil
	}
	s.lifecycleTestHooks.mu.Lock()
	hook := s.lifecycleTestHooks.observabilityFault
	s.lifecycleTestHooks.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(stage, eventFamily)
}

func (s *Store) emitRuntimeEventsBestEffort(ctx context.Context, eventFamily string, events []runtimeEvent) {
	if s != nil && s.lifecycleTestHooks != nil {
		s.lifecycleTestHooks.mu.Lock()
		hook := s.lifecycleTestHooks.postCommitEventEmissionFault
		s.lifecycleTestHooks.mu.Unlock()
		if hook != nil {
			if err := hook(eventFamily); err != nil {
				runtimeLog.WarnContext(ctx, "alert_lifecycle", "observability_collection_failed", "Post-commit event emission failed", slog.String("stage", "post_commit_event"), slog.String("event_family", eventFamily), slog.String("error", SanitizeRecheckError(err)))
				return
			}
		}
	}
	emitRuntimeEventsContext(ctx, events)
}

func (s *Store) signalLifecycleHook(signal chan struct{}) {
	if s == nil || s.lifecycleTestHooks == nil || signal == nil {
		return
	}
	s.lifecycleTestHooks.mu.Lock()
	defer s.lifecycleTestHooks.mu.Unlock()
	select {
	case <-signal:
	default:
		close(signal)
	}
}

const (
	lifecycleStateOpen lifecycleState = iota
	lifecycleStateClosing
	lifecycleStateClosed
)

type writerRecoveryState uint8

const (
	writerRecoveryHealthy writerRecoveryState = iota
	writerRecoveryUnavailable
	writerRecoveryCommitUnknown
	writerRecoveryInvariant
)

const (
	writerOperationSchemaBootstrap    = "schema_bootstrap"
	writerOperationUpsertCurrentAlert = "upsert_current_alert"
	writerOperationStateTransition    = "insert_state_transition"
	writerOperationHousekeeping       = "housekeeping_cleanup"
	writerOperationVacuum             = "housekeeping_vacuum"
	writerOperationResolve            = "resolve_alert"
	writerOperationPoll               = "apply_poll_cycle"
	writerOperationRegroupBegin       = "regroup_begin"
	writerOperationRegroupBatch       = "regroup_batch"
	writerOperationRegroupFailure     = "regroup_failure"
	writerOperationRecheckBatch       = "recheck_batch"
	writerOperationRecheckFailure     = "recheck_failure"
	writerOperationRecheckOperator    = "recheck_operator_action"
)

var validWriterOperationKinds = map[string]struct{}{
	writerOperationSchemaBootstrap:    {},
	writerOperationUpsertCurrentAlert: {},
	writerOperationStateTransition:    {},
	writerOperationHousekeeping:       {},
	writerOperationVacuum:             {},
	writerOperationResolve:            {},
	writerOperationPoll:               {},
	writerOperationRegroupBegin:       {},
	writerOperationRegroupBatch:       {},
	writerOperationRegroupFailure:     {},
	writerOperationRecheckBatch:       {},
	writerOperationRecheckFailure:     {},
	writerOperationRecheckOperator:    {},
}

type pendingWriterCommit struct {
	token     string
	kind      string
	cause     error
	finalizer *writerAfterCommitFinalizer
}

type writerAfterCommitFinalizer struct {
	ctx            context.Context
	operationKind  string
	reconciled     bool
	reconcile      func(bool) error
	reconcileDone  bool
	reconcileErr   error
	handoff        func(writerHandoffDisposition)
	onHandoffError func(error)
	handoffDone    bool
	handoffErr     error
	run            func(bool)
}

type writerHandoffDisposition uint8

const (
	writerHandoffImmediate writerHandoffDisposition = iota
	writerHandoffCrossOperation
)

type writerCommitOutcome uint8

const (
	writerCommitOutcomeNotCommitted writerCommitOutcome = iota
	writerCommitOutcomeCommitted
	writerCommitOutcomeUnknown
)

type writerCommitOutcomeError struct {
	outcome   writerCommitOutcome
	operation string
	err       error
	cause     error
}

type writerAcquisitionError struct {
	operation string
	err       error
}

func (e *writerAcquisitionError) Error() string {
	return fmt.Sprintf("failed to acquire writer transaction for %s: %v", e.operation, e.err)
}

func (e *writerAcquisitionError) Unwrap() error { return e.err }

func (e *writerAcquisitionError) Is(target error) bool {
	return target == ErrWriterAcquisition || errors.Is(e.err, target)
}

func (e *writerCommitOutcomeError) Error() string { return e.err.Error() }

func (e *writerCommitOutcomeError) Unwrap() error { return e.err }

func (e *writerCommitOutcomeError) OriginalCause() error {
	if e.cause != nil {
		return e.cause
	}
	return e.err
}

func (e *writerCommitOutcomeError) OperationKind() string {
	if e == nil {
		return ""
	}
	return e.operation
}

// RecheckWriterCommitDisposition exposes the writer operation provenance needed
// by the cmd-owned recheck scheduler without exposing the generic writer type.
func RecheckWriterCommitDisposition(err error) (operation, outcome string, ok bool) {
	var value *writerCommitOutcomeError
	if !errors.As(err, &value) {
		return "", "", false
	}
	switch value.outcome {
	case writerCommitOutcomeCommitted:
		return value.operation, "committed", true
	case writerCommitOutcomeNotCommitted:
		return value.operation, "not_committed", true
	case writerCommitOutcomeUnknown:
		return value.operation, "unknown", true
	default:
		return value.operation, "", false
	}
}

const (
	RecheckBatchWriterOperation   = writerOperationRecheckBatch
	RecheckFailureWriterOperation = writerOperationRecheckFailure
)

const writerRecoveryTimeout = 5 * time.Second

const writerCommitLedgerRetention = 30 * 24 * time.Hour

type writerTransactionStarter interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type sqlConnWriterTransactionStarter struct {
	conn *sql.Conn
}

func (s sqlConnWriterTransactionStarter) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return s.conn.BeginTx(ctx, opts)
}

type StoreOptions struct {
	Path                       string
	SilenceHistoryRetentionTTL time.Duration
}

type storeOpenDependencies struct {
	openDB        func(string) (*sql.DB, error)
	acquireWriter func(context.Context, *sql.DB) (*sql.Conn, error)
	pingWriter    func(context.Context, *sql.Conn) error
	preflight     func(context.Context, *sql.Conn) error
	configure     func(context.Context, *sql.Conn) error
	bootstrap     func(*Store) error
	closeWriter   func(*sql.Conn) error
	closeDatabase func(*sql.DB) error
}

type GroupingConfig struct {
	LabelsKeep          []string
	LabelsKeepRegex     []string
	LabelsStrip         []string
	LabelsStripRegex    []string
	ReceiversKeep       []string
	ReceiversKeepRegex  []string
	ReceiversStrip      []string
	ReceiversStripRegex []string
}

func OpenStore(opts StoreOptions) (*Store, error) {
	return openStoreWithDependencies(opts, storeOpenDependencies{
		openDB:        func(dsn string) (*sql.DB, error) { return sql.Open("sqlite", dsn) },
		acquireWriter: func(ctx context.Context, db *sql.DB) (*sql.Conn, error) { return db.Conn(ctx) },
		pingWriter:    func(ctx context.Context, conn *sql.Conn) error { return conn.PingContext(ctx) },
		preflight:     preflightSchemaBeforeSetup,
		configure:     configureWriterConnection,
		bootstrap:     func(store *Store) error { return store.bootstrapSchema() },
	})
}

func openStoreWithDependencies(opts StoreOptions, deps storeOpenDependencies) (*Store, error) {
	if strings.TrimSpace(opts.Path) == "" {
		return nil, errors.New("sqlite path cannot be empty")
	}

	dir := filepath.Dir(opts.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create sqlite directory: %w", err)
	}

	dsn := fmt.Sprintf("file:%s?_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)&_txlock=immediate", opts.Path)
	if deps.openDB == nil {
		deps.openDB = func(dsn string) (*sql.DB, error) { return sql.Open("sqlite", dsn) }
	}
	if deps.acquireWriter == nil {
		deps.acquireWriter = func(ctx context.Context, db *sql.DB) (*sql.Conn, error) { return db.Conn(ctx) }
	}
	if deps.pingWriter == nil {
		deps.pingWriter = func(ctx context.Context, conn *sql.Conn) error { return conn.PingContext(ctx) }
	}
	if deps.preflight == nil {
		deps.preflight = preflightSchemaBeforeSetup
	}
	if deps.configure == nil {
		deps.configure = configureWriterConnection
	}
	if deps.bootstrap == nil {
		deps.bootstrap = func(store *Store) error { return store.bootstrapSchema() }
	}

	db, err := deps.openDB(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	writerConn, err := deps.acquireWriter(context.Background(), db)
	if err != nil {
		if deps.closeDatabase != nil {
			_ = deps.closeDatabase(db)
		} else {
			_ = db.Close()
		}
		return nil, fmt.Errorf("failed to acquire sqlite writer connection: %w", err)
	}
	closeResources := func() {
		if deps.closeWriter != nil {
			_ = deps.closeWriter(writerConn)
		} else {
			_ = writerConn.Close()
		}
		if deps.closeDatabase != nil {
			_ = deps.closeDatabase(db)
		} else {
			_ = db.Close()
		}
	}
	if err = deps.pingWriter(context.Background(), writerConn); err != nil {
		closeResources()
		return nil, fmt.Errorf("failed to connect to sqlite writer connection: %w", err)
	}
	silenceHistoryRetentionTTL := opts.SilenceHistoryRetentionTTL
	if silenceHistoryRetentionTTL <= 0 {
		silenceHistoryRetentionTTL = defaultSilenceHistoryRetentionTTL
	}
	store := &Store{
		db:                         db,
		writerConn:                 writerConn,
		writerTransactionStarter:   sqlConnWriterTransactionStarter{conn: writerConn},
		writerConnectionAcquirer:   db.Conn,
		writerClose:                deps.closeWriter,
		dbClose:                    deps.closeDatabase,
		path:                       opts.Path,
		silenceHistoryRetentionTTL: silenceHistoryRetentionTTL,
	}
	if err = deps.preflight(context.Background(), writerConn); err != nil {
		closeResources()
		return nil, err
	}
	if err = deps.configure(context.Background(), writerConn); err != nil {
		closeResources()
		return nil, err
	}
	if err = deps.bootstrap(store); err != nil {
		_ = store.Close()
		return nil, err
	}

	return store, nil
}

func preflightSchemaBeforeSetup(ctx context.Context, conn *sql.Conn) error {
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("failed to inspect sqlite schema before setup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	empty, err := isEmptySchemaV1DatabaseTx(tx)
	if err != nil {
		return err
	}
	if empty {
		return nil
	}
	return validateExistingSchemaV1Tx(tx)
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.signalLifecycleHook(func() chan struct{} {
		if s.lifecycleTestHooks == nil {
			return nil
		}
		return s.lifecycleTestHooks.closeEntered
	}())
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.lifecycleState == lifecycleStateClosed {
		return nil
	}
	if s.db == nil && s.writerConn == nil {
		s.writerTransactionStarter = nil
		s.lifecycleState = lifecycleStateClosed
		return nil
	}
	s.lifecycleState = lifecycleStateClosing
	if s.lifecycleTestHooks != nil {
		s.signalLifecycleHook(s.lifecycleTestHooks.closingStateSet)
	}
	var writerErr error
	if s.writerConn != nil {
		if s.writerClose != nil {
			writerErr = s.writerClose(s.writerConn)
		} else {
			writerErr = s.writerConn.Close()
		}
		s.writerConn = nil
	}
	s.writerTransactionStarter = nil
	var dbErr error
	switch {
	case s.db == nil:
	case s.dbClose != nil:
		dbErr = s.dbClose(s.db)
	default:
		dbErr = s.db.Close()
	}
	s.lifecycleState = lifecycleStateClosed
	return errors.Join(writerErr, dbErr)
}

func (s *Store) SetRecheckCommitWake(wake func()) {
	if s == nil {
		return
	}
	s.writeMu.Lock()
	s.recheckCommitWake = wake
	s.writeMu.Unlock()
}

// SetRecheckReconciledWake installs the wake owner for a recheck commit that was
// confirmed by a later writer operation. Ordinary recheck commits do not use it.
func (s *Store) SetRecheckReconciledWake(wake func()) {
	if s == nil {
		return
	}
	s.writeMu.Lock()
	s.recheckReconciledWake = wake
	s.writeMu.Unlock()
}

const schemaRecreateInstruction = "stop Karma and recreate the disposable lifecycle database, including its -wal and -shm sidecar files; automatic migration is not supported"

func schemaIncompatibleError(message string, cause error) error {
	if cause != nil {
		return fmt.Errorf("unsupported sqlite database: %s: %w; %s", message, cause, schemaRecreateInstruction)
	}
	return fmt.Errorf("unsupported sqlite database: %s; %s", message, schemaRecreateInstruction)
}

func configureWriterConnection(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, `PRAGMA auto_vacuum=2`); err != nil {
		return fmt.Errorf("failed to enable sqlite incremental auto vacuum: %w", err)
	}
	var autoVacuum int
	if err := conn.QueryRowContext(ctx, `PRAGMA auto_vacuum`).Scan(&autoVacuum); err != nil {
		return fmt.Errorf("failed to verify sqlite incremental auto vacuum: %w", err)
	}
	if autoVacuum != 2 {
		return fmt.Errorf("failed to enable sqlite incremental auto vacuum: got mode %d", autoVacuum)
	}

	var journalMode string
	if err := conn.QueryRowContext(ctx, `PRAGMA journal_mode=WAL`).Scan(&journalMode); err != nil {
		return fmt.Errorf("failed to enable sqlite WAL mode: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(journalMode), "wal") {
		return fmt.Errorf("failed to enable sqlite WAL mode: got %q", journalMode)
	}
	return verifyWriterConnection(ctx, conn)
}

func verifyWriterConnection(ctx context.Context, conn *sql.Conn) error {
	var synchronous int
	if err := conn.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&synchronous); err != nil {
		return fmt.Errorf("failed to verify sqlite synchronous mode: %w", err)
	}
	if synchronous != 1 {
		return fmt.Errorf("unexpected sqlite synchronous mode: got %d want 1", synchronous)
	}
	var busyTimeout int
	if err := conn.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&busyTimeout); err != nil {
		return fmt.Errorf("failed to verify sqlite busy timeout: %w", err)
	}
	if busyTimeout != 5000 {
		return fmt.Errorf("unexpected sqlite busy timeout: got %d want 5000", busyTimeout)
	}
	var foreignKeys int
	if err := conn.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
		return fmt.Errorf("failed to verify sqlite foreign keys: %w", err)
	}
	if foreignKeys != 1 {
		return fmt.Errorf("unexpected sqlite foreign keys setting: got %d want 1", foreignKeys)
	}
	return nil
}

func (s *Store) withWriterTx(ctx context.Context, operationKind string, operation func(*sql.Tx) error) (err error) {
	_, err = withWriterTxValue(ctx, s, operationKind, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, operation(tx)
	})
	return err
}

func withWriterTxValue[T any](ctx context.Context, s *Store, operationKind string, operation func(*sql.Tx) (T, error)) (value T, err error) {
	return withWriterTxValueAfterCommit(ctx, s, operationKind, operation, nil)
}

func withWriterTxValueAfterCommit[T any](ctx context.Context, s *Store, operationKind string, operation func(*sql.Tx) (T, error), finalizer *writerAfterCommitFinalizer) (value T, err error) {
	if s == nil {
		return value, ErrStoreNotInitialized
	}
	s.writeMu.Lock()
	var detachedFinalizer *writerAfterCommitFinalizer
	defer func() {
		if s.lifecycleTestHooks != nil {
			s.signalLifecycleHook(s.lifecycleTestHooks.writerFinishing)
		}
		s.writeMu.Unlock()
		runWriterFinalizer(detachedFinalizer)
	}()
	value, detachedFinalizer, err = withOwnedWriterTxValueAfterCommit(ctx, s, operationKind, operation, finalizer)
	return value, err
}

func (s *Store) handoffRecheckCommitLocked() {
	if s != nil && s.recheckCommitWake != nil {
		s.recheckCommitWake()
	}
}

func (s *Store) handoffRecheckReconciledLocked() {
	if s != nil && s.recheckReconciledWake != nil {
		s.recheckReconciledWake()
	}
}

func withOwnedWriterTxValueAfterCommit[T any](ctx context.Context, s *Store, operationKind string, operation func(*sql.Tx) (T, error), finalizer *writerAfterCommitFinalizer) (value T, detachedFinalizer *writerAfterCommitFinalizer, err error) {
	if s == nil {
		return value, nil, ErrStoreNotInitialized
	}
	var readyFinalizer *writerAfterCommitFinalizer
	if readyFinalizer, err = s.ensureWriterReadyLocked(ctx); err != nil {
		if errors.Is(err, ErrWriterUnavailable) && !errors.Is(err, ErrWriterCommitOutcomeUnknown) && !IsWriterAcquisitionError(err) {
			err = &writerAcquisitionError{operation: operationKind, err: err}
		}
		return value, readyFinalizer, err
	}
	if err = validateWriterOperationKind(operationKind); err != nil {
		return value, nil, err
	}
	token, err := s.nextWriterOperationToken()
	if err != nil {
		return value, nil, fmt.Errorf("%w: %w", ErrWriterOperationToken, err)
	}
	tx, err := s.writerTransactionStarter.BeginTx(ctx, nil)
	if err != nil {
		acquisitionErr := &writerAcquisitionError{operation: operationKind, err: err}
		if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
			if recoveryErr := s.recoverWriterConnectionLocked(ctx); recoveryErr != nil {
				return value, nil, errors.Join(acquisitionErr, fmt.Errorf("%w: %w", ErrWriterUnavailable, recoveryErr))
			}
		}
		return value, nil, acquisitionErr
	}
	commitAttempted := false
	defer func() {
		if recovered := recover(); recovered != nil {
			if !commitAttempted {
				if rollbackErr := s.rollbackWriterTxLocked(tx); rollbackErr != nil {
					if recoveryErr := s.discardWriterAfterFailureLocked(ctx); recoveryErr != nil {
						runtimeLog.ErrorContext(ctx, "lifecycle_writer", "observability_collection_failed", "Writer panic cleanup could not recover the writer connection", slog.String("writer_operation", operationKind), slog.String("stage", "panic_rollback_recovery"), slog.Any("error", recoveryErr))
					}
				}
			} else {
				s.markWriterCommitUnknownLocked(token, operationKind, nil, finalizer)
				if recoveryErr := s.recoverWriterConnectionLocked(ctx); recoveryErr != nil {
					s.writerRecoveryState = writerRecoveryUnavailable
				}
			}
			panic(recovered)
		}
	}()

	if value, err = operation(tx); err != nil {
		return value, nil, s.finishWriterCallbackErrorLocked(ctx, tx, operationKind, err)
	}
	if s.writerTransactionFinished != nil {
		s.writerTransactionFinished(operationKind)
	}
	if s.beforeCommitLedgerInsert != nil {
		s.beforeCommitLedgerInsert(ctx, operationKind)
	}
	if cause := context.Cause(ctx); cause != nil {
		return value, nil, s.finishWriterCallbackErrorLocked(ctx, tx, operationKind, cause)
	}
	insertLedger := func() error {
		_, execErr := tx.ExecContext(ctx, `
INSERT INTO lifecycle_writer_commits(operation_token, operation_kind, committed_at)
VALUES(?, ?, ?)
`, token, operationKind, time.Now().UTC().Format(time.RFC3339Nano))
		return execErr
	}
	if err = insertLedger(); err != nil {
		return value, nil, s.finishWriterCallbackErrorLocked(ctx, tx, operationKind, fmt.Errorf("failed to record writer commit: %w", err))
	}
	commitAttempted = true
	commitFn := s.writerCommit
	if commitFn == nil {
		commitFn = func(tx *sql.Tx) error { return tx.Commit() }
	}
	commitErr := commitFn(tx)
	if commitErr == nil {
		if reconcileErr := runWriterReconciliationLocked(finalizer, true); reconcileErr != nil {
			if errors.Is(reconcileErr, ErrWriterCommitOutcomeUnknown) {
				s.markWriterCommitUnknownLocked(token, operationKind, nil, finalizer)
				if recoveryErr := s.recoverWriterConnectionLocked(ctx); recoveryErr != nil {
					return value, nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, operation: operationKind, err: errors.Join(ErrWriterCommitOutcomeUnknown, ErrWriterUnavailable, fmt.Errorf("failed to recover after reconciliation error: %w", recoveryErr)), cause: reconcileErr}
				}
				return value, nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, operation: operationKind, err: reconcileErr, cause: reconcileErr}
			}
			s.writerRecoveryState = writerRecoveryInvariant
			return value, finalizer, errors.Join(ErrWriterOperationInvariant, reconcileErr)
		}
		if handoffErr := runWriterHandoffLocked(finalizer, writerHandoffImmediate); handoffErr != nil {
			s.writerRecoveryState = writerRecoveryInvariant
			return value, finalizer, errors.Join(ErrWriterOperationInvariant, handoffErr)
		}
		return value, finalizer, nil
	}
	// Release the connection before replacement. After a successful commit this
	// is ErrTxDone; after an ambiguous/failed commit it closes any open tx.
	_ = s.rollbackWriterTxLocked(tx)

	s.markWriterCommitUnknownLocked(token, operationKind, commitErr, finalizer)
	if recoveryErr := s.recoverWriterConnectionLocked(ctx); recoveryErr != nil {
		return value, nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, operation: operationKind, err: fmt.Errorf("%w: failed to recover after commit error: %w", ErrWriterCommitOutcomeUnknown, recoveryErr), cause: commitErr}
	}
	recoveryCtx, cancel := newWriterRecoveryContext(ctx)
	committed, lookupErr := s.lookupWriterCommitLocked(recoveryCtx, token, operationKind)
	cancel()
	if lookupErr != nil {
		if errors.Is(lookupErr, ErrWriterOperationInvariant) {
			s.writerRecoveryState = writerRecoveryInvariant
			return value, nil, lookupErr
		}
		lookupErr = s.recoverWriterAfterReconciliationErrorLocked(ctx, lookupErr)
		return value, nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, operation: operationKind, err: fmt.Errorf("%w: failed to reconcile commit: %w", ErrWriterCommitOutcomeUnknown, lookupErr), cause: commitErr}
	}
	if committed {
		pendingFinalizer := s.pendingWriterCommit.finalizer
		if pendingFinalizer != nil {
			pendingFinalizer.reconciled = true
			if reconcileErr := runWriterReconciliationLocked(pendingFinalizer, true); reconcileErr != nil {
				if errors.Is(reconcileErr, ErrWriterCommitOutcomeUnknown) {
					reconcileErr = s.recoverWriterAfterReconciliationErrorLocked(ctx, reconcileErr)
					return value, nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, operation: operationKind, err: reconcileErr, cause: commitErr}
				}
				s.writerRecoveryState = writerRecoveryInvariant
				return value, pendingFinalizer, errors.Join(ErrWriterOperationInvariant, reconcileErr)
			}
			if handoffErr := runWriterHandoffLocked(pendingFinalizer, writerHandoffImmediate); handoffErr != nil {
				s.writerRecoveryState = writerRecoveryInvariant
				return value, pendingFinalizer, errors.Join(ErrWriterOperationInvariant, handoffErr)
			}
		}
		detachedFinalizer = pendingFinalizer
		s.clearPendingWriterCommitLocked()
		return value, detachedFinalizer, nil
	}
	pendingFinalizer := s.pendingWriterCommit.finalizer
	if pendingFinalizer != nil {
		if reconcileErr := runWriterReconciliationLocked(pendingFinalizer, false); reconcileErr != nil {
			if errors.Is(reconcileErr, ErrWriterCommitOutcomeUnknown) {
				reconcileErr = s.recoverWriterAfterReconciliationErrorLocked(ctx, reconcileErr)
				return value, nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, operation: operationKind, err: reconcileErr, cause: commitErr}
			}
			s.writerRecoveryState = writerRecoveryInvariant
			return value, pendingFinalizer, errors.Join(ErrWriterOperationInvariant, reconcileErr)
		}
	}
	s.clearPendingWriterCommitLocked()
	return value, nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeNotCommitted, operation: operationKind, err: fmt.Errorf("failed to commit writer transaction: %w", commitErr), cause: commitErr}
}

type writerTupleResult struct {
	done      bool
	processed int
	updated   int
	total     int
}

func newWriterOperationToken() (string, error) {
	var raw [16]byte
	if _, err := cryptorand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (s *Store) nextWriterOperationToken() (string, error) {
	if s != nil && s.writerOperationToken != nil {
		return s.writerOperationToken()
	}
	return newWriterOperationToken()
}

func validateWriterOperationKind(kind string) error {
	if _, ok := validWriterOperationKinds[kind]; !ok {
		return fmt.Errorf("%w: unknown operation kind %q", ErrWriterOperationInvariant, kind)
	}
	return nil
}

func runWriterFinalizer(finalizer *writerAfterCommitFinalizer) {
	if finalizer == nil {
		return
	}
	if finalizer.handoffErr != nil {
		runtimeLog.ErrorContext(finalizer.ctx, "lifecycle_writer", "observability_collection_failed", "Writer post-commit handoff failed", slog.String("writer_operation", finalizer.operationKind), slog.String("stage", "after_commit_handoff"), slog.Any("error", finalizer.handoffErr))
		return
	}
	if finalizer.run == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			runtimeLog.ErrorContext(finalizer.ctx, "lifecycle_writer", "observability_collection_failed", "Writer post-commit finalizer panicked", slog.String("writer_operation", finalizer.operationKind), slog.String("stage", "after_commit_run"), slog.Any("error", recovered))
		}
	}()
	if finalizer.reconciled {
		runtimeLog.InfoContext(finalizer.ctx, "lifecycle_writer", "writer_commit_reconciled", "Writer commit was reconciled from the durable ledger", slog.String("writer_operation", finalizer.operationKind))
	}
	finalizer.run(finalizer.reconciled)
}

func runWriterHandoffLocked(finalizer *writerAfterCommitFinalizer, disposition writerHandoffDisposition) (handoffErr error) {
	if finalizer == nil || finalizer.handoffDone {
		if finalizer == nil {
			return nil
		}
		return finalizer.handoffErr
	}
	finalizer.handoffDone = true
	if finalizer.handoff == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			handoffErr = fmt.Errorf("writer post-commit handoff panicked: %v", recovered)
			finalizer.handoffErr = handoffErr
			if finalizer.onHandoffError != nil {
				func() {
					defer func() { _ = recover() }()
					finalizer.onHandoffError(handoffErr)
				}()
			}
		}
	}()
	finalizer.handoff(disposition)
	return nil
}

func runWriterReconciliationLocked(finalizer *writerAfterCommitFinalizer, writerCommitted bool) error {
	if finalizer == nil || finalizer.reconcile == nil {
		return nil
	}
	if finalizer.reconcileDone {
		return finalizer.reconcileErr
	}
	reconcileErr := finalizer.reconcile(writerCommitted)
	if errors.Is(reconcileErr, ErrWriterCommitOutcomeUnknown) {
		return reconcileErr
	}
	finalizer.reconcileDone = true
	finalizer.reconcileErr = reconcileErr
	return finalizer.reconcileErr
}

func (s *Store) ensureWriterReadyLocked(ctx context.Context) (*writerAfterCommitFinalizer, error) {
	if s.writerRecoveryState == writerRecoveryInvariant {
		return nil, ErrWriterOperationInvariant
	}
	if s.lifecycleState == lifecycleStateClosing {
		return nil, ErrStoreClosing
	}
	if s.lifecycleState == lifecycleStateClosed {
		return nil, ErrStoreClosed
	}
	if s.db == nil {
		return nil, ErrStoreNotInitialized
	}
	if s.pendingWriterCommit != nil || s.writerRecoveryState == writerRecoveryCommitUnknown {
		if s.pendingWriterCommit == nil {
			return nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, err: ErrWriterCommitOutcomeUnknown}
		}
		if s.writerConn == nil || s.writerTransactionStarter == nil {
			if err := s.recoverWriterConnectionLocked(ctx); err != nil {
				return nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, operation: s.pendingWriterCommit.kind, err: errors.Join(ErrWriterCommitOutcomeUnknown, ErrWriterUnavailable, err), cause: s.pendingWriterCommit.cause}
			}
		}
		recoveryCtx, cancel := newWriterRecoveryContext(ctx)
		committed, err := s.lookupWriterCommitLocked(recoveryCtx, s.pendingWriterCommit.token, s.pendingWriterCommit.kind)
		cancel()
		if err != nil {
			if errors.Is(err, ErrWriterOperationInvariant) {
				s.writerRecoveryState = writerRecoveryInvariant
				return nil, err
			}
			err = s.recoverWriterAfterReconciliationErrorLocked(ctx, err)
			return nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, operation: s.pendingWriterCommit.kind, err: fmt.Errorf("%w: %w", ErrWriterCommitOutcomeUnknown, err), cause: s.pendingWriterCommit.cause}
		}
		if committed {
			detachedFinalizer := s.pendingWriterCommit.finalizer
			operation := s.pendingWriterCommit.kind
			if detachedFinalizer != nil {
				detachedFinalizer.reconciled = true
				if reconcileErr := runWriterReconciliationLocked(detachedFinalizer, true); reconcileErr != nil {
					if errors.Is(reconcileErr, ErrWriterCommitOutcomeUnknown) {
						reconcileErr = s.recoverWriterAfterReconciliationErrorLocked(ctx, reconcileErr)
						return nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, operation: s.pendingWriterCommit.kind, err: reconcileErr, cause: s.pendingWriterCommit.cause}
					}
					s.writerRecoveryState = writerRecoveryInvariant
					return detachedFinalizer, errors.Join(ErrWriterOperationInvariant, reconcileErr)
				}
				if handoffErr := runWriterHandoffLocked(detachedFinalizer, writerHandoffCrossOperation); handoffErr != nil {
					s.writerRecoveryState = writerRecoveryInvariant
					return detachedFinalizer, errors.Join(ErrWriterOperationInvariant, handoffErr)
				}
			}
			s.clearPendingWriterCommitLocked()
			return detachedFinalizer, &writerCommitOutcomeError{outcome: writerCommitOutcomeCommitted, operation: operation, err: ErrWriterCommitReconciled}
		}
		detachedFinalizer := s.pendingWriterCommit.finalizer
		if detachedFinalizer != nil {
			if reconcileErr := runWriterReconciliationLocked(detachedFinalizer, false); reconcileErr != nil {
				if errors.Is(reconcileErr, ErrWriterCommitOutcomeUnknown) {
					reconcileErr = s.recoverWriterAfterReconciliationErrorLocked(ctx, reconcileErr)
					return nil, &writerCommitOutcomeError{outcome: writerCommitOutcomeUnknown, operation: s.pendingWriterCommit.kind, err: reconcileErr, cause: s.pendingWriterCommit.cause}
				}
				s.writerRecoveryState = writerRecoveryInvariant
				return detachedFinalizer, errors.Join(ErrWriterOperationInvariant, reconcileErr)
			}
		}
		s.clearPendingWriterCommitLocked()
	}
	if s.writerRecoveryState == writerRecoveryUnavailable || s.writerConn == nil || s.writerTransactionStarter == nil {
		if err := s.recoverWriterConnectionLocked(ctx); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrWriterUnavailable, err)
		}
	}
	return nil, nil
}

type recheckWriterReconciliation struct {
	attempted bool
	resolved  bool
	operation string
	outcome   string
	result    *RecheckBatchResult
}

func (s *Store) reconcileWriterForRecheck(ctx context.Context, pending recheckPendingFailure) (recheckWriterReconciliation, error) {
	if s == nil {
		return recheckWriterReconciliation{}, ErrStoreNotInitialized
	}
	s.writeMu.Lock()
	result := recheckWriterReconciliation{}
	if s.pendingWriterCommit != nil {
		result.attempted = true
		result.operation = s.pendingWriterCommit.kind
	} else if s.writerRecoveryState == writerRecoveryCommitUnknown {
		result.attempted = true
	}
	finalizer, err := s.ensureWriterReadyLocked(ctx)
	if err != nil && errors.Is(err, ErrWriterUnavailable) && !errors.Is(err, ErrWriterCommitOutcomeUnknown) && !IsWriterAcquisitionError(err) {
		err = &writerAcquisitionError{operation: writerOperationRecheckFailure, err: err}
	}
	if err == nil && result.attempted && isRecheckWriterOperation(result.operation) {
		result.resolved = true
		result.outcome = "absent"
	}
	if outcomeOperation, outcome, ok := RecheckWriterCommitDisposition(err); ok && outcome == "committed" {
		result.attempted = true
		result.operation = outcomeOperation
		result.outcome = "committed"
		if isRecheckWriterOperation(outcomeOperation) {
			result.resolved = true
			if !pending.ResultReady || pending.Operation != outcomeOperation {
				s.writeMu.Unlock()
				runWriterFinalizer(finalizer)
				return result, fmt.Errorf("%w: retained recheck result missing for %s", ErrWriterOperationInvariant, outcomeOperation)
			}
			retained := pending.Result
			result.result = &retained
		}
	}
	s.writeMu.Unlock()
	runWriterFinalizer(finalizer)
	return result, err
}

func isRecheckWriterOperation(operation string) bool {
	return operation == writerOperationRecheckBatch || operation == writerOperationRecheckFailure
}

func (s *Store) rollbackWriterTxLocked(tx *sql.Tx) error {
	if s.writerRollback != nil {
		return s.writerRollback(tx)
	}
	return tx.Rollback()
}

func (s *Store) finishWriterCallbackErrorLocked(ctx context.Context, tx *sql.Tx, operationKind string, operationErr error) error {
	if s.writerTransactionFinished != nil {
		defer s.writerTransactionFinished(operationKind)
	}
	rollbackErr := s.rollbackWriterTxLocked(tx)
	if rollbackErr == nil {
		return operationErr
	}
	recoveryErr := s.discardWriterAfterFailureLocked(ctx)
	if recoveryErr == nil {
		return errors.Join(operationErr, errWriterRollbackFailed, rollbackErr)
	}
	runtimeLog.ErrorContext(ctx, "lifecycle_writer", "observability_collection_failed", "Writer rollback cleanup could not recover the writer connection", slog.String("writer_operation", operationKind), slog.String("stage", "rollback_recovery"), slog.Any("error", recoveryErr))
	return errors.Join(operationErr, errWriterRollbackFailed, rollbackErr, ErrWriterUnavailable, recoveryErr)
}

func (s *Store) markWriterCommitUnknownLocked(token, kind string, cause error, finalizer *writerAfterCommitFinalizer) {
	s.pendingWriterCommit = &pendingWriterCommit{token: token, kind: kind, cause: cause, finalizer: finalizer}
	s.writerRecoveryState = writerRecoveryCommitUnknown
}

func (s *Store) clearPendingWriterCommitLocked() {
	s.pendingWriterCommit = nil
	s.writerRecoveryState = writerRecoveryHealthy
}

func (s *Store) discardWriterAfterFailureLocked(ctx context.Context) error {
	if err := s.recoverWriterConnectionLocked(ctx); err != nil {
		s.writerRecoveryState = writerRecoveryUnavailable
		return fmt.Errorf("%w: %w", ErrWriterUnavailable, err)
	}
	return nil
}

func newWriterRecoveryContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), writerRecoveryTimeout)
}

func (s *Store) recoverWriterConnectionLocked(parent context.Context) error {
	ctx, cancel := newWriterRecoveryContext(parent)
	defer cancel()
	return s.replaceWriterConnectionLocked(ctx)
}

func (s *Store) recoverWriterAfterReconciliationErrorLocked(parent context.Context, reconciliationErr error) error {
	if !errors.Is(reconciliationErr, driver.ErrBadConn) && !errors.Is(reconciliationErr, sql.ErrConnDone) {
		return reconciliationErr
	}
	if recoveryErr := s.recoverWriterConnectionLocked(parent); recoveryErr != nil {
		return errors.Join(reconciliationErr, ErrWriterUnavailable, recoveryErr)
	}
	return reconciliationErr
}

func (s *Store) lookupWriterCommitLocked(ctx context.Context, token, kind string) (bool, error) {
	if s.writerCommitLookup != nil {
		return s.writerCommitLookup(ctx, token, kind)
	}
	if s.writerConn == nil {
		return false, ErrWriterUnavailable
	}
	var storedKind string
	err := s.writerConn.QueryRowContext(ctx, `
SELECT operation_kind FROM lifecycle_writer_commits WHERE operation_token=?
`, token).Scan(&storedKind)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if storedKind != kind {
		return false, fmt.Errorf("%w: writer commit token has operation kind %q, expected %q", ErrWriterOperationInvariant, storedKind, kind)
	}
	return true, nil
}

func withWriterTxTupleAfterCommit(ctx context.Context, s *Store, operationKind string, operation func(*sql.Tx) (bool, int, int, int, error), finalizer *writerAfterCommitFinalizer) (writerTupleResult, error) {
	return withWriterTxValueAfterCommit(ctx, s, operationKind, func(tx *sql.Tx) (writerTupleResult, error) {
		done, processed, updated, total, err := operation(tx)
		return writerTupleResult{done: done, processed: processed, updated: updated, total: total}, err
	}, finalizer)
}

func (s *Store) replaceWriterConnectionLocked(ctx context.Context) error {
	if s == nil || s.db == nil {
		return ErrStoreNotInitialized
	}
	oldConn := s.writerConn
	s.writerConn = nil
	s.writerTransactionStarter = nil
	if oldConn != nil {
		_ = oldConn.Raw(func(any) error { return driver.ErrBadConn })
		_ = oldConn.Close()
	}
	acquire := s.writerConnectionAcquirer
	if acquire == nil {
		acquire = s.db.Conn
	}
	conn, err := acquire(ctx)
	if err != nil {
		s.writerRecoveryState = writerRecoveryUnavailable
		return fmt.Errorf("failed to replace sqlite writer connection: %w", err)
	}
	if err = conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		s.writerRecoveryState = writerRecoveryUnavailable
		return fmt.Errorf("failed to ping replacement sqlite writer connection: %w", err)
	}
	if err = verifyWriterConnection(ctx, conn); err != nil {
		_ = conn.Close()
		s.writerRecoveryState = writerRecoveryUnavailable
		return fmt.Errorf("failed to verify replacement sqlite writer connection: %w", err)
	}
	s.writerConn = conn
	s.writerTransactionStarter = sqlConnWriterTransactionStarter{conn: conn}
	if s.pendingWriterCommit != nil {
		s.writerRecoveryState = writerRecoveryCommitUnknown
	} else {
		s.writerRecoveryState = writerRecoveryHealthy
	}
	return nil
}

func (s *Store) sqlitePath() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *Store) bootstrapSchema() error {
	if s == nil || s.db == nil {
		return ErrStoreNotInitialized
	}

	const schemaSQL = `
CREATE TABLE IF NOT EXISTS alert_states (
  alert_id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  raw_state TEXT NOT NULL DEFAULT 'active',
  is_live INTEGER NOT NULL DEFAULT 1,
  is_suppressed INTEGER NOT NULL DEFAULT 0,
  is_resolved INTEGER NOT NULL DEFAULT 0,
  receiver TEXT NOT NULL,
  group_id TEXT NOT NULL,
  group_labels_json TEXT NOT NULL DEFAULT '{}',
  labels_json TEXT NOT NULL,
  match_labels_json TEXT,
  annotations_json TEXT NOT NULL,
  starts_at TEXT NOT NULL,
  last_seen_at TEXT,
  relevant_upstreams_json TEXT NOT NULL,
  relevant_sources_json TEXT NOT NULL DEFAULT '[]',
  source_links_json TEXT NOT NULL DEFAULT '[]',
  suppression_observed_at TEXT,
  resolved_at TEXT,
  resolved_by TEXT,
  resolved_comment TEXT,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alert_states_state ON alert_states(state);
CREATE INDEX IF NOT EXISTS idx_alert_states_raw_state ON alert_states(raw_state);
CREATE INDEX IF NOT EXISTS idx_alert_states_is_live ON alert_states(is_live);
CREATE INDEX IF NOT EXISTS idx_alert_states_is_suppressed ON alert_states(is_suppressed);
CREATE INDEX IF NOT EXISTS idx_alert_states_last_seen ON alert_states(last_seen_at);

CREATE TABLE IF NOT EXISTS alert_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  old_state TEXT,
  new_state TEXT NOT NULL,
  old_raw_state TEXT,
  new_raw_state TEXT NOT NULL,
  old_is_live INTEGER,
  new_is_live INTEGER NOT NULL,
  old_is_suppressed INTEGER,
  new_is_suppressed INTEGER NOT NULL DEFAULT 0,
  old_is_resolved INTEGER,
  new_is_resolved INTEGER NOT NULL,
  changed_at TEXT NOT NULL,
  change_source TEXT NOT NULL,
  changed_by TEXT,
  reason_code TEXT,
  reason_text TEXT,
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_state_history_alert_changed_at
  ON alert_events(alert_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_state_history_changed_at
  ON alert_events(changed_at DESC);

CREATE TABLE IF NOT EXISTS alert_target_states (
  alert_id TEXT NOT NULL,
  target TEXT NOT NULL,
  state TEXT NOT NULL,
  raw_state TEXT NOT NULL,
  is_live INTEGER NOT NULL,
  is_suppressed INTEGER NOT NULL,
  is_resolved INTEGER NOT NULL DEFAULT 0,
  starts_at TEXT NOT NULL,
  last_seen_at TEXT,
  source_members_json TEXT NOT NULL,
  match_labels_fingerprint TEXT NOT NULL DEFAULT '',
  retired_at TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (alert_id, target),
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_alert_target_states_target_state
  ON alert_target_states(target, state, is_live);

CREATE INDEX IF NOT EXISTS idx_alert_target_states_live
  ON alert_target_states(target, is_live, updated_at DESC);

CREATE TABLE IF NOT EXISTS alert_target_match_labels (
  alert_id TEXT NOT NULL,
  target TEXT NOT NULL,
  label_name TEXT NOT NULL,
  label_value TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (alert_id, target, label_name),
  FOREIGN KEY (alert_id, target) REFERENCES alert_target_states(alert_id, target) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_alert_target_match_labels_target_name
  ON alert_target_match_labels(target, label_name);

CREATE INDEX IF NOT EXISTS idx_alert_target_match_labels_target_name_value
  ON alert_target_match_labels(target, label_name, label_value);

CREATE TABLE IF NOT EXISTS alert_target_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  target TEXT NOT NULL,
  old_state TEXT,
  new_state TEXT NOT NULL,
  old_raw_state TEXT,
  new_raw_state TEXT NOT NULL,
  old_is_live INTEGER,
  new_is_live INTEGER NOT NULL,
  old_is_suppressed INTEGER,
  new_is_suppressed INTEGER NOT NULL,
  old_is_resolved INTEGER,
  new_is_resolved INTEGER NOT NULL DEFAULT 0,
  old_retired_at TEXT,
  new_retired_at TEXT,
  changed_at TEXT NOT NULL,
  reason_code TEXT,
  FOREIGN KEY (alert_id, target) REFERENCES alert_target_states(alert_id, target) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_alert_target_events_changed_at
  ON alert_target_events(target, changed_at DESC);

CREATE TABLE IF NOT EXISTS metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lifecycle_writer_commits (
  operation_token TEXT PRIMARY KEY,
  operation_kind TEXT NOT NULL,
  committed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_lifecycle_writer_commits_committed_at
  ON lifecycle_writer_commits (committed_at);

CREATE TABLE IF NOT EXISTS alertmanager_sources (
  source_name TEXT PRIMARY KEY,
  current_cluster_id TEXT,
  sync_status TEXT NOT NULL CHECK (sync_status IN ('unknown', 'success', 'failed', 'invalid', 'retired')),
  silence_set_fingerprint TEXT,
  first_configured_at TEXT NOT NULL,
  last_configured_at TEXT NOT NULL,
  last_attempt_at TEXT,
  last_success_at TEXT,
  last_confirmed_at TEXT,
  last_error TEXT,
  retired_at TEXT,
  CHECK (sync_status = 'retired' OR current_cluster_id IS NOT NULL),
  CHECK (retired_at IS NULL OR sync_status = 'retired')
);

CREATE INDEX IF NOT EXISTS idx_alertmanager_sources_status
  ON alertmanager_sources (sync_status, source_name);
CREATE INDEX IF NOT EXISTS idx_alertmanager_sources_retired
  ON alertmanager_sources (retired_at);

CREATE TABLE IF NOT EXISTS alertmanager_cluster_consistency (
  cluster_id TEXT PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('unknown', 'consistent', 'inconsistent')),
  member_count INTEGER NOT NULL DEFAULT 0 CHECK (member_count >= 0),
  last_evaluated_at TEXT NOT NULL,
  last_consistent_at TEXT,
  last_inconsistent_at TEXT
);

CREATE TABLE IF NOT EXISTS silence_rules (
  rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id TEXT NOT NULL,
  silence_id TEXT NOT NULL,
  first_observed_at TEXT NOT NULL,
  last_observed_at TEXT NOT NULL,
  UNIQUE (cluster_id, silence_id)
);

CREATE INDEX IF NOT EXISTS idx_silence_rules_silence_id
  ON silence_rules (silence_id);

CREATE TABLE IF NOT EXISTS silence_rule_episodes (
  episode_id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id INTEGER NOT NULL,
  episode_number INTEGER NOT NULL,
  first_observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  ended_at TEXT,
  end_reason TEXT,
  open_marker INTEGER CHECK (open_marker IS NULL OR open_marker = 1),
  UNIQUE (rule_id, episode_number),
  UNIQUE (rule_id, open_marker),
  UNIQUE (episode_id, rule_id),
  CHECK ((open_marker = 1 AND ended_at IS NULL AND end_reason IS NULL)
    OR (open_marker IS NULL AND ended_at IS NOT NULL AND end_reason = 'absent_confirmed')),
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_silence_rule_episodes_rule
  ON silence_rule_episodes (rule_id, episode_number DESC);
CREATE INDEX IF NOT EXISTS idx_silence_rule_episodes_ended_at
  ON silence_rule_episodes (ended_at);

CREATE TABLE IF NOT EXISTS silence_revisions (
  revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
  episode_id INTEGER NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_number INTEGER NOT NULL,
  content_fingerprint TEXT NOT NULL,
  evaluation_fingerprint TEXT NOT NULL,
  matchers_json TEXT NOT NULL,
  starts_at TEXT NOT NULL,
  ends_at TEXT NOT NULL,
  source_updated_at TEXT,
  created_by TEXT NOT NULL,
  comment TEXT NOT NULL,
  annotations_json TEXT NOT NULL DEFAULT '{}',
  ticket_id TEXT NOT NULL DEFAULT '',
  ticket_url TEXT NOT NULL DEFAULT '',
  first_observed_at TEXT NOT NULL,
  last_observed_at TEXT NOT NULL,
  UNIQUE (episode_id, revision_number),
  UNIQUE (revision_id, rule_id),
  FOREIGN KEY (episode_id, rule_id) REFERENCES silence_rule_episodes(episode_id, rule_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_silence_revisions_episode
  ON silence_revisions (episode_id, revision_number DESC);
CREATE INDEX IF NOT EXISTS idx_silence_revisions_content_fingerprint
  ON silence_revisions (content_fingerprint);
CREATE INDEX IF NOT EXISTS idx_silence_revisions_evaluation_fingerprint
  ON silence_revisions (evaluation_fingerprint);
CREATE INDEX IF NOT EXISTS idx_silence_revisions_last_observed
  ON silence_revisions (last_observed_at);

CREATE TABLE IF NOT EXISTS alertmanager_source_silences (
  source_name TEXT NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_id INTEGER NOT NULL,
  observed_state TEXT NOT NULL CHECK (observed_state IN ('pending', 'active', 'expired')),
  first_observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  PRIMARY KEY (source_name, rule_id),
  FOREIGN KEY (source_name) REFERENCES alertmanager_sources(source_name) ON DELETE CASCADE,
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id) REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_source_silences_revision
  ON alertmanager_source_silences (revision_id);
CREATE INDEX IF NOT EXISTS idx_source_silences_state
  ON alertmanager_source_silences (source_name, observed_state);

CREATE TABLE IF NOT EXISTS silence_recheck_jobs (
  job_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_context_fingerprint TEXT NOT NULL,
  job_fingerprint TEXT NOT NULL UNIQUE,
  source_context_json TEXT NOT NULL,
  trigger_type TEXT NOT NULL CHECK (trigger_type IN ('source_context_changed', 'alert_became_eligible')),
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'retry_wait', 'failed', 'completed', 'abandoned')),
  cursor_alert_id TEXT,
  total_candidates INTEGER NOT NULL DEFAULT 0,
  processed_candidates INTEGER NOT NULL DEFAULT 0,
  promoted_candidates INTEGER NOT NULL DEFAULT 0,
  skipped_ineligible INTEGER NOT NULL DEFAULT 0,
  skipped_missing_labels INTEGER NOT NULL DEFAULT 0,
  skipped_incomplete_sources INTEGER NOT NULL DEFAULT 0,
  stale_conflicts INTEGER NOT NULL DEFAULT 0,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  consecutive_retry_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  first_error_message TEXT,
  first_error_class TEXT,
  first_error_stage TEXT,
  first_error_at TEXT,
  last_error_message TEXT,
  last_error_class TEXT,
  last_error_stage TEXT,
  last_error_at TEXT,
  recovery_count INTEGER NOT NULL DEFAULT 0,
  abandoned_at TEXT,
  abandoned_by TEXT,
  abandon_reason TEXT,
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  CHECK (
    total_candidates >= 0 AND processed_candidates >= 0 AND processed_candidates <= total_candidates AND promoted_candidates >= 0
    AND skipped_ineligible >= 0 AND skipped_missing_labels >= 0
    AND skipped_incomplete_sources >= 0 AND stale_conflicts >= 0
    AND attempt_count >= 0 AND retry_count >= 0 AND consecutive_retry_count >= 0
    AND recovery_count >= 0
  ),
  CHECK (
    processed_candidates = promoted_candidates + skipped_ineligible
      + skipped_missing_labels + skipped_incomplete_sources + stale_conflicts
  ),
  CHECK (
    (status = 'retry_wait' AND next_attempt_at IS NOT NULL AND finished_at IS NULL)
    OR (status IN ('failed', 'completed', 'abandoned') AND next_attempt_at IS NULL AND finished_at IS NOT NULL)
    OR (status IN ('queued', 'running') AND next_attempt_at IS NULL AND finished_at IS NULL)
  ),
  CHECK (status = 'queued' OR started_at IS NOT NULL),
  CHECK (
    (status = 'abandoned' AND abandoned_at IS NOT NULL AND abandoned_at = finished_at)
    OR (status != 'abandoned' AND abandoned_at IS NULL AND abandoned_by IS NULL AND abandon_reason IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_silence_recheck_jobs_fifo
  ON silence_recheck_jobs (status, next_attempt_at, job_id);
CREATE INDEX IF NOT EXISTS idx_silence_recheck_jobs_context
  ON silence_recheck_jobs (source_context_fingerprint, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_silence_recheck_jobs_finished
  ON silence_recheck_jobs (finished_at);

CREATE TABLE IF NOT EXISTS silence_recheck_job_rules (
  job_id INTEGER NOT NULL,
  source_name TEXT NOT NULL,
  cluster_id TEXT NOT NULL DEFAULT '',
  target_key TEXT NOT NULL DEFAULT '',
  rule_id INTEGER NOT NULL,
  revision_id INTEGER NOT NULL,
  silence_id TEXT NOT NULL DEFAULT '',
  content_fingerprint TEXT NOT NULL DEFAULT '',
  observed_state TEXT NOT NULL CHECK (observed_state IN ('pending', 'active', 'expired')),
  evaluation_fingerprint TEXT NOT NULL DEFAULT '',
  matchers_json TEXT NOT NULL DEFAULT '[]',
  PRIMARY KEY (job_id, source_name, target_key, rule_id),
  FOREIGN KEY (job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE CASCADE,
  FOREIGN KEY (source_name) REFERENCES alertmanager_sources(source_name) ON DELETE RESTRICT,
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id) REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_recheck_job_rules_revision
  ON silence_recheck_job_rules (revision_id);
CREATE INDEX IF NOT EXISTS idx_recheck_job_rules_source
  ON silence_recheck_job_rules (job_id, source_name, observed_state);

CREATE TABLE IF NOT EXISTS silence_recheck_job_candidates (
  job_id INTEGER NOT NULL,
  alert_id TEXT NOT NULL,
  target_key TEXT NOT NULL DEFAULT '',
  expected_alert_last_seen_at TEXT,
  expected_target_last_seen_at TEXT,
  match_labels_json TEXT,
  relevant_sources_json TEXT NOT NULL DEFAULT '[]',
  target_source_members_json TEXT,
  processed_at TEXT,
  outcome TEXT CHECK (outcome IS NULL OR outcome IN (
    'promoted', 'stale_alert_observation', 'stale_target_observation',
    'missing_observation_token', 'ineligible_state', 'missing_current_alert',
    'missing_current_target', 'missing_labels', 'invalid_labels', 'no_matching_rule',
    'incomplete_source_coverage', 'retired_target', 'already_suppressed',
    'already_resolved'
  )),
  CHECK (
    (outcome IS NULL AND processed_at IS NULL)
    OR (outcome IS NOT NULL AND processed_at IS NOT NULL)
  ),
  CHECK (
    (target_key = '' AND target_source_members_json IS NULL)
    OR (target_key <> '' AND target_source_members_json IS NOT NULL)
  ),
  PRIMARY KEY (job_id, alert_id, target_key),
  FOREIGN KEY (job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_recheck_job_candidates_pending
  ON silence_recheck_job_candidates(job_id, processed_at, alert_id, target_key);

CREATE TABLE IF NOT EXISTS silence_recheck_job_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER NOT NULL,
  attempt_token TEXT NOT NULL UNIQUE,
  attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('committed', 'retry_scheduled', 'failed')),
  failure_class TEXT,
  failure_stage TEXT,
  scheduled_retry_at TEXT,
  CHECK (
    (outcome = 'committed' AND failure_class IS NULL AND failure_stage IS NULL AND scheduled_retry_at IS NULL)
    OR (outcome = 'retry_scheduled' AND failure_class IS NOT NULL AND failure_stage IS NOT NULL AND scheduled_retry_at IS NOT NULL)
    OR (outcome = 'failed' AND failure_class IS NOT NULL AND failure_stage IS NOT NULL AND scheduled_retry_at IS NULL)
  ),
  UNIQUE (job_id, attempt_number),
  FOREIGN KEY (job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_recheck_job_attempts_job
  ON silence_recheck_job_attempts(job_id, attempt_number);

CREATE TABLE IF NOT EXISTS silence_recheck_job_operator_actions (
  action_id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER NOT NULL,
  request_id TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('retry', 'abandon')),
  actor TEXT,
  reason TEXT NOT NULL DEFAULT '',
  request_fingerprint TEXT NOT NULL,
  requested_at TEXT NOT NULL,
  applied_at TEXT NOT NULL,
  applied_result TEXT NOT NULL CHECK (applied_result <> ''),
  resulting_status TEXT NOT NULL CHECK (resulting_status IN ('queued', 'abandoned')),
  UNIQUE (job_id, request_id),
  FOREIGN KEY (job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_recheck_job_operator_actions_job
  ON silence_recheck_job_operator_actions(job_id, action_id);

CREATE TABLE IF NOT EXISTS alert_suppression_cause_history (
  history_id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  transition_event_id INTEGER UNIQUE,
  recheck_job_id INTEGER,
  source TEXT NOT NULL CHECK (source IN ('live_alertmanager', 'silence_recheck', 'legacy_unknown')),
  cause_status TEXT NOT NULL CHECK (cause_status IN ('known', 'partial', 'none', 'unknown')),
  cause_fingerprint TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  CHECK (
    (source = 'silence_recheck' AND cause_status = 'known' AND transition_event_id IS NOT NULL AND recheck_job_id IS NOT NULL)
    OR (source = 'live_alertmanager' AND cause_status IN ('known', 'partial', 'none') AND transition_event_id IS NULL AND recheck_job_id IS NULL)
    OR (source = 'legacy_unknown' AND cause_status = 'unknown' AND transition_event_id IS NULL AND recheck_job_id IS NULL)
  ),
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE,
  FOREIGN KEY (transition_event_id) REFERENCES alert_events(id) ON DELETE CASCADE,
  FOREIGN KEY (recheck_job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_alert_suppression_cause_latest
  ON alert_suppression_cause_history (alert_id, history_id DESC);
CREATE INDEX IF NOT EXISTS idx_alert_suppression_cause_fingerprint
  ON alert_suppression_cause_history (alert_id, cause_fingerprint);
CREATE INDEX IF NOT EXISTS idx_alert_suppression_cause_job
  ON alert_suppression_cause_history (recheck_job_id);

CREATE TABLE IF NOT EXISTS alert_target_suppression_cause_history (
  history_id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  target TEXT NOT NULL,
  target_transition_event_id INTEGER UNIQUE,
  recheck_job_id INTEGER,
  source TEXT NOT NULL CHECK (source IN ('live_alertmanager', 'silence_recheck', 'legacy_unknown')),
  cause_status TEXT NOT NULL CHECK (cause_status IN ('known', 'partial', 'none', 'unknown')),
  cause_fingerprint TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  CHECK (
    (source = 'silence_recheck' AND cause_status = 'known' AND target_transition_event_id IS NOT NULL AND recheck_job_id IS NOT NULL)
    OR (source = 'live_alertmanager' AND cause_status IN ('known', 'partial', 'none') AND target_transition_event_id IS NULL AND recheck_job_id IS NULL)
    OR (source = 'legacy_unknown' AND cause_status = 'unknown' AND target_transition_event_id IS NULL AND recheck_job_id IS NULL)
  ),
  FOREIGN KEY (alert_id, target) REFERENCES alert_target_states(alert_id, target) ON DELETE CASCADE,
  FOREIGN KEY (target_transition_event_id) REFERENCES alert_target_events(id) ON DELETE CASCADE,
  FOREIGN KEY (recheck_job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_target_suppression_cause_latest
  ON alert_target_suppression_cause_history (alert_id, target, history_id DESC);

CREATE TABLE IF NOT EXISTS alert_target_suppression_cause_members (
  member_id INTEGER PRIMARY KEY AUTOINCREMENT,
  history_id INTEGER NOT NULL,
  source_name TEXT NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_id INTEGER,
  resolution_status TEXT NOT NULL CHECK (resolution_status IN ('known', 'unresolved')),
  UNIQUE (history_id, source_name, rule_id),
  CHECK (
    (resolution_status = 'known' AND revision_id IS NOT NULL)
    OR (resolution_status = 'unresolved' AND revision_id IS NULL)
  ),
  FOREIGN KEY (history_id) REFERENCES alert_target_suppression_cause_history(history_id) ON DELETE CASCADE,
  FOREIGN KEY (source_name) REFERENCES alertmanager_sources(source_name) ON DELETE RESTRICT,
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id) REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS alert_target_aggregate_suppression_history (
  aggregate_id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  transition_event_id INTEGER NOT NULL UNIQUE,
  cause_fingerprint TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE,
  FOREIGN KEY (transition_event_id) REFERENCES alert_events(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS alert_target_aggregate_suppression_links (
  aggregate_id INTEGER NOT NULL,
  target_history_id INTEGER NOT NULL,
  PRIMARY KEY (aggregate_id, target_history_id),
  FOREIGN KEY (aggregate_id) REFERENCES alert_target_aggregate_suppression_history(aggregate_id) ON DELETE CASCADE,
  FOREIGN KEY (target_history_id) REFERENCES alert_target_suppression_cause_history(history_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS alert_suppression_cause_members (
  member_id INTEGER PRIMARY KEY AUTOINCREMENT,
  history_id INTEGER NOT NULL,
  source_name TEXT NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_id INTEGER,
  resolution_status TEXT NOT NULL CHECK (resolution_status IN ('known', 'unresolved')),
  UNIQUE (history_id, source_name, rule_id),
  CHECK (
    (resolution_status = 'known' AND revision_id IS NOT NULL)
    OR (resolution_status = 'unresolved' AND revision_id IS NULL)
  ),
  FOREIGN KEY (history_id) REFERENCES alert_suppression_cause_history(history_id) ON DELETE CASCADE,
  FOREIGN KEY (source_name) REFERENCES alertmanager_sources(source_name) ON DELETE RESTRICT,
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id) REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_alert_suppression_members_revision
  ON alert_suppression_cause_members (revision_id);
CREATE INDEX IF NOT EXISTS idx_alert_suppression_members_rule
  ON alert_suppression_cause_members (rule_id);
CREATE INDEX IF NOT EXISTS idx_alert_suppression_members_source
  ON alert_suppression_cause_members (source_name, history_id);
`

	return s.withWriterTx(context.Background(), writerOperationSchemaBootstrap, func(tx *sql.Tx) error {
		empty, err := isEmptySchemaV1DatabaseTx(tx)
		if err != nil {
			return err
		}
		if !empty {
			if err = validateExistingSchemaV1Tx(tx); err != nil {
				return err
			}
			return nil
		}

		if _, err = tx.Exec(schemaSQL); err != nil {
			return fmt.Errorf("failed to apply schema: %w", err)
		}
		actualFingerprint, err := schemaLayoutFingerprintTx(tx)
		if err != nil {
			return err
		}
		if actualFingerprint != expectedSchemaLayoutFingerprint {
			return schemaIncompatibleError(fmt.Sprintf("schema layout fingerprint mismatch for fresh database: actual=%q expected=%q", actualFingerprint, expectedSchemaLayoutFingerprint), nil)
		}
		if _, err = tx.Exec(`INSERT INTO metadata(key, value) VALUES(?, ?)`, metaSchemaVersion, schemaVersion); err != nil {
			return fmt.Errorf("failed to set schema version: %w", err)
		}
		if _, err = tx.Exec(`INSERT INTO metadata(key, value) VALUES(?, ?)`, metaSchemaLayoutFingerprint, actualFingerprint); err != nil {
			return fmt.Errorf("failed to set schema layout fingerprint: %w", err)
		}

		defaults := map[string]string{
			metaRegroupStatus:            regroupStatusReady,
			metaGroupingFingerprint:      "",
			metaRegroupTargetFingerprint: "",
			metaRegroupError:             "",
			metaRegroupProcessed:         "0",
			metaRegroupTotal:             "0",
			metaRegroupCursorRowID:       "0",
			metaRegroupStartedAt:         "",
			metaRegroupFinishedAt:        "",
		}
		for key, value := range defaults {
			if _, err := tx.Exec(`INSERT OR IGNORE INTO metadata(key, value) VALUES(?, ?)`, key, value); err != nil {
				return fmt.Errorf("failed to seed metadata %q: %w", key, err)
			}
		}

		return nil
	})
}

func isEmptySchemaV1DatabaseTx(tx *sql.Tx) (bool, error) {
	var objectCount int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'`).Scan(&objectCount); err != nil {
		return false, fmt.Errorf("failed to inspect sqlite schema: %w", err)
	}
	return objectCount == 0, nil
}

func validateExistingSchemaV1Tx(tx *sql.Tx) error {
	var version string
	if err := tx.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaSchemaVersion).Scan(&version); err != nil {
		return schemaIncompatibleError("missing schema version 1", err)
	}
	if version != schemaVersion {
		return schemaIncompatibleError(fmt.Sprintf("unsupported sqlite schema version %q: expected %q", version, schemaVersion), nil)
	}
	var storedFingerprint string
	if err := tx.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaSchemaLayoutFingerprint).Scan(&storedFingerprint); err != nil {
		return schemaIncompatibleError("missing schema layout fingerprint", err)
	}
	actualFingerprint, err := schemaLayoutFingerprintTx(tx)
	if err != nil {
		return err
	}
	if storedFingerprint != actualFingerprint || actualFingerprint != expectedSchemaLayoutFingerprint {
		return schemaIncompatibleError(fmt.Sprintf("schema layout fingerprint mismatch: actual=%q stored=%q expected=%q", actualFingerprint, storedFingerprint, expectedSchemaLayoutFingerprint), nil)
	}
	return nil
}

type schemaLayoutObject struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	Table string `json:"tbl_name"`
	SQL   string `json:"sql"`
}

func schemaLayoutFingerprintTx(tx *sql.Tx) (string, error) {
	rows, err := tx.Query(`
SELECT type, name, tbl_name, sql
FROM sqlite_schema
WHERE name NOT LIKE 'sqlite_%'
  AND sql IS NOT NULL
ORDER BY type, name, tbl_name
`)
	if err != nil {
		return "", fmt.Errorf("failed to read sqlite schema layout: %w", err)
	}
	defer rows.Close()
	objects := make([]schemaLayoutObject, 0)
	for rows.Next() {
		var object schemaLayoutObject
		if err = rows.Scan(&object.Type, &object.Name, &object.Table, &object.SQL); err != nil {
			return "", fmt.Errorf("failed to scan sqlite schema layout: %w", err)
		}
		objects = append(objects, object)
	}
	if err = rows.Err(); err != nil {
		return "", fmt.Errorf("failed to iterate sqlite schema layout: %w", err)
	}
	raw, err := json.Marshal(objects)
	if err != nil {
		return "", fmt.Errorf("failed to encode sqlite schema layout: %w", err)
	}
	digest := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", digest), nil
}

func (s *Store) UpsertCurrentAlert(alert AlertCurrent) error {
	if s == nil || s.db == nil {
		return ErrStoreNotInitialized
	}
	if strings.TrimSpace(alert.GroupLabelsJSON) == "" {
		alert.GroupLabelsJSON = "{}"
	}
	if alert.IsResolved && alert.IsLive {
		alert.IsResolved = false
	}
	if !alert.RawState.IsValid() {
		alert.RawState = alert.State
	}
	if alert.IsLive {
		alert.IsSuppressed = alert.RawState == StateSuppressed
	}
	if !alert.State.IsValid() {
		alert.State = projectedState(alert.RawState, alert.IsLive, alert.IsSuppressed, alert.IsResolved)
	}
	if alert.State == StateActive || alert.State == StateSuppressed || alert.State == StateUnprocessed {
		alert.IsLive = true
	}
	if alert.State == StateInactive {
		alert.IsLive = false
	}
	if alert.State == StateResolved {
		alert.IsResolved = true
		alert.IsLive = false
	}
	if alert.IsLive {
		alert.IsSuppressed = alert.RawState == StateSuppressed
	}
	if strings.TrimSpace(alert.RelevantSources) == "" {
		alert.RelevantSources = alert.RelevantUpstreams
	}
	if strings.TrimSpace(alert.SourceLinksJSON) == "" {
		alert.SourceLinksJSON = "[]"
	}
	normalizedSourceLinks, err := normalizeAlertSourceLinksJSON(alert.SourceLinksJSON)
	if err != nil {
		return err
	}
	alert.SourceLinksJSON = normalizedSourceLinks
	if alert.State.IsValid() {
		alert.State = projectedState(alert.RawState, alert.IsLive, alert.IsSuppressed, alert.IsResolved)
	}

	if validateErr := validateCurrentAlert(alert); validateErr != nil {
		return validateErr
	}

	if alert.UpdatedAt.IsZero() {
		alert.UpdatedAt = time.Now().UTC()
	}

	return s.withWriterTx(context.Background(), writerOperationUpsertCurrentAlert, func(tx *sql.Tx) error {
		_, err = tx.Exec(`
INSERT INTO alert_states(
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  source_links_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(alert_id) DO UPDATE SET
  state=excluded.state,
  raw_state=excluded.raw_state,
  is_live=excluded.is_live,
  is_suppressed=excluded.is_suppressed,
  is_resolved=excluded.is_resolved,
  receiver=excluded.receiver,
  group_id=excluded.group_id,
  group_labels_json=excluded.group_labels_json,
  labels_json=excluded.labels_json,
  match_labels_json=excluded.match_labels_json,
  annotations_json=excluded.annotations_json,
  starts_at=excluded.starts_at,
  last_seen_at=excluded.last_seen_at,
  relevant_upstreams_json=excluded.relevant_upstreams_json,
  relevant_sources_json=excluded.relevant_sources_json,
  source_links_json=excluded.source_links_json,
  suppression_observed_at=excluded.suppression_observed_at,
  resolved_at=excluded.resolved_at,
  resolved_by=excluded.resolved_by,
  resolved_comment=excluded.resolved_comment,
  updated_at=excluded.updated_at
`,
			alert.AlertID,
			string(alert.State),
			string(alert.RawState),
			boolToInt(alert.IsLive),
			boolToInt(alert.IsSuppressed),
			boolToInt(alert.IsResolved),
			alert.Receiver,
			alert.GroupID,
			alert.GroupLabelsJSON,
			alert.LabelsJSON,
			alert.MatchLabelsJSON,
			alert.AnnotationsJSON,
			alert.StartsAt.UTC().Format(time.RFC3339Nano),
			formatNullableTime(alert.LastSeenAt),
			alert.RelevantUpstreams,
			alert.RelevantSources,
			alert.SourceLinksJSON,
			formatNullableTime(alert.SuppressionObservedAt),
			formatNullableTime(alert.ResolvedAt),
			alert.ResolvedBy,
			alert.ResolvedComment,
			alert.UpdatedAt.UTC().Format(time.RFC3339Nano),
		)
		if err != nil {
			return fmt.Errorf("failed to upsert current alert: %w", err)
		}
		return nil
	})
}

func (s *Store) InsertStateTransition(transition AlertStateTransition) error {
	if s == nil || s.db == nil {
		return ErrStoreNotInitialized
	}
	if !transition.NewRawState.IsValid() {
		if transition.NewState == StateInactive || transition.NewState == StateResolved {
			transition.NewRawState = StateActive
		} else {
			transition.NewRawState = transition.NewState
		}
	}
	if transition.OldRawState == nil && transition.OldState != nil {
		v := *transition.OldState
		transition.OldRawState = &v
	}
	if transition.OldIsLive == nil {
		v := transition.OldState != nil && *transition.OldState != StateInactive && *transition.OldState != StateResolved
		transition.OldIsLive = &v
	}
	if transition.OldIsResolved == nil {
		v := transition.OldState != nil && *transition.OldState == StateResolved
		transition.OldIsResolved = &v
	}
	if transition.OldIsSuppressed == nil {
		v := transition.OldState != nil && *transition.OldState == StateSuppressed
		transition.OldIsSuppressed = &v
	}
	if transition.NewIsLive {
		transition.NewIsSuppressed = transition.NewRawState == StateSuppressed
	}
	if transition.NewState.IsValid() {
		transition.NewState = projectedState(transition.NewRawState, transition.NewIsLive, transition.NewIsSuppressed, transition.NewIsResolved)
	}

	return s.withWriterTx(context.Background(), writerOperationStateTransition, func(tx *sql.Tx) error {
		if err := insertTransitionTx(tx, transition); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) GetCurrentAlert(alertID string) (*AlertCurrent, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}

	if strings.TrimSpace(alertID) == "" {
		return nil, ErrInvalidAlertID
	}

	row := s.db.QueryRow(`
SELECT
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  source_links_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
FROM alert_states
WHERE alert_id = ?
`, alertID)

	var (
		stateRaw               string
		rawStateRaw            string
		isLiveRaw              int
		isSuppressedRaw        int
		isResolvedRaw          int
		matchLabelsRaw         sql.NullString
		startsAtRaw            string
		lastSeenRaw            sql.NullString
		suppressionObservedRaw sql.NullString
		resolvedAtRaw          sql.NullString
		resolvedByRaw          sql.NullString
		resolvedCommentRaw     sql.NullString
		updatedAtRaw           string
		current                AlertCurrent
	)

	err := row.Scan(
		&current.AlertID,
		&stateRaw,
		&rawStateRaw,
		&isLiveRaw,
		&isSuppressedRaw,
		&isResolvedRaw,
		&current.Receiver,
		&current.GroupID,
		&current.GroupLabelsJSON,
		&current.LabelsJSON,
		&matchLabelsRaw,
		&current.AnnotationsJSON,
		&startsAtRaw,
		&lastSeenRaw,
		&current.RelevantUpstreams,
		&current.RelevantSources,
		&current.SourceLinksJSON,
		&suppressionObservedRaw,
		&resolvedAtRaw,
		&resolvedByRaw,
		&resolvedCommentRaw,
		&updatedAtRaw,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get current alert: %w", err)
	}

	state := State(stateRaw)
	if !state.IsValid() {
		return nil, fmt.Errorf("invalid state in database: %q", stateRaw)
	}
	current.State = state
	rawState := State(rawStateRaw)
	if !rawState.IsValid() {
		return nil, fmt.Errorf("invalid raw_state in database: %q", rawStateRaw)
	}
	current.RawState = rawState
	current.IsLive = isLiveRaw != 0
	current.IsSuppressed = isSuppressedRaw != 0
	current.IsResolved = isResolvedRaw != 0
	if matchLabelsRaw.Valid {
		v := strings.TrimSpace(matchLabelsRaw.String)
		if v != "" {
			current.MatchLabelsJSON = &v
		}
	}

	startsAt, err := time.Parse(time.RFC3339Nano, startsAtRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse starts_at: %w", err)
	}
	current.StartsAt = startsAt

	if lastSeenRaw.Valid {
		lastSeenAt, parseErr := time.Parse(time.RFC3339Nano, lastSeenRaw.String)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse last_seen_at: %w", parseErr)
		}
		current.LastSeenAt = &lastSeenAt
	}
	if resolvedAtRaw.Valid {
		resolvedAt, parseErr := time.Parse(time.RFC3339Nano, resolvedAtRaw.String)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse resolved_at: %w", parseErr)
		}
		current.ResolvedAt = &resolvedAt
	}
	if suppressionObservedRaw.Valid {
		suppressionObservedAt, parseErr := time.Parse(time.RFC3339Nano, suppressionObservedRaw.String)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse suppression_observed_at: %w", parseErr)
		}
		current.SuppressionObservedAt = &suppressionObservedAt
	}
	if resolvedByRaw.Valid {
		v := strings.TrimSpace(resolvedByRaw.String)
		if v != "" {
			current.ResolvedBy = &v
		}
	}
	if resolvedCommentRaw.Valid {
		v := strings.TrimSpace(resolvedCommentRaw.String)
		if v != "" {
			current.ResolvedComment = &v
		}
	}

	updatedAt, err := time.Parse(time.RFC3339Nano, updatedAtRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse updated_at: %w", err)
	}
	current.UpdatedAt = updatedAt

	return &current, nil
}

func (s *Store) GetSchemaVersion() (string, error) {
	if s == nil || s.db == nil {
		return "", ErrStoreNotInitialized
	}

	var version string
	err := s.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaSchemaVersion).Scan(&version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to query schema version: %w", err)
	}

	return version, nil
}

func (s *Store) GetTransitionCount(alertID string) (int, error) {
	if s == nil || s.db == nil {
		return 0, ErrStoreNotInitialized
	}
	if strings.TrimSpace(alertID) == "" {
		return 0, ErrInvalidAlertID
	}

	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM alert_events WHERE alert_id = ?`, alertID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count alert transitions: %w", err)
	}

	return count, nil
}

func (s *Store) ListCurrentAlertsByStates(states []State) ([]AlertCurrent, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}
	if len(states) == 0 {
		return []AlertCurrent{}, nil
	}

	args := make([]any, 0, len(states))
	placeholders := make([]string, 0, len(states))
	for _, state := range states {
		if !state.IsValid() {
			return nil, fmt.Errorf("invalid state: %q", state)
		}
		placeholders = append(placeholders, "?")
		args = append(args, string(state))
	}

	query := fmt.Sprintf(`
SELECT
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  source_links_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
FROM alert_states
WHERE state IN (%s)
`, strings.Join(placeholders, ","))

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list current alerts by state: %w", err)
	}
	defer rows.Close()

	alerts := []AlertCurrent{}
	for rows.Next() {
		current, scanErr := scanCurrentAlert(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		alerts = append(alerts, current)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate listed alerts: %w", err)
	}

	return alerts, nil
}

func (s *Store) ListCurrentAlertsForMerge(includeResolved bool) ([]AlertCurrent, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}

	query := `
SELECT
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  source_links_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
FROM alert_states
WHERE is_live = 0
`
	args := []any{}
	if !includeResolved {
		query += ` AND is_resolved = 0`
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list alerts for merge: %w", err)
	}
	defer rows.Close()

	alerts := []AlertCurrent{}
	for rows.Next() {
		current, scanErr := scanCurrentAlert(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		alerts = append(alerts, current)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate merge alerts: %w", err)
	}
	if err = rows.Close(); err != nil {
		return nil, fmt.Errorf("failed to close merge alert rows: %w", err)
	}

	causes, err := s.listEffectiveSuppressionCausesForMerge()
	if err != nil {
		return nil, err
	}
	for index := range alerts {
		alerts[index].SuppressionCauses = causes[alerts[index].AlertID]
	}

	return alerts, nil
}

func (s *Store) listEffectiveSuppressionCausesForMerge() (map[string][]SuppressionCauseRef, error) {
	rows, err := s.db.Query(`
WITH cause_parents AS (
  SELECT alert_id, 'global' AS kind, history_id AS parent_id, observed_at
  FROM alert_suppression_cause_history
  UNION ALL
  SELECT alert_id, 'aggregate' AS kind, aggregate_id AS parent_id, observed_at
  FROM alert_target_aggregate_suppression_history
), ranked_parents AS (
  SELECT alert_id, kind, parent_id,
         ROW_NUMBER() OVER (
           PARTITION BY alert_id
           ORDER BY observed_at DESC, kind DESC, parent_id DESC
         ) AS rank
  FROM cause_parents
), cause_members AS (
  SELECT 'global' AS kind, members.history_id AS parent_id,
         members.source_name, members.rule_id, members.revision_id,
         members.resolution_status
  FROM alert_suppression_cause_members members
  UNION ALL
  SELECT 'aggregate' AS kind, links.aggregate_id AS parent_id,
         members.source_name, members.rule_id, members.revision_id,
         members.resolution_status
  FROM alert_target_aggregate_suppression_links links
  JOIN alert_target_suppression_cause_members members
    ON members.history_id = links.target_history_id
)
SELECT DISTINCT parents.alert_id, members.source_name, rules.cluster_id,
       rules.silence_id, members.resolution_status,
       revisions.starts_at, revisions.ends_at, revisions.created_by,
       revisions.comment, revisions.matchers_json
FROM ranked_parents parents
JOIN alert_states alerts ON alerts.alert_id = parents.alert_id
JOIN cause_members members
  ON members.kind = parents.kind AND members.parent_id = parents.parent_id
JOIN silence_rules rules ON rules.rule_id = members.rule_id
LEFT JOIN silence_revisions revisions
  ON revisions.revision_id = members.revision_id
  AND revisions.rule_id = members.rule_id
WHERE parents.rank = 1
  AND alerts.state = 'suppressed'
  AND alerts.is_live = 0
  AND alerts.is_suppressed = 1
ORDER BY parents.alert_id, members.source_name, rules.cluster_id, rules.silence_id
`)
	if err != nil {
		return nil, fmt.Errorf("failed to list effective suppression causes for merge: %w", err)
	}
	defer rows.Close()

	result := map[string][]SuppressionCauseRef{}
	for rows.Next() {
		var alertID string
		var cause SuppressionCauseRef
		var startsAtRaw, endsAtRaw, createdBy, comment, matchersRaw sql.NullString
		if err = rows.Scan(
			&alertID,
			&cause.SourceName,
			&cause.ClusterID,
			&cause.SilenceID,
			&cause.ResolutionStatus,
			&startsAtRaw,
			&endsAtRaw,
			&createdBy,
			&comment,
			&matchersRaw,
		); err != nil {
			return nil, fmt.Errorf("failed to scan effective suppression cause for merge: %w", err)
		}
		if startsAtRaw.Valid {
			startsAt, parseErr := time.Parse(time.RFC3339Nano, startsAtRaw.String)
			if parseErr != nil {
				return nil, fmt.Errorf("failed to parse effective suppression cause starts_at: %w", parseErr)
			}
			cause.StartsAt = &startsAt
		}
		if endsAtRaw.Valid {
			endsAt, parseErr := time.Parse(time.RFC3339Nano, endsAtRaw.String)
			if parseErr != nil {
				return nil, fmt.Errorf("failed to parse effective suppression cause ends_at: %w", parseErr)
			}
			cause.EndsAt = &endsAt
		}
		if createdBy.Valid {
			cause.CreatedBy = createdBy.String
		}
		if comment.Valid {
			cause.Comment = comment.String
		}
		if matchersRaw.Valid {
			if err = json.Unmarshal([]byte(matchersRaw.String), &cause.Matchers); err != nil {
				return nil, fmt.Errorf("failed to decode effective suppression cause matchers: %w", err)
			}
		}
		result[alertID] = append(result[alertID], cause)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate effective suppression causes for merge: %w", err)
	}
	return result, nil
}

func (s *Store) ListTargetAlertsForPreview(target string) ([]AlertTargetCurrent, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, errors.New("target cannot be empty")
	}

	rows, err := s.db.Query(`
SELECT
  t.alert_id,
  t.target,
  t.state,
  t.raw_state,
  t.is_live,
  t.is_suppressed,
  t.starts_at,
  t.last_seen_at,
  t.source_members_json,
  s.match_labels_json,
  s.labels_json,
  t.is_resolved
FROM alert_target_states t
JOIN alert_states s ON s.alert_id = t.alert_id
WHERE t.target = ? AND t.retired_at IS NULL
`, target)
	if err != nil {
		return nil, fmt.Errorf("failed to list target alerts for preview: %w", err)
	}
	defer rows.Close()

	out := []AlertTargetCurrent{}
	for rows.Next() {
		var (
			stateRaw        string
			rawStateRaw     string
			isLiveRaw       int
			isSuppressedRaw int
			startsAtRaw     string
			lastSeenRaw     sql.NullString
			matchLabelsRaw  sql.NullString
			isResolvedRaw   int
			row             AlertTargetCurrent
		)
		if err = rows.Scan(
			&row.AlertID,
			&row.Target,
			&stateRaw,
			&rawStateRaw,
			&isLiveRaw,
			&isSuppressedRaw,
			&startsAtRaw,
			&lastSeenRaw,
			&row.SourceMembersJSON,
			&matchLabelsRaw,
			&row.LabelsJSON,
			&isResolvedRaw,
		); err != nil {
			return nil, fmt.Errorf("failed to scan target alert row: %w", err)
		}
		row.State = State(stateRaw)
		row.RawState = State(rawStateRaw)
		if !row.State.IsValid() || !row.RawState.IsValid() {
			continue
		}
		row.IsLive = intToBool(isLiveRaw)
		row.IsSuppressed = intToBool(isSuppressedRaw)
		row.IsResolved = intToBool(isResolvedRaw)
		if matchLabelsRaw.Valid {
			v := strings.TrimSpace(matchLabelsRaw.String)
			if v != "" {
				row.MatchLabelsJSON = &v
			}
		}
		startsAt, parseErr := time.Parse(time.RFC3339Nano, startsAtRaw)
		if parseErr != nil {
			continue
		}
		row.StartsAt = startsAt
		if lastSeenRaw.Valid {
			seenAt, parseErr := time.Parse(time.RFC3339Nano, lastSeenRaw.String)
			if parseErr == nil {
				row.LastSeenAt = &seenAt
			}
		}
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate target alert rows: %w", err)
	}
	return out, nil
}

func (s *Store) ListTargetLabelNames(target, term string, limit int) ([]SilenceEditorLabelNameStat, int, error) {
	return s.ListTargetLabelNamesWithLive(target, term, limit, nil)
}

func (s *Store) ListTargetLabelNamesWithLive(target, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelNameStat, int, error) {
	if s == nil || s.db == nil {
		return nil, 0, ErrStoreNotInitialized
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, 0, errors.New("target cannot be empty")
	}
	if limit <= 0 {
		limit = 100
	}
	term = strings.ToLower(strings.TrimSpace(term))
	if len(live) == 0 {
		rows, err := s.db.Query(`
SELECT label_name, value_count, COUNT(*) OVER() AS total_count
FROM (
  SELECT l.label_name, COUNT(DISTINCT l.label_value) AS value_count
  FROM alert_target_match_labels l
  WHERE l.target=?
    AND (?='' OR substr(lower(l.label_name), 1, length(?))=?)
  GROUP BY l.label_name
)
ORDER BY label_name ASC
LIMIT ?
`, target, term, term, term, limit)
		return scanTargetLabelNameStats(rows, err)
	}
	liveJSON, err := json.Marshal(live)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to encode live target labels: %w", err)
	}

	rows, err := s.db.Query(`
WITH live_input AS MATERIALIZED (
  SELECT json_extract(value, '$.alertID') AS alert_id,
         json_extract(value, '$.labels') AS labels_json,
         json_extract(value, '$.matchLabelsFingerprint') AS match_labels_fingerprint
  FROM json_each(?)
),
live_alerts AS MATERIALIZED (
  SELECT input.alert_id, input.labels_json
  FROM live_input input
  LEFT JOIN alert_target_states state ON state.alert_id=input.alert_id AND state.target=?
  WHERE COALESCE(input.match_labels_fingerprint, '')=''
     OR state.alert_id IS NULL
     OR state.retired_at IS NOT NULL
     OR state.match_labels_fingerprint<>input.match_labels_fingerprint
),
live_labels AS (
  SELECT live.alert_id, labels.key AS label_name, CAST(labels.value AS TEXT) AS label_value
  FROM live_alerts live, json_each(live.labels_json) labels
  WHERE (? = '' OR substr(lower(labels.key), 1, length(?)) = ?)
),
stored_labels AS (
  SELECT l.alert_id, l.label_name, l.label_value
  FROM alert_target_match_labels l
  LEFT JOIN live_alerts live ON live.alert_id=l.alert_id
  WHERE l.target = ?
    AND live.alert_id IS NULL
    AND (? = '' OR substr(lower(l.label_name), 1, length(?)) = ?)
),
eligible_labels AS (
  SELECT alert_id, label_name, label_value FROM stored_labels
  UNION ALL
  SELECT alert_id, label_name, label_value FROM live_labels
)
SELECT label_name, value_count, COUNT(*) OVER() AS total_count
FROM (
  SELECT label_name, COUNT(DISTINCT label_value) AS value_count
  FROM eligible_labels
  GROUP BY label_name
)
ORDER BY label_name ASC
LIMIT ?
`, string(liveJSON), target, term, term, term, target, term, term, term, limit)
	return scanTargetLabelNameStats(rows, err)
}

func scanTargetLabelNameStats(rows *sql.Rows, err error) ([]SilenceEditorLabelNameStat, int, error) {
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list target label names: %w", err)
	}
	defer rows.Close()
	out := []SilenceEditorLabelNameStat{}
	total := 0
	for rows.Next() {
		var row SilenceEditorLabelNameStat
		if err = rows.Scan(&row.Name, &row.Values, &total); err != nil {
			return nil, 0, fmt.Errorf("failed scanning target label name row: %w", err)
		}
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed iterating target label names: %w", err)
	}
	return out, total, nil
}

func (s *Store) ListTargetLabelValues(target, name, term string, limit int) ([]SilenceEditorLabelValueStat, int, error) {
	return s.ListTargetLabelValuesWithLive(target, name, term, limit, nil)
}

func (s *Store) ListTargetLabelValuesWithLive(target, name, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelValueStat, int, error) {
	if s == nil || s.db == nil {
		return nil, 0, ErrStoreNotInitialized
	}
	target = strings.TrimSpace(target)
	name = strings.TrimSpace(name)
	if target == "" || name == "" {
		return nil, 0, errors.New("target and name cannot be empty")
	}
	if limit <= 0 {
		limit = 100
	}
	term = strings.ToLower(strings.TrimSpace(term))
	if len(live) == 0 {
		rows, err := s.db.Query(`
SELECT label_value, hits, COUNT(*) OVER() AS total_count
FROM (
  SELECT l.label_value, COUNT(*) AS hits
  FROM alert_target_match_labels l
  WHERE l.target=?
    AND l.label_name=?
    AND (?='' OR substr(lower(l.label_value), 1, length(?))=?)
  GROUP BY l.label_value
)
ORDER BY hits DESC, label_value ASC
LIMIT ?
`, target, name, term, term, term, limit)
		return scanTargetLabelValueStats(rows, err)
	}
	liveJSON, err := json.Marshal(live)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to encode live target labels: %w", err)
	}

	rows, err := s.db.Query(`
WITH live_input AS MATERIALIZED (
  SELECT json_extract(value, '$.alertID') AS alert_id,
         json_extract(value, '$.labels') AS labels_json,
         json_extract(value, '$.matchLabelsFingerprint') AS match_labels_fingerprint
  FROM json_each(?)
),
live_alerts AS MATERIALIZED (
  SELECT input.alert_id, input.labels_json
  FROM live_input input
  LEFT JOIN alert_target_states state ON state.alert_id=input.alert_id AND state.target=?
  WHERE COALESCE(input.match_labels_fingerprint, '')=''
     OR state.alert_id IS NULL
     OR state.retired_at IS NOT NULL
     OR state.match_labels_fingerprint<>input.match_labels_fingerprint
),
live_labels AS (
  SELECT live.alert_id, labels.key AS label_name, CAST(labels.value AS TEXT) AS label_value
  FROM live_alerts live, json_each(live.labels_json) labels
  WHERE labels.key = ?
    AND (? = '' OR substr(lower(CAST(labels.value AS TEXT)), 1, length(?)) = ?)
),
stored_labels AS (
  SELECT l.alert_id, l.label_name, l.label_value
  FROM alert_target_match_labels l
  LEFT JOIN live_alerts live ON live.alert_id=l.alert_id
  WHERE l.target = ?
    AND live.alert_id IS NULL
    AND l.label_name = ?
    AND (? = '' OR substr(lower(l.label_value), 1, length(?)) = ?)
),
eligible_labels AS (
  SELECT alert_id, label_name, label_value FROM stored_labels
  UNION ALL
  SELECT alert_id, label_name, label_value FROM live_labels
)
SELECT label_value, hits, COUNT(*) OVER() AS total_count
FROM (
  SELECT label_value, COUNT(*) AS hits
  FROM eligible_labels
  GROUP BY label_value
)
ORDER BY hits DESC, label_value ASC
LIMIT ?
`, string(liveJSON), target, name, term, term, term, target, name, term, term, term, limit)
	return scanTargetLabelValueStats(rows, err)
}

// ListTargetContextLabelNamesWithLive aggregates label names only after the
// supplied matchers select the target's candidate alerts.
func (s *Store) ListTargetContextLabelNamesWithLive(target string, matchers []models.SilenceMatcher, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelNameStat, int, error) {
	candidates, err := s.listTargetContextLabelCandidatesWithLive(target, matchers, live)
	if err != nil {
		return nil, 0, err
	}
	return aggregateTargetLabelNames(candidates, term, limit), countTargetLabelNames(candidates, term), nil
}

// ListTargetContextCandidateIDsWithLive returns the alert IDs selected before
// label aggregation so callers can reuse that selection for prefix requests.
func (s *Store) ListTargetContextCandidateIDsWithLive(target string, matchers []models.SilenceMatcher, live []TargetLabelOverlay) ([]string, error) {
	candidates, err := s.listTargetContextLabelCandidatesWithLive(target, matchers, live)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.AlertID)
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *Store) ListTargetLabelNamesForCandidateIDsWithLive(target string, candidateIDs []string, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelNameStat, int, error) {
	candidates, err := s.listTargetLabelCandidatesByIDWithLive(target, candidateIDs, live)
	if err != nil {
		return nil, 0, err
	}
	return aggregateTargetLabelNames(candidates, term, limit), countTargetLabelNames(candidates, term), nil
}

// ListTargetContextLabelValuesWithLive aggregates values only after the
// supplied matchers select the target's candidate alerts.
func (s *Store) ListTargetContextLabelValuesWithLive(target, name string, matchers []models.SilenceMatcher, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelValueStat, int, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, 0, errors.New("label name cannot be empty")
	}
	candidates, err := s.listTargetContextLabelCandidatesWithLive(target, matchers, live)
	if err != nil {
		return nil, 0, err
	}
	items, total := aggregateTargetLabelValues(candidates, name, term, limit)
	return items, total, nil
}

func (s *Store) ListTargetLabelValuesForCandidateIDsWithLive(target, name string, candidateIDs []string, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelValueStat, int, error) {
	candidates, err := s.listTargetLabelCandidatesByIDWithLive(target, candidateIDs, live)
	if err != nil {
		return nil, 0, err
	}
	items, total := aggregateTargetLabelValues(candidates, name, term, limit)
	return items, total, nil
}

func (s *Store) listTargetLabelCandidatesByIDWithLive(target string, candidateIDs []string, live []TargetLabelOverlay) ([]TargetLabelOverlay, error) {
	ids := map[string]struct{}{}
	for _, id := range candidateIDs {
		ids[id] = struct{}{}
	}
	candidates, err := s.listTargetLabelCandidatesWithLive(target, live, candidateIDs)
	if err != nil {
		return nil, err
	}
	result := make([]TargetLabelOverlay, 0, len(candidateIDs))
	for _, candidate := range candidates {
		if _, ok := ids[candidate.AlertID]; ok {
			result = append(result, candidate)
		}
	}
	return result, nil
}

func aggregateTargetLabelNames(candidates []TargetLabelOverlay, term string, limit int) []SilenceEditorLabelNameStat {
	term = strings.ToLower(strings.TrimSpace(term))
	valuesByName := map[string]map[string]struct{}{}
	for _, candidate := range candidates {
		for name, value := range candidate.Labels {
			if term != "" && !strings.HasPrefix(strings.ToLower(name), term) {
				continue
			}
			if valuesByName[name] == nil {
				valuesByName[name] = map[string]struct{}{}
			}
			valuesByName[name][value] = struct{}{}
		}
	}
	items := make([]SilenceEditorLabelNameStat, 0, len(valuesByName))
	for name, values := range valuesByName {
		items = append(items, SilenceEditorLabelNameStat{Name: name, Values: len(values)})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items
}

func countTargetLabelNames(candidates []TargetLabelOverlay, term string) int {
	return len(aggregateTargetLabelNames(candidates, term, 0))
}

func aggregateTargetLabelValues(candidates []TargetLabelOverlay, name, term string, limit int) ([]SilenceEditorLabelValueStat, int) {
	term = strings.ToLower(strings.TrimSpace(term))
	hits := map[string]int{}
	for _, candidate := range candidates {
		value, ok := candidate.Labels[name]
		if !ok || (term != "" && !strings.HasPrefix(strings.ToLower(value), term)) {
			continue
		}
		hits[value]++
	}
	items := make([]SilenceEditorLabelValueStat, 0, len(hits))
	for value, count := range hits {
		items = append(items, SilenceEditorLabelValueStat{Value: value, Hits: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Hits != items[j].Hits {
			return items[i].Hits > items[j].Hits
		}
		return items[i].Value < items[j].Value
	})
	total := len(items)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, total
}

func (s *Store) listTargetContextLabelCandidatesWithLive(target string, matchers []models.SilenceMatcher, live []TargetLabelOverlay) ([]TargetLabelOverlay, error) {
	seedIDs, err := s.targetExactContextCandidateIDs(target, matchers, live)
	if err != nil {
		return nil, err
	}
	candidates, err := s.listTargetLabelCandidatesWithLive(target, live, seedIDs)
	if err != nil {
		return nil, err
	}
	result := make([]TargetLabelOverlay, 0, len(candidates))
	for _, candidate := range candidates {
		matches := true
		for _, matcher := range matchers {
			if !matcher.IsMatch(candidate.Labels) {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, candidate)
		}
	}
	return result, nil
}

func (s *Store) targetExactContextCandidateIDs(target string, matchers []models.SilenceMatcher, live []TargetLabelOverlay) ([]string, error) {
	exact := make([]models.SilenceMatcher, 0, len(matchers))
	for _, matcher := range matchers {
		if matcher.IsEqual && !matcher.IsRegex {
			exact = append(exact, matcher)
		}
	}
	if len(exact) == 0 {
		return nil, nil
	}
	var query strings.Builder
	query.WriteString(`SELECT l.alert_id FROM alert_target_match_labels l WHERE l.target=? AND l.label_name=? AND l.label_value=?`)
	args := []any{target, exact[0].Name, exact[0].Value}
	for _, matcher := range exact[1:] {
		query.WriteString(` AND EXISTS (
SELECT 1 FROM alert_target_match_labels candidate
WHERE candidate.alert_id=l.alert_id AND candidate.target=l.target AND candidate.label_name=? AND candidate.label_value=?
)`)
		args = append(args, matcher.Name, matcher.Value)
	}
	rows, err := s.db.Query(query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to select exact context candidates: %w", err)
	}
	defer rows.Close()
	ids := map[string]struct{}{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed scanning exact context candidate: %w", err)
		}
		ids[id] = struct{}{}
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating exact context candidates: %w", err)
	}
	for _, overlay := range live {
		matches := true
		for _, matcher := range exact {
			if !matcher.IsMatch(overlay.Labels) {
				matches = false
				break
			}
		}
		if matches {
			ids[overlay.AlertID] = struct{}{}
		}
	}
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}

func (s *Store) listTargetLabelCandidatesWithLive(target string, live []TargetLabelOverlay, candidateIDs []string) ([]TargetLabelOverlay, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, errors.New("target cannot be empty")
	}
	liveJSON, err := json.Marshal(live)
	if err != nil {
		return nil, fmt.Errorf("failed to encode live target labels: %w", err)
	}
	if candidateIDs != nil && len(candidateIDs) == 0 {
		return []TargetLabelOverlay{}, nil
	}
	query := `
WITH live_input AS MATERIALIZED (
  SELECT json_extract(value, '$.alertID') AS alert_id,
         json_extract(value, '$.labels') AS labels_json,
         json_extract(value, '$.matchLabelsFingerprint') AS match_labels_fingerprint
  FROM json_each(?)
),
live_alerts AS MATERIALIZED (
  SELECT input.alert_id, input.labels_json
  FROM live_input input
  LEFT JOIN alert_target_states state ON state.alert_id=input.alert_id AND state.target=?
  WHERE COALESCE(input.match_labels_fingerprint, '')=''
     OR state.alert_id IS NULL
     OR state.retired_at IS NOT NULL
     OR state.match_labels_fingerprint<>input.match_labels_fingerprint
),
live_labels AS (
  SELECT live.alert_id, labels.key AS label_name, CAST(labels.value AS TEXT) AS label_value
  FROM live_alerts live, json_each(live.labels_json) labels
),
stored_labels AS (
  SELECT l.alert_id, l.label_name, l.label_value
  FROM alert_target_match_labels l
  LEFT JOIN live_alerts live ON live.alert_id=l.alert_id
  WHERE l.target=? AND live.alert_id IS NULL%s
)
SELECT alert_id, label_name, label_value FROM stored_labels
UNION ALL
SELECT alert_id, label_name, label_value FROM live_labels
`
	args := []any{string(liveJSON), target, target}
	if candidateIDs != nil {
		candidateJSON, marshalErr := json.Marshal(candidateIDs)
		if marshalErr != nil {
			return nil, fmt.Errorf("failed to encode candidate IDs: %w", marshalErr)
		}
		query = fmt.Sprintf(query, ` AND l.alert_id IN (SELECT value FROM json_each(?))`)
		args = append(args, string(candidateJSON))
	} else {
		query = fmt.Sprintf(query, "")
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to load target label candidates: %w", err)
	}
	defer rows.Close()
	labelsByAlert := map[string]map[string]string{}
	for rows.Next() {
		var alertID, name, value string
		if err = rows.Scan(&alertID, &name, &value); err != nil {
			return nil, fmt.Errorf("failed scanning target label candidate: %w", err)
		}
		if labelsByAlert[alertID] == nil {
			labelsByAlert[alertID] = map[string]string{}
		}
		labelsByAlert[alertID][name] = value
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating target label candidates: %w", err)
	}
	requested := map[string]struct{}{}
	for _, id := range candidateIDs {
		requested[id] = struct{}{}
	}
	result := make([]TargetLabelOverlay, 0, len(labelsByAlert))
	for alertID, labels := range labelsByAlert {
		if candidateIDs == nil {
			result = append(result, TargetLabelOverlay{AlertID: alertID, Labels: labels})
			continue
		}
		if _, ok := requested[alertID]; ok {
			result = append(result, TargetLabelOverlay{AlertID: alertID, Labels: labels})
		}
	}
	return result, nil
}

func scanTargetLabelValueStats(rows *sql.Rows, err error) ([]SilenceEditorLabelValueStat, int, error) {
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list target label values: %w", err)
	}
	defer rows.Close()
	out := []SilenceEditorLabelValueStat{}
	total := 0
	for rows.Next() {
		var row SilenceEditorLabelValueStat
		if err = rows.Scan(&row.Value, &row.Hits, &total); err != nil {
			return nil, 0, fmt.Errorf("failed scanning target label value row: %w", err)
		}
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed iterating target label values: %w", err)
	}
	return out, total, nil
}

func (s *Store) RunHousekeeping(retentionTTL time.Duration, maxAlerts int) (HousekeepingResult, error) {
	return s.RunHousekeepingContext(context.Background(), retentionTTL, maxAlerts)
}

func (s *Store) RunHousekeepingContext(ctx context.Context, retentionTTL time.Duration, maxAlerts int) (HousekeepingResult, error) {
	if s == nil || s.db == nil {
		return HousekeepingResult{}, ErrStoreNotInitialized
	}
	if retentionTTL <= 0 {
		return HousekeepingResult{}, errors.New("retentionTTL must be > 0")
	}
	if maxAlerts < 1 {
		return HousekeepingResult{}, errors.New("maxAlerts must be >= 1")
	}

	runAt := time.Now().UTC()
	cutoff := runAt.Add(-retentionTTL)

	s.writeMu.Lock()
	var finalizers []*writerAfterCommitFinalizer
	defer func() {
		s.writeMu.Unlock()
		for _, finalizer := range finalizers {
			runWriterFinalizer(finalizer)
		}
	}()
	retention, finalizer, err := withOwnedWriterTxValueAfterCommit(ctx, s, writerOperationHousekeeping, func(tx *sql.Tx) (housekeepingTransactionResult, error) {
		expiredDeleted, err := deleteExpiredAlertsTx(tx, cutoff)
		if err != nil {
			return housekeepingTransactionResult{}, err
		}

		limitDeleted, err := enforceMaxAlertsTx(tx, maxAlerts)
		if err != nil {
			return housekeepingTransactionResult{}, err
		}

		retention, err := runPhase2RetentionTx(ctx, tx, runAt, s.silenceHistoryRetentionTTL)
		if err != nil {
			return housekeepingTransactionResult{}, err
		}
		if err = setMetadataTx(tx, metaLastHousekeepingAt, runAt.Format(time.RFC3339Nano)); err != nil {
			return housekeepingTransactionResult{}, err
		}
		if _, err = tx.Exec(`DELETE FROM lifecycle_writer_commits WHERE committed_at < ?`, runAt.Add(-writerCommitLedgerRetention).Format(time.RFC3339Nano)); err != nil {
			return housekeepingTransactionResult{}, fmt.Errorf("failed to retain writer commit ledger: %w", err)
		}
		return housekeepingTransactionResult{
			expiredDeleted:         expiredDeleted,
			limitDeleted:           limitDeleted,
			silenceRetentionResult: retention,
		}, nil
	}, nil)
	if finalizer != nil {
		finalizers = append(finalizers, finalizer)
	}
	if err != nil {
		return HousekeepingResult{}, err
	}

	vacuumAt := time.Now().UTC()
	vacuumPagesFreed := 0
	_, finalizer, err = withOwnedWriterTxValueAfterCommit(ctx, s, writerOperationVacuum, func(tx *sql.Tx) (struct{}, error) {
		vacuumPagesFreed, err = runIncrementalVacuum(ctx, tx)
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, setMetadataTx(tx, metaLastIncrementalVacuumAt, vacuumAt.Format(time.RFC3339Nano))
	}, nil)
	if finalizer != nil {
		finalizers = append(finalizers, finalizer)
	}
	if err != nil {
		return HousekeepingResult{}, err
	}

	stats, err := s.GetStatsContext(ctx)
	if err != nil {
		return HousekeepingResult{}, err
	}

	return HousekeepingResult{
		ExpiredDeleted:        retention.expiredDeleted,
		LimitDeleted:          retention.limitDeleted,
		RecheckJobsDeleted:    retention.recheckJobs,
		RetiredSourcesDeleted: retention.retiredSources,
		RevisionsDeleted:      retention.revisions,
		EpisodesDeleted:       retention.episodes,
		RulesDeleted:          retention.rules,
		VacuumPagesFreed:      vacuumPagesFreed,
		CurrentRowsAfter:      stats.CurrentRows,
		HistoryRowsAfter:      stats.HistoryRows,
		RunAt:                 runAt,
		LastIncrementalVacuum: vacuumAt,
	}, nil
}

func (s *Store) GetStats() (Stats, error) {
	return s.GetStatsContext(context.Background())
}

func (s *Store) GetStatsContext(ctx context.Context) (Stats, error) {
	if s == nil || s.db == nil {
		return Stats{}, ErrStoreNotInitialized
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Stats{}, fmt.Errorf("failed to begin lifecycle stats snapshot: %w", err)
	}
	var statsBoundary func()
	if s.lifecycleTestHooks != nil {
		s.lifecycleTestHooks.mu.Lock()
		statsBoundary = s.lifecycleTestHooks.statsSnapshotAfterCurrentRows
		s.lifecycleTestHooks.mu.Unlock()
	}
	stats, err := getStatsSnapshotFromHook(tx, statsBoundary)
	if err != nil {
		_ = tx.Rollback()
		return Stats{}, err
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return Stats{}, fmt.Errorf("failed to commit lifecycle stats snapshot: %w", commitErr)
	}

	stat, err := os.Stat(s.sqlitePath())
	fileSize := int64(0)
	if err == nil {
		fileSize = stat.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return Stats{}, fmt.Errorf("failed to stat sqlite file: %w", err)
	}
	if fileSize == 0 && stats.PageCount > 0 && stats.PageSizeBytes > 0 {
		estimated := int64(stats.PageCount) * int64(stats.PageSizeBytes)
		if estimated > 0 {
			fileSize = estimated
		}
	}
	stats.FileSizeBytes = fileSize
	return stats, nil
}

func getStatsSnapshotFromHook(q lifecycleStatsQueryer, afterCurrentRows func()) (Stats, error) {
	currentRows, err := queryInt(q, `SELECT COUNT(*) FROM alert_states`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to count current alerts: %w", err)
	}
	if afterCurrentRows != nil {
		afterCurrentRows()
	}
	stateCounts, err := queryStateCountsFrom(q)
	if err != nil {
		return Stats{}, err
	}
	suppressedCurrent, suppressedPast, err := querySuppressedBreakdownFrom(q)
	if err != nil {
		return Stats{}, err
	}
	historyRows, err := queryInt(q, `SELECT COUNT(*) FROM alert_events`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to count state history rows: %w", err)
	}
	pageCount, err := queryInt(q, `PRAGMA page_count`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to query sqlite page_count: %w", err)
	}
	pageSize, err := queryInt(q, `PRAGMA page_size`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to query sqlite page_size: %w", err)
	}
	freelistCount, err := queryInt(q, `PRAGMA freelist_count`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to query sqlite freelist_count: %w", err)
	}

	lastHousekeepingAt, err := getOptionalMetadataTimeFrom(q, metaLastHousekeepingAt)
	if err != nil {
		return Stats{}, err
	}
	lastVacuumAt, err := getOptionalMetadataTimeFrom(q, metaLastIncrementalVacuumAt)
	if err != nil {
		return Stats{}, err
	}
	groupingFingerprint, err := getMetadataStringFrom(q, metaGroupingFingerprint)
	if err != nil {
		return Stats{}, err
	}
	regroupTargetFingerprint, err := getMetadataStringFrom(q, metaRegroupTargetFingerprint)
	if err != nil {
		return Stats{}, err
	}
	regroupStatus, err := getMetadataStringFrom(q, metaRegroupStatus)
	if err != nil {
		return Stats{}, err
	}
	regroupError, err := getMetadataStringFrom(q, metaRegroupError)
	if err != nil {
		return Stats{}, err
	}
	regroupProcessed, err := getMetadataIntFrom(q, metaRegroupProcessed)
	if err != nil {
		return Stats{}, err
	}
	regroupTotal, err := getMetadataIntFrom(q, metaRegroupTotal)
	if err != nil {
		return Stats{}, err
	}
	regroupStartedAt, err := getOptionalMetadataTimeFrom(q, metaRegroupStartedAt)
	if err != nil {
		return Stats{}, err
	}
	regroupFinishedAt, err := getOptionalMetadataTimeFrom(q, metaRegroupFinishedAt)
	if err != nil {
		return Stats{}, err
	}
	inconsistentClusters, err := queryInconsistentSilenceClustersFrom(q)
	if err != nil {
		return Stats{}, err
	}
	recheckStats, err := queryRecheckStatsFrom(q)
	if err != nil {
		return Stats{}, err
	}

	return Stats{
		CurrentRows:                 currentRows,
		HistoryRows:                 historyRows,
		StateCounts:                 stateCounts,
		SuppressedBreakdownCurrent:  suppressedCurrent,
		SuppressedBreakdownPast:     suppressedPast,
		GroupingFingerprint:         strings.TrimSpace(groupingFingerprint),
		RegroupTargetFingerprint:    strings.TrimSpace(regroupTargetFingerprint),
		RegroupStatus:               strings.TrimSpace(regroupStatus),
		RegroupError:                strings.TrimSpace(regroupError),
		RegroupProcessed:            regroupProcessed,
		RegroupTotal:                regroupTotal,
		RegroupStartedAt:            regroupStartedAt,
		RegroupFinishedAt:           regroupFinishedAt,
		FileSizeBytes:               0,
		PageCount:                   pageCount,
		PageSizeBytes:               pageSize,
		FreelistCount:               freelistCount,
		LastHousekeepingAt:          lastHousekeepingAt,
		LastIncrementalVacuumAt:     lastVacuumAt,
		SilenceInconsistentClusters: inconsistentClusters,
		Recheck:                     RecheckStats{QueueDepth: recheckStats.queueDepth, HeadJob: recheckStats.headJob},
	}, nil
}

type recheckStatsSnapshot struct {
	queueDepth int
	headJob    *RecheckHeadJob
}

type lifecycleStatsQueryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

func queryInconsistentSilenceClustersFrom(q lifecycleStatsQueryer) ([]string, error) {
	rows, err := q.Query(`SELECT cluster_id FROM alertmanager_cluster_consistency WHERE status='inconsistent' ORDER BY cluster_id`)
	if err != nil {
		return nil, fmt.Errorf("failed to query inconsistent silence clusters: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var clusterID string
		if err = rows.Scan(&clusterID); err != nil {
			return nil, fmt.Errorf("failed to scan inconsistent silence cluster: %w", err)
		}
		out = append(out, clusterID)
	}
	return out, rows.Err()
}

func queryRecheckStatsFrom(q lifecycleStatsQueryer) (recheckStatsSnapshot, error) {
	var out recheckStatsSnapshot
	if err := validateRecheckStatsRowsFrom(q); err != nil {
		return out, err
	}
	if err := q.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs WHERE status IN ('queued','running','retry_wait','failed')`).Scan(&out.queueDepth); err != nil {
		return out, fmt.Errorf("failed to query recheck queue depth: %w", err)
	}
	var (
		jobID, totalCandidates, processedCandidates, promotedCandidates                   int64
		skippedIneligible, skippedMissingLabels, skippedIncompleteSources, staleConflicts int64
		attemptCount, retryCount, consecutiveRetryCount, recoveryCount                    int64
		triggerType, status                                                               string
		createdAtRaw, startedAtRaw, finishedAtRaw, nextAttemptAtRaw                       sql.NullString
		firstErrorMessage, firstErrorClass, firstErrorStage, firstErrorAt                 sql.NullString
		lastErrorMessage, lastErrorClass, lastErrorStage, lastErrorAt                     sql.NullString
	)
	err := q.QueryRow(`
SELECT job_id, trigger_type, status, created_at, started_at, finished_at,
       total_candidates, processed_candidates, promoted_candidates,
       skipped_ineligible, skipped_missing_labels, skipped_incomplete_sources,
       stale_conflicts, attempt_count, retry_count, consecutive_retry_count,
       next_attempt_at, first_error_message, first_error_class, first_error_stage,
       first_error_at, last_error_message, last_error_class, last_error_stage,
       last_error_at, recovery_count
FROM silence_recheck_jobs
WHERE status IN ('queued','running','retry_wait','failed')
ORDER BY job_id ASC
LIMIT 1
`).Scan(
		&jobID, &triggerType, &status, &createdAtRaw, &startedAtRaw, &finishedAtRaw,
		&totalCandidates, &processedCandidates, &promotedCandidates,
		&skippedIneligible, &skippedMissingLabels, &skippedIncompleteSources,
		&staleConflicts, &attemptCount, &retryCount, &consecutiveRetryCount,
		&nextAttemptAtRaw, &firstErrorMessage, &firstErrorClass, &firstErrorStage,
		&firstErrorAt, &lastErrorMessage, &lastErrorClass, &lastErrorStage,
		&lastErrorAt, &recoveryCount,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return out, nil
	}
	if err != nil {
		return out, fmt.Errorf("failed to query current recheck job: %w", err)
	}
	createdAt, parseErr := parseRecheckStatsTime(createdAtRaw, "created_at")
	if parseErr != nil {
		return out, parseErr
	}
	if createdAt == nil {
		return out, errors.New("failed to read current recheck job: created_at is NULL")
	}
	startedAt, err := parseRecheckStatsTime(startedAtRaw, "started_at")
	if err != nil {
		return out, err
	}
	finishedAt, err := parseRecheckStatsTime(finishedAtRaw, "finished_at")
	if err != nil {
		return out, err
	}
	nextAttemptAt, err := parseRecheckStatsTime(nextAttemptAtRaw, "next_attempt_at")
	if err != nil {
		return out, err
	}
	firstError, err := buildRecheckStatsError(firstErrorMessage, firstErrorClass, firstErrorStage, firstErrorAt)
	if err != nil {
		return out, err
	}
	lastError, err := buildRecheckStatsError(lastErrorMessage, lastErrorClass, lastErrorStage, lastErrorAt)
	if err != nil {
		return out, err
	}
	if err := validateRecheckResultState(status, nextAttemptAt, firstError, lastError); err != nil {
		return out, err
	}
	out.headJob = &RecheckHeadJob{
		JobID: jobID, TriggerType: triggerType, Status: status, CreatedAt: *createdAt,
		StartedAt: startedAt, FinishedAt: finishedAt, TotalCandidates: int(totalCandidates),
		ProcessedCandidates: int(processedCandidates), OutcomeCounts: RecheckOutcomeCounts{
			Promoted: int(promotedCandidates), Stale: int(staleConflicts),
			Ineligible: int(skippedIneligible), MissingLabels: int(skippedMissingLabels),
			IncompleteSources: int(skippedIncompleteSources),
		}, AttemptCount: int(attemptCount), RetryCount: int(retryCount),
		ConsecutiveRetryCount: int(consecutiveRetryCount), NextAttemptAt: nextAttemptAt,
		FirstError: firstError, LastError: lastError, RecoveryCount: int(recoveryCount),
	}
	return out, nil
}

func validateRecheckStatsRowsFrom(q lifecycleStatsQueryer) error {
	rows, err := q.Query(`
SELECT status, next_attempt_at,
       first_error_message, first_error_class, first_error_stage, first_error_at,
       last_error_message, last_error_class, last_error_stage, last_error_at
FROM silence_recheck_jobs`)
	if err != nil {
		return fmt.Errorf("failed to validate recheck error tuples: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			status                                                     string
			nextAttemptRaw                                             sql.NullString
			firstMessage, firstClass, firstStage, firstAt, lastMessage sql.NullString
			lastClass, lastStage, lastAt                               sql.NullString
		)
		if err := rows.Scan(&status, &nextAttemptRaw, &firstMessage, &firstClass, &firstStage, &firstAt, &lastMessage, &lastClass, &lastStage, &lastAt); err != nil {
			return fmt.Errorf("failed to scan recheck error tuple: %w", err)
		}
		nextAttempt, err := parseRecheckStatsTime(nextAttemptRaw, "next_attempt_at")
		if err != nil {
			return err
		}
		firstError, err := buildRecheckStatsError(firstMessage, firstClass, firstStage, firstAt)
		if err != nil {
			return err
		}
		lastError, err := buildRecheckStatsError(lastMessage, lastClass, lastStage, lastAt)
		if err != nil {
			return err
		}
		if err := validateRecheckResultState(status, nextAttempt, firstError, lastError); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate recheck error tuples: %w", err)
	}
	return nil
}

func parseRecheckStatsTime(raw sql.NullString, field string) (*time.Time, error) {
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw.String)
	if err != nil {
		return nil, fmt.Errorf("failed to parse recheck %s: %w", field, err)
	}
	parsed = parsed.UTC()
	return &parsed, nil
}

func buildRecheckStatsError(message, class, stage, at sql.NullString) (*RecheckErrorSnapshot, error) {
	validCount := 0
	for _, field := range []sql.NullString{message, class, stage, at} {
		if field.Valid {
			validCount++
		}
	}
	if validCount == 0 {
		return nil, nil
	}
	if validCount != 4 || strings.TrimSpace(message.String) == "" || strings.TrimSpace(class.String) == "" || strings.TrimSpace(stage.String) == "" {
		return nil, errors.New("incomplete recheck error tuple")
	}
	errorAt, err := parseRecheckStatsTime(at, "error_at")
	if err != nil {
		return nil, err
	}
	if errorAt == nil {
		return nil, errors.New("recheck error tuple has NULL error_at")
	}
	return &RecheckErrorSnapshot{Class: strings.TrimSpace(class.String), Stage: strings.TrimSpace(stage.String), At: *errorAt, Message: strings.TrimSpace(message.String)}, nil
}

func queryStateCountsFrom(q lifecycleStatsQueryer) (map[string]int, error) {
	out := map[string]int{}

	rows, err := q.Query(`
SELECT state, COUNT(*)
FROM alert_states
GROUP BY state
`)
	if err != nil {
		return nil, fmt.Errorf("failed to query per-state counts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var raw string
		var count int
		if scanErr := rows.Scan(&raw, &count); scanErr != nil {
			return nil, fmt.Errorf("failed to scan per-state counts row: %w", scanErr)
		}
		state := strings.TrimSpace(raw)
		if state == "" {
			state = "unknown"
		}
		out[state] = count
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate per-state counts: %w", err)
	}

	return out, nil
}

func querySuppressedBreakdownFrom(q lifecycleStatsQueryer) (int, int, error) {
	rows, err := q.Query(`
SELECT is_live, COUNT(*)
FROM alert_states
WHERE state = ?
GROUP BY is_live
`, string(StateSuppressed))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query suppressed breakdown: %w", err)
	}
	defer rows.Close()

	current := 0
	past := 0
	for rows.Next() {
		var isLiveRaw int
		var count int
		if scanErr := rows.Scan(&isLiveRaw, &count); scanErr != nil {
			return 0, 0, fmt.Errorf("failed to scan suppressed breakdown row: %w", scanErr)
		}
		if isLiveRaw != 0 {
			current += count
		} else {
			past += count
		}
	}
	if err = rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("failed to iterate suppressed breakdown rows: %w", err)
	}

	return current, past, nil
}

func (s *Store) ResolveAlert(alertID string, changedBy, reason *string, changedAt time.Time) (*ResolveResult, error) {
	return s.ResolveAlertContext(context.Background(), alertID, changedBy, reason, changedAt)
}

func (s *Store) ResolveAlertContext(ctx context.Context, alertID string, changedBy, reason *string, changedAt time.Time) (*ResolveResult, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}
	if strings.TrimSpace(alertID) == "" {
		return nil, ErrInvalidAlertID
	}

	ts := changedAt.UTC()
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	var result *ResolveResult
	err := s.withWriterTx(ctx, writerOperationResolve, func(tx *sql.Tx) error {
		current, err := getCurrentAlertTx(tx, alertID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrAlertNotFound
			}
			return err
		}

		resolvedPlan, resolveApplied := ResolveDimensions(LifecycleDimensions{
			RawState: current.RawState, IsLive: current.IsLive,
			IsSuppressed: current.IsSuppressed, IsResolved: current.IsResolved,
		})
		if !resolveApplied && current.IsResolved {
			result = &ResolveResult{
				AlertID:   alertID,
				OldState:  current.State,
				NewState:  current.State,
				Applied:   false,
				ChangedAt: ts,
				ChangedBy: sanitizeNullableString(changedBy),
			}
			return nil
		}

		if !resolveApplied {
			return ErrResolveStateConflict
		}

		cleanChangedBy := sanitizeNullableString(changedBy)
		cleanComment := sanitizeNullableString(reason)
		if err = updateManualResolutionTx(tx, alertID, cleanChangedBy, cleanComment, ts); err != nil {
			return err
		}
		if err = updateTargetStatesResolvedTx(tx, alertID, true, ts, "manual_mark_resolved"); err != nil {
			return err
		}

		newRaw := resolvedPlan.RawState
		newState := resolvedPlan.ProjectedState()
		oldLive := current.IsLive
		newLive := resolvedPlan.IsLive
		oldResolved := current.IsResolved
		newResolved := resolvedPlan.IsResolved
		reasonCode := sanitizeReasonCode(ptrString("manual_mark_resolved"))
		if err = insertTransitionTx(tx, AlertStateTransition{
			AlertID:         alertID,
			OldState:        &current.State,
			NewState:        newState,
			OldRawState:     &current.RawState,
			NewRawState:     newRaw,
			OldIsLive:       &oldLive,
			NewIsLive:       newLive,
			OldIsSuppressed: &current.IsSuppressed,
			NewIsSuppressed: current.IsSuppressed,
			OldIsResolved:   &oldResolved,
			NewIsResolved:   newResolved,
			ChangedAt:       ts,
			ChangeSource:    ChangeSourceManual,
			ChangedBy:       cleanChangedBy,
			ReasonCode:      reasonCode,
			ReasonText:      cleanComment,
		}); err != nil {
			return err
		}

		result = &ResolveResult{
			AlertID:   alertID,
			OldState:  current.State,
			NewState:  newState,
			Applied:   true,
			ChangedAt: ts,
			ChangedBy: cleanChangedBy,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) ApplyPollCycle(input PollCycleInput) (PollCycleResult, error) {
	if s == nil || s.db == nil {
		return PollCycleResult{}, ErrStoreNotInitialized
	}

	now := input.Timestamp.UTC()
	configuredSources, sourceManifestKnown := configuredSourceNames(input.SilenceSources)
	configuredSourcesByTarget := configuredSourceNamesByTarget(input.SilenceSources)
	if now.IsZero() {
		now = time.Now().UTC()
	}

	orderedLiveAlerts := append([]LiveAlertSnapshot(nil), input.LiveAlerts...)
	slices.SortFunc(orderedLiveAlerts, func(a, b LiveAlertSnapshot) int { return strings.Compare(a.AlertID, b.AlertID) })
	liveByID := make(map[string]LiveAlertSnapshot, len(orderedLiveAlerts))
	allSuccessful := areAllUpstreamsSuccessful(input.SuccessfulUpstreams)
	for _, live := range orderedLiveAlerts {
		if len(live.RelevantUpstreams) == 0 && len(live.RelevantSources) > 0 {
			live.RelevantUpstreams = make([]string, 0, len(live.RelevantSources))
			for _, src := range live.RelevantSources {
				live.RelevantUpstreams = append(live.RelevantUpstreams, src.Name)
			}
		}
		if len(live.RelevantSources) == 0 && len(live.RelevantUpstreams) > 0 {
			live.RelevantSources = make([]SourceRef, 0, len(live.RelevantUpstreams))
			for _, name := range live.RelevantUpstreams {
				trimmed := strings.TrimSpace(name)
				if trimmed == "" {
					continue
				}
				live.RelevantSources = append(live.RelevantSources, SourceRef{Name: trimmed, Cluster: "__unknown__"})
			}
		}
		if strings.TrimSpace(live.GroupLabelsJSON) == "" {
			live.GroupLabelsJSON = "{}"
		}
		if err := validateLiveSnapshot(live); err != nil {
			return PollCycleResult{}, err
		}
		liveByID[live.AlertID] = live
	}

	ctx := context.Background()
	var committedPollResult pollTransactionResult
	pollEventsCollector := runtimeEventCollector{observabilityFault: s.observabilityCollectionFault}
	pollFinalizer := &writerAfterCommitFinalizer{
		ctx:           ctx,
		operationKind: writerOperationPoll,
		handoff: func(writerHandoffDisposition) {
			if committedPollResult.result.RecheckJobsQueued > 0 {
				s.handoffRecheckCommitLocked()
			}
		},
		run: func(bool) {
			s.emitRuntimeEventsBestEffort(ctx, "poll", committedPollResult.events)
		},
	}
	txResult, err := withWriterTxValueAfterCommit(ctx, s, writerOperationPoll, func(tx *sql.Tx) (pollTransactionResult, error) {
		result := PollCycleResult{}
		reconciliation, qerr := reconcileSilenceSourcesWithEventsTx(tx, input.SilenceSources, now, &pollEventsCollector)
		if qerr != nil {
			return pollTransactionResult{}, qerr
		}

		currentByID, err := readAllCurrentAlertsTx(tx)
		if err != nil {
			return pollTransactionResult{}, err
		}

		for _, inputLive := range orderedLiveAlerts {
			live := liveByID[inputLive.AlertID]
			targets := normalizeLiveTargets(live)
			current, found := currentByID[live.AlertID]
			if !found {
				if err = insertCurrentAlertTx(tx, live, now); err != nil {
					return pollTransactionResult{}, err
				}
				newProjected := projectedState(live.State, true, live.State == StateSuppressed, false)
				newLive := true
				newResolved := false
				transition := AlertStateTransition{
					AlertID:         live.AlertID,
					OldState:        nil,
					NewState:        newProjected,
					OldRawState:     nil,
					NewRawState:     live.State,
					OldIsLive:       nil,
					NewIsLive:       newLive,
					OldIsSuppressed: nil,
					NewIsSuppressed: live.State == StateSuppressed,
					OldIsResolved:   nil,
					NewIsResolved:   newResolved,
					ChangedAt:       now,
					ChangeSource:    ChangeSourcePoller,
					ReasonCode:      sanitizeReasonCode(ptrString("poll_first_seen")),
				}
				if err = insertTransitionTx(tx, transition); err != nil {
					return pollTransactionResult{}, err
				}
				pollEventsCollector.append(alertTransitionRuntimeEvent(transition))
				result.Inserted++
				result.Transitions++
				if err = upsertTargetStatesFromLiveWithEventsTx(tx, live.AlertID, targets, live.MatchLabelsJSON, input.SuccessfulUpstreams, configuredSourcesByTarget, sourceManifestKnown, now, &pollEventsCollector); err != nil {
					return pollTransactionResult{}, err
				}
				if live.State == StateSuppressed {
					if err = insertLiveSuppressionCauseTx(tx, live.AlertID, live.SilencedBySources, now); err != nil {
						return pollTransactionResult{}, err
					}
				}
				if err = insertLiveTargetSuppressionCausesTx(tx, live.AlertID, live.Targets, live.SilencedBySources, now); err != nil {
					return pollTransactionResult{}, err
				}
				continue
			}

			livePlan := ObserveLive(LifecycleDimensions{
				RawState: current.RawState, IsLive: current.IsLive,
				IsSuppressed: current.IsSuppressed, IsResolved: current.IsResolved,
			}, live.State)
			newRawState := livePlan.RawState
			newIsLive := livePlan.IsLive
			newIsResolved := livePlan.IsResolved
			newIsSuppressed := livePlan.IsSuppressed
			newState := projectedState(newRawState, newIsLive, newIsSuppressed, newIsResolved)
			if current.IsResolved {
				result.Reactivated++
			}

			existingRelevant, parseErr := parseRelevantUpstreams(current.RelevantUpstreams)
			if parseErr != nil {
				existingRelevant = nil
			}
			storedRelevant := selectStoredRelevantUpstreams(existingRelevant, live.RelevantUpstreams, allSuccessful)
			relevantJSON, marshalErr := marshalRelevantUpstreams(storedRelevant)
			if marshalErr != nil {
				return pollTransactionResult{}, marshalErr
			}

			existingRelevantSources, parseSourcesErr := parseRelevantSources(current.RelevantSources)
			if parseSourcesErr != nil {
				existingRelevantSources = nil
			}
			storedRelevantSources := selectStoredRelevantSources(existingRelevantSources, live.RelevantSources, allSuccessful)
			relevantSourcesJSON, marshalSourcesErr := marshalRelevantSources(storedRelevantSources)
			if marshalSourcesErr != nil {
				return pollTransactionResult{}, marshalSourcesErr
			}
			sourceLinksJSON, marshalLinksErr := marshalAlertSourceLinks(live.SourceLinks)
			if marshalLinksErr != nil {
				return pollTransactionResult{}, marshalLinksErr
			}

			if err = updateCurrentAlertFromLiveTx(tx, current.AlertID, live, newRawState, newIsLive, newIsSuppressed, newIsResolved, string(relevantJSON), string(relevantSourcesJSON), string(sourceLinksJSON), now); err != nil {
				return pollTransactionResult{}, err
			}
			result.Updated++
			if err = upsertTargetStatesFromLiveWithEventsTx(tx, current.AlertID, targets, live.MatchLabelsJSON, input.SuccessfulUpstreams, configuredSourcesByTarget, sourceManifestKnown, now, &pollEventsCollector); err != nil {
				return pollTransactionResult{}, err
			}

			if current.State != newState || current.RawState != newRawState || current.IsLive != newIsLive || current.IsSuppressed != newIsSuppressed || current.IsResolved != newIsResolved {
				old := current.State
				oldRaw := current.RawState
				oldLive := current.IsLive
				oldResolved := current.IsResolved
				reasonCode := "poll_raw_state_changed"
				if !oldLive {
					reasonCode = "poll_live_reappeared"
				}
				if oldResolved {
					reasonCode = "poll_reappear_clears_resolved"
				}
				transition := AlertStateTransition{
					AlertID:         current.AlertID,
					OldState:        &old,
					NewState:        newState,
					OldRawState:     &oldRaw,
					NewRawState:     newRawState,
					OldIsLive:       &oldLive,
					NewIsLive:       newIsLive,
					OldIsSuppressed: &current.IsSuppressed,
					NewIsSuppressed: newIsSuppressed,
					OldIsResolved:   &oldResolved,
					NewIsResolved:   newIsResolved,
					ChangedAt:       now,
					ChangeSource:    ChangeSourcePoller,
					ReasonCode:      sanitizeReasonCode(&reasonCode),
				}
				if err = insertTransitionTx(tx, transition); err != nil {
					return pollTransactionResult{}, err
				}
				pollEventsCollector.append(alertTransitionRuntimeEvent(transition))
				result.Transitions++
			}
			if newIsLive && newIsSuppressed {
				if err = insertLiveSuppressionCauseTx(tx, current.AlertID, live.SilencedBySources, now); err != nil {
					return pollTransactionResult{}, err
				}
			}
			if err = insertLiveTargetSuppressionCausesTx(tx, current.AlertID, live.Targets, live.SilencedBySources, now); err != nil {
				return pollTransactionResult{}, err
			}
		}

		alertIDs := make([]string, 0, len(currentByID))
		for alertID := range currentByID {
			alertIDs = append(alertIDs, alertID)
		}
		slices.Sort(alertIDs)
		for _, alertID := range alertIDs {
			current := currentByID[alertID]
			if _, exists := liveByID[alertID]; exists {
				continue
			}

			if err = markMissingTargetStatesNonLiveWithEventsTx(tx, current, input.SuccessfulUpstreams, configuredSourcesByTarget, sourceManifestKnown, now, &pollEventsCollector); err != nil {
				return pollTransactionResult{}, err
			}

			if !current.IsLive {
				continue
			}

			relevant, parseErr := parseRelevantUpstreams(current.RelevantUpstreams)
			if parseErr != nil {
				result.SkippedInactive++
				pollEventsCollector.append(runtimeEvent{level: slog.LevelDebug, component: "alert_lifecycle", event: "alert_absence_deferred", message: "Alert absence transition deferred", attrs: []slog.Attr{slog.String("alert_id", alertID), slog.String("reason_code", "invalid_stored_sources")}})
				continue
			}
			relevant = filterConfiguredSourceNames(relevant, configuredSources, sourceManifestKnown)

			if !allRelevantUpstreamsSuccessful(relevant, input.SuccessfulUpstreams) {
				result.SkippedInactive++
				pollEventsCollector.append(runtimeEvent{level: slog.LevelDebug, component: "alert_lifecycle", event: "alert_absence_deferred", message: "Alert absence transition deferred", attrs: []slog.Attr{slog.String("alert_id", alertID), slog.String("reason_code", "incomplete_source_coverage")}})
				continue
			}

			absencePlan := ObserveAbsent(LifecycleDimensions{
				RawState: current.RawState, IsLive: current.IsLive,
				IsSuppressed: current.IsSuppressed, IsResolved: current.IsResolved,
			})
			newRawState := absencePlan.RawState
			newIsLive := absencePlan.IsLive
			newIsSuppressed := absencePlan.IsSuppressed
			newIsResolved := absencePlan.IsResolved
			newState := projectedState(newRawState, newIsLive, newIsSuppressed, newIsResolved)
			if err = updateProjectedStateTx(tx, alertID, newRawState, newIsLive, newIsSuppressed, newIsResolved, now); err != nil {
				return pollTransactionResult{}, err
			}
			old := current.State
			oldRaw := current.RawState
			oldLive := current.IsLive
			oldResolved := current.IsResolved
			transition := AlertStateTransition{
				AlertID:         alertID,
				OldState:        &old,
				NewState:        newState,
				OldRawState:     &oldRaw,
				NewRawState:     newRawState,
				OldIsLive:       &oldLive,
				NewIsLive:       newIsLive,
				OldIsSuppressed: &current.IsSuppressed,
				NewIsSuppressed: newIsSuppressed,
				OldIsResolved:   &oldResolved,
				NewIsResolved:   newIsResolved,
				ChangedAt:       now,
				ChangeSource:    ChangeSourcePoller,
				ReasonCode:      sanitizeReasonCode(ptrString("poll_live_disappeared")),
			}
			if err = insertTransitionTx(tx, transition); err != nil {
				return pollTransactionResult{}, err
			}
			pollEventsCollector.append(alertTransitionRuntimeEvent(transition))
			if newState == StateInactive {
				result.SetInactive++
				pollEventsCollector.appendEligibilityScope(recheckCandidateScope{AlertID: alertID})
			}
			result.Transitions++
		}

		if reconciliation.sourceContextChanged {
			created, createErr := createRecheckJobFromFinalStateTx(tx, "source_context_changed", nil, now, &pollEventsCollector)
			if createErr != nil {
				return pollTransactionResult{}, createErr
			}
			if created.Created {
				result.RecheckJobsQueued++
				result.RecheckJobsSourceContextChanged++
			}
		} else {
			eligibleScopes := append([]recheckCandidateScope(nil), pollEventsCollector.eligibilityScopes...)
			slices.SortFunc(eligibleScopes, func(a, b recheckCandidateScope) int {
				if a.AlertID != b.AlertID {
					return strings.Compare(a.AlertID, b.AlertID)
				}
				return strings.Compare(a.TargetKey, b.TargetKey)
			})
			if len(eligibleScopes) > 0 {
				created, createErr := createRecheckJobFromFinalStateTx(tx, "alert_became_eligible", eligibleScopes, now, &pollEventsCollector)
				if createErr != nil {
					return pollTransactionResult{}, createErr
				}
				if created.Created {
					result.RecheckJobsQueued++
					result.RecheckJobsAlertBecameEligible++
				}
			}
		}

		committedPollResult = pollTransactionResult{result: result, events: append([]runtimeEvent(nil), pollEventsCollector.events...)}
		return committedPollResult, nil
	}, pollFinalizer)
	if err != nil {
		return PollCycleResult{}, err
	}

	return txResult.result, nil
}

type pollTransactionResult struct {
	result PollCycleResult
	events []runtimeEvent
}

type runtimeEvent struct {
	sequence  uint64
	level     slog.Level
	component string
	event     string
	message   string
	attrs     []slog.Attr
}

type runtimeEventCollector struct {
	events             []runtimeEvent
	nextSequence       uint64
	eligibilityScopes  []recheckCandidateScope
	observabilityFault func(string, string) error
}

func (c *runtimeEventCollector) append(event runtimeEvent) {
	if c == nil {
		return
	}
	event.sequence = c.nextSequence
	c.nextSequence++
	c.events = append(c.events, event)
}

func (c *runtimeEventCollector) observabilityError(stage, eventFamily string, err error) error {
	if c == nil {
		return err
	}
	if c.observabilityFault != nil {
		if injected := c.observabilityFault(stage, eventFamily); injected != nil {
			err = injected
		}
	}
	return err
}

func (c *runtimeEventCollector) appendObservabilityFailureValue(stage, eventFamily string, err error) {
	if c == nil || err == nil {
		return
	}
	c.append(runtimeEvent{level: slog.LevelWarn, component: "alert_lifecycle", event: "observability_collection_failed", message: "Poll observability metadata collection failed", attrs: []slog.Attr{slog.String("stage", stage), slog.String("event_family", eventFamily), slog.String("error", "observability_error")}})
}

func (c *runtimeEventCollector) appendEligibilityScope(scope recheckCandidateScope) {
	if c == nil {
		return
	}
	for _, existing := range c.eligibilityScopes {
		if existing == scope {
			return
		}
	}
	c.eligibilityScopes = append(c.eligibilityScopes, scope)
}

func runtimeEventStringAttr(name, value string) slog.Attr {
	return slog.String(name, value)
}

func runtimeEventOptionalBoolAttr(name string, value *bool) slog.Attr {
	if value == nil {
		return slog.Any(name, nil)
	}
	return slog.Bool(name, *value)
}

func nullableStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func nullableStateValue(value *State) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

func runtimeEventNullableIntBoolAttr(name string, value sql.NullInt64) slog.Attr {
	if !value.Valid {
		return slog.Any(name, nil)
	}
	return slog.Bool(name, value.Int64 != 0)
}

func alertTransitionRuntimeEvent(transition AlertStateTransition) runtimeEvent {
	return runtimeEvent{
		level: slog.LevelDebug, component: "alert_lifecycle", event: "alert_transition", message: "Alert lifecycle state changed",
		attrs: []slog.Attr{
			slog.String("alert_id", transition.AlertID),
			runtimeEventStringAttr("old_state", nullableStateValue(transition.OldState)),
			slog.String("new_state", string(transition.NewState)),
			runtimeEventStringAttr("old_raw_state", nullableStateValue(transition.OldRawState)),
			slog.String("new_raw_state", string(transition.NewRawState)),
			runtimeEventOptionalBoolAttr("old_is_live", transition.OldIsLive),
			slog.Bool("new_is_live", transition.NewIsLive),
			runtimeEventOptionalBoolAttr("old_is_suppressed", transition.OldIsSuppressed),
			slog.Bool("new_is_suppressed", transition.NewIsSuppressed),
			runtimeEventOptionalBoolAttr("old_is_resolved", transition.OldIsResolved),
			slog.Bool("new_is_resolved", transition.NewIsResolved),
			runtimeEventStringAttr("reason_code", nullableStringValue(transition.ReasonCode)),
		},
	}
}

func emitRuntimeEventsContext(ctx context.Context, events []runtimeEvent) {
	for _, event := range events {
		runtimeLog.Event(ctx, event.level, event.component, event.event, event.message, event.attrs...)
	}
}

func BuildGroupingFingerprint(cfg GroupingConfig) string {
	type fpPayload struct {
		LabelsKeep       []string `json:"labelsKeep"`
		LabelsKeepRegex  []string `json:"labelsKeepRegex"`
		LabelsStrip      []string `json:"labelsStrip"`
		LabelsStripRegex []string `json:"labelsStripRegex"`
	}

	p := fpPayload{
		LabelsKeep:       normalizeStringList(cfg.LabelsKeep),
		LabelsKeepRegex:  normalizeStringList(cfg.LabelsKeepRegex),
		LabelsStrip:      normalizeStringList(cfg.LabelsStrip),
		LabelsStripRegex: normalizeStringList(cfg.LabelsStripRegex),
	}

	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum)
}

func (s *Store) RunRegroupToConfig(ctx context.Context, cfg GroupingConfig, batchSize int) (RegroupResult, error) {
	return s.runRegroupToConfigWithReconciliation(ctx, cfg, batchSize)
}

func (s *Store) beginOrResumeRegroup(ctx context.Context, targetFingerprint string) (bool, bool, int, error) {
	result, err := s.beginOrResumeRegroupPhase(ctx, targetFingerprint, nil, nil)
	return result.started, result.resumed, result.total, err
}

func (s *Store) beginOrResumeRegroupPhase(ctx context.Context, targetFingerprint string, completion *regroupPhaseCompletion, finalizer *writerAfterCommitFinalizer) (regroupStartResult, error) {
	now := s.currentRegroupTime()
	return withWriterTxValueAfterCommit(ctx, s, writerOperationRegroupBegin, func(tx *sql.Tx) (regroupStartResult, error) {
		currentFingerprint, err := readMetadataStringTx(tx, metaGroupingFingerprint)
		if err != nil {
			return regroupStartResult{}, err
		}
		status, err := readMetadataStringTx(tx, metaRegroupStatus)
		if err != nil {
			return regroupStartResult{}, err
		}
		status = strings.TrimSpace(status)
		if status == "" {
			status = regroupStatusReady
		}
		activeTarget, err := readMetadataStringTx(tx, metaRegroupTargetFingerprint)
		if err != nil {
			return regroupStartResult{}, err
		}

		totalRows, err := queryInt(tx, `SELECT COUNT(*) FROM alert_states`)
		if err != nil {
			return regroupStartResult{}, fmt.Errorf("failed to count rows for regroup: %w", err)
		}

		if strings.TrimSpace(currentFingerprint) == targetFingerprint && status == regroupStatusReady {
			result := regroupStartResult{total: totalRows}
			if completion != nil {
				completion.setBegin(result)
			}
			return result, nil
		}

		resume := status == regroupStatusRunning && strings.TrimSpace(activeTarget) == targetFingerprint
		if !resume {
			if err = setMetadataIntTx(tx, metaRegroupProcessed, 0); err != nil {
				return regroupStartResult{}, err
			}
			if err = setMetadataInt64Tx(tx, metaRegroupCursorRowID, 0); err != nil {
				return regroupStartResult{}, err
			}
			if err = setMetadataTimeTx(tx, metaRegroupStartedAt, &now); err != nil {
				return regroupStartResult{}, err
			}
		}

		if err = setMetadataTx(tx, metaRegroupStatus, regroupStatusRunning); err != nil {
			return regroupStartResult{}, err
		}
		if err = setMetadataTx(tx, metaRegroupError, ""); err != nil {
			return regroupStartResult{}, err
		}
		if err = setMetadataTx(tx, metaRegroupTargetFingerprint, targetFingerprint); err != nil {
			return regroupStartResult{}, err
		}
		if err = setMetadataIntTx(tx, metaRegroupTotal, totalRows); err != nil {
			return regroupStartResult{}, err
		}
		if err = setMetadataTimeTx(tx, metaRegroupFinishedAt, nil); err != nil {
			return regroupStartResult{}, err
		}

		result := regroupStartResult{started: true, resumed: resume, total: totalRows}
		if completion != nil {
			completion.setBegin(result)
		}
		return result, nil
	}, finalizer)
}

type regroupStartResult struct {
	started bool
	resumed bool
	total   int
}

type regroupRow struct {
	RowID      int64
	AlertID    string
	Receiver   string
	LabelsJSON string
}

type compiledGroupingConfig struct {
	labelsKeep       []string
	labelsStrip      []string
	labelsKeepRegex  []*regexp.Regexp
	labelsStripRegex []*regexp.Regexp
}

func compileGroupingConfig(cfg GroupingConfig) (compiledGroupingConfig, error) {
	out := compiledGroupingConfig{
		labelsKeep:  append([]string{}, cfg.LabelsKeep...),
		labelsStrip: append([]string{}, cfg.LabelsStrip...),
	}

	for _, raw := range cfg.LabelsKeepRegex {
		re, err := regex.CompileAnchored(raw)
		if err != nil {
			return compiledGroupingConfig{}, fmt.Errorf("invalid labels keep regex %q: %w", raw, err)
		}
		out.labelsKeepRegex = append(out.labelsKeepRegex, re)
	}
	for _, raw := range cfg.LabelsStripRegex {
		re, err := regex.CompileAnchored(raw)
		if err != nil {
			return compiledGroupingConfig{}, fmt.Errorf("invalid labels strip regex %q: %w", raw, err)
		}
		out.labelsStripRegex = append(out.labelsStripRegex, re)
	}

	return out, nil
}

func (s *Store) runRegroupBatchPhase(ctx context.Context, targetFingerprint string, cfg compiledGroupingConfig, batchSize int, completion *regroupPhaseCompletion, finalizer *writerAfterCommitFinalizer) (writerTupleResult, error) {
	return withWriterTxTupleAfterCommit(ctx, s, writerOperationRegroupBatch, func(tx *sql.Tx) (bool, int, int, int, error) {
		status, err := readMetadataStringTx(tx, metaRegroupStatus)
		if err != nil {
			return false, 0, 0, 0, err
		}
		if strings.TrimSpace(status) != regroupStatusRunning {
			result := writerTupleResult{done: true}
			if completion != nil {
				completion.setBatch(result)
			}
			return result.done, result.processed, result.updated, result.total, nil
		}
		activeTarget, err := readMetadataStringTx(tx, metaRegroupTargetFingerprint)
		if err != nil {
			return false, 0, 0, 0, err
		}
		if strings.TrimSpace(activeTarget) != targetFingerprint {
			result := writerTupleResult{done: true}
			if completion != nil {
				completion.setBatch(result)
			}
			return result.done, result.processed, result.updated, result.total, nil
		}

		cursor, err := readMetadataInt64Tx(tx, metaRegroupCursorRowID)
		if err != nil {
			return false, 0, 0, 0, err
		}
		processed, err := readMetadataIntTx(tx, metaRegroupProcessed)
		if err != nil {
			return false, 0, 0, 0, err
		}
		totalRows, err := queryInt(tx, `SELECT COUNT(*) FROM alert_states`)
		if err != nil {
			return false, 0, 0, 0, fmt.Errorf("failed to count rows in regroup batch: %w", err)
		}

		rows, err := tx.Query(`
SELECT rowid, alert_id, receiver, labels_json
FROM alert_states
WHERE rowid > ?
ORDER BY rowid ASC
LIMIT ?
`, cursor, batchSize)
		if err != nil {
			return false, 0, 0, 0, fmt.Errorf("failed to select regroup batch rows: %w", err)
		}

		batch := make([]regroupRow, 0, batchSize)
		for rows.Next() {
			var row regroupRow
			if scanErr := rows.Scan(&row.RowID, &row.AlertID, &row.Receiver, &row.LabelsJSON); scanErr != nil {
				rows.Close()

				return false, 0, 0, 0, fmt.Errorf("failed to scan regroup row: %w", scanErr)
			}
			batch = append(batch, row)
		}
		if err = rows.Err(); err != nil {
			rows.Close()

			return false, 0, 0, 0, fmt.Errorf("failed to iterate regroup rows: %w", err)
		}
		rows.Close()

		if len(batch) == 0 {
			now := s.currentRegroupTime()
			if err = setMetadataTx(tx, metaGroupingFingerprint, targetFingerprint); err != nil {
				return false, 0, 0, 0, err
			}
			if err = setMetadataTx(tx, metaRegroupStatus, regroupStatusReady); err != nil {
				return false, 0, 0, 0, err
			}
			if err = setMetadataTx(tx, metaRegroupError, ""); err != nil {
				return false, 0, 0, 0, err
			}
			if err = setMetadataTimeTx(tx, metaRegroupFinishedAt, &now); err != nil {
				return false, 0, 0, 0, err
			}
			if err = setMetadataInt64Tx(tx, metaRegroupCursorRowID, 0); err != nil {
				return false, 0, 0, 0, err
			}
			if err = setMetadataIntTx(tx, metaRegroupProcessed, totalRows); err != nil {
				return false, 0, 0, 0, err
			}
			if err = setMetadataIntTx(tx, metaRegroupTotal, totalRows); err != nil {
				return false, 0, 0, 0, err
			}
			result := writerTupleResult{done: true, total: totalRows}
			if completion != nil {
				completion.setBatch(result)
			}
			return result.done, result.processed, result.updated, result.total, nil
		}

		updated := 0
		lastRowID := cursor
		for _, row := range batch {
			groupID, groupLabelsJSON, computeErr := computeGroupFromAlertLabels(row.Receiver, row.LabelsJSON, cfg)
			if computeErr != nil {
				return false, 0, 0, totalRows, fmt.Errorf("failed to regroup alert %s: %w", row.AlertID, computeErr)
			}

			updatedAt := s.currentRegroupTime().Format(time.RFC3339Nano)
			res, updateErr := tx.Exec(`
UPDATE alert_states
SET group_id = ?, group_labels_json = ?, updated_at = ?
WHERE rowid = ?
`, groupID, groupLabelsJSON, updatedAt, row.RowID)
			if updateErr != nil {
				return false, 0, 0, totalRows, fmt.Errorf("failed to update regrouped row: %w", updateErr)
			}
			affected, _ := res.RowsAffected()
			if affected > 0 {
				updated++
			}
			lastRowID = row.RowID
		}

		processed += len(batch)
		if processed > totalRows {
			totalRows = processed
		}

		if err = setMetadataInt64Tx(tx, metaRegroupCursorRowID, lastRowID); err != nil {
			return false, 0, 0, totalRows, err
		}
		if err = setMetadataIntTx(tx, metaRegroupProcessed, processed); err != nil {
			return false, 0, 0, totalRows, err
		}
		if err = setMetadataIntTx(tx, metaRegroupTotal, totalRows); err != nil {
			return false, 0, 0, totalRows, err
		}

		result := writerTupleResult{processed: len(batch), updated: updated, total: totalRows}
		if completion != nil {
			completion.setBatch(result)
		}
		return result.done, result.processed, result.updated, result.total, nil
	}, finalizer)
}

func (s *Store) failRegroup(ctx context.Context, targetFingerprint, reason string) error {
	return s.withWriterTx(ctx, writerOperationRegroupFailure, func(tx *sql.Tx) error {
		status, readErr := readMetadataStringTx(tx, metaRegroupStatus)
		if readErr != nil {
			return readErr
		}
		activeTarget, readErr := readMetadataStringTx(tx, metaRegroupTargetFingerprint)
		if readErr != nil {
			return readErr
		}
		if strings.TrimSpace(status) == regroupStatusRunning && strings.TrimSpace(activeTarget) == targetFingerprint {
			now := s.currentRegroupTime()
			if err := setMetadataTx(tx, metaRegroupStatus, regroupStatusError); err != nil {
				return err
			}
			if err := setMetadataTx(tx, metaRegroupError, strings.TrimSpace(reason)); err != nil {
				return err
			}
			if err := setMetadataTimeTx(tx, metaRegroupFinishedAt, &now); err != nil {
				return err
			}
		}

		return nil
	})
}

func computeGroupFromAlertLabels(receiver, labelsJSON string, cfg compiledGroupingConfig) (string, string, error) {
	labelsMap := map[string]string{}
	if err := json.Unmarshal([]byte(labelsJSON), &labelsMap); err != nil {
		return "", "", fmt.Errorf("failed to unmarshal labels json: %w", err)
	}

	labels := models.LabelsFromMap(labelsMap)
	groupLabels := transform.StripLabels(cfg.labelsKeep, cfg.labelsStrip, cfg.labelsKeepRegex, cfg.labelsStripRegex, labels)
	groupLabelsJSONRaw, err := json.Marshal(groupLabels.Map())
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal group labels json: %w", err)
	}

	groupID := models.AlertGroup{Receiver: receiver, Labels: groupLabels}.LabelsFingerprint()
	return groupID, string(groupLabelsJSONRaw), nil
}

func normalizeStringList(values []string) []string {
	uniq := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		vv := strings.TrimSpace(v)
		if vv == "" {
			continue
		}
		if _, found := uniq[vv]; found {
			continue
		}
		uniq[vv] = struct{}{}
		out = append(out, vv)
	}
	slices.Sort(out)
	return out
}

func validateCurrentAlert(alert AlertCurrent) error {
	if strings.TrimSpace(alert.AlertID) == "" {
		return errors.New("alertID cannot be empty")
	}
	if !alert.State.IsValid() {
		return fmt.Errorf("invalid state: %q", alert.State)
	}
	if !alert.RawState.IsValid() {
		return fmt.Errorf("invalid rawState: %q", alert.RawState)
	}
	if strings.TrimSpace(alert.Receiver) == "" {
		return errors.New("receiver cannot be empty")
	}
	if strings.TrimSpace(alert.GroupID) == "" {
		return errors.New("groupID cannot be empty")
	}
	if strings.TrimSpace(alert.GroupLabelsJSON) == "" {
		return errors.New("groupLabelsJSON cannot be empty")
	}
	if strings.TrimSpace(alert.LabelsJSON) == "" {
		return errors.New("labelsJSON cannot be empty")
	}
	if strings.TrimSpace(alert.AnnotationsJSON) == "" {
		return errors.New("annotationsJSON cannot be empty")
	}
	if alert.StartsAt.IsZero() {
		return errors.New("startsAt cannot be zero")
	}
	if strings.TrimSpace(alert.RelevantUpstreams) == "" {
		return errors.New("relevantUpstreams cannot be empty")
	}

	return nil
}

func validateTransition(transition AlertStateTransition) error {
	if strings.TrimSpace(transition.AlertID) == "" {
		return errors.New("alertID cannot be empty")
	}
	if !transition.NewState.IsValid() {
		return fmt.Errorf("invalid new state: %q", transition.NewState)
	}
	if !transition.NewRawState.IsValid() {
		switch transition.NewState {
		case StateInactive, StateResolved:
			transition.NewRawState = StateActive
		case StateUnprocessed, StateActive, StateSuppressed:
			transition.NewRawState = transition.NewState
		default:
			return fmt.Errorf("invalid new raw state: %q", transition.NewRawState)
		}
	}
	if transition.OldRawState != nil && !transition.OldRawState.IsValid() {
		return fmt.Errorf("invalid old raw state: %q", *transition.OldRawState)
	}
	if transition.OldState != nil && !transition.OldState.IsValid() {
		return fmt.Errorf("invalid old state: %q", *transition.OldState)
	}
	if !transition.ChangeSource.IsValid() {
		return fmt.Errorf("invalid change source: %q", transition.ChangeSource)
	}
	if transition.ReasonCode != nil {
		r := strings.TrimSpace(*transition.ReasonCode)
		if len(r) > reasonCodeMaxLength {
			return fmt.Errorf("reason_code length exceeds %d", reasonCodeMaxLength)
		}
		if r != "" {
			for _, ch := range r {
				if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '_' {
					return errors.New("reason_code must be snake_case")
				}
			}
		}
	}

	return nil
}

func formatNullableState(state *State) any {
	if state == nil {
		return nil
	}
	return string(*state)
}

func formatNullableBool(v *bool) any {
	if v == nil {
		return nil
	}
	if *v {
		return 1
	}
	return 0
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func intToBool(v int) bool {
	return v != 0
}

func sanitizeReasonCode(v *string) *string {
	if v == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*v)
	if trimmed == "" {
		return nil
	}
	if len(trimmed) > reasonCodeMaxLength {
		trimmed = trimmed[:reasonCodeMaxLength]
	}
	return &trimmed
}

func ptrString(v string) *string {
	return &v
}

func formatNullableTime(ts *time.Time) any {
	if ts == nil {
		return nil
	}
	return ts.UTC().Format(time.RFC3339Nano)
}

func nullableTimeString(ts *time.Time) string {
	if ts == nil {
		return ""
	}
	return ts.UTC().Format(time.RFC3339Nano)
}

func (s State) IsValid() bool {
	switch s {
	case StateActive, StateSuppressed, StateUnprocessed, StateInactive, StateResolved:
		return true
	default:
		return false
	}
}

func projectedState(rawState State, isLive, isSuppressed, isResolved bool) State {
	return (LifecycleDimensions{
		RawState: rawState, IsLive: isLive, IsSuppressed: isSuppressed, IsResolved: isResolved,
	}).ProjectedState()
}

func (s ChangeSource) IsValid() bool {
	switch s {
	case ChangeSourcePoller, ChangeSourceManual, ChangeSourceSilence:
		return true
	default:
		return false
	}
}

func validateLiveSnapshot(alert LiveAlertSnapshot) error {
	if strings.TrimSpace(alert.AlertID) == "" {
		return errors.New("live snapshot alertID cannot be empty")
	}
	if !alert.State.IsValid() {
		return fmt.Errorf("invalid live snapshot state: %q", alert.State)
	}
	if alert.State == StateInactive || alert.State == StateResolved {
		return fmt.Errorf("invalid live snapshot state %q, live states must be active/suppressed/unprocessed", alert.State)
	}
	if strings.TrimSpace(alert.Receiver) == "" {
		return errors.New("live snapshot receiver cannot be empty")
	}
	if strings.TrimSpace(alert.GroupID) == "" {
		return errors.New("live snapshot groupID cannot be empty")
	}
	if strings.TrimSpace(alert.GroupLabelsJSON) == "" {
		return errors.New("live snapshot groupLabelsJSON cannot be empty")
	}
	if strings.TrimSpace(alert.LabelsJSON) == "" {
		return errors.New("live snapshot labelsJSON cannot be empty")
	}
	if strings.TrimSpace(alert.AnnotationsJSON) == "" {
		return errors.New("live snapshot annotationsJSON cannot be empty")
	}
	if alert.StartsAt.IsZero() {
		return errors.New("live snapshot startsAt cannot be zero")
	}
	if alert.LastSeenAt.IsZero() {
		return errors.New("live snapshot lastSeenAt cannot be zero")
	}
	if len(alert.RelevantUpstreams) == 0 && len(alert.RelevantSources) == 0 {
		return errors.New("live snapshot relevant sources cannot be empty")
	}
	for _, upstream := range alert.RelevantUpstreams {
		if strings.TrimSpace(upstream) == "" {
			return errors.New("live snapshot relevantUpstreams contains empty name")
		}
	}
	for _, source := range alert.RelevantSources {
		if strings.TrimSpace(source.Name) == "" {
			return errors.New("live snapshot relevantSources contains empty name")
		}
		if strings.TrimSpace(source.Cluster) == "" {
			return errors.New("live snapshot relevantSources contains empty cluster")
		}
	}
	for _, link := range alert.SourceLinks {
		if strings.TrimSpace(link.Name) == "" || strings.TrimSpace(link.Cluster) == "" || strings.TrimSpace(link.URL) == "" {
			return errors.New("live snapshot sourceLinks contains an incomplete source link")
		}
	}
	for _, target := range alert.Targets {
		if strings.TrimSpace(target.Target) == "" {
			return errors.New("live snapshot targets contains empty target")
		}
		if !target.State.IsValid() {
			return fmt.Errorf("invalid live snapshot target state for %s: %q", target.Target, target.State)
		}
		if target.State == StateInactive || target.State == StateResolved {
			return fmt.Errorf("invalid live snapshot target state %q for %s", target.State, target.Target)
		}
		if target.StartsAt.IsZero() {
			return fmt.Errorf("live snapshot target startsAt cannot be zero for %s", target.Target)
		}
		if len(target.SourceNames) == 0 {
			return fmt.Errorf("live snapshot target sources cannot be empty for %s", target.Target)
		}
		for _, name := range target.SourceNames {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("live snapshot target sources contains empty source for %s", target.Target)
			}
		}
	}

	return nil
}

func normalizeLiveTargets(live LiveAlertSnapshot) []LiveTargetSnapshot {
	if len(live.Targets) > 0 {
		out := make([]LiveTargetSnapshot, 0, len(live.Targets))
		for _, target := range live.Targets {
			t := strings.TrimSpace(target.Target)
			if t == "" {
				continue
			}
			sources := uniqueSortedStrings(target.SourceNames)
			if len(sources) == 0 {
				continue
			}
			out = append(out, LiveTargetSnapshot{
				Target:      t,
				State:       target.State,
				StartsAt:    target.StartsAt,
				SourceNames: sources,
			})
		}
		slices.SortFunc(out, func(a, b LiveTargetSnapshot) int {
			return strings.Compare(a.Target, b.Target)
		})
		return out
	}

	byTarget := map[string]*LiveTargetSnapshot{}
	for _, source := range live.RelevantSources {
		target := strings.TrimSpace(source.Cluster)
		name := strings.TrimSpace(source.Name)
		if target == "" || name == "" {
			continue
		}
		entry, ok := byTarget[target]
		if !ok {
			entry = &LiveTargetSnapshot{Target: target, State: live.State, StartsAt: live.StartsAt}
			byTarget[target] = entry
		}
		entry.SourceNames = append(entry.SourceNames, name)
		if live.StartsAt.Before(entry.StartsAt) {
			entry.StartsAt = live.StartsAt
		}
	}
	out := make([]LiveTargetSnapshot, 0, len(byTarget))
	for _, entry := range byTarget {
		entry.SourceNames = uniqueSortedStrings(entry.SourceNames)
		if len(entry.SourceNames) == 0 {
			continue
		}
		out = append(out, *entry)
	}
	slices.SortFunc(out, func(a, b LiveTargetSnapshot) int {
		return strings.Compare(a.Target, b.Target)
	})
	return out
}

func readAllCurrentAlertsTx(tx *sql.Tx) (map[string]AlertCurrent, error) {
	rows, err := tx.Query(`
SELECT
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  source_links_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
FROM alert_states
`)
	if err != nil {
		return nil, fmt.Errorf("failed to query current alerts: %w", err)
	}
	defer rows.Close()

	result := map[string]AlertCurrent{}
	for rows.Next() {
		current, scanErr := scanCurrentAlert(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result[current.AlertID] = current
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate current alerts: %w", err)
	}

	return result, nil
}

func scanCurrentAlert(scanner interface{ Scan(dest ...any) error }) (AlertCurrent, error) {
	var (
		stateRaw               string
		rawStateRaw            string
		isLiveRaw              int
		isSuppressedRaw        int
		isResolvedRaw          int
		matchLabelsRaw         sql.NullString
		startsAtRaw            string
		lastSeenRaw            sql.NullString
		suppressionObservedRaw sql.NullString
		resolvedAtRaw          sql.NullString
		resolvedByRaw          sql.NullString
		resolvedCommentRaw     sql.NullString
		updatedAtRaw           string
		current                AlertCurrent
	)

	err := scanner.Scan(
		&current.AlertID,
		&stateRaw,
		&rawStateRaw,
		&isLiveRaw,
		&isSuppressedRaw,
		&isResolvedRaw,
		&current.Receiver,
		&current.GroupID,
		&current.GroupLabelsJSON,
		&current.LabelsJSON,
		&matchLabelsRaw,
		&current.AnnotationsJSON,
		&startsAtRaw,
		&lastSeenRaw,
		&current.RelevantUpstreams,
		&current.RelevantSources,
		&current.SourceLinksJSON,
		&suppressionObservedRaw,
		&resolvedAtRaw,
		&resolvedByRaw,
		&resolvedCommentRaw,
		&updatedAtRaw,
	)
	if err != nil {
		return AlertCurrent{}, err
	}

	state := State(stateRaw)
	if !state.IsValid() {
		return AlertCurrent{}, fmt.Errorf("invalid state in database: %q", stateRaw)
	}
	current.State = state
	rawState := State(rawStateRaw)
	if !rawState.IsValid() {
		return AlertCurrent{}, fmt.Errorf("invalid raw_state in database: %q", rawStateRaw)
	}
	current.RawState = rawState
	current.IsLive = intToBool(isLiveRaw)
	current.IsSuppressed = intToBool(isSuppressedRaw)
	current.IsResolved = intToBool(isResolvedRaw)
	if matchLabelsRaw.Valid {
		v := strings.TrimSpace(matchLabelsRaw.String)
		if v != "" {
			current.MatchLabelsJSON = &v
		}
	}

	startsAt, err := time.Parse(time.RFC3339Nano, startsAtRaw)
	if err != nil {
		return AlertCurrent{}, fmt.Errorf("failed to parse starts_at: %w", err)
	}
	current.StartsAt = startsAt

	if lastSeenRaw.Valid {
		lastSeenAt, parseErr := time.Parse(time.RFC3339Nano, lastSeenRaw.String)
		if parseErr != nil {
			return AlertCurrent{}, fmt.Errorf("failed to parse last_seen_at: %w", parseErr)
		}
		current.LastSeenAt = &lastSeenAt
	}
	if resolvedAtRaw.Valid {
		resolvedAt, parseErr := time.Parse(time.RFC3339Nano, resolvedAtRaw.String)
		if parseErr != nil {
			return AlertCurrent{}, fmt.Errorf("failed to parse resolved_at: %w", parseErr)
		}
		current.ResolvedAt = &resolvedAt
	}
	if suppressionObservedRaw.Valid {
		suppressionObservedAt, parseErr := time.Parse(time.RFC3339Nano, suppressionObservedRaw.String)
		if parseErr != nil {
			return AlertCurrent{}, fmt.Errorf("failed to parse suppression_observed_at: %w", parseErr)
		}
		current.SuppressionObservedAt = &suppressionObservedAt
	}
	if resolvedByRaw.Valid {
		v := strings.TrimSpace(resolvedByRaw.String)
		if v != "" {
			current.ResolvedBy = &v
		}
	}
	if resolvedCommentRaw.Valid {
		v := strings.TrimSpace(resolvedCommentRaw.String)
		if v != "" {
			current.ResolvedComment = &v
		}
	}

	updatedAt, err := time.Parse(time.RFC3339Nano, updatedAtRaw)
	if err != nil {
		return AlertCurrent{}, fmt.Errorf("failed to parse updated_at: %w", err)
	}
	current.UpdatedAt = updatedAt

	return current, nil
}

func insertCurrentAlertTx(tx *sql.Tx, live LiveAlertSnapshot, now time.Time) error {
	relevantJSON, err := marshalRelevantUpstreams(live.RelevantUpstreams)
	if err != nil {
		return err
	}
	relevantSourcesJSON, err := marshalRelevantSources(live.RelevantSources)
	if err != nil {
		return err
	}
	sourceLinksJSON, err := marshalAlertSourceLinks(live.SourceLinks)
	if err != nil {
		return err
	}
	livePlan := ObserveLive(LifecycleDimensions{}, live.State)
	rawState := livePlan.RawState
	isLive := livePlan.IsLive
	isSuppressed := livePlan.IsSuppressed
	isResolved := livePlan.IsResolved
	state := livePlan.ProjectedState()

	_, err = tx.Exec(`
INSERT INTO alert_states(
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  source_links_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
		live.AlertID,
		string(state),
		string(rawState),
		boolToInt(isLive),
		boolToInt(isSuppressed),
		boolToInt(isResolved),
		live.Receiver,
		live.GroupID,
		live.GroupLabelsJSON,
		live.LabelsJSON,
		live.MatchLabelsJSON,
		live.AnnotationsJSON,
		live.StartsAt.UTC().Format(time.RFC3339Nano),
		live.LastSeenAt.UTC().Format(time.RFC3339Nano),
		string(relevantJSON),
		string(relevantSourcesJSON),
		string(sourceLinksJSON),
		formatNullableTime(nil),
		nil,
		nil,
		nil,
		now.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("failed to insert current alert: %w", err)
	}

	return nil
}

func updateCurrentAlertFromLiveTx(tx *sql.Tx, alertID string, live LiveAlertSnapshot, newRawState State, newIsLive, newIsSuppressed, newIsResolved bool, relevantJSON, relevantSourcesJSON, sourceLinksJSON string, now time.Time) error {
	newState := projectedState(newRawState, newIsLive, newIsSuppressed, newIsResolved)
	_, err := tx.Exec(`
UPDATE alert_states SET
  state = ?,
  raw_state = ?,
  is_live = ?,
  is_suppressed = ?,
  is_resolved = ?,
  receiver = ?,
  group_id = ?,
  group_labels_json = ?,
  labels_json = ?,
  match_labels_json = ?,
  annotations_json = ?,
  starts_at = ?,
  last_seen_at = ?,
  relevant_upstreams_json = ?,
  relevant_sources_json = ?,
  source_links_json = ?,
  suppression_observed_at = CASE WHEN ? = 1 THEN COALESCE(suppression_observed_at, ?) ELSE NULL END,
  resolved_at = NULL,
  resolved_by = NULL,
  resolved_comment = NULL,
  updated_at = ?
WHERE alert_id = ?
`,
		string(newState),
		string(newRawState),
		boolToInt(newIsLive),
		boolToInt(newIsSuppressed),
		boolToInt(newIsResolved),
		live.Receiver,
		live.GroupID,
		live.GroupLabelsJSON,
		live.LabelsJSON,
		live.MatchLabelsJSON,
		live.AnnotationsJSON,
		live.StartsAt.UTC().Format(time.RFC3339Nano),
		live.LastSeenAt.UTC().Format(time.RFC3339Nano),
		relevantJSON,
		relevantSourcesJSON,
		sourceLinksJSON,
		boolToInt(newIsSuppressed),
		now.UTC().Format(time.RFC3339Nano),
		now.UTC().Format(time.RFC3339Nano),
		alertID,
	)
	if err != nil {
		return fmt.Errorf("failed to update current alert from live: %w", err)
	}

	return nil
}

func updateProjectedStateTx(tx *sql.Tx, alertID string, rawState State, isLive, isSuppressed, isResolved bool, now time.Time) error {
	newState := projectedState(rawState, isLive, isSuppressed, isResolved)
	_, err := tx.Exec(`
UPDATE alert_states
SET state = ?, raw_state = ?, is_live = ?, is_suppressed = ?, is_resolved = ?, suppression_observed_at = CASE WHEN ? = 1 THEN COALESCE(suppression_observed_at, ?) ELSE NULL END, updated_at = ?
WHERE alert_id = ?
`, string(newState), string(rawState), boolToInt(isLive), boolToInt(isSuppressed), boolToInt(isResolved), boolToInt(isSuppressed), now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), alertID)
	if err != nil {
		return fmt.Errorf("failed to update alert state: %w", err)
	}

	return nil
}

type targetStateRow struct {
	Target                 string
	State                  State
	RawState               State
	IsLive                 bool
	IsSuppressed           bool
	IsResolved             bool
	StartsAt               time.Time
	LastSeenAt             *time.Time
	SourceMembers          []string
	MatchLabelsFingerprint string
	RetiredAt              *time.Time
}

func readTargetStatesTx(tx *sql.Tx, alertID string) (map[string]targetStateRow, error) {
	rows, err := tx.Query(`
SELECT target, state, raw_state, is_live, is_suppressed, is_resolved, starts_at, last_seen_at, source_members_json, match_labels_fingerprint, retired_at
FROM alert_target_states
WHERE alert_id = ?
`, alertID)
	if err != nil {
		return nil, fmt.Errorf("failed to read target states: %w", err)
	}
	defer rows.Close()
	out := map[string]targetStateRow{}
	for rows.Next() {
		var (
			target                 string
			stateRaw               string
			rawStateRaw            string
			isLiveRaw              int
			isSuppressedRaw        int
			isResolvedRaw          int
			startsAtRaw            string
			lastSeenRaw            sql.NullString
			sourceMembersJSON      string
			matchLabelsFingerprint string
			retiredAtRaw           sql.NullString
		)
		if err := rows.Scan(&target, &stateRaw, &rawStateRaw, &isLiveRaw, &isSuppressedRaw, &isResolvedRaw, &startsAtRaw, &lastSeenRaw, &sourceMembersJSON, &matchLabelsFingerprint, &retiredAtRaw); err != nil {
			return nil, fmt.Errorf("failed to scan target state row: %w", err)
		}
		state := State(stateRaw)
		rawState := State(rawStateRaw)
		if !state.IsValid() || !rawState.IsValid() {
			continue
		}
		startsAt, err := time.Parse(time.RFC3339Nano, startsAtRaw)
		if err != nil {
			continue
		}
		var sourceMembers []string
		if strings.TrimSpace(sourceMembersJSON) != "" {
			_ = json.Unmarshal([]byte(sourceMembersJSON), &sourceMembers)
		}
		var lastSeenAt *time.Time
		if lastSeenRaw.Valid {
			ts, err := time.Parse(time.RFC3339Nano, lastSeenRaw.String)
			if err == nil {
				lastSeenAt = &ts
			}
		}
		var retiredAt *time.Time
		if retiredAtRaw.Valid {
			if ts, parseErr := time.Parse(time.RFC3339Nano, retiredAtRaw.String); parseErr == nil {
				retiredAt = &ts
			}
		}
		out[target] = targetStateRow{
			Target:                 target,
			State:                  state,
			RawState:               rawState,
			IsLive:                 intToBool(isLiveRaw),
			IsSuppressed:           intToBool(isSuppressedRaw),
			IsResolved:             intToBool(isResolvedRaw),
			StartsAt:               startsAt,
			LastSeenAt:             lastSeenAt,
			SourceMembers:          uniqueSortedStrings(sourceMembers),
			MatchLabelsFingerprint: matchLabelsFingerprint,
			RetiredAt:              retiredAt,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate target state rows: %w", err)
	}
	return out, nil
}

func upsertTargetStateTx(tx *sql.Tx, alertID string, row targetStateRow, reasonCode string, changedAt time.Time) error {
	return upsertTargetStateWithEventsTx(tx, alertID, row, reasonCode, changedAt, nil)
}

func upsertTargetStateWithEventsTx(tx *sql.Tx, alertID string, row targetStateRow, reasonCode string, changedAt time.Time, collector *runtimeEventCollector) error {
	sourceMembersJSON, err := json.Marshal(uniqueSortedStrings(row.SourceMembers))
	if err != nil {
		return fmt.Errorf("failed to marshal target source members: %w", err)
	}
	var oldStateRaw sql.NullString
	var oldRawStateRaw sql.NullString
	var oldIsLiveRaw sql.NullInt64
	var oldIsSuppressedRaw sql.NullInt64
	var oldIsResolvedRaw sql.NullInt64
	var oldMatchLabelsFingerprint string
	var oldRetiredAtRaw sql.NullString
	if row.MatchLabelsFingerprint == "" {
		row.MatchLabelsFingerprint, err = readAlertMatchLabelsFingerprintTx(tx, alertID)
		if err != nil {
			return err
		}
	}
	err = tx.QueryRow(`SELECT state, raw_state, is_live, is_suppressed, is_resolved, match_labels_fingerprint, retired_at FROM alert_target_states WHERE alert_id = ? AND target = ?`, alertID, row.Target).Scan(&oldStateRaw, &oldRawStateRaw, &oldIsLiveRaw, &oldIsSuppressedRaw, &oldIsResolvedRaw, &oldMatchLabelsFingerprint, &oldRetiredAtRaw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to read old target state: %w", err)
	}
	_, err = tx.Exec(`
INSERT INTO alert_target_states(
  alert_id,
  target,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  starts_at,
  last_seen_at,
  source_members_json,
  match_labels_fingerprint,
  retired_at,
  updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(alert_id, target) DO UPDATE SET
  state=excluded.state,
  raw_state=excluded.raw_state,
  is_live=excluded.is_live,
  is_suppressed=excluded.is_suppressed,
  is_resolved=excluded.is_resolved,
  starts_at=excluded.starts_at,
  last_seen_at=excluded.last_seen_at,
  source_members_json=excluded.source_members_json,
  match_labels_fingerprint=excluded.match_labels_fingerprint,
  retired_at=excluded.retired_at,
  updated_at=excluded.updated_at
`,
		alertID,
		row.Target,
		string(row.State),
		string(row.RawState),
		boolToInt(row.IsLive),
		boolToInt(row.IsSuppressed),
		boolToInt(row.IsResolved),
		row.StartsAt.UTC().Format(time.RFC3339Nano),
		formatNullableTime(row.LastSeenAt),
		string(sourceMembersJSON),
		row.MatchLabelsFingerprint,
		formatNullableTime(row.RetiredAt),
		changedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("failed to upsert target state: %w", err)
	}

	changed := true
	if oldStateRaw.Valid && oldRawStateRaw.Valid && oldIsLiveRaw.Valid && oldIsSuppressedRaw.Valid && oldIsResolvedRaw.Valid {
		changed = oldStateRaw.String != string(row.State) ||
			oldRawStateRaw.String != string(row.RawState) ||
			int(oldIsLiveRaw.Int64) != boolToInt(row.IsLive) ||
			int(oldIsSuppressedRaw.Int64) != boolToInt(row.IsSuppressed) ||
			int(oldIsResolvedRaw.Int64) != boolToInt(row.IsResolved) ||
			oldRetiredAtRaw.Valid != (row.RetiredAt != nil) ||
			oldRetiredAtRaw.String != nullableTimeString(row.RetiredAt)
	}
	if changed {
		_, err = tx.Exec(`
INSERT INTO alert_target_events(
  alert_id,
  target,
  old_state,
  new_state,
  old_raw_state,
  new_raw_state,
  old_is_live,
  new_is_live,
  old_is_suppressed,
  new_is_suppressed,
  old_is_resolved,
  new_is_resolved,
  old_retired_at,
  new_retired_at,
  changed_at,
  reason_code
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
			alertID,
			row.Target,
			nullableStringFromNull(oldStateRaw),
			string(row.State),
			nullableStringFromNull(oldRawStateRaw),
			string(row.RawState),
			nullableIntFromNull(oldIsLiveRaw),
			boolToInt(row.IsLive),
			nullableIntFromNull(oldIsSuppressedRaw),
			boolToInt(row.IsSuppressed),
			nullableIntFromNull(oldIsResolvedRaw),
			boolToInt(row.IsResolved),
			nullableStringFromNull(oldRetiredAtRaw),
			formatNullableTime(row.RetiredAt),
			changedAt.UTC().Format(time.RFC3339Nano),
			sanitizeReasonCode(ptrString(reasonCode)),
		)
		if err != nil {
			return fmt.Errorf("failed to insert target state event: %w", err)
		}
		eventName := "target_transition"
		eventLevel := slog.LevelDebug
		if reasonCode == "poll_target_retired" {
			eventName = "target_retired"
			eventLevel = slog.LevelInfo
		}
		collector.append(runtimeEvent{level: eventLevel, component: "alert_lifecycle", event: eventName, message: "Alert target lifecycle state changed", attrs: []slog.Attr{
			slog.String("alert_id", alertID), slog.String("target", row.Target),
			slog.String("old_state", nullableStringValue(nullableStringFromNull(oldStateRaw))), slog.String("new_state", string(row.State)),
			slog.String("old_raw_state", nullableStringValue(nullableStringFromNull(oldRawStateRaw))), slog.String("new_raw_state", string(row.RawState)),
			runtimeEventNullableIntBoolAttr("old_is_live", oldIsLiveRaw), slog.Bool("new_is_live", row.IsLive),
			runtimeEventNullableIntBoolAttr("old_is_suppressed", oldIsSuppressedRaw), slog.Bool("new_is_suppressed", row.IsSuppressed),
			runtimeEventNullableIntBoolAttr("old_is_resolved", oldIsResolvedRaw), slog.Bool("new_is_resolved", row.IsResolved),
			slog.Bool("old_retired", oldRetiredAtRaw.Valid), slog.Bool("new_retired", row.RetiredAt != nil),
			runtimeEventStringAttr("reason_code", nullableStringValue(sanitizeReasonCode(ptrString(reasonCode)))),
		}})
		if reasonCode == "poll_target_disappeared" && oldIsLiveRaw.Valid && oldIsLiveRaw.Int64 == 1 && !row.IsLive && row.State == StateInactive {
			collector.appendEligibilityScope(recheckCandidateScope{AlertID: alertID, TargetKey: row.Target})
		}
	}

	if row.RetiredAt != nil {
		if _, err := tx.Exec(`DELETE FROM alert_target_match_labels WHERE alert_id = ? AND target = ?`, alertID, row.Target); err != nil {
			return fmt.Errorf("failed to remove retired target label index: %w", err)
		}
	} else if !oldStateRaw.Valid || oldRetiredAtRaw.Valid || oldMatchLabelsFingerprint != row.MatchLabelsFingerprint {
		if err := upsertTargetMatchLabelsTx(tx, alertID, row.Target, changedAt); err != nil {
			return err
		}
	}
	if !oldStateRaw.Valid || oldRetiredAtRaw.Valid != (row.RetiredAt != nil) || oldMatchLabelsFingerprint != row.MatchLabelsFingerprint {
		key := metaSuggestionRevisionPrefix + row.Target
		revision, readErr := readMetadataInt64Tx(tx, key)
		if readErr != nil {
			return fmt.Errorf("failed to read target suggestion revision: %w", readErr)
		}
		if err := setMetadataInt64Tx(tx, key, revision+1); err != nil {
			return fmt.Errorf("failed to advance target suggestion revision: %w", err)
		}
	}
	return nil
}

// TargetSuggestionRevision returns the monotonic revision for the target's
// stored label relation. A missing row means the initial revision zero.
func (s *Store) TargetSuggestionRevision(target string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, ErrStoreNotInitialized
	}
	var raw sql.NullString
	err := s.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaSuggestionRevisionPrefix+strings.TrimSpace(target)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read target suggestion revision: %w", err)
	}
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return 0, nil
	}
	revision, parseErr := strconv.ParseInt(raw.String, 10, 64)
	if parseErr != nil || revision < 0 {
		return 0, errors.New("invalid target suggestion revision")
	}
	return revision, nil
}

func readAlertMatchLabelsFingerprintTx(tx *sql.Tx, alertID string) (string, error) {
	var raw sql.NullString
	if err := tx.QueryRow(`SELECT match_labels_json FROM alert_states WHERE alert_id = ?`, alertID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to read match labels fingerprint source: %w", err)
	}
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return "", nil
	}
	hash := sha256.Sum256([]byte(raw.String))
	return hex.EncodeToString(hash[:]), nil
}

func upsertTargetMatchLabelsTx(tx *sql.Tx, alertID, target string, changedAt time.Time) error {
	var matchLabelsRaw sql.NullString
	if err := tx.QueryRow(`SELECT match_labels_json FROM alert_states WHERE alert_id = ?`, alertID).Scan(&matchLabelsRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to read match labels for target index: %w", err)
	}
	if !matchLabelsRaw.Valid || strings.TrimSpace(matchLabelsRaw.String) == "" {
		if _, err := tx.Exec(`DELETE FROM alert_target_match_labels WHERE alert_id = ? AND target = ?`, alertID, target); err != nil {
			return fmt.Errorf("failed to clear target label index: %w", err)
		}
		return nil
	}
	labels := map[string]string{}
	if err := json.Unmarshal([]byte(matchLabelsRaw.String), &labels); err != nil {
		if _, clearErr := tx.Exec(`DELETE FROM alert_target_match_labels WHERE alert_id = ? AND target = ?`, alertID, target); clearErr != nil {
			return fmt.Errorf("failed to clear target label index after parse error: %w", clearErr)
		}
		return nil
	}

	if _, err := tx.Exec(`DELETE FROM alert_target_match_labels WHERE alert_id = ? AND target = ?`, alertID, target); err != nil {
		return fmt.Errorf("failed to replace target label index: %w", err)
	}
	nowRaw := changedAt.UTC().Format(time.RFC3339Nano)
	for name, value := range labels {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, err := tx.Exec(`
INSERT INTO alert_target_match_labels(alert_id, target, label_name, label_value, updated_at)
VALUES(?, ?, ?, ?, ?)
`, alertID, target, name, value, nowRaw); err != nil {
			return fmt.Errorf("failed to upsert target label index row: %w", err)
		}
	}
	return nil
}

func nullableStringFromNull(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	out := v.String
	return &out
}

func nullableIntFromNull(v sql.NullInt64) *int {
	if !v.Valid {
		return nil
	}
	i := int(v.Int64)
	return &i
}

func upsertTargetStatesFromLiveTx(tx *sql.Tx, alertID string, targets []LiveTargetSnapshot, matchLabelsJSON *string, successfulUpstreams map[string]bool, configuredSourcesByTarget map[string]map[string]struct{}, sourceManifestKnown bool, now time.Time) error {
	return upsertTargetStatesFromLiveWithEventsTx(tx, alertID, targets, matchLabelsJSON, successfulUpstreams, configuredSourcesByTarget, sourceManifestKnown, now, nil)
}

func upsertTargetStatesFromLiveWithEventsTx(tx *sql.Tx, alertID string, targets []LiveTargetSnapshot, matchLabelsJSON *string, successfulUpstreams map[string]bool, configuredSourcesByTarget map[string]map[string]struct{}, sourceManifestKnown bool, now time.Time, collector *runtimeEventCollector) error {
	if len(targets) == 0 {
		return nil
	}
	matchLabelsFingerprint := fingerprintMatchLabelsJSON(matchLabelsJSON)
	existing, err := readTargetStatesTx(tx, alertID)
	if err != nil {
		return err
	}

	orderedTargets := append([]LiveTargetSnapshot(nil), targets...)
	slices.SortFunc(orderedTargets, func(a, b LiveTargetSnapshot) int {
		return strings.Compare(strings.TrimSpace(a.Target), strings.TrimSpace(b.Target))
	})
	seenTargets := map[string]struct{}{}
	for _, target := range orderedTargets {
		targetName := strings.TrimSpace(target.Target)
		if targetName == "" {
			continue
		}
		seenTargets[targetName] = struct{}{}
		previous := existing[targetName]
		topology := PlanTargetTopology(TargetTopologyObservation{
			PreviousMembers: previous.SourceMembers,
			ObservedMembers: target.SourceNames,
			Configured:      configuredSourcesByTarget[targetName], ManifestKnown: sourceManifestKnown,
			Successful: successfulUpstreams,
		})
		livePlan := ObserveLive(LifecycleDimensions{
			RawState: previous.RawState, IsLive: previous.IsLive,
			IsSuppressed: previous.IsSuppressed, IsResolved: previous.IsResolved,
		}, target.State)
		row := targetStateRow{
			Target:                 targetName,
			State:                  livePlan.ProjectedState(),
			RawState:               livePlan.RawState,
			IsLive:                 livePlan.IsLive,
			IsSuppressed:           livePlan.IsSuppressed,
			IsResolved:             livePlan.IsResolved,
			StartsAt:               target.StartsAt,
			LastSeenAt:             ptrTime(target.StartsAt),
			SourceMembers:          topology.Members,
			MatchLabelsFingerprint: matchLabelsFingerprint,
		}
		lastSeen := now
		row.LastSeenAt = &lastSeen
		if err := upsertTargetStateWithEventsTx(tx, alertID, row, "poll_target_seen", now, collector); err != nil {
			return err
		}
	}

	existingTargetNames := make([]string, 0, len(existing))
	for targetName := range existing {
		existingTargetNames = append(existingTargetNames, targetName)
	}
	slices.Sort(existingTargetNames)
	for _, targetName := range existingTargetNames {
		row := existing[targetName]
		if _, ok := seenTargets[targetName]; ok {
			continue
		}
		if err := applyTargetAbsencePlanWithEventsTx(tx, alertID, row, successfulUpstreams, configuredSourcesByTarget[targetName], sourceManifestKnown, now, collector); err != nil {
			return err
		}
	}

	return nil
}

func fingerprintMatchLabelsJSON(raw *string) string {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return ""
	}
	hash := sha256.Sum256([]byte(*raw))
	return hex.EncodeToString(hash[:])
}

func markMissingTargetStatesNonLiveTx(tx *sql.Tx, current AlertCurrent, successfulUpstreams map[string]bool, configuredSourcesByTarget map[string]map[string]struct{}, sourceManifestKnown bool, now time.Time) error {
	return markMissingTargetStatesNonLiveWithEventsTx(tx, current, successfulUpstreams, configuredSourcesByTarget, sourceManifestKnown, now, nil)
}

func markMissingTargetStatesNonLiveWithEventsTx(tx *sql.Tx, current AlertCurrent, successfulUpstreams map[string]bool, configuredSourcesByTarget map[string]map[string]struct{}, sourceManifestKnown bool, now time.Time, collector *runtimeEventCollector) error {
	existing, err := readTargetStatesTx(tx, current.AlertID)
	if err != nil {
		return err
	}

	targetNames := make([]string, 0, len(existing))
	for targetName := range existing {
		targetNames = append(targetNames, targetName)
	}
	slices.Sort(targetNames)
	for _, targetName := range targetNames {
		row := existing[targetName]
		if err := applyTargetAbsencePlanWithEventsTx(tx, current.AlertID, row, successfulUpstreams, configuredSourcesByTarget[row.Target], sourceManifestKnown, now, collector); err != nil {
			return err
		}
	}
	return nil
}

func applyTargetAbsencePlanWithEventsTx(tx *sql.Tx, alertID string, row targetStateRow, successfulUpstreams map[string]bool, configuredSources map[string]struct{}, sourceManifestKnown bool, now time.Time, collector *runtimeEventCollector) error {
	topology := PlanTargetTopology(TargetTopologyObservation{
		PreviousMembers: row.SourceMembers,
		Configured:      configuredSources, ManifestKnown: sourceManifestKnown,
		Successful: successfulUpstreams,
	})
	if topology.Retire {
		return retireTargetStateWithEventsTx(tx, alertID, row, now, collector)
	}
	membersChanged := !slices.Equal(topology.Members, row.SourceMembers)
	row.SourceMembers = topology.Members
	if !topology.Complete || !row.IsLive {
		if membersChanged {
			return upsertTargetStateWithEventsTx(tx, alertID, row, "poll_target_topology_reconciled", now, collector)
		}
		return nil
	}
	absencePlan := ObserveAbsent(LifecycleDimensions{
		RawState: row.RawState, IsLive: row.IsLive,
		IsSuppressed: row.IsSuppressed, IsResolved: row.IsResolved,
	})
	row.RawState = absencePlan.RawState
	row.IsLive = absencePlan.IsLive
	row.IsSuppressed = absencePlan.IsSuppressed
	row.IsResolved = absencePlan.IsResolved
	row.State = absencePlan.ProjectedState()
	if err := upsertTargetStateWithEventsTx(tx, alertID, row, "poll_target_disappeared", now, collector); err != nil {
		return err
	}
	return nil
}

func retireTargetStateWithEventsTx(tx *sql.Tx, alertID string, row targetStateRow, now time.Time, collector *runtimeEventCollector) error {
	retiredAt := now.UTC()
	row.SourceMembers = nil
	row.RetiredAt = &retiredAt
	return upsertTargetStateWithEventsTx(tx, alertID, row, "poll_target_retired", now, collector)
}

func configuredSourceNames(sources []SourceSilenceState) (map[string]struct{}, bool) {
	if sources == nil {
		return nil, false
	}
	out := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		name := strings.TrimSpace(source.SourceName)
		if name != "" {
			out[name] = struct{}{}
		}
	}
	return out, true
}

func configuredSourceNamesByTarget(sources []SourceSilenceState) map[string]map[string]struct{} {
	out := map[string]map[string]struct{}{}
	for _, source := range sources {
		target := strings.TrimSpace(source.ClusterID)
		name := strings.TrimSpace(source.SourceName)
		if target == "" || name == "" {
			continue
		}
		if out[target] == nil {
			out[target] = map[string]struct{}{}
		}
		out[target][name] = struct{}{}
	}
	return out
}

func filterConfiguredSourceNames(sources []string, configured map[string]struct{}, manifestKnown bool) []string {
	if !manifestKnown {
		return uniqueSortedStrings(sources)
	}
	out := make([]string, 0, len(sources))
	for _, source := range uniqueSortedStrings(sources) {
		if _, ok := configured[source]; ok {
			out = append(out, source)
		}
	}
	return out
}

func ptrTime(v time.Time) *time.Time {
	out := v
	return &out
}

func updateManualResolutionTx(tx *sql.Tx, alertID string, changedBy, comment *string, now time.Time) error {
	_, err := tx.Exec(`
UPDATE alert_states
SET is_resolved = 1,
    state = CASE WHEN is_live = 0 THEN 'resolved' ELSE state END,
    resolved_at = ?,
    resolved_by = ?,
    resolved_comment = ?,
    updated_at = ?
WHERE alert_id = ?
`, now.UTC().Format(time.RFC3339Nano), changedBy, comment, now.UTC().Format(time.RFC3339Nano), alertID)
	if err != nil {
		return fmt.Errorf("failed to apply manual resolution: %w", err)
	}

	return nil
}

func updateTargetStatesResolvedTx(tx *sql.Tx, alertID string, isResolved bool, now time.Time, reasonCode string) error {
	existing, err := readTargetStatesTx(tx, alertID)
	if err != nil {
		return err
	}
	for _, row := range existing {
		row.IsResolved = isResolved
		row.State = projectedState(row.RawState, row.IsLive, row.IsSuppressed, row.IsResolved)
		if err := upsertTargetStateTx(tx, alertID, row, reasonCode, now); err != nil {
			return err
		}
	}
	return nil
}

func insertTransitionTx(tx *sql.Tx, transition AlertStateTransition) error {
	_, err := insertTransitionTxWithID(tx, transition)
	return err
}

func insertTransitionTxWithID(tx *sql.Tx, transition AlertStateTransition) (int64, error) {
	if !transition.NewRawState.IsValid() {
		if transition.NewState == StateInactive || transition.NewState == StateResolved {
			transition.NewRawState = StateActive
		} else {
			transition.NewRawState = transition.NewState
		}
	}
	if transition.OldRawState == nil && transition.OldState != nil {
		v := *transition.OldState
		transition.OldRawState = &v
	}
	if err := validateTransition(transition); err != nil {
		return 0, err
	}

	changedAt := transition.ChangedAt
	if changedAt.IsZero() {
		changedAt = time.Now().UTC()
	}

	res, err := tx.Exec(`
INSERT INTO alert_events(
  alert_id,
  old_state,
  new_state,
  old_raw_state,
  new_raw_state,
  old_is_live,
  new_is_live,
  old_is_suppressed,
  new_is_suppressed,
  old_is_resolved,
  new_is_resolved,
  changed_at,
  change_source,
  changed_by,
  reason_code,
  reason_text
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
		transition.AlertID,
		formatNullableState(transition.OldState),
		string(transition.NewState),
		formatNullableState(transition.OldRawState),
		string(transition.NewRawState),
		formatNullableBool(transition.OldIsLive),
		boolToInt(transition.NewIsLive),
		formatNullableBool(transition.OldIsSuppressed),
		boolToInt(transition.NewIsSuppressed),
		formatNullableBool(transition.OldIsResolved),
		boolToInt(transition.NewIsResolved),
		changedAt.UTC().Format(time.RFC3339Nano),
		string(transition.ChangeSource),
		transition.ChangedBy,
		sanitizeReasonCode(transition.ReasonCode),
		sanitizeNullableString(transition.ReasonText),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert state transition: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get inserted transition id: %w", err)
	}

	return id, nil
}

type recheckJobRule struct {
	SourceName            string
	ClusterID             string
	TargetKey             string
	RuleID                int64
	RevisionID            int64
	SilenceID             string
	ContentFingerprint    string
	ObservedState         string
	EvaluationFingerprint string
	MatchersJSON          string
}

type suppressionCauseMember struct {
	sourceName string
	ruleID     int64
	revisionID *int64
	known      bool
}

func (s *Store) RunSilenceRecheckBatch(batchSize int) (batchResult RecheckBatchResult, resultErr error) {
	return s.RunSilenceRecheckBatchContext(context.Background(), batchSize)
}

func recheckCandidateSkippedEvent(jobID int64, alertID, reason string) runtimeEvent {
	return runtimeEvent{level: slog.LevelDebug, component: "silence_recheck", event: "candidate_skipped", message: "Silence recheck candidate skipped", attrs: []slog.Attr{slog.Int64("job_id", jobID), slog.String("alert_id", alertID), slog.String("reason_code", reason)}}
}

type recheckCandidate struct {
	AlertID                 string
	TargetKey               string
	RawState                string
	LastSeenAt              sql.NullString
	TargetLastSeenAt        sql.NullString
	MatchLabelsJSON         string
	MatchLabelsValid        bool
	RelevantSourcesJSON     string
	TargetSourceMembersJSON sql.NullString
}

func selectNextRecheckJobTx(tx *sql.Tx) (int64, *string, string, error) {
	jobID, cursor, status, _, err := selectNextRecheckJobWithDueTx(tx)
	return jobID, cursor, status, err
}

func selectNextRecheckJobWithDueTx(tx *sql.Tx) (int64, *string, string, *string, error) {
	var (
		jobID       int64
		cursor      sql.NullString
		status      string
		nextAttempt sql.NullString
	)
	err := tx.QueryRow(`
SELECT job_id, cursor_alert_id, status, next_attempt_at
FROM silence_recheck_jobs
WHERE status IN ('queued','running','retry_wait','failed')
ORDER BY job_id ASC
LIMIT 1
`).Scan(&jobID, &cursor, &status, &nextAttempt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil, "", nil, nil
		}
		return 0, nil, "", nil, fmt.Errorf("failed to select next recheck job: %w", err)
	}
	var next *string
	if nextAttempt.Valid {
		value := nextAttempt.String
		next = &value
	}
	if cursor.Valid {
		v := cursor.String
		return jobID, &v, status, next, nil
	}
	return jobID, nil, status, next, nil
}

func loadRecheckJobRulesTx(tx *sql.Tx, jobID int64) ([]recheckJobRule, error) {
	rows, err := tx.Query(`
	SELECT jr.source_name, jr.cluster_id, jr.target_key, jr.rule_id, jr.revision_id,
	       jr.silence_id, jr.content_fingerprint, jr.observed_state,
	       jr.evaluation_fingerprint, jr.matchers_json
FROM silence_recheck_job_rules jr
WHERE jr.job_id = ?
	ORDER BY jr.source_name, jr.target_key, jr.silence_id, jr.content_fingerprint, jr.evaluation_fingerprint, jr.matchers_json
`, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to load recheck job rules: %w", err)
	}
	defer rows.Close()

	out := []recheckJobRule{}
	for rows.Next() {
		var r recheckJobRule
		if err = rows.Scan(&r.SourceName, &r.ClusterID, &r.TargetKey, &r.RuleID, &r.RevisionID, &r.SilenceID, &r.ContentFingerprint, &r.ObservedState, &r.EvaluationFingerprint, &r.MatchersJSON); err != nil {
			return nil, fmt.Errorf("failed to scan recheck job rule: %w", err)
		}
		out = append(out, r)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating recheck job rules: %w", err)
	}
	return out, nil
}

func loadRecheckCandidatesTx(tx *sql.Tx, jobID int64, batchSize int) ([]recheckCandidate, error) {
	rows, err := tx.Query(`
SELECT alert_id, target_key, expected_alert_last_seen_at, expected_target_last_seen_at,
       match_labels_json, relevant_sources_json, target_source_members_json
FROM silence_recheck_job_candidates
WHERE job_id=? AND processed_at IS NULL AND outcome IS NULL
ORDER BY alert_id, target_key
LIMIT ?
`, jobID, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to load recheck candidates: %w", err)
	}
	defer rows.Close()

	out := []recheckCandidate{}
	for rows.Next() {
		var c recheckCandidate
		var matchLabels sql.NullString
		if err = rows.Scan(&c.AlertID, &c.TargetKey, &c.LastSeenAt, &c.TargetLastSeenAt, &matchLabels, &c.RelevantSourcesJSON, &c.TargetSourceMembersJSON); err != nil {
			return nil, fmt.Errorf("failed scanning recheck candidate: %w", err)
		}
		c.MatchLabelsJSON = matchLabels.String
		c.MatchLabelsValid = matchLabels.Valid
		out = append(out, c)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating recheck candidates: %w", err)
	}
	return out, nil
}

type recheckSourceReference struct {
	SourceName string
	ClusterID  string
}

func parseRelevantSourceReferences(relevantSourcesJSON, relevantUpstreamsJSON string) (map[recheckSourceReference]struct{}, error) {
	out := map[recheckSourceReference]struct{}{}
	if strings.TrimSpace(relevantSourcesJSON) != "" {
		var refs []SourceRef
		if err := json.Unmarshal([]byte(relevantSourcesJSON), &refs); err == nil {
			for _, ref := range refs {
				name := strings.TrimSpace(ref.Name)
				if name != "" {
					out[recheckSourceReference{SourceName: name, ClusterID: strings.TrimSpace(ref.Cluster)}] = struct{}{}
				}
			}
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	if strings.TrimSpace(relevantUpstreamsJSON) == "" {
		return nil, errors.New("no relevant source data")
	}
	var names []string
	if err := json.Unmarshal([]byte(relevantUpstreamsJSON), &names); err != nil {
		return nil, err
	}
	for _, n := range names {
		n = strings.TrimSpace(n)
		if n != "" {
			out[recheckSourceReference{SourceName: n}] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("empty relevant source set")
	}
	return out, nil
}

func evaluateRecheckRules(rules []recheckJobRule, sourceStatuses map[recheckSourceReference]string, target *string, relevant map[recheckSourceReference]struct{}, labels map[string]string) ([]recheckJobRule, bool) {
	if len(relevant) == 0 {
		return nil, false
	}
	for source := range relevant {
		status, found := sourceStatuses[source]
		if !found || (status != "success" && status != "retired") {
			return nil, false
		}
	}
	matched := make([]recheckJobRule, 0)
	matchedBySource := make(map[recheckSourceReference]bool, len(relevant))
	for _, r := range rules {
		if r.ObservedState != "active" {
			continue
		}
		ruleSource := recheckSourceReference{SourceName: r.SourceName, ClusterID: r.ClusterID}
		status, found := sourceStatuses[ruleSource]
		if !found || status == "retired" {
			continue
		}
		if _, ok := relevant[ruleSource]; !ok {
			continue
		}
		if target != nil && r.ClusterID != *target {
			continue
		}
		ok, err := matchesSilenceMatchers(r.MatchersJSON, labels)
		if err != nil || !ok {
			continue
		}
		matched = append(matched, r)
		matchedBySource[ruleSource] = true
	}
	for source := range relevant {
		status := sourceStatuses[source]
		if status == "retired" {
			continue
		}
		if !matchedBySource[source] {
			return nil, true
		}
	}
	return matched, true
}

func loadRecheckJobSourceStatusesTx(tx *sql.Tx, jobID int64) (map[recheckSourceReference]string, error) {
	var raw string
	if err := tx.QueryRow(`SELECT source_context_json FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&raw); err != nil {
		return nil, fmt.Errorf("failed loading recheck source context: %w", err)
	}
	var context struct {
		Sources []struct {
			SourceName string `json:"sourceName"`
			ClusterID  string `json:"clusterId"`
			SyncStatus string `json:"syncStatus"`
			Retired    bool   `json:"retired"`
		} `json:"sources"`
	}
	if err := json.Unmarshal([]byte(raw), &context); err != nil {
		return nil, fmt.Errorf("failed decoding recheck source context: %w", err)
	}
	statuses := make(map[recheckSourceReference]string, len(context.Sources))
	for _, source := range context.Sources {
		name := strings.TrimSpace(source.SourceName)
		if name != "" {
			status := strings.TrimSpace(source.SyncStatus)
			if source.Retired {
				status = "retired"
			}
			statuses[recheckSourceReference{SourceName: name, ClusterID: strings.TrimSpace(source.ClusterID)}] = status
		}
	}
	return statuses, nil
}

func sourceReferencesFromMembers(members []string, clusterID string) map[recheckSourceReference]struct{} {
	out := map[recheckSourceReference]struct{}{}
	for _, member := range members {
		name := strings.TrimSpace(member)
		if name == "" {
			continue
		}
		out[recheckSourceReference{SourceName: name, ClusterID: strings.TrimSpace(clusterID)}] = struct{}{}
	}
	return out
}

func matchesSilenceMatchers(matchersJSON string, labels map[string]string) (bool, error) {
	var snapshots []SilenceMatcherSnapshot
	if err := json.Unmarshal([]byte(matchersJSON), &snapshots); err != nil {
		return false, err
	}
	for _, m := range snapshots {
		sm := models.NewSilenceMatcher(m.Name, m.Value, m.IsRegex, m.IsEqual)
		if !sm.IsMatch(labels) {
			return false, nil
		}
	}
	return true, nil
}

func promoteTargetSuppressedCASTx(
	tx *sql.Tx,
	alertID string,
	row targetStateRow,
	now time.Time,
	reasonCode string,
) (bool, int64, error) {
	promotionPlan, eligible := PromoteSuppressed(LifecycleDimensions{
		RawState: row.RawState, IsLive: row.IsLive,
		IsSuppressed: row.IsSuppressed, IsResolved: row.IsResolved,
	})
	if !eligible {
		return false, 0, nil
	}

	nowRaw := now.UTC().Format(time.RFC3339Nano)
	var res sql.Result
	var err error
	if row.LastSeenAt != nil {
		res, err = tx.Exec(`
UPDATE alert_target_states
SET is_suppressed = 1,
    state = 'suppressed',
    updated_at = ?
WHERE alert_id = ?
  AND target = ?
  AND is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND last_seen_at = ?
`, nowRaw, alertID, row.Target, row.LastSeenAt.UTC().Format(time.RFC3339Nano))
	} else {
		res, err = tx.Exec(`
UPDATE alert_target_states
SET is_suppressed = 1,
    state = 'suppressed',
    updated_at = ?
WHERE alert_id = ?
  AND target = ?
  AND is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND last_seen_at IS NULL
`, nowRaw, alertID, row.Target)
	}
	if err != nil {
		return false, 0, fmt.Errorf("failed to promote target state via CAS: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, 0, fmt.Errorf("failed to read target CAS affected rows: %w", err)
	}
	if affected == 0 {
		return false, 0, nil
	}

	oldState := row.State
	newState := promotionPlan.ProjectedState()
	oldRaw := row.RawState
	oldLive := row.IsLive
	newLive := promotionPlan.IsLive
	oldSupp := row.IsSuppressed
	newSupp := promotionPlan.IsSuppressed
	oldResolved := row.IsResolved
	newResolved := promotionPlan.IsResolved

	transitionID, err := insertTargetTransitionTxWithID(tx, alertID, targetStateRow{
		Target:        row.Target,
		State:         newState,
		RawState:      oldRaw,
		IsLive:        newLive,
		IsSuppressed:  newSupp,
		IsResolved:    newResolved,
		StartsAt:      row.StartsAt,
		LastSeenAt:    row.LastSeenAt,
		SourceMembers: row.SourceMembers,
	}, &oldState, &oldRaw, &oldLive, &oldSupp, &oldResolved, reasonCode, now)
	if err != nil {
		return false, 0, err
	}

	return true, transitionID, nil
}

func insertTargetTransitionTxWithID(
	tx *sql.Tx,
	alertID string,
	row targetStateRow,
	oldState *State,
	oldRaw *State,
	oldLive *bool,
	oldSupp *bool,
	oldResolved *bool,
	reasonCode string,
	changedAt time.Time,
) (int64, error) {
	res, err := tx.Exec(`
INSERT INTO alert_target_events(
  alert_id,
  target,
  old_state,
  new_state,
  old_raw_state,
  new_raw_state,
  old_is_live,
  new_is_live,
  old_is_suppressed,
  new_is_suppressed,
  old_is_resolved,
  new_is_resolved,
  changed_at,
  reason_code
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
		alertID,
		row.Target,
		formatNullableState(oldState),
		string(row.State),
		formatNullableState(oldRaw),
		string(row.RawState),
		formatNullableBool(oldLive),
		boolToInt(row.IsLive),
		formatNullableBool(oldSupp),
		boolToInt(row.IsSuppressed),
		formatNullableBool(oldResolved),
		boolToInt(row.IsResolved),
		changedAt.UTC().Format(time.RFC3339Nano),
		sanitizeReasonCode(ptrString(reasonCode)),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert target transition: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to read target transition id: %w", err)
	}
	return id, nil
}

func promoteAlertSuppressedCAS(tx *sql.Tx, cand recheckCandidate, now time.Time) (bool, int64, error) {
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	var res sql.Result
	var err error
	if cand.LastSeenAt.Valid {
		res, err = tx.Exec(`
UPDATE alert_states
SET is_suppressed = 1,
    suppression_observed_at = ?,
    state = 'suppressed',
    updated_at = ?
WHERE alert_id = ?
  AND state = 'inactive'
  AND is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND last_seen_at = ?
`, nowRaw, nowRaw, cand.AlertID, cand.LastSeenAt.String)
	} else {
		res, err = tx.Exec(`
UPDATE alert_states
SET is_suppressed = 1,
    suppression_observed_at = ?,
    state = 'suppressed',
    updated_at = ?
WHERE alert_id = ?
  AND state = 'inactive'
  AND is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND last_seen_at IS NULL
`, nowRaw, nowRaw, cand.AlertID)
	}
	if err != nil {
		return false, 0, fmt.Errorf("failed to promote alert via CAS: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, 0, fmt.Errorf("failed to read CAS affected rows: %w", err)
	}
	if affected == 0 {
		return false, 0, nil
	}

	oldState := StateInactive
	oldRaw := State(strings.TrimSpace(cand.RawState))
	if !oldRaw.IsValid() {
		oldRaw = StateActive
	}
	promotionPlan, _ := PromoteSuppressed(LifecycleDimensions{RawState: oldRaw})
	oldLive := false
	newLive := promotionPlan.IsLive
	oldSupp := false
	newSupp := promotionPlan.IsSuppressed
	oldResolved := false
	newResolved := promotionPlan.IsResolved
	newState := promotionPlan.ProjectedState()
	reason := sanitizeReasonCode(ptrString("silence_match_promoted"))
	transitionID, err := insertTransitionTxWithID(tx, AlertStateTransition{
		AlertID:         cand.AlertID,
		OldState:        &oldState,
		NewState:        newState,
		OldRawState:     &oldRaw,
		NewRawState:     oldRaw,
		OldIsLive:       &oldLive,
		NewIsLive:       newLive,
		OldIsSuppressed: &oldSupp,
		NewIsSuppressed: newSupp,
		OldIsResolved:   &oldResolved,
		NewIsResolved:   newResolved,
		ChangedAt:       now,
		ChangeSource:    ChangeSourceSilence,
		ReasonCode:      reason,
	})
	if err != nil {
		return false, 0, err
	}
	if err := updateTargetStatesSuppressedTx(tx, cand.AlertID, now, "silence_match_promoted"); err != nil {
		return false, 0, err
	}
	return true, transitionID, nil
}

func updateTargetStatesSuppressedTx(tx *sql.Tx, alertID string, now time.Time, reasonCode string) error {
	existing, err := readTargetStatesTx(tx, alertID)
	if err != nil {
		return err
	}
	for _, row := range existing {
		promotionPlan, eligible := PromoteSuppressed(LifecycleDimensions{
			RawState: row.RawState, IsLive: row.IsLive,
			IsSuppressed: row.IsSuppressed, IsResolved: row.IsResolved,
		})
		if !eligible {
			continue
		}
		row.IsSuppressed = promotionPlan.IsSuppressed
		row.State = promotionPlan.ProjectedState()
		if err := upsertTargetStateTx(tx, alertID, row, reasonCode, now); err != nil {
			return err
		}
	}
	return nil
}

func insertSuppressionCauseHistoryTx(tx *sql.Tx, alertID string, transitionEventID, jobID int64, now time.Time, matched []recheckJobRule) error {
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	fingerprint := buildCauseFingerprint(matched)
	res, err := tx.Exec(`
INSERT INTO alert_suppression_cause_history(
  alert_id, transition_event_id, recheck_job_id, source,
  cause_status, cause_fingerprint, observed_at, last_confirmed_at
) VALUES(?, ?, ?, 'silence_recheck', 'known', ?, ?, ?)
`, alertID, transitionEventID, jobID, fingerprint, nowRaw, nowRaw)
	if err != nil {
		return fmt.Errorf("failed to insert suppression cause history: %w", err)
	}
	historyID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to read suppression cause history id: %w", err)
	}

	for _, m := range matched {
		if _, err = tx.Exec(`
INSERT INTO alert_suppression_cause_members(history_id, source_name, rule_id, revision_id, resolution_status)
VALUES(?, ?, ?, ?, 'known')
`, historyID, m.SourceName, m.RuleID, m.RevisionID); err != nil {
			return fmt.Errorf("failed to insert suppression cause member: %w", err)
		}
	}
	return nil
}

func insertTargetRecheckSuppressionCauseTx(tx *sql.Tx, alertID, target string, targetEventID, jobID int64, now time.Time, matched []recheckJobRule) (int64, error) {
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	res, err := tx.Exec(`
INSERT INTO alert_target_suppression_cause_history(
  alert_id, target, target_transition_event_id, recheck_job_id,
  source, cause_status, cause_fingerprint, observed_at, last_confirmed_at
) VALUES(?, ?, ?, ?, 'silence_recheck', 'known', ?, ?, ?)
`, alertID, target, targetEventID, jobID, buildCauseFingerprint(matched), nowRaw, nowRaw)
	if err != nil {
		return 0, fmt.Errorf("failed to insert target suppression cause history: %w", err)
	}
	historyID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to read target suppression cause history id: %w", err)
	}
	for _, matchedRule := range matched {
		if _, err = tx.Exec(`
INSERT INTO alert_target_suppression_cause_members(
  history_id, source_name, rule_id, revision_id, resolution_status
) VALUES(?, ?, ?, ?, 'known')
`, historyID, matchedRule.SourceName, matchedRule.RuleID, matchedRule.RevisionID); err != nil {
			return 0, fmt.Errorf("failed to insert target suppression cause member: %w", err)
		}
	}
	return historyID, nil
}

func latestTargetSuppressionCauseHistoryTx(tx *sql.Tx, alertID, target string) (int64, bool, error) {
	var historyID int64
	err := tx.QueryRow(`
SELECT history_id
FROM alert_target_suppression_cause_history
WHERE alert_id=? AND target=?
ORDER BY history_id DESC LIMIT 1
`, alertID, target).Scan(&historyID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to read effective target suppression cause: %w", err)
	}
	return historyID, true, nil
}

func insertTargetAggregateSuppressionCauseTx(tx *sql.Tx, alertID string, transitionEventID int64, now time.Time, targetHistoryIDs []int64) error {
	if len(targetHistoryIDs) == 0 {
		return errors.New("target aggregate suppression cause requires target histories")
	}
	slices.Sort(targetHistoryIDs)
	uniqueIDs := targetHistoryIDs[:0]
	for _, historyID := range targetHistoryIDs {
		if len(uniqueIDs) == 0 || uniqueIDs[len(uniqueIDs)-1] != historyID {
			uniqueIDs = append(uniqueIDs, historyID)
		}
	}
	raw, _ := json.Marshal(uniqueIDs)
	sum := sha256.Sum256(raw)
	res, err := tx.Exec(`
INSERT INTO alert_target_aggregate_suppression_history(
  alert_id, transition_event_id, cause_fingerprint, observed_at
) VALUES(?, ?, ?, ?)
`, alertID, transitionEventID, fmt.Sprintf("sha256:%x", sum), now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("failed to insert target aggregate suppression history: %w", err)
	}
	aggregateID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to read target aggregate suppression id: %w", err)
	}
	for _, historyID := range uniqueIDs {
		if _, err = tx.Exec(`
INSERT INTO alert_target_aggregate_suppression_links(aggregate_id, target_history_id)
VALUES(?, ?)
`, aggregateID, historyID); err != nil {
			return fmt.Errorf("failed to link target aggregate suppression cause: %w", err)
		}
	}
	return nil
}

func insertLiveSuppressionCauseTx(tx *sql.Tx, alertID string, refs []SourceSilenceRef, now time.Time) error {
	members := make([]suppressionCauseMember, 0)
	unresolvedCount := 0

	for _, ref := range refs {
		sourceName := strings.TrimSpace(ref.SourceName)
		clusterID := strings.TrimSpace(ref.ClusterID)
		if sourceName == "" || clusterID == "" {
			continue
		}
		for _, silenceID := range ref.SilenceIDs {
			silenceID = strings.TrimSpace(silenceID)
			if silenceID == "" {
				continue
			}
			ruleID, revisionID, known, err := resolveLiveSilenceMemberTx(tx, sourceName, clusterID, silenceID, now)
			if err != nil {
				return err
			}
			m := suppressionCauseMember{sourceName: sourceName, ruleID: ruleID, known: known}
			if known {
				m.revisionID = &revisionID
			} else {
				unresolvedCount++
			}
			members = append(members, m)
		}
	}

	var status string
	switch {
	case len(members) == 0:
		status = "none"
	case unresolvedCount > 0:
		status = "partial"
	default:
		status = "known"
	}

	fingerprint := buildLiveCauseFingerprint(status, members)
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	var latestID int64
	var latestFP, latestStatus string
	err := tx.QueryRow(`
SELECT history_id, cause_fingerprint, cause_status
FROM alert_suppression_cause_history
WHERE alert_id = ? AND source = 'live_alertmanager'
ORDER BY history_id DESC
LIMIT 1
`, alertID).Scan(&latestID, &latestFP, &latestStatus)
	if err == nil {
		if latestFP == fingerprint && latestStatus == status {
			if _, err = tx.Exec(`UPDATE alert_suppression_cause_history SET last_confirmed_at = ? WHERE history_id = ?`, nowRaw, latestID); err != nil {
				return fmt.Errorf("failed to refresh live cause timestamp: %w", err)
			}
			return nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query latest live cause history: %w", err)
	}

	res, err := tx.Exec(`
INSERT INTO alert_suppression_cause_history(
  alert_id, transition_event_id, recheck_job_id, source,
  cause_status, cause_fingerprint, observed_at, last_confirmed_at
) VALUES(?, NULL, NULL, 'live_alertmanager', ?, ?, ?, ?)
`, alertID, status, fingerprint, nowRaw, nowRaw)
	if err != nil {
		return fmt.Errorf("failed to insert live suppression cause history: %w", err)
	}
	historyID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to read live suppression cause history id: %w", err)
	}
	for _, m := range members {
		resolution := "unresolved"
		var revision any
		if m.known && m.revisionID != nil {
			resolution = "known"
			revision = *m.revisionID
		}
		if _, err = tx.Exec(`
INSERT INTO alert_suppression_cause_members(history_id, source_name, rule_id, revision_id, resolution_status)
VALUES(?, ?, ?, ?, ?)
`, historyID, m.sourceName, m.ruleID, revision, resolution); err != nil {
			return fmt.Errorf("failed to insert live suppression cause member: %w", err)
		}
	}
	return nil
}

func insertLiveTargetSuppressionCausesTx(tx *sql.Tx, alertID string, targets []LiveTargetSnapshot, refs []SourceSilenceRef, now time.Time) error {
	for _, target := range targets {
		if target.State != StateSuppressed {
			continue
		}
		targetName := strings.TrimSpace(target.Target)
		if targetName == "" {
			continue
		}
		members := make([]suppressionCauseMember, 0)
		unresolvedCount := 0
		for _, ref := range refs {
			if strings.TrimSpace(ref.ClusterID) != targetName {
				continue
			}
			for _, silenceID := range ref.SilenceIDs {
				ruleID, revisionID, known, err := resolveLiveSilenceMemberTx(tx, strings.TrimSpace(ref.SourceName), targetName, strings.TrimSpace(silenceID), now)
				if err != nil {
					return err
				}
				member := suppressionCauseMember{sourceName: strings.TrimSpace(ref.SourceName), ruleID: ruleID, known: known}
				if known {
					member.revisionID = &revisionID
				} else {
					unresolvedCount++
				}
				members = append(members, member)
			}
		}
		status := "none"
		if len(members) > 0 {
			status = "known"
			if unresolvedCount > 0 {
				status = "partial"
			}
		}
		fingerprint := buildLiveCauseFingerprint(status, members)
		nowRaw := now.UTC().Format(time.RFC3339Nano)
		var latestID int64
		var latestFP, latestStatus string
		err := tx.QueryRow(`
SELECT history_id, cause_fingerprint, cause_status
FROM alert_target_suppression_cause_history
WHERE alert_id=? AND target=? AND source='live_alertmanager'
ORDER BY history_id DESC LIMIT 1
`, alertID, targetName).Scan(&latestID, &latestFP, &latestStatus)
		if err == nil && latestFP == fingerprint && latestStatus == status {
			if _, err = tx.Exec(`UPDATE alert_target_suppression_cause_history SET last_confirmed_at=? WHERE history_id=?`, nowRaw, latestID); err != nil {
				return fmt.Errorf("failed to refresh target live cause timestamp: %w", err)
			}
			continue
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to query latest target live cause: %w", err)
		}
		res, err := tx.Exec(`
INSERT INTO alert_target_suppression_cause_history(
  alert_id, target, target_transition_event_id, recheck_job_id,
  source, cause_status, cause_fingerprint, observed_at, last_confirmed_at
) VALUES(?, ?, NULL, NULL, 'live_alertmanager', ?, ?, ?, ?)
`, alertID, targetName, status, fingerprint, nowRaw, nowRaw)
		if err != nil {
			return fmt.Errorf("failed to insert target live suppression cause: %w", err)
		}
		historyID, _ := res.LastInsertId()
		for _, member := range members {
			resolution := "unresolved"
			var revision any
			if member.known && member.revisionID != nil {
				resolution = "known"
				revision = *member.revisionID
			}
			if _, err = tx.Exec(`
INSERT INTO alert_target_suppression_cause_members(history_id, source_name, rule_id, revision_id, resolution_status)
VALUES(?, ?, ?, ?, ?)
`, historyID, member.sourceName, member.ruleID, revision, resolution); err != nil {
				return fmt.Errorf("failed to insert target live suppression cause member: %w", err)
			}
		}
	}
	return nil
}

func resolveLiveSilenceMemberTx(tx *sql.Tx, sourceName, clusterID, silenceID string, now time.Time) (int64, int64, bool, error) {
	var ruleID int64
	err := tx.QueryRow(`SELECT rule_id FROM silence_rules WHERE cluster_id = ? AND silence_id = ?`, clusterID, silenceID).Scan(&ruleID)
	if errors.Is(err, sql.ErrNoRows) {
		nowRaw := now.UTC().Format(time.RFC3339Nano)
		res, ierr := tx.Exec(`INSERT INTO silence_rules(cluster_id, silence_id, first_observed_at, last_observed_at) VALUES(?, ?, ?, ?)`, clusterID, silenceID, nowRaw, nowRaw)
		if ierr != nil {
			return 0, 0, false, fmt.Errorf("failed to insert unresolved silence rule: %w", ierr)
		}
		ruleID, _ = res.LastInsertId()
		return ruleID, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("failed to query silence rule id: %w", err)
	}

	var revisionID int64
	err = tx.QueryRow(`
SELECT ass.revision_id
FROM alertmanager_source_silences ass
WHERE ass.source_name = ? AND ass.rule_id = ?
`, sourceName, ruleID).Scan(&revisionID)
	if errors.Is(err, sql.ErrNoRows) {
		return ruleID, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("failed to query source silence revision: %w", err)
	}
	return ruleID, revisionID, true, nil
}

func buildLiveCauseFingerprint(status string, members []suppressionCauseMember) string {
	keys := make([]string, 0, len(members)+1)
	keys = append(keys, "status:"+status)
	for _, m := range members {
		if m.known && m.revisionID != nil {
			keys = append(keys, fmt.Sprintf("k:%s:%d:%d", m.sourceName, m.ruleID, *m.revisionID))
		} else {
			keys = append(keys, fmt.Sprintf("u:%s:%d", m.sourceName, m.ruleID))
		}
	}
	slices.Sort(keys)
	raw, _ := json.Marshal(keys)
	s := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", s)
}

func buildCauseFingerprint(matched []recheckJobRule) string {
	keys := make([]string, 0, len(matched))
	for _, m := range matched {
		keys = append(keys, fmt.Sprintf("%s:%d:%d", m.SourceName, m.RuleID, m.RevisionID))
	}
	slices.Sort(keys)
	raw, _ := json.Marshal(keys)
	s := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", s)
}

func parseRelevantUpstreams(raw string) ([]string, error) {
	var upstreams []string
	if err := json.Unmarshal([]byte(raw), &upstreams); err != nil {
		return nil, err
	}
	if len(upstreams) == 0 {
		return nil, errors.New("empty relevant upstream list")
	}

	return uniqueSortedStrings(upstreams), nil
}

func marshalRelevantUpstreams(upstreams []string) ([]byte, error) {
	values := uniqueSortedStrings(upstreams)
	if len(values) == 0 {
		return nil, ErrRelevantUpstreamsCorrupt
	}

	out, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal relevant upstreams: %w", err)
	}

	return out, nil
}

func selectStoredRelevantUpstreams(existing, observed []string, allSuccessful bool) []string {
	existing = uniqueSortedStrings(existing)
	observed = uniqueSortedStrings(observed)

	if len(existing) == 0 {
		return observed
	}

	if allSuccessful {
		return observed
	}

	merged := append([]string{}, existing...)
	merged = append(merged, observed...)
	return uniqueSortedStrings(merged)
}

func parseRelevantSources(raw string) ([]SourceRef, error) {
	var refs []SourceRef
	if err := json.Unmarshal([]byte(raw), &refs); err != nil {
		return nil, err
	}
	refs = normalizeSourceRefs(refs)
	if len(refs) == 0 {
		return nil, errors.New("empty relevant source list")
	}
	return refs, nil
}

func marshalRelevantSources(refs []SourceRef) ([]byte, error) {
	refs = normalizeSourceRefs(refs)
	if len(refs) == 0 {
		return nil, ErrRelevantUpstreamsCorrupt
	}
	out, err := json.Marshal(refs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal relevant sources: %w", err)
	}
	return out, nil
}

func marshalAlertSourceLinks(links []AlertSourceLink) ([]byte, error) {
	bySource := map[string]AlertSourceLink{}
	for _, link := range links {
		name := strings.TrimSpace(link.Name)
		cluster := strings.TrimSpace(link.Cluster)
		url := strings.TrimSpace(link.URL)
		if name == "" || cluster == "" || url == "" {
			return nil, errors.New("source link requires name, cluster, and URL")
		}
		key := name + "\x00" + cluster
		if existing, found := bySource[key]; !found || url < existing.URL {
			bySource[key] = AlertSourceLink{Name: name, Cluster: cluster, URL: url}
		}
	}
	out := make([]AlertSourceLink, 0, len(bySource))
	for _, link := range bySource {
		out = append(out, link)
	}
	slices.SortFunc(out, func(a, b AlertSourceLink) int {
		if diff := strings.Compare(a.Name, b.Name); diff != 0 {
			return diff
		}
		return strings.Compare(a.Cluster, b.Cluster)
	})
	return json.Marshal(out)
}

func normalizeAlertSourceLinksJSON(raw string) (string, error) {
	var links []AlertSourceLink
	if err := json.Unmarshal([]byte(raw), &links); err != nil {
		return "", fmt.Errorf("invalid source links JSON: %w", err)
	}
	normalized, err := marshalAlertSourceLinks(links)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}

func selectStoredRelevantSources(existing, observed []SourceRef, allSuccessful bool) []SourceRef {
	existing = normalizeSourceRefs(existing)
	observed = normalizeSourceRefs(observed)

	if len(existing) == 0 {
		return observed
	}
	if allSuccessful {
		return observed
	}
	merged := append([]SourceRef{}, existing...)
	merged = append(merged, observed...)
	return normalizeSourceRefs(merged)
}

func normalizeSourceRefs(refs []SourceRef) []SourceRef {
	if len(refs) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]SourceRef, 0, len(refs))
	for _, ref := range refs {
		name := strings.TrimSpace(ref.Name)
		cluster := strings.TrimSpace(ref.Cluster)
		if name == "" || cluster == "" {
			continue
		}
		k := name + "\x00" + cluster
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, SourceRef{Name: name, Cluster: cluster})
	}
	slices.SortFunc(out, func(a, b SourceRef) int {
		if a.Name == b.Name {
			return strings.Compare(a.Cluster, b.Cluster)
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

func uniqueSortedStrings(values []string) []string {
	uniq := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		k := strings.TrimSpace(v)
		if k == "" {
			continue
		}
		if _, found := uniq[k]; found {
			continue
		}
		uniq[k] = struct{}{}
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func allRelevantUpstreamsSuccessful(relevant []string, successful map[string]bool) bool {
	if len(relevant) == 0 {
		return false
	}
	for _, upstream := range relevant {
		if !successful[upstream] {
			return false
		}
	}
	return true
}

func areAllUpstreamsSuccessful(successful map[string]bool) bool {
	if len(successful) == 0 {
		return false
	}
	for _, upstream := range sortedSuccessfulUpstreamNames(successful) {
		if !successful[upstream] {
			return false
		}
	}
	return true
}

func sortedSuccessfulUpstreamNames(successful map[string]bool) []string {
	names := make([]string, 0, len(successful))
	for upstream := range successful {
		names = append(names, upstream)
	}
	slices.Sort(names)
	return names
}

func getCurrentAlertTx(tx *sql.Tx, alertID string) (AlertCurrent, error) {
	row := tx.QueryRow(`
SELECT
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  source_links_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
FROM alert_states
WHERE alert_id = ?
`, alertID)

	return scanCurrentAlert(row)
}

func sanitizeNullableString(v *string) *string {
	if v == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*v)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func deleteExpiredAlertsTx(tx *sql.Tx, cutoff time.Time) (int, error) {
	res, err := tx.Exec(`
DELETE FROM alert_states
WHERE last_seen_at IS NOT NULL
  AND last_seen_at < ?
`, cutoff.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("failed to delete expired alerts: %w", err)
	}

	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to read expired delete count: %w", err)
	}

	return int(clampInt64ToInt(deleted)), nil
}

type housekeepingTransactionResult struct {
	expiredDeleted int
	limitDeleted   int
	silenceRetentionResult
}

type silenceRetentionResult struct {
	recheckJobs    int
	retiredSources int
	revisions      int
	episodes       int
	rules          int
}

func runPhase2RetentionTx(ctx context.Context, tx *sql.Tx, now time.Time, silenceHistoryRetentionTTL time.Duration) (silenceRetentionResult, error) {
	result := silenceRetentionResult{}
	cutoff := now.Add(-silenceHistoryRetentionTTL).UTC().Format(time.RFC3339Nano)

	deleteAndCount := func(query string, args ...any) (int, error) {
		res, err := tx.Exec(query, args...)
		if err != nil {
			return 0, err
		}
		count, err := res.RowsAffected()
		if err != nil {
			runtimeLog.ErrorContext(ctx, "lifecycle_housekeeping", "retention_count_failed", "Failed to read silence retention deletion count", slog.Any("error", err))
			return 0, nil
		}
		return int(clampInt64ToInt(count)), nil
	}
	var err error
	result.recheckJobs, err = deleteAndCount(`
DELETE FROM silence_recheck_jobs
WHERE status = 'completed'
  AND job_id NOT IN (
    SELECT recheck_job_id FROM alert_suppression_cause_history WHERE recheck_job_id IS NOT NULL
    UNION
    SELECT recheck_job_id FROM alert_target_suppression_cause_history WHERE recheck_job_id IS NOT NULL
  )
  AND job_id <> (
    SELECT COALESCE(MAX(job_id), 0) FROM silence_recheck_jobs WHERE status = 'completed'
  )
`)
	if err != nil {
		return silenceRetentionResult{}, fmt.Errorf("failed cleaning completed recheck jobs: %w", err)
	}
	result.retiredSources, err = deleteAndCount(`
DELETE FROM alertmanager_sources
WHERE sync_status = 'retired'
  AND retired_at IS NOT NULL
  AND retired_at < ?
  AND source_name NOT IN (
    SELECT source_name FROM alertmanager_source_silences
    UNION
    SELECT source_name FROM silence_recheck_job_rules
    UNION
    SELECT source_name FROM alert_suppression_cause_members
    UNION
    SELECT source_name FROM alert_target_suppression_cause_members
  )
`, cutoff)
	if err != nil {
		return silenceRetentionResult{}, fmt.Errorf("failed cleaning retired sources: %w", err)
	}

	result.revisions, err = deleteAndCount(`
DELETE FROM silence_revisions
WHERE last_observed_at < ?
  AND revision_id NOT IN (
    SELECT revision_id FROM alertmanager_source_silences
    UNION
    SELECT revision_id FROM silence_recheck_job_rules
    UNION
    SELECT revision_id FROM alert_suppression_cause_members WHERE revision_id IS NOT NULL
    UNION
    SELECT revision_id FROM alert_target_suppression_cause_members WHERE revision_id IS NOT NULL
  )
`, cutoff)
	if err != nil {
		return silenceRetentionResult{}, fmt.Errorf("failed cleaning silence revisions: %w", err)
	}

	result.episodes, err = deleteAndCount(`
DELETE FROM silence_rule_episodes
WHERE open_marker IS NULL
  AND ended_at IS NOT NULL
  AND ended_at < ?
  AND episode_id NOT IN (SELECT episode_id FROM silence_revisions)
`, cutoff)
	if err != nil {
		return silenceRetentionResult{}, fmt.Errorf("failed cleaning silence episodes: %w", err)
	}

	result.rules, err = deleteAndCount(`
DELETE FROM silence_rules
WHERE rule_id NOT IN (SELECT rule_id FROM silence_revisions)
  AND rule_id NOT IN (SELECT rule_id FROM alertmanager_source_silences)
  AND rule_id NOT IN (SELECT rule_id FROM silence_recheck_job_rules)
  AND rule_id NOT IN (SELECT rule_id FROM alert_suppression_cause_members)
  AND rule_id NOT IN (SELECT rule_id FROM alert_target_suppression_cause_members)
  AND last_observed_at < ?
`, cutoff)
	if err != nil {
		return silenceRetentionResult{}, fmt.Errorf("failed cleaning silence rules: %w", err)
	}

	return result, nil
}

func enforceMaxAlertsTx(tx *sql.Tx, maxAlerts int) (int, error) {
	currentCount, err := queryInt(tx, `SELECT COUNT(*) FROM alert_states`)
	if err != nil {
		return 0, fmt.Errorf("failed to count current alerts for max limit: %w", err)
	}

	if currentCount <= maxAlerts {
		return 0, nil
	}

	toDelete := currentCount - maxAlerts
	res, err := tx.Exec(`
DELETE FROM alert_states
WHERE alert_id IN (
  SELECT alert_id
  FROM alert_states
  ORDER BY COALESCE(
      (SELECT MAX(h.changed_at) FROM alert_events h WHERE h.alert_id = alert_states.alert_id),
      updated_at
    ) ASC,
    alert_id ASC
  LIMIT ?
)
`, toDelete)
	if err != nil {
		return 0, fmt.Errorf("failed to enforce max alerts limit: %w", err)
	}

	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to read max limit delete count: %w", err)
	}

	return int(clampInt64ToInt(deleted)), nil
}

func runIncrementalVacuum(ctx context.Context, tx *sql.Tx) (int, error) {
	if tx == nil {
		return 0, ErrStoreNotInitialized
	}
	before, err := queryIntContext(ctx, tx, `PRAGMA freelist_count`)
	if err != nil {
		return 0, fmt.Errorf("failed to read freelist_count before vacuum: %w", err)
	}

	if _, err = tx.ExecContext(ctx, `PRAGMA incremental_vacuum`); err != nil {
		return 0, fmt.Errorf("failed to run incremental vacuum: %w", err)
	}

	after, err := queryIntContext(ctx, tx, `PRAGMA freelist_count`)
	if err != nil {
		return 0, fmt.Errorf("failed to read freelist_count after vacuum: %w", err)
	}

	if before < after {
		return 0, nil
	}

	return before - after, nil
}

type silenceReconciliationResult struct {
	sourceContextChanged bool
}

func reconcileSilenceSourcesWithEventsTx(tx *sql.Tx, sources []SourceSilenceState, now time.Time, collector *runtimeEventCollector) (silenceReconciliationResult, error) {
	if sources == nil {
		return silenceReconciliationResult{}, nil
	}
	result := silenceReconciliationResult{}
	previousConsistency, consistencyErr := readClusterConsistencyStatusesTx(tx)
	if observabilityErr := collector.observabilityError("event_payload", "ha_consistency_previous", consistencyErr); observabilityErr != nil {
		collector.appendObservabilityFailureValue("event_payload", "ha_consistency_previous", observabilityErr)
		previousConsistency = map[string]clusterConsistencySnapshot{}
	}
	normalizedSources := make([]SourceSilenceState, len(sources))
	copy(normalizedSources, sources)
	slices.SortFunc(normalizedSources, func(a, b SourceSilenceState) int {
		if sourceCompare := strings.Compare(strings.TrimSpace(a.SourceName), strings.TrimSpace(b.SourceName)); sourceCompare != 0 {
			return sourceCompare
		}
		return strings.Compare(strings.TrimSpace(a.ClusterID), strings.TrimSpace(b.ClusterID))
	})
	for i := range normalizedSources {
		validCount := len(knownSourceSilences(normalizedSources[i].Silences))
		malformedCount := normalizedSources[i].MalformedRules + len(normalizedSources[i].Silences) - validCount
		normalizedSources[i].Silences = knownSourceSilences(normalizedSources[i].Silences)
		slices.SortFunc(normalizedSources[i].Silences, func(a, b SourceSilenceSnapshot) int {
			return strings.Compare(strings.TrimSpace(a.SilenceID), strings.TrimSpace(b.SilenceID))
		})
		collector.append(runtimeEvent{level: slog.LevelDebug, component: "silence_reconciliation", event: "source_snapshot_received", message: "Alertmanager silence source snapshot received", attrs: []slog.Attr{slog.String("source", strings.TrimSpace(normalizedSources[i].SourceName)), slog.String("target", strings.TrimSpace(normalizedSources[i].ClusterID)), slog.String("sync_status", normalizedSourceSyncStatus(normalizedSources[i].SyncStatus)), slog.Int("valid_rule_count", validCount), slog.Int("malformed_rule_count", malformedCount)}})
	}
	sources = normalizedSources
	shouldEnqueueRecheck := false
	configured := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		sourceName := strings.TrimSpace(source.SourceName)
		clusterID := strings.TrimSpace(source.ClusterID)
		if sourceName == "" || clusterID == "" {
			continue
		}
		configured[sourceName] = struct{}{}
		syncStatus := strings.TrimSpace(source.SyncStatus)
		if syncStatus == "" {
			syncStatus = "unknown"
		}
		if syncStatus != "success" && syncStatus != "failed" && syncStatus != "invalid" && syncStatus != "retired" && syncStatus != "unknown" {
			syncStatus = "invalid"
		}
		_, previousStatus, previousCluster, sourceReadErr := readSourceFingerprintAndStatusTx(tx, sourceName)
		if sourceReadErr != nil {
			return silenceReconciliationResult{}, fmt.Errorf("failed reading previous source state for %q: %w", sourceName, sourceReadErr)
		}
		newFP := buildSourceSilenceFingerprint(source)
		promotionCapableChange, promotionErr := sourceSilenceChangeCanPromoteTx(tx, sourceName, source.Silences)
		if promotionErr != nil {
			return silenceReconciliationResult{}, promotionErr
		}
		if previousCluster != "" && previousCluster != clusterID {
			promotionCapableChange = true
		}

		if err := upsertAlertmanagerSourceTx(tx, source, newFP, now); err != nil {
			return silenceReconciliationResult{}, err
		}
		if syncStatus == "success" {
			if err := replaceSourceSilenceRelationsWithEventsTx(tx, sourceName, clusterID, source.Silences, now, collector); err != nil {
				return silenceReconciliationResult{}, err
			}
		}

		recovered := syncStatus == "success" && (previousStatus == "failed" || previousStatus == "invalid" || previousStatus == "unknown" || previousStatus == "retired")
		if syncStatus == "success" && (promotionCapableChange || recovered) {
			shouldEnqueueRecheck = true
		}
		switch {
		case recovered:
			collector.append(runtimeEvent{level: slog.LevelInfo, component: "silence_reconciliation", event: "source_recovered", message: "Alertmanager silence source recovered", attrs: []slog.Attr{slog.String("source", sourceName), slog.String("target", clusterID), slog.String("previous_status", previousStatus)}})
		case syncStatus != "success" && previousStatus != syncStatus:
			collector.append(runtimeEvent{level: slog.LevelWarn, component: "silence_reconciliation", event: "source_sync_failed", message: "Alertmanager silence source synchronization failed", attrs: []slog.Attr{slog.String("source", sourceName), slog.String("target", clusterID), slog.String("sync_status", syncStatus)}})
		case syncStatus != "success":
			collector.append(runtimeEvent{level: slog.LevelDebug, component: "silence_reconciliation", event: "source_sync_failed_unchanged", message: "Alertmanager silence source remains unavailable", attrs: []slog.Attr{slog.String("source", sourceName), slog.String("target", clusterID), slog.String("sync_status", syncStatus)}})
		}
	}
	retired, retirementErr := retireDeconfiguredSourcesTx(tx, configured, now)
	if retirementErr != nil {
		return silenceReconciliationResult{}, retirementErr
	}
	if len(retired) > 0 {
		shouldEnqueueRecheck = true
		for _, source := range retired {
			collector.append(runtimeEvent{level: slog.LevelInfo, component: "silence_reconciliation", event: "source_retired", message: "Alertmanager silence source retired", attrs: []slog.Attr{slog.String("source", source.name), slog.String("target", source.target)}})
		}
	}
	if err := closeConfirmedAbsentSilenceEpisodesWithEventsTx(tx, sources, now, collector); err != nil {
		return silenceReconciliationResult{}, err
	}
	if err := reconcileClusterConsistencyTx(tx, sources, now); err != nil {
		return silenceReconciliationResult{}, err
	}
	currentConsistency, consistencyErr := readClusterConsistencyStatusesTx(tx)
	if observabilityErr := collector.observabilityError("event_payload", "ha_consistency_current", consistencyErr); observabilityErr != nil {
		collector.appendObservabilityFailureValue("event_payload", "ha_consistency_current", observabilityErr)
	} else {
		for _, event := range clusterConsistencyRuntimeEvents(previousConsistency, currentConsistency) {
			collector.append(event)
		}
	}
	if shouldEnqueueRecheck {
		generation, generationErr := readMetadataIntTx(tx, metaRecheckSourceContextGen)
		if generationErr != nil {
			return silenceReconciliationResult{}, generationErr
		}
		if generation == int(^uint(0)>>1) {
			return silenceReconciliationResult{}, errors.New("recheck source context generation overflow")
		}
		if err := setMetadataTx(tx, metaRecheckSourceContextGen, strconv.Itoa(generation+1)); err != nil {
			return silenceReconciliationResult{}, err
		}
	}
	result.sourceContextChanged = shouldEnqueueRecheck
	return result, nil
}

func normalizedSourceSyncStatus(status string) string {
	status = strings.TrimSpace(status)
	if status == "success" || status == "failed" || status == "invalid" || status == "retired" || status == "unknown" {
		return status
	}
	return "invalid"
}

type clusterConsistencySnapshot struct {
	status  string
	members int
}

func readClusterConsistencyStatusesTx(tx *sql.Tx) (map[string]clusterConsistencySnapshot, error) {
	rows, err := tx.Query(`SELECT cluster_id, status, member_count FROM alertmanager_cluster_consistency`)
	if err != nil {
		return nil, fmt.Errorf("failed reading cluster consistency for logging: %w", err)
	}
	defer rows.Close()
	result := map[string]clusterConsistencySnapshot{}
	for rows.Next() {
		var clusterID string
		var snapshot clusterConsistencySnapshot
		if scanErr := rows.Scan(&clusterID, &snapshot.status, &snapshot.members); scanErr != nil {
			return nil, fmt.Errorf("failed scanning cluster consistency for logging: %w", scanErr)
		}
		result[clusterID] = snapshot
	}
	return result, rows.Err()
}

func clusterConsistencyRuntimeEvents(previous, current map[string]clusterConsistencySnapshot) []runtimeEvent {
	events := make([]runtimeEvent, 0)
	targets := make([]string, 0, len(current))
	for target := range current {
		targets = append(targets, target)
	}
	slices.Sort(targets)
	for _, target := range targets {
		snapshot := current[target]
		old := previous[target]
		switch {
		case snapshot.status == "inconsistent" && old.status != "inconsistent":
			events = append(events, runtimeEvent{level: slog.LevelWarn, component: "silence_reconciliation", event: "ha_disagreement_detected", message: "Alertmanager silence members disagree", attrs: []slog.Attr{slog.String("target", target), slog.Int("member_count", snapshot.members)}})
		case snapshot.status == "inconsistent":
			events = append(events, runtimeEvent{level: slog.LevelDebug, component: "silence_reconciliation", event: "ha_disagreement_unchanged", message: "Alertmanager silence members still disagree", attrs: []slog.Attr{slog.String("target", target), slog.Int("member_count", snapshot.members)}})
		case snapshot.status == "consistent" && old.status == "inconsistent":
			events = append(events, runtimeEvent{level: slog.LevelInfo, component: "silence_reconciliation", event: "ha_converged", message: "Alertmanager silence members converged", attrs: []slog.Attr{slog.String("target", target), slog.Int("member_count", snapshot.members)}})
		}
	}
	return events
}

func closeConfirmedAbsentSilenceEpisodesWithEventsTx(tx *sql.Tx, sources []SourceSilenceState, now time.Time, collector *runtimeEventCollector) error {
	type coverage struct {
		members       map[string]struct{}
		allSuccessful bool
	}
	byCluster := map[string]*coverage{}
	for _, source := range sources {
		clusterID := strings.TrimSpace(source.ClusterID)
		sourceName := strings.TrimSpace(source.SourceName)
		status := strings.TrimSpace(source.SyncStatus)
		if clusterID == "" || sourceName == "" || status == "retired" {
			continue
		}
		cluster := byCluster[clusterID]
		if cluster == nil {
			cluster = &coverage{members: map[string]struct{}{}, allSuccessful: true}
			byCluster[clusterID] = cluster
		}
		cluster.members[sourceName] = struct{}{}
		if status != "success" {
			cluster.allSuccessful = false
		}
	}
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	clusters := make([]string, 0, len(byCluster))
	for clusterID := range byCluster {
		clusters = append(clusters, clusterID)
	}
	slices.Sort(clusters)
	for _, clusterID := range clusters {
		cluster := byCluster[clusterID]
		if len(cluster.members) == 0 || !cluster.allSuccessful {
			continue
		}
		type closedEpisode struct {
			target    string
			silenceID string
			episodeID int64
		}
		rows, queryErr := tx.Query(`
SELECT r.cluster_id, r.silence_id, e.episode_id
FROM silence_rule_episodes e
JOIN silence_rules r ON r.rule_id=e.rule_id
WHERE e.open_marker=1 AND r.cluster_id=?
  AND NOT EXISTS (
    SELECT 1 FROM alertmanager_source_silences ss
    JOIN alertmanager_sources src ON src.source_name=ss.source_name
    WHERE ss.rule_id=r.rule_id AND src.current_cluster_id=? AND src.retired_at IS NULL
  )
`, clusterID, clusterID)
		if queryErr != nil {
			return fmt.Errorf("failed selecting confirmed-absent silence episodes for cluster %q: %w", clusterID, queryErr)
		}
		closed := make([]closedEpisode, 0)
		for rows.Next() {
			var episode closedEpisode
			if scanErr := rows.Scan(&episode.target, &episode.silenceID, &episode.episodeID); scanErr != nil {
				rows.Close()
				return fmt.Errorf("failed scanning confirmed-absent silence episode for cluster %q: %w", clusterID, scanErr)
			}
			closed = append(closed, episode)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			rows.Close()
			return fmt.Errorf("failed iterating confirmed-absent silence episodes for cluster %q: %w", clusterID, rowsErr)
		}
		rows.Close()
		if _, err := tx.Exec(`
UPDATE silence_rule_episodes
SET ended_at=?, end_reason='absent_confirmed', open_marker=NULL
WHERE open_marker=1
  AND rule_id IN (
    SELECT r.rule_id
    FROM silence_rules r
    WHERE r.cluster_id=?
      AND NOT EXISTS (
        SELECT 1
        FROM alertmanager_source_silences ss
        JOIN alertmanager_sources src ON src.source_name=ss.source_name
        WHERE ss.rule_id=r.rule_id
          AND src.current_cluster_id=?
          AND src.retired_at IS NULL
      )
  )
		`, nowRaw, clusterID, clusterID); err != nil {
			return fmt.Errorf("failed closing confirmed-absent silence episodes for cluster %q: %w", clusterID, err)
		}
		for _, episode := range closed {
			collector.append(runtimeEvent{level: slog.LevelInfo, component: "silence_reconciliation", event: "rule_episode_closed", message: "Silence rule episode closed", attrs: []slog.Attr{slog.String("target", episode.target), slog.String("silence_id", episode.silenceID), slog.Int64("episode_id", episode.episodeID), slog.String("reason_code", "absent_confirmed")}})
		}
	}
	return nil
}

type retiredSilenceSource struct {
	name   string
	target string
}

func retireDeconfiguredSourcesTx(tx *sql.Tx, configured map[string]struct{}, now time.Time) ([]retiredSilenceSource, error) {
	rows, err := tx.Query(`SELECT source_name, COALESCE(current_cluster_id, '') FROM alertmanager_sources WHERE retired_at IS NULL ORDER BY source_name`)
	if err != nil {
		return nil, fmt.Errorf("failed to query configured source retirement candidates: %w", err)
	}
	existing := make([]retiredSilenceSource, 0)
	for rows.Next() {
		var source retiredSilenceSource
		if err = rows.Scan(&source.name, &source.target); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan source retirement candidate: %w", err)
		}
		existing = append(existing, source)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("failed to iterate source retirement candidates: %w", err)
	}
	rows.Close()

	retired := make([]retiredSilenceSource, 0)
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	for _, source := range existing {
		if _, ok := configured[source.name]; ok {
			continue
		}
		res, updateErr := tx.Exec(`
UPDATE alertmanager_sources
SET sync_status='retired', retired_at=?, last_configured_at=?
WHERE source_name=? AND retired_at IS NULL
`, nowRaw, nowRaw, source.name)
		if updateErr != nil {
			return nil, fmt.Errorf("failed to retire deconfigured source %q: %w", source.name, updateErr)
		}
		affected, _ := res.RowsAffected()
		if affected > 0 {
			if _, deleteErr := tx.Exec(`DELETE FROM alertmanager_source_silences WHERE source_name=?`, source.name); deleteErr != nil {
				return nil, fmt.Errorf("failed clearing mutable silence relations for retired source %q: %w", source.name, deleteErr)
			}
			retired = append(retired, source)
		}
	}
	return retired, nil
}

func reconcileClusterConsistencyTx(tx *sql.Tx, sources []SourceSilenceState, now time.Time) error {
	byCluster := map[string][]SourceSilenceState{}
	for _, source := range sources {
		clusterID := strings.TrimSpace(source.ClusterID)
		if clusterID != "" {
			byCluster[clusterID] = append(byCluster[clusterID], source)
		}
	}
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	existingRows, err := tx.Query(`SELECT cluster_id FROM alertmanager_cluster_consistency`)
	if err != nil {
		return fmt.Errorf("failed to query cluster consistency retirement candidates: %w", err)
	}
	existingClusters := make([]string, 0)
	for existingRows.Next() {
		var clusterID string
		if err = existingRows.Scan(&clusterID); err != nil {
			existingRows.Close()
			return fmt.Errorf("failed to scan cluster consistency retirement candidate: %w", err)
		}
		existingClusters = append(existingClusters, clusterID)
	}
	existingRows.Close()
	for _, clusterID := range existingClusters {
		if _, ok := byCluster[clusterID]; ok {
			continue
		}
		if _, err = tx.Exec(`DELETE FROM alertmanager_cluster_consistency WHERE cluster_id=?`, clusterID); err != nil {
			return fmt.Errorf("failed to delete deconfigured cluster consistency %q: %w", clusterID, err)
		}
	}
	clusters := make([]string, 0, len(byCluster))
	for clusterID := range byCluster {
		clusters = append(clusters, clusterID)
	}
	slices.Sort(clusters)
	for _, clusterID := range clusters {
		members := byCluster[clusterID]
		slices.SortFunc(members, func(a, b SourceSilenceState) int {
			if sourceCompare := strings.Compare(strings.TrimSpace(a.SourceName), strings.TrimSpace(b.SourceName)); sourceCompare != 0 {
				return sourceCompare
			}
			return strings.Compare(strings.TrimSpace(a.ClusterID), strings.TrimSpace(b.ClusterID))
		})
		status := "unknown"
		allSuccessful := true
		fingerprints := map[string]struct{}{}
		for _, member := range members {
			if strings.TrimSpace(member.SyncStatus) != "success" {
				allSuccessful = false
				break
			}
			fingerprints[buildSourceConsistencyFingerprint(member)] = struct{}{}
		}
		if allSuccessful {
			if len(fingerprints) <= 1 {
				status = "consistent"
			} else {
				status = "inconsistent"
			}
		}
		if status == "unknown" {
			if _, err := tx.Exec(`
INSERT INTO alertmanager_cluster_consistency(cluster_id, status, member_count, last_evaluated_at)
VALUES(?, 'unknown', ?, ?)
ON CONFLICT(cluster_id) DO UPDATE SET member_count=excluded.member_count, last_evaluated_at=excluded.last_evaluated_at
`, clusterID, len(members), nowRaw); err != nil {
				return fmt.Errorf("failed to preserve unknown cluster consistency for %q: %w", clusterID, err)
			}
			continue
		}
		consistentAt := any(nil)
		inconsistentAt := any(nil)
		if status == "consistent" {
			consistentAt = nowRaw
		} else {
			inconsistentAt = nowRaw
		}
		if _, err := tx.Exec(`
INSERT INTO alertmanager_cluster_consistency(
  cluster_id, status, member_count, last_evaluated_at, last_consistent_at, last_inconsistent_at
) VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(cluster_id) DO UPDATE SET
  status=excluded.status,
  member_count=excluded.member_count,
  last_evaluated_at=excluded.last_evaluated_at,
  last_consistent_at=COALESCE(excluded.last_consistent_at, alertmanager_cluster_consistency.last_consistent_at),
  last_inconsistent_at=COALESCE(excluded.last_inconsistent_at, alertmanager_cluster_consistency.last_inconsistent_at)
`, clusterID, status, len(members), nowRaw, consistentAt, inconsistentAt); err != nil {
			return fmt.Errorf("failed to update cluster consistency for %q: %w", clusterID, err)
		}
	}
	return nil
}

func buildSourceConsistencyFingerprint(source SourceSilenceState) string {
	items := make([]string, 0, len(source.Silences))
	for _, silence := range source.Silences {
		contentFP, evaluationFP, _ := buildSilenceRevisionFingerprints(silence)
		items = append(items, strings.Join([]string{
			strings.TrimSpace(silence.SilenceID), contentFP, evaluationFP, strings.TrimSpace(silence.Status),
		}, "\x00"))
	}
	slices.Sort(items)
	raw, _ := json.Marshal(items)
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum)
}

func readSourceFingerprintAndStatusTx(tx *sql.Tx, sourceName string) (string, string, string, error) {
	var fingerprint sql.NullString
	var status string
	var clusterID string
	err := tx.QueryRow(`SELECT silence_set_fingerprint, sync_status, current_cluster_id FROM alertmanager_sources WHERE source_name = ?`, sourceName).Scan(&fingerprint, &status, &clusterID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "unknown", "", nil
	}
	if err != nil {
		return "", "", "", err
	}
	return fingerprint.String, status, clusterID, nil
}

type sourceEvaluationRule struct {
	status                string
	evaluationFingerprint string
}

func sourceSilenceChangeCanPromoteTx(tx *sql.Tx, sourceName string, next []SourceSilenceSnapshot) (bool, error) {
	previous := map[string]sourceEvaluationRule{}
	rows, err := tx.Query(`
SELECT r.silence_id, ss.observed_state, rev.evaluation_fingerprint
FROM alertmanager_source_silences ss
JOIN silence_rules r ON r.rule_id=ss.rule_id
JOIN silence_revisions rev ON rev.revision_id=ss.revision_id
WHERE ss.source_name=?
`, sourceName)
	if err != nil {
		return false, fmt.Errorf("failed loading source evaluation state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var silenceID string
		var rule sourceEvaluationRule
		if err = rows.Scan(&silenceID, &rule.status, &rule.evaluationFingerprint); err != nil {
			return false, fmt.Errorf("failed scanning source evaluation state: %w", err)
		}
		previous[silenceID] = rule
	}
	if err = rows.Err(); err != nil {
		return false, fmt.Errorf("failed iterating source evaluation state: %w", err)
	}
	for _, silence := range next {
		if silence.Status != "active" {
			continue
		}
		_, evaluationFingerprint, _ := buildSilenceRevisionFingerprints(silence)
		old, found := previous[strings.TrimSpace(silence.SilenceID)]
		if !found || old.status != "active" || old.evaluationFingerprint != evaluationFingerprint {
			return true, nil
		}
	}
	return false, nil
}

func buildSourceSilenceFingerprint(source SourceSilenceState) string {
	type silenceFP struct {
		ID                    string `json:"id"`
		Status                string `json:"status"`
		StartsAt              string `json:"startsAt"`
		EndsAt                string `json:"endsAt"`
		UpdatedAt             string `json:"updatedAt"`
		EvaluationFingerprint string `json:"evaluationFingerprint"`
	}
	type sourceFP struct {
		SourceName string      `json:"sourceName"`
		ClusterID  string      `json:"clusterId"`
		Silences   []silenceFP `json:"silences"`
	}
	entries := make([]silenceFP, 0, len(source.Silences))
	for _, s := range source.Silences {
		updatedAt := ""
		if s.UpdatedAt != nil {
			updatedAt = s.UpdatedAt.UTC().Format(time.RFC3339Nano)
		}
		_, evaluationFingerprint, _ := buildSilenceRevisionFingerprints(s)
		entries = append(entries, silenceFP{
			ID:                    strings.TrimSpace(s.SilenceID),
			Status:                strings.TrimSpace(s.Status),
			StartsAt:              s.StartsAt.UTC().Format(time.RFC3339Nano),
			EndsAt:                s.EndsAt.UTC().Format(time.RFC3339Nano),
			UpdatedAt:             updatedAt,
			EvaluationFingerprint: evaluationFingerprint,
		})
	}
	slices.SortFunc(entries, func(a, b silenceFP) int {
		if a.ID == b.ID {
			return strings.Compare(a.UpdatedAt+a.Status, b.UpdatedAt+b.Status)
		}
		return strings.Compare(a.ID, b.ID)
	})
	raw, _ := json.Marshal(sourceFP{
		SourceName: strings.TrimSpace(source.SourceName),
		ClusterID:  strings.TrimSpace(source.ClusterID),
		Silences:   entries,
	})
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum)
}

func upsertAlertmanagerSourceTx(tx *sql.Tx, source SourceSilenceState, sourceFP string, now time.Time) error {
	sourceName := strings.TrimSpace(source.SourceName)
	clusterID := strings.TrimSpace(source.ClusterID)
	syncStatus := strings.TrimSpace(source.SyncStatus)
	if syncStatus == "" {
		syncStatus = "unknown"
	}
	attemptAt := now.UTC().Format(time.RFC3339Nano)
	successAt := any(nil)
	fingerprint := any(nil)
	if syncStatus == "success" {
		successAt = attemptAt
		fingerprint = sourceFP
	}
	_, err := tx.Exec(`
INSERT INTO alertmanager_sources(
  source_name, current_cluster_id, sync_status, silence_set_fingerprint,
  first_configured_at, last_configured_at, last_attempt_at, last_success_at,
  last_confirmed_at, last_error, retired_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source_name) DO UPDATE SET
  current_cluster_id=excluded.current_cluster_id,
  sync_status=excluded.sync_status,
  silence_set_fingerprint=COALESCE(excluded.silence_set_fingerprint, alertmanager_sources.silence_set_fingerprint),
  last_configured_at=excluded.last_configured_at,
  last_attempt_at=excluded.last_attempt_at,
  last_success_at=COALESCE(excluded.last_success_at, alertmanager_sources.last_success_at),
  last_confirmed_at=COALESCE(excluded.last_success_at, alertmanager_sources.last_confirmed_at),
  last_error=excluded.last_error,
  retired_at=CASE WHEN excluded.sync_status='retired' THEN excluded.last_attempt_at ELSE NULL END
`, sourceName, clusterID, syncStatus, fingerprint, attemptAt, attemptAt, attemptAt, successAt, successAt, sanitizeNullableString(ptrString(source.LastError)), nil)
	if err != nil {
		return fmt.Errorf("failed to upsert alertmanager source %q: %w", sourceName, err)
	}
	return nil
}

func replaceSourceSilenceRelationsWithEventsTx(tx *sql.Tx, sourceName, clusterID string, silences []SourceSilenceSnapshot, now time.Time, collector *runtimeEventCollector) error {
	if _, err := tx.Exec(`DELETE FROM alertmanager_source_silences WHERE source_name = ?`, sourceName); err != nil {
		return fmt.Errorf("failed clearing source silence relations for %q: %w", sourceName, err)
	}
	for _, silence := range silences {
		ruleID, revisionID, err := upsertSilenceRuleAndRevisionWithEventsTx(tx, sourceName, clusterID, silence, now, collector)
		if err != nil {
			return err
		}
		nowRaw := now.UTC().Format(time.RFC3339Nano)
		_, err = tx.Exec(`
INSERT INTO alertmanager_source_silences(source_name, rule_id, revision_id, observed_state, first_observed_at, last_confirmed_at)
VALUES(?, ?, ?, ?, ?, ?)
		`, sourceName, ruleID, revisionID, silence.Status, nowRaw, nowRaw)
		if err != nil {
			return fmt.Errorf("failed inserting source silence relation: %w", err)
		}
	}
	return nil
}

// knownSourceSilences enforces the mapper's status contract at the storage
// boundary. The complete Alertmanager response validation belongs to the mapper.
func knownSourceSilences(silences []SourceSilenceSnapshot) []SourceSilenceSnapshot {
	known := make([]SourceSilenceSnapshot, 0, len(silences))
	for _, silence := range silences {
		if models.IsKnownSilenceStatus(silence.Status) {
			known = append(known, silence)
		}
	}
	slices.SortFunc(known, func(a, b SourceSilenceSnapshot) int {
		return strings.Compare(strings.TrimSpace(a.SilenceID), strings.TrimSpace(b.SilenceID))
	})
	return known
}

func upsertSilenceRuleAndRevisionTx(tx *sql.Tx, clusterID string, silence SourceSilenceSnapshot, now time.Time) (int64, int64, error) {
	return upsertSilenceRuleAndRevisionWithEventsTx(tx, "", clusterID, silence, now, nil)
}

func upsertSilenceRuleAndRevisionWithEventsTx(tx *sql.Tx, sourceName, clusterID string, silence SourceSilenceSnapshot, now time.Time, collector *runtimeEventCollector) (int64, int64, error) {
	silenceID := strings.TrimSpace(silence.SilenceID)
	if silenceID == "" {
		return 0, 0, errors.New("silence ID cannot be empty")
	}
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	var existingRuleID int64
	ruleLookupErr := tx.QueryRow(`SELECT rule_id FROM silence_rules WHERE cluster_id=? AND silence_id=?`, clusterID, silenceID).Scan(&existingRuleID)
	ruleDiscovered := errors.Is(ruleLookupErr, sql.ErrNoRows)
	if ruleLookupErr != nil && !ruleDiscovered {
		return 0, 0, fmt.Errorf("failed checking silence rule existence: %w", ruleLookupErr)
	}
	_, err := tx.Exec(`
INSERT INTO silence_rules(cluster_id, silence_id, first_observed_at, last_observed_at)
VALUES(?, ?, ?, ?)
ON CONFLICT(cluster_id, silence_id) DO UPDATE SET last_observed_at=excluded.last_observed_at
`, clusterID, silenceID, nowRaw, nowRaw)
	if err != nil {
		return 0, 0, fmt.Errorf("failed upserting silence rule: %w", err)
	}
	var ruleID int64
	if err = tx.QueryRow(`SELECT rule_id FROM silence_rules WHERE cluster_id = ? AND silence_id = ?`, clusterID, silenceID).Scan(&ruleID); err != nil {
		return 0, 0, fmt.Errorf("failed loading silence rule id: %w", err)
	}
	if ruleDiscovered {
		collector.append(runtimeEvent{level: slog.LevelDebug, component: "silence_reconciliation", event: "rule_discovered", message: "Silence rule discovered", attrs: []slog.Attr{slog.String("source", sourceName), slog.String("target", clusterID), slog.String("silence_id", silenceID), slog.Int64("rule_id", ruleID), slog.String("status", strings.TrimSpace(silence.Status))}})
	}
	var episodeID int64
	err = tx.QueryRow(`SELECT episode_id FROM silence_rule_episodes WHERE rule_id = ? AND open_marker = 1`, ruleID).Scan(&episodeID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		nextEpisode := 1
		_ = tx.QueryRow(`SELECT COALESCE(MAX(episode_number),0)+1 FROM silence_rule_episodes WHERE rule_id = ?`, ruleID).Scan(&nextEpisode)
		res, ierr := tx.Exec(`
INSERT INTO silence_rule_episodes(rule_id, episode_number, first_observed_at, last_confirmed_at, ended_at, end_reason, open_marker)
VALUES(?, ?, ?, ?, NULL, NULL, 1)
`, ruleID, nextEpisode, nowRaw, nowRaw)
		if ierr != nil {
			return 0, 0, fmt.Errorf("failed creating silence episode: %w", ierr)
		}
		episodeID, _ = res.LastInsertId()
		collector.append(runtimeEvent{level: slog.LevelDebug, component: "silence_reconciliation", event: "rule_episode_opened", message: "Silence rule episode opened", attrs: []slog.Attr{slog.String("target", clusterID), slog.String("silence_id", silenceID), slog.Int64("episode_id", episodeID), slog.Int("episode_number", nextEpisode)}})
	case err != nil:
		return 0, 0, fmt.Errorf("failed loading silence episode: %w", err)
	default:
		if _, err = tx.Exec(`UPDATE silence_rule_episodes SET last_confirmed_at = ? WHERE episode_id = ?`, nowRaw, episodeID); err != nil {
			return 0, 0, fmt.Errorf("failed updating silence episode: %w", err)
		}
	}

	contentFP, evalFP, matchersJSON := buildSilenceRevisionFingerprints(silence)
	var (
		revisionID            int64
		latestContentFP       string
		latestSourceUpdatedAt sql.NullString
	)
	updatedAtRaw := any(nil)
	updatedAtNormalized := sql.NullString{}
	if silence.UpdatedAt != nil {
		value := silence.UpdatedAt.UTC().Format(time.RFC3339Nano)
		updatedAtRaw = value
		updatedAtNormalized = sql.NullString{String: value, Valid: true}
	}
	err = tx.QueryRow(`
SELECT revision_id, content_fingerprint, source_updated_at
FROM silence_revisions
WHERE episode_id = ?
ORDER BY revision_number DESC
LIMIT 1
	`, episodeID).Scan(&revisionID, &latestContentFP, &latestSourceUpdatedAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, 0, fmt.Errorf("failed loading latest silence revision: %w", err)
	}
	latestMatches := err == nil && latestContentFP == contentFP && latestSourceUpdatedAt == updatedAtNormalized
	if errors.Is(err, sql.ErrNoRows) || !latestMatches {
		nextRevision := 1
		_ = tx.QueryRow(`SELECT COALESCE(MAX(revision_number),0)+1 FROM silence_revisions WHERE episode_id = ?`, episodeID).Scan(&nextRevision)
		annotationsJSON := strings.TrimSpace(silence.AnnotationsJSON)
		if annotationsJSON == "" {
			annotationsJSON = "{}"
		}
		res, ierr := tx.Exec(`
INSERT INTO silence_revisions(
  episode_id, rule_id, revision_number, content_fingerprint, evaluation_fingerprint,
  matchers_json, starts_at, ends_at, source_updated_at, created_by, comment,
  annotations_json, ticket_id, ticket_url, first_observed_at, last_observed_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, episodeID, ruleID, nextRevision, contentFP, evalFP, matchersJSON,
			silence.StartsAt.UTC().Format(time.RFC3339Nano), silence.EndsAt.UTC().Format(time.RFC3339Nano),
			updatedAtRaw, silence.CreatedBy, silence.Comment, annotationsJSON, silence.TicketID, silence.TicketURL,
			nowRaw, nowRaw)
		if ierr != nil {
			return 0, 0, fmt.Errorf("failed creating silence revision: %w", ierr)
		}
		revisionID, _ = res.LastInsertId()
		reasonCode := "content_changed"
		if errors.Is(err, sql.ErrNoRows) {
			reasonCode = "initial_observation"
		} else if latestContentFP == contentFP {
			reasonCode = "source_updated_at_changed"
		}
		collector.append(runtimeEvent{level: slog.LevelDebug, component: "silence_reconciliation", event: "rule_revision_created", message: "Silence rule revision created", attrs: []slog.Attr{slog.String("target", clusterID), slog.String("silence_id", silenceID), slog.Int64("rule_id", ruleID), slog.Int64("revision_id", revisionID), slog.String("reason_code", reasonCode)}})
	} else {
		if _, err = tx.Exec(`UPDATE silence_revisions SET last_observed_at = ? WHERE revision_id = ?`, nowRaw, revisionID); err != nil {
			return 0, 0, fmt.Errorf("failed updating silence revision observation time: %w", err)
		}
	}

	return ruleID, revisionID, nil
}

func buildSilenceRevisionFingerprints(silence SourceSilenceSnapshot) (string, string, string) {
	matchers := append([]SilenceMatcherSnapshot(nil), silence.Matchers...)
	slices.SortFunc(matchers, func(a, b SilenceMatcherSnapshot) int {
		if a.Name == b.Name {
			if a.Value == b.Value {
				if a.IsRegex == b.IsRegex {
					if a.IsEqual == b.IsEqual {
						return 0
					}
					if !a.IsEqual {
						return -1
					}
					return 1
				}
				if !a.IsRegex {
					return -1
				}
				return 1
			}
			return strings.Compare(a.Value, b.Value)
		}
		return strings.Compare(a.Name, b.Name)
	})
	matchersJSONRaw, _ := json.Marshal(matchers)
	matchersJSON := string(matchersJSONRaw)
	type fp struct {
		Matchers    []SilenceMatcherSnapshot `json:"matchers"`
		StartsAt    string                   `json:"startsAt"`
		EndsAt      string                   `json:"endsAt"`
		CreatedBy   string                   `json:"createdBy"`
		Comment     string                   `json:"comment"`
		Annotations string                   `json:"annotations"`
	}
	contentRaw, _ := json.Marshal(fp{
		Matchers:    matchers,
		StartsAt:    silence.StartsAt.UTC().Format(time.RFC3339Nano),
		EndsAt:      silence.EndsAt.UTC().Format(time.RFC3339Nano),
		CreatedBy:   silence.CreatedBy,
		Comment:     silence.Comment,
		Annotations: silence.AnnotationsJSON,
	})
	evalRaw, _ := json.Marshal(struct {
		Matchers []SilenceMatcherSnapshot `json:"matchers"`
		StartsAt string                   `json:"startsAt"`
		EndsAt   string                   `json:"endsAt"`
	}{
		Matchers: matchers,
		StartsAt: silence.StartsAt.UTC().Format(time.RFC3339Nano),
		EndsAt:   silence.EndsAt.UTC().Format(time.RFC3339Nano),
	})
	contentSum := sha256.Sum256(contentRaw)
	evalSum := sha256.Sum256(evalRaw)
	return fmt.Sprintf("sha256:%x", contentSum), fmt.Sprintf("sha256:%x", evalSum), matchersJSON
}

func setMetadataTx(tx *sql.Tx, key, value string) error {
	_, err := tx.Exec(`INSERT INTO metadata(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("failed to upsert metadata key %q: %w", key, err)
	}
	return nil
}

func getOptionalMetadataTimeFrom(q lifecycleStatsQueryer, key string) (*time.Time, error) {
	var raw sql.NullString
	err := q.QueryRow(`SELECT value FROM metadata WHERE key = ?`, key).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read metadata key %q: %w", key, err)
	}
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return nil, nil
	}
	ts, err := time.Parse(time.RFC3339Nano, raw.String)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata key %q: %w", key, err)
	}
	return &ts, nil
}

func queryInt(q interface {
	QueryRow(query string, args ...any) *sql.Row
}, query string, args ...any,
) (int, error) {
	var out int
	if err := q.QueryRow(query, args...).Scan(&out); err != nil {
		return 0, err
	}
	return out, nil
}

func queryIntContext(
	ctx context.Context,
	q interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	query string,
	args ...any,
) (int, error) {
	var out int
	if err := q.QueryRowContext(ctx, query, args...).Scan(&out); err != nil {
		return 0, err
	}
	return out, nil
}

func readMetadataStringTx(tx *sql.Tx, key string) (string, error) {
	var value sql.NullString
	err := tx.QueryRow(`SELECT value FROM metadata WHERE key = ?`, key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to read metadata key %q: %w", key, err)
	}
	if !value.Valid {
		return "", nil
	}
	return value.String, nil
}

func readMetadataIntTx(tx *sql.Tx, key string) (int, error) {
	raw, err := readMetadataStringTx(tx, key)
	if err != nil {
		return 0, err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("failed to parse metadata int key %q value %q: %w", key, raw, err)
	}
	if v < 0 {
		return 0, nil
	}
	return v, nil
}

func readMetadataInt64Tx(tx *sql.Tx, key string) (int64, error) {
	raw, err := readMetadataStringTx(tx, key)
	if err != nil {
		return 0, err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse metadata int64 key %q value %q: %w", key, raw, err)
	}
	if v < 0 {
		return 0, nil
	}
	return v, nil
}

func setMetadataIntTx(tx *sql.Tx, key string, value int) error {
	if value < 0 {
		value = 0
	}
	return setMetadataTx(tx, key, strconv.Itoa(value))
}

func setMetadataInt64Tx(tx *sql.Tx, key string, value int64) error {
	if value < 0 {
		value = 0
	}
	return setMetadataTx(tx, key, strconv.FormatInt(value, 10))
}

func setMetadataTimeTx(tx *sql.Tx, key string, ts *time.Time) error {
	if ts == nil {
		return setMetadataTx(tx, key, "")
	}
	v := ts.UTC().Format(time.RFC3339Nano)
	return setMetadataTx(tx, key, v)
}

func getMetadataStringFrom(q lifecycleStatsQueryer, key string) (string, error) {
	var value sql.NullString
	err := q.QueryRow(`SELECT value FROM metadata WHERE key = ?`, key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to read metadata key %q: %w", key, err)
	}
	if !value.Valid {
		return "", nil
	}
	return value.String, nil
}

func getMetadataIntFrom(q lifecycleStatsQueryer, key string) (int, error) {
	raw, err := getMetadataStringFrom(q, key)
	if err != nil {
		return 0, err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("failed to parse metadata int key %q value %q: %w", key, raw, err)
	}
	if v < 0 {
		return 0, nil
	}
	return v, nil
}

func clampInt64ToInt(v int64) int64 {
	if v <= 0 {
		return 0
	}
	if v > int64(math.MaxInt) {
		return int64(math.MaxInt)
	}
	return v
}
