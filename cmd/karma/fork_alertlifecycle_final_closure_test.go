package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
)

type finalClosurePanickingHandler struct{}

func (finalClosurePanickingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (finalClosurePanickingHandler) Handle(context.Context, slog.Record) error {
	panic("controlled scheduler event failure")
}

func (h finalClosurePanickingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h finalClosurePanickingHandler) WithGroup(string) slog.Handler { return h }

type finalClosureSchedulerSnapshot struct {
	ResultTerminal           bool
	TimerC                   bool
	TimerActive              bool
	TimerKind                recheckSchedulerTimerKind
	TimerOwner               recheckSchedulerBackoffOwner
	TimerAt                  time.Time
	Backoff                  map[recheckSchedulerBackoffOwner]int
	TimerResets              []time.Duration
	RunnerCalls              int
	WakePending              bool
	ReconciledWakePending    bool
	Processing               bool
	Terminal                 bool
	AcquisitionFailureActive bool
	BlockedHeadID            int64
	BlockedHeadStatus        string
}

func runFinalClosureSchedulerCase(t *testing.T, panicOnEvents bool, runner func() (alertlifecycle.RecheckBatchResult, error)) finalClosureSchedulerSnapshot {
	t.Helper()
	previousLogger := slog.Default()
	if panicOnEvents {
		slog.SetDefault(slog.New(finalClosurePanickingHandler{}))
	} else {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	runnerCalls := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		runnerCalls++
		return runner()
	}
	result := scheduler.process(clock.NewTimer(time.Hour), nil)
	scheduler.stateMu.Lock()
	snapshot := finalClosureSchedulerSnapshot{
		ResultTerminal:           result.terminal,
		TimerC:                   result.timerC != nil,
		TimerActive:              scheduler.timerActive,
		TimerKind:                scheduler.timerKind,
		TimerOwner:               scheduler.timerOwner,
		TimerAt:                  scheduler.timerAt,
		Backoff:                  mapsClone(scheduler.backoffStep),
		TimerResets:              append([]time.Duration(nil), clock.timer.resets...),
		RunnerCalls:              runnerCalls,
		WakePending:              scheduler.wakePending,
		ReconciledWakePending:    scheduler.reconciledWakePending,
		Processing:               scheduler.processing,
		Terminal:                 scheduler.terminal,
		AcquisitionFailureActive: scheduler.acquisitionFailureActive,
		BlockedHeadID:            scheduler.blockedHeadID,
		BlockedHeadStatus:        scheduler.blockedHeadStatus,
	}
	scheduler.stateMu.Unlock()
	return snapshot
}

type finalClosureRealSchedulerSnapshot struct {
	Scheduler finalClosureSchedulerSnapshot
	Durable   string
	Pending   alertlifecycle.RecheckPendingStateSnapshot
}

type finalClosureSchedulerEventPanicHandler struct{}

func (finalClosureSchedulerEventPanicHandler) Enabled(context.Context, slog.Level) bool { return true }

func (finalClosureSchedulerEventPanicHandler) Handle(_ context.Context, record slog.Record) error {
	switch record.Message {
	case "Recheck retry was durably scheduled", "Recheck scheduler event emission failed":
		panic("controlled scheduler event failure")
	default:
		return nil
	}
}

func (h finalClosureSchedulerEventPanicHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h finalClosureSchedulerEventPanicHandler) WithGroup(string) slog.Handler { return h }

type finalClosureFallbackLevelHandler struct {
	fallbackLevel slog.Level
	fallbackSeen  bool
}

func (h *finalClosureFallbackLevelHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *finalClosureFallbackLevelHandler) Handle(_ context.Context, record slog.Record) error {
	event := ""
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "event" {
			event = attr.Value.String()
		}
		return true
	})
	if event == "retry_scheduled" {
		panic("controlled primary scheduler event failure")
	}
	if event == "observability_collection_failed" {
		h.fallbackSeen = true
		h.fallbackLevel = record.Level
	}
	return nil
}

func (h *finalClosureFallbackLevelHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *finalClosureFallbackLevelHandler) WithGroup(string) slog.Handler { return h }

func TestProblem4SchedulerFallbackKeepsRegisteredWarningLevel(t *testing.T) {
	handler := &finalClosureFallbackLevelHandler{}
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	scheduler := newRecheckScheduler(nil, &reviewSchedulerClock{now: time.Now().UTC()})
	scheduler.logBestEffort(func() {
		runtimeLog.Event(context.Background(), slog.LevelWarn, "silence_recheck", "retry_scheduled", "retry scheduled")
	})
	if !handler.fallbackSeen || handler.fallbackLevel != slog.LevelWarn {
		t.Fatalf("fallback level=%s seen=%v want warning/true", handler.fallbackLevel, handler.fallbackSeen)
	}
}

func runFinalClosureRealSchedulerCase(t *testing.T, panicOnSchedulerEvent bool) finalClosureRealSchedulerSnapshot {
	t.Helper()
	store, db, now := seedProblem3Review3SchedulerStore(t)
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		RecheckFault: func(stage string) error {
			if stage == "persist_candidate" {
				return schedulerSQLiteError(5)
			}
			return nil
		},
		Now: func() time.Time { return now },
		OperationToken: func() (string, error) {
			return "final-closure-operation-token", nil
		},
	})
	previousLogger := slog.Default()
	if panicOnSchedulerEvent {
		slog.SetDefault(slog.New(finalClosureSchedulerEventPanicHandler{}))
	} else {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}
	defer slog.SetDefault(previousLogger)
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	runnerCalls := 0
	runner := scheduler.runner
	scheduler.runner = func(ctx context.Context, batchSize int) (alertlifecycle.RecheckBatchResult, error) {
		runnerCalls++
		return runner(ctx, batchSize)
	}
	result := scheduler.process(clock.NewTimer(time.Hour), nil)
	scheduler.stateMu.Lock()
	snapshot := finalClosureSchedulerSnapshot{
		ResultTerminal:           result.terminal,
		TimerC:                   result.timerC != nil,
		TimerActive:              scheduler.timerActive,
		TimerKind:                scheduler.timerKind,
		TimerOwner:               scheduler.timerOwner,
		TimerAt:                  scheduler.timerAt,
		Backoff:                  mapsClone(scheduler.backoffStep),
		TimerResets:              append([]time.Duration(nil), clock.timer.resets...),
		RunnerCalls:              runnerCalls,
		WakePending:              scheduler.wakePending,
		ReconciledWakePending:    scheduler.reconciledWakePending,
		Processing:               scheduler.processing,
		Terminal:                 scheduler.terminal,
		AcquisitionFailureActive: scheduler.acquisitionFailureActive,
		BlockedHeadID:            scheduler.blockedHeadID,
		BlockedHeadStatus:        scheduler.blockedHeadStatus,
	}
	scheduler.stateMu.Unlock()
	return finalClosureRealSchedulerSnapshot{
		Scheduler: snapshot,
		Durable:   captureFinalClosureRealDurableState(t, db),
		Pending:   store.RecheckPendingStateSnapshot(),
	}
}

func captureFinalClosureRealDurableState(t *testing.T, db *sql.DB) string {
	t.Helper()
	parts := make([]string, 0, 11)
	for _, table := range []string{
		"silence_recheck_jobs",
		"silence_recheck_job_rules",
		"silence_recheck_job_candidates",
		"silence_recheck_job_attempts",
		"silence_recheck_job_operator_actions",
		"alert_states",
		"alert_target_states",
		"alert_events",
		"alert_target_events",
		"alert_suppression_cause_history",
		"alert_target_suppression_cause_history",
	} {
		parts = append(parts, table+":"+captureReview6Rows(t, db, "SELECT * FROM "+table+" ORDER BY rowid"))
	}
	// The seed's unrelated poll ledger token is random; the retry operation token is fixed above.
	parts = append(parts, "lifecycle_writer_commits:"+captureReview6Rows(t, db, "SELECT operation_kind FROM lifecycle_writer_commits ORDER BY rowid"))
	return strings.Join(parts, "\n")
}

func mapsClone(input map[recheckSchedulerBackoffOwner]int) map[recheckSchedulerBackoffOwner]int {
	output := make(map[recheckSchedulerBackoffOwner]int, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func TestProblem4SchedulerEventPanicsDoNotChangeControlState(t *testing.T) {
	now := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	next := now.Add(time.Minute)
	busy := problem4SQLiteBusyError{}
	cases := []struct {
		name   string
		runner func() (alertlifecycle.RecheckBatchResult, error)
		check  func(t *testing.T, snapshot finalClosureSchedulerSnapshot)
	}{
		{
			name: "retry scheduling",
			runner: func() (alertlifecycle.RecheckBatchResult, error) {
				return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchRetryScheduled, JobID: 1, Status: "retry_wait", AttemptCount: 1, RetryCount: 1, ConsecutiveRetryCount: 1, FailureClass: "retryable", FailureStage: "persist_candidate", NextAttemptAt: &next}, errors.New("operational retry result")
			},
			check: func(t *testing.T, snapshot finalClosureSchedulerSnapshot) {
				if !snapshot.TimerActive || snapshot.TimerKind != recheckSchedulerTimerPersistedDue || !snapshot.TimerAt.Equal(next) {
					t.Fatalf("persisted retry timer was not armed exactly: %+v", snapshot)
				}
			},
		},
		{
			name: "deferral",
			runner: func() (alertlifecycle.RecheckBatchResult, error) {
				return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchRetryDeferred, JobID: 1, Status: "retry_wait", NextAttemptAt: &next}, nil
			},
			check: func(t *testing.T, snapshot finalClosureSchedulerSnapshot) {
				if !snapshot.TimerActive || snapshot.TimerKind != recheckSchedulerTimerPersistedDue || !snapshot.TimerAt.Equal(next) {
					t.Fatalf("deferred timer changed: %+v", snapshot)
				}
			},
		},
		{
			name: "blocker",
			runner: func() (alertlifecycle.RecheckBatchResult, error) {
				return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchQueueBlocked, JobID: 1, Status: "failed"}, nil
			},
		},
		{
			name: "acquisition",
			runner: func() (alertlifecycle.RecheckBatchResult, error) {
				return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchAcquisitionFailed}, errors.Join(alertlifecycle.ErrWriterAcquisition, busy)
			},
			check: func(t *testing.T, snapshot finalClosureSchedulerSnapshot) {
				if !snapshot.TimerActive || snapshot.TimerKind != recheckSchedulerTimerBackoff || snapshot.TimerOwner != recheckBackoffAcquisition {
					t.Fatalf("acquisition backoff changed: %+v", snapshot)
				}
			},
		},
		{
			name: "terminal infrastructure",
			runner: func() (alertlifecycle.RecheckBatchResult, error) {
				return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchInfrastructureFailed, FailureClass: "writer_invariant", FailureStage: "batch"}, errors.New("terminal infrastructure")
			},
			check: func(t *testing.T, snapshot finalClosureSchedulerSnapshot) {
				if !snapshot.ResultTerminal || snapshot.TimerActive {
					t.Fatalf("terminal infrastructure control state changed: %+v", snapshot)
				}
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			control := runFinalClosureSchedulerCase(t, false, test.runner)
			fault := runFinalClosureSchedulerCase(t, true, test.runner)
			if !reflect.DeepEqual(fault, control) {
				t.Fatalf("event panic changed scheduler control state: fault=%+v control=%+v", fault, control)
			}
			if test.check != nil {
				test.check(t, fault)
			}
		})
	}
}

func TestProblem4RealRetrySchedulingSurvivesSchedulerEventPanic(t *testing.T) {
	control := runFinalClosureRealSchedulerCase(t, false)
	fault := runFinalClosureRealSchedulerCase(t, true)
	if !reflect.DeepEqual(fault, control) {
		t.Fatalf("scheduler event panic changed real durable retry control: fault=%+v control=%+v", fault, control)
	}
	wantAt := time.Date(2026, time.August, 19, 11, 0, 1, 0, time.UTC)
	if !fault.Scheduler.TimerC || !fault.Scheduler.TimerActive || fault.Scheduler.TimerKind != recheckSchedulerTimerPersistedDue || !fault.Scheduler.TimerAt.Equal(wantAt) {
		t.Fatalf("real persisted retry timer was not preserved: %+v", fault.Scheduler)
	}
	if fault.Scheduler.RunnerCalls != 1 || fault.Scheduler.ResultTerminal || fault.Scheduler.Terminal {
		t.Fatalf("real retry scheduler control changed: %+v", fault.Scheduler)
	}
}

func TestProblem4RegisteredRouterRejectsUnchangedRequestID(t *testing.T) {
	path := t.TempDir() + "/final-closure.sqlite"
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	previousStore := lifecycleStore
	previousHeader := config.Config.Authentication.Header
	previousUsers := config.Config.Authentication.BasicAuth.Users
	previousPrefix := config.Config.Listen.Prefix
	lifecycleStore = store
	config.Config.Authentication.Header.Name = ""
	config.Config.Authentication.BasicAuth.Users = nil
	config.Config.Listen.Prefix = "/"
	t.Cleanup(func() {
		lifecycleStore = previousStore
		config.Config.Authentication.Header = previousHeader
		config.Config.Authentication.BasicAuth.Users = previousUsers
		config.Config.Listen.Prefix = previousPrefix
		_ = store.Close()
	})
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := "2026-08-26T12:00:00Z"
	if _, err = db.Exec(`INSERT INTO silence_recheck_jobs(status,created_at,started_at,finished_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type) VALUES('failed',?,?,?,'{"formatVersion":1,"sources":[],"clusters":[]}',?,?,?)`, now, now, now, "final-closure", "sha256:final-closure", "source_context_changed"); err != nil {
		t.Fatal(err)
	}
	var callbackCount atomic.Int32
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		AttemptLookup: func(context.Context, string) (int64, int, string, error) {
			callbackCount.Add(1)
			return 0, 0, "", nil
		},
		TransactionFinished: func(string) { callbackCount.Add(1) },
	})
	var wakeCount atomic.Int32
	store.SetRecheckCommitWake(func() { wakeCount.Add(1) })
	store.SetRecheckReconciledWake(func() { wakeCount.Add(1) })
	before := captureFinalClosureRouteState(t, db)
	beforePending := store.RecheckPendingStateSnapshot()
	router := chi.NewRouter()
	setupRouter(router, nil)
	for _, requestID := range []string{" request-1", "request-2 ", "request\t3", "request\n4", "request/5"} {
		t.Run(strings.ReplaceAll(requestID, "\n", "\\n"), func(t *testing.T) {
			body, marshalErr := json.Marshal(map[string]string{"requestId": requestID})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/alerts-history/recheck/1/retry", strings.NewReader(string(body)))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(recheckOperatorRequestHeader, "recheck-operator")
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("request ID %q status=%d body=%s", requestID, recorder.Code, recorder.Body.String())
			}
			if got := captureFinalClosureRouteState(t, db); got != before {
				t.Fatalf("request ID %q changed durable state", requestID)
			}
			if got := store.RecheckPendingStateSnapshot(); got != beforePending {
				t.Fatalf("request ID %q changed pending finalizer state: got=%+v want=%+v", requestID, got, beforePending)
			}
			if callbackCount.Load() != 0 || wakeCount.Load() != 0 {
				t.Fatalf("request ID %q reached Store side effects callbacks=%d wakes=%d", requestID, callbackCount.Load(), wakeCount.Load())
			}
		})
	}
}

func captureFinalClosureRouteState(t *testing.T, db *sql.DB) string {
	t.Helper()
	parts := make([]string, 0, 3)
	parts = append(parts, captureProblem4RouterState(t, db))
	for _, table := range []string{"alert_events", "alert_target_events"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, table+":"+strconv.Itoa(count))
	}
	return strings.Join(parts, "\n")
}
