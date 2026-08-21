package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

type problem4LockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

type problem4SchedulerPanicHandler struct {
	base  slog.Handler
	event string
}

func (h problem4SchedulerPanicHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level)
}

func (h problem4SchedulerPanicHandler) Handle(ctx context.Context, record slog.Record) error {
	event := ""
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "event" {
			event = attr.Value.String()
		}
		return true
	})
	if event == h.event {
		panic("controlled scheduler event failure")
	}
	return h.base.Handle(ctx, record)
}

func (h problem4SchedulerPanicHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return problem4SchedulerPanicHandler{base: h.base.WithAttrs(attrs), event: h.event}
}

func (h problem4SchedulerPanicHandler) WithGroup(name string) slog.Handler {
	return problem4SchedulerPanicHandler{base: h.base.WithGroup(name), event: h.event}
}

func (b *problem4LockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(data)
}

func (b *problem4LockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestProblem4SchedulerOwnsFailedHeadEventAndSuppressesRepeatedWarnings(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	scheduler := newRecheckScheduler(nil, clock)
	queueDepth := 2
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchQueueBlocked, JobID: 547, Status: "failed", QueueDepth: &queueDepth}, nil
	}
	scheduler.process(clock.NewTimer(time.Second), nil)
	scheduler.process(clock.NewTimer(time.Second), nil)
	output := logs.String()
	if got := strings.Count(output, `"event":"failed_job_blocks_queue"`); got != 2 {
		t.Fatalf("expected one warning and one unchanged debug blocker event, got=%d output=%s", got, output)
	}
	if !strings.Contains(output, `"level":"WARN"`) || !strings.Contains(output, `"level":"DEBUG"`) {
		t.Fatalf("expected edge warning and unchanged debug levels: %s", output)
	}
	if strings.Contains(output, `"event":"job_selected"`) || strings.Contains(output, `"event":"job_failed"`) {
		t.Fatalf("scheduler emitted worker-owned event for persisted blocker: %s", output)
	}
}

func TestProblem4SchedulerOwnsPreselectionAcquisitionEvent(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	scheduler := newRecheckScheduler(nil, clock)
	sentinel := "attempt-token-problem4-scheduler-redaction"
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchAcquisitionFailed}, errors.Join(alertlifecycle.ErrWriterAcquisition, errors.New(sentinel))
	}
	result := scheduler.process(clock.NewTimer(time.Second), nil)
	if !result.terminal {
		t.Fatal("terminal preselection acquisition failure did not stop scheduler")
	}
	if !strings.Contains(logs.String(), `"event":"worker_acquisition_failed"`) {
		t.Fatalf("missing scheduler-owned acquisition event: %s", logs.String())
	}
	if strings.Contains(logs.String(), sentinel) {
		t.Fatalf("scheduler acquisition event disclosed raw error: %s", logs.String())
	}
}

func TestProblem4AcquisitionWarningEdgeResetsAfterRetryablePreAttempt(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	scheduler := newRecheckScheduler(nil, clock)
	run := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		run++
		switch run {
		case 1, 2, 4:
			return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchAcquisitionFailed}, errors.Join(alertlifecycle.ErrWriterAcquisition, problem4SQLiteBusyError{})
		case 3:
			return alertlifecycle.RecheckBatchResult{}, schedulerPreAttemptError{}
		default:
			return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchQueueEmpty}, nil
		}
	}
	for range 4 {
		scheduler.process(clock.NewTimer(time.Second), nil)
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	levels := make([]string, 0, 2)
	for _, line := range lines {
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) == nil && event["event"] == "worker_acquisition_failed" {
			levels = append(levels, event["level"].(string))
		}
	}
	if len(levels) != 3 || levels[0] != "WARN" || levels[1] != "DEBUG" || levels[2] != "WARN" {
		t.Fatalf("acquisition edge levels=%v logs=%s", levels, logs.String())
	}
}

func TestProblem4SchedulerOwnsRetryScheduledEvent(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	scheduler := newRecheckScheduler(nil, clock)
	next := clock.now.Add(time.Second)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{
			Disposition: alertlifecycle.RecheckBatchRetryScheduled, JobID: 547, Status: "retry_wait",
			AttemptCount: 2, RetryCount: 1, ConsecutiveRetryCount: 1,
			FailureClass: "retryable_storage", FailureStage: "persist_candidate", NextAttemptAt: &next,
		}, errors.New("operational retry result")
	}
	result := scheduler.process(clock.NewTimer(time.Second), nil)
	if result.timerC == nil {
		t.Fatal("retry-scheduled result did not arm persisted timer")
	}
	output := logs.String()
	if !strings.Contains(output, `"event":"retry_scheduled"`) || !strings.Contains(output, `"level":"WARN"`) || !strings.Contains(output, `"attempt_count":2`) || !strings.Contains(output, `"retry_count":1`) {
		t.Fatalf("missing scheduler-owned retry event fields: %s", output)
	}
}

type problem4SQLiteBusyError struct{}

func (problem4SQLiteBusyError) Error() string { return "controlled sqlite busy" }

func (problem4SQLiteBusyError) SQLiteCode() int { return 5 }

func TestProblem4RealStoreFailureSequenceHasOneOwnerPerEvent(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, _, now := seedProblem3Review3SchedulerStore(t)
	current := now
	faultActive := true
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		Now: func() time.Time { return current },
		RecheckFault: func(stage string) error {
			if stage == "persist_candidate" && faultActive {
				return problem4SQLiteBusyError{}
			}
			return nil
		},
	})
	t.Cleanup(func() { store.SetRecheckIntegrationTestHooks(nil) })
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(scheduler.Signal)
	result := scheduler.process(clock.NewTimer(time.Second), nil)
	if result.timerC == nil {
		t.Fatal("retry scheduling did not arm a timer")
	}
	output := logs.String()
	selected := strings.Index(output, `"event":"job_selected"`)
	retry := strings.Index(output, `"event":"retry_scheduled"`)
	if selected < 0 || retry < 0 || selected > retry {
		t.Fatalf("expected Store job_selected before scheduler retry_scheduled: %s", output)
	}
	selectedLine := output[selected:]
	if newline := strings.IndexByte(selectedLine, '\n'); newline >= 0 {
		selectedLine = selectedLine[:newline]
	}
	if !strings.Contains(selectedLine, `"status":"queued"`) {
		t.Fatalf("job_selected did not retain selection-time status: %s", selectedLine)
	}
	if strings.Contains(output, `"event":"job_failed"`) || strings.Count(output, `"event":"retry_scheduled"`) != 1 {
		t.Fatalf("unexpected failure event sequence: %s", output)
	}
	if !strings.Contains(output, `"level":"WARN"`) || !strings.Contains(output, `"attempt_count":1`) || !strings.Contains(output, `"retry_count":1`) {
		t.Fatalf("retry event lacks durable fields: %s", output)
	}
	eventRecord := func(output, event string) map[string]any {
		t.Helper()
		for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
			var record map[string]any
			if json.Unmarshal([]byte(line), &record) == nil && record["event"] == event {
				return record
			}
		}
		t.Fatalf("missing %s event: %s", event, output)
		return nil
	}
	selectedRecord := eventRecord(output, "job_selected")
	retryRecord := eventRecord(output, "retry_scheduled")
	if len(selectedRecord) != 12 || selectedRecord["level"] != "DEBUG" || selectedRecord["msg"] != "Silence recheck job selected" || selectedRecord["job_id"] != float64(1) || selectedRecord["status"] != "queued" || selectedRecord["queue_depth_available"] != true || selectedRecord["queue_depth"] != float64(1) || selectedRecord["attempt_count"] != float64(1) || selectedRecord["retry_count"] != float64(1) || selectedRecord["batch_size"] != float64(1000) {
		t.Fatalf("exact job_selected record=%v", selectedRecord)
	}
	if len(retryRecord) != 13 || retryRecord["level"] != "WARN" || retryRecord["msg"] != "Recheck retry was durably scheduled" || retryRecord["job_id"] != float64(1) || retryRecord["status"] != "retry_wait" || retryRecord["attempt_count"] != float64(1) || retryRecord["retry_count"] != float64(1) || retryRecord["consecutive_retry_count"] != float64(1) || retryRecord["failure_class"] != "retryable" || retryRecord["failure_stage"] != "persist_candidate" || retryRecord["next_attempt_at"] != now.Add(time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("exact retry_scheduled record=%v", retryRecord)
	}
	logs.Reset()
	current = now.Add(500 * time.Millisecond)
	clock.now = current
	deferred := scheduler.process(clock.NewTimer(time.Second), nil)
	if deferred.timerC == nil || !strings.Contains(logs.String(), `"event":"retry_deferred"`) || strings.Contains(logs.String(), `"event":"job_selected"`) {
		t.Fatalf("not-due retry disposition/events=%s", logs.String())
	}
	firstDeferred := eventRecord(logs.String(), "retry_deferred")
	if len(firstDeferred) != 10 || firstDeferred["level"] != "DEBUG" || firstDeferred["job_id"] != float64(1) || firstDeferred["status"] != "retry_wait" || firstDeferred["next_attempt_at"] != now.Add(time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("exact retry_deferred record=%v", firstDeferred)
	}
	if repeated := scheduler.process(clock.NewTimer(time.Second), nil); repeated.timerC == nil || strings.Count(logs.String(), `"event":"retry_deferred"`) != 2 {
		t.Fatalf("repeated retry deferral timer=%v logs=%s", repeated.timerC, logs.String())
	}
	var deferredRecords []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["event"] == "retry_deferred" {
			delete(record, "time")
			deferredRecords = append(deferredRecords, record)
		}
	}
	if len(deferredRecords) != 2 || !reflect.DeepEqual(deferredRecords[0], deferredRecords[1]) {
		t.Fatalf("repeated deferral records differ=%v", deferredRecords)
	}
	logs.Reset()
	faultActive = false
	current = now.Add(time.Second)
	clock.now = current
	completed := scheduler.process(clock.NewTimer(time.Second), nil)
	if completed.timerC != nil || !strings.Contains(logs.String(), `"event":"job_selected"`) || !strings.Contains(logs.String(), `"status":"retry_wait"`) || !strings.Contains(logs.String(), `"event":"batch_completed"`) || !strings.Contains(logs.String(), `"event":"job_completed"`) {
		t.Fatalf("due retry completion events=%s timer=%v", logs.String(), completed.timerC)
	}
}

func TestProblem4RetryScheduledEventPanicDoesNotChangeTimerOrDurableResult(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(problem4SchedulerPanicHandler{base: slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}), event: "retry_scheduled"}))
	defer slog.SetDefault(previousLogger)
	store, db, now := seedProblem3Review3SchedulerStore(t)
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		Now: func() time.Time { return now },
		RecheckFault: func(stage string) error {
			if stage == "persist_candidate" {
				return problem4SQLiteBusyError{}
			}
			return nil
		},
	})
	t.Cleanup(func() { store.SetRecheckIntegrationTestHooks(nil) })
	scheduler := newRecheckScheduler(store, &reviewSchedulerClock{now: now})
	processed := scheduler.process((&reviewSchedulerClock{now: now}).NewTimer(time.Second), nil)
	if processed.timerC == nil {
		t.Fatal("retry event panic removed retry timer")
	}
	var status string
	var attempts int
	if err := db.QueryRow(`SELECT status,attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "retry_wait" || attempts != 1 || !strings.Contains(logs.String(), `"event":"observability_collection_failed"`) || strings.Contains(logs.String(), "panic") {
		t.Fatalf("retry event panic changed durable result status=%s attempts=%d logs=%s", status, attempts, logs.String())
	}
}

func TestProblem4SchedulerOwnsSelectedShutdownCancellationEvent(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, _, now := seedProblem3Review3SchedulerStore(t)
	t.Cleanup(func() { _ = store.Close() })
	scheduler := newRecheckScheduler(store, &reviewSchedulerClock{now: now})
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		BeforeCommitLedgerInsert: func(_ context.Context, operation string) {
			if operation == "recheck_batch" {
				scheduler.cancel()
			}
		},
	})
	t.Cleanup(func() { store.SetRecheckIntegrationTestHooks(nil) })
	result := scheduler.process((&reviewSchedulerClock{now: now}).NewTimer(time.Second), nil)
	if !result.terminal {
		t.Fatal("selected shutdown cancellation did not terminate the scheduler owner")
	}
	output := logs.String()
	if !strings.Contains(output, `"event":"job_infrastructure_blocked"`) || !strings.Contains(output, `"reason_code":"worker_cancelled"`) || !strings.Contains(output, `"level":"DEBUG"`) {
		t.Fatalf("missing expected-shutdown infrastructure event: %s", output)
	}
	if strings.Contains(output, `"event":"job_commit_outcome_unknown"`) || strings.Contains(output, `"event":"job_failed"`) {
		t.Fatalf("shutdown cancellation emitted an incorrect event: %s", output)
	}
}

func TestProblem4InfrastructureReasonStageMatrix(t *testing.T) {
	cases := []struct {
		stage, reason string
		level         slog.Level
	}{
		{stage: "batch", reason: "writer_unavailable", level: slog.LevelError},
		{stage: "batch", reason: "writer_invariant", level: slog.LevelError},
		{stage: "batch", reason: "rollback_failed", level: slog.LevelError},
		{stage: "batch", reason: "store_closing", level: slog.LevelDebug},
		{stage: "batch", reason: "store_closed", level: slog.LevelDebug},
		{stage: "batch", reason: "worker_cancelled", level: slog.LevelDebug},
		{stage: "batch", reason: "worker_deadline", level: slog.LevelDebug},
		{stage: "batch", reason: "infrastructure_unclassified", level: slog.LevelError},
		{stage: "batch", reason: "queue_read_terminal", level: slog.LevelError},
		{stage: "failure_persistence", reason: "failure_persistence_sql_error", level: slog.LevelError},
		{stage: "failure_persistence", reason: "failure_persistence_rollback_failed", level: slog.LevelError},
		{stage: "failure_persistence", reason: "failure_persistence_writer_invariant", level: slog.LevelError},
		{stage: "failure_persistence", reason: "failure_persistence_writer_unavailable", level: slog.LevelError},
		{stage: "failure_persistence", reason: "failure_persistence_store_closing", level: slog.LevelDebug},
		{stage: "failure_persistence", reason: "failure_persistence_store_closed", level: slog.LevelDebug},
		{stage: "failure_persistence", reason: "failure_persistence_cancelled", level: slog.LevelDebug},
		{stage: "failure_persistence", reason: "failure_persistence_deadline", level: slog.LevelDebug},
	}
	for _, tc := range cases {
		t.Run(tc.stage+"/"+tc.reason, func(t *testing.T) {
			if !alertlifecycle.ValidRecheckInfrastructurePair(tc.stage, tc.reason) {
				t.Fatalf("unregistered infrastructure pair")
			}
			if got := alertlifecycle.RecheckInfrastructureLogLevel(tc.stage, tc.reason); got != tc.level {
				t.Fatalf("level=%s want=%s", got, tc.level)
			}
		})
	}
}

func TestProblem4SchedulerObservationEdgesResetOnStateChanges(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	clock := &reviewSchedulerClock{now: time.Now().UTC()}
	scheduler := newRecheckScheduler(nil, clock)
	call := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		switch call {
		case 1:
			return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchQueueBlocked, JobID: 1, Status: "failed"}, nil
		case 2:
			return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchQueueBlocked, JobID: 1, Status: "queued"}, nil
		case 3:
			return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchQueueBlocked, JobID: 1, Status: "failed"}, nil
		case 4:
			return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchQueueBlocked, JobID: 2, Status: "failed"}, nil
		case 5, 7:
			return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchAcquisitionFailed}, errors.Join(alertlifecycle.ErrWriterAcquisition, problem4SQLiteBusyError{})
		case 6:
			return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchQueueBlocked, JobID: 2, Status: "queued"}, nil
		default:
			return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchQueueEmpty}, nil
		}
	}
	for range 7 {
		scheduler.process(clock.NewTimer(time.Second), nil)
	}
	output := logs.String()
	if got := strings.Count(output, `"event":"failed_job_blocks_queue"`); got != 5 {
		t.Fatalf("expected a fresh blocker edge for each changed head/status, got=%d output=%s", got, output)
	}
	if got := strings.Count(output, `"event":"worker_acquisition_failed"`); got != 2 {
		t.Fatalf("expected acquisition edges before and after an intervening queue state, got=%d output=%s", got, output)
	}
	if strings.Count(output, `"level":"WARN"`) < 7 {
		t.Fatalf("changed observation edges were not warnings: %s", output)
	}
}

func TestProblem4WarningUsesTypedIssueArrayWithoutLegacyFields(t *testing.T) {
	setupLifecycleStoreForResolveTest(t)
	previousEnabled := config.Config.AlertLifecycle.Enabled
	config.Config.AlertLifecycle.Enabled = true
	t.Cleanup(func() { config.Config.AlertLifecycle.Enabled = previousEnabled })

	req := httptest.NewRequest(http.MethodGet, "/alerts-history/warning", http.NoBody)
	recorder := httptest.NewRecorder()
	alertLifecycleWarning(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected warning endpoint success, got=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode warning response: %v", err)
	}
	if _, ok := response["issues"]; !ok {
		t.Fatalf("warning response must expose issues: %s", recorder.Body.String())
	}
	for key := range response {
		switch key {
		case "status", "storageEnabled", "dbExpectedState", "issues":
		default:
			t.Fatalf("unexpected warning response field %q: %s", key, recorder.Body.String())
		}
	}
	if _, ok := response["reason"]; ok {
		t.Fatalf("legacy scalar reason must be absent: %s", recorder.Body.String())
	}
}

func TestProblem4StatsUsesNestedRecheckShapeWithoutLegacyFields(t *testing.T) {
	setupLifecycleStoreForResolveTest(t)
	previousEnabled := config.Config.AlertLifecycle.Enabled
	config.Config.AlertLifecycle.Enabled = true
	t.Cleanup(func() { config.Config.AlertLifecycle.Enabled = previousEnabled })

	req := httptest.NewRequest(http.MethodGet, "/alerts-history/stats", http.NoBody)
	recorder := httptest.NewRecorder()
	alertLifecycleStats(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected stats endpoint success, got=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode stats response: %v", err)
	}
	recheck, ok := response["recheck"].(map[string]any)
	if !ok {
		t.Fatalf("stats response must expose nested recheck object: %s", recorder.Body.String())
	}
	if _, ok := recheck["queueDepth"]; !ok {
		t.Fatalf("nested recheck object must expose queueDepth: %s", recorder.Body.String())
	}
	for _, legacy := range []string{"recheckQueueDepth", "recheckCurrentStatus", "recheckOldestCreatedAt"} {
		if _, ok := response[legacy]; ok {
			t.Fatalf("legacy stats field %q must be absent: %s", legacy, recorder.Body.String())
		}
	}
}

func TestProblem4HTTPStatsAndWarningStatusMatrix(t *testing.T) {
	statuses := []string{"queued", "running", "retry_wait", "failed", "completed", "abandoned"}
	for _, status := range statuses {
		t.Run(status, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.sqlite")
			store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			lifecycleStore = store
			t.Cleanup(func() { _ = closeAlertLifecycleStore() })
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			previousEnabled := config.Config.AlertLifecycle.Enabled
			config.Config.AlertLifecycle.Enabled = true
			t.Cleanup(func() { config.Config.AlertLifecycle.Enabled = previousEnabled })
			now := time.Now().UTC().Format(time.RFC3339Nano)
			next := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
			query := `INSERT INTO silence_recheck_jobs(status,created_at,started_at,finished_at,next_attempt_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,total_candidates,processed_candidates) VALUES(?,?,?,?,?,'{"formatVersion":1,"sources":[],"clusters":[]}',?,?,?,0,0)`
			args := []any{status, now, nil, nil, nil, "p4-http-" + status, "sha256:p4-http-" + status, "source_context_changed"}
			switch status {
			case "running":
				args[2], args[3] = now, nil
			case "retry_wait":
				query = `INSERT INTO silence_recheck_jobs(status,created_at,started_at,finished_at,next_attempt_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,total_candidates,processed_candidates,first_error_message,first_error_class,first_error_stage,first_error_at,last_error_message,last_error_class,last_error_stage,last_error_at) VALUES(?,?,?,?,?,'{"formatVersion":1,"sources":[],"clusters":[]}',?,?,?,0,0,?,?,?,?,?,?,?,?)`
				args = []any{status, now, now, nil, next, "p4-http-" + status, "sha256:p4-http-" + status, "source_context_changed", "first", "retryable_storage", "persist_candidate", now, "last", "retryable_storage", "persist_candidate", now}
			case "failed", "abandoned":
				query = `INSERT INTO silence_recheck_jobs(status,created_at,started_at,finished_at,next_attempt_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,total_candidates,processed_candidates,first_error_message,first_error_class,first_error_stage,first_error_at,last_error_message,last_error_class,last_error_stage,last_error_at,abandoned_at,abandoned_by,abandon_reason) VALUES(?,?,?,?,?,'{"formatVersion":1,"sources":[],"clusters":[]}',?,?,?,0,0,?,?,?,?,?,?,?,?,?,?,?)`
				abandonedAt := any(nil)
				abandonedBy := any(nil)
				reason := any(nil)
				if status == "abandoned" {
					abandonedAt, abandonedBy, reason = now, "http-test", "released"
				}
				args = []any{status, now, now, now, nil, "p4-http-" + status, "sha256:p4-http-" + status, "source_context_changed", "first", "terminal", "persist_candidate", now, "last", "terminal", "persist_candidate", now, abandonedAt, abandonedBy, reason}
			case "completed":
				args[2], args[3] = now, now
			}
			if _, err = db.Exec(query, args...); err != nil {
				t.Fatal(err)
			}

			statsRecorder := httptest.NewRecorder()
			alertLifecycleStats(statsRecorder, httptest.NewRequest(http.MethodGet, "/alerts-history/stats", http.NoBody))
			if statsRecorder.Code != http.StatusOK {
				t.Fatalf("stats status=%d body=%s", statsRecorder.Code, statsRecorder.Body.String())
			}
			var statsResponse map[string]any
			if err := json.Unmarshal(statsRecorder.Body.Bytes(), &statsResponse); err != nil {
				t.Fatal(err)
			}
			expectedTopKeys := []string{"status", "storageEnabled", "sqlitePath", "retentionTTLHours", "maxAlerts", "currentRows", "stateCounts", "suppressedBreakdown", "historyRows", "groupingFingerprint", "regroupTargetFingerprint", "regroupStatus", "regroupError", "regroupProcessed", "regroupTotal", "dbExpectedState", "dbExpectedStateReason", "lastRegroupStartedAt", "lastRegroupFinishedAt", "fileSizeBytes", "pageCount", "pageSizeBytes", "freelistCount", "lastHousekeepingAt", "lastIncrementalVacuumAt", "silenceHistoryTTLHours", "silenceInconsistentClusters", "recheck"}
			if len(statsResponse) != len(expectedTopKeys) {
				t.Fatalf("stats top-level fields=%v", statsResponse)
			}
			for _, key := range expectedTopKeys {
				if _, exists := statsResponse[key]; !exists {
					t.Fatalf("stats missing exact field %q: %s", key, statsRecorder.Body.String())
				}
			}
			recheck, ok := statsResponse["recheck"].(map[string]any)
			if !ok || len(recheck) != 2 {
				t.Fatalf("missing nested recheck response: %s", statsRecorder.Body.String())
			}
			if statsResponse["status"] != "ok" || recheck["queueDepth"] == nil {
				t.Fatalf("stats envelope values=%s", statsRecorder.Body.String())
			}
			head, hasHead := recheck["headJob"].(map[string]any)
			if status == "completed" || status == "abandoned" {
				if hasHead || recheck["headJob"] != nil || recheck["queueDepth"] != float64(0) {
					t.Fatalf("terminal status unexpectedly exposed as FIFO head: %s", statsRecorder.Body.String())
				}
			} else {
				if !hasHead || head["status"] != status || len(head) != 16 {
					t.Fatalf("head status=%v response=%s", head["status"], statsRecorder.Body.String())
				}
				for _, key := range []string{"jobId", "triggerType", "status", "createdAt", "startedAt", "finishedAt", "totalCandidates", "processedCandidates", "outcomeCounts", "attemptCount", "retryCount", "consecutiveRetryCount", "nextAttemptAt", "firstError", "lastError", "recoveryCount"} {
					if _, exists := head[key]; !exists {
						t.Fatalf("head missing key %q: %s", key, statsRecorder.Body.String())
					}
				}
				outcomes, ok := head["outcomeCounts"].(map[string]any)
				if !ok || len(outcomes) != 5 {
					t.Fatalf("outcomeCounts is not an object: %s", statsRecorder.Body.String())
				}
				for _, key := range []string{"promoted", "stale", "ineligible", "missingLabels", "incompleteSources"} {
					if value, exists := outcomes[key]; !exists {
						t.Fatalf("outcomeCounts missing %q: %s", key, statsRecorder.Body.String())
					} else if _, ok := value.(float64); !ok {
						t.Fatalf("outcomeCounts %q type=%T", key, value)
					}
				}
				for _, key := range []string{"jobId", "totalCandidates", "processedCandidates", "attemptCount", "retryCount", "consecutiveRetryCount", "recoveryCount"} {
					if _, ok := head[key].(float64); !ok {
						t.Fatalf("head numeric field %q type=%T", key, head[key])
					}
				}
				for _, key := range []string{"triggerType", "status", "createdAt"} {
					if _, ok := head[key].(string); !ok {
						t.Fatalf("head string field %q type=%T", key, head[key])
					}
				}
				if status == "retry_wait" || status == "failed" {
					for _, field := range []string{"firstError", "lastError"} {
						errorTuple, ok := head[field].(map[string]any)
						if !ok || len(errorTuple) != 4 || errorTuple["class"] == nil || errorTuple["stage"] == nil || errorTuple["at"] == nil || errorTuple["message"] != "recheck attempt failed" {
							t.Fatalf("invalid %s tuple: %s", field, statsRecorder.Body.String())
						}
					}
				}
				expectedStarted, expectedFinished, expectedNext := any(nil), any(nil), any(nil)
				if status != "queued" {
					expectedStarted = now
				}
				if status == "failed" {
					expectedFinished = now
				}
				if status == "retry_wait" {
					expectedNext = next
				}
				expectedFirst, expectedLast := any(nil), any(nil)
				if status == "retry_wait" || status == "failed" {
					class := "terminal"
					if status == "retry_wait" {
						class = "retryable_storage"
					}
					expectedFirst = map[string]any{"class": class, "stage": "persist_candidate", "at": now, "message": "recheck attempt failed"}
					expectedLast = map[string]any{"class": class, "stage": "persist_candidate", "at": now, "message": "recheck attempt failed"}
				}
				expectedHead := map[string]any{
					"jobId": float64(1), "triggerType": "source_context_changed", "status": status,
					"createdAt": now, "startedAt": expectedStarted, "finishedAt": expectedFinished,
					"totalCandidates": float64(0), "processedCandidates": float64(0),
					"outcomeCounts": map[string]any{"promoted": float64(0), "stale": float64(0), "ineligible": float64(0), "missingLabels": float64(0), "incompleteSources": float64(0)},
					"attemptCount":  float64(0), "retryCount": float64(0), "consecutiveRetryCount": float64(0),
					"nextAttemptAt": expectedNext, "firstError": expectedFirst, "lastError": expectedLast, "recoveryCount": float64(0),
				}
				if !reflect.DeepEqual(head, expectedHead) || recheck["queueDepth"] != float64(1) {
					t.Fatalf("stats recursive projection=%#v want=%#v recheck=%#v", head, expectedHead, recheck)
				}
			}

			warningRecorder := httptest.NewRecorder()
			alertLifecycleWarning(warningRecorder, httptest.NewRequest(http.MethodGet, "/alerts-history/warning", http.NoBody))
			if warningRecorder.Code != http.StatusOK {
				t.Fatalf("warning status=%d body=%s", warningRecorder.Code, warningRecorder.Body.String())
			}
			var warningResponse map[string]any
			if err := json.Unmarshal(warningRecorder.Body.Bytes(), &warningResponse); err != nil {
				t.Fatal(err)
			}
			if _, ok := warningResponse["issues"].([]any); !ok {
				t.Fatalf("warning issues is not an array: %s", warningRecorder.Body.String())
			}
			issues := warningResponse["issues"].([]any)
			if status == "retry_wait" || status == "failed" {
				if len(issues) == 0 {
					t.Fatalf("warning omitted issue for %s head: %s", status, warningRecorder.Body.String())
				}
				wantKind := map[bool]string{true: "recheck_blocked", false: "recheck_retrying"}[status == "failed"]
				found := false
				for _, rawIssue := range issues {
					issue, ok := rawIssue.(map[string]any)
					if ok && issue["kind"] == wantKind {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("warning omitted %s issue: %s", wantKind, warningRecorder.Body.String())
				}
			}
		})
	}
}

func TestProblem4DisabledStorageHTTPShape(t *testing.T) {
	previousEnabled := config.Config.AlertLifecycle.Enabled
	config.Config.AlertLifecycle.Enabled = false
	t.Cleanup(func() { config.Config.AlertLifecycle.Enabled = previousEnabled })
	lifecycleStore = nil
	warningRecorder := httptest.NewRecorder()
	alertLifecycleWarning(warningRecorder, httptest.NewRequest(http.MethodGet, "/alerts-history/warning", http.NoBody))
	statsRecorder := httptest.NewRecorder()
	alertLifecycleStats(statsRecorder, httptest.NewRequest(http.MethodGet, "/alerts-history/stats", http.NoBody))
	if warningRecorder.Code != http.StatusOK || statsRecorder.Code != http.StatusOK {
		t.Fatalf("disabled storage statuses warning=%d stats=%d", warningRecorder.Code, statsRecorder.Code)
	}
	var warning map[string]any
	if err := json.Unmarshal(warningRecorder.Body.Bytes(), &warning); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"status", "storageEnabled", "dbExpectedState", "issues"} {
		if _, ok := warning[key]; !ok {
			t.Fatalf("disabled warning missing %q: %s", key, warningRecorder.Body.String())
		}
	}
	if warning["storageEnabled"] != false {
		t.Fatalf("disabled warning claimed storage enabled: %s", warningRecorder.Body.String())
	}
	var stats map[string]any
	if err := json.Unmarshal(statsRecorder.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	recheck, ok := stats["recheck"].(map[string]any)
	if !ok || recheck["queueDepth"] != float64(0) || recheck["headJob"] != nil {
		t.Fatalf("disabled stats recheck shape: %s", statsRecorder.Body.String())
	}
}

func TestProblem4WarningIssueJSONMetadataMatrix(t *testing.T) {
	base := alertlifecycle.Stats{GroupingFingerprint: "stored", RegroupStatus: "idle"}
	issues := buildAlertLifecycleWarningIssues(withRecheckHead(withInconsistentClusters(withRegroupStats(base, "running")), "failed"), "expected")
	encoded, err := json.Marshal(alertLifecycleWarningResponse{Status: "ok", StorageEnabled: true, DBExpectedState: false, Issues: issues})
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(encoded, &response); err != nil {
		t.Fatal(err)
	}
	rawIssues, ok := response["issues"].([]any)
	if !ok || len(rawIssues) != 4 {
		t.Fatalf("serialized issue matrix=%v", response["issues"])
	}
	wantKinds := []string{"recheck_blocked", "regroup_in_progress", "regroup_out_of_date", "silence_ha_inconsistent"}
	wantMetadata := map[string]map[string]string{
		"recheck_blocked":         {"jobId": "number", "status": "string", "firstFailureStage": "string", "firstFailureAt": "string"},
		"regroup_in_progress":     {"processed": "number", "total": "number"},
		"regroup_out_of_date":     {"storedFingerprint": "string", "expectedFingerprint": "string"},
		"silence_ha_inconsistent": {"targetNames": "array"},
	}
	for index, raw := range rawIssues {
		issue, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("issue is not an object: %v", raw)
		}
		if issue["kind"] != wantKinds[index] {
			t.Fatalf("issue order=%v want=%s", issue["kind"], wantKinds[index])
		}
		for _, key := range []string{"kind", "severity", "summary", "metadata"} {
			if _, exists := issue[key]; !exists {
				t.Fatalf("issue missing %q: %v", key, issue)
			}
		}
		metadata, ok := issue["metadata"].(map[string]any)
		if !ok {
			t.Fatalf("issue metadata is not an object: %v", issue)
		}
		wantFields := wantMetadata[wantKinds[index]]
		if len(metadata) != len(wantFields) {
			t.Fatalf("metadata fields=%v want=%v", metadata, wantFields)
		}
		for field, kind := range wantFields {
			value, exists := metadata[field]
			if !exists {
				t.Fatalf("metadata missing %q: %v", field, metadata)
			}
			switch kind {
			case "number":
				if _, ok := value.(float64); !ok {
					t.Fatalf("metadata %q has type %T, want number", field, value)
				}
			case "string":
				if _, ok := value.(string); !ok {
					t.Fatalf("metadata %q has type %T, want string", field, value)
				}
			case "array":
				if _, ok := value.([]any); !ok {
					t.Fatalf("metadata %q has type %T, want array", field, value)
				}
			}
		}
	}
}

func TestProblem4WarningHTTPProjectsEveryIssueKindExactly(t *testing.T) {
	expectedFingerprint := alertlifecycle.BuildGroupingFingerprint(alertlifecycle.GroupingConfig{
		LabelsKeep:       append([]string{}, config.Config.Labels.Keep...),
		LabelsKeepRegex:  append([]string{}, config.Config.Labels.KeepRegex...),
		LabelsStrip:      append([]string{}, config.Config.Labels.Strip...),
		LabelsStripRegex: append([]string{}, config.Config.Labels.StripRegex...),
	})
	now := "2026-08-19T12:00:00Z"
	next := "2026-08-19T12:01:00Z"
	tests := []struct {
		name     string
		prepare  func(*testing.T, *sql.DB)
		expected map[string]any
	}{
		{name: "regroup_out_of_date", prepare: func(t *testing.T, db *sql.DB) {
			_, err := db.Exec(`INSERT INTO metadata(key,value) VALUES('grouping_fingerprint','stored-old') ON CONFLICT(key) DO UPDATE SET value=excluded.value`)
			if err != nil {
				t.Fatal(err)
			}
		}, expected: map[string]any{"kind": "regroup_out_of_date", "severity": "warning", "summary": "Alert lifecycle grouping is out of date", "metadata": map[string]any{"storedFingerprint": "stored-old", "expectedFingerprint": expectedFingerprint}}},
		{name: "regroup_in_progress", prepare: func(t *testing.T, db *sql.DB) {
			_, err := db.Exec(`INSERT INTO metadata(key,value) VALUES('grouping_fingerprint',?),('regroup_status','running'),('regroup_processed','2'),('regroup_total','4') ON CONFLICT(key) DO UPDATE SET value=excluded.value`, expectedFingerprint)
			if err != nil {
				t.Fatal(err)
			}
		}, expected: map[string]any{"kind": "regroup_in_progress", "severity": "warning", "summary": "Alert lifecycle regroup is in progress", "metadata": map[string]any{"processed": float64(2), "total": float64(4)}}},
		{name: "regroup_failed", prepare: func(t *testing.T, db *sql.DB) {
			_, err := db.Exec(`INSERT INTO metadata(key,value) VALUES('grouping_fingerprint',?),('regroup_status','error'),('regroup_error','private SQL'),('regroup_processed','3'),('regroup_total','4') ON CONFLICT(key) DO UPDATE SET value=excluded.value`, expectedFingerprint)
			if err != nil {
				t.Fatal(err)
			}
		}, expected: map[string]any{"kind": "regroup_failed", "severity": "error", "summary": "Alert lifecycle regroup failed", "metadata": map[string]any{"error": "regroup operation failed", "processed": float64(3), "total": float64(4)}}},
		{name: "silence_ha_inconsistent", prepare: func(t *testing.T, db *sql.DB) {
			if _, err := db.Exec(`INSERT INTO metadata(key,value) VALUES('grouping_fingerprint',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, expectedFingerprint); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO alertmanager_cluster_consistency(cluster_id,status,member_count,last_evaluated_at,last_inconsistent_at) VALUES('cluster-a','inconsistent',2,?,?)`, now, now); err != nil {
				t.Fatal(err)
			}
		}, expected: map[string]any{"kind": "silence_ha_inconsistent", "severity": "warning", "summary": "Alertmanager silence state is inconsistent across HA members", "metadata": map[string]any{"targetNames": []any{"cluster-a"}}}},
		{name: "recheck_retrying", prepare: func(t *testing.T, db *sql.DB) {
			if _, err := db.Exec(`INSERT INTO metadata(key,value) VALUES('grouping_fingerprint',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, expectedFingerprint); err != nil {
				t.Fatal(err)
			}
			_, err := db.Exec(`INSERT INTO silence_recheck_jobs(status,created_at,started_at,next_attempt_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,retry_count,first_error_message,first_error_class,first_error_stage,first_error_at,last_error_message,last_error_class,last_error_stage,last_error_at) VALUES('retry_wait',?,?,?,'{"formatVersion":1,"sources":[],"clusters":[]}','http-retry','sha256:http-retry','source_context_changed',2,'first','retryable_storage','persist_candidate',?,'last','retryable_storage','persist_candidate',?)`, now, now, next, now, now)
			if err != nil {
				t.Fatal(err)
			}
		}, expected: map[string]any{"kind": "recheck_retrying", "severity": "warning", "summary": "Silence recheck is waiting to retry", "metadata": map[string]any{"jobId": float64(1), "status": "retry_wait", "retryCount": float64(2), "nextAttemptAt": next, "lastFailureStage": "persist_candidate"}}},
		{name: "recheck_blocked", prepare: func(t *testing.T, db *sql.DB) {
			if _, err := db.Exec(`INSERT INTO metadata(key,value) VALUES('grouping_fingerprint',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, expectedFingerprint); err != nil {
				t.Fatal(err)
			}
			_, err := db.Exec(`INSERT INTO silence_recheck_jobs(status,created_at,started_at,finished_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,first_error_message,first_error_class,first_error_stage,first_error_at,last_error_message,last_error_class,last_error_stage,last_error_at) VALUES('failed',?,?,?,'{"formatVersion":1,"sources":[],"clusters":[]}','http-failed','sha256:http-failed','source_context_changed','first','terminal','persist_candidate',?,'last','terminal','persist_candidate',?)`, now, now, now, now, now)
			if err != nil {
				t.Fatal(err)
			}
		}, expected: map[string]any{"kind": "recheck_blocked", "severity": "error", "summary": "Silence recheck queue is blocked", "metadata": map[string]any{"jobId": float64(1), "status": "failed", "firstFailureStage": "persist_candidate", "firstFailureAt": now}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.sqlite")
			store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			previousStore, previousEnabled := lifecycleStore, config.Config.AlertLifecycle.Enabled
			lifecycleStore, config.Config.AlertLifecycle.Enabled = store, true
			t.Cleanup(func() {
				lifecycleStore = previousStore
				config.Config.AlertLifecycle.Enabled = previousEnabled
				_ = store.Close()
			})
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			test.prepare(t, db)
			recorder := httptest.NewRecorder()
			alertLifecycleWarning(recorder, httptest.NewRequest(http.MethodGet, "/alerts-history/warning", http.NoBody))
			if recorder.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			var response map[string]any
			if err = json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			expected := map[string]any{"status": "ok", "storageEnabled": true, "dbExpectedState": false, "issues": []any{test.expected}}
			if !reflect.DeepEqual(response, expected) {
				t.Fatalf("HTTP issue response=%#v want=%#v", response, expected)
			}
		})
	}
}

func TestProblem4HTTPRejectsMalformedDurableErrorTuple(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	lifecycleStore = store
	t.Cleanup(func() { _ = closeAlertLifecycleStore() })
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	previousEnabled := config.Config.AlertLifecycle.Enabled
	config.Config.AlertLifecycle.Enabled = true
	t.Cleanup(func() { config.Config.AlertLifecycle.Enabled = previousEnabled })
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err = db.Exec(`INSERT INTO silence_recheck_jobs(status,created_at,started_at,finished_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,first_error_message) VALUES('failed',?,?,?,'{"formatVersion":1,"sources":[],"clusters":[]}',?,?,?,'partial')`, now, now, now, "p4-malformed", "sha256:p4-malformed", "source_context_changed"); err != nil {
		t.Fatal(err)
	}
	statsRecorder := httptest.NewRecorder()
	alertLifecycleStats(statsRecorder, httptest.NewRequest(http.MethodGet, "/alerts-history/stats", http.NoBody))
	warningRecorder := httptest.NewRecorder()
	alertLifecycleWarning(warningRecorder, httptest.NewRequest(http.MethodGet, "/alerts-history/warning", http.NoBody))
	if statsRecorder.Code != http.StatusInternalServerError || warningRecorder.Code != http.StatusInternalServerError {
		t.Fatalf("malformed tuple was not rejected closed: stats=%d warning=%d", statsRecorder.Code, warningRecorder.Code)
	}
	if !strings.Contains(logs.String(), `"reason_code":"stats_query_failed"`) || !strings.Contains(logs.String(), `"reason_code":"warning_projection_failed"`) || strings.Contains(logs.String(), "partial") {
		t.Fatalf("stats/warning failure taxonomy or redaction=%s", logs.String())
	}
}

func TestProblem4HTTPRejectsEveryMalformedDurableErrorTupleField(t *testing.T) {
	for _, mutation := range []string{
		`first_error_message=NULL`,
		`first_error_message=''`,
		`first_error_message='   '`,
		`first_error_class=NULL`,
		`first_error_class='   '`,
		`first_error_stage=NULL`,
		`first_error_stage='   '`,
		`first_error_at=NULL`,
		`first_error_at='not-a-time'`,
		`last_error_message=NULL`,
		`last_error_message=''`,
		`last_error_message='   '`,
		`last_error_class=NULL`,
		`last_error_class='   '`,
		`last_error_stage=NULL`,
		`last_error_stage='   '`,
		`last_error_at=NULL`,
		`last_error_at='not-a-time'`,
	} {
		t.Run(strings.ReplaceAll(mutation, "'", ""), func(t *testing.T) {
			var logs bytes.Buffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			defer slog.SetDefault(previousLogger)
			path := filepath.Join(t.TempDir(), "history.sqlite")
			store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			previousStore, previousEnabled := lifecycleStore, config.Config.AlertLifecycle.Enabled
			lifecycleStore, config.Config.AlertLifecycle.Enabled = store, true
			t.Cleanup(func() {
				lifecycleStore = previousStore
				config.Config.AlertLifecycle.Enabled = previousEnabled
				_ = store.Close()
			})
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if _, err = db.Exec(`PRAGMA ignore_check_constraints=ON`); err != nil {
				t.Fatal(err)
			}
			now := "2026-08-19T12:00:00Z"
			if _, err = db.Exec(`INSERT INTO silence_recheck_jobs(status,created_at,started_at,finished_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,first_error_message,first_error_class,first_error_stage,first_error_at,last_error_message,last_error_class,last_error_stage,last_error_at) VALUES('failed',?,?,?,'{"formatVersion":1,"sources":[],"clusters":[]}','malformed-matrix','sha256:malformed-matrix','source_context_changed','private first','terminal','persist_candidate',?,'private last','terminal','persist_candidate',?)`, now, now, now, now, now); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(`UPDATE silence_recheck_jobs SET ` + mutation + ` WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
			before := captureProblem4RouterState(t, db)
			statsRecorder := httptest.NewRecorder()
			alertLifecycleStats(statsRecorder, httptest.NewRequest(http.MethodGet, "/alerts-history/stats", http.NoBody))
			warningRecorder := httptest.NewRecorder()
			alertLifecycleWarning(warningRecorder, httptest.NewRequest(http.MethodGet, "/alerts-history/warning", http.NoBody))
			if statsRecorder.Code != http.StatusInternalServerError || statsRecorder.Body.String() != `{"error":"failed to read alert lifecycle stats"}` || warningRecorder.Code != http.StatusInternalServerError || warningRecorder.Body.String() != `{"error":"failed to read alert lifecycle warning state"}` {
				t.Fatalf("malformed %s responses stats=%d/%s warning=%d/%s", mutation, statsRecorder.Code, statsRecorder.Body.String(), warningRecorder.Code, warningRecorder.Body.String())
			}
			if after := captureProblem4RouterState(t, db); after != before {
				t.Fatalf("read-only HTTP projection changed durable state for %s", mutation)
			}
			if strings.Contains(logs.String(), "private first") || strings.Contains(logs.String(), "private last") || !strings.Contains(logs.String(), `"error":"storage_error"`) {
				t.Fatalf("malformed %s leaked or misclassified logs=%s", mutation, logs.String())
			}
		})
	}
}

func TestProblem4HTTPAndSuccessProjectionsRedactInternalErrors(t *testing.T) {
	sentinel := "SQLITE_BUSY token=secret operator-reason"
	if got := recheckOperatorResponseMessage(http.StatusInternalServerError, errors.New(sentinel)); got != "recheck operator action failed" {
		t.Fatalf("operator error response=%q", got)
	}
	if got := recheckOperatorErrorCategory(http.StatusInternalServerError, "operator_action_failed", errors.New(sentinel)); got != "storage_error" {
		t.Fatalf("operator error category=%q", got)
	}
	if got := safeRegroupError(sentinel); got != "regroup operation failed" {
		t.Fatalf("regroup stats projection=%q", got)
	}
	errorResponse := alertLifecycleRecheckErrorResponseFor(&alertlifecycle.RecheckErrorSnapshot{Class: sentinel, Stage: "persist_candidate", At: time.Now().UTC(), Message: sentinel})
	if errorResponse.Message != "recheck attempt failed" || strings.Contains(errorResponse.Message, sentinel) {
		t.Fatalf("recheck error projection=%+v", errorResponse)
	}
	issues := buildAlertLifecycleWarningIssues(withRegroupStats(alertlifecycle.Stats{}, "error"), "expected")
	for _, issue := range issues {
		if issue.Kind == "regroup_failed" {
			metadata := issue.Metadata.(alertLifecycleRegroupFailedMetadata)
			if metadata.Error != "regroup operation failed" || strings.Contains(metadata.Error, sentinel) {
				t.Fatalf("regroup warning projection=%+v", metadata)
			}
		}
	}
}

func TestProblem4OperatorCategorySwitchContainsOnlyRegisteredReasons(t *testing.T) {
	registered := map[string]string{
		"cross_origin_request":   "request_origin",
		"storage_unavailable":    "storage_unavailable",
		"job_not_found":          "not_found",
		"job_state_conflict":     "state_conflict",
		"request_conflict":       "state_conflict",
		"operator_action_failed": "storage_error",
		"unsupported_media_type": "client_input",
		"invalid_request":        "client_input",
	}
	for reason, expected := range registered {
		if got := recheckOperatorErrorCategory(http.StatusBadRequest, reason, errors.New("ignored")); got != expected {
			t.Fatalf("reason %q category=%q want=%q", reason, got, expected)
		}
	}
	for _, stale := range []string{"authentication_required", "not_found"} {
		if got := recheckOperatorErrorCategory(http.StatusBadRequest, stale, errors.New("ignored")); got != "client_input" {
			t.Fatalf("stale reason %q remained registered as %q", stale, got)
		}
	}
}

func TestProblem4RealSchedulerCannotOvertakeOperatorAppliedEvent(t *testing.T) {
	for _, action := range []string{"retry", "abandon"} {
		t.Run(action, func(t *testing.T) {
			store, db, now := seedProblem3Review3SchedulerStore(t)
			store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{RecheckFault: func(stage string) error {
				if stage == "persist_candidate" {
					return errors.New("terminal failure before operator race")
				}
				return nil
			}})
			if result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100); !alertlifecycle.IsRecheckDurableFailure(err) || result.Status != "failed" {
				t.Fatalf("failed-head setup result=%+v err=%v", result, err)
			}
			store.SetRecheckIntegrationTestHooks(nil)

			var logs problem4LockedBuffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			defer slog.SetDefault(previousLogger)
			operatorAtCommit := make(chan struct{})
			workerAfterPrecheck := make(chan struct{})
			workerInsideBarrier := make(chan struct{})
			finalizerEntered := make(chan struct{})
			releaseFinalizer := make(chan struct{})
			var onceCommit, oncePrecheck, onceInside, onceFinalizer sync.Once
			store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
				BeforeCommitLedgerInsert: func(_ context.Context, operation string) {
					if operation != "recheck_operator_action" {
						return
					}
					onceCommit.Do(func() { close(operatorAtCommit) })
					<-workerAfterPrecheck
				},
				WorkerBarrier: func(stage string) {
					switch stage {
					case "after_precheck":
						oncePrecheck.Do(func() { close(workerAfterPrecheck) })
					case "inside_transaction":
						onceInside.Do(func() { close(workerInsideBarrier) })
					}
				},
				OperatorFinalizer: func() {
					onceFinalizer.Do(func() { close(finalizerEntered) })
					<-releaseFinalizer
				},
			})
			t.Cleanup(func() { store.SetRecheckIntegrationTestHooks(nil) })

			clock := &reviewSchedulerClock{now: now.Add(time.Minute)}
			scheduler := newRecheckScheduler(store, clock)
			wakeCalls := 0
			store.SetRecheckCommitWake(func() { wakeCalls++; scheduler.Signal() })
			store.SetRecheckReconciledWake(scheduler.SignalReconciled)
			observations, _ := observeProblem3Review3Scheduler(scheduler)
			operatorResult := make(chan error, 1)
			go func() {
				actor := "reviewer"
				if action == "retry" {
					_, err := store.RetryRecheckJobContext(context.Background(), 1, "review5-retry", "retry", &actor, now.Add(time.Minute))
					operatorResult <- err
					return
				}
				_, err := store.AbandonRecheckJobContext(context.Background(), 1, "review5-abandon", "abandon", &actor, now.Add(time.Minute))
				operatorResult <- err
			}()
			<-operatorAtCommit
			scheduler.Start()
			t.Cleanup(scheduler.Stop)
			scheduler.Signal()
			<-finalizerEntered
			<-workerInsideBarrier
			var attemptRows int
			if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attemptRows); err != nil {
				t.Fatal(err)
			}
			if attemptRows != 1 || strings.Contains(logs.String(), `"event":"operator_action_applied"`) || strings.Contains(logs.String(), `"event":"job_selected"`) {
				t.Fatalf("scheduler overtook finalization: attempts=%d logs=%s", attemptRows, logs.String())
			}
			blockedState := captureProblem4RouterState(t, db)
			if !strings.Contains(blockedState, "review5-"+action) || wakeCalls != 0 {
				t.Fatalf("operator-finalizer phase state=%s wakes=%d", blockedState, wakeCalls)
			}
			close(releaseFinalizer)
			if err := <-operatorResult; err != nil {
				t.Fatalf("operator result=%v", err)
			}
			observation := <-observations
			if observation.err != nil {
				t.Fatalf("scheduler result=%+v err=%v", observation.result, observation.err)
			}
			if action == "retry" && observation.result.Status != "completed" {
				t.Fatalf("retry scheduler result=%+v", observation.result)
			}
			if action == "abandon" && observation.result.Disposition != alertlifecycle.RecheckBatchQueueEmpty {
				t.Fatalf("abandon scheduler result=%+v", observation.result)
			}
			var contractEvents []map[string]any
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var record map[string]any
				if json.Unmarshal([]byte(line), &record) == nil && (record["event"] == "operator_action_applied" || record["event"] == "job_selected") {
					contractEvents = append(contractEvents, record)
				}
			}
			if len(contractEvents) == 0 || contractEvents[0]["event"] != "operator_action_applied" || len(contractEvents[0]) != 10 || contractEvents[0]["operator"] != "reviewer" || wakeCalls != 1 {
				t.Fatalf("exact operator/scheduler records=%v wakes=%d", contractEvents, wakeCalls)
			}
			if action == "retry" && (len(contractEvents) != 2 || contractEvents[1]["event"] != "job_selected" || len(contractEvents[1]) != 12) {
				t.Fatalf("retry exact sequence=%v", contractEvents)
			}
			if action == "abandon" && len(contractEvents) != 1 {
				t.Fatalf("abandon selected work after empty queue records=%v", contractEvents)
			}
		})
	}
}

func TestProblem4RegisteredOperatorRejectionTaxonomy(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, db, _ := seedProblem3Review4RouterJob(t)
	previousStore := lifecycleStore
	previousConfig := config.Config
	lifecycleStore = store
	config.Config.Listen.Prefix = "/problem4"
	config.Config.Authentication.Enabled = false
	wakeCalls := 0
	store.SetRecheckCommitWake(func() { wakeCalls++ })
	t.Cleanup(func() {
		lifecycleStore = previousStore
		config.Config = previousConfig
	})
	router := chi.NewRouter()
	setupRouter(router, nil)
	var rejectedActionsBefore int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions`).Scan(&rejectedActionsBefore); err != nil {
		t.Fatal(err)
	}
	request := func(path, body, contentType, marker, fetchSite string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("Content-Type", contentType)
		if marker != "" {
			req.Header.Set(recheckOperatorRequestHeader, marker)
		}
		if fetchSite != "" {
			req.Header.Set("Sec-Fetch-Site", fetchSite)
		}
		return req
	}
	assertEvent := func(t *testing.T, status int, level, reason, category, action string, jobID *int64) {
		t.Helper()
		if status <= 0 {
			t.Fatal("invalid expected status")
		}
		var record map[string]any
		lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
		for index := len(lines) - 1; index >= 0; index-- {
			if json.Unmarshal([]byte(lines[index]), &record) == nil && record["event"] == "operator_action_rejected" {
				break
			}
		}
		if record["component"] != "silence_recheck" || record["event"] != "operator_action_rejected" || record["level"] != level || record["reason_code"] != reason || record["error"] != category || record["action"] != action {
			t.Fatalf("operator rejection event=%v want level=%s reason=%s category=%s action=%s", record, level, reason, category, action)
		}
		if jobID == nil {
			if _, present := record["job_id"]; present {
				t.Fatalf("unknown job id was emitted: %v", record)
			}
		} else if got, ok := record["job_id"].(float64); !ok || int64(got) != *jobID {
			t.Fatalf("job id event=%v want=%d", record, *jobID)
		}
	}

	invalid := httptest.NewRecorder()
	router.ServeHTTP(invalid, request("/problem4/alerts-history/recheck/1/retry", `{"unknown":"request-fragment"}`, "application/json", "recheck-operator", "same-origin"))
	if invalid.Code != http.StatusBadRequest || strings.Contains(invalid.Body.String(), "unknown") {
		t.Fatalf("invalid request response=%d body=%s", invalid.Code, invalid.Body.String())
	}
	jobID := int64(1)
	assertEvent(t, invalid.Code, "DEBUG", "invalid_request", "client_input", "retry", &jobID)

	missingMarker := httptest.NewRecorder()
	router.ServeHTTP(missingMarker, request("/problem4/alerts-history/recheck/1/abandon", `{"requestId":"missing-marker","reason":"release"}`, "application/json", "", "same-origin"))
	if missingMarker.Code != http.StatusForbidden {
		t.Fatalf("missing marker status=%d body=%s", missingMarker.Code, missingMarker.Body.String())
	}

	unsupportedMedia := httptest.NewRecorder()
	router.ServeHTTP(unsupportedMedia, request("/problem4/alerts-history/recheck/1/abandon", `{"requestId":"wrong-media","reason":"release"}`, "text/plain", "recheck-operator", "same-origin"))
	if unsupportedMedia.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("unsupported media status=%d body=%s", unsupportedMedia.Code, unsupportedMedia.Body.String())
	}
	var rejectedActionsAfter int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions`).Scan(&rejectedActionsAfter); err != nil {
		t.Fatal(err)
	}
	if rejectedActionsAfter != rejectedActionsBefore || wakeCalls != 0 {
		t.Fatalf("rejected routes mutated lifecycle actions=%d/%d wakes=%d", rejectedActionsBefore, rejectedActionsAfter, wakeCalls)
	}

	crossSite := httptest.NewRecorder()
	router.ServeHTTP(crossSite, request("/problem4/alerts-history/recheck/0/retry", `{"requestId":"cross-site"}`, "application/json", "recheck-operator", "cross-site"))
	if crossSite.Code != http.StatusForbidden {
		t.Fatalf("cross-site status=%d body=%s", crossSite.Code, crossSite.Body.String())
	}
	assertEvent(t, crossSite.Code, "DEBUG", "cross_origin_request", "request_origin", "retry", nil)

	notFound := httptest.NewRecorder()
	router.ServeHTTP(notFound, request("/problem4/alerts-history/recheck/99/retry", `{"requestId":"not-found"}`, "application/json", "recheck-operator", "same-origin"))
	if notFound.Code != http.StatusNotFound {
		t.Fatalf("not-found status=%d body=%s", notFound.Code, notFound.Body.String())
	}
	notFoundID := int64(99)
	assertEvent(t, notFound.Code, "WARN", "job_not_found", "not_found", "retry", &notFoundID)

	if _, err := db.Exec(`UPDATE silence_recheck_jobs SET status='completed' WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"retry", "abandon"} {
		body := `{"requestId":"state-conflict-` + action + `"}`
		if action == "abandon" {
			body = `{"requestId":"state-conflict-abandon","reason":"operator decision"}`
		}
		stateConflict := httptest.NewRecorder()
		router.ServeHTTP(stateConflict, request("/problem4/alerts-history/recheck/1/"+action, body, "application/json", "recheck-operator", "same-origin"))
		if stateConflict.Code != http.StatusConflict || stateConflict.Body.String() != `{"error":"recheck operator action conflict"}` {
			t.Fatalf("%s state-conflict status=%d body=%s", action, stateConflict.Code, stateConflict.Body.String())
		}
		assertEvent(t, stateConflict.Code, "WARN", "job_state_conflict", "state_conflict", action, &jobID)
	}
}

func TestProblem4RegisteredRouterRejectsEveryCrossOriginBoundaryWithoutEffects(t *testing.T) {
	tests := []struct {
		name, contentType, marker, fetchSite, reason string
		status                                       int
	}{
		{name: "missing marker", contentType: "application/json", fetchSite: "same-origin", reason: "cross_origin_request", status: http.StatusForbidden},
		{name: "invalid marker", contentType: "application/json", marker: "not-recheck-operator", fetchSite: "same-origin", reason: "cross_origin_request", status: http.StatusForbidden},
		{name: "cross site", contentType: "application/json", marker: "recheck-operator", fetchSite: "cross-site", reason: "cross_origin_request", status: http.StatusForbidden},
		{name: "wrong media", contentType: "text/plain", marker: "recheck-operator", fetchSite: "same-origin", reason: "unsupported_media_type", status: http.StatusUnsupportedMediaType},
	}
	for _, action := range []string{"retry", "abandon"} {
		for _, test := range tests {
			t.Run(action+"/"+test.name, func(t *testing.T) {
				var logs bytes.Buffer
				previousLogger := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
				defer slog.SetDefault(previousLogger)
				store, db, _ := seedProblem3Review4RouterJob(t)
				previousStore, previousConfig := lifecycleStore, config.Config
				lifecycleStore = store
				config.Config.Listen.Prefix = "/problem4-security"
				config.Config.Authentication.Enabled = true
				config.Config.Authentication.Header.Name = "X-Review5-User"
				config.Config.Authentication.Header.ValueRegex = "(.+)"
				t.Cleanup(func() { lifecycleStore = previousStore; config.Config = previousConfig })
				wakeCalls := 0
				store.SetRecheckCommitWake(func() { wakeCalls++ })
				router := chi.NewRouter()
				setupRouter(router, nil)
				before := captureProblem4RouterState(t, db)
				body := `{"requestId":"security-matrix"}`
				if action == "abandon" {
					body = `{"requestId":"security-matrix","reason":"operator decision"}`
				}
				req := httptest.NewRequest(http.MethodPost, "/problem4-security/alerts-history/recheck/1/"+action, strings.NewReader(body))
				req.Header.Set("Content-Type", test.contentType)
				if test.marker != "" {
					req.Header.Set(recheckOperatorRequestHeader, test.marker)
				}
				req.Header.Set("Sec-Fetch-Site", test.fetchSite)
				req.Header.Set("X-Review5-User", "review5-operator")
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, req)
				if recorder.Code != test.status || captureProblem4RouterState(t, db) != before || wakeCalls != 0 {
					t.Fatalf("rejection status=%d state_changed=%v wakes=%d body=%s", recorder.Code, captureProblem4RouterState(t, db) != before, wakeCalls, recorder.Body.String())
				}
				var records []map[string]any
				for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
					var record map[string]any
					if json.Unmarshal([]byte(line), &record) == nil && record["event"] == "operator_action_rejected" {
						records = append(records, record)
					}
				}
				if len(records) != 1 || len(records[0]) != 9 || records[0]["component"] != "silence_recheck" || records[0]["event"] != "operator_action_rejected" || records[0]["msg"] != "Recheck operator action was rejected" || records[0]["action"] != action || records[0]["reason_code"] != test.reason || records[0]["level"] != "DEBUG" || records[0]["error"] == nil || records[0]["operator"] != "review5-operator" {
					t.Fatalf("rejection records=%v", records)
				}
				if strings.Contains(logs.String(), "security-matrix") || strings.Contains(logs.String(), "operator decision") {
					t.Fatalf("rejection leaked request body: %s", logs.String())
				}
			})
		}
	}
}

func TestProblem4RegisteredOperatorStorageFailureIsRedactedAndHasNoEffects(t *testing.T) {
	const sentinel = "private-operation-token-review5"
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previousLogger)
	store, db, _ := seedProblem3Review4RouterJob(t)
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{OperationToken: func() (string, error) {
		return "", errors.New(sentinel)
	}})
	t.Cleanup(func() { store.SetRecheckIntegrationTestHooks(nil) })
	previousStore, previousConfig := lifecycleStore, config.Config
	lifecycleStore = store
	config.Config.Listen.Prefix = "/problem4-storage-error"
	config.Config.Authentication.Enabled = true
	config.Config.Authentication.Header.Name = "X-Review5-User"
	config.Config.Authentication.Header.ValueRegex = "(.+)"
	t.Cleanup(func() { lifecycleStore = previousStore; config.Config = previousConfig })
	wakes := 0
	store.SetRecheckCommitWake(func() { wakes++ })
	router := chi.NewRouter()
	setupRouter(router, nil)
	before := captureProblem4RouterState(t, db)
	request := httptest.NewRequest(http.MethodPost, "/problem4-storage-error/alerts-history/recheck/1/retry", strings.NewReader(`{"requestId":"storage-error"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(recheckOperatorRequestHeader, "recheck-operator")
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	request.Header.Set("X-Review5-User", "review5-operator")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusInternalServerError || recorder.Body.String() != `{"error":"recheck operator action failed"}` || captureProblem4RouterState(t, db) != before || wakes != 0 {
		t.Fatalf("storage failure status=%d body=%s state_changed=%v wakes=%d", recorder.Code, recorder.Body.String(), captureProblem4RouterState(t, db) != before, wakes)
	}
	if strings.Contains(logs.String(), sentinel) || !strings.Contains(logs.String(), `"event":"operator_action_rejected"`) || !strings.Contains(logs.String(), `"reason_code":"operator_action_failed"`) || !strings.Contains(logs.String(), `"error":"storage_error"`) || !strings.Contains(logs.String(), `"operator":"review5-operator"`) {
		t.Fatalf("storage failure log taxonomy/redaction=%s", logs.String())
	}
}

func TestProblem4RegisteredRouterExactMatrix(t *testing.T) {
	type testCase struct {
		name, marker, site, media, body, prepare, reason string
		status                                           int
	}
	cases := []testCase{
		{name: "unauthorized", marker: "recheck-operator", site: "same-origin", media: "application/json", status: http.StatusUnauthorized},
		{name: "missing_marker", site: "same-origin", media: "application/json", status: http.StatusForbidden, reason: "cross_origin_request"},
		{name: "invalid_marker", marker: "invalid", site: "same-origin", media: "application/json", status: http.StatusForbidden, reason: "cross_origin_request"},
		{name: "cross_site", marker: "recheck-operator", site: "cross-site", media: "application/json", status: http.StatusForbidden, reason: "cross_origin_request"},
		{name: "unsupported_media", marker: "recheck-operator", site: "same-origin", media: "text/plain", status: http.StatusUnsupportedMediaType, reason: "unsupported_media_type"},
		{name: "invalid_body", marker: "recheck-operator", site: "same-origin", media: "application/json", body: `{"requestId":"review7-matrix","extra":true}`, status: http.StatusBadRequest, reason: "invalid_request"},
		{name: "not_found", marker: "recheck-operator", site: "same-origin", media: "application/json", prepare: "not_found", status: http.StatusNotFound, reason: "job_not_found"},
		{name: "state_conflict", marker: "recheck-operator", site: "same-origin", media: "application/json", prepare: "state_conflict", status: http.StatusConflict, reason: "job_state_conflict"},
		{name: "request_conflict", marker: "recheck-operator", site: "same-origin", media: "application/json", prepare: "request_conflict", status: http.StatusConflict, reason: "request_conflict"},
		{name: "storage_failure", marker: "recheck-operator", site: "same-origin", media: "application/json", prepare: "storage_failure", status: http.StatusInternalServerError, reason: "operator_action_failed"},
		{name: "success", marker: "recheck-operator", site: "same-origin", media: "application/json", prepare: "success", status: http.StatusOK},
		{name: "replay", marker: "recheck-operator", site: "same-origin", media: "application/json", prepare: "replay", status: http.StatusOK},
	}
	for _, action := range []string{"retry", "abandon"} {
		for _, tc := range cases {
			t.Run(action+"/"+tc.name, func(t *testing.T) {
				store, db, now := seedProblem3Review3SchedulerStore(t)
				store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{Now: func() time.Time { return now }, RecheckFault: func(stage string) error {
					if stage == "persist_candidate" {
						return errors.New("review6 terminal seed")
					}
					return nil
				}})
				if result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100); !alertlifecycle.IsRecheckDurableFailure(err) || result.Status != "failed" {
					t.Fatalf("failed seed result=%+v err=%v", result, err)
				}
				store.SetRecheckIntegrationTestHooks(nil)
				operationTokenIndex := 0
				operationTokens := make([]string, 0, 4)
				operationToken := func() (string, error) {
					operationTokenIndex++
					token := fmt.Sprintf("review7-operation-token-%d", operationTokenIndex)
					operationTokens = append(operationTokens, token)
					return token, nil
				}
				store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{OperationToken: operationToken})
				previousStore, previousConfig := lifecycleStore, config.Config
				lifecycleStore = store
				config.Config.Listen.Prefix = "/review7"
				config.Config.Authentication.Enabled = true
				config.Config.Authentication.Header.Name, config.Config.Authentication.Header.ValueRegex = "X-Review7-User", "(.+)"
				t.Cleanup(func() { lifecycleStore, config.Config = previousStore, previousConfig })
				wakeCalls := 0
				store.SetRecheckCommitWake(func() { wakeCalls++ })
				finalizerCalls := 0
				store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{OperationToken: operationToken, OperatorFinalizer: func() { finalizerCalls++ }})
				router := chi.NewRouter()
				setupRouter(router, nil)
				requestID := "review7-request-" + action + "-" + tc.name
				reason := "review7-reason-" + action + "-" + tc.name
				body := tc.body
				if body == "" {
					body = fmt.Sprintf(`{"requestId":%q,"reason":%q}`, requestID, reason)
				} else {
					body = strings.ReplaceAll(body, "review7-matrix", requestID)
				}
				jobID := 1
				if tc.prepare == "not_found" {
					jobID = 99
				}
				path := fmt.Sprintf("/review7/alerts-history/recheck/%d/%s", jobID, action)
				makeRequest := func(payload string, authenticated bool) *httptest.ResponseRecorder {
					req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(payload))
					if tc.media != "" {
						req.Header.Set("Content-Type", tc.media)
					}
					if tc.marker != "" {
						req.Header.Set("X-Karma-Request", tc.marker)
					}
					if tc.site != "" {
						req.Header.Set("Sec-Fetch-Site", tc.site)
					}
					if authenticated {
						req.Header.Set("X-Review7-User", "review7-user")
					}
					recorder := httptest.NewRecorder()
					router.ServeHTTP(recorder, req)
					return recorder
				}
				if tc.prepare == "state_conflict" {
					if _, err := db.Exec(`UPDATE silence_recheck_jobs SET status='completed' WHERE job_id=1`); err != nil {
						t.Fatal(err)
					}
				}
				if tc.prepare == "request_conflict" || tc.prepare == "replay" {
					if first := makeRequest(body, true); first.Code != http.StatusOK {
						t.Fatalf("setup=%d %s", first.Code, first.Body.String())
					}
					if tc.prepare == "request_conflict" {
						body = fmt.Sprintf(`{"requestId":%q,"reason":%q}`, requestID, "review7-changed-reason-"+tc.name)
					}
				}
				if tc.prepare == "storage_failure" {
					store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{OperationToken: func() (string, error) { return "", errors.New("review7-storage-sentinel") }})
				}
				wakeBefore, stateBefore := wakeCalls, captureProblem4RouterState(t, db)
				tablesBefore := captureProblem4RouterTables(t, db)
				ledgerTokensBefore := captureProblem4RouterLedgerTokens(t, db)
				var retainedAttemptToken string
				if err := db.QueryRow(`SELECT attempt_token FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&retainedAttemptToken); err != nil {
					t.Fatal(err)
				}
				var logs bytes.Buffer
				previousLogger := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
				defer slog.SetDefault(previousLogger)
				var recorder *httptest.ResponseRecorder
				var requestStarted, requestFinished time.Time
				var blockedState string
				var tablesBlocked map[string]problem4RouterTableSnapshot
				if tc.prepare == "success" {
					requestStarted = time.Now().UTC()
					entered, release := make(chan struct{}), make(chan struct{})
					store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{OperationToken: operationToken, OperatorFinalizer: func() { finalizerCalls++; close(entered); <-release }})
					response := make(chan *httptest.ResponseRecorder, 1)
					go func() { response <- makeRequest(body, true) }()
					<-entered
					blockedState = captureProblem4RouterState(t, db)
					tablesBlocked = captureProblem4RouterTables(t, db)
					if blockedState == stateBefore || strings.Contains(logs.String(), `"event":"operator_action_applied"`) || wakeCalls != wakeBefore || finalizerCalls != 1 {
						t.Fatalf("blocked finalizer state=%s logs=%s wakes=%d/%d", blockedState, logs.String(), wakeCalls, wakeBefore)
					}
					assertProblem4RouterCommittedState(t, db, action, requestID, "review7-user", reason, 1)
					close(release)
					recorder = <-response
					requestFinished = time.Now().UTC()
				} else {
					if tc.prepare == "replay" {
						requestStarted = time.Now().UTC()
					}
					recorder = makeRequest(body, tc.name != "unauthorized")
					if tc.prepare == "replay" {
						requestFinished = time.Now().UTC()
					}
				}
				if recorder.Code != tc.status {
					t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
				}
				expected := map[int]string{http.StatusUnauthorized: "Access denied\n", http.StatusForbidden: `{"error":"cross_origin_request"}`, http.StatusUnsupportedMediaType: `{"error":"unsupported_media_type"}`, http.StatusBadRequest: `{"error":"invalid recheck operator request"}`, http.StatusNotFound: `{"error":"recheck job not found"}`, http.StatusConflict: `{"error":"recheck operator action conflict"}`, http.StatusInternalServerError: `{"error":"recheck operator action failed"}`}
				if tc.status == http.StatusOK {
					current := "queued"
					if action == "abandon" {
						current = "abandoned"
					}
					expected[http.StatusOK] = fmt.Sprintf(`{"status":"success","jobId":1,"action":%q,"requestId":%q,"applied":true,"currentStatus":%q}`, action, requestID, current)
				}
				if recorder.Body.String() != expected[tc.status] {
					t.Fatalf("body=%q want=%q", recorder.Body.String(), expected[tc.status])
				}
				stateAfter := captureProblem4RouterState(t, db)
				tablesAfter := captureProblem4RouterTables(t, db)
				if tc.status != http.StatusOK && stateAfter != stateBefore {
					t.Fatalf("rejection changed state")
				}
				if tc.status == http.StatusOK {
					output := logs.String() + recorder.Body.String()
					for _, record := range problem4CommandProblem4Records(logs.String()) {
						encoded, marshalErr := json.Marshal(record)
						if marshalErr != nil {
							t.Fatal(marshalErr)
						}
						if strings.Contains(string(encoded), reason) || strings.Contains(string(encoded), retainedAttemptToken) || strings.Contains(string(encoded), "review7-storage-sentinel") {
							t.Fatalf("success structured record leaked sensitive value: %s", encoded)
						}
					}
					if strings.Contains(output, reason) || strings.Contains(output, retainedAttemptToken) || strings.Contains(output, "review7-storage-sentinel") {
						t.Fatalf("success/replay output leaked sensitive value: %s", output)
					}
				}
				switch {
				case tc.status == http.StatusOK && tc.prepare != "replay":
					current := "queued"
					if action == "abandon" {
						current = "abandoned"
					}
					expectedEvent := map[string]any{"level": "INFO", "msg": "Silence recheck operator action applied", "component": "silence_recheck", "event": "operator_action_applied", "job_id": float64(1), "action": action, "request_id": requestID, "resulting_status": current, "operator": "review7-user"}
					if wakeCalls != wakeBefore+1 || finalizerCalls != 1 || stateAfter == stateBefore || stateAfter != blockedState || !reflect.DeepEqual(problem4CommandRecords(logs.String(), "operator_action_applied"), []map[string]any{expectedEvent}) || !reflect.DeepEqual(problem4CommandProblem4Records(logs.String()), []map[string]any{expectedEvent}) {
						t.Fatalf("success ownership wakes=%d/%d state=%s records=%v", wakeCalls, wakeBefore, stateAfter, problem4CommandRecords(logs.String(), "operator_action_applied"))
					}
					if stateAfter != captureProblem4RouterState(t, db) {
						t.Fatal("success durable snapshot changed after finalizer release")
					}
					if !reflect.DeepEqual(tablesBlocked, tablesAfter) {
						t.Fatal("success post-release table snapshot changed")
					}
					if operationTokenIndex != 1 || len(operationTokens) != 1 || operationTokens[0] != "review7-operation-token-1" {
						t.Fatalf("success operation token sequence=%v index=%d", operationTokens, operationTokenIndex)
					}
					assertProblem4RouterExpectedSuccessTransition(t, tablesBefore, tablesBlocked, action, requestID, "review7-user", reason, current, "review7-operation-token-1", requestStarted, requestFinished)
				case tc.prepare == "replay":
					var actionRows, attemptRows int
					if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions WHERE job_id=1`).Scan(&actionRows); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attemptRows); err != nil {
						t.Fatal(err)
					}
					beforeParts, afterParts := strings.SplitN(stateBefore, "\nlifecycle_writer_commits:", 2), strings.SplitN(stateAfter, "\nlifecycle_writer_commits:", 2)
					ledgerTokensAfter := captureProblem4RouterLedgerTokens(t, db)
					latestToken, latestOperation, latestCommittedAt := captureProblem4RouterLatestWriterLedgerRow(t, db)
					if actionRows != 1 || attemptRows != 1 || len(beforeParts) != 2 || len(afterParts) != 2 || beforeParts[0] != afterParts[0] || !strings.HasPrefix(afterParts[1], beforeParts[1]+";") || len(ledgerTokensAfter) != len(ledgerTokensBefore)+1 || operationTokenIndex != 2 || latestToken != "review7-operation-token-2" || latestOperation != "recheck_operator_action" || latestCommittedAt == "" || wakeCalls != wakeBefore || len(problem4CommandProblem4Records(logs.String())) != 0 {
						t.Fatalf("replay changed ownership actions=%d attempts=%d\nbefore=%s\nafter=%s", actionRows, attemptRows, stateBefore, stateAfter)
					}
					committedAt, err := time.Parse(time.RFC3339Nano, latestCommittedAt)
					if err != nil {
						t.Fatalf("replay committed_at=%q: %v", latestCommittedAt, err)
					}
					_, offset := committedAt.Zone()
					if offset != 0 || committedAt.Before(requestStarted) || committedAt.After(requestFinished) {
						t.Fatalf("replay committed_at=%s outside request bound [%s,%s]", committedAt, requestStarted, requestFinished)
					}
					for _, token := range ledgerTokensBefore {
						if token == latestToken {
							t.Fatalf("replay reused writer token %q", latestToken)
						}
					}
				case tc.name == "unauthorized":
					if strings.Contains(logs.String(), `"event":"operator_action_`) || len(problem4CommandProblem4Records(logs.String())) != 0 || strings.Contains(logs.String()+recorder.Body.String(), requestID) || strings.Contains(logs.String()+recorder.Body.String(), reason) || strings.Contains(logs.String()+recorder.Body.String(), retainedAttemptToken) || wakeCalls != wakeBefore || finalizerCalls != 0 {
						t.Fatalf("unauthorized reached operator app logs=%s", logs.String())
					}
				default:
					records := problem4CommandRecords(logs.String(), "operator_action_rejected")
					category := map[string]string{"cross_origin_request": "request_origin", "unsupported_media_type": "client_input", "invalid_request": "client_input", "job_not_found": "not_found", "job_state_conflict": "state_conflict", "request_conflict": "state_conflict", "operator_action_failed": "storage_error"}[tc.reason]
					level := "DEBUG"
					if category == "not_found" || category == "state_conflict" {
						level = "WARN"
					}
					if category == "storage_error" {
						level = "ERROR"
					}
					expectedRecord := map[string]any{"level": level, "msg": "Recheck operator action was rejected", "component": "silence_recheck", "event": "operator_action_rejected", "action": action, "operator": "review7-user", "reason_code": tc.reason, "error": category}
					if tc.reason != "cross_origin_request" && tc.reason != "unsupported_media_type" {
						expectedRecord["job_id"] = float64(jobID)
					}
					actualRequestID, actualReason := "", ""
					var submitted map[string]any
					if json.Unmarshal([]byte(body), &submitted) == nil {
						if value, ok := submitted["requestId"].(string); ok {
							actualRequestID = value
						}
						if value, ok := submitted["reason"].(string); ok {
							actualReason = value
						}
					}
					output := logs.String() + recorder.Body.String()
					for _, record := range problem4CommandProblem4Records(logs.String()) {
						encoded, marshalErr := json.Marshal(record)
						if marshalErr != nil {
							t.Fatal(marshalErr)
						}
						if (actualReason != "" && strings.Contains(string(encoded), actualReason)) || strings.Contains(string(encoded), retainedAttemptToken) || strings.Contains(string(encoded), "review7-storage-sentinel") {
							t.Fatalf("structured record leaked sensitive value: %s", encoded)
						}
					}
					if !reflect.DeepEqual(records, []map[string]any{expectedRecord}) || wakeCalls != wakeBefore || (actualRequestID != "" && strings.Contains(output, actualRequestID)) || (actualReason != "" && strings.Contains(output, actualReason)) || strings.Contains(output, retainedAttemptToken) || strings.Contains(output, "review7-storage-sentinel") {
						t.Fatalf("rejection records=%v wakes=%d/%d logs=%s", records, wakeCalls, wakeBefore, logs.String())
					}
				}
			})
		}
	}
}

func problem4CommandRecords(output, event string) []map[string]any {
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["event"] == event {
			delete(record, "time")
			records = append(records, record)
		}
	}
	return records
}

func problem4CommandProblem4Records(output string) []map[string]any {
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["component"] == "silence_recheck" {
			delete(record, "time")
			records = append(records, record)
		}
	}
	return records
}

type problem4RouterTableSnapshot struct {
	Columns []string
	Rows    []map[string]string
}

func captureProblem4RouterTables(t *testing.T, db *sql.DB) map[string]problem4RouterTableSnapshot {
	t.Helper()
	result := make(map[string]problem4RouterTableSnapshot)
	for _, table := range []string{"silence_recheck_jobs", "silence_recheck_job_rules", "silence_recheck_job_candidates", "silence_recheck_job_attempts", "silence_recheck_job_operator_actions", "lifecycle_writer_commits"} {
		rows, err := db.Query(`SELECT * FROM ` + table + ` ORDER BY rowid`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		snapshot := problem4RouterTableSnapshot{Columns: append([]string(nil), columns...)}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(values))
			for index := range values {
				pointers[index] = &values[index]
			}
			if err := rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			row := make(map[string]string, len(columns))
			for index, value := range values {
				switch typed := value.(type) {
				case nil:
					row[columns[index]] = "<NULL>"
				case []byte:
					row[columns[index]] = string(typed)
				default:
					row[columns[index]] = fmt.Sprint(typed)
				}
			}
			snapshot.Rows = append(snapshot.Rows, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		result[table] = snapshot
	}
	return result
}

func assertProblem4RouterExpectedSuccessTransition(t *testing.T, before, after map[string]problem4RouterTableSnapshot, action, requestID, actor, reason, resultingStatus, expectedToken string, started, finished time.Time) {
	t.Helper()
	for _, table := range []string{"silence_recheck_job_rules", "silence_recheck_job_candidates", "silence_recheck_job_attempts"} {
		if !reflect.DeepEqual(before[table], after[table]) {
			t.Fatalf("success changed preserved %s rows", table)
		}
	}
	beforeJobs, afterJobs := before["silence_recheck_jobs"], after["silence_recheck_jobs"]
	if !reflect.DeepEqual(beforeJobs.Columns, afterJobs.Columns) || len(beforeJobs.Rows) != len(afterJobs.Rows) || len(afterJobs.Rows) != 1 {
		t.Fatalf("job table shape changed before=%+v after=%+v", beforeJobs, afterJobs)
	}
	changes := map[string]string{"status": resultingStatus}
	if action == "retry" {
		beforeRecovery, err := strconv.Atoi(beforeJobs.Rows[0]["recovery_count"])
		if err != nil {
			t.Fatal(err)
		}
		changes["recovery_count"] = strconv.Itoa(beforeRecovery + 1)
		changes["finished_at"] = "<NULL>"
	} else {
		changes["abandoned_by"] = actor
		changes["abandon_reason"] = reason
	}
	for _, column := range beforeJobs.Columns {
		want := beforeJobs.Rows[0][column]
		if expected, ok := changes[column]; ok {
			want = expected
		}
		if action == "abandon" && (column == "abandoned_at" || column == "finished_at") {
			value := afterJobs.Rows[0][column]
			parsed, err := time.Parse(time.RFC3339Nano, value)
			if err != nil || parsed.Before(started.Add(-time.Second)) || parsed.After(finished.Add(time.Second)) {
				t.Fatalf("job %s timestamp=%q outside request bound [%s,%s]", column, value, started, finished)
			}
			continue
		}
		if afterJobs.Rows[0][column] != want {
			t.Fatalf("unexpected job column=%s: got=%q want=%q", column, afterJobs.Rows[0][column], want)
		}
	}
	beforeActions, afterActions := before["silence_recheck_job_operator_actions"], after["silence_recheck_job_operator_actions"]
	fingerprint := problem4RouterFingerprint(action, reason, &actor)
	expectedAction := map[string]string{"action_id": strconv.Itoa(len(beforeActions.Rows) + 1), "job_id": "1", "request_id": requestID, "action": action, "actor": actor, "reason": reason, "request_fingerprint": fingerprint, "applied_result": "applied", "resulting_status": resultingStatus}
	if len(afterActions.Rows) != len(beforeActions.Rows)+1 {
		t.Fatalf("operator action row delta=%d", len(afterActions.Rows)-len(beforeActions.Rows))
	}
	for _, column := range beforeActions.Columns {
		if expected, ok := expectedAction[column]; ok && afterActions.Rows[len(afterActions.Rows)-1][column] != expected {
			t.Fatalf("unexpected action column=%s: got=%q want=%q", column, afterActions.Rows[len(afterActions.Rows)-1][column], expected)
		}
	}
	actionRow := afterActions.Rows[len(afterActions.Rows)-1]
	requestedAt, requestedErr := time.Parse(time.RFC3339Nano, actionRow["requested_at"])
	appliedAt, appliedErr := time.Parse(time.RFC3339Nano, actionRow["applied_at"])
	_, requestedOffset := requestedAt.Zone()
	_, appliedOffset := appliedAt.Zone()
	if requestedErr != nil || appliedErr != nil || requestedOffset != 0 || appliedOffset != 0 || !requestedAt.Equal(appliedAt) || requestedAt.Before(started) || requestedAt.After(finished) {
		t.Fatalf("operator action timestamps requested=%q applied=%q outside [%s,%s]", actionRow["requested_at"], actionRow["applied_at"], started, finished)
	}
	if len(beforeActions.Rows) > 0 && !reflect.DeepEqual(beforeActions.Rows, afterActions.Rows[:len(beforeActions.Rows)]) {
		t.Fatalf("success changed prior operator rows before=%v after=%v", beforeActions.Rows, afterActions.Rows)
	}
	beforeLedger, afterLedger := before["lifecycle_writer_commits"], after["lifecycle_writer_commits"]
	if !reflect.DeepEqual(beforeLedger.Columns, afterLedger.Columns) || len(afterLedger.Rows) != len(beforeLedger.Rows)+1 || !reflect.DeepEqual(beforeLedger.Rows, afterLedger.Rows[:len(beforeLedger.Rows)]) {
		t.Fatalf("unexpected writer ledger transition")
	}
	newLedger := afterLedger.Rows[len(afterLedger.Rows)-1]
	if newLedger["operation_token"] != expectedToken || newLedger["operation_kind"] != "recheck_operator_action" {
		t.Fatalf("unexpected expected writer row=%v", newLedger)
	}
	committedAt, err := time.Parse(time.RFC3339Nano, newLedger["committed_at"])
	_, offset := committedAt.Zone()
	if err != nil || offset != 0 || committedAt.Before(started) || committedAt.After(finished) {
		t.Fatalf("writer committed_at=%q outside request bound [%s,%s]", newLedger["committed_at"], started, finished)
	}
}

func assertProblem4RouterCommittedState(t *testing.T, db *sql.DB, action, requestID, actor, reason string, attemptCountWant int) {
	t.Helper()
	wantStatus := "queued"
	if action == "abandon" {
		wantStatus = "abandoned"
	}
	var status string
	var attemptCount, attemptRows, actionRows int
	if err := db.QueryRow(`SELECT status, attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attemptCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attemptRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions WHERE job_id=1`).Scan(&actionRows); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || attemptCount != attemptCountWant || attemptRows != 1 || actionRows != 1 {
		t.Fatalf("committed operator state status=%q attempt_count=%d attempt_rows=%d action_rows=%d", status, attemptCount, attemptRows, actionRows)
	}
	var actionID, jobID int64
	var persistedRequestID, persistedAction, persistedActor, persistedReason, persistedFingerprint, requestedAt, appliedAt, appliedResult, resultingStatus string
	if err := db.QueryRow(`SELECT action_id,job_id,request_id,action,COALESCE(actor,''),reason,request_fingerprint,requested_at,applied_at,applied_result,resulting_status FROM silence_recheck_job_operator_actions WHERE job_id=1`).Scan(&actionID, &jobID, &persistedRequestID, &persistedAction, &persistedActor, &persistedReason, &persistedFingerprint, &requestedAt, &appliedAt, &appliedResult, &resultingStatus); err != nil {
		t.Fatal(err)
	}
	if actionID <= 0 || jobID != 1 || persistedRequestID != requestID || persistedAction != action || persistedActor != actor || persistedReason != reason || persistedFingerprint != problem4RouterFingerprint(action, reason, &actor) || requestedAt == "" || appliedAt != requestedAt || appliedResult != "applied" || resultingStatus != wantStatus {
		t.Fatalf("unexpected operator row id=%d job=%d request=%q action=%q actor=%q reason=%q fingerprint=%q requested=%q applied=%q result=%q status=%q", actionID, jobID, persistedRequestID, persistedAction, persistedActor, persistedReason, persistedFingerprint, requestedAt, appliedAt, appliedResult, resultingStatus)
	}
	if _, err := time.Parse(time.RFC3339Nano, requestedAt); err != nil {
		t.Fatalf("operator requested_at=%q: %v", requestedAt, err)
	}
}

func problem4RouterFingerprint(action, reason string, actor *string) string {
	type fingerprintInput struct {
		FormatVersion    int     `json:"formatVersion"`
		Action           string  `json:"action"`
		NormalizedReason string  `json:"normalizedReason"`
		NormalizedActor  *string `json:"normalizedActor,omitempty"`
	}
	input := fingerprintInput{FormatVersion: 1, Action: action, NormalizedReason: strings.TrimSpace(reason)}
	if actor != nil && strings.TrimSpace(*actor) != "" {
		trimmed := strings.TrimSpace(*actor)
		input.NormalizedActor = &trimmed
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func captureProblem4RouterState(t *testing.T, db *sql.DB) string {
	t.Helper()
	parts := make([]string, 0, 6)
	for _, table := range []string{"silence_recheck_jobs", "silence_recheck_job_rules", "silence_recheck_job_candidates", "silence_recheck_job_attempts", "silence_recheck_job_operator_actions", "lifecycle_writer_commits"} {
		rows, err := db.Query(`SELECT * FROM ` + table + ` ORDER BY rowid`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		var tableRows []string
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for index := range values {
				pointers[index] = &values[index]
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
			tableRows = append(tableRows, string(encoded))
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		parts = append(parts, table+":"+strings.Join(tableRows, ";"))
	}
	return strings.Join(parts, "\n")
}

func captureProblem4RouterLedgerTokens(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT operation_token FROM lifecycle_writer_commits ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var tokens []string
	for rows.Next() {
		var token string
		if err = rows.Scan(&token); err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return tokens
}

func captureProblem4RouterLatestWriterLedgerRow(t *testing.T, db *sql.DB) (string, string, string) {
	t.Helper()
	var token, operation, committedAt string
	if err := db.QueryRow(`
SELECT operation_token, operation_kind, committed_at
FROM lifecycle_writer_commits
ORDER BY rowid DESC LIMIT 1`).Scan(&token, &operation, &committedAt); err != nil {
		t.Fatal(err)
	}
	return token, operation, committedAt
}

func TestProblem4WarningIssueMatrixUsesStableKindsAndMetadata(t *testing.T) {
	base := alertlifecycle.Stats{GroupingFingerprint: "stored", RegroupStatus: "idle"}
	cases := []struct {
		name     string
		stats    alertlifecycle.Stats
		expected string
		metadata any
	}{
		{name: "regroup out of date", stats: base, expected: "regroup_out_of_date", metadata: alertLifecycleRegroupOutOfDateMetadata{}},
		{name: "regroup in progress", stats: withRegroupStats(base, "running"), expected: "regroup_in_progress", metadata: alertLifecycleRegroupInProgressMetadata{}},
		{name: "regroup failed", stats: withRegroupStats(base, "error"), expected: "regroup_failed", metadata: alertLifecycleRegroupFailedMetadata{}},
		{name: "HA inconsistency", stats: withInconsistentClusters(base), expected: "silence_ha_inconsistent", metadata: alertLifecycleSilenceHAInconsistentMetadata{}},
		{name: "retrying head", stats: withRecheckHead(base, "retry_wait"), expected: "recheck_retrying", metadata: alertLifecycleRecheckRetryingMetadata{}},
		{name: "blocked head", stats: withRecheckHead(base, "failed"), expected: "recheck_blocked", metadata: alertLifecycleRecheckBlockedMetadata{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issues := buildAlertLifecycleWarningIssues(tc.stats, "expected")
			var found *alertLifecycleWarningIssue
			for i := range issues {
				if issues[i].Kind == tc.expected {
					found = &issues[i]
					break
				}
			}
			if found == nil {
				t.Fatalf("missing %q in %+v", tc.expected, issues)
			}
			switch tc.metadata.(type) {
			case alertLifecycleRegroupOutOfDateMetadata:
				if _, ok := found.Metadata.(alertLifecycleRegroupOutOfDateMetadata); !ok {
					t.Fatalf("metadata type for %q: %T", tc.expected, found.Metadata)
				}
			case alertLifecycleRegroupInProgressMetadata:
				if _, ok := found.Metadata.(alertLifecycleRegroupInProgressMetadata); !ok {
					t.Fatalf("metadata type for %q: %T", tc.expected, found.Metadata)
				}
			case alertLifecycleRegroupFailedMetadata:
				if _, ok := found.Metadata.(alertLifecycleRegroupFailedMetadata); !ok {
					t.Fatalf("metadata type for %q: %T", tc.expected, found.Metadata)
				}
			case alertLifecycleSilenceHAInconsistentMetadata:
				if _, ok := found.Metadata.(alertLifecycleSilenceHAInconsistentMetadata); !ok {
					t.Fatalf("metadata type for %q: %T", tc.expected, found.Metadata)
				}
			case alertLifecycleRecheckRetryingMetadata:
				if _, ok := found.Metadata.(alertLifecycleRecheckRetryingMetadata); !ok {
					t.Fatalf("metadata type for %q: %T", tc.expected, found.Metadata)
				}
			case alertLifecycleRecheckBlockedMetadata:
				if _, ok := found.Metadata.(alertLifecycleRecheckBlockedMetadata); !ok {
					t.Fatalf("metadata type for %q: %T", tc.expected, found.Metadata)
				}
			}
		})
	}
}

func withRegroupStats(stats alertlifecycle.Stats, status string) alertlifecycle.Stats {
	stats.RegroupStatus = status
	stats.RegroupProcessed = 2
	stats.RegroupTotal = 4
	stats.RegroupError = "controlled regroup error"
	return stats
}

func withInconsistentClusters(stats alertlifecycle.Stats) alertlifecycle.Stats {
	stats.SilenceInconsistentClusters = []string{"production"}
	return stats
}

func withRecheckHead(stats alertlifecycle.Stats, status string) alertlifecycle.Stats {
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	stats.Recheck.HeadJob = &alertlifecycle.RecheckHeadJob{
		JobID: 547, Status: status, CreatedAt: now, RetryCount: 2,
		NextAttemptAt: func() *time.Time { value := now.Add(time.Minute); return &value }(),
		LastError:     &alertlifecycle.RecheckErrorSnapshot{Stage: "persist_candidate", At: now},
		FirstError:    &alertlifecycle.RecheckErrorSnapshot{Stage: "persist_candidate", At: now},
	}
	return stats
}
