package alertlifecycle

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type problem4SynchronizedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *problem4SynchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(data)
}

func (b *problem4SynchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestProblem4PollObservabilityFailureDoesNotChangeCommittedDomain(t *testing.T) {
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	input := PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am", ClusterID: "cluster-a", SyncStatus: "success"}},
		Timestamp:           time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC),
	}
	run := func(fault bool) (PollCycleResult, string, string, int) {
		t.Helper()
		var logs bytes.Buffer
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if fault {
			store.lifecycleTestHooks = &storeLifecycleTestHooks{observabilityFault: func(string, string) error { return errors.New("controlled observability failure") }}
		}
		wakes := 0
		store.recheckCommitWake = func() { wakes++ }
		result, err := store.ApplyPollCycle(input)
		if err != nil {
			t.Fatal(err)
		}
		return result, captureProblem3Review2DomainState(t, store.db), logs.String(), wakes
	}
	controlResult, controlState, _, controlWakes := run(false)
	faultResult, faultState, faultLogs, faultWakes := run(true)
	if !reflect.DeepEqual(faultResult, controlResult) || faultState != controlState || faultWakes != controlWakes {
		t.Fatalf("poll observability fault changed outcome:\nresult=%+v/%+v\nstate=%s\ncontrol=%s", faultResult, controlResult, faultState, controlState)
	}
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(faultLogs), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["event"] == "observability_collection_failed" {
			records = append(records, record)
		}
	}
	if len(records) == 0 {
		t.Fatalf("missing structured best-effort observability event: %s", faultLogs)
	}
	for _, record := range records {
		if record["component"] != "alert_lifecycle" || record["level"] != "WARN" || record["error"] != "observability_error" || strings.Contains(fmt.Sprint(record), "controlled observability failure") {
			t.Fatalf("unsafe observability record=%v", record)
		}
	}
}

func TestProblem4EveryPollEventMetadataBoundaryIsBestEffort(t *testing.T) {
	for _, family := range []string{"ha_consistency_previous", "ha_consistency_current", "recheck_job_queued"} {
		t.Run(family, func(t *testing.T) {
			var logs bytes.Buffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(previousLogger) })
			store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			sentinel := "poll-observability-sentinel-" + family
			called := false
			store.lifecycleTestHooks = &storeLifecycleTestHooks{
				observabilityFault: func(_, eventFamily string) error {
					if eventFamily == family {
						called = true
						return errors.New(sentinel)
					}
					return nil
				},
			}
			result, err := store.ApplyPollCycle(PollCycleInput{
				SuccessfulUpstreams: map[string]bool{"am": true},
				SilenceSources:      []SourceSilenceState{{SourceName: "am", ClusterID: "cluster-a", SyncStatus: "success"}},
				Timestamp:           time.Now().UTC(),
			})
			if err != nil || result.RecheckJobsQueued != 1 || !called {
				t.Fatalf("metadata fault changed poll result: family=%s result=%+v err=%v called=%t", family, result, err, called)
			}
			var jobs int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobs); err != nil {
				t.Fatal(err)
			}
			if jobs != 1 || !strings.Contains(logs.String(), `"event":"observability_collection_failed"`) {
				t.Fatalf("metadata fault changed committed state or produced no warning: jobs=%d logs=%s", jobs, logs.String())
			}
			if strings.Contains(logs.String(), sentinel) {
				t.Fatalf("poll observability log disclosed raw error: %s", logs.String())
			}
		})
	}
}

func TestProblem4GenericFinalizerStopsAfterHandoffFailure(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	runCalls := 0
	runWriterFinalizer(&writerAfterCommitFinalizer{
		ctx:           context.Background(),
		operationKind: writerOperationPoll,
		handoffErr:    errors.New("handoff failed"),
		run: func(bool) {
			runCalls++
		},
	})
	if runCalls != 0 {
		t.Fatalf("generic finalizer continued after handoff failure: runCalls=%d", runCalls)
	}
}

func TestProblem4RecheckQueueMetadataFailureDoesNotChangeCommittedResult(t *testing.T) {
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	run := func(fault bool) (RecheckBatchResult, string, string) {
		t.Helper()
		var logs bytes.Buffer
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		fixedNow := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
		store, _, _ := seedProblem2OpenJobAt(t, fixedNow)
		t.Cleanup(func() { _ = store.Close() })
		store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckNow: func() time.Time { return fixedNow }}
		if fault {
			store.lifecycleTestHooks.observabilityFault = func(stage, family string) error {
				if stage == "queue_depth" && family == "recheck_batch" {
					return errors.New("controlled queue metadata failure")
				}
				return nil
			}
		}
		logs.Reset()
		result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
		if err != nil {
			t.Fatal(err)
		}
		return result, captureProblem3Review2DomainState(t, store.db), logs.String()
	}
	controlResult, controlState, _ := run(false)
	faultResult, faultState, faultLogs := run(true)
	controlResult.QueueDepth = nil
	if !reflect.DeepEqual(faultResult, controlResult) || faultState != controlState {
		t.Fatalf("queue metadata fault changed result/domain:\nresult=%+v/%+v\nstate=%s\ncontrol=%s", faultResult, controlResult, faultState, controlState)
	}
	if !strings.Contains(faultLogs, `"stage":"queue_depth"`) || !strings.Contains(faultLogs, `"event_family":"recheck_batch"`) || !strings.Contains(faultLogs, `"queue_depth_available":false`) || strings.Contains(faultLogs, "controlled queue metadata failure") {
		t.Fatalf("missing or unsafe metadata warning: %s", faultLogs)
	}
}

func TestProblem4PostCommitEventFailureDoesNotChangePollOrHandoff(t *testing.T) {
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	run := func(fault bool) (PollCycleResult, string, int, string) {
		t.Helper()
		var logs bytes.Buffer
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		wakes := 0
		store.recheckCommitWake = func() { wakes++ }
		if fault {
			store.lifecycleTestHooks = &storeLifecycleTestHooks{postCommitEventEmissionFault: func(string) error { return errors.New("post-commit event failure") }}
		}
		result, err := store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: map[string]bool{"am": true}, SilenceSources: []SourceSilenceState{{SourceName: "am", ClusterID: "cluster-a", SyncStatus: "success"}}, Timestamp: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)})
		if err != nil || result.RecheckJobsQueued != 1 {
			t.Fatalf("post-commit event result=%+v err=%v", result, err)
		}
		return result, captureProblem3Review2DomainState(t, store.db), wakes, logs.String()
	}
	controlResult, controlState, controlWakes, _ := run(false)
	faultResult, faultState, faultWakes, faultLogs := run(true)
	if !reflect.DeepEqual(faultResult, controlResult) || faultState != controlState || faultWakes != controlWakes || !strings.Contains(faultLogs, `"stage":"post_commit_event"`) {
		t.Fatalf("post-commit event failure changed poll: result=%+v/%+v state=%t wakes=%d/%d logs=%s", faultResult, controlResult, faultState != controlState, faultWakes, controlWakes, faultLogs)
	}
}

func TestProblem4PostCommitRecheckEventFailureDoesNotChangeWorkerResult(t *testing.T) {
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	run := func(fault string) (RecheckBatchResult, string, int, string) {
		t.Helper()
		var logs bytes.Buffer
		handler := slog.Handler(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		if fault == "panic" {
			handler = problem4PanicEventHandler{base: handler, event: "job_completed"}
		}
		slog.SetDefault(slog.New(handler))
		fixedNow := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
		store, _, _ := seedProblem2OpenJobAt(t, fixedNow)
		t.Cleanup(func() { _ = store.Close() })
		wakes := 0
		store.recheckCommitWake = func() { wakes++ }
		store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckNow: func() time.Time { return fixedNow }}
		if fault == "hook" {
			store.lifecycleTestHooks.postCommitEventEmissionFault = func(string) error { return errors.New("controlled post-commit recheck event failure") }
		}
		logs.Reset()
		result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
		if err != nil || result.Disposition != RecheckBatchCommitted || result.Status != "completed" {
			t.Fatalf("post-commit recheck result=%+v err=%v", result, err)
		}
		return result, captureProblem3Review2DomainState(t, store.db), wakes, logs.String()
	}
	controlResult, controlState, controlWakes, _ := run("")
	faultResult, faultState, faultWakes, faultLogs := run("hook")
	if !reflect.DeepEqual(faultResult, controlResult) || faultState != controlState || faultWakes != controlWakes || !strings.Contains(faultLogs, `"stage":"post_commit_event"`) {
		t.Fatalf("post-commit worker event changed outcome: result=%+v/%+v state=%t wakes=%d/%d logs=%s", faultResult, controlResult, faultState != controlState, faultWakes, controlWakes, faultLogs)
	}
	panicResult, panicState, panicWakes, _ := run("panic")
	if !reflect.DeepEqual(panicResult, controlResult) || panicState != controlState || panicWakes != controlWakes {
		t.Fatalf("job_completed panic changed outcome: result=%+v/%+v state=%t wakes=%d/%d", panicResult, controlResult, panicState != controlState, panicWakes, controlWakes)
	}
}

type problem4PanicEventHandler struct {
	base  slog.Handler
	event string
	once  *sync.Once
}

func problem4ParsedEvents(t *testing.T, output string, names ...string) []map[string]any {
	t.Helper()
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[name] = struct{}{}
	}
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if _, ok := wanted[fmt.Sprint(record["event"])]; ok {
			delete(record, "time")
			records = append(records, record)
		}
	}
	return records
}

func problem4AllStructuredEvents(t *testing.T, output string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil || record["component"] != "silence_recheck" {
			continue
		}
		delete(record, "time")
		records = append(records, record)
	}
	return records
}

func problem4ExpectedTerminalOperatorEvents(action, requestID, operator string) []map[string]any {
	resultingStatus := "queued"
	if action == "abandon" {
		resultingStatus = "abandoned"
	}
	return []map[string]any{
		{"level": "DEBUG", "msg": "Silence recheck job selected", "component": "silence_recheck", "event": "job_selected", "job_id": float64(1), "status": "queued", "queue_depth_available": true, "queue_depth": float64(1), "attempt_count": float64(1), "retry_count": float64(0), "batch_size": float64(100)},
		{"level": "ERROR", "msg": "Silence recheck job reached terminal failure", "component": "silence_recheck", "event": "job_failed", "job_id": float64(1), "status": "failed", "attempt_count": float64(1), "retry_count": float64(0), "consecutive_retry_count": float64(0), "failure_class": "terminal", "failure_stage": "persist_candidate", "error": "storage_error"},
		{"level": "INFO", "msg": "Silence recheck operator action applied", "component": "silence_recheck", "event": "operator_action_applied", "job_id": float64(1), "action": action, "request_id": requestID, "resulting_status": resultingStatus, "operator": operator},
	}
}

func (h problem4PanicEventHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level)
}

func (h problem4PanicEventHandler) Handle(ctx context.Context, record slog.Record) error {
	event := ""
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "event" {
			event = attr.Value.String()
		}
		return true
	})
	if event == h.event {
		if h.once == nil {
			panic("controlled operator event failure")
		}
		shouldPanic := false
		h.once.Do(func() { shouldPanic = true })
		if shouldPanic {
			panic("controlled operator event failure")
		}
	}
	return h.base.Handle(ctx, record)
}

func (h problem4PanicEventHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return problem4PanicEventHandler{base: h.base.WithAttrs(attrs), event: h.event, once: h.once}
}

func (h problem4PanicEventHandler) WithGroup(name string) slog.Handler {
	return problem4PanicEventHandler{base: h.base.WithGroup(name), event: h.event, once: h.once}
}

func seedProblem4QueuedSuccessor(t *testing.T, store *Store, now time.Time) {
	t.Helper()
	labels := `{"service":"successor"}`
	if err := store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "review8-successor-alert", State: StateInactive, RawState: StateActive,
		IsLive: false, Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`,
		LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
		RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = upsertTargetStateTx(tx, "review8-successor-alert", targetStateRow{
		Target: "cluster-a", State: StateInactive, RawState: StateActive,
		IsLive: false, IsSuppressed: false, IsResolved: false,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now, SourceMembers: []string{"am-a"},
	}, "review8-fixture", now); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	result, err := store.db.Exec(`
INSERT INTO silence_recheck_jobs(
 source_context_fingerprint, job_fingerprint, source_context_json, trigger_type,
 status, total_candidates, created_at
)
SELECT 'review8-successor-context', 'review8-successor-job', source_context_json,
       'source_context_changed', 'queued', 1, ?
FROM silence_recheck_jobs WHERE job_id=1`, now.Add(time.Second).UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	jobID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`
INSERT INTO silence_recheck_job_rules(
 job_id, source_name, cluster_id, target_key, rule_id, revision_id,
 silence_id, content_fingerprint, observed_state, evaluation_fingerprint, matchers_json
)
SELECT ?, source_name, cluster_id, target_key, rule_id, revision_id,
       silence_id, content_fingerprint, observed_state, evaluation_fingerprint, matchers_json
FROM silence_recheck_job_rules WHERE job_id=1`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`
INSERT INTO silence_recheck_job_candidates(
 job_id, alert_id, target_key, expected_alert_last_seen_at,
 expected_target_last_seen_at, match_labels_json, relevant_sources_json,
 target_source_members_json
)
SELECT ?, 'review8-successor-alert', 'cluster-a', last_seen_at, last_seen_at, ?, ?, '["am-a"]'
FROM alert_target_states WHERE alert_id='review8-successor-alert' AND target='cluster-a'`, jobID, labels, `[{"name":"am-a","cluster":"cluster-a"}]`); err != nil {
		t.Fatal(err)
	}
}

func TestProblem4OperatorEventFailureDoesNotChangeCommitOrWake(t *testing.T) {
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	baseline, _, now := seedProblem2OpenJob(t)
	baseline.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("terminal candidate failure")
		}
		return nil
	}}
	if result, err := baseline.RunSilenceRecheckBatchContext(context.Background(), 100); !IsRecheckDurableFailure(err) || result.Status != "failed" {
		t.Fatalf("failed-head seed=%+v err=%v", result, err)
	}
	baseline.lifecycleTestHooks = nil
	if _, err := baseline.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	baselinePath := baseline.path
	if err := baseline.Close(); err != nil {
		t.Fatal(err)
	}
	baselineBytes, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	run := func(fault bool) (RecheckOperatorActionResult, string, int) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "history.sqlite")
		if writeErr := os.WriteFile(path, baselineBytes, 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
		store, openErr := OpenStore(StoreOptions{Path: path})
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() { _ = store.Close() })
		wakes := 0
		store.SetRecheckCommitWake(func() { wakes++ })
		var logs bytes.Buffer
		handler := slog.Handler(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		if fault {
			handler = problem4PanicEventHandler{base: handler, event: "operator_action_applied"}
		}
		slog.SetDefault(slog.New(handler))
		actor := "operator"
		result, err := store.RetryRecheckJobContext(context.Background(), 1, "event-failure-retry", "recover", &actor, now)
		if err != nil || !result.Applied || result.CurrentStatus != "queued" || wakes != 1 || store.pendingRecheckOperatorCompletion() != nil {
			t.Fatalf("operator result=%+v err=%v wakes=%d", result, err, wakes)
		}
		return result, captureProblem4RecheckWriteSet(t, store.db), wakes
	}
	controlResult, controlState, controlWakes := run(false)
	faultResult, faultState, faultWakes := run(true)
	if !reflect.DeepEqual(faultResult, controlResult) || faultState != controlState || faultWakes != controlWakes {
		t.Fatalf("operator event failure changed result/state/wake: result=%+v/%+v wakes=%d/%d\n%s\n%s", faultResult, controlResult, faultWakes, controlWakes, faultState, controlState)
	}
}

func captureProblem4RecheckWriteSet(t *testing.T, db *sql.DB) string {
	t.Helper()
	parts := make([]string, 0, 7)
	for _, table := range []string{"silence_recheck_jobs", "silence_recheck_job_rules", "silence_recheck_job_candidates", "silence_recheck_job_attempts", "silence_recheck_job_operator_actions"} {
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
			encodedRows = append(encodedRows, string(encoded))
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		parts = append(parts, table+":"+strings.Join(encodedRows, ";"))
	}
	rows, err := db.Query(`SELECT operation_kind,COUNT(*) FROM lifecycle_writer_commits GROUP BY operation_kind ORDER BY operation_kind`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var operation string
		var count int
		if err = rows.Scan(&operation, &count); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, fmt.Sprintf("writer:%s=%d", operation, count))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(parts, "\n")
}

func captureProblem4WriterLedgerRows(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT * FROM lifecycle_writer_commits ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var encodedRows []string
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err = rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		encoded, marshalErr := json.Marshal(values)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		encodedRows = append(encodedRows, string(encoded))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return encodedRows
}

type problem4PendingState struct {
	FailurePresent                     bool
	FailureJobID                       int64
	FailureAttemptToken                string
	FailureAttemptNumber               int
	FailureStartedAt                   time.Time
	FailureRetryCountBefore            int
	FailureConsecutiveRetryCountBefore int
	FailureClass                       string
	FailureStage                       string
	FailureCause                       string
	FailureAllowCommitted              bool
	FailureOperation                   string
	FailureResult                      RecheckBatchResult
	FailureResultReady                 bool
	FailureSelectionStatus             string
	FailureSelectionQueueDepth         *int
	FailureSelectionBatchSize          int
	FailureCompletionCommitted         bool
	FailureCompletionFinalized         bool
	OperatorPresent                    bool
	OperatorJobID                      int64
	OperatorRequestID                  string
	OperatorCompletionCommitted        bool
	OperatorCompletionFinalized        bool
}

func captureProblem4PendingState(t *testing.T, store *Store) string {
	t.Helper()
	store.recheckStateMu.Lock()
	defer store.recheckStateMu.Unlock()
	state := problem4PendingState{}
	if pending := store.pendingRecheckFailure; pending != nil {
		state.FailurePresent = true
		state.FailureJobID = pending.Attempt.JobID
		state.FailureAttemptToken = pending.Attempt.Token
		state.FailureAttemptNumber = pending.Attempt.Number
		state.FailureStartedAt = pending.Attempt.StartedAt
		state.FailureRetryCountBefore = pending.Attempt.RetryCountBefore
		state.FailureConsecutiveRetryCountBefore = pending.Attempt.ConsecutiveRetryCountBefore
		state.FailureClass = string(pending.Attempt.FailureClass)
		state.FailureStage = pending.Attempt.FailureStage
		if pending.Attempt.FailureCause != nil {
			state.FailureCause = pending.Attempt.FailureCause.Error()
		}
		state.FailureAllowCommitted = pending.Attempt.AllowCommitted
		state.FailureOperation = pending.Operation
		state.FailureResult = pending.Result
		state.FailureResultReady = pending.ResultReady
		state.FailureSelectionStatus = pending.Selection.Status
		state.FailureSelectionQueueDepth = pending.Selection.QueueDepth
		state.FailureSelectionBatchSize = pending.Selection.BatchSize
		if pending.Completion != nil {
			state.FailureCompletionCommitted = pending.Completion.isCommitted()
			state.FailureCompletionFinalized = pending.Completion.isFinalized()
		}
	}
	if pending := store.pendingRecheckOperator; pending != nil {
		state.OperatorPresent = true
		state.OperatorJobID = pending.JobID
		state.OperatorRequestID = pending.RequestID
		if pending.Completion != nil {
			state.OperatorCompletionCommitted = pending.Completion.isCommitted()
			state.OperatorCompletionFinalized = pending.Completion.isFinalized()
		}
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func countProblem4OperatorActions(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions WHERE job_id=1`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type problem4DurableSnapshot struct {
	SemanticRows     string
	SemanticTables   map[string]problem4TableSnapshot
	WriterLedgerRows []string
	AttemptRows      []string
	PendingState     string
	WakeCount        int
}

type problem4TableSnapshot struct {
	Columns []string
	Rows    []map[string]string
}

func captureProblem4DurableSnapshot(t *testing.T, store *Store, wakeCount int) problem4DurableSnapshot {
	t.Helper()
	semanticRows := captureProblem4RecheckWriteSet(t, store.db)
	semanticRows = strings.SplitN(semanticRows, "\nwriter:", 2)[0]
	ledgerRows := captureProblem4WriterLedgerRows(t, store.db)
	ledgerMetadata := captureProblem4WriterLedgerMetadata(t, store.db)
	for index := range ledgerRows {
		for metadataIndex, metadata := range ledgerMetadata {
			ledgerRows[index] = strings.ReplaceAll(ledgerRows[index], metadata.token, fmt.Sprintf("<writer-token-%d>", metadataIndex))
			ledgerRows[index] = strings.ReplaceAll(ledgerRows[index], metadata.committedAt, fmt.Sprintf("<committed-at-%d>", metadataIndex))
		}
	}
	return problem4DurableSnapshot{
		SemanticRows:     semanticRows,
		SemanticTables:   captureProblem4SemanticTables(t, store.db),
		WriterLedgerRows: ledgerRows,
		AttemptRows:      captureProblem4AttemptRows(t, store.db),
		PendingState:     captureProblem4PendingState(t, store),
		WakeCount:        wakeCount,
	}
}

func captureProblem4SemanticTables(t *testing.T, db *sql.DB) map[string]problem4TableSnapshot {
	t.Helper()
	result := make(map[string]problem4TableSnapshot)
	for _, table := range []string{"silence_recheck_jobs", "silence_recheck_job_rules", "silence_recheck_job_candidates", "silence_recheck_job_attempts", "silence_recheck_job_operator_actions"} {
		rows, err := db.Query(`SELECT * FROM ` + table + ` ORDER BY rowid`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		snapshot := problem4TableSnapshot{Columns: append([]string(nil), columns...)}
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

func captureProblem4AttemptRows(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT * FROM silence_recheck_job_attempts ORDER BY attempt_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result []string
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(values))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, string(encoded))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertProblem4PhaseSnapshot(t *testing.T, store *Store, snapshot problem4DurableSnapshot, pendingWant problem4PendingState, status string, attemptCountWant int, failurePending, operatorPending, failureCompletionCommittedWant bool, failureAttemptTokenWant, failureClassWant, failureStageWant, failureOperationWant, operatorRequestIDWant string, wakeCount int) {
	t.Helper()
	var pending problem4PendingState
	if err := json.Unmarshal([]byte(snapshot.PendingState), &pending); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pending, pendingWant) {
		t.Fatalf("unexpected complete pending state: got=%+v want=%+v", pending, pendingWant)
	}
	var currentStatus string
	var attemptCount int
	if err := store.db.QueryRow(`SELECT status, attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&currentStatus, &attemptCount); err != nil {
		t.Fatal(err)
	}
	if currentStatus != status || attemptCount != attemptCountWant {
		t.Fatalf("phase durable state status=%q attempt_count=%d want status=%q attempt_count=%d", currentStatus, attemptCount, status, attemptCountWant)
	}
	if pending.FailurePresent != failurePending || pending.OperatorPresent != operatorPending || snapshot.WakeCount != wakeCount {
		t.Fatalf("phase pending/wake state=%+v wake=%d want failure=%t operator=%t wake=%d", pending, snapshot.WakeCount, failurePending, operatorPending, wakeCount)
	}
	if snapshot.SemanticRows == "" || len(snapshot.WriterLedgerRows) == 0 || len(snapshot.AttemptRows) != attemptCountWant {
		t.Fatalf("phase snapshot is incomplete: %+v", snapshot)
	}
	if failurePending && (!pending.FailureResultReady || pending.FailureAttemptToken != failureAttemptTokenWant || pending.FailureJobID != 1 || pending.FailureAttemptNumber != attemptCountWant || pending.FailureClass != failureClassWant || pending.FailureStage != failureStageWant || pending.FailureOperation != failureOperationWant) {
		t.Fatalf("phase retained failure state=%+v", pending)
	}
	if failurePending && (pending.FailureCompletionCommitted != failureCompletionCommittedWant || pending.FailureCompletionFinalized) {
		t.Fatalf("phase failure completion=%+v", pending)
	}
	if operatorPending && (pending.OperatorJobID != 1 || pending.OperatorRequestID != operatorRequestIDWant) {
		t.Fatalf("phase retained operator state=%+v", pending)
	}
	if operatorPending && (!pending.OperatorCompletionCommitted || pending.OperatorCompletionFinalized) {
		t.Fatalf("phase operator completion=%+v", pending)
	}
}

func assertProblem4ExpectedTableTransition(t *testing.T, before, after problem4DurableSnapshot, table string, changes map[string]string) {
	t.Helper()
	beforeTable, beforeOK := before.SemanticTables[table]
	afterTable, afterOK := after.SemanticTables[table]
	if !beforeOK || !afterOK || !reflect.DeepEqual(beforeTable.Columns, afterTable.Columns) || len(beforeTable.Rows) != len(afterTable.Rows) {
		t.Fatalf("table shape changed for %s: before=%+v after=%+v", table, beforeTable, afterTable)
	}
	for rowIndex := range beforeTable.Rows {
		for _, column := range beforeTable.Columns {
			want := beforeTable.Rows[rowIndex][column]
			if expected, ok := changes[column]; ok {
				want = expected
			}
			if afterTable.Rows[rowIndex][column] != want {
				t.Fatalf("unexpected %s row=%d column=%s: got=%q want=%q", table, rowIndex, column, afterTable.Rows[rowIndex][column], want)
			}
		}
	}
}

func assertProblem4ExpectedAttemptTransition(t *testing.T, before, after problem4DurableSnapshot, token, failureClass, failureStage string, startedAt, finishedAt time.Time) {
	t.Helper()
	beforeTable := before.SemanticTables["silence_recheck_job_attempts"]
	afterTable := after.SemanticTables["silence_recheck_job_attempts"]
	expectedRows := append([]map[string]string(nil), beforeTable.Rows...)
	expectedRows = append(expectedRows, map[string]string{
		"attempt_id":         strconv.Itoa(len(expectedRows) + 1),
		"job_id":             "1",
		"attempt_token":      token,
		"attempt_number":     "1",
		"started_at":         startedAt.UTC().Format(time.RFC3339Nano),
		"finished_at":        finishedAt.UTC().Format(time.RFC3339Nano),
		"outcome":            "failed",
		"failure_class":      failureClass,
		"failure_stage":      failureStage,
		"scheduled_retry_at": "<NULL>",
	})
	if !reflect.DeepEqual(afterTable.Rows, expectedRows) {
		t.Fatalf("unexpected complete attempt transition: got=%v want=%v", afterTable.Rows, expectedRows)
	}
}

func assertProblem4ExpectedOperatorActionTransition(t *testing.T, before, after problem4DurableSnapshot, action, requestID, actor, reason, resultingStatus string, appliedAt time.Time) {
	t.Helper()
	beforeTable := before.SemanticTables["silence_recheck_job_operator_actions"]
	afterTable := after.SemanticTables["silence_recheck_job_operator_actions"]
	expectedRows := append([]map[string]string(nil), beforeTable.Rows...)
	fingerprint, err := recheckOperatorFingerprint(action, reason, &actor)
	if err != nil {
		t.Fatal(err)
	}
	timestamp := appliedAt.UTC().Format(time.RFC3339Nano)
	expectedRows = append(expectedRows, map[string]string{
		"action_id":           strconv.Itoa(len(expectedRows) + 1),
		"job_id":              "1",
		"request_id":          requestID,
		"action":              action,
		"actor":               actor,
		"reason":              reason,
		"request_fingerprint": fingerprint,
		"requested_at":        timestamp,
		"applied_at":          timestamp,
		"applied_result":      "applied",
		"resulting_status":    resultingStatus,
	})
	if !reflect.DeepEqual(afterTable.Rows, expectedRows) {
		t.Fatalf("unexpected complete operator action transition: got=%v want=%v", afterTable.Rows, expectedRows)
	}
}

func expectedProblem4TerminalFailureResult(jobID int64, trigger string, createdAt, transitionAt time.Time, message string) RecheckBatchResult {
	queueDepth := 1
	created := createdAt
	started := transitionAt
	finished := transitionAt
	firstError := &RecheckErrorSnapshot{Class: "terminal", Stage: "persist_candidate", At: transitionAt, Message: message}
	lastError := &RecheckErrorSnapshot{Class: "terminal", Stage: "persist_candidate", At: transitionAt, Message: message}
	return RecheckBatchResult{
		Disposition:     RecheckBatchJobFailed,
		JobID:           jobID,
		TriggerType:     trigger,
		Status:          "failed",
		QueueDepth:      &queueDepth,
		CreatedAt:       &created,
		StartedAt:       &started,
		FinishedAt:      &finished,
		TotalCandidates: 1,
		AttemptCount:    1,
		FailureClass:    "terminal",
		FailureStage:    "persist_candidate",
		FirstError:      firstError,
		LastError:       lastError,
	}
}

func expectedProblem4FailurePendingState(createdAt, startedAt time.Time, token, message string, completionCommitted bool) problem4PendingState {
	queueDepth := 1
	return problem4PendingState{
		FailurePresent:                     true,
		FailureJobID:                       1,
		FailureAttemptToken:                token,
		FailureAttemptNumber:               1,
		FailureStartedAt:                   startedAt,
		FailureRetryCountBefore:            0,
		FailureConsecutiveRetryCountBefore: 0,
		FailureClass:                       "terminal",
		FailureStage:                       "persist_candidate",
		FailureCause:                       message,
		FailureAllowCommitted:              false,
		FailureOperation:                   "recheck_failure",
		FailureResult:                      expectedProblem4TerminalFailureResult(1, "source_context_changed", createdAt, startedAt, message),
		FailureResultReady:                 true,
		FailureSelectionStatus:             "queued",
		FailureSelectionQueueDepth:         &queueDepth,
		FailureSelectionBatchSize:          100,
		FailureCompletionCommitted:         completionCommitted,
		FailureCompletionFinalized:         false,
	}
}

func expectedProblem4OperatorPendingState(requestID string) problem4PendingState {
	return problem4PendingState{
		OperatorPresent:             true,
		OperatorJobID:               1,
		OperatorRequestID:           requestID,
		OperatorCompletionCommitted: true,
		OperatorCompletionFinalized: false,
	}
}

type problem4WriterLedgerMetadata struct {
	token, committedAt string
}

type problem4ExpectedWriterLedgerRow struct {
	token, operation string
}

func assertProblem4WriterLedgerSequence(t *testing.T, db *sql.DB, prefix string, expected []problem4ExpectedWriterLedgerRow, started, finished time.Time) {
	t.Helper()
	rows, err := db.Query(`SELECT operation_token, operation_kind, committed_at FROM lifecycle_writer_commits ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	actual := make([]problem4ExpectedWriterLedgerRow, 0, len(expected))
	for rows.Next() {
		var token, operation, committedAt string
		if err := rows.Scan(&token, &operation, &committedAt); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(token, prefix) {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, committedAt)
		_, offset := parsed.Zone()
		if err != nil || offset != 0 || parsed.Before(started) || parsed.After(finished) {
			t.Fatalf("writer row %q committed_at=%q outside [%s,%s]", token, committedAt, started, finished)
		}
		actual = append(actual, problem4ExpectedWriterLedgerRow{token: token, operation: operation})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("writer ledger sequence=%v want=%v", actual, expected)
	}
}

func assertProblem4LedgerSnapshotTransition(t *testing.T, before, after problem4DurableSnapshot, addedOperations []string) {
	t.Helper()
	if len(after.WriterLedgerRows) != len(before.WriterLedgerRows)+len(addedOperations) || !reflect.DeepEqual(before.WriterLedgerRows, after.WriterLedgerRows[:len(before.WriterLedgerRows)]) {
		t.Fatalf("writer ledger snapshot changed unexpectedly before=%v after=%v", before.WriterLedgerRows, after.WriterLedgerRows)
	}
	for offset, operation := range addedOperations {
		index := len(before.WriterLedgerRows) + offset
		want := fmt.Sprintf(`["<writer-token-%d>","%s","<committed-at-%d>"]`, index, operation, index)
		if after.WriterLedgerRows[index] != want {
			t.Fatalf("writer ledger row %d=%q want=%q", index, after.WriterLedgerRows[index], want)
		}
	}
}

func captureProblem4WriterLedgerMetadata(t *testing.T, db *sql.DB) []problem4WriterLedgerMetadata {
	t.Helper()
	rows, err := db.Query(`SELECT operation_token, committed_at FROM lifecycle_writer_commits ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var metadata []problem4WriterLedgerMetadata
	for rows.Next() {
		var entry problem4WriterLedgerMetadata
		if err = rows.Scan(&entry.token, &entry.committedAt); err != nil {
			t.Fatal(err)
		}
		metadata = append(metadata, entry)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func assertProblem4LedgerRowsBounded(t *testing.T, db *sql.DB, started, finished time.Time) {
	t.Helper()
	rows, err := db.Query(`SELECT committed_at FROM lifecycle_writer_commits ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		committedAt, err := time.Parse(time.RFC3339Nano, raw)
		_, offset := committedAt.Zone()
		if err != nil || offset != 0 || committedAt.Before(started) || committedAt.After(finished) {
			t.Fatalf("ledger committed_at=%q outside [%s,%s]", raw, started, finished)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func captureProblem4WriterLedgerTokens(t *testing.T, db *sql.DB) []string {
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

func installProblem4DeterministicOperationTokens(store *Store, prefix string) {
	index := 0
	store.writerOperationToken = func() (string, error) {
		index++
		return fmt.Sprintf("%s-%d", prefix, index), nil
	}
}

func captureProblem4LatestWriterLedgerRow(t *testing.T, db *sql.DB) (string, string, string) {
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

func assertProblem4ReplayLedgerDelta(t *testing.T, db *sql.DB, before, after problem4DurableSnapshot, expectedToken string, started, finished time.Time) {
	t.Helper()
	if before.SemanticRows != after.SemanticRows || before.PendingState != after.PendingState || before.WakeCount != after.WakeCount {
		t.Fatalf("replay changed semantic/pending/wake state before=%+v after=%+v", before, after)
	}
	if len(after.WriterLedgerRows) != len(before.WriterLedgerRows)+1 || !reflect.DeepEqual(after.WriterLedgerRows[:len(before.WriterLedgerRows)], before.WriterLedgerRows) {
		t.Fatalf("replay changed more than one writer ledger row before=%v after=%v", before.WriterLedgerRows, after.WriterLedgerRows)
	}
	token, operation, committedAt := captureProblem4LatestWriterLedgerRow(t, db)
	if token != expectedToken || operation != writerOperationRecheckOperator || committedAt == "" {
		t.Fatalf("invalid replay writer row token=%q want=%q operation=%q committed_at=%q", token, expectedToken, operation, committedAt)
	}
	committedAtTime, err := time.Parse(time.RFC3339Nano, committedAt)
	if err != nil {
		t.Fatalf("replay writer row has invalid committed_at=%q: %v", committedAt, err)
	}
	_, offset := committedAtTime.Zone()
	if offset != 0 || committedAtTime.Before(started) || committedAtTime.After(finished) {
		t.Fatalf("replay writer row committed_at=%s outside [%s,%s]", committedAtTime, started, finished)
	}
	for _, previousToken := range captureProblem4WriterLedgerTokens(t, db)[:len(before.WriterLedgerRows)] {
		if token == previousToken {
			t.Fatalf("replay reused writer token %q", token)
		}
	}
}

func TestProblem4ImmediateFailureFinalizerCleansPendingWhenStoreEventPanics(t *testing.T) {
	for _, event := range []string{"job_selected", "job_failed"} {
		t.Run(event, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
				if stage == recheckStagePersistCandidate {
					return errors.New("terminal immediate candidate failure")
				}
				return nil
			}}
			var logs bytes.Buffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(problem4PanicEventHandler{base: slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}), event: event}))
			defer slog.SetDefault(previousLogger)
			result, runErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
			if !IsRecheckDurableFailure(runErr) || result.Disposition != RecheckBatchJobFailed || result.Status != "failed" {
				t.Fatalf("immediate terminal result=%+v err=%v", result, runErr)
			}
			if pending := store.pendingRecheckFailureSnapshot(); pending != nil {
				t.Fatalf("%s panic left pending failure: %+v", event, pending)
			}
			var status string
			if err := store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=?`, result.JobID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != "failed" {
				t.Fatalf("durable status=%q", status)
			}
		})
	}
}

func TestProblem4FailureEventPanicMatchesControlAndRecovers(t *testing.T) {
	fixedNow := time.Date(2026, 8, 19, 17, 0, 0, 0, time.UTC)
	baseline, _, _ := seedProblem2OpenJobAt(t, fixedNow)
	if _, err := baseline.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	path := baseline.path
	if err := baseline.Close(); err != nil {
		t.Fatal(err)
	}
	baselineBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	type observation struct {
		result         RecheckBatchResult
		initialResult  RecheckBatchResult
		initialError   string
		progressResult RecheckBatchResult
		failureState   string
		recoveryState  string
		wakes          []string
		phaseSnapshots map[string]problem4DurableSnapshot
		attempts       int
		pending        bool
		failureLogs    string
		failureEvents  []map[string]any
		operatorEvents []map[string]any
		progressEvents []map[string]any
		writerRows     []string
		logs           string
	}
	run := func(event, action string, delayed bool) observation {
		t.Helper()
		clonePath := filepath.Join(t.TempDir(), "history.sqlite")
		if err := os.WriteFile(clonePath, baselineBytes, 0o600); err != nil {
			t.Fatal(err)
		}
		store, err := OpenStore(StoreOptions{Path: clonePath})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = store.Close() }()
		seedProblem4QueuedSuccessor(t, store, fixedNow)
		operationTokenIndex := 0
		store.writerOperationToken = func() (string, error) {
			operationTokenIndex++
			return fmt.Sprintf("review7-operation-token-%d", operationTokenIndex), nil
		}
		store.lifecycleTestHooks = &storeLifecycleTestHooks{
			recheckNow:           func() time.Time { return fixedNow },
			recheckBatchDuration: func(time.Duration) time.Duration { return time.Nanosecond },
			recheckFault: func(stage string) error {
				if stage == recheckStagePersistCandidate {
					return errors.New("terminal panic matrix failure")
				}
				return nil
			},
		}
		if delayed {
			store.writerCommit = func(tx *sql.Tx) error {
				if commitErr := tx.Commit(); commitErr != nil {
					return commitErr
				}
				return review2SQLiteError{code: 5, message: "panic matrix delayed commit"}
			}
			store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return false, context.DeadlineExceeded }
		}
		var logs bytes.Buffer
		handler := slog.Handler(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		if event != "" {
			handler = problem4PanicEventHandler{base: handler, event: event, once: &sync.Once{}}
		}
		previousLogger := slog.Default()
		slog.SetDefault(slog.New(handler))
		defer slog.SetDefault(previousLogger)
		var wakes []string
		phase := "failure"
		store.SetRecheckCommitWake(func() { wakes = append(wakes, phase) })
		result, runErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
		initialResult := result
		initialError := fmt.Sprintf("%T:%v", runErr, runErr)
		if delayed {
			if !errors.Is(runErr, ErrWriterCommitOutcomeUnknown) {
				t.Fatalf("initial delayed error=%v", runErr)
			}
			if premature := problem4ParsedEvents(t, logs.String(), "job_selected", "job_failed", "operator_action_applied"); len(premature) != 0 {
				t.Fatalf("delayed commit emitted premature contract events: %v", premature)
			}
			store.writerCommit, store.writerCommitLookup = nil, nil
			result, runErr = store.RunSilenceRecheckBatchContext(context.Background(), 100)
		}
		if !IsRecheckDurableFailure(runErr) || result.Status != "failed" {
			t.Fatalf("failure result=%+v err=%v", result, runErr)
		}
		phase = "after_failure_finalization"
		normalizedState := func() string {
			t.Helper()
			state := captureProblem4RecheckWriteSet(t, store.db)
			rows, queryErr := store.db.Query(`SELECT attempt_token FROM silence_recheck_job_attempts WHERE job_id=1 ORDER BY attempt_number`)
			if queryErr != nil {
				t.Fatal(queryErr)
			}
			defer rows.Close()
			index := 0
			for rows.Next() {
				var token string
				if err = rows.Scan(&token); err != nil {
					t.Fatal(err)
				}
				index++
				state = strings.ReplaceAll(state, token, fmt.Sprintf("<attempt-token-%d>", index))
			}
			return state
		}
		failureState := normalizedState()
		phaseSnapshots := map[string]problem4DurableSnapshot{
			"failure_finalized": captureProblem4DurableSnapshot(t, store, len(wakes)),
		}
		failureLogs := logs.String()
		failureEvents := problem4ParsedEvents(t, failureLogs, "job_selected", "job_failed")
		if event != "" {
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		}
		store.lifecycleTestHooks = &storeLifecycleTestHooks{
			recheckNow:           func() time.Time { return fixedNow },
			recheckBatchDuration: func(time.Duration) time.Duration { return time.Nanosecond },
		}
		actor := "review7-operator"
		requestID := "review7-" + action
		if action == "retry" {
			_, err = store.RetryRecheckJobContext(context.Background(), 1, requestID, "recover", &actor, fixedNow.Add(time.Second))
		} else {
			_, err = store.AbandonRecheckJobContext(context.Background(), 1, requestID, "operator decision", &actor, fixedNow.Add(time.Second))
		}
		if err != nil {
			t.Fatal(err)
		}
		phase = "operator_finalization"
		phaseSnapshots["operator_finalized"] = captureProblem4DurableSnapshot(t, store, len(wakes))
		operatorEvents := problem4ParsedEvents(t, logs.String(), "operator_action_applied")
		logs.Reset()
		phase = "scheduler_progress"
		progress, progressErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
		if action == "retry" && (progressErr != nil || progress.Status != "completed") {
			t.Fatalf("retry progress=%+v err=%v", progress, progressErr)
		}
		if action == "abandon" && (progressErr != nil || progress.JobID != 2 || progress.Status != "completed" || progress.Disposition != RecheckBatchCommitted || progress.ProcessedCandidates != 1 || progress.OutcomeCounts.Ineligible != 1) {
			t.Fatalf("abandon successor progress=%+v err=%v", progress, progressErr)
		}
		progressEvents := problem4ParsedEvents(t, logs.String(), "job_selected", "job_started", "job_resumed", "batch_completed", "job_completed", "job_failed", "operator_action_applied")
		phaseSnapshots["scheduler_progress"] = captureProblem4DurableSnapshot(t, store, len(wakes))
		recoveryState := normalizedState()
		writerRows := captureProblem4WriterLedgerRows(t, store.db)
		ledgerRows, ledgerErr := store.db.Query(`SELECT operation_token,committed_at FROM lifecycle_writer_commits ORDER BY rowid`)
		if ledgerErr != nil {
			t.Fatal(ledgerErr)
		}
		ledgerIndex := 0
		for ledgerRows.Next() {
			var token, committedAt string
			if ledgerErr = ledgerRows.Scan(&token, &committedAt); ledgerErr != nil {
				ledgerRows.Close()
				t.Fatal(ledgerErr)
			}
			for rowIndex := range writerRows {
				writerRows[rowIndex] = strings.ReplaceAll(strings.ReplaceAll(writerRows[rowIndex], token, fmt.Sprintf("<writer-token-%d>", ledgerIndex)), committedAt, fmt.Sprintf("<committed-at-%d>", ledgerIndex))
			}
			ledgerIndex++
		}
		ledgerRows.Close()
		var attempts int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		return observation{result: result, initialResult: initialResult, initialError: initialError, progressResult: progress, failureState: failureState, recoveryState: recoveryState, wakes: append([]string(nil), wakes...), phaseSnapshots: phaseSnapshots, attempts: attempts, pending: store.pendingRecheckFailureSnapshot() != nil || store.pendingRecheckOperatorCompletion() != nil, failureLogs: failureLogs, failureEvents: failureEvents, operatorEvents: operatorEvents, progressEvents: progressEvents, writerRows: writerRows, logs: logs.String()}
	}
	for _, delayed := range []bool{false, true} {
		for _, action := range []string{"retry", "abandon"} {
			control := run("", action, delayed)
			expectedControlEvents := problem4ExpectedTerminalOperatorEvents(action, "review7-"+action, "review7-operator")
			expectedControlEvents[0]["queue_depth"] = float64(2)
			if !reflect.DeepEqual(control.failureEvents, expectedControlEvents[:2]) || !reflect.DeepEqual(control.operatorEvents, expectedControlEvents[2:]) {
				t.Fatalf("control event sequence delayed=%t action=%s failure=%v operator=%v", delayed, action, control.failureEvents, control.operatorEvents)
			}
			if action == "abandon" && (control.progressResult.JobID != 2 || control.progressResult.Status != "completed" || control.progressResult.Disposition != RecheckBatchCommitted || control.progressResult.ProcessedCandidates != 1 || control.progressResult.OutcomeCounts.Ineligible != 1) {
				t.Fatalf("abandon did not execute FIFO successor: job=%d status=%q disposition=%q processed=%d ineligible=%d result=%+v", control.progressResult.JobID, control.progressResult.Status, control.progressResult.Disposition, control.progressResult.ProcessedCandidates, control.progressResult.OutcomeCounts.Ineligible, control.progressResult)
			}
			if action == "retry" && len(control.progressEvents) == 0 {
				t.Fatalf("retry emitted no progress events")
			}
			if len(control.wakes) == 0 || control.wakes[0] == "failure" {
				t.Fatalf("wake was not phase-attributed after failure: %v", control.wakes)
			}
			expectedWakePhases := []string{"after_failure_finalization"}
			if !reflect.DeepEqual(control.wakes, expectedWakePhases) {
				t.Fatalf("unexpected wake sequence delayed=%t action=%s: got=%v want=%v", delayed, action, control.wakes, expectedWakePhases)
			}
			for _, event := range []string{"job_selected", "job_failed"} {
				t.Run(fmt.Sprintf("delayed=%t/%s/%s", delayed, action, event), func(t *testing.T) {
					fault := run(event, action, delayed)
					expectedFailureEvents := expectedControlEvents[:2]
					if event == "job_selected" {
						expectedFailureEvents = nil
					} else {
						expectedFailureEvents = expectedFailureEvents[:1]
					}
					if !reflect.DeepEqual(fault.initialResult, control.initialResult) || fault.initialError != control.initialError || !reflect.DeepEqual(fault.result, control.result) || !reflect.DeepEqual(fault.progressResult, control.progressResult) || fault.failureState != control.failureState || fault.recoveryState != control.recoveryState || !reflect.DeepEqual(fault.phaseSnapshots, control.phaseSnapshots) || !reflect.DeepEqual(fault.writerRows, control.writerRows) || !reflect.DeepEqual(fault.wakes, control.wakes) || !reflect.DeepEqual(fault.failureEvents, expectedFailureEvents) || !reflect.DeepEqual(fault.operatorEvents, control.operatorEvents) || !reflect.DeepEqual(fault.progressEvents, control.progressEvents) || fault.attempts != control.attempts || fault.pending {
						t.Fatalf("panic changed outcome event=%s result=%+v/%+v wakes=%v/%v attempts=%d/%d pending=%v failure_events=%v/%v operator=%v/%v progress=%v/%v\ncontrol=%s\nfault=%s", event, fault.result, control.result, fault.wakes, control.wakes, fault.attempts, control.attempts, fault.pending, fault.failureEvents, expectedFailureEvents, fault.operatorEvents, control.operatorEvents, fault.progressEvents, control.progressEvents, control.logs, fault.logs)
					}
				})
			}
		}
	}
}

func TestProblem4ImmediateFailureFinalizationOrdersWaitingOperator(t *testing.T) {
	for _, action := range []string{"retry", "abandon"} {
		t.Run(action, func(t *testing.T) {
			matrixStarted := time.Now().UTC()
			store, _, seededAt := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			installProblem4DeterministicOperationTokens(store, "review7-immediate-token")
			fixedNow := time.Date(2026, 8, 19, 16, 0, 0, 0, time.UTC)
			failureAtCommit := make(chan struct{})
			releaseFailureCommit := make(chan struct{})
			operatorAfterPrecheck := make(chan struct{})
			failureFinalizerEntered := make(chan struct{})
			releaseFailureFinalizer := make(chan struct{})
			operatorInsideBarrier := make(chan struct{})
			releaseOperatorInside := make(chan struct{})
			var releaseOperatorInsideOnce sync.Once
			t.Cleanup(func() { releaseOperatorInsideOnce.Do(func() { close(releaseOperatorInside) }) })
			operatorFinalizerEntered := make(chan struct{})
			releaseOperatorFinalizer := make(chan struct{})
			var commitOnce, precheckOnce, finalizerOnce, insideOnce, operatorFinalizerOnce sync.Once
			store.lifecycleTestHooks = &storeLifecycleTestHooks{
				recheckNow: func() time.Time { return fixedNow },
				recheckFault: func(stage string) error {
					if stage == recheckStagePersistCandidate {
						return errors.New("immediate terminal race")
					}
					return nil
				},
				recheckFailureFinalizer: func() {
					finalizerOnce.Do(func() { close(failureFinalizerEntered) })
					<-releaseFailureFinalizer
				},
				recheckOperatorBarrier: func(stage string) {
					switch stage {
					case "after_precheck":
						precheckOnce.Do(func() { close(operatorAfterPrecheck) })
					case "inside_transaction":
						insideOnce.Do(func() { close(operatorInsideBarrier) })
						<-releaseOperatorInside
					}
				},
				recheckOperatorFinalizer: func() {
					operatorFinalizerOnce.Do(func() { close(operatorFinalizerEntered) })
					<-releaseOperatorFinalizer
				},
			}
			store.beforeCommitLedgerInsert = func(_ context.Context, operation string) {
				if operation != writerOperationRecheckFailure {
					return
				}
				commitOnce.Do(func() { close(failureAtCommit) })
				<-releaseFailureCommit
			}
			var logs problem4SynchronizedBuffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			defer slog.SetDefault(previousLogger)
			wakeCalls := 0
			store.SetRecheckCommitWake(func() { wakeCalls++ })
			initialSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			operatorAt := time.Date(2026, 8, 19, 16, 0, 0, 0, time.UTC)
			requestID := "review5-immediate-" + action
			applyAction := func() (RecheckOperatorActionResult, error) {
				actor := "reviewer"
				if action == "retry" {
					return store.RetryRecheckJobContext(context.Background(), 1, requestID, "retry", &actor, operatorAt)
				}
				return store.AbandonRecheckJobContext(context.Background(), 1, requestID, "abandon", &actor, operatorAt)
			}
			workerResult := make(chan struct {
				result RecheckBatchResult
				err    error
			}, 1)
			go func() {
				result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
				workerResult <- struct {
					result RecheckBatchResult
					err    error
				}{result: result, err: err}
			}()
			<-failureAtCommit
			operatorResult := make(chan struct {
				result RecheckOperatorActionResult
				err    error
			}, 1)
			go func() {
				result, err := applyAction()
				operatorResult <- struct {
					result RecheckOperatorActionResult
					err    error
				}{result: result, err: err}
			}()
			<-operatorAfterPrecheck
			close(releaseFailureCommit)
			<-failureFinalizerEntered
			<-operatorInsideBarrier
			var actions int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions WHERE job_id=1`).Scan(&actions); err != nil {
				t.Fatal(err)
			}
			if actions != 0 || strings.Contains(logs.String(), `"event":"operator_action_applied"`) {
				t.Fatalf("operator overtook immediate failure finalization: actions=%d logs=%s", actions, logs.String())
			}
			failureCommittedSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			assertProblem4LedgerSnapshotTransition(t, initialSnapshot, failureCommittedSnapshot, []string{"recheck_failure"})
			assertProblem4WriterLedgerSequence(t, store.db, "review7-immediate-token", []problem4ExpectedWriterLedgerRow{{"review7-immediate-token-3", "recheck_failure"}}, matrixStarted, time.Now().UTC())
			var failureAttemptCount int
			var failureAttemptToken string
			if err := store.db.QueryRow(`SELECT COUNT(*),MIN(attempt_token) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&failureAttemptCount, &failureAttemptToken); err != nil {
				t.Fatal(err)
			}
			assertProblem4PhaseSnapshot(t, store, failureCommittedSnapshot, expectedProblem4FailurePendingState(seededAt, fixedNow, "review7-immediate-token-2", "persist_candidate: immediate terminal race", true), "failed", 1, true, false, true, "review7-immediate-token-2", "terminal", "persist_candidate", "recheck_failure", "", 0)
			assertProblem4ExpectedTableTransition(t, initialSnapshot, failureCommittedSnapshot, "silence_recheck_job_rules", nil)
			assertProblem4ExpectedTableTransition(t, initialSnapshot, failureCommittedSnapshot, "silence_recheck_job_candidates", nil)
			assertProblem4ExpectedTableTransition(t, initialSnapshot, failureCommittedSnapshot, "silence_recheck_jobs", map[string]string{
				"status":              "failed",
				"attempt_count":       "1",
				"first_error_message": "persist_candidate: immediate terminal race",
				"first_error_class":   "terminal",
				"first_error_stage":   "persist_candidate",
				"first_error_at":      fixedNow.Format(time.RFC3339Nano),
				"last_error_message":  "persist_candidate: immediate terminal race",
				"last_error_class":    "terminal",
				"last_error_stage":    "persist_candidate",
				"last_error_at":       fixedNow.Format(time.RFC3339Nano),
				"started_at":          fixedNow.Format(time.RFC3339Nano),
				"finished_at":         fixedNow.Format(time.RFC3339Nano),
			})
			var failurePending problem4PendingState
			if err := json.Unmarshal([]byte(failureCommittedSnapshot.PendingState), &failurePending); err != nil {
				t.Fatal(err)
			}
			assertProblem4ExpectedAttemptTransition(t, initialSnapshot, failureCommittedSnapshot, "review7-immediate-token-2", "terminal", "persist_candidate", fixedNow, fixedNow)
			if failureAttemptCount != 1 || failureAttemptToken != "review7-immediate-token-2" {
				t.Fatalf("incomplete failure attempt phase count=%d token=%q", failureAttemptCount, failureAttemptToken)
			}
			close(releaseFailureFinalizer)
			worker := <-workerResult
			if !IsRecheckDurableFailure(worker.err) || worker.result.Status != "failed" {
				t.Fatalf("worker result=%+v err=%v", worker.result, worker.err)
			}
			if !reflect.DeepEqual(failurePending.FailureResult, worker.result) {
				t.Fatalf("retained pending result differs from returned durable result: pending=%+v worker=%+v", failurePending.FailureResult, worker.result)
			}
			expectedFailureResult := expectedProblem4TerminalFailureResult(1, "source_context_changed", seededAt, fixedNow, "persist_candidate: immediate terminal race")
			if !reflect.DeepEqual(worker.result, expectedFailureResult) || !reflect.DeepEqual(failurePending.FailureResult, expectedFailureResult) {
				t.Fatalf("failure result differs from independent expected result: pending=%+v worker=%+v expected=%+v", failurePending.FailureResult, worker.result, expectedFailureResult)
			}
			failureFinalizedSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			assertProblem4PhaseSnapshot(t, store, failureFinalizedSnapshot, problem4PendingState{}, "failed", 1, false, false, false, "", "", "", "", "", 0)
			if !reflect.DeepEqual(failureCommittedSnapshot.AttemptRows, failureFinalizedSnapshot.AttemptRows) {
				t.Fatalf("failure finalization changed attempt rows: before=%v after=%v", failureCommittedSnapshot.AttemptRows, failureFinalizedSnapshot.AttemptRows)
			}
			if !reflect.DeepEqual(failureCommittedSnapshot.SemanticTables, failureFinalizedSnapshot.SemanticTables) || !reflect.DeepEqual(failureCommittedSnapshot.WriterLedgerRows, failureFinalizedSnapshot.WriterLedgerRows) {
				t.Fatalf("failure finalization changed durable tables or writer ledger")
			}
			assertProblem4LedgerSnapshotTransition(t, failureCommittedSnapshot, failureFinalizedSnapshot, nil)
			assertProblem4WriterLedgerSequence(t, store.db, "review7-immediate-token", []problem4ExpectedWriterLedgerRow{{"review7-immediate-token-3", "recheck_failure"}}, matrixStarted, time.Now().UTC())
			assertProblem4ExpectedTableTransition(t, failureCommittedSnapshot, failureFinalizedSnapshot, "silence_recheck_job_rules", nil)
			assertProblem4ExpectedTableTransition(t, failureCommittedSnapshot, failureFinalizedSnapshot, "silence_recheck_job_candidates", nil)
			releaseOperatorInsideOnce.Do(func() { close(releaseOperatorInside) })
			<-operatorFinalizerEntered
			operatorCommittedSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			assertProblem4PhaseSnapshot(t, store, operatorCommittedSnapshot, expectedProblem4OperatorPendingState(requestID), map[string]string{"retry": "queued", "abandon": "abandoned"}[action], 1, false, true, false, "", "", "", "", requestID, 0)
			if !reflect.DeepEqual(failureCommittedSnapshot.AttemptRows, operatorCommittedSnapshot.AttemptRows) {
				t.Fatalf("operator commit changed attempt rows: before=%v after=%v", failureCommittedSnapshot.AttemptRows, operatorCommittedSnapshot.AttemptRows)
			}
			assertProblem4LedgerSnapshotTransition(t, failureCommittedSnapshot, operatorCommittedSnapshot, []string{"recheck_operator_action"})
			assertProblem4WriterLedgerSequence(t, store.db, "review7-immediate-token", []problem4ExpectedWriterLedgerRow{{"review7-immediate-token-3", "recheck_failure"}, {"review7-immediate-token-5", "recheck_operator_action"}}, matrixStarted, time.Now().UTC())
			assertProblem4ExpectedTableTransition(t, failureCommittedSnapshot, operatorCommittedSnapshot, "silence_recheck_job_rules", nil)
			assertProblem4ExpectedTableTransition(t, failureCommittedSnapshot, operatorCommittedSnapshot, "silence_recheck_job_candidates", nil)
			operatorJobChanges := map[string]string{"status": map[string]string{"retry": "queued", "abandon": "abandoned"}[action]}
			if action == "retry" {
				operatorJobChanges["recovery_count"] = "1"
				operatorJobChanges["finished_at"] = "<NULL>"
			} else {
				operatorJobChanges["abandoned_at"] = operatorAt.Format(time.RFC3339Nano)
				operatorJobChanges["abandoned_by"] = "reviewer"
				operatorJobChanges["abandon_reason"] = "abandon"
				operatorJobChanges["finished_at"] = operatorAt.Format(time.RFC3339Nano)
			}
			assertProblem4ExpectedTableTransition(t, failureCommittedSnapshot, operatorCommittedSnapshot, "silence_recheck_jobs", operatorJobChanges)
			assertProblem4ExpectedOperatorActionTransition(t, failureCommittedSnapshot, operatorCommittedSnapshot, action, requestID, "reviewer", action, map[string]string{"retry": "queued", "abandon": "abandoned"}[action], operatorAt)
			if strings.Contains(logs.String(), `"event":"operator_action_applied"`) {
				t.Fatalf("operator event appeared before finalizer release: %s", logs.String())
			}
			close(releaseOperatorFinalizer)
			operator := <-operatorResult
			if operator.err != nil || !operator.result.Applied || operator.result.RequestID != requestID {
				t.Fatalf("operator result=%+v err=%v", operator.result, operator.err)
			}
			if pending := store.pendingRecheckFailureSnapshot(); pending != nil {
				t.Fatalf("pending failure remained: %+v", pending)
			}
			contractEvents := problem4ParsedEvents(t, logs.String(), "job_selected", "job_failed", "operator_action_applied")
			if !reflect.DeepEqual(contractEvents, problem4ExpectedTerminalOperatorEvents(action, requestID, "reviewer")) || wakeCalls != 1 || store.pendingRecheckOperatorCompletion() != nil {
				t.Fatalf("exact immediate records=%v wakes=%d pending=%+v", contractEvents, wakeCalls, store.pendingRecheckOperatorCompletion())
			}
			finalizedSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			assertProblem4PhaseSnapshot(t, store, finalizedSnapshot, problem4PendingState{}, map[string]string{"retry": "queued", "abandon": "abandoned"}[action], 1, false, false, false, "", "", "", "", "", 1)
			assertProblem4WriterLedgerSequence(t, store.db, "review7-immediate-token", []problem4ExpectedWriterLedgerRow{{"review7-immediate-token-3", "recheck_failure"}, {"review7-immediate-token-5", "recheck_operator_action"}}, matrixStarted, time.Now().UTC())
			beforeReplay := finalizedSnapshot
			attemptsBeforeReplay := worker.result.AttemptCount
			beforeReplayEvents := problem4AllStructuredEvents(t, logs.String())
			replayStarted := time.Now().UTC()
			replay, replayErr := applyAction()
			replayFinished := time.Now().UTC()
			afterReplay := captureProblem4DurableSnapshot(t, store, wakeCalls)
			afterReplayEvents := problem4AllStructuredEvents(t, logs.String())
			if replayErr != nil || !reflect.DeepEqual(replay, operator.result) || !reflect.DeepEqual(afterReplayEvents, beforeReplayEvents) || wakeCalls != 1 {
				t.Fatalf("replay result=%+v/%+v err=%v wakes=%d logs=%s", replay, operator.result, replayErr, wakeCalls, logs.String())
			}
			var attemptsAfterReplay int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attemptsAfterReplay); err != nil {
				t.Fatal(err)
			}
			if attemptsAfterReplay != attemptsBeforeReplay || strings.Count(beforeReplay.SemanticRows, requestID) != 1 || strings.Count(afterReplay.SemanticRows, requestID) != 1 {
				t.Fatalf("replay duplicated durable state attempts=%d/%d before=%s after=%s", attemptsAfterReplay, attemptsBeforeReplay, beforeReplay.SemanticRows, afterReplay.SemanticRows)
			}
			assertProblem4ReplayLedgerDelta(t, store.db, beforeReplay, afterReplay, "review7-immediate-token-6", replayStarted, replayFinished)
			assertProblem4LedgerSnapshotTransition(t, finalizedSnapshot, afterReplay, []string{"recheck_operator_action"})
			assertProblem4WriterLedgerSequence(t, store.db, "review7-immediate-token", []problem4ExpectedWriterLedgerRow{{"review7-immediate-token-3", "recheck_failure"}, {"review7-immediate-token-5", "recheck_operator_action"}, {"review7-immediate-token-6", "recheck_operator_action"}}, matrixStarted, time.Now().UTC())
			store.lifecycleTestHooks = nil
			store.beforeCommitLedgerInsert = nil
			progress, progressErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
			if action == "retry" && (progressErr != nil || progress.Status != "completed") {
				t.Fatalf("retry progress=%+v err=%v", progress, progressErr)
			}
			if action == "abandon" && (progressErr != nil || progress.Disposition != RecheckBatchQueueEmpty) {
				t.Fatalf("abandon progress=%+v err=%v", progress, progressErr)
			}
			assertProblem4LedgerRowsBounded(t, store.db, matrixStarted, time.Now().UTC())
		})
	}
}

func TestProblem4RegroupEventFailureDoesNotChangeTransition(t *testing.T) {
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	fixedNow := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	run := func(fault bool) (RegroupResult, string) {
		t.Helper()
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "regroup-alert", State: StateInactive, RawState: StateActive, Receiver: "team", GroupID: "old", GroupLabelsJSON: `{"alertname":"High","instance":"host"}`, LabelsJSON: `{"alertname":"High","instance":"host"}`, AnnotationsJSON: `{}`, StartsAt: fixedNow.Add(-time.Hour), LastSeenAt: &fixedNow, RelevantUpstreams: `[]`, RelevantSources: `[]`, SourceLinksJSON: `[]`, UpdatedAt: fixedNow}); err != nil {
			t.Fatal(err)
		}
		if _, err = store.db.Exec(`UPDATE alert_states SET updated_at=? WHERE alert_id='regroup-alert'`, fixedNow.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		store.lifecycleTestHooks = &storeLifecycleTestHooks{regroupNow: func() time.Time { return fixedNow }}
		var logs bytes.Buffer
		handler := slog.Handler(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		if fault {
			handler = problem4PanicEventHandler{base: handler, event: "regroup_batch_completed"}
		}
		slog.SetDefault(slog.New(handler))
		result, err := store.RunRegroupToConfig(context.Background(), GroupingConfig{LabelsStrip: []string{"instance"}}, 100)
		if err != nil {
			t.Fatal(err)
		}
		return result, captureProblem3Review2DomainState(t, store.db)
	}
	controlResult, controlState := run(false)
	faultResult, faultState := run(true)
	if !reflect.DeepEqual(faultResult, controlResult) || faultState != controlState {
		t.Fatalf("regroup event failure changed result/state: %+v/%+v\n%s\n%s", faultResult, controlResult, faultState, controlState)
	}
}

func TestProblem4RegroupFailureEventDoesNotChangePersistedFailureState(t *testing.T) {
	previousLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	run := func(fault bool) (string, string, string) {
		t.Helper()
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		target := BuildGroupingFingerprint(GroupingConfig{LabelsKeep: []string{"alertname"}})
		for key, value := range map[string]string{metaRegroupStatus: regroupStatusRunning, metaRegroupTargetFingerprint: target, metaRegroupProcessed: "2", metaRegroupTotal: "4"} {
			if _, err = store.db.Exec(`INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value); err != nil {
				t.Fatal(err)
			}
		}
		var logs bytes.Buffer
		handler := slog.Handler(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		if fault {
			handler = problem4PanicEventHandler{base: handler, event: "regroup_failed"}
		}
		slog.SetDefault(slog.New(handler))
		domainErr := errors.New("controlled regroup domain failure")
		finished := make(chan struct{}, 1)
		store.writeMu.Lock()
		store.writerCommit = func(tx *sql.Tx) error {
			if commitErr := tx.Commit(); commitErr != nil {
				return commitErr
			}
			return review2SQLiteError{code: 5, message: "delayed regroup failure"}
		}
		store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return false, context.DeadlineExceeded }
		store.writerTransactionFinished = func(operation string) {
			if operation == writerOperationRegroupFailure {
				select {
				case finished <- struct{}{}:
				default:
				}
			}
		}
		store.writeMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		persisted := make(chan error, 1)
		go func() {
			persisted <- store.persistRegroupFailure(ctx, target, RegroupResult{Processed: 2, Total: 4}, domainErr)
		}()
		<-finished
		store.writeMu.Lock()
		store.writerCommit = nil
		store.writerCommitLookup = nil
		store.writeMu.Unlock()
		if reconcileErr := store.failRegroup(context.Background(), target, "must not replace"); !errors.Is(reconcileErr, ErrWriterCommitReconciled) {
			t.Fatalf("regroup failure reconciliation=%v", reconcileErr)
		}
		persistErr := <-persisted
		if !errors.Is(persistErr, ErrRegroupFailurePersisted) || !errors.Is(persistErr, domainErr) {
			t.Fatalf("persisted regroup error=%v", persistErr)
		}
		var status, reason, processed, total string
		for key, destination := range map[string]*string{metaRegroupStatus: &status, metaRegroupError: &reason, metaRegroupProcessed: &processed, metaRegroupTotal: &total} {
			if err = store.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, key).Scan(destination); err != nil {
				t.Fatal(err)
			}
		}
		var ledgerRows int
		if err = store.db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind=?`, writerOperationRegroupFailure).Scan(&ledgerRows); err != nil {
			t.Fatal(err)
		}
		if ledgerRows != 1 {
			t.Fatalf("regroup failure ledger rows=%d", ledgerRows)
		}
		return persistErr.Error(), strings.Join([]string{status, reason, processed, total}, "|"), logs.String()
	}
	controlErr, controlState, controlLogs := run(false)
	faultErr, faultState, faultLogs := run(true)
	if faultErr != controlErr || faultState != controlState {
		t.Fatalf("regroup failure event changed result/state: %q/%q %q/%q", faultErr, controlErr, faultState, controlState)
	}
	if strings.Count(controlLogs, `"event":"regroup_failed"`) != 1 || strings.Count(faultLogs, `"event":"regroup_failed"`) != 0 {
		t.Fatalf("regroup failure event ownership control=%s fault=%s", controlLogs, faultLogs)
	}
}

func TestProblem4RecheckWorkerReturnsTypedQueueBlockedResult(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err = store.db.Exec(`
INSERT INTO silence_recheck_jobs(
 status, created_at, started_at, finished_at, source_context_json,
 source_context_fingerprint, job_fingerprint, trigger_type,
 first_error_message, last_error_message
) VALUES('failed', ?, ?, ?, '{"formatVersion":1,"sources":[],"clusters":[]}',
 'problem4-failed', 'sha256:problem4-failed', 'source_context_changed',
 'root failure', 'root failure')`, now, now, now); err != nil {
		t.Fatal(err)
	}

	result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if err != nil {
		t.Fatalf("durable queue state was returned as an execution error: %v", err)
	}
	if result.Disposition != RecheckBatchQueueBlocked {
		t.Fatalf("expected typed queue-blocked disposition, got=%q", result.Disposition)
	}
	if result.JobID == 0 || result.Status != "failed" {
		t.Fatalf("expected committed failed-head metadata, got=%+v", result)
	}
}

func TestProblem4SelectedResultUsesDurablePostTransitionCounters(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET retry_count=2, consecutive_retry_count=3 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.AttemptCount != 1 || result.RetryCount != 2 || result.ConsecutiveRetryCount != 0 {
		t.Fatalf("typed result exposed pre-transition metadata: %+v", result)
	}
}

func TestProblem4WorkerResultDoesNotDependOnPostCommitReread(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if err != nil || result.Disposition != RecheckBatchCommitted || result.Status != "completed" {
		t.Fatalf("worker result lost committed transaction payload: result=%+v err=%v", result, err)
	}
	if result.QueueDepth == nil || *result.QueueDepth != 0 || result.AttemptCount != 1 || result.ProcessedCandidates != result.TotalCandidates {
		t.Fatalf("worker result did not expose one committed snapshot: %+v", result)
	}
}

func TestProblem4DelayedCommittedBatchReturnsDurableResultWithoutFalseFailurePersistence(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.writerCommit = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return review2SQLiteError{code: 5, message: "delayed committed batch"}
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		return false, context.DeadlineExceeded
	}
	first, firstErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if !errors.Is(firstErr, ErrWriterCommitOutcomeUnknown) || first.Disposition != RecheckBatchCommitOutcomeUnknown {
		t.Fatalf("initial delayed batch result=%+v err=%v", first, firstErr)
	}
	if store.pendingRecheckFailureSnapshot() == nil {
		t.Fatal("delayed batch did not retain the pending attempt")
	}
	expected, err := store.readRecheckJobResult(context.Background(), first.JobID, true)
	if err != nil {
		t.Fatal(err)
	}
	readCalls := 0
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckResultReadFault: func() error {
		readCalls++
		return errors.New("post-commit result reader unavailable")
	}}
	logs.Reset()
	store.writerCommit = nil
	store.writerCommitLookup = nil
	second, secondErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if secondErr != nil || second.Disposition != RecheckBatchCommitted || second.Status != "completed" {
		t.Fatalf("reconciled batch result=%+v err=%v", second, secondErr)
	}
	expected.Disposition = second.Disposition
	expected.ReconciledOperation = second.ReconciledOperation
	if !reflect.DeepEqual(second, expected) {
		t.Fatalf("reconciled result differs from durable snapshot:\nresult=%+v\ndurable=%+v", second, expected)
	}
	if store.pendingRecheckFailureSnapshot() != nil {
		t.Fatal("reconciled batch left a pending attempt")
	}
	if readCalls != 0 {
		t.Fatalf("confirmed batch reconciliation called fallible result reader %d times", readCalls)
	}
	if strings.Contains(logs.String(), `"event":"failure_persistence_failed"`) {
		t.Fatalf("reconciled batch emitted false failure persistence telemetry: %s", logs.String())
	}
	if strings.Count(logs.String(), `"event":"batch_completed"`) != 1 || strings.Count(logs.String(), `"event":"job_completed"`) != 1 {
		t.Fatalf("reconciled batch event counts=%s", logs.String())
	}
}

func TestProblem4DelayedCommittedFailureReturnsDurableRetryResult(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return review2SQLiteError{code: 5, message: "delayed failure bookkeeping"}
		}
		return nil
	}}
	store.writerCommit = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return review2SQLiteError{code: 5, message: "delayed committed failure"}
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		return false, context.DeadlineExceeded
	}
	first, firstErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if !errors.Is(firstErr, ErrWriterCommitOutcomeUnknown) || first.Disposition != RecheckBatchFailurePersistenceFailed {
		t.Fatalf("initial delayed failure result=%+v err=%v", first, firstErr)
	}
	if store.pendingRecheckFailureSnapshot() == nil {
		t.Fatal("delayed failure did not retain the pending attempt")
	}
	logs.Reset()
	readCalls := 0
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckResultReadFault: func() error {
		readCalls++
		return errors.New("post-commit retry result reader unavailable")
	}}
	store.writerCommit = nil
	store.writerCommitLookup = nil
	second, secondErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	var retryErr *recheckRetryScheduledError
	if secondErr == nil || !errors.As(secondErr, &retryErr) || second.Disposition != RecheckBatchRetryScheduled || second.Status != "retry_wait" {
		t.Fatalf("reconciled failure result=%+v err=%v", second, secondErr)
	}
	if second.NextAttemptAt == nil || second.AttemptCount != 1 || store.pendingRecheckFailureSnapshot() != nil {
		t.Fatalf("reconciled retry snapshot=%+v pending=%+v", second, store.pendingRecheckFailureSnapshot())
	}
	if readCalls != 0 {
		t.Fatalf("confirmed retry reconciliation called fallible result reader %d times", readCalls)
	}
	if strings.Contains(logs.String(), `"event":"failure_persistence_failed"`) || strings.Contains(logs.String(), `"event":"job_failed"`) {
		t.Fatalf("reconciled retry emitted terminal telemetry: %s", logs.String())
	}
}

func TestProblem4DelayedTerminalFailureUsesRetainedResultWhenReaderFails(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("terminal candidate failure")
		}
		return nil
	}}
	store.writerCommit = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return review2SQLiteError{code: 5, message: "delayed terminal bookkeeping"}
	}
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		return false, context.DeadlineExceeded
	}
	first, firstErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if !errors.Is(firstErr, ErrWriterCommitOutcomeUnknown) || first.Disposition != RecheckBatchFailurePersistenceFailed {
		t.Fatalf("initial terminal result=%+v err=%v", first, firstErr)
	}
	readCalls := 0
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckResultReadFault: func() error {
		readCalls++
		return errors.New("post-commit terminal result reader unavailable")
	}}
	store.writerCommit = nil
	store.writerCommitLookup = nil
	logs.Reset()
	result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if !IsRecheckDurableFailure(err) || result.Disposition != RecheckBatchJobFailed || result.Status != "failed" || result.AttemptCount != 1 {
		t.Fatalf("terminal reconciliation result=%+v err=%v", result, err)
	}
	if readCalls != 0 || store.pendingRecheckFailureSnapshot() != nil {
		t.Fatalf("terminal reconciliation reads=%d pending=%+v", readCalls, store.pendingRecheckFailureSnapshot())
	}
	if strings.Count(logs.String(), `"event":"job_selected"`) != 1 || strings.Count(logs.String(), `"event":"job_failed"`) != 1 || strings.Contains(logs.String(), `"event":"failure_persistence_failed"`) {
		t.Fatalf("terminal reconciliation events=%s", logs.String())
	}
}

func TestProblem4DelayedTerminalFailureFinalizerCleansPendingWhenStoreEventPanics(t *testing.T) {
	for _, event := range []string{"job_selected", "job_failed"} {
		t.Run(event, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
				if stage == recheckStagePersistCandidate {
					return errors.New("terminal delayed event failure")
				}
				return nil
			}}
			store.writerCommit = func(tx *sql.Tx) error {
				if err := tx.Commit(); err != nil {
					return err
				}
				return review2SQLiteError{code: 5, message: "delayed terminal event commit"}
			}
			store.writerCommitLookup = func(context.Context, string, string) (bool, error) { return false, context.DeadlineExceeded }
			if first, err := store.RunSilenceRecheckBatchContext(context.Background(), 100); !errors.Is(err, ErrWriterCommitOutcomeUnknown) || first.Disposition != RecheckBatchFailurePersistenceFailed {
				t.Fatalf("initial result=%+v err=%v", first, err)
			}
			previousLogger := slog.Default()
			var logs bytes.Buffer
			slog.SetDefault(slog.New(problem4PanicEventHandler{base: slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}), event: event}))
			defer slog.SetDefault(previousLogger)
			store.writerCommit, store.writerCommitLookup = nil, nil
			store.lifecycleTestHooks = nil
			result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
			if !IsRecheckDurableFailure(err) || result.Status != "failed" || store.pendingRecheckFailureSnapshot() != nil {
				t.Fatalf("%s delayed panic result=%+v err=%v pending=%+v", event, result, err, store.pendingRecheckFailureSnapshot())
			}
		})
	}
}

func TestProblem4OperatorWaitsForDelayedTerminalFailureFinalization(t *testing.T) {
	for _, action := range []string{"retry", "abandon"} {
		t.Run(action, func(t *testing.T) {
			matrixStarted := time.Now().UTC()
			var logs bytes.Buffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(previousLogger) })
			store, _, now := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			installProblem4DeterministicOperationTokens(store, "review7-delayed-token")
			fixedNow := now
			wakeCalls := 0
			wakeAfterApplied := true
			store.SetRecheckCommitWake(func() {
				wakeCalls++
				if !strings.Contains(logs.String(), `"event":"operator_action_applied"`) {
					wakeAfterApplied = false
				}
			})
			initialSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckNow: func() time.Time { return fixedNow }, recheckFault: func(stage string) error {
				if stage == recheckStagePersistCandidate {
					return errors.New("terminal candidate failure")
				}
				return nil
			}}
			store.writerCommit = func(tx *sql.Tx) error {
				if err := tx.Commit(); err != nil {
					return err
				}
				return review2SQLiteError{code: 5, message: "delayed terminal bookkeeping"}
			}
			var writerCommitLookupCalls int
			store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
				writerCommitLookupCalls++
				if writerCommitLookupCalls == 1 {
					return false, context.DeadlineExceeded
				}
				return true, nil
			}
			first, firstErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
			if !errors.Is(firstErr, ErrWriterCommitOutcomeUnknown) || first.Disposition != RecheckBatchFailurePersistenceFailed {
				t.Fatalf("initial terminal result=%+v err=%v", first, firstErr)
			}
			delayedPendingSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			assertProblem4LedgerSnapshotTransition(t, initialSnapshot, delayedPendingSnapshot, []string{"recheck_failure"})
			assertProblem4WriterLedgerSequence(t, store.db, "review7-delayed-token", []problem4ExpectedWriterLedgerRow{{"review7-delayed-token-3", "recheck_failure"}}, matrixStarted, time.Now().UTC())
			assertProblem4PhaseSnapshot(t, store, delayedPendingSnapshot, expectedProblem4FailurePendingState(now, now, "review7-delayed-token-2", "persist_candidate: terminal candidate failure", false), "failed", 1, true, false, false, "review7-delayed-token-2", "terminal", "persist_candidate", "recheck_failure", "", 0)
			assertProblem4ExpectedTableTransition(t, initialSnapshot, delayedPendingSnapshot, "silence_recheck_job_rules", nil)
			assertProblem4ExpectedTableTransition(t, initialSnapshot, delayedPendingSnapshot, "silence_recheck_job_candidates", nil)
			assertProblem4ExpectedTableTransition(t, initialSnapshot, delayedPendingSnapshot, "silence_recheck_jobs", map[string]string{
				"status":              "failed",
				"attempt_count":       "1",
				"first_error_message": "persist_candidate: terminal candidate failure",
				"first_error_class":   "terminal",
				"first_error_stage":   "persist_candidate",
				"first_error_at":      now.Format(time.RFC3339Nano),
				"last_error_message":  "persist_candidate: terminal candidate failure",
				"last_error_class":    "terminal",
				"last_error_stage":    "persist_candidate",
				"last_error_at":       now.Format(time.RFC3339Nano),
				"started_at":          now.Format(time.RFC3339Nano),
				"finished_at":         now.Format(time.RFC3339Nano),
			})
			assertProblem4ExpectedAttemptTransition(t, initialSnapshot, delayedPendingSnapshot, "review7-delayed-token-2", "terminal", "persist_candidate", now, now)
			var failureFinalizedPending problem4PendingState
			if err := json.Unmarshal([]byte(delayedPendingSnapshot.PendingState), &failureFinalizedPending); err != nil {
				t.Fatal(err)
			}
			expectedFailureResult := expectedProblem4TerminalFailureResult(1, "source_context_changed", now, now, "persist_candidate: terminal candidate failure")
			if !reflect.DeepEqual(failureFinalizedPending.FailureResult, expectedFailureResult) {
				t.Fatalf("delayed retained result differs from independent expected result: pending=%+v expected=%+v", failureFinalizedPending.FailureResult, expectedFailureResult)
			}
			store.lifecycleTestHooks = nil
			store.writerCommit = nil
			expectedPreFinalizationEvents := []map[string]any{{"level": "ERROR", "msg": "Silence recheck failure bookkeeping was not confirmed", "component": "silence_recheck", "event": "failure_persistence_failed", "job_id": float64(1), "stage": "persist_failure", "error": "deadline"}}
			if events := problem4AllStructuredEvents(t, logs.String()); !reflect.DeepEqual(events, expectedPreFinalizationEvents) {
				t.Fatalf("delayed pre-finalization events=%v want=%v", events, expectedPreFinalizationEvents)
			}
			actor := "review4-operator"
			requestID := "review4-" + action
			operatorAt := now.Add(time.Second)
			logs.Reset()
			firstAfterPrecheck := make(chan struct{})
			secondAfterPrecheck := make(chan struct{})
			failureFinalizerEntered := make(chan struct{})
			releaseFailureFinalizer := make(chan struct{})
			releaseFirstPrecheck := make(chan struct{})
			releaseSecondPrecheck := make(chan struct{})
			var precheckCount int
			operatorFinalizerEntered := make(chan struct{})
			releaseOperatorFinalizer := make(chan struct{})
			store.lifecycleTestHooks = &storeLifecycleTestHooks{
				recheckFailureFinalizer: func() {
					close(failureFinalizerEntered)
					<-releaseFailureFinalizer
				},
				recheckOperatorBarrier: func(stage string) {
					if stage == "after_precheck" {
						precheckCount++
						switch precheckCount {
						case 1:
							close(firstAfterPrecheck)
							<-releaseFirstPrecheck
						case 2:
							close(secondAfterPrecheck)
							<-releaseSecondPrecheck
						}
					}
				},
				recheckOperatorFinalizer: func() { close(operatorFinalizerEntered); <-releaseOperatorFinalizer },
			}
			type operatorOutcome struct {
				result RecheckOperatorActionResult
				err    error
			}
			operatorDone := make(chan operatorOutcome, 1)
			go func() {
				var result RecheckOperatorActionResult
				var err error
				if action == "retry" {
					result, err = store.RetryRecheckJobContext(context.Background(), 1, requestID, "recover", &actor, operatorAt)
				} else {
					result, err = store.AbandonRecheckJobContext(context.Background(), 1, requestID, "operator decision", &actor, operatorAt)
				}
				operatorDone <- operatorOutcome{result: result, err: err}
			}()
			<-firstAfterPrecheck
			firstOperatorSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			if !reflect.DeepEqual(firstOperatorSnapshot, delayedPendingSnapshot) {
				t.Fatalf("operator first precheck did not precede reconciliation: first=%+v pending=%+v", firstOperatorSnapshot, delayedPendingSnapshot)
			}
			if writerCommitLookupCalls != 1 {
				t.Fatalf("operator first precheck unexpectedly reconciled writer commit: lookups=%d", writerCommitLookupCalls)
			}
			select {
			case <-secondAfterPrecheck:
				t.Fatal("operator reached second precheck before detached failure finalizer")
			default:
			}
			close(releaseFirstPrecheck)
			<-failureFinalizerEntered
			if writerCommitLookupCalls != 2 {
				t.Fatalf("operator did not reconcile the pending failure commit: lookups=%d", writerCommitLookupCalls)
			}
			select {
			case <-secondAfterPrecheck:
				t.Fatal("operator reached second precheck while failure finalizer was blocked")
			default:
			}
			close(releaseFailureFinalizer)
			<-secondAfterPrecheck
			operatorBarrierSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			assertProblem4PhaseSnapshot(t, store, operatorBarrierSnapshot, problem4PendingState{}, "failed", 1, false, false, false, "", "", "", "", "", 0)
			if !reflect.DeepEqual(delayedPendingSnapshot.SemanticTables, operatorBarrierSnapshot.SemanticTables) || !reflect.DeepEqual(delayedPendingSnapshot.AttemptRows, operatorBarrierSnapshot.AttemptRows) || !reflect.DeepEqual(delayedPendingSnapshot.WriterLedgerRows, operatorBarrierSnapshot.WriterLedgerRows) {
				t.Fatalf("operator precheck changed the finalized failure snapshot")
			}
			expectedReconciliationEvents := problem4ExpectedTerminalOperatorEvents(action, requestID, actor)[:2]
			if events := problem4AllStructuredEvents(t, logs.String()); !reflect.DeepEqual(events, expectedReconciliationEvents) {
				t.Fatalf("operator-triggered failure reconciliation events=%v want=%v", events, expectedReconciliationEvents)
			}
			if actionRows := countProblem4OperatorActions(t, store.db); actionRows != 0 || strings.Contains(logs.String(), `"event":"operator_action_applied"`) {
				t.Fatalf("operator transaction began before finalized barrier: actions=%d logs=%s", actionRows, logs.String())
			}
			close(releaseSecondPrecheck)
			<-operatorFinalizerEntered
			operatorCommittedSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			assertProblem4PhaseSnapshot(t, store, operatorCommittedSnapshot, expectedProblem4OperatorPendingState(requestID), map[string]string{"retry": "queued", "abandon": "abandoned"}[action], 1, false, true, false, "", "", "", "", requestID, 0)
			if !reflect.DeepEqual(delayedPendingSnapshot.AttemptRows, operatorCommittedSnapshot.AttemptRows) {
				t.Fatalf("delayed operator phase changed attempt rows before finalization: before=%v after=%v", delayedPendingSnapshot.AttemptRows, operatorCommittedSnapshot.AttemptRows)
			}
			assertProblem4LedgerSnapshotTransition(t, operatorBarrierSnapshot, operatorCommittedSnapshot, []string{"recheck_operator_action"})
			assertProblem4WriterLedgerSequence(t, store.db, "review7-delayed-token", []problem4ExpectedWriterLedgerRow{{"review7-delayed-token-3", "recheck_failure"}, {"review7-delayed-token-4", "recheck_operator_action"}}, matrixStarted, time.Now().UTC())
			assertProblem4ExpectedTableTransition(t, operatorBarrierSnapshot, operatorCommittedSnapshot, "silence_recheck_job_rules", nil)
			assertProblem4ExpectedTableTransition(t, operatorBarrierSnapshot, operatorCommittedSnapshot, "silence_recheck_job_candidates", nil)
			operatorJobChanges := map[string]string{"status": map[string]string{"retry": "queued", "abandon": "abandoned"}[action]}
			if action == "retry" {
				operatorJobChanges["recovery_count"] = "1"
				operatorJobChanges["finished_at"] = "<NULL>"
			} else {
				operatorJobChanges["abandoned_at"] = operatorAt.Format(time.RFC3339Nano)
				operatorJobChanges["abandoned_by"] = actor
				operatorJobChanges["abandon_reason"] = "operator decision"
				operatorJobChanges["finished_at"] = operatorAt.Format(time.RFC3339Nano)
			}
			assertProblem4ExpectedTableTransition(t, operatorBarrierSnapshot, operatorCommittedSnapshot, "silence_recheck_jobs", operatorJobChanges)
			assertProblem4ExpectedOperatorActionTransition(t, operatorBarrierSnapshot, operatorCommittedSnapshot, action, requestID, actor, map[string]string{"retry": "recover", "abandon": "operator decision"}[action], map[string]string{"retry": "queued", "abandon": "abandoned"}[action], operatorAt)
			if strings.Contains(logs.String(), `"event":"operator_action_applied"`) {
				t.Fatalf("delayed operator event appeared before finalizer release: %s", logs.String())
			}
			close(releaseOperatorFinalizer)
			operator := <-operatorDone
			result, err := operator.result, operator.err
			if err != nil || !result.Applied {
				t.Fatalf("operator result=%+v err=%v", result, err)
			}
			wantStatus := "queued"
			if action == "abandon" {
				wantStatus = "abandoned"
			}
			if result.CurrentStatus != wantStatus || store.pendingRecheckFailureSnapshot() != nil {
				t.Fatalf("operator status=%s pending=%+v", result.CurrentStatus, store.pendingRecheckFailureSnapshot())
			}
			output := logs.String()
			contractEvents := problem4ParsedEvents(t, output, "job_selected", "job_failed", "operator_action_applied")
			if !reflect.DeepEqual(contractEvents, problem4ExpectedTerminalOperatorEvents(action, requestID, actor)) {
				t.Fatalf("exact delayed records=%v", contractEvents)
			}
			if wakeCalls != 1 || !wakeAfterApplied {
				t.Fatalf("operator wake calls=%d after_applied=%v events=%s", wakeCalls, wakeAfterApplied, output)
			}
			var status string
			var attemptCount, attemptRows, actionRows int
			if err = store.db.QueryRow(`SELECT status,attempt_count FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status, &attemptCount); err != nil {
				t.Fatal(err)
			}
			if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attemptRows); err != nil {
				t.Fatal(err)
			}
			if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions WHERE job_id=1`).Scan(&actionRows); err != nil {
				t.Fatal(err)
			}
			if status != wantStatus || attemptCount != 1 || attemptRows != 1 || actionRows != 1 {
				t.Fatalf("durable state status=%s attempts=%d/%d actions=%d", status, attemptCount, attemptRows, actionRows)
			}
			finalizedSnapshot := captureProblem4DurableSnapshot(t, store, wakeCalls)
			assertProblem4PhaseSnapshot(t, store, finalizedSnapshot, problem4PendingState{}, map[string]string{"retry": "queued", "abandon": "abandoned"}[action], 1, false, false, false, "", "", "", "", "", 1)
			assertProblem4WriterLedgerSequence(t, store.db, "review7-delayed-token", []problem4ExpectedWriterLedgerRow{{"review7-delayed-token-3", "recheck_failure"}, {"review7-delayed-token-4", "recheck_operator_action"}}, matrixStarted, time.Now().UTC())
			beforeReplayEvents := problem4AllStructuredEvents(t, logs.String())
			replayStarted := time.Now().UTC()
			var replay RecheckOperatorActionResult
			if action == "retry" {
				replay, err = store.RetryRecheckJobContext(context.Background(), 1, requestID, "recover", &actor, now.Add(time.Second))
			} else {
				replay, err = store.AbandonRecheckJobContext(context.Background(), 1, requestID, "operator decision", &actor, now.Add(time.Second))
			}
			replayFinished := time.Now().UTC()
			afterReplay := captureProblem4DurableSnapshot(t, store, wakeCalls)
			assertProblem4WriterLedgerSequence(t, store.db, "review7-delayed-token", []problem4ExpectedWriterLedgerRow{{"review7-delayed-token-3", "recheck_failure"}, {"review7-delayed-token-4", "recheck_operator_action"}, {"review7-delayed-token-5", "recheck_operator_action"}}, matrixStarted, time.Now().UTC())
			afterReplayEvents := problem4AllStructuredEvents(t, logs.String())
			if err != nil || !reflect.DeepEqual(replay, result) || !reflect.DeepEqual(afterReplayEvents, beforeReplayEvents) || wakeCalls != 1 {
				t.Fatalf("delayed replay result=%+v/%+v err=%v wakes=%d", replay, result, err, wakeCalls)
			}
			assertProblem4ReplayLedgerDelta(t, store.db, finalizedSnapshot, afterReplay, "review7-delayed-token-5", replayStarted, replayFinished)
			assertProblem4LedgerSnapshotTransition(t, finalizedSnapshot, afterReplay, []string{"recheck_operator_action"})
			logs.Reset()
			next, nextErr := store.RunSilenceRecheckBatchContext(context.Background(), 100)
			if action == "retry" {
				if nextErr != nil || next.Disposition != RecheckBatchCommitted || next.Status != "completed" {
					t.Fatalf("retried execution result=%+v err=%v", next, nextErr)
				}
				if !strings.Contains(logs.String(), `"event":"job_selected"`) {
					t.Fatalf("retried execution had no selection: %s", logs.String())
				}
			} else if nextErr != nil || next.Disposition != RecheckBatchQueueEmpty {
				t.Fatalf("abandoned queue result=%+v err=%v", next, nextErr)
			}
			assertProblem4LedgerRowsBounded(t, store.db, matrixStarted, time.Now().UTC())
		})
	}
}

func TestProblem4OperatorChecksFailureBarrierInsideWriterTransaction(t *testing.T) {
	for _, action := range []string{"retry", "abandon"} {
		t.Run(action, func(t *testing.T) {
			store, _, now := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
				if stage == recheckStagePersistCandidate {
					return errors.New("terminal candidate failure")
				}
				return nil
			}}
			if result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100); !IsRecheckDurableFailure(err) || result.Status != "failed" {
				t.Fatalf("failed-head seed result=%+v err=%v", result, err)
			}
			completion := newRecheckFailureCompletion()
			store.setPendingRecheckFailure(recheckPendingFailure{Attempt: recheckAttemptInfo{JobID: 1, Token: "barrier-token"}, Operation: writerOperationRecheckFailure, Result: RecheckBatchResult{JobID: 1, Status: "failed"}, ResultReady: true, Completion: completion})
			afterPrecheck := make(chan struct{})
			releasePrecheck := make(chan struct{})
			insideTransaction := make(chan struct{})
			store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckOperatorBarrier: func(stage string) {
				switch stage {
				case "after_precheck":
					select {
					case afterPrecheck <- struct{}{}:
						<-releasePrecheck
					default:
					}
				case "inside_transaction":
					select {
					case insideTransaction <- struct{}{}:
					default:
					}
				}
			}}
			type outcome struct {
				result RecheckOperatorActionResult
				err    error
			}
			outcomes := make(chan outcome, 1)
			go func() {
				actor := "barrier-operator"
				var result RecheckOperatorActionResult
				var err error
				if action == "retry" {
					result, err = store.RetryRecheckJobContext(context.Background(), 1, "barrier-retry", "recover", &actor, now)
				} else {
					result, err = store.AbandonRecheckJobContext(context.Background(), 1, "barrier-abandon", "operator decision", &actor, now)
				}
				outcomes <- outcome{result: result, err: err}
			}()
			<-afterPrecheck
			completion.completeCommitted()
			close(releasePrecheck)
			<-insideTransaction
			var actions int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions`).Scan(&actions); err != nil {
				t.Fatal(err)
			}
			if actions != 0 {
				t.Fatalf("operator crossed unfinished barrier: actions=%d", actions)
			}
			select {
			case result := <-outcomes:
				t.Fatalf("operator returned before finalization: %+v", result)
			default:
			}
			completion.completeFinalized()
			result := <-outcomes
			if result.err != nil || !result.result.Applied {
				t.Fatalf("operator result=%+v err=%v", result.result, result.err)
			}
			store.clearPendingRecheckFailure("barrier-token")
		})
	}
}

func TestProblem4MultiBatchEventsKeepBatchCountersAndDurationDomains(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	durations := []time.Duration{11 * time.Millisecond, 37 * time.Millisecond}
	batchNumber := 0
	store.lifecycleTestHooks = &storeLifecycleTestHooks{
		recheckBatchDuration: func(time.Duration) time.Duration {
			if batchNumber >= len(durations) {
				t.Fatal("unexpected extra recheck batch")
			}
			duration := durations[batchNumber]
			batchNumber++
			return duration
		},
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, service := range []string{"api", "payments"} {
		labels := `{"service":"` + service + `"}`
		if upsertErr := store.UpsertCurrentAlert(AlertCurrent{
			AlertID: "problem4-" + service, State: StateInactive, RawState: StateActive, IsLive: false,
			Receiver: "team", GroupID: "group-" + service, GroupLabelsJSON: `{}`, LabelsJSON: labels,
			MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
			RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: now,
		}); upsertErr != nil {
			t.Fatal(upsertErr)
		}
	}
	result, err := store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{
			{SilenceID: "problem4-api", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}}},
			{SilenceID: "problem4-payments", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "payments", IsEqual: true}}},
		}}},
		Timestamp: now,
	})
	if err != nil || result.RecheckJobsQueued != 1 {
		t.Fatalf("two-candidate job creation result=%+v err=%v", result, err)
	}
	first, err := store.RunSilenceRecheckBatchContext(context.Background(), 1)
	if err != nil || first.Status != "running" || first.ProcessedCandidates != 1 {
		t.Fatalf("first batch result=%+v err=%v", first, err)
	}
	second, err := store.RunSilenceRecheckBatchContext(context.Background(), 1)
	if err != nil || second.Status != "completed" || second.ProcessedCandidates != 2 {
		t.Fatalf("second batch result=%+v err=%v", second, err)
	}
	var events []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) == nil {
			events = append(events, event)
		}
	}
	var batchEvents, selectedEvents []map[string]any
	var completed map[string]any
	for _, event := range events {
		switch event["event"] {
		case "job_selected":
			selectedEvents = append(selectedEvents, event)
		case "batch_completed":
			batchEvents = append(batchEvents, event)
		case "job_completed":
			completed = event
		}
	}
	if len(selectedEvents) != 2 || len(batchEvents) != 2 || completed == nil {
		t.Fatalf("event matrix selected=%d batches=%d completed=%v logs=%s", len(selectedEvents), len(batchEvents), completed, logs.String())
	}
	if selectedEvents[0]["status"] != "queued" || selectedEvents[1]["status"] != "running" {
		t.Fatalf("selection statuses=%v", selectedEvents)
	}
	for index, event := range batchEvents {
		if event["processed"] != float64(1) || event["promoted"] != float64(1) {
			t.Fatalf("batch[%d] counters=%v", index, event)
		}
		if duration, ok := event["duration"].(float64); !ok || math.Abs(duration-float64(durations[index].Nanoseconds())) > 0.5 {
			t.Fatalf("batch[%d] duration=%v", index, event["duration"])
		}
	}
	if completed["processed"] != float64(2) || completed["promoted"] != float64(2) {
		t.Fatalf("cumulative counters=%v", completed)
	}
	if duration, ok := completed["duration"].(float64); !ok || duration <= 0 {
		t.Fatalf("cumulative duration=%v", completed["duration"])
	}
	lastBatchIndex, completedIndex := -1, -1
	for index, event := range events {
		if event["event"] == "batch_completed" {
			lastBatchIndex = index
		}
		if event["event"] == "job_completed" {
			completedIndex = index
		}
	}
	if lastBatchIndex < 0 || completedIndex <= lastBatchIndex {
		t.Fatalf("batch/job completion order=%v", events)
	}
}

func TestProblem4StatsUsesOneReadTransactionSnapshot(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	interleavedDB, err := sql.Open("sqlite", store.sqlitePath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = interleavedDB.Close() })
	if _, err = interleavedDB.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, metaRegroupStatus, "stable"); err != nil {
		t.Fatal(err)
	}
	baseline, err := store.GetStats()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	writerDone := make(chan error, 1)
	triggered := false
	var interleavedWriterErr error
	store.lifecycleTestHooks = &storeLifecycleTestHooks{
		statsSnapshotAfterCurrentRows: func() {
			if triggered {
				return
			}
			triggered = true
			go func() {
				close(started)
				_, writerErr := interleavedDB.Exec(`UPDATE metadata SET value=? WHERE key=?`, "interleaved", metaRegroupStatus)
				writerDone <- writerErr
			}()
			<-started
			interleavedWriterErr = <-writerDone
		},
	}
	stats, err := store.GetStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.RegroupStatus != "stable" {
		t.Fatalf("stats mixed rows from another commit: status=%q", stats.RegroupStatus)
	}
	comparison := stats
	comparison.RegroupStatus = baseline.RegroupStatus
	if !reflect.DeepEqual(comparison, baseline) {
		t.Fatalf("fault-free stats snapshot changed outside the interleaved field:\n got=%+v\nwant=%+v", comparison, baseline)
	}
	if interleavedWriterErr != nil {
		t.Fatalf("interleaved writer failed: %v", interleavedWriterErr)
	}
	updated, err := store.GetStats()
	if err != nil || updated.RegroupStatus != "interleaved" {
		t.Fatalf("post-snapshot writer was not visible on next read: stats=%+v err=%v", updated, err)
	}
}

func TestProblem4StoreDoesNotOwnSelectedInfrastructureEvent(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return ErrWriterOperationInvariant
		}
		return nil
	}}
	result, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if err == nil || result.Disposition != RecheckBatchInfrastructureFailed {
		t.Fatalf("expected typed selected infrastructure failure, result=%+v err=%v", result, err)
	}
	if strings.Contains(logs.String(), `"event":"job_infrastructure_blocked"`) {
		t.Fatalf("Store emitted scheduler-owned selected infrastructure event: %s", logs.String())
	}
}

func TestProblem4SelectedCancellationUsesExpectedShutdownDisposition(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
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
	result, err := store.RunSilenceRecheckBatchContext(ctx, 100)
	if !errors.Is(err, context.Canceled) || result.Disposition != RecheckBatchInfrastructureFailed || result.FailureClass != "worker_cancelled" {
		t.Fatalf("unexpected selected cancellation result=%+v err=%v", result, err)
	}
	if strings.Contains(logs.String(), `"event":"job_commit_outcome_unknown"`) || strings.Contains(logs.String(), `"event":"job_failed"`) {
		t.Fatalf("shutdown cancellation emitted the wrong event: %s", logs.String())
	}
}

func TestProblem4BetweenBatchCancellationHasNoEffects(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	for _, alertID := range []string{"cancel-a", "cancel-b"} {
		if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,target_key,match_labels_json,relevant_sources_json) VALUES(?,?,?,?,?)`, 1, alertID, "", `{}`, `[]`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=3 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	if first, err := store.RunSilenceRecheckBatchContext(context.Background(), 1); err != nil || first.Status != "running" || first.ProcessedCandidates != 1 {
		t.Fatalf("first batch=%+v err=%v", first, err)
	}
	before := captureProblem3Review2State(t, store.db)
	logs.Reset()
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(context.Canceled)
	result, err := store.RunSilenceRecheckBatchContext(ctx, 1)
	if err == nil || (result.Disposition != RecheckBatchAcquisitionFailed && result.Disposition != RecheckBatchInfrastructureFailed) {
		t.Fatalf("cancelled between-batch result=%+v err=%v", result, err)
	}
	if after := captureProblem3Review2State(t, store.db); after != before {
		t.Fatal("between-batch cancellation changed durable state")
	}
	if strings.Contains(logs.String(), `"event":"job_selected"`) || strings.Contains(logs.String(), `"event":"batch_completed"`) || strings.Contains(logs.String(), `"event":"job_failed"`) {
		t.Fatalf("between-batch cancellation emitted domain events: %s", logs.String())
	}
}

func TestProblem4PromotionEventsCarryCommittedTransitionIDs(t *testing.T) {
	verify := func(t *testing.T, logs, eventName, alertID, query string, store *Store) {
		t.Helper()
		var committedID int64
		if err := store.db.QueryRow(query, alertID).Scan(&committedID); err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
			var record map[string]any
			if err := json.Unmarshal([]byte(line), &record); err != nil || record["event"] != eventName || record["alert_id"] != alertID {
				continue
			}
			if got, ok := record["transition_id"].(float64); ok && int64(got) == committedID {
				return
			}
			t.Fatalf("promotion event transition_id=%v want=%d: %s", record["transition_id"], committedID, line)
		}
		t.Fatalf("missing %s event for %s: %s", eventName, alertID, logs)
	}

	t.Run("global", func(t *testing.T) {
		var logs bytes.Buffer
		previousLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		t.Cleanup(func() { slog.SetDefault(previousLogger) })
		store, _, _ := seedProblem2OpenJob(t)
		t.Cleanup(func() { _ = store.Close() })
		if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 100); err != nil {
			t.Fatal(err)
		}
		verify(t, logs.String(), "candidate_promoted", "problem2-alert", `SELECT id FROM alert_events WHERE alert_id=? ORDER BY id DESC LIMIT 1`, store)
	})

	t.Run("target", func(t *testing.T) {
		var logs bytes.Buffer
		previousLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		t.Cleanup(func() { slog.SetDefault(previousLogger) })
		store, _ := seedProblem2TargetJob(t)
		t.Cleanup(func() { _ = store.Close() })
		if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 100); err != nil {
			t.Fatal(err)
		}
		verify(t, logs.String(), "candidate_promoted", "target-episode", `SELECT id FROM alert_target_events WHERE alert_id=? ORDER BY id DESC LIMIT 1`, store)
	})
}

func TestProblem4AttemptDiagnosticsRedactAttemptToken(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	sentinel := "attempt-token-problem4-redaction-sentinel"
	err := store.validateRecheckAttemptTupleLocked(context.Background(), recheckAttemptInfo{JobID: 1, Number: 1, Token: sentinel}, "committed", false)
	if err == nil || strings.Contains(err.Error(), sentinel) {
		t.Fatalf("attempt tuple diagnostic exposed token: %v", err)
	}
}

func TestProblem4FailurePersistenceLogsRedactAttemptToken(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	sentinel := "attempt-token-problem4-failure-redaction"
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		switch stage {
		case recheckStagePersistCandidate:
			return problem3SQLiteError(5)
		case recheckStagePersistFailure:
			return errors.Join(ErrWriterOperationInvariant, errors.New(sentinel))
		default:
			return nil
		}
	}}
	_, err := store.RunSilenceRecheckBatchContext(context.Background(), 100)
	if err == nil || !strings.Contains(logs.String(), `"event":"failure_persistence_failed"`) {
		t.Fatalf("expected failure persistence logging, err=%v logs=%s", err, logs.String())
	}
	if strings.Contains(logs.String(), sentinel) {
		t.Fatalf("failure persistence log disclosed raw error: %s", logs.String())
	}
}

func TestProblem4StatsExposeCompleteNonterminalHead(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err = store.db.Exec(`
INSERT INTO silence_recheck_jobs(
 status, created_at, started_at, finished_at, source_context_json,
 source_context_fingerprint, job_fingerprint, trigger_type,
 total_candidates, processed_candidates, promoted_candidates
 ) VALUES('completed', ?, ?, ?, '{"formatVersion":1,"sources":[],"clusters":[]}',
 'problem4-completed', 'sha256:problem4-completed', 'source_context_changed', 1, 1, 1)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`
INSERT INTO silence_recheck_jobs(
 status, created_at, started_at, finished_at, next_attempt_at, source_context_json,
 source_context_fingerprint, job_fingerprint, trigger_type,
 total_candidates, processed_candidates, promoted_candidates, skipped_ineligible,
 skipped_missing_labels, skipped_incomplete_sources, stale_conflicts, attempt_count,
 retry_count, consecutive_retry_count, recovery_count, first_error_message,
 first_error_class, first_error_stage, first_error_at, last_error_message,
 last_error_class, last_error_stage, last_error_at
) VALUES('retry_wait', ?, ?, NULL, ?, '{"formatVersion":1,"sources":[],"clusters":[]}',
 'problem4-head', 'sha256:problem4-head', 'alert_became_eligible',
  5, 5, 1, 1, 1, 1, 1, 2, 3, 2, 4, 'first failure',
  'retryable_storage', 'persist_candidate', ?, 'last failure',
  'retryable_storage', 'persist_candidate', ?)`, now, now, now, now, now); err != nil {
		t.Fatal(err)
	}

	stats, err := store.GetStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Recheck.QueueDepth != 1 || stats.Recheck.HeadJob == nil {
		t.Fatalf("expected one complete recheck head, got=%+v", stats.Recheck)
	}
	head := stats.Recheck.HeadJob
	if head.JobID != 2 || head.TriggerType != "alert_became_eligible" || head.TotalCandidates != 5 || head.ProcessedCandidates != 5 {
		t.Fatalf("unexpected head metadata: %+v", head)
	}
	if head.OutcomeCounts != (RecheckOutcomeCounts{Promoted: 1, Stale: 1, Ineligible: 1, MissingLabels: 1, IncompleteSources: 1}) || head.AttemptCount != 2 || head.RetryCount != 3 || head.ConsecutiveRetryCount != 2 || head.RecoveryCount != 4 {
		t.Fatalf("unexpected head counters: %+v", head)
	}
	if head.FirstError == nil || head.LastError == nil {
		t.Fatalf("expected complete first/last error snapshots: %+v", head)
	}
}

func TestProblem4StatsRejectsIncompleteErrorTuples(t *testing.T) {
	for _, status := range []string{"retry_wait", "failed", "abandoned"} {
		t.Run(status, func(t *testing.T) {
			store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			now := time.Now().UTC().Format(time.RFC3339Nano)
			var query string
			var args []any
			switch status {
			case "retry_wait":
				query = `INSERT INTO silence_recheck_jobs(status,created_at,started_at,finished_at,next_attempt_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,first_error_message) VALUES(?,?,?,NULL,?,'{"formatVersion":1,"sources":[],"clusters":[]}',?,?, 'source_context_changed', 'partial')`
				args = []any{status, now, now, now, "problem4-partial-" + status, "sha256:problem4-partial-" + status}
			case "abandoned":
				query = `INSERT INTO silence_recheck_jobs(status,created_at,started_at,finished_at,next_attempt_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,first_error_message,abandoned_at,abandoned_by) VALUES(?,?,?, ?,NULL,'{"formatVersion":1,"sources":[],"clusters":[]}',?,?, 'source_context_changed', 'partial', ?, 'operator')`
				args = []any{status, now, now, now, "problem4-partial-" + status, "sha256:problem4-partial-" + status, now}
			default:
				query = `INSERT INTO silence_recheck_jobs(status,created_at,started_at,finished_at,next_attempt_at,source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,first_error_message) VALUES(?,?,?,?,?,'{"formatVersion":1,"sources":[],"clusters":[]}',? ,?, 'source_context_changed', 'partial')`
				args = []any{status, now, now, now, nil, "problem4-partial-" + status, "sha256:problem4-partial-" + status}
			}
			if _, err = store.db.Exec(query, args...); err != nil {
				t.Fatal(err)
			}
			if _, err = store.GetStats(); err == nil {
				t.Fatal("stats accepted an incomplete durable error tuple")
			}
		})
	}
}

func TestProblem4MultiBatchCompletionLogsCumulativeTotals(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	durations := []time.Duration{11 * time.Millisecond, 22 * time.Millisecond, 33 * time.Millisecond}
	durationIndex := 0
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckBatchDuration: func(time.Duration) time.Duration {
		result := durations[durationIndex]
		durationIndex++
		return result
	}}
	for _, alertID := range []string{"missing-a", "missing-b"} {
		if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,target_key,match_labels_json,relevant_sources_json) VALUES(?,?,?,?,?)`, 1, alertID, "", `{}`, `[]`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=3 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	for batch := 0; batch < 3; batch++ {
		if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	output := logs.String()
	if strings.Count(output, `"event":"batch_completed"`) != 3 {
		t.Fatalf("expected one batch event for each commit, output=%s", output)
	}
	if strings.Count(output, `"event":"batch_completed"`) != strings.Count(output, `"event":"job_selected"`) {
		t.Fatalf("batch and selection event counts diverged: %s", output)
	}
	var batchLevels []string
	var batchDurations []time.Duration
	var jobDuration time.Duration
	var batchProcessed, batchPromoted, batchMissing, batchIneligible, batchIncomplete, batchStale int
	var jobProcessed, jobPromoted, jobMissing, jobIneligible, jobIncomplete, jobStale int
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var record struct {
			Level             string        `json:"level"`
			Event             string        `json:"event"`
			Duration          time.Duration `json:"duration"`
			Processed         int           `json:"processed"`
			Promoted          int           `json:"promoted"`
			MissingLabels     int           `json:"missing_labels"`
			Ineligible        int           `json:"ineligible"`
			IncompleteSources int           `json:"incomplete_sources"`
			Stale             int           `json:"stale"`
		}
		if err := json.Unmarshal([]byte(line), &record); err == nil && record.Event == "batch_completed" {
			batchLevels = append(batchLevels, record.Level)
			batchDurations = append(batchDurations, record.Duration)
			batchProcessed += record.Processed
			batchPromoted += record.Promoted
			batchMissing += record.MissingLabels
			batchIneligible += record.Ineligible
			batchIncomplete += record.IncompleteSources
			batchStale += record.Stale
		}
		if err := json.Unmarshal([]byte(line), &record); err == nil && record.Event == "job_completed" {
			jobDuration = record.Duration
			jobProcessed, jobPromoted = record.Processed, record.Promoted
			jobMissing, jobIneligible = record.MissingLabels, record.Ineligible
			jobIncomplete, jobStale = record.IncompleteSources, record.Stale
		}
	}
	if got, want := strings.Join(batchLevels, ","), "DEBUG,DEBUG,INFO"; got != want {
		t.Fatalf("batch levels=%q, want %q; output=%s", got, want, output)
	}
	if !reflect.DeepEqual(batchDurations, durations) || jobDuration <= 0 {
		t.Fatalf("final batch duration=%v, job duration=%v; want distinct batch-local duration: %s", batchDurations, jobDuration, output)
	}
	if batchProcessed != 3 || jobProcessed != batchProcessed || jobPromoted != batchPromoted || jobMissing != batchMissing || jobIneligible != batchIneligible || jobIncomplete != batchIncomplete || jobStale != batchStale {
		t.Fatalf("batch totals processed=%d promoted=%d missing=%d ineligible=%d incomplete=%d stale=%d; job=%d/%d/%d/%d/%d/%d", batchProcessed, batchPromoted, batchMissing, batchIneligible, batchIncomplete, batchStale, jobProcessed, jobPromoted, jobMissing, jobIneligible, jobIncomplete, jobStale)
	}
	if !strings.Contains(output, `"event":"job_completed"`) || !strings.Contains(output, `"processed":3`) || !strings.Contains(output, `"promoted":1`) {
		t.Fatalf("job completion did not report cumulative durable counters: %s", output)
	}
	var selectedStatuses []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var record struct {
			Event  string `json:"event"`
			Status string `json:"status"`
		}
		if err := json.Unmarshal([]byte(line), &record); err == nil && record.Event == "job_selected" {
			selectedStatuses = append(selectedStatuses, record.Status)
		}
	}
	if got, want := strings.Join(selectedStatuses, ","), "queued,running,running"; got != want {
		t.Fatalf("job_selected statuses=%q, want %q; output=%s", got, want, output)
	}
	selected := strings.Index(output, `"event":"job_selected"`)
	started := strings.Index(output, `"event":"job_started"`)
	if started < 0 {
		started = strings.Index(output, `"event":"job_resumed"`)
	}
	completedBatch := strings.LastIndex(output, `"event":"batch_completed"`)
	completedJob := strings.LastIndex(output, `"event":"job_completed"`)
	if selected < 0 || started < selected || completedBatch < started || completedJob < completedBatch {
		t.Fatalf("worker event order is not causal: %s", output)
	}
}
