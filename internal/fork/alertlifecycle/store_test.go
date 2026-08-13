package alertlifecycle

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestOpenStoreBootstrapsSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")

	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	version, err := store.GetSchemaVersion()
	if err != nil {
		t.Fatalf("GetSchemaVersion() returned error: %v", err)
	}
	if version != schemaVersion {
		t.Fatalf("unexpected schema version: got=%q want=%q", version, schemaVersion)
	}

	var busyTimeout int
	if err = store.db.QueryRow(`PRAGMA busy_timeout`).Scan(&busyTimeout); err != nil {
		t.Fatalf("failed reading busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("unexpected busy_timeout: got=%d want=5000", busyTimeout)
	}
}

func TestOpenStoreRejectsNonV1SchemaWithoutChangingIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL); INSERT INTO metadata(key, value) VALUES('schema_version', '2')`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := OpenStore(StoreOptions{Path: path})
	if store != nil {
		_ = store.Close()
	}
	if err == nil || !strings.Contains(err.Error(), `unsupported sqlite schema version "2"`) {
		t.Fatalf("expected unsupported schema error, got %v", err)
	}

	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version string
	if err = db.QueryRow(`SELECT value FROM metadata WHERE key='schema_version'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != "2" {
		t.Fatalf("schema version changed: got %q want %q", version, "2")
	}
}

func TestOpenStoreBootstrapsPhase2Tables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")

	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	expected := []string{
		"alert_target_states",
		"alert_target_events",
		"alert_target_match_labels",
		"alertmanager_cluster_consistency",
		"alertmanager_sources",
		"silence_rules",
		"silence_rule_episodes",
		"silence_revisions",
		"alertmanager_source_silences",
		"silence_recheck_jobs",
		"silence_recheck_job_rules",
		"silence_recheck_job_candidates",
		"alert_suppression_cause_history",
		"alert_suppression_cause_members",
		"alert_target_suppression_cause_history",
		"alert_target_suppression_cause_members",
		"alert_target_aggregate_suppression_history",
		"alert_target_aggregate_suppression_links",
	}

	for _, table := range expected {
		var count int
		err = store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count)
		if err != nil {
			t.Fatalf("failed checking table %q: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("expected table %q to exist", table)
		}
	}
}

func TestUpsertAndReadCurrentAlert(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")

	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	alert := AlertCurrent{
		AlertID:           "id-1",
		State:             StateActive,
		Receiver:          "team-a",
		GroupID:           "group-1",
		LabelsJSON:        `{"alertname":"DiskFull"}`,
		AnnotationsJSON:   `{"summary":"Disk full"}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}

	if err = store.UpsertCurrentAlert(alert); err != nil {
		t.Fatalf("UpsertCurrentAlert() returned error: %v", err)
	}

	got, err := store.GetCurrentAlert("id-1")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if got == nil {
		t.Fatal("GetCurrentAlert() returned nil alert")
	}

	if got.AlertID != alert.AlertID {
		t.Fatalf("alert id mismatch: got=%q want=%q", got.AlertID, alert.AlertID)
	}
	if got.State != alert.State {
		t.Fatalf("alert state mismatch: got=%q want=%q", got.State, alert.State)
	}
	if got.RelevantUpstreams != alert.RelevantUpstreams {
		t.Fatalf("relevant_upstreams mismatch: got=%q want=%q", got.RelevantUpstreams, alert.RelevantUpstreams)
	}
	if got.LastSeenAt == nil {
		t.Fatal("expected last_seen_at to be set")
	}
	if got.GroupLabelsJSON != "{}" {
		t.Fatalf("group_labels_json mismatch: got=%q want=%q", got.GroupLabelsJSON, "{}")
	}
}

func TestInsertStateTransition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")

	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "id-2",
		State:             StateInactive,
		Receiver:          "team-a",
		GroupID:           "group-1",
		LabelsJSON:        `{"alertname":"DiskFull"}`,
		AnnotationsJSON:   `{"summary":"Disk full"}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() setup failed: %v", err)
	}

	oldState := StateInactive
	if err = store.InsertStateTransition(AlertStateTransition{
		AlertID:      "id-2",
		OldState:     &oldState,
		NewState:     StateResolved,
		ChangeSource: ChangeSourceManual,
		ChangedBy:    nil,
	}); err != nil {
		t.Fatalf("InsertStateTransition() returned error: %v", err)
	}

	count, err := store.GetTransitionCount("id-2")
	if err != nil {
		t.Fatalf("GetTransitionCount() returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("transition count mismatch: got=%d want=1", count)
	}
}

func TestOpenStoreEmptyPathError(t *testing.T) {
	_, err := OpenStore(StoreOptions{Path: ""})
	if err == nil {
		t.Fatal("expected error for empty sqlite path")
	}
}

func TestResolveAlertSuccessFromInactive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "id-resolve-ok",
		State:             StateInactive,
		Receiver:          "team-a",
		GroupID:           "group-1",
		LabelsJSON:        `{"alertname":"DiskFull"}`,
		AnnotationsJSON:   `{"summary":"Disk full"}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() setup failed: %v", err)
	}

	user := "alice"
	comment := "resolved in UI"
	res, err := store.ResolveAlert("id-resolve-ok", &user, &comment, now)
	if err != nil {
		t.Fatalf("ResolveAlert() returned error: %v", err)
	}
	if res.NewState != StateResolved || res.OldState != StateInactive {
		t.Fatalf("unexpected resolve result states old=%s new=%s", res.OldState, res.NewState)
	}
	if res.ChangedBy == nil || *res.ChangedBy != user {
		t.Fatalf("expected changedBy=%q got=%v", user, res.ChangedBy)
	}

	current, err := store.GetCurrentAlert("id-resolve-ok")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if current == nil || current.State != StateResolved {
		t.Fatalf("expected state resolved got=%v", current)
	}
}

func TestResolveAlertNotFound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	_, err = store.ResolveAlert("missing", nil, nil, time.Now().UTC())
	if !errors.Is(err, ErrAlertNotFound) {
		t.Fatalf("expected ErrAlertNotFound got=%v", err)
	}
}

func TestResolveAlertConflictFromActive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "id-resolve-active",
		State:             StateActive,
		Receiver:          "team-a",
		GroupID:           "group-1",
		LabelsJSON:        `{"alertname":"DiskFull"}`,
		AnnotationsJSON:   `{"summary":"Disk full"}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() setup failed: %v", err)
	}

	_, err = store.ResolveAlert("id-resolve-active", nil, nil, now)
	if !errors.Is(err, ErrResolveStateConflict) {
		t.Fatalf("expected ErrResolveStateConflict got=%v", err)
	}
}

func TestResolveAlertKeepsNullActor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "id-resolve-null-actor",
		State:             StateInactive,
		Receiver:          "team-a",
		GroupID:           "group-1",
		LabelsJSON:        `{"alertname":"DiskFull"}`,
		AnnotationsJSON:   `{"summary":"Disk full"}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() setup failed: %v", err)
	}

	res, err := store.ResolveAlert("id-resolve-null-actor", nil, nil, now)
	if err != nil {
		t.Fatalf("ResolveAlert() returned error: %v", err)
	}
	if res.ChangedBy != nil {
		t.Fatalf("expected nil changedBy got=%v", res.ChangedBy)
	}
}

func TestApplyPollCycleSetsInactiveOnFullCoverage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	base := LiveAlertSnapshot{
		AlertID:           "id-coverage",
		State:             StateActive,
		Receiver:          "team-a",
		GroupID:           "group-1",
		LabelsJSON:        `{"alertname":"DiskFull"}`,
		AnnotationsJSON:   `{"summary":"Disk full"}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        now,
		RelevantUpstreams: []string{"am-a", "am-b"},
	}

	_, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{base},
		SuccessfulUpstreams: map[string]bool{
			"am-a": true,
			"am-b": true,
		},
		Timestamp: now,
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(seed) returned error: %v", err)
	}

	res, err := store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: nil,
		SuccessfulUpstreams: map[string]bool{
			"am-a": true,
			"am-b": true,
		},
		Timestamp: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(absent) returned error: %v", err)
	}
	if res.SetInactive != 1 {
		t.Fatalf("expected SetInactive=1 got=%d", res.SetInactive)
	}

	current, err := store.GetCurrentAlert("id-coverage")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if current == nil || current.State != StateInactive {
		t.Fatalf("expected state inactive, got=%v", current)
	}
}

func TestApplyPollCycleSkipsInactiveOnPartialCoverage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	base := LiveAlertSnapshot{
		AlertID:           "id-partial",
		State:             StateSuppressed,
		Receiver:          "team-a",
		GroupID:           "group-1",
		LabelsJSON:        `{"alertname":"LatencyHigh"}`,
		AnnotationsJSON:   `{"summary":"Latency high"}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        now,
		RelevantUpstreams: []string{"am-a", "am-b"},
	}

	_, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{base},
		SuccessfulUpstreams: map[string]bool{
			"am-a": true,
			"am-b": true,
		},
		Timestamp: now,
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(seed) returned error: %v", err)
	}

	res, err := store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: nil,
		SuccessfulUpstreams: map[string]bool{
			"am-a": true,
			"am-b": false,
		},
		Timestamp: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(absent) returned error: %v", err)
	}
	if res.SetInactive != 0 {
		t.Fatalf("expected SetInactive=0 got=%d", res.SetInactive)
	}
	if res.SkippedInactive != 1 {
		t.Fatalf("expected SkippedInactive=1 got=%d", res.SkippedInactive)
	}

	current, err := store.GetCurrentAlert("id-partial")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if current == nil || current.State != StateSuppressed {
		t.Fatalf("expected state suppressed, got=%v", current)
	}
}

func TestApplyPollCycleReactivatesResolved(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)

	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "id-resolved",
		State:             StateResolved,
		Receiver:          "team-a",
		GroupID:           "group-1",
		LabelsJSON:        `{"alertname":"CPUHigh"}`,
		AnnotationsJSON:   `{"summary":"CPU high"}`,
		StartsAt:          now.Add(-2 * time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() setup failed: %v", err)
	}

	old := StateInactive
	if err = store.InsertStateTransition(AlertStateTransition{
		AlertID:      "id-resolved",
		OldState:     &old,
		NewState:     StateResolved,
		ChangeSource: ChangeSourceManual,
		ChangedAt:    now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("InsertStateTransition() setup failed: %v", err)
	}

	res, err := store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{
			{
				AlertID:           "id-resolved",
				State:             StateActive,
				Receiver:          "team-a",
				GroupID:           "group-1",
				LabelsJSON:        `{"alertname":"CPUHigh"}`,
				AnnotationsJSON:   `{"summary":"CPU high"}`,
				StartsAt:          now.Add(-2 * time.Hour),
				LastSeenAt:        now,
				RelevantUpstreams: []string{"am-a"},
			},
		},
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		Timestamp:           now,
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle() returned error: %v", err)
	}
	if res.Reactivated != 1 {
		t.Fatalf("expected Reactivated=1 got=%d", res.Reactivated)
	}

	current, err := store.GetCurrentAlert("id-resolved")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if current == nil || current.State != StateActive {
		t.Fatalf("expected state active, got=%v", current)
	}
}

func TestRunHousekeepingDeletesExpiredThenEnforcesLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)

	seed := func(id string, state State, lastSeen *time.Time, updatedAt time.Time) {
		t.Helper()
		err = store.UpsertCurrentAlert(AlertCurrent{
			AlertID:           id,
			State:             state,
			Receiver:          "team-a",
			GroupID:           "group-1",
			LabelsJSON:        `{"alertname":"Test"}`,
			AnnotationsJSON:   `{"summary":"Test"}`,
			StartsAt:          now.Add(-time.Hour),
			LastSeenAt:        lastSeen,
			RelevantUpstreams: `["am-a"]`,
			UpdatedAt:         updatedAt,
		})
		if err != nil {
			t.Fatalf("UpsertCurrentAlert(%s) failed: %v", id, err)
		}
	}

	expiredSeen := now.Add(-50 * time.Hour)
	freshSeen := now.Add(-30 * time.Minute)
	seed("expired-1", StateInactive, &expiredSeen, now.Add(-50*time.Hour))
	seed("keep-oldest", StateInactive, &freshSeen, now.Add(-20*time.Minute))
	seed("keep-newer", StateActive, &freshSeen, now.Add(-10*time.Minute))
	seed("drop-by-limit", StateSuppressed, &freshSeen, now.Add(-40*time.Minute))

	result, err := store.RunHousekeeping(48*time.Hour, 2)
	if err != nil {
		t.Fatalf("RunHousekeeping() returned error: %v", err)
	}
	if result.ExpiredDeleted != 1 {
		t.Fatalf("expected ExpiredDeleted=1 got=%d", result.ExpiredDeleted)
	}
	if result.LimitDeleted != 1 {
		t.Fatalf("expected LimitDeleted=1 got=%d", result.LimitDeleted)
	}
	if result.CurrentRowsAfter != 2 {
		t.Fatalf("expected CurrentRowsAfter=2 got=%d", result.CurrentRowsAfter)
	}

	if got, _ := store.GetCurrentAlert("expired-1"); got != nil {
		t.Fatal("expected expired-1 to be deleted")
	}
	if got, _ := store.GetCurrentAlert("drop-by-limit"); got != nil {
		t.Fatal("expected drop-by-limit to be deleted by max limit")
	}
	if got, _ := store.GetCurrentAlert("keep-oldest"); got == nil {
		t.Fatal("expected keep-oldest to remain")
	}
	if got, _ := store.GetCurrentAlert("keep-newer"); got == nil {
		t.Fatal("expected keep-newer to remain")
	}
}

func TestGetStatsIncludesFileAndMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "id-stats",
		State:             StateInactive,
		Receiver:          "team-a",
		GroupID:           "group-1",
		LabelsJSON:        `{"alertname":"Stats"}`,
		AnnotationsJSON:   `{"summary":"Stats"}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() setup failed: %v", err)
	}

	oldState := StateInactive
	if err = store.InsertStateTransition(AlertStateTransition{
		AlertID:      "id-stats",
		OldState:     &oldState,
		NewState:     StateResolved,
		ChangeSource: ChangeSourceManual,
	}); err != nil {
		t.Fatalf("InsertStateTransition() setup failed: %v", err)
	}

	_, err = store.RunHousekeeping(24*time.Hour, 1000)
	if err != nil {
		t.Fatalf("RunHousekeeping() returned error: %v", err)
	}

	stats, err := store.GetStats()
	if err != nil {
		t.Fatalf("GetStats() returned error: %v", err)
	}
	if stats.CurrentRows < 1 {
		t.Fatalf("expected CurrentRows >= 1 got=%d", stats.CurrentRows)
	}
	if stats.HistoryRows < 1 {
		t.Fatalf("expected HistoryRows >= 1 got=%d", stats.HistoryRows)
	}
	if stats.FileSizeBytes <= 0 {
		t.Fatalf("expected FileSizeBytes > 0 got=%d", stats.FileSizeBytes)
	}
	if stats.PageCount <= 0 || stats.PageSizeBytes <= 0 {
		t.Fatalf("invalid sqlite page stats: pageCount=%d pageSize=%d", stats.PageCount, stats.PageSizeBytes)
	}
	if stats.LastHousekeepingAt == nil {
		t.Fatal("expected LastHousekeepingAt to be set")
	}
	if stats.LastIncrementalVacuumAt == nil {
		t.Fatal("expected LastIncrementalVacuumAt to be set")
	}

	if st, statErr := os.Stat(path); statErr == nil {
		if st.Size() != stats.FileSizeBytes {
			t.Fatalf("file size mismatch got=%d want=%d", stats.FileSizeBytes, st.Size())
		}
	}
}

func TestRunHousekeepingLimitUsesLastStateChangeAge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	oldSeen := now.Add(-time.Hour)

	seed := func(id string, state State, updatedAt time.Time) {
		t.Helper()
		err = store.UpsertCurrentAlert(AlertCurrent{
			AlertID:           id,
			State:             state,
			Receiver:          "team-a",
			GroupID:           "group-1",
			LabelsJSON:        `{"alertname":"LimitAge"}`,
			AnnotationsJSON:   `{"summary":"LimitAge"}`,
			StartsAt:          now.Add(-2 * time.Hour),
			LastSeenAt:        &oldSeen,
			RelevantUpstreams: `["am-a"]`,
			UpdatedAt:         updatedAt,
		})
		if err != nil {
			t.Fatalf("UpsertCurrentAlert(%s) failed: %v", id, err)
		}
	}

	seed("state-old", StateInactive, now.Add(-3*time.Hour))
	seed("state-new", StateInactive, now.Add(-3*time.Hour))

	old := StateInactive
	if err = store.InsertStateTransition(AlertStateTransition{
		AlertID:      "state-new",
		OldState:     &old,
		NewState:     StateResolved,
		ChangedAt:    now,
		ChangeSource: ChangeSourceManual,
	}); err != nil {
		t.Fatalf("InsertStateTransition(state-new) failed: %v", err)
	}

	res, err := store.RunHousekeeping(24*time.Hour, 1)
	if err != nil {
		t.Fatalf("RunHousekeeping() returned error: %v", err)
	}
	if res.LimitDeleted != 1 {
		t.Fatalf("expected LimitDeleted=1 got=%d", res.LimitDeleted)
	}

	if got, _ := store.GetCurrentAlert("state-old"); got != nil {
		t.Fatal("expected oldest by last state change to be deleted")
	}
	if got, _ := store.GetCurrentAlert("state-new"); got == nil {
		t.Fatal("expected recently changed alert to remain")
	}
}

func TestApplyPollCycleKeepsExistingRelevantUpstreamsOnPartialCoverage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	_, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{
			{
				AlertID:           "id-rel",
				State:             StateActive,
				Receiver:          "team-a",
				GroupID:           "group-1",
				LabelsJSON:        `{"alertname":"NetIssue"}`,
				AnnotationsJSON:   `{"summary":"Network issue"}`,
				StartsAt:          now.Add(-time.Hour),
				LastSeenAt:        now,
				RelevantUpstreams: []string{"am-a", "am-b"},
			},
		},
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true},
		Timestamp:           now,
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(seed) returned error: %v", err)
	}

	_, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{
			{
				AlertID:           "id-rel",
				State:             StateActive,
				Receiver:          "team-a",
				GroupID:           "group-1",
				LabelsJSON:        `{"alertname":"NetIssue"}`,
				AnnotationsJSON:   `{"summary":"Network issue"}`,
				StartsAt:          now.Add(-time.Hour),
				LastSeenAt:        now.Add(time.Minute),
				RelevantUpstreams: []string{"am-a"},
			},
		},
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": false},
		Timestamp:           now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(partial) returned error: %v", err)
	}

	res, err := store.ApplyPollCycle(PollCycleInput{
		LiveAlerts:          nil,
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": false},
		Timestamp:           now.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(absent) returned error: %v", err)
	}
	if res.SetInactive != 0 {
		t.Fatalf("expected SetInactive=0 got=%d", res.SetInactive)
	}
	if res.SkippedInactive != 1 {
		t.Fatalf("expected SkippedInactive=1 got=%d", res.SkippedInactive)
	}

	current, err := store.GetCurrentAlert("id-rel")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if current == nil || current.State != StateActive {
		t.Fatalf("expected state active, got=%v", current)
	}

	res, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts:          nil,
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true},
		Timestamp:           now.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(full coverage absent) returned error: %v", err)
	}
	if res.SetInactive != 1 {
		t.Fatalf("expected SetInactive=1 after coverage restored, got=%d", res.SetInactive)
	}
}

func TestRunRegroupToConfigUpdatesGroupFromCurrentRules(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "id-regroup",
		State:             StateInactive,
		Receiver:          "team-a",
		GroupID:           "legacy-group-id",
		GroupLabelsJSON:   `{"alertname":"CPUHigh","instance":"host-1"}`,
		LabelsJSON:        `{"alertname":"CPUHigh","instance":"host-1"}`,
		AnnotationsJSON:   `{"summary":"CPU high"}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() setup failed: %v", err)
	}

	cfg := GroupingConfig{
		LabelsStrip: []string{"instance"},
	}

	result, err := store.RunRegroupToConfig(context.Background(), cfg, 1000)
	if err != nil {
		t.Fatalf("RunRegroupToConfig() returned error: %v", err)
	}
	if result.Total != 1 {
		t.Fatalf("expected regroup total=1 got=%d", result.Total)
	}
	if result.Processed != 1 {
		t.Fatalf("expected regroup processed=1 got=%d", result.Processed)
	}

	current, err := store.GetCurrentAlert("id-regroup")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if current == nil {
		t.Fatal("expected alert after regroup")
	}

	expectedGroupID, expectedGroupLabelsJSON, err := computeGroupFromAlertLabels("team-a", `{"alertname":"CPUHigh","instance":"host-1"}`, compileMustGroupingConfig(t, cfg))
	if err != nil {
		t.Fatalf("computeGroupFromAlertLabels() returned error: %v", err)
	}
	if current.GroupID != expectedGroupID {
		t.Fatalf("group id mismatch after regroup: got=%q want=%q", current.GroupID, expectedGroupID)
	}
	if current.GroupLabelsJSON != expectedGroupLabelsJSON {
		t.Fatalf("group labels mismatch after regroup: got=%q want=%q", current.GroupLabelsJSON, expectedGroupLabelsJSON)
	}

	stats, err := store.GetStats()
	if err != nil {
		t.Fatalf("GetStats() returned error: %v", err)
	}
	if stats.RegroupStatus != regroupStatusReady {
		t.Fatalf("expected regroup status ready got=%q", stats.RegroupStatus)
	}
	expectedFP := BuildGroupingFingerprint(cfg)
	if stats.GroupingFingerprint != expectedFP {
		t.Fatalf("grouping fingerprint mismatch: got=%q want=%q", stats.GroupingFingerprint, expectedFP)
	}
}

func TestApplyPollCycleRecordsUpdatedAtRevisionWithoutRecheck(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC().Truncate(time.Millisecond)
	input := PollCycleInput{
		LiveAlerts:          nil,
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{
			{
				SourceName: "am-a",
				ClusterID:  "cluster-a",
				SyncStatus: "success",
				Silences: []SourceSilenceSnapshot{
					{
						SilenceID: "sil-1",
						Status:    "active",
						StartsAt:  now.Add(-time.Hour),
						EndsAt:    now.Add(time.Hour),
						CreatedBy: "operator@example.com",
						Comment:   "planned maintenance",
						Matchers:  []SilenceMatcherSnapshot{{Name: "alertname", Value: "CPUHigh", IsRegex: false, IsEqual: true}},
					},
				},
			},
		},
		Timestamp: now,
	}

	res, err := store.ApplyPollCycle(input)
	if err != nil {
		t.Fatalf("ApplyPollCycle() returned error: %v", err)
	}
	if res.RecheckJobsQueued != 1 {
		t.Fatalf("expected first silence sync to queue 1 job, got=%d", res.RecheckJobsQueued)
	}

	input.Timestamp = now.Add(2 * time.Minute)
	updated := input.SilenceSources[0].Silences[0]
	updatedAt := now.Add(2 * time.Minute)
	updated.UpdatedAt = &updatedAt
	input.SilenceSources[0].Silences = []SourceSilenceSnapshot{updated}
	res, err = store.ApplyPollCycle(input)
	if err != nil {
		t.Fatalf("ApplyPollCycle() second run returned error: %v", err)
	}
	if res.RecheckJobsQueued != 0 {
		t.Fatalf("updatedAt-only change must not queue a promotion scan, got=%d", res.RecheckJobsQueued)
	}
	var revisions int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_revisions`).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if revisions != 2 {
		t.Fatalf("updatedAt change must append a revision, got=%d", revisions)
	}
}

func TestRunSilenceRecheckBatchPromotesInactiveAndAddsCause(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC().Truncate(time.Millisecond)
	input := PollCycleInput{
		LiveAlerts:          nil,
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{
			{
				SourceName: "am-a",
				ClusterID:  "cluster-a",
				SyncStatus: "success",
				Silences: []SourceSilenceSnapshot{
					{
						SilenceID: "sil-1",
						Status:    "active",
						StartsAt:  now.Add(-time.Hour),
						EndsAt:    now.Add(time.Hour),
						CreatedBy: "operator@example.com",
						Comment:   "planned maintenance",
						Matchers:  []SilenceMatcherSnapshot{{Name: "alertname", Value: "CPUHigh", IsRegex: false, IsEqual: true}},
					},
				},
			},
		},
		Timestamp: now,
	}
	if _, err = store.ApplyPollCycle(input); err != nil {
		t.Fatalf("ApplyPollCycle() setup returned error: %v", err)
	}

	matchLabels := `{"alertname":"CPUHigh"}`
	relevantSources := `[{"name":"am-a","cluster":"cluster-a"}]`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "inactive-a",
		State:             StateInactive,
		RawState:          StateActive,
		IsLive:            false,
		IsSuppressed:      false,
		IsResolved:        false,
		Receiver:          "team-a",
		GroupID:           "grp-a",
		GroupLabelsJSON:   `{"alertname":"CPUHigh"}`,
		LabelsJSON:        matchLabels,
		MatchLabelsJSON:   &matchLabels,
		AnnotationsJSON:   `{}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
		RelevantSources:   relevantSources,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() returned error: %v", err)
	}

	promoted, err := store.RunSilenceRecheckBatch(1000)
	if err != nil {
		t.Fatalf("RunSilenceRecheckBatch() returned error: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("expected promoted=1, got=%d", promoted)
	}

	got, err := store.GetCurrentAlert("inactive-a")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if got == nil || got.State != StateSuppressed || !got.IsSuppressed {
		t.Fatalf("expected alert promoted to suppressed with is_suppressed=true, got=%+v", got)
	}

	var causes int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_suppression_cause_history WHERE alert_id = ? AND source = 'silence_recheck'`, "inactive-a").Scan(&causes); err != nil {
		t.Fatalf("failed to count suppression causes: %v", err)
	}
	if causes != 1 {
		t.Fatalf("expected 1 suppression cause history record, got=%d", causes)
	}
	listed, err := store.ListCurrentAlertsForMerge(false)
	if err != nil {
		t.Fatalf("ListCurrentAlertsForMerge() returned error: %v", err)
	}
	if len(listed) != 1 || len(listed[0].SuppressionCauses) != 1 {
		t.Fatalf("expected one effective suppression cause, got=%+v", listed)
	}
	cause := listed[0].SuppressionCauses[0]
	if cause.SourceName != "am-a" || cause.ClusterID != "cluster-a" || cause.SilenceID != "sil-1" {
		t.Fatalf("unexpected effective suppression cause: %+v", cause)
	}
	if cause.ResolutionStatus != "known" || cause.CreatedBy != "operator@example.com" || cause.Comment != "planned maintenance" || len(cause.Matchers) != 1 || cause.Matchers[0].Name != "alertname" {
		t.Fatalf("effective cause lost immutable revision details: %+v", cause)
	}
}

func TestListCurrentAlertsForMergeReturnsLatestLiveSuppressionCause(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	live := LiveAlertSnapshot{
		AlertID: "live-cause-card", State: StateSuppressed, Receiver: "team", GroupID: "group",
		GroupLabelsJSON: labels, LabelsJSON: labels, MatchLabelsJSON: &labels,
		AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now,
		RelevantUpstreams: []string{"am-a"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-a"}},
		Targets:           []LiveTargetSnapshot{{Target: "cluster-a", State: StateSuppressed, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-a"}}},
		SilencedBySources: []SourceSilenceRef{{SourceName: "am-a", ClusterID: "cluster-a", SilenceIDs: []string{"sil-live"}}},
	}
	source := SourceSilenceState{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{{
		SilenceID: "sil-live", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}}}
	if _, err = store.ApplyPollCycle(PollCycleInput{LiveAlerts: []LiveAlertSnapshot{live}, SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{source}, Timestamp: now}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{source}, Timestamp: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	listed, err := store.ListCurrentAlertsForMerge(false)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || len(listed[0].SuppressionCauses) != 1 || listed[0].SuppressionCauses[0].SilenceID != "sil-live" {
		t.Fatalf("expected live cause to survive disappearance, got=%+v", listed)
	}
}

func TestListCurrentAlertsForMergeReturnsUnresolvedCauseWithoutInventedDetails(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	live := LiveAlertSnapshot{
		AlertID: "unresolved-cause-card", State: StateSuppressed,
		Receiver: "team", GroupID: "group", GroupLabelsJSON: labels, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now,
		RelevantUpstreams: []string{"am-a"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-a"}},
		Targets:           []LiveTargetSnapshot{{Target: "cluster-a", State: StateSuppressed, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-a"}}},
		SilencedBySources: []SourceSilenceRef{{SourceName: "am-a", ClusterID: "cluster-a", SilenceIDs: []string{"sil-unresolved"}}},
	}
	source := SourceSilenceState{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success"}
	if _, err = store.ApplyPollCycle(PollCycleInput{LiveAlerts: []LiveAlertSnapshot{live}, SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{source}, Timestamp: now}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{source}, Timestamp: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	listed, err := store.ListCurrentAlertsForMerge(false)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || len(listed[0].SuppressionCauses) != 1 {
		t.Fatalf("expected one unresolved effective cause, got=%+v", listed)
	}
	cause := listed[0].SuppressionCauses[0]
	if cause.ResolutionStatus != "unresolved" || cause.SilenceID != "sil-unresolved" || cause.StartsAt != nil || cause.EndsAt != nil || cause.CreatedBy != "" || cause.Comment != "" || len(cause.Matchers) != 0 {
		t.Fatalf("unresolved cause contains invented details: %+v", cause)
	}
}

func TestRunHousekeepingCleansCompletedUnreferencedJobs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i := 0; i < 2; i++ {
		fp := "sha256:test-" + string(rune('a'+i))
		if _, err = store.db.Exec(`
INSERT INTO silence_recheck_jobs(context_fingerprint, open_context_fingerprint, source_context_json, trigger_type, status, cursor_alert_id, created_at, finished_at)
VALUES(?, NULL, '{"formatVersion":1}', 'source_changed', 'completed', NULL, ?, ?)
`, fp, now, now); err != nil {
			t.Fatalf("failed to insert completed job: %v", err)
		}
	}

	if _, err = store.RunHousekeeping(24*time.Hour, 100000); err != nil {
		t.Fatalf("RunHousekeeping() returned error: %v", err)
	}

	var count int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs WHERE status='completed'`).Scan(&count); err != nil {
		t.Fatalf("failed counting remaining completed jobs: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 completed unreferenced job after cleanup, got=%d", count)
	}
}

func TestRunHousekeepingPreservesRulesOfLatestCompletedJob(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	if _, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{
			SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success",
			Silences: []SourceSilenceSnapshot{{
				SilenceID: "retained-job-rule", Status: "active",
				StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
			}},
		}}, Timestamp: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RunSilenceRecheckBatch(100); err != nil {
		t.Fatal(err)
	}
	old := now.Add(-60 * 24 * time.Hour).Format(time.RFC3339Nano)
	if _, err = store.db.Exec(`UPDATE silence_recheck_jobs SET finished_at=? WHERE status='completed'`, old); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RunHousekeeping(24*time.Hour, 100000); err != nil {
		t.Fatal(err)
	}
	var jobs, rules int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_rules`).Scan(&rules); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || rules != 1 {
		t.Fatalf("latest completed job lost immutable input: jobs=%d rules=%d", jobs, rules)
	}
}

func TestRunHousekeepingCascadesSupersededJobGraphAndPreservesLatestGraph(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	old := now.Add(-60 * 24 * time.Hour).Format(time.RFC3339Nano)
	nowRaw := now.Format(time.RFC3339Nano)
	if _, err = store.db.Exec(`
INSERT INTO alert_states(
  alert_id,state,raw_state,is_live,is_suppressed,is_resolved,receiver,group_id,
  group_labels_json,labels_json,match_labels_json,annotations_json,starts_at,
  last_seen_at,relevant_upstreams_json,relevant_sources_json,updated_at
) VALUES('job-child-alert','inactive','active',0,0,0,'team','group','{}','{}','{}','{}',?,?, '[]','[]',?)
`, nowRaw, nowRaw, nowRaw); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`
INSERT INTO alertmanager_sources(
  source_name,current_cluster_id,sync_status,first_configured_at,last_configured_at
) VALUES('am-a','cluster-a','success',?,?)
`, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`
INSERT INTO silence_rules(cluster_id,silence_id,first_observed_at,last_observed_at)
VALUES('cluster-a','rule-a',?,?)
`, old, old); err != nil {
		t.Fatal(err)
	}
	var ruleID int64
	if err = store.db.QueryRow(`SELECT rule_id FROM silence_rules WHERE silence_id='rule-a' AND cluster_id='cluster-a'`).Scan(&ruleID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`
INSERT INTO silence_rule_episodes(rule_id,episode_number,first_observed_at,last_confirmed_at,open_marker)
VALUES(?,1,?,?,1)
`, ruleID, old, old); err != nil {
		t.Fatal(err)
	}
	var episodeID int64
	if err = store.db.QueryRow(`SELECT episode_id FROM silence_rule_episodes WHERE rule_id=?`, ruleID).Scan(&episodeID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`
INSERT INTO silence_revisions(
  episode_id,rule_id,revision_number,content_fingerprint,evaluation_fingerprint,
  matchers_json,starts_at,ends_at,created_by,comment,first_observed_at,last_observed_at
) VALUES(?,?,1,'sha256:revision','sha256:evaluation','[]',?,?, '', '', ?,?)
`, episodeID, ruleID, old, old, old, old); err != nil {
		t.Fatal(err)
	}
	var revisionID int64
	if err = store.db.QueryRow(`SELECT revision_id FROM silence_revisions WHERE rule_id=?`, ruleID).Scan(&revisionID); err != nil {
		t.Fatal(err)
	}
	for _, job := range []struct {
		fingerprint string
		finishedAt  string
	}{
		{fingerprint: "sha256:old-job", finishedAt: old},
		{fingerprint: "sha256:latest-job", finishedAt: now.Format(time.RFC3339Nano)},
	} {
		res, insertErr := store.db.Exec(`
INSERT INTO silence_recheck_jobs(
  context_fingerprint,source_context_json,trigger_type,status,created_at,finished_at
) VALUES(?, '{"formatVersion":1,"sources":[]}', 'source_changed','completed',?,?)
`, job.fingerprint, job.finishedAt, job.finishedAt)
		if insertErr != nil {
			t.Fatal(insertErr)
		}
		jobID, _ := res.LastInsertId()
		if _, err = store.db.Exec(`
INSERT INTO silence_recheck_job_rules(job_id,source_name,cluster_id,rule_id,revision_id,observed_state)
VALUES(?,?,?,?,?,'active')
`, jobID, "am-a", "cluster-a", ruleID, revisionID); err != nil {
			t.Fatal(err)
		}
		if _, err = store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,processed_at) VALUES(?,?,?)`, jobID, "job-child-alert", job.finishedAt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = store.RunHousekeeping(24*time.Hour, 100000); err != nil {
		t.Fatal(err)
	}
	var jobs, rules, candidates int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_rules`).Scan(&rules); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates`).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || rules != 1 || candidates != 1 {
		t.Fatalf("job graph retention mismatch: jobs=%d rules=%d candidates=%d", jobs, rules, candidates)
	}
	var fingerprint string
	if err = store.db.QueryRow(`SELECT context_fingerprint FROM silence_recheck_jobs`).Scan(&fingerprint); err != nil {
		t.Fatal(err)
	}
	if fingerprint != "sha256:latest-job" {
		t.Fatalf("retained wrong job graph: %s", fingerprint)
	}
}

func TestRunSilenceRecheckBatchRequiresAllRelevantSourcesToMatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC().Truncate(time.Millisecond)
	input := PollCycleInput{
		LiveAlerts:          nil,
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true},
		SilenceSources: []SourceSilenceState{
			{
				SourceName: "am-a",
				ClusterID:  "cluster-a",
				SyncStatus: "success",
				Silences: []SourceSilenceSnapshot{
					{
						SilenceID: "sil-1",
						Status:    "active",
						StartsAt:  now.Add(-time.Hour),
						EndsAt:    now.Add(time.Hour),
						Matchers:  []SilenceMatcherSnapshot{{Name: "alertname", Value: "CPUHigh", IsRegex: false, IsEqual: true}},
					},
				},
			},
			{
				SourceName: "am-b",
				ClusterID:  "cluster-b",
				SyncStatus: "success",
				Silences:   nil,
			},
		},
		Timestamp: now,
	}
	if _, err = store.ApplyPollCycle(input); err != nil {
		t.Fatalf("ApplyPollCycle() setup returned error: %v", err)
	}

	matchLabels := `{"alertname":"CPUHigh"}`
	relevantSources := `[{"name":"am-a","cluster":"cluster-a"},{"name":"am-b","cluster":"cluster-b"}]`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "inactive-multi",
		State:             StateInactive,
		RawState:          StateActive,
		IsLive:            false,
		IsSuppressed:      false,
		IsResolved:        false,
		Receiver:          "team-a",
		GroupID:           "grp-a",
		GroupLabelsJSON:   `{"alertname":"CPUHigh"}`,
		LabelsJSON:        matchLabels,
		MatchLabelsJSON:   &matchLabels,
		AnnotationsJSON:   `{}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a","am-b"]`,
		RelevantSources:   relevantSources,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() returned error: %v", err)
	}

	promoted, err := store.RunSilenceRecheckBatch(1000)
	if err != nil {
		t.Fatalf("RunSilenceRecheckBatch() returned error: %v", err)
	}
	if promoted != 0 {
		t.Fatalf("expected promoted=0 when only one relevant source matches, got=%d", promoted)
	}

	got, err := store.GetCurrentAlert("inactive-multi")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if got == nil || got.State != StateInactive || got.IsSuppressed {
		t.Fatalf("expected alert to remain inactive unsuppressed, got=%+v", got)
	}
	var skippedIncomplete, skippedIneligible int
	if err = store.db.QueryRow(`
SELECT skipped_incomplete_sources, skipped_ineligible
FROM silence_recheck_jobs ORDER BY job_id DESC LIMIT 1
`).Scan(&skippedIncomplete, &skippedIneligible); err != nil {
		t.Fatal(err)
	}
	if skippedIncomplete != 0 || skippedIneligible != 1 {
		t.Fatalf("successful source with no matching rule is a complete non-match: incomplete=%d ineligible=%d", skippedIncomplete, skippedIneligible)
	}
}

func TestRunSilenceRecheckBatchPromotesOnlyMatchedTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC().Truncate(time.Millisecond)
	input := PollCycleInput{
		LiveAlerts:          nil,
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true},
		SilenceSources: []SourceSilenceState{
			{
				SourceName: "am-a",
				ClusterID:  "cluster-a",
				SyncStatus: "success",
				Silences: []SourceSilenceSnapshot{
					{
						SilenceID: "sil-1",
						Status:    "active",
						StartsAt:  now.Add(-time.Hour),
						EndsAt:    now.Add(time.Hour),
						Matchers: []SilenceMatcherSnapshot{{
							Name:    "alertname",
							Value:   "CPUHigh",
							IsRegex: false,
							IsEqual: true,
						}},
					},
				},
			},
			{
				SourceName: "am-b",
				ClusterID:  "cluster-b",
				SyncStatus: "success",
				Silences:   nil,
			},
		},
		Timestamp: now,
	}
	if _, err = store.ApplyPollCycle(input); err != nil {
		t.Fatalf("ApplyPollCycle() setup returned error: %v", err)
	}

	matchLabels := `{"alertname":"CPUHigh"}`
	relevantSources := `[{"name":"am-a","cluster":"cluster-a"},{"name":"am-b","cluster":"cluster-b"}]`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "inactive-target-split",
		State:             StateActive,
		RawState:          StateActive,
		IsLive:            true,
		IsSuppressed:      false,
		IsResolved:        false,
		Receiver:          "team-a",
		GroupID:           "grp-a",
		GroupLabelsJSON:   `{"alertname":"CPUHigh"}`,
		LabelsJSON:        matchLabels,
		MatchLabelsJSON:   &matchLabels,
		AnnotationsJSON:   `{}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a","am-b"]`,
		RelevantSources:   relevantSources,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() returned error: %v", err)
	}

	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}
	err = upsertTargetStateTx(tx, "inactive-target-split", targetStateRow{
		Target:        "cluster-a",
		State:         StateInactive,
		RawState:      StateActive,
		IsLive:        false,
		IsSuppressed:  false,
		IsResolved:    false,
		StartsAt:      now.Add(-time.Hour),
		LastSeenAt:    &now,
		SourceMembers: []string{"am-a"},
	}, "seed", now)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("failed to seed target cluster-a: %v", err)
	}
	err = upsertTargetStateTx(tx, "inactive-target-split", targetStateRow{
		Target:        "cluster-b",
		State:         StateActive,
		RawState:      StateActive,
		IsLive:        true,
		IsSuppressed:  false,
		IsResolved:    false,
		StartsAt:      now.Add(-time.Hour),
		LastSeenAt:    &now,
		SourceMembers: []string{"am-b"},
	}, "seed", now)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("failed to seed target cluster-b: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("failed to commit seed tx: %v", err)
	}

	promoted, err := store.RunSilenceRecheckBatch(1000)
	if err != nil {
		t.Fatalf("RunSilenceRecheckBatch() returned error: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("expected promoted=1 for target-only promotion, got=%d", promoted)
	}
	if _, err = store.RunSilenceRecheckBatch(1000); err != nil {
		t.Fatalf("complete first target recheck job: %v", err)
	}

	global, err := store.GetCurrentAlert("inactive-target-split")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if global == nil {
		t.Fatal("expected global alert row")
	}
	if global.State != StateActive || !global.IsLive || global.IsSuppressed {
		t.Fatalf("expected global alert to remain live active and unsuppressed, got=%+v", global)
	}

	rows, err := store.ListTargetAlertsForPreview("cluster-a")
	if err != nil {
		t.Fatalf("ListTargetAlertsForPreview(cluster-a) returned error: %v", err)
	}
	if len(rows) != 1 || rows[0].State != StateSuppressed || !rows[0].IsSuppressed {
		t.Fatalf("expected cluster-a target suppressed, got=%+v", rows)
	}
	var targetCauseCount int
	if err = store.db.QueryRow(`
SELECT COUNT(*)
FROM alert_target_suppression_cause_history
WHERE alert_id='inactive-target-split' AND target='cluster-a' AND source='silence_recheck'
`).Scan(&targetCauseCount); err != nil {
		t.Fatalf("failed to count target suppression causes: %v", err)
	}
	if targetCauseCount != 1 {
		t.Fatalf("expected one immutable target suppression cause, got=%d", targetCauseCount)
	}

	rows, err = store.ListTargetAlertsForPreview("cluster-b")
	if err != nil {
		t.Fatalf("ListTargetAlertsForPreview(cluster-b) returned error: %v", err)
	}
	if len(rows) != 1 || rows[0].State != StateActive || !rows[0].IsLive || rows[0].IsSuppressed {
		t.Fatalf("expected cluster-b target to remain live active and unsuppressed, got=%+v", rows)
	}
	secondPoll := now.Add(time.Minute)
	secondResult, err := store.ApplyPollCycle(PollCycleInput{
		LiveAlerts:          nil,
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true},
		SilenceSources: []SourceSilenceState{
			{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{{
				SilenceID: "sil-a", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []SilenceMatcherSnapshot{{Name: "alertname", Value: "CPUHigh", IsEqual: true}},
			}}},
			{SourceName: "am-b", ClusterID: "cluster-b", SyncStatus: "success", Silences: []SourceSilenceSnapshot{{
				SilenceID: "sil-b", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []SilenceMatcherSnapshot{{Name: "alertname", Value: "CPUHigh", IsEqual: true}},
			}}},
		},
		Timestamp: secondPoll,
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle() second target setup returned error: %v", err)
	}
	if secondResult.RecheckJobsQueued != 1 {
		t.Fatalf("expected one second-target recheck job, got=%+v", secondResult)
	}
	beforeSecond, _ := store.GetCurrentAlert("inactive-target-split")
	if beforeSecond == nil || beforeSecond.State != StateInactive {
		t.Fatalf("expected global inactive candidate before second recheck, got=%+v", beforeSecond)
	}
	probeTx, _ := store.db.Begin()
	probeJobID, _, probeStatus, probeErr := selectNextRecheckJobTx(probeTx)
	_ = probeTx.Rollback()
	if probeErr != nil || probeJobID == 0 || probeStatus != "queued" {
		t.Fatalf("expected queued second job before worker: id=%d status=%q err=%v", probeJobID, probeStatus, probeErr)
	}
	promoted, err = store.RunSilenceRecheckBatch(1000)
	if err != nil {
		t.Fatalf("RunSilenceRecheckBatch() second target returned error: %v", err)
	}
	if promoted != 1 {
		var jobStates string
		_ = store.db.QueryRow(`SELECT group_concat(status || ':' || processed_candidates || ':' || promoted_alerts, ',') FROM silence_recheck_jobs ORDER BY job_id`).Scan(&jobStates)
		var ruleCounts string
		_ = store.db.QueryRow(`SELECT group_concat(job_id || ':' || (SELECT COUNT(*) FROM silence_recheck_job_rules r WHERE r.job_id=j.job_id), ',') FROM silence_recheck_jobs j ORDER BY job_id`).Scan(&ruleCounts)
		bRows, _ := store.ListTargetAlertsForPreview("cluster-b")
		t.Fatalf("expected global promotion after second target cause, got=%d jobs=%s rules=%s targetB=%+v", promoted, jobStates, ruleCounts, bRows)
	}
	global, err = store.GetCurrentAlert("inactive-target-split")
	if err != nil || global == nil || global.State != StateSuppressed || !global.IsSuppressed {
		t.Fatalf("expected aggregate global suppression, got=%+v err=%v", global, err)
	}
	var aggregateLinks int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_target_aggregate_suppression_links`).Scan(&aggregateLinks); err != nil {
		t.Fatalf("count aggregate target causes: %v", err)
	}
	if aggregateLinks != 2 {
		t.Fatalf("expected aggregate links to effective causes from both jobs, got=%d", aggregateLinks)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store before target evidence restart: %v", err)
	}
	reopened, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("reopen store for target evidence restart: %v", err)
	}
	store = reopened
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_target_suppression_cause_history WHERE alert_id='inactive-target-split'`).Scan(&targetCauseCount); err != nil {
		t.Fatalf("count target causes after restart: %v", err)
	}
	if targetCauseCount != 2 {
		t.Fatalf("expected both target causes to survive restart, got=%d", targetCauseCount)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_target_aggregate_suppression_links`).Scan(&aggregateLinks); err != nil {
		t.Fatalf("count aggregate links after restart: %v", err)
	}
	if aggregateLinks != 2 {
		t.Fatalf("expected aggregate evidence links to survive restart, got=%d", aggregateLinks)
	}
	if _, err = store.db.Exec(`UPDATE alert_states SET last_seen_at='2000-01-01T00:00:00Z' WHERE alert_id='inactive-target-split'`); err != nil {
		t.Fatal(err)
	}
	housekeeping, err := store.RunHousekeeping(time.Hour, 100000)
	if err != nil {
		t.Fatalf("aggregate-promoted alert must not block housekeeping: %v", err)
	}
	if housekeeping.ExpiredDeleted != 1 {
		t.Fatalf("expected aggregate-promoted alert TTL deletion, got=%+v", housekeeping)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_target_aggregate_suppression_links`).Scan(&aggregateLinks); err != nil {
		t.Fatal(err)
	}
	if aggregateLinks != 0 {
		t.Fatalf("aggregate links must cascade during alert retention, got=%d", aggregateLinks)
	}
}

func TestTargetLabelStatsQueryUsesTargetValueIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	defer store.Close()
	rows, err := store.db.Query(`
EXPLAIN QUERY PLAN
SELECT label_value, COUNT(*) AS hits
FROM alert_target_match_labels
WHERE target = ? AND label_name = ?
  AND (? = '' OR substr(lower(label_value), 1, length(?)) = ?)
GROUP BY label_value
ORDER BY hits DESC, label_value ASC
LIMIT ?
`, "cluster-a", "service", "api", "api", "api", 51)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()
	usedIndex := false
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err = rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan query plan: %v", err)
		}
		if strings.Contains(detail, "idx_alert_target_match_labels_target_name_value") {
			usedIndex = true
		}
	}
	if !usedIndex {
		t.Fatal("target label value query did not use target/name/value index")
	}
}

func TestSilenceHAConsistencyPersistsUntilSuccessfulConvergence(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	source := func(name, id, value, status string) SourceSilenceState {
		out := SourceSilenceState{SourceName: name, ClusterID: "cluster-ha", SyncStatus: status}
		if status == "success" {
			out.Silences = []SourceSilenceSnapshot{{
				SilenceID: id, Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: value, IsEqual: true}},
			}}
		}
		return out
	}
	apply := func(at time.Time, sources ...SourceSilenceState) {
		t.Helper()
		successful := map[string]bool{}
		for _, source := range sources {
			successful[source.SourceName] = source.SyncStatus == "success"
		}
		if _, applyErr := store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: successful, SilenceSources: sources, Timestamp: at}); applyErr != nil {
			t.Fatalf("ApplyPollCycle(): %v", applyErr)
		}
	}
	apply(now, source("am-a", "id-a", "api", "success"), source("am-b", "different-id", "api", "success"))
	stats, _ := store.GetStats()
	if !slices.Equal(stats.SilenceInconsistentClusters, []string{"cluster-ha"}) {
		t.Fatalf("different HA silence identities must be inconsistent: %+v", stats.SilenceInconsistentClusters)
	}
	apply(now.Add(time.Minute), source("am-a", "id-a", "api", "success"), source("am-b", "id-a", "api", "success"))
	stats, _ = store.GetStats()
	if len(stats.SilenceInconsistentClusters) != 0 {
		t.Fatalf("same identity and payload must converge: %+v", stats.SilenceInconsistentClusters)
	}
	apply(now.Add(2*time.Minute), source("am-a", "id-a", "api", "success"), source("am-b", "id-a", "payments", "success"))
	stats, _ = store.GetStats()
	if !slices.Equal(stats.SilenceInconsistentClusters, []string{"cluster-ha"}) {
		t.Fatalf("expected cluster inconsistency, got=%+v", stats.SilenceInconsistentClusters)
	}
	apply(now.Add(3*time.Minute), source("am-a", "id-a", "api", "success"), source("am-b", "id-a", "", "failed"))
	stats, _ = store.GetStats()
	if len(stats.SilenceInconsistentClusters) != 1 {
		t.Fatalf("failed coverage must preserve last confirmed inconsistency: %+v", stats.SilenceInconsistentClusters)
	}
	apply(now.Add(4*time.Minute), source("am-a", "id-a", "api", "success"), source("am-b", "id-a", "api", "success"))
	stats, _ = store.GetStats()
	if len(stats.SilenceInconsistentClusters) != 0 {
		t.Fatalf("successful convergence must clear inconsistency: %+v", stats.SilenceInconsistentClusters)
	}
	logged := logOutput.String()
	for _, expected := range []string{`"level":"WARN"`, `"event":"ha_disagreement_detected"`, `"target":"cluster-ha"`, `"level":"DEBUG"`, `"event":"ha_disagreement_unchanged"`, `"level":"INFO"`, `"event":"ha_converged"`} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("missing HA consistency log field %s in %s", expected, logged)
		}
	}
}

func TestApplyPollCycleQueuesRecheckForEveryRecoveryStatus(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	successSource := func() []SourceSilenceState {
		return []SourceSilenceState{{
			SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success",
			Silences: []SourceSilenceSnapshot{{
				SilenceID: "sil-a", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
			}},
		}}
	}
	result, err := store.ApplyPollCycle(PollCycleInput{SilenceSources: successSource(), Timestamp: now})
	if err != nil || result.RecheckJobsQueued != 1 {
		t.Fatalf("initial source job: result=%+v err=%v", result, err)
	}
	if _, err = store.RunSilenceRecheckBatch(100); err != nil {
		t.Fatal(err)
	}
	var confirmedFingerprint string
	if err = store.db.QueryRow(`SELECT silence_set_fingerprint FROM alertmanager_sources WHERE source_name='am-a'`).Scan(&confirmedFingerprint); err != nil {
		t.Fatal(err)
	}
	for index, priorStatus := range []string{"failed", "invalid", "unknown", "retired"} {
		failureAt := now.Add(time.Duration(index*2+1) * time.Minute)
		result, err = store.ApplyPollCycle(PollCycleInput{
			SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: priorStatus}},
			Timestamp:      failureAt,
		})
		if err != nil || result.RecheckJobsQueued != 0 {
			t.Fatalf("%s observation must not enqueue: result=%+v err=%v", priorStatus, result, err)
		}
		var retainedFingerprint string
		var retainedRelations, revisions int
		if err = store.db.QueryRow(`SELECT silence_set_fingerprint FROM alertmanager_sources WHERE source_name='am-a'`).Scan(&retainedFingerprint); err != nil {
			t.Fatal(err)
		}
		if err = store.db.QueryRow(`SELECT COUNT(*) FROM alertmanager_source_silences WHERE source_name='am-a'`).Scan(&retainedRelations); err != nil {
			t.Fatal(err)
		}
		if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_revisions`).Scan(&revisions); err != nil {
			t.Fatal(err)
		}
		if retainedFingerprint != confirmedFingerprint || retainedRelations != 1 || revisions != 1 {
			t.Fatalf("%s must preserve confirmed set/revision: fp=%q relations=%d revisions=%d", priorStatus, retainedFingerprint, retainedRelations, revisions)
		}
		result, err = store.ApplyPollCycle(PollCycleInput{SilenceSources: successSource(), Timestamp: failureAt.Add(time.Minute)})
		if err != nil || result.RecheckJobsQueued != 1 {
			t.Fatalf("%s->success must enqueue: result=%+v err=%v", priorStatus, result, err)
		}
		if _, err = store.RunSilenceRecheckBatch(100); err != nil {
			t.Fatalf("run %s recovery job: %v", priorStatus, err)
		}
	}
	var jobs int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobs); err != nil || jobs != 5 {
		t.Fatalf("expected initial plus four recovery jobs, got=%d err=%v", jobs, err)
	}
}

func TestRecheckSkipsStaleRulesFromFailedSourceUntilRecovery(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	baseSilence := func(id string, matchers []SilenceMatcherSnapshot) SourceSilenceSnapshot {
		return SourceSilenceSnapshot{
			SilenceID: id, Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: matchers,
		}
	}
	source := func(name, status string, silence SourceSilenceSnapshot) SourceSilenceState {
		out := SourceSilenceState{SourceName: name, ClusterID: "cluster-ha", SyncStatus: status}
		if status == "success" {
			out.Silences = []SourceSilenceSnapshot{silence}
		}
		return out
	}
	aInitial := baseSilence("sil-a", []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}})
	bSilence := baseSilence("sil-b", []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}})
	if _, err = store.ApplyPollCycle(PollCycleInput{
		SilenceSources: []SourceSilenceState{source("am-a", "success", aInitial), source("am-b", "success", bSilence)}, Timestamp: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RunSilenceRecheckBatch(100); err != nil {
		t.Fatal(err)
	}
	labels := `{"service":"api","team":"payments"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "failed-source-candidate", State: StateInactive, RawState: StateActive,
		IsLive: false, IsSuppressed: false, IsResolved: false,
		Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`,
		LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
		RelevantUpstreams: `["am-a","am-b"]`,
		RelevantSources:   `[{"name":"am-a","cluster":"cluster-ha"},{"name":"am-b","cluster":"cluster-ha"}]`,
	}); err != nil {
		t.Fatal(err)
	}
	aChanged := baseSilence("sil-a", []SilenceMatcherSnapshot{
		{Name: "service", Value: "api", IsEqual: true},
		{Name: "team", Value: "payments", IsEqual: true},
	})
	if result, applyErr := store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": false},
		SilenceSources:      []SourceSilenceState{source("am-a", "success", aChanged), source("am-b", "failed", bSilence)},
		Timestamp:           now.Add(time.Minute),
	}); applyErr != nil || result.RecheckJobsQueued != 1 {
		t.Fatalf("failed-source context was not queued: result=%+v err=%v", result, applyErr)
	}
	if promoted, runErr := store.RunSilenceRecheckBatch(100); runErr != nil || promoted != 0 {
		t.Fatalf("stale failed-source rule promoted candidate: promoted=%d err=%v", promoted, runErr)
	}
	current, err := store.GetCurrentAlert("failed-source-candidate")
	if err != nil || current == nil || current.IsSuppressed || current.State != StateInactive {
		t.Fatalf("candidate changed before source recovery: current=%+v err=%v", current, err)
	}
	var incomplete int
	if err = store.db.QueryRow(`SELECT skipped_incomplete_sources FROM silence_recheck_jobs ORDER BY job_id DESC LIMIT 1`).Scan(&incomplete); err != nil {
		t.Fatal(err)
	}
	if incomplete != 1 {
		t.Fatalf("failed source must be counted as incomplete, got=%d", incomplete)
	}
	if _, err = store.RunSilenceRecheckBatch(100); err != nil {
		t.Fatal(err)
	}
	if result, applyErr := store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true},
		SilenceSources:      []SourceSilenceState{source("am-a", "success", aChanged), source("am-b", "success", bSilence)},
		Timestamp:           now.Add(2 * time.Minute),
	}); applyErr != nil || result.RecheckJobsQueued != 1 {
		t.Fatalf("source recovery did not queue successor: result=%+v err=%v", result, applyErr)
	}
	if promoted, runErr := store.RunSilenceRecheckBatch(100); runErr != nil || promoted != 1 {
		t.Fatalf("recovered complete source set did not promote: promoted=%d err=%v", promoted, runErr)
	}
}

func TestEvaluateRecheckRulesExhaustiveSourceStatusProduct(t *testing.T) {
	statuses := []string{"success", "failed", "invalid", "unknown", "retired"}
	bools := []bool{false, true}
	labels := map[string]string{"service": "api"}
	for _, statusA := range statuses {
		for _, statusB := range statuses {
			for _, ruleA := range bools {
				for _, ruleB := range bools {
					for _, matchA := range bools {
						for _, matchB := range bools {
							rules := []recheckJobRule{}
							if ruleA {
								value := "other"
								if matchA {
									value = "api"
								}
								rules = append(rules, recheckJobRule{SourceName: "a", ObservedState: "active", MatchersJSON: `[{"name":"service","value":"` + value + `","isEqual":true}]`})
							}
							if ruleB {
								value := "other"
								if matchB {
									value = "api"
								}
								rules = append(rules, recheckJobRule{SourceName: "b", ObservedState: "active", MatchersJSON: `[{"name":"service","value":"` + value + `","isEqual":true}]`})
							}
							matched, complete := evaluateRecheckRules(rules, map[string]string{"a": statusA, "b": statusB}, nil, map[string]struct{}{"a": {}, "b": {}}, labels)
							statusComplete := func(status string) bool { return status == "success" || status == "retired" }
							wantComplete := statusComplete(statusA) && statusComplete(statusB)
							wantMatch := wantComplete && ((statusA != "retired" && ruleA && matchA) || (statusB != "retired" && ruleB && matchB)) && (statusA == "retired" || (ruleA && matchA)) && (statusB == "retired" || (ruleB && matchB))
							if complete != wantComplete || (len(matched) > 0) != wantMatch {
								t.Fatalf("statuses=(%s,%s) rules=(%v,%v) matches=(%v,%v): matched=%d complete=%v wantMatch=%v wantComplete=%v", statusA, statusB, ruleA, ruleB, matchA, matchB, len(matched), complete, wantMatch, wantComplete)
							}
						}
					}
				}
			}
		}
	}
}

func TestRecheckEnqueueUsesPromotionCapableEvaluationChangesOnly(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	updatedA := now.Add(-time.Minute)
	updatedB := now
	base := SourceSilenceSnapshot{
		SilenceID: "sil-a", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), UpdatedAt: &updatedA,
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}}, Comment: "first",
	}
	apply := func(at time.Time, silences ...SourceSilenceSnapshot) PollCycleResult {
		t.Helper()
		result, applyErr := store.ApplyPollCycle(PollCycleInput{
			SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: silences}},
			Timestamp:      at,
		})
		if applyErr != nil {
			t.Fatalf("ApplyPollCycle(): %v", applyErr)
		}
		return result
	}
	completeJob := func() {
		t.Helper()
		if _, runErr := store.RunSilenceRecheckBatch(100); runErr != nil {
			t.Fatal(runErr)
		}
	}
	if got := apply(now, base).RecheckJobsQueued; got != 1 {
		t.Fatalf("new active rule must enqueue, got=%d", got)
	}
	completeJob()
	base.UpdatedAt = &updatedB
	if got := apply(now.Add(time.Minute), base).RecheckJobsQueued; got != 0 {
		t.Fatalf("updatedAt-only change must not enqueue, got=%d", got)
	}
	base.Comment = "display-only change"
	if got := apply(now.Add(2*time.Minute), base).RecheckJobsQueued; got != 0 {
		t.Fatalf("display-only change must not enqueue, got=%d", got)
	}
	if got := apply(now.Add(3 * time.Minute)).RecheckJobsQueued; got != 0 {
		t.Fatalf("deletion must not enqueue, got=%d", got)
	}
	pending := base
	pending.SilenceID = "sil-pending"
	pending.Status = "pending"
	if got := apply(now.Add(4*time.Minute), pending).RecheckJobsQueued; got != 0 {
		t.Fatalf("new pending rule must not enqueue, got=%d", got)
	}
	pending.Status = "active"
	if got := apply(now.Add(5*time.Minute), pending).RecheckJobsQueued; got != 1 {
		t.Fatalf("pending->active must enqueue, got=%d", got)
	}
	completeJob()
	pending.Matchers[0].Value = "payments"
	if got := apply(now.Add(6*time.Minute), pending).RecheckJobsQueued; got != 1 {
		t.Fatalf("active matcher change must enqueue, got=%d", got)
	}
}

func TestMatcherChangeQueuesSuccessorWhilePreviousRecheckJobIsOpen(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	silence := SourceSilenceSnapshot{
		SilenceID: "open-job-rule", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}
	apply := func(at time.Time) PollCycleResult {
		t.Helper()
		result, applyErr := store.ApplyPollCycle(PollCycleInput{
			SilenceSources: []SourceSilenceState{{
				SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence},
			}}, Timestamp: at,
		})
		if applyErr != nil {
			t.Fatal(applyErr)
		}
		return result
	}
	if result := apply(now); result.RecheckJobsQueued != 1 {
		t.Fatalf("initial active rule must queue first job: %+v", result)
	}
	silence.Matchers[0].Value = "payments"
	if result := apply(now.Add(time.Minute)); result.RecheckJobsQueued != 1 {
		t.Fatalf("matcher change must queue a successor despite open predecessor: %+v", result)
	}
	var jobs, contexts, revisions int
	if err = store.db.QueryRow(`SELECT COUNT(*), COUNT(DISTINCT context_fingerprint) FROM silence_recheck_jobs`).Scan(&jobs, &contexts); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(DISTINCT revision_id) FROM silence_recheck_job_rules`).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if jobs != 2 || contexts != 2 || revisions != 2 {
		t.Fatalf("successor must pin the new matcher revision: jobs=%d contexts=%d revisions=%d", jobs, contexts, revisions)
	}
}

func TestFailedRecheckJobBlocksLaterJobsAndIsObservable(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	nowRaw := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err = store.db.Exec(`
INSERT INTO silence_recheck_jobs(status, created_at, source_context_json, context_fingerprint, open_context_fingerprint, trigger_type, last_error)
VALUES('failed', ?, '{}', 'failed-fp', 'failed-fp', 'silence_context_changed', 'forced failure'),
      ('queued', ?, '{}', 'later-fp', 'later-fp', 'silence_context_changed', NULL)
`, nowRaw, nowRaw); err != nil {
		t.Fatalf("seed recheck jobs: %v", err)
	}
	stats, err := store.GetStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.RecheckQueueDepth != 2 || stats.RecheckCurrentStatus != "failed" || stats.RecheckCurrentError != "forced failure" {
		t.Fatalf("unexpected recheck observability: %+v", stats)
	}
	if _, err = store.RunSilenceRecheckBatch(100); err == nil || !strings.Contains(err.Error(), "blocks later jobs") {
		t.Fatalf("expected failed FIFO job to block later jobs, got=%v", err)
	}
	var laterStatus string
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE context_fingerprint='later-fp'`).Scan(&laterStatus); err != nil {
		t.Fatal(err)
	}
	if laterStatus != "queued" {
		t.Fatalf("later job must remain queued, got=%q", laterStatus)
	}
	stats, err = store.GetStats()
	if err != nil || stats.RecheckCurrentError != "forced failure" {
		t.Fatalf("failed job root cause must remain immutable: stats=%+v err=%v", stats, err)
	}
}

func TestGlobalAndTargetPromotionCASRejectStaleObservation(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	observed := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "stale-cas", State: StateInactive, RawState: StateActive,
		IsLive: false, Receiver: "team", GroupID: "group", GroupLabelsJSON: labels,
		LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
		StartsAt: observed.Add(-time.Hour), LastSeenAt: &observed,
		RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`,
	}); err != nil {
		t.Fatal(err)
	}
	tx, _ := store.db.Begin()
	if err = upsertTargetStateTx(tx, "stale-cas", targetStateRow{
		Target: "cluster-a", State: StateInactive, RawState: StateActive, StartsAt: observed.Add(-time.Hour),
		LastSeenAt: &observed, SourceMembers: []string{"am-a"},
	}, "seed", observed); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	newer := observed.Add(time.Minute)
	if _, err = store.db.Exec(`UPDATE alert_states SET last_seen_at=? WHERE alert_id='stale-cas'`, newer.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`UPDATE alert_target_states SET last_seen_at=? WHERE alert_id='stale-cas'`, newer.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	tx, _ = store.db.Begin()
	globalApplied, _, err := promoteAlertSuppressedCAS(tx, recheckCandidate{
		AlertID: "stale-cas", RawState: "active", LastSeenAt: sql.NullString{String: observed.Format(time.RFC3339Nano), Valid: true},
	}, newer.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	targetApplied, _, err := promoteTargetSuppressedCASTx(tx, "stale-cas", targetStateRow{
		Target: "cluster-a", State: StateInactive, RawState: StateActive, LastSeenAt: &observed,
	}, newer.Add(time.Minute), "silence_match_promoted")
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
	if globalApplied || targetApplied {
		t.Fatalf("stale CAS must be a no-op: global=%v target=%v", globalApplied, targetApplied)
	}
}

func TestLiveSuppressionCauseResolvesLaterWithoutRewritingHistory(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	live := LiveAlertSnapshot{
		AlertID: "lagged-cause", State: StateSuppressed,
		Receiver: "team", GroupID: "group", GroupLabelsJSON: labels, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now,
		RelevantUpstreams: []string{"am-a"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-a"}},
		SilencedBySources: []SourceSilenceRef{{SourceName: "am-a", ClusterID: "cluster-a", SilenceIDs: []string{"sil-lag"}}},
		Targets:           []LiveTargetSnapshot{{Target: "cluster-a", State: StateSuppressed, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-a"}}},
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{live}, SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success"}}, Timestamp: now,
	}); err != nil {
		t.Fatal(err)
	}
	var firstHistoryID int64
	var firstStatus string
	if err = store.db.QueryRow(`SELECT history_id, cause_status FROM alert_target_suppression_cause_history WHERE alert_id='lagged-cause' ORDER BY history_id LIMIT 1`).Scan(&firstHistoryID, &firstStatus); err != nil {
		t.Fatal(err)
	}
	var firstResolution string
	var firstRevision sql.NullInt64
	if err = store.db.QueryRow(`SELECT resolution_status, revision_id FROM alert_target_suppression_cause_members WHERE history_id=?`, firstHistoryID).Scan(&firstResolution, &firstRevision); err != nil {
		t.Fatal(err)
	}
	if firstStatus != "partial" || firstResolution != "unresolved" || firstRevision.Valid {
		t.Fatalf("unexpected unresolved first evidence: status=%s resolution=%s revision=%+v", firstStatus, firstResolution, firstRevision)
	}
	live.LastSeenAt = now.Add(time.Minute)
	if _, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{live}, SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{
			SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success",
			Silences: []SourceSilenceSnapshot{{
				SilenceID: "sil-lag", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
			}},
		}}, Timestamp: now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	var historyCount int
	var latestStatus, latestResolution string
	var latestRevision sql.NullInt64
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_target_suppression_cause_history WHERE alert_id='lagged-cause'`).Scan(&historyCount); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`
SELECT h.cause_status, m.resolution_status, m.revision_id
FROM alert_target_suppression_cause_history h
JOIN alert_target_suppression_cause_members m ON m.history_id=h.history_id
WHERE h.alert_id='lagged-cause' ORDER BY h.history_id DESC LIMIT 1
`).Scan(&latestStatus, &latestResolution, &latestRevision); err != nil {
		t.Fatal(err)
	}
	if historyCount != 2 || latestStatus != "known" || latestResolution != "known" || !latestRevision.Valid {
		t.Fatalf("expected appended resolved evidence: count=%d status=%s resolution=%s revision=%+v", historyCount, latestStatus, latestResolution, latestRevision)
	}
	if err = store.db.QueryRow(`SELECT resolution_status, revision_id FROM alert_target_suppression_cause_members WHERE history_id=?`, firstHistoryID).Scan(&firstResolution, &firstRevision); err != nil {
		t.Fatal(err)
	}
	if firstResolution != "unresolved" || firstRevision.Valid {
		t.Fatalf("first evidence was rewritten: resolution=%s revision=%+v", firstResolution, firstRevision)
	}
}

func TestStoreUsesConfiguredSilenceHistoryRetention(t *testing.T) {
	configured := 7 * 24 * time.Hour
	store, err := OpenStore(StoreOptions{
		Path: filepath.Join(t.TempDir(), "history.sqlite"), SilenceHistoryRetentionTTL: configured,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.silenceHistoryRetentionTTL != configured {
		t.Fatalf("retention=%s want=%s", store.silenceHistoryRetentionTTL, configured)
	}
}

func TestSilenceRevisionBoundaryIncludesUpdatedAtAndLatestSequence(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	updatedA := now.Add(-2 * time.Minute)
	updatedB := now.Add(-time.Minute)
	base := SourceSilenceSnapshot{
		SilenceID: "silence-revision", Status: "active",
		StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), UpdatedAt: &updatedA,
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}
	upsert := func(snapshot SourceSilenceSnapshot, at time.Time) int64 {
		t.Helper()
		tx, txErr := store.db.Begin()
		if txErr != nil {
			t.Fatal(txErr)
		}
		_, revisionID, txErr := upsertSilenceRuleAndRevisionTx(tx, "cluster-a", snapshot, at)
		if txErr != nil {
			_ = tx.Rollback()
			t.Fatal(txErr)
		}
		if txErr = tx.Commit(); txErr != nil {
			t.Fatal(txErr)
		}
		return revisionID
	}
	revision1 := upsert(base, now)
	if repeated := upsert(base, now.Add(time.Second)); repeated != revision1 {
		t.Fatalf("unchanged latest revision must be reused: first=%d repeated=%d", revision1, repeated)
	}
	base.UpdatedAt = &updatedB
	revision2 := upsert(base, now.Add(2*time.Second))
	if revision2 == revision1 {
		t.Fatal("updatedAt change must create a new revision")
	}
	if repeated := upsert(base, now.Add(3*time.Second)); repeated != revision2 {
		t.Fatalf("unchanged second revision must be reused: second=%d repeated=%d", revision2, repeated)
	}
	base.Matchers[0].Value = "payments"
	revision3 := upsert(base, now.Add(4*time.Second))
	if revision3 == revision2 {
		t.Fatal("content change must create a new revision")
	}
	base.Matchers[0].Value = "api"
	base.UpdatedAt = &updatedA
	revision4 := upsert(base, now.Add(5*time.Second))
	if revision4 == revision1 || revision4 == revision2 || revision4 == revision3 {
		t.Fatalf("A->B->A sequence must append, not reuse an old revision: ids=%d,%d,%d,%d", revision1, revision2, revision3, revision4)
	}
	var count int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_revisions`).Scan(&count); err != nil || count != 4 {
		t.Fatalf("expected four append-only revision boundaries, count=%d err=%v", count, err)
	}
	var storedUpdated string
	if err = store.db.QueryRow(`SELECT source_updated_at FROM silence_revisions WHERE revision_id=?`, revision4).Scan(&storedUpdated); err != nil {
		t.Fatal(err)
	}
	if storedUpdated != updatedA.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("source_updated_at=%q want=%q", storedUpdated, updatedA.UTC().Format(time.RFC3339Nano))
	}
}

func TestSilenceEpisodeClosesOnlyAfterConfirmedHAWideAbsence(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	silence := SourceSilenceSnapshot{
		SilenceID: "ha-episode", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}
	source := func(name, status string, present bool) SourceSilenceState {
		state := SourceSilenceState{SourceName: name, ClusterID: "cluster-ha", SyncStatus: status}
		if present {
			state.Silences = []SourceSilenceSnapshot{silence}
		}
		return state
	}
	apply := func(at time.Time, sources ...SourceSilenceState) {
		t.Helper()
		successful := map[string]bool{}
		for _, item := range sources {
			successful[item.SourceName] = item.SyncStatus == "success"
		}
		if _, applyErr := store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: successful, SilenceSources: sources, Timestamp: at}); applyErr != nil {
			t.Fatalf("ApplyPollCycle(): %v", applyErr)
		}
	}
	apply(now, source("am-a", "success", true), source("am-b", "success", true))
	var firstEpisodeID int64
	if err = store.db.QueryRow(`SELECT episode_id FROM silence_rule_episodes WHERE open_marker=1`).Scan(&firstEpisodeID); err != nil {
		t.Fatal(err)
	}
	apply(now.Add(time.Minute), source("am-a", "success", false), source("am-b", "failed", false))
	var openMarker sql.NullInt64
	if err = store.db.QueryRow(`SELECT open_marker FROM silence_rule_episodes WHERE episode_id=?`, firstEpisodeID).Scan(&openMarker); err != nil {
		t.Fatal(err)
	}
	if !openMarker.Valid || openMarker.Int64 != 1 {
		t.Fatalf("partial HA coverage must preserve open episode: %+v", openMarker)
	}
	apply(now.Add(2*time.Minute), source("am-a", "success", false), source("am-b", "success", false))
	var endedAt, endReason sql.NullString
	if err = store.db.QueryRow(`SELECT open_marker, ended_at, end_reason FROM silence_rule_episodes WHERE episode_id=?`, firstEpisodeID).Scan(&openMarker, &endedAt, &endReason); err != nil {
		t.Fatal(err)
	}
	if openMarker.Valid || !endedAt.Valid || endReason.String != "absent_confirmed" {
		t.Fatalf("HA-wide absence must close episode: open=%+v ended=%+v reason=%+v", openMarker, endedAt, endReason)
	}
	apply(now.Add(3*time.Minute), source("am-a", "success", true), source("am-b", "success", true))
	var secondEpisodeID int64
	var episodeNumber int
	if err = store.db.QueryRow(`SELECT episode_id, episode_number FROM silence_rule_episodes WHERE open_marker=1`).Scan(&secondEpisodeID, &episodeNumber); err != nil {
		t.Fatal(err)
	}
	if secondEpisodeID == firstEpisodeID || episodeNumber != 2 {
		t.Fatalf("reappearance must start episode 2: first=%d second=%d number=%d", firstEpisodeID, secondEpisodeID, episodeNumber)
	}
}

func TestUnknownSilenceStatusMeansConfirmedAbsence(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Millisecond)
	active := SourceSilenceSnapshot{
		SilenceID: "status-became-unknown",
		Status:    "active",
		StartsAt:  now.Add(-time.Hour),
		EndsAt:    now.Add(time.Hour),
		CreatedBy: "sensitive-author@example.com",
		Comment:   "sensitive silence comment",
		Matchers:  []SilenceMatcherSnapshot{{Name: "service", Value: "unique-sensitive-matcher-value", IsEqual: true}},
	}
	apply := func(at time.Time, silenceA, silenceB SourceSilenceSnapshot) {
		t.Helper()
		_, applyErr := store.ApplyPollCycle(PollCycleInput{
			SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true},
			SilenceSources: []SourceSilenceState{
				{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silenceA}},
				{SourceName: "am-b", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silenceB}},
			},
			Timestamp: at,
		})
		if applyErr != nil {
			t.Fatalf("ApplyPollCycle(): %v", applyErr)
		}
	}

	apply(now, active, active)
	var jobsBefore int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobsBefore); err != nil {
		t.Fatal(err)
	}
	unknown := active
	unknown.Status = "future-state"
	apply(now.Add(time.Minute), unknown, active)

	var currentRelations int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alertmanager_source_silences`).Scan(&currentRelations); err != nil {
		t.Fatal(err)
	}
	if currentRelations != 1 {
		t.Fatalf("current relations after one unknown member=%d, want 1", currentRelations)
	}
	var openMarker sql.NullInt64
	if err = store.db.QueryRow(`SELECT open_marker FROM silence_rule_episodes WHERE open_marker=1`).Scan(&openMarker); err != nil {
		t.Fatal(err)
	}
	if !openMarker.Valid {
		t.Fatal("one valid HA member must keep the episode open")
	}

	apply(now.Add(2*time.Minute), unknown, unknown)

	var revisions int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alertmanager_source_silences`).Scan(&currentRelations); err != nil {
		t.Fatal(err)
	}
	if currentRelations != 0 {
		t.Fatalf("current relations=%d, want 0", currentRelations)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_revisions`).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if revisions != 1 {
		t.Fatalf("revisions=%d, want the original revision only", revisions)
	}
	var jobsAfter int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobsAfter); err != nil {
		t.Fatal(err)
	}
	if jobsAfter != jobsBefore {
		t.Fatalf("unknown status queued a recheck job: before=%d after=%d", jobsBefore, jobsAfter)
	}
	var fingerprint string
	if err = store.db.QueryRow(`SELECT silence_set_fingerprint FROM alertmanager_sources WHERE source_name='am-a'`).Scan(&fingerprint); err != nil {
		t.Fatal(err)
	}
	wantFingerprint := buildSourceSilenceFingerprint(SourceSilenceState{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success"})
	if fingerprint != wantFingerprint {
		t.Fatalf("source fingerprint includes unknown rule: got=%q want=%q", fingerprint, wantFingerprint)
	}

	var endReason sql.NullString
	if err = store.db.QueryRow(`
SELECT open_marker, end_reason
FROM silence_rule_episodes
WHERE rule_id=(SELECT rule_id FROM silence_rules WHERE silence_id=?)
`, active.SilenceID).Scan(&openMarker, &endReason); err != nil {
		t.Fatal(err)
	}
	if openMarker.Valid || !endReason.Valid || endReason.String != "absent_confirmed" {
		t.Fatalf("unknown status must close the previous episode as absent: open=%+v reason=%+v", openMarker, endReason)
	}
	logged := logOutput.String()
	for _, expected := range []string{`"component":"silence_reconciliation"`, `"event":"source_snapshot_received"`, `"malformed_rule_count":1`, `"event":"rule_episode_closed"`, `"reason_code":"absent_confirmed"`} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("missing reconciliation log field %s in %s", expected, logged)
		}
	}
	for _, forbidden := range []string{"unique-sensitive-matcher-value", "sensitive silence comment", "sensitive-author@example.com"} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("sensitive silence data %q leaked into log: %s", forbidden, logged)
		}
	}
}

func TestRetiredSourceReleasesMutableSilenceHistoryForRetention(t *testing.T) {
	store, err := OpenStore(StoreOptions{
		Path: filepath.Join(t.TempDir(), "history.sqlite"), SilenceHistoryRetentionTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	_, err = store.ApplyPollCycle(PollCycleInput{
		SilenceSources: []SourceSilenceState{{
			SourceName: "retired-am", ClusterID: "retired-cluster", SyncStatus: "success",
			Silences: []SourceSilenceSnapshot{{
				SilenceID: "retired-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
			}},
		}}, Timestamp: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.RunSilenceRecheckBatch(100); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{SilenceSources: []SourceSilenceState{}, Timestamp: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	var relations int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alertmanager_source_silences WHERE source_name='retired-am'`).Scan(&relations); err != nil {
		t.Fatal(err)
	}
	if relations != 0 {
		t.Fatalf("retirement must clear mutable current relations, got=%d", relations)
	}
	old := "2000-01-01T00:00:00Z"
	if _, err = store.db.Exec(`UPDATE alertmanager_sources SET retired_at=?, last_configured_at=? WHERE source_name='retired-am'`, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`UPDATE silence_revisions SET last_observed_at=?`, old); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`UPDATE silence_rules SET last_observed_at=?`, old); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`UPDATE silence_recheck_jobs SET finished_at=? WHERE status='completed'`, old); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`
INSERT INTO silence_recheck_jobs(
  context_fingerprint, open_context_fingerprint, source_context_json,
  trigger_type, status, created_at, finished_at
) VALUES('sha256:newer-empty-context', NULL, '{"formatVersion":1,"sources":[]}',
         'source_changed', 'completed', ?, ?)
`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RunHousekeeping(365*24*time.Hour, 100000); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"alertmanager_sources", "silence_revisions", "silence_rule_episodes", "silence_rules"} {
		var count int
		if err = store.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("retired unreferenced history remained in %s: %d", table, count)
		}
	}
}

func TestListTargetAlertsForPreviewUsesTargetResolvedFlag(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC().Truncate(time.Millisecond)
	matchLabels := `{"alertname":"CPUHigh"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "resolved-target-only",
		State:             StateActive,
		RawState:          StateActive,
		IsLive:            true,
		IsSuppressed:      false,
		IsResolved:        false,
		Receiver:          "team-a",
		GroupID:           "grp-a",
		GroupLabelsJSON:   `{"alertname":"CPUHigh"}`,
		LabelsJSON:        matchLabels,
		MatchLabelsJSON:   &matchLabels,
		AnnotationsJSON:   `{}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a","am-b"]`,
		RelevantSources:   `[{"name":"am-a","cluster":"cluster-a"},{"name":"am-b","cluster":"cluster-b"}]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() returned error: %v", err)
	}

	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}
	err = upsertTargetStateTx(tx, "resolved-target-only", targetStateRow{
		Target:        "cluster-b",
		State:         StateResolved,
		RawState:      StateActive,
		IsLive:        false,
		IsSuppressed:  false,
		IsResolved:    true,
		StartsAt:      now.Add(-time.Hour),
		LastSeenAt:    &now,
		SourceMembers: []string{"am-b"},
	}, "seed", now)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("failed to seed target state: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("failed to commit seed tx: %v", err)
	}

	rows, err := store.ListTargetAlertsForPreview("cluster-b")
	if err != nil {
		t.Fatalf("ListTargetAlertsForPreview(cluster-b) returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one target row, got=%d", len(rows))
	}
	if !rows[0].IsResolved || rows[0].State != StateResolved {
		t.Fatalf("expected target resolved projection, got=%+v", rows[0])
	}
}

func TestTargetMatchLabelIndexAndStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC().Truncate(time.Millisecond)
	labelsA := `{"service":"api","team":"payments","pod_name":"one","pattern":"100%ready"}`
	labelsB := `{"service":"payments","team":"payments","podXname":"two","pattern":"100Xready"}`

	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "idx-a",
		State:             StateInactive,
		RawState:          StateActive,
		IsLive:            false,
		IsSuppressed:      false,
		IsResolved:        false,
		Receiver:          "team-a",
		GroupID:           "grp-a",
		GroupLabelsJSON:   `{"service":"api"}`,
		LabelsJSON:        labelsA,
		MatchLabelsJSON:   &labelsA,
		AnnotationsJSON:   `{}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
		RelevantSources:   `[{"name":"am-a","cluster":"cluster-a"}]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert(idx-a) failed: %v", err)
	}
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "idx-b",
		State:             StateInactive,
		RawState:          StateActive,
		IsLive:            false,
		IsSuppressed:      false,
		IsResolved:        false,
		Receiver:          "team-a",
		GroupID:           "grp-a",
		GroupLabelsJSON:   `{"service":"payments"}`,
		LabelsJSON:        labelsB,
		MatchLabelsJSON:   &labelsB,
		AnnotationsJSON:   `{}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-b"]`,
		RelevantSources:   `[{"name":"am-b","cluster":"cluster-a"}]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert(idx-b) failed: %v", err)
	}
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	for _, alertID := range []string{"idx-a", "idx-b"} {
		if err = upsertTargetStateTx(tx, alertID, targetStateRow{
			Target:        "cluster-a",
			State:         StateInactive,
			RawState:      StateActive,
			IsLive:        false,
			IsSuppressed:  false,
			IsResolved:    false,
			StartsAt:      now.Add(-time.Hour),
			LastSeenAt:    &now,
			SourceMembers: []string{"am-a", "am-b"},
		}, "seed", now); err != nil {
			_ = tx.Rollback()
			t.Fatalf("seed target state failed for %s: %v", alertID, err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	names, nameTotal, err := store.ListTargetLabelNames("cluster-a", "se", 10)
	if err != nil {
		t.Fatalf("ListTargetLabelNames failed: %v", err)
	}
	if len(names) != 1 || names[0].Name != "service" || names[0].Values != 2 {
		t.Fatalf("unexpected label name stats: %+v", names)
	}
	if nameTotal != 1 {
		t.Fatalf("unexpected exact label name total: %d", nameTotal)
	}

	values, valueTotal, err := store.ListTargetLabelValues("cluster-a", "service", "", 10)
	if err != nil {
		t.Fatalf("ListTargetLabelValues failed: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 value rows, got %d", len(values))
	}
	if valueTotal != 2 {
		t.Fatalf("unexpected exact label value total: %d", valueTotal)
	}
	if values[0].Value != "api" || values[0].Hits != 1 {
		t.Fatalf("unexpected first value row: %+v", values[0])
	}
	legacyNames, legacyTotal, err := store.ListTargetLabelNames("cluster-a", "legacy", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(legacyNames) != 0 || legacyTotal != 0 {
		t.Fatalf("legacy-only labels must not leak into autocomplete: names=%+v total=%d", legacyNames, legacyTotal)
	}
	overlay := []TargetLabelOverlay{
		{AlertID: "idx-a", Labels: map[string]string{"service": "api-v2", "live_only": "current"}},
		{AlertID: "idx-live", Labels: map[string]string{"service": "search", "live_only": "new"}},
	}
	mergedNames, _, err := store.ListTargetLabelNamesWithLive("cluster-a", "", 10, overlay)
	if err != nil {
		t.Fatal(err)
	}
	mergedByName := map[string]int{}
	for _, item := range mergedNames {
		mergedByName[item.Name] = item.Values
	}
	if mergedByName["service"] != 3 || mergedByName["live_only"] != 2 {
		t.Fatalf("live overlay did not replace stale rows and add new rows: %+v", mergedNames)
	}
	mergedValues, mergedTotal, err := store.ListTargetLabelValuesWithLive("cluster-a", "service", "", 10, overlay)
	if err != nil {
		t.Fatal(err)
	}
	mergedByValue := map[string]int{}
	for _, item := range mergedValues {
		mergedByValue[item.Value] = item.Hits
	}
	if mergedTotal != 3 || mergedByValue["api"] != 0 || mergedByValue["api-v2"] != 1 || mergedByValue["payments"] != 1 || mergedByValue["search"] != 1 {
		t.Fatalf("unexpected merged live/stored values: total=%d values=%+v", mergedTotal, mergedValues)
	}
	if values[1].Value != "payments" || values[1].Hits != 1 {
		t.Fatalf("unexpected second value row: %+v", values[1])
	}
	limitedValues, limitedTotal, err := store.ListTargetLabelValues("cluster-a", "service", "", 1)
	if err != nil {
		t.Fatalf("limited ListTargetLabelValues failed: %v", err)
	}
	if len(limitedValues) != 1 || limitedTotal != 2 {
		t.Fatalf("expected one returned value with exact total=2, rows=%+v total=%d", limitedValues, limitedTotal)
	}
	literalNames, literalNameTotal, err := store.ListTargetLabelNames("cluster-a", "pod_", 10)
	if err != nil {
		t.Fatalf("literal underscore label prefix failed: %v", err)
	}
	if literalNameTotal != 1 || len(literalNames) != 1 || literalNames[0].Name != "pod_name" {
		t.Fatalf("expected '_' to be a literal label prefix, rows=%+v total=%d", literalNames, literalNameTotal)
	}
	literalValues, literalValueTotal, err := store.ListTargetLabelValues("cluster-a", "pattern", "100%", 10)
	if err != nil {
		t.Fatalf("literal percent value prefix failed: %v", err)
	}
	if literalValueTotal != 1 || len(literalValues) != 1 || literalValues[0].Value != "100%ready" {
		t.Fatalf("expected '%%' to be a literal value prefix, rows=%+v total=%d", literalValues, literalValueTotal)
	}

	var indexedAtBefore string
	if err = store.db.QueryRow(`SELECT updated_at FROM alert_target_match_labels WHERE alert_id='idx-a' AND target='cluster-a' AND label_name='service'`).Scan(&indexedAtBefore); err != nil {
		t.Fatalf("read label index timestamp: %v", err)
	}
	tx, err = store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin unchanged target tx: %v", err)
	}
	if err = upsertTargetStateTx(tx, "idx-a", targetStateRow{
		Target: "cluster-a", State: StateInactive, RawState: StateActive,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now, SourceMembers: []string{"am-a", "am-b"},
	}, "unchanged", now.Add(time.Hour)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("unchanged target upsert failed: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit unchanged target tx: %v", err)
	}
	var indexedAtAfter string
	if err = store.db.QueryRow(`SELECT updated_at FROM alert_target_match_labels WHERE alert_id='idx-a' AND target='cluster-a' AND label_name='service'`).Scan(&indexedAtAfter); err != nil {
		t.Fatalf("read unchanged label index timestamp: %v", err)
	}
	if indexedAtAfter != indexedAtBefore {
		t.Fatalf("unchanged match labels rebuilt index: before=%s after=%s", indexedAtBefore, indexedAtAfter)
	}
}

func TestTargetSourceMembersSurvivePartialHAFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"alertname":"CPUHigh"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "ha-partial", State: StateActive, RawState: StateActive, IsLive: true,
		Receiver: "team-a", GroupID: "grp-a", GroupLabelsJSON: labels, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
		RelevantUpstreams: `["am-a","am-b"]`,
		RelevantSources:   `[{"name":"am-a","cluster":"cluster-a"},{"name":"am-b","cluster":"cluster-a"}]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() error: %v", err)
	}

	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	if err = upsertTargetStateTx(tx, "ha-partial", targetStateRow{
		Target: "cluster-a", State: StateActive, RawState: StateActive, IsLive: true,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now, SourceMembers: []string{"am-a", "am-b"},
	}, "seed", now); err != nil {
		_ = tx.Rollback()
		t.Fatalf("seed target: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}

	tx, err = store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin partial tx: %v", err)
	}
	if err = upsertTargetStatesFromLiveTx(tx, "ha-partial", []LiveTargetSnapshot{{
		Target: "cluster-a", State: StateActive, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-a"},
	}}, &labels, map[string]bool{"am-a": true, "am-b": false}, map[string]map[string]struct{}{"cluster-a": {"am-a": {}, "am-b": {}}}, true, now.Add(time.Minute)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("partial target update: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit partial update: %v", err)
	}

	current, err := store.GetCurrentAlert("ha-partial")
	if err != nil || current == nil {
		t.Fatalf("GetCurrentAlert(): current=%+v err=%v", current, err)
	}
	tx, err = store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin disappearance tx: %v", err)
	}
	if err = markMissingTargetStatesNonLiveTx(tx, *current, map[string]bool{"am-a": true, "am-b": false}, map[string]map[string]struct{}{"cluster-a": {"am-a": {}, "am-b": {}}}, true, now.Add(2*time.Minute)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("partial disappearance: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit disappearance: %v", err)
	}

	rows, err := store.ListTargetAlertsForPreview("cluster-a")
	if err != nil || len(rows) != 1 {
		t.Fatalf("target rows=%+v err=%v", rows, err)
	}
	if !rows[0].IsLive || rows[0].SourceMembersJSON != `["am-a","am-b"]` {
		t.Fatalf("expected last-known live with full HA members, got=%+v", rows[0])
	}

	tx, err = store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin confirmed disappearance tx: %v", err)
	}
	if err = markMissingTargetStatesNonLiveTx(tx, *current, map[string]bool{"am-a": true, "am-b": true}, map[string]map[string]struct{}{"cluster-a": {"am-a": {}, "am-b": {}}}, true, now.Add(3*time.Minute)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("confirmed disappearance: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit confirmed disappearance: %v", err)
	}
	rows, err = store.ListTargetAlertsForPreview("cluster-a")
	if err != nil || len(rows) != 1 {
		t.Fatalf("confirmed target rows=%+v err=%v", rows, err)
	}
	if rows[0].IsLive || rows[0].State != StateInactive {
		t.Fatalf("expected target inactive after complete HA coverage, got=%+v", rows[0])
	}
}

func TestTargetSourceMembersDropDeconfiguredMemberAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"alertname":"CPUHigh"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "ha-topology", State: StateActive, RawState: StateActive, IsLive: true,
		Receiver: "team-a", GroupID: "grp-a", GroupLabelsJSON: labels, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
		RelevantUpstreams: `["am-a","am-b"]`,
		RelevantSources:   `[{"name":"am-a","cluster":"cluster-a"},{"name":"am-b","cluster":"cluster-a"}]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() error: %v", err)
	}
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	if err = upsertTargetStateTx(tx, "ha-topology", targetStateRow{
		Target: "cluster-a", State: StateActive, RawState: StateActive, IsLive: true,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now, SourceMembers: []string{"am-a", "am-b"},
	}, "seed", now); err != nil {
		_ = tx.Rollback()
		t.Fatalf("seed target: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{
			SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success",
		}},
		Timestamp: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("ApplyPollCycle() after topology change: %v", err)
	}
	rows, err := store.ListTargetAlertsForPreview("cluster-a")
	if err != nil || len(rows) != 1 {
		t.Fatalf("target rows=%+v err=%v", rows, err)
	}
	if rows[0].IsLive || rows[0].State != StateInactive || rows[0].SourceMembersJSON != `["am-a"]` {
		t.Fatalf("expected inactive target with deconfigured member removed, got=%+v", rows[0])
	}
	global, err := store.GetCurrentAlert("ha-topology")
	if err != nil || global == nil {
		t.Fatalf("global alert after topology change=%+v err=%v", global, err)
	}
	if global.IsLive || global.State != StateInactive {
		t.Fatalf("expected global alert inactive after configured sources confirmed absence, got=%+v", global)
	}
}

func TestSourceReassignmentRetiresOldTargetProjection(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	liveFor := func(target string, at time.Time) LiveAlertSnapshot {
		return LiveAlertSnapshot{
			AlertID: "reassigned-source", State: StateActive, Receiver: "team", GroupID: "group",
			GroupLabelsJSON: labels, LabelsJSON: labels, MatchLabelsJSON: &labels,
			AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: at,
			RelevantUpstreams: []string{"am-a"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: target}},
			Targets: []LiveTargetSnapshot{{Target: target, State: StateActive, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-a"}}},
		}
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{liveFor("cluster-a", now)}, SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success"}}, Timestamp: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{liveFor("cluster-b", now.Add(time.Minute))}, SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-b", SyncStatus: "success"}}, Timestamp: now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	oldTarget, err := store.ListTargetAlertsForPreview("cluster-a")
	if err != nil || len(oldTarget) != 0 {
		t.Fatalf("old target must be retired after source reassignment: rows=%+v err=%v", oldTarget, err)
	}
	newTarget, err := store.ListTargetAlertsForPreview("cluster-b")
	if err != nil || len(newTarget) != 1 || !newTarget[0].IsLive {
		t.Fatalf("new target projection missing after reassignment: rows=%+v err=%v", newTarget, err)
	}
	var retiredAt sql.NullString
	var oldMembers string
	if err = store.db.QueryRow(`SELECT retired_at, source_members_json FROM alert_target_states WHERE alert_id='reassigned-source' AND target='cluster-a'`).Scan(&retiredAt, &oldMembers); err != nil {
		t.Fatal(err)
	}
	if !retiredAt.Valid || oldMembers != `[]` {
		t.Fatalf("old target was not retired cleanly: retired=%+v members=%s", retiredAt, oldMembers)
	}
}

func TestTargetRetiresWhenAllMembersAreDeconfigured(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"alertname":"CPUHigh"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "retired-target", State: StateActive, RawState: StateActive, IsLive: true,
		Receiver: "team-a", GroupID: "grp-a", GroupLabelsJSON: labels, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
		RelevantUpstreams: `["am-a","am-b"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"},{"name":"am-b","cluster":"cluster-a"}]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() error: %v", err)
	}
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	if err = upsertTargetStateTx(tx, "retired-target", targetStateRow{
		Target: "cluster-a", State: StateActive, RawState: StateActive, IsLive: true,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now, SourceMembers: []string{"am-a", "am-b"},
	}, "seed", now); err != nil {
		_ = tx.Rollback()
		t.Fatalf("seed target: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	store, err = OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if _, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{},
		SilenceSources:      []SourceSilenceState{},
		Timestamp:           now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("ApplyPollCycle() with empty topology: %v", err)
	}
	rows, err := store.ListTargetAlertsForPreview("cluster-a")
	if err != nil {
		t.Fatalf("ListTargetAlertsForPreview(): %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected retired target excluded from preview, got=%+v", rows)
	}
	var retiredAt sql.NullString
	var isLive int
	if err = store.db.QueryRow(`SELECT retired_at, is_live FROM alert_target_states WHERE alert_id='retired-target' AND target='cluster-a'`).Scan(&retiredAt, &isLive); err != nil {
		t.Fatalf("query retired target row: %v", err)
	}
	if !retiredAt.Valid || isLive != 1 {
		t.Fatalf("expected retired target to preserve last-known live state, retired_at=%+v is_live=%d", retiredAt, isLive)
	}
	global, err := store.GetCurrentAlert("retired-target")
	if err != nil || global == nil || !global.IsLive {
		t.Fatalf("expected global last-known live state after topology retirement, global=%+v err=%v", global, err)
	}
}

func TestInactiveTargetRetiresWhenAllMembersAreDeconfigured(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"alertname":"CPUHigh"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "retired-inactive-target", State: StateInactive, RawState: StateActive, IsLive: false,
		Receiver: "team-a", GroupID: "grp-a", GroupLabelsJSON: labels, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
		RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() error: %v", err)
	}
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	if err = upsertTargetStateTx(tx, "retired-inactive-target", targetStateRow{
		Target: "cluster-a", State: StateInactive, RawState: StateActive, IsLive: false,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now, SourceMembers: []string{"am-a"},
	}, "seed", now); err != nil {
		_ = tx.Rollback()
		t.Fatalf("seed inactive target: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}

	if _, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{
			SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success",
		}},
		Timestamp: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("ApplyPollCycle() after partial topology removal: %v", err)
	}
	var sourceMembersRaw string
	if err = store.db.QueryRow(`SELECT source_members_json FROM alert_target_states WHERE alert_id='retired-inactive-target' AND target='cluster-a'`).Scan(&sourceMembersRaw); err != nil {
		t.Fatalf("query reconciled inactive target: %v", err)
	}
	if sourceMembersRaw != `["am-a"]` {
		t.Fatalf("expected inactive target source membership reconciled to am-a, got=%s", sourceMembersRaw)
	}

	if _, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{},
		SilenceSources:      []SourceSilenceState{},
		Timestamp:           now.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("ApplyPollCycle() with empty topology: %v", err)
	}
	var retiredAt sql.NullString
	if err = store.db.QueryRow(`SELECT retired_at FROM alert_target_states WHERE alert_id='retired-inactive-target' AND target='cluster-a'`).Scan(&retiredAt); err != nil {
		t.Fatalf("query retired inactive target: %v", err)
	}
	if !retiredAt.Valid {
		t.Fatal("expected inactive target to be retired after topology removal")
	}
}

func TestTargetInitialEventOldResolvedRemainsNullAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"alertname":"CPUHigh"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "initial-event", State: StateActive, RawState: StateActive, IsLive: true,
		Receiver: "team-a", GroupID: "grp-a", GroupLabelsJSON: labels, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now, LastSeenAt: &now,
		RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert(): %v", err)
	}
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err = upsertTargetStateTx(tx, "initial-event", targetStateRow{
		Target: "cluster-a", State: StateActive, RawState: StateActive, IsLive: true,
		StartsAt: now, LastSeenAt: &now, SourceMembers: []string{"am-a"},
	}, "initial", now); err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert target: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	store, err = OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var oldResolved sql.NullInt64
	if err = store.db.QueryRow(`SELECT old_is_resolved FROM alert_target_events WHERE alert_id=? ORDER BY id LIMIT 1`, "initial-event").Scan(&oldResolved); err != nil {
		t.Fatalf("query initial event: %v", err)
	}
	if oldResolved.Valid {
		t.Fatalf("expected old_is_resolved NULL after restart, got=%d", oldResolved.Int64)
	}
}

func TestApplyPollCycleEnqueuesRecheckAfterSourceRecovery(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC().Truncate(time.Millisecond)
	baseSilence := SourceSilenceSnapshot{
		SilenceID: "sil-1",
		Status:    "active",
		StartsAt:  now.Add(-time.Hour),
		EndsAt:    now.Add(time.Hour),
		Matchers:  []SilenceMatcherSnapshot{{Name: "alertname", Value: "CPUHigh", IsRegex: false, IsEqual: true}},
	}

	res, err := store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{
			SourceName: "am-a",
			ClusterID:  "cluster-a",
			SyncStatus: "success",
			Silences:   []SourceSilenceSnapshot{baseSilence},
		}},
		Timestamp: now,
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(initial success) returned error: %v", err)
	}
	if res.RecheckJobsQueued != 1 {
		t.Fatalf("expected initial queue=1 got=%d", res.RecheckJobsQueued)
	}
	if _, err = store.RunSilenceRecheckBatch(1000); err != nil {
		t.Fatalf("RunSilenceRecheckBatch(initial) returned error: %v", err)
	}

	res, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": false},
		SilenceSources: []SourceSilenceState{{
			SourceName: "am-a",
			ClusterID:  "cluster-a",
			SyncStatus: "failed",
			Silences:   nil,
		}},
		Timestamp: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(failed) returned error: %v", err)
	}
	if res.RecheckJobsQueued != 0 {
		t.Fatalf("expected failed cycle queue=0 got=%d", res.RecheckJobsQueued)
	}

	res, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{
			SourceName: "am-a",
			ClusterID:  "cluster-a",
			SyncStatus: "success",
			Silences:   []SourceSilenceSnapshot{baseSilence},
		}},
		Timestamp: now.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(recovered) returned error: %v", err)
	}
	if res.RecheckJobsQueued != 1 {
		t.Fatalf("expected recovered source queue=1 got=%d", res.RecheckJobsQueued)
	}
	logged := logOutput.String()
	for _, expected := range []string{`"level":"WARN"`, `"event":"source_sync_failed"`, `"source":"am-a"`, `"level":"INFO"`, `"event":"source_recovered"`, `"previous_status":"failed"`} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("missing source recovery log field %s in %s", expected, logged)
		}
	}
}

func TestApplyPollCycleFreshDBStoresLiveSuppressionCause(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC().Truncate(time.Millisecond)
	matchLabels := `{"alertname":"CPUHigh"}`
	result, err := store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{
			{
				AlertID:           "suppressed-live",
				State:             StateSuppressed,
				Receiver:          "team-a",
				GroupID:           "group-a",
				GroupLabelsJSON:   matchLabels,
				LabelsJSON:        matchLabels,
				MatchLabelsJSON:   &matchLabels,
				AnnotationsJSON:   `{}`,
				StartsAt:          now.Add(-time.Minute),
				LastSeenAt:        now,
				RelevantUpstreams: []string{"local"},
				RelevantSources:   []SourceRef{{Name: "local", Cluster: "local"}},
				SilencedBySources: []SourceSilenceRef{{SourceName: "local", ClusterID: "local", SilenceIDs: []string{"sil-1"}}},
			},
		},
		SuccessfulUpstreams: map[string]bool{"local": true},
		SilenceSources: []SourceSilenceState{
			{
				SourceName: "local",
				ClusterID:  "local",
				SyncStatus: "success",
				Silences: []SourceSilenceSnapshot{
					{
						SilenceID: "sil-1",
						Status:    "active",
						StartsAt:  now.Add(-time.Hour),
						EndsAt:    now.Add(time.Hour),
						Matchers:  []SilenceMatcherSnapshot{{Name: "alertname", Value: "CPUHigh", IsEqual: true}},
					},
				},
			},
		},
		Timestamp: now,
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle() error: %v", err)
	}
	if result.Inserted != 1 {
		t.Fatalf("expected one inserted alert, got=%d", result.Inserted)
	}

	var status string
	var members int
	err = store.db.QueryRow(`
SELECT h.cause_status, COUNT(m.member_id)
FROM alert_suppression_cause_history h
LEFT JOIN alert_suppression_cause_members m ON m.history_id = h.history_id
WHERE h.alert_id = ? AND h.source = 'live_alertmanager'
GROUP BY h.history_id
`, "suppressed-live").Scan(&status, &members)
	if err != nil {
		t.Fatalf("failed reading live suppression cause: %v", err)
	}
	if status != "known" || members != 1 {
		t.Fatalf("expected known cause with one member, got status=%q members=%d", status, members)
	}
}

func TestStoreSerializesConcurrentWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	store.writeMu.Lock()
	housekeepingDone := make(chan error, 1)
	regroupDone := make(chan error, 1)
	go func() {
		_, runErr := store.RunHousekeeping(24*time.Hour, 100000)
		housekeepingDone <- runErr
	}()
	go func() {
		_, runErr := store.RunRegroupToConfig(context.Background(), GroupingConfig{}, 1000)
		regroupDone <- runErr
	}()

	select {
	case runErr := <-housekeepingDone:
		store.writeMu.Unlock()
		t.Fatalf("housekeeping bypassed writer serialization: %v", runErr)
	case runErr := <-regroupDone:
		store.writeMu.Unlock()
		t.Fatalf("regroup bypassed writer serialization: %v", runErr)
	case <-time.After(20 * time.Millisecond):
	}
	store.writeMu.Unlock()

	if runErr := <-housekeepingDone; runErr != nil {
		t.Fatalf("RunHousekeeping() returned error: %v", runErr)
	}
	if runErr := <-regroupDone; runErr != nil {
		t.Fatalf("RunRegroupToConfig() returned error: %v", runErr)
	}
}

func compileMustGroupingConfig(t *testing.T, cfg GroupingConfig) compiledGroupingConfig {
	t.Helper()
	compiled, err := compileGroupingConfig(cfg)
	if err != nil {
		t.Fatalf("compileGroupingConfig() returned error: %v", err)
	}
	return compiled
}
