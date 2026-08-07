package alertlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
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
		"alertmanager_sources",
		"silence_rules",
		"silence_rule_episodes",
		"silence_revisions",
		"alertmanager_source_silences",
		"silence_recheck_jobs",
		"silence_recheck_job_rules",
		"alert_suppression_cause_history",
		"alert_suppression_cause_members",
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

func TestOpenStoreFailsOnLegacyPreviewTableRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error: %v", err)
	}
	if _, err = db.Exec(`CREATE TABLE alerts_current(id TEXT); INSERT INTO alerts_current(id) VALUES('x');`); err != nil {
		_ = db.Close()
		t.Fatalf("failed to seed legacy table: %v", err)
	}
	_ = db.Close()

	store, err := OpenStore(StoreOptions{Path: path})
	if err == nil {
		if store != nil {
			_ = store.Close()
		}
		t.Fatal("expected OpenStore() to fail for legacy preview table rows")
	}
	if !errors.Is(err, ErrLegacyPreviewTablesNotMigrated) {
		t.Fatalf("expected ErrLegacyPreviewTablesNotMigrated, got: %v", err)
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
		State:             StateInactive,
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

func TestBootstrapReprojectsSuppressedPastState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")

	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID:           "id-suppressed-past",
		State:             StateSuppressed,
		RawState:          StateSuppressed,
		IsLive:            false,
		IsResolved:        false,
		Receiver:          "team-a",
		GroupID:           "group-1",
		LabelsJSON:        `{"alertname":"DiskFull"}`,
		AnnotationsJSON:   `{"summary":"Disk full"}`,
		StartsAt:          now.Add(-time.Hour),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		_ = store.Close()
		t.Fatalf("UpsertCurrentAlert() setup failed: %v", err)
	}

	// Simulate old/stale projected state kept as inactive.
	if _, err = store.db.Exec(`
UPDATE alert_states
SET state='inactive', raw_state='suppressed', is_live=0, is_resolved=0
WHERE alert_id='id-suppressed-past'
`); err != nil {
		_ = store.Close()
		t.Fatalf("failed to seed stale projected state: %v", err)
	}

	if err = store.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	store, err = OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() reopen returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	current, err := store.GetCurrentAlert("id-suppressed-past")
	if err != nil {
		t.Fatalf("GetCurrentAlert() returned error: %v", err)
	}
	if current == nil {
		t.Fatal("GetCurrentAlert() returned nil alert")
	}
	if current.State != StateSuppressed {
		t.Fatalf("expected projected state suppressed after bootstrap normalize, got=%q", current.State)
	}

	stats, err := store.GetStats()
	if err != nil {
		t.Fatalf("GetStats() returned error: %v", err)
	}
	if stats.SuppressedBreakdownPast != 1 {
		t.Fatalf("expected suppressed past=1 got=%d", stats.SuppressedBreakdownPast)
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

func TestApplyPollCycleEnqueuesRecheckOnSilenceChange(t *testing.T) {
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
	if res.RecheckJobsQueued != 1 {
		t.Fatalf("expected changed silence set to queue 1 job, got=%d", res.RecheckJobsQueued)
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
}

func TestApplyPollCycleDoesNotEnqueueRecheckOnStatusFlipOnly(t *testing.T) {
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

	res, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": false},
		SilenceSources: []SourceSilenceState{{
			SourceName: "am-a",
			ClusterID:  "cluster-a",
			SyncStatus: "failed",
			Silences:   []SourceSilenceSnapshot{baseSilence},
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
	if res.RecheckJobsQueued != 0 {
		t.Fatalf("expected recovered unchanged silence set queue=0 got=%d", res.RecheckJobsQueued)
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
