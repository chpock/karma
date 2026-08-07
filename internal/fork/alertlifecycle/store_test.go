package alertlifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func compileMustGroupingConfig(t *testing.T, cfg GroupingConfig) compiledGroupingConfig {
	t.Helper()
	compiled, err := compileGroupingConfig(cfg)
	if err != nil {
		t.Fatalf("compileGroupingConfig() returned error: %v", err)
	}
	return compiled
}
