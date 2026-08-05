package main

import (
	"testing"
	"time"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	"github.com/prymitive/karma/internal/models"
)

func TestMergeLifecycleStoredAlertsResolvedVisibilityByFilter(t *testing.T) {
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: t.TempDir() + "/lifecycle.sqlite"})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	prev := lifecycleStore
	lifecycleStore = store
	t.Cleanup(func() {
		lifecycleStore = prev
	})

	resolved := alertlifecycle.AlertCurrent{
		AlertID:           "id-resolved-2",
		State:             alertlifecycle.StateResolved,
		Receiver:          "team-a",
		GroupID:           "group-r",
		LabelsJSON:        `{"alertname":"DiskFull","instance":"srv-2"}`,
		AnnotationsJSON:   `{"summary":"Disk full"}`,
		StartsAt:          mustParseTS(t, "2026-08-01T10:00:00Z"),
		RelevantUpstreams: `["am-a"]`,
	}
	if err = store.UpsertCurrentAlert(resolved); err != nil {
		t.Fatalf("UpsertCurrentAlert(resolved) failed: %v", err)
	}

	inactive := alertlifecycle.AlertCurrent{
		AlertID:           "id-inactive-1",
		State:             alertlifecycle.StateInactive,
		Receiver:          "team-a",
		GroupID:           "group-i",
		LabelsJSON:        `{"alertname":"CpuHigh","instance":"srv-3"}`,
		AnnotationsJSON:   `{"summary":"CPU high"}`,
		StartsAt:          mustParseTS(t, "2026-08-01T11:00:00Z"),
		RelevantUpstreams: `["am-a"]`,
	}
	if err = store.UpsertCurrentAlert(inactive); err != nil {
		t.Fatalf("UpsertCurrentAlert(inactive) failed: %v", err)
	}

	withoutResolved := mergeLifecycleStoredAlerts([]models.AlertGroup{}, false)
	if len(withoutResolved) != 1 {
		t.Fatalf("expected 1 group without resolved, got %d", len(withoutResolved))
	}
	if !containsAlertState(withoutResolved, models.AlertStateInactive) {
		t.Fatal("expected inactive alert in merged result")
	}
	if containsAlertState(withoutResolved, models.AlertStateResolved) {
		t.Fatal("did not expect resolved alert without explicit include")
	}

	withResolved := mergeLifecycleStoredAlerts([]models.AlertGroup{}, true)
	if len(withResolved) != 2 {
		t.Fatalf("expected 2 groups with resolved included, got %d", len(withResolved))
	}
	if !containsAlertState(withResolved, models.AlertStateInactive) {
		t.Fatal("expected inactive alert in merged result")
	}
	if !containsAlertState(withResolved, models.AlertStateResolved) {
		t.Fatal("expected resolved alert when explicitly included")
	}
}

func TestMergeLifecycleStoredAlertsUsesLastSeenForRecency(t *testing.T) {
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: t.TempDir() + "/lifecycle.sqlite"})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	prev := lifecycleStore
	lifecycleStore = store
	t.Cleanup(func() {
		lifecycleStore = prev
	})

	oldLastSeen := mustParseTS(t, "2026-08-01T10:00:00Z")
	if err = store.UpsertCurrentAlert(alertlifecycle.AlertCurrent{
		AlertID:           "id-inactive-old",
		State:             alertlifecycle.StateInactive,
		Receiver:          "team-a",
		GroupID:           "group-inactive-old",
		LabelsJSON:        `{"alertname":"CpuHigh","instance":"srv-old"}`,
		AnnotationsJSON:   `{"summary":"CPU high"}`,
		StartsAt:          mustParseTS(t, "2026-07-01T10:00:00Z"),
		LastSeenAt:        &oldLastSeen,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert(old inactive) failed: %v", err)
	}

	newLastSeen := mustParseTS(t, "2026-08-01T12:00:00Z")
	if err = store.UpsertCurrentAlert(alertlifecycle.AlertCurrent{
		AlertID:           "id-inactive-new",
		State:             alertlifecycle.StateInactive,
		Receiver:          "team-a",
		GroupID:           "group-inactive-new",
		LabelsJSON:        `{"alertname":"DiskHigh","instance":"srv-new"}`,
		AnnotationsJSON:   `{"summary":"Disk high"}`,
		StartsAt:          mustParseTS(t, "2026-07-01T10:00:00Z"),
		LastSeenAt:        &newLastSeen,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert(new inactive) failed: %v", err)
	}

	merged := mergeLifecycleStoredAlerts([]models.AlertGroup{}, false)
	if len(merged) != 2 {
		t.Fatalf("expected 2 merged groups, got %d", len(merged))
	}

	byID := map[string]models.AlertGroup{}
	for _, group := range merged {
		byID[group.ID] = group
	}
	newGroup, foundNew := byID["group-inactive-new"]
	oldGroup, foundOld := byID["group-inactive-old"]
	if !foundNew || !foundOld {
		t.Fatalf("expected both lifecycle groups in merge result, got IDs: %#v", map[string]bool{"group-inactive-new": foundNew, "group-inactive-old": foundOld})
	}

	if !newGroup.LatestStartsAt.Equal(newLastSeen) {
		t.Fatalf("expected new group LatestStartsAt=%s, got %s", newLastSeen.UTC().Format(time.RFC3339), newGroup.LatestStartsAt.UTC().Format(time.RFC3339))
	}
	if !oldGroup.LatestStartsAt.Equal(oldLastSeen) {
		t.Fatalf("expected old group LatestStartsAt=%s, got %s", oldLastSeen.UTC().Format(time.RFC3339), oldGroup.LatestStartsAt.UTC().Format(time.RFC3339))
	}
	if !newGroup.LatestStartsAt.After(oldGroup.LatestStartsAt) {
		t.Fatalf("expected new group recency (%s) to be after old group (%s)", newGroup.LatestStartsAt.UTC().Format(time.RFC3339), oldGroup.LatestStartsAt.UTC().Format(time.RFC3339))
	}
}

func containsAlertState(groups []models.AlertGroup, state models.AlertState) bool {
	for _, group := range groups {
		for _, alert := range group.Alerts {
			if alert.State == state {
				return true
			}
		}
	}
	return false
}

func mustParseTS(t *testing.T, raw string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("failed to parse timestamp %q: %v", raw, err)
	}
	return ts
}
