package main

import (
	"context"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/filters"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	"github.com/prymitive/karma/internal/models"
)

func TestLifecycleMergePreservesStoredAlertIdentityForResolve(t *testing.T) {
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: t.TempDir() + "/lifecycle.sqlite"})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	previousStore := lifecycleStore
	lifecycleStore = store
	t.Cleanup(func() { lifecycleStore = previousStore })

	current := alertlifecycle.AlertCurrent{
		AlertID:           "persisted-source-alert-id",
		State:             alertlifecycle.StateInactive,
		RawState:          alertlifecycle.StateActive,
		Receiver:          "team-a",
		GroupID:           "group-filtered-labels",
		GroupLabelsJSON:   `{"alertname":"DiskFull"}`,
		LabelsJSON:        `{"alertname":"DiskFull"}`,
		AnnotationsJSON:   `{"summary":"Disk full"}`,
		StartsAt:          mustParseTS(t, "2026-08-01T10:00:00Z"),
		RelevantUpstreams: `["am-a"]`,
	}
	if err = store.UpsertCurrentAlert(current); err != nil {
		t.Fatalf("UpsertCurrentAlert() returned error: %v", err)
	}

	merged := mergeLifecycleStoredAlerts([]models.AlertGroup{}, false)
	if len(merged) != 1 || len(merged[0].Alerts) != 1 {
		t.Fatalf("merged groups=%d alerts=%d, want one reconstructed alert", len(merged), len(merged[0].Alerts))
	}
	reconstructed := merged[0].Alerts[0]
	if got := reconstructed.LabelsFingerprint(); got != current.AlertID {
		t.Fatalf("reconstructed alert ID=%q, want persisted ID=%q", got, current.AlertID)
	}

	live := models.AlertGroup{
		ID:       current.GroupID,
		Receiver: current.Receiver,
		Alerts:   models.AlertList{{LabelsFP: current.AlertID, State: models.AlertStateActive}},
	}
	merged = mergeLifecycleStoredAlerts([]models.AlertGroup{live}, false)
	if len(merged) != 1 || len(merged[0].Alerts) != 1 {
		t.Fatalf("live/stored merge groups=%d alerts=%d, want one deduplicated alert", len(merged), len(merged[0].Alerts))
	}

	comment := "resolved from reconstructed card"
	result, err := store.ResolveAlertContext(context.Background(), reconstructed.LabelsFingerprint(), nil, &comment, time.Now().UTC())
	if err != nil {
		t.Fatalf("ResolveAlertContext() returned error: %v", err)
	}
	if !result.Applied {
		t.Fatal("expected resolve through reconstructed alert ID to be applied")
	}
}

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
		GroupLabelsJSON:   `{"alertname":"DiskFull"}`,
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
		GroupLabelsJSON:   `{"alertname":"CpuHigh"}`,
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

func TestFilterAlertsKeepsSuppressedNonLiveWithoutAlertmanager(t *testing.T) {
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

	now := time.Now().UTC().Truncate(time.Millisecond)
	_, err = lifecycleStore.ApplyPollCycle(alertlifecycle.PollCycleInput{
		Timestamp: now,
		LiveAlerts: []alertlifecycle.LiveAlertSnapshot{{
			AlertID:           "suppressed-past-1",
			State:             alertlifecycle.StateSuppressed,
			Receiver:          "default",
			GroupID:           "g-suppressed-past",
			GroupLabelsJSON:   `{"alertname":"SuppressedPast"}`,
			LabelsJSON:        `{"alertname":"SuppressedPast","severity":"warning"}`,
			AnnotationsJSON:   `{"summary":"suppressed past"}`,
			StartsAt:          now.Add(-2 * time.Hour),
			LastSeenAt:        now,
			RelevantUpstreams: []string{"local"},
		}},
		SuccessfulUpstreams: map[string]bool{"local": true},
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(live insert) returned error: %v", err)
	}
	_, err = lifecycleStore.ApplyPollCycle(alertlifecycle.PollCycleInput{
		Timestamp:           now.Add(time.Minute),
		LiveAlerts:          nil,
		SuccessfulUpstreams: map[string]bool{"local": true},
	})
	if err != nil {
		t.Fatalf("ApplyPollCycle(disappear) returned error: %v", err)
	}

	liveOnly := models.AlertGroup{
		ID:                "g-live-only",
		Receiver:          "default",
		Labels:            models.LabelsFromMap(map[string]string{"alertname": "LiveOnly"}),
		Alerts:            []models.Alert{},
		AlertmanagerCount: map[string]int{},
		StateCount:        map[string]int{},
	}
	liveAlert := models.Alert{
		StartsAt:    now,
		Receiver:    "default",
		Labels:      models.LabelsFromMap(map[string]string{"alertname": "LiveOnly", "instance": "srv-1"}),
		Annotations: models.AnnotationsFromMap(map[string]string{"summary": "live"}),
		State:       models.AlertStateSuppressed,
		RawState:    models.AlertStateSuppressed,
		HasRawState: true,
		IsLive:      true,
		HasIsLive:   true,
		IsResolved:  false,
		Alertmanager: []models.AlertmanagerInstance{{
			Name:  "local",
			State: models.AlertStateSuppressed,
		}},
	}
	liveAlert.UpdateFingerprints()
	liveOnly.Alerts = append(liveOnly.Alerts, liveAlert)
	liveOnly.StateCount = map[string]int{
		models.AlertStateSuppressed.String(): 1,
	}

	groups := mergeLifecycleStoredAlerts([]models.AlertGroup{liveOnly}, false)
	fs := []filters.Filter{
		filters.NewFilter("@state=suppressed"),
		filters.NewFilter("@is_live=false"),
	}
	filtered := filterAlerts(groups, fs)

	count := 0
	for _, g := range filtered {
		for _, a := range g.Alerts {
			if a.State == models.AlertStateSuppressed && a.HasIsLive && !a.IsLive {
				count++
			}
		}
	}

	if count != 1 {
		t.Fatalf("expected 1 suppressed non-live alert after filtering, got=%d", count)
	}
}

func TestMergeLifecycleStoredAlertsRestoresSuppressionCauseForCard(t *testing.T) {
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: t.TempDir() + "/lifecycle.sqlite"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	previousStore := lifecycleStore
	lifecycleStore = store
	t.Cleanup(func() { lifecycleStore = previousStore })

	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	live := alertlifecycle.LiveAlertSnapshot{
		AlertID: "suppressed-card-cause", State: alertlifecycle.StateSuppressed,
		Receiver: "team", GroupID: "group", GroupLabelsJSON: labels, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now,
		RelevantUpstreams: []string{"am-a", "am-b"}, RelevantSources: []alertlifecycle.SourceRef{{Name: "am-a", Cluster: "cluster-a"}, {Name: "am-b", Cluster: "cluster-a"}},
		Targets: []alertlifecycle.LiveTargetSnapshot{{Target: "cluster-a", State: alertlifecycle.StateSuppressed, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-a", "am-b"}}},
		SilencedBySources: []alertlifecycle.SourceSilenceRef{
			{SourceName: "am-a", ClusterID: "cluster-a", SilenceIDs: []string{"sil-card-a"}},
			{SourceName: "am-b", ClusterID: "cluster-a", SilenceIDs: []string{"sil-card-b"}},
		},
	}
	sourceA := alertlifecycle.SourceSilenceState{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []alertlifecycle.SourceSilenceSnapshot{{
		SilenceID: "sil-card-a", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}}}
	sourceB := alertlifecycle.SourceSilenceState{SourceName: "am-b", ClusterID: "cluster-a", SyncStatus: "success", Silences: []alertlifecycle.SourceSilenceSnapshot{{
		SilenceID: "sil-card-b", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}}}
	if _, err = store.ApplyPollCycle(alertlifecycle.PollCycleInput{LiveAlerts: []alertlifecycle.LiveAlertSnapshot{live}, SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true}, SilenceSources: []alertlifecycle.SourceSilenceState{sourceA, sourceB}, Timestamp: now}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyPollCycle(alertlifecycle.PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true}, SilenceSources: []alertlifecycle.SourceSilenceState{sourceA, sourceB}, Timestamp: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}

	groups := mergeLifecycleStoredAlerts(nil, false)
	if len(groups) != 1 || len(groups[0].Alerts) != 1 || len(groups[0].Alerts[0].SuppressionCauses) != 2 {
		t.Fatalf("unexpected merged alert cause shape: %+v", groups)
	}
	causes := groups[0].Alerts[0].SuppressionCauses
	if causes[0].Target != "cluster-a" || causes[0].Source != "am-a" || causes[0].SilenceID != "sil-card-a" || causes[1].Source != "am-b" || causes[1].SilenceID != "sil-card-b" {
		t.Fatalf("unexpected restored card causes: %+v", causes)
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
		GroupLabelsJSON:   `{"alertname":"CpuHigh"}`,
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
		GroupLabelsJSON:   `{"alertname":"DiskHigh"}`,
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

func TestMergeLifecycleStoredAlertsRestoresGroupLabels(t *testing.T) {
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

	now := mustParseTS(t, "2026-08-01T12:00:00Z")
	if err = store.UpsertCurrentAlert(alertlifecycle.AlertCurrent{
		AlertID:           "id-group-labels",
		State:             alertlifecycle.StateInactive,
		Receiver:          "team-a",
		GroupID:           "group-with-labels",
		GroupLabelsJSON:   `{"alertname":"NodeDown","severity":"critical"}`,
		LabelsJSON:        `{"alertname":"NodeDown","severity":"critical","instance":"srv-42"}`,
		AnnotationsJSON:   `{"summary":"Node is down"}`,
		StartsAt:          mustParseTS(t, "2026-07-01T12:00:00Z"),
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() failed: %v", err)
	}

	merged := mergeLifecycleStoredAlerts([]models.AlertGroup{}, false)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged group, got %d", len(merged))
	}
	if merged[0].Labels.Get("alertname") != "NodeDown" {
		t.Fatalf("expected group alertname label to be restored, got=%q", merged[0].Labels.Get("alertname"))
	}
	if merged[0].Labels.Get("severity") != "critical" {
		t.Fatalf("expected group severity label to be restored, got=%q", merged[0].Labels.Get("severity"))
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
