package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	plabels "github.com/prometheus/prometheus/model/labels"

	"github.com/prymitive/karma/internal/alertmanager"
	"github.com/prymitive/karma/internal/models"
)

func TestBuildLiveLifecycleSnapshotUsesRawMatchLabelsIndex(t *testing.T) {
	alert := models.Alert{
		LabelsFP:    "fp-1",
		State:       models.AlertStateActive,
		Receiver:    "team-a",
		Labels:      plabels.FromMap(map[string]string{"alertname": "CPUHigh"}),
		Annotations: models.Annotations{},
		StartsAt:    time.Now().UTC().Add(-time.Minute),
		Alertmanager: []models.AlertmanagerInstance{
			{Name: "am-a", Cluster: "cluster-a"},
		},
	}
	group := models.AlertGroup{
		ID:       "g-1",
		Receiver: "team-a",
		Labels:   plabels.FromMap(map[string]string{"alertname": "CPUHigh"}),
		Alerts:   models.AlertList{alert},
	}

	raw := `{"alertname":"CPUHigh","namespace":"prod"}`
	snapshots := buildLiveLifecycleSnapshot([]models.AlertGroup{group}, map[string]*string{"fp-1": &raw})
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got=%d", len(snapshots))
	}
	if snapshots[0].MatchLabelsJSON == nil {
		t.Fatal("expected MatchLabelsJSON to be set")
	}
	if *snapshots[0].MatchLabelsJSON != raw {
		t.Fatalf("expected raw match labels %q, got %q", raw, *snapshots[0].MatchLabelsJSON)
	}
}

func TestBuildLiveLifecycleSnapshotMergesTargetsAcrossGroups(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	base := models.Alert{
		LabelsFP: "shared-fp", State: models.AlertStateActive,
		Labels:      plabels.FromMap(map[string]string{"alertname": "CPUHigh"}),
		Annotations: models.Annotations{}, StartsAt: now.Add(-time.Minute),
	}
	alertA := base
	alertA.Receiver = "team-a"
	alertA.Alertmanager = []models.AlertmanagerInstance{{
		Name: "am-a", Cluster: "cluster-a", State: models.AlertStateActive, StartsAt: now.Add(-time.Minute),
	}}
	alertB := base
	alertB.Receiver = "team-b"
	alertB.Alertmanager = []models.AlertmanagerInstance{{
		Name: "am-b", Cluster: "cluster-b", State: models.AlertStateSuppressed, StartsAt: now.Add(-2 * time.Minute),
	}}
	groups := []models.AlertGroup{
		{ID: "g-b", Receiver: "team-b", Labels: plabels.FromMap(map[string]string{"route": "b"}), Alerts: models.AlertList{alertB}},
		{ID: "g-a", Receiver: "team-a", Labels: plabels.FromMap(map[string]string{"route": "a"}), Alerts: models.AlertList{alertA}},
	}

	snapshots := buildLiveLifecycleSnapshot(groups, map[string]*string{})
	if len(snapshots) != 1 {
		t.Fatalf("expected one merged snapshot, got=%d", len(snapshots))
	}
	got := snapshots[0]
	if got.GroupID != "g-a" || got.Receiver != "team-a" {
		t.Fatalf("expected deterministic canonical group g-a/team-a, got=%s/%s", got.GroupID, got.Receiver)
	}
	if got.State != "active" {
		t.Fatalf("expected active-first merged state, got=%s", got.State)
	}
	if len(got.Targets) != 2 || got.Targets[0].Target != "cluster-a" || got.Targets[1].Target != "cluster-b" {
		t.Fatalf("expected both target memberships, got=%+v", got.Targets)
	}
	if len(got.RelevantUpstreams) != 2 || got.RelevantUpstreams[0] != "am-a" || got.RelevantUpstreams[1] != "am-b" {
		t.Fatalf("expected both upstreams, got=%+v", got.RelevantUpstreams)
	}
}

func TestBuildSilenceSourceSnapshotExcludesUnknownStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			_, _ = w.Write([]byte("alertmanager_build_info{version=\"0.27.0\"} 1\n"))
		case "/api/v2/alerts/groups":
			_, _ = w.Write([]byte(`[]`))
		case "/api/v2/silences":
			_, _ = w.Write([]byte(`[
  {
    "id":"valid",
    "status":{"state":"active"},
    "startsAt":"2026-01-01T00:00:00Z",
    "endsAt":"2026-01-02T00:00:00Z",
	"updatedAt":"2026-01-01T01:00:00Z",
    "createdBy":"operator",
    "comment":"valid",
    "matchers":[{"name":"service","value":"api","isRegex":false}]
  },
  {
    "id":"unknown",
    "status":{"state":"future-state"},
    "startsAt":"2026-01-01T00:00:00Z",
    "endsAt":"2026-01-02T00:00:00Z",
	"updatedAt":"2026-01-01T01:00:00Z",
    "createdBy":"operator",
    "comment":"invalid",
    "matchers":[]
  }
]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	am, err := alertmanager.NewAlertmanager("cluster-a", "am-a", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err = am.Pull(); err != nil {
		t.Fatal(err)
	}
	if warnings := am.SilenceWarnings(); len(warnings) != 1 || warnings[0].SilenceID != "unknown" {
		t.Fatalf("warnings=%+v, want unknown rule warning", warnings)
	}

	sources := buildSilenceSourceSnapshotFrom([]*alertmanager.Alertmanager{am}, map[string]bool{"am-a": true})
	if len(sources) != 1 || sources[0].SyncStatus != "success" {
		t.Fatalf("sources=%+v, want one successful source", sources)
	}
	if len(sources[0].Silences) != 1 || sources[0].Silences[0].SilenceID != "valid" {
		t.Fatalf("silences=%+v, want only valid rule", sources[0].Silences)
	}
}
