package main

import (
	"testing"
	"time"

	plabels "github.com/prometheus/prometheus/model/labels"
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
