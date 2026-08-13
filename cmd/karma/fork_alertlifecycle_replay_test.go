package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	plabels "github.com/prometheus/prometheus/model/labels"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	"github.com/prymitive/karma/internal/models"
)

type recordedReplay struct {
	Grids []struct {
		AlertGroups []struct {
			ID       string            `json:"id"`
			Receiver string            `json:"receiver"`
			Labels   map[string]string `json:"labels"`
			Alerts   []struct {
				Labels       map[string]string  `json:"labels"`
				Annotations  models.Annotations `json:"annotations"`
				Receiver     string             `json:"receiver"`
				State        string             `json:"state"`
				StartsAt     time.Time          `json:"startsAt"`
				Alertmanager []struct {
					Name       string    `json:"name"`
					Cluster    string    `json:"cluster"`
					State      string    `json:"state"`
					StartsAt   time.Time `json:"startsAt"`
					SilencedBy []string  `json:"silencedBy"`
				} `json:"alertmanager"`
			} `json:"alerts"`
		} `json:"alertGroups"`
	} `json:"grids"`
}

func TestRecordedTrafficReplayThroughLifecycleStore(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "tests", "compress", "alerts.json"))
	if err != nil {
		t.Fatalf("read recorded traffic: %v", err)
	}
	var replay recordedReplay
	if err = json.Unmarshal(raw, &replay); err != nil {
		t.Fatalf("decode recorded traffic: %v", err)
	}

	groups := make([]models.AlertGroup, 0)
	rawLabels := map[string]*string{}
	successful := map[string]bool{}
	sourceByName := map[string]alertlifecycle.SourceSilenceState{}
	for _, grid := range replay.Grids {
		for _, recordedGroup := range grid.AlertGroups {
			group := models.AlertGroup{
				ID: recordedGroup.ID, Receiver: recordedGroup.Receiver,
				Labels: plabels.FromMap(recordedGroup.Labels), Alerts: models.AlertList{},
			}
			for _, recordedAlert := range recordedGroup.Alerts {
				alertLabels := plabels.FromMap(recordedAlert.Labels)
				alert := models.Alert{
					LabelsFP: strconv.FormatUint(alertLabels.Hash(), 16),
					State:    replayAlertState(recordedAlert.State), Receiver: recordedAlert.Receiver,
					Labels: alertLabels, Annotations: recordedAlert.Annotations,
					StartsAt: recordedAlert.StartsAt,
				}
				for _, recordedMember := range recordedAlert.Alertmanager {
					alert.Alertmanager = append(alert.Alertmanager, models.AlertmanagerInstance{
						Name: recordedMember.Name, Cluster: recordedMember.Cluster,
						State: replayAlertState(recordedMember.State), StartsAt: recordedMember.StartsAt,
						SilencedBy: append([]string{}, recordedMember.SilencedBy...),
					})
					successful[recordedMember.Name] = true
					sourceByName[recordedMember.Name] = alertlifecycle.SourceSilenceState{
						SourceName: recordedMember.Name, ClusterID: recordedMember.Cluster, SyncStatus: "success",
					}
				}
				fingerprint := alert.LabelsFingerprint()
				labelsJSON, _ := json.Marshal(recordedAlert.Labels)
				labelsString := string(labelsJSON)
				rawLabels[fingerprint] = &labelsString
				group.Alerts = append(group.Alerts, alert)
			}
			groups = append(groups, group)
		}
	}

	snapshots := buildLiveLifecycleSnapshot(groups, rawLabels)
	if len(groups) < 1000 || len(snapshots) == 0 {
		t.Fatalf("recorded replay did not load expected traffic: groups=%d snapshots=%d", len(groups), len(snapshots))
	}
	sources := make([]alertlifecycle.SourceSilenceState, 0, len(sourceByName))
	for _, source := range sourceByName {
		sources = append(sources, source)
	}
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: filepath.Join(t.TempDir(), "replay.sqlite")})
	if err != nil {
		t.Fatalf("open replay store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err = store.ApplyPollCycle(alertlifecycle.PollCycleInput{
		LiveAlerts: snapshots, SuccessfulUpstreams: successful, SilenceSources: sources,
		Timestamp: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("apply recorded traffic replay: %v", err)
	}

	var target string
	for _, source := range sources {
		if source.ClusterID != "" {
			target = source.ClusterID
			break
		}
	}
	projected, err := store.ListTargetAlertsForPreview(target)
	if err != nil {
		t.Fatalf("read replay target projection: %v", err)
	}
	if len(projected) == 0 {
		t.Fatalf("recorded replay produced no target projection for %q", target)
	}
	labelNames, total, err := store.ListTargetLabelNames(target, "alert", 1000)
	if err != nil {
		t.Fatalf("read replay label index: %v", err)
	}
	if total == 0 || len(labelNames) == 0 || labelNames[0].Name != "alertname" {
		t.Fatalf("unexpected replay label index: total=%d labels=%+v", total, labelNames)
	}
}

func replayAlertState(raw string) models.AlertState {
	switch raw {
	case "active":
		return models.AlertStateActive
	case "suppressed":
		return models.AlertStateSuppressed
	default:
		return models.AlertStateUnprocessed
	}
}
