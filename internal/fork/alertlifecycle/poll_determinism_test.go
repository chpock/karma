package alertlifecycle

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPollEventPayloadOrderingIsStableAcrossAlertsTargetsSourcesAndSilences(t *testing.T) {
	previousLogger := slog.Default()
	var logOutput bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)

	collect := func(alertOrder []int, reverseTargets, reverseSources bool) ([]string, PollCycleInput) {
		logOutput.Reset()
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = store.Close() }()
		matchLabels := `[{"name":"alertname","value":"A","isRegex":false,"isEqual":true}]`
		alerts := []LiveAlertSnapshot{
			{AlertID: "deterministic-a", State: StateSuppressed, Receiver: "team-a", GroupID: "group-a", GroupLabelsJSON: `{}`, LabelsJSON: `{"alertname":"A","instance":"one"}`, MatchLabelsJSON: &matchLabels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a", "am-b"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-a"}, {Name: "am-b", Cluster: "cluster-b"}}, SilencedBySources: []SourceSilenceRef{{SourceName: "am-a", ClusterID: "cluster-a", SilenceIDs: []string{"sil-a"}}, {SourceName: "am-b", ClusterID: "cluster-b", SilenceIDs: []string{"sil-b"}}}, Targets: []LiveTargetSnapshot{{Target: "cluster-a", State: StateSuppressed, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-a"}}, {Target: "cluster-b", State: StateActive, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-b"}}}},
			{AlertID: "deterministic-b", State: StateActive, Receiver: "team-b", GroupID: "group-b", GroupLabelsJSON: `{}`, LabelsJSON: `{"alertname":"B","instance":"two"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-2 * time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-b", "am-a"}, RelevantSources: []SourceRef{{Name: "am-b", Cluster: "cluster-b"}, {Name: "am-a", Cluster: "cluster-a"}}, Targets: []LiveTargetSnapshot{{Target: "cluster-b", State: StateActive, StartsAt: now.Add(-2 * time.Hour), SourceNames: []string{"am-b"}}, {Target: "cluster-a", State: StateActive, StartsAt: now.Add(-2 * time.Hour), SourceNames: []string{"am-a"}}}},
			{AlertID: "deterministic-c", State: StateUnprocessed, Receiver: "team-c", GroupID: "group-c", GroupLabelsJSON: `{}`, LabelsJSON: `{"alertname":"C","instance":"three"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-3 * time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a", "am-b"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-a"}, {Name: "am-b", Cluster: "cluster-b"}}, Targets: []LiveTargetSnapshot{{Target: "cluster-a", State: StateUnprocessed, StartsAt: now.Add(-3 * time.Hour), SourceNames: []string{"am-a"}}, {Target: "cluster-b", State: StateUnprocessed, StartsAt: now.Add(-3 * time.Hour), SourceNames: []string{"am-b"}}}},
		}
		sources := []SourceSilenceState{
			{SourceName: "am-b", ClusterID: "cluster-b", SyncStatus: "success", Silences: []SourceSilenceSnapshot{{SilenceID: "sil-b", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), CreatedBy: "test", Comment: "B", Matchers: []SilenceMatcherSnapshot{{Name: "alertname", Value: "A", IsEqual: true}}}, {SilenceID: "sil-b-2", Status: "pending", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(2 * time.Hour), CreatedBy: "test", Comment: "B2", Matchers: []SilenceMatcherSnapshot{{Name: "instance", Value: "two", IsEqual: true}}}}},
			{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{{SilenceID: "sil-a", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), CreatedBy: "test", Comment: "A", Matchers: []SilenceMatcherSnapshot{{Name: "alertname", Value: "A", IsEqual: true}}}, {SilenceID: "sil-a-2", Status: "pending", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(2 * time.Hour), CreatedBy: "test", Comment: "A2", Matchers: []SilenceMatcherSnapshot{{Name: "instance", Value: "three", IsEqual: true}}}}},
		}
		if reverseSources {
			sources[0], sources[1] = sources[1], sources[0]
		}
		input := PollCycleInput{Timestamp: now, SuccessfulUpstreams: map[string]bool{"am-b": true, "am-a": true}, SilenceSources: sources}
		for _, index := range alertOrder {
			alert := alerts[index]
			if reverseTargets {
				alert.Targets[0], alert.Targets[1] = alert.Targets[1], alert.Targets[0]
			}
			input.LiveAlerts = append(input.LiveAlerts, alert)
		}
		encodedInput, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		var before PollCycleInput
		if err = json.Unmarshal(encodedInput, &before); err != nil {
			t.Fatal(err)
		}
		if _, err = store.ApplyPollCycle(input); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(input, before) {
			t.Fatal("ApplyPollCycle mutated caller-owned input")
		}
		var normalized []string
		for _, line := range strings.Split(strings.TrimSpace(logOutput.String()), "\n") {
			var record map[string]any
			if err = json.Unmarshal([]byte(line), &record); err != nil || record["event"] == nil {
				continue
			}
			delete(record, "time")
			if _, hasDuration := record["duration"]; hasDuration {
				record["duration"] = "present"
			}
			encoded, marshalErr := json.Marshal(record)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			normalized = append(normalized, string(encoded))
		}
		return normalized, input
	}

	first, _ := collect([]int{0, 1, 2}, false, false)
	second, _ := collect([]int{2, 0, 1}, true, true)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("poll event payload order depends on input order:\nfirst=%v\nsecond=%v", first, second)
	}
}

func TestSuccessfulUpstreamKeysUseCanonicalOrder(t *testing.T) {
	got := sortedSuccessfulUpstreamNames(map[string]bool{"am-b": true, "am-a": false, "am-c": true})
	want := []string{"am-a", "am-b", "am-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("successful upstream key order: got=%v want=%v", got, want)
	}
}
