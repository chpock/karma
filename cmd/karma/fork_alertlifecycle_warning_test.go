package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestAlertLifecycleWarningShowsPersistentWarningWhenDBNotExpected(t *testing.T) {
	store := setupLifecycleStoreForResolveTest(t)

	prevEnabled := config.Config.AlertLifecycle.Enabled
	config.Config.AlertLifecycle.Enabled = true
	t.Cleanup(func() {
		config.Config.AlertLifecycle.Enabled = prevEnabled
	})

	prevKeep := append([]string{}, config.Config.Labels.Keep...)
	prevKeepRE := append([]string{}, config.Config.Labels.KeepRegex...)
	prevStrip := append([]string{}, config.Config.Labels.Strip...)
	prevStripRE := append([]string{}, config.Config.Labels.StripRegex...)
	config.Config.Labels.Keep = []string{"alertname"}
	config.Config.Labels.KeepRegex = nil
	config.Config.Labels.Strip = []string{"instance"}
	config.Config.Labels.StripRegex = nil
	t.Cleanup(func() {
		config.Config.Labels.Keep = prevKeep
		config.Config.Labels.KeepRegex = prevKeepRE
		config.Config.Labels.Strip = prevStrip
		config.Config.Labels.StripRegex = prevStripRE
		_ = store
	})

	req := httptest.NewRequest(http.MethodGet, "/alerts-history/warning", http.NoBody)
	rr := httptest.NewRecorder()
	alertLifecycleWarning(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200 got=%d body=%s", rr.Code, rr.Body.String())
	}

	resp := decodeWarningResponse(t, rr)
	if resp.DBExpectedState {
		t.Fatalf("expected DBExpectedState=false, got true: %+v", resp)
	}
	if resp.RegroupStatus != "ready" {
		t.Fatalf("expected regroupStatus=ready, got=%q", resp.RegroupStatus)
	}
	if resp.Reason == "" {
		t.Fatal("expected warning reason to be non-empty")
	}
}

func TestAlertLifecycleWarningClearsAfterRegroupReachesExpectedState(t *testing.T) {
	store := setupLifecycleStoreForResolveTest(t)

	prevEnabled := config.Config.AlertLifecycle.Enabled
	config.Config.AlertLifecycle.Enabled = true
	t.Cleanup(func() {
		config.Config.AlertLifecycle.Enabled = prevEnabled
	})

	prevKeep := append([]string{}, config.Config.Labels.Keep...)
	prevKeepRE := append([]string{}, config.Config.Labels.KeepRegex...)
	prevStrip := append([]string{}, config.Config.Labels.Strip...)
	prevStripRE := append([]string{}, config.Config.Labels.StripRegex...)
	config.Config.Labels.Keep = []string{"alertname"}
	config.Config.Labels.KeepRegex = nil
	config.Config.Labels.Strip = []string{"instance"}
	config.Config.Labels.StripRegex = nil
	t.Cleanup(func() {
		config.Config.Labels.Keep = prevKeep
		config.Config.Labels.KeepRegex = prevKeepRE
		config.Config.Labels.Strip = prevStrip
		config.Config.Labels.StripRegex = prevStripRE
	})

	now := mustParseTS(t, "2026-08-01T12:00:00Z")
	if err := store.UpsertCurrentAlert(alertlifecycle.AlertCurrent{
		AlertID:           "id-warning-regroup",
		State:             alertlifecycle.StateInactive,
		Receiver:          "team-a",
		GroupID:           "legacy-group",
		GroupLabelsJSON:   `{"alertname":"NodeDown","instance":"srv-1"}`,
		LabelsJSON:        `{"alertname":"NodeDown","instance":"srv-1"}`,
		AnnotationsJSON:   `{"summary":"node down"}`,
		StartsAt:          now,
		LastSeenAt:        &now,
		RelevantUpstreams: `["am-a"]`,
	}); err != nil {
		t.Fatalf("UpsertCurrentAlert() setup failed: %v", err)
	}

	_, err := store.RunRegroupToConfig(context.Background(), alertlifecycle.GroupingConfig{
		LabelsKeep:       append([]string{}, config.Config.Labels.Keep...),
		LabelsKeepRegex:  append([]string{}, config.Config.Labels.KeepRegex...),
		LabelsStrip:      append([]string{}, config.Config.Labels.Strip...),
		LabelsStripRegex: append([]string{}, config.Config.Labels.StripRegex...),
	}, 1000)
	if err != nil {
		t.Fatalf("RunRegroupToConfig() returned error: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/alerts-history/warning", http.NoBody)
	rr := httptest.NewRecorder()
	alertLifecycleWarning(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200 got=%d body=%s", rr.Code, rr.Body.String())
	}

	resp := decodeWarningResponse(t, rr)
	if !resp.DBExpectedState {
		t.Fatalf("expected DBExpectedState=true after regroup, got false: %+v", resp)
	}
	if resp.Reason != "" {
		t.Fatalf("expected empty warning reason after regroup, got=%q", resp.Reason)
	}
	if resp.RegroupStatus != "ready" {
		t.Fatalf("expected regroupStatus=ready, got=%q", resp.RegroupStatus)
	}
}

func TestAlertLifecycleWarningReportsSilenceHAInconsistency(t *testing.T) {
	store := setupLifecycleStoreForResolveTest(t)
	prevEnabled := config.Config.AlertLifecycle.Enabled
	config.Config.AlertLifecycle.Enabled = true
	t.Cleanup(func() { config.Config.AlertLifecycle.Enabled = prevEnabled })
	now := mustParseTS(t, "2026-08-01T12:00:00Z")
	_, err := store.ApplyPollCycle(alertlifecycle.PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true},
		SilenceSources: []alertlifecycle.SourceSilenceState{
			{SourceName: "am-a", ClusterID: "cluster-ha", SyncStatus: "success", Silences: []alertlifecycle.SourceSilenceSnapshot{{
				SilenceID: "a", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
			}}},
			{SourceName: "am-b", ClusterID: "cluster-ha", SyncStatus: "success", Silences: []alertlifecycle.SourceSilenceSnapshot{{
				SilenceID: "b", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "service", Value: "payments", IsEqual: true}},
			}}},
		},
		Timestamp: now,
	})
	if err != nil {
		t.Fatalf("seed HA inconsistency: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/alerts-history/warning", http.NoBody)
	rr := httptest.NewRecorder()
	alertLifecycleWarning(rr, req)
	resp := decodeWarningResponse(t, rr)
	if resp.DBExpectedState || len(resp.SilenceInconsistentClusters) != 1 || resp.SilenceInconsistentClusters[0] != "cluster-ha" {
		t.Fatalf("expected persistent HA inconsistency warning, got=%+v", resp)
	}
}

func decodeWarningResponse(t *testing.T, rr *httptest.ResponseRecorder) alertLifecycleWarningResponse {
	t.Helper()

	var resp alertLifecycleWarningResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode warning response: %v, raw=%s", err, rr.Body.String())
	}
	return resp
}
