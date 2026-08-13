package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestResolveAlertLogsStructuredSuccessWithoutComment(t *testing.T) {
	store := setupLifecycleStoreForResolveTest(t)
	seedInactiveAlert(t, store, "id-handler-log")

	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	req := httptest.NewRequest(http.MethodPost, "/alerts/id-handler-log/resolve", bytes.NewReader([]byte(`{"comment":"secret resolve comment"}`)))
	req = withRouteParam(req, "id", "id-handler-log")
	rr := httptest.NewRecorder()
	resolveAlert(rr, req)

	logged := output.String()
	for _, expected := range []string{`"level":"INFO"`, `"component":"alert_lifecycle"`, `"event":"resolve_applied"`, `"alert_id":"id-handler-log"`, `"new_state":"resolved"`} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("missing log field %s in %s", expected, logged)
		}
	}
	if strings.Contains(logged, "secret resolve comment") {
		t.Fatalf("resolve comment leaked into log: %s", logged)
	}
}

func TestResolveAlertHandlerSuccess(t *testing.T) {
	store := setupLifecycleStoreForResolveTest(t)
	seedInactiveAlert(t, store, "id-handler-ok")

	body := []byte(`{"comment":"resolved by operator"}`)
	req := httptest.NewRequest(http.MethodPost, "/alerts/id-handler-ok/resolve", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = withRouteParam(req, "id", "id-handler-ok")

	rr := httptest.NewRecorder()
	resolveAlert(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200 got=%d body=%s", rr.Code, rr.Body.String())
	}

	resp := map[string]any{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["newState"] != "resolved" {
		t.Fatalf("unexpected newState: %#v", resp["newState"])
	}
}

func TestResolveAlertHandlerConflict(t *testing.T) {
	store := setupLifecycleStoreForResolveTest(t)
	seedAlertWithState(t, store, "id-handler-conflict", alertlifecycle.StateActive)

	req := httptest.NewRequest(http.MethodPost, "/alerts/id-handler-conflict/resolve", http.NoBody)
	req = withRouteParam(req, "id", "id-handler-conflict")

	rr := httptest.NewRecorder()
	resolveAlert(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected status 409 got=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestResolveAlertHandlerNotFound(t *testing.T) {
	_ = setupLifecycleStoreForResolveTest(t)

	req := httptest.NewRequest(http.MethodPost, "/alerts/missing/resolve", http.NoBody)
	req = withRouteParam(req, "id", "missing")

	rr := httptest.NewRecorder()
	resolveAlert(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected status 404 got=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestResolveAlertHandlerBadJSON(t *testing.T) {
	store := setupLifecycleStoreForResolveTest(t)
	seedInactiveAlert(t, store, "id-handler-bad-json")

	req := httptest.NewRequest(http.MethodPost, "/alerts/id-handler-bad-json/resolve", bytes.NewReader([]byte("{")))
	req = withRouteParam(req, "id", "id-handler-bad-json")

	rr := httptest.NewRecorder()
	resolveAlert(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 got=%d body=%s", rr.Code, rr.Body.String())
	}
}

func setupLifecycleStoreForResolveTest(t *testing.T) *alertlifecycle.Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenStore() returned error: %v", err)
	}
	lifecycleStore = store
	t.Cleanup(func() {
		closeAlertLifecycleStorage()
	})

	return store
}

func seedInactiveAlert(t *testing.T, store *alertlifecycle.Store, alertID string) {
	t.Helper()
	seedAlertWithState(t, store, alertID, alertlifecycle.StateInactive)
}

func seedAlertWithState(t *testing.T, store *alertlifecycle.Store, alertID string, state alertlifecycle.State) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := store.UpsertCurrentAlert(alertlifecycle.AlertCurrent{
		AlertID:           alertID,
		State:             state,
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
}

func withRouteParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, value)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}
