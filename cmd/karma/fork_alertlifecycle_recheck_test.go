package main

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestRecheckOperatorRoutesValidateBodyAuthenticationAndJob(t *testing.T) {
	path := t.TempDir() + "/lifecycle.sqlite"
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	previousStore := lifecycleStore
	lifecycleStore = store
	t.Cleanup(func() { lifecycleStore = previousStore })
	previousAuth := config.Config.Authentication.Enabled
	config.Config.Authentication.Enabled = false
	t.Cleanup(func() { config.Config.Authentication.Enabled = previousAuth })
	requestWithJob := func(body []byte) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/alerts-history/recheck/1/retry", bytes.NewReader(body))
		routeContext := chi.NewRouteContext()
		routeContext.URLParams.Add("jobID", "1")
		return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeContext))
	}
	requestWithJobID := func(jobID string, body []byte) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/alerts-history/recheck/"+jobID+"/retry", bytes.NewReader(body))
		routeContext := chi.NewRouteContext()
		routeContext.URLParams.Add("jobID", jobID)
		return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeContext))
	}

	t.Run("unknown field is bad request", func(t *testing.T) {
		req := requestWithJob([]byte(`{"requestId":"r1","extra":true}`))
		recorder := httptest.NewRecorder()
		retryRecheckJob(recorder, req)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status=%d, want 400", recorder.Code)
		}
	})

	for name, body := range map[string][]byte{
		"duplicate key":     []byte(`{"requestId":"r1","requestId":"r2"}`),
		"case variant key":  []byte(`{"RequestId":"r1"}`),
		"null reason":       []byte(`{"requestId":"r1","reason":null}`),
		"trailing value":    []byte(`{"requestId":"r1"} {}`),
		"invalid UTF-8":     {0x7b, 0x22, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x22, 0x3a, 0xff, 0x7d},
		"oversized body":    bytes.Repeat([]byte("x"), 4097),
		"non-string ID":     []byte(`{"requestId":true}`),
		"non-string reason": []byte(`{"requestId":"r1","reason":true}`),
		"oversized reason":  []byte(`{"requestId":"r1","reason":"` + strings.Repeat("x", 1025) + `"}`),
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			retryRecheckJob(recorder, requestWithJob(body))
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status=%d, want 400", recorder.Code)
			}
		})
	}

	t.Run("malformed job id is bad request", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		retryRecheckJob(recorder, requestWithJobID("not-a-job", []byte(`{"requestId":"r1"}`)))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status=%d, want 400", recorder.Code)
		}
	})

	for name, requestID := range map[string]string{
		"empty request id":     "",
		"spaced request id":    "not valid",
		"oversized request id": strings.Repeat("x", 129),
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			retryRecheckJob(recorder, requestWithJob([]byte(`{"requestId":"`+requestID+`"}`)))
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status=%d, want 400", recorder.Code)
			}
		})
	}

	t.Run("blank abandon reason is bad request", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		abandonRecheckJob(recorder, requestWithJob([]byte(`{"requestId":"r1","reason":"   "}`)))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status=%d, want 400", recorder.Code)
		}
	})

	t.Run("unknown job is not found", func(t *testing.T) {
		req := requestWithJob([]byte(`{"requestId":"r1"}`))
		recorder := httptest.NewRecorder()
		retryRecheckJob(recorder, req)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("status=%d, want 404", recorder.Code)
		}
	})

	t.Run("completed job maps to conflict", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Millisecond)
		result, err := store.ApplyPollCycle(alertlifecycle.PollCycleInput{
			SuccessfulUpstreams: map[string]bool{"am-route": true},
			SilenceSources: []alertlifecycle.SourceSilenceState{{
				SourceName: "am-route", ClusterID: "cluster-route", SyncStatus: "success",
				Silences: []alertlifecycle.SourceSilenceSnapshot{{
					SilenceID: "route-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
					Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "service", Value: "route", IsEqual: true}},
				}},
			}}, Timestamp: now,
		})
		if err != nil || result.RecheckJobsQueued != 1 {
			t.Fatalf("route job creation result=%+v err=%v", result, err)
		}
		if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
			t.Fatal(err)
		}
		recorder := httptest.NewRecorder()
		retryRecheckJob(recorder, requestWithJob([]byte(`{"requestId":"completed-retry"}`)))
		if recorder.Code != http.StatusConflict {
			t.Fatalf("status=%d, want 409", recorder.Code)
		}
	})

	t.Run("successful retry and abandon have exact bodies and one wake each", func(t *testing.T) {
		nowRaw := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := db.Exec(`UPDATE silence_recheck_jobs SET status='failed',finished_at=? WHERE job_id=1`, nowRaw); err != nil {
			t.Fatal(err)
		}
		wakeCalls := 0
		store.SetRecheckCommitWake(func() { wakeCalls++ })
		var logs bytes.Buffer
		previousLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
		t.Cleanup(func() { slog.SetDefault(previousLogger) })
		retryBody := []byte(`{"requestId":"route-retry","reason":"private retry reason"}`)
		recorder := httptest.NewRecorder()
		retryRecheckJob(recorder, requestWithJob(retryBody))
		wantRetry := `{"status":"success","jobId":1,"action":"retry","requestId":"route-retry","applied":true,"currentStatus":"queued"}`
		if recorder.Code != http.StatusOK || strings.TrimSpace(recorder.Body.String()) != wantRetry || wakeCalls != 1 {
			t.Fatalf("retry status=%d body=%s wakes=%d", recorder.Code, recorder.Body.String(), wakeCalls)
		}
		replay := httptest.NewRecorder()
		retryRecheckJob(replay, requestWithJob(retryBody))
		if replay.Code != http.StatusOK || strings.TrimSpace(replay.Body.String()) != wantRetry || wakeCalls != 1 {
			t.Fatalf("retry replay status=%d body=%s wakes=%d", replay.Code, replay.Body.String(), wakeCalls)
		}
		changed := httptest.NewRecorder()
		retryRecheckJob(changed, requestWithJob([]byte(`{"requestId":"route-retry","reason":"changed"}`)))
		if changed.Code != http.StatusConflict {
			t.Fatalf("changed retry status=%d", changed.Code)
		}
		if _, err := db.Exec(`UPDATE silence_recheck_jobs SET status='failed',finished_at=? WHERE job_id=1`, nowRaw); err != nil {
			t.Fatal(err)
		}
		abandon := httptest.NewRecorder()
		abandonRecheckJob(abandon, requestWithJob([]byte(`{"requestId":"route-abandon","reason":"private abandon reason"}`)))
		wantAbandon := `{"status":"success","jobId":1,"action":"abandon","requestId":"route-abandon","applied":true,"currentStatus":"abandoned"}`
		if abandon.Code != http.StatusOK || strings.TrimSpace(abandon.Body.String()) != wantAbandon || wakeCalls != 2 {
			t.Fatalf("abandon status=%d body=%s wakes=%d", abandon.Code, abandon.Body.String(), wakeCalls)
		}
		if text := logs.String(); strings.Contains(text, "private retry reason") || strings.Contains(text, "private abandon reason") {
			t.Fatalf("operator reason leaked to runtime logs: %s", text)
		}
	})

	t.Run("configured route prefix reaches operator handler", func(t *testing.T) {
		previousPrefix := config.Config.Listen.Prefix
		config.Config.Listen.Prefix = "/review-prefix"
		defer func() { config.Config.Listen.Prefix = previousPrefix }()
		router := chi.NewRouter()
		router.Post(getViewURL("/alerts-history/recheck/{jobID}/retry"), retryRecheckJob)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/review-prefix/alerts-history/recheck/1/retry", bytes.NewReader([]byte(`{"requestId":"prefixed"}`))))
		if recorder.Code != http.StatusConflict {
			t.Fatalf("prefixed route status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("missing actor is unauthorized", func(t *testing.T) {
		config.Config.Authentication.Enabled = true
		req := requestWithJob([]byte(`{"requestId":"r1"}`))
		recorder := httptest.NewRecorder()
		retryRecheckJob(recorder, req)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("status=%d, want 401", recorder.Code)
		}
		config.Config.Authentication.Enabled = false
	})

	t.Run("authentication wins over disabled storage", func(t *testing.T) {
		config.Config.Authentication.Enabled = true
		previous := lifecycleStore
		lifecycleStore = nil
		req := requestWithJob([]byte(`{"requestId":"r1"}`))
		recorder := httptest.NewRecorder()
		retryRecheckJob(recorder, req)
		lifecycleStore = previous
		config.Config.Authentication.Enabled = false
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("status=%d, want 401", recorder.Code)
		}
	})
}
