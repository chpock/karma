package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func seedProblem3Review4RouterJob(t *testing.T) (*alertlifecycle.Store, *sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.sqlite")
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err = db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,total_candidates,processed_candidates,created_at,started_at,finished_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, "review4-router-context", "sha256:review4-router-job", `{"formatVersion":1,"sources":[],"clusters":[]}`, "source_context_changed", "failed", 0, 0, now, now, now); err != nil {
		t.Fatal(err)
	}
	return store, db, now
}

func TestProblem3Review4ApplicationRouterAuthenticationAndActorPropagation(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previousLogger)
	store, db, now := seedProblem3Review4RouterJob(t)
	previousStore := lifecycleStore
	previousConfig := config.Config
	lifecycleStore = store
	config.Config.Listen.Prefix = "/review4"
	config.Config.Authentication.Enabled = true
	config.Config.Authentication.Header.Name = "X-Review4-User"
	config.Config.Authentication.Header.ValueRegex = "(.+)"
	t.Cleanup(func() {
		lifecycleStore = previousStore
		config.Config = previousConfig
	})

	router := chi.NewRouter()
	setupRouter(router, nil)
	wakeCalls := 0
	store.SetRecheckCommitWake(func() { wakeCalls++ })

	beforeUnauthorized := captureProblem4RouterState(t, db)
	for _, action := range []string{"retry", "abandon"} {
		body := `{"requestId":"review4-unauthorized"}`
		if action == "abandon" {
			body = `{"requestId":"review4-unauthorized","reason":"denied"}`
		}
		unauthorizedRequest := httptest.NewRequest(http.MethodPost, "/review4/alerts-history/recheck/1/"+action, bytes.NewBufferString(body))
		unauthorizedRequest.Header.Set("Content-Type", "application/json")
		unauthorizedRequest.Header.Set(recheckOperatorRequestHeader, "recheck-operator")
		unauthorized := httptest.NewRecorder()
		router.ServeHTTP(unauthorized, unauthorizedRequest)
		if unauthorized.Code != http.StatusUnauthorized || unauthorized.Body.String() != "Access denied\n" {
			t.Fatalf("application router %s unauthorized status=%d body=%q", action, unauthorized.Code, unauthorized.Body.String())
		}
	}
	if after := captureProblem4RouterState(t, db); after != beforeUnauthorized || strings.Contains(logs.String(), `"event":"operator_action_rejected"`) {
		t.Fatalf("global auth boundary reached operator handler: state_changed=%v logs=%s", after != beforeUnauthorized, logs.String())
	}

	request := httptest.NewRequest(http.MethodPost, "/review4/alerts-history/recheck/1/retry", bytes.NewBufferString(`{"requestId":"review4-retry","reason":"retry"}`))
	request.Header.Set("X-Review4-User", "alice")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(recheckOperatorRequestHeader, "recheck-operator")
	retry := httptest.NewRecorder()
	router.ServeHTTP(retry, request)
	var retryBody map[string]any
	if err := json.Unmarshal(retry.Body.Bytes(), &retryBody); err != nil {
		t.Fatal(err)
	}
	expectedRetry := map[string]any{"status": "success", "jobId": float64(1), "action": "retry", "requestId": "review4-retry", "applied": true, "currentStatus": "queued"}
	if retry.Code != http.StatusOK || !reflect.DeepEqual(retryBody, expectedRetry) || wakeCalls != 1 {
		t.Fatalf("application router retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	var actor string
	if err := db.QueryRowContext(context.Background(), `SELECT actor FROM silence_recheck_job_operator_actions WHERE request_id='review4-retry'`).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if actor != "alice" {
		t.Fatalf("application auth actor=%q want=alice", actor)
	}
	beforeConflict := captureProblem4RouterState(t, db)
	conflictRequest := httptest.NewRequest(http.MethodPost, "/review4/alerts-history/recheck/1/retry", bytes.NewBufferString(`{"requestId":"review4-retry","reason":"changed"}`))
	conflictRequest.Header.Set("X-Review4-User", "alice")
	conflictRequest.Header.Set("Content-Type", "application/json")
	conflictRequest.Header.Set(recheckOperatorRequestHeader, "recheck-operator")
	conflict := httptest.NewRecorder()
	router.ServeHTTP(conflict, conflictRequest)
	if conflict.Code != http.StatusConflict || conflict.Body.String() != `{"error":"recheck operator action conflict"}` || captureProblem4RouterState(t, db) != beforeConflict || wakeCalls != 1 {
		t.Fatalf("request conflict status=%d body=%s state_changed=%v wakes=%d", conflict.Code, conflict.Body.String(), captureProblem4RouterState(t, db) != beforeConflict, wakeCalls)
	}

	if _, err := db.Exec(`UPDATE silence_recheck_jobs SET status='failed',started_at=?,finished_at=? WHERE job_id=1`, now, now); err != nil {
		t.Fatal(err)
	}
	abandonRequest := httptest.NewRequest(http.MethodPost, "/review4/alerts-history/recheck/1/abandon", bytes.NewBufferString(`{"requestId":"review4-abandon","reason":"done"}`))
	abandonRequest.Header.Set("X-Review4-User", "bob")
	abandonRequest.Header.Set("Content-Type", "application/json")
	abandonRequest.Header.Set(recheckOperatorRequestHeader, "recheck-operator")
	abandon := httptest.NewRecorder()
	router.ServeHTTP(abandon, abandonRequest)
	var abandonBody map[string]any
	if err := json.Unmarshal(abandon.Body.Bytes(), &abandonBody); err != nil {
		t.Fatal(err)
	}
	expectedAbandon := map[string]any{"status": "success", "jobId": float64(1), "action": "abandon", "requestId": "review4-abandon", "applied": true, "currentStatus": "abandoned"}
	if abandon.Code != http.StatusOK || !reflect.DeepEqual(abandonBody, expectedAbandon) || wakeCalls != 2 {
		t.Fatalf("application router abandon status=%d body=%s", abandon.Code, abandon.Body.String())
	}
	if err := db.QueryRowContext(context.Background(), `SELECT actor FROM silence_recheck_job_operator_actions WHERE request_id='review4-abandon'`).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if actor != "bob" {
		t.Fatalf("application auth abandon actor=%q want=bob", actor)
	}
	var actionRows, writerRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_operator_actions`).Scan(&actionRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM lifecycle_writer_commits WHERE operation_kind='recheck_operator_action'`).Scan(&writerRows); err != nil {
		t.Fatal(err)
	}
	if actionRows != 2 || writerRows != 2 {
		t.Fatalf("operator durable rows actions=%d writer=%d", actionRows, writerRows)
	}
}
