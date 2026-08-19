package main

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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

	unauthorized := httptest.NewRecorder()
	router.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/review4/alerts-history/recheck/1/retry", bytes.NewBufferString(`{"requestId":"review4-unauthorized"}`)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("application router unauthorized status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}

	request := httptest.NewRequest(http.MethodPost, "/review4/alerts-history/recheck/1/retry", bytes.NewBufferString(`{"requestId":"review4-retry","reason":"retry"}`))
	request.Header.Set("X-Review4-User", "alice")
	retry := httptest.NewRecorder()
	router.ServeHTTP(retry, request)
	if retry.Code != http.StatusOK || !strings.Contains(retry.Body.String(), `"currentStatus":"queued"`) {
		t.Fatalf("application router retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	var actor string
	if err := db.QueryRowContext(context.Background(), `SELECT actor FROM silence_recheck_job_operator_actions WHERE request_id='review4-retry'`).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if actor != "alice" {
		t.Fatalf("application auth actor=%q want=alice", actor)
	}

	if _, err := db.Exec(`UPDATE silence_recheck_jobs SET status='failed',started_at=?,finished_at=? WHERE job_id=1`, now, now); err != nil {
		t.Fatal(err)
	}
	abandonRequest := httptest.NewRequest(http.MethodPost, "/review4/alerts-history/recheck/1/abandon", bytes.NewBufferString(`{"requestId":"review4-abandon","reason":"done"}`))
	abandonRequest.Header.Set("X-Review4-User", "bob")
	abandon := httptest.NewRecorder()
	router.ServeHTTP(abandon, abandonRequest)
	if abandon.Code != http.StatusOK || !strings.Contains(abandon.Body.String(), `"currentStatus":"abandoned"`) {
		t.Fatalf("application router abandon status=%d body=%s", abandon.Code, abandon.Body.String())
	}
	if err := db.QueryRowContext(context.Background(), `SELECT actor FROM silence_recheck_job_operator_actions WHERE request_id='review4-abandon'`).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if actor != "bob" {
		t.Fatalf("application auth abandon actor=%q want=bob", actor)
	}
}
