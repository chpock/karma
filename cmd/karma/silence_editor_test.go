package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"

	"github.com/prymitive/karma/internal/alertmanager"
	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	"github.com/prymitive/karma/internal/mock"
	"github.com/prymitive/karma/internal/models"
)

func TestSilenceEditorPreviewRequiresTarget(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	mockAlerts(mock.ListAllMocks()[0])
	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"matchers": []map[string]any{{
			"name":    "alertname",
			"value":   ".*",
			"isRegex": true,
			"isEqual": true,
		}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing target, got %d: %s", resp.Code, resp.Body.String())
	}
}

func TestFindSilenceEditorStatusUsesRequestTime(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	silence := models.Silence{ID: "silence-1", EndsAt: now.Add(time.Minute)}
	member := writableMember{
		Name: "am-a",
		FetchSilenceByID: func(_ context.Context, id string) (models.Silence, error) {
			if id == silence.ID {
				return silence, nil
			}
			return models.Silence{}, errors.New("not found")
		},
	}
	status, err := findSilenceEditorStatus(context.Background(), member, "silence-1", now)
	if err != nil || status.IsExpired || status.Member != "am-a" {
		t.Fatalf("expected active request-time status, err=%v status=%+v", err, status)
	}
	status, err = findSilenceEditorStatus(context.Background(), member, "silence-1", now.Add(2*time.Minute))
	if err != nil || !status.IsExpired {
		t.Fatalf("expected expired request-time status, err=%v status=%+v", err, status)
	}
}

func TestSilenceEditorEditRejectsRuleChangedAfterStatusRead(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	silenceACLs = nil

	currentComment := strings.Repeat("large-existing-comment-", 4096)
	originalComment := currentComment
	getCalls := 0
	postCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/silence/silence-1":
			getCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{
  "id":"silence-1",
  "status":{"state":"active"},
  "matchers":[{"name":"service","value":"api","isRegex":false,"isEqual":true}],
  "startsAt":"2099-01-01T10:00:00Z",
  "endsAt":"2099-01-01T12:00:00Z",
  "createdAt":"2099-01-01T09:00:00Z",
  "updatedAt":"2099-01-01T09:30:00Z",
  "createdBy":"operator@example.com",
  "comment":%q,
  "annotations":{"ticket":"OPS-1"}
}`, currentComment)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/silences":
			postCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"silenceID":"silence-1"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	am, err := alertmanager.NewAlertmanager(
		"prod-concurrency",
		"am-concurrency",
		upstream.URL,
		alertmanager.WithProxy(true),
		alertmanager.WithRequestTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = alertmanager.RegisterAlertmanager(am); err != nil {
		t.Fatal(err)
	}

	r := testRouter()
	setupRouter(r, nil)
	statusReq := httptest.NewRequest(http.MethodGet, "/silence-editor/status?target=prod-concurrency&id=silence-1", nil)
	statusResp := httptest.NewRecorder()
	r.ServeHTTP(statusResp, statusReq)
	if statusResp.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", statusResp.Code, statusResp.Body.String())
	}
	var status struct {
		IsExpired     bool           `json:"isExpired"`
		Member        string         `json:"member"`
		RevisionToken string         `json:"revisionToken"`
		Silence       models.Silence `json:"silence"`
	}
	if err = json.Unmarshal(statusResp.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Member != "am-concurrency" || status.RevisionToken == "" || status.Silence.Comment != originalComment {
		t.Fatalf("unexpected status response: %+v", status)
	}

	currentComment = "changed by another operator"
	submitBody := map[string]any{
		"target":            "prod-concurrency",
		"operation":         "edit",
		"originalSilenceID": "silence-1",
		"routeMember":       status.Member,
		"revisionToken":     status.RevisionToken,
		"payload": map[string]any{
			"id": "silence-1",
			"matchers": []map[string]any{{
				"name": "service", "value": "api", "isRegex": false, "isEqual": true,
			}},
			"startsAt": "2099-01-01T10:00:00Z", "endsAt": "2099-01-01T12:30:00Z",
			"createdBy": "operator@example.com", "comment": "my edit",
		},
	}
	raw, _ := json.Marshal(submitBody)
	submitReq := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
	submitReq.Header.Set("Content-Type", "application/json")
	submitResp := httptest.NewRecorder()
	r.ServeHTTP(submitResp, submitReq)
	if submitResp.Code != http.StatusConflict {
		t.Fatalf("submit code=%d body=%s", submitResp.Code, submitResp.Body.String())
	}
	if !strings.Contains(submitResp.Body.String(), "changed after the editor was opened") {
		t.Fatalf("unexpected conflict body: %s", submitResp.Body.String())
	}
	if getCalls != 2 || postCalls != 0 {
		t.Fatalf("GET calls=%d POST calls=%d, want 2 and 0", getCalls, postCalls)
	}
}

func TestSilenceEditorRevisionTokenCanonicalizesMatcherOrder(t *testing.T) {
	now := time.Date(2099, 1, 1, 9, 0, 0, 0, time.UTC)
	a := models.Silence{
		ID: "silence-1", StartsAt: now.Add(time.Hour), EndsAt: now.Add(2 * time.Hour), CreatedAt: now,
		CreatedBy: "operator", Comment: "maintenance",
		Matchers: []models.SilenceMatcher{
			models.NewSilenceMatcher("service", "api", false, true),
			models.NewSilenceMatcher("severity", "warning|critical", true, true),
		},
	}
	b := a
	b.Matchers = []models.SilenceMatcher{a.Matchers[1], a.Matchers[0]}
	if silenceEditorRevisionToken(a) != silenceEditorRevisionToken(b) {
		t.Fatal("equivalent matcher order produced different revision tokens")
	}
	b.Comment = "changed"
	if silenceEditorRevisionToken(a) == silenceEditorRevisionToken(b) {
		t.Fatal("content change did not change revision token")
	}
}

func TestValidateSilenceEditorEditAtSubmitUsesRequestTime(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	active := models.Silence{ID: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Minute)}
	member := writableMember{
		Name: "am-a",
		FetchSilenceByID: func(_ context.Context, id string) (models.Silence, error) {
			if id == active.ID {
				return active, nil
			}
			return models.Silence{}, &silenceEditorFreshReadError{StatusCode: http.StatusNotFound, Err: errors.New("not found")}
		},
	}
	payload := models.Silence{ID: "active"}
	token := silenceEditorRevisionToken(active)
	if err := validateSilenceEditorOperationAtSubmit(context.Background(), member, "edit", "active", token, payload, now); err != nil {
		t.Fatalf("active edit unexpectedly rejected: %v", err)
	}
	if err := validateSilenceEditorOperationAtSubmit(context.Background(), member, "edit", "active", token, payload, now.Add(2*time.Minute)); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected request-time expiry rejection, got=%v", err)
	}
	payload.ID = "missing"
	if err := validateSilenceEditorOperationAtSubmit(context.Background(), member, "edit", "missing", token, payload, now); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected missing edit rejection, got=%v", err)
	}
	payload.ID = ""
	if err := validateSilenceEditorOperationAtSubmit(context.Background(), writableMember{}, "create", "", "", payload, now); err != nil {
		t.Fatalf("create/recreate must not require an existing ID: %v", err)
	}
}

func TestMemberRoutedStatusAndSubmitRejectHAReplicationLag(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	active := models.Silence{ID: "silence-ha", EndsAt: now.Add(time.Hour)}
	expired := models.Silence{ID: "silence-ha", EndsAt: now.Add(-time.Hour)}
	memberA := writableMember{
		Name: "am-a", FetchSilenceByID: func(context.Context, string) (models.Silence, error) { return expired, nil },
	}
	memberB := writableMember{
		Name: "am-b", FetchSilenceByID: func(context.Context, string) (models.Silence, error) { return active, nil },
	}

	statusA, err := findSilenceEditorStatus(context.Background(), memberA, active.ID, now)
	if err != nil || !statusA.IsExpired || statusA.Member != "am-a" {
		t.Fatalf("unexpected selected-member status: err=%v status=%+v", err, statusA)
	}
	if err := validateSilenceEditorOperationAtSubmit(context.Background(), memberB, "recreate", active.ID, silenceEditorRevisionToken(active), models.Silence{}, now); err == nil || !strings.Contains(err.Error(), "still active") {
		t.Fatalf("recreate routed to active HA member must be rejected, got=%v", err)
	}
	if err := validateSilenceEditorOperationAtSubmit(context.Background(), memberA, "edit", active.ID, silenceEditorRevisionToken(expired), models.Silence{ID: active.ID}, now); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("edit routed to expired HA member must be rejected, got=%v", err)
	}
}

func TestValidateSilenceEditorOperationBindsPayloadIdentity(t *testing.T) {
	now := time.Now().UTC()
	member := writableMember{
		Name: "am-a",
		FetchSilenceByID: func(_ context.Context, id string) (models.Silence, error) {
			switch id {
			case "active-a", "active-b":
				return models.Silence{ID: id, EndsAt: now.Add(time.Hour)}, nil
			case "expired-a":
				return models.Silence{ID: id, EndsAt: now.Add(-time.Hour)}, nil
			default:
				return models.Silence{}, errors.New("not found")
			}
		},
	}
	if err := validateSilenceEditorOperationAtSubmit(context.Background(), member, "edit", "active-a", "token", models.Silence{ID: "active-b"}, now); err == nil || !strings.Contains(err.Error(), "must match") {
		t.Fatalf("edit must not validate A then mutate B: %v", err)
	}
	if err := validateSilenceEditorOperationAtSubmit(context.Background(), member, "recreate", "expired-a", "token", models.Silence{ID: "active-b"}, now); err == nil || !strings.Contains(err.Error(), "must not contain") {
		t.Fatalf("recreate must not mutate an existing ID: %v", err)
	}
	activeA, _ := member.FetchSilenceByID(context.Background(), "active-a")
	if err := validateSilenceEditorOperationAtSubmit(context.Background(), member, "edit", "active-a", silenceEditorRevisionToken(activeA), models.Silence{ID: "active-a"}, now); err != nil {
		t.Fatalf("matching edit identity rejected: %v", err)
	}
	expiredA, _ := member.FetchSilenceByID(context.Background(), "expired-a")
	if err := validateSilenceEditorOperationAtSubmit(context.Background(), member, "recreate", "expired-a", silenceEditorRevisionToken(expiredA), models.Silence{}, now); err != nil {
		t.Fatalf("empty-ID recreate rejected: %v", err)
	}
}

func TestSilenceEditorMutationIdentityExhaustiveProduct(t *testing.T) {
	now := time.Now().UTC()
	type memberState string
	const (
		memberActive      memberState = "active"
		memberExpired     memberState = "expired"
		memberMissing     memberState = "missing"
		memberUnavailable memberState = "unavailable"
	)
	operations := []string{"create", "edit", "recreate"}
	routeRelations := []string{"same", "changed", "missing"}
	memberStates := []memberState{memberActive, memberExpired, memberMissing, memberUnavailable}
	for _, operation := range operations {
		for _, routeRelation := range routeRelations {
			for _, state := range memberStates {
				for _, proxy := range []bool{false, true} {
					name := operation + "/route=" + routeRelation + "/member=" + string(state) + "/proxy=" + boolName(proxy)
					t.Run(name, func(t *testing.T) {
						member := writableMember{Name: "am-a", Healthy: state != memberUnavailable, ProxyRequests: proxy}
						member.FetchSilenceByID = func(_ context.Context, id string) (models.Silence, error) {
							switch state {
							case memberActive:
								return models.Silence{ID: id, EndsAt: now.Add(time.Hour)}, nil
							case memberExpired:
								return models.Silence{ID: id, EndsAt: now.Add(-time.Hour)}, nil
							case memberMissing, memberUnavailable:
								return models.Silence{}, errors.New("not found")
							}
							return models.Silence{}, errors.New("unknown member state")
						}
						route := map[string]string{"same": "am-a", "changed": "am-b", "missing": ""}[routeRelation]
						originalID := ""
						payload := models.Silence{}
						switch operation {
						case "edit":
							originalID = "silence-a"
							payload.ID = originalID
						case "recreate":
							originalID = "silence-a"
						}
						var token string
						if operation != "create" && (state == memberActive || state == memberExpired) {
							original, _ := member.FetchSilenceByID(context.Background(), originalID)
							token = silenceEditorRevisionToken(original)
						}
						err := validateSilenceEditorMutationIdentity(context.Background(), member, route, operation, originalID, token, payload, now)
						wantAllowed := operation == "create" || (routeRelation == "same" && state != memberUnavailable && ((operation == "edit" && state == memberActive) || (operation == "recreate" && state == memberExpired)))
						if (err == nil) != wantAllowed {
							t.Fatalf("allowed=%v err=%v", wantAllowed, err)
						}
					})
				}
			}
		}
	}
}

func TestSilenceEditorMutationIdentityRejectsMissingFields(t *testing.T) {
	now := time.Now().UTC()
	member := writableMember{
		Name: "am-a", Healthy: true,
		FetchSilenceByID: func(_ context.Context, id string) (models.Silence, error) {
			return models.Silence{ID: id, EndsAt: now.Add(time.Hour)}, nil
		},
	}
	tests := []struct {
		name, route, operation, original string
		payload                          models.Silence
	}{
		{name: "missing-operation"},
		{name: "unknown-operation", operation: "clone"},
		{name: "edit-missing-original", route: "am-a", operation: "edit", payload: models.Silence{ID: "silence-a"}},
		{name: "recreate-missing-original", route: "am-a", operation: "recreate"},
		{name: "create-with-original", operation: "create", original: "silence-a"},
		{name: "create-with-payload-id", operation: "create", payload: models.Silence{ID: "silence-a"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateSilenceEditorMutationIdentity(context.Background(), member, tc.route, tc.operation, tc.original, "", tc.payload, now); err == nil {
				t.Fatal("malformed identity was accepted")
			}
		})
	}
}

func TestSilenceEditorMutationOutcomeClassificationProduct(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		err        error
		unknown    bool
	}{
		{name: "success", statusCode: http.StatusOK},
		{name: "known-4xx-json", statusCode: http.StatusBadRequest, err: errors.New("rejected")},
		{name: "known-4xx-text", statusCode: http.StatusForbidden, err: errors.New("forbidden")},
		{name: "server-5xx", statusCode: http.StatusInternalServerError, err: errors.New("upstream"), unknown: true},
		{name: "malformed-2xx", statusCode: http.StatusOK, err: errors.New("malformed"), unknown: true},
		{name: "network", err: &net.DNSError{Err: "timeout", IsTimeout: true}, unknown: true},
		{name: "url", err: &url.Error{Op: "Post", URL: "http://am", Err: errors.New("closed")}, unknown: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := outcomeUnknown(tc.statusCode, tc.err); got != tc.unknown {
				t.Fatalf("outcomeUnknown(%d,%v)=%v want=%v", tc.statusCode, tc.err, got, tc.unknown)
			}
		})
	}
}

func boolName(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func TestSilenceEditorPreviewRejectsExcessiveMatchersAndRequestBody(t *testing.T) {
	matchers := make([]silenceEditorMatcher, silenceEditorMatcherLimit+1)
	for i := range matchers {
		matchers[i] = silenceEditorMatcher{Name: "service", Value: "api", IsEqual: true}
	}
	body, err := json.Marshal(silenceEditorPreviewRequest{Target: "cluster-a", Matchers: matchers})
	if err != nil {
		t.Fatalf("marshal matcher request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(body))
	w := httptest.NewRecorder()
	silenceEditorPreview(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected excessive matcher count 400, got=%d body=%s", w.Code, w.Body.String())
	}

	oversized := `{"target":"cluster-a","matchers":[{"name":"service","value":"api","isEqual":true}],"padding":"` + strings.Repeat("x", int(silenceEditorPreviewMaxRequestBytes)) + `"}`
	req = httptest.NewRequest(http.MethodPost, "/silence-editor/preview", strings.NewReader(oversized))
	w = httptest.NewRecorder()
	silenceEditorPreview(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected oversized request 400, got=%d body=%s", w.Code, w.Body.String())
	}
}

func TestSilenceEditorPreviewRejectsBroadMatcherSet(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	mockAlerts(mock.ListAllMocks()[0])
	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"target": "default",
		"matchers": []map[string]any{{
			"name":    "alertname",
			"value":   ".*",
			"isRegex": true,
			"isEqual": true,
		}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for broad matcher set, got %d: %s", resp.Code, resp.Body.String())
	}
}

func TestSilenceEditorPreviewRejectsInvalidRE2WithoutPanic(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	mockAlerts(mock.ListAllMocks()[0])
	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"target": "default",
		"matchers": []map[string]any{{
			"name":    "service",
			"value":   "[",
			"isRegex": true,
			"isEqual": true,
		}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid RE2 matcher, got %d: %s", resp.Code, resp.Body.String())
	}
	var out struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if out.Error == "" {
		t.Fatal("expected non-empty validation error")
	}
}

func TestSilenceEditorPreviewReturnsTargetScopedResults(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	mockAlerts(mock.ListAllMocks()[0])
	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"target": "default",
		"matchers": []map[string]any{{
			"name":    "alertname",
			"value":   "Host_Down",
			"isRegex": false,
			"isEqual": true,
		}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}

	var payload struct {
		Status string `json:"status"`
		Target string `json:"target"`
		Total  int    `json:"total"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if payload.Status != "success" {
		t.Fatalf("expected status=success, got %q", payload.Status)
	}
	if payload.Target != "default" {
		t.Fatalf("expected target=default, got %q", payload.Target)
	}
	if payload.Total < 1 {
		t.Fatalf("expected at least one matched alert, got %d", payload.Total)
	}
}

func TestSilenceEditorPreviewRejectsInvalidCursor(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	mockAlerts(mock.ListAllMocks()[0])
	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"target": "default",
		"cursor": "not-a-valid-cursor",
		"matchers": []map[string]any{{
			"name":    "alertname",
			"value":   "Host_Down",
			"isRegex": false,
			"isEqual": true,
		}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid cursor, got %d: %s", resp.Code, resp.Body.String())
	}
}

func TestSilenceEditorPreviewRejectsCursorForDifferentMatcherSet(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	mockAlerts(mock.ListAllMocks()[0])
	r := testRouter()
	setupRouter(r, nil)

	firstReqBody := map[string]any{
		"target": "default",
		"limit":  1,
		"matchers": []map[string]any{{
			"name":    "alertname",
			"value":   "Host_.*",
			"isRegex": true,
			"isEqual": true,
		}},
	}
	firstRaw, _ := json.Marshal(firstReqBody)
	firstReq := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(firstRaw))
	firstReq.Header.Set("Content-Type", "application/json")
	firstResp := httptest.NewRecorder()
	r.ServeHTTP(firstResp, firstReq)
	if firstResp.Code != http.StatusOK {
		t.Fatalf("expected first response 200, got %d: %s", firstResp.Code, firstResp.Body.String())
	}
	var firstPayload struct {
		NextCursor string `json:"nextCursor"`
	}
	if err := json.Unmarshal(firstResp.Body.Bytes(), &firstPayload); err != nil {
		t.Fatalf("failed to parse first response: %v", err)
	}
	if strings.TrimSpace(firstPayload.NextCursor) == "" {
		t.Fatal("expected non-empty nextCursor")
	}

	secondReqBody := map[string]any{
		"target": "default",
		"cursor": firstPayload.NextCursor,
		"matchers": []map[string]any{{
			"name":    "alertname",
			"value":   "Host_Down",
			"isRegex": false,
			"isEqual": true,
		}},
	}
	secondRaw, _ := json.Marshal(secondReqBody)
	secondReq := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(secondRaw))
	secondReq.Header.Set("Content-Type", "application/json")
	secondResp := httptest.NewRecorder()
	r.ServeHTTP(secondResp, secondReq)

	if secondResp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for mismatched cursor fingerprint, got %d: %s", secondResp.Code, secondResp.Body.String())
	}
}

func TestSilenceEditorPreviewReusesImmutableSnapshotAndRejectsEvictedSnapshot(t *testing.T) {
	resetSilenceEditorPreviewSnapshots()
	t.Cleanup(resetSilenceEditorPreviewSnapshots)
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	mockAlerts(mock.ListAllMocks()[0])
	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"target": "default",
		"matchers": []map[string]any{{
			"name": "alertname", "value": "Host_Down", "isRegex": false, "isEqual": true,
		}},
	}
	raw, _ := json.Marshal(body)
	firstReq := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(raw))
	firstReq.Header.Set("Content-Type", "application/json")
	firstResp := httptest.NewRecorder()
	r.ServeHTTP(firstResp, firstReq)
	if firstResp.Code != http.StatusOK {
		t.Fatalf("expected first response 200, got %d: %s", firstResp.Code, firstResp.Body.String())
	}
	var first struct {
		Snapshot string `json:"snapshot"`
		Total    int    `json:"total"`
	}
	if err := json.Unmarshal(firstResp.Body.Bytes(), &first); err != nil {
		t.Fatalf("parse first response: %v", err)
	}
	if first.Snapshot == "" {
		t.Fatal("expected snapshot token")
	}

	body["snapshot"] = first.Snapshot
	raw, _ = json.Marshal(body)
	secondReq := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(raw))
	secondReq.Header.Set("Content-Type", "application/json")
	secondResp := httptest.NewRecorder()
	r.ServeHTTP(secondResp, secondReq)
	if secondResp.Code != http.StatusOK {
		t.Fatalf("expected cached response 200, got %d: %s", secondResp.Code, secondResp.Body.String())
	}
	var second struct {
		Snapshot string `json:"snapshot"`
		Total    int    `json:"total"`
	}
	if err := json.Unmarshal(secondResp.Body.Bytes(), &second); err != nil {
		t.Fatalf("parse second response: %v", err)
	}
	if second.Snapshot != first.Snapshot || second.Total != first.Total {
		t.Fatalf("snapshot reuse mismatch: first=%+v second=%+v", first, second)
	}

	resetSilenceEditorPreviewSnapshots()
	raw, _ = json.Marshal(body)
	thirdReq := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(raw))
	thirdReq.Header.Set("Content-Type", "application/json")
	thirdResp := httptest.NewRecorder()
	r.ServeHTTP(thirdResp, thirdReq)
	if thirdResp.Code != http.StatusConflict {
		t.Fatalf("expected expired snapshot 409, got %d: %s", thirdResp.Code, thirdResp.Body.String())
	}
}

func TestSilenceEditorPreviewAlternationAnchoringMatchesWholeValue(t *testing.T) {
	compiled, err := compileSilenceEditorMatchers([]silenceEditorMatcher{{
		Name:    "service",
		Value:   "api|payments",
		IsRegex: true,
		IsEqual: true,
	}})
	if err != nil {
		t.Fatalf("expected compile success: %v", err)
	}

	cases := []struct {
		value string
		match bool
	}{
		{value: "api", match: true},
		{value: "payments", match: true},
		{value: "api-v2", match: false},
		{value: "legacy-payments", match: false},
	}
	for _, tc := range cases {
		if silenceEditorMatcherMatches(compiled[0], tc.value) != tc.match {
			t.Fatalf("unexpected match result for %q", tc.value)
		}
	}
}

func TestFindSilenceEditorPageStartUsesKeysetOrdering(t *testing.T) {
	ls1 := "2026-08-10T11:00:00Z"
	ls2 := "2026-08-10T10:00:00Z"
	alerts := []silenceEditorPreviewAlert{
		{ID: "a-prev", State: "active", IsLive: true, LastSeenAt: &ls1},
		{ID: "cursor-alert", State: "active", IsLive: true, LastSeenAt: &ls1},
		{ID: "next-alert", State: "active", IsLive: true, LastSeenAt: &ls2},
	}
	cursor := &silenceEditorPreviewCursor{
		Version:     1,
		Fingerprint: "fp",
		Rank:        previewStateRank("active", true),
		LastSeenAt:  &ls1,
		ID:          "cursor-alert",
	}
	start := findSilenceEditorPageStart(alerts, cursor)
	if start != 2 {
		t.Fatalf("expected page start at index 2 after cursor, got %d", start)
	}
}

func TestSilenceEditorPreviewFingerprintUsesUnambiguousStructuredEncoding(t *testing.T) {
	oneMatcher, err := compileSilenceEditorMatchers([]silenceEditorMatcher{{
		Name: "a", Value: "x;b|e=|y", IsEqual: true,
	}})
	if err != nil {
		t.Fatalf("compile one matcher: %v", err)
	}
	twoMatchers, err := compileSilenceEditorMatchers([]silenceEditorMatcher{
		{Name: "a", Value: "x", IsEqual: true},
		{Name: "b", Value: "y", IsEqual: true},
	})
	if err != nil {
		t.Fatalf("compile two matchers: %v", err)
	}
	if got, want := silenceEditorPreviewFingerprint("cluster-a", oneMatcher), silenceEditorPreviewFingerprint("cluster-a", twoMatchers); got == want {
		t.Fatalf("different structured matcher sets produced the same fingerprint: %s", got)
	}
}

func TestSilenceEditorLabelsRequiresTarget(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	mockAlerts(mock.ListAllMocks()[0])
	r := testRouter()
	setupRouter(r, nil)

	req := httptest.NewRequest(http.MethodGet, "/silence-editor/labels", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing target, got %d: %s", resp.Code, resp.Body.String())
	}
}

func TestSortSilenceEditorPreviewAlertsUsesContractOrder(t *testing.T) {
	newer := "2026-08-09T12:00:00Z"
	older := "2026-08-08T12:00:00Z"
	alerts := []silenceEditorPreviewAlert{
		{ID: "resolved", State: "resolved", LastSeenAt: &newer},
		{ID: "inactive-old", State: "inactive", LastSeenAt: &older},
		{ID: "active-z", State: "active", StartsAt: "2030-01-01T00:00:00Z"},
		{ID: "suppressed-current", State: "suppressed", IsLive: true},
		{ID: "inactive-new", State: "inactive", LastSeenAt: &newer},
		{ID: "active-a", State: "active", StartsAt: "2020-01-01T00:00:00Z"},
	}

	sortSilenceEditorPreviewAlerts(alerts)

	want := []string{
		"active-a",
		"active-z",
		"suppressed-current",
		"inactive-new",
		"inactive-old",
		"resolved",
	}
	for i, id := range want {
		if alerts[i].ID != id {
			t.Fatalf("unexpected alert at index %d: got %q, want %q", i, alerts[i].ID, id)
		}
	}
}

func TestSilenceEditorMatcherUsesPrecompiledRegexWithoutRecompile(t *testing.T) {
	re := regexp.MustCompile("^api$")

	compiled := compiledSilenceEditorMatcher{
		matcher: models.NewSilenceMatcher("service", "api", true, true),
		regex:   re,
	}
	compiled.matcher.Value = "["

	if !silenceEditorMatcherMatches(compiled, "api") {
		t.Fatal("expected precompiled regex matcher to match")
	}
}

func TestSilenceEditorSubmitPreservesOperatorAuthor(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	silenceACLs = nil

	var capturedCreatedBy string
	var capturedMatcherName string
	var capturedComment string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/silences" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		var payload struct {
			CreatedBy string `json:"createdBy"`
			Comment   string `json:"comment"`
			Matchers  []struct {
				Name string `json:"name"`
			} `json:"matchers"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		capturedCreatedBy = payload.CreatedBy
		capturedComment = payload.Comment
		if len(payload.Matchers) > 0 {
			capturedMatcherName = payload.Matchers[0].Name
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"silenceID":"silence-123"}`))
	}))
	defer upstream.Close()

	am, err := alertmanager.NewAlertmanager(
		"prod-eu",
		"am-submit-preserve-author",
		upstream.URL,
		alertmanager.WithProxy(true),
		alertmanager.WithRequestTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create alertmanager: %v", err)
	}
	if err := alertmanager.RegisterAlertmanager(am); err != nil {
		t.Fatalf("failed to register alertmanager: %v", err)
	}

	r := testRouter()
	setupRouter(r, nil)

	largeComment := strings.Repeat("large-proxy-comment-", 4096)
	body := map[string]any{
		"target":    "prod-eu",
		"operation": "create",
		"payload": map[string]any{
			"matchers": []map[string]any{{
				"name":    "alertname",
				"value":   "Host_Down",
				"isRegex": false,
				"isEqual": true,
			}},
			"startsAt":  "2026-01-01T10:00:00Z",
			"endsAt":    "2026-01-01T11:00:00Z",
			"createdBy": "operator@example.com",
			"comment":   largeComment,
		},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
	req = req.WithContext(context.WithValue(req.Context(), authUserKey("user"), "auth-user"))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if capturedCreatedBy != "operator@example.com" {
		t.Fatalf("expected createdBy preserved, got %q", capturedCreatedBy)
	}
	if capturedMatcherName != "alertname" {
		t.Fatalf("expected matcher name normalized in submitted payload, got %q", capturedMatcherName)
	}
	if capturedComment != largeComment {
		t.Fatalf("submitted comment length=%d, want %d", len(capturedComment), len(largeComment))
	}
	var out struct {
		Status      string `json:"status"`
		Target      string `json:"target"`
		SilenceID   string `json:"silenceID"`
		SubmittedBy string `json:"submittedBy"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if out.SubmittedBy != "auth-user" {
		t.Fatalf("expected submittedBy=auth-user got=%q", out.SubmittedBy)
	}
	if out.SilenceID != "silence-123" {
		t.Fatalf("expected silenceID=silence-123 got=%q", out.SilenceID)
	}
	if out.Target != "prod-eu" {
		t.Fatalf("expected target=prod-eu got=%q", out.Target)
	}
}

func TestSilenceEditorConfiguredPayloadLimitRejectsProxyAndDirectBeforeRouting(t *testing.T) {
	for _, proxy := range []bool{true, false} {
		t.Run(boolName(proxy), func(t *testing.T) {
			alertmanager.UnregisterAll()
			upstreamSetup = false
			mockConfig(t.Setenv)
			t.Cleanup(func() {
				alertmanager.UnregisterAll()
				upstreamSetup = false
			})
			config.Config.SilenceEditor.MaxPayloadBytes = 1024
			t.Cleanup(func() { config.Config.SilenceEditor.MaxPayloadBytes = 0 })
			silenceACLs = nil

			upstreamCalls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				upstreamCalls++
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"silenceID":"unexpected"}`))
			}))
			defer upstream.Close()

			am, err := alertmanager.NewAlertmanager(
				"limited-target",
				"limited-member",
				upstream.URL,
				alertmanager.WithProxy(proxy),
				alertmanager.WithExternalURI(upstream.URL),
				alertmanager.WithRequestTimeout(5*time.Second),
			)
			if err != nil {
				t.Fatalf("NewAlertmanager() returned error: %v", err)
			}
			if err = alertmanager.RegisterAlertmanager(am); err != nil {
				t.Fatalf("RegisterAlertmanager() returned error: %v", err)
			}

			r := testRouter()
			setupRouter(r, nil)
			raw, _ := json.Marshal(map[string]any{
				"target": "limited-target", "operation": "create",
				"payload": map[string]any{
					"matchers": []map[string]any{{"name": "service", "value": "api", "isRegex": false, "isEqual": true}},
					"startsAt": "2026-01-01T10:00:00Z", "endsAt": "2026-01-01T11:00:00Z",
					"createdBy": "operator@example.com", "comment": strings.Repeat("x", 2048),
				},
			})
			req := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, req)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s, want 400", resp.Code, resp.Body.String())
			}
			if !strings.Contains(resp.Body.String(), "request body too large") {
				t.Fatalf("unexpected response: %s", resp.Body.String())
			}
			if upstreamCalls != 0 {
				t.Fatalf("upstream calls=%d, want 0", upstreamCalls)
			}
		})
	}
}

func TestFetchSilenceEditorSilenceUsesConfiguredPayloadLimit(t *testing.T) {
	largeComment := strings.Repeat("large-existing-comment-", 4096)
	response, err := json.Marshal(map[string]any{
		"id": "silence-large", "status": map[string]string{"state": "active"},
		"matchers": []map[string]any{{"name": "service", "value": "api", "isRegex": false, "isEqual": true}},
		"startsAt": "2099-01-01T10:00:00Z", "endsAt": "2099-01-01T12:00:00Z", "updatedAt": "2099-01-01T09:30:00Z",
		"createdBy": "operator@example.com", "comment": largeComment,
	})
	if err != nil {
		t.Fatalf("json.Marshal() returned error: %v", err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(response)
	}))
	defer upstream.Close()

	member := writableMember{URI: upstream.URL, RequestTimeout: 5 * time.Second}
	previousLimit := config.Config.SilenceEditor.MaxPayloadBytes
	t.Cleanup(func() { config.Config.SilenceEditor.MaxPayloadBytes = previousLimit })

	config.Config.SilenceEditor.MaxPayloadBytes = 0
	silence, err := fetchSilenceEditorSilence(context.Background(), member, "0.33.1", "silence-large")
	if err != nil {
		t.Fatalf("unlimited fetch returned error: %v", err)
	}
	if silence.Comment != largeComment {
		t.Fatalf("large silence comment length=%d, want %d", len(silence.Comment), len(largeComment))
	}

	config.Config.SilenceEditor.MaxPayloadBytes = 1024
	_, err = fetchSilenceEditorSilence(context.Background(), member, "0.33.1", "silence-large")
	if err == nil || !strings.Contains(err.Error(), "configured 1024-byte limit") {
		t.Fatalf("limited fetch error=%v, want configured limit error", err)
	}
}

func TestSilenceEditorSubmitBlockedByACL(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})

	am, err := alertmanager.NewAlertmanager(
		"prod-eu",
		"am-submit-blocked",
		"http://127.0.0.1:1",
		alertmanager.WithProxy(true),
		alertmanager.WithRequestTimeout(1*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create alertmanager: %v", err)
	}
	if err := alertmanager.RegisterAlertmanager(am); err != nil {
		t.Fatalf("failed to register alertmanager: %v", err)
	}

	silenceACLs = []*silenceACL{{
		Action: aclActionBlock,
		Reason: "blocked in test",
		Scope: silenceACLScope{
			Alertmanagers: []string{"am-submit-blocked"},
		},
	}}
	defer func() { silenceACLs = nil }()

	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"target":    "prod-eu",
		"operation": "create",
		"payload": map[string]any{
			"matchers": []map[string]any{{
				"name":    " alertname ",
				"value":   "Host_Down",
				"isRegex": false,
				"isEqual": true,
			}},
			"startsAt":  "2026-01-01T10:00:00Z",
			"endsAt":    "2026-01-01T11:00:00Z",
			"createdBy": "operator@example.com",
			"comment":   "manual silence",
		},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.Code, resp.Body.String())
	}
	logged := logOutput.String()
	for _, expected := range []string{`"level":"WARN"`, `"component":"silence_editor"`, `"event":"submission_acl_rejected"`, `"target":"prod-eu"`, `"member":"am-submit-blocked"`} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("missing ACL log field %s in %s", expected, logged)
		}
	}
	for _, forbidden := range []string{"Host_Down", "manual silence", "127.0.0.1:1"} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("sensitive ACL data %q leaked into log: %s", forbidden, logged)
		}
	}
}

func TestSilenceEditorSubmitUsesACLDefaultAllow(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})

	var submissions int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		submissions++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"silenceID":"silence-default-allow"}`))
	}))
	defer upstream.Close()

	am, err := alertmanager.NewAlertmanager(
		"acl-default-allow",
		"am-acl-default-allow",
		upstream.URL,
		alertmanager.WithProxy(true),
		alertmanager.WithRequestTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create alertmanager: %v", err)
	}
	if err := alertmanager.RegisterAlertmanager(am); err != nil {
		t.Fatalf("failed to register alertmanager: %v", err)
	}

	silenceACLs = []*silenceACL{{
		Action: aclActionRequireMatcher,
		Reason: "alertname is required",
		Matchers: aclMatchers{Required: []silenceMatcher{{
			Name:  "alertname",
			Value: "Host_Down",
		}}},
	}}
	defer func() { silenceACLs = nil }()

	r := testRouter()
	setupRouter(r, nil)
	raw, _ := json.Marshal(map[string]any{
		"target":    "acl-default-allow",
		"operation": "create",
		"payload": map[string]any{
			"matchers": []map[string]any{{
				"name": "alertname", "value": "Host_Down", "isRegex": false, "isEqual": true,
			}},
			"startsAt":  "2026-01-01T10:00:00Z",
			"endsAt":    "2026-01-01T11:00:00Z",
			"createdBy": "operator@example.com",
			"comment":   "manual silence",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected default-allow ACL to return 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if submissions != 1 {
		t.Fatalf("expected one upstream submission, got %d", submissions)
	}
}

func TestSilenceEditorSubmitReturnsOutcomeUnknownOnTransportError(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	silenceACLs = nil

	am, err := alertmanager.NewAlertmanager(
		"prod-unknown",
		"am-submit-unknown",
		"http://127.0.0.1:1",
		alertmanager.WithProxy(true),
		alertmanager.WithRequestTimeout(1*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create alertmanager: %v", err)
	}
	if err := alertmanager.RegisterAlertmanager(am); err != nil {
		t.Fatalf("failed to register alertmanager: %v", err)
	}

	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"target":    "prod-unknown",
		"operation": "create",
		"payload": map[string]any{
			"matchers": []map[string]any{{
				"name":    "alertname",
				"value":   "Host_Down",
				"isRegex": false,
				"isEqual": true,
			}},
			"startsAt":  "2026-01-01T10:00:00Z",
			"endsAt":    "2026-01-01T11:00:00Z",
			"createdBy": "operator@example.com",
			"comment":   "manual silence",
		},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d: %s", resp.Code, resp.Body.String())
	}

	var out struct {
		Error          string `json:"error"`
		OutcomeUnknown bool   `json:"outcomeUnknown"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if !out.OutcomeUnknown {
		t.Fatal("expected outcomeUnknown=true for transport failure")
	}
	logged := logOutput.String()
	for _, expected := range []string{`"level":"ERROR"`, `"component":"silence_editor"`, `"event":"upstream_write_outcome_unknown"`, `"operation":"create"`, `"target":"prod-unknown"`, `"member":"am-submit-unknown"`, `"failure_stage":"request"`} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("missing log field %s in %s", expected, logged)
		}
	}
	for _, forbidden := range []string{"Host_Down", "manual silence", "127.0.0.1:1", "revisionToken", `"matchers"`} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("sensitive submission data %q leaked into log: %s", forbidden, logged)
		}
	}
}

func TestCollectSilenceEditorPreviewCandidatesFailsClosedOnStoreError(t *testing.T) {
	prev := lifecycleStore
	lifecycleStore = nil
	t.Cleanup(func() {
		lifecycleStore = prev
	})

	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: t.TempDir() + "/lifecycle.sqlite"})
	if err != nil {
		t.Fatalf("failed to open lifecycle store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lifecycleStore = store

	if err := store.Close(); err != nil {
		t.Fatalf("failed to close lifecycle store: %v", err)
	}

	_, _, collectErr := collectSilenceEditorPreviewCandidates("default")
	if collectErr == nil {
		t.Fatal("expected lifecycle store error, got nil")
	}
}

func TestSilenceEditorPreviewEndpointFailsClosedOnStoreError(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	mockAlerts(mock.ListAllMocks()[0])

	prev := lifecycleStore
	lifecycleStore = nil
	t.Cleanup(func() {
		lifecycleStore = prev
	})

	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: t.TempDir() + "/lifecycle.sqlite"})
	if err != nil {
		t.Fatalf("failed to open lifecycle store: %v", err)
	}
	lifecycleStore = store
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close lifecycle store: %v", err)
	}

	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"target": "default",
		"matchers": []map[string]any{{
			"name":    "alertname",
			"value":   "Host_Down",
			"isRegex": false,
			"isEqual": true,
		}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/preview", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on lifecycle store failure, got %d: %s", resp.Code, resp.Body.String())
	}
	var out struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if out.Error == "" {
		t.Fatal("expected non-empty error message")
	}
}

func TestSilenceEditorSubmitDoesNotRetryOtherMembers(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	silenceACLs = nil

	firstCalls := 0
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/silences" {
			firstCalls++
			http.Error(w, "upstream failed", http.StatusBadGateway)
			return
		}
		http.NotFound(w, r)
	}))
	defer first.Close()

	secondCalls := 0
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/silences" {
			secondCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"silenceID":"should-not-be-used"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer second.Close()

	amA, err := alertmanager.NewAlertmanager(
		"prod-no-retry",
		"am-a",
		first.URL,
		alertmanager.WithProxy(true),
		alertmanager.WithRequestTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create alertmanager am-a: %v", err)
	}
	if err = alertmanager.RegisterAlertmanager(amA); err != nil {
		t.Fatalf("failed to register alertmanager am-a: %v", err)
	}

	amB, err := alertmanager.NewAlertmanager(
		"prod-no-retry",
		"am-b",
		second.URL,
		alertmanager.WithProxy(true),
		alertmanager.WithRequestTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create alertmanager am-b: %v", err)
	}
	if err := alertmanager.RegisterAlertmanager(amB); err != nil {
		t.Fatalf("failed to register alertmanager am-b: %v", err)
	}

	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"target":    "prod-no-retry",
		"operation": "create",
		"payload": map[string]any{
			"matchers": []map[string]any{{
				"name":    "alertname",
				"value":   "Host_Down",
				"isRegex": false,
				"isEqual": true,
			}},
			"startsAt":  "2026-01-01T10:00:00Z",
			"endsAt":    "2026-01-01T11:00:00Z",
			"createdBy": "operator@example.com",
			"comment":   "manual silence",
		},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d: %s", resp.Code, resp.Body.String())
	}
	if firstCalls != 1 {
		t.Fatalf("expected first member to receive one request, got %d", firstCalls)
	}
	if secondCalls != 0 {
		t.Fatalf("expected no retry to second member, got %d", secondCalls)
	}
}

func TestSilenceEditorSubmitRejectsInvalidMatchersBeforeMutation(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	silenceACLs = nil

	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/silences" {
			upstreamCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"silenceID":"unexpected"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer upstream.Close()

	am, err := alertmanager.NewAlertmanager(
		"prod-submit-validate",
		"am-submit-validate",
		upstream.URL,
		alertmanager.WithProxy(true),
		alertmanager.WithRequestTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create alertmanager: %v", err)
	}
	if err := alertmanager.RegisterAlertmanager(am); err != nil {
		t.Fatalf("failed to register alertmanager: %v", err)
	}

	r := testRouter()
	setupRouter(r, nil)

	cases := []struct {
		name     string
		matcher  map[string]any
		expected string
	}{
		{
			name: "invalid regex",
			matcher: map[string]any{
				"name":    "service",
				"value":   "[",
				"isRegex": true,
				"isEqual": true,
			},
			expected: "invalid regex",
		},
		{
			name: "unsafe matcher set",
			matcher: map[string]any{
				"name":    "service",
				"value":   ".*",
				"isRegex": true,
				"isEqual": true,
			},
			expected: "must not match the empty string",
		},
		{
			name: "invalid matcher name with dash",
			matcher: map[string]any{
				"name":    "service-name",
				"value":   "api",
				"isRegex": false,
				"isEqual": true,
			},
			expected: "invalid matcher name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{
				"target":    "prod-submit-validate",
				"operation": "create",
				"payload": map[string]any{
					"matchers":  []map[string]any{tc.matcher},
					"startsAt":  "2026-01-01T10:00:00Z",
					"endsAt":    "2026-01-01T11:00:00Z",
					"createdBy": "operator@example.com",
					"comment":   "manual silence",
				},
			}
			raw, _ := json.Marshal(body)
			req := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			r.ServeHTTP(resp, req)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", resp.Code, resp.Body.String())
			}
			if !strings.Contains(resp.Body.String(), tc.expected) {
				t.Fatalf("expected error containing %q, got: %s", tc.expected, resp.Body.String())
			}
		})
	}

	if upstreamCalls != 0 {
		t.Fatalf("expected zero upstream mutation calls, got %d", upstreamCalls)
	}
}

func TestSilenceEditorPreviewProjectsLiveStatePerTarget(t *testing.T) {
	alert := models.Alert{
		Labels: promlabels.FromMap(map[string]string{
			"alertname": "Host_Down",
			"service":   "api",
		}),
		LabelsFP: "lfp-target-projection",
		StartsAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		Alertmanager: []models.AlertmanagerInstance{
			{
				Name:     "am-a",
				Cluster:  "prod-a",
				State:    models.AlertStateSuppressed,
				StartsAt: time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			},
			{
				Name:     "am-b",
				Cluster:  "prod-b",
				State:    models.AlertStateActive,
				StartsAt: time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC),
			},
		},
		State:       models.AlertStateActive,
		RawState:    models.AlertStateActive,
		HasRawState: true,
		IsLive:      true,
		HasIsLive:   true,
	}

	stateA, rawA, liveA, suppressedA, resolvedA, startsA, okA := projectLiveAlertForTarget(alert, "prod-a")
	if !okA {
		t.Fatal("expected projection for prod-a")
	}
	if stateA != models.AlertStateSuppressed || rawA != models.AlertStateSuppressed || !liveA || !suppressedA || resolvedA {
		t.Fatalf("unexpected prod-a projection: state=%s raw=%s live=%v suppressed=%v resolved=%v", stateA, rawA, liveA, suppressedA, resolvedA)
	}
	if startsA.UTC().Format(time.RFC3339Nano) != "2026-01-01T10:00:00Z" {
		t.Fatalf("unexpected prod-a startsAt: %s", startsA.UTC().Format(time.RFC3339Nano))
	}

	stateB, rawB, _, suppressedB, _, startsB, okB := projectLiveAlertForTarget(alert, "prod-b")
	if !okB {
		t.Fatal("expected projection for prod-b")
	}
	if stateB != models.AlertStateActive || rawB != models.AlertStateActive || suppressedB {
		t.Fatalf("unexpected prod-b projection: state=%s raw=%s suppressed=%v", stateB, rawB, suppressedB)
	}
	if startsB.UTC().Format(time.RFC3339Nano) != "2026-01-01T09:00:00Z" {
		t.Fatalf("unexpected prod-b startsAt: %s", startsB.UTC().Format(time.RFC3339Nano))
	}
}

func TestSilenceEditorPreviewMergesSameFingerprintAcrossGroups(t *testing.T) {
	suppressed := silenceEditorPreviewCandidate{
		AlertID: "same-fingerprint", State: models.AlertStateSuppressed,
		RawState: models.AlertStateSuppressed, IsLive: true, IsSuppressed: true,
		StartsAt: "2026-01-01T10:00:00Z",
	}
	active := silenceEditorPreviewCandidate{
		AlertID: "same-fingerprint", State: models.AlertStateActive,
		RawState: models.AlertStateActive, IsLive: true,
		StartsAt: "2026-01-01T09:00:00Z",
	}

	merged := mergeSilenceEditorLivePreviewCandidates(suppressed, active)
	if merged.State != models.AlertStateActive || merged.RawState != models.AlertStateActive || merged.IsSuppressed {
		t.Fatalf("expected active-first merged projection, got=%+v", merged)
	}
	if merged.StartsAt != "2026-01-01T09:00:00Z" {
		t.Fatalf("expected earliest startsAt, got=%s", merged.StartsAt)
	}

	reversed := mergeSilenceEditorLivePreviewCandidates(active, suppressed)
	if reversed.State != merged.State || reversed.StartsAt != merged.StartsAt {
		t.Fatalf("merge depends on group iteration order: forward=%+v reverse=%+v", merged, reversed)
	}
	unprocessed := suppressed
	unprocessed.State = models.AlertStateUnprocessed
	unprocessed.RawState = models.AlertStateUnprocessed
	unprocessed.IsSuppressed = false
	if got := mergeSilenceEditorLivePreviewCandidates(unprocessed, suppressed); got.State != models.AlertStateSuppressed {
		t.Fatalf("expected suppressed to win over unprocessed, got=%+v", got)
	}
}

func TestSilenceEditorSubmitTargetUsesClusterOnly(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	silenceACLs = nil

	wrongCalls := 0
	wrong := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/silences" {
			wrongCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"silenceID":"wrong-cluster"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer wrong.Close()

	rightCalls := 0
	right := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/silences" {
			rightCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"silenceID":"right-cluster"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer right.Close()

	physicalCollision, err := alertmanager.NewAlertmanager(
		"another-cluster",
		"target-cluster",
		wrong.URL,
		alertmanager.WithProxy(true),
		alertmanager.WithRequestTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create physical-collision alertmanager: %v", err)
	}
	if err = alertmanager.RegisterAlertmanager(physicalCollision); err != nil {
		t.Fatalf("failed to register physical-collision alertmanager: %v", err)
	}

	logicalTarget, err := alertmanager.NewAlertmanager(
		"target-cluster",
		"am-target",
		right.URL,
		alertmanager.WithProxy(true),
		alertmanager.WithRequestTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create logical-target alertmanager: %v", err)
	}
	if err := alertmanager.RegisterAlertmanager(logicalTarget); err != nil {
		t.Fatalf("failed to register logical-target alertmanager: %v", err)
	}

	r := testRouter()
	setupRouter(r, nil)

	body := map[string]any{
		"target":    "target-cluster",
		"operation": "create",
		"payload": map[string]any{
			"matchers": []map[string]any{{
				"name":    "alertname",
				"value":   "Host_Down",
				"isRegex": false,
				"isEqual": true,
			}},
			"startsAt":  "2026-01-01T10:00:00Z",
			"endsAt":    "2026-01-01T11:00:00Z",
			"createdBy": "operator@example.com",
			"comment":   "manual silence",
		},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if wrongCalls != 0 {
		t.Fatalf("expected zero calls to physical-name collision member, got %d", wrongCalls)
	}
	if rightCalls != 1 {
		t.Fatalf("expected one call to logical target cluster member, got %d", rightCalls)
	}
}

func TestSilenceEditorSubmitOutcomeUnknownForMalformedSuccessResponses(t *testing.T) {
	testCases := []struct {
		name         string
		handler      http.HandlerFunc
		expectStatus int
	}{
		{
			name: "malformed json",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v2/silences" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"silenceID":`))
					return
				}
				http.NotFound(w, r)
			},
			expectStatus: http.StatusBadGateway,
		},
		{
			name: "missing silence id",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v2/silences" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"silenceID":""}`))
					return
				}
				http.NotFound(w, r)
			},
			expectStatus: http.StatusBadGateway,
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockConfig(t.Setenv)
			alertmanager.UnregisterAll()
			silenceACLs = nil

			srv := httptest.NewServer(tc.handler)
			defer srv.Close()

			am, err := alertmanager.NewAlertmanager(
				"prod-malformed",
				fmt.Sprintf("am-malformed-%d", i),
				srv.URL,
				alertmanager.WithProxy(true),
				alertmanager.WithRequestTimeout(5*time.Second),
			)
			if err != nil {
				t.Fatalf("failed to create alertmanager: %v", err)
			}
			if err := alertmanager.RegisterAlertmanager(am); err != nil {
				t.Fatalf("failed to register alertmanager: %v", err)
			}

			r := testRouter()
			setupRouter(r, nil)

			body := map[string]any{
				"target":    "prod-malformed",
				"operation": "create",
				"payload": map[string]any{
					"matchers": []map[string]any{{
						"name":    "alertname",
						"value":   "Host_Down",
						"isRegex": false,
						"isEqual": true,
					}},
					"startsAt":  "2026-01-01T10:00:00Z",
					"endsAt":    "2026-01-01T11:00:00Z",
					"createdBy": "operator@example.com",
					"comment":   "manual silence",
				},
			}
			raw, _ := json.Marshal(body)
			req := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			r.ServeHTTP(resp, req)

			if resp.Code != tc.expectStatus {
				t.Fatalf("expected %d, got %d: %s", tc.expectStatus, resp.Code, resp.Body.String())
			}
			var out struct {
				Error          string `json:"error"`
				OutcomeUnknown bool   `json:"outcomeUnknown"`
			}
			if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
				t.Fatalf("failed to parse response: %v", err)
			}
			if !out.OutcomeUnknown {
				t.Fatalf("expected outcomeUnknown=true, got response: %s", resp.Body.String())
			}
		})
	}
}

func TestSilenceEditorSubmitReturnsDirectDescriptorForNonProxiedTarget(t *testing.T) {
	alertmanager.UnregisterAll()
	upstreamSetup = false
	mockConfig(t.Setenv)
	t.Cleanup(func() {
		alertmanager.UnregisterAll()
		upstreamSetup = false
	})
	silenceACLs = nil

	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/silences" {
			upstreamCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"silenceID":"should-not-be-created-by-backend"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer upstream.Close()

	am, err := alertmanager.NewAlertmanager(
		"prod-direct",
		"am-direct",
		upstream.URL,
		alertmanager.WithProxy(false),
		alertmanager.WithExternalURI("http://external-am.example.com/am"),
		alertmanager.WithRequestTimeout(5*time.Second),
		alertmanager.WithHTTPHeaders(map[string]string{"X-Test": "value"}),
	)
	if err != nil {
		t.Fatalf("failed to create alertmanager: %v", err)
	}
	if err := alertmanager.RegisterAlertmanager(am); err != nil {
		t.Fatalf("failed to register alertmanager: %v", err)
	}

	r := testRouter()
	setupRouter(r, nil)

	largeComment := strings.Repeat("large-direct-comment-", 4096)
	body := map[string]any{
		"target":    "prod-direct",
		"operation": "create",
		"payload": map[string]any{
			"matchers": []map[string]any{{
				"name":    " alertname ",
				"value":   "Host_Down",
				"isRegex": false,
				"isEqual": true,
			}},
			"startsAt":  "2026-01-01T10:00:00Z",
			"endsAt":    "2026-01-01T11:00:00Z",
			"createdBy": "operator@example.com",
			"comment":   largeComment,
		},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/silence-editor/submit", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if upstreamCalls != 0 {
		t.Fatalf("expected backend not to mutate upstream in direct mode, got calls=%d", upstreamCalls)
	}

	var out struct {
		Status        string `json:"status"`
		Target        string `json:"target"`
		Mode          string `json:"mode"`
		CorrelationID string `json:"correlationID"`
		Direct        struct {
			Endpoint        string            `json:"endpoint"`
			Headers         map[string]string `json:"headers"`
			CORSCredentials string            `json:"corsCredentials"`
			Member          string            `json:"member"`
			Payload         struct {
				Comment  string `json:"comment"`
				Matchers []struct {
					Name string `json:"name"`
				} `json:"matchers"`
			} `json:"payload"`
		} `json:"direct"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if out.Status != "direct" || out.Mode != "direct" {
		t.Fatalf("expected direct mode, got status=%q mode=%q", out.Status, out.Mode)
	}
	if strings.TrimSpace(out.CorrelationID) == "" {
		t.Fatal("direct descriptor is missing correlationID")
	}
	if out.Direct.Endpoint != "http://external-am.example.com/am/api/v2/silences" {
		t.Fatalf("unexpected direct endpoint: %q", out.Direct.Endpoint)
	}
	if out.Direct.Member != "am-direct" {
		t.Fatalf("unexpected direct member: %q", out.Direct.Member)
	}
	if out.Direct.Headers["X-Test"] != "value" {
		t.Fatalf("expected forwarded header in direct descriptor")
	}
	if len(out.Direct.Payload.Matchers) != 1 || out.Direct.Payload.Matchers[0].Name != "alertname" {
		t.Fatalf("expected normalized matcher in direct payload, got=%+v", out.Direct.Payload.Matchers)
	}
	if out.Direct.Payload.Comment != largeComment {
		t.Fatalf("direct payload comment length=%d, want %d", len(out.Direct.Payload.Comment), len(largeComment))
	}
	logged := logOutput.String()
	for _, expected := range []string{
		`"component":"silence_editor"`,
		`"event":"direct_descriptor_issued"`,
		`"correlation_id":"` + out.CorrelationID + `"`,
		`"operation":"create"`,
		`"target":"prod-direct"`,
		`"member":"am-direct"`,
		`"server_result_observable":false`,
	} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("missing log field %s in %s", expected, logged)
		}
	}
	for _, forbidden := range []string{"large-direct-comment-", "Host_Down", "X-Test", "value", "external-am.example.com"} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("sensitive direct descriptor data %q leaked into log: %s", forbidden, logged)
		}
	}
}

func TestSortWritableMembersPrefersHealthyThenName(t *testing.T) {
	members := []writableMember{
		{Name: "z-unhealthy", Healthy: false},
		{Name: "b-healthy", Healthy: true},
		{Name: "a-healthy", Healthy: true},
		{Name: "a-unhealthy", Healthy: false},
	}

	sortWritableMembers(members)

	got := []string{members[0].Name, members[1].Name, members[2].Name, members[3].Name}
	want := []string{"a-healthy", "b-healthy", "a-unhealthy", "z-unhealthy"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected order at %d: got=%q want=%q", i, got[i], want[i])
		}
	}
}

func TestCurrentBelongsToTargetDoesNotOverrideRelevantSources(t *testing.T) {
	current := alertlifecycle.AlertCurrent{
		RelevantSources:   `[{"name":"am-moved","cluster":"old-cluster"}]`,
		RelevantUpstreams: `["am-moved"]`,
	}

	if currentBelongsToTarget(current, "new-cluster", map[string]string{"am-moved": "new-cluster"}) {
		t.Fatal("expected valid relevant_sources_json to remain authoritative after topology change")
	}
}
