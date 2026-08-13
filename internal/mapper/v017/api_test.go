package v017

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"

	"github.com/prymitive/karma/internal/models"
)

func TestDateTimeUnmarshalJSONFromValid(t *testing.T) {
	// verifies that a valid RFC3339 timestamp is parsed correctly
	var dt dateTime
	err := json.Unmarshal([]byte(`"2025-03-10T12:00:00.000Z"`), &dt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)
	if !time.Time(dt).Equal(expected) {
		t.Errorf("got %v, want %v", time.Time(dt), expected)
	}
}

func TestDateTimeUnmarshalJSONFromInvalidJSON(t *testing.T) {
	// verifies that broken JSON input returns a read error
	var dt dateTime
	err := json.Unmarshal([]byte(`{not json`), &dt)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestDateTimeUnmarshalJSONFromInvalidTimestamp(t *testing.T) {
	// verifies that a string that is not a valid RFC3339 timestamp returns a parse error
	var dt dateTime
	err := json.Unmarshal([]byte(`"not-a-date"`), &dt)
	if err == nil {
		t.Fatal("expected error for invalid timestamp, got nil")
	}
}

func TestDateTimeUnmarshalJSONFromNonString(t *testing.T) {
	// verifies that a non-string JSON value (number) returns an unquote error
	var dt dateTime
	err := json.Unmarshal([]byte(`12345`), &dt)
	if err == nil {
		t.Fatal("expected error for non-string JSON value, got nil")
	}
}

func TestDateTimeMarshalRoundTrip(t *testing.T) {
	// verifies that marshal then unmarshal produces the same time
	original := dateTime(time.Date(2025, 6, 15, 8, 30, 0, 0, time.UTC))
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	var decoded dateTime
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if !time.Time(original).Equal(time.Time(decoded)) {
		t.Errorf("round-trip mismatch: got %v, want %v", time.Time(decoded), time.Time(original))
	}
}

func TestJsonLabelsUnmarshalValid(t *testing.T) {
	// verifies that a valid JSON object is decoded into labels
	var jl jsonLabels
	err := json.Unmarshal([]byte(`{"alertname":"TestAlert","job":"test"}`), &jl)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if jl.ls.Get("alertname") != "TestAlert" {
		t.Errorf("alertname = %q, want %q", jl.ls.Get("alertname"), "TestAlert")
	}
	if jl.ls.Get("job") != "test" {
		t.Errorf("job = %q, want %q", jl.ls.Get("job"), "test")
	}
}

func TestJsonLabelsUnmarshalInvalidJSON(t *testing.T) {
	// verifies that broken JSON returns an error from the opening token read
	var jl jsonLabels
	err := json.Unmarshal([]byte(`not json`), &jl)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestJsonLabelsUnmarshalTruncatedKey(t *testing.T) {
	// verifies that a truncated object (missing value after key) returns an error
	var jl jsonLabels
	err := json.Unmarshal([]byte(`{"key":`), &jl)
	if err == nil {
		t.Fatal("expected error for truncated key/value, got nil")
	}
}

func TestJsonAnnotationsUnmarshalValid(t *testing.T) {
	// verifies that a valid JSON object is decoded into sorted annotations
	var ja jsonAnnotations
	err := json.Unmarshal([]byte(`{"summary":"test summary","description":"test desc"}`), &ja)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ja.annos) != 2 {
		t.Fatalf("got %d annotations, want 2", len(ja.annos))
	}
	names := make(map[string]bool)
	for _, a := range ja.annos {
		names[a.Name] = true
	}
	if !names["summary"] {
		t.Error("missing annotation 'summary'")
	}
	if !names["description"] {
		t.Error("missing annotation 'description'")
	}
}

func TestJsonAnnotationsUnmarshalInvalidJSON(t *testing.T) {
	// verifies that broken JSON returns an error from the opening token read
	var ja jsonAnnotations
	err := json.Unmarshal([]byte(`not json`), &ja)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestJsonAnnotationsUnmarshalTruncatedKey(t *testing.T) {
	// verifies that a truncated object (missing value after key) returns an error
	var ja jsonAnnotations
	err := json.Unmarshal([]byte(`{"key":`), &ja)
	if err == nil {
		t.Fatal("expected error for truncated key/value, got nil")
	}
}

func TestJsonLabelsUnmarshalTruncatedAfterOpen(t *testing.T) {
	// verifies that EOF right after the opening brace returns an error from the key ReadToken
	var jl jsonLabels
	dec := jsontext.NewDecoder(bytes.NewReader([]byte(`{`)))
	err := jl.UnmarshalJSONFrom(dec)
	if err == nil {
		t.Fatal("expected error for truncated object after open brace, got nil")
	}
}

func TestJsonLabelsUnmarshalTruncatedValue(t *testing.T) {
	// verifies that a truncated value after a valid key returns an error from the value ReadToken
	var jl jsonLabels
	dec := jsontext.NewDecoder(bytes.NewReader([]byte(`{"key": `)))
	err := jl.UnmarshalJSONFrom(dec)
	if err == nil {
		t.Fatal("expected error for truncated value, got nil")
	}
}

func TestJsonLabelsUnmarshalTruncatedClose(t *testing.T) {
	// verifies that a missing closing brace after valid key-value pairs returns an error
	var jl jsonLabels
	dec := jsontext.NewDecoder(bytes.NewReader([]byte(`{"key": "val"`)))
	err := jl.UnmarshalJSONFrom(dec)
	if err == nil {
		t.Fatal("expected error for missing closing brace, got nil")
	}
}

func TestJsonAnnotationsUnmarshalTruncatedAfterOpen(t *testing.T) {
	// verifies that EOF right after the opening brace returns an error from the key ReadToken
	var ja jsonAnnotations
	dec := jsontext.NewDecoder(bytes.NewReader([]byte(`{`)))
	err := ja.UnmarshalJSONFrom(dec)
	if err == nil {
		t.Fatal("expected error for truncated object after open brace, got nil")
	}
}

func TestJsonAnnotationsUnmarshalTruncatedValue(t *testing.T) {
	// verifies that a truncated value after a valid key returns an error from the value ReadToken
	var ja jsonAnnotations
	dec := jsontext.NewDecoder(bytes.NewReader([]byte(`{"key": `)))
	err := ja.UnmarshalJSONFrom(dec)
	if err == nil {
		t.Fatal("expected error for truncated value, got nil")
	}
}

func TestJsonAnnotationsUnmarshalTruncatedClose(t *testing.T) {
	// verifies that a missing closing brace after valid key-value pairs returns an error
	var ja jsonAnnotations
	dec := jsontext.NewDecoder(bytes.NewReader([]byte(`{"key": "val"`)))
	err := ja.UnmarshalJSONFrom(dec)
	if err == nil {
		t.Fatal("expected error for missing closing brace, got nil")
	}
}

func TestApiGetNon200(t *testing.T) {
	// verifies that a non-200 HTTP response returns an error with the status code
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	_, err := apiGet(context.Background(), srv.Client(), u, "alerts/groups")
	if err == nil {
		t.Fatal("expected error for non-200 status, got nil")
	}
}

func TestApiGetRequestError(t *testing.T) {
	// verifies that a connection error (server closed) returns an error
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	u, _ := url.Parse(srv.URL)
	srv.Close()

	_, err := apiGet(context.Background(), srv.Client(), u, "alerts/groups")
	if err == nil {
		t.Fatal("expected error for closed server, got nil")
	}
}

func TestApiGetInvalidURL(t *testing.T) {
	// verifies that an invalid URL that causes NewRequestWithContext to fail returns an error
	u := &url.URL{Scheme: "http", Host: "invalid host with spaces"}
	_, err := apiGet(context.Background(), http.DefaultClient, u, "alerts/groups")
	if err == nil {
		t.Fatal("expected error for invalid URL, got nil")
	}
}

func TestGroupsInvalidJSON(t *testing.T) {
	// verifies that invalid JSON in the groups response returns a decode error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	_, err := groups(srv.Client(), u, 5*time.Second)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestGroupsValid(t *testing.T) {
	// verifies that a valid groups JSON response is decoded correctly
	payload := `[{
		"labels": {"alertname": "TestAlert"},
		"receiver": {"name": "default"},
		"alerts": [{
			"startsAt": "2025-03-10T12:00:00.000Z",
			"annotations": {"summary": "test"},
			"labels": {"alertname": "TestAlert", "job": "test"},
			"fingerprint": "abc123",
			"generatorURL": "http://prom:9090",
			"status": {
				"state": "active",
				"inhibitedBy": [],
				"silencedBy": []
			}
		}]
	}]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	result, err := groups(srv.Client(), u, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("got %d groups, want 1", len(result))
	}
	if result[0].Receiver != "default" {
		t.Errorf("receiver = %q, want %q", result[0].Receiver, "default")
	}
	if len(result[0].Alerts) != 1 {
		t.Fatalf("got %d alerts, want 1", len(result[0].Alerts))
	}
	if result[0].Alerts[0].Fingerprint != "abc123" {
		t.Errorf("fingerprint = %q, want %q", result[0].Alerts[0].Fingerprint, "abc123")
	}
}

func TestSilencesInvalidJSON(t *testing.T) {
	// verifies that invalid JSON in the silences response returns a decode error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	_, _, err := silences(srv.Client(), u, 5*time.Second)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestSilencesValid(t *testing.T) {
	// verifies that a valid silences JSON response is decoded correctly,
	// including matcher with explicit isEqual=false
	payload := `[{
		"id": "silence-1",
		"status": {"state": "active"},
		"startsAt": "2025-03-10T12:00:00.000Z",
		"endsAt": "2025-03-11T12:00:00.000Z",
		"updatedAt": "2025-03-10T13:00:00.000Z",
		"createdBy": "user@example.com",
		"comment": "test silence",
		"matchers": [
			{"name": "alertname", "value": "TestAlert", "isRegex": false},
			{"name": "env", "value": "prod", "isRegex": true, "isEqual": false}
		]
	}]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	result, warnings, err := silences(srv.Client(), u, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("got %d silences, want 1", len(result))
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings=%+v, want none", warnings)
	}
	if result[0].ID != "silence-1" {
		t.Errorf("ID = %q, want %q", result[0].ID, "silence-1")
	}
	if result[0].CreatedBy != "user@example.com" {
		t.Errorf("CreatedBy = %q, want %q", result[0].CreatedBy, "user@example.com")
	}
	if len(result[0].Matchers) != 2 {
		t.Fatalf("got %d matchers, want 2", len(result[0].Matchers))
	}
}

func TestSilencesSkipsMissingAndUnsupportedStatus(t *testing.T) {
	payload := `[
  {
    "id":"valid",
    "status":{"state":"active"},
    "startsAt":"2025-03-10T12:00:00Z",
    "endsAt":"2025-03-11T12:00:00Z",
	"updatedAt":"2025-03-10T13:00:00Z",
    "createdBy":"user@example.com",
    "comment":"valid",
    "matchers":[{"name":"service","value":"api","isRegex":false,"isEqual":true}]
  },
  {
    "id":"missing-status",
    "startsAt":"2025-03-10T12:00:00Z",
    "endsAt":"2025-03-11T12:00:00Z",
	"updatedAt":"2025-03-10T13:00:00Z",
    "createdBy":"user@example.com",
    "comment":"invalid",
    "matchers":[]
  },
  {
    "id":"unknown-status",
    "status":{"state":"future-state"},
    "startsAt":"2025-03-10T12:00:00Z",
    "endsAt":"2025-03-11T12:00:00Z",
	"updatedAt":"2025-03-10T13:00:00Z",
    "createdBy":"user@example.com",
    "comment":"invalid",
    "matchers":[]
  }
]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	result, warnings, err := silences(srv.Client(), u, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].ID != "valid" {
		t.Fatalf("silences=%+v, want only valid", result)
	}
	if len(warnings) != 2 {
		t.Fatalf("warnings=%+v, want two", warnings)
	}
	if warnings[0].SilenceID != "missing-status" || warnings[0].Status != "missing" {
		t.Fatalf("missing warning=%+v", warnings[0])
	}
	if warnings[1].SilenceID != "unknown-status" || warnings[1].Status != "future-state" {
		t.Fatalf("unknown warning=%+v", warnings[1])
	}
}

func TestSilencesSkipsInvalidEntriesIndependently(t *testing.T) {
	valid := func(id string) string {
		return fmt.Sprintf(`{
  "id":%q,
  "status":{"state":"active"},
  "startsAt":"2025-03-10T12:00:00Z",
  "endsAt":"2025-03-11T12:00:00Z",
  "updatedAt":"2025-03-10T13:00:00Z",
  "createdBy":"user@example.com",
  "comment":"maintenance",
  "matchers":[{"name":"service","value":"api","isRegex":false,"isEqual":true}]
}`, id)
	}
	tests := []struct {
		name       string
		entry      string
		field      string
		reasonCode string
	}{
		{name: "non object", entry: `42`, field: "$", reasonCode: "wrong_type"},
		{name: "wrong field type", entry: `{"id":"bad","status":{"state":"active"},"startsAt":42,"endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[]}`, field: "startsAt", reasonCode: "wrong_type"},
		{name: "missing id", entry: `{"status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[]}`, field: "id", reasonCode: "missing_required_field"},
		{name: "empty id", entry: `{"id":" ","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[]}`, field: "id", reasonCode: "empty_required_field"},
		{name: "missing status", entry: `{"id":"bad","startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[]}`, field: "status.state", reasonCode: "missing_required_field"},
		{name: "unknown status", entry: `{"id":"bad","status":{"state":"future"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[]}`, field: "status.state", reasonCode: "unsupported_value"},
		{name: "invalid updated at", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"not-a-date","createdBy":"user","comment":"x","matchers":[]}`, field: "updatedAt", reasonCode: "invalid_format"},
		{name: "missing start", entry: `{"id":"bad","status":{"state":"active"},"endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[]}`, field: "startsAt", reasonCode: "missing_required_field"},
		{name: "missing end", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[]}`, field: "endsAt", reasonCode: "missing_required_field"},
		{name: "reversed interval", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-11T12:00:00Z","endsAt":"2025-03-10T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[]}`, field: "endsAt", reasonCode: "invalid_time_range"},
		{name: "missing author", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","comment":"x","matchers":[]}`, field: "createdBy", reasonCode: "missing_required_field"},
		{name: "missing comment", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","matchers":[]}`, field: "comment", reasonCode: "missing_required_field"},
		{name: "missing matchers", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x"}`, field: "matchers", reasonCode: "missing_required_field"},
		{name: "empty matchers", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[]}`, field: "matchers", reasonCode: "empty_matcher_set"},
		{name: "missing matcher name", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"value":"api","isRegex":false}]}`, field: "matchers[0].name", reasonCode: "missing_required_field"},
		{name: "invalid matcher name", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"bad.name","value":"api","isRegex":false}]}`, field: "matchers[0].name", reasonCode: "invalid_matcher_name"},
		{name: "missing matcher value", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","isRegex":false}]}`, field: "matchers[0].value", reasonCode: "missing_required_field"},
		{name: "missing regex flag", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api"}]}`, field: "matchers[0].isRegex", reasonCode: "missing_required_field"},
		{name: "invalid regex", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"[","isRegex":true}]}`, field: "matchers[0].value", reasonCode: "invalid_regex"},
		{name: "matches empty", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":".*","isRegex":true}]}`, field: "matchers", reasonCode: "matches_empty"},
		{name: "invalid annotations", entry: `{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}],"annotations":{"ticket":42}}`, field: "annotations.ticket", reasonCode: "wrong_type"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := "[" + valid("before") + "," + tc.entry + "," + valid("after") + "]"
			result, warnings, err := collectSilencesPayload(t, payload)
			if err != nil {
				t.Fatalf("mixed response failed: %v", err)
			}
			if len(result) != 2 || result[0].ID != "before" || result[1].ID != "after" {
				t.Fatalf("valid entries were not preserved: %+v", result)
			}
			if len(warnings) != 1 {
				t.Fatalf("warnings=%+v, want one", warnings)
			}
			if warnings[0].EntryIndex != 1 || warnings[0].Field != tc.field || warnings[0].ReasonCode != tc.reasonCode {
				t.Fatalf("warning=%+v, want index=1 field=%q reason=%q", warnings[0], tc.field, tc.reasonCode)
			}
		})
	}
}

func TestSilencesSkipsDuplicateValidIDs(t *testing.T) {
	payload := `[
{"id":"before","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}]},
{"id":"duplicate","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}]},
{"id":"duplicate","status":{"state":"pending"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T14:00:00Z","createdBy":"user","comment":"y","matchers":[{"name":"service","value":"web","isRegex":false}]},
{"id":"after","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}]}
]`
	result, warnings, err := collectSilencesPayload(t, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 || result[0].ID != "before" || result[1].ID != "after" {
		t.Fatalf("result=%+v, want before and after", result)
	}
	if len(warnings) != 2 || warnings[0].ReasonCode != "duplicate_id" || warnings[1].ReasonCode != "duplicate_id" {
		t.Fatalf("warnings=%+v, want both duplicate entries rejected", warnings)
	}
}

func TestSilencesAcceptsUnknownExtraFields(t *testing.T) {
	payload := `[{"id":"valid","status":{"state":"active","futureStatusField":true},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false,"futureMatcherField":42}],"futureTopLevelField":{"nested":true}}]`
	result, warnings, err := collectSilencesPayload(t, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0].ID != "valid" || len(warnings) != 0 {
		t.Fatalf("result=%+v warnings=%+v, want one valid entry", result, warnings)
	}
}

func TestSilencesAcceptsMissingOptionalFields(t *testing.T) {
	payload := `[{"id":"valid","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}]}]`
	result, warnings, err := collectSilencesPayload(t, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0].ID != "valid" || result[0].UpdatedAt != nil || len(warnings) != 0 {
		t.Fatalf("result=%+v warnings=%+v, want one valid entry with optional fields absent", result, warnings)
	}
}

func TestSilencesMalformedArrayFailsWholeRefresh(t *testing.T) {
	payload := `[{"id":"valid","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","updatedAt":"2025-03-10T13:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}]}, {"id":]`
	result, warnings, err := collectSilencesPayload(t, payload)
	if err == nil {
		t.Fatalf("result=%+v warnings=%+v, want malformed array error", result, warnings)
	}
}

func TestSilencesWarningSelectionIsDeterministic(t *testing.T) {
	payload := `[{"id":42,"status":false,"startsAt":1,"endsAt":2,"updatedAt":3,"createdBy":4,"comment":5,"matchers":6}]`
	for run := range 100 {
		_, warnings, err := collectSilencesPayload(t, payload)
		if err != nil {
			t.Fatal(err)
		}
		if len(warnings) != 1 || warnings[0].Field != "id" || warnings[0].ReasonCode != "wrong_type" {
			t.Fatalf("run %d warning=%+v, want deterministic id wrong_type", run, warnings)
		}
	}
}

func TestSilencesDuplicateMemberNamesAreIsolatedToEntry(t *testing.T) {
	payload := `[
{"id":"before","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}]},
{"id":"invalid","id":"ambiguous","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}]},
{"id":"after","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}]}
]`
	result, warnings, err := collectSilencesPayload(t, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 || result[0].ID != "before" || result[1].ID != "after" {
		t.Fatalf("result=%+v, want valid neighbors", result)
	}
	if len(warnings) != 1 || warnings[0].EntryIndex != 1 || warnings[0].ReasonCode != "duplicate_field" {
		t.Fatalf("warnings=%+v, want duplicate-field warning for entry 1", warnings)
	}
}

func TestSilencesInvalidUTF8IsIsolatedToEntry(t *testing.T) {
	before := `{"id":"before","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}]}`
	after := `{"id":"after","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","createdBy":"user","comment":"x","matchers":[{"name":"service","value":"api","isRegex":false}]}`
	payload := append([]byte("["+before+`,{"id":"bad","status":{"state":"active"},"startsAt":"2025-03-10T12:00:00Z","endsAt":"2025-03-11T12:00:00Z","createdBy":"user","comment":"`), 0xff)
	payload = append(payload, []byte(`","matchers":[{"name":"service","value":"api","isRegex":false}]},`+after+"]")...)
	result, warnings, err := collectSilencesBytes(t, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 || len(warnings) != 1 || warnings[0].EntryIndex != 1 || warnings[0].ReasonCode != "invalid_utf8" {
		t.Fatalf("result=%+v warnings=%+v, want invalid UTF-8 entry isolated", result, warnings)
	}
}

func collectSilencesPayload(t *testing.T, payload string) ([]models.Silence, []models.SilenceWarning, error) {
	return collectSilencesBytes(t, []byte(payload))
}

func collectSilencesBytes(t *testing.T, payload []byte) ([]models.Silence, []models.SilenceWarning, error) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return silences(srv.Client(), u, 5*time.Second)
}

func TestUnmarshalSilencePreservesRevisionFields(t *testing.T) {
	payload := []byte(`{
  "id":"silence-1",
  "startsAt":"2025-03-10T12:00:00.000Z",
  "endsAt":"2025-03-11T12:00:00.000Z",
  "createdAt":"2025-03-10T10:00:00.000Z",
  "updatedAt":"2025-03-10T11:00:00.000Z",
  "createdBy":"user@example.com",
  "comment":"maintenance",
  "status":{"state":"active"},
  "annotations":{"ticket":"OPS-1"},
  "matchers":[{"name":"service","value":"api","isRegex":false,"isEqual":true}]
}`)
	silence, err := unmarshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if silence.UpdatedAt == nil || silence.UpdatedAt.UTC().Format(time.RFC3339) != "2025-03-10T11:00:00Z" {
		t.Fatalf("updatedAt not preserved: %+v", silence.UpdatedAt)
	}
	if silence.StatusState != "active" {
		t.Fatalf("status=%q want active", silence.StatusState)
	}
	if len(silence.Annotations) != 1 || silence.Annotations[0].Name != "ticket" || silence.Annotations[0].Value != "OPS-1" {
		t.Fatalf("annotations not preserved: %+v", silence.Annotations)
	}
}

func TestUnmarshalSilenceStatusBoundary(t *testing.T) {
	payload := []byte(`{
  "id":"silence-1",
  "startsAt":"2025-03-10T12:00:00Z",
  "endsAt":"2025-03-11T12:00:00Z",
  "createdBy":"user@example.com",
  "comment":"maintenance",
  "matchers":[]
}`)

	if _, err := unmarshal(payload); err != nil {
		t.Fatalf("outgoing request payload without status rejected: %v", err)
	}
	if _, err := unmarshalResponse(payload); err == nil {
		t.Fatal("existing silence response without status was accepted")
	}
}

func TestGroupsApiGetError(t *testing.T) {
	// verifies that when the API server is unreachable, groups returns an error
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	u, _ := url.Parse(srv.URL)
	srv.Close()

	result, err := groups(srv.Client(), u, 5*time.Second)
	if err == nil {
		t.Fatal("expected error for closed server, got nil")
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d groups", len(result))
	}
}

func TestSilencesApiGetError(t *testing.T) {
	// verifies that when the API server is unreachable, silences returns an error
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	u, _ := url.Parse(srv.URL)
	srv.Close()

	result, _, err := silences(srv.Client(), u, 5*time.Second)
	if err == nil {
		t.Fatal("expected error for closed server, got nil")
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d silences", len(result))
	}
}
