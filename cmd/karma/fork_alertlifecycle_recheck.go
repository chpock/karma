package main

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
)

const recheckOperatorBodyLimit int64 = 4096

const recheckOperatorRequestHeader = "X-Karma-Request"

type recheckOperatorRequest struct {
	RequestID string `json:"requestId"`
	Reason    string `json:"reason"`
}

func registerAlertLifecycleRecheckRoutes(router chi.Router) {
	router.Post(getViewURL("/alerts-history/recheck/{jobID}/retry"), retryRecheckJob)
	router.Post(getViewURL("/alerts-history/recheck/{jobID}/abandon"), abandonRecheckJob)
}

func retryRecheckJob(w http.ResponseWriter, r *http.Request) {
	handleRecheckOperatorAction(w, r, "retry")
}

func abandonRecheckJob(w http.ResponseWriter, r *http.Request) {
	handleRecheckOperatorAction(w, r, "abandon")
}

func handleRecheckOperatorAction(w http.ResponseWriter, r *http.Request, action string) {
	noCache(w)
	mimeJSON(w)
	if config.Config.Authentication.Enabled && strings.TrimSpace(getUserFromContext(r)) == "" {
		writeRecheckOperatorError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if status, reason := validateRecheckOperatorHTTPBoundary(r); status != 0 {
		logRejectedRecheckOperatorActionReason(r, action, 0, status, reason, errors.New(reason))
		writeRecheckOperatorError(w, status, reason)
		return
	}
	if lifecycleStore == nil {
		logRejectedRecheckOperatorActionReason(r, action, 0, http.StatusServiceUnavailable, "storage_unavailable", errors.New("alert lifecycle storage is disabled"))
		writeRecheckOperatorError(w, http.StatusServiceUnavailable, "alert lifecycle storage is disabled")
		return
	}
	jobID, err := strconv.ParseInt(strings.TrimSpace(chi.URLParam(r, "jobID")), 10, 64)
	if err != nil || jobID <= 0 {
		logRejectedRecheckOperatorActionReason(r, action, 0, http.StatusBadRequest, "invalid_request", errors.New("invalid recheck job id"))
		writeRecheckOperatorError(w, http.StatusBadRequest, "invalid recheck job id")
		return
	}
	request, err := parseRecheckOperatorRequest(w, r)
	if err != nil {
		logRejectedRecheckOperatorActionReason(r, action, jobID, http.StatusBadRequest, "invalid_request", err)
		writeRecheckOperatorError(w, http.StatusBadRequest, recheckOperatorResponseMessage(http.StatusBadRequest, err))
		return
	}
	actor := resolveActorFromRequest(r)
	var result alertlifecycle.RecheckOperatorActionResult
	if action == "retry" {
		result, err = lifecycleStore.RetryRecheckJobContext(r.Context(), jobID, request.RequestID, request.Reason, actor, time.Now().UTC())
	} else {
		result, err = lifecycleStore.AbandonRecheckJobContext(r.Context(), jobID, request.RequestID, request.Reason, actor, time.Now().UTC())
	}
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, alertlifecycle.ErrInvalidRecheckRequest):
			status = http.StatusBadRequest
		case errors.Is(err, alertlifecycle.ErrRecheckJobNotFound):
			status = http.StatusNotFound
		case errors.Is(err, alertlifecycle.ErrRecheckJobStateConflict), errors.Is(err, alertlifecycle.ErrRecheckOperatorConflict):
			status = http.StatusConflict
		}
		logRejectedRecheckOperatorAction(r, action, jobID, status, err)
		writeRecheckOperatorError(w, status, recheckOperatorResponseMessage(status, err))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(marshalJSONValue(struct {
		Status string `json:"status"`
		alertlifecycle.RecheckOperatorActionResult
	}{Status: "success", RecheckOperatorActionResult: result}))
}

func parseRecheckOperatorRequest(w http.ResponseWriter, r *http.Request) (recheckOperatorRequest, error) {
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, recheckOperatorBodyLimit)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return recheckOperatorRequest{}, errors.New("invalid request body")
	}
	if !utf8.Valid(body) {
		return recheckOperatorRequest{}, errors.New("request body must be valid UTF-8")
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(bytes.TrimSpace(body), &fields); err != nil {
		return recheckOperatorRequest{}, errors.New("request body must be one strict JSON object")
	}
	if fields == nil {
		return recheckOperatorRequest{}, errors.New("request body must be one JSON object")
	}
	var request recheckOperatorRequest
	for name, value := range fields {
		if name != "requestId" && name != "reason" {
			return recheckOperatorRequest{}, errors.New("invalid request body")
		}
		if value.Kind() != '"' {
			return recheckOperatorRequest{}, errors.New("invalid request body")
		}
		var field string
		if err := json.Unmarshal(value, &field); err != nil {
			return recheckOperatorRequest{}, errors.New("invalid request body")
		}
		if name == "requestId" {
			request.RequestID = field
		} else {
			request.Reason = field
		}
	}
	request.RequestID = strings.TrimSpace(request.RequestID)
	request.Reason = strings.TrimSpace(request.Reason)
	if !validRecheckRequestIDForHTTP(request.RequestID) {
		return recheckOperatorRequest{}, errors.New("invalid request id")
	}
	if len([]byte(request.Reason)) > 1024 {
		return recheckOperatorRequest{}, errors.New("reason is too long")
	}
	if chi.URLParam(r, "jobID") != "" && strings.HasSuffix(r.URL.Path, "/abandon") && request.Reason == "" {
		return recheckOperatorRequest{}, errors.New("abandon reason is required")
	}
	return request, nil
}

func validateRecheckOperatorHTTPBoundary(r *http.Request) (int, string) {
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")), "cross-site") {
		return http.StatusForbidden, "cross_origin_request"
	}
	if r.Header.Get(recheckOperatorRequestHeader) != "recheck-operator" {
		return http.StatusForbidden, "cross_origin_request"
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return http.StatusUnsupportedMediaType, "unsupported_media_type"
	}
	return 0, ""
}

func validRecheckRequestIDForHTTP(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '.' || char == '_' || char == ':' || char == '-' {
			continue
		}
		return false
	}
	return true
}

func writeRecheckOperatorError(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	_, _ = w.Write(marshalJSONValue(map[string]string{"error": message}))
}

func logRejectedRecheckOperatorAction(r *http.Request, action string, jobID int64, status int, err error) {
	logRejectedRecheckOperatorActionReason(r, action, jobID, status, recheckOperatorReasonForError(status, err), err)
}

func logRejectedRecheckOperatorActionReason(r *http.Request, action string, jobID int64, status int, reason string, err error) {
	attrs := []slog.Attr{slog.String("action", action), slog.String("reason_code", reason), slog.String("error", recheckOperatorErrorCategory(status, reason, err))}
	if jobID > 0 {
		attrs = append(attrs, slog.Int64("job_id", jobID))
	}
	if actor := strings.TrimSpace(getUserFromContext(r)); actor != "" {
		attrs = append(attrs, slog.String("operator", actor))
	}
	runtimeLog.Event(r.Context(), slogLevelForRecheckOperatorError(status, reason), "silence_recheck", "operator_action_rejected", "Recheck operator action was rejected", attrs...)
}

func recheckOperatorErrorCategory(status int, reason string, _ error) string {
	switch reason {
	case "cross_origin_request":
		return "request_origin"
	case "storage_unavailable":
		return "storage_unavailable"
	case "job_not_found":
		return "not_found"
	case "job_state_conflict", "request_conflict":
		return "state_conflict"
	case "operator_action_failed":
		return "storage_error"
	case "unsupported_media_type", "invalid_request":
		return "client_input"
	default:
		if status >= http.StatusInternalServerError {
			return "storage_error"
		}
		return "client_input"
	}
}

func recheckOperatorResponseMessage(status int, _ error) string {
	switch status {
	case http.StatusBadRequest:
		return "invalid recheck operator request"
	case http.StatusForbidden:
		return "recheck operator request rejected"
	case http.StatusUnsupportedMediaType:
		return "content type must be application/json"
	case http.StatusServiceUnavailable:
		return "alert lifecycle storage unavailable"
	case http.StatusInternalServerError:
		return "recheck operator action failed"
	case http.StatusNotFound:
		return "recheck job not found"
	case http.StatusConflict:
		return "recheck operator action conflict"
	default:
		return "recheck operator action failed"
	}
}

func marshalJSONValue(value any) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}

func slogLevelForRecheckOperatorError(_ int, reason string) slog.Level {
	switch reason {
	case "invalid_request", "cross_origin_request", "unsupported_media_type":
		return slog.LevelDebug
	case "job_not_found", "job_state_conflict", "request_conflict":
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

func recheckOperatorReason(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "invalid_request"
	case http.StatusUnauthorized:
		return "invalid_request"
	case http.StatusNotFound:
		return "job_not_found"
	case http.StatusConflict:
		return "job_state_conflict"
	case http.StatusForbidden:
		return "cross_origin_request"
	case http.StatusUnsupportedMediaType:
		return "unsupported_media_type"
	case http.StatusServiceUnavailable:
		return "storage_unavailable"
	default:
		return "operator_action_failed"
	}
}

func recheckOperatorReasonForError(status int, err error) string {
	switch {
	case errors.Is(err, alertlifecycle.ErrInvalidRecheckRequest):
		return "invalid_request"
	case errors.Is(err, alertlifecycle.ErrRecheckJobNotFound):
		return "job_not_found"
	case errors.Is(err, alertlifecycle.ErrRecheckJobStateConflict):
		return "job_state_conflict"
	case errors.Is(err, alertlifecycle.ErrRecheckOperatorConflict):
		return "request_conflict"
	default:
		return recheckOperatorReason(status)
	}
}
