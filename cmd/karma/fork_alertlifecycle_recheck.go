package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	if lifecycleStore == nil {
		writeRecheckOperatorError(w, http.StatusServiceUnavailable, "alert lifecycle storage is disabled")
		return
	}
	jobID, err := strconv.ParseInt(strings.TrimSpace(chi.URLParam(r, "jobID")), 10, 64)
	if err != nil || jobID <= 0 {
		writeRecheckOperatorError(w, http.StatusBadRequest, "invalid recheck job id")
		return
	}
	request, err := parseRecheckOperatorRequest(w, r)
	if err != nil {
		writeRecheckOperatorError(w, http.StatusBadRequest, err.Error())
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
		runtimeLog.Event(r.Context(), slogLevelForRecheckOperatorError(status), "silence_recheck", "operator_action_rejected", "Recheck operator action was rejected", slog.String("action", action), slog.Int64("job_id", jobID), slog.String("reason_code", recheckOperatorReason(status)), slog.Any("error", err))
		writeRecheckOperatorError(w, status, err.Error())
		return
	}
	runtimeLog.Event(r.Context(), slog.LevelInfo, "silence_recheck", "operator_action_applied", "Recheck operator action was committed", slog.Int64("job_id", result.JobID), slog.String("action", result.Action), slog.String("request_id", result.RequestID), slog.String("resulting_status", result.CurrentStatus))
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
			return recheckOperatorRequest{}, fmt.Errorf("unknown request field %q", name)
		}
		if value.Kind() != '"' {
			return recheckOperatorRequest{}, fmt.Errorf("request field %q must be a string", name)
		}
		var field string
		if err := json.Unmarshal(value, &field); err != nil {
			return recheckOperatorRequest{}, fmt.Errorf("invalid request field %q", name)
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

func marshalJSONValue(value any) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}

func slogLevelForRecheckOperatorError(status int) slog.Level {
	if status >= 400 && status < 500 {
		return slog.LevelWarn
	}
	return slog.LevelError
}

func recheckOperatorReason(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "invalid_request"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "state_conflict"
	default:
		return "internal_error"
	}
}
