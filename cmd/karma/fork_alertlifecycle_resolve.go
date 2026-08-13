package main

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	jsonv2 "github.com/go-json-experiment/json"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
)

type resolveAlertRequest struct {
	Comment string `json:"comment"`
}

type resolveAlertResponse struct {
	Status    string  `json:"status"`
	AlertID   string  `json:"alertId"`
	OldState  string  `json:"oldState"`
	NewState  string  `json:"newState"`
	Applied   bool    `json:"applied"`
	Reason    string  `json:"reason,omitempty"`
	ChangedAt string  `json:"changedAt"`
	ChangedBy *string `json:"changedBy"`
}

func resolveAlert(w http.ResponseWriter, r *http.Request) {
	noCache(w)
	mimeJSON(w)

	if lifecycleStore == nil {
		runtimeLog.WarnContext(r.Context(), "alert_lifecycle", "resolve_rejected", "Manual alert resolve rejected because lifecycle storage is disabled", slog.String("reason_code", "storage_disabled"))
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := marshalJSON(map[string]string{"error": "alert lifecycle storage is disabled"})
		_, _ = w.Write(out)
		return
	}

	alertID := strings.TrimSpace(chi.URLParam(r, "id"))
	if alertID == "" {
		runtimeLog.DebugContext(r.Context(), "alert_lifecycle", "resolve_rejected", "Manual alert resolve request rejected", slog.String("reason_code", "missing_alert_id"))
		badRequestJSON(w, "missing alert id")
		return
	}

	comment, err := parseResolveComment(r)
	if err != nil {
		runtimeLog.DebugContext(r.Context(), "alert_lifecycle", "resolve_rejected", "Manual alert resolve request rejected", slog.String("alert_id", alertID), slog.String("reason_code", "invalid_request_body"))
		badRequestJSON(w, err.Error())
		return
	}

	changedBy := resolveActorFromRequest(r)
	attrs := []slog.Attr{slog.String("alert_id", alertID)}
	if changedBy != nil {
		attrs = append(attrs, slog.String("operator", *changedBy))
	}
	runtimeLog.Event(r.Context(), slog.LevelDebug, "alert_lifecycle", "resolve_requested", "Manual alert resolve requested", attrs...)
	result, err := lifecycleStore.ResolveAlertContext(r.Context(), alertID, changedBy, comment, time.Now().UTC())
	if err != nil {
		level := slog.LevelError
		event := "resolve_failed"
		reason := "internal_error"
		if errors.Is(err, alertlifecycle.ErrAlertNotFound) || errors.Is(err, alertlifecycle.ErrResolveStateConflict) || errors.Is(err, alertlifecycle.ErrInvalidAlertID) {
			level = slog.LevelWarn
			event = "resolve_rejected"
			switch {
			case errors.Is(err, alertlifecycle.ErrAlertNotFound):
				reason = "not_found"
			case errors.Is(err, alertlifecycle.ErrResolveStateConflict):
				reason = "live_state_conflict"
			case errors.Is(err, alertlifecycle.ErrInvalidAlertID):
				reason = "invalid_alert_id"
			}
		}
		runtimeLog.Event(r.Context(), level, "alert_lifecycle", event, "Manual alert resolve did not complete", append(attrs, slog.String("reason_code", reason), slog.Any("error", err))...)
		handleResolveAlertError(w, err)
		return
	}
	if result.Applied {
		runtimeLog.Event(r.Context(), slog.LevelInfo, "alert_lifecycle", "resolve_applied", "Manual alert resolve applied", append(attrs, slog.String("old_state", string(result.OldState)), slog.String("new_state", string(result.NewState)))...)
	} else {
		runtimeLog.Event(r.Context(), slog.LevelDebug, "alert_lifecycle", "resolve_noop", "Manual alert resolve was already applied", append(attrs, slog.String("reason_code", "already_resolved"))...)
	}

	w.WriteHeader(http.StatusOK)
	reason := ""
	if !result.Applied {
		reason = "already_resolved"
	}
	out, _ := marshalJSON(resolveAlertResponse{
		Status:    "success",
		AlertID:   result.AlertID,
		OldState:  string(result.OldState),
		NewState:  string(result.NewState),
		Applied:   result.Applied,
		Reason:    reason,
		ChangedAt: result.ChangedAt.UTC().Format(time.RFC3339Nano),
		ChangedBy: result.ChangedBy,
	})
	_, _ = w.Write(out)
}

func parseResolveComment(r *http.Request) (*string, error) {
	defer r.Body.Close()

	req := resolveAlertRequest{}
	if r.ContentLength == 0 {
		return nil, nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, nil
	}
	if err := jsonv2.Unmarshal(body, &req); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}

	trimmed := strings.TrimSpace(req.Comment)
	if trimmed == "" {
		return nil, nil
	}
	return &trimmed, nil
}

func resolveActorFromRequest(r *http.Request) *string {
	if !config.Config.Authentication.Enabled {
		return nil
	}
	username := strings.TrimSpace(getUserFromContext(r))
	if username == "" {
		return nil
	}
	return &username
}

func handleResolveAlertError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	message := "failed to resolve alert"

	switch {
	case errors.Is(err, alertlifecycle.ErrAlertNotFound):
		status = http.StatusNotFound
		message = err.Error()
	case errors.Is(err, alertlifecycle.ErrResolveStateConflict):
		status = http.StatusConflict
		message = err.Error()
	case errors.Is(err, alertlifecycle.ErrInvalidAlertID):
		status = http.StatusBadRequest
		message = err.Error()
	}

	w.WriteHeader(status)
	out, _ := marshalJSON(map[string]string{"error": message})
	_, _ = w.Write(out)
}
