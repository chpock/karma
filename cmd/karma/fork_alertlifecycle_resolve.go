package main

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	jsonv2 "github.com/go-json-experiment/json"
	"github.com/go-chi/chi/v5"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

type resolveAlertRequest struct {
	Comment string `json:"comment"`
}

type resolveAlertResponse struct {
	Status    string  `json:"status"`
	AlertID   string  `json:"alertId"`
	OldState  string  `json:"oldState"`
	NewState  string  `json:"newState"`
	ChangedAt string  `json:"changedAt"`
	ChangedBy *string `json:"changedBy"`
}

func resolveAlert(w http.ResponseWriter, r *http.Request) {
	noCache(w)
	mimeJSON(w)

	if lifecycleStore == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := marshalJSON(map[string]string{"error": "alert lifecycle storage is disabled"})
		_, _ = w.Write(out)
		return
	}

	alertID := strings.TrimSpace(chi.URLParam(r, "id"))
	if alertID == "" {
		badRequestJSON(w, "missing alert id")
		return
	}

	comment, err := parseResolveComment(r)
	if err != nil {
		badRequestJSON(w, err.Error())
		return
	}

	changedBy := resolveActorFromRequest(r)
	result, err := lifecycleStore.ResolveAlert(alertID, changedBy, comment, time.Now().UTC())
	if err != nil {
		handleResolveAlertError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	out, _ := marshalJSON(resolveAlertResponse{
		Status:    "success",
		AlertID:   result.AlertID,
		OldState:  string(result.OldState),
		NewState:  string(result.NewState),
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
	case errors.Is(err, alertlifecycle.ErrResolveStateConflict), errors.Is(err, alertlifecycle.ErrAlertAlreadyResolved):
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
