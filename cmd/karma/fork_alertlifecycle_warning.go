package main

import (
	"log/slog"
	"net/http"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
)

type alertLifecycleWarningResponse struct {
	Status                      string   `json:"status"`
	StorageEnabled              bool     `json:"storageEnabled"`
	DBExpectedState             bool     `json:"dbExpectedState"`
	Reason                      string   `json:"reason"`
	RegroupStatus               string   `json:"regroupStatus"`
	RegroupError                string   `json:"regroupError"`
	RegroupProcessed            int      `json:"regroupProcessed"`
	RegroupTotal                int      `json:"regroupTotal"`
	SilenceInconsistentClusters []string `json:"silenceInconsistentClusters"`
	RecheckStatus               string   `json:"recheckStatus"`
	RecheckError                string   `json:"recheckError"`
}

func alertLifecycleWarning(w http.ResponseWriter, r *http.Request) {
	noCache(w)
	mimeJSON(w)

	if !config.Config.AlertLifecycle.Enabled || lifecycleStore == nil {
		w.WriteHeader(http.StatusOK)
		out, _ := marshalJSON(alertLifecycleWarningResponse{
			Status:          "ok",
			StorageEnabled:  false,
			DBExpectedState: true,
		})
		_, _ = w.Write(out)
		return
	}

	stats, err := lifecycleStore.GetStats()
	if err != nil {
		runtimeLog.Event(r.Context(), slog.LevelError, "alert_lifecycle", "warning_state_failed", "Failed to read alert lifecycle warning state", slog.Any("error", err))
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := marshalJSON(map[string]string{"error": "failed to read alert lifecycle warning state"})
		_, _ = w.Write(out)
		return
	}

	fingerprint := alertlifecycle.BuildGroupingFingerprint(alertlifecycle.GroupingConfig{
		LabelsKeep:       append([]string{}, config.Config.Labels.Keep...),
		LabelsKeepRegex:  append([]string{}, config.Config.Labels.KeepRegex...),
		LabelsStrip:      append([]string{}, config.Config.Labels.Strip...),
		LabelsStripRegex: append([]string{}, config.Config.Labels.StripRegex...),
	})

	reason := ""
	expected := true
	status := stats.RegroupStatus
	if status == "" {
		status = "ready"
	}
	if stats.GroupingFingerprint != fingerprint {
		expected = false
		reason = "Stored grouping fingerprint does not match current config"
	}
	if status != "ready" {
		expected = false
		if reason == "" {
			reason = "Background regroup is in progress or failed"
		}
	}
	if status == "error" && stats.RegroupError != "" {
		expected = false
		reason = stats.RegroupError
	}
	if len(stats.SilenceInconsistentClusters) > 0 {
		expected = false
		reason = "Alertmanager silence state is inconsistent across HA members"
	}
	if stats.RecheckCurrentStatus == "failed" {
		expected = false
		reason = stats.RecheckCurrentError
	}

	w.WriteHeader(http.StatusOK)
	out, _ := marshalJSON(alertLifecycleWarningResponse{
		Status:                      "ok",
		StorageEnabled:              true,
		DBExpectedState:             expected,
		Reason:                      reason,
		RegroupStatus:               status,
		RegroupError:                stats.RegroupError,
		RegroupProcessed:            stats.RegroupProcessed,
		RegroupTotal:                stats.RegroupTotal,
		SilenceInconsistentClusters: append([]string{}, stats.SilenceInconsistentClusters...),
		RecheckStatus:               stats.RecheckCurrentStatus,
		RecheckError:                stats.RecheckCurrentError,
	})
	_, _ = w.Write(out)
}
