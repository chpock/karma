package main

import (
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
)

type alertLifecycleWarningResponse struct {
	Status          string                       `json:"status"`
	StorageEnabled  bool                         `json:"storageEnabled"`
	DBExpectedState bool                         `json:"dbExpectedState"`
	Issues          []alertLifecycleWarningIssue `json:"issues"`
}

type alertLifecycleWarningIssue struct {
	Kind     string `json:"kind"`
	Severity string `json:"severity"`
	Summary  string `json:"summary"`
	Metadata any    `json:"metadata"`
}

type alertLifecycleRegroupOutOfDateMetadata struct {
	StoredFingerprint   string `json:"storedFingerprint"`
	ExpectedFingerprint string `json:"expectedFingerprint"`
}

type alertLifecycleRegroupInProgressMetadata struct {
	Processed int `json:"processed"`
	Total     int `json:"total"`
}

type alertLifecycleRegroupFailedMetadata struct {
	Error     string `json:"error"`
	Processed int    `json:"processed"`
	Total     int    `json:"total"`
}

type alertLifecycleSilenceHAInconsistentMetadata struct {
	TargetNames []string `json:"targetNames"`
}

type alertLifecycleRecheckRetryingMetadata struct {
	JobID            int64  `json:"jobId"`
	Status           string `json:"status"`
	RetryCount       int    `json:"retryCount"`
	NextAttemptAt    string `json:"nextAttemptAt"`
	LastFailureStage string `json:"lastFailureStage"`
}

type alertLifecycleRecheckBlockedMetadata struct {
	JobID             int64  `json:"jobId"`
	Status            string `json:"status"`
	FirstFailureStage string `json:"firstFailureStage"`
	FirstFailureAt    string `json:"firstFailureAt"`
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
			Issues:          []alertLifecycleWarningIssue{},
		})
		_, _ = w.Write(out)
		return
	}

	stats, err := lifecycleStore.GetStatsContext(r.Context())
	if err != nil {
		runtimeLog.Event(r.Context(), slog.LevelError, "alert_lifecycle", "warning_state_failed", "Failed to read alert lifecycle warning state", slog.String("reason_code", "warning_projection_failed"), slog.String("error", "storage_error"))
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

	issues := buildAlertLifecycleWarningIssues(stats, fingerprint)

	w.WriteHeader(http.StatusOK)
	out, _ := marshalJSON(alertLifecycleWarningResponse{
		Status:          "ok",
		StorageEnabled:  true,
		DBExpectedState: len(issues) == 0,
		Issues:          issues,
	})
	_, _ = w.Write(out)
}

func buildAlertLifecycleWarningIssues(stats alertlifecycle.Stats, expectedFingerprint string) []alertLifecycleWarningIssue {
	issues := make([]alertLifecycleWarningIssue, 0, 6)
	if stats.GroupingFingerprint != expectedFingerprint {
		issues = append(issues, alertLifecycleWarningIssue{
			Kind: "regroup_out_of_date", Severity: "warning", Summary: "Alert lifecycle grouping is out of date",
			Metadata: alertLifecycleRegroupOutOfDateMetadata{StoredFingerprint: stats.GroupingFingerprint, ExpectedFingerprint: expectedFingerprint},
		})
	}
	switch stats.RegroupStatus {
	case "running":
		issues = append(issues, alertLifecycleWarningIssue{
			Kind: "regroup_in_progress", Severity: "warning", Summary: "Alert lifecycle regroup is in progress",
			Metadata: alertLifecycleRegroupInProgressMetadata{Processed: stats.RegroupProcessed, Total: stats.RegroupTotal},
		})
	case "error":
		errorMessage := "regroup operation failed"
		issues = append(issues, alertLifecycleWarningIssue{
			Kind: "regroup_failed", Severity: "error", Summary: "Alert lifecycle regroup failed",
			Metadata: alertLifecycleRegroupFailedMetadata{Error: errorMessage, Processed: stats.RegroupProcessed, Total: stats.RegroupTotal},
		})
	}
	if len(stats.SilenceInconsistentClusters) > 0 {
		targetNames := append([]string{}, stats.SilenceInconsistentClusters...)
		sort.Strings(targetNames)
		issues = append(issues, alertLifecycleWarningIssue{
			Kind: "silence_ha_inconsistent", Severity: "warning", Summary: "Alertmanager silence state is inconsistent across HA members",
			Metadata: alertLifecycleSilenceHAInconsistentMetadata{TargetNames: targetNames},
		})
	}
	if head := stats.Recheck.HeadJob; head != nil {
		switch head.Status {
		case "retry_wait":
			if head.NextAttemptAt != nil && head.LastError != nil && strings.TrimSpace(head.LastError.Stage) != "" {
				issues = append(issues, alertLifecycleWarningIssue{
					Kind: "recheck_retrying", Severity: "warning", Summary: "Silence recheck is waiting to retry",
					Metadata: alertLifecycleRecheckRetryingMetadata{JobID: head.JobID, Status: head.Status, RetryCount: head.RetryCount, NextAttemptAt: head.NextAttemptAt.UTC().Format(time.RFC3339Nano), LastFailureStage: head.LastError.Stage},
				})
			}
		case "failed":
			if head.FirstError != nil && strings.TrimSpace(head.FirstError.Stage) != "" && !head.FirstError.At.IsZero() {
				issues = append(issues, alertLifecycleWarningIssue{
					Kind: "recheck_blocked", Severity: "error", Summary: "Silence recheck queue is blocked",
					Metadata: alertLifecycleRecheckBlockedMetadata{JobID: head.JobID, Status: head.Status, FirstFailureStage: head.FirstError.Stage, FirstFailureAt: head.FirstError.At.UTC().Format(time.RFC3339Nano)},
				})
			}
		}
	}
	sort.SliceStable(issues, func(i, j int) bool {
		if issues[i].Severity != issues[j].Severity {
			return issues[i].Severity == "error"
		}
		return issues[i].Kind < issues[j].Kind
	})
	return issues
}
