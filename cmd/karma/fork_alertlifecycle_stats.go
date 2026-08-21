package main

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
)

type alertLifecycleStatsResponse struct {
	Status            string  `json:"status"`
	StorageEnabled    bool    `json:"storageEnabled"`
	SQLitePath        string  `json:"sqlitePath"`
	RetentionTTLHours float64 `json:"retentionTTLHours"`
	MaxAlerts         int     `json:"maxAlerts"`

	CurrentRows                 int                                `json:"currentRows"`
	StateCounts                 map[string]int                     `json:"stateCounts"`
	SuppressedBreakdown         map[string]int                     `json:"suppressedBreakdown,omitempty"`
	HistoryRows                 int                                `json:"historyRows"`
	GroupingFingerprint         string                             `json:"groupingFingerprint"`
	RegroupTargetFingerprint    string                             `json:"regroupTargetFingerprint"`
	RegroupStatus               string                             `json:"regroupStatus"`
	RegroupError                string                             `json:"regroupError"`
	RegroupProcessed            int                                `json:"regroupProcessed"`
	RegroupTotal                int                                `json:"regroupTotal"`
	DBExpectedState             bool                               `json:"dbExpectedState"`
	DBExpectedStateReason       string                             `json:"dbExpectedStateReason"`
	LastRegroupStartedAt        *string                            `json:"lastRegroupStartedAt"`
	LastRegroupFinishedAt       *string                            `json:"lastRegroupFinishedAt"`
	FileSizeBytes               int64                              `json:"fileSizeBytes"`
	PageCount                   int                                `json:"pageCount"`
	PageSizeBytes               int                                `json:"pageSizeBytes"`
	FreelistCount               int                                `json:"freelistCount"`
	LastHousekeepingAt          *string                            `json:"lastHousekeepingAt"`
	LastIncrementalVacuumAt     *string                            `json:"lastIncrementalVacuumAt"`
	SilenceHistoryTTLHours      float64                            `json:"silenceHistoryTTLHours"`
	SilenceInconsistentClusters []string                           `json:"silenceInconsistentClusters"`
	Recheck                     alertLifecycleRecheckStatsResponse `json:"recheck"`
}

type alertLifecycleRecheckStatsResponse struct {
	QueueDepth int                                   `json:"queueDepth"`
	HeadJob    *alertLifecycleRecheckHeadJobResponse `json:"headJob"`
}

type alertLifecycleRecheckOutcomeCountsResponse struct {
	Promoted          int `json:"promoted"`
	Stale             int `json:"stale"`
	Ineligible        int `json:"ineligible"`
	MissingLabels     int `json:"missingLabels"`
	IncompleteSources int `json:"incompleteSources"`
}

type alertLifecycleRecheckErrorResponse struct {
	Class   string `json:"class"`
	Stage   string `json:"stage"`
	At      string `json:"at"`
	Message string `json:"message"`
}

type alertLifecycleRecheckHeadJobResponse struct {
	JobID                 int64                                      `json:"jobId"`
	TriggerType           string                                     `json:"triggerType"`
	Status                string                                     `json:"status"`
	CreatedAt             string                                     `json:"createdAt"`
	StartedAt             *string                                    `json:"startedAt"`
	FinishedAt            *string                                    `json:"finishedAt"`
	TotalCandidates       int                                        `json:"totalCandidates"`
	ProcessedCandidates   int                                        `json:"processedCandidates"`
	OutcomeCounts         alertLifecycleRecheckOutcomeCountsResponse `json:"outcomeCounts"`
	AttemptCount          int                                        `json:"attemptCount"`
	RetryCount            int                                        `json:"retryCount"`
	ConsecutiveRetryCount int                                        `json:"consecutiveRetryCount"`
	NextAttemptAt         *string                                    `json:"nextAttemptAt"`
	FirstError            *alertLifecycleRecheckErrorResponse        `json:"firstError"`
	LastError             *alertLifecycleRecheckErrorResponse        `json:"lastError"`
	RecoveryCount         int                                        `json:"recoveryCount"`
}

func toAlertLifecycleRecheckHeadJobResponse(job *alertlifecycle.RecheckHeadJob) *alertLifecycleRecheckHeadJobResponse {
	if job == nil {
		return nil
	}
	return &alertLifecycleRecheckHeadJobResponse{
		JobID: job.JobID, TriggerType: job.TriggerType, Status: job.Status,
		CreatedAt: job.CreatedAt.UTC().Format(time.RFC3339Nano),
		StartedAt: formatOptionalLifecycleTime(job.StartedAt), FinishedAt: formatOptionalLifecycleTime(job.FinishedAt),
		TotalCandidates: job.TotalCandidates, ProcessedCandidates: job.ProcessedCandidates,
		OutcomeCounts: alertLifecycleRecheckOutcomeCountsResponse{
			Promoted: job.OutcomeCounts.Promoted, Stale: job.OutcomeCounts.Stale,
			Ineligible: job.OutcomeCounts.Ineligible, MissingLabels: job.OutcomeCounts.MissingLabels,
			IncompleteSources: job.OutcomeCounts.IncompleteSources,
		}, AttemptCount: job.AttemptCount, RetryCount: job.RetryCount,
		ConsecutiveRetryCount: job.ConsecutiveRetryCount, NextAttemptAt: formatOptionalLifecycleTime(job.NextAttemptAt),
		FirstError: alertLifecycleRecheckErrorResponseFor(job.FirstError), LastError: alertLifecycleRecheckErrorResponseFor(job.LastError),
		RecoveryCount: job.RecoveryCount,
	}
}

func alertLifecycleRecheckErrorResponseFor(value *alertlifecycle.RecheckErrorSnapshot) *alertLifecycleRecheckErrorResponse {
	if value == nil {
		return nil
	}
	return &alertLifecycleRecheckErrorResponse{Class: value.Class, Stage: value.Stage, At: value.At.UTC().Format(time.RFC3339Nano), Message: "recheck attempt failed"}
}

func safeRegroupError(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return "regroup operation failed"
}

func formatOptionalLifecycleTime(value *time.Time) *string {
	if value == nil {
		return nil
	}
	formatted := value.UTC().Format(time.RFC3339Nano)
	return &formatted
}

func alertLifecycleStats(w http.ResponseWriter, r *http.Request) {
	noCache(w)
	mimeJSON(w)

	if !config.Config.AlertLifecycle.Enabled || lifecycleStore == nil {
		w.WriteHeader(http.StatusOK)
		out, _ := marshalJSON(alertLifecycleStatsResponse{
			Status:                 "ok",
			StorageEnabled:         false,
			SQLitePath:             config.Config.AlertLifecycle.SQLite.Path,
			RetentionTTLHours:      config.Config.AlertLifecycle.Retention.TTL.Hours(),
			SilenceHistoryTTLHours: config.Config.AlertLifecycle.SilenceHistory.Retention.TTL.Hours(),
			MaxAlerts:              config.Config.AlertLifecycle.Retention.MaxAlerts,
			StateCounts:            map[string]int{},
			SuppressedBreakdown:    map[string]int{"current": 0, "past": 0},
			Recheck:                alertLifecycleRecheckStatsResponse{QueueDepth: 0, HeadJob: nil},
			DBExpectedState:        true,
		})
		_, _ = w.Write(out)
		return
	}

	stats, err := lifecycleStore.GetStatsContext(r.Context())
	if err != nil {
		runtimeLog.Event(r.Context(), slog.LevelError, "alert_lifecycle", "stats_failed", "Failed to read alert lifecycle stats", slog.String("reason_code", "stats_query_failed"), slog.String("error", "storage_error"))
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := marshalJSON(map[string]string{"error": "failed to read alert lifecycle stats"})
		_, _ = w.Write(out)
		return
	}

	groupingCfg := alertlifecycle.GroupingConfig{
		LabelsKeep:       append([]string{}, config.Config.Labels.Keep...),
		LabelsKeepRegex:  append([]string{}, config.Config.Labels.KeepRegex...),
		LabelsStrip:      append([]string{}, config.Config.Labels.Strip...),
		LabelsStripRegex: append([]string{}, config.Config.Labels.StripRegex...),
	}
	expectedFingerprint := alertlifecycle.BuildGroupingFingerprint(groupingCfg)

	response := alertLifecycleStatsResponse{
		Status:                      "ok",
		StorageEnabled:              true,
		SQLitePath:                  config.Config.AlertLifecycle.SQLite.Path,
		RetentionTTLHours:           config.Config.AlertLifecycle.Retention.TTL.Hours(),
		SilenceHistoryTTLHours:      config.Config.AlertLifecycle.SilenceHistory.Retention.TTL.Hours(),
		MaxAlerts:                   config.Config.AlertLifecycle.Retention.MaxAlerts,
		CurrentRows:                 stats.CurrentRows,
		StateCounts:                 map[string]int{},
		SuppressedBreakdown:         map[string]int{"current": stats.SuppressedBreakdownCurrent, "past": stats.SuppressedBreakdownPast},
		HistoryRows:                 stats.HistoryRows,
		GroupingFingerprint:         stats.GroupingFingerprint,
		RegroupTargetFingerprint:    stats.RegroupTargetFingerprint,
		RegroupStatus:               stats.RegroupStatus,
		RegroupError:                safeRegroupError(stats.RegroupError),
		RegroupProcessed:            stats.RegroupProcessed,
		RegroupTotal:                stats.RegroupTotal,
		DBExpectedState:             true,
		FileSizeBytes:               stats.FileSizeBytes,
		PageCount:                   stats.PageCount,
		PageSizeBytes:               stats.PageSizeBytes,
		FreelistCount:               stats.FreelistCount,
		SilenceInconsistentClusters: append([]string{}, stats.SilenceInconsistentClusters...),
		Recheck:                     alertLifecycleRecheckStatsResponse{QueueDepth: stats.Recheck.QueueDepth, HeadJob: toAlertLifecycleRecheckHeadJobResponse(stats.Recheck.HeadJob)},
	}
	for state, count := range stats.StateCounts {
		response.StateCounts[state] = count
	}

	if stats.RegroupStartedAt != nil {
		value := stats.RegroupStartedAt.UTC().Format(time.RFC3339Nano)
		response.LastRegroupStartedAt = &value
	}
	if stats.RegroupFinishedAt != nil {
		value := stats.RegroupFinishedAt.UTC().Format(time.RFC3339Nano)
		response.LastRegroupFinishedAt = &value
	}
	status := stats.RegroupStatus
	if status == "" {
		status = "ready"
		response.RegroupStatus = status
	}
	issues := buildAlertLifecycleWarningIssues(stats, expectedFingerprint)
	response.DBExpectedState = len(issues) == 0
	if len(issues) > 0 {
		response.DBExpectedStateReason = issues[0].Summary
	}
	if response.RegroupTargetFingerprint == "" {
		response.RegroupTargetFingerprint = expectedFingerprint
	}
	if stats.LastHousekeepingAt != nil {
		value := stats.LastHousekeepingAt.UTC().Format(time.RFC3339Nano)
		response.LastHousekeepingAt = &value
	}
	if stats.LastIncrementalVacuumAt != nil {
		value := stats.LastIncrementalVacuumAt.UTC().Format(time.RFC3339Nano)
		response.LastIncrementalVacuumAt = &value
	}

	w.WriteHeader(http.StatusOK)
	out, _ := marshalJSON(response)
	_, _ = w.Write(out)
}
