package main

import (
	"log/slog"
	"net/http"
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

	CurrentRows                 int            `json:"currentRows"`
	StateCounts                 map[string]int `json:"stateCounts"`
	SuppressedBreakdown         map[string]int `json:"suppressedBreakdown,omitempty"`
	HistoryRows                 int            `json:"historyRows"`
	GroupingFingerprint         string         `json:"groupingFingerprint"`
	RegroupTargetFingerprint    string         `json:"regroupTargetFingerprint"`
	RegroupStatus               string         `json:"regroupStatus"`
	RegroupError                string         `json:"regroupError"`
	RegroupProcessed            int            `json:"regroupProcessed"`
	RegroupTotal                int            `json:"regroupTotal"`
	DBExpectedState             bool           `json:"dbExpectedState"`
	DBExpectedStateReason       string         `json:"dbExpectedStateReason"`
	LastRegroupStartedAt        *string        `json:"lastRegroupStartedAt"`
	LastRegroupFinishedAt       *string        `json:"lastRegroupFinishedAt"`
	FileSizeBytes               int64          `json:"fileSizeBytes"`
	PageCount                   int            `json:"pageCount"`
	PageSizeBytes               int            `json:"pageSizeBytes"`
	FreelistCount               int            `json:"freelistCount"`
	LastHousekeepingAt          *string        `json:"lastHousekeepingAt"`
	LastIncrementalVacuumAt     *string        `json:"lastIncrementalVacuumAt"`
	SilenceHistoryTTLHours      float64        `json:"silenceHistoryTTLHours"`
	SilenceInconsistentClusters []string       `json:"silenceInconsistentClusters"`
	RecheckQueueDepth           int            `json:"recheckQueueDepth"`
	RecheckCurrentJobID         int64          `json:"recheckCurrentJobId"`
	RecheckCurrentStatus        string         `json:"recheckCurrentStatus"`
	RecheckCurrentProcessed     int            `json:"recheckCurrentProcessed"`
	RecheckCurrentPromoted      int            `json:"recheckCurrentPromoted"`
	RecheckCurrentSkipped       int            `json:"recheckCurrentSkipped"`
	RecheckCurrentError         string         `json:"recheckCurrentError"`
	RecheckOldestCreatedAt      *string        `json:"recheckOldestCreatedAt"`
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
			DBExpectedState:        true,
		})
		_, _ = w.Write(out)
		return
	}

	stats, err := lifecycleStore.GetStats()
	if err != nil {
		runtimeLog.Event(r.Context(), slog.LevelError, "alert_lifecycle", "stats_failed", "Failed to read alert lifecycle stats", slog.Any("error", err))
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
		RegroupError:                stats.RegroupError,
		RegroupProcessed:            stats.RegroupProcessed,
		RegroupTotal:                stats.RegroupTotal,
		DBExpectedState:             true,
		FileSizeBytes:               stats.FileSizeBytes,
		PageCount:                   stats.PageCount,
		PageSizeBytes:               stats.PageSizeBytes,
		FreelistCount:               stats.FreelistCount,
		SilenceInconsistentClusters: append([]string{}, stats.SilenceInconsistentClusters...),
		RecheckQueueDepth:           stats.RecheckQueueDepth,
		RecheckCurrentJobID:         stats.RecheckCurrentJobID,
		RecheckCurrentStatus:        stats.RecheckCurrentStatus,
		RecheckCurrentProcessed:     stats.RecheckCurrentProcessed,
		RecheckCurrentPromoted:      stats.RecheckCurrentPromoted,
		RecheckCurrentSkipped:       stats.RecheckCurrentSkipped,
		RecheckCurrentError:         stats.RecheckCurrentError,
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
	if stats.RecheckOldestCreatedAt != nil {
		value := stats.RecheckOldestCreatedAt.UTC().Format(time.RFC3339Nano)
		response.RecheckOldestCreatedAt = &value
	}

	status := stats.RegroupStatus
	if status == "" {
		status = "ready"
		response.RegroupStatus = status
	}
	if status != "ready" {
		response.DBExpectedState = false
		response.DBExpectedStateReason = "Background regroup is in progress or failed"
	}
	if stats.GroupingFingerprint != expectedFingerprint {
		response.DBExpectedState = false
		if response.DBExpectedStateReason == "" {
			response.DBExpectedStateReason = "Stored grouping fingerprint does not match current config"
		}
	}
	if status == "error" {
		response.DBExpectedState = false
		if stats.RegroupError != "" {
			response.DBExpectedStateReason = stats.RegroupError
		}
	}
	if len(stats.SilenceInconsistentClusters) > 0 {
		response.DBExpectedState = false
		response.DBExpectedStateReason = "Alertmanager silence state is inconsistent across HA members"
	}
	if stats.RecheckCurrentStatus == "failed" {
		response.DBExpectedState = false
		response.DBExpectedStateReason = stats.RecheckCurrentError
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
