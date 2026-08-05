package main

import (
	"net/http"
	"time"

	"github.com/prymitive/karma/internal/config"
)

type alertLifecycleStatsResponse struct {
	Status            string  `json:"status"`
	StorageEnabled    bool    `json:"storageEnabled"`
	SQLitePath        string  `json:"sqlitePath"`
	RetentionTTLHours float64 `json:"retentionTTLHours"`
	MaxAlerts         int     `json:"maxAlerts"`

	CurrentRows             int     `json:"currentRows"`
	StateCounts             map[string]int `json:"stateCounts"`
	HistoryRows             int     `json:"historyRows"`
	FileSizeBytes           int64   `json:"fileSizeBytes"`
	PageCount               int     `json:"pageCount"`
	PageSizeBytes           int     `json:"pageSizeBytes"`
	FreelistCount           int     `json:"freelistCount"`
	LastHousekeepingAt      *string `json:"lastHousekeepingAt"`
	LastIncrementalVacuumAt *string `json:"lastIncrementalVacuumAt"`
}

func alertLifecycleStats(w http.ResponseWriter, _ *http.Request) {
	noCache(w)
	mimeJSON(w)

	if !config.Config.AlertLifecycle.Enabled || lifecycleStore == nil {
		w.WriteHeader(http.StatusOK)
		out, _ := marshalJSON(alertLifecycleStatsResponse{
			Status:            "ok",
			StorageEnabled:    false,
			SQLitePath:        config.Config.AlertLifecycle.SQLite.Path,
			RetentionTTLHours: config.Config.AlertLifecycle.Retention.TTL.Hours(),
			MaxAlerts:         config.Config.AlertLifecycle.Retention.MaxAlerts,
			StateCounts:       map[string]int{},
		})
		_, _ = w.Write(out)
		return
	}

	stats, err := lifecycleStore.GetStats()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := marshalJSON(map[string]string{"error": "failed to read alert lifecycle stats"})
		_, _ = w.Write(out)
		return
	}

	response := alertLifecycleStatsResponse{
		Status:            "ok",
		StorageEnabled:    true,
		SQLitePath:        config.Config.AlertLifecycle.SQLite.Path,
		RetentionTTLHours: config.Config.AlertLifecycle.Retention.TTL.Hours(),
		MaxAlerts:         config.Config.AlertLifecycle.Retention.MaxAlerts,
		CurrentRows:       stats.CurrentRows,
		StateCounts:       map[string]int{},
		HistoryRows:       stats.HistoryRows,
		FileSizeBytes:     stats.FileSizeBytes,
		PageCount:         stats.PageCount,
		PageSizeBytes:     stats.PageSizeBytes,
		FreelistCount:     stats.FreelistCount,
	}
	for state, count := range stats.StateCounts {
		response.StateCounts[string(state)] = count
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
