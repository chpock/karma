package main

import (
	"encoding/json"
	"log/slog"
	"sort"

	"github.com/prymitive/karma/internal/alertmanager"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	"github.com/prymitive/karma/internal/filters"
	"github.com/prymitive/karma/internal/models"
)

func dedupedAlertsWithLifecycle(matchFilters []filters.Filter) []models.AlertGroup {
	deduped := alertmanager.DedupAlerts()
	return mergeLifecycleStoredAlerts(deduped, shouldIncludeResolved(matchFilters))
}

func mergeLifecycleStoredAlerts(live []models.AlertGroup, includeResolved bool) []models.AlertGroup {
	if lifecycleStore == nil {
		return live
	}

	stateFilter := []alertlifecycle.State{alertlifecycle.StateInactive}
	if includeResolved {
		stateFilter = append(stateFilter, alertlifecycle.StateResolved)
	}
	stored, err := lifecycleStore.ListCurrentAlertsByStates(stateFilter)
	if err != nil {
		slog.Error("Failed to query lifecycle alerts for merge", slog.Any("error", err))
		return live
	}
	if len(stored) == 0 {
		return live
	}

	byGroup := make(map[string]*models.AlertGroup)
	for i := range live {
		group := &live[i]
		group.StateCount = ensureStateCountKeys(group.StateCount)
		byGroup[group.ID] = group
	}

	for _, current := range stored {
		alert, ok := lifecycleCurrentToModelAlert(current)
		if !ok {
			continue
		}

		group, found := byGroup[current.GroupID]
		if !found {
			newGroup := models.AlertGroup{
				ID:                current.GroupID,
				Receiver:          current.Receiver,
				Labels:            models.LabelsFromMap(nil),
				Alerts:            models.AlertList{},
				AlertmanagerCount: map[string]int{},
				StateCount:        newStateCount(),
			}
			live = append(live, newGroup)
			group = &live[len(live)-1]
			byGroup[current.GroupID] = group
		}

		if hasAlertByID(group.Alerts, alert.LabelsFingerprint()) {
			continue
		}

		group.Alerts = append(group.Alerts, alert)
		group.StateCount[alert.State.String()]++
		group.Alerts = sortAlerts(group.Alerts)
		group.LatestStartsAt = group.FindLatestStartsAt()
		group.Hash = group.ContentFingerprint()
	}

	for i := range live {
		live[i].Alerts = sortAlerts(live[i].Alerts)
		live[i].LatestStartsAt = live[i].FindLatestStartsAt()
		live[i].Hash = live[i].ContentFingerprint()
	}

	return live
}

func lifecycleCurrentToModelAlert(current alertlifecycle.AlertCurrent) (models.Alert, bool) {
	labelsMap := map[string]string{}
	if err := json.Unmarshal([]byte(current.LabelsJSON), &labelsMap); err != nil {
		slog.Error("Failed to unmarshal lifecycle labels", slog.String("alertID", current.AlertID), slog.Any("error", err))
		return models.Alert{}, false
	}

	annotationsMap := map[string]string{}
	if err := json.Unmarshal([]byte(current.AnnotationsJSON), &annotationsMap); err != nil {
		slog.Error("Failed to unmarshal lifecycle annotations", slog.String("alertID", current.AlertID), slog.Any("error", err))
		return models.Alert{}, false
	}

	alertState, ok := models.AlertStateFromString(string(current.State))
	if !ok {
		slog.Error("Failed to map lifecycle state to alert model", slog.String("state", string(current.State)))
		return models.Alert{}, false
	}

	alert := models.Alert{
		StartsAt:     current.StartsAt,
		Receiver:     current.Receiver,
		Labels:       models.LabelsFromMap(labelsMap),
		Annotations:  models.AnnotationsFromMap(annotationsMap),
		Alertmanager: []models.AlertmanagerInstance{},
		State:        alertState,
	}
	alert.UpdateFingerprints()

	return alert, true
}

func hasAlertByID(alerts []models.Alert, alertID string) bool {
	for _, alert := range alerts {
		if alert.LabelsFingerprint() == alertID {
			return true
		}
	}
	return false
}

func ensureStateCountKeys(stateCount map[string]int) map[string]int {
	if stateCount == nil {
		stateCount = map[string]int{}
	}
	for _, state := range models.AlertStateList {
		if _, found := stateCount[state.String()]; !found {
			stateCount[state.String()] = 0
		}
	}
	return stateCount
}

func sortAlerts(alerts []models.Alert) []models.Alert {
	sort.Slice(alerts, func(i, j int) bool {
		return models.CompareAlerts(alerts[i], alerts[j]) < 0
	})
	return alerts
}

func shouldIncludeResolved(matchFilters []filters.Filter) bool {
	for _, f := range matchFilters {
		if !f.Valid() {
			continue
		}
		if f.Name() == "@state" && f.MatcherOperation() == "=" && f.Value() == models.AlertStateResolved.String() {
			return true
		}
	}
	return false
}
