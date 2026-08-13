package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"

	"github.com/prymitive/karma/internal/alertmanager"
	"github.com/prymitive/karma/internal/filters"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
	"github.com/prymitive/karma/internal/models"
)

func dedupedAlertsWithLifecycleContext(ctx context.Context, matchFilters []filters.Filter) []models.AlertGroup {
	deduped := alertmanager.DedupAlerts()
	return mergeLifecycleStoredAlertsContext(ctx, deduped, shouldIncludeResolved(matchFilters))
}

func mergeLifecycleStoredAlerts(live []models.AlertGroup, includeResolved bool) []models.AlertGroup {
	return mergeLifecycleStoredAlertsContext(context.Background(), live, includeResolved)
}

func mergeLifecycleStoredAlertsContext(ctx context.Context, live []models.AlertGroup, includeResolved bool) []models.AlertGroup {
	if lifecycleStore == nil {
		return live
	}

	stored, err := lifecycleStore.ListCurrentAlertsForMerge(includeResolved)
	if err != nil {
		runtimeLog.ErrorContext(ctx, "alert_lifecycle", "merge_query_failed", "Failed to query lifecycle alerts for merge", slog.Any("error", err))
		return live
	}
	if len(stored) == 0 {
		return live
	}

	byGroup := make(map[string]*models.AlertGroup)
	lifecycleRecencyByGroup := make(map[string]time.Time)
	for i := range live {
		group := &live[i]
		group.StateCount = ensureStateCountKeys(group.StateCount)
		byGroup[group.ID] = group
	}

	for _, current := range stored {
		alert, ok := lifecycleCurrentToModelAlert(ctx, current)
		if !ok {
			continue
		}
		groupLabels, ok := lifecycleCurrentGroupLabels(ctx, current)
		if !ok {
			continue
		}

		group, found := byGroup[current.GroupID]
		if !found {
			newGroup := models.AlertGroup{
				ID:                current.GroupID,
				Receiver:          current.Receiver,
				Labels:            groupLabels,
				Alerts:            models.AlertList{},
				AlertmanagerCount: map[string]int{},
				StateCount:        newStateCount(),
			}
			live = append(live, newGroup)
			group = &live[len(live)-1]
			byGroup[current.GroupID] = group
		} else if group.Labels.Len() == 0 && groupLabels.Len() > 0 {
			group.Labels = groupLabels
		}

		if current.LastSeenAt != nil {
			if lastSeen, hasLastSeen := lifecycleRecencyByGroup[current.GroupID]; !hasLastSeen || current.LastSeenAt.After(lastSeen) {
				lifecycleRecencyByGroup[current.GroupID] = *current.LastSeenAt
			}
		}

		if hasAlertByID(group.Alerts, alert.LabelsFingerprint()) {
			continue
		}

		group.Alerts = append(group.Alerts, alert)
		if group.SuppressedBreakdown == nil {
			group.SuppressedBreakdown = map[string]int{"current": 0, "past": 0}
		}
		if alert.State == models.AlertStateSuppressed {
			if alert.IsLive {
				group.SuppressedBreakdown["current"]++
			} else {
				group.SuppressedBreakdown["past"]++
			}
		}
		group.StateCount[alert.State.String()]++
		group.Alerts = sortAlerts(group.Alerts)
		group.LatestStartsAt = group.FindLatestStartsAt()
		group.Hash = group.ContentFingerprint()
	}

	for i := range live {
		live[i].Alerts = sortAlerts(live[i].Alerts)
		latest := live[i].FindLatestStartsAt()
		if lifecycleLatest, found := lifecycleRecencyByGroup[live[i].ID]; found && lifecycleLatest.After(latest) {
			latest = lifecycleLatest
		}
		live[i].LatestStartsAt = latest
		live[i].Hash = live[i].ContentFingerprint()
	}

	return live
}

func lifecycleCurrentToModelAlert(ctx context.Context, current alertlifecycle.AlertCurrent) (models.Alert, bool) {
	labelsMap := map[string]string{}
	if err := json.Unmarshal([]byte(current.LabelsJSON), &labelsMap); err != nil {
		runtimeLog.ErrorContext(ctx, "alert_lifecycle", "merge_decode_failed", "Failed to unmarshal lifecycle labels", slog.String("alert_id", current.AlertID), slog.String("field", "labels"), slog.Any("error", err))
		return models.Alert{}, false
	}

	annotationsMap := map[string]string{}
	if err := json.Unmarshal([]byte(current.AnnotationsJSON), &annotationsMap); err != nil {
		runtimeLog.ErrorContext(ctx, "alert_lifecycle", "merge_decode_failed", "Failed to unmarshal lifecycle annotations", slog.String("alert_id", current.AlertID), slog.String("field", "annotations"), slog.Any("error", err))
		return models.Alert{}, false
	}

	alertState, ok := models.AlertStateFromString(string(current.State))
	if !ok {
		runtimeLog.ErrorContext(ctx, "alert_lifecycle", "merge_state_invalid", "Failed to map lifecycle state to alert model", slog.String("state", string(current.State)))
		return models.Alert{}, false
	}

	alert := models.Alert{
		StartsAt:     current.StartsAt,
		Receiver:     current.Receiver,
		Labels:       models.LabelsFromMap(labelsMap),
		Annotations:  models.AnnotationsFromMap(annotationsMap),
		Alertmanager: []models.AlertmanagerInstance{},
		State:        alertState,
		RawState:     models.ParseAlertState(string(current.RawState)),
		HasRawState:  true,
		IsLive:       current.IsLive,
		HasIsLive:    true,
		IsResolved:   current.IsResolved,
	}
	for _, cause := range current.SuppressionCauses {
		matchers := make([]models.SuppressionCauseMatcher, 0, len(cause.Matchers))
		for _, matcher := range cause.Matchers {
			matchers = append(matchers, models.SuppressionCauseMatcher{
				Name: matcher.Name, Value: matcher.Value,
				IsRegex: matcher.IsRegex, IsEqual: matcher.IsEqual,
			})
		}
		alert.SuppressionCauses = append(alert.SuppressionCauses, models.SuppressionCause{
			Target:           cause.ClusterID,
			Source:           cause.SourceName,
			SilenceID:        cause.SilenceID,
			ResolutionStatus: cause.ResolutionStatus,
			StartsAt:         cause.StartsAt,
			EndsAt:           cause.EndsAt,
			CreatedBy:        cause.CreatedBy,
			Comment:          cause.Comment,
			Matchers:         matchers,
		})
	}
	alert.UpdateFingerprints()
	// Display labels may be filtered by labels.keep/strip. The lifecycle key is
	// the source alert identity captured before those presentation transforms.
	alert.LabelsFP = current.AlertID

	return alert, true
}

func lifecycleCurrentGroupLabels(ctx context.Context, current alertlifecycle.AlertCurrent) (promlabels.Labels, bool) {
	labelsMap := map[string]string{}
	if err := json.Unmarshal([]byte(current.GroupLabelsJSON), &labelsMap); err != nil {
		runtimeLog.ErrorContext(ctx, "alert_lifecycle", "merge_decode_failed", "Failed to unmarshal lifecycle group labels", slog.String("alert_id", current.AlertID), slog.String("field", "group_labels"), slog.Any("error", err))
		return promlabels.Labels{}, false
	}

	return models.LabelsFromMap(labelsMap), true
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
