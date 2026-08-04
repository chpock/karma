package main

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/prymitive/karma/internal/alertmanager"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	"github.com/prymitive/karma/internal/models"
)

func runAlertLifecyclePollCycle(successfulUpstreams map[string]bool) {
	if lifecycleStore == nil {
		return
	}

	live := buildLiveLifecycleSnapshot(alertmanager.DedupAlerts())
	result, err := lifecycleStore.ApplyPollCycle(alertlifecycle.PollCycleInput{
		LiveAlerts:          live,
		SuccessfulUpstreams: successfulUpstreams,
		Timestamp:           time.Now().UTC(),
	})
	if err != nil {
		slog.Error("Alert lifecycle poll cycle failed", slog.Any("error", err))
		return
	}

	slog.Info(
		"Alert lifecycle poll cycle completed",
		slog.Int("inserted", result.Inserted),
		slog.Int("updated", result.Updated),
		slog.Int("setInactive", result.SetInactive),
		slog.Int("reactivated", result.Reactivated),
		slog.Int("skippedInactive", result.SkippedInactive),
		slog.Int("transitions", result.Transitions),
	)
}

func buildLiveLifecycleSnapshot(groups []models.AlertGroup) []alertlifecycle.LiveAlertSnapshot {
	result := make([]alertlifecycle.LiveAlertSnapshot, 0)
	seen := map[string]struct{}{}

	for _, group := range groups {
		for _, alert := range group.Alerts {
			alertID := alert.LabelsFingerprint()
			if _, found := seen[alertID]; found {
				continue
			}
			seen[alertID] = struct{}{}

			labelsMap := alert.Labels.Map()
			labelsJSON, err := json.Marshal(labelsMap)
			if err != nil {
				slog.Error("Failed to marshal alert labels for lifecycle snapshot", slog.String("alertID", alertID), slog.Any("error", err))
				continue
			}

			annotationsMap := map[string]string{}
			for _, annotation := range alert.Annotations {
				annotationsMap[annotation.Name] = annotation.Value
			}
			annotationsJSON, err := json.Marshal(annotationsMap)
			if err != nil {
				slog.Error("Failed to marshal alert annotations for lifecycle snapshot", slog.String("alertID", alertID), slog.Any("error", err))
				continue
			}

			upstreams := make([]string, 0, len(alert.Alertmanager))
			for _, am := range alert.Alertmanager {
				upstreams = append(upstreams, am.Name)
			}

			result = append(result, alertlifecycle.LiveAlertSnapshot{
				AlertID:           alertID,
				State:             modelStateToLifecycleState(alert.State),
				Receiver:          alert.Receiver,
				GroupID:           group.ID,
				LabelsJSON:        string(labelsJSON),
				AnnotationsJSON:   string(annotationsJSON),
				StartsAt:          alert.StartsAt,
				LastSeenAt:        time.Now().UTC(),
				RelevantUpstreams: upstreams,
			})
		}
	}

	return result
}

func modelStateToLifecycleState(state models.AlertState) alertlifecycle.State {
	switch state {
	case models.AlertStateActive:
		return alertlifecycle.StateActive
	case models.AlertStateSuppressed:
		return alertlifecycle.StateSuppressed
	default:
		return alertlifecycle.StateUnprocessed
	}
}
