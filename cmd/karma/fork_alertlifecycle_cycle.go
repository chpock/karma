package main

import (
	"encoding/json"
	"log/slog"
	"sort"
	"time"

	"github.com/prymitive/karma/internal/alertmanager"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	"github.com/prymitive/karma/internal/models"
)

func runAlertLifecyclePollCycle(successfulUpstreams map[string]bool) {
	if lifecycleStore == nil {
		return
	}

	rawMatchLabels := buildRawMatchLabelsIndex()
	live := buildLiveLifecycleSnapshot(alertmanager.DedupAlerts(), rawMatchLabels)
	silenceSources := buildSilenceSourceSnapshot(successfulUpstreams)
	result, err := lifecycleStore.ApplyPollCycle(alertlifecycle.PollCycleInput{
		LiveAlerts:          live,
		SuccessfulUpstreams: successfulUpstreams,
		SilenceSources:      silenceSources,
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
		slog.Int("recheckJobsQueued", result.RecheckJobsQueued),
	)

	runAlertLifecycleRecheckOnce()
}

func buildLiveLifecycleSnapshot(groups []models.AlertGroup, rawMatchLabels map[string]*string) []alertlifecycle.LiveAlertSnapshot {
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

			groupLabelsJSON, err := json.Marshal(group.Labels.Map())
			if err != nil {
				slog.Error("Failed to marshal group labels for lifecycle snapshot", slog.String("alertID", alertID), slog.Any("error", err))
				continue
			}

			upstreams := make([]string, 0, len(alert.Alertmanager))
			sources := make([]alertlifecycle.SourceRef, 0, len(alert.Alertmanager))
			silencedBySources := make([]alertlifecycle.SourceSilenceRef, 0, len(alert.Alertmanager))
			for _, am := range alert.Alertmanager {
				upstreams = append(upstreams, am.Name)
				sources = append(sources, alertlifecycle.SourceRef{Name: am.Name, Cluster: am.Cluster})
				silencedBySources = append(silencedBySources, alertlifecycle.SourceSilenceRef{
					SourceName: am.Name,
					ClusterID:  am.Cluster,
					SilenceIDs: append([]string{}, am.SilencedBy...),
				})
			}

			result = append(result, alertlifecycle.LiveAlertSnapshot{
				AlertID:           alertID,
				State:             modelStateToLifecycleState(alert.State),
				Receiver:          alert.Receiver,
				GroupID:           group.ID,
				GroupLabelsJSON:   string(groupLabelsJSON),
				LabelsJSON:        string(labelsJSON),
				MatchLabelsJSON:   rawMatchLabels[alertID],
				AnnotationsJSON:   string(annotationsJSON),
				StartsAt:          alert.StartsAt,
				LastSeenAt:        time.Now().UTC(),
				RelevantUpstreams: upstreams,
				RelevantSources:   sources,
				SilencedBySources: silencedBySources,
			})
		}
	}

	return result
}

func buildRawMatchLabelsIndex() map[string]*string {
	byFP := make(map[string]*string)
	for _, am := range alertmanager.GetAlertmanagers() {
		for _, group := range am.Alerts() {
			for _, alert := range group.Alerts {
				fp := alert.LabelsFingerprint()
				if fp == "" {
					continue
				}
				rawLabelsJSON, err := json.Marshal(alert.Labels.Map())
				if err != nil {
					slog.Error("Failed to marshal raw labels for lifecycle matching snapshot", slog.String("alertID", fp), slog.Any("error", err))
					continue
				}
				value := string(rawLabelsJSON)
				if prev, exists := byFP[fp]; exists {
					if prev == nil || *prev != value {
						byFP[fp] = nil
					}
					continue
				}
				copied := value
				byFP[fp] = &copied
			}
		}
	}
	return byFP
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

func buildSilenceSourceSnapshot(successfulUpstreams map[string]bool) []alertlifecycle.SourceSilenceState {
	upstreams := alertmanager.GetAlertmanagers()
	out := make([]alertlifecycle.SourceSilenceState, 0, len(upstreams))

	for _, am := range upstreams {
		syncStatus := "failed"
		if successfulUpstreams[am.Name] {
			syncStatus = "success"
		}

		silencesMap := am.Silences()
		silenceIDs := make([]string, 0, len(silencesMap))
		for id := range silencesMap {
			silenceIDs = append(silenceIDs, id)
		}
		sort.Strings(silenceIDs)

		snapshots := make([]alertlifecycle.SourceSilenceSnapshot, 0, len(silenceIDs))
		for _, silenceID := range silenceIDs {
			silence := silencesMap[silenceID]
			matchers := make([]alertlifecycle.SilenceMatcherSnapshot, 0, len(silence.Matchers))
			for _, m := range silence.Matchers {
				matchers = append(matchers, alertlifecycle.SilenceMatcherSnapshot{
					Name:    m.Name,
					Value:   m.Value,
					IsRegex: m.IsRegex,
					IsEqual: m.IsEqual,
				})
			}
			annotationsMap := map[string]string{}
			for _, a := range silence.Annotations {
				annotationsMap[a.Name] = a.Value
			}
			annotationsJSON, err := json.Marshal(annotationsMap)
			if err != nil {
				annotationsJSON = []byte("{}")
			}
			snapshots = append(snapshots, alertlifecycle.SourceSilenceSnapshot{
				SilenceID:       silence.ID,
				Status:          silence.StatusState,
				StartsAt:        silence.StartsAt,
				EndsAt:          silence.EndsAt,
				CreatedBy:       silence.CreatedBy,
				Comment:         silence.Comment,
				Matchers:        matchers,
				UpdatedAt:       silence.UpdatedAt,
				AnnotationsJSON: string(annotationsJSON),
				TicketID:        silence.TicketID,
				TicketURL:       silence.TicketURL,
				RawPayloadJSON:  "",
			})
		}

		out = append(out, alertlifecycle.SourceSilenceState{
			SourceName: am.Name,
			ClusterID:  am.Cluster,
			SyncStatus: syncStatus,
			Silences:   snapshots,
		})
	}

	return out
}
