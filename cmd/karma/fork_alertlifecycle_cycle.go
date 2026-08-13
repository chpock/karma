package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"time"

	"github.com/prymitive/karma/internal/alertmanager"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
	"github.com/prymitive/karma/internal/models"
)

func runAlertLifecyclePollCycle(successfulUpstreams map[string]bool) {
	if lifecycleStore == nil {
		return
	}

	startedAt := time.Now()
	ctx := context.Background()
	rawMatchLabels := buildRawMatchLabelsIndexContext(ctx)
	live := buildLiveLifecycleSnapshot(alertmanager.DedupAlerts(), rawMatchLabels)
	silenceSources := buildSilenceSourceSnapshot(successfulUpstreams)
	successful, failed := countSuccessfulUpstreams(successfulUpstreams)
	runtimeLog.Debug("alert_lifecycle", "poll_started", "Alert lifecycle poll cycle started", slog.Int("live_alerts", len(live)), slog.Int("sources", len(silenceSources)), slog.Int("successful_sources", successful), slog.Int("failed_sources", failed))
	result, err := lifecycleStore.ApplyPollCycle(alertlifecycle.PollCycleInput{
		LiveAlerts:          live,
		SuccessfulUpstreams: successfulUpstreams,
		SilenceSources:      silenceSources,
		Timestamp:           time.Now().UTC(),
	})
	if err != nil {
		runtimeLog.Error("alert_lifecycle", "poll_failed", "Alert lifecycle poll cycle failed", slog.String("stage", "apply"), slog.Any("error", err), slog.Duration("duration", time.Since(startedAt)))
		return
	}

	level := slog.LevelDebug
	if result.Inserted+result.SetInactive+result.Reactivated+result.Transitions+result.RecheckJobsQueued > 0 {
		level = slog.LevelInfo
	}
	runtimeLog.Event(context.Background(), level,
		"alert_lifecycle", "poll_completed", "Alert lifecycle poll cycle completed",
		slog.Int("inserted", result.Inserted),
		slog.Int("updated", result.Updated),
		slog.Int("setInactive", result.SetInactive),
		slog.Int("reactivated", result.Reactivated),
		slog.Int("skippedInactive", result.SkippedInactive),
		slog.Int("transitions", result.Transitions),
		slog.Int("recheckJobsQueued", result.RecheckJobsQueued),
		slog.Duration("duration", time.Since(startedAt)),
	)

	runAlertLifecycleRecheckOnce(context.Background())
}

func countSuccessfulUpstreams(upstreams map[string]bool) (int, int) {
	var successful, failed int
	for _, ok := range upstreams {
		if ok {
			successful++
		} else {
			failed++
		}
	}
	return successful, failed
}

func buildLiveLifecycleSnapshot(groups []models.AlertGroup, rawMatchLabels map[string]*string) []alertlifecycle.LiveAlertSnapshot {
	byAlertID := map[string]alertlifecycle.LiveAlertSnapshot{}

	for _, group := range groups {
		for _, alert := range group.Alerts {
			alertID := alert.LabelsFingerprint()

			labelsMap := alert.Labels.Map()
			labelsJSON, err := json.Marshal(labelsMap)
			if err != nil {
				runtimeLog.Error("alert_lifecycle", "snapshot_serialization_failed", "Failed to marshal alert labels for lifecycle snapshot", slog.String("alert_id", alertID), slog.String("field", "labels"), slog.Any("error", err))
				continue
			}

			annotationsMap := map[string]string{}
			for _, annotation := range alert.Annotations {
				annotationsMap[annotation.Name] = annotation.Value
			}
			annotationsJSON, err := json.Marshal(annotationsMap)
			if err != nil {
				runtimeLog.Error("alert_lifecycle", "snapshot_serialization_failed", "Failed to marshal alert annotations for lifecycle snapshot", slog.String("alert_id", alertID), slog.String("field", "annotations"), slog.Any("error", err))
				continue
			}

			groupLabelsJSON, err := json.Marshal(group.Labels.Map())
			if err != nil {
				runtimeLog.Error("alert_lifecycle", "snapshot_serialization_failed", "Failed to marshal group labels for lifecycle snapshot", slog.String("alert_id", alertID), slog.String("field", "group_labels"), slog.Any("error", err))
				continue
			}

			upstreams := make([]string, 0, len(alert.Alertmanager))
			sources := make([]alertlifecycle.SourceRef, 0, len(alert.Alertmanager))
			targetStates := map[string]alertlifecycle.LiveTargetSnapshot{}
			silencedBySources := make([]alertlifecycle.SourceSilenceRef, 0, len(alert.Alertmanager))
			for _, am := range alert.Alertmanager {
				upstreams = append(upstreams, am.Name)
				sources = append(sources, alertlifecycle.SourceRef{Name: am.Name, Cluster: am.Cluster})
				t := targetStates[am.Cluster]
				if t.Target == "" {
					t = alertlifecycle.LiveTargetSnapshot{Target: am.Cluster, State: modelStateToLifecycleState(am.State), StartsAt: am.StartsAt}
				}
				if am.StartsAt.Before(t.StartsAt) {
					t.StartsAt = am.StartsAt
				}
				state := modelStateToLifecycleState(am.State)
				switch {
				case state == alertlifecycle.StateActive:
					t.State = alertlifecycle.StateActive
				case t.State != alertlifecycle.StateActive && state == alertlifecycle.StateSuppressed:
					t.State = alertlifecycle.StateSuppressed
				case t.State != alertlifecycle.StateActive && t.State != alertlifecycle.StateSuppressed:
					t.State = alertlifecycle.StateUnprocessed
				}
				t.SourceNames = append(t.SourceNames, am.Name)
				targetStates[am.Cluster] = t
				silencedBySources = append(silencedBySources, alertlifecycle.SourceSilenceRef{
					SourceName: am.Name,
					ClusterID:  am.Cluster,
					SilenceIDs: append([]string{}, am.SilencedBy...),
				})
			}
			targets := make([]alertlifecycle.LiveTargetSnapshot, 0, len(targetStates))
			for _, target := range targetStates {
				sort.Strings(target.SourceNames)
				targets = append(targets, target)
			}
			sort.Slice(targets, func(i, j int) bool {
				return targets[i].Target < targets[j].Target
			})

			candidate := alertlifecycle.LiveAlertSnapshot{
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
				Targets:           targets,
				SilencedBySources: silencedBySources,
			}
			if current, ok := byAlertID[alertID]; ok {
				byAlertID[alertID] = mergeLiveLifecycleSnapshots(current, candidate)
			} else {
				byAlertID[alertID] = candidate
			}
		}
	}
	result := make([]alertlifecycle.LiveAlertSnapshot, 0, len(byAlertID))
	for _, snapshot := range byAlertID {
		result = append(result, snapshot)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].AlertID < result[j].AlertID })
	return result
}

func mergeLiveLifecycleSnapshots(left, right alertlifecycle.LiveAlertSnapshot) alertlifecycle.LiveAlertSnapshot {
	merged := left
	if right.GroupID+"\x00"+right.Receiver < left.GroupID+"\x00"+left.Receiver {
		merged = right
	}
	merged.AlertID = left.AlertID
	merged.State = mergeLifecycleRawState(left.State, right.State)
	merged.StartsAt = left.StartsAt
	if right.StartsAt.Before(merged.StartsAt) {
		merged.StartsAt = right.StartsAt
	}
	merged.RelevantUpstreams = mergeLifecycleStrings(left.RelevantUpstreams, right.RelevantUpstreams)
	merged.RelevantSources = mergeLifecycleSourceRefs(left.RelevantSources, right.RelevantSources)
	merged.SilencedBySources = mergeLifecycleSilenceRefs(left.SilencedBySources, right.SilencedBySources)
	merged.Targets = mergeLifecycleTargets(left.Targets, right.Targets)
	return merged
}

func mergeLifecycleRawState(left, right alertlifecycle.State) alertlifecycle.State {
	rank := func(state alertlifecycle.State) int {
		switch state {
		case alertlifecycle.StateActive:
			return 0
		case alertlifecycle.StateSuppressed:
			return 1
		case alertlifecycle.StateUnprocessed, alertlifecycle.StateInactive, alertlifecycle.StateResolved:
			return 2
		}
		return 2
	}
	if rank(right) < rank(left) {
		return right
	}
	return left
}

func mergeLifecycleStrings(left, right []string) []string {
	seen := map[string]struct{}{}
	for _, value := range append(append([]string{}, left...), right...) {
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func mergeLifecycleSourceRefs(left, right []alertlifecycle.SourceRef) []alertlifecycle.SourceRef {
	byKey := map[string]alertlifecycle.SourceRef{}
	for _, source := range append(append([]alertlifecycle.SourceRef{}, left...), right...) {
		byKey[source.Name+"\x00"+source.Cluster] = source
	}
	out := make([]alertlifecycle.SourceRef, 0, len(byKey))
	for _, source := range byKey {
		out = append(out, source)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Cluster < out[j].Cluster
	})
	return out
}

func mergeLifecycleSilenceRefs(left, right []alertlifecycle.SourceSilenceRef) []alertlifecycle.SourceSilenceRef {
	byKey := map[string]alertlifecycle.SourceSilenceRef{}
	for _, ref := range append(append([]alertlifecycle.SourceSilenceRef{}, left...), right...) {
		key := ref.SourceName + "\x00" + ref.ClusterID
		current := byKey[key]
		current.SourceName = ref.SourceName
		current.ClusterID = ref.ClusterID
		current.SilenceIDs = mergeLifecycleStrings(current.SilenceIDs, ref.SilenceIDs)
		byKey[key] = current
	}
	out := make([]alertlifecycle.SourceSilenceRef, 0, len(byKey))
	for _, ref := range byKey {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SourceName < out[j].SourceName })
	return out
}

func mergeLifecycleTargets(left, right []alertlifecycle.LiveTargetSnapshot) []alertlifecycle.LiveTargetSnapshot {
	byTarget := map[string]alertlifecycle.LiveTargetSnapshot{}
	for _, target := range append(append([]alertlifecycle.LiveTargetSnapshot{}, left...), right...) {
		current, ok := byTarget[target.Target]
		if !ok {
			byTarget[target.Target] = target
			continue
		}
		current.State = mergeLifecycleRawState(current.State, target.State)
		if target.StartsAt.Before(current.StartsAt) {
			current.StartsAt = target.StartsAt
		}
		current.SourceNames = mergeLifecycleStrings(current.SourceNames, target.SourceNames)
		byTarget[target.Target] = current
	}
	out := make([]alertlifecycle.LiveTargetSnapshot, 0, len(byTarget))
	for _, target := range byTarget {
		out = append(out, target)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Target < out[j].Target })
	return out
}

func buildRawMatchLabelsIndexContext(ctx context.Context) map[string]*string {
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
					runtimeLog.ErrorContext(ctx, "alert_lifecycle", "snapshot_serialization_failed", "Failed to marshal raw labels for lifecycle matching snapshot", slog.String("alert_id", fp), slog.String("field", "match_labels"), slog.Any("error", err))
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
	case models.AlertStateUnprocessed, models.AlertStateInactive, models.AlertStateResolved:
		return alertlifecycle.StateUnprocessed
	}
	return alertlifecycle.StateUnprocessed
}

func buildSilenceSourceSnapshot(successfulUpstreams map[string]bool) []alertlifecycle.SourceSilenceState {
	return buildSilenceSourceSnapshotFrom(alertmanager.GetAlertmanagers(), successfulUpstreams)
}

func buildSilenceSourceSnapshotFrom(upstreams []*alertmanager.Alertmanager, successfulUpstreams map[string]bool) []alertlifecycle.SourceSilenceState {
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
			if !models.IsKnownSilenceStatus(silence.StatusState) {
				runtimeLog.Warn(
					"silence_reconciliation", "invalid_rule_ignored", "Ignored invalid silence rule before lifecycle reconciliation",
					slog.String("alertmanager", am.Name),
					slog.String("silence_id", silence.ID),
					slog.String("status", silence.StatusState),
					slog.String("reason_code", "invalid_status"),
				)
				continue
			}
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
			})
		}

		out = append(out, alertlifecycle.SourceSilenceState{
			SourceName:     am.Name,
			ClusterID:      am.Cluster,
			SyncStatus:     syncStatus,
			Silences:       snapshots,
			MalformedRules: len(am.SilenceWarnings()),
		})
	}

	return out
}
