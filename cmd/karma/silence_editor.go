package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	stdregexp "regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/prymitive/karma/internal/alertmanager"
	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
	"github.com/prymitive/karma/internal/mapper"
	"github.com/prymitive/karma/internal/models"
	"github.com/prymitive/karma/internal/regex"
	"github.com/prymitive/karma/internal/uri"
)

const (
	silenceEditorLabelLimit                   = 1000
	silenceEditorValueLimit                   = 50
	silenceEditorPageLimit                    = 100
	silenceEditorMatcherLimit                 = 100
	silenceEditorPreviewMaxRequestBytes int64 = 64 * 1024
)

var silenceEditorMatcherNamePattern = stdregexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

type silenceEditorMatcher struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"isRegex"`
	IsEqual bool   `json:"isEqual"`
}

type silenceEditorPreviewRequest struct {
	Target   string                 `json:"target"`
	Matchers []silenceEditorMatcher `json:"matchers"`
	Cursor   string                 `json:"cursor,omitempty"`
	Snapshot string                 `json:"snapshot,omitempty"`
	Limit    int                    `json:"limit,omitempty"`
}

type silenceEditorSubmitRequest struct {
	Target            string         `json:"target"`
	Operation         string         `json:"operation"`
	OriginalSilenceID string         `json:"originalSilenceID,omitempty"`
	RouteMember       string         `json:"routeMember,omitempty"`
	RevisionToken     string         `json:"revisionToken,omitempty"`
	Payload           models.Silence `json:"payload"`
}

type silenceEditorSubmitResponse struct {
	Status        string                     `json:"status"`
	Target        string                     `json:"target"`
	Mode          string                     `json:"mode"`
	SilenceID     string                     `json:"silenceID"`
	SubmittedBy   string                     `json:"submittedBy"`
	CorrelationID string                     `json:"correlationID"`
	Direct        *silenceEditorDirectSubmit `json:"direct,omitempty"`
}

type silenceEditorDirectSubmit struct {
	Endpoint        string            `json:"endpoint"`
	Headers         map[string]string `json:"headers"`
	CORSCredentials string            `json:"corsCredentials"`
	Member          string            `json:"member"`
	Payload         models.Silence    `json:"payload"`
}

type silenceEditorStatusResponse struct {
	IsExpired     bool           `json:"isExpired"`
	Member        string         `json:"member"`
	RevisionToken string         `json:"revisionToken"`
	Silence       models.Silence `json:"silence"`
}

type silenceEditorErrorResponse struct {
	Error          string `json:"error"`
	OutcomeUnknown bool   `json:"outcomeUnknown,omitempty"`
	RequiresReopen bool   `json:"requiresReopen,omitempty"`
}

type silenceEditorPreviewAlert struct {
	ID                string                `json:"id"`
	State             string                `json:"state"`
	RawState          string                `json:"rawState"`
	IsLive            bool                  `json:"isLive"`
	IsSuppressed      bool                  `json:"isSuppressed"`
	IsResolved        bool                  `json:"isResolved"`
	StartsAt          string                `json:"startsAt"`
	LastSeenAt        *string               `json:"lastSeenAt,omitempty"`
	Labels            []models.OrderedLabel `json:"labels"`
	MatchedLabels     []models.OrderedLabel `json:"matchedLabels"`
	MatchedLabelNames []string              `json:"matchedLabelNames"`
	MissingLabelNames []string              `json:"missingLabelNames"`
}

type silenceEditorPreviewResponse struct {
	Status              string                      `json:"status"`
	Target              string                      `json:"target"`
	Total               int                         `json:"total"`
	Returned            int                         `json:"returned"`
	Limit               int                         `json:"limit"`
	NextCursor          string                      `json:"nextCursor,omitempty"`
	Snapshot            string                      `json:"snapshot"`
	ExcludedNoRawLabels int                         `json:"excludedNoRawLabels"`
	Alerts              []silenceEditorPreviewAlert `json:"alerts"`
}

type silenceEditorLabelItem struct {
	Name   string `json:"name"`
	Values int    `json:"values"`
}

type silenceEditorLabelsResponse struct {
	Status    string                   `json:"status"`
	Target    string                   `json:"target"`
	Total     int                      `json:"total"`
	Truncated bool                     `json:"truncated"`
	Labels    []silenceEditorLabelItem `json:"labels"`
}

type silenceEditorValueItem struct {
	Value string `json:"value"`
	Hits  int    `json:"hits"`
}

type silenceEditorValuesResponse struct {
	Status    string                   `json:"status"`
	Target    string                   `json:"target"`
	Name      string                   `json:"name"`
	Total     int                      `json:"total"`
	Truncated bool                     `json:"truncated"`
	Values    []silenceEditorValueItem `json:"values"`
}

type silenceEditorPreviewCandidate struct {
	AlertID      string
	State        models.AlertState
	RawState     models.AlertState
	IsLive       bool
	IsSuppressed bool
	IsResolved   bool
	StartsAt     string
	LastSeenAt   *string
	Labels       models.OrderedLabels
	RawLabels    map[string]string
}

type compiledSilenceEditorMatcher struct {
	matcher models.SilenceMatcher
	regex   *stdregexp.Regexp
}

type silenceEditorPreviewCursor struct {
	Version     int     `json:"v"`
	Fingerprint string  `json:"f"`
	Snapshot    string  `json:"s"`
	Rank        int     `json:"r"`
	LastSeenAt  *string `json:"l,omitempty"`
	ID          string  `json:"i"`
}

func silenceEditorPreview(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	noCache(w)
	r.Body = http.MaxBytesReader(w, r.Body, silenceEditorPreviewMaxRequestBytes)
	var req silenceEditorPreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		runtimeLog.DebugContext(r.Context(), "silence_preview", "preview_rejected", "Silence preview request rejected", slog.String("reason_code", "invalid_request_body"))
		badRequestJSON(w, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	req.Target = strings.TrimSpace(req.Target)
	runtimeLog.DebugContext(r.Context(), "silence_preview", "preview_requested", "Silence preview requested", slog.String("target", req.Target), slog.Int("matcher_count", len(req.Matchers)), slog.Int("requested_limit", req.Limit), slog.Bool("cursor_present", strings.TrimSpace(req.Cursor) != ""), slog.Bool("snapshot_present", strings.TrimSpace(req.Snapshot) != ""))
	if req.Target == "" {
		logSilencePreviewRejection(r.Context(), req.Target, "missing_target", slog.LevelDebug)
		badRequestJSON(w, "target is required")
		return
	}
	if len(req.Matchers) == 0 {
		logSilencePreviewRejection(r.Context(), req.Target, "missing_matchers", slog.LevelDebug)
		badRequestJSON(w, "at least one matcher is required")
		return
	}

	compiled, err := compileSilenceEditorMatchers(req.Matchers)
	if err != nil {
		logSilencePreviewRejection(r.Context(), req.Target, "invalid_matcher", slog.LevelDebug)
		badRequestJSON(w, err.Error())
		return
	}
	if !silenceEditorMatcherSetIsSafe(compiled) {
		logSilencePreviewRejection(r.Context(), req.Target, "unsafe_matcher_set", slog.LevelDebug)
		badRequestJSON(w, "at least one matcher must not match the empty string")
		return
	}
	fingerprint := silenceEditorPreviewFingerprint(req.Target, compiled)

	limit := req.Limit
	if limit < 1 || limit > silenceEditorPageLimit {
		limit = silenceEditorPageLimit
	}
	var cursor *silenceEditorPreviewCursor
	if req.Cursor != "" {
		cursor, err = decodeSilenceEditorPreviewCursor(req.Cursor)
		if err != nil {
			logSilencePreviewRejection(r.Context(), req.Target, "invalid_cursor", slog.LevelDebug)
			badRequestJSON(w, "invalid cursor")
			return
		}
		if cursor.Fingerprint != fingerprint {
			logSilencePreviewRejection(r.Context(), req.Target, "cursor_mismatch", slog.LevelDebug)
			badRequestJSON(w, "cursor does not match current target/matchers")
			return
		}
	}

	requestedSnapshot := strings.TrimSpace(req.Snapshot)
	if cursor != nil {
		requestedSnapshot = cursor.Snapshot
	}
	var snapshot *silenceEditorPreviewSnapshot
	if requestedSnapshot != "" {
		var found bool
		snapshot, found = getSilenceEditorPreviewSnapshotContext(r.Context(), requestedSnapshot, fingerprint)
		if !found {
			runtimeLog.DebugContext(r.Context(), "silence_preview", "preview_snapshot_expired", "Silence preview snapshot must be rebuilt", slog.String("target", req.Target), slog.String("snapshot_id", requestedSnapshot))
			silenceEditorErrorJSON(w, http.StatusConflict, "preview snapshot expired; restart preview", false)
			return
		}
	} else {
		candidates, excludedNoRaw, collectErr := collectSilenceEditorPreviewCandidatesContext(r.Context(), req.Target)
		if collectErr != nil {
			runtimeLog.ErrorContext(r.Context(), "silence_preview", "preview_failed", "Failed to collect silence preview candidates", slog.String("target", req.Target), slog.String("stage", "collect"), slog.Any("error", collectErr), slog.Duration("duration", time.Since(startedAt)))
			silenceEditorErrorJSON(w, http.StatusInternalServerError, "failed to collect preview alerts", false)
			return
		}
		matched, tooLarge := matchSilenceEditorPreviewCandidates(compiled, candidates)
		if tooLarge {
			logSilencePreviewRejection(r.Context(), req.Target, "match_limit_exceeded", slog.LevelWarn)
			silenceEditorErrorJSON(w, http.StatusUnprocessableEntity, fmt.Sprintf("preview matches more than %d alerts; narrow matchers", silenceEditorPreviewSnapshotMaxRecords), false)
			return
		}
		sortSilenceEditorPreviewAlerts(matched)
		snapshot = newSilenceEditorPreviewSnapshot(fingerprint, matched, excludedNoRaw)
		if !putSilenceEditorPreviewSnapshotContext(r.Context(), snapshot) {
			logSilencePreviewRejection(r.Context(), req.Target, "snapshot_capacity_exceeded", slog.LevelWarn)
			silenceEditorErrorJSON(w, http.StatusUnprocessableEntity, "preview exceeds snapshot capacity; narrow matchers", false)
			return
		}
		runtimeLog.DebugContext(r.Context(), "silence_preview", "preview_built", "Silence preview snapshot built", slog.String("target", req.Target), slog.Int("candidate_count", len(candidates)), slog.Int("matched_count", len(matched)), slog.Int("excluded_no_raw_labels", excludedNoRaw), slog.String("snapshot_id", snapshot.ID), slog.Duration("duration", time.Since(startedAt)))
	}
	matched := snapshot.Alerts

	total := len(matched)
	start := 0
	if cursor != nil {
		start = findSilenceEditorPageStart(matched, cursor)
		if start > total {
			start = total
		}
	}
	end := start + limit
	if end > total {
		end = total
	}
	page := matched[start:end]

	resp := silenceEditorPreviewResponse{
		Status:              "success",
		Target:              req.Target,
		Total:               total,
		Returned:            len(page),
		Limit:               limit,
		Snapshot:            snapshot.ID,
		ExcludedNoRawLabels: snapshot.ExcludedNoRawLabels,
		Alerts:              page,
	}
	if len(page) > 0 && end < total {
		last := page[len(page)-1]
		resp.NextCursor = encodeSilenceEditorPreviewCursor(silenceEditorPreviewCursor{
			Version:     1,
			Fingerprint: fingerprint,
			Snapshot:    snapshot.ID,
			Rank:        previewStateRank(last.State, last.IsLive),
			LastSeenAt:  last.LastSeenAt,
			ID:          last.ID,
		})
	}

	data, _ := marshalJSON(resp)
	mimeJSON(w)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
	runtimeLog.DebugContext(r.Context(), "silence_preview", "preview_page_served", "Silence preview page served", slog.String("target", req.Target), slog.String("snapshot_id", snapshot.ID), slog.Int("returned", len(page)), slog.Int("total", total), slog.Duration("duration", time.Since(startedAt)))
}

func logSilencePreviewRejection(ctx context.Context, target, reason string, level slog.Level) {
	runtimeLog.Event(ctx, level, "silence_preview", "preview_rejected", "Silence preview request rejected", slog.String("target", target), slog.String("reason_code", reason))
}

func matchSilenceEditorPreviewCandidates(matchers []compiledSilenceEditorMatcher, candidates []silenceEditorPreviewCandidate) ([]silenceEditorPreviewAlert, bool) {
	matched := make([]silenceEditorPreviewAlert, 0)
	for _, candidate := range candidates {
		ok, matchedNames, missingNames := silenceEditorMatches(matchers, candidate.RawLabels)
		if !ok {
			continue
		}
		if len(matched) >= silenceEditorPreviewSnapshotMaxRecords {
			return nil, true
		}
		matchedLabels := make([]models.OrderedLabel, 0, len(matchedNames))
		for _, name := range matchedNames {
			if value, ok := candidate.RawLabels[name]; ok {
				matchedLabels = append(matchedLabels, models.OrderedLabel{Name: name, Value: value})
			}
		}
		matched = append(matched, silenceEditorPreviewAlert{
			ID: candidate.AlertID, State: candidate.State.String(), RawState: candidate.RawState.String(),
			IsLive: candidate.IsLive, IsSuppressed: candidate.IsSuppressed, IsResolved: candidate.IsResolved,
			StartsAt: candidate.StartsAt, LastSeenAt: candidate.LastSeenAt, Labels: candidate.Labels,
			MatchedLabels: matchedLabels, MatchedLabelNames: matchedNames, MissingLabelNames: missingNames,
		})
	}
	return matched, false
}

func silenceEditorPreviewFingerprint(target string, matchers []compiledSilenceEditorMatcher) string {
	type fingerprintMatcher struct {
		Name    string `json:"name"`
		Value   string `json:"value"`
		IsRegex bool   `json:"isRegex"`
		IsEqual bool   `json:"isEqual"`
	}
	payload := struct {
		Target   string               `json:"target"`
		Matchers []fingerprintMatcher `json:"matchers"`
	}{Target: strings.TrimSpace(target), Matchers: make([]fingerprintMatcher, 0, len(matchers))}
	for _, matcher := range matchers {
		payload.Matchers = append(payload.Matchers, fingerprintMatcher{
			Name: matcher.matcher.Name, Value: matcher.matcher.Value,
			IsRegex: matcher.matcher.IsRegex, IsEqual: matcher.matcher.IsEqual,
		})
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal silence editor preview fingerprint: %v", err))
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func encodeSilenceEditorPreviewCursor(cursor silenceEditorPreviewCursor) string {
	raw, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeSilenceEditorPreviewCursor(raw string) (*silenceEditorPreviewCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var cursor silenceEditorPreviewCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return nil, err
	}
	if cursor.Version != 1 || strings.TrimSpace(cursor.Fingerprint) == "" || strings.TrimSpace(cursor.Snapshot) == "" || strings.TrimSpace(cursor.ID) == "" {
		return nil, errors.New("invalid cursor")
	}
	return &cursor, nil
}

func comparePreviewAlertToCursor(alert silenceEditorPreviewAlert, cursor *silenceEditorPreviewCursor) int {
	if cursor == nil {
		return 1
	}
	rank := previewStateRank(alert.State, alert.IsLive)
	if rank < cursor.Rank {
		return -1
	}
	if rank > cursor.Rank {
		return 1
	}
	if alert.LastSeenAt != nil && cursor.LastSeenAt == nil {
		return -1
	}
	if alert.LastSeenAt == nil && cursor.LastSeenAt != nil {
		return 1
	}
	if alert.LastSeenAt != nil && cursor.LastSeenAt != nil {
		if *alert.LastSeenAt > *cursor.LastSeenAt {
			return -1
		}
		if *alert.LastSeenAt < *cursor.LastSeenAt {
			return 1
		}
	}
	if alert.ID < cursor.ID {
		return -1
	}
	if alert.ID > cursor.ID {
		return 1
	}
	return 0
}

func findSilenceEditorPageStart(alerts []silenceEditorPreviewAlert, cursor *silenceEditorPreviewCursor) int {
	for i, alert := range alerts {
		if comparePreviewAlertToCursor(alert, cursor) > 0 {
			return i
		}
	}
	return len(alerts)
}

func sortSilenceEditorPreviewAlerts(alerts []silenceEditorPreviewAlert) {
	sort.SliceStable(alerts, func(i, j int) bool {
		ri := previewStateRank(alerts[i].State, alerts[i].IsLive)
		rj := previewStateRank(alerts[j].State, alerts[j].IsLive)
		if ri != rj {
			return ri < rj
		}
		if alerts[i].LastSeenAt != nil && alerts[j].LastSeenAt != nil && *alerts[i].LastSeenAt != *alerts[j].LastSeenAt {
			return *alerts[i].LastSeenAt > *alerts[j].LastSeenAt
		}
		if alerts[i].LastSeenAt != nil && alerts[j].LastSeenAt == nil {
			return true
		}
		if alerts[i].LastSeenAt == nil && alerts[j].LastSeenAt != nil {
			return false
		}
		return alerts[i].ID < alerts[j].ID
	})
}

func silenceEditorLabelNames(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	noCache(w)
	target := strings.TrimSpace(r.URL.Query().Get("target"))
	if target == "" {
		runtimeLog.DebugContext(r.Context(), "silence_preview", "autocomplete_rejected", "Silence editor autocomplete request rejected", slog.String("kind", "labels"), slog.String("reason_code", "missing_target"))
		badRequestJSON(w, "target is required")
		return
	}
	term := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("term")))
	items, total, liveOverlayCount, err := collectSilenceEditorLabelNames(r.Context(), target, term)
	if err != nil {
		runtimeLog.ErrorContext(r.Context(), "silence_preview", "autocomplete_failed", "Failed to collect silence editor label names", slog.String("target", target), slog.String("kind", "labels"), slog.String("stage", "collect"), slog.Any("error", err), slog.Duration("duration", time.Since(startedAt)))
		silenceEditorErrorJSON(w, http.StatusInternalServerError, "failed to collect labels", false)
		return
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Name < items[j].Name
	})

	resp := silenceEditorLabelsResponse{
		Status:    "success",
		Target:    target,
		Total:     total,
		Truncated: total > silenceEditorLabelLimit,
		Labels:    items,
	}
	if len(resp.Labels) > silenceEditorLabelLimit {
		resp.Labels = resp.Labels[:silenceEditorLabelLimit]
	}

	data, _ := marshalJSON(resp)
	mimeJSON(w)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
	runtimeLog.DebugContext(r.Context(), "silence_preview", "autocomplete_completed", "Silence editor label autocomplete completed", slog.String("target", target), slog.String("kind", "labels"), slog.Int("result_count", len(resp.Labels)), slog.Int("exact_total", total), slog.Int("live_overlay_count", liveOverlayCount), slog.Duration("duration", time.Since(startedAt)))
}

func silenceEditorLabelValues(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	noCache(w)
	target := strings.TrimSpace(r.URL.Query().Get("target"))
	if target == "" {
		runtimeLog.DebugContext(r.Context(), "silence_preview", "autocomplete_rejected", "Silence editor autocomplete request rejected", slog.String("kind", "values"), slog.String("reason_code", "missing_target"))
		badRequestJSON(w, "target is required")
		return
	}
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		runtimeLog.DebugContext(r.Context(), "silence_preview", "autocomplete_rejected", "Silence editor autocomplete request rejected", slog.String("target", target), slog.String("kind", "values"), slog.String("reason_code", "missing_label_name"))
		badRequestJSON(w, "name is required")
		return
	}
	term := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("term")))
	items, total, liveOverlayCount, err := collectSilenceEditorLabelValues(r.Context(), target, name, term)
	if err != nil {
		runtimeLog.ErrorContext(r.Context(), "silence_preview", "autocomplete_failed", "Failed to collect silence editor label values", slog.String("target", target), slog.String("kind", "values"), slog.String("stage", "collect"), slog.Any("error", err), slog.Duration("duration", time.Since(startedAt)))
		silenceEditorErrorJSON(w, http.StatusInternalServerError, "failed to collect values", false)
		return
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Hits != items[j].Hits {
			return items[i].Hits > items[j].Hits
		}
		return items[i].Value < items[j].Value
	})

	resp := silenceEditorValuesResponse{
		Status:    "success",
		Target:    target,
		Name:      name,
		Total:     total,
		Truncated: total > silenceEditorValueLimit,
		Values:    items,
	}
	if len(resp.Values) > silenceEditorValueLimit {
		resp.Values = resp.Values[:silenceEditorValueLimit]
	}

	data, _ := marshalJSON(resp)
	mimeJSON(w)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
	runtimeLog.DebugContext(r.Context(), "silence_preview", "autocomplete_completed", "Silence editor value autocomplete completed", slog.String("target", target), slog.String("kind", "values"), slog.Int("result_count", len(resp.Values)), slog.Int("exact_total", total), slog.Int("live_overlay_count", liveOverlayCount), slog.Duration("duration", time.Since(startedAt)))
}

func collectSilenceEditorLabelNames(ctx context.Context, target, term string) ([]silenceEditorLabelItem, int, int, error) {
	live := collectSilenceEditorLiveRawLabelSets(ctx, target)
	if lifecycleStore != nil {
		stats, total, err := lifecycleStore.ListTargetLabelNamesWithLive(target, term, silenceEditorLabelLimit+1, live)
		if err != nil {
			return nil, 0, len(live), err
		}
		items := make([]silenceEditorLabelItem, 0, len(stats))
		for _, stat := range stats {
			items = append(items, silenceEditorLabelItem{Name: stat.Name, Values: stat.Values})
		}
		return items, total, len(live), nil
	}
	items := collectSilenceEditorLiveLabelNames(live, term)
	return items, len(items), len(live), nil
}

func collectSilenceEditorLabelValues(ctx context.Context, target, name, term string) ([]silenceEditorValueItem, int, int, error) {
	live := collectSilenceEditorLiveRawLabelSets(ctx, target)
	if lifecycleStore != nil {
		stats, total, err := lifecycleStore.ListTargetLabelValuesWithLive(target, name, term, silenceEditorValueLimit+1, live)
		if err != nil {
			return nil, 0, len(live), err
		}
		items := make([]silenceEditorValueItem, 0, len(stats))
		for _, stat := range stats {
			items = append(items, silenceEditorValueItem{Value: stat.Value, Hits: stat.Hits})
		}
		return items, total, len(live), nil
	}
	items := collectSilenceEditorLiveLabelValues(live, name, term)
	return items, len(items), len(live), nil
}

func collectSilenceEditorLiveLabelNames(live []alertlifecycle.TargetLabelOverlay, term string) []silenceEditorLabelItem {
	result := map[string]map[string]struct{}{}
	for _, alert := range live {
		for k, v := range alert.Labels {
			if term != "" && !strings.HasPrefix(strings.ToLower(k), term) {
				continue
			}
			if _, ok := result[k]; !ok {
				result[k] = map[string]struct{}{}
			}
			result[k][v] = struct{}{}
		}
	}
	items := make([]silenceEditorLabelItem, 0, len(result))
	for name, vals := range result {
		items = append(items, silenceEditorLabelItem{Name: name, Values: len(vals)})
	}
	return items
}

func collectSilenceEditorLiveLabelValues(live []alertlifecycle.TargetLabelOverlay, name, term string) []silenceEditorValueItem {
	hits := map[string]int{}
	for _, alert := range live {
		value, ok := alert.Labels[name]
		if !ok {
			continue
		}
		if term != "" && !strings.HasPrefix(strings.ToLower(value), term) {
			continue
		}
		hits[value]++
	}
	items := make([]silenceEditorValueItem, 0, len(hits))
	for value, count := range hits {
		items = append(items, silenceEditorValueItem{Value: value, Hits: count})
	}
	return items
}

func collectSilenceEditorLiveRawLabelSets(ctx context.Context, target string) []alertlifecycle.TargetLabelOverlay {
	rawLabels := buildRawMatchLabelsIndexContext(ctx)
	seen := map[string]struct{}{}
	result := []alertlifecycle.TargetLabelOverlay{}
	for _, group := range alertmanager.DedupAlerts() {
		for _, alert := range group.Alerts {
			if !alertBelongsToTarget(alert, target) {
				continue
			}
			alertID := alert.LabelsFingerprint()
			if _, ok := seen[alertID]; ok {
				continue
			}
			seen[alertID] = struct{}{}
			raw := rawLabels[alertID]
			if raw == nil {
				continue
			}
			labels := map[string]string{}
			if err := json.Unmarshal([]byte(*raw), &labels); err != nil {
				continue
			}
			fingerprint := sha256.Sum256([]byte(*raw))
			result = append(result, alertlifecycle.TargetLabelOverlay{
				AlertID: alertID, Labels: labels, MatchLabelsFingerprint: hex.EncodeToString(fingerprint[:]),
			})
		}
	}
	return result
}

func compileSilenceEditorMatchers(src []silenceEditorMatcher) ([]compiledSilenceEditorMatcher, error) {
	if len(src) > silenceEditorMatcherLimit {
		return nil, fmt.Errorf("matcher count exceeds limit %d", silenceEditorMatcherLimit)
	}
	result := make([]compiledSilenceEditorMatcher, 0, len(src))
	for _, matcher := range src {
		name := strings.TrimSpace(matcher.Name)
		if name == "" {
			return nil, errors.New("matcher name is required")
		}
		if !silenceEditorMatcherNamePattern.MatchString(name) {
			return nil, fmt.Errorf("invalid matcher name %q", name)
		}
		compiled := compiledSilenceEditorMatcher{matcher: models.SilenceMatcher{
			Name:    name,
			Value:   matcher.Value,
			IsRegex: matcher.IsRegex,
			IsEqual: matcher.IsEqual,
		}}
		if matcher.IsRegex {
			re, err := regex.CompileAnchored(matcher.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid regex for %s: %w", name, err)
			}
			compiled.regex = re
		}
		result = append(result, compiled)
	}
	return result, nil
}

func silenceEditorMatcherSetIsSafe(matchers []compiledSilenceEditorMatcher) bool {
	if len(matchers) == 0 {
		return false
	}
	for _, matcher := range matchers {
		if !silenceEditorMatcherMatches(matcher, "") {
			return true
		}
	}
	return false
}

func silenceEditorMatcherMatches(matcher compiledSilenceEditorMatcher, value string) bool {
	if matcher.matcher.IsRegex {
		re := matcher.regex
		if re == nil {
			return false
		}
		matched := re.MatchString(value)
		if matcher.matcher.IsEqual {
			return matched
		}
		return !matched
	}
	matched := value == matcher.matcher.Value
	if matcher.matcher.IsEqual {
		return matched
	}
	return !matched
}

func silenceEditorMatches(matchers []compiledSilenceEditorMatcher, labels map[string]string) (bool, []string, []string) {
	matched := map[string]struct{}{}
	missing := map[string]struct{}{}
	for _, matcher := range matchers {
		value, ok := labels[matcher.matcher.Name]
		if !ok {
			value = ""
		}
		if !silenceEditorMatcherMatches(matcher, value) {
			return false, nil, nil
		}
		matched[matcher.matcher.Name] = struct{}{}
		if !ok {
			missing[matcher.matcher.Name] = struct{}{}
		}
	}
	matchedNames := make([]string, 0, len(matched))
	for name := range matched {
		matchedNames = append(matchedNames, name)
	}
	sort.Strings(matchedNames)
	missingNames := make([]string, 0, len(missing))
	for name := range missing {
		missingNames = append(missingNames, name)
	}
	sort.Strings(missingNames)
	return true, matchedNames, missingNames
}

func previewStateRank(state string, isLive bool) int {
	switch state {
	case models.AlertStateActive.String():
		return 0
	case models.AlertStateUnprocessed.String():
		return 1
	case models.AlertStateSuppressed.String():
		if isLive {
			return 2
		}
		return 4
	case models.AlertStateInactive.String():
		return 3
	case models.AlertStateResolved.String():
		return 5
	default:
		return 6
	}
}

func collectSilenceEditorPreviewCandidates(target string) ([]silenceEditorPreviewCandidate, int, error) {
	return collectSilenceEditorPreviewCandidatesContext(context.Background(), target)
}

func collectSilenceEditorPreviewCandidatesContext(ctx context.Context, target string) ([]silenceEditorPreviewCandidate, int, error) {
	rawLiveLabels := buildRawMatchLabelsIndexContext(ctx)
	liveGroups := alertmanager.DedupAlerts()
	seen := map[string]struct{}{}
	result := []silenceEditorPreviewCandidate{}
	excludedNoRawLabels := 0
	liveByID := map[string]silenceEditorPreviewCandidate{}

	for _, group := range liveGroups {
		for _, alert := range group.Alerts {
			if !alertBelongsToTarget(alert, target) {
				continue
			}
			alertID := alert.LabelsFingerprint()
			seen[alertID] = struct{}{}

			state, rawState, isLive, isSuppressed, isResolved, startsAt, ok := projectLiveAlertForTarget(alert, target)
			if !ok {
				continue
			}

			candidate := silenceEditorPreviewCandidate{
				AlertID:      alertID,
				State:        state,
				RawState:     rawState,
				IsLive:       isLive,
				IsSuppressed: isSuppressed,
				IsResolved:   isResolved,
				StartsAt:     startsAt.UTC().Format(time.RFC3339Nano),
				LastSeenAt:   nil,
				Labels:       models.LabelsToOrderedLabels(alert.Labels),
			}
			if current, exists := liveByID[alertID]; exists {
				liveByID[alertID] = mergeSilenceEditorLivePreviewCandidates(current, candidate)
			} else {
				liveByID[alertID] = candidate
			}
		}
	}
	for alertID, candidate := range liveByID {
		rawJSON := rawLiveLabels[alertID]
		if rawJSON == nil {
			excludedNoRawLabels++
			continue
		}
		raw := map[string]string{}
		if err := json.Unmarshal([]byte(*rawJSON), &raw); err != nil {
			excludedNoRawLabels++
			continue
		}
		candidate.RawLabels = raw
		result = append(result, candidate)
	}

	if lifecycleStore == nil {
		return result, excludedNoRawLabels, nil
	}

	targetRows, err := lifecycleStore.ListTargetAlertsForPreview(target)
	if err != nil {
		return nil, 0, err
	}
	for _, row := range targetRows {
		if _, ok := seen[row.AlertID]; ok {
			continue
		}
		seen[row.AlertID] = struct{}{}
		if row.MatchLabelsJSON == nil {
			excludedNoRawLabels++
			continue
		}
		raw := map[string]string{}
		if decodeErr := json.Unmarshal([]byte(*row.MatchLabelsJSON), &raw); decodeErr != nil {
			excludedNoRawLabels++
			continue
		}
		labelsMap := map[string]string{}
		if decodeErr := json.Unmarshal([]byte(row.LabelsJSON), &labelsMap); decodeErr != nil {
			continue
		}
		var lastSeen *string
		if row.LastSeenAt != nil {
			v := row.LastSeenAt.UTC().Format(time.RFC3339Nano)
			lastSeen = &v
		}
		result = append(result, silenceEditorPreviewCandidate{
			AlertID:      row.AlertID,
			State:        models.ParseAlertState(string(row.State)),
			RawState:     models.ParseAlertState(string(row.RawState)),
			IsLive:       row.IsLive,
			IsSuppressed: row.IsSuppressed,
			IsResolved:   row.IsResolved,
			StartsAt:     row.StartsAt.UTC().Format(time.RFC3339Nano),
			LastSeenAt:   lastSeen,
			Labels:       models.LabelsToOrderedLabels(models.LabelsFromMap(labelsMap)),
			RawLabels:    raw,
		})
	}

	return result, excludedNoRawLabels, nil
}

func mergeSilenceEditorLivePreviewCandidates(left, right silenceEditorPreviewCandidate) silenceEditorPreviewCandidate {
	merged := left
	stateRank := func(state models.AlertState) int {
		switch state {
		case models.AlertStateActive:
			return 0
		case models.AlertStateSuppressed:
			return 1
		case models.AlertStateUnprocessed, models.AlertStateInactive, models.AlertStateResolved:
			return 2
		}
		return 2
	}
	if stateRank(right.State) < stateRank(left.State) {
		merged.State = right.State
		merged.RawState = right.RawState
		merged.IsLive = right.IsLive
		merged.IsSuppressed = right.IsSuppressed
		merged.IsResolved = right.IsResolved
	}
	leftStarts, leftErr := time.Parse(time.RFC3339Nano, left.StartsAt)
	rightStarts, rightErr := time.Parse(time.RFC3339Nano, right.StartsAt)
	if rightErr == nil && (leftErr != nil || rightStarts.Before(leftStarts)) {
		merged.StartsAt = right.StartsAt
	}
	return merged
}

func projectLiveAlertForTarget(alert models.Alert, target string) (models.AlertState, models.AlertState, bool, bool, bool, time.Time, bool) {
	targetInstances := make([]models.AlertmanagerInstance, 0, len(alert.Alertmanager))
	for _, am := range alert.Alertmanager {
		if am.Cluster == target {
			targetInstances = append(targetInstances, am)
		}
	}
	if len(targetInstances) == 0 {
		return models.AlertStateUnprocessed, models.AlertStateUnprocessed, false, false, false, time.Time{}, false
	}

	states := make([]models.AlertState, 0, len(targetInstances))
	startsAt := targetInstances[0].StartsAt
	for _, am := range targetInstances {
		states = append(states, am.State)
		if am.StartsAt.Before(startsAt) {
			startsAt = am.StartsAt
		}
	}

	state := models.AlertStateUnprocessed
	if slices.Contains(states, models.AlertStateActive) {
		state = models.AlertStateActive
	} else if slices.Contains(states, models.AlertStateSuppressed) {
		state = models.AlertStateSuppressed
	}

	rawState := state
	isLive := true
	isSuppressed := rawState == models.AlertStateSuppressed
	isResolved := false

	return state, rawState, isLive, isSuppressed, isResolved, startsAt, true
}

func alertBelongsToTarget(alert models.Alert, target string) bool {
	for _, am := range alert.Alertmanager {
		if am.Cluster == target {
			return true
		}
	}
	return false
}

func currentBelongsToTarget(current alertlifecycle.AlertCurrent, target string, memberToCluster map[string]string) bool {
	if strings.TrimSpace(current.RelevantSources) != "" {
		var refs []alertlifecycle.SourceRef
		if err := json.Unmarshal([]byte(current.RelevantSources), &refs); err == nil && refs != nil {
			for _, ref := range refs {
				if ref.Cluster == target {
					return true
				}
			}
			// A valid source snapshot is the historical source of truth. Falling back
			// to member names would reinterpret the alert through today's topology.
			return false
		}
	}

	if strings.TrimSpace(current.RelevantUpstreams) != "" {
		var members []string
		if err := json.Unmarshal([]byte(current.RelevantUpstreams), &members); err == nil {
			for _, member := range members {
				if memberToCluster[member] == target {
					return true
				}
			}
		}
	}

	return false
}

func silenceEditorSubmit(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	noCache(w)
	if maxPayloadBytes := config.Config.SilenceEditor.MaxPayloadBytes; maxPayloadBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, maxPayloadBytes)
	}
	correlationID, correlationErr := newSilenceEditorCorrelationID()
	if correlationErr != nil {
		runtimeLog.ErrorContext(r.Context(), "silence_editor", "submission_initialization_failed", "Failed to initialize silence editor submission correlation", slog.Any("error", correlationErr))
		silenceEditorErrorJSON(w, http.StatusInternalServerError, "failed to initialize submission correlation", false)
		return
	}
	logValidationFailure := func(ctx context.Context, target, operation, reason string) {
		runtimeLog.DebugContext(ctx, "silence_editor", "submission_validation_failed", "Silence editor submission validation failed", slog.String("correlation_id", correlationID), slog.String("operation", operation), slog.String("target", target), slog.String("reason_code", reason))
	}

	var req silenceEditorSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logValidationFailure(r.Context(), "", "", "invalid_request_body")
		badRequestJSON(w, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	req.Target = strings.TrimSpace(req.Target)
	req.Operation = strings.TrimSpace(req.Operation)
	req.OriginalSilenceID = strings.TrimSpace(req.OriginalSilenceID)
	req.RouteMember = strings.TrimSpace(req.RouteMember)
	req.RevisionToken = strings.TrimSpace(req.RevisionToken)
	if req.Target == "" {
		logValidationFailure(r.Context(), req.Target, req.Operation, "missing_target")
		badRequestJSON(w, "target is required")
		return
	}
	if strings.TrimSpace(req.Payload.CreatedBy) == "" {
		logValidationFailure(r.Context(), req.Target, req.Operation, "missing_author")
		badRequestJSON(w, "author is required")
		return
	}
	if strings.TrimSpace(req.Payload.Comment) == "" {
		logValidationFailure(r.Context(), req.Target, req.Operation, "missing_comment")
		badRequestJSON(w, "comment is required")
		return
	}
	if req.Payload.StartsAt.IsZero() {
		logValidationFailure(r.Context(), req.Target, req.Operation, "missing_starts_at")
		badRequestJSON(w, "startsAt is required")
		return
	}
	if req.Payload.EndsAt.IsZero() {
		logValidationFailure(r.Context(), req.Target, req.Operation, "missing_ends_at")
		badRequestJSON(w, "endsAt is required")
		return
	}
	if !req.Payload.EndsAt.After(req.Payload.StartsAt) {
		logValidationFailure(r.Context(), req.Target, req.Operation, "invalid_time_range")
		badRequestJSON(w, "endsAt must be after startsAt")
		return
	}
	req.Payload.CreatedBy = strings.TrimSpace(req.Payload.CreatedBy)
	if req.Payload.ID != "" {
		req.Payload.ID = strings.TrimSpace(req.Payload.ID)
	}
	req.Payload.StartsAt = req.Payload.StartsAt.UTC()
	req.Payload.EndsAt = req.Payload.EndsAt.UTC()
	if len(req.Payload.Matchers) == 0 {
		logValidationFailure(r.Context(), req.Target, req.Operation, "missing_matchers")
		badRequestJSON(w, "at least one matcher is required")
		return
	}

	compiledMatchers := make([]silenceEditorMatcher, 0, len(req.Payload.Matchers))
	for index := range req.Payload.Matchers {
		req.Payload.Matchers[index].Name = strings.TrimSpace(req.Payload.Matchers[index].Name)
		m := req.Payload.Matchers[index]
		compiledMatchers = append(compiledMatchers, silenceEditorMatcher{
			Name:    m.Name,
			Value:   m.Value,
			IsRegex: m.IsRegex,
			IsEqual: m.IsEqual,
		})
	}
	compiled, err := compileSilenceEditorMatchers(compiledMatchers)
	if err != nil {
		logValidationFailure(r.Context(), req.Target, req.Operation, "invalid_matcher")
		badRequestJSON(w, err.Error())
		return
	}
	if !silenceEditorMatcherSetIsSafe(compiled) {
		logValidationFailure(r.Context(), req.Target, req.Operation, "unsafe_matcher_set")
		badRequestJSON(w, "at least one matcher must not match the empty string")
		return
	}
	targetMembers := writableMembersByTarget(req.Target)
	if len(targetMembers) == 0 {
		runtimeLog.WarnContext(r.Context(), "silence_editor", "submission_rejected", "Silence editor submission has no writable member", slog.String("correlation_id", correlationID), slog.String("operation", req.Operation), slog.String("target", req.Target), slog.String("reason_code", "no_writable_member"))
		badRequestJSON(w, "no writable Alertmanager members for target "+req.Target)
		return
	}
	member := targetMembers[0]
	if validationErr := validateSilenceEditorMutationIdentity(r.Context(), member, req.RouteMember, req.Operation, req.OriginalSilenceID, req.RevisionToken, req.Payload, time.Now().UTC()); validationErr != nil {
		var readErr *silenceEditorFreshReadError
		if errors.As(validationErr, &readErr) && readErr.StatusCode != http.StatusNotFound {
			runtimeLog.ErrorContext(r.Context(), "silence_editor", "submission_verification_failed", "Silence editor submission could not verify the original rule", slog.String("correlation_id", correlationID), slog.String("operation", req.Operation), slog.String("target", req.Target), slog.String("member", member.Name), slog.Int("http_status", readErr.StatusCode), slog.Any("error", sanitizeRemoteLogError(validationErr)), slog.Duration("duration", time.Since(startedAt)))
			silenceEditorErrorJSON(w, http.StatusBadGateway, validationErr.Error(), false)
			return
		}
		runtimeLog.WarnContext(r.Context(), "silence_editor", "submission_conflict", "Silence editor submission conflicts with the current rule", slog.String("correlation_id", correlationID), slog.String("operation", req.Operation), slog.String("target", req.Target), slog.String("member", member.Name), slog.String("silence_id", req.OriginalSilenceID), slog.String("reason_code", silenceEditorConflictReason(validationErr)))
		silenceEditorRequiresReopenJSON(w, validationErr.Error())
		return
	}

	body, _ := marshalJSON(req.Payload)
	operator := getUserFromContext(r)
	groups := getGroupsFromContext(r)
	mode := "server_routed"
	if !member.ProxyRequests {
		mode = "browser_routed"
	}
	runtimeLog.InfoContext(r.Context(), "silence_editor", "submission_received", "Silence editor submission received", slog.String("correlation_id", correlationID), slog.String("operation", req.Operation), slog.String("target", req.Target), slog.String("member", member.Name), slog.String("mode", mode), slog.String("operator", operator), slog.String("author", req.Payload.CreatedBy), slog.Int("matcher_count", len(req.Payload.Matchers)))

	aclRuleIndex, aclErr := checkSilenceEditorACL(r.Context(), member.Name, &req.Payload, groups)
	if aclErr != nil {
		runtimeLog.WarnContext(r.Context(), "silence_editor", "submission_acl_rejected", "Silence editor submission rejected by ACL", slog.String("correlation_id", correlationID), slog.String("operation", req.Operation), slog.String("target", req.Target), slog.String("member", member.Name), slog.String("operator", operator), slog.Int("acl_rule_index", aclRuleIndex), slog.Any("error", sanitizeRemoteLogError(aclErr)))
		badRequestJSON(w, aclErr.Error())
		return
	}
	if !member.ProxyRequests {
		endpoint, endpointErr := uri.JoinURL(member.InternalURI, "/api/v2/silences")
		if endpointErr != nil {
			runtimeLog.ErrorContext(r.Context(), "silence_editor", "submission_failed", "Failed to build browser-routed silence endpoint", slog.String("correlation_id", correlationID), slog.String("operation", req.Operation), slog.String("target", req.Target), slog.String("member", member.Name), slog.Any("error", endpointErr))
			silenceEditorErrorJSON(w, http.StatusInternalServerError, endpointErr.Error(), false)
			return
		}

		resp := silenceEditorSubmitResponse{
			Status:        "direct",
			Target:        req.Target,
			Mode:          "direct",
			SubmittedBy:   operator,
			CorrelationID: correlationID,
			Direct: &silenceEditorDirectSubmit{
				Endpoint:        endpoint,
				Headers:         member.HeadersForBrowser,
				CORSCredentials: member.CORSCredentials,
				Member:          member.Name,
				Payload:         req.Payload,
			},
		}
		runtimeLog.InfoContext(
			r.Context(), "silence_editor", "direct_descriptor_issued", "Silence editor issued browser-routed submit descriptor",
			slog.String("correlation_id", correlationID),
			slog.String("operation", req.Operation),
			slog.String("target", req.Target),
			slog.String("member", member.Name),
			slog.String("operator", operator),
			slog.String("mode", "browser_routed"),
			slog.Bool("server_result_observable", false),
		)
		data, _ := marshalJSON(resp)
		mimeJSON(w)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		return
	}

	runtimeLog.InfoContext(r.Context(), "silence_editor", "upstream_write_started", "Alertmanager silence write started", slog.String("correlation_id", correlationID), slog.String("operation", req.Operation), slog.String("target", req.Target), slog.String("member", member.Name), slog.String("silence_id", req.Payload.ID))
	silenceID, statusCode, err := submitSilenceToMember(r.Context(), member, body)
	if err == nil {
		runtimeLog.InfoContext(r.Context(), "silence_editor", "upstream_write_succeeded", "Alertmanager silence write succeeded", slog.String("correlation_id", correlationID), slog.String("operation", req.Operation), slog.String("target", req.Target), slog.String("member", member.Name), slog.String("silence_id", silenceID), slog.Int("http_status", statusCode), slog.Duration("duration", time.Since(startedAt)))
		resp := silenceEditorSubmitResponse{
			Status:        "success",
			Target:        req.Target,
			Mode:          "proxy",
			SilenceID:     silenceID,
			SubmittedBy:   operator,
			CorrelationID: correlationID,
		}
		data, _ := marshalJSON(resp)
		mimeJSON(w)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		return
	}

	if outcomeUnknown(statusCode, err) {
		runtimeLog.ErrorContext(r.Context(), "silence_editor", "upstream_write_outcome_unknown", "Alertmanager silence write outcome is unknown", slog.String("correlation_id", correlationID), slog.String("operation", req.Operation), slog.String("target", req.Target), slog.String("member", member.Name), slog.Int("http_status", statusCode), slog.String("failure_stage", silenceEditorWriteFailureStage(statusCode, err)), slog.Any("error", sanitizeRemoteLogError(err)), slog.Duration("duration", time.Since(startedAt)))
		silenceEditorErrorJSON(w, http.StatusBadGateway, err.Error(), true)
		return
	}
	runtimeLog.WarnContext(r.Context(), "silence_editor", "upstream_write_rejected", "Alertmanager silence write was rejected", slog.String("correlation_id", correlationID), slog.String("operation", req.Operation), slog.String("target", req.Target), slog.String("member", member.Name), slog.Int("http_status", statusCode), slog.String("failure_stage", silenceEditorWriteFailureStage(statusCode, err)), slog.Any("error", sanitizeRemoteLogError(err)), slog.Duration("duration", time.Since(startedAt)))
	silenceEditorErrorJSON(w, http.StatusBadGateway, err.Error(), false)
}

func newSilenceEditorCorrelationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("failed to generate correlation ID: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func silenceEditorStatus(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	noCache(w)
	target := strings.TrimSpace(r.URL.Query().Get("target"))
	silenceID := strings.TrimSpace(r.URL.Query().Get("id"))
	if target == "" || silenceID == "" {
		runtimeLog.DebugContext(r.Context(), "silence_editor", "status_verification_rejected", "Silence editor status verification request rejected", slog.String("target", target), slog.String("silence_id", silenceID), slog.String("reason_code", "missing_identity"))
		badRequestJSON(w, "target and id are required")
		return
	}
	members := writableMembersByTarget(target)
	if len(members) == 0 {
		runtimeLog.WarnContext(r.Context(), "silence_editor", "status_verification_failed", "No writable Alertmanager member for status verification", slog.String("target", target), slog.String("silence_id", silenceID), slog.String("reason_code", "no_writable_member"))
		badRequestJSON(w, "no writable Alertmanager members for target "+target)
		return
	}
	if !members[0].Healthy {
		runtimeLog.WarnContext(r.Context(), "silence_editor", "status_verification_failed", "Selected Alertmanager member is unavailable for status verification", slog.String("target", target), slog.String("member", members[0].Name), slog.String("silence_id", silenceID), slog.String("reason_code", "member_unavailable"))
		silenceEditorErrorJSON(w, http.StatusServiceUnavailable, "selected Alertmanager member is unavailable; retry status verification", false)
		return
	}
	runtimeLog.DebugContext(r.Context(), "silence_editor", "status_verification_started", "Silence editor status verification started", slog.String("target", target), slog.String("member", members[0].Name), slog.String("silence_id", silenceID))
	status, err := findSilenceEditorStatus(r.Context(), members[0], silenceID, time.Now().UTC())
	if err == nil {
		data, _ := marshalJSON(status)
		mimeJSON(w)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		operation := "edit"
		if status.IsExpired {
			operation = "recreate"
		}
		runtimeLog.DebugContext(r.Context(), "silence_editor", "status_verification_completed", "Silence editor status verification completed", slog.String("target", target), slog.String("member", members[0].Name), slog.String("silence_id", silenceID), slog.String("operation", operation), slog.Bool("is_expired", status.IsExpired), slog.Duration("duration", time.Since(startedAt)))
		return
	}
	var readErr *silenceEditorFreshReadError
	if errors.As(err, &readErr) && readErr.StatusCode == http.StatusNotFound {
		runtimeLog.WarnContext(r.Context(), "silence_editor", "status_verification_not_found", "Silence editor rule was not found during status verification", slog.String("target", target), slog.String("member", members[0].Name), slog.String("silence_id", silenceID), slog.Duration("duration", time.Since(startedAt)))
		http.Error(w, "silence not found", http.StatusNotFound)
		return
	}
	httpStatus := 0
	if readErr != nil {
		httpStatus = readErr.StatusCode
	}
	runtimeLog.ErrorContext(r.Context(), "silence_editor", "status_verification_failed", "Silence editor status verification failed", slog.String("target", target), slog.String("member", members[0].Name), slog.String("silence_id", silenceID), slog.Int("http_status", httpStatus), slog.Any("error", sanitizeRemoteLogError(err)), slog.Duration("duration", time.Since(startedAt)))
	silenceEditorErrorJSON(w, http.StatusBadGateway, fmt.Sprintf("failed to read silence from selected Alertmanager: %v", err), false)
}

func findSilenceEditorStatus(ctx context.Context, member writableMember, silenceID string, now time.Time) (silenceEditorStatusResponse, error) {
	silence, err := member.FetchSilenceByID(ctx, silenceID)
	if err != nil {
		return silenceEditorStatusResponse{}, err
	}
	return silenceEditorStatusResponse{
		IsExpired: !silence.EndsAt.After(now), Member: member.Name,
		RevisionToken: silenceEditorRevisionToken(silence), Silence: silence,
	}, nil
}

func validateSilenceEditorOperationAtSubmit(ctx context.Context, member writableMember, operation, originalSilenceID, revisionToken string, payload models.Silence, now time.Time) error {
	operation = strings.TrimSpace(operation)
	originalSilenceID = strings.TrimSpace(originalSilenceID)
	payloadID := strings.TrimSpace(payload.ID)
	if operation == "create" && originalSilenceID == "" && payloadID == "" {
		if strings.TrimSpace(revisionToken) != "" {
			return errors.New("create must not contain a silence revision token")
		}
		return nil
	}
	if operation != "edit" && operation != "recreate" {
		return errors.New("operation must be create, edit, or recreate")
	}
	if originalSilenceID == "" {
		return errors.New("original silence ID is required; reopen the operation")
	}
	if operation == "edit" && payloadID != originalSilenceID {
		return errors.New("edit payload ID must match the original silence ID; reopen the operation")
	}
	if operation == "recreate" && payloadID != "" {
		return errors.New("recreate payload must not contain a silence ID; reopen the operation")
	}
	if strings.TrimSpace(revisionToken) == "" {
		return errors.New("original silence revision is required; reopen the operation")
	}
	silence, err := member.FetchSilenceByID(ctx, originalSilenceID)
	if err != nil {
		return err
	}
	if silenceEditorRevisionToken(silence) != revisionToken {
		return errors.New("the silence changed after the editor was opened; close the editor and open the silence again")
	}
	isExpired := !silence.EndsAt.After(now)
	if operation == "edit" && isExpired {
		return errors.New("silence expired while editing; reopen it as a recreate operation")
	}
	if operation == "recreate" && !isExpired {
		return errors.New("silence is still active on the selected Alertmanager; reopen it as an edit operation")
	}
	return nil
}

func validateSilenceEditorMutationIdentity(ctx context.Context, member writableMember, routeMember, operation, originalSilenceID, revisionToken string, payload models.Silence, now time.Time) error {
	operation = strings.TrimSpace(operation)
	routeMember = strings.TrimSpace(routeMember)
	if operation == "edit" || operation == "recreate" {
		if routeMember == "" {
			return errors.New("selected Alertmanager member is required; reopen the operation")
		}
		if routeMember != member.Name {
			return errors.New("selected Alertmanager member changed; reopen the silence before submitting")
		}
		if !member.Healthy {
			return errors.New("selected Alertmanager member is unavailable; reopen the operation")
		}
	}
	return validateSilenceEditorOperationAtSubmit(ctx, member, operation, originalSilenceID, revisionToken, payload, now)
}

type silenceEditorCanonicalMatcher struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"isRegex"`
	IsEqual bool   `json:"isEqual"`
}

type silenceEditorCanonicalAnnotation struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func silenceEditorRevisionToken(silence models.Silence) string {
	matchers := make([]silenceEditorCanonicalMatcher, 0, len(silence.Matchers))
	for _, matcher := range silence.Matchers {
		matchers = append(matchers, silenceEditorCanonicalMatcher{matcher.Name, matcher.Value, matcher.IsRegex, matcher.IsEqual})
	}
	sort.Slice(matchers, func(i, j int) bool {
		a, b := matchers[i], matchers[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.Value != b.Value {
			return a.Value < b.Value
		}
		if a.IsRegex != b.IsRegex {
			return !a.IsRegex
		}
		return !a.IsEqual && b.IsEqual
	})
	annotations := make([]silenceEditorCanonicalAnnotation, 0, len(silence.Annotations))
	for _, annotation := range silence.Annotations {
		annotations = append(annotations, silenceEditorCanonicalAnnotation{annotation.Name, annotation.Value})
	}
	sort.Slice(annotations, func(i, j int) bool {
		if annotations[i].Name != annotations[j].Name {
			return annotations[i].Name < annotations[j].Name
		}
		return annotations[i].Value < annotations[j].Value
	})
	var updatedAt *string
	if silence.UpdatedAt != nil {
		formatted := silence.UpdatedAt.UTC().Format(time.RFC3339Nano)
		updatedAt = &formatted
	}
	canonical := struct {
		ID          string                             `json:"id"`
		Matchers    []silenceEditorCanonicalMatcher    `json:"matchers"`
		StartsAt    string                             `json:"startsAt"`
		EndsAt      string                             `json:"endsAt"`
		CreatedAt   string                             `json:"createdAt"`
		UpdatedAt   *string                            `json:"updatedAt"`
		CreatedBy   string                             `json:"createdBy"`
		Comment     string                             `json:"comment"`
		Annotations []silenceEditorCanonicalAnnotation `json:"annotations"`
	}{
		ID: silence.ID, Matchers: matchers,
		StartsAt:  silence.StartsAt.UTC().Format(time.RFC3339Nano),
		EndsAt:    silence.EndsAt.UTC().Format(time.RFC3339Nano),
		CreatedAt: silence.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: updatedAt,
		CreatedBy: silence.CreatedBy, Comment: silence.Comment, Annotations: annotations,
	}
	raw, _ := json.Marshal(canonical)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

func checkSilenceEditorACL(ctx context.Context, memberName string, silence *models.Silence, groups []string) (int, error) {
	for i, acl := range silenceACLs {
		isAllowed, err := acl.isAllowed(memberName, silence, groups)
		runtimeLog.DebugContext(ctx, "silence_editor", "acl_rule_checked", "Silence editor ACL rule checked", slog.Int("acl_rule_index", i), slog.Bool("allowed", isAllowed), slog.Bool("evaluation_error", err != nil))
		if err != nil {
			return i, err
		}
		if isAllowed {
			break
		}
	}
	return -1, nil
}

type writableMember struct {
	Name              string
	URI               string
	InternalURI       string
	ProxyRequests     bool
	CORSCredentials   string
	HeadersForBrowser map[string]string
	Healthy           bool
	HTTPHeaders       map[string]string
	HTTPTransport     http.RoundTripper
	RequestTimeout    time.Duration
	FetchSilenceByID  func(context.Context, string) (models.Silence, error)
}

func writableMembersByTarget(target string) []writableMember {
	members := make([]writableMember, 0)
	for _, am := range alertmanager.GetAlertmanagers() {
		if am.ReadOnly {
			continue
		}
		if am.Cluster != target {
			continue
		}
		headersForBrowser := map[string]string{}
		if !am.ProxyRequests {
			maps.Copy(headersForBrowser, uri.HeadersForBasicAuth(am.URI))
			maps.Copy(headersForBrowser, am.HTTPHeaders)
		}
		member := writableMember{
			Name:              am.Name,
			URI:               am.URI,
			InternalURI:       am.InternalURI(),
			ProxyRequests:     am.ProxyRequests,
			CORSCredentials:   am.CORSCredentials,
			HeadersForBrowser: headersForBrowser,
			Healthy:           am.IsHealthy(),
			HTTPHeaders:       am.HTTPHeaders,
			HTTPTransport:     am.HTTPTransport,
			RequestTimeout:    am.RequestTimeout,
		}
		member.FetchSilenceByID = func(ctx context.Context, silenceID string) (models.Silence, error) {
			return fetchSilenceEditorSilence(ctx, member, am.Version(), silenceID)
		}
		members = append(members, member)
	}
	sortWritableMembers(members)
	return members
}

type silenceEditorFreshReadError struct {
	StatusCode int
	Err        error
}

type silenceEditorUpstreamWriteError struct {
	Stage string
	Err   error
}

func (err *silenceEditorUpstreamWriteError) Error() string { return err.Err.Error() }
func (err *silenceEditorUpstreamWriteError) Unwrap() error { return err.Err }

func (err *silenceEditorFreshReadError) Error() string { return err.Err.Error() }
func (err *silenceEditorFreshReadError) Unwrap() error { return err.Err }

func fetchSilenceEditorSilence(ctx context.Context, member writableMember, version, silenceID string) (models.Silence, error) {
	endpoint, err := uri.JoinURL(member.URI, "/api/v2/silence/"+url.PathEscape(silenceID))
	if err != nil {
		return models.Silence{}, &silenceEditorFreshReadError{Err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return models.Silence{}, &silenceEditorFreshReadError{Err: err}
	}
	for key, value := range uri.HeadersForBasicAuth(member.URI) {
		req.Header.Set(key, value)
	}
	for key, value := range member.HTTPHeaders {
		req.Header.Set(key, value)
	}
	transport := member.HTTPTransport
	if transport == nil {
		transport = http.DefaultTransport
	}
	client := &http.Client{Timeout: member.RequestTimeout, Transport: transport}
	resp, err := client.Do(req)
	if err != nil {
		return models.Silence{}, &silenceEditorFreshReadError{Err: err}
	}
	defer resp.Body.Close()
	maxPayloadBytes := config.Config.SilenceEditor.MaxPayloadBytes
	reader := io.Reader(resp.Body)
	if maxPayloadBytes > 0 {
		reader = io.LimitReader(resp.Body, maxPayloadBytes+1)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		return models.Silence{}, &silenceEditorFreshReadError{StatusCode: resp.StatusCode, Err: err}
	}
	if maxPayloadBytes > 0 && int64(len(body)) > maxPayloadBytes {
		return models.Silence{}, &silenceEditorFreshReadError{StatusCode: resp.StatusCode, Err: fmt.Errorf("alertmanager silence response exceeds configured %d-byte limit", maxPayloadBytes)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = resp.Status
		}
		return models.Silence{}, &silenceEditorFreshReadError{StatusCode: resp.StatusCode, Err: errors.New(message)}
	}
	silenceMapper, err := mapper.GetSilenceMapper(version)
	if err != nil {
		return models.Silence{}, &silenceEditorFreshReadError{StatusCode: resp.StatusCode, Err: err}
	}
	silence, err := silenceMapper.UnmarshalResponse(body)
	if err != nil {
		return models.Silence{}, &silenceEditorFreshReadError{StatusCode: resp.StatusCode, Err: fmt.Errorf("failed to decode Alertmanager silence: %w", err)}
	}
	return *silence, nil
}

func sortWritableMembers(members []writableMember) {
	sort.Slice(members, func(i, j int) bool {
		if members[i].Healthy != members[j].Healthy {
			return members[i].Healthy
		}
		return members[i].Name < members[j].Name
	})
}

func submitSilenceToMember(ctx context.Context, member writableMember, payload []byte) (string, int, error) {
	endpoint, err := uri.JoinURL(member.URI, "/api/v2/silences")
	if err != nil {
		return "", 0, &silenceEditorUpstreamWriteError{Stage: "request", Err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", 0, &silenceEditorUpstreamWriteError{Stage: "request", Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range uri.HeadersForBasicAuth(member.URI) {
		req.Header.Set(k, v)
	}
	for k, v := range member.HTTPHeaders {
		req.Header.Set(k, v)
	}

	transport := member.HTTPTransport
	if transport == nil {
		transport = http.DefaultTransport
	}
	client := &http.Client{
		Timeout:   member.RequestTimeout,
		Transport: transport,
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, &silenceEditorUpstreamWriteError{Stage: "request", Err: err}
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", resp.StatusCode, &silenceEditorUpstreamWriteError{Stage: "read_body", Err: readErr}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return "", resp.StatusCode, &silenceEditorUpstreamWriteError{Stage: "upstream_response", Err: errors.New(msg)}
	}

	var out struct {
		SilenceID string `json:"silenceID"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", resp.StatusCode, &silenceEditorUpstreamWriteError{Stage: "decode_body", Err: err}
	}
	if strings.TrimSpace(out.SilenceID) == "" {
		return "", resp.StatusCode, &silenceEditorUpstreamWriteError{Stage: "missing_id", Err: errors.New("missing silenceID in Alertmanager response")}
	}

	return out.SilenceID, resp.StatusCode, nil
}

func silenceEditorWriteFailureStage(statusCode int, err error) string {
	if statusCode >= http.StatusInternalServerError {
		return "upstream_5xx"
	}
	var writeErr *silenceEditorUpstreamWriteError
	if errors.As(err, &writeErr) {
		var netErr net.Error
		if errors.As(writeErr, &netErr) && netErr.Timeout() {
			return "timeout"
		}
		return writeErr.Stage
	}
	return "request"
}

func silenceEditorConflictReason(err error) string {
	message := err.Error()
	switch {
	case strings.Contains(message, "member changed"):
		return "route_changed"
	case strings.Contains(message, "not found"):
		return "original_missing"
	case strings.Contains(message, "changed after"):
		return "original_changed"
	case strings.Contains(message, "expired"), strings.Contains(message, "still active"):
		return "state_changed"
	default:
		return "invalid_identity"
	}
}

func sanitizeRemoteLogError(err error) error {
	if err == nil {
		return nil
	}
	var freshErr *silenceEditorFreshReadError
	if errors.As(err, &freshErr) {
		return fmt.Errorf("remote silence request failed (status=%d)", freshErr.StatusCode)
	}
	var writeErr *silenceEditorUpstreamWriteError
	if errors.As(err, &writeErr) {
		return fmt.Errorf("remote silence write failed at %s", writeErr.Stage)
	}
	return errors.New("silence editor operation failed")
}

func outcomeUnknown(statusCode int, err error) bool {
	if err == nil {
		return false
	}
	if statusCode >= 200 && statusCode < 300 {
		return true
	}
	if statusCode >= 500 {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var urlErr *url.Error
	return errors.As(err, &urlErr)
}

func silenceEditorErrorJSON(w http.ResponseWriter, status int, msg string, outcomeUnknown bool) {
	mimeJSON(w)
	w.WriteHeader(status)
	out, _ := marshalJSON(silenceEditorErrorResponse{Error: msg, OutcomeUnknown: outcomeUnknown})
	_, _ = w.Write(out)
}

func silenceEditorRequiresReopenJSON(w http.ResponseWriter, msg string) {
	mimeJSON(w)
	w.WriteHeader(http.StatusConflict)
	out, _ := marshalJSON(silenceEditorErrorResponse{Error: msg, RequiresReopen: true})
	_, _ = w.Write(out)
}
