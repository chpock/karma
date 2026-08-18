package alertlifecycle

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

const recheckJobFingerprintFormatVersion = 1

type recheckSourceContextEntry struct {
	SourceName         string `json:"sourceName"`
	ClusterID          string `json:"clusterId"`
	SyncStatus         string `json:"syncStatus"`
	SilenceFingerprint string `json:"silenceFingerprint"`
	Retired            bool   `json:"retired"`
}

type recheckClusterContextEntry struct {
	ClusterID   string `json:"clusterId"`
	Status      string `json:"status"`
	MemberCount int    `json:"memberCount"`
}

type recheckSourceContext struct {
	FormatVersion int                          `json:"formatVersion"`
	Generation    int                          `json:"generation"`
	Sources       []recheckSourceContextEntry  `json:"sources"`
	Clusters      []recheckClusterContextEntry `json:"clusters"`
}

type recheckJobRuleInput struct {
	SourceName            string `json:"sourceName"`
	ClusterID             string `json:"clusterId"`
	TargetKey             string `json:"targetKey"`
	RuleID                int64  `json:"-"`
	RevisionID            int64  `json:"-"`
	SilenceID             string `json:"silenceId"`
	ContentFingerprint    string `json:"contentFingerprint"`
	ObservedState         string `json:"observedState"`
	EvaluationFingerprint string `json:"evaluationFingerprint"`
	MatchersJSON          string `json:"matchersJson"`
}

type recheckCandidateInput struct {
	AlertID                  string  `json:"alertId"`
	TargetKey                string  `json:"targetKey"`
	ExpectedAlertLastSeenAt  *string `json:"expectedAlertLastSeenAt"`
	ExpectedTargetLastSeenAt *string `json:"expectedTargetLastSeenAt"`
	MatchLabelsJSON          *string `json:"matchLabelsJson"`
	RelevantSourcesJSON      string  `json:"relevantSourcesJson"`
	TargetSourceMembersJSON  *string `json:"targetSourceMembersJson"`
}

type recheckJobInput struct {
	TriggerType              string                  `json:"triggerType"`
	SourceContext            recheckSourceContext    `json:"sourceContext"`
	SourceContextFingerprint string                  `json:"sourceContextFingerprint"`
	Rules                    []recheckJobRuleInput   `json:"rules"`
	Candidates               []recheckCandidateInput `json:"candidates"`
}

type recheckJobCreateResult struct {
	JobID                    int64
	Created                  bool
	TriggerType              string
	CandidateCount           int
	SourceContextFingerprint string
	JobFingerprint           string
}

type recheckCandidateScope struct {
	AlertID   string
	TargetKey string
}

func createRecheckJobFromFinalStateTx(tx *sql.Tx, triggerType string, scopes []recheckCandidateScope, now time.Time, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
	input, err := buildRecheckJobInputTx(tx, triggerType, scopes)
	if err != nil {
		return recheckJobCreateResult{}, err
	}
	return createRecheckJobTx(tx, input, now, collector)
}

func buildRecheckJobInputTx(tx *sql.Tx, triggerType string, scopes []recheckCandidateScope) (recheckJobInput, error) {
	if triggerType != "source_context_changed" && triggerType != "alert_became_eligible" {
		return recheckJobInput{}, fmt.Errorf("invalid recheck job trigger type %q", triggerType)
	}
	contextSnapshot, sourceContextFingerprint, err := loadRecheckSourceContextTx(tx)
	if err != nil {
		return recheckJobInput{}, err
	}
	rules, err := loadRecheckJobRuleInputsTx(tx)
	if err != nil {
		return recheckJobInput{}, err
	}
	candidates, err := loadRecheckCandidateInputsTx(tx, scopes)
	if err != nil {
		return recheckJobInput{}, err
	}
	return recheckJobInput{
		TriggerType:              triggerType,
		SourceContext:            contextSnapshot,
		SourceContextFingerprint: sourceContextFingerprint,
		Rules:                    rules,
		Candidates:               candidates,
	}, nil
}

func loadRecheckSourceContextTx(tx *sql.Tx) (recheckSourceContext, string, error) {
	generation, err := readMetadataIntTx(tx, metaRecheckSourceContextGen)
	if err != nil {
		return recheckSourceContext{}, "", err
	}
	contextSnapshot := recheckSourceContext{FormatVersion: recheckJobFingerprintFormatVersion, Generation: generation, Sources: []recheckSourceContextEntry{}, Clusters: []recheckClusterContextEntry{}}
	rows, err := tx.Query(`
SELECT source_name, COALESCE(current_cluster_id, ''), sync_status,
       COALESCE(silence_set_fingerprint, ''), retired_at
FROM alertmanager_sources
ORDER BY source_name
`)
	if err != nil {
		return recheckSourceContext{}, "", fmt.Errorf("failed to load recheck source context: %w", err)
	}
	for rows.Next() {
		var entry recheckSourceContextEntry
		var retiredAt sql.NullString
		if err = rows.Scan(&entry.SourceName, &entry.ClusterID, &entry.SyncStatus, &entry.SilenceFingerprint, &retiredAt); err != nil {
			rows.Close()
			return recheckSourceContext{}, "", fmt.Errorf("failed to scan recheck source context: %w", err)
		}
		entry.Retired = retiredAt.Valid
		contextSnapshot.Sources = append(contextSnapshot.Sources, entry)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return recheckSourceContext{}, "", fmt.Errorf("failed to iterate recheck source context: %w", err)
	}
	rows.Close()

	rows, err = tx.Query(`
	SELECT cluster_id, status, member_count
FROM alertmanager_cluster_consistency
ORDER BY cluster_id
`)
	if err != nil {
		return recheckSourceContext{}, "", fmt.Errorf("failed to load recheck cluster context: %w", err)
	}
	for rows.Next() {
		var entry recheckClusterContextEntry
		if err = rows.Scan(&entry.ClusterID, &entry.Status, &entry.MemberCount); err != nil {
			rows.Close()
			return recheckSourceContext{}, "", fmt.Errorf("failed to scan recheck cluster context: %w", err)
		}
		contextSnapshot.Clusters = append(contextSnapshot.Clusters, entry)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return recheckSourceContext{}, "", fmt.Errorf("failed to iterate recheck cluster context: %w", err)
	}
	rows.Close()

	raw, err := json.Marshal(contextSnapshot)
	if err != nil {
		return recheckSourceContext{}, "", fmt.Errorf("failed to encode recheck source context: %w", err)
	}
	return contextSnapshot, recheckFingerprint(raw), nil
}

func loadRecheckJobRuleInputsTx(tx *sql.Tx) ([]recheckJobRuleInput, error) {
	rows, err := tx.Query(`
	SELECT current.source_name, COALESCE(source.current_cluster_id, ''), current.rule_id,
	       current.revision_id, COALESCE(rule.silence_id, ''), COALESCE(revision.content_fingerprint, ''),
	       current.observed_state, COALESCE(revision.evaluation_fingerprint, ''),
	       COALESCE(revision.matchers_json, '[]')
FROM alertmanager_source_silences current
JOIN alertmanager_sources source ON source.source_name = current.source_name
JOIN silence_rules rule ON rule.rule_id = current.rule_id
JOIN silence_revisions revision ON revision.revision_id = current.revision_id
  AND revision.rule_id = current.rule_id
WHERE source.retired_at IS NULL AND source.sync_status = 'success'
ORDER BY current.source_name, source.current_cluster_id, current.rule_id, current.revision_id
`)
	if err != nil {
		return nil, fmt.Errorf("failed to load recheck job rule inputs: %w", err)
	}
	defer rows.Close()
	rules := []recheckJobRuleInput{}
	for rows.Next() {
		var rule recheckJobRuleInput
		if err = rows.Scan(&rule.SourceName, &rule.ClusterID, &rule.RuleID, &rule.RevisionID, &rule.SilenceID, &rule.ContentFingerprint, &rule.ObservedState, &rule.EvaluationFingerprint, &rule.MatchersJSON); err != nil {
			return nil, fmt.Errorf("failed to scan recheck job rule input: %w", err)
		}
		rule.TargetKey = rule.ClusterID
		canonicalMatchers, canonicalErr := canonicalMatcherJSON(rule.MatchersJSON)
		if canonicalErr != nil {
			return nil, fmt.Errorf("failed to canonicalize recheck rule matchers: %w", canonicalErr)
		}
		rule.MatchersJSON = canonicalMatchers
		rules = append(rules, rule)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate recheck job rule inputs: %w", err)
	}
	sort.Slice(rules, func(i, j int) bool {
		if rules[i].SourceName != rules[j].SourceName {
			return rules[i].SourceName < rules[j].SourceName
		}
		if rules[i].TargetKey != rules[j].TargetKey {
			return rules[i].TargetKey < rules[j].TargetKey
		}
		if rules[i].RuleID != rules[j].RuleID {
			return rules[i].RuleID < rules[j].RuleID
		}
		return rules[i].RevisionID < rules[j].RevisionID
	})
	return rules, nil
}

func loadRecheckCandidateInputsTx(tx *sql.Tx, scopes []recheckCandidateScope) ([]recheckCandidateInput, error) {
	filter := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		filter[scope.AlertID+"\x00"+scope.TargetKey] = struct{}{}
	}
	rows, err := tx.Query(`
SELECT alert_id, last_seen_at, match_labels_json, relevant_sources_json
FROM alert_states
ORDER BY alert_id
`)
	if err != nil {
		return nil, fmt.Errorf("failed to load recheck candidate alerts: %w", err)
	}
	defer rows.Close()
	candidates := []recheckCandidateInput{}
	for rows.Next() {
		var alertID string
		var lastSeenAt, matchLabels, relevantSources sql.NullString
		if err = rows.Scan(&alertID, &lastSeenAt, &matchLabels, &relevantSources); err != nil {
			return nil, fmt.Errorf("failed to scan recheck candidate alert: %w", err)
		}
		fullSnapshot := scopes == nil
		alert, alertErr := getCurrentAlertTx(tx, alertID)
		if alertErr != nil {
			return nil, alertErr
		}
		targetRows, targetErr := readTargetStatesTx(tx, alertID)
		if targetErr != nil {
			return nil, targetErr
		}
		hasNonRetiredTarget := false
		for _, target := range targetRows {
			if target.RetiredAt != nil {
				continue
			}
			hasNonRetiredTarget = true
			if !fullSnapshot {
				if _, ok := filter[alertID+"\x00"+target.Target]; !ok {
					continue
				}
			}
			if !recheckAlertOrTargetEligible(target.State, target.IsLive, target.IsResolved, target.IsSuppressed) {
				continue
			}
			members, marshalErr := marshalCanonicalStringSlice(target.SourceMembers)
			if marshalErr != nil {
				return nil, fmt.Errorf("failed to encode target source members for %q: %w", alertID, marshalErr)
			}
			candidate := recheckCandidateInput{
				AlertID:                  alertID,
				TargetKey:                target.Target,
				ExpectedAlertLastSeenAt:  nullableTimeStringPtr(alert.LastSeenAt),
				ExpectedTargetLastSeenAt: nullableTimeStringPtr(target.LastSeenAt),
				MatchLabelsJSON:          canonicalNullableJSON(matchLabels),
				RelevantSourcesJSON:      canonicalOrEmptyJSON(relevantSources.String),
				TargetSourceMembersJSON:  stringPtr(members),
			}
			candidates = append(candidates, candidate)
		}
		if !hasNonRetiredTarget && recheckAlertOrTargetEligible(alert.State, alert.IsLive, alert.IsResolved, alert.IsSuppressed) && (fullSnapshot || hasScope(filter, alertID, "")) {
			candidates = append(candidates, recheckCandidateInput{
				AlertID:                 alertID,
				TargetKey:               "",
				ExpectedAlertLastSeenAt: nullableTimeStringPtr(alert.LastSeenAt),
				MatchLabelsJSON:         canonicalNullableJSON(matchLabels),
				RelevantSourcesJSON:     canonicalOrEmptyJSON(relevantSources.String),
			})
		}
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate recheck candidate alerts: %w", err)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].AlertID != candidates[j].AlertID {
			return candidates[i].AlertID < candidates[j].AlertID
		}
		return candidates[i].TargetKey < candidates[j].TargetKey
	})
	return candidates, nil
}

func hasScope(scopes map[string]struct{}, alertID, targetKey string) bool {
	_, ok := scopes[alertID+"\x00"+targetKey]
	return ok
}

func recheckAlertOrTargetEligible(state State, isLive, isResolved, isSuppressed bool) bool {
	return state == StateInactive && !isLive && !isResolved && !isSuppressed
}

func createRecheckJobTx(tx *sql.Tx, input recheckJobInput, now time.Time, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
	canonicalInput, canonicalBytes, sourceContextJSON, sourceContextFingerprint, jobFingerprint, err := canonicalizeRecheckJobInput(input)
	if err != nil {
		return recheckJobCreateResult{}, err
	}
	input = canonicalInput
	if collisionID, collisionErr := findSemanticRecheckJobCollisionTx(tx, input.TriggerType, canonicalBytes, jobFingerprint); collisionErr != nil {
		return recheckJobCreateResult{}, collisionErr
	} else if collisionID != 0 {
		return recheckJobCreateResult{}, fmt.Errorf("recheck job fingerprint collision for job %d: stored fingerprint does not match canonical input", collisionID)
	}
	var jobID int64
	err = tx.QueryRow(`
INSERT INTO silence_recheck_jobs(source_context_fingerprint, job_fingerprint, source_context_json, trigger_type, status, created_at, total_candidates)
VALUES(?, ?, ?, ?, 'queued', ?, ?)
ON CONFLICT(job_fingerprint) DO NOTHING
RETURNING job_id
`, sourceContextFingerprint, jobFingerprint, sourceContextJSON, input.TriggerType, now.UTC().Format(time.RFC3339Nano), len(input.Candidates)).Scan(&jobID)
	created := true
	if errors.Is(err, sql.ErrNoRows) {
		if err = tx.QueryRow(`SELECT job_id FROM silence_recheck_jobs WHERE job_fingerprint=?`, jobFingerprint).Scan(&jobID); err != nil {
			return recheckJobCreateResult{}, fmt.Errorf("failed to load existing recheck job: %w", err)
		}
		if err = verifyPersistedRecheckJobInputTx(tx, jobID, input, jobFingerprint, sourceContextFingerprint, sourceContextJSON, canonicalBytes); err != nil {
			return recheckJobCreateResult{}, err
		}
		created = false
	} else if err != nil {
		return recheckJobCreateResult{}, fmt.Errorf("failed to create recheck job: %w", err)
	}
	if !created {
		return recheckJobCreateResult{JobID: jobID, TriggerType: input.TriggerType, CandidateCount: len(input.Candidates), SourceContextFingerprint: sourceContextFingerprint, JobFingerprint: jobFingerprint}, nil
	}
	for _, rule := range input.Rules {
		if _, err = tx.Exec(`
INSERT INTO silence_recheck_job_rules(job_id, source_name, cluster_id, target_key, rule_id, revision_id, silence_id, content_fingerprint, observed_state, evaluation_fingerprint, matchers_json)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, jobID, rule.SourceName, rule.ClusterID, rule.TargetKey, rule.RuleID, rule.RevisionID, rule.SilenceID, rule.ContentFingerprint, rule.ObservedState, rule.EvaluationFingerprint, rule.MatchersJSON); err != nil {
			return recheckJobCreateResult{}, fmt.Errorf("failed to create recheck job rule: %w", err)
		}
	}
	for _, candidate := range input.Candidates {
		if _, err = tx.Exec(`
INSERT INTO silence_recheck_job_candidates(job_id, alert_id, target_key, expected_alert_last_seen_at, expected_target_last_seen_at, match_labels_json, relevant_sources_json, target_source_members_json)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
`, jobID, candidate.AlertID, candidate.TargetKey, candidate.ExpectedAlertLastSeenAt, candidate.ExpectedTargetLastSeenAt, candidate.MatchLabelsJSON, candidate.RelevantSourcesJSON, candidate.TargetSourceMembersJSON); err != nil {
			return recheckJobCreateResult{}, fmt.Errorf("failed to create recheck job candidate: %w", err)
		}
	}
	appendRecheckJobCreatedEventTx(tx, collector, jobID, input.TriggerType, sourceContextFingerprint, jobFingerprint, len(input.Candidates))
	return recheckJobCreateResult{JobID: jobID, Created: true, TriggerType: input.TriggerType, CandidateCount: len(input.Candidates), SourceContextFingerprint: sourceContextFingerprint, JobFingerprint: jobFingerprint}, nil
}

func findSemanticRecheckJobCollisionTx(tx *sql.Tx, triggerType string, canonicalBytes []byte, jobFingerprint string) (int64, error) {
	rows, err := tx.Query(`SELECT job_id, job_fingerprint FROM silence_recheck_jobs WHERE trigger_type=?`, triggerType)
	if err != nil {
		return 0, fmt.Errorf("failed to inspect possible recheck job collision: %w", err)
	}
	var possibleCollisions []struct {
		jobID          int64
		jobFingerprint string
	}
	for rows.Next() {
		var candidate struct {
			jobID          int64
			jobFingerprint string
		}
		if err = rows.Scan(&candidate.jobID, &candidate.jobFingerprint); err != nil {
			rows.Close()
			return 0, fmt.Errorf("failed to scan possible recheck job collision: %w", err)
		}
		possibleCollisions = append(possibleCollisions, candidate)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("failed to iterate possible recheck job collisions: %w", err)
	}
	rows.Close()
	for _, possible := range possibleCollisions {
		persisted, persistedErr := loadPersistedRecheckJobInputTx(tx, possible.jobID)
		if persistedErr != nil {
			return 0, persistedErr
		}
		_, persistedCanonical, _, _, _, canonicalErr := canonicalizeRecheckJobInput(persisted)
		if canonicalErr != nil {
			return 0, canonicalErr
		}
		if string(persistedCanonical) == string(canonicalBytes) && possible.jobFingerprint != jobFingerprint {
			return possible.jobID, nil
		}
	}
	return 0, nil
}

func verifyPersistedRecheckJobInputTx(tx *sql.Tx, jobID int64, expected recheckJobInput, expectedJobFingerprint, expectedSourceFingerprint, expectedSourceJSON string, expectedCanonical []byte) error {
	var persistedJobFingerprint, persistedSourceFingerprint, persistedSourceJSON, persistedTrigger string
	var persistedTotal int
	if err := tx.QueryRow(`SELECT job_fingerprint, source_context_fingerprint, source_context_json, trigger_type, total_candidates FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&persistedJobFingerprint, &persistedSourceFingerprint, &persistedSourceJSON, &persistedTrigger, &persistedTotal); err != nil {
		return fmt.Errorf("failed to verify existing recheck job parent: %w", err)
	}
	if persistedJobFingerprint != expectedJobFingerprint || persistedSourceFingerprint != expectedSourceFingerprint || persistedSourceJSON != expectedSourceJSON || persistedTrigger != expected.TriggerType || persistedTotal != len(expected.Candidates) {
		return fmt.Errorf("recheck job fingerprint collision for job %d", jobID)
	}

	rules, err := tx.Query(`
SELECT source_name, cluster_id, target_key, rule_id, revision_id, silence_id, content_fingerprint,
       observed_state, evaluation_fingerprint, matchers_json
FROM silence_recheck_job_rules
WHERE job_id=?
ORDER BY source_name, target_key, silence_id, content_fingerprint, evaluation_fingerprint, matchers_json, rule_id, revision_id
`, jobID)
	if err != nil {
		return fmt.Errorf("failed to verify existing recheck job rules: %w", err)
	}
	ruleIndex := 0
	for rules.Next() {
		if ruleIndex >= len(expected.Rules) {
			rules.Close()
			return fmt.Errorf("recheck job fingerprint collision for job %d: extra rule row", jobID)
		}
		var actual recheckJobRuleInput
		if err = rules.Scan(&actual.SourceName, &actual.ClusterID, &actual.TargetKey, &actual.RuleID, &actual.RevisionID, &actual.SilenceID, &actual.ContentFingerprint, &actual.ObservedState, &actual.EvaluationFingerprint, &actual.MatchersJSON); err != nil {
			rules.Close()
			return fmt.Errorf("failed to scan existing recheck job rule: %w", err)
		}
		if actual != expected.Rules[ruleIndex] {
			rules.Close()
			return fmt.Errorf("recheck job fingerprint collision for job %d: rule row %d differs", jobID, ruleIndex)
		}
		if err = verifyRecheckJobRuleProvenanceTx(tx, actual); err != nil {
			rules.Close()
			return fmt.Errorf("recheck job fingerprint collision for job %d: rule row %d provenance: %w", jobID, ruleIndex, err)
		}
		ruleIndex++
	}
	if err = rules.Err(); err != nil {
		rules.Close()
		return fmt.Errorf("failed to iterate existing recheck job rules: %w", err)
	}
	rules.Close()
	if ruleIndex != len(expected.Rules) {
		return fmt.Errorf("recheck job fingerprint collision for job %d: missing rule row", jobID)
	}

	candidates, err := tx.Query(`
SELECT alert_id, target_key, expected_alert_last_seen_at, expected_target_last_seen_at,
       match_labels_json, relevant_sources_json, target_source_members_json
FROM silence_recheck_job_candidates
WHERE job_id=?
ORDER BY alert_id, target_key
`, jobID)
	if err != nil {
		return fmt.Errorf("failed to verify existing recheck job candidates: %w", err)
	}
	candidateIndex := 0
	for candidates.Next() {
		if candidateIndex >= len(expected.Candidates) {
			candidates.Close()
			return fmt.Errorf("recheck job fingerprint collision for job %d: extra candidate row", jobID)
		}
		var actual recheckCandidateInput
		var expectedAlert, expectedTarget, labels, relevant, members sql.NullString
		if err = candidates.Scan(&actual.AlertID, &actual.TargetKey, &expectedAlert, &expectedTarget, &labels, &relevant, &members); err != nil {
			candidates.Close()
			return fmt.Errorf("failed to scan existing recheck job candidate: %w", err)
		}
		actual.ExpectedAlertLastSeenAt = nullableStringFromNull(expectedAlert)
		actual.ExpectedTargetLastSeenAt = nullableStringFromNull(expectedTarget)
		actual.MatchLabelsJSON = nullableStringFromNull(labels)
		actual.RelevantSourcesJSON = relevant.String
		actual.TargetSourceMembersJSON = nullableStringFromNull(members)
		if !equalRecheckCandidateInput(actual, expected.Candidates[candidateIndex]) {
			candidates.Close()
			return fmt.Errorf("recheck job fingerprint collision for job %d: candidate row %d differs", jobID, candidateIndex)
		}
		candidateIndex++
	}
	if err = candidates.Err(); err != nil {
		candidates.Close()
		return fmt.Errorf("failed to iterate existing recheck job candidates: %w", err)
	}
	candidates.Close()
	if candidateIndex != len(expected.Candidates) {
		return fmt.Errorf("recheck job fingerprint collision for job %d: missing candidate row", jobID)
	}

	canonical, err := json.Marshal(expected)
	if err != nil || string(canonical) != string(expectedCanonical) {
		return fmt.Errorf("recheck job fingerprint collision for job %d: canonical expected input changed", jobID)
	}
	return nil
}

func equalRecheckCandidateInput(actual, expected recheckCandidateInput) bool {
	return actual.AlertID == expected.AlertID &&
		actual.TargetKey == expected.TargetKey &&
		equalOptionalString(actual.ExpectedAlertLastSeenAt, expected.ExpectedAlertLastSeenAt) &&
		equalOptionalString(actual.ExpectedTargetLastSeenAt, expected.ExpectedTargetLastSeenAt) &&
		equalOptionalString(actual.MatchLabelsJSON, expected.MatchLabelsJSON) &&
		actual.RelevantSourcesJSON == expected.RelevantSourcesJSON &&
		equalOptionalString(actual.TargetSourceMembersJSON, expected.TargetSourceMembersJSON)
}

func equalOptionalString(actual, expected *string) bool {
	if actual == nil || expected == nil {
		return actual == nil && expected == nil
	}
	return *actual == *expected
}

func verifyRecheckJobRuleProvenanceTx(tx *sql.Tx, rule recheckJobRuleInput) error {
	var clusterID, silenceID, contentFingerprint, evaluationFingerprint, matchersJSON string
	if err := tx.QueryRow(`
SELECT sr.cluster_id, sr.silence_id, rev.content_fingerprint, rev.evaluation_fingerprint, rev.matchers_json
FROM silence_revisions rev
JOIN silence_rules sr ON sr.rule_id=rev.rule_id
WHERE rev.revision_id=? AND rev.rule_id=?
`, rule.RevisionID, rule.RuleID).Scan(&clusterID, &silenceID, &contentFingerprint, &evaluationFingerprint, &matchersJSON); err != nil {
		return err
	}
	if clusterID != rule.ClusterID || silenceID != rule.SilenceID || contentFingerprint != rule.ContentFingerprint || evaluationFingerprint != rule.EvaluationFingerprint || matchersJSON != rule.MatchersJSON {
		return errors.New("linked silence revision fields differ from pinned rule fields")
	}
	return nil
}

func canonicalRecheckJobInput(input recheckJobInput) ([]byte, string, string, string, error) {
	_, raw, sourceContextJSON, sourceFingerprint, jobFingerprint, err := canonicalizeRecheckJobInput(input)
	return raw, sourceContextJSON, sourceFingerprint, jobFingerprint, err
}

func canonicalizeRecheckJobInput(input recheckJobInput) (recheckJobInput, []byte, string, string, string, error) {
	if input.TriggerType != "source_context_changed" && input.TriggerType != "alert_became_eligible" {
		return recheckJobInput{}, nil, "", "", "", fmt.Errorf("unsupported recheck trigger type %q", input.TriggerType)
	}
	input.SourceContext.Sources = append([]recheckSourceContextEntry(nil), input.SourceContext.Sources...)
	input.SourceContext.Clusters = append([]recheckClusterContextEntry(nil), input.SourceContext.Clusters...)
	input.Rules = append([]recheckJobRuleInput(nil), input.Rules...)
	input.Candidates = append([]recheckCandidateInput(nil), input.Candidates...)
	sort.Slice(input.SourceContext.Sources, func(i, j int) bool {
		a, b := input.SourceContext.Sources[i], input.SourceContext.Sources[j]
		if a.SourceName != b.SourceName {
			return a.SourceName < b.SourceName
		}
		if a.ClusterID != b.ClusterID {
			return a.ClusterID < b.ClusterID
		}
		if a.SyncStatus != b.SyncStatus {
			return a.SyncStatus < b.SyncStatus
		}
		if a.SilenceFingerprint != b.SilenceFingerprint {
			return a.SilenceFingerprint < b.SilenceFingerprint
		}
		return !a.Retired && b.Retired
	})
	sort.Slice(input.SourceContext.Clusters, func(i, j int) bool {
		return input.SourceContext.Clusters[i].ClusterID < input.SourceContext.Clusters[j].ClusterID
	})
	for i := range input.Rules {
		canonical, err := canonicalMatcherJSON(input.Rules[i].MatchersJSON)
		if err != nil {
			return recheckJobInput{}, nil, "", "", "", err
		}
		input.Rules[i].MatchersJSON = canonical
	}
	sort.Slice(input.Rules, func(i, j int) bool {
		a, b := input.Rules[i], input.Rules[j]
		if a.SourceName != b.SourceName {
			return a.SourceName < b.SourceName
		}
		if a.ClusterID != b.ClusterID {
			return a.ClusterID < b.ClusterID
		}
		if a.TargetKey != b.TargetKey {
			return a.TargetKey < b.TargetKey
		}
		if a.SilenceID != b.SilenceID {
			return a.SilenceID < b.SilenceID
		}
		if a.ContentFingerprint != b.ContentFingerprint {
			return a.ContentFingerprint < b.ContentFingerprint
		}
		if a.EvaluationFingerprint != b.EvaluationFingerprint {
			return a.EvaluationFingerprint < b.EvaluationFingerprint
		}
		if a.ObservedState != b.ObservedState {
			return a.ObservedState < b.ObservedState
		}
		return a.MatchersJSON < b.MatchersJSON
	})
	for i := range input.Candidates {
		input.Candidates[i].RelevantSourcesJSON = canonicalSourceReferencesJSON(input.Candidates[i].RelevantSourcesJSON)
		canonicalAlertTimestamp, timestampErr := canonicalTimestampPointer(input.Candidates[i].ExpectedAlertLastSeenAt)
		if timestampErr != nil {
			return recheckJobInput{}, nil, "", "", "", timestampErr
		}
		canonicalTargetTimestamp, timestampErr := canonicalTimestampPointer(input.Candidates[i].ExpectedTargetLastSeenAt)
		if timestampErr != nil {
			return recheckJobInput{}, nil, "", "", "", timestampErr
		}
		input.Candidates[i].ExpectedAlertLastSeenAt = canonicalAlertTimestamp
		input.Candidates[i].ExpectedTargetLastSeenAt = canonicalTargetTimestamp
		if input.Candidates[i].MatchLabelsJSON != nil {
			canonical, err := canonicalJSONString(*input.Candidates[i].MatchLabelsJSON)
			if err != nil {
				return recheckJobInput{}, nil, "", "", "", err
			}
			input.Candidates[i].MatchLabelsJSON = &canonical
		}
		if input.Candidates[i].TargetSourceMembersJSON != nil {
			canonical, err := canonicalStringSliceJSON(*input.Candidates[i].TargetSourceMembersJSON)
			if err != nil {
				return recheckJobInput{}, nil, "", "", "", err
			}
			input.Candidates[i].TargetSourceMembersJSON = &canonical
		}
	}
	sort.Slice(input.Candidates, func(i, j int) bool {
		if input.Candidates[i].AlertID != input.Candidates[j].AlertID {
			return input.Candidates[i].AlertID < input.Candidates[j].AlertID
		}
		return input.Candidates[i].TargetKey < input.Candidates[j].TargetKey
	})
	contextRaw, err := json.Marshal(input.SourceContext)
	if err != nil {
		return recheckJobInput{}, nil, "", "", "", fmt.Errorf("failed to encode canonical recheck source context: %w", err)
	}
	sourceFingerprint := recheckFingerprint(contextRaw)
	input.SourceContextFingerprint = sourceFingerprint
	jobRaw, err := json.Marshal(input)
	if err != nil {
		return recheckJobInput{}, nil, "", "", "", fmt.Errorf("failed to encode canonical recheck job input: %w", err)
	}
	return input, jobRaw, string(contextRaw), sourceFingerprint, recheckFingerprint(jobRaw), nil
}

func loadPersistedRecheckJobInputTx(tx *sql.Tx, jobID int64) (recheckJobInput, error) {
	var input recheckJobInput
	var sourceContextJSON string
	if err := tx.QueryRow(`SELECT source_context_json, trigger_type, source_context_fingerprint FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&sourceContextJSON, &input.TriggerType, &input.SourceContextFingerprint); err != nil {
		return input, fmt.Errorf("failed to load persisted recheck job parent: %w", err)
	}
	if err := json.Unmarshal([]byte(sourceContextJSON), &input.SourceContext); err != nil {
		return input, fmt.Errorf("failed to decode persisted recheck source context: %w", err)
	}
	rules, err := tx.Query(`
	SELECT source_name, cluster_id, target_key, rule_id, revision_id, silence_id, content_fingerprint,
	       observed_state, evaluation_fingerprint, matchers_json
	FROM silence_recheck_job_rules WHERE job_id=? ORDER BY source_name, target_key, silence_id, content_fingerprint, evaluation_fingerprint, matchers_json
`, jobID)
	if err != nil {
		return input, err
	}
	for rules.Next() {
		var rule recheckJobRuleInput
		if err = rules.Scan(&rule.SourceName, &rule.ClusterID, &rule.TargetKey, &rule.RuleID, &rule.RevisionID, &rule.SilenceID, &rule.ContentFingerprint, &rule.ObservedState, &rule.EvaluationFingerprint, &rule.MatchersJSON); err != nil {
			rules.Close()
			return input, err
		}
		input.Rules = append(input.Rules, rule)
	}
	if err = rules.Err(); err != nil {
		rules.Close()
		return input, err
	}
	rules.Close()
	candidates, err := tx.Query(`
SELECT alert_id, target_key, expected_alert_last_seen_at, expected_target_last_seen_at,
       match_labels_json, relevant_sources_json, target_source_members_json
FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id, target_key
`, jobID)
	if err != nil {
		return input, err
	}
	for candidates.Next() {
		var candidate recheckCandidateInput
		var expectedAlert, expectedTarget, labels, members, relevant sql.NullString
		if err = candidates.Scan(&candidate.AlertID, &candidate.TargetKey, &expectedAlert, &expectedTarget, &labels, &relevant, &members); err != nil {
			candidates.Close()
			return input, err
		}
		candidate.ExpectedAlertLastSeenAt = nullableStringFromNull(expectedAlert)
		candidate.ExpectedTargetLastSeenAt = nullableStringFromNull(expectedTarget)
		candidate.MatchLabelsJSON = nullableStringFromNull(labels)
		candidate.RelevantSourcesJSON = relevant.String
		candidate.TargetSourceMembersJSON = nullableStringFromNull(members)
		input.Candidates = append(input.Candidates, candidate)
	}
	if err = candidates.Err(); err != nil {
		candidates.Close()
		return input, err
	}
	candidates.Close()
	return input, nil
}

func appendRecheckJobCreatedEventTx(tx *sql.Tx, collector *runtimeEventCollector, jobID int64, triggerType, sourceContextFingerprint, jobFingerprint string, candidateCount int) {
	if collector == nil {
		return
	}
	queueDepth := -1
	if err := tx.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs WHERE status IN ('queued','running','retry_wait','failed')`).Scan(&queueDepth); err != nil {
		collector.append(runtimeEvent{level: slog.LevelWarn, component: "silence_reconciliation", event: "observability_collection_failed", message: "Failed to collect recheck queue depth for logging", attrs: []slog.Attr{slog.String("stage", "queue_depth"), slog.Any("error", err)}})
	}
	collector.append(runtimeEvent{level: slog.LevelInfo, component: "silence_reconciliation", event: "recheck_job_queued", message: "Silence recheck job queued", attrs: []slog.Attr{slog.Int64("job_id", jobID), slog.String("source_context_fingerprint", sourceContextFingerprint), slog.String("job_fingerprint", jobFingerprint), slog.Int("candidate_count", candidateCount), slog.Int("queue_depth", queueDepth), slog.String("trigger_type", triggerType), slog.String("reason_code", triggerType)}})
}

func recheckFingerprint(raw []byte) string {
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum)
}

func canonicalJSONString(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(canonical), nil
}

func canonicalOrEmptyJSON(raw string) string {
	canonical, err := canonicalJSONString(raw)
	if err != nil || strings.TrimSpace(canonical) == "" {
		return "[]"
	}
	return canonical
}

func canonicalSourceReferencesJSON(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "[]"
	}
	var refs []SourceRef
	if err := json.Unmarshal([]byte(raw), &refs); err == nil {
		sort.Slice(refs, func(i, j int) bool {
			if refs[i].Name != refs[j].Name {
				return refs[i].Name < refs[j].Name
			}
			return refs[i].Cluster < refs[j].Cluster
		})
		canonical, marshalErr := json.Marshal(refs)
		if marshalErr == nil {
			return string(canonical)
		}
	}
	return canonicalOrEmptyJSON(raw)
}

func canonicalStringSliceJSON(raw string) (string, error) {
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return "", err
	}
	sort.Strings(values)
	canonical, err := json.Marshal(values)
	return string(canonical), err
}

func canonicalMatcherJSON(raw string) (string, error) {
	var matchers []SilenceMatcherSnapshot
	if err := json.Unmarshal([]byte(raw), &matchers); err != nil {
		return canonicalJSONString(raw)
	}
	sort.Slice(matchers, func(i, j int) bool {
		if matchers[i].Name != matchers[j].Name {
			return matchers[i].Name < matchers[j].Name
		}
		if matchers[i].Value != matchers[j].Value {
			return matchers[i].Value < matchers[j].Value
		}
		if matchers[i].IsRegex != matchers[j].IsRegex {
			return !matchers[i].IsRegex
		}
		return !matchers[i].IsEqual && matchers[j].IsEqual
	})
	canonical, err := json.Marshal(matchers)
	return string(canonical), err
}

func canonicalTimestampPointer(value *string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	if strings.TrimSpace(*value) == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, *value)
	if err != nil {
		return nil, fmt.Errorf("invalid recheck observation timestamp %q: %w", *value, err)
	}
	canonical := parsed.UTC().Format(time.RFC3339Nano)
	return &canonical, nil
}

func canonicalNullableJSON(raw sql.NullString) *string {
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return nil
	}
	value := raw.String
	if canonical, err := canonicalJSONString(raw.String); err == nil {
		value = canonical
	}
	return &value
}

func marshalCanonicalStringSlice(values []string) (string, error) {
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	raw, err := json.Marshal(copyValues)
	return string(raw), err
}

func nullableTimeStringPtr(value *time.Time) *string {
	if value == nil {
		return nil
	}
	raw := value.UTC().Format(time.RFC3339Nano)
	return &raw
}

func stringPtr(value string) *string { return &value }

type recheckCandidateDecision struct {
	outcome  string
	promoted bool
	events   []runtimeEvent
}

type recheckCandidateState struct {
	alert      AlertCurrent
	targetRows map[string]targetStateRow
	target     targetStateRow
}

// classifyRecheckCandidateStateTx owns the stable state/observation priority.
// Label and source evaluation happen only after this boundary has accepted the
// current global or target state.
func classifyRecheckCandidateStateTx(tx *sql.Tx, candidate recheckCandidate) (recheckCandidateState, string, error) {
	state := recheckCandidateState{}
	alert, err := getCurrentAlertTx(tx, candidate.AlertID)
	if errors.Is(err, sql.ErrNoRows) {
		return state, "missing_current_alert", nil
	}
	if err != nil {
		return state, "", err
	}
	state.alert = alert
	if !candidate.LastSeenAt.Valid {
		return state, "missing_observation_token", nil
	}
	if candidate.TargetKey != "" && !candidate.TargetLastSeenAt.Valid {
		return state, "missing_observation_token", nil
	}
	if !sameExpectedObservation(candidate.LastSeenAt.String, alert.LastSeenAt) {
		return state, "stale_alert_observation", nil
	}
	if candidate.TargetKey == "" {
		switch {
		case alert.IsResolved:
			return state, "already_resolved", nil
		case alert.IsSuppressed:
			return state, "already_suppressed", nil
		case !recheckAlertOrTargetEligible(alert.State, alert.IsLive, alert.IsResolved, alert.IsSuppressed):
			return state, "ineligible_state", nil
		default:
			return state, "", nil
		}
	}

	targetRows, err := readTargetStatesTx(tx, candidate.AlertID)
	if err != nil {
		return state, "", err
	}
	target, found := targetRows[candidate.TargetKey]
	if !found {
		return state, "missing_current_target", nil
	}
	if target.RetiredAt != nil {
		return state, "retired_target", nil
	}
	if !sameExpectedObservation(candidate.TargetLastSeenAt.String, target.LastSeenAt) {
		return state, "stale_target_observation", nil
	}
	switch {
	case alert.IsResolved || target.IsResolved:
		return state, "already_resolved", nil
	case alert.IsSuppressed || target.IsSuppressed:
		return state, "already_suppressed", nil
	case !recheckAlertOrTargetEligible(target.State, target.IsLive, target.IsResolved, target.IsSuppressed):
		return state, "ineligible_state", nil
	default:
		state.targetRows = targetRows
		state.target = target
		return state, "", nil
	}
}

func processRecheckCandidateTx(tx *sql.Tx, candidate recheckCandidate, rules []recheckJobRule, sourceStatuses map[recheckSourceReference]string, jobID int64, now time.Time, postCASReadError func(string) error) (recheckCandidateDecision, error) {
	decision := recheckCandidateDecision{}
	state, stateOutcome, err := classifyRecheckCandidateStateTx(tx, candidate)
	if err != nil {
		return decision, err
	}
	if stateOutcome != "" {
		decision.outcome = stateOutcome
		return decision, nil
	}
	alert := state.alert

	labels, outcome := pinnedCandidateLabels(candidate.MatchLabelsJSON, candidate.MatchLabelsValid)
	if outcome != "" {
		decision.outcome = outcome
		return decision, nil
	}
	if candidate.TargetKey == "" {
		relevant, relevantErr := parseRelevantSourceReferences(candidate.RelevantSourcesJSON, "")
		if relevantErr != nil {
			decision.outcome = "incomplete_source_coverage"
			return decision, nil
		}
		matched, complete := evaluateRecheckRules(rules, sourceStatuses, nil, relevant, labels)
		if !complete {
			decision.outcome = "incomplete_source_coverage"
			return decision, nil
		}
		if len(matched) == 0 {
			decision.outcome = "no_matching_rule"
			return decision, nil
		}
		decisionCandidate := candidate
		decisionCandidate.RawState = string(alert.RawState)
		updated, transitionID, promoteErr := promoteAlertSuppressedCAS(tx, decisionCandidate, now)
		if promoteErr != nil {
			return decision, promoteErr
		}
		if !updated {
			decision.outcome, err = classifyGlobalAfterCASFailureTx(tx, candidate.AlertID, candidate.LastSeenAt.String, postCASReadError)
			if err != nil {
				return decision, err
			}
			return decision, nil
		}
		if err = insertSuppressionCauseHistoryTx(tx, candidate.AlertID, transitionID, jobID, now, matched); err != nil {
			return decision, err
		}
		decision.outcome = "promoted"
		decision.promoted = true
		decision.events = append(decision.events, runtimeEvent{level: slog.LevelDebug, component: "silence_recheck", event: "candidate_promoted", message: "Silence recheck candidate promoted", attrs: []slog.Attr{slog.Int64("job_id", jobID), slog.String("alert_id", candidate.AlertID)}})
		return decision, nil
	}

	targetRows := state.targetRows
	target := state.target
	if !candidate.TargetSourceMembersJSON.Valid {
		decision.outcome = "incomplete_source_coverage"
		return decision, nil
	}
	var members []string
	if err = json.Unmarshal([]byte(candidate.TargetSourceMembersJSON.String), &members); err != nil || len(members) == 0 {
		decision.outcome = "incomplete_source_coverage"
		return decision, nil
	}
	targetRelevant := sourceReferencesFromMembers(members, candidate.TargetKey)
	matched, complete := evaluateRecheckRules(rules, sourceStatuses, &candidate.TargetKey, targetRelevant, labels)
	if !complete {
		decision.outcome = "incomplete_source_coverage"
		return decision, nil
	}
	if len(matched) == 0 {
		decision.outcome = "no_matching_rule"
		return decision, nil
	}
	target.LastSeenAt = parseOptionalTime(candidate.TargetLastSeenAt.String)
	applied, eventID, promoteErr := promoteTargetSuppressedCASTx(tx, candidate.AlertID, target, now, "silence_match_promoted")
	if promoteErr != nil {
		return decision, promoteErr
	}
	if !applied {
		decision.outcome, err = classifyTargetAfterCASFailureTx(tx, candidate.AlertID, candidate.TargetKey, candidate.LastSeenAt.String, candidate.TargetLastSeenAt.String, postCASReadError)
		if err != nil {
			return decision, err
		}
		return decision, nil
	}
	historyID, promoteErr := insertTargetRecheckSuppressionCauseTx(tx, candidate.AlertID, candidate.TargetKey, eventID, jobID, now, matched)
	if promoteErr != nil {
		return decision, promoteErr
	}
	decision.events = append(decision.events, runtimeEvent{level: slog.LevelDebug, component: "silence_recheck", event: "candidate_promoted", message: "Silence recheck target candidate promoted", attrs: []slog.Attr{slog.Int64("job_id", jobID), slog.String("alert_id", candidate.AlertID), slog.String("target", candidate.TargetKey)}})
	decision.outcome = "promoted"
	decision.promoted = true
	target.IsSuppressed = true
	target.State = StateSuppressed
	targetRows[candidate.TargetKey] = target

	allTargetsSuppressed := true
	for _, row := range targetRows {
		if row.RetiredAt != nil {
			continue
		}
		if !row.IsSuppressed {
			allTargetsSuppressed = false
			break
		}
	}
	if allTargetsSuppressed {
		globalCandidate := candidate
		globalCandidate.TargetKey = ""
		globalCandidate.RawState = string(alert.RawState)
		updated, transitionID, globalErr := promoteAlertSuppressedCAS(tx, globalCandidate, now)
		if globalErr != nil {
			return decision, globalErr
		}
		if updated {
			targetHistoryIDs := []int64{historyID}
			for targetName, row := range targetRows {
				if targetName == candidate.TargetKey || !row.IsSuppressed {
					continue
				}
				if existingHistoryID, found, historyErr := latestTargetSuppressionCauseHistoryTx(tx, candidate.AlertID, targetName); historyErr != nil {
					return decision, historyErr
				} else if found {
					targetHistoryIDs = append(targetHistoryIDs, existingHistoryID)
				}
			}
			if globalErr = insertTargetAggregateSuppressionCauseTx(tx, candidate.AlertID, transitionID, now, targetHistoryIDs); globalErr != nil {
				return decision, globalErr
			}
		} else {
			if postCASReadError != nil {
				if err = postCASReadError("aggregate_global"); err != nil {
					return decision, err
				}
			}
			_, aggregateOutcome, classifyErr := classifyRecheckCandidateStateTx(tx, recheckCandidate{
				AlertID:    candidate.AlertID,
				LastSeenAt: candidate.LastSeenAt,
			})
			if classifyErr != nil {
				return decision, classifyErr
			}
			if aggregateOutcome == "" {
				return decision, fmt.Errorf("%w: aggregate global CAS affected zero rows without a classified state", ErrWriterOperationInvariant)
			}
		}
	}
	return decision, nil
}

func pinnedCandidateLabels(raw string, valid bool) (map[string]string, string) {
	if !valid || strings.TrimSpace(raw) == "" {
		return nil, "missing_labels"
	}
	labels := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &labels); err != nil {
		return nil, "invalid_labels"
	}
	return labels, ""
}

func classifyGlobalAfterCASFailureTx(tx *sql.Tx, alertID, expected string, postCASReadError ...func(string) error) (string, error) {
	if len(postCASReadError) > 0 && postCASReadError[0] != nil {
		if err := postCASReadError[0]("primary_global"); err != nil {
			return "", err
		}
	}
	_, outcome, err := classifyRecheckCandidateStateTx(tx, recheckCandidate{
		AlertID:    alertID,
		LastSeenAt: sql.NullString{String: expected, Valid: expected != ""},
	})
	if err != nil {
		return "", err
	}
	if outcome == "" {
		return "", fmt.Errorf("%w: global post-CAS zero-row result has no classified state", ErrWriterOperationInvariant)
	}
	return outcome, nil
}

func classifyTargetAfterCASFailureTx(tx *sql.Tx, alertID, targetKey, expectedAlert, expectedTarget string, postCASReadError ...func(string) error) (string, error) {
	if len(postCASReadError) > 0 && postCASReadError[0] != nil {
		if err := postCASReadError[0]("primary_target"); err != nil {
			return "", err
		}
	}
	_, outcome, err := classifyRecheckCandidateStateTx(tx, recheckCandidate{
		AlertID:          alertID,
		TargetKey:        targetKey,
		LastSeenAt:       sql.NullString{String: expectedAlert, Valid: expectedAlert != ""},
		TargetLastSeenAt: sql.NullString{String: expectedTarget, Valid: expectedTarget != ""},
	})
	if err != nil {
		return "", err
	}
	if outcome == "" {
		return "", fmt.Errorf("%w: target post-CAS zero-row result has no classified state", ErrWriterOperationInvariant)
	}
	return outcome, nil
}

func validateRecheckCounterSnapshot(processed, total, promoted, missingLabels, ineligible, incompleteSources, stale int) error {
	if processed < 0 || total < 0 || processed > total {
		return fmt.Errorf("invalid recheck counter bounds: processed=%d total=%d", processed, total)
	}
	if processed != promoted+missingLabels+ineligible+incompleteSources+stale {
		return fmt.Errorf("recheck counter invariant mismatch: processed=%d promoted=%d missing_labels=%d ineligible=%d incomplete_sources=%d stale=%d", processed, promoted, missingLabels, ineligible, incompleteSources, stale)
	}
	return nil
}

type recheckJobAccounting struct {
	Processed     int
	Total         int
	Promoted      int
	MissingLabels int
	Ineligible    int
	Incomplete    int
	Stale         int
	Pending       int
}

func validateRecheckJobAccountingTx(tx *sql.Tx, jobID int64) (recheckJobAccounting, bool, error) {
	var accounting recheckJobAccounting
	if err := tx.QueryRow(`
SELECT processed_candidates, total_candidates, promoted_candidates,
       skipped_missing_labels, skipped_ineligible, skipped_incomplete_sources, stale_conflicts
FROM silence_recheck_jobs WHERE job_id=?
`, jobID).Scan(&accounting.Processed, &accounting.Total, &accounting.Promoted, &accounting.MissingLabels, &accounting.Ineligible, &accounting.Incomplete, &accounting.Stale); err != nil {
		return accounting, false, fmt.Errorf("failed to read recheck job accounting: %w", err)
	}
	if err := tx.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=? AND processed_at IS NULL AND outcome IS NULL`, jobID).Scan(&accounting.Pending); err != nil {
		return accounting, false, fmt.Errorf("failed to count pending recheck candidates: %w", err)
	}
	if err := validateRecheckCounterSnapshot(accounting.Processed, accounting.Total, accounting.Promoted, accounting.MissingLabels, accounting.Ineligible, accounting.Incomplete, accounting.Stale); err != nil {
		return accounting, false, err
	}
	if accounting.Processed == accounting.Total {
		if accounting.Pending != 0 {
			return accounting, false, fmt.Errorf("recheck job %d has %d pending candidates after reaching total", jobID, accounting.Pending)
		}
		return accounting, true, nil
	}
	if accounting.Pending == 0 {
		return accounting, false, fmt.Errorf("recheck job %d has no pending candidates before reaching total: processed=%d total=%d", jobID, accounting.Processed, accounting.Total)
	}
	return accounting, false, nil
}

func updateRecheckCandidateTerminalTx(tx *sql.Tx, jobID int64, candidate recheckCandidate, now time.Time, outcome string) error {
	result, err := tx.Exec(`
UPDATE silence_recheck_job_candidates
SET processed_at=?, outcome=?
WHERE job_id=? AND alert_id=? AND target_key=? AND processed_at IS NULL AND outcome IS NULL
`, now.UTC().Format(time.RFC3339Nano), outcome, jobID, candidate.AlertID, candidate.TargetKey)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("recheck candidate terminal update affected %d rows", affected)
	}
	return nil
}

func updateRecheckCandidateTerminalAndCounterTx(tx *sql.Tx, jobID int64, candidate recheckCandidate, now time.Time, outcome string) error {
	if err := updateRecheckCandidateTerminalTx(tx, jobID, candidate, now, outcome); err != nil {
		return err
	}
	counterColumn := "skipped_ineligible"
	switch outcome {
	case "promoted":
		counterColumn = "promoted_candidates"
	case "stale_alert_observation", "stale_target_observation":
		counterColumn = "stale_conflicts"
	case "missing_labels", "invalid_labels":
		counterColumn = "skipped_missing_labels"
	case "incomplete_source_coverage":
		counterColumn = "skipped_incomplete_sources"
	}
	result, err := tx.Exec(fmt.Sprintf(`
UPDATE silence_recheck_jobs
SET cursor_alert_id=?, processed_candidates=processed_candidates+1, %s=%s+1
WHERE job_id=?
`, counterColumn, counterColumn), candidate.AlertID, jobID)
	if err != nil {
		return fmt.Errorf("failed to update recheck parent counter: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read recheck parent counter affected rows: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("recheck parent counter update affected %d rows", affected)
	}
	return nil
}

func sameExpectedObservation(expected string, current *time.Time) bool {
	if current == nil {
		return false
	}
	return current.UTC().Format(time.RFC3339Nano) == expected
}

func parseOptionalTime(raw string) *time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return nil
	}
	parsed = parsed.UTC()
	return &parsed
}
