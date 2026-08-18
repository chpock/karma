package alertlifecycle

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestProblem2SchemaContainsImmutableJobInputAndCombinedRetryTables(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for table, columns := range map[string][]string{
		"silence_recheck_jobs": {
			"source_context_fingerprint", "job_fingerprint", "trigger_type", "promoted_candidates",
			"stale_conflicts", "skipped_ineligible", "skipped_missing_labels", "skipped_incomplete_sources",
			"attempt_count", "retry_count", "consecutive_retry_count", "next_attempt_at",
			"first_error_message", "first_error_class", "first_error_stage", "first_error_at",
			"last_error_message", "last_error_class", "last_error_stage", "last_error_at",
			"recovery_count", "abandoned_at", "abandoned_by", "abandon_reason",
		},
		"silence_recheck_job_candidates": {
			"target_key", "expected_alert_last_seen_at", "expected_target_last_seen_at",
			"match_labels_json", "relevant_sources_json", "target_source_members_json", "outcome",
		},
		"silence_recheck_job_rules": {
			"job_id", "source_name", "cluster_id", "target_key", "rule_id", "revision_id",
			"silence_id", "content_fingerprint", "observed_state", "evaluation_fingerprint", "matchers_json",
		},
	} {
		got := problem2TableColumns(t, store.db, table)
		for _, column := range columns {
			if !got[column] {
				t.Errorf("table %s is missing immutable Problem 2/3 column %s", table, column)
			}
		}
	}
	for _, table := range []string{"silence_recheck_job_attempts", "silence_recheck_job_operator_actions"} {
		var count int
		if err = store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Errorf("combined schema is missing table %s", table)
		}
	}

	rows, err := store.db.Query(`PRAGMA foreign_key_list('silence_recheck_job_candidates')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, sequence int
		var table, from, to, onUpdate, onDelete, match string
		if err = rows.Scan(&id, &sequence, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatal(err)
		}
		if table == "alert_states" || table == "alert_target_states" {
			t.Errorf("candidate input still references mutable table %s with %s delete", table, onDelete)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestProblem2SchemaEnforcesInputConstraintsAndIndexes(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err = store.db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,created_at) VALUES('sha256:ctx','sha256:job','{"formatVersion":1,"sources":[],"clusters":[]}','source_context_changed','queued',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,created_at) VALUES('sha256:ctx2','sha256:job2','{}','source_changed','queued',?)`, now); err == nil {
		t.Fatal("invalid recheck trigger was accepted")
	}
	if _, err = store.db.Exec(`UPDATE silence_recheck_jobs SET processed_candidates=1 WHERE job_id=1`); err == nil {
		t.Fatal("inconsistent parent counters were accepted")
	}
	if _, err = store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,target_key,target_source_members_json) VALUES(1,'target-alert','cluster-a',NULL)`); err == nil {
		t.Fatal("target candidate without source members was accepted")
	}
	if _, err = store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,outcome) VALUES(1,'unprocessed','promoted')`); err == nil {
		t.Fatal("terminal outcome without processed timestamp was accepted")
	}
	if _, err = store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json,processed_at,outcome) VALUES(1,'target-alert','cluster-a',?,?,?, ?,?,?,'no_matching_rule')`, now, now, `{"service":"api"}`, `[{"name":"am-a","cluster":"cluster-a"}]`, `["am-a"]`, now); err != nil {
		t.Fatal(err)
	}
	indexes := map[string]bool{}
	rows, err := store.db.Query(`PRAGMA index_list('silence_recheck_job_candidates')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var sequence, unique, partial int
		var origin string
		var name string
		if err = rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			t.Fatal(err)
		}
		indexes[name] = true
	}
	if !indexes["idx_recheck_job_candidates_pending"] {
		t.Fatal("pending candidate index is missing")
	}
	for table, required := range map[string][]string{
		"silence_recheck_jobs":           {"job_fingerprint", "processed_candidates <= total_candidates", "status IN ('queued'"},
		"silence_recheck_job_rules":      {"observed_state IN ('pending', 'active', 'expired')", "FOREIGN KEY (revision_id, rule_id)"},
		"silence_recheck_job_candidates": {"target_source_members_json IS NULL", "outcome IS NULL OR outcome IN"},
	} {
		var ddl string
		if err = store.db.QueryRow(`SELECT sql FROM sqlite_schema WHERE type='table' AND name=?`, table).Scan(&ddl); err != nil {
			t.Fatal(err)
		}
		for _, fragment := range required {
			if !strings.Contains(ddl, fragment) {
				t.Errorf("table %s is missing schema contract fragment %q", table, fragment)
			}
		}
	}
}

func TestProblem2UpsertCurrentAlertDoesNotAttachCandidateToOpenJob(t *testing.T) {
	store, alertID, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })

	labels := `{"service":"api"}`
	newAlertID := alertID + "-late"
	if err := store.UpsertCurrentAlert(AlertCurrent{
		AlertID: newAlertID, State: StateInactive, RawState: StateActive, IsLive: false,
		Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now, LastSeenAt: &now,
		RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`,
	}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("compatibility upsert attached a candidate to an open job: count=%d", count)
	}
}

func TestProblem2CandidateSnapshotSurvivesAlertEviction(t *testing.T) {
	store, _, _ := seedProblem2OpenJobAt(t, time.Now().UTC().Add(-2*time.Hour))
	t.Cleanup(func() { _ = store.Close() })

	if _, err := store.RunHousekeeping(time.Hour, 100); err != nil {
		t.Fatal(err)
	}
	var alerts, candidates int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM alert_states`).Scan(&alerts); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates`).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if alerts != 0 || candidates != 1 {
		t.Fatalf("immutable candidate was deleted with current alert: alerts=%d candidates=%d", alerts, candidates)
	}
}

func TestProblem2WorkerRecordsOneTerminalOutcomePerCandidate(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })

	promoted, err := store.RunSilenceRecheckBatch(100)
	if err != nil {
		t.Fatal(err)
	}
	if promoted != 1 {
		t.Fatalf("expected one promoted candidate, got=%d", promoted)
	}
	var outcome string
	if err = store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates`).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "promoted" {
		t.Fatalf("candidate terminal outcome=%q", outcome)
	}
	var processed, total, promotedCandidates, stale, ineligible, missing, incomplete int
	if err = store.db.QueryRow(`SELECT processed_candidates, total_candidates, promoted_candidates, stale_conflicts, skipped_ineligible, skipped_missing_labels, skipped_incomplete_sources FROM silence_recheck_jobs`).Scan(&processed, &total, &promotedCandidates, &stale, &ineligible, &missing, &incomplete); err != nil {
		t.Fatal(err)
	}
	if processed != total || processed != promotedCandidates+stale+ineligible+missing+incomplete {
		t.Fatalf("candidate accounting invariant failed: processed=%d total=%d counters=%d/%d/%d/%d/%d", processed, total, promotedCandidates, stale, ineligible, missing, incomplete)
	}
}

func TestProblem2RestartResumesPinnedMembershipBetweenBatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	for _, alertID := range []string{"problem2-resume-a", "problem2-resume-b"} {
		if err = store.UpsertCurrentAlert(AlertCurrent{
			AlertID: alertID, State: StateInactive, RawState: StateActive, Receiver: "team", GroupID: "group",
			GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
			StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `["am-a"]`,
			RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: now,
		}); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	silence := SourceSilenceSnapshot{SilenceID: "problem2-resume-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}}}
	if _, err = store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}}, Timestamp: now}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err = store.RunSilenceRecheckBatch(1); err != nil {
		t.Fatal(err)
	}
	before := captureProblem2RestartPinnedSnapshot(t, store.db)
	beforeTerminal := captureProblem2CandidateRuntimeState(t, store.db)
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	after := captureProblem2RestartPinnedSnapshot(t, store.db)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("reopen changed pinned input: before=%+v after=%+v", before, after)
	}
	if reopenedTerminal := captureProblem2CandidateRuntimeState(t, store.db); !reflect.DeepEqual(beforeTerminal, reopenedTerminal) {
		t.Fatalf("reopen changed terminal candidate state: before=%+v after=%+v", beforeTerminal, reopenedTerminal)
	}
	if _, err = store.RunSilenceRecheckBatch(1); err != nil {
		t.Fatal(err)
	}
	var status string
	if err = store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("restarted job status=%q", status)
	}
	var processed, total int
	if err = store.db.QueryRow(`SELECT processed_candidates,total_candidates FROM silence_recheck_jobs WHERE job_id=1`).Scan(&processed, &total); err != nil {
		t.Fatal(err)
	}
	if processed != total || processed != 2 {
		t.Fatalf("restarted membership progress=%d/%d", processed, total)
	}
	afterTerminal := captureProblem2CandidateRuntimeState(t, store.db)
	for alertID, beforeState := range beforeTerminal {
		afterState, found := afterTerminal[alertID]
		if !found || beforeState.Outcome != "" && afterState != beforeState {
			t.Fatalf("restarted worker replayed or changed terminal candidate %s: before=%+v after=%+v", alertID, beforeState, afterState)
		}
	}
	if len(afterTerminal) != 2 {
		t.Fatalf("restarted worker terminal candidate count=%d", len(afterTerminal))
	}
}

type problem2CandidateRuntimeState struct {
	ProcessedAt string
	Outcome     string
}

func captureProblem2CandidateRuntimeState(t *testing.T, db *sql.DB) map[string]problem2CandidateRuntimeState {
	t.Helper()
	rows, err := db.Query(`SELECT alert_id,COALESCE(processed_at,''),COALESCE(outcome,'') FROM silence_recheck_job_candidates WHERE job_id=1 ORDER BY alert_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := map[string]problem2CandidateRuntimeState{}
	for rows.Next() {
		var alertID string
		var state problem2CandidateRuntimeState
		if err = rows.Scan(&alertID, &state.ProcessedAt, &state.Outcome); err != nil {
			t.Fatal(err)
		}
		result[alertID] = state
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestProblem2MissingCurrentAlertBecomesTerminalCandidateOutcome(t *testing.T) {
	store, alertID, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })

	if _, err := store.db.Exec(`DELETE FROM alert_states WHERE alert_id=?`, alertID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunSilenceRecheckBatch(100); err != nil {
		t.Fatal(err)
	}
	var outcome string
	if err := store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates`).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "missing_current_alert" {
		t.Fatalf("missing current alert outcome=%q", outcome)
	}
}

func TestProblem2GlobalObservationTokenRejectsNewLifecycleEpisode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "episode-alert", State: StateInactive, RawState: StateActive, Receiver: "team", GroupID: "group",
		GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `["am-a"]`,
		RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	silence := SourceSilenceSnapshot{SilenceID: "episode-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}}}
	if _, err = store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}}, Timestamp: now}); err != nil {
		t.Fatal(err)
	}
	refired := now.Add(time.Minute).Format(time.RFC3339Nano)
	if _, err = store.db.Exec(`UPDATE alert_states SET state='active', is_live=1, last_seen_at=?, updated_at=? WHERE alert_id=?`, refired, refired, "episode-alert"); err != nil {
		t.Fatal(err)
	}
	newEpisode := now.Add(2 * time.Minute).Format(time.RFC3339Nano)
	if _, err = store.db.Exec(`UPDATE alert_states SET state='inactive', is_live=0, last_seen_at=?, updated_at=? WHERE alert_id=?`, newEpisode, newEpisode, "episode-alert"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RunSilenceRecheckBatch(100); err != nil {
		t.Fatal(err)
	}
	var outcome string
	if err = store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates WHERE job_id=1`).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "stale_alert_observation" {
		t.Fatalf("old lifecycle episode outcome=%q", outcome)
	}
	current, err := store.GetCurrentAlert("episode-alert")
	if err != nil || current == nil || current.IsSuppressed {
		t.Fatalf("old job suppressed newer lifecycle episode: current=%+v err=%v", current, err)
	}
}

func TestProblem2TargetObservationTokenIsIndependent(t *testing.T) {
	store, targetAt := seedProblem2TargetJob(t)
	t.Cleanup(func() { _ = store.Close() })
	newTargetAt := targetAt.Add(time.Minute).Format(time.RFC3339Nano)
	if _, err := store.db.Exec(`UPDATE alert_target_states SET state='active', is_live=1, last_seen_at=?, updated_at=? WHERE alert_id='target-episode' AND target='cluster-a'`, newTargetAt, newTargetAt); err != nil {
		t.Fatal(err)
	}
	latestTargetAt := targetAt.Add(2 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.db.Exec(`UPDATE alert_target_states SET state='inactive', is_live=0, last_seen_at=?, updated_at=? WHERE alert_id='target-episode' AND target='cluster-a'`, latestTargetAt, latestTargetAt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunSilenceRecheckBatch(100); err != nil {
		t.Fatal(err)
	}
	var outcome string
	if err := store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates WHERE target_key='cluster-a'`).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "stale_target_observation" {
		t.Fatalf("old target lifecycle episode outcome=%q", outcome)
	}
}

func TestProblem2CanonicalJobFingerprintIgnoresInputOrderAndPreservesCaller(t *testing.T) {
	labels := `{"b":"2","a":"1"}`
	first := recheckJobInput{
		TriggerType:   "alert_became_eligible",
		SourceContext: recheckSourceContext{FormatVersion: 1, Sources: []recheckSourceContextEntry{{SourceName: "b"}, {SourceName: "a"}}},
		Rules: []recheckJobRuleInput{
			{SourceName: "b", TargetKey: "", RevisionID: 2, MatchersJSON: `[{"name":"z","value":"2"},{"name":"a","value":"1"}]`},
			{SourceName: "a", TargetKey: "", RevisionID: 1, MatchersJSON: `[]`},
		},
		Candidates: []recheckCandidateInput{{AlertID: "b", ExpectedAlertLastSeenAt: stringPointerForProblem2("2026-08-17T00:00:00+02:00"), MatchLabelsJSON: &labels, RelevantSourcesJSON: `[{"name":"b","cluster":"b"},{"name":"a","cluster":"a"}]`, TargetSourceMembersJSON: stringPointerForProblem2(`["b","a"]`), TargetKey: "target"}, {AlertID: "a"}},
	}
	beforeRaw, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.SourceContext.Sources = []recheckSourceContextEntry{{SourceName: "a"}, {SourceName: "b"}}
	second.Rules = []recheckJobRuleInput{first.Rules[1], first.Rules[0]}
	second.Candidates = []recheckCandidateInput{first.Candidates[1], first.Candidates[0]}
	second.Candidates[1].MatchLabelsJSON = stringPointerForProblem2(`{"a":"1","b":"2"}`)
	second.Candidates[1].RelevantSourcesJSON = `[{"name":"a","cluster":"a"},{"name":"b","cluster":"b"}]`
	second.Candidates[1].TargetSourceMembersJSON = stringPointerForProblem2(`["a","b"]`)
	firstCanonical, _, _, firstFingerprint, err := canonicalRecheckJobInput(first)
	if err != nil {
		t.Fatal(err)
	}
	secondCanonical, _, _, secondFingerprint, err := canonicalRecheckJobInput(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstFingerprint != secondFingerprint || string(firstCanonical) != string(secondCanonical) {
		t.Fatalf("permuted input changed canonical fingerprint: first=%s second=%s", firstCanonical, secondCanonical)
	}
	if firstFingerprint != "sha256:a801bfb6a86d3fe0a0f874150c5ac0bb967d87473ff75f5d600a3b71244b60a9" {
		t.Fatalf("canonical fingerprint vector changed: %s", firstFingerprint)
	}
	afterRaw, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeRaw, afterRaw) {
		t.Fatalf("canonicalization mutated caller input: before=%s after=%s", beforeRaw, afterRaw)
	}
}

func TestProblem2CreatorDuplicateIsIdempotent(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	var duplicate recheckJobCreateResult
	value, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (recheckJobCreateResult, error) {
		return createRecheckJobFromFinalStateTx(tx, "source_context_changed", nil, now, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate = value
	if duplicate.Created {
		t.Fatal("duplicate creator request created a second job")
	}
	var jobs, rules, candidates int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_rules`).Scan(&rules); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates`).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || rules != 1 || candidates != 1 {
		t.Fatalf("duplicate creator changed children: jobs=%d rules=%d candidates=%d", jobs, rules, candidates)
	}
}

func TestProblem2EligibilitySuccessorContainsOnlyExactTargetScopes(t *testing.T) {
	store, now := seedProblem2TargetJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`INSERT INTO alert_target_states(alert_id,target,state,raw_state,is_live,is_suppressed,is_resolved,starts_at,last_seen_at,source_members_json,updated_at) VALUES('target-episode','cluster-b',?,?,?,?,?,?,?,?,?)`, string(StateInactive), string(StateActive), 0, 0, 0, now.Add(-time.Hour).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), `[{"name":"am-b","cluster":"cluster-b"}]`, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	value, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (int, error) {
		input, buildErr := buildRecheckJobInputTx(tx, "alert_became_eligible", []recheckCandidateScope{{AlertID: "target-episode", TargetKey: "cluster-a"}})
		if buildErr != nil {
			return 0, buildErr
		}
		return len(input.Candidates), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if value != 1 {
		t.Fatalf("targeted successor included unrelated targets: candidates=%d", value)
	}
}

func seedProblem2OpenJob(t *testing.T) (*Store, string, time.Time) {
	return seedProblem2OpenJobAt(t, time.Now().UTC().Truncate(time.Millisecond))
}

func seedProblem2OpenJobAt(t *testing.T, now time.Time) (*Store, string, time.Time) {
	t.Helper()
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	labels := `{"service":"api"}`
	alertID := "problem2-alert"
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: alertID, State: StateInactive, RawState: StateActive, IsLive: false,
		Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
		RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: now,
	}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	silence := SourceSilenceSnapshot{
		SilenceID: "problem2-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}
	result, err := store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}},
		Timestamp:           now,
	})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if result.RecheckJobsQueued != 1 {
		store.Close()
		t.Fatalf("expected one initial Problem 2 job, got=%+v", result)
	}
	return store, alertID, now
}

func seedProblem2TargetJob(t *testing.T) (*Store, time.Time) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	labels := `{"service":"api"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "target-episode", State: StateInactive, RawState: StateActive, Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: now}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	tx, err := store.db.Begin()
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err = upsertTargetStateTx(tx, "target-episode", targetStateRow{Target: "cluster-a", State: StateInactive, RawState: StateActive, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, SourceMembers: []string{"am-a"}}, "seed", now); err != nil {
		_ = tx.Rollback()
		store.Close()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		store.Close()
		t.Fatal(err)
	}
	silence := SourceSilenceSnapshot{SilenceID: "target-episode-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}}}
	if _, err = store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}}, Timestamp: now}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	return store, now
}

func stringPointerForProblem2(value string) *string { return &value }

func problem2TableColumns(t *testing.T, db *sql.DB, table string) map[string]bool {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err = rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns[name] = true
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return columns
}
