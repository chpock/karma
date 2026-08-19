package alertlifecycle

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProblem2GlobalCandidateRequiresPinnedSourceCluster(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "cluster-reassignment-alert", State: StateInactive, RawState: StateActive, Receiver: "team", GroupID: "group",
		GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `["am-a"]`,
		RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	silence := SourceSilenceSnapshot{SilenceID: "cluster-reassignment-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}}}
	if _, err = store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-b", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}}, Timestamp: now}); err != nil {
		t.Fatal(err)
	}

	promoted, err := store.RunSilenceRecheckBatch(100)
	if err != nil {
		t.Fatal(err)
	}
	if promoted != 0 {
		t.Fatalf("global candidate crossed pinned source cluster: promoted=%d", promoted)
	}
	var outcome string
	if err = store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates`).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "incomplete_source_coverage" {
		t.Fatalf("cross-cluster global outcome=%q", outcome)
	}
	var incomplete int
	if err = store.db.QueryRow(`SELECT skipped_incomplete_sources FROM silence_recheck_jobs`).Scan(&incomplete); err != nil {
		t.Fatal(err)
	}
	if incomplete != 1 {
		t.Fatalf("cross-cluster global incomplete counter=%d", incomplete)
	}
	var status string
	var processed, total, pending int
	if err = store.db.QueryRow(`SELECT status,processed_candidates,total_candidates FROM silence_recheck_jobs`).Scan(&status, &processed, &total); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE processed_at IS NULL AND outcome IS NULL`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || processed != total || pending != 0 {
		t.Fatalf("cross-cluster global accounting status=%s processed=%d total=%d pending=%d", status, processed, total, pending)
	}
	current, err := store.GetCurrentAlert("cluster-reassignment-alert")
	if err != nil || current == nil || current.IsSuppressed {
		t.Fatalf("cross-cluster rule suppressed alert: current=%+v err=%v", current, err)
	}
}

func TestProblem2TargetCandidateRequiresPinnedSourceClusterCompleteness(t *testing.T) {
	store, _ := seedProblem2TargetJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET source_context_json=REPLACE(source_context_json,'cluster-a','cluster-b') WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	promoted, err := store.RunSilenceRecheckBatch(100)
	if err != nil {
		t.Fatal(err)
	}
	if promoted != 0 {
		t.Fatalf("cross-cluster target candidate promoted=%d", promoted)
	}
	var outcome string
	if err = store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates`).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "incomplete_source_coverage" {
		t.Fatalf("cross-cluster target outcome=%q", outcome)
	}
	var incomplete int
	if err = store.db.QueryRow(`SELECT skipped_incomplete_sources FROM silence_recheck_jobs`).Scan(&incomplete); err != nil {
		t.Fatal(err)
	}
	if incomplete != 1 {
		t.Fatalf("cross-cluster target incomplete counter=%d", incomplete)
	}
	var status string
	var processed, total, pending int
	if err = store.db.QueryRow(`SELECT status,processed_candidates,total_candidates FROM silence_recheck_jobs`).Scan(&status, &processed, &total); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE processed_at IS NULL AND outcome IS NULL`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || processed != total || pending != 0 {
		t.Fatalf("cross-cluster target accounting status=%s processed=%d total=%d pending=%d", status, processed, total, pending)
	}
}

func TestProblem2ReplayingSourceChangeAndAbsencePollIsIdempotent(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, time.August, 18, 0, 0, 0, 0, time.UTC)
	initial := SourceSilenceSnapshot{
		SilenceID: "replay-initial", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}
	changed := SourceSilenceSnapshot{
		SilenceID: "replay-changed", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "payments", IsEqual: true}},
	}
	live := LiveAlertSnapshot{
		AlertID: "replay-alert", State: StateActive, Receiver: "team", GroupID: "group",
		GroupLabelsJSON: `{}`, LabelsJSON: `{"service":"api"}`, MatchLabelsJSON: stringPointerForProblem2(`{"service":"api"}`),
		AnnotationsJSON: `{}`, StartsAt: now, LastSeenAt: now,
		RelevantUpstreams: []string{"am-a"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-a"}},
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{live}, SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{initial}}},
		Timestamp:      now,
	}); err != nil {
		t.Fatal(err)
	}
	var jobsBeforeSecondPoll int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobsBeforeSecondPoll); err != nil {
		t.Fatal(err)
	}
	secondPoll := PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{changed}}},
		Timestamp:           now.Add(time.Minute),
	}
	result, err := store.ApplyPollCycle(secondPoll)
	if err != nil {
		t.Fatal(err)
	}
	if result.RecheckJobsQueued != 1 || result.RecheckJobsSourceContextChanged != 1 {
		t.Fatalf("source-change poll result=%+v", result)
	}
	var before int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != 2 {
		t.Fatalf("source-change creation count=%d expected 2", before)
	}
	if before-jobsBeforeSecondPoll != result.RecheckJobsQueued {
		t.Fatalf("source-change physical creation delta=%d result queued=%d", before-jobsBeforeSecondPoll, result.RecheckJobsQueued)
	}
	jobsBeforeSecond := before
	beforeBytes := captureProblem2ImmutableBytes(t, store.db)
	result, err = store.ApplyPollCycle(secondPoll)
	if err != nil {
		t.Fatal(err)
	}
	var after int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if result.RecheckJobsQueued != 0 || result.RecheckJobsSourceContextChanged != 0 || result.RecheckJobsAlertBecameEligible != 0 || after != before || after-jobsBeforeSecond != result.RecheckJobsQueued || captureProblem2ImmutableBytes(t, store.db) != beforeBytes {
		t.Fatalf("replayed source-change poll created work: result=%+v jobs before=%d after=%d", result, before, after)
	}
}

func TestProblem2ReplayingTargetEligibilityPollIsIdempotent(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, time.August, 18, 0, 0, 0, 0, time.UTC)
	silence := SourceSilenceSnapshot{
		SilenceID: "target-replay", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}
	live := LiveAlertSnapshot{
		AlertID: "target-replay-alert", State: StateActive, Receiver: "team", GroupID: "group",
		GroupLabelsJSON: `{}`, LabelsJSON: `{"service":"api"}`, MatchLabelsJSON: stringPointerForProblem2(`{"service":"api"}`),
		AnnotationsJSON: `{}`, StartsAt: now, LastSeenAt: now,
		RelevantUpstreams: []string{"am-a"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-a"}},
		Targets: []LiveTargetSnapshot{{Target: "cluster-a", State: StateActive, StartsAt: now, SourceNames: []string{"am-a"}}},
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{live}, SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}}, Timestamp: now,
	}); err != nil {
		t.Fatal(err)
	}
	var jobsBeforeSecondPoll int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobsBeforeSecondPoll); err != nil {
		t.Fatal(err)
	}
	secondPoll := PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}}, Timestamp: now.Add(time.Minute),
	}
	firstTargetResult, err := store.ApplyPollCycle(secondPoll)
	if err != nil {
		t.Fatal(err)
	}
	if firstTargetResult.RecheckJobsQueued != 1 || firstTargetResult.RecheckJobsAlertBecameEligible != 1 || firstTargetResult.RecheckJobsSourceContextChanged != 0 {
		t.Fatalf("target eligibility poll result=%+v", firstTargetResult)
	}
	var before int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != 2 {
		t.Fatalf("target-successor creation count=%d expected 2", before)
	}
	if before-jobsBeforeSecondPoll != firstTargetResult.RecheckJobsQueued {
		t.Fatalf("target-successor physical creation delta=%d result queued=%d", before-jobsBeforeSecondPoll, firstTargetResult.RecheckJobsQueued)
	}
	jobsBeforeSecond := before
	beforeBytes := captureProblem2ImmutableBytes(t, store.db)
	result, err := store.ApplyPollCycle(secondPoll)
	if err != nil {
		t.Fatal(err)
	}
	var after int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if result.RecheckJobsQueued != 0 || result.RecheckJobsSourceContextChanged != 0 || result.RecheckJobsAlertBecameEligible != 0 || after != before || after-jobsBeforeSecond != result.RecheckJobsQueued || captureProblem2ImmutableBytes(t, store.db) != beforeBytes {
		t.Fatalf("replayed target poll created work: result=%+v jobs before=%d after=%d", result, before, after)
	}
}

func TestProblem2CompletionRequiresExactCandidateAccounting(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`DELETE FROM silence_recheck_job_candidates`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=1 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunSilenceRecheckBatch(100); err == nil {
		t.Fatal("worker completed a job with processed_candidates below total_candidates")
	}
	var status string
	if err := store.db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status == "completed" {
		t.Fatal("inconsistent job was marked completed")
	}
}

func TestProblem2CompletionRejectsPendingRowsAfterProcessedTotal(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET processed_candidates=1, promoted_candidates=1 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunSilenceRecheckBatch(100); err == nil {
		t.Fatal("worker ignored a pending row after processed_candidates reached total_candidates")
	}
}

func TestProblem2SchemaRejectsProcessedCandidatesAboveTotal(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=0, processed_candidates=1, promoted_candidates=1 WHERE job_id=1`); err == nil {
		t.Fatal("schema accepted processed_candidates greater than total_candidates")
	}
}

func TestProblem2RuleFingerprintIgnoresLocalIDs(t *testing.T) {
	input := recheckJobInput{
		TriggerType:   "source_context_changed",
		SourceContext: recheckSourceContext{FormatVersion: 1, Generation: 1},
		Rules:         []recheckJobRuleInput{{SourceName: "am-a", ClusterID: "cluster-a", TargetKey: "cluster-a", RuleID: 1, RevisionID: 2, ObservedState: "active", EvaluationFingerprint: "eval-a", MatchersJSON: `[]`}},
	}
	variant := input
	variant.Rules = append([]recheckJobRuleInput(nil), input.Rules...)
	variant.Rules[0].RuleID = 1001
	variant.Rules[0].RevisionID = 2002
	_, _, _, first, err := canonicalRecheckJobInput(input)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, second, err := canonicalRecheckJobInput(variant)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("local rule/revision IDs changed semantic fingerprint: %s != %s", first, second)
	}
}

func TestProblem2CreatorPersistsCanonicalInputItHashes(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	var input recheckJobInput
	if _, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		var loadErr error
		input, loadErr = loadPersistedRecheckJobInputTx(tx, 1)
		return struct{}{}, loadErr
	}); err != nil {
		t.Fatal(err)
	}
	input.SourceContext.Generation = 99
	input.Rules[0].MatchersJSON = `[{"name":"z","value":"2"},{"name":"a","value":"1"}]`
	input.Candidates[0].RelevantSourcesJSON = `[{"name":"am-z","cluster":"cluster-z"},{"name":"am-a","cluster":"cluster-a"}]`
	input.Candidates[0].ExpectedAlertLastSeenAt = stringPointerForProblem2(now.Format(time.RFC3339Nano))
	canonical, _, _, _, err := canonicalRecheckJobInput(input)
	if err != nil {
		t.Fatal(err)
	}
	var created recheckJobCreateResult
	if _, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		var createErr error
		created, createErr = createRecheckJobTx(tx, input, now, nil)
		return struct{}{}, createErr
	}); err != nil {
		t.Fatal(err)
	}
	if !created.Created {
		t.Fatal("canonical persistence test unexpectedly deduplicated")
	}
	var matchers, relevant, expected string
	if err = store.db.QueryRow(`SELECT matchers_json FROM silence_recheck_job_rules WHERE job_id=?`, created.JobID).Scan(&matchers); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT relevant_sources_json, expected_alert_last_seen_at FROM silence_recheck_job_candidates WHERE job_id=?`, created.JobID).Scan(&relevant, &expected); err != nil {
		t.Fatal(err)
	}
	var persisted recheckJobInput
	if err = json.Unmarshal(canonical, &persisted); err != nil {
		t.Fatal(err)
	}
	expectedCanonical := ""
	if persisted.Candidates[0].ExpectedAlertLastSeenAt != nil {
		expectedCanonical = *persisted.Candidates[0].ExpectedAlertLastSeenAt
	}
	if matchers != persisted.Rules[0].MatchersJSON || relevant != persisted.Candidates[0].RelevantSourcesJSON || expected != expectedCanonical {
		t.Fatalf("creator persisted noncanonical input: matchers=%s relevant=%s expected=%s canonical=%s", matchers, relevant, expected, canonical)
	}
}

func TestProblem2DuplicateRejectsCorruptImmutableParent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
	}{
		{name: "total", query: `UPDATE silence_recheck_jobs SET total_candidates=2 WHERE job_id=1`},
		{name: "source fingerprint", query: `UPDATE silence_recheck_jobs SET source_context_fingerprint='sha256:corrupt' WHERE job_id=1`},
		{name: "source context bytes", query: `UPDATE silence_recheck_jobs SET source_context_json=' {"formatVersion":1,"generation":1,"sources":[],"clusters":[]} ' WHERE job_id=1`},
		{name: "trigger", query: `UPDATE silence_recheck_jobs SET trigger_type='alert_became_eligible' WHERE job_id=1`},
		{name: "job fingerprint", query: `UPDATE silence_recheck_jobs SET job_fingerprint='sha256:corrupt' WHERE job_id=1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _, now := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.Exec(tc.query); err != nil {
				t.Fatal(err)
			}
			assertProblem2DuplicateConflict(t, store, func(tx *sql.Tx, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
				return createRecheckJobFromFinalStateTx(tx, "source_context_changed", nil, now, collector)
			})
		})
	}
}

func TestProblem2DuplicateRejectsRuleRowShapeConflicts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
	}{
		{name: "missing rule", query: `DELETE FROM silence_recheck_job_rules WHERE job_id=1`},
		{name: "extra rule", query: `INSERT INTO silence_recheck_job_rules(job_id,source_name,cluster_id,target_key,rule_id,revision_id,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json) SELECT job_id,source_name,cluster_id,'cluster-extra',rule_id,revision_id,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json FROM silence_recheck_job_rules WHERE job_id=1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _, now := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.Exec(tc.query); err != nil {
				t.Fatal(err)
			}
			assertProblem2DuplicateConflict(t, store, func(tx *sql.Tx, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
				return createRecheckJobFromFinalStateTx(tx, "source_context_changed", nil, now, collector)
			})
		})
	}
}

func TestProblem2DuplicateIntegrityMatrix(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *Store)
	}{
		{name: "source context bytes", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET source_context_json=' '||source_context_json||' ' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "source context fingerprint", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET source_context_fingerprint='sha256:drift' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "job fingerprint", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET job_fingerprint='sha256:drift' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "trigger", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET trigger_type='alert_became_eligible' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "total candidates", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=2 WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "rule cluster", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET cluster_id='cluster-drift' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "rule source name", mutate: func(t *testing.T, store *Store) {
			raw := time.Now().UTC().Format(time.RFC3339Nano)
			if _, err := store.db.Exec(`INSERT INTO alertmanager_sources(source_name,current_cluster_id,sync_status,first_configured_at,last_configured_at) VALUES('am-b','cluster-a','success',?,?)`, raw, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET source_name='am-b' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "rule target", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET target_key='target-drift' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "rule silence identity", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET silence_id='silence-drift' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "rule content fingerprint", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET content_fingerprint='content-drift' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "rule observed state", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET observed_state='pending' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "rule evaluation fingerprint", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET evaluation_fingerprint='evaluation-drift' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "rule matcher ordering and whitespace", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET matchers_json=' [ {"name":"service","value":"api","isEqual":true,"isRegex":false} ] ' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "candidate identity", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET alert_id='alert-drift' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "candidate alert token equivalent offset", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET expected_alert_last_seen_at=REPLACE(expected_alert_last_seen_at,'Z','+00:00') WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "candidate target token blank instead of null", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET expected_target_last_seen_at='' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "candidate labels ordering and whitespace", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json=' { "service": "api" } ' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "candidate source references ordering and whitespace", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET relevant_sources_json=' [ {"name":"am-a","cluster":"cluster-a"} ] ' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, _, now := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			input := loadProblem2JobInput(t, store)
			tc.mutate(t, store)
			assertProblem2DuplicateConflict(t, store, func(tx *sql.Tx, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
				return createRecheckJobTx(tx, input, now, collector)
			})
		})
	}
}

func TestProblem2TargetDuplicateIntegrityMatrix(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *Store)
	}{
		{name: "target token equivalent offset", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET expected_target_last_seen_at=REPLACE(expected_target_last_seen_at,'Z','+00:00') WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "target key", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET target_key='cluster-b' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "target source members ordering and whitespace", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET target_source_members_json=' [ "am-a" ] ' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "target source members order", mutate: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET target_source_members_json='["am-b","am-a"]' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := seedProblem2TargetJob(t)
			t.Cleanup(func() { _ = store.Close() })
			input := loadProblem2JobInput(t, store)
			tc.mutate(t, store)
			assertProblem2DuplicateConflict(t, store, func(tx *sql.Tx, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
				return createRecheckJobTx(tx, input, time.Now().UTC(), collector)
			})
		})
	}
}

func TestProblem2DuplicateRejectsOrderingOnlyPersistedByteDrift(t *testing.T) {
	cases := []struct {
		name    string
		reverse func(*testing.T, *Store, recheckJobInput)
		stage   string
	}{
		{name: "matchers", stage: "rule row 0 differs", reverse: func(t *testing.T, store *Store, input recheckJobInput) {
			var ruleID, revisionID int64
			if err := store.db.QueryRow(`SELECT rule_id,revision_id FROM silence_recheck_job_rules WHERE job_id=1 ORDER BY source_name LIMIT 1`).Scan(&ruleID, &revisionID); err != nil {
				t.Fatal(err)
			}
			const reversed = `[{"name":"team","value":"platform","isEqual":true,"isRegex":false},{"name":"service","value":"api","isEqual":true,"isRegex":false}]`
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET matchers_json=? WHERE job_id=1 AND rule_id=?`, reversed, ruleID); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_revisions SET matchers_json=? WHERE revision_id=? AND rule_id=?`, reversed, revisionID, ruleID); err != nil {
				t.Fatal(err)
			}
			assertProblem2RuleProvenance(t, store)
			_ = input
		}},
		{name: "source references", stage: "candidate row 0 differs", reverse: func(t *testing.T, store *Store, _ recheckJobInput) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET relevant_sources_json='[{"name":"am-b","cluster":"cluster-a"},{"name":"am-a","cluster":"cluster-a"}]' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "target source members", stage: "candidate row 0 differs", reverse: func(t *testing.T, store *Store, _ recheckJobInput) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET target_source_members_json='["am-b","am-a"]' WHERE job_id=1`); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := seedProblem2OrderingProductionJob(t)
			t.Cleanup(func() { _ = store.Close() })
			input := loadProblem2JobInput(t, store)
			assertProblem2OrderingProductionFixture(t, store, input)
			tc.reverse(t, store, input)
			assertProblem2DuplicateConflict(t, store, func(tx *sql.Tx, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
				return createRecheckJobTx(tx, input, time.Now().UTC(), collector)
			}, tc.stage)
		})
	}
}

func assertProblem2OrderingProductionFixture(t *testing.T, store *Store, input recheckJobInput) {
	t.Helper()
	if len(input.Rules) < 1 || len(input.Candidates) != 1 {
		t.Fatalf("ordering fixture has unexpected input shape: rules=%d candidates=%d", len(input.Rules), len(input.Candidates))
	}
	var matchers, relevant, members string
	if err := store.db.QueryRow(`SELECT matchers_json FROM silence_recheck_job_rules WHERE job_id=1 ORDER BY source_name LIMIT 1`).Scan(&matchers); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT relevant_sources_json,target_source_members_json FROM silence_recheck_job_candidates WHERE job_id=1`).Scan(&relevant, &members); err != nil {
		t.Fatal(err)
	}
	var parsedMatchers []SilenceMatcherSnapshot
	var parsedSources []SourceRef
	var parsedMembers []string
	if err := json.Unmarshal([]byte(matchers), &parsedMatchers); err != nil || len(parsedMatchers) != 2 {
		t.Fatalf("production creator did not persist two matchers: %q err=%v", matchers, err)
	}
	if err := json.Unmarshal([]byte(relevant), &parsedSources); err != nil || len(parsedSources) != 2 {
		t.Fatalf("production creator did not persist two source references: %q err=%v", relevant, err)
	}
	if err := json.Unmarshal([]byte(members), &parsedMembers); err != nil || len(parsedMembers) != 2 {
		t.Fatalf("production creator did not persist two target members: %q err=%v", members, err)
	}
	canonicalMatchers, err := canonicalMatcherJSON(matchers)
	if err != nil || canonicalMatchers != matchers {
		t.Fatalf("production creator did not persist canonical matchers: actual=%q canonical=%q err=%v", matchers, canonicalMatchers, err)
	}
	if canonical := canonicalSourceReferencesJSON(relevant); canonical != relevant {
		t.Fatalf("production creator did not persist canonical source references: actual=%q canonical=%q", relevant, canonical)
	}
	if canonical, err := canonicalStringSliceJSON(members); err != nil || canonical != members {
		t.Fatalf("production creator did not persist canonical target members: actual=%q canonical=%q err=%v", members, canonical, err)
	}
	assertProblem2RuleProvenance(t, store)
}

func assertProblem2RuleProvenance(t *testing.T, store *Store) {
	t.Helper()
	if _, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		rows, queryErr := tx.Query(`SELECT source_name,cluster_id,target_key,rule_id,revision_id,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json FROM silence_recheck_job_rules WHERE job_id=1 ORDER BY source_name,target_key,silence_id,content_fingerprint,evaluation_fingerprint,matchers_json,rule_id,revision_id`)
		if queryErr != nil {
			return struct{}{}, queryErr
		}
		defer rows.Close()
		for rows.Next() {
			var rule recheckJobRuleInput
			if queryErr = rows.Scan(&rule.SourceName, &rule.ClusterID, &rule.TargetKey, &rule.RuleID, &rule.RevisionID, &rule.SilenceID, &rule.ContentFingerprint, &rule.ObservedState, &rule.EvaluationFingerprint, &rule.MatchersJSON); queryErr != nil {
				return struct{}{}, queryErr
			}
			if queryErr = verifyRecheckJobRuleProvenanceTx(tx, rule); queryErr != nil {
				return struct{}{}, queryErr
			}
		}
		return struct{}{}, rows.Err()
	}); err != nil {
		t.Fatalf("ordering fixture provenance is not valid before candidate comparison: %v", err)
	}
}

func seedProblem2OrderingProductionJob(t *testing.T) (*Store, time.Time) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	labels := `{"service":"api","team":"platform"}`
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "ordering-production-alert", State: StateInactive, RawState: StateActive, IsLive: false,
		Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
		RelevantUpstreams: `["am-a","am-b"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"},{"name":"am-b","cluster":"cluster-a"}]`, UpdatedAt: now,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	tx, err := store.db.Begin()
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err = upsertTargetStateTx(tx, "ordering-production-alert", targetStateRow{
		Target: "cluster-a", State: StateInactive, RawState: StateActive, StartsAt: now.Add(-time.Hour), LastSeenAt: &now,
		SourceMembers: []string{"am-a", "am-b"},
	}, "seed", now); err != nil {
		_ = tx.Rollback()
		_ = store.Close()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	matchers := []SilenceMatcherSnapshot{
		{Name: "service", Value: "api", IsEqual: true},
		{Name: "team", Value: "platform", IsEqual: true},
	}
	silenceA := SourceSilenceSnapshot{SilenceID: "ordering-production-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: matchers}
	silenceB := silenceA
	if _, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true},
		SilenceSources: []SourceSilenceState{
			{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silenceA}},
			{SourceName: "am-b", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silenceB}},
		},
		Timestamp: now,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	var jobs int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobs); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if jobs != 1 {
		_ = store.Close()
		t.Fatalf("ordering production fixture created %d jobs, want 1", jobs)
	}
	return store, now
}

type problem2CapturedEvent struct {
	component string
	event     string
}

type problem2EventHandler struct {
	mu     sync.Mutex
	events []problem2CapturedEvent
}

func (h *problem2EventHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *problem2EventHandler) Handle(_ context.Context, record slog.Record) error {
	captured := problem2CapturedEvent{}
	record.Attrs(func(attr slog.Attr) bool {
		attr.Value = attr.Value.Resolve()
		switch attr.Key {
		case "component":
			captured.component = attr.Value.String()
		case "event":
			captured.event = attr.Value.String()
		}
		return true
	})
	h.mu.Lock()
	h.events = append(h.events, captured)
	h.mu.Unlock()
	return nil
}

func (h *problem2EventHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *problem2EventHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *problem2EventHandler) queuedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, event := range h.events {
		if event.component == "silence_reconciliation" && event.event == "recheck_job_queued" {
			count++
		}
	}
	return count
}

func installProblem2EventCapture(t *testing.T) *problem2EventHandler {
	t.Helper()
	handler := &problem2EventHandler{}
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	})
	return handler
}

func assertProblem2ProductionJobAccounting(t *testing.T, store *Store, beforeJobs, beforeEvents int, result PollCycleResult, wantSource, wantTarget int, handler *problem2EventHandler) {
	t.Helper()
	var afterJobs int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&afterJobs); err != nil {
		t.Fatal(err)
	}
	afterEvents := handler.queuedCount()
	jobDelta := afterJobs - beforeJobs
	eventDelta := afterEvents - beforeEvents
	if result.RecheckJobsQueued != wantSource+wantTarget || result.RecheckJobsSourceContextChanged != wantSource || result.RecheckJobsAlertBecameEligible != wantTarget || jobDelta != result.RecheckJobsQueued || eventDelta != result.RecheckJobsQueued {
		t.Fatalf("production job accounting result=%+v jobDelta=%d eventDelta=%d expected source=%d target=%d", result, jobDelta, eventDelta, wantSource, wantTarget)
	}
}

func TestProblem2ApplyPollCycleCommittedJobEventAccounting(t *testing.T) {
	handler := installProblem2EventCapture(t)
	t.Run("source-context creation duplicate and replay", func(t *testing.T) {
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		now := time.Date(2026, time.August, 18, 2, 0, 0, 0, time.UTC)
		labels := `{"service":"api"}`
		if err = store.UpsertCurrentAlert(AlertCurrent{AlertID: "event-source-alert", State: StateInactive, RawState: StateActive, Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		silence := SourceSilenceSnapshot{SilenceID: "event-source-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}}}
		poll := PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}}, Timestamp: now}
		beforeJobs, beforeEvents := 0, handler.queuedCount()
		result, err := store.ApplyPollCycle(poll)
		if err != nil {
			t.Fatal(err)
		}
		assertProblem2ProductionJobAccounting(t, store, beforeJobs, beforeEvents, result, 1, 0, handler)

		input := loadProblem2JobInput(t, store)
		var collector runtimeEventCollector
		beforeBytes := captureProblem2ImmutableBytes(t, store.db)
		duplicate, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (recheckJobCreateResult, error) {
			return createRecheckJobTx(tx, input, now, &collector)
		})
		if err != nil || duplicate.Created || len(collector.events) != 0 {
			t.Fatalf("healthy duplicate result=%+v err=%v events=%v", duplicate, err, collector.events)
		}
		if captureProblem2ImmutableBytes(t, store.db) != beforeBytes {
			t.Fatal("healthy duplicate changed immutable bytes")
		}

		beforeJobs, beforeEvents = countProblem2Jobs(t, store), handler.queuedCount()
		beforeBytes = captureProblem2ImmutableBytes(t, store.db)
		replay, err := store.ApplyPollCycle(poll)
		if err != nil {
			t.Fatal(err)
		}
		if replay.RecheckJobsQueued != 0 || countProblem2Jobs(t, store) != beforeJobs || handler.queuedCount() != beforeEvents || captureProblem2ImmutableBytes(t, store.db) != beforeBytes {
			t.Fatalf("source replay changed production work: result=%+v jobs=%d/%d events=%d/%d", replay, countProblem2Jobs(t, store), beforeJobs, handler.queuedCount(), beforeEvents)
		}
	})

	t.Run("target successor and replay", func(t *testing.T) {
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		now := time.Date(2026, time.August, 18, 3, 0, 0, 0, time.UTC)
		labels := `{"service":"api"}`
		live := LiveAlertSnapshot{AlertID: "event-target-alert", State: StateActive, Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-a"}}, Targets: []LiveTargetSnapshot{{Target: "cluster-a", State: StateActive, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-a"}}}}
		silence := SourceSilenceSnapshot{SilenceID: "event-target-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}}}
		sources := []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}}
		if _, err = store.ApplyPollCycle(PollCycleInput{LiveAlerts: []LiveAlertSnapshot{live}, SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: sources, Timestamp: now}); err != nil {
			t.Fatal(err)
		}
		beforeJobs, beforeEvents := countProblem2Jobs(t, store), handler.queuedCount()
		secondPoll := PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: sources, Timestamp: now.Add(time.Minute)}
		result, err := store.ApplyPollCycle(secondPoll)
		if err != nil {
			t.Fatal(err)
		}
		assertProblem2ProductionJobAccounting(t, store, beforeJobs, beforeEvents, result, 0, 1, handler)
		beforeJobs, beforeEvents = countProblem2Jobs(t, store), handler.queuedCount()
		beforeBytes := captureProblem2ImmutableBytes(t, store.db)
		replay, err := store.ApplyPollCycle(secondPoll)
		if err != nil {
			t.Fatal(err)
		}
		if replay.RecheckJobsQueued != 0 || countProblem2Jobs(t, store) != beforeJobs || handler.queuedCount() != beforeEvents || captureProblem2ImmutableBytes(t, store.db) != beforeBytes {
			t.Fatalf("target replay changed production work: result=%+v jobs=%d/%d events=%d/%d", replay, countProblem2Jobs(t, store), beforeJobs, handler.queuedCount(), beforeEvents)
		}
	})
}

func countProblem2Jobs(t *testing.T, store *Store) int {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func loadProblem2JobInput(t *testing.T, store *Store) recheckJobInput {
	t.Helper()
	var input recheckJobInput
	if _, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		var loadErr error
		input, loadErr = loadPersistedRecheckJobInputTx(tx, 1)
		return struct{}{}, loadErr
	}); err != nil {
		t.Fatal(err)
	}
	return input
}

type problem2DuplicateSnapshot struct {
	bytes          string
	completeRows   string
	jobCount       int
	ruleCount      int
	candidateCount int
}

func captureProblem2DuplicateSnapshot(t *testing.T, store *Store) problem2DuplicateSnapshot {
	t.Helper()
	snapshot := problem2DuplicateSnapshot{bytes: captureProblem2ImmutableBytes(t, store.db), completeRows: captureProblem2CompleteDuplicateRows(t, store.db)}
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM silence_recheck_jobs`:           &snapshot.jobCount,
		`SELECT COUNT(*) FROM silence_recheck_job_rules`:      &snapshot.ruleCount,
		`SELECT COUNT(*) FROM silence_recheck_job_candidates`: &snapshot.candidateCount,
	} {
		if err := store.db.QueryRow(query).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	return snapshot
}

func captureProblem2CompleteDuplicateRows(t *testing.T, db *sql.DB) string {
	t.Helper()
	parts := make([]string, 0, 3)
	for _, spec := range []struct {
		table string
		order string
	}{
		{table: "silence_recheck_jobs", order: "job_id"},
		{table: "silence_recheck_job_rules", order: "job_id,source_name,target_key,rule_id"},
		{table: "silence_recheck_job_candidates", order: "job_id,alert_id,target_key"},
	} {
		rows, err := db.Query(`SELECT * FROM ` + spec.table + ` ORDER BY ` + spec.order)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		var encodedRows []string
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(values))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			encoded, marshalErr := json.Marshal(values)
			if marshalErr != nil {
				rows.Close()
				t.Fatal(marshalErr)
			}
			encodedRows = append(encodedRows, string(encoded))
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		parts = append(parts, spec.table+":"+strings.Join(encodedRows, "\n"))
	}
	return strings.Join(parts, "\n")
}

func assertProblem2DuplicateConflict(t *testing.T, store *Store, create func(*sql.Tx, *runtimeEventCollector) (recheckJobCreateResult, error), expectedStage ...string) {
	t.Helper()
	before := captureProblem2DuplicateSnapshot(t, store)
	var collector runtimeEventCollector
	result, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (recheckJobCreateResult, error) {
		return create(tx, &collector)
	})
	if err == nil || result.Created {
		t.Fatalf("immutable duplicate conflict was accepted: result=%+v err=%v", result, err)
	}
	if len(expectedStage) > 0 && !strings.Contains(err.Error(), expectedStage[0]) {
		t.Fatalf("duplicate conflict reached wrong stage: error=%q expected substring=%q", err, expectedStage[0])
	}
	after := captureProblem2DuplicateSnapshot(t, store)
	if after != before {
		t.Fatalf("duplicate conflict changed persisted rows: before=%+v after=%+v", before, after)
	}
	if len(collector.events) != 0 {
		t.Fatalf("duplicate conflict emitted committed queue events: %v", collector.events)
	}
}

func TestProblem2DuplicateIgnoresLegitimateRuntimeFields(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *Store, string)
	}{
		{name: "progress errors and retry", mutate: func(t *testing.T, store *Store, raw string) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='completed',cursor_alert_id='cursor',processed_candidates=1,promoted_candidates=1,started_at=?,finished_at=?,first_error_message='first',first_error_class='domain',first_error_stage='worker',first_error_at=?,last_error_message='last',last_error_class='domain',last_error_stage='worker',last_error_at=?,attempt_count=3,retry_count=2,consecutive_retry_count=1,recovery_count=1 WHERE job_id=1`, raw, raw, raw, raw, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET processed_at=?,outcome='promoted' WHERE job_id=1`, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "running timestamps and cursor", mutate: func(t *testing.T, store *Store, raw string) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='running',cursor_alert_id='cursor',started_at=?,attempt_count=1 WHERE job_id=1`, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "retry wait timestamp and errors", mutate: func(t *testing.T, store *Store, raw string) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='retry_wait',cursor_alert_id='cursor',started_at=?,next_attempt_at=?,first_error_message='first',first_error_class='busy',first_error_stage='worker',first_error_at=?,last_error_message='last',last_error_class='busy',last_error_stage='worker',last_error_at=?,attempt_count=2,retry_count=1,consecutive_retry_count=1 WHERE job_id=1`, raw, raw, raw, raw, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "stale conflict counter", mutate: func(t *testing.T, store *Store, raw string) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='completed',processed_candidates=1,stale_conflicts=1,started_at=?,finished_at=? WHERE job_id=1`, raw, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET processed_at=?,outcome='stale_alert_observation' WHERE job_id=1`, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "ineligible counter", mutate: func(t *testing.T, store *Store, raw string) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='completed',processed_candidates=1,skipped_ineligible=1,started_at=?,finished_at=? WHERE job_id=1`, raw, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET processed_at=?,outcome='ineligible_state' WHERE job_id=1`, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing labels counter", mutate: func(t *testing.T, store *Store, raw string) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='completed',processed_candidates=1,skipped_missing_labels=1,started_at=?,finished_at=? WHERE job_id=1`, raw, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET processed_at=?,outcome='missing_labels' WHERE job_id=1`, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "incomplete source counter", mutate: func(t *testing.T, store *Store, raw string) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='completed',processed_candidates=1,skipped_incomplete_sources=1,started_at=?,finished_at=? WHERE job_id=1`, raw, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET processed_at=?,outcome='incomplete_source_coverage' WHERE job_id=1`, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "operator abandonment", mutate: func(t *testing.T, store *Store, raw string) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='abandoned',processed_candidates=1,promoted_candidates=1,started_at=?,finished_at=?,abandoned_at=?,abandoned_by='operator',abandon_reason='review' WHERE job_id=1`, raw, raw, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET processed_at=?,outcome='promoted' WHERE job_id=1`, raw); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, _, now := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			input := loadProblem2JobInput(t, store)
			raw := now.Add(time.Minute).Format(time.RFC3339Nano)
			tc.mutate(t, store, raw)
			before := captureProblem2ImmutableBytes(t, store.db)
			var collector runtimeEventCollector
			result, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (recheckJobCreateResult, error) {
				return createRecheckJobTx(tx, input, now, &collector)
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Created || len(collector.events) != 0 {
				t.Fatalf("runtime-only drift was treated as new creation: result=%+v events=%v", result, collector.events)
			}
			if captureProblem2ImmutableBytes(t, store.db) != before {
				t.Fatal("runtime-only duplicate changed immutable bytes")
			}
		})
	}
}

func TestProblem2DuplicateRejectsImmutableChildConflicts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
	}{
		{name: "missing child", query: `DELETE FROM silence_recheck_job_candidates WHERE job_id=1`},
		{name: "extra child", query: `INSERT INTO silence_recheck_job_candidates(job_id,alert_id,expected_alert_last_seen_at,match_labels_json,relevant_sources_json) VALUES(1,'extra-child',?,?,?)`},
		{name: "changed child", query: `UPDATE silence_recheck_job_candidates SET match_labels_json='{"service":"other"}' WHERE job_id=1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _, now := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			if tc.name == "extra child" {
				if _, err := store.db.Exec(tc.query, now.Format(time.RFC3339Nano), `{"service":"api"}`, `[{"name":"am-a","cluster":"cluster-a"}]`); err != nil {
					t.Fatal(err)
				}
			} else if _, err := store.db.Exec(tc.query); err != nil {
				t.Fatal(err)
			}
			assertProblem2DuplicateConflict(t, store, func(tx *sql.Tx, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
				return createRecheckJobFromFinalStateTx(tx, "source_context_changed", nil, now, collector)
			})
		})
	}
}

func TestProblem2DuplicateRejectsRawByteDrift(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET relevant_sources_json='[ {"name":"am-a","cluster":"cluster-a"} ]' WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	assertProblem2DuplicateConflict(t, store, func(tx *sql.Tx, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
		return createRecheckJobFromFinalStateTx(tx, "source_context_changed", nil, now, collector)
	})
}

func TestProblem2DuplicateRejectsRuleProvenanceDrift(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	var ruleID, revisionID int64
	if err := store.db.QueryRow(`SELECT rule_id,revision_id FROM silence_recheck_job_rules WHERE job_id=1`).Scan(&ruleID, &revisionID); err != nil {
		t.Fatal(err)
	}
	insertProblem2UnrelatedRule(t, store, now)
	var replacementRule, replacementRevision int64
	if err := store.db.QueryRow(`SELECT rule_id,revision_id FROM silence_revisions WHERE rule_id<>? ORDER BY rule_id DESC LIMIT 1`, ruleID).Scan(&replacementRule, &replacementRevision); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET rule_id=?,revision_id=? WHERE job_id=1`, replacementRule, replacementRevision); err != nil {
		t.Fatal(err)
	}
	assertProblem2DuplicateConflict(t, store, func(tx *sql.Tx, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
		return createRecheckJobFromFinalStateTx(tx, "source_context_changed", nil, now, collector)
	})
}

func TestProblem2DuplicateRejectsLinkedSemanticDrift(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	var input recheckJobInput
	if _, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		var loadErr error
		input, loadErr = loadPersistedRecheckJobInputTx(tx, 1)
		return struct{}{}, loadErr
	}); err != nil {
		t.Fatal(err)
	}
	var revisionID int64
	if err := store.db.QueryRow(`SELECT revision_id FROM silence_recheck_job_rules WHERE job_id=1`).Scan(&revisionID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_revisions SET content_fingerprint='sha256:linked-drift' WHERE revision_id=?`, revisionID); err != nil {
		t.Fatal(err)
	}
	assertProblem2DuplicateConflict(t, store, func(tx *sql.Tx, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
		return createRecheckJobTx(tx, input, now, collector)
	})
}

func TestProblem2DuplicateRejectsRevisionLinkageDriftWithinSameRule(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	input := loadProblem2JobInput(t, store)
	var ruleID, revisionID int64
	if err := store.db.QueryRow(`SELECT rule_id,revision_id FROM silence_recheck_job_rules WHERE job_id=1`).Scan(&ruleID, &revisionID); err != nil {
		t.Fatal(err)
	}
	newRevisionID := insertProblem2SameRuleRevision(t, store, ruleID, revisionID)
	if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET revision_id=? WHERE job_id=1`, newRevisionID); err != nil {
		t.Fatal(err)
	}
	assertProblem2DuplicateConflict(t, store, func(tx *sql.Tx, collector *runtimeEventCollector) (recheckJobCreateResult, error) {
		return createRecheckJobTx(tx, input, now, collector)
	})
}

func insertProblem2SameRuleRevision(t *testing.T, store *Store, ruleID, revisionID int64) int64 {
	t.Helper()
	if _, err := store.db.Exec(`INSERT INTO silence_revisions(episode_id,rule_id,revision_number,content_fingerprint,evaluation_fingerprint,matchers_json,starts_at,ends_at,source_updated_at,created_by,comment,annotations_json,ticket_id,ticket_url,first_observed_at,last_observed_at) SELECT episode_id,rule_id,revision_number+1,content_fingerprint,evaluation_fingerprint,matchers_json,starts_at,ends_at,source_updated_at,created_by,comment,annotations_json,ticket_id,ticket_url,first_observed_at,last_observed_at FROM silence_revisions WHERE revision_id=? AND rule_id=?`, revisionID, ruleID); err != nil {
		t.Fatal(err)
	}
	var newRevisionID int64
	if err := store.db.QueryRow(`SELECT revision_id FROM silence_revisions WHERE rule_id=? AND revision_id<>? ORDER BY revision_id DESC LIMIT 1`, ruleID, revisionID).Scan(&newRevisionID); err != nil {
		t.Fatal(err)
	}
	return newRevisionID
}

func TestProblem2EachTerminalCandidateUpdatesExactlyOneParentCounter(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	labels := `{"service":"api"}`
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=2 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,expected_alert_last_seen_at,match_labels_json,relevant_sources_json) VALUES(1,'counter-second',?,?,?)`, now.Format(time.RFC3339Nano), labels, `[ {"name":"am-a","cluster":"cluster-a"} ]`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TABLE problem2_counter_probe(update_count INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO problem2_counter_probe(update_count) VALUES(0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER problem2_counter_probe_trigger AFTER UPDATE OF processed_candidates ON silence_recheck_jobs WHEN NEW.job_id=1 BEGIN UPDATE problem2_counter_probe SET update_count=update_count+1; END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunSilenceRecheckBatch(100); err != nil {
		t.Fatal(err)
	}
	var updates int
	if err := store.db.QueryRow(`SELECT update_count FROM problem2_counter_probe`).Scan(&updates); err != nil {
		t.Fatal(err)
	}
	if updates != 2 {
		t.Fatalf("parent counter update count=%d expected 2", updates)
	}
}

func TestProblem2CreatorEventCountMatchesCommittedCreation(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	var input recheckJobInput
	if _, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		var loadErr error
		input, loadErr = loadPersistedRecheckJobInputTx(tx, 1)
		return struct{}{}, loadErr
	}); err != nil {
		t.Fatal(err)
	}
	input.SourceContext.Generation++
	var collector runtimeEventCollector
	created, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (recheckJobCreateResult, error) {
		return createRecheckJobTx(tx, input, now, &collector)
	})
	if err != nil {
		t.Fatal(err)
	}
	queuedEvents := 0
	for _, event := range collector.events {
		if event.event == "recheck_job_queued" {
			queuedEvents++
		}
	}
	if !created.Created || queuedEvents != 1 {
		t.Fatalf("created=%v queuedEvents=%d events=%v", created.Created, queuedEvents, collector.events)
	}
	var duplicateCollector runtimeEventCollector
	duplicate, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (recheckJobCreateResult, error) {
		return createRecheckJobTx(tx, input, now, &duplicateCollector)
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Created || len(duplicateCollector.events) != 0 {
		t.Fatalf("duplicate creation changed events: created=%v events=%v", duplicate.Created, duplicateCollector.events)
	}
}

func TestProblem2CanonicalObservationTimestampVectors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    *string
		expected *string
		wantErr  bool
	}{
		{name: "nil", input: nil, expected: nil},
		{name: "blank", input: stringPointerForProblem2(""), expected: nil},
		{name: "utc", input: stringPointerForProblem2("2026-08-18T00:00:00Z"), expected: stringPointerForProblem2("2026-08-18T00:00:00Z")},
		{name: "offset", input: stringPointerForProblem2("2026-08-18T03:00:00+03:00"), expected: stringPointerForProblem2("2026-08-18T00:00:00Z")},
		{name: "fraction", input: stringPointerForProblem2("2026-08-18T00:00:00.123456789+00:00"), expected: stringPointerForProblem2("2026-08-18T00:00:00.123456789Z")},
		{name: "invalid", input: stringPointerForProblem2("not-a-time"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := canonicalTimestampPointer(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("timestamp error=%v wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if (got == nil) != (tc.expected == nil) || got != nil && *got != *tc.expected {
				t.Fatalf("canonical timestamp=%v expected=%v", got, tc.expected)
			}
		})
	}
}

func TestProblem2SchemaMatrixCoversAllImmutableTables(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	expectedColumns := map[string][]string{
		"silence_recheck_jobs": {
			"job_id", "source_context_fingerprint", "job_fingerprint", "source_context_json", "trigger_type", "status", "cursor_alert_id",
			"total_candidates", "processed_candidates", "promoted_candidates", "skipped_ineligible", "skipped_missing_labels", "skipped_incomplete_sources", "stale_conflicts",
			"attempt_count", "retry_count", "consecutive_retry_count", "next_attempt_at", "first_error_message", "first_error_class", "first_error_stage", "first_error_at",
			"last_error_message", "last_error_class", "last_error_stage", "last_error_at", "recovery_count", "abandoned_at", "abandoned_by", "abandon_reason", "created_at", "started_at", "finished_at",
		},
		"silence_recheck_job_rules":      {"job_id", "source_name", "cluster_id", "target_key", "rule_id", "revision_id", "silence_id", "content_fingerprint", "observed_state", "evaluation_fingerprint", "matchers_json"},
		"silence_recheck_job_candidates": {"job_id", "alert_id", "target_key", "expected_alert_last_seen_at", "expected_target_last_seen_at", "match_labels_json", "relevant_sources_json", "target_source_members_json", "processed_at", "outcome"},
	}
	for table, columns := range expectedColumns {
		actual := problem2TableColumns(t, store.db, table)
		if len(actual) != len(columns) {
			t.Fatalf("table %s column count=%d expected=%d columns=%v", table, len(actual), len(columns), actual)
		}
		for _, column := range columns {
			if !actual[column] {
				t.Errorf("table %s missing column %s", table, column)
			}
		}
	}
	for table, expected := range map[string][]string{
		"silence_recheck_jobs":           {"idx_silence_recheck_jobs_fifo", "idx_silence_recheck_jobs_context", "idx_silence_recheck_jobs_finished"},
		"silence_recheck_job_rules":      {"idx_recheck_job_rules_revision", "idx_recheck_job_rules_source"},
		"silence_recheck_job_candidates": {"idx_recheck_job_candidates_pending"},
	} {
		actual := problem2Indexes(t, store.db, table)
		for _, index := range expected {
			if !actual[index] {
				t.Errorf("table %s missing index %s", table, index)
			}
		}
	}
	for table, expected := range map[string][]string{
		"silence_recheck_jobs":           {"job_fingerprint"},
		"silence_recheck_job_rules":      {"job_id", "source_name", "target_key", "rule_id"},
		"silence_recheck_job_candidates": {"job_id", "alert_id", "target_key"},
	} {
		if !problem2HasUniqueKey(t, store.db, table, expected) {
			t.Errorf("table %s is missing unique key on %v", table, expected)
		}
	}
	assertProblem2ForeignKeySet(t, store.db, "silence_recheck_job_rules", map[string]string{
		"job_id->silence_recheck_jobs->CASCADE":            "",
		"source_name->alertmanager_sources->RESTRICT":      "",
		"rule_id->silence_rules->RESTRICT":                 "",
		"revision_id,rule_id->silence_revisions->RESTRICT": "",
	})
	assertProblem2ForeignKeySet(t, store.db, "silence_recheck_job_candidates", map[string]string{
		"job_id->silence_recheck_jobs->CASCADE": "",
	})
}

func TestProblem2SchemaIndexColumnOrderAndCompositeForeignKeys(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for index, expected := range map[string][]string{
		"idx_silence_recheck_jobs_fifo":      {"status", "next_attempt_at", "job_id"},
		"idx_silence_recheck_jobs_context":   {"source_context_fingerprint", "job_id"},
		"idx_silence_recheck_jobs_finished":  {"finished_at"},
		"idx_recheck_job_rules_revision":     {"revision_id"},
		"idx_recheck_job_rules_source":       {"job_id", "source_name", "observed_state"},
		"idx_recheck_job_candidates_pending": {"job_id", "processed_at", "alert_id", "target_key"},
	} {
		if actual := problem2IndexColumns(t, store.db, index); !reflect.DeepEqual(actual, expected) {
			t.Errorf("index %s columns=%v expected=%v", index, actual, expected)
		}
	}
	for table, expected := range map[string][]string{
		"silence_recheck_jobs":           {"job_fingerprint"},
		"silence_recheck_job_rules":      {"job_id", "source_name", "target_key", "rule_id"},
		"silence_recheck_job_candidates": {"job_id", "alert_id", "target_key"},
	} {
		var found bool
		for _, index := range problem2UniqueIndexes(t, store.db, table) {
			if reflect.DeepEqual(problem2IndexColumns(t, store.db, index), expected) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("table %s has no unique key with ordered columns %v", table, expected)
		}
	}
	assertProblem2ForeignKeysExact(t, store.db, "silence_recheck_job_rules", []problem2ForeignKeyGroup{
		{table: "silence_recheck_jobs", onDelete: "CASCADE", onUpdate: "NO ACTION", members: []problem2ForeignKeyMember{{sequence: 0, from: "job_id", to: "job_id"}}},
		{table: "alertmanager_sources", onDelete: "RESTRICT", onUpdate: "NO ACTION", members: []problem2ForeignKeyMember{{sequence: 0, from: "source_name", to: "source_name"}}},
		{table: "silence_rules", onDelete: "RESTRICT", onUpdate: "NO ACTION", members: []problem2ForeignKeyMember{{sequence: 0, from: "rule_id", to: "rule_id"}}},
		{table: "silence_revisions", onDelete: "RESTRICT", onUpdate: "NO ACTION", members: []problem2ForeignKeyMember{{sequence: 0, from: "revision_id", to: "revision_id"}, {sequence: 1, from: "rule_id", to: "rule_id"}}},
	})
	assertProblem2ForeignKeysExact(t, store.db, "silence_recheck_job_candidates", []problem2ForeignKeyGroup{
		{table: "silence_recheck_jobs", onDelete: "CASCADE", onUpdate: "NO ACTION", members: []problem2ForeignKeyMember{{sequence: 0, from: "job_id", to: "job_id"}}},
	})
}

type problem2ForeignKeyMember struct {
	sequence int
	from     string
	to       string
}

type problem2ForeignKeyGroup struct {
	table    string
	onDelete string
	onUpdate string
	members  []problem2ForeignKeyMember
}

func problem2IndexColumns(t *testing.T, db *sql.DB, index string) []string {
	t.Helper()
	rows, err := db.Query(`PRAGMA index_xinfo('` + index + `')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := []string{}
	for rows.Next() {
		var sequence, columnID, descending, key int
		var name, collation sql.NullString
		if err = rows.Scan(&sequence, &columnID, &name, &descending, &collation, &key); err != nil {
			t.Fatal(err)
		}
		if key == 1 && columnID >= 0 {
			if name.Valid {
				columns = append(columns, name.String)
			}
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return columns
}

func problem2UniqueIndexes(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query(`PRAGMA index_list('` + table + `')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	indexes := []string{}
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err = rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			t.Fatal(err)
		}
		if unique != 0 {
			indexes = append(indexes, name)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return indexes
}

func assertProblem2ForeignKeysExact(t *testing.T, db *sql.DB, table string, expected []problem2ForeignKeyGroup) {
	t.Helper()
	rows, err := db.Query(`PRAGMA foreign_key_list('` + table + `')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	actual := map[string]problem2ForeignKeyGroup{}
	for rows.Next() {
		var id, sequence int
		var referencedTable, from, to, onUpdate, onDelete, match string
		if err = rows.Scan(&id, &sequence, &referencedTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatal(err)
		}
		key := strconv.Itoa(id)
		group := actual[key]
		group.table, group.onDelete, group.onUpdate = referencedTable, onDelete, onUpdate
		group.members = append(group.members, problem2ForeignKeyMember{sequence: sequence, from: from, to: to})
		actual[key] = group
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(actual) != len(expected) {
		t.Fatalf("table %s foreign key group count=%d expected=%d actual=%v", table, len(actual), len(expected), actual)
	}
	for _, want := range expected {
		found := false
		for _, got := range actual {
			if got.table == want.table && got.onDelete == want.onDelete && got.onUpdate == want.onUpdate && reflect.DeepEqual(got.members, want.members) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("table %s missing exact foreign key %+v; actual=%v", table, want, actual)
		}
	}
}

func TestProblem2SchemaChecksUniqueKeysAndForeignKeyActions(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,created_at) SELECT source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,created_at FROM silence_recheck_jobs WHERE job_id=1`); err == nil {
		t.Fatal("duplicate job fingerprint was accepted")
	}
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_jobs(job_id,source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,created_at) SELECT 1,source_context_fingerprint,'distinct-parent-fingerprint',source_context_json,trigger_type,status,created_at FROM silence_recheck_jobs WHERE job_id=1`); err == nil {
		t.Fatal("duplicate parent job_id was accepted with a distinct fingerprint")
	}
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_rules(job_id,source_name,cluster_id,target_key,rule_id,revision_id,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json) SELECT job_id,source_name,cluster_id,target_key,rule_id,revision_id,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json FROM silence_recheck_job_rules WHERE job_id=1`); err == nil {
		t.Fatal("duplicate job rule primary key was accepted")
	}
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json) SELECT job_id,alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json FROM silence_recheck_job_candidates WHERE job_id=1`); err == nil {
		t.Fatal("duplicate job candidate primary key was accepted")
	}
	checks := []struct {
		name string
		fn   func(*testing.T, *Store)
	}{
		{name: "invalid trigger", fn: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET trigger_type='invalid' WHERE job_id=1`); err == nil {
				t.Fatal("invalid trigger was accepted")
			}
		}},
		{name: "invalid status", fn: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='invalid' WHERE job_id=1`); err == nil {
				t.Fatal("invalid status was accepted")
			}
		}},
		{name: "negative counter", fn: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET promoted_candidates=-1 WHERE job_id=1`); err == nil {
				t.Fatal("negative counter was accepted")
			}
		}},
		{name: "processed above total", fn: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET processed_candidates=2 WHERE job_id=1`); err == nil {
				t.Fatal("processed above total was accepted")
			}
		}},
		{name: "retry pair", fn: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='retry_wait',next_attempt_at=NULL WHERE job_id=1`); err == nil {
				t.Fatal("invalid retry terminal pair was accepted")
			}
		}},
		{name: "invalid rule state", fn: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET observed_state='invalid' WHERE job_id=1`); err == nil {
				t.Fatal("invalid rule state was accepted")
			}
		}},
		{name: "invalid candidate outcome", fn: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET outcome='invalid' WHERE job_id=1`); err == nil {
				t.Fatal("invalid candidate outcome was accepted")
			}
		}},
		{name: "terminal pair", fn: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET outcome='promoted',processed_at=NULL WHERE job_id=1`); err == nil {
				t.Fatal("terminal pair CHECK was bypassed")
			}
		}},
		{name: "target scope", fn: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET target_key='cluster-a',target_source_members_json=NULL WHERE job_id=1`); err == nil {
				t.Fatal("target scope CHECK was bypassed")
			}
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			checkStore, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = checkStore.Close() })
			check.fn(t, checkStore)
		})
	}

	if _, err := store.db.Exec(`DELETE FROM alertmanager_sources WHERE source_name='am-a'`); err == nil {
		t.Fatal("source referenced by recheck input was deleted")
	}
	var ruleID, revisionID int64
	if err := store.db.QueryRow(`SELECT rule_id,revision_id FROM silence_recheck_job_rules WHERE job_id=1`).Scan(&ruleID, &revisionID); err != nil {
		t.Fatal(err)
	}
	for name, action := range map[string]struct {
		query string
		args  []any
	}{
		"parent job id": {query: `UPDATE silence_recheck_jobs SET job_id=2 WHERE job_id=1`},
		"source name":   {query: `UPDATE alertmanager_sources SET source_name='am-renamed' WHERE source_name='am-a'`},
		"rule id":       {query: `UPDATE silence_rules SET rule_id=? WHERE rule_id=?`, args: []any{ruleID + 1000, ruleID}},
		"revision id":   {query: `UPDATE silence_revisions SET revision_id=? WHERE revision_id=?`, args: []any{revisionID + 1000, revisionID}},
	} {
		if _, err := store.db.Exec(action.query, action.args...); err == nil {
			t.Errorf("foreign-key update action %s unexpectedly succeeded", name)
		}
	}
	if _, err := store.db.Exec(`DELETE FROM silence_revisions WHERE revision_id=?`, revisionID); err == nil {
		t.Fatal("revision referenced by recheck input was deleted")
	}
	if _, err := store.db.Exec(`DELETE FROM silence_rules WHERE rule_id=?`, ruleID); err == nil {
		t.Fatal("rule referenced by recheck input was deleted")
	}
	if _, err := store.db.Exec(`DELETE FROM silence_recheck_jobs WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	var rules, candidates int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_rules WHERE job_id=1`).Scan(&rules); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=1`).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if rules != 0 || candidates != 0 {
		t.Fatalf("job CASCADE delete left children rules=%d candidates=%d", rules, candidates)
	}
}

func TestProblem2SchemaAcceptsAllProblem2EnumAndTerminalValues(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	for _, trigger := range []string{"source_context_changed", "alert_became_eligible"} {
		if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET trigger_type=? WHERE job_id=1`, trigger); err != nil {
			t.Fatalf("valid trigger %s rejected: %v", trigger, err)
		}
	}
	for _, state := range []string{"pending", "active", "expired"} {
		if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET observed_state=? WHERE job_id=1`, state); err != nil {
			t.Fatalf("valid rule state %s rejected: %v", state, err)
		}
	}
	finished := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='running',started_at=?,next_attempt_at=NULL,finished_at=NULL WHERE job_id=1`, finished); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='retry_wait',next_attempt_at=?,finished_at=NULL WHERE job_id=1`, finished); err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"failed", "completed"} {
		if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status=?,next_attempt_at=NULL,finished_at=? WHERE job_id=1`, status, finished); err != nil {
			t.Fatalf("valid status %s rejected: %v", status, err)
		}
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='abandoned',next_attempt_at=NULL,finished_at=?,abandoned_at=?,abandoned_by='operator',abandon_reason='test' WHERE job_id=1`, finished, finished); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET status='queued',next_attempt_at=NULL,finished_at=NULL,abandoned_at=NULL,abandoned_by=NULL,abandon_reason=NULL WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	for _, outcome := range []string{"promoted", "stale_alert_observation", "stale_target_observation", "missing_observation_token", "ineligible_state", "missing_current_alert", "missing_current_target", "missing_labels", "invalid_labels", "no_matching_rule", "incomplete_source_coverage", "retired_target", "already_suppressed", "already_resolved"} {
		if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET outcome=?,processed_at=? WHERE job_id=1`, outcome, finished); err != nil {
			t.Fatalf("valid outcome %s rejected: %v", outcome, err)
		}
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET target_key='cluster-a',expected_target_last_seen_at=?,target_source_members_json='["am-a"]' WHERE job_id=1`, finished); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET target_key='',expected_target_last_seen_at=NULL,target_source_members_json=NULL,outcome=NULL,processed_at=NULL WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET processed_candidates=1,promoted_candidates=1 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET processed_candidates=0,promoted_candidates=0 WHERE job_id=1`); err != nil {
		t.Fatal(err)
	}
}

func TestProblem2SchemaAccountingAndBoundaryChecks(t *testing.T) {
	for _, column := range []string{"total_candidates", "processed_candidates", "promoted_candidates", "stale_conflicts", "skipped_ineligible", "skipped_missing_labels", "skipped_incomplete_sources"} {
		t.Run(column+" negative", func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET ` + column + `=-1 WHERE job_id=1`); err == nil {
				t.Fatalf("negative %s was accepted", column)
			}
		})
		t.Run(column+" zero", func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET ` + column + `=0 WHERE job_id=1`); err != nil {
				t.Fatalf("zero %s was rejected: %v", column, err)
			}
		})
	}
	for _, column := range []string{"promoted_candidates", "stale_conflicts", "skipped_ineligible", "skipped_missing_labels", "skipped_incomplete_sources"} {
		t.Run(column+" balanced distribution", func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=2,processed_candidates=1,` + column + `=1 WHERE job_id=1`); err != nil {
				t.Fatalf("balanced %s was rejected: %v", column, err)
			}
		})
	}
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET total_candidates=2,processed_candidates=1,promoted_candidates=1,stale_conflicts=1 WHERE job_id=1`); err == nil {
		t.Fatal("accounting-sum CHECK was not isolated from processed<=total")
	}

	for name, query := range map[string]string{
		"terminal outcome without processed": `UPDATE silence_recheck_job_candidates SET outcome='promoted',processed_at=NULL WHERE job_id=1`,
		"terminal processed without outcome": `UPDATE silence_recheck_job_candidates SET outcome=NULL,processed_at='2026-08-18T00:00:00Z' WHERE job_id=1`,
	} {
		t.Run(name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.Exec(query); err == nil {
				t.Fatal("invalid terminal pair was accepted")
			}
		})
	}

	for name, query := range map[string]string{
		"global with member bytes":    `UPDATE silence_recheck_job_candidates SET target_key='',target_source_members_json='[]' WHERE job_id=1`,
		"target without member bytes": `UPDATE silence_recheck_job_candidates SET target_key='cluster-a',target_source_members_json=NULL WHERE job_id=1`,
	} {
		t.Run(name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.Exec(query); err == nil {
				t.Fatal("invalid target-scope nullability was accepted")
			}
		})
	}
	for name, query := range map[string]string{
		"global valid": `UPDATE silence_recheck_job_candidates SET target_key='',target_source_members_json=NULL WHERE job_id=1`,
		"target valid": `UPDATE silence_recheck_job_candidates SET target_key='cluster-a',target_source_members_json='[]' WHERE job_id=1`,
	} {
		t.Run(name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.Exec(query); err != nil {
				t.Fatalf("valid target-scope nullability was rejected: %v", err)
			}
		})
	}

	finished := "2026-08-18T00:00:00Z"
	for name, query := range map[string]string{
		"queued with finished":       `UPDATE silence_recheck_jobs SET status='queued',finished_at=? WHERE job_id=1`,
		"running with finished":      `UPDATE silence_recheck_jobs SET status='running',finished_at=? WHERE job_id=1`,
		"failed without finished":    `UPDATE silence_recheck_jobs SET status='failed',finished_at=NULL WHERE job_id=1`,
		"completed without finished": `UPDATE silence_recheck_jobs SET status='completed',finished_at=NULL WHERE job_id=1`,
	} {
		t.Run(name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			var args []any
			if strings.Contains(query, "?") {
				args = []any{finished}
			}
			if _, err := store.db.Exec(query, args...); err == nil {
				t.Fatal("invalid Problem 2 status timestamp pairing was accepted")
			}
		})
	}
}

func TestProblem2ForeignKeyActionsAreIsolatedToPinnedInput(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	raw := now.Format(time.RFC3339Nano)
	if _, err := store.db.Exec(`INSERT INTO alertmanager_sources(source_name,current_cluster_id,sync_status,first_configured_at,last_configured_at) VALUES('am-isolated','cluster-a','success',?,?)`, raw, raw); err != nil {
		t.Fatal(err)
	}
	var ruleID, revisionID int64
	if err := store.db.QueryRow(`SELECT rule_id,revision_id FROM silence_recheck_job_rules WHERE job_id=1`).Scan(&ruleID, &revisionID); err != nil {
		t.Fatal(err)
	}
	insertProblem2IsolatedJob(t, store, 2002, "am-isolated", 0, 0, true)
	insertProblem2IsolatedJob(t, store, 2003, "am-isolated", ruleID, revisionID, false)
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET job_id=2102 WHERE job_id=2002`); err == nil {
		t.Fatal("candidate-only job parent update was not restricted by candidate FK")
	}
	if _, err := store.db.Exec(`UPDATE silence_recheck_jobs SET job_id=2103 WHERE job_id=2003`); err == nil {
		t.Fatal("rule-only job parent update was not restricted by rule FK")
	}

	newRevisionID := insertProblem2SameRuleRevision(t, store, ruleID, revisionID)
	if _, err := store.db.Exec(`UPDATE silence_recheck_job_rules SET revision_id=? WHERE job_id=2003`, newRevisionID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE silence_revisions SET revision_id=? WHERE revision_id=?`, newRevisionID+1000, newRevisionID); err == nil {
		t.Fatal("non-current revision update was not restricted by composite job FK")
	}
	if _, err := store.db.Exec(`DELETE FROM silence_revisions WHERE revision_id=?`, newRevisionID); err == nil {
		t.Fatal("non-current revision deletion was not restricted by composite job FK")
	}

	insertProblem2UnrelatedRule(t, store, now)
	var unrelatedRuleID, unrelatedRevisionID int64
	if err := store.db.QueryRow(`SELECT rule_id FROM silence_rules WHERE cluster_id='noise' ORDER BY rule_id DESC LIMIT 1`).Scan(&unrelatedRuleID); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT revision_id FROM silence_revisions WHERE rule_id=? ORDER BY revision_id DESC LIMIT 1`, unrelatedRuleID).Scan(&unrelatedRevisionID); err != nil {
		t.Fatal(err)
	}
	insertProblem2IsolatedJob(t, store, 2004, "am-isolated", unrelatedRuleID, unrelatedRevisionID, false)
	if _, err := store.db.Exec(`DELETE FROM silence_rules WHERE rule_id=?`, unrelatedRuleID); err == nil {
		t.Fatal("non-current rule deletion was not restricted by direct/composite job FKs")
	}
	if _, err := store.db.Exec(`UPDATE alertmanager_sources SET source_name='am-isolated-renamed' WHERE source_name='am-isolated'`); err == nil {
		t.Fatal("source key update was not restricted by pinned job-rule FK")
	}
	if _, err := store.db.Exec(`DELETE FROM alertmanager_sources WHERE source_name='am-isolated'`); err == nil {
		t.Fatal("source deletion was not restricted by pinned job-rule FK")
	}
}

func insertProblem2IsolatedJob(t *testing.T, store *Store, jobID int64, sourceName string, ruleID, revisionID int64, withCandidate bool) {
	t.Helper()
	raw := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.db.Exec(`INSERT INTO silence_recheck_jobs(job_id,source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,created_at) VALUES(?,?,?,?,?,?,?)`, jobID, fmt.Sprintf("isolated-source-%d", jobID), fmt.Sprintf("isolated-job-%d", jobID), `{"formatVersion":1,"generation":1,"sources":[],"clusters":[]}`, "source_context_changed", "queued", raw); err != nil {
		t.Fatal(err)
	}
	if ruleID != 0 {
		if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_rules(job_id,source_name,cluster_id,target_key,rule_id,revision_id,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json) SELECT ?,?,?,target_key,?,?,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json FROM silence_recheck_job_rules WHERE job_id=1 LIMIT 1`, jobID, sourceName, "cluster-a", ruleID, revisionID); err != nil {
			t.Fatal(err)
		}
	}
	if withCandidate {
		if _, err := store.db.Exec(`INSERT INTO silence_recheck_job_candidates(job_id,alert_id,target_key,match_labels_json,relevant_sources_json) VALUES(?,?,'',?,?)`, jobID, fmt.Sprintf("isolated-alert-%d", jobID), `{"service":"api"}`, `[]`); err != nil {
			t.Fatal(err)
		}
	}
}

func problem2HasUniqueKey(t *testing.T, db *sql.DB, table string, expected []string) bool {
	t.Helper()
	rows, err := db.Query(`PRAGMA index_list('` + table + `')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err = rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			t.Fatal(err)
		}
		if unique == 0 {
			continue
		}
		columnsRows, queryErr := db.Query(`PRAGMA index_info('` + name + `')`)
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		columns := []string{}
		for columnsRows.Next() {
			var indexSequence, columnNumber int
			var columnName string
			if queryErr = columnsRows.Scan(&indexSequence, &columnNumber, &columnName); queryErr != nil {
				columnsRows.Close()
				t.Fatal(queryErr)
			}
			columns = append(columns, columnName)
		}
		if queryErr = columnsRows.Err(); queryErr != nil {
			columnsRows.Close()
			t.Fatal(queryErr)
		}
		columnsRows.Close()
		if reflect.DeepEqual(columns, expected) {
			return true
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return false
}

func problem2Indexes(t *testing.T, db *sql.DB, table string) map[string]bool {
	t.Helper()
	rows, err := db.Query(`PRAGMA index_list('` + table + `')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	indexes := map[string]bool{}
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err = rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			t.Fatal(err)
		}
		indexes[name] = true
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return indexes
}

func assertProblem2ForeignKeySet(t *testing.T, db *sql.DB, table string, expected map[string]string) {
	t.Helper()
	rows, err := db.Query(`PRAGMA foreign_key_list('` + table + `')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	actual := map[string]bool{}
	for rows.Next() {
		var id, sequence int
		var referencedTable, from, to, onUpdate, onDelete, match string
		if err = rows.Scan(&id, &sequence, &referencedTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatal(err)
		}
		key := from + "->" + referencedTable + "->" + onDelete
		if sequence > 0 {
			key = "composite->" + referencedTable + "->" + onDelete
		}
		actual[key] = true
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	for key := range expected {
		if !actual[key] && key != "revision_id,rule_id->silence_revisions->RESTRICT" {
			t.Errorf("table %s missing foreign key %s; actual=%v", table, key, actual)
		}
	}
	if table == "silence_recheck_job_rules" && !actual["composite->silence_revisions->RESTRICT"] {
		t.Errorf("table %s missing composite revision foreign key; actual=%v", table, actual)
	}
}

func TestProblem2CreatorRejectsInvalidObservationTimestampBeforeInsert(t *testing.T) {
	store, _, now := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	var input recheckJobInput
	if _, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (struct{}, error) {
		var loadErr error
		input, loadErr = loadPersistedRecheckJobInputTx(tx, 1)
		return struct{}{}, loadErr
	}); err != nil {
		t.Fatal(err)
	}
	input.SourceContext.Generation++
	input.Candidates[0].ExpectedAlertLastSeenAt = stringPointerForProblem2("not-a-time")
	_, err := withWriterTxValue(context.Background(), store, writerOperationPoll, func(tx *sql.Tx) (recheckJobCreateResult, error) {
		return createRecheckJobTx(tx, input, now, nil)
	})
	if err == nil {
		t.Fatal("creator accepted invalid observation timestamp")
	}
	var jobs int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Fatalf("invalid timestamp changed job count=%d", jobs)
	}
}

func TestProblem2PostCASTargetClassifierPropagatesReadErrors(t *testing.T) {
	store, _ := seedProblem2TargetJob(t)
	t.Cleanup(func() { _ = store.Close() })
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = classifyTargetAfterCASFailureTx(tx, "target-episode", "cluster-a", "missing", "missing"); err == nil {
		t.Fatal("target post-CAS read error was converted to a stale outcome")
	}
}

func TestProblem2TargetPromotionClassifiesZeroRowAggregateCAS(t *testing.T) {
	store, _ := seedProblem2TargetJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`CREATE TRIGGER problem2_aggregate_cas_probe AFTER UPDATE OF is_suppressed ON alert_target_states WHEN NEW.alert_id='target-episode' BEGIN UPDATE alert_states SET state='active', is_suppressed=0 WHERE alert_id=NEW.alert_id; END`); err != nil {
		t.Fatal(err)
	}
	if promoted, err := store.RunSilenceRecheckBatch(100); err != nil || promoted != 1 {
		t.Fatalf("target promotion with zero-row aggregate CAS promoted=%d err=%v", promoted, err)
	}
	var globalState string
	if err := store.db.QueryRow(`SELECT state FROM alert_states WHERE alert_id='target-episode'`).Scan(&globalState); err != nil {
		t.Fatal(err)
	}
	if globalState != "active" {
		t.Fatalf("aggregate zero-row CAS unexpectedly changed global state=%q", globalState)
	}
}

func TestProblem2PostCASClassifiersPropagateReadErrorsAndInvariants(t *testing.T) {
	store, alertID, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = classifyGlobalAfterCASFailureTx(tx, alertID, "missing"); err == nil {
		t.Fatal("global post-CAS read error was converted to a stale outcome")
	}

	store2, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store2.Close() })
	var candidate recheckCandidate
	if err = store2.db.QueryRow(`SELECT alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at FROM silence_recheck_job_candidates`).Scan(&candidate.AlertID, &candidate.TargetKey, &candidate.LastSeenAt, &candidate.TargetLastSeenAt); err != nil {
		t.Fatal(err)
	}
	_, err = withWriterTxValue(context.Background(), store2, writerOperationPoll, func(tx *sql.Tx) (string, error) {
		return classifyGlobalAfterCASFailureTx(tx, candidate.AlertID, candidate.LastSeenAt.String)
	})
	if err == nil {
		t.Fatal("unexplained global zero-row CAS did not return an invariant error")
	}

	store3, _ := seedProblem2TargetJob(t)
	t.Cleanup(func() { _ = store3.Close() })
	var targetCandidate recheckCandidate
	if err = store3.db.QueryRow(`SELECT alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at FROM silence_recheck_job_candidates`).Scan(&targetCandidate.AlertID, &targetCandidate.TargetKey, &targetCandidate.LastSeenAt, &targetCandidate.TargetLastSeenAt); err != nil {
		t.Fatal(err)
	}
	_, err = withWriterTxValue(context.Background(), store3, writerOperationPoll, func(tx *sql.Tx) (string, error) {
		return classifyTargetAfterCASFailureTx(tx, targetCandidate.AlertID, targetCandidate.TargetKey, targetCandidate.LastSeenAt.String, targetCandidate.TargetLastSeenAt.String)
	})
	if err == nil {
		t.Fatal("unexplained target zero-row CAS did not return an invariant error")
	}
}

func TestProblem2WorkerPrimaryGlobalCASZeroRowRollsBack(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`CREATE TRIGGER problem2_primary_global_zero BEFORE UPDATE OF state ON alert_states WHEN NEW.state='suppressed' BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	before := captureProblem2WorkerSideEffects(t, store)
	if _, err := store.RunSilenceRecheckBatch(100); err == nil {
		t.Fatal("primary global unexplained zero-row CAS did not fail")
	}
	assertProblem2WorkerRollback(t, store, before)
}

func TestProblem2WorkerPrimaryTargetCASZeroRowRollsBack(t *testing.T) {
	store, _ := seedProblem2TargetJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`CREATE TRIGGER problem2_primary_target_zero BEFORE UPDATE OF state ON alert_target_states WHEN NEW.state='suppressed' BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	before := captureProblem2WorkerSideEffects(t, store)
	if _, err := store.RunSilenceRecheckBatch(100); err == nil {
		t.Fatal("primary target unexplained zero-row CAS did not fail")
	}
	assertProblem2WorkerRollback(t, store, before)
}

func TestProblem2WorkerClassifiesPrimaryPostCASStates(t *testing.T) {
	cases := []struct {
		name       string
		targetCase bool
		outcome    string
		counter    string
		trigger    string
	}{
		{name: "global stale", outcome: "stale_alert_observation", counter: "stale_conflicts", trigger: `CREATE TRIGGER problem2_global_stale BEFORE UPDATE OF state ON alert_states WHEN NEW.state='suppressed' BEGIN UPDATE alert_states SET last_seen_at='2099-01-01T00:00:00Z' WHERE alert_id=NEW.alert_id; SELECT RAISE(IGNORE); END`},
		{name: "global ineligible", outcome: "ineligible_state", counter: "skipped_ineligible", trigger: `CREATE TRIGGER problem2_global_ineligible BEFORE UPDATE OF state ON alert_states WHEN NEW.state='suppressed' BEGIN UPDATE alert_states SET is_live=1 WHERE alert_id=NEW.alert_id; SELECT RAISE(IGNORE); END`},
		{name: "target stale", targetCase: true, outcome: "stale_target_observation", counter: "stale_conflicts", trigger: `CREATE TRIGGER problem2_target_stale BEFORE UPDATE OF state ON alert_target_states WHEN NEW.state='suppressed' BEGIN UPDATE alert_target_states SET last_seen_at='2099-01-01T00:00:00Z' WHERE alert_id=NEW.alert_id AND target=NEW.target; SELECT RAISE(IGNORE); END`},
		{name: "target ineligible", targetCase: true, outcome: "ineligible_state", counter: "skipped_ineligible", trigger: `CREATE TRIGGER problem2_target_ineligible BEFORE UPDATE OF state ON alert_target_states WHEN NEW.state='suppressed' BEGIN UPDATE alert_target_states SET is_live=1 WHERE alert_id=NEW.alert_id AND target=NEW.target; SELECT RAISE(IGNORE); END`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var store *Store
			if tc.targetCase {
				store, _ = seedProblem2TargetJob(t)
			} else {
				store, _, _ = seedProblem2OpenJob(t)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.Exec(tc.trigger); err != nil {
				t.Fatal(err)
			}
			if _, err := store.RunSilenceRecheckBatch(100); err != nil {
				t.Fatal(err)
			}
			var outcome string
			if err := store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates`).Scan(&outcome); err != nil {
				t.Fatal(err)
			}
			if outcome != tc.outcome {
				t.Fatalf("post-CAS outcome=%q expected=%q", outcome, tc.outcome)
			}
			assertProblem2OutcomeAccounting(t, store, outcome, tc.counter)
		})
	}
}

func TestProblem2WorkerPostCASReadFailuresRollbackPrimaryAndAggregate(t *testing.T) {
	cases := []struct {
		name       string
		targetCase bool
		stage      string
		trigger    string
	}{
		{name: "primary global read", stage: "primary_global", trigger: `CREATE TRIGGER problem2_primary_global_read BEFORE UPDATE OF state ON alert_states WHEN NEW.state='suppressed' BEGIN SELECT RAISE(IGNORE); END`},
		{name: "primary target read", targetCase: true, stage: "primary_target", trigger: `CREATE TRIGGER problem2_primary_target_read BEFORE UPDATE OF state ON alert_target_states WHEN NEW.state='suppressed' BEGIN SELECT RAISE(IGNORE); END`},
		{name: "aggregate global read", targetCase: true, stage: "aggregate_global", trigger: `CREATE TRIGGER problem2_aggregate_global_read BEFORE UPDATE OF state ON alert_states WHEN NEW.state='suppressed' BEGIN SELECT RAISE(IGNORE); END`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var store *Store
			if tc.targetCase {
				store, _ = seedProblem2TargetJob(t)
			} else {
				store, _, _ = seedProblem2OpenJob(t)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.Exec(tc.trigger); err != nil {
				t.Fatal(err)
			}
			store.lifecycleTestHooks = &storeLifecycleTestHooks{postCASReadError: func(stage string) error {
				if stage == tc.stage {
					return errors.New("injected Problem 2 post-CAS read failure")
				}
				return nil
			}}
			before := captureProblem2WorkerSideEffects(t, store)
			if _, err := store.RunSilenceRecheckBatch(100); err == nil {
				t.Fatal("post-CAS read failure was swallowed")
			}
			assertProblem2WorkerRollback(t, store, before)
		})
	}
}

func TestProblem2WorkerAggregateCASZeroRowIsInvariant(t *testing.T) {
	store, _ := seedProblem2TargetJob(t)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.Exec(`CREATE TRIGGER problem2_aggregate_global_zero BEFORE UPDATE OF state ON alert_states WHEN NEW.state='suppressed' BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	before := captureProblem2WorkerSideEffects(t, store)
	if _, err := store.RunSilenceRecheckBatch(100); err == nil {
		t.Fatal("aggregate unexplained zero-row CAS did not fail")
	}
	assertProblem2WorkerRollback(t, store, before)
}

type problem2WorkerSideEffects struct {
	candidate    string
	processed    int
	promoted     int
	ineligible   int
	missing      int
	incomplete   int
	stale        int
	alerts       int
	targets      int
	causes       int
	targetCauses int
	aggregates   int
	bytes        string
}

func captureProblem2WorkerSideEffects(t *testing.T, store *Store) problem2WorkerSideEffects {
	t.Helper()
	var result problem2WorkerSideEffects
	if err := store.db.QueryRow(`SELECT COALESCE(outcome,''),processed_candidates,promoted_candidates,skipped_ineligible,skipped_missing_labels,skipped_incomplete_sources,stale_conflicts FROM silence_recheck_job_candidates c CROSS JOIN silence_recheck_jobs j WHERE c.job_id=j.job_id AND c.job_id=1`).Scan(&result.candidate, &result.processed, &result.promoted, &result.ineligible, &result.missing, &result.incomplete, &result.stale); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM alert_events`).Scan(&result.alerts); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM alert_target_events`).Scan(&result.targets); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM alert_suppression_cause_history`).Scan(&result.causes); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM alert_target_suppression_cause_history`).Scan(&result.targetCauses); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM alert_target_aggregate_suppression_history`).Scan(&result.aggregates); err != nil {
		t.Fatal(err)
	}
	result.bytes = captureProblem2WorkerEvidenceBytes(t, store.db)
	return result
}

func captureProblem2WorkerEvidenceBytes(t *testing.T, db *sql.DB) string {
	t.Helper()
	parts := make([]string, 0, 10)
	for _, table := range []string{
		"alert_states",
		"alert_target_states",
		"alert_events",
		"alert_target_events",
		"alert_suppression_cause_history",
		"alert_suppression_cause_members",
		"alert_target_suppression_cause_history",
		"alert_target_suppression_cause_members",
		"alert_target_aggregate_suppression_history",
		"alert_target_aggregate_suppression_links",
	} {
		rows, err := db.Query(`SELECT * FROM ` + table + ` ORDER BY rowid`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		var tableRows []string
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(values))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			encoded, marshalErr := json.Marshal(values)
			if marshalErr != nil {
				rows.Close()
				t.Fatal(marshalErr)
			}
			tableRows = append(tableRows, string(encoded))
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		parts = append(parts, table+":"+strings.Join(tableRows, "\n"))
	}
	return strings.Join(parts, "\n")
}

func assertProblem2WorkerRollback(t *testing.T, store *Store, before problem2WorkerSideEffects) {
	t.Helper()
	after := captureProblem2WorkerSideEffects(t, store)
	if after != before {
		t.Fatalf("failed worker transaction changed candidate/counters/evidence: before=%+v after=%+v", before, after)
	}
}

func TestProblem2TerminalOutcomePriority(t *testing.T) {
	cases := []struct {
		name      string
		expected  string
		setupFunc func(*testing.T, *Store)
	}{
		{name: "missing target before missing labels", expected: "missing_current_target", setupFunc: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`DELETE FROM alert_target_states WHERE alert_id='target-episode' AND target='cluster-a'`); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json=NULL`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing target before global resolved", expected: "missing_current_target", setupFunc: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_states SET is_resolved=1 WHERE alert_id='target-episode'`); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`DELETE FROM alert_target_states WHERE alert_id='target-episode' AND target='cluster-a'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "retired target before missing labels", expected: "retired_target", setupFunc: func(t *testing.T, store *Store) {
			retired := time.Now().UTC().Format(time.RFC3339Nano)
			if _, err := store.db.Exec(`UPDATE alert_target_states SET retired_at=?, state='inactive', is_live=0 WHERE alert_id='target-episode' AND target='cluster-a'`, retired); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json=NULL`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "resolved target before missing labels", expected: "already_resolved", setupFunc: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_target_states SET is_resolved=1 WHERE alert_id='target-episode' AND target='cluster-a'`); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json=NULL`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "global eligibility before missing labels", expected: "ineligible_state", setupFunc: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_states SET state='active', is_live=1 WHERE alert_id='problem2-alert'`); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json=NULL`); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var store *Store
			if tc.name == "global eligibility before missing labels" {
				store, _, _ = seedProblem2OpenJob(t)
			} else {
				store, _ = seedProblem2TargetJob(t)
			}
			t.Cleanup(func() { _ = store.Close() })
			tc.setupFunc(t, store)
			if _, err := store.RunSilenceRecheckBatch(100); err != nil {
				t.Fatal(err)
			}
			var outcome string
			if err := store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates`).Scan(&outcome); err != nil {
				t.Fatal(err)
			}
			if outcome != tc.expected {
				t.Fatalf("outcome=%q expected=%q", outcome, tc.expected)
			}
		})
	}
}

func TestProblem2CompletePriorityBoundaryMatrix(t *testing.T) {
	cases := []struct {
		name       string
		expected   string
		counter    string
		targetCase bool
		setup      func(*testing.T, *Store)
	}{
		{name: "missing alert before token", expected: "missing_current_alert", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`DELETE FROM alert_states WHERE alert_id='problem2-alert'`); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET expected_alert_last_seen_at=NULL`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing token before stale global", expected: "missing_observation_token", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET expected_alert_last_seen_at=NULL`); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE alert_states SET last_seen_at=? WHERE alert_id='problem2-alert'`, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "stale global before target", expected: "stale_alert_observation", counter: "stale_conflicts", targetCase: true, setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_states SET last_seen_at=? WHERE alert_id='target-episode'`, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`DELETE FROM alert_target_states WHERE alert_id='target-episode' AND target='cluster-a'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing target independent", expected: "missing_current_target", counter: "skipped_ineligible", targetCase: true, setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`DELETE FROM alert_target_states WHERE alert_id='target-episode' AND target='cluster-a'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "retired before stale target", expected: "retired_target", counter: "skipped_ineligible", targetCase: true, setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_target_states SET retired_at=?,state='inactive',is_live=0,last_seen_at=? WHERE alert_id='target-episode' AND target='cluster-a'`, time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "stale target before resolved", expected: "stale_target_observation", counter: "stale_conflicts", targetCase: true, setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_target_states SET last_seen_at=?,is_resolved=1 WHERE alert_id='target-episode' AND target='cluster-a'`, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "resolved before suppressed", expected: "already_resolved", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_states SET state='suppressed',is_suppressed=1,is_resolved=1 WHERE alert_id='problem2-alert'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "suppressed before ineligible", expected: "already_suppressed", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_states SET state='suppressed',is_suppressed=1,is_live=1 WHERE alert_id='problem2-alert'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "ineligible before labels", expected: "ineligible_state", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_states SET state='active',is_live=1 WHERE alert_id='problem2-alert'`); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json=NULL`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "labels before source coverage", expected: "missing_labels", counter: "skipped_missing_labels", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json=NULL,relevant_sources_json='[]'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "source coverage before no match", expected: "incomplete_source_coverage", counter: "skipped_incomplete_sources", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json='{"service":"other"}',relevant_sources_json='[]'`); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var store *Store
			if tc.targetCase {
				store, _ = seedProblem2TargetJob(t)
			} else {
				store, _, _ = seedProblem2OpenJob(t)
			}
			t.Cleanup(func() { _ = store.Close() })
			tc.setup(t, store)
			if _, err := store.RunSilenceRecheckBatch(100); err != nil {
				t.Fatal(err)
			}
			var outcome string
			if err := store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates`).Scan(&outcome); err != nil {
				t.Fatal(err)
			}
			if outcome != tc.expected {
				t.Fatalf("outcome=%q expected=%q", outcome, tc.expected)
			}
			assertProblem2OutcomeAccounting(t, store, outcome, tc.counter)
		})
	}
}

func TestProblem2WorkerCoversEveryTerminalOutcome(t *testing.T) {
	cases := []struct {
		name       string
		expected   string
		counter    string
		targetCase bool
		setup      func(*testing.T, *Store)
	}{
		{name: "promoted", expected: "promoted", counter: "promoted_candidates", setup: func(*testing.T, *Store) {}},
		{name: "stale alert observation", expected: "stale_alert_observation", counter: "stale_conflicts", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_states SET last_seen_at=? WHERE alert_id='problem2-alert'`, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing observation token", expected: "missing_observation_token", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET expected_alert_last_seen_at=NULL`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing current alert", expected: "missing_current_alert", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`DELETE FROM alert_states WHERE alert_id='problem2-alert'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing labels", expected: "missing_labels", counter: "skipped_missing_labels", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json=NULL`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "invalid labels", expected: "invalid_labels", counter: "skipped_missing_labels", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json='not-json'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "no matching rule", expected: "no_matching_rule", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET match_labels_json='{"service":"other"}'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "incomplete source coverage", expected: "incomplete_source_coverage", counter: "skipped_incomplete_sources", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE silence_recheck_job_candidates SET relevant_sources_json='[]'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "already suppressed", expected: "already_suppressed", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_states SET state='suppressed', is_suppressed=1 WHERE alert_id='problem2-alert'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "already resolved", expected: "already_resolved", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_states SET is_resolved=1 WHERE alert_id='problem2-alert'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing current target", expected: "missing_current_target", counter: "skipped_ineligible", targetCase: true, setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`DELETE FROM alert_target_states WHERE alert_id='target-episode' AND target='cluster-a'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "retired target", expected: "retired_target", counter: "skipped_ineligible", targetCase: true, setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_target_states SET retired_at=?, state='inactive', is_live=0 WHERE alert_id='target-episode' AND target='cluster-a'`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "stale target observation", expected: "stale_target_observation", counter: "stale_conflicts", targetCase: true, setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_target_states SET last_seen_at=? WHERE alert_id='target-episode' AND target='cluster-a'`, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "already suppressed target", expected: "already_suppressed", counter: "skipped_ineligible", targetCase: true, setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_target_states SET is_suppressed=1, state='suppressed' WHERE alert_id='target-episode' AND target='cluster-a'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "already resolved target", expected: "already_resolved", counter: "skipped_ineligible", targetCase: true, setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_target_states SET is_resolved=1 WHERE alert_id='target-episode' AND target='cluster-a'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "ineligible state", expected: "ineligible_state", counter: "skipped_ineligible", setup: func(t *testing.T, store *Store) {
			if _, err := store.db.Exec(`UPDATE alert_states SET state='active',is_live=1 WHERE alert_id='problem2-alert'`); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var store *Store
			if tc.targetCase {
				store, _ = seedProblem2TargetJob(t)
			} else {
				store, _, _ = seedProblem2OpenJob(t)
			}
			t.Cleanup(func() { _ = store.Close() })
			tc.setup(t, store)
			if _, err := store.RunSilenceRecheckBatch(100); err != nil {
				t.Fatal(err)
			}
			var outcome string
			if err := store.db.QueryRow(`SELECT outcome FROM silence_recheck_job_candidates`).Scan(&outcome); err != nil {
				t.Fatal(err)
			}
			if outcome != tc.expected {
				t.Fatalf("outcome=%q expected=%q", outcome, tc.expected)
			}
			assertProblem2OutcomeAccounting(t, store, tc.expected, tc.counter)
		})
	}
}

func assertProblem2OutcomeAccounting(t *testing.T, store *Store, outcome, expectedCounter string) {
	t.Helper()
	var status string
	var processed, total, promoted, ineligible, missingLabels, incomplete, stale, pending int
	if err := store.db.QueryRow(`SELECT status,processed_candidates,total_candidates,promoted_candidates,skipped_ineligible,skipped_missing_labels,skipped_incomplete_sources,stale_conflicts FROM silence_recheck_jobs`).Scan(&status, &processed, &total, &promoted, &ineligible, &missingLabels, &incomplete, &stale); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE outcome IS NULL AND processed_at IS NULL`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || processed != 1 || total != 1 || pending != 0 {
		t.Fatalf("outcome=%s terminal accounting status=%s processed=%d total=%d pending=%d", outcome, status, processed, total, pending)
	}
	counters := map[string]int{"promoted_candidates": promoted, "skipped_ineligible": ineligible, "skipped_missing_labels": missingLabels, "skipped_incomplete_sources": incomplete, "stale_conflicts": stale}
	for name, value := range counters {
		want := 0
		if name == expectedCounter {
			want = 1
		}
		if value != want {
			t.Fatalf("outcome=%s counter %s=%d expected=%d all=%v", outcome, name, value, want, counters)
		}
	}
}

type problem2PersistedInputSnapshot struct {
	SourceContext string
	Fingerprint   string
	Rules         []string
	Candidates    []string
}

func TestProblem2ProductionCanonicalizationIsStableAcrossFreshStores(t *testing.T) {
	first := buildProblem2CanonicalProductionStore(t, false)
	second := buildProblem2CanonicalProductionStore(t, true)

	if first.SourceContext != second.SourceContext || first.Fingerprint != second.Fingerprint {
		t.Fatalf("semantically identical production polls differ at parent: first=%+v second=%+v", first, second)
	}
	if len(first.Rules) != len(second.Rules) || len(first.Candidates) != len(second.Candidates) {
		t.Fatalf("semantically identical production polls differ in child counts: first=%+v second=%+v", first, second)
	}
	for i := range first.Rules {
		if first.Rules[i] != second.Rules[i] {
			t.Fatalf("canonical rule bytes differ at %d: first=%s second=%s", i, first.Rules[i], second.Rules[i])
		}
	}
	for i := range first.Candidates {
		if first.Candidates[i] != second.Candidates[i] {
			t.Fatalf("canonical candidate bytes differ at %d: first=%s second=%s", i, first.Candidates[i], second.Candidates[i])
		}
	}
}

func TestProblem2ProductionCanonicalizationCoversPermutedLiveTargets(t *testing.T) {
	first := buildProblem2CanonicalTargetProductionStore(t, false)
	second := buildProblem2CanonicalTargetProductionStore(t, true)
	if first.SourceContext != second.SourceContext || first.Fingerprint != second.Fingerprint {
		t.Fatalf("permuted live target production inputs differ at parent: first=%+v second=%+v", first, second)
	}
	if !reflect.DeepEqual(first.Rules, second.Rules) || !reflect.DeepEqual(first.Candidates, second.Candidates) {
		t.Fatalf("permuted live target production inputs differ in pinned children: first=%+v second=%+v", first, second)
	}
}

func buildProblem2CanonicalTargetProductionStore(t *testing.T, permuted bool) problem2PersistedInputSnapshot {
	t.Helper()
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, time.August, 18, 1, 0, 0, 0, time.UTC)
	labels := `{"alertname":"target-demo","service":"api"}`
	relevantSources := []SourceRef{{Name: "am-a", Cluster: "cluster-a"}, {Name: "am-b", Cluster: "cluster-a"}, {Name: "am-c", Cluster: "cluster-b"}, {Name: "am-d", Cluster: "cluster-b"}}
	targetA := LiveTargetSnapshot{Target: "cluster-a", State: StateActive, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-a", "am-b"}}
	targetB := LiveTargetSnapshot{Target: "cluster-b", State: StateActive, StartsAt: now.Add(-time.Hour), SourceNames: []string{"am-c", "am-d"}}
	if permuted {
		labels = `{"service":"api","alertname":"target-demo"}`
		for left, right := 0, len(relevantSources)-1; left < right; left, right = left+1, right-1 {
			relevantSources[left], relevantSources[right] = relevantSources[right], relevantSources[left]
		}
		for left, right := 0, len(targetA.SourceNames)-1; left < right; left, right = left+1, right-1 {
			targetA.SourceNames[left], targetA.SourceNames[right] = targetA.SourceNames[right], targetA.SourceNames[left]
		}
		for left, right := 0, len(targetB.SourceNames)-1; left < right; left, right = left+1, right-1 {
			targetB.SourceNames[left], targetB.SourceNames[right] = targetB.SourceNames[right], targetB.SourceNames[left]
		}
	}
	makeSource := func(name, cluster string, reverseMatchers bool) SourceSilenceState {
		matchers := []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}, {Name: "alertname", Value: "target-demo", IsEqual: true}}
		if reverseMatchers {
			matchers[0], matchers[1] = matchers[1], matchers[0]
		}
		return SourceSilenceState{SourceName: name, ClusterID: cluster, SyncStatus: "success", Silences: []SourceSilenceSnapshot{{SilenceID: name + "-target", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: matchers}}}
	}
	sources := []SourceSilenceState{makeSource("am-a", "cluster-a", permuted), makeSource("am-b", "cluster-a", !permuted), makeSource("am-c", "cluster-b", !permuted), makeSource("am-d", "cluster-b", permuted)}
	if permuted {
		for left, right := 0, len(sources)-1; left < right; left, right = left+1, right-1 {
			sources[left], sources[right] = sources[right], sources[left]
		}
	}
	live := LiveAlertSnapshot{
		AlertID: "canonical-target-alert", State: StateActive, Receiver: "team", GroupID: "group",
		GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: stringPointerForProblem2(labels), AnnotationsJSON: `{}`,
		StartsAt: now.Add(-time.Hour), LastSeenAt: now, RelevantUpstreams: []string{"am-a", "am-b", "am-c", "am-d"}, RelevantSources: relevantSources,
		Targets: []LiveTargetSnapshot{targetA, targetB},
	}
	firstPoll := PollCycleInput{LiveAlerts: []LiveAlertSnapshot{live}, SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true, "am-c": true, "am-d": true}, SilenceSources: sources, Timestamp: now}
	secondPoll := PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true, "am-c": true, "am-d": true}, SilenceSources: sources, Timestamp: now.Add(time.Minute)}
	for _, poll := range []PollCycleInput{firstPoll, secondPoll} {
		before, marshalErr := json.Marshal(poll)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if _, err = store.ApplyPollCycle(poll); err != nil {
			t.Fatal(err)
		}
		after, marshalErr := json.Marshal(poll)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if !bytes.Equal(before, after) {
			t.Fatal("production target poll mutated caller input")
		}
	}
	var jobID int64
	if err = store.db.QueryRow(`SELECT job_id FROM silence_recheck_jobs ORDER BY job_id DESC LIMIT 1`).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	var targetRows int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_target_states WHERE alert_id='canonical-target-alert'`).Scan(&targetRows); err != nil {
		t.Fatal(err)
	}
	if targetRows != 2 {
		t.Fatalf("nested production fixture created %d target rows", targetRows)
	}
	var candidateRows int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=?`, jobID).Scan(&candidateRows); err != nil {
		t.Fatal(err)
	}
	if candidateRows != 2 {
		t.Fatalf("nested production fixture created %d candidates", candidateRows)
	}
	var snapshot problem2PersistedInputSnapshot
	if err = store.db.QueryRow(`SELECT source_context_json,job_fingerprint FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&snapshot.SourceContext, &snapshot.Fingerprint); err != nil {
		t.Fatal(err)
	}
	rows, err := store.db.Query(`SELECT source_name,cluster_id,target_key,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json FROM silence_recheck_job_rules WHERE job_id=? ORDER BY source_name,target_key,silence_id,content_fingerprint,evaluation_fingerprint,matchers_json`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		values := make([]any, 8)
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err = rows.Scan(pointers...); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		encoded, marshalErr := json.Marshal(values)
		if marshalErr != nil {
			rows.Close()
			t.Fatal(marshalErr)
		}
		snapshot.Rules = append(snapshot.Rules, string(encoded))
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	rows, err = store.db.Query(`SELECT alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		values := make([]sql.NullString, 7)
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err = rows.Scan(pointers...); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		parts := make([]string, len(values))
		for i, value := range values {
			if value.Valid {
				parts[i] = value.String
			}
		}
		encoded, marshalErr := json.Marshal(parts)
		if marshalErr != nil {
			rows.Close()
			t.Fatal(marshalErr)
		}
		snapshot.Candidates = append(snapshot.Candidates, string(encoded))
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	return snapshot
}

func TestProblem2CandidateSnapshotSurvivesMaxAlertEviction(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	for index, alertID := range []string{"max-eviction-old", "max-eviction-new"} {
		lastSeen := now.Add(time.Duration(index-1) * time.Minute)
		if err = store.UpsertCurrentAlert(AlertCurrent{
			AlertID: alertID, State: StateInactive, RawState: StateActive, Receiver: "team", GroupID: "group",
			GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
			StartsAt: now.Add(-time.Hour), LastSeenAt: &lastSeen, RelevantUpstreams: `["am-a"]`,
			RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`, UpdatedAt: lastSeen,
		}); err != nil {
			t.Fatal(err)
		}
	}
	silence := SourceSilenceSnapshot{SilenceID: "max-eviction-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}}}
	if _, err = store.ApplyPollCycle(PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true}, SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}}, Timestamp: now}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RunHousekeeping(24*time.Hour, 1); err != nil {
		t.Fatal(err)
	}
	var alerts, candidates int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alert_states`).Scan(&alerts); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates`).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if alerts != 1 || candidates != 2 {
		t.Fatalf("max-alert eviction changed immutable job membership: alerts=%d candidates=%d", alerts, candidates)
	}
	if promoted, runErr := store.RunSilenceRecheckBatch(100); runErr != nil || promoted != 1 {
		t.Fatalf("max-alert eviction worker result promoted=%d err=%v", promoted, runErr)
	}
	var missing, pending, processed, total, skippedMissing, ineligible, incomplete, stale int
	var status string
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE outcome='missing_current_alert'`).Scan(&missing); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE processed_at IS NULL AND outcome IS NULL`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT status,processed_candidates,total_candidates,skipped_ineligible,skipped_missing_labels,skipped_incomplete_sources,stale_conflicts FROM silence_recheck_jobs`).Scan(&status, &processed, &total, &ineligible, &skippedMissing, &incomplete, &stale); err != nil {
		t.Fatal(err)
	}
	if missing != 1 || pending != 0 || status != "completed" || processed != total || ineligible != 1 || skippedMissing != 0 || incomplete != 0 || stale != 0 {
		t.Fatalf("max-alert eviction terminal accounting status=%s missing=%d pending=%d processed=%d total=%d ineligible=%d skippedMissing=%d incomplete=%d stale=%d", status, missing, pending, processed, total, ineligible, skippedMissing, incomplete, stale)
	}
}

func buildProblem2CanonicalProductionStore(t *testing.T, permuted bool) problem2PersistedInputSnapshot {
	t.Helper()
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, time.August, 18, 0, 0, 0, 0, time.UTC)
	if permuted {
		insertProblem2UnrelatedRule(t, store, now)
	}
	labels := `{"team":"core","service":"api","alertname":"demo"}`
	relevant := `[ {"name":"am-b","cluster":"cluster-a"},{"name":"am-a","cluster":"cluster-a"} ]`
	if !permuted {
		labels = `{"alertname":"demo","service":"api","team":"core"}`
		relevant = `[{"name":"am-a","cluster":"cluster-a"},{"name":"am-b","cluster":"cluster-a"}]`
	}
	if err = store.UpsertCurrentAlert(AlertCurrent{
		AlertID: "canonical-production-alert", State: StateInactive, RawState: StateActive, Receiver: "team", GroupID: "group",
		GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: stringPointerForProblem2(labels), AnnotationsJSON: `{}`,
		StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `["am-a","am-b"]`, RelevantSources: relevant, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	makeSilence := func(source, id, value string, reverseMatchers bool) SourceSilenceSnapshot {
		matchers := []SilenceMatcherSnapshot{{Name: "service", Value: value, IsEqual: true}, {Name: "alertname", Value: "demo", IsEqual: true}}
		if reverseMatchers {
			matchers[0], matchers[1] = matchers[1], matchers[0]
		}
		return SourceSilenceSnapshot{SilenceID: source + "-" + id, Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Matchers: matchers}
	}
	aSilences := []SourceSilenceSnapshot{makeSilence("am-a", "one", "api", permuted), makeSilence("am-a", "two", "api", !permuted)}
	bSilences := []SourceSilenceSnapshot{makeSilence("am-b", "one", "api", !permuted), makeSilence("am-b", "two", "api", permuted)}
	sources := []SourceSilenceState{
		{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: aSilences},
		{SourceName: "am-b", ClusterID: "cluster-a", SyncStatus: "success", Silences: bSilences},
	}
	if permuted {
		sources[0], sources[1] = sources[1], sources[0]
		sources[0].Silences[0], sources[0].Silences[1] = sources[0].Silences[1], sources[0].Silences[0]
		sources[1].Silences[0], sources[1].Silences[1] = sources[1].Silences[1], sources[1].Silences[0]
	}
	pollInput := PollCycleInput{SuccessfulUpstreams: map[string]bool{"am-a": true, "am-b": true}, SilenceSources: sources, Timestamp: now}
	beforeInput, err := json.Marshal(pollInput)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.ApplyPollCycle(pollInput)
	if err != nil {
		t.Fatal(err)
	}
	afterInput, err := json.Marshal(pollInput)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeInput, afterInput) {
		t.Fatalf("production creator mutated caller poll input: before=%s after=%s", beforeInput, afterInput)
	}
	if result.RecheckJobsQueued != 1 {
		t.Fatalf("canonical production poll did not create one job: %+v", result)
	}
	var snapshot problem2PersistedInputSnapshot
	if err = store.db.QueryRow(`SELECT source_context_json, job_fingerprint FROM silence_recheck_jobs WHERE job_id=1`).Scan(&snapshot.SourceContext, &snapshot.Fingerprint); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`SELECT source_name,cluster_id,target_key,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json FROM silence_recheck_job_rules WHERE job_id=1 ORDER BY source_name,target_key,silence_id,content_fingerprint,evaluation_fingerprint,matchers_json`,
		`SELECT alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json FROM silence_recheck_job_candidates WHERE job_id=1 ORDER BY alert_id,target_key`,
	} {
		rows, queryErr := store.db.Query(query)
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		for rows.Next() {
			values := make([]any, 7)
			pointers := make([]any, len(values))
			for i := range values {
				pointers[i] = &values[i]
			}
			if query == `SELECT source_name,cluster_id,target_key,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json FROM silence_recheck_job_rules WHERE job_id=1 ORDER BY source_name,target_key,silence_id,content_fingerprint,evaluation_fingerprint,matchers_json` {
				values = make([]any, 8)
				pointers = make([]any, len(values))
				for i := range values {
					pointers[i] = &values[i]
				}
			}
			if scanErr := rows.Scan(pointers...); scanErr != nil {
				rows.Close()
				t.Fatal(scanErr)
			}
			encoded, marshalErr := json.Marshal(values)
			if marshalErr != nil {
				rows.Close()
				t.Fatal(marshalErr)
			}
			if len(values) == 8 {
				snapshot.Rules = append(snapshot.Rules, string(encoded))
			} else {
				snapshot.Candidates = append(snapshot.Candidates, string(encoded))
			}
		}
		if queryErr = rows.Err(); queryErr != nil {
			rows.Close()
			t.Fatal(queryErr)
		}
		rows.Close()
	}
	return snapshot
}

func insertProblem2UnrelatedRule(t *testing.T, store *Store, now time.Time) {
	t.Helper()
	raw := now.Format(time.RFC3339Nano)
	result, err := store.db.Exec(`INSERT INTO silence_rules(cluster_id,silence_id,first_observed_at,last_observed_at) VALUES('noise','noise',?,?)`, raw, raw)
	if err != nil {
		t.Fatal(err)
	}
	ruleID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	result, err = store.db.Exec(`INSERT INTO silence_rule_episodes(rule_id,episode_number,first_observed_at,last_confirmed_at,open_marker) VALUES(?,?,?,?,1)`, ruleID, 1, raw, raw)
	if err != nil {
		t.Fatal(err)
	}
	episodeID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`INSERT INTO silence_revisions(episode_id,rule_id,revision_number,content_fingerprint,evaluation_fingerprint,matchers_json,starts_at,ends_at,created_by,comment,first_observed_at,last_observed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, episodeID, ruleID, 1, "noise-content", "noise-evaluation", `[]`, raw, raw, "noise", "noise", raw, raw); err != nil {
		t.Fatal(err)
	}
}

type problem2RestartPinnedSnapshot struct {
	Parent     []string
	Rules      []string
	Candidates []string
}

func captureProblem2RestartPinnedSnapshot(t *testing.T, db *sql.DB) problem2RestartPinnedSnapshot {
	t.Helper()
	snapshot := problem2RestartPinnedSnapshot{}
	var sourceContext, sourceFingerprint, jobFingerprint, trigger, status, cursor sql.NullString
	var total, processed, promoted, ineligible, missingLabels, incomplete, stale int
	if err := db.QueryRow(`SELECT source_context_json,source_context_fingerprint,job_fingerprint,trigger_type,status,cursor_alert_id,total_candidates,processed_candidates,promoted_candidates,skipped_ineligible,skipped_missing_labels,skipped_incomplete_sources,stale_conflicts FROM silence_recheck_jobs WHERE job_id=1`).Scan(&sourceContext, &sourceFingerprint, &jobFingerprint, &trigger, &status, &cursor, &total, &processed, &promoted, &ineligible, &missingLabels, &incomplete, &stale); err != nil {
		t.Fatal(err)
	}
	snapshot.Parent = []string{sourceContext.String, sourceFingerprint.String, jobFingerprint.String, trigger.String, status.String, cursor.String, strconv.Itoa(total), strconv.Itoa(processed), strconv.Itoa(promoted), strconv.Itoa(ineligible), strconv.Itoa(missingLabels), strconv.Itoa(incomplete), strconv.Itoa(stale)}
	queries := []struct {
		query  string
		count  int
		target *[]string
	}{
		{query: `SELECT source_name,cluster_id,target_key,rule_id,revision_id,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json FROM silence_recheck_job_rules WHERE job_id=1 ORDER BY source_name,target_key,silence_id,content_fingerprint,evaluation_fingerprint,matchers_json`, count: 10, target: &snapshot.Rules},
		{query: `SELECT alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json,processed_at,outcome FROM silence_recheck_job_candidates WHERE job_id=1 ORDER BY alert_id,target_key`, count: 9, target: &snapshot.Candidates},
	}
	for _, item := range queries {
		rows, err := db.Query(item.query)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]sql.NullString, item.count)
			pointers := make([]any, len(values))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			parts := make([]string, len(values))
			for i, value := range values {
				if value.Valid {
					parts[i] = value.String
				}
			}
			encoded, marshalErr := json.Marshal(parts)
			if marshalErr != nil {
				rows.Close()
				t.Fatal(marshalErr)
			}
			*item.target = append(*item.target, string(encoded))
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
	}
	return snapshot
}

func captureProblem2ImmutableBytes(t *testing.T, db *sql.DB) string {
	t.Helper()
	result := struct {
		Jobs       []string `json:"jobs"`
		Rules      []string `json:"rules"`
		Candidates []string `json:"candidates"`
	}{}
	queries := []struct {
		query  string
		count  int
		target *[]string
	}{
		{query: `SELECT job_id,source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,total_candidates FROM silence_recheck_jobs ORDER BY job_id`, count: 6, target: &result.Jobs},
		{query: `SELECT job_id,source_name,cluster_id,target_key,rule_id,revision_id,silence_id,content_fingerprint,observed_state,evaluation_fingerprint,matchers_json FROM silence_recheck_job_rules ORDER BY job_id,source_name,target_key,silence_id,content_fingerprint,evaluation_fingerprint,matchers_json,rule_id,revision_id`, count: 11, target: &result.Rules},
		{query: `SELECT job_id,alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json FROM silence_recheck_job_candidates ORDER BY job_id,alert_id,target_key`, count: 8, target: &result.Candidates},
	}
	for _, item := range queries {
		rows, err := db.Query(item.query)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]sql.NullString, item.count)
			pointers := make([]any, len(values))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			parts := make([]string, len(values))
			for i, value := range values {
				if value.Valid {
					parts[i] = value.String
				}
			}
			encoded, marshalErr := json.Marshal(parts)
			if marshalErr != nil {
				rows.Close()
				t.Fatal(marshalErr)
			}
			*item.target = append(*item.target, string(encoded))
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
