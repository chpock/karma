package alertlifecycle

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestPinnedRecheckRuleCannotCrossSourceReassignment(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	silence := SourceSilenceSnapshot{
		SilenceID: "cluster-a-rule", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}},
		Timestamp:           now,
	}); err != nil {
		t.Fatal(err)
	}
	labels := `{"service":"api"}`
	live := LiveAlertSnapshot{
		AlertID: "cluster-b-alert", State: StateActive, Receiver: "team", GroupID: "group",
		GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
		StartsAt: now, LastSeenAt: now, RelevantUpstreams: []string{"am-a"},
		RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-b"}},
		Targets:         []LiveTargetSnapshot{{Target: "cluster-b", State: StateActive, StartsAt: now, SourceNames: []string{"am-a"}}},
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{live}, SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources: []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-b", SyncStatus: "success"}}, Timestamp: now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-b", SyncStatus: "success"}}, Timestamp: now.Add(2 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if promoted, runErr := store.RunSilenceRecheckBatch(100); runErr != nil || promoted.OutcomeCounts.Promoted != 0 {
		t.Fatalf("old target rule crossed reassignment: promoted=%d err=%v", promoted.OutcomeCounts.Promoted, runErr)
	}
	current, err := store.GetCurrentAlert("cluster-b-alert")
	if err != nil || current == nil || current.IsSuppressed {
		t.Fatalf("reassigned target was suppressed by old rule: current=%+v err=%v", current, err)
	}
}

func TestRecheckCandidateBecomingEligibleCreatesImmutableSuccessor(t *testing.T) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	silence := SourceSilenceSnapshot{
		SilenceID: "cursor-rule", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}
	labels := `{"service":"api"}`
	for _, id := range []string{"b-inactive", "c-inactive"} {
		if err = store.UpsertCurrentAlert(AlertCurrent{
			AlertID: id, State: StateInactive, RawState: StateActive, IsLive: false,
			Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`, LabelsJSON: labels,
			MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now, LastSeenAt: &now,
			RelevantUpstreams: `["am-a"]`, RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}},
		Timestamp:           now,
	}); err != nil {
		t.Fatal(err)
	}
	if promoted, runErr := store.RunSilenceRecheckBatch(1); runErr != nil || promoted.OutcomeCounts.Promoted != 1 {
		t.Fatalf("first batch promoted=%d err=%v", promoted.OutcomeCounts.Promoted, runErr)
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		LiveAlerts: []LiveAlertSnapshot{{
			AlertID: "a-late", State: StateActive, Receiver: "team", GroupID: "group",
			GroupLabelsJSON: `{}`, LabelsJSON: labels, MatchLabelsJSON: &labels,
			AnnotationsJSON: `{}`, StartsAt: now, LastSeenAt: now,
			RelevantUpstreams: []string{"am-a"}, RelevantSources: []SourceRef{{Name: "am-a", Cluster: "cluster-a"}},
		}},
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}},
		Timestamp:           now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	result, err := store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}},
		Timestamp:           now.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RecheckJobsQueued != 1 {
		t.Fatalf("eligible alert must create one successor job: %+v", result)
	}
	var originalCandidates, originalTotal int
	if err = store.db.QueryRow(`SELECT COUNT(*), total_candidates FROM silence_recheck_job_candidates candidates JOIN silence_recheck_jobs jobs USING(job_id) WHERE jobs.job_id=1`).Scan(&originalCandidates, &originalTotal); err != nil {
		t.Fatal(err)
	}
	if originalCandidates != 2 || originalTotal != 2 {
		t.Fatalf("original job membership changed: candidates=%d total=%d", originalCandidates, originalTotal)
	}
	var successorID int64
	var trigger string
	var successorCandidates, successorTotal int
	if err = store.db.QueryRow(`SELECT job_id, trigger_type, (SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=jobs.job_id), total_candidates FROM silence_recheck_jobs jobs WHERE job_id > 1`).Scan(&successorID, &trigger, &successorCandidates, &successorTotal); err != nil {
		t.Fatal(err)
	}
	if successorID == 0 || trigger != "alert_became_eligible" || successorCandidates != 1 || successorTotal != 1 {
		t.Fatalf("invalid immutable successor: id=%d trigger=%q candidates=%d total=%d", successorID, trigger, successorCandidates, successorTotal)
	}
	var lateMembership int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=? AND alert_id='a-late'`, successorID).Scan(&lateMembership); err != nil {
		t.Fatal(err)
	}
	if lateMembership != 1 {
		t.Fatalf("successor does not own newly eligible alert: count=%d", lateMembership)
	}
}

func TestC4RecheckResumesPinnedContextAndCandidatesAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	labels := `{"service":"api"}`
	for _, id := range []string{"a-first", "b-second"} {
		if err = store.UpsertCurrentAlert(AlertCurrent{
			AlertID: id, State: StateInactive, RawState: StateActive,
			Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`,
			LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
			StartsAt: now, LastSeenAt: &now, RelevantUpstreams: `["am-a"]`,
			RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`,
		}); err != nil {
			t.Fatal(err)
		}
		tx, txErr := store.db.BeginTx(context.Background(), nil)
		if txErr != nil {
			t.Fatal(txErr)
		}
		if txErr = upsertTargetStateTx(tx, id, targetStateRow{
			Target: "cluster-a", State: StateInactive, RawState: StateActive,
			IsLive: false, StartsAt: now, LastSeenAt: &now,
			SourceMembers: []string{"am-a"},
		}, "seed", now); txErr != nil {
			_ = tx.Rollback()
			t.Fatal(txErr)
		}
		if txErr = tx.Commit(); txErr != nil {
			t.Fatal(txErr)
		}
	}
	silence := SourceSilenceSnapshot{
		SilenceID: "restart-rule", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
	}
	if _, err = store.ApplyPollCycle(PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"am-a": true},
		SilenceSources:      []SourceSilenceState{{SourceName: "am-a", ClusterID: "cluster-a", SyncStatus: "success", Silences: []SourceSilenceSnapshot{silence}}},
		Timestamp:           now,
	}); err != nil {
		t.Fatal(err)
	}
	if promoted, runErr := store.RunSilenceRecheckBatch(1); runErr != nil || promoted.OutcomeCounts.Promoted != 1 {
		t.Fatalf("first batch promoted=%d err=%v", promoted.OutcomeCounts.Promoted, runErr)
	}
	var jobID int64
	var status, source, cluster string
	var processed, pending int
	if err = store.db.QueryRow(`SELECT job_id,status,processed_candidates FROM silence_recheck_jobs ORDER BY job_id LIMIT 1`).Scan(&jobID, &status, &processed); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT source_name,cluster_id FROM silence_recheck_job_rules WHERE job_id=?`, jobID).Scan(&source, &cluster); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=? AND processed_at IS NULL`, jobID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if status != "running" || processed != 1 || pending != 1 || source != "am-a" || cluster != "cluster-a" {
		t.Fatalf("pre-restart context status=%s processed=%d pending=%d source=%s cluster=%s", status, processed, pending, source, cluster)
	}
	if _, err = store.db.Exec(`DELETE FROM alertmanager_source_silences WHERE source_name='am-a'`); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`UPDATE alertmanager_sources SET current_cluster_id='cluster-b' WHERE source_name='am-a'`); err != nil {
		t.Fatal(err)
	}
	var currentCluster string
	var mutableRules int
	if err = store.db.QueryRow(`SELECT current_cluster_id FROM alertmanager_sources WHERE source_name='am-a'`).Scan(&currentCluster); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM alertmanager_source_silences WHERE source_name='am-a'`).Scan(&mutableRules); err != nil {
		t.Fatal(err)
	}
	if currentCluster != "cluster-b" || mutableRules != 0 {
		t.Fatalf("mutable topology was not reassigned: cluster=%s rules=%d", currentCluster, mutableRules)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if promoted, runErr := store.RunSilenceRecheckBatch(1); runErr != nil || promoted.OutcomeCounts.Promoted != 2 {
		t.Fatalf("resumed batch promoted=%d err=%v", promoted.OutcomeCounts.Promoted, runErr)
	}
	var targetSuppressed int
	if err = store.db.QueryRow(`SELECT is_suppressed FROM alert_target_states WHERE alert_id='b-second' AND target='cluster-a'`).Scan(&targetSuppressed); err != nil {
		t.Fatal(err)
	}
	if targetSuppressed != 1 {
		t.Fatal("restarted worker did not evaluate the persisted cluster-a rule for cluster-a target")
	}
	if _, err = store.RunSilenceRecheckBatch(1); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(`SELECT status,processed_candidates FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&status, &processed); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || processed != 2 {
		t.Fatalf("resumed job status=%s processed=%d", status, processed)
	}
}
