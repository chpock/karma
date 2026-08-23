package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	plabels "github.com/prometheus/prometheus/model/labels"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	"github.com/prymitive/karma/internal/models"
)

type recordedReplay struct {
	Grids []struct {
		AlertGroups []struct {
			ID       string            `json:"id"`
			Receiver string            `json:"receiver"`
			Labels   map[string]string `json:"labels"`
			Alerts   []struct {
				Labels       map[string]string  `json:"labels"`
				Annotations  models.Annotations `json:"annotations"`
				Receiver     string             `json:"receiver"`
				State        string             `json:"state"`
				StartsAt     time.Time          `json:"startsAt"`
				Alertmanager []struct {
					Name       string    `json:"name"`
					Cluster    string    `json:"cluster"`
					State      string    `json:"state"`
					StartsAt   time.Time `json:"startsAt"`
					SilencedBy []string  `json:"silencedBy"`
				} `json:"alertmanager"`
			} `json:"alerts"`
		} `json:"alertGroups"`
	} `json:"grids"`
}

type problem5ReplayCapability string

const (
	problem5CapabilityAll       = problem5ReplayCapability("all")
	problem5CapabilityReplay    = problem5ReplayCapability("recorded_replay")
	problem5CapabilitySuccessor = problem5ReplayCapability("eligibility_successor")
	problem5CapabilityInput     = problem5ReplayCapability("immutable_input")
	problem5CapabilityFinal     = problem5ReplayCapability("final_rows")
	problem5CapabilityAPI       = problem5ReplayCapability("stats_warning")
)

func TestRecordedTrafficReplayThroughLifecycleStore(t *testing.T) {
	runProblem5RecordedTraffic(t, problem5CapabilityAll)
}

func runProblem5RecordedTraffic(t *testing.T, capability problem5ReplayCapability) {
	_, filename, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "tests", "compress", "alerts.json"))
	if err != nil {
		t.Fatalf("read recorded traffic: %v", err)
	}
	var replay recordedReplay
	if err = json.Unmarshal(raw, &replay); err != nil {
		t.Fatalf("decode recorded traffic: %v", err)
	}

	groups := make([]models.AlertGroup, 0)
	rawLabels := map[string]*string{}
	successful := map[string]bool{}
	sourceByName := map[string]alertlifecycle.SourceSilenceState{}
	for _, grid := range replay.Grids {
		for _, recordedGroup := range grid.AlertGroups {
			group := models.AlertGroup{
				ID: recordedGroup.ID, Receiver: recordedGroup.Receiver,
				Labels: plabels.FromMap(recordedGroup.Labels), Alerts: models.AlertList{},
			}
			for _, recordedAlert := range recordedGroup.Alerts {
				alertLabels := plabels.FromMap(recordedAlert.Labels)
				alert := models.Alert{
					LabelsFP: strconv.FormatUint(alertLabels.Hash(), 16),
					State:    replayAlertState(recordedAlert.State), Receiver: recordedAlert.Receiver,
					Labels: alertLabels, Annotations: recordedAlert.Annotations,
					StartsAt: recordedAlert.StartsAt,
				}
				for _, recordedMember := range recordedAlert.Alertmanager {
					alert.Alertmanager = append(alert.Alertmanager, models.AlertmanagerInstance{
						Name: recordedMember.Name, Cluster: recordedMember.Cluster,
						State: replayAlertState(recordedMember.State), StartsAt: recordedMember.StartsAt,
						SilencedBy: append([]string{}, recordedMember.SilencedBy...),
					})
					successful[recordedMember.Name] = true
					sourceByName[recordedMember.Name] = alertlifecycle.SourceSilenceState{
						SourceName: recordedMember.Name, ClusterID: recordedMember.Cluster, SyncStatus: "success",
					}
				}
				fingerprint := alert.LabelsFingerprint()
				labelsJSON, _ := json.Marshal(recordedAlert.Labels)
				labelsString := string(labelsJSON)
				rawLabels[fingerprint] = &labelsString
				group.Alerts = append(group.Alerts, alert)
			}
			groups = append(groups, group)
		}
	}

	snapshots := buildLiveLifecycleSnapshot(groups, rawLabels)
	if len(groups) < 1000 || len(snapshots) == 0 {
		t.Fatalf("recorded replay did not load expected traffic: groups=%d snapshots=%d", len(groups), len(snapshots))
	}
	matcherValue := replayInstanceMatcherValue(snapshots)
	allManifest := buildProblem5ReplayManifest(snapshots)
	manifest := make([]problem5ReplayScope, 0, len(allManifest))
	for _, scope := range allManifest {
		if scope.State == alertlifecycle.StateActive && replayScopeInstance(scope) != matcherValue {
			manifest = append(manifest, scope)
		}
	}
	if len(manifest) < 80 {
		t.Fatalf("recorded eligible replay manifest scopes=%d want at least 80", len(manifest))
	}
	successorRecordedID := manifest[len(manifest)-1].AlertID
	successorRecorded := replaySnapshotByID(snapshots, successorRecordedID)
	if successorRecorded == nil {
		t.Fatalf("missing retained successor scope %q", successorRecordedID)
	}
	sources := make([]alertlifecycle.SourceSilenceState, 0, len(sourceByName))
	for _, source := range sourceByName {
		sources = append(sources, source)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].SourceName < sources[j].SourceName })
	if len(sources) == 0 {
		t.Fatal("recorded replay has no configured source")
	}
	failedSourceName := "problem5-recorded-failed-source"
	failedSourceCluster := "problem5-recorded-failed-cluster"
	sources = append(sources, alertlifecycle.SourceSilenceState{SourceName: failedSourceName, ClusterID: failedSourceCluster, SyncStatus: "failed", LastError: "recorded source failure"})
	path := filepath.Join(t.TempDir(), "replay.sqlite")
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatalf("open replay store: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		_ = store.Close()
		t.Fatalf("open replay inspection db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = store.Close()
	})
	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	if _, err = store.ApplyPollCycle(alertlifecycle.PollCycleInput{
		LiveAlerts: snapshots, SuccessfulUpstreams: successful, SilenceSources: sources,
		Timestamp: now,
	}); err != nil {
		t.Fatalf("apply recorded traffic replay: %v", err)
	}
	if bootstrap, bootstrapErr := store.RunSilenceRecheckBatchContext(context.Background(), 20); bootstrapErr != nil || bootstrap.Status != "completed" {
		t.Fatalf("complete bootstrap replay job: result=%+v err=%v", bootstrap, bootstrapErr)
	}
	failedLabels := `{"instance":"problem5-failed-context"}`
	failedAt := now.Add(-time.Minute)
	if err = store.UpsertCurrentAlert(alertlifecycle.AlertCurrent{
		AlertID: "problem5-failed-context", State: alertlifecycle.StateInactive, RawState: alertlifecycle.StateActive,
		Receiver: "problem5", GroupID: "problem5-failed", GroupLabelsJSON: `{}`, LabelsJSON: failedLabels,
		MatchLabelsJSON: &failedLabels, AnnotationsJSON: `{}`, StartsAt: now.Add(-2 * time.Hour), LastSeenAt: &failedAt,
		RelevantUpstreams: fmt.Sprintf(`[%q]`, failedSourceName), RelevantSources: fmt.Sprintf(`[{"name":%q,"cluster":%q}]`, failedSourceName, failedSourceCluster), UpdatedAt: now,
	}); err != nil {
		t.Fatalf("seed pinned-failed replay scope: %v", err)
	}

	var target string
	for _, source := range sources {
		if source.ClusterID != "" {
			target = source.ClusterID
			break
		}
	}
	projected, err := store.ListTargetAlertsForPreview(target)
	if err != nil {
		t.Fatalf("read replay target projection: %v", err)
	}
	if len(projected) == 0 {
		t.Fatalf("recorded replay produced no target projection for %q", target)
	}
	labelNames, total, err := store.ListTargetLabelNames(target, "alert", 1000)
	if err != nil {
		t.Fatalf("read replay label index: %v", err)
	}
	if total == 0 || len(labelNames) == 0 || labelNames[0].Name != "alertname" {
		t.Fatalf("unexpected replay label index: total=%d labels=%+v", total, labelNames)
	}

	activeSources := replaySourcesWithSilence(sources, alertlifecycle.SourceSilenceSnapshot{
		SilenceID: "problem5-recorded-active-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "instance", Value: matcherValue, IsEqual: true}},
	})
	creation, err := store.ApplyPollCycle(alertlifecycle.PollCycleInput{
		LiveAlerts: []alertlifecycle.LiveAlertSnapshot{*successorRecorded}, SuccessfulUpstreams: successful, SilenceSources: activeSources, Timestamp: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("apply recorded disappearance: %v", err)
	}
	if creation.RecheckJobsQueued != 1 || creation.RecheckJobsSourceContextChanged != 1 || creation.RecheckJobsAlertBecameEligible != 0 {
		t.Fatalf("recorded source-context creation=%+v", creation)
	}
	var sourceJobID, totalCandidates int
	if err = db.QueryRow(`SELECT job_id,total_candidates FROM silence_recheck_jobs WHERE total_candidates>0 ORDER BY job_id DESC LIMIT 1`).Scan(&sourceJobID, &totalCandidates); err != nil {
		t.Fatal(err)
	}
	if totalCandidates < 80 {
		t.Fatalf("recorded source-context job candidates=%d want at least 80", totalCandidates)
	}
	controlledRules, err := captureProblem5ControlledRules(db, activeSources)
	if err != nil {
		t.Fatal(err)
	}
	expectedRelevantSources, expectedTargetMembers, err := buildProblem5ControlledCandidateRefs(snapshots, failedSourceName, failedSourceCluster)
	if err != nil {
		t.Fatal(err)
	}
	if err = assertProblem5ReplayManifest(db, sourceJobID, manifest[:len(manifest)-1]); err != nil {
		t.Fatalf("retained=%s first=%s last=%s: %v", successorRecordedID, manifest[0].AlertID, manifest[len(manifest)-1].AlertID, err)
	}

	candidateIDs, err := replayCandidateIDs(db, sourceJobID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidateIDs) < 3 {
		t.Fatalf("recorded candidate partition fixture=%d want at least 3", len(candidateIDs))
	}
	resolvedID, refiredID := candidateIDs[0], candidateIDs[1]
	if _, err = store.ResolveAlertContext(context.Background(), resolvedID, replayStringPtr("problem5-operator"), replayStringPtr("recorded resolved"), now.Add(2*time.Minute)); err != nil {
		t.Fatalf("resolve recorded scope: %v", err)
	}
	refired := replaySnapshotByID(snapshots, refiredID)
	if refired == nil {
		t.Fatalf("missing recorded refired scope %q", refiredID)
	}
	if _, err = store.ApplyPollCycle(alertlifecycle.PollCycleInput{LiveAlerts: []alertlifecycle.LiveAlertSnapshot{*refired}, SuccessfulUpstreams: successful, SilenceSources: activeSources, Timestamp: now.Add(3 * time.Minute)}); err != nil {
		t.Fatalf("refire recorded scope: %v", err)
	}
	if _, err = store.ApplyPollCycle(alertlifecycle.PollCycleInput{SuccessfulUpstreams: successful, SilenceSources: activeSources, Timestamp: now.Add(4 * time.Minute)}); err != nil {
		t.Fatalf("re-disappear recorded scope: %v", err)
	}
	var successorCount, successorJobID int
	if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs WHERE trigger_type='alert_became_eligible'`).Scan(&successorCount); err != nil {
		t.Fatal(err)
	}
	if successorCount < 2 {
		t.Fatalf("recorded eligibility successor count=%d want at least 2", successorCount)
	}
	if err = db.QueryRow(`SELECT j.job_id FROM silence_recheck_jobs j WHERE j.trigger_type='alert_became_eligible' AND EXISTS (SELECT 1 FROM silence_recheck_job_candidates c WHERE c.job_id=j.job_id AND c.alert_id=?)`, successorRecordedID).Scan(&successorJobID); err != nil {
		t.Fatal(err)
	}
	var sourceSuccessorCount, disjointSuccessorCount int
	if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=? AND alert_id=?`, sourceJobID, successorRecordedID).Scan(&sourceSuccessorCount); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=? AND alert_id=?`, successorJobID, successorRecordedID).Scan(&disjointSuccessorCount); err != nil {
		t.Fatal(err)
	}
	if sourceSuccessorCount != 0 || disjointSuccessorCount == 0 {
		t.Fatalf("recorded successor is not disjoint: source=%d successor=%d alert=%s", sourceSuccessorCount, disjointSuccessorCount, successorRecordedID)
	}
	eligibilityJobIDs, err := replayEligibilityJobIDs(db)
	if err != nil {
		t.Fatal(err)
	}
	expectedSuccessorSnapshots := map[string]alertlifecycle.LiveAlertSnapshot{}
	if snapshot := replaySnapshotByID(snapshots, refiredID); snapshot != nil {
		expectedSuccessorSnapshots[refiredID] = *snapshot
	}
	if snapshot := replaySnapshotByID(snapshots, successorRecordedID); snapshot != nil {
		expectedSuccessorSnapshots[successorRecordedID] = *snapshot
	}
	expectedSuccessorTimes := map[string]time.Time{successorRecordedID: now.Add(time.Minute), refiredID: now.Add(3 * time.Minute)}
	if err = assertReplayEligibilitySuccessors(db, eligibilityJobIDs, expectedSuccessorSnapshots, expectedSuccessorTimes); err != nil {
		t.Fatal(err)
	}

	replayWorkerNow := now.Add(5 * time.Minute)
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		Now: func() time.Time { return replayWorkerNow },
	})
	firstBatch, err := store.RunSilenceRecheckBatchContext(context.Background(), 20)
	if err != nil || firstBatch.Disposition != alertlifecycle.RecheckBatchCommitted {
		t.Fatalf("recorded first worker batch=%+v err=%v", firstBatch, err)
	}
	if firstBatch.JobID != int64(sourceJobID) || firstBatch.TotalCandidates != totalCandidates || firstBatch.ProcessedCandidates == 0 || firstBatch.Status != "running" {
		t.Fatalf("recorded first batch metadata=%+v", firstBatch)
	}
	inputBeforeRestart, err := captureReplayInputRows(db, sourceJobID)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{
		Now: func() time.Time { return replayWorkerNow },
	})
	inputAfterRestart, err := captureReplayInputRows(db, sourceJobID)
	if err != nil {
		t.Fatal(err)
	}
	if inputAfterRestart != inputBeforeRestart {
		t.Fatal("recorded immutable job input changed across Store restart")
	}
	if err = assertProblem5IndependentInputContract(db, sourceJobID, len(manifest)+1, len(sources), 2); err != nil {
		t.Fatal(err)
	}
	if err = assertProblem5ControlledInput(db, sourceJobID, sources, controlledRules, expectedRelevantSources, expectedTargetMembers); err != nil {
		t.Fatal(err)
	}
	if err = assertProblem5IndependentImmutableModel(db, sourceJobID, activeSources, sources, controlledRules, expectedRelevantSources, expectedTargetMembers, snapshots, manifest, successorRecordedID, matcherValue, now); err != nil {
		t.Fatal(err)
	}

	batchCount := 1
	for {
		batch, batchErr := store.RunSilenceRecheckBatchContext(context.Background(), 20)
		if batchErr != nil {
			t.Fatalf("finish recorded source-context batches: result=%+v err=%v", batch, batchErr)
		}
		batchCount++
		if batch.JobID == int64(sourceJobID) && batch.Status == "completed" {
			break
		}
		if batchCount > 100 {
			t.Fatalf("recorded source-context job did not complete after %d batches", batchCount)
		}
	}
	if batchCount < 3 {
		t.Fatalf("recorded source-context batches=%d want at least 3", batchCount)
	}
	if err = assertReplayJobAccounting(db, sourceJobID); err != nil {
		t.Fatal(err)
	}
	if err = assertReplayPinnedSourceFailure(db, sourceJobID, failedSourceName); err != nil {
		t.Fatal(err)
	}
	var incompleteCount int
	if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=? AND outcome='incomplete_source_coverage'`, sourceJobID).Scan(&incompleteCount); err != nil {
		t.Fatal(err)
	}
	if incompleteCount == 0 {
		t.Fatalf("recorded pinned-failed source job=%d produced no incomplete_source_coverage outcomes", sourceJobID)
	}
	if err = assertReplayPartition(db, sourceJobID, resolvedID, refiredID, "problem5-failed-context"); err != nil {
		t.Fatal(err)
	}

	for {
		head, headErr := store.RecheckQueueHead(context.Background())
		if headErr != nil {
			t.Fatal(headErr)
		}
		if head.JobID == 0 {
			break
		}
		if head.Status == "failed" || head.Status == "abandoned" {
			t.Fatalf("recorded successor queue head=%+v", head)
		}
		if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 20); err != nil {
			t.Fatal(err)
		}
	}

	operatorSources := replaySourcesWithSilence(replaySourcesWithStatus(activeSources, "success"), alertlifecycle.SourceSilenceSnapshot{
		SilenceID: "problem5-recorded-retry-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "instance", Value: ".*", IsRegex: true}},
	})
	retryJobID, err := seedReplayOperatorJob(t, store, db, operatorSources, successful, now.Add(9*time.Minute), "problem5-recorded-retry-alert")
	if err != nil {
		t.Fatal(err)
	}
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{Now: func() time.Time { return replayWorkerNow }, RecheckFault: func(stage string) error {
		if stage == "persist_candidate" {
			return errors.New("problem5 recorded retry failure")
		}
		return nil
	}})
	failedRetry, retryErr := store.RunSilenceRecheckBatchContext(context.Background(), 20)
	if retryErr == nil || !alertlifecycle.IsRecheckDurableFailure(retryErr) || failedRetry.JobID != int64(retryJobID) || failedRetry.Status != "failed" {
		t.Fatalf("recorded retry failed-head result=%+v err=%v", failedRetry, retryErr)
	}
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{Now: func() time.Time { return replayWorkerNow }})
	retryAction, retryActionErr := store.RetryRecheckJobContext(context.Background(), int64(retryJobID), "problem5-recorded-retry", "recorded retry", nil, now.Add(10*time.Minute))
	if retryActionErr != nil || retryAction.JobID != int64(retryJobID) || retryAction.CurrentStatus != "queued" {
		t.Fatalf("recorded retry action=%+v err=%v", retryAction, retryActionErr)
	}
	for {
		batch, batchErr := store.RunSilenceRecheckBatchContext(context.Background(), 20)
		if batchErr != nil {
			t.Fatalf("recorded retry continuation result=%+v err=%v", batch, batchErr)
		}
		if batch.JobID == int64(retryJobID) && batch.Status == "completed" {
			break
		}
	}

	abandonSources := replaySourcesWithSilence(operatorSources, alertlifecycle.SourceSilenceSnapshot{
		SilenceID: "problem5-recorded-abandon-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
		Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "instance", Value: ".*", IsRegex: true}},
	})
	abandonJobID, err := seedReplayOperatorJob(t, store, db, abandonSources, successful, now.Add(11*time.Minute), "problem5-recorded-abandon-alert")
	if err != nil {
		t.Fatal(err)
	}
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{Now: func() time.Time { return replayWorkerNow }, RecheckFault: func(stage string) error {
		if stage == "persist_candidate" {
			return errors.New("problem5 recorded abandon failure")
		}
		return nil
	}})
	failedAbandon, abandonRunErr := store.RunSilenceRecheckBatchContext(context.Background(), 20)
	if abandonRunErr == nil || !alertlifecycle.IsRecheckDurableFailure(abandonRunErr) || failedAbandon.JobID != int64(abandonJobID) || failedAbandon.Status != "failed" {
		t.Fatalf("recorded abandon failed-head result=%+v err=%v", failedAbandon, abandonRunErr)
	}
	store.SetRecheckIntegrationTestHooks(&alertlifecycle.RecheckIntegrationTestHooks{Now: func() time.Time { return replayWorkerNow }})
	abandonAction, abandonActionErr := store.AbandonRecheckJobContext(context.Background(), int64(abandonJobID), "problem5-recorded-abandon", "recorded abandon", nil, now.Add(12*time.Minute))
	if abandonActionErr != nil || abandonAction.JobID != int64(abandonJobID) || abandonAction.CurrentStatus != "abandoned" {
		t.Fatalf("recorded abandon action=%+v err=%v", abandonAction, abandonActionErr)
	}

	if err = assertReplaySuppressionRows(db); err != nil {
		t.Fatal(err)
	}
	previousStore, previousEnabled := lifecycleStore, config.Config.AlertLifecycle.Enabled
	lifecycleStore, config.Config.AlertLifecycle.Enabled = store, true
	t.Cleanup(func() { lifecycleStore, config.Config.AlertLifecycle.Enabled = previousStore, previousEnabled })
	merged := mergeLifecycleStoredAlertsContext(context.Background(), nil, true)
	if len(merged) == 0 || len(merged[0].Alerts) == 0 {
		t.Fatal("recorded replay merge returned no lifecycle alerts")
	}
	statsRecorder := httptest.NewRecorder()
	alertLifecycleStats(statsRecorder, httptest.NewRequest(http.MethodGet, "/alerts-history/stats", http.NoBody))
	if statsRecorder.Code != http.StatusOK {
		t.Fatalf("recorded stats HTTP status=%d body=%s", statsRecorder.Code, statsRecorder.Body.String())
	}
	warningRecorder := httptest.NewRecorder()
	alertLifecycleWarning(warningRecorder, httptest.NewRequest(http.MethodGet, "/alerts-history/warning", http.NoBody))
	if warningRecorder.Code != http.StatusOK {
		t.Fatalf("recorded warning HTTP status=%d body=%s", warningRecorder.Code, warningRecorder.Body.String())
	}
	if err = assertProblem5ExactStatsWarningResponses(db, path, statsRecorder.Body.Bytes(), warningRecorder.Body.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err = assertProblem5ReplayFinalState(db, sourceJobID, successorJobID, retryJobID, abandonJobID, len(manifest)+1); err != nil {
		t.Fatal(err)
	}
	if err = assertProblem5IndependentFinalModel(db, snapshots, resolvedID, matcherValue, sourceJobID, successorJobID, retryJobID, abandonJobID); err != nil {
		t.Fatal(err)
	}
	expectedFinalCandidates, expectedFinalErr := problem5ExpectedCandidateInputs(snapshots, manifest, successorRecordedID, matcherValue, expectedRelevantSources, expectedTargetMembers, now)
	if expectedFinalErr != nil {
		t.Fatal(expectedFinalErr)
	}
	if err = assertProblem5IndependentFinalSourceJob(db, sourceJobID, activeSources, controlledRules, expectedFinalCandidates, snapshots, now); err != nil {
		t.Fatal(err)
	}
	if err = assertProblem5IndependentFullAlertModel(db, snapshots, resolvedID, matcherValue, now); err != nil {
		t.Fatal(err)
	}
	if err = assertProblem5ReplayCapability(capability, db, path, sourceJobID, successorJobID, eligibilityJobIDs, expectedSuccessorSnapshots, manifest, inputBeforeRestart, inputAfterRestart, sources, controlledRules, expectedRelevantSources, expectedTargetMembers, statsRecorder.Body.Bytes(), warningRecorder.Body.Bytes(), snapshots, resolvedID, matcherValue, retryJobID, abandonJobID); err != nil {
		t.Fatalf("capability %s: %v", capability, err)
	}
}

func assertProblem5ReplayCapability(capability problem5ReplayCapability, db *sql.DB, sqlitePath string, sourceJobID, successorJobID int, eligibilityJobIDs []int, expectedSuccessors map[string]alertlifecycle.LiveAlertSnapshot, manifest []problem5ReplayScope, inputBefore, inputAfter string, sources []alertlifecycle.SourceSilenceState, rules []problem5ControlledRule, expectedRelevant, expectedMembers map[string]string, statsBody, warningBody []byte, snapshots []alertlifecycle.LiveAlertSnapshot, resolvedID, matcherValue string, retryJobID, abandonJobID int) error {
	switch capability {
	case problem5CapabilityAll:
		if len(manifest) == 0 || sourceJobID == successorJobID {
			return errors.New("recorded replay capability did not reach the source/successor branch")
		}
		if err := assertProblem5ExactSuccessorMembership(db, sourceJobID, eligibilityJobIDs, manifest[:len(manifest)-1], len(manifest), expectedSuccessors); err != nil {
			return err
		}
		if inputBefore != inputAfter {
			return errors.New("immutable input changed across restart")
		}
		if err := assertProblem5CompleteImmutableInput(db, sourceJobID, sources, rules, expectedRelevant, expectedMembers); err != nil {
			return err
		}
		if err := assertProblem5IndependentFinalModel(db, snapshots, resolvedID, matcherValue, sourceJobID, successorJobID, retryJobID, abandonJobID); err != nil {
			return err
		}
		if err := assertProblem5CompleteFinalRows(db, sourceJobID, successorJobID, retryJobID, abandonJobID); err != nil {
			return err
		}
		if err := assertProblem5ExactStatsWarningResponses(db, sqlitePath, statsBody, warningBody); err != nil {
			return err
		}
	case problem5CapabilitySuccessor:
		if len(manifest) == 0 {
			return errors.New("successor capability has no retained manifest scope")
		}
		if err := assertProblem5ExactSuccessorMembership(db, sourceJobID, eligibilityJobIDs, manifest[:len(manifest)-1], len(manifest), expectedSuccessors); err != nil {
			return err
		}
	case problem5CapabilityInput:
		if inputBefore != inputAfter {
			return errors.New("immutable input changed across restart")
		}
		if err := assertProblem5CompleteImmutableInput(db, sourceJobID, sources, rules, expectedRelevant, expectedMembers); err != nil {
			return err
		}
	case problem5CapabilityFinal:
		if err := assertProblem5IndependentFinalModel(db, snapshots, resolvedID, matcherValue, sourceJobID, successorJobID, retryJobID, abandonJobID); err != nil {
			return err
		}
		if err := assertProblem5CompleteFinalRows(db, sourceJobID, successorJobID, retryJobID, abandonJobID); err != nil {
			return err
		}
	case problem5CapabilityAPI:
		if err := assertProblem5ExactStatsWarningResponses(db, sqlitePath, statsBody, warningBody); err != nil {
			return err
		}
	case problem5CapabilityReplay:
		if len(manifest) == 0 || sourceJobID == successorJobID {
			return errors.New("recorded replay capability did not reach the source/successor branch")
		}
	default:
		return fmt.Errorf("unknown Problem 5 capability %q", capability)
	}
	return nil
}

func assertProblem5ExactSuccessorMembership(db *sql.DB, sourceJobID int, successorJobIDs []int, manifest []problem5ReplayScope, recordedManifestCount int, expected map[string]alertlifecycle.LiveAlertSnapshot) error {
	var sourceCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=?`, sourceJobID).Scan(&sourceCount); err != nil {
		return err
	}
	if sourceCount != recordedManifestCount+1 {
		return fmt.Errorf("source membership=%d want=%d recorded scopes plus failed-source context", sourceCount, recordedManifestCount+1)
	}
	for alertID := range expected {
		var source, successor int
		if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=? AND alert_id=?`, sourceJobID, alertID).Scan(&source); err != nil {
			return err
		}
		for _, successorJobID := range successorJobIDs {
			var count int
			if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=? AND alert_id=?`, successorJobID, alertID).Scan(&count); err != nil {
				return err
			}
			successor += count
		}
		wantSource := 0
		for _, scope := range manifest {
			if scope.AlertID == alertID {
				wantSource = 1
				break
			}
		}
		if source != wantSource || successor != 1 {
			return fmt.Errorf("successor membership alert=%s source=%d/%d successor=%d", alertID, source, wantSource, successor)
		}
	}
	var sourceTrigger string
	if err := db.QueryRow(`SELECT trigger_type FROM silence_recheck_jobs WHERE job_id=?`, sourceJobID).Scan(&sourceTrigger); err != nil {
		return err
	}
	if sourceTrigger != "source_context_changed" {
		return fmt.Errorf("trigger provenance source=%q", sourceTrigger)
	}
	for _, successorJobID := range successorJobIDs {
		var successorTrigger string
		if err := db.QueryRow(`SELECT trigger_type FROM silence_recheck_jobs WHERE job_id=?`, successorJobID).Scan(&successorTrigger); err != nil {
			return err
		}
		if successorTrigger != "alert_became_eligible" {
			return fmt.Errorf("trigger provenance successor_job=%d trigger=%q", successorJobID, successorTrigger)
		}
	}
	return nil
}

func assertProblem5CompleteImmutableInput(db *sql.DB, jobID int, sources []alertlifecycle.SourceSilenceState, rules []problem5ControlledRule, expectedRelevant, expectedMembers map[string]string) error {
	if err := assertProblem5ControlledInput(db, jobID, sources, rules, expectedRelevant, expectedMembers); err != nil {
		return err
	}
	ruleRows, err := captureReplayRows(db, `SELECT * FROM silence_recheck_job_rules WHERE job_id=? ORDER BY source_name,silence_id`, jobID)
	if err != nil {
		return err
	}
	candidateRows, err := captureReplayRows(db, `SELECT * FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`, jobID)
	if err != nil {
		return err
	}
	if ruleRows == "" || candidateRows == "" {
		return errors.New("complete immutable input has an empty rule or candidate row set")
	}
	var jobFingerprint, sourceContext, trigger string
	if err = db.QueryRow(`SELECT job_fingerprint,source_context_json,trigger_type FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&jobFingerprint, &sourceContext, &trigger); err != nil {
		return err
	}
	if !strings.HasPrefix(jobFingerprint, "sha256:") || !strings.HasPrefix(sourceContext, "{") || trigger != "source_context_changed" {
		return fmt.Errorf("immutable job header fingerprint=%q context=%q trigger=%q", jobFingerprint, sourceContext, trigger)
	}
	return nil
}

func assertProblem5CompleteFinalRows(db *sql.DB, sourceJobID, successorJobID, retryJobID, abandonJobID int) error {
	for _, table := range []string{"alert_states", "alert_target_states", "alert_events", "silence_recheck_jobs", "silence_recheck_job_candidates", "silence_recheck_job_attempts", "silence_recheck_job_operator_actions", "alert_suppression_cause_history", "alert_suppression_cause_members"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("final model table %s is empty", table)
		}
	}
	for _, jobID := range []int{sourceJobID, successorJobID, retryJobID, abandonJobID} {
		rows, err := captureReplayRows(db, `SELECT * FROM silence_recheck_jobs WHERE job_id=?`, jobID)
		if err != nil {
			return err
		}
		if rows == "" {
			return fmt.Errorf("final model missing complete job row %d", jobID)
		}
		var candidateCount, attemptCount int
		if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=?`, jobID).Scan(&candidateCount); err != nil {
			return err
		}
		if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=?`, jobID).Scan(&attemptCount); err != nil {
			return err
		}
		if candidateCount == 0 || attemptCount == 0 {
			return fmt.Errorf("final model job %d candidate_rows=%d attempt_rows=%d", jobID, candidateCount, attemptCount)
		}
	}
	return nil
}

func assertProblem5ExactStatsWarningResponses(db *sql.DB, sqlitePath string, statsBody, warningBody []byte) error {
	stats, err := problem5IndependentStatsFromDB(db, sqlitePath)
	if err != nil {
		return fmt.Errorf("independent stats model: %w", err)
	}
	var actualStats, actualWarning map[string]any
	if err := json.Unmarshal(statsBody, &actualStats); err != nil {
		return fmt.Errorf("stats JSON: %w", err)
	}
	if err := json.Unmarshal(warningBody, &actualWarning); err != nil {
		return fmt.Errorf("warning JSON: %w", err)
	}
	expectedFingerprint := alertlifecycle.BuildGroupingFingerprint(alertlifecycle.GroupingConfig{
		LabelsKeep: append([]string{}, config.Config.Labels.Keep...), LabelsKeepRegex: append([]string{}, config.Config.Labels.KeepRegex...),
		LabelsStrip: append([]string{}, config.Config.Labels.Strip...), LabelsStripRegex: append([]string{}, config.Config.Labels.StripRegex...),
	})
	expectedWarning := problem5ExpectedWarningResponse(stats, expectedFingerprint)
	expectedStats := problem5ExpectedStatsResponse(stats, expectedFingerprint, expectedWarning)
	if !reflect.DeepEqual(actualStats, expectedStats) {
		return fmt.Errorf("stats response differs from independent durable model: actual=%v expected=%v", actualStats, expectedStats)
	}
	if !reflect.DeepEqual(actualWarning, expectedWarning) {
		return fmt.Errorf("warning response differs from independent durable model: actual=%v expected=%v", actualWarning, expectedWarning)
	}
	return assertProblem5StatsWarningResponses(statsBody, warningBody)
}

func problem5IndependentStatsFromDB(db *sql.DB, sqlitePath string) (alertlifecycle.Stats, error) {
	var stats alertlifecycle.Stats
	for query, destination := range map[string]any{
		`SELECT COUNT(*) FROM alert_states`: &stats.CurrentRows,
		`SELECT COUNT(*) FROM alert_events`: &stats.HistoryRows,
		`PRAGMA page_count`:                 &stats.PageCount,
		`PRAGMA page_size`:                  &stats.PageSizeBytes,
		`PRAGMA freelist_count`:             &stats.FreelistCount,
	} {
		if err := db.QueryRow(query).Scan(destination); err != nil {
			return alertlifecycle.Stats{}, err
		}
	}
	stats.StateCounts = map[string]int{}
	rows, err := db.Query(`SELECT state,COUNT(*) FROM alert_states GROUP BY state`)
	if err != nil {
		return alertlifecycle.Stats{}, err
	}
	for rows.Next() {
		var state string
		var count int
		if err = rows.Scan(&state, &count); err != nil {
			rows.Close()
			return alertlifecycle.Stats{}, err
		}
		if strings.TrimSpace(state) == "" {
			state = "unknown"
		}
		stats.StateCounts[state] = count
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return alertlifecycle.Stats{}, err
	}
	rows.Close()
	rows, err = db.Query(`SELECT is_live,COUNT(*) FROM alert_states WHERE state=? GROUP BY is_live`, string(alertlifecycle.StateSuppressed))
	if err != nil {
		return alertlifecycle.Stats{}, err
	}
	for rows.Next() {
		var live, count int
		if err = rows.Scan(&live, &count); err != nil {
			rows.Close()
			return alertlifecycle.Stats{}, err
		}
		if live != 0 {
			stats.SuppressedBreakdownCurrent += count
		} else {
			stats.SuppressedBreakdownPast += count
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return alertlifecycle.Stats{}, err
	}
	rows.Close()
	metadata := func(key string) (string, error) {
		var value string
		metadataErr := db.QueryRow(`SELECT value FROM metadata WHERE key=?`, key).Scan(&value)
		if errors.Is(metadataErr, sql.ErrNoRows) {
			return "", nil
		}
		return value, metadataErr
	}
	parseMetadataTime := func(raw, key string) (*time.Time, error) {
		if strings.TrimSpace(raw) == "" {
			return nil, nil
		}
		value, parseErr := time.Parse(time.RFC3339Nano, raw)
		if parseErr != nil {
			return nil, fmt.Errorf("metadata %s: %w", key, parseErr)
		}
		value = value.UTC()
		return &value, nil
	}
	var metadataErr error
	if stats.GroupingFingerprint, metadataErr = metadata("grouping_fingerprint"); metadataErr != nil {
		return alertlifecycle.Stats{}, metadataErr
	}
	if stats.RegroupTargetFingerprint, metadataErr = metadata("regroup_target_fingerprint"); metadataErr != nil {
		return alertlifecycle.Stats{}, metadataErr
	}
	if stats.RegroupStatus, metadataErr = metadata("regroup_status"); metadataErr != nil {
		return alertlifecycle.Stats{}, metadataErr
	}
	if stats.RegroupError, metadataErr = metadata("regroup_error"); metadataErr != nil {
		return alertlifecycle.Stats{}, metadataErr
	}
	var processed, total string
	if processed, metadataErr = metadata("regroup_processed"); metadataErr != nil {
		return alertlifecycle.Stats{}, metadataErr
	}
	if total, metadataErr = metadata("regroup_total"); metadataErr != nil {
		return alertlifecycle.Stats{}, metadataErr
	}
	if stats.RegroupProcessed, metadataErr = strconv.Atoi(processed); metadataErr != nil && strings.TrimSpace(processed) != "" {
		return alertlifecycle.Stats{}, metadataErr
	}
	if stats.RegroupTotal, metadataErr = strconv.Atoi(total); metadataErr != nil && strings.TrimSpace(total) != "" {
		return alertlifecycle.Stats{}, metadataErr
	}
	for key, destination := range map[string]**time.Time{
		"regroup_started_at":         &stats.RegroupStartedAt,
		"regroup_finished_at":        &stats.RegroupFinishedAt,
		"last_housekeeping_at":       &stats.LastHousekeepingAt,
		"last_incremental_vacuum_at": &stats.LastIncrementalVacuumAt,
	} {
		value, readErr := metadata(key)
		if readErr != nil {
			return alertlifecycle.Stats{}, readErr
		}
		parsed, parseErr := parseMetadataTime(value, key)
		if parseErr != nil {
			return alertlifecycle.Stats{}, parseErr
		}
		*destination = parsed
	}
	clusterRows, err := db.Query(`SELECT cluster_id FROM alertmanager_cluster_consistency WHERE status='inconsistent' ORDER BY cluster_id`)
	if err != nil {
		return alertlifecycle.Stats{}, err
	}
	for clusterRows.Next() {
		var cluster string
		if err = clusterRows.Scan(&cluster); err != nil {
			clusterRows.Close()
			return alertlifecycle.Stats{}, err
		}
		stats.SilenceInconsistentClusters = append(stats.SilenceInconsistentClusters, cluster)
	}
	if err = clusterRows.Err(); err != nil {
		clusterRows.Close()
		return alertlifecycle.Stats{}, err
	}
	clusterRows.Close()
	if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs WHERE status IN ('queued','running','retry_wait','failed')`).Scan(&stats.Recheck.QueueDepth); err != nil {
		return alertlifecycle.Stats{}, err
	}
	if stats.Recheck.QueueDepth != 0 {
		return alertlifecycle.Stats{}, fmt.Errorf("independent final stats model has active recheck queue depth=%d", stats.Recheck.QueueDepth)
	}
	fileInfo, statErr := os.Stat(sqlitePath)
	if statErr == nil {
		stats.FileSizeBytes = fileInfo.Size()
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return alertlifecycle.Stats{}, statErr
	}
	return stats, nil
}

func problem5ExpectedWarningResponse(stats alertlifecycle.Stats, expectedFingerprint string) map[string]any {
	issues := make([]any, 0, 6)
	appendIssue := func(kind, severity, summary string, metadata map[string]any) {
		issues = append(issues, map[string]any{"kind": kind, "severity": severity, "summary": summary, "metadata": metadata})
	}
	if stats.GroupingFingerprint != expectedFingerprint {
		appendIssue("regroup_out_of_date", "warning", "Alert lifecycle grouping is out of date", map[string]any{"storedFingerprint": stats.GroupingFingerprint, "expectedFingerprint": expectedFingerprint})
	}
	switch stats.RegroupStatus {
	case "running":
		appendIssue("regroup_in_progress", "warning", "Alert lifecycle regroup is in progress", map[string]any{"processed": float64(stats.RegroupProcessed), "total": float64(stats.RegroupTotal)})
	case "error":
		appendIssue("regroup_failed", "error", "Alert lifecycle regroup failed", map[string]any{"error": "regroup operation failed", "processed": float64(stats.RegroupProcessed), "total": float64(stats.RegroupTotal)})
	}
	if len(stats.SilenceInconsistentClusters) > 0 {
		targets := append([]string{}, stats.SilenceInconsistentClusters...)
		sort.Strings(targets)
		values := make([]any, len(targets))
		for i, target := range targets {
			values[i] = target
		}
		appendIssue("silence_ha_inconsistent", "warning", "Alertmanager silence state is inconsistent across HA members", map[string]any{"targetNames": values})
	}
	if head := stats.Recheck.HeadJob; head != nil {
		switch head.Status {
		case "retry_wait":
			if head.NextAttemptAt != nil && head.LastError != nil && strings.TrimSpace(head.LastError.Stage) != "" {
				appendIssue("recheck_retrying", "warning", "Silence recheck is waiting to retry", map[string]any{
					"jobId": float64(head.JobID), "status": head.Status, "retryCount": float64(head.RetryCount),
					"nextAttemptAt": head.NextAttemptAt.UTC().Format(time.RFC3339Nano), "lastFailureStage": head.LastError.Stage,
				})
			}
		case "failed":
			if head.FirstError != nil && strings.TrimSpace(head.FirstError.Stage) != "" && !head.FirstError.At.IsZero() {
				appendIssue("recheck_blocked", "error", "Silence recheck queue is blocked", map[string]any{
					"jobId": float64(head.JobID), "status": head.Status, "firstFailureStage": head.FirstError.Stage,
					"firstFailureAt": head.FirstError.At.UTC().Format(time.RFC3339Nano),
				})
			}
		}
	}
	sort.SliceStable(issues, func(i, j int) bool {
		left, right := issues[i].(map[string]any), issues[j].(map[string]any)
		if left["severity"] != right["severity"] {
			return left["severity"] == "error"
		}
		return left["kind"].(string) < right["kind"].(string)
	})
	return map[string]any{
		"status": "ok", "storageEnabled": true, "dbExpectedState": len(issues) == 0, "issues": issues,
	}
}

func problem5ExpectedStatsResponse(stats alertlifecycle.Stats, expectedFingerprint string, warning map[string]any) map[string]any {
	stateCounts := make(map[string]any, len(stats.StateCounts))
	for state, count := range stats.StateCounts {
		stateCounts[state] = float64(count)
	}
	suppressedBreakdown := map[string]any{"current": float64(stats.SuppressedBreakdownCurrent), "past": float64(stats.SuppressedBreakdownPast)}
	clusters := make([]any, len(stats.SilenceInconsistentClusters))
	for i, cluster := range stats.SilenceInconsistentClusters {
		clusters[i] = cluster
	}
	regroupStatus := stats.RegroupStatus
	if regroupStatus == "" {
		regroupStatus = "ready"
	}
	regroupTargetFingerprint := stats.RegroupTargetFingerprint
	if regroupTargetFingerprint == "" {
		regroupTargetFingerprint = expectedFingerprint
	}
	issues := warning["issues"].([]any)
	dbExpectedReason := ""
	if len(issues) > 0 {
		dbExpectedReason = issues[0].(map[string]any)["summary"].(string)
	}
	response := map[string]any{
		"status": "ok", "storageEnabled": true, "sqlitePath": config.Config.AlertLifecycle.SQLite.Path,
		"retentionTTLHours": config.Config.AlertLifecycle.Retention.TTL.Hours(), "maxAlerts": float64(config.Config.AlertLifecycle.Retention.MaxAlerts),
		"currentRows": float64(stats.CurrentRows), "stateCounts": stateCounts, "suppressedBreakdown": suppressedBreakdown,
		"historyRows": float64(stats.HistoryRows), "groupingFingerprint": stats.GroupingFingerprint,
		"regroupTargetFingerprint": regroupTargetFingerprint, "regroupStatus": regroupStatus,
		"regroupError": safeRegroupError(stats.RegroupError), "regroupProcessed": float64(stats.RegroupProcessed), "regroupTotal": float64(stats.RegroupTotal),
		"dbExpectedState": len(issues) == 0, "dbExpectedStateReason": dbExpectedReason,
		"fileSizeBytes": float64(stats.FileSizeBytes), "pageCount": float64(stats.PageCount), "pageSizeBytes": float64(stats.PageSizeBytes), "freelistCount": float64(stats.FreelistCount),
		"silenceHistoryTTLHours": config.Config.AlertLifecycle.SilenceHistory.Retention.TTL.Hours(), "silenceInconsistentClusters": clusters,
		"recheck": map[string]any{"queueDepth": float64(stats.Recheck.QueueDepth), "headJob": problem5ExpectedHeadJob(stats.Recheck.HeadJob)},
	}
	for _, field := range []struct {
		name  string
		value *time.Time
	}{{"lastRegroupStartedAt", stats.RegroupStartedAt}, {"lastRegroupFinishedAt", stats.RegroupFinishedAt}, {"lastHousekeepingAt", stats.LastHousekeepingAt}, {"lastIncrementalVacuumAt", stats.LastIncrementalVacuumAt}} {
		if field.value == nil {
			response[field.name] = nil
		} else {
			response[field.name] = field.value.UTC().Format(time.RFC3339Nano)
		}
	}
	return response
}

func problem5ExpectedHeadJob(job *alertlifecycle.RecheckHeadJob) any {
	if job == nil {
		return nil
	}
	response := map[string]any{
		"jobId": float64(job.JobID), "triggerType": job.TriggerType, "status": job.Status,
		"createdAt": job.CreatedAt.UTC().Format(time.RFC3339Nano), "startedAt": nil, "finishedAt": nil,
		"totalCandidates": float64(job.TotalCandidates), "processedCandidates": float64(job.ProcessedCandidates),
		"outcomeCounts": map[string]any{"promoted": float64(job.OutcomeCounts.Promoted), "stale": float64(job.OutcomeCounts.Stale), "ineligible": float64(job.OutcomeCounts.Ineligible), "missingLabels": float64(job.OutcomeCounts.MissingLabels), "incompleteSources": float64(job.OutcomeCounts.IncompleteSources)},
		"attemptCount":  float64(job.AttemptCount), "retryCount": float64(job.RetryCount), "consecutiveRetryCount": float64(job.ConsecutiveRetryCount), "nextAttemptAt": nil,
		"firstError": nil, "lastError": nil, "recoveryCount": float64(job.RecoveryCount),
	}
	if job.StartedAt != nil {
		response["startedAt"] = job.StartedAt.UTC().Format(time.RFC3339Nano)
	}
	if job.FinishedAt != nil {
		response["finishedAt"] = job.FinishedAt.UTC().Format(time.RFC3339Nano)
	}
	if job.NextAttemptAt != nil {
		response["nextAttemptAt"] = job.NextAttemptAt.UTC().Format(time.RFC3339Nano)
	}
	for _, field := range []struct {
		name  string
		value *alertlifecycle.RecheckErrorSnapshot
	}{{"firstError", job.FirstError}, {"lastError", job.LastError}} {
		if field.value != nil {
			response[field.name] = map[string]any{"class": field.value.Class, "stage": field.value.Stage, "at": field.value.At.UTC().Format(time.RFC3339Nano), "message": "recheck attempt failed"}
		}
	}
	return response
}

func replaySourcesWithSilence(sources []alertlifecycle.SourceSilenceState, silence alertlifecycle.SourceSilenceSnapshot) []alertlifecycle.SourceSilenceState {
	result := append([]alertlifecycle.SourceSilenceState(nil), sources...)
	for i := range result {
		result[i].Silences = append(append([]alertlifecycle.SourceSilenceSnapshot(nil), result[i].Silences...), silence)
	}
	return result
}

func replaySourcesWithStatus(sources []alertlifecycle.SourceSilenceState, status string) []alertlifecycle.SourceSilenceState {
	result := append([]alertlifecycle.SourceSilenceState(nil), sources...)
	for i := range result {
		result[i].SyncStatus = status
		result[i].LastError = "recorded source failure"
	}
	return result
}

func seedReplayOperatorJob(t *testing.T, store *alertlifecycle.Store, db *sql.DB, sources []alertlifecycle.SourceSilenceState, successful map[string]bool, now time.Time, alertID string) (int, error) {
	t.Helper()
	if len(sources) == 0 {
		return 0, errors.New("operator fixture has no sources")
	}
	labels := `{"instance":"problem5-operator"}`
	lastSeen := now.Add(-time.Hour)
	relevantSources := fmt.Sprintf(`[{"name":%q,"cluster":%q}]`, sources[0].SourceName, sources[0].ClusterID)
	if err := store.UpsertCurrentAlert(alertlifecycle.AlertCurrent{
		AlertID: alertID, State: alertlifecycle.StateInactive, RawState: alertlifecycle.StateActive,
		Receiver: "problem5", GroupID: "problem5", GroupLabelsJSON: `{}`, LabelsJSON: labels,
		MatchLabelsJSON: &labels, AnnotationsJSON: `{}`, StartsAt: now.Add(-2 * time.Hour), LastSeenAt: &lastSeen,
		RelevantUpstreams: fmt.Sprintf(`["%s"]`, sources[0].SourceName), RelevantSources: relevantSources, UpdatedAt: now,
	}); err != nil {
		return 0, err
	}
	operatorSuccessful := map[string]bool{}
	for _, source := range sources {
		operatorSuccessful[source.SourceName] = source.SyncStatus == "success"
	}
	for source, ok := range successful {
		if _, found := operatorSuccessful[source]; !found {
			operatorSuccessful[source] = ok
		}
	}
	creation, err := store.ApplyPollCycle(alertlifecycle.PollCycleInput{SuccessfulUpstreams: operatorSuccessful, SilenceSources: sources, Timestamp: now})
	if err != nil {
		return 0, err
	}
	if creation.RecheckJobsQueued != 1 {
		return 0, fmt.Errorf("operator fixture creation=%+v", creation)
	}
	var jobID int
	if err = db.QueryRow(`SELECT job_id FROM silence_recheck_jobs ORDER BY job_id DESC LIMIT 1`).Scan(&jobID); err != nil {
		return 0, err
	}
	return jobID, nil
}

func replaySnapshotByID(snapshots []alertlifecycle.LiveAlertSnapshot, alertID string) *alertlifecycle.LiveAlertSnapshot {
	for i := range snapshots {
		if snapshots[i].AlertID == alertID {
			snapshotCopy := snapshots[i]
			return &snapshotCopy
		}
	}
	return nil
}

type problem5ReplayScope struct {
	AlertID         string
	TargetKey       string
	MatchLabelsJSON string
	Instance        string
	State           alertlifecycle.State
}

func buildProblem5ReplayManifest(snapshots []alertlifecycle.LiveAlertSnapshot) []problem5ReplayScope {
	manifest := make([]problem5ReplayScope, 0, len(snapshots))
	for _, snapshot := range snapshots {
		matchLabels := snapshot.LabelsJSON
		if snapshot.MatchLabelsJSON != nil {
			matchLabels = *snapshot.MatchLabelsJSON
		}
		var labels map[string]string
		_ = json.Unmarshal([]byte(snapshot.LabelsJSON), &labels)
		if len(snapshot.Targets) == 0 {
			manifest = append(manifest, problem5ReplayScope{AlertID: snapshot.AlertID, MatchLabelsJSON: matchLabels, Instance: labels["instance"], State: snapshot.State})
			continue
		}
		for _, target := range snapshot.Targets {
			manifest = append(manifest, problem5ReplayScope{AlertID: snapshot.AlertID, TargetKey: target.Target, MatchLabelsJSON: matchLabels, Instance: labels["instance"], State: snapshot.State})
		}
	}
	sort.Slice(manifest, func(i, j int) bool {
		if manifest[i].AlertID == manifest[j].AlertID {
			return manifest[i].TargetKey < manifest[j].TargetKey
		}
		return manifest[i].AlertID < manifest[j].AlertID
	})
	return manifest
}

func replayInstanceMatcherValue(snapshots []alertlifecycle.LiveAlertSnapshot) string {
	for _, snapshot := range snapshots {
		var labels map[string]string
		if json.Unmarshal([]byte(snapshot.LabelsJSON), &labels) == nil && labels["instance"] != "" {
			return labels["instance"]
		}
	}
	return "problem5-missing-instance"
}

func replayScopeInstance(scope problem5ReplayScope) string {
	return scope.Instance
}

func assertProblem5ReplayManifest(db *sql.DB, jobID int, manifest []problem5ReplayScope) error {
	for _, scope := range manifest {
		var count int
		var matchLabels string
		if err := db.QueryRow(`SELECT COUNT(*),COALESCE(MAX(match_labels_json),'') FROM silence_recheck_job_candidates WHERE job_id=? AND alert_id=? AND target_key=?`, jobID, scope.AlertID, scope.TargetKey).Scan(&count, &matchLabels); err != nil {
			return err
		}
		if count != 1 {
			var available string
			var stateCount int
			var state string
			var live, suppressed, resolved int
			_ = db.QueryRow(`SELECT COUNT(*) FROM alert_states WHERE alert_id=?`, scope.AlertID).Scan(&stateCount)
			_ = db.QueryRow(`SELECT state,is_live,is_suppressed,is_resolved FROM alert_states WHERE alert_id=?`, scope.AlertID).Scan(&state, &live, &suppressed, &resolved)
			_ = db.QueryRow(`SELECT COALESCE(GROUP_CONCAT(target_key,','),'') FROM silence_recheck_job_candidates WHERE job_id=? AND alert_id=?`, jobID, scope.AlertID).Scan(&available)
			return fmt.Errorf("job %d manifest scope=%s/%s count=%d state_rows=%d state=%s live=%d suppressed=%d resolved=%d available=%s", jobID, scope.AlertID, scope.TargetKey, count, stateCount, state, live, suppressed, resolved, available)
		}
		if matchLabels != scope.MatchLabelsJSON {
			return fmt.Errorf("job %d manifest scope=%s/%s match_labels=%q want %q", jobID, scope.AlertID, scope.TargetKey, matchLabels, scope.MatchLabelsJSON)
		}
	}
	return nil
}

func replayEligibilityJobIDs(db *sql.DB) ([]int, error) {
	rows, err := db.Query(`SELECT job_id FROM silence_recheck_jobs WHERE trigger_type='alert_became_eligible' ORDER BY job_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobIDs []int
	for rows.Next() {
		var jobID int
		if err = rows.Scan(&jobID); err != nil {
			return nil, err
		}
		jobIDs = append(jobIDs, jobID)
	}
	return jobIDs, rows.Err()
}

func assertReplayEligibilitySuccessors(db *sql.DB, jobIDs []int, expected map[string]alertlifecycle.LiveAlertSnapshot, expectedTimes map[string]time.Time) error {
	if len(jobIDs) != len(expected) {
		return fmt.Errorf("eligibility successor jobs=%d want=%d", len(jobIDs), len(expected))
	}
	seen := make(map[string]bool, len(expected))
	for _, jobID := range jobIDs {
		rows, err := db.Query(`
SELECT alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,
       match_labels_json,relevant_sources_json,target_source_members_json,processed_at,outcome
FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`, jobID)
		if err != nil {
			return err
		}
		var alertID string
		if !rows.Next() {
			rows.Close()
			return fmt.Errorf("eligibility job %d has no candidate rows", jobID)
		}
		var targetKey string
		var expectedAlert, expectedTarget, labels, relevant, members, processed, outcome sql.NullString
		if err = rows.Scan(&alertID, &targetKey, &expectedAlert, &expectedTarget, &labels, &relevant, &members, &processed, &outcome); err != nil {
			rows.Close()
			return err
		}
		snapshot, ok := expected[alertID]
		if !ok || seen[alertID] {
			rows.Close()
			return fmt.Errorf("eligibility job %d has unexpected or duplicate alert %q", jobID, alertID)
		}
		seen[alertID] = true
		matchLabels := snapshot.LabelsJSON
		if snapshot.MatchLabelsJSON != nil {
			matchLabels = *snapshot.MatchLabelsJSON
		}
		relevantSources, marshalErr := json.Marshal(snapshot.RelevantSources)
		if marshalErr != nil {
			rows.Close()
			return marshalErr
		}
		expectedRows := 1
		if len(snapshot.Targets) > 0 {
			expectedRows = len(snapshot.Targets)
		}
		expectedTime, hasExpectedTime := expectedTimes[alertID]
		if !hasExpectedTime {
			rows.Close()
			return fmt.Errorf("eligibility job %d has no controlled timestamp for alert %q", jobID, alertID)
		}
		if targetKey != "" {
			var target *alertlifecycle.LiveTargetSnapshot
			for index := range snapshot.Targets {
				if snapshot.Targets[index].Target == targetKey {
					target = &snapshot.Targets[index]
					break
				}
			}
			if target == nil {
				rows.Close()
				return fmt.Errorf("eligibility job %d unexpected target %q for alert %q", jobID, targetKey, alertID)
			}
			membersJSON, membersErr := json.Marshal(target.SourceNames)
			if membersErr != nil {
				rows.Close()
				return membersErr
			}
			if !members.Valid || members.String != string(membersJSON) {
				rows.Close()
				return fmt.Errorf("eligibility job %d target members=%q want %q", jobID, members.String, membersJSON)
			}
			if !expectedTarget.Valid || expectedTarget.String != expectedTime.UTC().Format(time.RFC3339Nano) {
				rows.Close()
				return fmt.Errorf("eligibility job %d target observation=%q want %q", jobID, expectedTarget.String, expectedTime.UTC().Format(time.RFC3339Nano))
			}
		} else if members.Valid {
			rows.Close()
			return fmt.Errorf("eligibility job %d global candidate has target members %q", jobID, members.String)
		}
		if expectedRows == 0 || !expectedAlert.Valid || expectedAlert.String != snapshot.LastSeenAt.UTC().Format(time.RFC3339Nano) || labels.String != matchLabels || relevant.String != string(relevantSources) || processed.Valid || outcome.Valid {
			rows.Close()
			return fmt.Errorf("eligibility job %d candidate=%q/%q fields alert=%q target=%q labels=%q relevant=%q processed=%v outcome=%v", jobID, alertID, targetKey, expectedAlert.String, expectedTarget.String, labels.String, relevant.String, processed.Valid, outcome.Valid)
		}
		rowCount := 1
		for rows.Next() {
			rowCount++
			var extra [9]any
			if err = rows.Scan(&extra[0], &extra[1], &extra[2], &extra[3], &extra[4], &extra[5], &extra[6], &extra[7], &extra[8]); err != nil {
				rows.Close()
				return err
			}
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if rowCount != expectedRows {
			return fmt.Errorf("eligibility job %d rows=%d want=%d", jobID, rowCount, expectedRows)
		}
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("eligibility successor coverage=%v want=%v", seen, expected)
	}
	return nil
}

func assertProblem5SyntheticAlertRow(alertID, state, rawState string, live, suppressed, resolved int, receiver, groupID, groupLabels, labels string, matchLabels sql.NullString, annotations, startsAt string, lastSeen sql.NullString, _, _, sourceLinks string, suppressionObserved, resolvedAt, resolvedBy, resolvedComment sql.NullString, updatedAt string, now time.Time) error {
	if state != "inactive" || rawState != "active" || live != 0 || suppressed != 0 || resolved != 0 || !matchLabels.Valid || annotations != "{}" || sourceLinks != "[]" || suppressionObserved.Valid || resolvedAt.Valid || resolvedBy.Valid || resolvedComment.Valid || strings.TrimSpace(updatedAt) == "" {
		return fmt.Errorf("synthetic alert %q has wrong lifecycle fields state=%q raw=%q live=%d suppressed=%d resolved=%d", alertID, state, rawState, live, suppressed, resolved)
	}
	var wantLastSeen time.Time
	switch alertID {
	case "problem5-failed-context":
		if receiver != "problem5" || groupID != "problem5-failed" || groupLabels != `{}` || labels != `{"instance":"problem5-failed-context"}` || matchLabels.String != labels {
			return fmt.Errorf("synthetic failed alert %q has wrong identity", alertID)
		}
		wantLastSeen = now.Add(-time.Minute)
	case "problem5-recorded-retry-alert", "problem5-recorded-abandon-alert":
		if receiver != "problem5" || groupID != "problem5" || groupLabels != `{}` || labels != `{"instance":"problem5-operator"}` || matchLabels.String != labels {
			return fmt.Errorf("synthetic operator alert %q has wrong identity", alertID)
		}
		operatorNow := now.Add(9 * time.Minute)
		if alertID == "problem5-recorded-abandon-alert" {
			operatorNow = now.Add(11 * time.Minute)
		}
		if startsAt != operatorNow.Add(-2*time.Hour).UTC().Format(time.RFC3339Nano) {
			return fmt.Errorf("synthetic alert %q has wrong start timestamp %q", alertID, startsAt)
		}
		wantLastSeen = operatorNow.Add(-time.Hour)
	default:
		return fmt.Errorf("final alert row %q is not in the controlled model", alertID)
	}
	if alertID == "problem5-failed-context" && startsAt != now.Add(-2*time.Hour).UTC().Format(time.RFC3339Nano) {
		return fmt.Errorf("synthetic alert %q has wrong start timestamp %q", alertID, startsAt)
	}
	if !lastSeen.Valid || lastSeen.String != wantLastSeen.UTC().Format(time.RFC3339Nano) {
		return fmt.Errorf("synthetic alert %q has wrong controlled timestamps starts=%q last=%q", alertID, startsAt, lastSeen.String)
	}
	return nil
}

func assertReplayPinnedSourceFailure(db *sql.DB, jobID int, sourceName string) error {
	var raw string
	if err := db.QueryRow(`SELECT source_context_json FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&raw); err != nil {
		return err
	}
	var payload struct {
		Sources []struct {
			SourceName string `json:"sourceName"`
			SyncStatus string `json:"syncStatus"`
			LastError  string `json:"lastError"`
		} `json:"sources"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return err
	}
	for _, source := range payload.Sources {
		if source.SourceName == sourceName {
			if source.SyncStatus != "failed" {
				return fmt.Errorf("job %d source=%q status=%q last_error=%q", jobID, sourceName, source.SyncStatus, source.LastError)
			}
			return nil
		}
	}
	return fmt.Errorf("job %d has no pinned source %q", jobID, sourceName)
}

func replayCandidateIDs(db *sql.DB, jobID, limit int) ([]string, error) {
	rows, err := db.Query(`SELECT DISTINCT alert_id FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id LIMIT ?`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func captureReplayRows(db *sql.DB, query string, args ...any) (string, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	records := []string{}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err = rows.Scan(pointers...); err != nil {
			return "", err
		}
		fields := make([]string, len(values))
		for i, value := range values {
			if bytes, ok := value.([]byte); ok {
				fields[i] = string(bytes)
			} else {
				fields[i] = fmt.Sprintf("%v", value)
			}
		}
		records = append(records, strings.Join(fields, "\x1f"))
	}
	return strings.Join(records, "\n"), rows.Err()
}

func captureReplayInputRows(db *sql.DB, jobID int) (string, error) {
	rules, err := captureReplayRows(db, `SELECT * FROM silence_recheck_job_rules WHERE job_id=? ORDER BY rowid`, jobID)
	if err != nil {
		return "", err
	}
	candidates, err := captureReplayRows(db, `SELECT * FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`, jobID)
	if err != nil {
		return "", err
	}
	return rules + "\n--candidates--\n" + candidates, nil
}

func assertProblem5IndependentInputContract(db *sql.DB, jobID, expectedCandidates, expectedSources, expectedRules int) error {
	var total, processed, sourceCount, ruleCount int
	var trigger, status string
	if err := db.QueryRow(`SELECT total_candidates,processed_candidates,trigger_type,status FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&total, &processed, &trigger, &status); err != nil {
		return err
	}
	if total != expectedCandidates || processed != 20 || trigger != "source_context_changed" || status != "running" {
		return fmt.Errorf("job %d independent header total=%d want=%d processed=%d trigger=%q status=%q", jobID, total, expectedCandidates, processed, trigger, status)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_rules WHERE job_id=?`, jobID).Scan(&ruleCount); err != nil {
		return err
	}
	if ruleCount != expectedRules {
		return fmt.Errorf("job %d pinned rules=%d want %d", jobID, ruleCount, expectedRules)
	}
	if err := db.QueryRow(`SELECT json_array_length(source_context_json,'$.sources') FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&sourceCount); err != nil {
		return err
	}
	if sourceCount != expectedSources {
		return fmt.Errorf("job %d pinned sources=%d want %d", jobID, sourceCount, expectedSources)
	}
	return nil
}

type problem5ControlledRule struct {
	SourceName, ClusterID, TargetKey, SilenceID, ObservedState, MatchersJSON  string
	ContentFingerprint, EvaluationFingerprint, StartsAt, EndsAt               string
	CreatedBy, Comment, AnnotationsJSON, SourceUpdatedAt, TicketID, TicketURL string
	RuleID, RevisionID, EpisodeID, RevisionNumber                             int64
}

func problem5SilenceRevisionFingerprints(silence alertlifecycle.SourceSilenceSnapshot) (string, string, string) {
	matchers := append([]alertlifecycle.SilenceMatcherSnapshot(nil), silence.Matchers...)
	sort.Slice(matchers, func(i, j int) bool {
		left, right := matchers[i], matchers[j]
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		if left.Value != right.Value {
			return left.Value < right.Value
		}
		if left.IsRegex != right.IsRegex {
			return !left.IsRegex
		}
		return !left.IsEqual && right.IsEqual
	})
	matchersRaw, _ := json.Marshal(matchers)
	matchersJSON := string(matchersRaw)
	type contentInput struct {
		Matchers    []alertlifecycle.SilenceMatcherSnapshot `json:"matchers"`
		StartsAt    string                                  `json:"startsAt"`
		EndsAt      string                                  `json:"endsAt"`
		CreatedBy   string                                  `json:"createdBy"`
		Comment     string                                  `json:"comment"`
		Annotations string                                  `json:"annotations"`
	}
	contentRaw, _ := json.Marshal(contentInput{Matchers: matchers, StartsAt: silence.StartsAt.UTC().Format(time.RFC3339Nano), EndsAt: silence.EndsAt.UTC().Format(time.RFC3339Nano), CreatedBy: silence.CreatedBy, Comment: silence.Comment, Annotations: silence.AnnotationsJSON})
	evaluationRaw, _ := json.Marshal(struct {
		Matchers []alertlifecycle.SilenceMatcherSnapshot `json:"matchers"`
		StartsAt string                                  `json:"startsAt"`
		EndsAt   string                                  `json:"endsAt"`
	}{Matchers: matchers, StartsAt: silence.StartsAt.UTC().Format(time.RFC3339Nano), EndsAt: silence.EndsAt.UTC().Format(time.RFC3339Nano)})
	contentSum := sha256.Sum256(contentRaw)
	evaluationSum := sha256.Sum256(evaluationRaw)
	return fmt.Sprintf("sha256:%x", contentSum), fmt.Sprintf("sha256:%x", evaluationSum), matchersJSON
}

func captureProblem5ControlledRules(db *sql.DB, activeSources []alertlifecycle.SourceSilenceState) ([]problem5ControlledRule, error) {
	var expected []problem5ControlledRule
	for _, source := range activeSources {
		if source.SyncStatus != "success" {
			continue
		}
		for _, silence := range source.Silences {
			if silence.Status != "active" && silence.Status != "pending" {
				continue
			}
			contentFingerprint, evaluationFingerprint, matchersJSON := problem5SilenceRevisionFingerprints(silence)
			var rule problem5ControlledRule
			var sourceUpdatedAt sql.NullString
			err := db.QueryRow(`
				SELECT r.rule_id, ass.revision_id, rev.episode_id, rev.revision_number, rev.content_fingerprint,
	       rev.evaluation_fingerprint, rev.matchers_json, rev.starts_at, rev.ends_at,
	       rev.source_updated_at, rev.created_by, rev.comment, rev.annotations_json,
       rev.ticket_id, rev.ticket_url, ass.observed_state
FROM silence_rules r
JOIN alertmanager_source_silences ass ON ass.rule_id=r.rule_id
JOIN silence_revisions rev ON rev.revision_id=ass.revision_id AND rev.rule_id=ass.rule_id
WHERE ass.source_name=? AND r.cluster_id=? AND r.silence_id=?
ORDER BY rev.revision_number DESC LIMIT 1`, source.SourceName, source.ClusterID, silence.SilenceID).Scan(
				&rule.RuleID, &rule.RevisionID, &rule.EpisodeID, &rule.RevisionNumber, &rule.ContentFingerprint,
				&rule.EvaluationFingerprint, &rule.MatchersJSON, &rule.StartsAt, &rule.EndsAt,
				&sourceUpdatedAt, &rule.CreatedBy, &rule.Comment, &rule.AnnotationsJSON,
				&rule.TicketID, &rule.TicketURL, &rule.ObservedState,
			)
			if err != nil {
				return nil, fmt.Errorf("controlled source rule %s/%s: %w", source.SourceName, silence.SilenceID, err)
			}
			if sourceUpdatedAt.Valid {
				rule.SourceUpdatedAt = sourceUpdatedAt.String
			}
			if rule.ObservedState != silence.Status || rule.ContentFingerprint != contentFingerprint || rule.EvaluationFingerprint != evaluationFingerprint || rule.MatchersJSON != matchersJSON {
				return nil, fmt.Errorf("controlled source rule %s/%s provenance mismatch", source.SourceName, silence.SilenceID)
			}
			expectedAnnotations := silence.AnnotationsJSON
			if expectedAnnotations == "" {
				expectedAnnotations = "{}"
			}
			expectedUpdatedAt := ""
			if silence.UpdatedAt != nil {
				expectedUpdatedAt = silence.UpdatedAt.UTC().Format(time.RFC3339Nano)
			}
			if rule.StartsAt != silence.StartsAt.UTC().Format(time.RFC3339Nano) || rule.EndsAt != silence.EndsAt.UTC().Format(time.RFC3339Nano) || rule.SourceUpdatedAt != expectedUpdatedAt || rule.CreatedBy != silence.CreatedBy || rule.Comment != silence.Comment || rule.AnnotationsJSON != expectedAnnotations || rule.TicketID != silence.TicketID || rule.TicketURL != silence.TicketURL {
				return nil, fmt.Errorf("controlled source rule %s/%s semantic payload mismatch starts=%q/%q ends=%q/%q updated=%q/%q created=%q/%q comment=%q/%q annotations=%q/%q ticket=%q/%q", source.SourceName, silence.SilenceID, rule.StartsAt, silence.StartsAt.UTC().Format(time.RFC3339Nano), rule.EndsAt, silence.EndsAt.UTC().Format(time.RFC3339Nano), rule.SourceUpdatedAt, expectedUpdatedAt, rule.CreatedBy, silence.CreatedBy, rule.Comment, silence.Comment, rule.AnnotationsJSON, expectedAnnotations, rule.TicketID+"/"+rule.TicketURL, silence.TicketID+"/"+silence.TicketURL)
			}
			// IDs are local bindings; every semantic expected value below comes from the controlled poll input.
			rule.SourceName, rule.ClusterID, rule.TargetKey, rule.SilenceID = source.SourceName, source.ClusterID, source.ClusterID, silence.SilenceID
			rule.ObservedState = silence.Status
			rule.ContentFingerprint, rule.EvaluationFingerprint, rule.MatchersJSON = contentFingerprint, evaluationFingerprint, matchersJSON
			rule.StartsAt, rule.EndsAt = silence.StartsAt.UTC().Format(time.RFC3339Nano), silence.EndsAt.UTC().Format(time.RFC3339Nano)
			rule.SourceUpdatedAt = ""
			if silence.UpdatedAt != nil {
				rule.SourceUpdatedAt = silence.UpdatedAt.UTC().Format(time.RFC3339Nano)
			}
			rule.CreatedBy, rule.Comment, rule.AnnotationsJSON = silence.CreatedBy, silence.Comment, expectedAnnotations
			rule.TicketID, rule.TicketURL = silence.TicketID, silence.TicketURL
			expected = append(expected, rule)
		}
	}
	if len(expected) == 0 {
		return nil, errors.New("controlled source has no non-empty active/pending rules")
	}
	return expected, nil
}

func buildProblem5ControlledCandidateRefs(snapshots []alertlifecycle.LiveAlertSnapshot, failedSourceName, failedSourceCluster string) (map[string]string, map[string]string, error) {
	relevant := make(map[string]string, len(snapshots)+1)
	members := make(map[string]string)
	nonEmpty := 0
	for _, snapshot := range snapshots {
		relevantJSON, err := json.Marshal(snapshot.RelevantSources)
		if err != nil {
			return nil, nil, err
		}
		relevant[snapshot.AlertID] = string(relevantJSON)
		if len(snapshot.RelevantSources) > 0 {
			nonEmpty++
		}
		for _, target := range snapshot.Targets {
			membersJSON, marshalErr := json.Marshal(target.SourceNames)
			if marshalErr != nil {
				return nil, nil, marshalErr
			}
			members[snapshot.AlertID+"\x00"+target.Target] = string(membersJSON)
		}
	}
	relevant["problem5-failed-context"] = fmt.Sprintf(`[{"name":%q,"cluster":%q}]`, failedSourceName, failedSourceCluster)
	if nonEmpty == 0 {
		return nil, nil, errors.New("controlled candidates have no non-empty source references")
	}
	return relevant, members, nil
}

func assertProblem5ControlledInput(db *sql.DB, jobID int, sources []alertlifecycle.SourceSilenceState, expectedRules []problem5ControlledRule, expectedRelevant, expectedMembers map[string]string) error {
	var sourceContext string
	if err := db.QueryRow(`SELECT source_context_json FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&sourceContext); err != nil {
		return err
	}
	var payload struct {
		Sources []struct {
			SourceName string `json:"sourceName"`
			ClusterID  string `json:"clusterId"`
			SyncStatus string `json:"syncStatus"`
		} `json:"sources"`
	}
	if err := json.Unmarshal([]byte(sourceContext), &payload); err != nil {
		return err
	}
	if len(payload.Sources) != len(sources) {
		return fmt.Errorf("controlled source context sources=%d want=%d", len(payload.Sources), len(sources))
	}
	expectedSources := make(map[string]alertlifecycle.SourceSilenceState, len(sources))
	for _, source := range sources {
		expectedSources[source.SourceName] = source
	}
	for _, actual := range payload.Sources {
		expected, ok := expectedSources[actual.SourceName]
		if !ok || actual.ClusterID != expected.ClusterID || actual.SyncStatus != expected.SyncStatus {
			return fmt.Errorf("controlled source context row=%+v expected=%+v", actual, expected)
		}
	}
	var ruleCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_rules WHERE job_id=?`, jobID).Scan(&ruleCount); err != nil {
		return err
	}
	if ruleCount != len(expectedRules) {
		return fmt.Errorf("controlled job rules=%d want=%d", ruleCount, len(expectedRules))
	}
	expectedRuleByKey := make(map[string]problem5ControlledRule, len(expectedRules))
	for _, rule := range expectedRules {
		expectedRuleByKey[rule.SourceName+"\x00"+rule.SilenceID] = rule
	}
	rows, err := db.Query(`SELECT source_name,cluster_id,target_key,rule_id,revision_id,silence_id,observed_state,content_fingerprint,evaluation_fingerprint,matchers_json FROM silence_recheck_job_rules WHERE job_id=? ORDER BY source_name,silence_id`, jobID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var sourceName, clusterID, targetKey, silenceID, observedState, contentFingerprint, evaluationFingerprint, matchers string
		var ruleID, revisionID int64
		if err = rows.Scan(&sourceName, &clusterID, &targetKey, &ruleID, &revisionID, &silenceID, &observedState, &contentFingerprint, &evaluationFingerprint, &matchers); err != nil {
			return err
		}
		expected, ok := expectedRuleByKey[sourceName+"\x00"+silenceID]
		if !ok || clusterID != expected.ClusterID || targetKey != expected.TargetKey || ruleID != expected.RuleID || revisionID != expected.RevisionID || observedState != expected.ObservedState || contentFingerprint != expected.ContentFingerprint || evaluationFingerprint != expected.EvaluationFingerprint || matchers != expected.MatchersJSON {
			return fmt.Errorf("controlled rule provenance source=%s cluster=%s target=%s silence=%s rule=%d revision=%d", sourceName, clusterID, targetKey, silenceID, ruleID, revisionID)
		}
		var revisionNumber int64
		var startsAt, endsAt, createdBy, comment, annotations string
		if err = db.QueryRow(`SELECT revision_number,starts_at,ends_at,created_by,comment,annotations_json FROM silence_revisions WHERE revision_id=? AND rule_id=?`, revisionID, ruleID).Scan(&revisionNumber, &startsAt, &endsAt, &createdBy, &comment, &annotations); err != nil {
			return err
		}
		if revisionNumber != expected.RevisionNumber || startsAt != expected.StartsAt || endsAt != expected.EndsAt || createdBy != expected.CreatedBy || comment != expected.Comment || annotations != expected.AnnotationsJSON {
			return fmt.Errorf("controlled revision provenance source=%s silence=%s revision=%d", sourceName, silenceID, revisionID)
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	candidateRows, err := db.Query(`SELECT alert_id,target_key,relevant_sources_json,target_source_members_json FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`, jobID)
	if err != nil {
		return err
	}
	defer candidateRows.Close()
	nonEmptyRefs := 0
	for candidateRows.Next() {
		var alertID, targetKey, relevant, members sql.NullString
		if err = candidateRows.Scan(&alertID, &targetKey, &relevant, &members); err != nil {
			return err
		}
		wantRelevant, ok := expectedRelevant[alertID.String]
		if !ok || !relevant.Valid || relevant.String != wantRelevant {
			return fmt.Errorf("controlled candidate %s/%s source references=%q want=%q", alertID.String, targetKey.String, relevant.String, wantRelevant)
		}
		if relevant.String != "[]" {
			nonEmptyRefs++
		}
		wantMembers := expectedMembers[alertID.String+"\x00"+targetKey.String]
		if wantMembers == "" {
			if members.Valid {
				return fmt.Errorf("controlled global candidate %s has target members=%q", alertID.String, members.String)
			}
		} else if !members.Valid || members.String != wantMembers {
			return fmt.Errorf("controlled candidate %s/%s source members=%q want=%q", alertID.String, targetKey.String, members.String, wantMembers)
		}
	}
	if err = candidateRows.Err(); err != nil {
		return err
	}
	if nonEmptyRefs == 0 {
		return errors.New("controlled job candidates have no non-empty source references")
	}
	return nil
}

func assertProblem5IndependentImmutableModel(db *sql.DB, jobID int, sources, initialSources []alertlifecycle.SourceSilenceState, rules []problem5ControlledRule, expectedRelevant, expectedMembers map[string]string, snapshots []alertlifecycle.LiveAlertSnapshot, manifest []problem5ReplayScope, successorID, matcherValue string, now time.Time) error {
	contextJSON, sourceFingerprint, err := problem5ExpectedSourceContext(sources, 2)
	if err != nil {
		return err
	}
	expectedRules := make([]problem5IndependentRuleInput, 0, len(rules))
	for _, rule := range rules {
		expectedRules = append(expectedRules, problem5IndependentRuleInput{
			RuleID: rule.RuleID, RevisionID: rule.RevisionID,
			SourceName: rule.SourceName, ClusterID: rule.ClusterID, TargetKey: rule.TargetKey,
			SilenceID: rule.SilenceID, ContentFingerprint: rule.ContentFingerprint, ObservedState: rule.ObservedState,
			EvaluationFingerprint: rule.EvaluationFingerprint, MatchersJSON: rule.MatchersJSON,
		})
	}
	sort.Slice(expectedRules, func(i, j int) bool {
		if expectedRules[i].SourceName != expectedRules[j].SourceName {
			return expectedRules[i].SourceName < expectedRules[j].SourceName
		}
		if expectedRules[i].TargetKey != expectedRules[j].TargetKey {
			return expectedRules[i].TargetKey < expectedRules[j].TargetKey
		}
		return expectedRules[i].SilenceID < expectedRules[j].SilenceID
	})
	expectedCandidates, err := problem5ExpectedCandidateInputs(snapshots, manifest, successorID, matcherValue, expectedRelevant, expectedMembers, now)
	if err != nil {
		return err
	}
	expectedInput := problem5IndependentJobInput{
		TriggerType: "source_context_changed", SourceContext: json.RawMessage(contextJSON), SourceContextFingerprint: sourceFingerprint,
		Rules: expectedRules, Candidates: expectedCandidates,
	}
	jobRaw, err := json.Marshal(expectedInput)
	if err != nil {
		return err
	}
	expectedJobFingerprint := problem5Fingerprint(jobRaw)
	if len(expectedCandidates) < 20 {
		return fmt.Errorf("independent parent model has only %d candidates before first batch", len(expectedCandidates))
	}
	var actualSourceFingerprint, actualJobFingerprint, actualContext, trigger, status, createdAt string
	var cursor, nextAttempt, firstMessage, firstClass, firstStage, firstAt, lastMessage, lastClass, lastStage, lastAt, abandonedAt, abandonedBy, abandonReason, startedAt, finishedAt sql.NullString
	var total, processed, promoted, skippedIneligible, skippedMissingLabels, skippedIncompleteSources, staleConflicts, attempts, retries, consecutive, recovery int
	if err = db.QueryRow(`SELECT source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,cursor_alert_id,
       total_candidates,processed_candidates,promoted_candidates,skipped_ineligible,skipped_missing_labels,
       skipped_incomplete_sources,stale_conflicts,attempt_count,retry_count,consecutive_retry_count,next_attempt_at,
       first_error_message,first_error_class,first_error_stage,first_error_at,last_error_message,last_error_class,
       last_error_stage,last_error_at,recovery_count,abandoned_at,abandoned_by,abandon_reason,created_at,started_at,finished_at
FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(
		&actualSourceFingerprint, &actualJobFingerprint, &actualContext, &trigger, &status, &cursor,
		&total, &processed, &promoted, &skippedIneligible, &skippedMissingLabels, &skippedIncompleteSources,
		&staleConflicts, &attempts, &retries, &consecutive, &nextAttempt, &firstMessage, &firstClass,
		&firstStage, &firstAt, &lastMessage, &lastClass, &lastStage, &lastAt, &recovery, &abandonedAt,
		&abandonedBy, &abandonReason, &createdAt, &startedAt, &finishedAt,
	); err != nil {
		return err
	}
	created, createdErr := time.Parse(time.RFC3339Nano, createdAt)
	started, startedErr := time.Parse(time.RFC3339Nano, startedAt.String)
	_, startedOffset := started.UTC().Zone()
	if actualSourceFingerprint != sourceFingerprint || actualContext != contextJSON || trigger != expectedInput.TriggerType || status != "running" || !cursor.Valid || cursor.String != expectedCandidates[19].AlertID || total != len(expectedCandidates) || processed != 20 || promoted+skippedIneligible+skippedMissingLabels+skippedIncompleteSources+staleConflicts != processed || attempts != 1 || retries != 0 || consecutive != 0 || recovery != 0 || !createdAtUTCAtOrAfter(created, now.Add(time.Minute)) || createdErr != nil || startedErr != nil || startedOffset != 0 || nextAttempt.Valid || firstMessage.Valid || firstClass.Valid || firstStage.Valid || firstAt.Valid || lastMessage.Valid || lastClass.Valid || lastStage.Valid || lastAt.Valid || abandonedAt.Valid || abandonedBy.Valid || abandonReason.Valid || finishedAt.Valid {
		return fmt.Errorf("independent parent model mismatch source_fingerprint=%q/%q job_fingerprint=%q/%q context_equal=%t cursor=%q total=%d/%d processed=%d counters=%d/%d/%d/%d/%d attempts=%d retries=%d consecutive=%d recovery=%d status=%q", actualSourceFingerprint, sourceFingerprint, actualJobFingerprint, expectedJobFingerprint, actualContext == contextJSON, cursor.String, total, len(expectedCandidates), processed, promoted, skippedIneligible, skippedMissingLabels, skippedIncompleteSources, staleConflicts, attempts, retries, consecutive, recovery, status)
	}
	rows, err := db.Query(`SELECT alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`, jobID)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var alertID, targetKey string
		var actualAlert, actualTarget, actualLabels, actualRelevant, actualMembers sql.NullString
		if err = rows.Scan(&alertID, &targetKey, &actualAlert, &actualTarget, &actualLabels, &actualRelevant, &actualMembers); err != nil {
			return err
		}
		key := alertID + "\x00" + targetKey
		expected, ok := expectedCandidatesByKey(expectedCandidates)[key]
		if !ok || !problem5NullableEqual(actualAlert, expected.ExpectedAlertLastSeenAt) || !problem5NullableEqual(actualTarget, expected.ExpectedTargetLastSeenAt) || !problem5NullableEqual(actualLabels, expected.MatchLabelsJSON) || actualRelevant.String != expected.RelevantSourcesJSON || !problem5NullableEqual(actualMembers, expected.TargetSourceMembersJSON) {
			var snapshotInfo string
			for _, snapshot := range snapshots {
				if snapshot.AlertID == alertID {
					snapshotInfo = fmt.Sprintf("state=%s targets=%d", snapshot.State, len(snapshot.Targets))
					break
				}
			}
			return fmt.Errorf("independent candidate model mismatch key=%s (%s) alert=%q/%v target=%q/%v labels=%q/%v relevant=%q/%q members=%q/%v", key, snapshotInfo, actualAlert.String, expected.ExpectedAlertLastSeenAt, actualTarget.String, expected.ExpectedTargetLastSeenAt, actualLabels.String, expected.MatchLabelsJSON, actualRelevant.String, expected.RelevantSourcesJSON, actualMembers.String, expected.TargetSourceMembersJSON)
		}
		seen++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if seen != len(expectedCandidates) {
		return fmt.Errorf("independent candidate model coverage=%d want=%d", seen, len(expectedCandidates))
	}
	if total != len(expectedCandidates) {
		return fmt.Errorf("independent parent candidate total=%d want=%d", total, len(expectedCandidates))
	}
	if actualJobFingerprint != expectedJobFingerprint {
		return fmt.Errorf("independent job fingerprint mismatch actual=%q expected=%q after all candidate columns matched", actualJobFingerprint, expectedJobFingerprint)
	}
	if err := assertProblem5IndependentImmutableRows(db, jobID, rules, initialSources, expectedCandidates, snapshots, now); err != nil {
		return err
	}
	return nil
}

func assertProblem5IndependentImmutableRows(db *sql.DB, jobID int, rules []problem5ControlledRule, initialSources []alertlifecycle.SourceSilenceState, expectedCandidates []problem5IndependentCandidateInput, snapshots []alertlifecycle.LiveAlertSnapshot, now time.Time) error {
	expectedRules := make(map[string]problem5ControlledRule, len(rules))
	for _, rule := range rules {
		expectedRules[problem5RuleKey(rule.SourceName, rule.ClusterID, rule.TargetKey, rule.SilenceID)] = rule
	}
	ruleRows, err := db.Query(`
SELECT job_id,source_name,cluster_id,target_key,rule_id,revision_id,silence_id,content_fingerprint,
       observed_state,evaluation_fingerprint,matchers_json
FROM silence_recheck_job_rules WHERE job_id=? ORDER BY source_name,cluster_id,target_key,silence_id`, jobID)
	if err != nil {
		return err
	}
	seenRules := make(map[string]bool, len(expectedRules))
	for ruleRows.Next() {
		var actualJobID, ruleID, revisionID int64
		var sourceName, clusterID, targetKey, silenceID, contentFingerprint, observedState, evaluationFingerprint, matchersJSON string
		if err = ruleRows.Scan(&actualJobID, &sourceName, &clusterID, &targetKey, &ruleID, &revisionID, &silenceID, &contentFingerprint, &observedState, &evaluationFingerprint, &matchersJSON); err != nil {
			ruleRows.Close()
			return err
		}
		key := problem5RuleKey(sourceName, clusterID, targetKey, silenceID)
		want, ok := expectedRules[key]
		if !ok || seenRules[key] || actualJobID != int64(jobID) || ruleID != want.RuleID || revisionID != want.RevisionID || contentFingerprint != want.ContentFingerprint || observedState != want.ObservedState || evaluationFingerprint != want.EvaluationFingerprint || matchersJSON != want.MatchersJSON {
			ruleRows.Close()
			return fmt.Errorf("immutable job rule mismatch key=%q job=%d/%d rule=%d/%d revision=%d/%d content=%q/%q state=%q/%q evaluation=%q/%q matchers=%q/%q", key, actualJobID, jobID, ruleID, want.RuleID, revisionID, want.RevisionID, contentFingerprint, want.ContentFingerprint, observedState, want.ObservedState, evaluationFingerprint, want.EvaluationFingerprint, matchersJSON, want.MatchersJSON)
		}
		seenRules[key] = true
	}
	if err = ruleRows.Err(); err != nil {
		ruleRows.Close()
		return err
	}
	ruleRows.Close()
	if len(seenRules) != len(expectedRules) {
		return fmt.Errorf("immutable job rule coverage=%d want=%d", len(seenRules), len(expectedRules))
	}
	for _, rule := range rules {
		if err = assertProblem5IndependentRuleProvenance(db, rule, initialSources, now); err != nil {
			return err
		}
	}

	expectedByKey := expectedCandidatesByKey(expectedCandidates)
	candidateRows, err := db.Query(`
SELECT job_id,alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,
       relevant_sources_json,target_source_members_json,processed_at,outcome
FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`, jobID)
	if err != nil {
		return err
	}
	seenCandidates := make(map[string]bool, len(expectedByKey))
	for candidateRows.Next() {
		var actualJobID int64
		var alertID, targetKey string
		var expectedAlert, expectedTarget, matchLabels, relevantSources, targetMembers, processedAt, outcome sql.NullString
		if err = candidateRows.Scan(&actualJobID, &alertID, &targetKey, &expectedAlert, &expectedTarget, &matchLabels, &relevantSources, &targetMembers, &processedAt, &outcome); err != nil {
			candidateRows.Close()
			return err
		}
		key := alertID + "\x00" + targetKey
		want, ok := expectedByKey[key]
		if !ok || seenCandidates[key] || actualJobID != int64(jobID) || !problem5NullableEqual(expectedAlert, want.ExpectedAlertLastSeenAt) || !problem5NullableEqual(expectedTarget, want.ExpectedTargetLastSeenAt) || !problem5NullableEqual(matchLabels, want.MatchLabelsJSON) || !replayNullStringEqual(relevantSources, want.RelevantSourcesJSON) || !problem5NullableEqual(targetMembers, want.TargetSourceMembersJSON) {
			candidateRows.Close()
			return fmt.Errorf("immutable candidate mismatch key=%q job=%d/%d alert=%q target=%q labels=%q relevant=%q members=%q", key, actualJobID, jobID, alertID, targetKey, matchLabels.String, relevantSources.String, targetMembers.String)
		}
		wantProcessed, wantOutcome := problem5ExpectedCandidateProgress(alertID, targetKey, snapshots, expectedCandidates, now)
		if !replayNullStringEqual(processedAt, wantProcessed) || !replayNullStringEqual(outcome, wantOutcome) {
			candidateRows.Close()
			return fmt.Errorf("candidate progress mismatch key=%q processed=%q/%q outcome=%q/%q", key, processedAt.String, wantProcessed, outcome.String, wantOutcome)
		}
		seenCandidates[key] = true
	}
	if err = candidateRows.Err(); err != nil {
		candidateRows.Close()
		return err
	}
	candidateRows.Close()
	if len(seenCandidates) != len(expectedByKey) {
		return fmt.Errorf("immutable candidate coverage=%d want=%d", len(seenCandidates), len(expectedByKey))
	}
	return nil
}

func problem5RuleKey(sourceName, clusterID, targetKey, silenceID string) string {
	return sourceName + "\x00" + clusterID + "\x00" + targetKey + "\x00" + silenceID
}

func assertProblem5IndependentRuleProvenance(db *sql.DB, rule problem5ControlledRule, initialSources []alertlifecycle.SourceSilenceState, now time.Time) error {
	ruleFirstObserved := problem5RuleFirstObservedAt(initialSources, rule, now)
	provenanceFirstObserved := ruleFirstObserved
	sourceFirstObserved := now.Add(4 * time.Minute).UTC().Format(time.RFC3339Nano)
	lastObserved := now.Add(4 * time.Minute).UTC().Format(time.RFC3339Nano)
	var actualCluster, actualSilence, ruleFirst, ruleLast string
	if err := db.QueryRow(`SELECT cluster_id,silence_id,first_observed_at,last_observed_at FROM silence_rules WHERE rule_id=?`, rule.RuleID).Scan(&actualCluster, &actualSilence, &ruleFirst, &ruleLast); err != nil {
		return err
	}
	if actualCluster != rule.ClusterID || actualSilence != rule.SilenceID || ruleFirst != ruleFirstObserved || ruleLast != lastObserved {
		return fmt.Errorf("silence rule %d provenance=%q/%q/%q/%q want=%q/%q/%q/%q", rule.RuleID, actualCluster, actualSilence, ruleFirst, ruleLast, rule.ClusterID, rule.SilenceID, ruleFirstObserved, lastObserved)
	}
	var episodeID, episodeRuleID, episodeNumber int64
	var episodeFirst, episodeLast string
	var endedAt, endReason, openMarker sql.NullString
	if err := db.QueryRow(`SELECT episode_id,rule_id,episode_number,first_observed_at,last_confirmed_at,ended_at,end_reason,open_marker FROM silence_rule_episodes WHERE rule_id=? AND open_marker=1`, rule.RuleID).Scan(&episodeID, &episodeRuleID, &episodeNumber, &episodeFirst, &episodeLast, &endedAt, &endReason, &openMarker); err != nil {
		return err
	}
	if episodeID != rule.EpisodeID || episodeRuleID != rule.RuleID || episodeNumber != 1 || episodeFirst != provenanceFirstObserved || episodeLast != lastObserved || endedAt.Valid || endReason.Valid || !openMarker.Valid || openMarker.String != "1" {
		return fmt.Errorf("silence episode %d has unexpected complete row rule=%d/%d number=%d first=%q/%q last=%q/%q ended=%v reason=%v open=%q", episodeID, episodeRuleID, rule.RuleID, episodeNumber, episodeFirst, provenanceFirstObserved, episodeLast, lastObserved, endedAt.Valid, endReason.Valid, openMarker.String)
	}
	var revisionEpisodeID, revisionRuleID, revisionNumber int64
	var revisionContent, revisionEvaluation, revisionMatchers, revisionStarts, revisionEnds string
	var sourceUpdated, revisionCreated, revisionComment, revisionAnnotations, revisionTicketID, revisionTicketURL, revisionFirst, revisionLast sql.NullString
	if err := db.QueryRow(`
SELECT episode_id,rule_id,revision_number,content_fingerprint,evaluation_fingerprint,matchers_json,starts_at,ends_at,
       source_updated_at,created_by,comment,annotations_json,ticket_id,ticket_url,first_observed_at,last_observed_at
FROM silence_revisions WHERE revision_id=? AND rule_id=?`, rule.RevisionID, rule.RuleID).Scan(
		&revisionEpisodeID, &revisionRuleID, &revisionNumber, &revisionContent, &revisionEvaluation, &revisionMatchers, &revisionStarts, &revisionEnds,
		&sourceUpdated, &revisionCreated, &revisionComment, &revisionAnnotations, &revisionTicketID, &revisionTicketURL, &revisionFirst, &revisionLast,
	); err != nil {
		return err
	}
	if revisionEpisodeID != rule.EpisodeID || revisionRuleID != rule.RuleID || revisionNumber != rule.RevisionNumber || revisionContent != rule.ContentFingerprint || revisionEvaluation != rule.EvaluationFingerprint || revisionMatchers != rule.MatchersJSON || revisionStarts != rule.StartsAt || revisionEnds != rule.EndsAt || !replayNullStringEqual(sourceUpdated, rule.SourceUpdatedAt) || !replayNullStringEqual(revisionCreated, rule.CreatedBy) || !replayNullStringEqual(revisionComment, rule.Comment) || !replayNullStringEqual(revisionAnnotations, rule.AnnotationsJSON) || !replayNullStringEqual(revisionTicketID, rule.TicketID) || !replayNullStringEqual(revisionTicketURL, rule.TicketURL) || !replayNullStringEqual(revisionFirst, provenanceFirstObserved) || !replayNullStringEqual(revisionLast, lastObserved) {
		return fmt.Errorf("silence revision %d has unexpected complete row", rule.RevisionID)
	}
	var sourceRuleID, sourceRevisionID int64
	var sourceState, sourceFirst, sourceLast string
	if err := db.QueryRow(`SELECT rule_id,revision_id,observed_state,first_observed_at,last_confirmed_at FROM alertmanager_source_silences WHERE source_name=? AND rule_id=?`, rule.SourceName, rule.RuleID).Scan(&sourceRuleID, &sourceRevisionID, &sourceState, &sourceFirst, &sourceLast); err != nil {
		return err
	}
	if sourceRuleID != rule.RuleID || sourceRevisionID != rule.RevisionID || sourceState != rule.ObservedState || sourceFirst != sourceFirstObserved || sourceLast != lastObserved {
		return fmt.Errorf("source silence %s/%s/%d has unexpected complete row ids=%d/%d revision=%d/%d state=%q/%q first=%q/%q last=%q/%q", rule.SourceName, rule.SilenceID, rule.RuleID, sourceRuleID, rule.RuleID, sourceRevisionID, rule.RevisionID, sourceState, rule.ObservedState, sourceFirst, sourceFirstObserved, sourceLast, lastObserved)
	}
	return nil
}

func problem5RuleFirstObservedAt(initialSources []alertlifecycle.SourceSilenceState, rule problem5ControlledRule, now time.Time) string {
	for _, source := range initialSources {
		if source.SourceName != rule.SourceName || source.ClusterID != rule.ClusterID {
			continue
		}
		for _, silence := range source.Silences {
			if silence.SilenceID == rule.SilenceID {
				return now.UTC().Format(time.RFC3339Nano)
			}
		}
	}
	return now.Add(time.Minute).UTC().Format(time.RFC3339Nano)
}

func replayNullStringEqual(actual sql.NullString, expected string) bool {
	if expected == "" {
		return !actual.Valid || actual.String == ""
	}
	return actual.Valid && actual.String == expected
}

func problem5ExpectedCandidateProgress(alertID, targetKey string, snapshots []alertlifecycle.LiveAlertSnapshot, candidates []problem5IndependentCandidateInput, now time.Time) (string, string) {
	index := problem5ExpectedCandidateIndex(alertID, targetKey, candidates)
	if index < 0 {
		return "", ""
	}
	wantOutcome := problem5ExpectedCandidateOutcome(index, alertID, targetKey, snapshots)
	if index >= 20 {
		return "", ""
	}
	processedAt := now.Add(5 * time.Minute).UTC().Format(time.RFC3339Nano)
	return processedAt, wantOutcome
}

func problem5ExpectedCandidateIndex(alertID, targetKey string, candidates []problem5IndependentCandidateInput) int {
	for index, candidate := range candidates {
		if candidate.AlertID == alertID && candidate.TargetKey == targetKey {
			return index
		}
	}
	return -1
}

func problem5ExpectedCandidateOutcome(index int, alertID, targetKey string, snapshots []alertlifecycle.LiveAlertSnapshot) string {
	if alertID == "problem5-failed-context" {
		return "incomplete_source_coverage"
	}
	if index == 0 {
		return "already_resolved"
	}
	if index == 1 {
		if targetKey == "" {
			return "stale_alert_observation"
		}
		return "stale_target_observation"
	}
	for _, snapshot := range snapshots {
		if snapshot.AlertID == alertID {
			if snapshot.State == alertlifecycle.StateSuppressed || replaySnapshotInstance(snapshot) == replayInstanceMatcherValue(snapshots) {
				return "already_suppressed"
			}
			break
		}
	}
	return "no_matching_rule"
}

type problem5IndependentSourceContext struct {
	FormatVersion int                          `json:"formatVersion"`
	Generation    int                          `json:"generation"`
	Sources       []problem5IndependentSource  `json:"sources"`
	Clusters      []problem5IndependentCluster `json:"clusters"`
}

type problem5IndependentSource struct {
	SourceName         string `json:"sourceName"`
	ClusterID          string `json:"clusterId"`
	SyncStatus         string `json:"syncStatus"`
	SilenceFingerprint string `json:"silenceFingerprint"`
	Retired            bool   `json:"retired"`
}

type problem5IndependentCluster struct {
	ClusterID   string `json:"clusterId"`
	Status      string `json:"status"`
	MemberCount int    `json:"memberCount"`
}

type problem5IndependentRuleInput struct {
	RuleID                int64  `json:"-"`
	RevisionID            int64  `json:"-"`
	SourceName            string `json:"sourceName"`
	ClusterID             string `json:"clusterId"`
	TargetKey             string `json:"targetKey"`
	SilenceID             string `json:"silenceId"`
	ContentFingerprint    string `json:"contentFingerprint"`
	ObservedState         string `json:"observedState"`
	EvaluationFingerprint string `json:"evaluationFingerprint"`
	MatchersJSON          string `json:"matchersJson"`
}

type problem5IndependentCandidateInput struct {
	AlertID                  string  `json:"alertId"`
	TargetKey                string  `json:"targetKey"`
	ExpectedAlertLastSeenAt  *string `json:"expectedAlertLastSeenAt"`
	ExpectedTargetLastSeenAt *string `json:"expectedTargetLastSeenAt"`
	MatchLabelsJSON          *string `json:"matchLabelsJson"`
	RelevantSourcesJSON      string  `json:"relevantSourcesJson"`
	TargetSourceMembersJSON  *string `json:"targetSourceMembersJson"`
}

type problem5IndependentJobInput struct {
	TriggerType              string                              `json:"triggerType"`
	SourceContext            json.RawMessage                     `json:"sourceContext"`
	SourceContextFingerprint string                              `json:"sourceContextFingerprint"`
	Rules                    []problem5IndependentRuleInput      `json:"rules"`
	Candidates               []problem5IndependentCandidateInput `json:"candidates"`
}

func problem5ExpectedCandidateInputs(snapshots []alertlifecycle.LiveAlertSnapshot, manifest []problem5ReplayScope, successorID, matcherValue string, relevant, members map[string]string, now time.Time) ([]problem5IndependentCandidateInput, error) {
	snapshotByID := make(map[string]alertlifecycle.LiveAlertSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		snapshotByID[snapshot.AlertID] = snapshot
	}
	scopes := make([]problem5ReplayScope, 0, len(manifest)+len(snapshots))
	for _, scope := range manifest {
		if scope.AlertID != successorID {
			scopes = append(scopes, scope)
		}
	}
	for _, snapshot := range snapshots {
		var labels map[string]string
		_ = json.Unmarshal([]byte(snapshot.LabelsJSON), &labels)
		if snapshot.State != alertlifecycle.StateActive || labels["instance"] != matcherValue || snapshot.AlertID == successorID {
			continue
		}
		if len(snapshot.Targets) == 0 {
			scopes = append(scopes, problem5ReplayScope{AlertID: snapshot.AlertID, MatchLabelsJSON: snapshot.LabelsJSON})
			continue
		}
		for _, target := range snapshot.Targets {
			scopes = append(scopes, problem5ReplayScope{AlertID: snapshot.AlertID, TargetKey: target.Target, MatchLabelsJSON: snapshot.LabelsJSON})
		}
	}
	expected := make([]problem5IndependentCandidateInput, 0, len(scopes)+1)
	for _, scope := range scopes {
		snapshot, ok := snapshotByID[scope.AlertID]
		if !ok {
			return nil, fmt.Errorf("independent candidate model missing snapshot %q", scope.AlertID)
		}
		alertLastSeen := snapshot.LastSeenAt.UTC().Format(time.RFC3339Nano)
		labels := scope.MatchLabelsJSON
		labels = problem5CanonicalJSONString(labels)
		candidate := problem5IndependentCandidateInput{AlertID: scope.AlertID, TargetKey: scope.TargetKey, ExpectedAlertLastSeenAt: &alertLastSeen, MatchLabelsJSON: &labels, RelevantSourcesJSON: problem5CanonicalSourceReferencesJSON(relevant[scope.AlertID])}
		if scope.TargetKey != "" {
			targetLastSeen := now.UTC().Format(time.RFC3339Nano)
			candidate.ExpectedTargetLastSeenAt = &targetLastSeen
			membersJSON := problem5CanonicalStringSliceJSON(members[scope.AlertID+"\x00"+scope.TargetKey])
			candidate.TargetSourceMembersJSON = &membersJSON
		}
		expected = append(expected, candidate)
	}
	failedLastSeen := now.Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	failedLabels := problem5CanonicalJSONString(`{"instance":"problem5-failed-context"}`)
	failedRelevant := problem5CanonicalSourceReferencesJSON(relevant["problem5-failed-context"])
	expected = append(expected, problem5IndependentCandidateInput{AlertID: "problem5-failed-context", ExpectedAlertLastSeenAt: &failedLastSeen, MatchLabelsJSON: &failedLabels, RelevantSourcesJSON: failedRelevant})
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].AlertID != expected[j].AlertID {
			return expected[i].AlertID < expected[j].AlertID
		}
		return expected[i].TargetKey < expected[j].TargetKey
	})
	return expected, nil
}

func expectedCandidatesByKey(candidates []problem5IndependentCandidateInput) map[string]problem5IndependentCandidateInput {
	result := make(map[string]problem5IndependentCandidateInput, len(candidates))
	for _, candidate := range candidates {
		result[candidate.AlertID+"\x00"+candidate.TargetKey] = candidate
	}
	return result
}

func problem5NullableEqual(actual sql.NullString, expected *string) bool {
	if expected == nil {
		return !actual.Valid
	}
	return actual.Valid && actual.String == *expected
}

func problem5CanonicalJSONString(raw string) string {
	var value any
	if json.Unmarshal([]byte(raw), &value) != nil {
		return raw
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return raw
	}
	return string(encoded)
}

func problem5CanonicalSourceReferencesJSON(raw string) string {
	var refs []struct {
		Name    string `json:"name"`
		Cluster string `json:"cluster"`
	}
	if err := json.Unmarshal([]byte(raw), &refs); err != nil {
		return problem5CanonicalJSONString(raw)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Name != refs[j].Name {
			return refs[i].Name < refs[j].Name
		}
		return refs[i].Cluster < refs[j].Cluster
	})
	encoded, err := json.Marshal(refs)
	if err != nil {
		return raw
	}
	return string(encoded)
}

func problem5CanonicalStringSliceJSON(raw string) string {
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return raw
	}
	sort.Strings(values)
	encoded, err := json.Marshal(values)
	if err != nil {
		return raw
	}
	return string(encoded)
}

func problem5ExpectedSourceContext(sources []alertlifecycle.SourceSilenceState, generation int) (string, string, error) {
	context := problem5IndependentSourceContext{FormatVersion: 1, Generation: generation}
	clusters := make(map[string][]alertlifecycle.SourceSilenceState)
	for _, source := range sources {
		silenceFingerprint := ""
		if source.SyncStatus == "success" {
			silenceFingerprint = problem5SourceSilenceFingerprint(source)
		}
		context.Sources = append(context.Sources, problem5IndependentSource{SourceName: source.SourceName, ClusterID: source.ClusterID, SyncStatus: source.SyncStatus, SilenceFingerprint: silenceFingerprint, Retired: false})
		if source.ClusterID != "" {
			clusters[source.ClusterID] = append(clusters[source.ClusterID], source)
		}
	}
	sort.Slice(context.Sources, func(i, j int) bool { return context.Sources[i].SourceName < context.Sources[j].SourceName })
	for clusterID, members := range clusters {
		status := "unknown"
		allSuccessful := true
		fingerprints := map[string]bool{}
		for _, member := range members {
			if member.SyncStatus != "success" {
				allSuccessful = false
				break
			}
			fingerprints[problem5SourceConsistencyFingerprint(member)] = true
		}
		if allSuccessful {
			status = "consistent"
			if len(fingerprints) > 1 {
				status = "inconsistent"
			}
		}
		context.Clusters = append(context.Clusters, problem5IndependentCluster{ClusterID: clusterID, Status: status, MemberCount: len(members)})
	}
	sort.Slice(context.Clusters, func(i, j int) bool { return context.Clusters[i].ClusterID < context.Clusters[j].ClusterID })
	raw, err := json.Marshal(context)
	if err != nil {
		return "", "", err
	}
	return string(raw), problem5Fingerprint(raw), nil
}

func problem5SourceSilenceFingerprint(source alertlifecycle.SourceSilenceState) string {
	type silenceFingerprint struct {
		ID                    string `json:"id"`
		Status                string `json:"status"`
		StartsAt              string `json:"startsAt"`
		EndsAt                string `json:"endsAt"`
		UpdatedAt             string `json:"updatedAt"`
		EvaluationFingerprint string `json:"evaluationFingerprint"`
	}
	type sourceFingerprint struct {
		SourceName string               `json:"sourceName"`
		ClusterID  string               `json:"clusterId"`
		Silences   []silenceFingerprint `json:"silences"`
	}
	entries := make([]silenceFingerprint, 0, len(source.Silences))
	for _, silence := range source.Silences {
		updatedAt := ""
		if silence.UpdatedAt != nil {
			updatedAt = silence.UpdatedAt.UTC().Format(time.RFC3339Nano)
		}
		_, evaluation, _ := problem5SilenceRevisionFingerprints(silence)
		entries = append(entries, silenceFingerprint{ID: strings.TrimSpace(silence.SilenceID), Status: strings.TrimSpace(silence.Status), StartsAt: silence.StartsAt.UTC().Format(time.RFC3339Nano), EndsAt: silence.EndsAt.UTC().Format(time.RFC3339Nano), UpdatedAt: updatedAt, EvaluationFingerprint: evaluation})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	raw, _ := json.Marshal(sourceFingerprint{SourceName: strings.TrimSpace(source.SourceName), ClusterID: strings.TrimSpace(source.ClusterID), Silences: entries})
	return problem5Fingerprint(raw)
}

func problem5SourceConsistencyFingerprint(source alertlifecycle.SourceSilenceState) string {
	items := make([]string, 0, len(source.Silences))
	for _, silence := range source.Silences {
		content, evaluation, _ := problem5SilenceRevisionFingerprints(silence)
		items = append(items, strings.Join([]string{strings.TrimSpace(silence.SilenceID), content, evaluation, strings.TrimSpace(silence.Status)}, "\x00"))
	}
	sort.Strings(items)
	raw, _ := json.Marshal(items)
	return problem5Fingerprint(raw)
}

func problem5Fingerprint(raw []byte) string {
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum)
}

func assertReplayJobAccounting(db *sql.DB, jobID int) error {
	var total, processed, promoted, stale, ineligible, missingLabels, incomplete int
	if err := db.QueryRow(`SELECT total_candidates,processed_candidates,promoted_candidates,stale_conflicts,skipped_ineligible,skipped_missing_labels,skipped_incomplete_sources FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&total, &processed, &promoted, &stale, &ineligible, &missingLabels, &incomplete); err != nil {
		return err
	}
	if processed != total || processed != promoted+stale+ineligible+missingLabels+incomplete {
		return fmt.Errorf("job %d accounting total=%d processed=%d outcomes=%d", jobID, total, processed, promoted+stale+ineligible+missingLabels+incomplete)
	}
	return nil
}

func assertProblem5IndependentFinalSourceJob(db *sql.DB, jobID int, activeSources []alertlifecycle.SourceSilenceState, rules []problem5ControlledRule, expectedCandidates []problem5IndependentCandidateInput, snapshots []alertlifecycle.LiveAlertSnapshot, now time.Time) error {
	contextJSON, sourceFingerprint, err := problem5ExpectedSourceContext(activeSources, 2)
	if err != nil {
		return err
	}
	expectedJobFingerprint, err := problem5ExpectedJobFingerprint(contextJSON, sourceFingerprint, rules, expectedCandidates)
	if err != nil {
		return err
	}
	var actualSourceFingerprint, actualJobFingerprint, actualContext, trigger, status, createdAt string
	var cursor, nextAttempt, firstMessage, firstClass, firstStage, firstAt, lastMessage, lastClass, lastStage, lastAt, abandonedAt, abandonedBy, abandonReason, startedAt, finishedAt sql.NullString
	var total, processed, promoted, skippedIneligible, skippedMissingLabels, skippedIncompleteSources, staleConflicts, attempts, retries, consecutive, recovery int
	if err = db.QueryRow(`SELECT source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,cursor_alert_id,
       total_candidates,processed_candidates,promoted_candidates,skipped_ineligible,skipped_missing_labels,
       skipped_incomplete_sources,stale_conflicts,attempt_count,retry_count,consecutive_retry_count,next_attempt_at,
       first_error_message,first_error_class,first_error_stage,first_error_at,last_error_message,last_error_class,
       last_error_stage,last_error_at,recovery_count,abandoned_at,abandoned_by,abandon_reason,created_at,started_at,finished_at
FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(
		&actualSourceFingerprint, &actualJobFingerprint, &actualContext, &trigger, &status, &cursor,
		&total, &processed, &promoted, &skippedIneligible, &skippedMissingLabels, &skippedIncompleteSources,
		&staleConflicts, &attempts, &retries, &consecutive, &nextAttempt, &firstMessage, &firstClass,
		&firstStage, &firstAt, &lastMessage, &lastClass, &lastStage, &lastAt, &recovery, &abandonedAt,
		&abandonedBy, &abandonReason, &createdAt, &startedAt, &finishedAt,
	); err != nil {
		return err
	}
	wantAttempts := (len(expectedCandidates) + 19) / 20
	wantPromoted, wantIneligible, wantStale, wantIncomplete := problem5ExpectedOutcomeCounts(expectedCandidates, snapshots)
	wantCreatedAt := now.Add(time.Minute).UTC().Format(time.RFC3339Nano)
	wantWorkerAt := now.Add(5 * time.Minute).UTC().Format(time.RFC3339Nano)
	wantCursor := expectedCandidates[len(expectedCandidates)-1].AlertID
	if actualSourceFingerprint != sourceFingerprint || actualJobFingerprint != expectedJobFingerprint || actualContext != contextJSON || trigger != "source_context_changed" || status != "completed" || !cursor.Valid || cursor.String != wantCursor || total != len(expectedCandidates) || processed != total || promoted != wantPromoted || skippedIneligible != wantIneligible || skippedMissingLabels != 0 || skippedIncompleteSources != wantIncomplete || staleConflicts != wantStale || attempts != wantAttempts || retries != 0 || consecutive != 0 || recovery != 0 || createdAt != wantCreatedAt || !replayNullStringEqual(startedAt, wantWorkerAt) || !replayNullStringEqual(finishedAt, wantWorkerAt) || nextAttempt.Valid || firstMessage.Valid || firstClass.Valid || firstStage.Valid || firstAt.Valid || lastMessage.Valid || lastClass.Valid || lastStage.Valid || lastAt.Valid || abandonedAt.Valid || abandonedBy.Valid || abandonReason.Valid {
		return fmt.Errorf("independent final source job mismatch id=%d status=%q total=%d/%d processed=%d counters=%d/%d/%d/%d attempts=%d/%d cursor=%q/%q", jobID, status, total, len(expectedCandidates), processed, promoted, skippedIneligible, staleConflicts, skippedIncompleteSources, attempts, wantAttempts, cursor.String, wantCursor)
	}
	rows, err := db.Query(`SELECT job_id,alert_id,target_key,expected_alert_last_seen_at,expected_target_last_seen_at,match_labels_json,relevant_sources_json,target_source_members_json,processed_at,outcome FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`, jobID)
	if err != nil {
		return err
	}
	expectedByKey := expectedCandidatesByKey(expectedCandidates)
	seen := make(map[string]bool, len(expectedByKey))
	for rows.Next() {
		var actualJobID int64
		var alertID, targetKey string
		var expectedAlert, expectedTarget, matchLabels, relevantSources, targetMembers, processedAt, outcome sql.NullString
		if err = rows.Scan(&actualJobID, &alertID, &targetKey, &expectedAlert, &expectedTarget, &matchLabels, &relevantSources, &targetMembers, &processedAt, &outcome); err != nil {
			rows.Close()
			return err
		}
		key := alertID + "\x00" + targetKey
		want, ok := expectedByKey[key]
		index := problem5ExpectedCandidateIndex(alertID, targetKey, expectedCandidates)
		wantProcessed := now.Add(5 * time.Minute).UTC().Format(time.RFC3339Nano)
		wantOutcome := ""
		if index >= 0 {
			wantOutcome = problem5ExpectedCandidateOutcome(index, alertID, targetKey, snapshots)
		}
		if !ok || seen[key] || actualJobID != int64(jobID) || !problem5NullableEqual(expectedAlert, want.ExpectedAlertLastSeenAt) || !problem5NullableEqual(expectedTarget, want.ExpectedTargetLastSeenAt) || !problem5NullableEqual(matchLabels, want.MatchLabelsJSON) || !replayNullStringEqual(relevantSources, want.RelevantSourcesJSON) || !problem5NullableEqual(targetMembers, want.TargetSourceMembersJSON) || !replayNullStringEqual(processedAt, wantProcessed) || !replayNullStringEqual(outcome, wantOutcome) {
			rows.Close()
			return fmt.Errorf("independent final source candidate mismatch key=%q processed=%q/%q outcome=%q/%q", key, processedAt.String, wantProcessed, outcome.String, wantOutcome)
		}
		seen[key] = true
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(seen) != len(expectedByKey) {
		return fmt.Errorf("independent final source candidate coverage=%d want=%d", len(seen), len(expectedByKey))
	}
	return nil
}

func problem5ExpectedJobFingerprint(contextJSON, sourceFingerprint string, rules []problem5ControlledRule, candidates []problem5IndependentCandidateInput) (string, error) {
	expectedRules := make([]problem5IndependentRuleInput, 0, len(rules))
	for _, rule := range rules {
		expectedRules = append(expectedRules, problem5IndependentRuleInput{RuleID: rule.RuleID, RevisionID: rule.RevisionID, SourceName: rule.SourceName, ClusterID: rule.ClusterID, TargetKey: rule.TargetKey, SilenceID: rule.SilenceID, ContentFingerprint: rule.ContentFingerprint, ObservedState: rule.ObservedState, EvaluationFingerprint: rule.EvaluationFingerprint, MatchersJSON: rule.MatchersJSON})
	}
	sort.Slice(expectedRules, func(i, j int) bool {
		if expectedRules[i].SourceName != expectedRules[j].SourceName {
			return expectedRules[i].SourceName < expectedRules[j].SourceName
		}
		if expectedRules[i].ClusterID != expectedRules[j].ClusterID {
			return expectedRules[i].ClusterID < expectedRules[j].ClusterID
		}
		if expectedRules[i].TargetKey != expectedRules[j].TargetKey {
			return expectedRules[i].TargetKey < expectedRules[j].TargetKey
		}
		return expectedRules[i].SilenceID < expectedRules[j].SilenceID
	})
	raw, err := json.Marshal(problem5IndependentJobInput{TriggerType: "source_context_changed", SourceContext: json.RawMessage(contextJSON), SourceContextFingerprint: sourceFingerprint, Rules: expectedRules, Candidates: candidates})
	if err != nil {
		return "", err
	}
	return problem5Fingerprint(raw), nil
}

func problem5ExpectedOutcomeCounts(candidates []problem5IndependentCandidateInput, snapshots []alertlifecycle.LiveAlertSnapshot) (promoted, ineligible, stale, incomplete int) {
	for index, candidate := range candidates {
		switch problem5ExpectedCandidateOutcome(index, candidate.AlertID, candidate.TargetKey, snapshots) {
		case "promoted":
			promoted++
		case "stale_alert_observation", "stale_target_observation":
			stale++
		case "incomplete_source_coverage":
			incomplete++
		default:
			ineligible++
		}
	}
	return promoted, ineligible, stale, incomplete
}

func assertProblem5IndependentFinalModel(db *sql.DB, snapshots []alertlifecycle.LiveAlertSnapshot, resolvedID, matcherValue string, sourceJobID, successorJobID, retryJobID, abandonJobID int) error {
	expectedAlerts := make(map[string]alertlifecycle.LiveAlertSnapshot, len(snapshots)+3)
	for _, snapshot := range snapshots {
		expectedAlerts[snapshot.AlertID] = snapshot
	}
	for _, alertID := range []string{"problem5-failed-context", "problem5-recorded-retry-alert", "problem5-recorded-abandon-alert"} {
		if _, exists := expectedAlerts[alertID]; exists {
			return fmt.Errorf("synthetic alert id collides with recorded input: %s", alertID)
		}
		expectedAlerts[alertID] = alertlifecycle.LiveAlertSnapshot{AlertID: alertID}
	}
	rows, err := db.Query(`SELECT alert_id,state,raw_state,is_live,is_suppressed,is_resolved FROM alert_states ORDER BY alert_id`)
	if err != nil {
		return err
	}
	seenAlerts := make(map[string]bool, len(expectedAlerts))
	for rows.Next() {
		var alertID, state, rawState string
		var live, suppressed, resolved int
		if err = rows.Scan(&alertID, &state, &rawState, &live, &suppressed, &resolved); err != nil {
			rows.Close()
			return err
		}
		snapshot, ok := expectedAlerts[alertID]
		if !ok || seenAlerts[alertID] {
			rows.Close()
			return fmt.Errorf("final alert row %q is not in the controlled model", alertID)
		}
		seenAlerts[alertID] = true
		wantResolved := alertID == resolvedID
		wantSuppressed := !wantResolved && (snapshot.State == alertlifecycle.StateSuppressed || replaySnapshotInstance(snapshot) == matcherValue)
		if live != 0 || resolved != boolInt(wantResolved) || suppressed != boolInt(wantSuppressed) {
			rows.Close()
			return fmt.Errorf("final alert %q state=%s raw=%s live=%d suppressed=%d resolved=%d want suppressed=%t resolved=%t", alertID, state, rawState, live, suppressed, resolved, wantSuppressed, wantResolved)
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(seenAlerts) != len(expectedAlerts) {
		return fmt.Errorf("final alert model coverage=%d want=%d", len(seenAlerts), len(expectedAlerts))
	}
	expectedTargets := make(map[string]bool)
	for _, snapshot := range snapshots {
		for _, target := range snapshot.Targets {
			expectedTargets[snapshot.AlertID+"\x00"+target.Target] = true
		}
	}
	targetRows, err := db.Query(`SELECT alert_id,target,is_live,is_suppressed,is_resolved FROM alert_target_states ORDER BY alert_id,target`)
	if err != nil {
		return err
	}
	seenTargets := make(map[string]bool, len(expectedTargets))
	for targetRows.Next() {
		var alertID, targetKey string
		var live, suppressed, resolved int
		if err = targetRows.Scan(&alertID, &targetKey, &live, &suppressed, &resolved); err != nil {
			targetRows.Close()
			return err
		}
		key := alertID + "\x00" + targetKey
		if !expectedTargets[key] || seenTargets[key] {
			targetRows.Close()
			return fmt.Errorf("final target row %q is not in the controlled model", key)
		}
		seenTargets[key] = true
		if live != 0 || resolved != boolInt(alertID == resolvedID) {
			targetRows.Close()
			return fmt.Errorf("final target row %q has live=%d resolved=%d", key, live, resolved)
		}
	}
	if err = targetRows.Err(); err != nil {
		targetRows.Close()
		return err
	}
	targetRows.Close()
	if len(seenTargets) != len(expectedTargets) {
		return fmt.Errorf("final target model coverage=%d want=%d", len(seenTargets), len(expectedTargets))
	}
	var eventCount, resolvedEventCount int
	if err = db.QueryRow(`SELECT COUNT(*) FROM alert_events`).Scan(&eventCount); err != nil {
		return err
	}
	if err = db.QueryRow(`SELECT COUNT(*) FROM alert_events WHERE alert_id=? AND changed_by='problem5-operator' AND new_is_resolved=1`, resolvedID).Scan(&resolvedEventCount); err != nil {
		return err
	}
	if eventCount == 0 || resolvedEventCount != 1 {
		return fmt.Errorf("final event model count=%d resolved_operator=%d", eventCount, resolvedEventCount)
	}
	for _, jobID := range []int{sourceJobID, successorJobID, retryJobID, abandonJobID} {
		var count int
		if err = db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=?`, jobID).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("job %d has no durable attempt rows", jobID)
		}
	}
	return nil
}

func assertProblem5IndependentFullAlertModel(db *sql.DB, snapshots []alertlifecycle.LiveAlertSnapshot, resolvedID, matcherValue string, now time.Time) error {
	expected := make(map[string]alertlifecycle.LiveAlertSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		expected[snapshot.AlertID] = snapshot
	}
	rows, err := db.Query(`SELECT alert_id,state,raw_state,is_live,is_suppressed,is_resolved,receiver,group_id,group_labels_json,labels_json,match_labels_json,annotations_json,starts_at,last_seen_at,relevant_upstreams_json,relevant_sources_json,source_links_json,suppression_observed_at,resolved_at,resolved_by,resolved_comment,updated_at FROM alert_states ORDER BY alert_id`)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(expected))
	for rows.Next() {
		var alertID, state, rawState, receiver, groupID, groupLabels, labels, annotations, startsAt, relevantUpstreams, relevantSources, sourceLinks, updatedAt string
		var matchLabels, lastSeen, suppressionObserved, resolvedAt, resolvedBy, resolvedComment sql.NullString
		var live, suppressed, resolved int
		if err = rows.Scan(&alertID, &state, &rawState, &live, &suppressed, &resolved, &receiver, &groupID, &groupLabels, &labels, &matchLabels, &annotations, &startsAt, &lastSeen, &relevantUpstreams, &relevantSources, &sourceLinks, &suppressionObserved, &resolvedAt, &resolvedBy, &resolvedComment, &updatedAt); err != nil {
			rows.Close()
			return err
		}
		snapshot, ok := expected[alertID]
		if !ok {
			syntheticErr := assertProblem5SyntheticAlertRow(alertID, state, rawState, live, suppressed, resolved, receiver, groupID, groupLabels, labels, matchLabels, annotations, startsAt, lastSeen, relevantUpstreams, relevantSources, sourceLinks, suppressionObserved, resolvedAt, resolvedBy, resolvedComment, updatedAt, now)
			if syntheticErr != nil {
				rows.Close()
				return syntheticErr
			}
			continue
		}
		if seen[alertID] {
			rows.Close()
			return fmt.Errorf("full final alert model unexpected row %q", alertID)
		}
		seen[alertID] = true
		wantResolved := alertID == resolvedID
		wantSuppressed := !wantResolved && (snapshot.State == alertlifecycle.StateSuppressed || replaySnapshotInstance(snapshot) == matcherValue)
		wantState := "inactive"
		if wantResolved {
			wantState = "resolved"
		} else if wantSuppressed {
			wantState = "suppressed"
		}
		wantMatchLabels := ""
		if snapshot.MatchLabelsJSON != nil {
			wantMatchLabels = problem5CanonicalJSONString(*snapshot.MatchLabelsJSON)
		}
		wantRelevantUpstreams, _ := json.Marshal(snapshot.RelevantUpstreams)
		wantRelevantSources, _ := json.Marshal(snapshot.RelevantSources)
		wantSourceLinks, _ := json.Marshal(snapshot.SourceLinks)
		if state != wantState || rawState != string(snapshot.State) || live != 0 || suppressed != boolInt(wantSuppressed) || resolved != boolInt(wantResolved) || receiver != snapshot.Receiver || groupID != snapshot.GroupID || groupLabels != snapshot.GroupLabelsJSON || labels != snapshot.LabelsJSON || (matchLabels.Valid != (snapshot.MatchLabelsJSON != nil)) || (matchLabels.Valid && matchLabels.String != wantMatchLabels) || annotations != snapshot.AnnotationsJSON || startsAt != snapshot.StartsAt.UTC().Format(time.RFC3339Nano) || !lastSeen.Valid || lastSeen.String != snapshot.LastSeenAt.UTC().Format(time.RFC3339Nano) || relevantUpstreams != problem5CanonicalStringSliceJSON(string(wantRelevantUpstreams)) || relevantSources != problem5CanonicalSourceReferencesJSON(string(wantRelevantSources)) || sourceLinks != string(wantSourceLinks) || strings.TrimSpace(updatedAt) == "" {
			rows.Close()
			return fmt.Errorf("full final alert row %q differs from controlled model state=%q/%q raw=%q/%q live=%d suppressed=%d resolved=%d", alertID, state, wantState, rawState, snapshot.State, live, suppressed, resolved)
		}
		if wantResolved {
			if !resolvedAt.Valid || resolvedBy.String != "problem5-operator" || resolvedComment.String != "recorded resolved" {
				rows.Close()
				return fmt.Errorf("full final resolved metadata alert=%q at=%q by=%q comment=%q", alertID, resolvedAt.String, resolvedBy.String, resolvedComment.String)
			}
		} else if resolvedAt.Valid || resolvedBy.Valid || resolvedComment.Valid {
			rows.Close()
			return fmt.Errorf("full final unresolved alert=%q has resolved metadata", alertID)
		}
		if wantSuppressed && !suppressionObserved.Valid {
			rows.Close()
			return fmt.Errorf("full final suppressed alert=%q has no suppression timestamp", alertID)
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(seen) != len(expected) {
		return fmt.Errorf("full final alert coverage=%d want=%d", len(seen), len(expected))
	}
	targetRows, err := db.Query(`SELECT alert_id,target,state,raw_state,is_live,is_suppressed,is_resolved,starts_at,last_seen_at,source_members_json,match_labels_fingerprint,retired_at,updated_at FROM alert_target_states ORDER BY alert_id,target`)
	if err != nil {
		return err
	}
	targetSeen := map[string]bool{}
	for targetRows.Next() {
		var alertID, target, state, rawState, startsAt, sourceMembers, fingerprint, updatedAt string
		var lastSeen, retiredAt sql.NullString
		var live, suppressed, resolved int
		if err = targetRows.Scan(&alertID, &target, &state, &rawState, &live, &suppressed, &resolved, &startsAt, &lastSeen, &sourceMembers, &fingerprint, &retiredAt, &updatedAt); err != nil {
			targetRows.Close()
			return err
		}
		key := alertID + "\x00" + target
		if targetSeen[key] {
			targetRows.Close()
			return fmt.Errorf("duplicate full final target row %q", key)
		}
		targetSeen[key] = true
		snapshot, ok := expected[alertID]
		if !ok {
			targetRows.Close()
			return fmt.Errorf("full final target row %q has no controlled alert", key)
		}
		var targetSnapshot *alertlifecycle.LiveTargetSnapshot
		for i := range snapshot.Targets {
			if snapshot.Targets[i].Target == target {
				targetSnapshot = &snapshot.Targets[i]
				break
			}
		}
		if targetSnapshot == nil {
			targetRows.Close()
			return fmt.Errorf("full final target row %q has no controlled target", key)
		}
		wantResolved := alertID == resolvedID
		wantSuppressed := !wantResolved && (snapshot.State == alertlifecycle.StateSuppressed || replaySnapshotInstance(snapshot) == matcherValue)
		wantState := "inactive"
		if wantResolved {
			wantState = "resolved"
		} else if wantSuppressed {
			wantState = "suppressed"
		}
		members, _ := json.Marshal(targetSnapshot.SourceNames)
		if state != wantState || rawState != string(targetSnapshot.State) || live != 0 || suppressed != boolInt(wantSuppressed) || resolved != boolInt(wantResolved) || startsAt != targetSnapshot.StartsAt.UTC().Format(time.RFC3339Nano) || !lastSeen.Valid || lastSeen.String < now.UTC().Format(time.RFC3339Nano) || sourceMembers != problem5CanonicalStringSliceJSON(string(members)) || strings.TrimSpace(fingerprint) == "" || strings.TrimSpace(updatedAt) == "" || retiredAt.Valid {
			targetRows.Close()
			return fmt.Errorf("full final target row %q differs state=%q/%q raw=%q/%q", key, state, wantState, rawState, targetSnapshot.State)
		}
	}
	if err = targetRows.Err(); err != nil {
		targetRows.Close()
		return err
	}
	targetRows.Close()
	return nil
}

func replaySnapshotInstance(snapshot alertlifecycle.LiveAlertSnapshot) string {
	var labels map[string]string
	if json.Unmarshal([]byte(snapshot.LabelsJSON), &labels) == nil {
		return labels["instance"]
	}
	return ""
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func createdAtUTCAtOrAfter(value, lower time.Time) bool {
	_, offset := value.UTC().Zone()
	return !value.IsZero() && offset == 0 && !value.Before(lower)
}

func assertReplayPartition(db *sql.DB, jobID int, resolvedID, refiredID, incompleteID string) error {
	rows, err := db.Query(`SELECT alert_id,outcome FROM silence_recheck_job_candidates WHERE job_id=? ORDER BY alert_id,target_key`, jobID)
	if err != nil {
		return err
	}
	defer rows.Close()
	validOutcomes := map[string]bool{
		"already_resolved": true, "stale_alert_observation": true, "stale_target_observation": true,
		"no_matching_rule": true,
	}
	seenOutcomes := map[string]bool{}
	for rows.Next() {
		var alertID, outcome string
		if err = rows.Scan(&alertID, &outcome); err != nil {
			return err
		}
		if alertID == incompleteID {
			if outcome != "incomplete_source_coverage" {
				return fmt.Errorf("incomplete scope %s outcome=%s", alertID, outcome)
			}
			continue
		}
		if !validOutcomes[outcome] {
			return fmt.Errorf("scope %s has unexpected partition outcome=%s", alertID, outcome)
		}
		seenOutcomes[outcome] = true
		if alertID == resolvedID && outcome != "already_resolved" {
			return fmt.Errorf("resolved scope %s outcome=%s", alertID, outcome)
		}
		if alertID == refiredID && outcome != "stale_alert_observation" && outcome != "stale_target_observation" {
			return fmt.Errorf("refired scope %s outcome=%s", alertID, outcome)
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if !seenOutcomes["no_matching_rule"] {
		return fmt.Errorf("recorded partition lacks the expected unmatched category: %v", seenOutcomes)
	}
	return nil
}

func assertReplaySuppressionRows(db *sql.DB) error {
	var history, members int
	if err := db.QueryRow(`SELECT COUNT(*) FROM alert_suppression_cause_history`).Scan(&history); err != nil {
		return err
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM alert_suppression_cause_members`).Scan(&members); err != nil {
		return err
	}
	if history == 0 || members == 0 {
		return fmt.Errorf("suppression evidence history=%d members=%d", history, members)
	}
	return nil
}

func assertProblem5ReplayFinalState(db *sql.DB, sourceJobID, successorJobID, retryJobID, abandonJobID, sourceTotal int) error {
	for _, want := range []struct {
		jobID    int
		status   string
		attempts int
		total    *int
	}{
		{sourceJobID, "completed", (sourceTotal + 19) / 20, &sourceTotal},
		{successorJobID, "completed", 1, nil},
		{retryJobID, "completed", 1 + (sourceTotal+1+19)/20, nil},
		{abandonJobID, "abandoned", 1, nil},
	} {
		var status string
		var attempts, total, processed int
		if err := db.QueryRow(`SELECT status,attempt_count,total_candidates,processed_candidates FROM silence_recheck_jobs WHERE job_id=?`, want.jobID).Scan(&status, &attempts, &total, &processed); err != nil {
			return err
		}
		processedValid := (status == "completed" && processed == total) || (status == "abandoned" && processed == 0)
		if status != want.status || attempts != want.attempts || !processedValid || (want.total != nil && total != *want.total) {
			return fmt.Errorf("job %d final status=%s attempts=%d total=%d processed=%d", want.jobID, status, attempts, total, processed)
		}
	}
	for _, want := range []struct {
		jobID     int
		requestID string
		action    string
		status    string
	}{
		{retryJobID, "problem5-recorded-retry", "retry", "queued"},
		{abandonJobID, "problem5-recorded-abandon", "abandon", "abandoned"},
	} {
		var requestID, action, appliedResult, resultingStatus string
		if err := db.QueryRow(`SELECT request_id,action,applied_result,resulting_status FROM silence_recheck_job_operator_actions WHERE job_id=? AND request_id=?`, want.jobID, want.requestID).Scan(&requestID, &action, &appliedResult, &resultingStatus); err != nil {
			return err
		}
		if requestID != want.requestID || action != want.action || appliedResult == "" || resultingStatus != want.status {
			return fmt.Errorf("job %d operator row request=%q action=%q applied=%q resulting=%q", want.jobID, requestID, action, appliedResult, resultingStatus)
		}
	}
	return nil
}

func assertProblem5StatsWarningResponses(statsBody, warningBody []byte) error {
	var stats map[string]any
	if err := json.Unmarshal(statsBody, &stats); err != nil {
		return fmt.Errorf("stats JSON: %w", err)
	}
	expectedStatsKeys := []string{"status", "storageEnabled", "sqlitePath", "retentionTTLHours", "maxAlerts", "currentRows", "stateCounts", "suppressedBreakdown", "historyRows", "groupingFingerprint", "regroupTargetFingerprint", "regroupStatus", "regroupError", "regroupProcessed", "regroupTotal", "dbExpectedState", "dbExpectedStateReason", "lastRegroupStartedAt", "lastRegroupFinishedAt", "fileSizeBytes", "pageCount", "pageSizeBytes", "freelistCount", "lastHousekeepingAt", "lastIncrementalVacuumAt", "silenceHistoryTTLHours", "silenceInconsistentClusters", "recheck"}
	if len(stats) != len(expectedStatsKeys) {
		return fmt.Errorf("stats top-level fields=%v", stats)
	}
	for _, key := range expectedStatsKeys {
		if _, exists := stats[key]; !exists {
			return fmt.Errorf("stats missing exact field %q", key)
		}
	}
	for _, key := range []string{"status", "sqlitePath", "groupingFingerprint", "regroupTargetFingerprint", "regroupStatus", "regroupError", "dbExpectedStateReason"} {
		if _, ok := stats[key].(string); !ok {
			return fmt.Errorf("stats field %s has wrong type/value=%v", key, stats[key])
		}
	}
	for _, key := range []string{"storageEnabled", "dbExpectedState"} {
		if _, ok := stats[key].(bool); !ok {
			return fmt.Errorf("stats field %s has wrong type/value=%v", key, stats[key])
		}
	}
	for _, key := range []string{"retentionTTLHours", "maxAlerts", "currentRows", "historyRows", "regroupProcessed", "regroupTotal", "fileSizeBytes", "pageCount", "pageSizeBytes", "freelistCount", "silenceHistoryTTLHours"} {
		if _, ok := stats[key].(float64); !ok {
			return fmt.Errorf("stats numeric field %s has wrong type/value=%v", key, stats[key])
		}
	}
	for _, key := range []string{"lastRegroupStartedAt", "lastRegroupFinishedAt", "lastHousekeepingAt", "lastIncrementalVacuumAt"} {
		if value, ok := stats[key]; !ok || (value != nil && !isProblem5String(value)) {
			return fmt.Errorf("stats nullable timestamp field %s has wrong type/value=%v", key, value)
		}
	}
	for _, key := range []string{"stateCounts", "suppressedBreakdown"} {
		values, ok := stats[key].(map[string]any)
		if !ok {
			return fmt.Errorf("stats map field %s has wrong type/value=%v", key, stats[key])
		}
		for name, value := range values {
			if _, ok = value.(float64); !ok {
				return fmt.Errorf("stats map field %s[%s] has wrong type/value=%v", key, name, value)
			}
		}
	}
	clusters, ok := stats["silenceInconsistentClusters"].([]any)
	if !ok {
		return fmt.Errorf("stats silenceInconsistentClusters has wrong type/value=%v", stats["silenceInconsistentClusters"])
	}
	for _, cluster := range clusters {
		if !isProblem5String(cluster) {
			return fmt.Errorf("stats silenceInconsistentClusters item has wrong type/value=%v", cluster)
		}
	}
	if stats["status"] != "ok" || stats["storageEnabled"] != true || stats["dbExpectedState"] != false {
		return fmt.Errorf("stats envelope=%v", stats)
	}
	recheck, ok := stats["recheck"].(map[string]any)
	if !ok {
		return fmt.Errorf("stats has no typed recheck object: %s", statsBody)
	}
	if len(recheck) != 2 {
		return fmt.Errorf("stats recheck fields=%v", recheck)
	}
	if _, exists := recheck["queueDepth"]; !exists {
		return errors.New("stats recheck missing queueDepth")
	}
	if _, exists := recheck["headJob"]; !exists {
		return errors.New("stats recheck missing headJob")
	}
	if recheck["queueDepth"] != float64(0) {
		return fmt.Errorf("stats queueDepth=%v want 0", recheck["queueDepth"])
	}
	if head, present := recheck["headJob"]; present && head != nil {
		return fmt.Errorf("stats headJob=%v want null", head)
	}
	var warning map[string]any
	if err := json.Unmarshal(warningBody, &warning); err != nil {
		return fmt.Errorf("warning JSON: %w", err)
	}
	if len(warning) != 4 || warning["status"] != "ok" || warning["storageEnabled"] != true || warning["dbExpectedState"] != false {
		return fmt.Errorf("warning envelope=%v", warning)
	}
	for _, key := range []string{"status", "storageEnabled", "dbExpectedState", "issues"} {
		if _, exists := warning[key]; !exists {
			return fmt.Errorf("warning missing exact field %q", key)
		}
	}
	issues, ok := warning["issues"].([]any)
	if !ok || len(issues) != 1 {
		return fmt.Errorf("warning issues=%v want one regroup_out_of_date issue", warning["issues"])
	}
	issue, ok := issues[0].(map[string]any)
	if !ok || len(issue) != 4 || issue["kind"] != "regroup_out_of_date" || issue["severity"] != "warning" || issue["summary"] != "Alert lifecycle grouping is out of date" {
		return fmt.Errorf("warning issue=%v", issues[0])
	}
	metadata, ok := issue["metadata"].(map[string]any)
	expectedFingerprint := alertlifecycle.BuildGroupingFingerprint(alertlifecycle.GroupingConfig{
		LabelsKeep: append([]string{}, config.Config.Labels.Keep...), LabelsKeepRegex: append([]string{}, config.Config.Labels.KeepRegex...),
		LabelsStrip: append([]string{}, config.Config.Labels.Strip...), LabelsStripRegex: append([]string{}, config.Config.Labels.StripRegex...),
	})
	expectedMetadata := map[string]any{"storedFingerprint": "", "expectedFingerprint": expectedFingerprint}
	if !ok || !reflect.DeepEqual(metadata, expectedMetadata) {
		return fmt.Errorf("warning metadata=%v", issue["metadata"])
	}
	for _, body := range [][]byte{statsBody, warningBody} {
		text := string(body)
		if strings.Contains(text, "problem5 recorded") || strings.Contains(text, "scheduler sqlite error") {
			return fmt.Errorf("HTTP observability leaked test/storage diagnostics: %s", body)
		}
	}
	return nil
}

func isProblem5String(value any) bool {
	_, ok := value.(string)
	return ok
}

func replayStringPtr(value string) *string { return &value }

func replayAlertState(raw string) models.AlertState {
	switch raw {
	case "active":
		return models.AlertStateActive
	case "suppressed":
		return models.AlertStateSuppressed
	default:
		return models.AlertStateUnprocessed
	}
}
