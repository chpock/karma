package alertlifecycle

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSilenceEpisodeRevisionSequenceProduct(t *testing.T) {
	contentRelations := []string{"same", "changed", "returned"}
	terminalEvents := []string{"present", "partial-absence", "complete-absence", "reappear"}
	for _, contentRelation := range contentRelations {
		for _, terminalEvent := range terminalEvents {
			t.Run(contentRelation+"/"+terminalEvent, func(t *testing.T) {
				store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()

				now := time.Now().UTC().Truncate(time.Millisecond)
				silence := SourceSilenceSnapshot{
					SilenceID: "episode-product", Status: "active",
					StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
					Matchers: []SilenceMatcherSnapshot{{Name: "service", Value: "api", IsEqual: true}},
				}
				source := func(name, status string, present bool) SourceSilenceState {
					out := SourceSilenceState{SourceName: name, ClusterID: "cluster-ha", SyncStatus: status}
					if present {
						out.Silences = []SourceSilenceSnapshot{silence}
					}
					return out
				}
				apply := func(at time.Time, a, b SourceSilenceState) {
					t.Helper()
					_, applyErr := store.ApplyPollCycle(PollCycleInput{
						SuccessfulUpstreams: map[string]bool{"am-a": a.SyncStatus == "success", "am-b": b.SyncStatus == "success"},
						SilenceSources:      []SourceSilenceState{a, b}, Timestamp: at,
					})
					if applyErr != nil {
						t.Fatal(applyErr)
					}
				}

				apply(now, source("am-a", "success", true), source("am-b", "success", true))
				wantRevisions := 1
				if contentRelation == "changed" || contentRelation == "returned" {
					silence.Matchers[0].Value = "payments"
					apply(now.Add(time.Minute), source("am-a", "success", true), source("am-b", "success", true))
					wantRevisions++
				}
				if contentRelation == "returned" {
					silence.Matchers[0].Value = "api"
					apply(now.Add(2*time.Minute), source("am-a", "success", true), source("am-b", "success", true))
					wantRevisions++
				}

				at := now.Add(3 * time.Minute)
				switch terminalEvent {
				case "present":
					apply(at, source("am-a", "success", true), source("am-b", "success", true))
				case "partial-absence":
					apply(at, source("am-a", "success", false), source("am-b", "failed", false))
				case "complete-absence", "reappear":
					apply(at, source("am-a", "success", false), source("am-b", "success", false))
				}
				if terminalEvent == "reappear" {
					apply(at.Add(time.Minute), source("am-a", "success", true), source("am-b", "success", true))
					wantRevisions++
				}

				var revisions, episodes, openEpisodes int
				if err = store.db.QueryRow(`SELECT COUNT(*) FROM silence_revisions`).Scan(&revisions); err != nil {
					t.Fatal(err)
				}
				if err = store.db.QueryRow(`SELECT COUNT(*), COUNT(open_marker) FROM silence_rule_episodes`).Scan(&episodes, &openEpisodes); err != nil {
					t.Fatal(err)
				}
				wantEpisodes, wantOpen := 1, 1
				if terminalEvent == "complete-absence" {
					wantOpen = 0
				}
				if terminalEvent == "reappear" {
					wantEpisodes = 2
				}
				if revisions != wantRevisions || episodes != wantEpisodes || openEpisodes != wantOpen {
					t.Fatalf("revisions=%d/%d episodes=%d/%d open=%d/%d", revisions, wantRevisions, episodes, wantEpisodes, openEpisodes, wantOpen)
				}
			})
		}
	}
}
