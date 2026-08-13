package alertlifecycle

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestC8TargetPreviewAndAutocompleteVisibilityProduct(t *testing.T) {
	projections := []string{"current", "retired"}
	for _, projection := range projections {
		for _, live := range []bool{false, true} {
			for _, indexed := range []bool{false, true} {
				t.Run(projection+"/live="+boolName(live)+"/indexed="+boolName(indexed), func(t *testing.T) {
					store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
					if err != nil {
						t.Fatal(err)
					}
					defer store.Close()
					now := time.Now().UTC().Truncate(time.Millisecond)
					labels := `{"service":"api"}`
					if err = store.UpsertCurrentAlert(AlertCurrent{
						AlertID: "matrix-alert", State: StateInactive, RawState: StateActive,
						IsLive: false, Receiver: "team", GroupID: "group", GroupLabelsJSON: `{}`,
						LabelsJSON: labels, MatchLabelsJSON: &labels, AnnotationsJSON: `{}`,
						StartsAt: now, LastSeenAt: &now, RelevantUpstreams: `["am-a"]`,
						RelevantSources: `[{"name":"am-a","cluster":"cluster-a"}]`,
					}); err != nil {
						t.Fatal(err)
					}
					tx, err := store.db.BeginTx(context.Background(), nil)
					if err != nil {
						t.Fatal(err)
					}
					row := targetStateRow{
						Target: "cluster-a", State: StateInactive, RawState: StateActive,
						IsLive: live, StartsAt: now, LastSeenAt: &now, SourceMembers: []string{"am-a"},
					}
					if projection == "retired" {
						row.RetiredAt = &now
					}
					if err = upsertTargetStateTx(tx, "matrix-alert", row, "matrix", now); err != nil {
						_ = tx.Rollback()
						t.Fatal(err)
					}
					if !indexed {
						if _, err = tx.Exec(`DELETE FROM alert_target_match_labels WHERE alert_id='matrix-alert'`); err != nil {
							_ = tx.Rollback()
							t.Fatal(err)
						}
					}
					if err = tx.Commit(); err != nil {
						t.Fatal(err)
					}

					preview, err := store.ListTargetAlertsForPreview("cluster-a")
					if err != nil {
						t.Fatal(err)
					}
					wantPreview := projection != "retired"
					if (len(preview) == 1) != wantPreview {
						t.Fatalf("preview rows=%d wantVisible=%v", len(preview), wantPreview)
					}
					names, total, err := store.ListTargetLabelNames("cluster-a", "", 10)
					if err != nil {
						t.Fatal(err)
					}
					wantIndexed := projection == "current" && indexed
					if (len(names) == 1 && total == 1) != wantIndexed {
						t.Fatalf("autocomplete names=%+v total=%d wantIndexed=%v", names, total, wantIndexed)
					}
				})
			}
		}
	}
}
