package alertlifecycle

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkTargetLabelValueStats100k(b *testing.B) {
	store, err := OpenStore(StoreOptions{Path: filepath.Join(b.TempDir(), "benchmark.sqlite")})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	_, _ = store.db.Exec(`PRAGMA foreign_keys=OFF`)
	tx, err := store.db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.Prepare(`
INSERT INTO alert_target_match_labels(alert_id, target, label_name, label_value, updated_at)
VALUES(?, 'cluster-a', 'service', ?, ?)
`)
	if err != nil {
		b.Fatal(err)
	}
	targetStmt, err := tx.Prepare(`
INSERT INTO alert_target_states(
  alert_id, target, state, raw_state, is_live, is_suppressed, is_resolved,
  starts_at, last_seen_at, source_members_json, updated_at
) VALUES(?, 'cluster-a', 'active', 'active', 1, 0, 0, ?, ?, '["am-a"]', ?)
`)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i := 0; i < 100000; i++ {
		alertID := fmt.Sprintf("alert-%06d", i)
		if _, err = targetStmt.Exec(alertID, now, now, now); err != nil {
			b.Fatal(err)
		}
		if _, err = stmt.Exec(alertID, fmt.Sprintf("service-%03d", i%500), now); err != nil {
			b.Fatal(err)
		}
	}
	_ = stmt.Close()
	_ = targetStmt.Close()
	if err = tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.Run("indexed", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			values, total, queryErr := store.ListTargetLabelValues("cluster-a", "service", "service-", 51)
			if queryErr != nil || len(values) != 51 || total != 500 {
				b.Fatalf("values=%d total=%d err=%v", len(values), total, queryErr)
			}
		}
	})
	b.Run("one-live-overlay", func(b *testing.B) {
		overlay := []TargetLabelOverlay{{
			AlertID: "alert-000000", Labels: map[string]string{"service": "live-only"}, MatchLabelsFingerprint: "changed",
		}}
		for i := 0; i < b.N; i++ {
			values, total, queryErr := store.ListTargetLabelValuesWithLive("cluster-a", "service", "", 51, overlay)
			if queryErr != nil || len(values) != 51 || total != 501 {
				b.Fatalf("values=%d total=%d err=%v", len(values), total, queryErr)
			}
		}
	})
}
