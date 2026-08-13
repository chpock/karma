package main

import (
	"fmt"
	"testing"
	"time"
)

func TestC8PreviewCacheStateProduct(t *testing.T) {
	states := []string{"current", "stale", "missing"}
	for _, state := range states {
		for _, fingerprintMatches := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fingerprint=%v", state, fingerprintMatches), func(t *testing.T) {
				resetSilenceEditorPreviewSnapshots()
				defer resetSilenceEditorPreviewSnapshots()
				snapshot := newSilenceEditorPreviewSnapshot("expected", []silenceEditorPreviewAlert{{ID: "alert"}}, 0)
				if state != "missing" {
					if state == "stale" {
						snapshot.CreatedAt = time.Now().UTC().Add(-silenceEditorPreviewSnapshotTTL - time.Second)
					}
					if !putSilenceEditorPreviewSnapshot(snapshot) {
						t.Fatal("snapshot rejected")
					}
				}
				fingerprint := "other"
				if fingerprintMatches {
					fingerprint = "expected"
				}
				_, ok := getSilenceEditorPreviewSnapshot(snapshot.ID, fingerprint)
				want := state == "current" && fingerprintMatches
				if ok != want {
					t.Fatalf("cache hit=%v want=%v", ok, want)
				}
			})
		}
	}
}

func TestSilenceEditorPreviewSnapshotCacheIsBounded(t *testing.T) {
	resetSilenceEditorPreviewSnapshots()
	t.Cleanup(resetSilenceEditorPreviewSnapshots)

	ids := make([]string, 0, silenceEditorPreviewSnapshotMaxEntries+1)
	for i := 0; i < silenceEditorPreviewSnapshotMaxEntries+1; i++ {
		snapshot := newSilenceEditorPreviewSnapshot(
			fmt.Sprintf("fingerprint-%d", i),
			[]silenceEditorPreviewAlert{{ID: fmt.Sprintf("alert-%d", i)}},
			0,
		)
		ids = append(ids, snapshot.ID)
		if !putSilenceEditorPreviewSnapshot(snapshot) {
			t.Fatalf("expected snapshot %d to fit cache", i)
		}
	}

	if _, ok := getSilenceEditorPreviewSnapshot(ids[0], "fingerprint-0"); ok {
		t.Fatal("expected oldest snapshot to be evicted")
	}
	if _, ok := getSilenceEditorPreviewSnapshot(ids[len(ids)-1], fmt.Sprintf("fingerprint-%d", len(ids)-1)); !ok {
		t.Fatal("expected newest snapshot to remain cached")
	}
}

func TestSilenceEditorPreviewSnapshotRequiresMatchingFingerprint(t *testing.T) {
	resetSilenceEditorPreviewSnapshots()
	t.Cleanup(resetSilenceEditorPreviewSnapshots)

	snapshot := newSilenceEditorPreviewSnapshot("expected", []silenceEditorPreviewAlert{{ID: "alert"}}, 2)
	if !putSilenceEditorPreviewSnapshot(snapshot) {
		t.Fatal("expected snapshot to fit cache")
	}
	if _, ok := getSilenceEditorPreviewSnapshot(snapshot.ID, "other"); ok {
		t.Fatal("expected fingerprint mismatch to reject snapshot")
	}
	got, ok := getSilenceEditorPreviewSnapshot(snapshot.ID, "expected")
	if !ok || got.ExcludedNoRawLabels != 2 {
		t.Fatalf("unexpected cached snapshot: got=%+v ok=%v", got, ok)
	}
}

func TestSilenceEditorPreviewSnapshotRejectsOversizedSingleEntry(t *testing.T) {
	resetSilenceEditorPreviewSnapshots()
	t.Cleanup(resetSilenceEditorPreviewSnapshots)

	snapshot := newSilenceEditorPreviewSnapshot(
		"oversized",
		make([]silenceEditorPreviewAlert, silenceEditorPreviewSnapshotMaxRecords+1),
		0,
	)
	if putSilenceEditorPreviewSnapshot(snapshot) {
		t.Fatal("expected oversized snapshot to be rejected")
	}
	if _, ok := getSilenceEditorPreviewSnapshot(snapshot.ID, snapshot.Fingerprint); ok {
		t.Fatal("oversized snapshot must not be retained")
	}
}
