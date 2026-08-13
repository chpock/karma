package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	runtimeLog "github.com/prymitive/karma/internal/log"
)

const (
	silenceEditorPreviewSnapshotTTL        = 2 * time.Minute
	silenceEditorPreviewSnapshotMaxEntries = 8
	silenceEditorPreviewSnapshotMaxRecords = 100000
)

type silenceEditorPreviewSnapshot struct {
	ID                  string
	Fingerprint         string
	Alerts              []silenceEditorPreviewAlert
	ExcludedNoRawLabels int
	CreatedAt           time.Time
	lastAccessedAt      time.Time
}

var silenceEditorPreviewSnapshots = struct {
	sync.Mutex
	entries map[string]*silenceEditorPreviewSnapshot
	records int
}{entries: map[string]*silenceEditorPreviewSnapshot{}}

var silenceEditorPreviewSnapshotSequence atomic.Uint64

func newSilenceEditorPreviewSnapshot(fingerprint string, alerts []silenceEditorPreviewAlert, excludedNoRawLabels int) *silenceEditorPreviewSnapshot {
	now := time.Now().UTC()
	sequence := silenceEditorPreviewSnapshotSequence.Add(1)
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", fingerprint, now.UnixNano(), sequence)))
	return &silenceEditorPreviewSnapshot{
		ID: hex.EncodeToString(hash[:16]), Fingerprint: fingerprint, Alerts: alerts,
		ExcludedNoRawLabels: excludedNoRawLabels,
		CreatedAt:           now, lastAccessedAt: now,
	}
}

func putSilenceEditorPreviewSnapshot(snapshot *silenceEditorPreviewSnapshot) bool {
	return putSilenceEditorPreviewSnapshotContext(context.Background(), snapshot)
}

func putSilenceEditorPreviewSnapshotContext(ctx context.Context, snapshot *silenceEditorPreviewSnapshot) bool {
	if snapshot == nil || len(snapshot.Alerts) > silenceEditorPreviewSnapshotMaxRecords {
		count := 0
		if snapshot != nil {
			count = len(snapshot.Alerts)
		}
		runtimeLog.WarnContext(ctx, "silence_preview", "preview_snapshot_rejected", "Silence preview snapshot exceeds the per-snapshot limit", slog.Int("record_count", count), slog.Int("record_limit", silenceEditorPreviewSnapshotMaxRecords))
		return false
	}
	silenceEditorPreviewSnapshots.Lock()
	defer silenceEditorPreviewSnapshots.Unlock()
	pruneSilenceEditorPreviewSnapshotsLocked(ctx, time.Now().UTC())
	silenceEditorPreviewSnapshots.entries[snapshot.ID] = snapshot
	silenceEditorPreviewSnapshots.records += len(snapshot.Alerts)
	runtimeLog.DebugContext(ctx, "silence_preview", "preview_snapshot_stored", "Silence preview snapshot stored", slog.String("snapshot_id", snapshot.ID), slog.Int("record_count", len(snapshot.Alerts)), slog.Int("cache_entries", len(silenceEditorPreviewSnapshots.entries)), slog.Int("cache_records", silenceEditorPreviewSnapshots.records))
	for len(silenceEditorPreviewSnapshots.entries) > silenceEditorPreviewSnapshotMaxEntries ||
		silenceEditorPreviewSnapshots.records > silenceEditorPreviewSnapshotMaxRecords {
		evictOldestSilenceEditorPreviewSnapshotLocked(ctx)
	}
	return true
}

func getSilenceEditorPreviewSnapshot(id, fingerprint string) (*silenceEditorPreviewSnapshot, bool) {
	return getSilenceEditorPreviewSnapshotContext(context.Background(), id, fingerprint)
}

func getSilenceEditorPreviewSnapshotContext(ctx context.Context, id, fingerprint string) (*silenceEditorPreviewSnapshot, bool) {
	silenceEditorPreviewSnapshots.Lock()
	defer silenceEditorPreviewSnapshots.Unlock()
	now := time.Now().UTC()
	pruneSilenceEditorPreviewSnapshotsLocked(ctx, now)
	snapshot, ok := silenceEditorPreviewSnapshots.entries[id]
	if !ok || snapshot.Fingerprint != fingerprint {
		runtimeLog.DebugContext(ctx, "silence_preview", "preview_snapshot_miss", "Silence preview snapshot was not found or did not match", slog.String("snapshot_id", id))
		return nil, false
	}
	snapshot.lastAccessedAt = now
	runtimeLog.DebugContext(ctx, "silence_preview", "preview_snapshot_hit", "Silence preview snapshot cache hit", slog.String("snapshot_id", id), slog.Int("record_count", len(snapshot.Alerts)))
	return snapshot, true
}

func pruneSilenceEditorPreviewSnapshotsLocked(ctx context.Context, now time.Time) {
	for id, snapshot := range silenceEditorPreviewSnapshots.entries {
		if now.Sub(snapshot.CreatedAt) <= silenceEditorPreviewSnapshotTTL {
			continue
		}
		delete(silenceEditorPreviewSnapshots.entries, id)
		silenceEditorPreviewSnapshots.records -= len(snapshot.Alerts)
		runtimeLog.DebugContext(ctx, "silence_preview", "preview_snapshot_expired", "Silence preview snapshot expired", slog.String("snapshot_id", id), slog.Duration("age", now.Sub(snapshot.CreatedAt)), slog.Int("record_count", len(snapshot.Alerts)))
	}
}

func evictOldestSilenceEditorPreviewSnapshotLocked(ctx context.Context) {
	var oldest *silenceEditorPreviewSnapshot
	for _, snapshot := range silenceEditorPreviewSnapshots.entries {
		if oldest == nil || snapshot.lastAccessedAt.Before(oldest.lastAccessedAt) {
			oldest = snapshot
		}
	}
	if oldest != nil {
		delete(silenceEditorPreviewSnapshots.entries, oldest.ID)
		silenceEditorPreviewSnapshots.records -= len(oldest.Alerts)
		runtimeLog.DebugContext(ctx, "silence_preview", "preview_snapshot_evicted", "Silence preview snapshot evicted", slog.String("snapshot_id", oldest.ID), slog.Int("record_count", len(oldest.Alerts)), slog.Int("cache_entries", len(silenceEditorPreviewSnapshots.entries)), slog.Int("cache_records", silenceEditorPreviewSnapshots.records))
	}
}

func resetSilenceEditorPreviewSnapshots() {
	silenceEditorPreviewSnapshots.Lock()
	defer silenceEditorPreviewSnapshots.Unlock()
	silenceEditorPreviewSnapshots.entries = map[string]*silenceEditorPreviewSnapshot{}
	silenceEditorPreviewSnapshots.records = 0
}
