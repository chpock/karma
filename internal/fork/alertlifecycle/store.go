package alertlifecycle

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	runtimeLog "github.com/prymitive/karma/internal/log"
	"github.com/prymitive/karma/internal/models"
	"github.com/prymitive/karma/internal/regex"
	"github.com/prymitive/karma/internal/transform"

	_ "modernc.org/sqlite" // Register the SQLite database/sql driver.
)

const schemaVersion = "1"

const (
	metaSchemaVersion            = "schema_version"
	metaLastHousekeepingAt       = "last_housekeeping_at"
	metaLastIncrementalVacuumAt  = "last_incremental_vacuum_at"
	metaGroupingFingerprint      = "grouping_fingerprint"
	metaRegroupStatus            = "regroup_status"
	metaRegroupError             = "regroup_error"
	metaRegroupProcessed         = "regroup_processed"
	metaRegroupTotal             = "regroup_total"
	metaRegroupStartedAt         = "regroup_started_at"
	metaRegroupFinishedAt        = "regroup_finished_at"
	metaRegroupTargetFingerprint = "regroup_target_fingerprint"
	metaRegroupCursorRowID       = "regroup_cursor_rowid"
	metaSuggestionRevisionPrefix = "suggestion_revision/"
)

const reasonCodeMaxLength = 64

const defaultSilenceHistoryRetentionTTL = 30 * 24 * time.Hour

const (
	regroupStatusReady   = "ready"
	regroupStatusRunning = "running"
	regroupStatusError   = "error"
)

type Store struct {
	db                         *sql.DB
	path                       string
	silenceHistoryRetentionTTL time.Duration
	writeMu                    sync.Mutex
	recheckBlockedJobLogged    int64
}

type StoreOptions struct {
	Path                       string
	SilenceHistoryRetentionTTL time.Duration
}

type GroupingConfig struct {
	LabelsKeep          []string
	LabelsKeepRegex     []string
	LabelsStrip         []string
	LabelsStripRegex    []string
	ReceiversKeep       []string
	ReceiversKeepRegex  []string
	ReceiversStrip      []string
	ReceiversStripRegex []string
}

func OpenStore(opts StoreOptions) (*Store, error) {
	if strings.TrimSpace(opts.Path) == "" {
		return nil, errors.New("sqlite path cannot be empty")
	}

	dir := filepath.Dir(opts.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create sqlite directory: %w", err)
	}

	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=auto_vacuum(INCREMENTAL)&_pragma=busy_timeout(5000)", opts.Path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to connect to sqlite database: %w", err)
	}

	silenceHistoryRetentionTTL := opts.SilenceHistoryRetentionTTL
	if silenceHistoryRetentionTTL <= 0 {
		silenceHistoryRetentionTTL = defaultSilenceHistoryRetentionTTL
	}
	store := &Store{db: db, path: opts.Path, silenceHistoryRetentionTTL: silenceHistoryRetentionTTL}
	if err = store.bootstrapSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) sqlitePath() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *Store) bootstrapSchema() error {
	if s == nil || s.db == nil {
		return ErrStoreNotInitialized
	}

	const schemaSQL = `
CREATE TABLE IF NOT EXISTS alert_states (
  alert_id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  raw_state TEXT NOT NULL DEFAULT 'active',
  is_live INTEGER NOT NULL DEFAULT 1,
  is_suppressed INTEGER NOT NULL DEFAULT 0,
  is_resolved INTEGER NOT NULL DEFAULT 0,
  receiver TEXT NOT NULL,
  group_id TEXT NOT NULL,
  group_labels_json TEXT NOT NULL DEFAULT '{}',
  labels_json TEXT NOT NULL,
  match_labels_json TEXT,
  annotations_json TEXT NOT NULL,
  starts_at TEXT NOT NULL,
  last_seen_at TEXT,
  relevant_upstreams_json TEXT NOT NULL,
  relevant_sources_json TEXT NOT NULL DEFAULT '[]',
  suppression_observed_at TEXT,
  resolved_at TEXT,
  resolved_by TEXT,
  resolved_comment TEXT,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alert_states_state ON alert_states(state);
CREATE INDEX IF NOT EXISTS idx_alert_states_raw_state ON alert_states(raw_state);
CREATE INDEX IF NOT EXISTS idx_alert_states_is_live ON alert_states(is_live);
CREATE INDEX IF NOT EXISTS idx_alert_states_is_suppressed ON alert_states(is_suppressed);
CREATE INDEX IF NOT EXISTS idx_alert_states_last_seen ON alert_states(last_seen_at);

CREATE TABLE IF NOT EXISTS alert_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  old_state TEXT,
  new_state TEXT NOT NULL,
  old_raw_state TEXT,
  new_raw_state TEXT NOT NULL,
  old_is_live INTEGER,
  new_is_live INTEGER NOT NULL,
  old_is_suppressed INTEGER,
  new_is_suppressed INTEGER NOT NULL DEFAULT 0,
  old_is_resolved INTEGER,
  new_is_resolved INTEGER NOT NULL,
  changed_at TEXT NOT NULL,
  change_source TEXT NOT NULL,
  changed_by TEXT,
  reason_code TEXT,
  reason_text TEXT,
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_state_history_alert_changed_at
  ON alert_events(alert_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_state_history_changed_at
  ON alert_events(changed_at DESC);

CREATE TABLE IF NOT EXISTS alert_target_states (
  alert_id TEXT NOT NULL,
  target TEXT NOT NULL,
  state TEXT NOT NULL,
  raw_state TEXT NOT NULL,
  is_live INTEGER NOT NULL,
  is_suppressed INTEGER NOT NULL,
  is_resolved INTEGER NOT NULL DEFAULT 0,
  starts_at TEXT NOT NULL,
  last_seen_at TEXT,
  source_members_json TEXT NOT NULL,
  match_labels_fingerprint TEXT NOT NULL DEFAULT '',
  retired_at TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (alert_id, target),
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_alert_target_states_target_state
  ON alert_target_states(target, state, is_live);

CREATE INDEX IF NOT EXISTS idx_alert_target_states_live
  ON alert_target_states(target, is_live, updated_at DESC);

CREATE TABLE IF NOT EXISTS alert_target_match_labels (
  alert_id TEXT NOT NULL,
  target TEXT NOT NULL,
  label_name TEXT NOT NULL,
  label_value TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (alert_id, target, label_name),
  FOREIGN KEY (alert_id, target) REFERENCES alert_target_states(alert_id, target) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_alert_target_match_labels_target_name
  ON alert_target_match_labels(target, label_name);

CREATE INDEX IF NOT EXISTS idx_alert_target_match_labels_target_name_value
  ON alert_target_match_labels(target, label_name, label_value);

CREATE TABLE IF NOT EXISTS alert_target_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  target TEXT NOT NULL,
  old_state TEXT,
  new_state TEXT NOT NULL,
  old_raw_state TEXT,
  new_raw_state TEXT NOT NULL,
  old_is_live INTEGER,
  new_is_live INTEGER NOT NULL,
  old_is_suppressed INTEGER,
  new_is_suppressed INTEGER NOT NULL,
  old_is_resolved INTEGER,
  new_is_resolved INTEGER NOT NULL DEFAULT 0,
  old_retired_at TEXT,
  new_retired_at TEXT,
  changed_at TEXT NOT NULL,
  reason_code TEXT,
  FOREIGN KEY (alert_id, target) REFERENCES alert_target_states(alert_id, target) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_alert_target_events_changed_at
  ON alert_target_events(target, changed_at DESC);

CREATE TABLE IF NOT EXISTS metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alertmanager_sources (
  source_name TEXT PRIMARY KEY,
  current_cluster_id TEXT,
  sync_status TEXT NOT NULL CHECK (sync_status IN ('unknown', 'success', 'failed', 'invalid', 'retired')),
  silence_set_fingerprint TEXT,
  first_configured_at TEXT NOT NULL,
  last_configured_at TEXT NOT NULL,
  last_attempt_at TEXT,
  last_success_at TEXT,
  last_confirmed_at TEXT,
  last_error TEXT,
  retired_at TEXT,
  CHECK (sync_status = 'retired' OR current_cluster_id IS NOT NULL),
  CHECK (retired_at IS NULL OR sync_status = 'retired')
);

CREATE INDEX IF NOT EXISTS idx_alertmanager_sources_status
  ON alertmanager_sources (sync_status, source_name);
CREATE INDEX IF NOT EXISTS idx_alertmanager_sources_retired
  ON alertmanager_sources (retired_at);

CREATE TABLE IF NOT EXISTS alertmanager_cluster_consistency (
  cluster_id TEXT PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('unknown', 'consistent', 'inconsistent')),
  member_count INTEGER NOT NULL DEFAULT 0 CHECK (member_count >= 0),
  last_evaluated_at TEXT NOT NULL,
  last_consistent_at TEXT,
  last_inconsistent_at TEXT
);

CREATE TABLE IF NOT EXISTS silence_rules (
  rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id TEXT NOT NULL,
  silence_id TEXT NOT NULL,
  first_observed_at TEXT NOT NULL,
  last_observed_at TEXT NOT NULL,
  UNIQUE (cluster_id, silence_id)
);

CREATE INDEX IF NOT EXISTS idx_silence_rules_silence_id
  ON silence_rules (silence_id);

CREATE TABLE IF NOT EXISTS silence_rule_episodes (
  episode_id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id INTEGER NOT NULL,
  episode_number INTEGER NOT NULL,
  first_observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  ended_at TEXT,
  end_reason TEXT,
  open_marker INTEGER CHECK (open_marker IS NULL OR open_marker = 1),
  UNIQUE (rule_id, episode_number),
  UNIQUE (rule_id, open_marker),
  UNIQUE (episode_id, rule_id),
  CHECK ((open_marker = 1 AND ended_at IS NULL AND end_reason IS NULL)
    OR (open_marker IS NULL AND ended_at IS NOT NULL AND end_reason = 'absent_confirmed')),
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_silence_rule_episodes_rule
  ON silence_rule_episodes (rule_id, episode_number DESC);
CREATE INDEX IF NOT EXISTS idx_silence_rule_episodes_ended_at
  ON silence_rule_episodes (ended_at);

CREATE TABLE IF NOT EXISTS silence_revisions (
  revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
  episode_id INTEGER NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_number INTEGER NOT NULL,
  content_fingerprint TEXT NOT NULL,
  evaluation_fingerprint TEXT NOT NULL,
  matchers_json TEXT NOT NULL,
  starts_at TEXT NOT NULL,
  ends_at TEXT NOT NULL,
  source_updated_at TEXT,
  created_by TEXT NOT NULL,
  comment TEXT NOT NULL,
  annotations_json TEXT NOT NULL DEFAULT '{}',
  ticket_id TEXT NOT NULL DEFAULT '',
  ticket_url TEXT NOT NULL DEFAULT '',
  first_observed_at TEXT NOT NULL,
  last_observed_at TEXT NOT NULL,
  UNIQUE (episode_id, revision_number),
  UNIQUE (revision_id, rule_id),
  FOREIGN KEY (episode_id, rule_id) REFERENCES silence_rule_episodes(episode_id, rule_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_silence_revisions_episode
  ON silence_revisions (episode_id, revision_number DESC);
CREATE INDEX IF NOT EXISTS idx_silence_revisions_content_fingerprint
  ON silence_revisions (content_fingerprint);
CREATE INDEX IF NOT EXISTS idx_silence_revisions_evaluation_fingerprint
  ON silence_revisions (evaluation_fingerprint);
CREATE INDEX IF NOT EXISTS idx_silence_revisions_last_observed
  ON silence_revisions (last_observed_at);

CREATE TABLE IF NOT EXISTS alertmanager_source_silences (
  source_name TEXT NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_id INTEGER NOT NULL,
  observed_state TEXT NOT NULL CHECK (observed_state IN ('pending', 'active', 'expired')),
  first_observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  PRIMARY KEY (source_name, rule_id),
  FOREIGN KEY (source_name) REFERENCES alertmanager_sources(source_name) ON DELETE CASCADE,
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id) REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_source_silences_revision
  ON alertmanager_source_silences (revision_id);
CREATE INDEX IF NOT EXISTS idx_source_silences_state
  ON alertmanager_source_silences (source_name, observed_state);

CREATE TABLE IF NOT EXISTS silence_recheck_jobs (
  job_id INTEGER PRIMARY KEY AUTOINCREMENT,
  context_fingerprint TEXT NOT NULL,
  open_context_fingerprint TEXT,
  source_context_json TEXT NOT NULL,
  trigger_type TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'failed', 'completed')),
  cursor_alert_id TEXT,
  total_candidates INTEGER NOT NULL DEFAULT 0,
  processed_candidates INTEGER NOT NULL DEFAULT 0,
  promoted_alerts INTEGER NOT NULL DEFAULT 0,
  skipped_ineligible INTEGER NOT NULL DEFAULT 0,
  skipped_missing_labels INTEGER NOT NULL DEFAULT 0,
  skipped_incomplete_sources INTEGER NOT NULL DEFAULT 0,
  stale_conflicts INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  last_error TEXT,
  UNIQUE (open_context_fingerprint),
  CHECK (
    total_candidates >= 0 AND processed_candidates >= 0 AND promoted_alerts >= 0
    AND skipped_ineligible >= 0 AND skipped_missing_labels >= 0
    AND skipped_incomplete_sources >= 0 AND stale_conflicts >= 0 AND retry_count >= 0
  ),
  CHECK (
    (status = 'completed' AND open_context_fingerprint IS NULL)
    OR (status != 'completed' AND open_context_fingerprint = context_fingerprint)
  )
);

CREATE INDEX IF NOT EXISTS idx_silence_recheck_jobs_fifo
  ON silence_recheck_jobs (status, job_id);
CREATE INDEX IF NOT EXISTS idx_silence_recheck_jobs_context
  ON silence_recheck_jobs (context_fingerprint, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_silence_recheck_jobs_finished
  ON silence_recheck_jobs (finished_at);

CREATE TABLE IF NOT EXISTS silence_recheck_job_rules (
  job_id INTEGER NOT NULL,
  source_name TEXT NOT NULL,
  cluster_id TEXT NOT NULL DEFAULT '',
  rule_id INTEGER NOT NULL,
  revision_id INTEGER NOT NULL,
  observed_state TEXT NOT NULL CHECK (observed_state IN ('pending', 'active', 'expired')),
  PRIMARY KEY (job_id, source_name, rule_id),
  FOREIGN KEY (job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE CASCADE,
  FOREIGN KEY (source_name) REFERENCES alertmanager_sources(source_name) ON DELETE RESTRICT,
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id) REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_recheck_job_rules_revision
  ON silence_recheck_job_rules (revision_id);
CREATE INDEX IF NOT EXISTS idx_recheck_job_rules_source
  ON silence_recheck_job_rules (job_id, source_name, observed_state);

CREATE TABLE IF NOT EXISTS silence_recheck_job_candidates (
  job_id INTEGER NOT NULL,
  alert_id TEXT NOT NULL,
  processed_at TEXT,
  PRIMARY KEY (job_id, alert_id),
  FOREIGN KEY (job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE CASCADE,
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_recheck_job_candidates_pending
  ON silence_recheck_job_candidates(job_id, processed_at, alert_id);

CREATE TABLE IF NOT EXISTS alert_suppression_cause_history (
  history_id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  transition_event_id INTEGER UNIQUE,
  recheck_job_id INTEGER,
  source TEXT NOT NULL CHECK (source IN ('live_alertmanager', 'silence_recheck', 'legacy_unknown')),
  cause_status TEXT NOT NULL CHECK (cause_status IN ('known', 'partial', 'none', 'unknown')),
  cause_fingerprint TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  CHECK (
    (source = 'silence_recheck' AND cause_status = 'known' AND transition_event_id IS NOT NULL AND recheck_job_id IS NOT NULL)
    OR (source = 'live_alertmanager' AND cause_status IN ('known', 'partial', 'none') AND transition_event_id IS NULL AND recheck_job_id IS NULL)
    OR (source = 'legacy_unknown' AND cause_status = 'unknown' AND transition_event_id IS NULL AND recheck_job_id IS NULL)
  ),
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE,
  FOREIGN KEY (transition_event_id) REFERENCES alert_events(id) ON DELETE CASCADE,
  FOREIGN KEY (recheck_job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_alert_suppression_cause_latest
  ON alert_suppression_cause_history (alert_id, history_id DESC);
CREATE INDEX IF NOT EXISTS idx_alert_suppression_cause_fingerprint
  ON alert_suppression_cause_history (alert_id, cause_fingerprint);
CREATE INDEX IF NOT EXISTS idx_alert_suppression_cause_job
  ON alert_suppression_cause_history (recheck_job_id);

CREATE TABLE IF NOT EXISTS alert_target_suppression_cause_history (
  history_id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  target TEXT NOT NULL,
  target_transition_event_id INTEGER UNIQUE,
  recheck_job_id INTEGER,
  source TEXT NOT NULL CHECK (source IN ('live_alertmanager', 'silence_recheck', 'legacy_unknown')),
  cause_status TEXT NOT NULL CHECK (cause_status IN ('known', 'partial', 'none', 'unknown')),
  cause_fingerprint TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  CHECK (
    (source = 'silence_recheck' AND cause_status = 'known' AND target_transition_event_id IS NOT NULL AND recheck_job_id IS NOT NULL)
    OR (source = 'live_alertmanager' AND cause_status IN ('known', 'partial', 'none') AND target_transition_event_id IS NULL AND recheck_job_id IS NULL)
    OR (source = 'legacy_unknown' AND cause_status = 'unknown' AND target_transition_event_id IS NULL AND recheck_job_id IS NULL)
  ),
  FOREIGN KEY (alert_id, target) REFERENCES alert_target_states(alert_id, target) ON DELETE CASCADE,
  FOREIGN KEY (target_transition_event_id) REFERENCES alert_target_events(id) ON DELETE CASCADE,
  FOREIGN KEY (recheck_job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_target_suppression_cause_latest
  ON alert_target_suppression_cause_history (alert_id, target, history_id DESC);

CREATE TABLE IF NOT EXISTS alert_target_suppression_cause_members (
  member_id INTEGER PRIMARY KEY AUTOINCREMENT,
  history_id INTEGER NOT NULL,
  source_name TEXT NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_id INTEGER,
  resolution_status TEXT NOT NULL CHECK (resolution_status IN ('known', 'unresolved')),
  UNIQUE (history_id, source_name, rule_id),
  CHECK (
    (resolution_status = 'known' AND revision_id IS NOT NULL)
    OR (resolution_status = 'unresolved' AND revision_id IS NULL)
  ),
  FOREIGN KEY (history_id) REFERENCES alert_target_suppression_cause_history(history_id) ON DELETE CASCADE,
  FOREIGN KEY (source_name) REFERENCES alertmanager_sources(source_name) ON DELETE RESTRICT,
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id) REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS alert_target_aggregate_suppression_history (
  aggregate_id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  transition_event_id INTEGER NOT NULL UNIQUE,
  cause_fingerprint TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE,
  FOREIGN KEY (transition_event_id) REFERENCES alert_events(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS alert_target_aggregate_suppression_links (
  aggregate_id INTEGER NOT NULL,
  target_history_id INTEGER NOT NULL,
  PRIMARY KEY (aggregate_id, target_history_id),
  FOREIGN KEY (aggregate_id) REFERENCES alert_target_aggregate_suppression_history(aggregate_id) ON DELETE CASCADE,
  FOREIGN KEY (target_history_id) REFERENCES alert_target_suppression_cause_history(history_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS alert_suppression_cause_members (
  member_id INTEGER PRIMARY KEY AUTOINCREMENT,
  history_id INTEGER NOT NULL,
  source_name TEXT NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_id INTEGER,
  resolution_status TEXT NOT NULL CHECK (resolution_status IN ('known', 'unresolved')),
  UNIQUE (history_id, source_name, rule_id),
  CHECK (
    (resolution_status = 'known' AND revision_id IS NOT NULL)
    OR (resolution_status = 'unresolved' AND revision_id IS NULL)
  ),
  FOREIGN KEY (history_id) REFERENCES alert_suppression_cause_history(history_id) ON DELETE CASCADE,
  FOREIGN KEY (source_name) REFERENCES alertmanager_sources(source_name) ON DELETE RESTRICT,
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id) REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_alert_suppression_members_revision
  ON alert_suppression_cause_members (revision_id);
CREATE INDEX IF NOT EXISTS idx_alert_suppression_members_rule
  ON alert_suppression_cause_members (rule_id);
CREATE INDEX IF NOT EXISTS idx_alert_suppression_members_source
  ON alert_suppression_cause_members (source_name, history_id);
`

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin schema transaction: %w", err)
	}
	if err = validateSchemaV1Tx(tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	if _, err = tx.Exec(schemaSQL); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to apply schema: %w", err)
	}

	if _, err = tx.Exec(`INSERT OR IGNORE INTO metadata(key, value) VALUES(?, ?)`, metaSchemaVersion, schemaVersion); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to set schema version: %w", err)
	}

	defaults := map[string]string{
		metaRegroupStatus:            regroupStatusReady,
		metaGroupingFingerprint:      "",
		metaRegroupTargetFingerprint: "",
		metaRegroupError:             "",
		metaRegroupProcessed:         "0",
		metaRegroupTotal:             "0",
		metaRegroupCursorRowID:       "0",
		metaRegroupStartedAt:         "",
		metaRegroupFinishedAt:        "",
	}
	for key, value := range defaults {
		if _, err = tx.Exec(`INSERT OR IGNORE INTO metadata(key, value) VALUES(?, ?)`, key, value); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to seed metadata %q: %w", key, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit schema transaction: %w", err)
	}

	return nil
}

func validateSchemaV1Tx(tx *sql.Tx) error {
	var metadataExists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='metadata'`).Scan(&metadataExists); err != nil {
		return fmt.Errorf("failed to inspect sqlite schema: %w", err)
	}
	if metadataExists == 0 {
		var tableCount int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tableCount); err != nil {
			return fmt.Errorf("failed to inspect sqlite tables: %w", err)
		}
		if tableCount != 0 {
			return errors.New("unsupported sqlite database: lifecycle storage requires a new schema v1 database")
		}
		return nil
	}

	var version string
	if err := tx.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaSchemaVersion).Scan(&version); err != nil {
		return fmt.Errorf("unsupported sqlite database: missing schema version 1: %w", err)
	}
	if version != schemaVersion {
		return fmt.Errorf("unsupported sqlite schema version %q: expected %q", version, schemaVersion)
	}
	return nil
}

func (s *Store) UpsertCurrentAlert(alert AlertCurrent) error {
	if s == nil || s.db == nil {
		return ErrStoreNotInitialized
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if strings.TrimSpace(alert.GroupLabelsJSON) == "" {
		alert.GroupLabelsJSON = "{}"
	}
	if alert.IsResolved && alert.IsLive {
		alert.IsResolved = false
	}
	if !alert.RawState.IsValid() {
		alert.RawState = alert.State
	}
	if alert.IsLive {
		alert.IsSuppressed = alert.RawState == StateSuppressed
	}
	if !alert.State.IsValid() {
		alert.State = projectedState(alert.RawState, alert.IsLive, alert.IsSuppressed, alert.IsResolved)
	}
	if alert.State == StateActive || alert.State == StateSuppressed || alert.State == StateUnprocessed {
		alert.IsLive = true
	}
	if alert.State == StateInactive {
		alert.IsLive = false
	}
	if alert.State == StateResolved {
		alert.IsResolved = true
		alert.IsLive = false
	}
	if alert.IsLive {
		alert.IsSuppressed = alert.RawState == StateSuppressed
	}
	if strings.TrimSpace(alert.RelevantSources) == "" {
		alert.RelevantSources = alert.RelevantUpstreams
	}
	if alert.State.IsValid() {
		alert.State = projectedState(alert.RawState, alert.IsLive, alert.IsSuppressed, alert.IsResolved)
	}

	if err := validateCurrentAlert(alert); err != nil {
		return err
	}

	now := time.Now().UTC()
	if alert.UpdatedAt.IsZero() {
		alert.UpdatedAt = now
	}

	_, err := s.db.Exec(`
INSERT INTO alert_states(
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(alert_id) DO UPDATE SET
  state=excluded.state,
  raw_state=excluded.raw_state,
  is_live=excluded.is_live,
  is_suppressed=excluded.is_suppressed,
  is_resolved=excluded.is_resolved,
  receiver=excluded.receiver,
  group_id=excluded.group_id,
  group_labels_json=excluded.group_labels_json,
  labels_json=excluded.labels_json,
  match_labels_json=excluded.match_labels_json,
  annotations_json=excluded.annotations_json,
  starts_at=excluded.starts_at,
  last_seen_at=excluded.last_seen_at,
  relevant_upstreams_json=excluded.relevant_upstreams_json,
  relevant_sources_json=excluded.relevant_sources_json,
  suppression_observed_at=excluded.suppression_observed_at,
  resolved_at=excluded.resolved_at,
  resolved_by=excluded.resolved_by,
  resolved_comment=excluded.resolved_comment,
  updated_at=excluded.updated_at
`,
		alert.AlertID,
		string(alert.State),
		string(alert.RawState),
		boolToInt(alert.IsLive),
		boolToInt(alert.IsSuppressed),
		boolToInt(alert.IsResolved),
		alert.Receiver,
		alert.GroupID,
		alert.GroupLabelsJSON,
		alert.LabelsJSON,
		alert.MatchLabelsJSON,
		alert.AnnotationsJSON,
		alert.StartsAt.UTC().Format(time.RFC3339Nano),
		formatNullableTime(alert.LastSeenAt),
		alert.RelevantUpstreams,
		alert.RelevantSources,
		formatNullableTime(alert.SuppressionObservedAt),
		formatNullableTime(alert.ResolvedAt),
		alert.ResolvedBy,
		alert.ResolvedComment,
		alert.UpdatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("failed to upsert current alert: %w", err)
	}
	if alert.State == StateInactive && !alert.IsLive && !alert.IsResolved && !alert.IsSuppressed && alert.MatchLabelsJSON != nil && strings.TrimSpace(*alert.MatchLabelsJSON) != "" {
		if _, err = s.db.Exec(`
INSERT OR IGNORE INTO silence_recheck_job_candidates(job_id, alert_id)
SELECT job_id, ? FROM silence_recheck_jobs WHERE status IN ('queued','running')
`, alert.AlertID); err != nil {
			return fmt.Errorf("failed attaching inactive alert to open recheck jobs: %w", err)
		}
	}

	return nil
}

func (s *Store) InsertStateTransition(transition AlertStateTransition) error {
	if s == nil || s.db == nil {
		return ErrStoreNotInitialized
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if !transition.NewRawState.IsValid() {
		if transition.NewState == StateInactive || transition.NewState == StateResolved {
			transition.NewRawState = StateActive
		} else {
			transition.NewRawState = transition.NewState
		}
	}
	if transition.OldRawState == nil && transition.OldState != nil {
		v := *transition.OldState
		transition.OldRawState = &v
	}
	if transition.OldIsLive == nil {
		v := transition.OldState != nil && *transition.OldState != StateInactive && *transition.OldState != StateResolved
		transition.OldIsLive = &v
	}
	if transition.OldIsResolved == nil {
		v := transition.OldState != nil && *transition.OldState == StateResolved
		transition.OldIsResolved = &v
	}
	if transition.OldIsSuppressed == nil {
		v := transition.OldState != nil && *transition.OldState == StateSuppressed
		transition.OldIsSuppressed = &v
	}
	if transition.NewIsLive {
		transition.NewIsSuppressed = transition.NewRawState == StateSuppressed
	}
	if transition.NewState.IsValid() {
		transition.NewState = projectedState(transition.NewRawState, transition.NewIsLive, transition.NewIsSuppressed, transition.NewIsResolved)
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin transition transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err = insertTransitionTx(tx, transition); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transition transaction: %w", err)
	}

	return nil
}

func (s *Store) GetCurrentAlert(alertID string) (*AlertCurrent, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}

	if strings.TrimSpace(alertID) == "" {
		return nil, ErrInvalidAlertID
	}

	row := s.db.QueryRow(`
SELECT
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
FROM alert_states
WHERE alert_id = ?
`, alertID)

	var (
		stateRaw               string
		rawStateRaw            string
		isLiveRaw              int
		isSuppressedRaw        int
		isResolvedRaw          int
		matchLabelsRaw         sql.NullString
		startsAtRaw            string
		lastSeenRaw            sql.NullString
		suppressionObservedRaw sql.NullString
		resolvedAtRaw          sql.NullString
		resolvedByRaw          sql.NullString
		resolvedCommentRaw     sql.NullString
		updatedAtRaw           string
		current                AlertCurrent
	)

	err := row.Scan(
		&current.AlertID,
		&stateRaw,
		&rawStateRaw,
		&isLiveRaw,
		&isSuppressedRaw,
		&isResolvedRaw,
		&current.Receiver,
		&current.GroupID,
		&current.GroupLabelsJSON,
		&current.LabelsJSON,
		&matchLabelsRaw,
		&current.AnnotationsJSON,
		&startsAtRaw,
		&lastSeenRaw,
		&current.RelevantUpstreams,
		&current.RelevantSources,
		&suppressionObservedRaw,
		&resolvedAtRaw,
		&resolvedByRaw,
		&resolvedCommentRaw,
		&updatedAtRaw,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get current alert: %w", err)
	}

	state := State(stateRaw)
	if !state.IsValid() {
		return nil, fmt.Errorf("invalid state in database: %q", stateRaw)
	}
	current.State = state
	rawState := State(rawStateRaw)
	if !rawState.IsValid() {
		return nil, fmt.Errorf("invalid raw_state in database: %q", rawStateRaw)
	}
	current.RawState = rawState
	current.IsLive = isLiveRaw != 0
	current.IsSuppressed = isSuppressedRaw != 0
	current.IsResolved = isResolvedRaw != 0
	if matchLabelsRaw.Valid {
		v := strings.TrimSpace(matchLabelsRaw.String)
		if v != "" {
			current.MatchLabelsJSON = &v
		}
	}

	startsAt, err := time.Parse(time.RFC3339Nano, startsAtRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse starts_at: %w", err)
	}
	current.StartsAt = startsAt

	if lastSeenRaw.Valid {
		lastSeenAt, parseErr := time.Parse(time.RFC3339Nano, lastSeenRaw.String)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse last_seen_at: %w", parseErr)
		}
		current.LastSeenAt = &lastSeenAt
	}
	if resolvedAtRaw.Valid {
		resolvedAt, parseErr := time.Parse(time.RFC3339Nano, resolvedAtRaw.String)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse resolved_at: %w", parseErr)
		}
		current.ResolvedAt = &resolvedAt
	}
	if suppressionObservedRaw.Valid {
		suppressionObservedAt, parseErr := time.Parse(time.RFC3339Nano, suppressionObservedRaw.String)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse suppression_observed_at: %w", parseErr)
		}
		current.SuppressionObservedAt = &suppressionObservedAt
	}
	if resolvedByRaw.Valid {
		v := strings.TrimSpace(resolvedByRaw.String)
		if v != "" {
			current.ResolvedBy = &v
		}
	}
	if resolvedCommentRaw.Valid {
		v := strings.TrimSpace(resolvedCommentRaw.String)
		if v != "" {
			current.ResolvedComment = &v
		}
	}

	updatedAt, err := time.Parse(time.RFC3339Nano, updatedAtRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse updated_at: %w", err)
	}
	current.UpdatedAt = updatedAt

	return &current, nil
}

func (s *Store) GetSchemaVersion() (string, error) {
	if s == nil || s.db == nil {
		return "", ErrStoreNotInitialized
	}

	var version string
	err := s.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaSchemaVersion).Scan(&version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to query schema version: %w", err)
	}

	return version, nil
}

func (s *Store) GetTransitionCount(alertID string) (int, error) {
	if s == nil || s.db == nil {
		return 0, ErrStoreNotInitialized
	}
	if strings.TrimSpace(alertID) == "" {
		return 0, ErrInvalidAlertID
	}

	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM alert_events WHERE alert_id = ?`, alertID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count alert transitions: %w", err)
	}

	return count, nil
}

func (s *Store) ListCurrentAlertsByStates(states []State) ([]AlertCurrent, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}
	if len(states) == 0 {
		return []AlertCurrent{}, nil
	}

	args := make([]any, 0, len(states))
	placeholders := make([]string, 0, len(states))
	for _, state := range states {
		if !state.IsValid() {
			return nil, fmt.Errorf("invalid state: %q", state)
		}
		placeholders = append(placeholders, "?")
		args = append(args, string(state))
	}

	query := fmt.Sprintf(`
SELECT
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
FROM alert_states
WHERE state IN (%s)
`, strings.Join(placeholders, ","))

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list current alerts by state: %w", err)
	}
	defer rows.Close()

	alerts := []AlertCurrent{}
	for rows.Next() {
		current, scanErr := scanCurrentAlert(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		alerts = append(alerts, current)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate listed alerts: %w", err)
	}

	return alerts, nil
}

func (s *Store) ListCurrentAlertsForMerge(includeResolved bool) ([]AlertCurrent, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}

	query := `
SELECT
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
FROM alert_states
WHERE is_live = 0
`
	args := []any{}
	if !includeResolved {
		query += ` AND is_resolved = 0`
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list alerts for merge: %w", err)
	}
	defer rows.Close()

	alerts := []AlertCurrent{}
	for rows.Next() {
		current, scanErr := scanCurrentAlert(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		alerts = append(alerts, current)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate merge alerts: %w", err)
	}
	if err = rows.Close(); err != nil {
		return nil, fmt.Errorf("failed to close merge alert rows: %w", err)
	}

	causes, err := s.listEffectiveSuppressionCausesForMerge()
	if err != nil {
		return nil, err
	}
	for index := range alerts {
		alerts[index].SuppressionCauses = causes[alerts[index].AlertID]
	}

	return alerts, nil
}

func (s *Store) listEffectiveSuppressionCausesForMerge() (map[string][]SuppressionCauseRef, error) {
	rows, err := s.db.Query(`
WITH cause_parents AS (
  SELECT alert_id, 'global' AS kind, history_id AS parent_id, observed_at
  FROM alert_suppression_cause_history
  UNION ALL
  SELECT alert_id, 'aggregate' AS kind, aggregate_id AS parent_id, observed_at
  FROM alert_target_aggregate_suppression_history
), ranked_parents AS (
  SELECT alert_id, kind, parent_id,
         ROW_NUMBER() OVER (
           PARTITION BY alert_id
           ORDER BY observed_at DESC, kind DESC, parent_id DESC
         ) AS rank
  FROM cause_parents
), cause_members AS (
  SELECT 'global' AS kind, members.history_id AS parent_id,
         members.source_name, members.rule_id, members.revision_id,
         members.resolution_status
  FROM alert_suppression_cause_members members
  UNION ALL
  SELECT 'aggregate' AS kind, links.aggregate_id AS parent_id,
         members.source_name, members.rule_id, members.revision_id,
         members.resolution_status
  FROM alert_target_aggregate_suppression_links links
  JOIN alert_target_suppression_cause_members members
    ON members.history_id = links.target_history_id
)
SELECT DISTINCT parents.alert_id, members.source_name, rules.cluster_id,
       rules.silence_id, members.resolution_status,
       revisions.starts_at, revisions.ends_at, revisions.created_by,
       revisions.comment, revisions.matchers_json
FROM ranked_parents parents
JOIN alert_states alerts ON alerts.alert_id = parents.alert_id
JOIN cause_members members
  ON members.kind = parents.kind AND members.parent_id = parents.parent_id
JOIN silence_rules rules ON rules.rule_id = members.rule_id
LEFT JOIN silence_revisions revisions
  ON revisions.revision_id = members.revision_id
  AND revisions.rule_id = members.rule_id
WHERE parents.rank = 1
  AND alerts.state = 'suppressed'
  AND alerts.is_live = 0
  AND alerts.is_suppressed = 1
ORDER BY parents.alert_id, members.source_name, rules.cluster_id, rules.silence_id
`)
	if err != nil {
		return nil, fmt.Errorf("failed to list effective suppression causes for merge: %w", err)
	}
	defer rows.Close()

	result := map[string][]SuppressionCauseRef{}
	for rows.Next() {
		var alertID string
		var cause SuppressionCauseRef
		var startsAtRaw, endsAtRaw, createdBy, comment, matchersRaw sql.NullString
		if err = rows.Scan(
			&alertID,
			&cause.SourceName,
			&cause.ClusterID,
			&cause.SilenceID,
			&cause.ResolutionStatus,
			&startsAtRaw,
			&endsAtRaw,
			&createdBy,
			&comment,
			&matchersRaw,
		); err != nil {
			return nil, fmt.Errorf("failed to scan effective suppression cause for merge: %w", err)
		}
		if startsAtRaw.Valid {
			startsAt, parseErr := time.Parse(time.RFC3339Nano, startsAtRaw.String)
			if parseErr != nil {
				return nil, fmt.Errorf("failed to parse effective suppression cause starts_at: %w", parseErr)
			}
			cause.StartsAt = &startsAt
		}
		if endsAtRaw.Valid {
			endsAt, parseErr := time.Parse(time.RFC3339Nano, endsAtRaw.String)
			if parseErr != nil {
				return nil, fmt.Errorf("failed to parse effective suppression cause ends_at: %w", parseErr)
			}
			cause.EndsAt = &endsAt
		}
		if createdBy.Valid {
			cause.CreatedBy = createdBy.String
		}
		if comment.Valid {
			cause.Comment = comment.String
		}
		if matchersRaw.Valid {
			if err = json.Unmarshal([]byte(matchersRaw.String), &cause.Matchers); err != nil {
				return nil, fmt.Errorf("failed to decode effective suppression cause matchers: %w", err)
			}
		}
		result[alertID] = append(result[alertID], cause)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate effective suppression causes for merge: %w", err)
	}
	return result, nil
}

func (s *Store) ListTargetAlertsForPreview(target string) ([]AlertTargetCurrent, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, errors.New("target cannot be empty")
	}

	rows, err := s.db.Query(`
SELECT
  t.alert_id,
  t.target,
  t.state,
  t.raw_state,
  t.is_live,
  t.is_suppressed,
  t.starts_at,
  t.last_seen_at,
  t.source_members_json,
  s.match_labels_json,
  s.labels_json,
  t.is_resolved
FROM alert_target_states t
JOIN alert_states s ON s.alert_id = t.alert_id
WHERE t.target = ? AND t.retired_at IS NULL
`, target)
	if err != nil {
		return nil, fmt.Errorf("failed to list target alerts for preview: %w", err)
	}
	defer rows.Close()

	out := []AlertTargetCurrent{}
	for rows.Next() {
		var (
			stateRaw        string
			rawStateRaw     string
			isLiveRaw       int
			isSuppressedRaw int
			startsAtRaw     string
			lastSeenRaw     sql.NullString
			matchLabelsRaw  sql.NullString
			isResolvedRaw   int
			row             AlertTargetCurrent
		)
		if err = rows.Scan(
			&row.AlertID,
			&row.Target,
			&stateRaw,
			&rawStateRaw,
			&isLiveRaw,
			&isSuppressedRaw,
			&startsAtRaw,
			&lastSeenRaw,
			&row.SourceMembersJSON,
			&matchLabelsRaw,
			&row.LabelsJSON,
			&isResolvedRaw,
		); err != nil {
			return nil, fmt.Errorf("failed to scan target alert row: %w", err)
		}
		row.State = State(stateRaw)
		row.RawState = State(rawStateRaw)
		if !row.State.IsValid() || !row.RawState.IsValid() {
			continue
		}
		row.IsLive = intToBool(isLiveRaw)
		row.IsSuppressed = intToBool(isSuppressedRaw)
		row.IsResolved = intToBool(isResolvedRaw)
		if matchLabelsRaw.Valid {
			v := strings.TrimSpace(matchLabelsRaw.String)
			if v != "" {
				row.MatchLabelsJSON = &v
			}
		}
		startsAt, parseErr := time.Parse(time.RFC3339Nano, startsAtRaw)
		if parseErr != nil {
			continue
		}
		row.StartsAt = startsAt
		if lastSeenRaw.Valid {
			seenAt, parseErr := time.Parse(time.RFC3339Nano, lastSeenRaw.String)
			if parseErr == nil {
				row.LastSeenAt = &seenAt
			}
		}
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate target alert rows: %w", err)
	}
	return out, nil
}

func (s *Store) ListTargetLabelNames(target, term string, limit int) ([]SilenceEditorLabelNameStat, int, error) {
	return s.ListTargetLabelNamesWithLive(target, term, limit, nil)
}

func (s *Store) ListTargetLabelNamesWithLive(target, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelNameStat, int, error) {
	if s == nil || s.db == nil {
		return nil, 0, ErrStoreNotInitialized
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, 0, errors.New("target cannot be empty")
	}
	if limit <= 0 {
		limit = 100
	}
	term = strings.ToLower(strings.TrimSpace(term))
	if len(live) == 0 {
		rows, err := s.db.Query(`
SELECT label_name, value_count, COUNT(*) OVER() AS total_count
FROM (
  SELECT l.label_name, COUNT(DISTINCT l.label_value) AS value_count
  FROM alert_target_match_labels l
  WHERE l.target=?
    AND (?='' OR substr(lower(l.label_name), 1, length(?))=?)
  GROUP BY l.label_name
)
ORDER BY label_name ASC
LIMIT ?
`, target, term, term, term, limit)
		return scanTargetLabelNameStats(rows, err)
	}
	liveJSON, err := json.Marshal(live)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to encode live target labels: %w", err)
	}

	rows, err := s.db.Query(`
WITH live_input AS MATERIALIZED (
  SELECT json_extract(value, '$.alertID') AS alert_id,
         json_extract(value, '$.labels') AS labels_json,
         json_extract(value, '$.matchLabelsFingerprint') AS match_labels_fingerprint
  FROM json_each(?)
),
live_alerts AS MATERIALIZED (
  SELECT input.alert_id, input.labels_json
  FROM live_input input
  LEFT JOIN alert_target_states state ON state.alert_id=input.alert_id AND state.target=?
  WHERE COALESCE(input.match_labels_fingerprint, '')=''
     OR state.alert_id IS NULL
     OR state.retired_at IS NOT NULL
     OR state.match_labels_fingerprint<>input.match_labels_fingerprint
),
live_labels AS (
  SELECT live.alert_id, labels.key AS label_name, CAST(labels.value AS TEXT) AS label_value
  FROM live_alerts live, json_each(live.labels_json) labels
  WHERE (? = '' OR substr(lower(labels.key), 1, length(?)) = ?)
),
stored_labels AS (
  SELECT l.alert_id, l.label_name, l.label_value
  FROM alert_target_match_labels l
  LEFT JOIN live_alerts live ON live.alert_id=l.alert_id
  WHERE l.target = ?
    AND live.alert_id IS NULL
    AND (? = '' OR substr(lower(l.label_name), 1, length(?)) = ?)
),
eligible_labels AS (
  SELECT alert_id, label_name, label_value FROM stored_labels
  UNION ALL
  SELECT alert_id, label_name, label_value FROM live_labels
)
SELECT label_name, value_count, COUNT(*) OVER() AS total_count
FROM (
  SELECT label_name, COUNT(DISTINCT label_value) AS value_count
  FROM eligible_labels
  GROUP BY label_name
)
ORDER BY label_name ASC
LIMIT ?
`, string(liveJSON), target, term, term, term, target, term, term, term, limit)
	return scanTargetLabelNameStats(rows, err)
}

func scanTargetLabelNameStats(rows *sql.Rows, err error) ([]SilenceEditorLabelNameStat, int, error) {
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list target label names: %w", err)
	}
	defer rows.Close()
	out := []SilenceEditorLabelNameStat{}
	total := 0
	for rows.Next() {
		var row SilenceEditorLabelNameStat
		if err = rows.Scan(&row.Name, &row.Values, &total); err != nil {
			return nil, 0, fmt.Errorf("failed scanning target label name row: %w", err)
		}
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed iterating target label names: %w", err)
	}
	return out, total, nil
}

func (s *Store) ListTargetLabelValues(target, name, term string, limit int) ([]SilenceEditorLabelValueStat, int, error) {
	return s.ListTargetLabelValuesWithLive(target, name, term, limit, nil)
}

func (s *Store) ListTargetLabelValuesWithLive(target, name, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelValueStat, int, error) {
	if s == nil || s.db == nil {
		return nil, 0, ErrStoreNotInitialized
	}
	target = strings.TrimSpace(target)
	name = strings.TrimSpace(name)
	if target == "" || name == "" {
		return nil, 0, errors.New("target and name cannot be empty")
	}
	if limit <= 0 {
		limit = 100
	}
	term = strings.ToLower(strings.TrimSpace(term))
	if len(live) == 0 {
		rows, err := s.db.Query(`
SELECT label_value, hits, COUNT(*) OVER() AS total_count
FROM (
  SELECT l.label_value, COUNT(*) AS hits
  FROM alert_target_match_labels l
  WHERE l.target=?
    AND l.label_name=?
    AND (?='' OR substr(lower(l.label_value), 1, length(?))=?)
  GROUP BY l.label_value
)
ORDER BY hits DESC, label_value ASC
LIMIT ?
`, target, name, term, term, term, limit)
		return scanTargetLabelValueStats(rows, err)
	}
	liveJSON, err := json.Marshal(live)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to encode live target labels: %w", err)
	}

	rows, err := s.db.Query(`
WITH live_input AS MATERIALIZED (
  SELECT json_extract(value, '$.alertID') AS alert_id,
         json_extract(value, '$.labels') AS labels_json,
         json_extract(value, '$.matchLabelsFingerprint') AS match_labels_fingerprint
  FROM json_each(?)
),
live_alerts AS MATERIALIZED (
  SELECT input.alert_id, input.labels_json
  FROM live_input input
  LEFT JOIN alert_target_states state ON state.alert_id=input.alert_id AND state.target=?
  WHERE COALESCE(input.match_labels_fingerprint, '')=''
     OR state.alert_id IS NULL
     OR state.retired_at IS NOT NULL
     OR state.match_labels_fingerprint<>input.match_labels_fingerprint
),
live_labels AS (
  SELECT live.alert_id, labels.key AS label_name, CAST(labels.value AS TEXT) AS label_value
  FROM live_alerts live, json_each(live.labels_json) labels
  WHERE labels.key = ?
    AND (? = '' OR substr(lower(CAST(labels.value AS TEXT)), 1, length(?)) = ?)
),
stored_labels AS (
  SELECT l.alert_id, l.label_name, l.label_value
  FROM alert_target_match_labels l
  LEFT JOIN live_alerts live ON live.alert_id=l.alert_id
  WHERE l.target = ?
    AND live.alert_id IS NULL
    AND l.label_name = ?
    AND (? = '' OR substr(lower(l.label_value), 1, length(?)) = ?)
),
eligible_labels AS (
  SELECT alert_id, label_name, label_value FROM stored_labels
  UNION ALL
  SELECT alert_id, label_name, label_value FROM live_labels
)
SELECT label_value, hits, COUNT(*) OVER() AS total_count
FROM (
  SELECT label_value, COUNT(*) AS hits
  FROM eligible_labels
  GROUP BY label_value
)
ORDER BY hits DESC, label_value ASC
LIMIT ?
`, string(liveJSON), target, name, term, term, term, target, name, term, term, term, limit)
	return scanTargetLabelValueStats(rows, err)
}

// ListTargetContextLabelNamesWithLive aggregates label names only after the
// supplied matchers select the target's candidate alerts.
func (s *Store) ListTargetContextLabelNamesWithLive(target string, matchers []models.SilenceMatcher, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelNameStat, int, error) {
	candidates, err := s.listTargetContextLabelCandidatesWithLive(target, matchers, live)
	if err != nil {
		return nil, 0, err
	}
	return aggregateTargetLabelNames(candidates, term, limit), countTargetLabelNames(candidates, term), nil
}

// ListTargetContextCandidateIDsWithLive returns the alert IDs selected before
// label aggregation so callers can reuse that selection for prefix requests.
func (s *Store) ListTargetContextCandidateIDsWithLive(target string, matchers []models.SilenceMatcher, live []TargetLabelOverlay) ([]string, error) {
	candidates, err := s.listTargetContextLabelCandidatesWithLive(target, matchers, live)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.AlertID)
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *Store) ListTargetLabelNamesForCandidateIDsWithLive(target string, candidateIDs []string, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelNameStat, int, error) {
	candidates, err := s.listTargetLabelCandidatesByIDWithLive(target, candidateIDs, live)
	if err != nil {
		return nil, 0, err
	}
	return aggregateTargetLabelNames(candidates, term, limit), countTargetLabelNames(candidates, term), nil
}

// ListTargetContextLabelValuesWithLive aggregates values only after the
// supplied matchers select the target's candidate alerts.
func (s *Store) ListTargetContextLabelValuesWithLive(target, name string, matchers []models.SilenceMatcher, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelValueStat, int, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, 0, errors.New("label name cannot be empty")
	}
	candidates, err := s.listTargetContextLabelCandidatesWithLive(target, matchers, live)
	if err != nil {
		return nil, 0, err
	}
	items, total := aggregateTargetLabelValues(candidates, name, term, limit)
	return items, total, nil
}

func (s *Store) ListTargetLabelValuesForCandidateIDsWithLive(target, name string, candidateIDs []string, term string, limit int, live []TargetLabelOverlay) ([]SilenceEditorLabelValueStat, int, error) {
	candidates, err := s.listTargetLabelCandidatesByIDWithLive(target, candidateIDs, live)
	if err != nil {
		return nil, 0, err
	}
	items, total := aggregateTargetLabelValues(candidates, name, term, limit)
	return items, total, nil
}

func (s *Store) listTargetLabelCandidatesByIDWithLive(target string, candidateIDs []string, live []TargetLabelOverlay) ([]TargetLabelOverlay, error) {
	ids := map[string]struct{}{}
	for _, id := range candidateIDs {
		ids[id] = struct{}{}
	}
	candidates, err := s.listTargetLabelCandidatesWithLive(target, live, candidateIDs)
	if err != nil {
		return nil, err
	}
	result := make([]TargetLabelOverlay, 0, len(candidateIDs))
	for _, candidate := range candidates {
		if _, ok := ids[candidate.AlertID]; ok {
			result = append(result, candidate)
		}
	}
	return result, nil
}

func aggregateTargetLabelNames(candidates []TargetLabelOverlay, term string, limit int) []SilenceEditorLabelNameStat {
	term = strings.ToLower(strings.TrimSpace(term))
	valuesByName := map[string]map[string]struct{}{}
	for _, candidate := range candidates {
		for name, value := range candidate.Labels {
			if term != "" && !strings.HasPrefix(strings.ToLower(name), term) {
				continue
			}
			if valuesByName[name] == nil {
				valuesByName[name] = map[string]struct{}{}
			}
			valuesByName[name][value] = struct{}{}
		}
	}
	items := make([]SilenceEditorLabelNameStat, 0, len(valuesByName))
	for name, values := range valuesByName {
		items = append(items, SilenceEditorLabelNameStat{Name: name, Values: len(values)})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items
}

func countTargetLabelNames(candidates []TargetLabelOverlay, term string) int {
	return len(aggregateTargetLabelNames(candidates, term, 0))
}

func aggregateTargetLabelValues(candidates []TargetLabelOverlay, name, term string, limit int) ([]SilenceEditorLabelValueStat, int) {
	term = strings.ToLower(strings.TrimSpace(term))
	hits := map[string]int{}
	for _, candidate := range candidates {
		value, ok := candidate.Labels[name]
		if !ok || (term != "" && !strings.HasPrefix(strings.ToLower(value), term)) {
			continue
		}
		hits[value]++
	}
	items := make([]SilenceEditorLabelValueStat, 0, len(hits))
	for value, count := range hits {
		items = append(items, SilenceEditorLabelValueStat{Value: value, Hits: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Hits != items[j].Hits {
			return items[i].Hits > items[j].Hits
		}
		return items[i].Value < items[j].Value
	})
	total := len(items)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, total
}

func (s *Store) listTargetContextLabelCandidatesWithLive(target string, matchers []models.SilenceMatcher, live []TargetLabelOverlay) ([]TargetLabelOverlay, error) {
	seedIDs, err := s.targetExactContextCandidateIDs(target, matchers, live)
	if err != nil {
		return nil, err
	}
	candidates, err := s.listTargetLabelCandidatesWithLive(target, live, seedIDs)
	if err != nil {
		return nil, err
	}
	result := make([]TargetLabelOverlay, 0, len(candidates))
	for _, candidate := range candidates {
		matches := true
		for _, matcher := range matchers {
			if !matcher.IsMatch(candidate.Labels) {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, candidate)
		}
	}
	return result, nil
}

func (s *Store) targetExactContextCandidateIDs(target string, matchers []models.SilenceMatcher, live []TargetLabelOverlay) ([]string, error) {
	exact := make([]models.SilenceMatcher, 0, len(matchers))
	for _, matcher := range matchers {
		if matcher.IsEqual && !matcher.IsRegex {
			exact = append(exact, matcher)
		}
	}
	if len(exact) == 0 {
		return nil, nil
	}
	var query strings.Builder
	query.WriteString(`SELECT l.alert_id FROM alert_target_match_labels l WHERE l.target=? AND l.label_name=? AND l.label_value=?`)
	args := []any{target, exact[0].Name, exact[0].Value}
	for _, matcher := range exact[1:] {
		query.WriteString(` AND EXISTS (
SELECT 1 FROM alert_target_match_labels candidate
WHERE candidate.alert_id=l.alert_id AND candidate.target=l.target AND candidate.label_name=? AND candidate.label_value=?
)`)
		args = append(args, matcher.Name, matcher.Value)
	}
	rows, err := s.db.Query(query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to select exact context candidates: %w", err)
	}
	defer rows.Close()
	ids := map[string]struct{}{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed scanning exact context candidate: %w", err)
		}
		ids[id] = struct{}{}
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating exact context candidates: %w", err)
	}
	for _, overlay := range live {
		matches := true
		for _, matcher := range exact {
			if !matcher.IsMatch(overlay.Labels) {
				matches = false
				break
			}
		}
		if matches {
			ids[overlay.AlertID] = struct{}{}
		}
	}
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}

func (s *Store) listTargetLabelCandidatesWithLive(target string, live []TargetLabelOverlay, candidateIDs []string) ([]TargetLabelOverlay, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, errors.New("target cannot be empty")
	}
	liveJSON, err := json.Marshal(live)
	if err != nil {
		return nil, fmt.Errorf("failed to encode live target labels: %w", err)
	}
	if candidateIDs != nil && len(candidateIDs) == 0 {
		return []TargetLabelOverlay{}, nil
	}
	query := `
WITH live_input AS MATERIALIZED (
  SELECT json_extract(value, '$.alertID') AS alert_id,
         json_extract(value, '$.labels') AS labels_json,
         json_extract(value, '$.matchLabelsFingerprint') AS match_labels_fingerprint
  FROM json_each(?)
),
live_alerts AS MATERIALIZED (
  SELECT input.alert_id, input.labels_json
  FROM live_input input
  LEFT JOIN alert_target_states state ON state.alert_id=input.alert_id AND state.target=?
  WHERE COALESCE(input.match_labels_fingerprint, '')=''
     OR state.alert_id IS NULL
     OR state.retired_at IS NOT NULL
     OR state.match_labels_fingerprint<>input.match_labels_fingerprint
),
live_labels AS (
  SELECT live.alert_id, labels.key AS label_name, CAST(labels.value AS TEXT) AS label_value
  FROM live_alerts live, json_each(live.labels_json) labels
),
stored_labels AS (
  SELECT l.alert_id, l.label_name, l.label_value
  FROM alert_target_match_labels l
  LEFT JOIN live_alerts live ON live.alert_id=l.alert_id
  WHERE l.target=? AND live.alert_id IS NULL%s
)
SELECT alert_id, label_name, label_value FROM stored_labels
UNION ALL
SELECT alert_id, label_name, label_value FROM live_labels
`
	args := []any{string(liveJSON), target, target}
	if candidateIDs != nil {
		candidateJSON, marshalErr := json.Marshal(candidateIDs)
		if marshalErr != nil {
			return nil, fmt.Errorf("failed to encode candidate IDs: %w", marshalErr)
		}
		query = fmt.Sprintf(query, ` AND l.alert_id IN (SELECT value FROM json_each(?))`)
		args = append(args, string(candidateJSON))
	} else {
		query = fmt.Sprintf(query, "")
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to load target label candidates: %w", err)
	}
	defer rows.Close()
	labelsByAlert := map[string]map[string]string{}
	for rows.Next() {
		var alertID, name, value string
		if err = rows.Scan(&alertID, &name, &value); err != nil {
			return nil, fmt.Errorf("failed scanning target label candidate: %w", err)
		}
		if labelsByAlert[alertID] == nil {
			labelsByAlert[alertID] = map[string]string{}
		}
		labelsByAlert[alertID][name] = value
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating target label candidates: %w", err)
	}
	requested := map[string]struct{}{}
	for _, id := range candidateIDs {
		requested[id] = struct{}{}
	}
	result := make([]TargetLabelOverlay, 0, len(labelsByAlert))
	for alertID, labels := range labelsByAlert {
		if candidateIDs == nil {
			result = append(result, TargetLabelOverlay{AlertID: alertID, Labels: labels})
			continue
		}
		if _, ok := requested[alertID]; ok {
			result = append(result, TargetLabelOverlay{AlertID: alertID, Labels: labels})
		}
	}
	return result, nil
}

func scanTargetLabelValueStats(rows *sql.Rows, err error) ([]SilenceEditorLabelValueStat, int, error) {
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list target label values: %w", err)
	}
	defer rows.Close()
	out := []SilenceEditorLabelValueStat{}
	total := 0
	for rows.Next() {
		var row SilenceEditorLabelValueStat
		if err = rows.Scan(&row.Value, &row.Hits, &total); err != nil {
			return nil, 0, fmt.Errorf("failed scanning target label value row: %w", err)
		}
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed iterating target label values: %w", err)
	}
	return out, total, nil
}

func (s *Store) RunHousekeeping(retentionTTL time.Duration, maxAlerts int) (HousekeepingResult, error) {
	return s.RunHousekeepingContext(context.Background(), retentionTTL, maxAlerts)
}

func (s *Store) RunHousekeepingContext(ctx context.Context, retentionTTL time.Duration, maxAlerts int) (HousekeepingResult, error) {
	if s == nil || s.db == nil {
		return HousekeepingResult{}, ErrStoreNotInitialized
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if retentionTTL <= 0 {
		return HousekeepingResult{}, errors.New("retentionTTL must be > 0")
	}
	if maxAlerts < 1 {
		return HousekeepingResult{}, errors.New("maxAlerts must be >= 1")
	}

	runAt := time.Now().UTC()
	cutoff := runAt.Add(-retentionTTL)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return HousekeepingResult{}, fmt.Errorf("failed to begin housekeeping transaction: %w", err)
	}

	expiredDeleted, err := deleteExpiredAlertsTx(tx, cutoff)
	if err != nil {
		_ = tx.Rollback()
		return HousekeepingResult{}, err
	}

	limitDeleted, err := enforceMaxAlertsTx(tx, maxAlerts)
	if err != nil {
		_ = tx.Rollback()
		return HousekeepingResult{}, err
	}

	retention, err := runPhase2RetentionTx(ctx, tx, runAt, s.silenceHistoryRetentionTTL)
	if err != nil {
		_ = tx.Rollback()
		return HousekeepingResult{}, err
	}

	if err = setMetadataTx(tx, metaLastHousekeepingAt, runAt.Format(time.RFC3339Nano)); err != nil {
		_ = tx.Rollback()
		return HousekeepingResult{}, err
	}

	if err = tx.Commit(); err != nil {
		return HousekeepingResult{}, fmt.Errorf("failed to commit housekeeping transaction: %w", err)
	}

	vacuumPagesFreed, err := runIncrementalVacuum(s.db)
	if err != nil {
		return HousekeepingResult{}, err
	}

	vacuumAt := time.Now().UTC()
	if err = s.setMetadata(metaLastIncrementalVacuumAt, vacuumAt.Format(time.RFC3339Nano)); err != nil {
		return HousekeepingResult{}, err
	}

	stats, err := s.GetStats()
	if err != nil {
		return HousekeepingResult{}, err
	}

	return HousekeepingResult{
		ExpiredDeleted:        expiredDeleted,
		LimitDeleted:          limitDeleted,
		RecheckJobsDeleted:    retention.recheckJobs,
		RetiredSourcesDeleted: retention.retiredSources,
		RevisionsDeleted:      retention.revisions,
		EpisodesDeleted:       retention.episodes,
		RulesDeleted:          retention.rules,
		VacuumPagesFreed:      vacuumPagesFreed,
		CurrentRowsAfter:      stats.CurrentRows,
		HistoryRowsAfter:      stats.HistoryRows,
		RunAt:                 runAt,
		LastIncrementalVacuum: vacuumAt,
	}, nil
}

func (s *Store) GetStats() (Stats, error) {
	if s == nil || s.db == nil {
		return Stats{}, ErrStoreNotInitialized
	}

	currentRows, err := queryInt(s.db, `SELECT COUNT(*) FROM alert_states`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to count current alerts: %w", err)
	}
	stateCounts, err := s.queryStateCounts()
	if err != nil {
		return Stats{}, err
	}
	suppressedCurrent, suppressedPast, err := s.querySuppressedBreakdown()
	if err != nil {
		return Stats{}, err
	}
	historyRows, err := queryInt(s.db, `SELECT COUNT(*) FROM alert_events`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to count state history rows: %w", err)
	}
	pageCount, err := queryInt(s.db, `PRAGMA page_count`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to query sqlite page_count: %w", err)
	}
	pageSize, err := queryInt(s.db, `PRAGMA page_size`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to query sqlite page_size: %w", err)
	}
	freelistCount, err := queryInt(s.db, `PRAGMA freelist_count`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to query sqlite freelist_count: %w", err)
	}

	lastHousekeepingAt, err := s.getOptionalMetadataTime(metaLastHousekeepingAt)
	if err != nil {
		return Stats{}, err
	}
	lastVacuumAt, err := s.getOptionalMetadataTime(metaLastIncrementalVacuumAt)
	if err != nil {
		return Stats{}, err
	}
	groupingFingerprint, err := s.getMetadataString(metaGroupingFingerprint)
	if err != nil {
		return Stats{}, err
	}
	regroupTargetFingerprint, err := s.getMetadataString(metaRegroupTargetFingerprint)
	if err != nil {
		return Stats{}, err
	}
	regroupStatus, err := s.getMetadataString(metaRegroupStatus)
	if err != nil {
		return Stats{}, err
	}
	regroupError, err := s.getMetadataString(metaRegroupError)
	if err != nil {
		return Stats{}, err
	}
	regroupProcessed, err := s.getMetadataInt(metaRegroupProcessed)
	if err != nil {
		return Stats{}, err
	}
	regroupTotal, err := s.getMetadataInt(metaRegroupTotal)
	if err != nil {
		return Stats{}, err
	}
	regroupStartedAt, err := s.getOptionalMetadataTime(metaRegroupStartedAt)
	if err != nil {
		return Stats{}, err
	}
	regroupFinishedAt, err := s.getOptionalMetadataTime(metaRegroupFinishedAt)
	if err != nil {
		return Stats{}, err
	}
	inconsistentClusters, err := s.queryInconsistentSilenceClusters()
	if err != nil {
		return Stats{}, err
	}
	recheckStats, err := s.queryRecheckStats()
	if err != nil {
		return Stats{}, err
	}

	stat, err := os.Stat(s.sqlitePath())
	fileSize := int64(0)
	if err == nil {
		fileSize = stat.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return Stats{}, fmt.Errorf("failed to stat sqlite file: %w", err)
	}

	if fileSize == 0 && pageCount > 0 && pageSize > 0 {
		estimated := int64(pageCount) * int64(pageSize)
		if estimated > 0 {
			fileSize = estimated
		}
	}

	return Stats{
		CurrentRows:                 currentRows,
		HistoryRows:                 historyRows,
		StateCounts:                 stateCounts,
		SuppressedBreakdownCurrent:  suppressedCurrent,
		SuppressedBreakdownPast:     suppressedPast,
		GroupingFingerprint:         strings.TrimSpace(groupingFingerprint),
		RegroupTargetFingerprint:    strings.TrimSpace(regroupTargetFingerprint),
		RegroupStatus:               strings.TrimSpace(regroupStatus),
		RegroupError:                strings.TrimSpace(regroupError),
		RegroupProcessed:            regroupProcessed,
		RegroupTotal:                regroupTotal,
		RegroupStartedAt:            regroupStartedAt,
		RegroupFinishedAt:           regroupFinishedAt,
		FileSizeBytes:               fileSize,
		PageCount:                   pageCount,
		PageSizeBytes:               pageSize,
		FreelistCount:               freelistCount,
		LastHousekeepingAt:          lastHousekeepingAt,
		LastIncrementalVacuumAt:     lastVacuumAt,
		SilenceInconsistentClusters: inconsistentClusters,
		RecheckQueueDepth:           recheckStats.queueDepth,
		RecheckCurrentJobID:         recheckStats.jobID,
		RecheckCurrentStatus:        recheckStats.status,
		RecheckCurrentProcessed:     recheckStats.processed,
		RecheckCurrentPromoted:      recheckStats.promoted,
		RecheckCurrentSkipped:       recheckStats.skipped,
		RecheckCurrentError:         recheckStats.lastError,
		RecheckOldestCreatedAt:      recheckStats.oldestCreatedAt,
	}, nil
}

type recheckStatsSnapshot struct {
	queueDepth      int
	jobID           int64
	status          string
	processed       int
	promoted        int
	skipped         int
	lastError       string
	oldestCreatedAt *time.Time
}

func (s *Store) queryInconsistentSilenceClusters() ([]string, error) {
	rows, err := s.db.Query(`SELECT cluster_id FROM alertmanager_cluster_consistency WHERE status='inconsistent' ORDER BY cluster_id`)
	if err != nil {
		return nil, fmt.Errorf("failed to query inconsistent silence clusters: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var clusterID string
		if err = rows.Scan(&clusterID); err != nil {
			return nil, fmt.Errorf("failed to scan inconsistent silence cluster: %w", err)
		}
		out = append(out, clusterID)
	}
	return out, rows.Err()
}

func (s *Store) queryRecheckStats() (recheckStatsSnapshot, error) {
	var out recheckStatsSnapshot
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs WHERE status IN ('queued','running','failed')`).Scan(&out.queueDepth); err != nil {
		return out, fmt.Errorf("failed to query recheck queue depth: %w", err)
	}
	var createdAtRaw, lastError sql.NullString
	err := s.db.QueryRow(`
SELECT job_id, status, processed_candidates, promoted_alerts,
       skipped_ineligible + skipped_missing_labels + skipped_incomplete_sources + stale_conflicts,
       last_error, created_at
FROM silence_recheck_jobs
WHERE status IN ('queued','running','failed')
ORDER BY job_id ASC
LIMIT 1
`).Scan(&out.jobID, &out.status, &out.processed, &out.promoted, &out.skipped, &lastError, &createdAtRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return out, nil
	}
	if err != nil {
		return out, fmt.Errorf("failed to query current recheck job: %w", err)
	}
	out.lastError = lastError.String
	if createdAtRaw.Valid {
		parsed, parseErr := time.Parse(time.RFC3339Nano, createdAtRaw.String)
		if parseErr != nil {
			return out, fmt.Errorf("failed to parse oldest recheck job timestamp: %w", parseErr)
		}
		out.oldestCreatedAt = &parsed
	}
	return out, nil
}

func (s *Store) queryStateCounts() (map[string]int, error) {
	out := map[string]int{}

	rows, err := s.db.Query(`
SELECT state, COUNT(*)
FROM alert_states
GROUP BY state
`)
	if err != nil {
		return nil, fmt.Errorf("failed to query per-state counts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var raw string
		var count int
		if scanErr := rows.Scan(&raw, &count); scanErr != nil {
			return nil, fmt.Errorf("failed to scan per-state counts row: %w", scanErr)
		}
		state := strings.TrimSpace(raw)
		if state == "" {
			state = "unknown"
		}
		out[state] = count
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate per-state counts: %w", err)
	}

	return out, nil
}

func (s *Store) querySuppressedBreakdown() (int, int, error) {
	rows, err := s.db.Query(`
SELECT is_live, COUNT(*)
FROM alert_states
WHERE state = ?
GROUP BY is_live
`, string(StateSuppressed))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query suppressed breakdown: %w", err)
	}
	defer rows.Close()

	current := 0
	past := 0
	for rows.Next() {
		var isLiveRaw int
		var count int
		if scanErr := rows.Scan(&isLiveRaw, &count); scanErr != nil {
			return 0, 0, fmt.Errorf("failed to scan suppressed breakdown row: %w", scanErr)
		}
		if isLiveRaw != 0 {
			current += count
		} else {
			past += count
		}
	}
	if err = rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("failed to iterate suppressed breakdown rows: %w", err)
	}

	return current, past, nil
}

func (s *Store) ResolveAlert(alertID string, changedBy, reason *string, changedAt time.Time) (*ResolveResult, error) {
	return s.ResolveAlertContext(context.Background(), alertID, changedBy, reason, changedAt)
}

func (s *Store) ResolveAlertContext(ctx context.Context, alertID string, changedBy, reason *string, changedAt time.Time) (*ResolveResult, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if strings.TrimSpace(alertID) == "" {
		return nil, ErrInvalidAlertID
	}

	ts := changedAt.UTC()
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin resolve transaction: %w", err)
	}

	current, err := getCurrentAlertTx(tx, alertID)
	if err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAlertNotFound
		}
		return nil, err
	}

	resolvedPlan, resolveApplied := ResolveDimensions(LifecycleDimensions{
		RawState: current.RawState, IsLive: current.IsLive,
		IsSuppressed: current.IsSuppressed, IsResolved: current.IsResolved,
	})
	if !resolveApplied && current.IsResolved {
		_ = tx.Rollback()
		return &ResolveResult{
			AlertID:   alertID,
			OldState:  current.State,
			NewState:  current.State,
			Applied:   false,
			ChangedAt: ts,
			ChangedBy: sanitizeNullableString(changedBy),
		}, nil
	}

	if !resolveApplied {
		_ = tx.Rollback()
		return nil, ErrResolveStateConflict
	}

	cleanChangedBy := sanitizeNullableString(changedBy)
	cleanComment := sanitizeNullableString(reason)
	if err = updateManualResolutionTx(tx, alertID, cleanChangedBy, cleanComment, ts); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err = updateTargetStatesResolvedTx(tx, alertID, true, ts, "manual_mark_resolved"); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	newRaw := resolvedPlan.RawState
	newState := resolvedPlan.ProjectedState()
	oldLive := current.IsLive
	newLive := resolvedPlan.IsLive
	oldResolved := current.IsResolved
	newResolved := resolvedPlan.IsResolved
	reasonCode := sanitizeReasonCode(ptrString("manual_mark_resolved"))
	if err = insertTransitionTx(tx, AlertStateTransition{
		AlertID:         alertID,
		OldState:        &current.State,
		NewState:        newState,
		OldRawState:     &current.RawState,
		NewRawState:     newRaw,
		OldIsLive:       &oldLive,
		NewIsLive:       newLive,
		OldIsSuppressed: &current.IsSuppressed,
		NewIsSuppressed: current.IsSuppressed,
		OldIsResolved:   &oldResolved,
		NewIsResolved:   newResolved,
		ChangedAt:       ts,
		ChangeSource:    ChangeSourceManual,
		ChangedBy:       cleanChangedBy,
		ReasonCode:      reasonCode,
		ReasonText:      cleanComment,
	}); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit resolve transaction: %w", err)
	}

	return &ResolveResult{
		AlertID:   alertID,
		OldState:  current.State,
		NewState:  newState,
		Applied:   true,
		ChangedAt: ts,
		ChangedBy: cleanChangedBy,
	}, nil
}

func (s *Store) ApplyPollCycle(input PollCycleInput) (PollCycleResult, error) {
	if s == nil || s.db == nil {
		return PollCycleResult{}, ErrStoreNotInitialized
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	now := input.Timestamp.UTC()
	configuredSources, sourceManifestKnown := configuredSourceNames(input.SilenceSources)
	configuredSourcesByTarget := configuredSourceNamesByTarget(input.SilenceSources)
	if now.IsZero() {
		now = time.Now().UTC()
	}

	liveByID := make(map[string]LiveAlertSnapshot, len(input.LiveAlerts))
	allSuccessful := areAllUpstreamsSuccessful(input.SuccessfulUpstreams)
	for _, live := range input.LiveAlerts {
		if len(live.RelevantUpstreams) == 0 && len(live.RelevantSources) > 0 {
			live.RelevantUpstreams = make([]string, 0, len(live.RelevantSources))
			for _, src := range live.RelevantSources {
				live.RelevantUpstreams = append(live.RelevantUpstreams, src.Name)
			}
		}
		if len(live.RelevantSources) == 0 && len(live.RelevantUpstreams) > 0 {
			live.RelevantSources = make([]SourceRef, 0, len(live.RelevantUpstreams))
			for _, name := range live.RelevantUpstreams {
				trimmed := strings.TrimSpace(name)
				if trimmed == "" {
					continue
				}
				live.RelevantSources = append(live.RelevantSources, SourceRef{Name: trimmed, Cluster: "__unknown__"})
			}
		}
		if strings.TrimSpace(live.GroupLabelsJSON) == "" {
			live.GroupLabelsJSON = "{}"
		}
		if err := validateLiveSnapshot(live); err != nil {
			return PollCycleResult{}, err
		}
		liveByID[live.AlertID] = live
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PollCycleResult{}, fmt.Errorf("failed to begin poll cycle transaction: %w", err)
	}

	result := PollCycleResult{}
	reconciliation, qerr := reconcileSilenceSourcesWithEventsTx(tx, input.SilenceSources, now)
	if qerr != nil {
		_ = tx.Rollback()
		return PollCycleResult{}, qerr
	}
	result.RecheckJobsQueued = reconciliation.queued

	pollEvents := append([]runtimeEvent(nil), reconciliation.events...)
	currentByID, err := readAllCurrentAlertsTx(tx)
	if err != nil {
		_ = tx.Rollback()
		return PollCycleResult{}, err
	}

	for _, inputLive := range input.LiveAlerts {
		live := liveByID[inputLive.AlertID]
		targets := normalizeLiveTargets(live)
		current, found := currentByID[live.AlertID]
		if !found {
			if err = insertCurrentAlertTx(tx, live, now); err != nil {
				_ = tx.Rollback()
				return PollCycleResult{}, err
			}
			newProjected := projectedState(live.State, true, live.State == StateSuppressed, false)
			newLive := true
			newResolved := false
			if err = insertTransitionTx(tx, AlertStateTransition{
				AlertID:         live.AlertID,
				OldState:        nil,
				NewState:        newProjected,
				OldRawState:     nil,
				NewRawState:     live.State,
				OldIsLive:       nil,
				NewIsLive:       newLive,
				OldIsSuppressed: nil,
				NewIsSuppressed: live.State == StateSuppressed,
				OldIsResolved:   nil,
				NewIsResolved:   newResolved,
				ChangedAt:       now,
				ChangeSource:    ChangeSourcePoller,
				ReasonCode:      sanitizeReasonCode(ptrString("poll_first_seen")),
			}); err != nil {
				_ = tx.Rollback()
				return PollCycleResult{}, err
			}
			result.Inserted++
			result.Transitions++
			if err = upsertTargetStatesFromLiveTx(tx, live.AlertID, targets, live.MatchLabelsJSON, input.SuccessfulUpstreams, configuredSourcesByTarget, sourceManifestKnown, now); err != nil {
				_ = tx.Rollback()
				return PollCycleResult{}, err
			}
			if live.State == StateSuppressed {
				if err = insertLiveSuppressionCauseTx(tx, live.AlertID, live.SilencedBySources, now); err != nil {
					_ = tx.Rollback()
					return PollCycleResult{}, err
				}
			}
			if err = insertLiveTargetSuppressionCausesTx(tx, live.AlertID, live.Targets, live.SilencedBySources, now); err != nil {
				_ = tx.Rollback()
				return PollCycleResult{}, err
			}
			continue
		}

		livePlan := ObserveLive(LifecycleDimensions{
			RawState: current.RawState, IsLive: current.IsLive,
			IsSuppressed: current.IsSuppressed, IsResolved: current.IsResolved,
		}, live.State)
		newRawState := livePlan.RawState
		newIsLive := livePlan.IsLive
		newIsResolved := livePlan.IsResolved
		newIsSuppressed := livePlan.IsSuppressed
		newState := projectedState(newRawState, newIsLive, newIsSuppressed, newIsResolved)
		if current.IsResolved {
			result.Reactivated++
		}

		existingRelevant, parseErr := parseRelevantUpstreams(current.RelevantUpstreams)
		if parseErr != nil {
			existingRelevant = nil
		}
		storedRelevant := selectStoredRelevantUpstreams(existingRelevant, live.RelevantUpstreams, allSuccessful)
		relevantJSON, marshalErr := marshalRelevantUpstreams(storedRelevant)
		if marshalErr != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, marshalErr
		}

		existingRelevantSources, parseSourcesErr := parseRelevantSources(current.RelevantSources)
		if parseSourcesErr != nil {
			existingRelevantSources = nil
		}
		storedRelevantSources := selectStoredRelevantSources(existingRelevantSources, live.RelevantSources, allSuccessful)
		relevantSourcesJSON, marshalSourcesErr := marshalRelevantSources(storedRelevantSources)
		if marshalSourcesErr != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, marshalSourcesErr
		}

		if err = updateCurrentAlertFromLiveTx(tx, current.AlertID, live, newRawState, newIsLive, newIsSuppressed, newIsResolved, string(relevantJSON), string(relevantSourcesJSON), now); err != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, err
		}
		result.Updated++
		if err = upsertTargetStatesFromLiveTx(tx, current.AlertID, targets, live.MatchLabelsJSON, input.SuccessfulUpstreams, configuredSourcesByTarget, sourceManifestKnown, now); err != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, err
		}

		if current.State != newState || current.RawState != newRawState || current.IsLive != newIsLive || current.IsSuppressed != newIsSuppressed || current.IsResolved != newIsResolved {
			old := current.State
			oldRaw := current.RawState
			oldLive := current.IsLive
			oldResolved := current.IsResolved
			reasonCode := "poll_raw_state_changed"
			if !oldLive {
				reasonCode = "poll_live_reappeared"
			}
			if oldResolved {
				reasonCode = "poll_reappear_clears_resolved"
			}
			if err = insertTransitionTx(tx, AlertStateTransition{
				AlertID:         current.AlertID,
				OldState:        &old,
				NewState:        newState,
				OldRawState:     &oldRaw,
				NewRawState:     newRawState,
				OldIsLive:       &oldLive,
				NewIsLive:       newIsLive,
				OldIsSuppressed: &current.IsSuppressed,
				NewIsSuppressed: newIsSuppressed,
				OldIsResolved:   &oldResolved,
				NewIsResolved:   newIsResolved,
				ChangedAt:       now,
				ChangeSource:    ChangeSourcePoller,
				ReasonCode:      sanitizeReasonCode(&reasonCode),
			}); err != nil {
				_ = tx.Rollback()
				return PollCycleResult{}, err
			}
			result.Transitions++
		}
		if newIsLive && newIsSuppressed {
			if err = insertLiveSuppressionCauseTx(tx, current.AlertID, live.SilencedBySources, now); err != nil {
				_ = tx.Rollback()
				return PollCycleResult{}, err
			}
		}
		if err = insertLiveTargetSuppressionCausesTx(tx, current.AlertID, live.Targets, live.SilencedBySources, now); err != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, err
		}
	}

	for alertID, current := range currentByID {
		if _, exists := liveByID[alertID]; exists {
			continue
		}

		if err = markMissingTargetStatesNonLiveTx(tx, current, input.SuccessfulUpstreams, configuredSourcesByTarget, sourceManifestKnown, now); err != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, err
		}

		if !current.IsLive {
			continue
		}

		relevant, parseErr := parseRelevantUpstreams(current.RelevantUpstreams)
		if parseErr != nil {
			result.SkippedInactive++
			pollEvents = append(pollEvents, runtimeEvent{level: slog.LevelDebug, component: "alert_lifecycle", event: "alert_absence_deferred", message: "Alert absence transition deferred", attrs: []slog.Attr{slog.String("alert_id", alertID), slog.String("reason_code", "invalid_stored_sources")}})
			continue
		}
		relevant = filterConfiguredSourceNames(relevant, configuredSources, sourceManifestKnown)

		if !allRelevantUpstreamsSuccessful(relevant, input.SuccessfulUpstreams) {
			result.SkippedInactive++
			pollEvents = append(pollEvents, runtimeEvent{level: slog.LevelDebug, component: "alert_lifecycle", event: "alert_absence_deferred", message: "Alert absence transition deferred", attrs: []slog.Attr{slog.String("alert_id", alertID), slog.String("reason_code", "incomplete_source_coverage")}})
			continue
		}

		absencePlan := ObserveAbsent(LifecycleDimensions{
			RawState: current.RawState, IsLive: current.IsLive,
			IsSuppressed: current.IsSuppressed, IsResolved: current.IsResolved,
		})
		newRawState := absencePlan.RawState
		newIsLive := absencePlan.IsLive
		newIsSuppressed := absencePlan.IsSuppressed
		newIsResolved := absencePlan.IsResolved
		newState := projectedState(newRawState, newIsLive, newIsSuppressed, newIsResolved)
		if err = updateProjectedStateTx(tx, alertID, newRawState, newIsLive, newIsSuppressed, newIsResolved, now); err != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, err
		}
		old := current.State
		oldRaw := current.RawState
		oldLive := current.IsLive
		oldResolved := current.IsResolved
		if err = insertTransitionTx(tx, AlertStateTransition{
			AlertID:         alertID,
			OldState:        &old,
			NewState:        newState,
			OldRawState:     &oldRaw,
			NewRawState:     newRawState,
			OldIsLive:       &oldLive,
			NewIsLive:       newIsLive,
			OldIsSuppressed: &current.IsSuppressed,
			NewIsSuppressed: newIsSuppressed,
			OldIsResolved:   &oldResolved,
			NewIsResolved:   newIsResolved,
			ChangedAt:       now,
			ChangeSource:    ChangeSourcePoller,
			ReasonCode:      sanitizeReasonCode(ptrString("poll_live_disappeared")),
		}); err != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, err
		}
		if newState == StateInactive {
			result.SetInactive++
			if err = enqueueAlertForCurrentRecheckContextTx(tx, alertID, now); err != nil {
				_ = tx.Rollback()
				return PollCycleResult{}, err
			}
		}
		result.Transitions++
	}

	if err = tx.Commit(); err != nil {
		return PollCycleResult{}, fmt.Errorf("failed to commit poll cycle transaction: %w", err)
	}
	emitRuntimeEvents(pollEvents)
	logCommittedPollEvents(s.db, now)

	return result, nil
}

type runtimeEvent struct {
	level     slog.Level
	component string
	event     string
	message   string
	attrs     []slog.Attr
}

func emitRuntimeEvents(events []runtimeEvent) {
	emitRuntimeEventsContext(context.Background(), events)
}

func emitRuntimeEventsContext(ctx context.Context, events []runtimeEvent) {
	for _, event := range events {
		runtimeLog.Event(ctx, event.level, event.component, event.event, event.message, event.attrs...)
	}
}

func logCommittedPollEvents(db *sql.DB, changedAt time.Time) {
	if db == nil {
		return
	}
	changedRaw := changedAt.UTC().Format(time.RFC3339Nano)
	if !runtimeLog.Enabled(context.Background(), slog.LevelInfo) {
		return
	}
	rows, err := db.Query(`
SELECT alert_id, COALESCE(old_state, ''), new_state, old_is_live, new_is_live,
       old_is_suppressed, new_is_suppressed, old_is_resolved, new_is_resolved,
       COALESCE(reason_code, '')
FROM alert_events WHERE changed_at=? ORDER BY id
`, changedRaw)
	if err != nil {
		runtimeLog.Error("alert_lifecycle", "transition_log_failed", "Failed to load committed alert transitions for logging", slog.Any("error", err))
		return
	}
	for rows.Next() {
		var alertID, oldState, newState, reason string
		var oldLive, oldSuppressed, oldResolved sql.NullBool
		var newLive, newSuppressed, newResolved bool
		if scanErr := rows.Scan(&alertID, &oldState, &newState, &oldLive, &newLive, &oldSuppressed, &newSuppressed, &oldResolved, &newResolved, &reason); scanErr != nil {
			rows.Close()
			runtimeLog.Error("alert_lifecycle", "transition_log_failed", "Failed to scan committed alert transition for logging", slog.Any("error", scanErr))
			return
		}
		runtimeLog.Debug("alert_lifecycle", "alert_transition", "Alert lifecycle state changed",
			slog.String("alert_id", alertID), slog.String("old_state", oldState), slog.String("new_state", newState),
			slog.Any("old_is_live", nullableBoolValue(oldLive)), slog.Bool("new_is_live", newLive),
			slog.Any("old_is_suppressed", nullableBoolValue(oldSuppressed)), slog.Bool("new_is_suppressed", newSuppressed),
			slog.Any("old_is_resolved", nullableBoolValue(oldResolved)), slog.Bool("new_is_resolved", newResolved),
			slog.String("reason_code", reason))
	}
	rows.Close()

	targetRows, err := db.Query(`
SELECT alert_id, target, COALESCE(old_state, ''), new_state, COALESCE(reason_code, '')
FROM alert_target_events WHERE changed_at=? ORDER BY id
`, changedRaw)
	if err != nil {
		runtimeLog.Error("alert_lifecycle", "target_transition_log_failed", "Failed to load committed target transitions for logging", slog.Any("error", err))
		return
	}
	defer targetRows.Close()
	for targetRows.Next() {
		var alertID, target, oldState, newState, reason string
		if scanErr := targetRows.Scan(&alertID, &target, &oldState, &newState, &reason); scanErr != nil {
			runtimeLog.Error("alert_lifecycle", "target_transition_log_failed", "Failed to scan committed target transition for logging", slog.Any("error", scanErr))
			return
		}
		event := "target_transition"
		level := slog.LevelDebug
		if reason == "poll_target_retired" {
			event = "target_retired"
			level = slog.LevelInfo
		}
		runtimeLog.Event(context.Background(), level, "alert_lifecycle", event, "Alert target lifecycle state changed", slog.String("alert_id", alertID), slog.String("target", target), slog.String("old_state", oldState), slog.String("new_state", newState), slog.String("reason_code", reason))
	}
	logCommittedSilenceHistoryEvents(db, changedRaw)
}

func nullableBoolValue(value sql.NullBool) any {
	if !value.Valid {
		return nil
	}
	return value.Bool
}

func logCommittedSilenceHistoryEvents(db *sql.DB, changedRaw string) {
	logRows := func(query, event, message string, level slog.Level, scan func(*sql.Rows) ([]slog.Attr, error)) {
		rows, err := db.Query(query, changedRaw)
		if err != nil {
			runtimeLog.Error("silence_reconciliation", "history_log_failed", "Failed to load committed silence history for logging", slog.String("history_event", event), slog.Any("error", err))
			return
		}
		defer rows.Close()
		for rows.Next() {
			attrs, scanErr := scan(rows)
			if scanErr != nil {
				runtimeLog.Error("silence_reconciliation", "history_log_failed", "Failed to scan committed silence history for logging", slog.String("history_event", event), slog.Any("error", scanErr))
				return
			}
			runtimeLog.Event(context.Background(), level, "silence_reconciliation", event, message, attrs...)
		}
	}
	logRows(`SELECT r.cluster_id, r.silence_id, r.rule_id, COALESCE((SELECT ss.observed_state FROM alertmanager_source_silences ss WHERE ss.rule_id=r.rule_id ORDER BY ss.source_name LIMIT 1), '') FROM silence_rules r WHERE r.first_observed_at=?`, "rule_discovered", "Silence rule discovered", slog.LevelDebug, func(rows *sql.Rows) ([]slog.Attr, error) {
		var target, silenceID, status string
		var ruleID int64
		err := rows.Scan(&target, &silenceID, &ruleID, &status)
		return []slog.Attr{slog.String("target", target), slog.String("silence_id", silenceID), slog.Int64("rule_id", ruleID), slog.String("status", status)}, err
	})
	logRows(`SELECT r.cluster_id, r.silence_id, e.episode_id, e.episode_number FROM silence_rule_episodes e JOIN silence_rules r ON r.rule_id=e.rule_id WHERE e.first_observed_at=?`, "rule_episode_opened", "Silence rule episode opened", slog.LevelDebug, func(rows *sql.Rows) ([]slog.Attr, error) {
		var target, silenceID string
		var episodeID int64
		var episodeNumber int
		err := rows.Scan(&target, &silenceID, &episodeID, &episodeNumber)
		return []slog.Attr{slog.String("target", target), slog.String("silence_id", silenceID), slog.Int64("episode_id", episodeID), slog.Int("episode_number", episodeNumber)}, err
	})
	logRows(`SELECT r.cluster_id, r.silence_id, e.episode_id FROM silence_rule_episodes e JOIN silence_rules r ON r.rule_id=e.rule_id WHERE e.ended_at=?`, "rule_episode_closed", "Silence rule episode closed", slog.LevelInfo, func(rows *sql.Rows) ([]slog.Attr, error) {
		var target, silenceID string
		var episodeID int64
		err := rows.Scan(&target, &silenceID, &episodeID)
		return []slog.Attr{slog.String("target", target), slog.String("silence_id", silenceID), slog.Int64("episode_id", episodeID), slog.String("reason_code", "absent_confirmed")}, err
	})
	logRows(`SELECT r.cluster_id, r.silence_id, rev.rule_id, rev.revision_id, rev.revision_number FROM silence_revisions rev JOIN silence_rules r ON r.rule_id=rev.rule_id WHERE rev.first_observed_at=?`, "rule_revision_created", "Silence rule revision created", slog.LevelDebug, func(rows *sql.Rows) ([]slog.Attr, error) {
		var target, silenceID string
		var ruleID, revisionID int64
		var revisionNumber int
		err := rows.Scan(&target, &silenceID, &ruleID, &revisionID, &revisionNumber)
		return []slog.Attr{slog.String("target", target), slog.String("silence_id", silenceID), slog.Int64("rule_id", ruleID), slog.Int64("revision_id", revisionID), slog.Int("revision_number", revisionNumber)}, err
	})
	logRows(`SELECT job_id, total_candidates, trigger_type, (SELECT COUNT(*) FROM silence_recheck_jobs open_jobs WHERE open_jobs.status IN ('queued','running','failed')) FROM silence_recheck_jobs WHERE created_at=?`, "recheck_job_queued", "Silence recheck job queued", slog.LevelInfo, func(rows *sql.Rows) ([]slog.Attr, error) {
		var jobID int64
		var candidates int
		var trigger string
		var queueDepth int
		err := rows.Scan(&jobID, &candidates, &trigger, &queueDepth)
		return []slog.Attr{slog.Int64("job_id", jobID), slog.Int("candidate_count", candidates), slog.Int("queue_depth", queueDepth), slog.String("reason_code", trigger)}, err
	})
}

func BuildGroupingFingerprint(cfg GroupingConfig) string {
	type fpPayload struct {
		LabelsKeep       []string `json:"labelsKeep"`
		LabelsKeepRegex  []string `json:"labelsKeepRegex"`
		LabelsStrip      []string `json:"labelsStrip"`
		LabelsStripRegex []string `json:"labelsStripRegex"`
	}

	p := fpPayload{
		LabelsKeep:       normalizeStringList(cfg.LabelsKeep),
		LabelsKeepRegex:  normalizeStringList(cfg.LabelsKeepRegex),
		LabelsStrip:      normalizeStringList(cfg.LabelsStrip),
		LabelsStripRegex: normalizeStringList(cfg.LabelsStripRegex),
	}

	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum)
}

func (s *Store) RunRegroupToConfig(ctx context.Context, cfg GroupingConfig, batchSize int) (RegroupResult, error) {
	if s == nil || s.db == nil {
		return RegroupResult{}, ErrStoreNotInitialized
	}
	if batchSize < 1 {
		batchSize = 1000
	}

	targetFingerprint := BuildGroupingFingerprint(cfg)
	started, resumed, total, err := s.beginOrResumeRegroup(ctx, targetFingerprint)
	if err != nil {
		return RegroupResult{}, err
	}
	if !started {
		return RegroupResult{Total: total}, nil
	}
	if resumed {
		runtimeLog.InfoContext(ctx, "lifecycle_regroup", "regroup_resumed", "Alert lifecycle regroup resumed", slog.String("target_fingerprint", targetFingerprint), slog.Int("total", total))
	} else {
		runtimeLog.InfoContext(ctx, "lifecycle_regroup", "regroup_started", "Alert lifecycle regroup started", slog.String("target_fingerprint", targetFingerprint), slog.Int("total", total), slog.Int("batch_size", batchSize))
	}

	compiled, err := compileGroupingConfig(cfg)
	if err != nil {
		_ = s.failRegroup(ctx, targetFingerprint, fmt.Sprintf("invalid grouping config: %v", err))
		return RegroupResult{}, err
	}

	result := RegroupResult{Total: total}

	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			default:
			}
		}

		batchStartedAt := time.Now()
		done, processedInBatch, updatedInBatch, totalRows, batchErr := s.runRegroupBatch(ctx, targetFingerprint, compiled, batchSize)
		if batchErr != nil {
			_ = s.failRegroup(ctx, targetFingerprint, batchErr.Error())
			return result, batchErr
		}
		result.Processed += processedInBatch
		result.Updated += updatedInBatch
		result.Total = totalRows
		runtimeLog.DebugContext(ctx, "lifecycle_regroup", "regroup_batch_completed", "Alert lifecycle regroup batch completed", slog.Int("processed", processedInBatch), slog.Int("updated", updatedInBatch), slog.Int("processed_total", result.Processed), slog.Int("total", totalRows), slog.Duration("duration", time.Since(batchStartedAt)))

		if done {
			return result, nil
		}
	}
}

func (s *Store) beginOrResumeRegroup(ctx context.Context, targetFingerprint string) (bool, bool, int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	now := time.Now().UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, false, 0, fmt.Errorf("failed to begin regroup transaction: %w", err)
	}

	currentFingerprint, err := readMetadataStringTx(tx, metaGroupingFingerprint)
	if err != nil {
		_ = tx.Rollback()
		return false, false, 0, err
	}
	status, err := readMetadataStringTx(tx, metaRegroupStatus)
	if err != nil {
		_ = tx.Rollback()
		return false, false, 0, err
	}
	status = strings.TrimSpace(status)
	if status == "" {
		status = regroupStatusReady
	}
	activeTarget, err := readMetadataStringTx(tx, metaRegroupTargetFingerprint)
	if err != nil {
		_ = tx.Rollback()
		return false, false, 0, err
	}

	totalRows, err := queryInt(tx, `SELECT COUNT(*) FROM alert_states`)
	if err != nil {
		_ = tx.Rollback()
		return false, false, 0, fmt.Errorf("failed to count rows for regroup: %w", err)
	}

	if strings.TrimSpace(currentFingerprint) == targetFingerprint && status == regroupStatusReady {
		if err = tx.Commit(); err != nil {
			return false, false, 0, fmt.Errorf("failed to commit regroup readiness check: %w", err)
		}
		return false, false, totalRows, nil
	}

	resume := status == regroupStatusRunning && strings.TrimSpace(activeTarget) == targetFingerprint
	if !resume {
		if err = setMetadataIntTx(tx, metaRegroupProcessed, 0); err != nil {
			_ = tx.Rollback()
			return false, false, 0, err
		}
		if err = setMetadataInt64Tx(tx, metaRegroupCursorRowID, 0); err != nil {
			_ = tx.Rollback()
			return false, false, 0, err
		}
		if err = setMetadataTimeTx(tx, metaRegroupStartedAt, &now); err != nil {
			_ = tx.Rollback()
			return false, false, 0, err
		}
	}

	if err = setMetadataTx(tx, metaRegroupStatus, regroupStatusRunning); err != nil {
		_ = tx.Rollback()
		return false, false, 0, err
	}
	if err = setMetadataTx(tx, metaRegroupError, ""); err != nil {
		_ = tx.Rollback()
		return false, false, 0, err
	}
	if err = setMetadataTx(tx, metaRegroupTargetFingerprint, targetFingerprint); err != nil {
		_ = tx.Rollback()
		return false, false, 0, err
	}
	if err = setMetadataIntTx(tx, metaRegroupTotal, totalRows); err != nil {
		_ = tx.Rollback()
		return false, false, 0, err
	}
	if err = setMetadataTimeTx(tx, metaRegroupFinishedAt, nil); err != nil {
		_ = tx.Rollback()
		return false, false, 0, err
	}

	if err = tx.Commit(); err != nil {
		return false, false, 0, fmt.Errorf("failed to commit regroup metadata transaction: %w", err)
	}

	return true, resume, totalRows, nil
}

type regroupRow struct {
	RowID      int64
	AlertID    string
	Receiver   string
	LabelsJSON string
}

type compiledGroupingConfig struct {
	labelsKeep       []string
	labelsStrip      []string
	labelsKeepRegex  []*regexp.Regexp
	labelsStripRegex []*regexp.Regexp
}

func compileGroupingConfig(cfg GroupingConfig) (compiledGroupingConfig, error) {
	out := compiledGroupingConfig{
		labelsKeep:  append([]string{}, cfg.LabelsKeep...),
		labelsStrip: append([]string{}, cfg.LabelsStrip...),
	}

	for _, raw := range cfg.LabelsKeepRegex {
		re, err := regex.CompileAnchored(raw)
		if err != nil {
			return compiledGroupingConfig{}, fmt.Errorf("invalid labels keep regex %q: %w", raw, err)
		}
		out.labelsKeepRegex = append(out.labelsKeepRegex, re)
	}
	for _, raw := range cfg.LabelsStripRegex {
		re, err := regex.CompileAnchored(raw)
		if err != nil {
			return compiledGroupingConfig{}, fmt.Errorf("invalid labels strip regex %q: %w", raw, err)
		}
		out.labelsStripRegex = append(out.labelsStripRegex, re)
	}

	return out, nil
}

func (s *Store) runRegroupBatch(ctx context.Context, targetFingerprint string, cfg compiledGroupingConfig, batchSize int) (bool, int, int, int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, 0, 0, 0, fmt.Errorf("failed to begin regroup batch transaction: %w", err)
	}

	status, err := readMetadataStringTx(tx, metaRegroupStatus)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, 0, 0, err
	}
	if strings.TrimSpace(status) != regroupStatusRunning {
		_ = tx.Rollback()
		return true, 0, 0, 0, nil
	}
	activeTarget, err := readMetadataStringTx(tx, metaRegroupTargetFingerprint)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, 0, 0, err
	}
	if strings.TrimSpace(activeTarget) != targetFingerprint {
		_ = tx.Rollback()
		return true, 0, 0, 0, nil
	}

	cursor, err := readMetadataInt64Tx(tx, metaRegroupCursorRowID)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, 0, 0, err
	}
	processed, err := readMetadataIntTx(tx, metaRegroupProcessed)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, 0, 0, err
	}
	totalRows, err := queryInt(tx, `SELECT COUNT(*) FROM alert_states`)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, 0, 0, fmt.Errorf("failed to count rows in regroup batch: %w", err)
	}

	rows, err := tx.Query(`
SELECT rowid, alert_id, receiver, labels_json
FROM alert_states
WHERE rowid > ?
ORDER BY rowid ASC
LIMIT ?
`, cursor, batchSize)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, 0, 0, fmt.Errorf("failed to select regroup batch rows: %w", err)
	}

	batch := make([]regroupRow, 0, batchSize)
	for rows.Next() {
		var row regroupRow
		if scanErr := rows.Scan(&row.RowID, &row.AlertID, &row.Receiver, &row.LabelsJSON); scanErr != nil {
			rows.Close()
			_ = tx.Rollback()
			return false, 0, 0, 0, fmt.Errorf("failed to scan regroup row: %w", scanErr)
		}
		batch = append(batch, row)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		_ = tx.Rollback()
		return false, 0, 0, 0, fmt.Errorf("failed to iterate regroup rows: %w", err)
	}
	rows.Close()

	if len(batch) == 0 {
		now := time.Now().UTC()
		if err = setMetadataTx(tx, metaGroupingFingerprint, targetFingerprint); err != nil {
			_ = tx.Rollback()
			return false, 0, 0, 0, err
		}
		if err = setMetadataTx(tx, metaRegroupStatus, regroupStatusReady); err != nil {
			_ = tx.Rollback()
			return false, 0, 0, 0, err
		}
		if err = setMetadataTx(tx, metaRegroupError, ""); err != nil {
			_ = tx.Rollback()
			return false, 0, 0, 0, err
		}
		if err = setMetadataTimeTx(tx, metaRegroupFinishedAt, &now); err != nil {
			_ = tx.Rollback()
			return false, 0, 0, 0, err
		}
		if err = setMetadataInt64Tx(tx, metaRegroupCursorRowID, 0); err != nil {
			_ = tx.Rollback()
			return false, 0, 0, 0, err
		}
		if err = setMetadataIntTx(tx, metaRegroupProcessed, totalRows); err != nil {
			_ = tx.Rollback()
			return false, 0, 0, 0, err
		}
		if err = setMetadataIntTx(tx, metaRegroupTotal, totalRows); err != nil {
			_ = tx.Rollback()
			return false, 0, 0, 0, err
		}
		if err = tx.Commit(); err != nil {
			return false, 0, 0, 0, fmt.Errorf("failed to commit regroup finalize: %w", err)
		}
		return true, 0, 0, totalRows, nil
	}

	updated := 0
	lastRowID := cursor
	for _, row := range batch {
		groupID, groupLabelsJSON, computeErr := computeGroupFromAlertLabels(row.Receiver, row.LabelsJSON, cfg)
		if computeErr != nil {
			_ = tx.Rollback()
			return false, 0, 0, totalRows, fmt.Errorf("failed to regroup alert %s: %w", row.AlertID, computeErr)
		}

		updatedAt := time.Now().UTC().Format(time.RFC3339Nano)
		res, updateErr := tx.Exec(`
UPDATE alert_states
SET group_id = ?, group_labels_json = ?, updated_at = ?
WHERE rowid = ?
`, groupID, groupLabelsJSON, updatedAt, row.RowID)
		if updateErr != nil {
			_ = tx.Rollback()
			return false, 0, 0, totalRows, fmt.Errorf("failed to update regrouped row: %w", updateErr)
		}
		affected, _ := res.RowsAffected()
		if affected > 0 {
			updated++
		}
		lastRowID = row.RowID
	}

	processed += len(batch)
	if processed > totalRows {
		totalRows = processed
	}

	if err = setMetadataInt64Tx(tx, metaRegroupCursorRowID, lastRowID); err != nil {
		_ = tx.Rollback()
		return false, 0, 0, totalRows, err
	}
	if err = setMetadataIntTx(tx, metaRegroupProcessed, processed); err != nil {
		_ = tx.Rollback()
		return false, 0, 0, totalRows, err
	}
	if err = setMetadataIntTx(tx, metaRegroupTotal, totalRows); err != nil {
		_ = tx.Rollback()
		return false, 0, 0, totalRows, err
	}

	if err = tx.Commit(); err != nil {
		return false, 0, 0, totalRows, fmt.Errorf("failed to commit regroup batch: %w", err)
	}

	return false, len(batch), updated, totalRows, nil
}

func (s *Store) failRegroup(ctx context.Context, targetFingerprint, reason string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	status, readErr := readMetadataStringTx(tx, metaRegroupStatus)
	if readErr != nil {
		_ = tx.Rollback()
		return readErr
	}
	activeTarget, readErr := readMetadataStringTx(tx, metaRegroupTargetFingerprint)
	if readErr != nil {
		_ = tx.Rollback()
		return readErr
	}
	if strings.TrimSpace(status) == regroupStatusRunning && strings.TrimSpace(activeTarget) == targetFingerprint {
		now := time.Now().UTC()
		if err = setMetadataTx(tx, metaRegroupStatus, regroupStatusError); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err = setMetadataTx(tx, metaRegroupError, strings.TrimSpace(reason)); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err = setMetadataTimeTx(tx, metaRegroupFinishedAt, &now); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func computeGroupFromAlertLabels(receiver, labelsJSON string, cfg compiledGroupingConfig) (string, string, error) {
	labelsMap := map[string]string{}
	if err := json.Unmarshal([]byte(labelsJSON), &labelsMap); err != nil {
		return "", "", fmt.Errorf("failed to unmarshal labels json: %w", err)
	}

	labels := models.LabelsFromMap(labelsMap)
	groupLabels := transform.StripLabels(cfg.labelsKeep, cfg.labelsStrip, cfg.labelsKeepRegex, cfg.labelsStripRegex, labels)
	groupLabelsJSONRaw, err := json.Marshal(groupLabels.Map())
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal group labels json: %w", err)
	}

	groupID := models.AlertGroup{Receiver: receiver, Labels: groupLabels}.LabelsFingerprint()
	return groupID, string(groupLabelsJSONRaw), nil
}

func normalizeStringList(values []string) []string {
	uniq := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		vv := strings.TrimSpace(v)
		if vv == "" {
			continue
		}
		if _, found := uniq[vv]; found {
			continue
		}
		uniq[vv] = struct{}{}
		out = append(out, vv)
	}
	slices.Sort(out)
	return out
}

func validateCurrentAlert(alert AlertCurrent) error {
	if strings.TrimSpace(alert.AlertID) == "" {
		return errors.New("alertID cannot be empty")
	}
	if !alert.State.IsValid() {
		return fmt.Errorf("invalid state: %q", alert.State)
	}
	if !alert.RawState.IsValid() {
		return fmt.Errorf("invalid rawState: %q", alert.RawState)
	}
	if strings.TrimSpace(alert.Receiver) == "" {
		return errors.New("receiver cannot be empty")
	}
	if strings.TrimSpace(alert.GroupID) == "" {
		return errors.New("groupID cannot be empty")
	}
	if strings.TrimSpace(alert.GroupLabelsJSON) == "" {
		return errors.New("groupLabelsJSON cannot be empty")
	}
	if strings.TrimSpace(alert.LabelsJSON) == "" {
		return errors.New("labelsJSON cannot be empty")
	}
	if strings.TrimSpace(alert.AnnotationsJSON) == "" {
		return errors.New("annotationsJSON cannot be empty")
	}
	if alert.StartsAt.IsZero() {
		return errors.New("startsAt cannot be zero")
	}
	if strings.TrimSpace(alert.RelevantUpstreams) == "" {
		return errors.New("relevantUpstreams cannot be empty")
	}

	return nil
}

func validateTransition(transition AlertStateTransition) error {
	if strings.TrimSpace(transition.AlertID) == "" {
		return errors.New("alertID cannot be empty")
	}
	if !transition.NewState.IsValid() {
		return fmt.Errorf("invalid new state: %q", transition.NewState)
	}
	if !transition.NewRawState.IsValid() {
		switch transition.NewState {
		case StateInactive, StateResolved:
			transition.NewRawState = StateActive
		case StateUnprocessed, StateActive, StateSuppressed:
			transition.NewRawState = transition.NewState
		default:
			return fmt.Errorf("invalid new raw state: %q", transition.NewRawState)
		}
	}
	if transition.OldRawState != nil && !transition.OldRawState.IsValid() {
		return fmt.Errorf("invalid old raw state: %q", *transition.OldRawState)
	}
	if transition.OldState != nil && !transition.OldState.IsValid() {
		return fmt.Errorf("invalid old state: %q", *transition.OldState)
	}
	if !transition.ChangeSource.IsValid() {
		return fmt.Errorf("invalid change source: %q", transition.ChangeSource)
	}
	if transition.ReasonCode != nil {
		r := strings.TrimSpace(*transition.ReasonCode)
		if len(r) > reasonCodeMaxLength {
			return fmt.Errorf("reason_code length exceeds %d", reasonCodeMaxLength)
		}
		if r != "" {
			for _, ch := range r {
				if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '_' {
					return errors.New("reason_code must be snake_case")
				}
			}
		}
	}

	return nil
}

func formatNullableState(state *State) any {
	if state == nil {
		return nil
	}
	return string(*state)
}

func formatNullableBool(v *bool) any {
	if v == nil {
		return nil
	}
	if *v {
		return 1
	}
	return 0
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func intToBool(v int) bool {
	return v != 0
}

func sanitizeReasonCode(v *string) *string {
	if v == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*v)
	if trimmed == "" {
		return nil
	}
	if len(trimmed) > reasonCodeMaxLength {
		trimmed = trimmed[:reasonCodeMaxLength]
	}
	return &trimmed
}

func ptrString(v string) *string {
	return &v
}

func formatNullableTime(ts *time.Time) any {
	if ts == nil {
		return nil
	}
	return ts.UTC().Format(time.RFC3339Nano)
}

func nullableTimeString(ts *time.Time) string {
	if ts == nil {
		return ""
	}
	return ts.UTC().Format(time.RFC3339Nano)
}

func (s State) IsValid() bool {
	switch s {
	case StateActive, StateSuppressed, StateUnprocessed, StateInactive, StateResolved:
		return true
	default:
		return false
	}
}

func projectedState(rawState State, isLive, isSuppressed, isResolved bool) State {
	return (LifecycleDimensions{
		RawState: rawState, IsLive: isLive, IsSuppressed: isSuppressed, IsResolved: isResolved,
	}).ProjectedState()
}

func (s ChangeSource) IsValid() bool {
	switch s {
	case ChangeSourcePoller, ChangeSourceManual, ChangeSourceSilence:
		return true
	default:
		return false
	}
}

func validateLiveSnapshot(alert LiveAlertSnapshot) error {
	if strings.TrimSpace(alert.AlertID) == "" {
		return errors.New("live snapshot alertID cannot be empty")
	}
	if !alert.State.IsValid() {
		return fmt.Errorf("invalid live snapshot state: %q", alert.State)
	}
	if alert.State == StateInactive || alert.State == StateResolved {
		return fmt.Errorf("invalid live snapshot state %q, live states must be active/suppressed/unprocessed", alert.State)
	}
	if strings.TrimSpace(alert.Receiver) == "" {
		return errors.New("live snapshot receiver cannot be empty")
	}
	if strings.TrimSpace(alert.GroupID) == "" {
		return errors.New("live snapshot groupID cannot be empty")
	}
	if strings.TrimSpace(alert.GroupLabelsJSON) == "" {
		return errors.New("live snapshot groupLabelsJSON cannot be empty")
	}
	if strings.TrimSpace(alert.LabelsJSON) == "" {
		return errors.New("live snapshot labelsJSON cannot be empty")
	}
	if strings.TrimSpace(alert.AnnotationsJSON) == "" {
		return errors.New("live snapshot annotationsJSON cannot be empty")
	}
	if alert.StartsAt.IsZero() {
		return errors.New("live snapshot startsAt cannot be zero")
	}
	if alert.LastSeenAt.IsZero() {
		return errors.New("live snapshot lastSeenAt cannot be zero")
	}
	if len(alert.RelevantUpstreams) == 0 && len(alert.RelevantSources) == 0 {
		return errors.New("live snapshot relevant sources cannot be empty")
	}
	for _, upstream := range alert.RelevantUpstreams {
		if strings.TrimSpace(upstream) == "" {
			return errors.New("live snapshot relevantUpstreams contains empty name")
		}
	}
	for _, source := range alert.RelevantSources {
		if strings.TrimSpace(source.Name) == "" {
			return errors.New("live snapshot relevantSources contains empty name")
		}
		if strings.TrimSpace(source.Cluster) == "" {
			return errors.New("live snapshot relevantSources contains empty cluster")
		}
	}
	for _, target := range alert.Targets {
		if strings.TrimSpace(target.Target) == "" {
			return errors.New("live snapshot targets contains empty target")
		}
		if !target.State.IsValid() {
			return fmt.Errorf("invalid live snapshot target state for %s: %q", target.Target, target.State)
		}
		if target.State == StateInactive || target.State == StateResolved {
			return fmt.Errorf("invalid live snapshot target state %q for %s", target.State, target.Target)
		}
		if target.StartsAt.IsZero() {
			return fmt.Errorf("live snapshot target startsAt cannot be zero for %s", target.Target)
		}
		if len(target.SourceNames) == 0 {
			return fmt.Errorf("live snapshot target sources cannot be empty for %s", target.Target)
		}
		for _, name := range target.SourceNames {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("live snapshot target sources contains empty source for %s", target.Target)
			}
		}
	}

	return nil
}

func normalizeLiveTargets(live LiveAlertSnapshot) []LiveTargetSnapshot {
	if len(live.Targets) > 0 {
		out := make([]LiveTargetSnapshot, 0, len(live.Targets))
		for _, target := range live.Targets {
			t := strings.TrimSpace(target.Target)
			if t == "" {
				continue
			}
			sources := uniqueSortedStrings(target.SourceNames)
			if len(sources) == 0 {
				continue
			}
			out = append(out, LiveTargetSnapshot{
				Target:      t,
				State:       target.State,
				StartsAt:    target.StartsAt,
				SourceNames: sources,
			})
		}
		return out
	}

	byTarget := map[string]*LiveTargetSnapshot{}
	for _, source := range live.RelevantSources {
		target := strings.TrimSpace(source.Cluster)
		name := strings.TrimSpace(source.Name)
		if target == "" || name == "" {
			continue
		}
		entry, ok := byTarget[target]
		if !ok {
			entry = &LiveTargetSnapshot{Target: target, State: live.State, StartsAt: live.StartsAt}
			byTarget[target] = entry
		}
		entry.SourceNames = append(entry.SourceNames, name)
		if live.StartsAt.Before(entry.StartsAt) {
			entry.StartsAt = live.StartsAt
		}
	}
	out := make([]LiveTargetSnapshot, 0, len(byTarget))
	for _, entry := range byTarget {
		entry.SourceNames = uniqueSortedStrings(entry.SourceNames)
		if len(entry.SourceNames) == 0 {
			continue
		}
		out = append(out, *entry)
	}
	slices.SortFunc(out, func(a, b LiveTargetSnapshot) int {
		return strings.Compare(a.Target, b.Target)
	})
	return out
}

func readAllCurrentAlertsTx(tx *sql.Tx) (map[string]AlertCurrent, error) {
	rows, err := tx.Query(`
SELECT
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
FROM alert_states
`)
	if err != nil {
		return nil, fmt.Errorf("failed to query current alerts: %w", err)
	}
	defer rows.Close()

	result := map[string]AlertCurrent{}
	for rows.Next() {
		current, scanErr := scanCurrentAlert(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result[current.AlertID] = current
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate current alerts: %w", err)
	}

	return result, nil
}

func scanCurrentAlert(scanner interface{ Scan(dest ...any) error }) (AlertCurrent, error) {
	var (
		stateRaw               string
		rawStateRaw            string
		isLiveRaw              int
		isSuppressedRaw        int
		isResolvedRaw          int
		matchLabelsRaw         sql.NullString
		startsAtRaw            string
		lastSeenRaw            sql.NullString
		suppressionObservedRaw sql.NullString
		resolvedAtRaw          sql.NullString
		resolvedByRaw          sql.NullString
		resolvedCommentRaw     sql.NullString
		updatedAtRaw           string
		current                AlertCurrent
	)

	err := scanner.Scan(
		&current.AlertID,
		&stateRaw,
		&rawStateRaw,
		&isLiveRaw,
		&isSuppressedRaw,
		&isResolvedRaw,
		&current.Receiver,
		&current.GroupID,
		&current.GroupLabelsJSON,
		&current.LabelsJSON,
		&matchLabelsRaw,
		&current.AnnotationsJSON,
		&startsAtRaw,
		&lastSeenRaw,
		&current.RelevantUpstreams,
		&current.RelevantSources,
		&suppressionObservedRaw,
		&resolvedAtRaw,
		&resolvedByRaw,
		&resolvedCommentRaw,
		&updatedAtRaw,
	)
	if err != nil {
		return AlertCurrent{}, err
	}

	state := State(stateRaw)
	if !state.IsValid() {
		return AlertCurrent{}, fmt.Errorf("invalid state in database: %q", stateRaw)
	}
	current.State = state
	rawState := State(rawStateRaw)
	if !rawState.IsValid() {
		return AlertCurrent{}, fmt.Errorf("invalid raw_state in database: %q", rawStateRaw)
	}
	current.RawState = rawState
	current.IsLive = intToBool(isLiveRaw)
	current.IsSuppressed = intToBool(isSuppressedRaw)
	current.IsResolved = intToBool(isResolvedRaw)
	if matchLabelsRaw.Valid {
		v := strings.TrimSpace(matchLabelsRaw.String)
		if v != "" {
			current.MatchLabelsJSON = &v
		}
	}

	startsAt, err := time.Parse(time.RFC3339Nano, startsAtRaw)
	if err != nil {
		return AlertCurrent{}, fmt.Errorf("failed to parse starts_at: %w", err)
	}
	current.StartsAt = startsAt

	if lastSeenRaw.Valid {
		lastSeenAt, parseErr := time.Parse(time.RFC3339Nano, lastSeenRaw.String)
		if parseErr != nil {
			return AlertCurrent{}, fmt.Errorf("failed to parse last_seen_at: %w", parseErr)
		}
		current.LastSeenAt = &lastSeenAt
	}
	if resolvedAtRaw.Valid {
		resolvedAt, parseErr := time.Parse(time.RFC3339Nano, resolvedAtRaw.String)
		if parseErr != nil {
			return AlertCurrent{}, fmt.Errorf("failed to parse resolved_at: %w", parseErr)
		}
		current.ResolvedAt = &resolvedAt
	}
	if suppressionObservedRaw.Valid {
		suppressionObservedAt, parseErr := time.Parse(time.RFC3339Nano, suppressionObservedRaw.String)
		if parseErr != nil {
			return AlertCurrent{}, fmt.Errorf("failed to parse suppression_observed_at: %w", parseErr)
		}
		current.SuppressionObservedAt = &suppressionObservedAt
	}
	if resolvedByRaw.Valid {
		v := strings.TrimSpace(resolvedByRaw.String)
		if v != "" {
			current.ResolvedBy = &v
		}
	}
	if resolvedCommentRaw.Valid {
		v := strings.TrimSpace(resolvedCommentRaw.String)
		if v != "" {
			current.ResolvedComment = &v
		}
	}

	updatedAt, err := time.Parse(time.RFC3339Nano, updatedAtRaw)
	if err != nil {
		return AlertCurrent{}, fmt.Errorf("failed to parse updated_at: %w", err)
	}
	current.UpdatedAt = updatedAt

	return current, nil
}

func insertCurrentAlertTx(tx *sql.Tx, live LiveAlertSnapshot, now time.Time) error {
	relevantJSON, err := marshalRelevantUpstreams(live.RelevantUpstreams)
	if err != nil {
		return err
	}
	relevantSourcesJSON, err := marshalRelevantSources(live.RelevantSources)
	if err != nil {
		return err
	}
	livePlan := ObserveLive(LifecycleDimensions{}, live.State)
	rawState := livePlan.RawState
	isLive := livePlan.IsLive
	isSuppressed := livePlan.IsSuppressed
	isResolved := livePlan.IsResolved
	state := livePlan.ProjectedState()

	_, err = tx.Exec(`
INSERT INTO alert_states(
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
		live.AlertID,
		string(state),
		string(rawState),
		boolToInt(isLive),
		boolToInt(isSuppressed),
		boolToInt(isResolved),
		live.Receiver,
		live.GroupID,
		live.GroupLabelsJSON,
		live.LabelsJSON,
		live.MatchLabelsJSON,
		live.AnnotationsJSON,
		live.StartsAt.UTC().Format(time.RFC3339Nano),
		live.LastSeenAt.UTC().Format(time.RFC3339Nano),
		string(relevantJSON),
		string(relevantSourcesJSON),
		formatNullableTime(nil),
		nil,
		nil,
		nil,
		now.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("failed to insert current alert: %w", err)
	}

	return nil
}

func updateCurrentAlertFromLiveTx(tx *sql.Tx, alertID string, live LiveAlertSnapshot, newRawState State, newIsLive, newIsSuppressed, newIsResolved bool, relevantJSON, relevantSourcesJSON string, now time.Time) error {
	newState := projectedState(newRawState, newIsLive, newIsSuppressed, newIsResolved)
	_, err := tx.Exec(`
UPDATE alert_states SET
  state = ?,
  raw_state = ?,
  is_live = ?,
  is_suppressed = ?,
  is_resolved = ?,
  receiver = ?,
  group_id = ?,
  group_labels_json = ?,
  labels_json = ?,
  match_labels_json = ?,
  annotations_json = ?,
  starts_at = ?,
  last_seen_at = ?,
  relevant_upstreams_json = ?,
  relevant_sources_json = ?,
  suppression_observed_at = CASE WHEN ? = 1 THEN COALESCE(suppression_observed_at, ?) ELSE NULL END,
  resolved_at = NULL,
  resolved_by = NULL,
  resolved_comment = NULL,
  updated_at = ?
WHERE alert_id = ?
`,
		string(newState),
		string(newRawState),
		boolToInt(newIsLive),
		boolToInt(newIsSuppressed),
		boolToInt(newIsResolved),
		live.Receiver,
		live.GroupID,
		live.GroupLabelsJSON,
		live.LabelsJSON,
		live.MatchLabelsJSON,
		live.AnnotationsJSON,
		live.StartsAt.UTC().Format(time.RFC3339Nano),
		live.LastSeenAt.UTC().Format(time.RFC3339Nano),
		relevantJSON,
		relevantSourcesJSON,
		boolToInt(newIsSuppressed),
		now.UTC().Format(time.RFC3339Nano),
		now.UTC().Format(time.RFC3339Nano),
		alertID,
	)
	if err != nil {
		return fmt.Errorf("failed to update current alert from live: %w", err)
	}

	return nil
}

func updateProjectedStateTx(tx *sql.Tx, alertID string, rawState State, isLive, isSuppressed, isResolved bool, now time.Time) error {
	newState := projectedState(rawState, isLive, isSuppressed, isResolved)
	_, err := tx.Exec(`
UPDATE alert_states
SET state = ?, raw_state = ?, is_live = ?, is_suppressed = ?, is_resolved = ?, suppression_observed_at = CASE WHEN ? = 1 THEN COALESCE(suppression_observed_at, ?) ELSE NULL END, updated_at = ?
WHERE alert_id = ?
`, string(newState), string(rawState), boolToInt(isLive), boolToInt(isSuppressed), boolToInt(isResolved), boolToInt(isSuppressed), now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), alertID)
	if err != nil {
		return fmt.Errorf("failed to update alert state: %w", err)
	}

	return nil
}

type targetStateRow struct {
	Target                 string
	State                  State
	RawState               State
	IsLive                 bool
	IsSuppressed           bool
	IsResolved             bool
	StartsAt               time.Time
	LastSeenAt             *time.Time
	SourceMembers          []string
	MatchLabelsFingerprint string
	RetiredAt              *time.Time
}

func readTargetStatesTx(tx *sql.Tx, alertID string) (map[string]targetStateRow, error) {
	rows, err := tx.Query(`
SELECT target, state, raw_state, is_live, is_suppressed, is_resolved, starts_at, last_seen_at, source_members_json, match_labels_fingerprint, retired_at
FROM alert_target_states
WHERE alert_id = ?
`, alertID)
	if err != nil {
		return nil, fmt.Errorf("failed to read target states: %w", err)
	}
	defer rows.Close()
	out := map[string]targetStateRow{}
	for rows.Next() {
		var (
			target                 string
			stateRaw               string
			rawStateRaw            string
			isLiveRaw              int
			isSuppressedRaw        int
			isResolvedRaw          int
			startsAtRaw            string
			lastSeenRaw            sql.NullString
			sourceMembersJSON      string
			matchLabelsFingerprint string
			retiredAtRaw           sql.NullString
		)
		if err := rows.Scan(&target, &stateRaw, &rawStateRaw, &isLiveRaw, &isSuppressedRaw, &isResolvedRaw, &startsAtRaw, &lastSeenRaw, &sourceMembersJSON, &matchLabelsFingerprint, &retiredAtRaw); err != nil {
			return nil, fmt.Errorf("failed to scan target state row: %w", err)
		}
		state := State(stateRaw)
		rawState := State(rawStateRaw)
		if !state.IsValid() || !rawState.IsValid() {
			continue
		}
		startsAt, err := time.Parse(time.RFC3339Nano, startsAtRaw)
		if err != nil {
			continue
		}
		var sourceMembers []string
		if strings.TrimSpace(sourceMembersJSON) != "" {
			_ = json.Unmarshal([]byte(sourceMembersJSON), &sourceMembers)
		}
		var lastSeenAt *time.Time
		if lastSeenRaw.Valid {
			ts, err := time.Parse(time.RFC3339Nano, lastSeenRaw.String)
			if err == nil {
				lastSeenAt = &ts
			}
		}
		var retiredAt *time.Time
		if retiredAtRaw.Valid {
			if ts, parseErr := time.Parse(time.RFC3339Nano, retiredAtRaw.String); parseErr == nil {
				retiredAt = &ts
			}
		}
		out[target] = targetStateRow{
			Target:                 target,
			State:                  state,
			RawState:               rawState,
			IsLive:                 intToBool(isLiveRaw),
			IsSuppressed:           intToBool(isSuppressedRaw),
			IsResolved:             intToBool(isResolvedRaw),
			StartsAt:               startsAt,
			LastSeenAt:             lastSeenAt,
			SourceMembers:          uniqueSortedStrings(sourceMembers),
			MatchLabelsFingerprint: matchLabelsFingerprint,
			RetiredAt:              retiredAt,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate target state rows: %w", err)
	}
	return out, nil
}

func upsertTargetStateTx(tx *sql.Tx, alertID string, row targetStateRow, reasonCode string, changedAt time.Time) error {
	sourceMembersJSON, err := json.Marshal(uniqueSortedStrings(row.SourceMembers))
	if err != nil {
		return fmt.Errorf("failed to marshal target source members: %w", err)
	}
	var oldStateRaw sql.NullString
	var oldRawStateRaw sql.NullString
	var oldIsLiveRaw sql.NullInt64
	var oldIsSuppressedRaw sql.NullInt64
	var oldIsResolvedRaw sql.NullInt64
	var oldMatchLabelsFingerprint string
	var oldRetiredAtRaw sql.NullString
	if row.MatchLabelsFingerprint == "" {
		row.MatchLabelsFingerprint, err = readAlertMatchLabelsFingerprintTx(tx, alertID)
		if err != nil {
			return err
		}
	}
	err = tx.QueryRow(`SELECT state, raw_state, is_live, is_suppressed, is_resolved, match_labels_fingerprint, retired_at FROM alert_target_states WHERE alert_id = ? AND target = ?`, alertID, row.Target).Scan(&oldStateRaw, &oldRawStateRaw, &oldIsLiveRaw, &oldIsSuppressedRaw, &oldIsResolvedRaw, &oldMatchLabelsFingerprint, &oldRetiredAtRaw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to read old target state: %w", err)
	}
	_, err = tx.Exec(`
INSERT INTO alert_target_states(
  alert_id,
  target,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  starts_at,
  last_seen_at,
  source_members_json,
  match_labels_fingerprint,
  retired_at,
  updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(alert_id, target) DO UPDATE SET
  state=excluded.state,
  raw_state=excluded.raw_state,
  is_live=excluded.is_live,
  is_suppressed=excluded.is_suppressed,
  is_resolved=excluded.is_resolved,
  starts_at=excluded.starts_at,
  last_seen_at=excluded.last_seen_at,
  source_members_json=excluded.source_members_json,
  match_labels_fingerprint=excluded.match_labels_fingerprint,
  retired_at=excluded.retired_at,
  updated_at=excluded.updated_at
`,
		alertID,
		row.Target,
		string(row.State),
		string(row.RawState),
		boolToInt(row.IsLive),
		boolToInt(row.IsSuppressed),
		boolToInt(row.IsResolved),
		row.StartsAt.UTC().Format(time.RFC3339Nano),
		formatNullableTime(row.LastSeenAt),
		string(sourceMembersJSON),
		row.MatchLabelsFingerprint,
		formatNullableTime(row.RetiredAt),
		changedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("failed to upsert target state: %w", err)
	}

	changed := true
	if oldStateRaw.Valid && oldRawStateRaw.Valid && oldIsLiveRaw.Valid && oldIsSuppressedRaw.Valid && oldIsResolvedRaw.Valid {
		changed = oldStateRaw.String != string(row.State) ||
			oldRawStateRaw.String != string(row.RawState) ||
			int(oldIsLiveRaw.Int64) != boolToInt(row.IsLive) ||
			int(oldIsSuppressedRaw.Int64) != boolToInt(row.IsSuppressed) ||
			int(oldIsResolvedRaw.Int64) != boolToInt(row.IsResolved) ||
			oldRetiredAtRaw.Valid != (row.RetiredAt != nil) ||
			oldRetiredAtRaw.String != nullableTimeString(row.RetiredAt)
	}
	if changed {
		_, err = tx.Exec(`
INSERT INTO alert_target_events(
  alert_id,
  target,
  old_state,
  new_state,
  old_raw_state,
  new_raw_state,
  old_is_live,
  new_is_live,
  old_is_suppressed,
  new_is_suppressed,
  old_is_resolved,
  new_is_resolved,
  old_retired_at,
  new_retired_at,
  changed_at,
  reason_code
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
			alertID,
			row.Target,
			nullableStringFromNull(oldStateRaw),
			string(row.State),
			nullableStringFromNull(oldRawStateRaw),
			string(row.RawState),
			nullableIntFromNull(oldIsLiveRaw),
			boolToInt(row.IsLive),
			nullableIntFromNull(oldIsSuppressedRaw),
			boolToInt(row.IsSuppressed),
			nullableIntFromNull(oldIsResolvedRaw),
			boolToInt(row.IsResolved),
			nullableStringFromNull(oldRetiredAtRaw),
			formatNullableTime(row.RetiredAt),
			changedAt.UTC().Format(time.RFC3339Nano),
			sanitizeReasonCode(ptrString(reasonCode)),
		)
		if err != nil {
			return fmt.Errorf("failed to insert target state event: %w", err)
		}
	}

	if row.RetiredAt != nil {
		if _, err := tx.Exec(`DELETE FROM alert_target_match_labels WHERE alert_id = ? AND target = ?`, alertID, row.Target); err != nil {
			return fmt.Errorf("failed to remove retired target label index: %w", err)
		}
	} else if !oldStateRaw.Valid || oldRetiredAtRaw.Valid || oldMatchLabelsFingerprint != row.MatchLabelsFingerprint {
		if err := upsertTargetMatchLabelsTx(tx, alertID, row.Target, changedAt); err != nil {
			return err
		}
	}
	if !oldStateRaw.Valid || oldRetiredAtRaw.Valid != (row.RetiredAt != nil) || oldMatchLabelsFingerprint != row.MatchLabelsFingerprint {
		key := metaSuggestionRevisionPrefix + row.Target
		revision, readErr := readMetadataInt64Tx(tx, key)
		if readErr != nil {
			return fmt.Errorf("failed to read target suggestion revision: %w", readErr)
		}
		if err := setMetadataInt64Tx(tx, key, revision+1); err != nil {
			return fmt.Errorf("failed to advance target suggestion revision: %w", err)
		}
	}
	return nil
}

// TargetSuggestionRevision returns the monotonic revision for the target's
// stored label relation. A missing row means the initial revision zero.
func (s *Store) TargetSuggestionRevision(target string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, ErrStoreNotInitialized
	}
	var raw sql.NullString
	err := s.db.QueryRow(`SELECT value FROM metadata WHERE key=?`, metaSuggestionRevisionPrefix+strings.TrimSpace(target)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read target suggestion revision: %w", err)
	}
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return 0, nil
	}
	revision, parseErr := strconv.ParseInt(raw.String, 10, 64)
	if parseErr != nil || revision < 0 {
		return 0, errors.New("invalid target suggestion revision")
	}
	return revision, nil
}

func readAlertMatchLabelsFingerprintTx(tx *sql.Tx, alertID string) (string, error) {
	var raw sql.NullString
	if err := tx.QueryRow(`SELECT match_labels_json FROM alert_states WHERE alert_id = ?`, alertID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to read match labels fingerprint source: %w", err)
	}
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return "", nil
	}
	hash := sha256.Sum256([]byte(raw.String))
	return hex.EncodeToString(hash[:]), nil
}

func upsertTargetMatchLabelsTx(tx *sql.Tx, alertID, target string, changedAt time.Time) error {
	var matchLabelsRaw sql.NullString
	if err := tx.QueryRow(`SELECT match_labels_json FROM alert_states WHERE alert_id = ?`, alertID).Scan(&matchLabelsRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to read match labels for target index: %w", err)
	}
	if !matchLabelsRaw.Valid || strings.TrimSpace(matchLabelsRaw.String) == "" {
		if _, err := tx.Exec(`DELETE FROM alert_target_match_labels WHERE alert_id = ? AND target = ?`, alertID, target); err != nil {
			return fmt.Errorf("failed to clear target label index: %w", err)
		}
		return nil
	}
	labels := map[string]string{}
	if err := json.Unmarshal([]byte(matchLabelsRaw.String), &labels); err != nil {
		if _, clearErr := tx.Exec(`DELETE FROM alert_target_match_labels WHERE alert_id = ? AND target = ?`, alertID, target); clearErr != nil {
			return fmt.Errorf("failed to clear target label index after parse error: %w", clearErr)
		}
		return nil
	}

	if _, err := tx.Exec(`DELETE FROM alert_target_match_labels WHERE alert_id = ? AND target = ?`, alertID, target); err != nil {
		return fmt.Errorf("failed to replace target label index: %w", err)
	}
	nowRaw := changedAt.UTC().Format(time.RFC3339Nano)
	for name, value := range labels {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, err := tx.Exec(`
INSERT INTO alert_target_match_labels(alert_id, target, label_name, label_value, updated_at)
VALUES(?, ?, ?, ?, ?)
`, alertID, target, name, value, nowRaw); err != nil {
			return fmt.Errorf("failed to upsert target label index row: %w", err)
		}
	}
	return nil
}

func nullableStringFromNull(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	out := v.String
	return &out
}

func nullableIntFromNull(v sql.NullInt64) *int {
	if !v.Valid {
		return nil
	}
	i := int(v.Int64)
	return &i
}

func upsertTargetStatesFromLiveTx(tx *sql.Tx, alertID string, targets []LiveTargetSnapshot, matchLabelsJSON *string, successfulUpstreams map[string]bool, configuredSourcesByTarget map[string]map[string]struct{}, sourceManifestKnown bool, now time.Time) error {
	if len(targets) == 0 {
		return nil
	}
	matchLabelsFingerprint := fingerprintMatchLabelsJSON(matchLabelsJSON)
	existing, err := readTargetStatesTx(tx, alertID)
	if err != nil {
		return err
	}

	seenTargets := map[string]struct{}{}
	for _, target := range targets {
		targetName := strings.TrimSpace(target.Target)
		if targetName == "" {
			continue
		}
		seenTargets[targetName] = struct{}{}
		previous := existing[targetName]
		topology := PlanTargetTopology(TargetTopologyObservation{
			PreviousMembers: previous.SourceMembers,
			ObservedMembers: target.SourceNames,
			Configured:      configuredSourcesByTarget[targetName], ManifestKnown: sourceManifestKnown,
			Successful: successfulUpstreams,
		})
		livePlan := ObserveLive(LifecycleDimensions{
			RawState: previous.RawState, IsLive: previous.IsLive,
			IsSuppressed: previous.IsSuppressed, IsResolved: previous.IsResolved,
		}, target.State)
		row := targetStateRow{
			Target:                 targetName,
			State:                  livePlan.ProjectedState(),
			RawState:               livePlan.RawState,
			IsLive:                 livePlan.IsLive,
			IsSuppressed:           livePlan.IsSuppressed,
			IsResolved:             livePlan.IsResolved,
			StartsAt:               target.StartsAt,
			LastSeenAt:             ptrTime(target.StartsAt),
			SourceMembers:          topology.Members,
			MatchLabelsFingerprint: matchLabelsFingerprint,
		}
		lastSeen := now
		row.LastSeenAt = &lastSeen
		if err := upsertTargetStateTx(tx, alertID, row, "poll_target_seen", now); err != nil {
			return err
		}
	}

	for targetName, row := range existing {
		if _, ok := seenTargets[targetName]; ok {
			continue
		}
		if err := applyTargetAbsencePlanTx(tx, alertID, row, successfulUpstreams, configuredSourcesByTarget[targetName], sourceManifestKnown, now); err != nil {
			return err
		}
	}

	return nil
}

func fingerprintMatchLabelsJSON(raw *string) string {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return ""
	}
	hash := sha256.Sum256([]byte(*raw))
	return hex.EncodeToString(hash[:])
}

func markMissingTargetStatesNonLiveTx(tx *sql.Tx, current AlertCurrent, successfulUpstreams map[string]bool, configuredSourcesByTarget map[string]map[string]struct{}, sourceManifestKnown bool, now time.Time) error {
	existing, err := readTargetStatesTx(tx, current.AlertID)
	if err != nil {
		return err
	}

	for _, row := range existing {
		if err := applyTargetAbsencePlanTx(tx, current.AlertID, row, successfulUpstreams, configuredSourcesByTarget[row.Target], sourceManifestKnown, now); err != nil {
			return err
		}
	}
	return nil
}

func applyTargetAbsencePlanTx(tx *sql.Tx, alertID string, row targetStateRow, successfulUpstreams map[string]bool, configuredSources map[string]struct{}, sourceManifestKnown bool, now time.Time) error {
	topology := PlanTargetTopology(TargetTopologyObservation{
		PreviousMembers: row.SourceMembers,
		Configured:      configuredSources, ManifestKnown: sourceManifestKnown,
		Successful: successfulUpstreams,
	})
	if topology.Retire {
		return retireTargetStateTx(tx, alertID, row, now)
	}
	membersChanged := !slices.Equal(topology.Members, row.SourceMembers)
	row.SourceMembers = topology.Members
	if !topology.Complete || !row.IsLive {
		if membersChanged {
			return upsertTargetStateTx(tx, alertID, row, "poll_target_topology_reconciled", now)
		}
		return nil
	}
	absencePlan := ObserveAbsent(LifecycleDimensions{
		RawState: row.RawState, IsLive: row.IsLive,
		IsSuppressed: row.IsSuppressed, IsResolved: row.IsResolved,
	})
	row.RawState = absencePlan.RawState
	row.IsLive = absencePlan.IsLive
	row.IsSuppressed = absencePlan.IsSuppressed
	row.IsResolved = absencePlan.IsResolved
	row.State = absencePlan.ProjectedState()
	if err := upsertTargetStateTx(tx, alertID, row, "poll_target_disappeared", now); err != nil {
		return err
	}
	if row.State == StateInactive {
		return enqueueAlertForCurrentRecheckContextTx(tx, alertID, now)
	}
	return nil
}

func retireTargetStateTx(tx *sql.Tx, alertID string, row targetStateRow, now time.Time) error {
	retiredAt := now.UTC()
	row.SourceMembers = nil
	row.RetiredAt = &retiredAt
	return upsertTargetStateTx(tx, alertID, row, "poll_target_retired", now)
}

func configuredSourceNames(sources []SourceSilenceState) (map[string]struct{}, bool) {
	if sources == nil {
		return nil, false
	}
	out := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		name := strings.TrimSpace(source.SourceName)
		if name != "" {
			out[name] = struct{}{}
		}
	}
	return out, true
}

func configuredSourceNamesByTarget(sources []SourceSilenceState) map[string]map[string]struct{} {
	out := map[string]map[string]struct{}{}
	for _, source := range sources {
		target := strings.TrimSpace(source.ClusterID)
		name := strings.TrimSpace(source.SourceName)
		if target == "" || name == "" {
			continue
		}
		if out[target] == nil {
			out[target] = map[string]struct{}{}
		}
		out[target][name] = struct{}{}
	}
	return out
}

func filterConfiguredSourceNames(sources []string, configured map[string]struct{}, manifestKnown bool) []string {
	if !manifestKnown {
		return uniqueSortedStrings(sources)
	}
	out := make([]string, 0, len(sources))
	for _, source := range uniqueSortedStrings(sources) {
		if _, ok := configured[source]; ok {
			out = append(out, source)
		}
	}
	return out
}

func ptrTime(v time.Time) *time.Time {
	out := v
	return &out
}

func updateManualResolutionTx(tx *sql.Tx, alertID string, changedBy, comment *string, now time.Time) error {
	_, err := tx.Exec(`
UPDATE alert_states
SET is_resolved = 1,
    state = CASE WHEN is_live = 0 THEN 'resolved' ELSE state END,
    resolved_at = ?,
    resolved_by = ?,
    resolved_comment = ?,
    updated_at = ?
WHERE alert_id = ?
`, now.UTC().Format(time.RFC3339Nano), changedBy, comment, now.UTC().Format(time.RFC3339Nano), alertID)
	if err != nil {
		return fmt.Errorf("failed to apply manual resolution: %w", err)
	}

	return nil
}

func updateTargetStatesResolvedTx(tx *sql.Tx, alertID string, isResolved bool, now time.Time, reasonCode string) error {
	existing, err := readTargetStatesTx(tx, alertID)
	if err != nil {
		return err
	}
	for _, row := range existing {
		row.IsResolved = isResolved
		row.State = projectedState(row.RawState, row.IsLive, row.IsSuppressed, row.IsResolved)
		if err := upsertTargetStateTx(tx, alertID, row, reasonCode, now); err != nil {
			return err
		}
	}
	return nil
}

func insertTransitionTx(tx *sql.Tx, transition AlertStateTransition) error {
	_, err := insertTransitionTxWithID(tx, transition)
	return err
}

func insertTransitionTxWithID(tx *sql.Tx, transition AlertStateTransition) (int64, error) {
	if !transition.NewRawState.IsValid() {
		if transition.NewState == StateInactive || transition.NewState == StateResolved {
			transition.NewRawState = StateActive
		} else {
			transition.NewRawState = transition.NewState
		}
	}
	if transition.OldRawState == nil && transition.OldState != nil {
		v := *transition.OldState
		transition.OldRawState = &v
	}
	if err := validateTransition(transition); err != nil {
		return 0, err
	}

	changedAt := transition.ChangedAt
	if changedAt.IsZero() {
		changedAt = time.Now().UTC()
	}

	res, err := tx.Exec(`
INSERT INTO alert_events(
  alert_id,
  old_state,
  new_state,
  old_raw_state,
  new_raw_state,
  old_is_live,
  new_is_live,
  old_is_suppressed,
  new_is_suppressed,
  old_is_resolved,
  new_is_resolved,
  changed_at,
  change_source,
  changed_by,
  reason_code,
  reason_text
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
		transition.AlertID,
		formatNullableState(transition.OldState),
		string(transition.NewState),
		formatNullableState(transition.OldRawState),
		string(transition.NewRawState),
		formatNullableBool(transition.OldIsLive),
		boolToInt(transition.NewIsLive),
		formatNullableBool(transition.OldIsSuppressed),
		boolToInt(transition.NewIsSuppressed),
		formatNullableBool(transition.OldIsResolved),
		boolToInt(transition.NewIsResolved),
		changedAt.UTC().Format(time.RFC3339Nano),
		string(transition.ChangeSource),
		transition.ChangedBy,
		sanitizeReasonCode(transition.ReasonCode),
		sanitizeNullableString(transition.ReasonText),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert state transition: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get inserted transition id: %w", err)
	}

	return id, nil
}

type recheckJobRule struct {
	SourceName    string
	ClusterID     string
	RuleID        int64
	RevisionID    int64
	ObservedState string
	MatchersJSON  string
}

type suppressionCauseMember struct {
	sourceName string
	ruleID     int64
	revisionID *int64
	known      bool
}

func (s *Store) RunSilenceRecheckBatch(batchSize int) (promotedResult int, resultErr error) {
	return s.RunSilenceRecheckBatchContext(context.Background(), batchSize)
}

func (s *Store) RunSilenceRecheckBatchContext(ctx context.Context, batchSize int) (promotedResult int, resultErr error) {
	if s == nil || s.db == nil {
		return 0, ErrStoreNotInitialized
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var selectedJobID int64
	startedAt := time.Now()
	defer func() {
		if resultErr == nil || selectedJobID == 0 {
			return
		}
		nowRaw := time.Now().UTC().Format(time.RFC3339Nano)
		_, _ = s.db.ExecContext(ctx, `
UPDATE silence_recheck_jobs
SET status='failed', last_error=COALESCE(last_error, ?), finished_at=COALESCE(finished_at, ?)
WHERE job_id=? AND status IN ('queued','running','failed')
`, resultErr.Error(), nowRaw, selectedJobID)
		runtimeLog.ErrorContext(ctx, "silence_recheck", "job_failed", "Silence recheck job failed", slog.Int64("job_id", selectedJobID), slog.String("stage", "worker"), slog.Any("error", resultErr), slog.Duration("duration", time.Since(startedAt)))
	}()
	if batchSize <= 0 {
		batchSize = 1000
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin silence recheck transaction: %w", err)
	}

	jobID, cursor, status, err := selectNextRecheckJobTx(tx)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	if jobID == 0 {
		_ = tx.Rollback()
		s.recheckBlockedJobLogged = 0
		runtimeLog.DebugContext(ctx, "silence_recheck", "queue_empty", "Silence recheck queue is empty")
		return 0, nil
	}
	queueDepth, queueErr := queryInt(tx, `SELECT COUNT(*) FROM silence_recheck_jobs WHERE status IN ('queued','running','failed')`)
	if queueErr != nil {
		queueDepth = -1
		runtimeLog.ErrorContext(ctx, "silence_recheck", "observability_collection_failed", "Failed to count silence recheck queue for logging", slog.Int64("job_id", jobID), slog.Any("error", queueErr))
	}
	var priorProcessed, totalCandidates int
	if queryErr := tx.QueryRow(`SELECT processed_candidates, total_candidates FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&priorProcessed, &totalCandidates); queryErr != nil {
		priorProcessed = -1
		totalCandidates = -1
		runtimeLog.ErrorContext(ctx, "silence_recheck", "observability_collection_failed", "Failed to read silence recheck job progress for logging", slog.Int64("job_id", jobID), slog.Any("error", queryErr))
	}
	runtimeLog.DebugContext(ctx, "silence_recheck", "job_selected", "Silence recheck job selected", slog.Int64("job_id", jobID), slog.String("status", status), slog.Int("queue_depth", queueDepth), slog.Int("batch_size", batchSize))
	if !RecheckJobCanRun(status, false) {
		var lastError sql.NullString
		_ = tx.QueryRow(`SELECT last_error FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&lastError)
		_ = tx.Rollback()
		if s.recheckBlockedJobLogged != jobID {
			runtimeLog.WarnContext(ctx, "silence_recheck", "failed_job_blocks_queue", "Failed silence recheck job blocks the queue", slog.Int64("job_id", jobID), slog.Int("queue_depth", queueDepth))
			s.recheckBlockedJobLogged = jobID
		} else {
			runtimeLog.DebugContext(ctx, "silence_recheck", "failed_job_blocks_queue", "Failed silence recheck job still blocks the queue", slog.Int64("job_id", jobID), slog.Int("queue_depth", queueDepth))
		}
		return 0, fmt.Errorf("silence recheck job %d is failed and blocks later jobs: %s", jobID, lastError.String)
	}
	s.recheckBlockedJobLogged = 0
	selectedJobID = jobID

	now := time.Now().UTC()
	nowRaw := now.Format(time.RFC3339Nano)
	if status == "queued" {
		if _, err = tx.Exec(`UPDATE silence_recheck_jobs SET status='running', started_at=COALESCE(started_at, ?) WHERE job_id = ?`, nowRaw, jobID); err != nil {
			_ = tx.Rollback()
			return 0, fmt.Errorf("failed to mark recheck job running: %w", err)
		}
	}

	rules, err := loadRecheckJobRulesTx(tx, jobID)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	sourceStatuses, err := loadRecheckJobSourceStatusesTx(tx, jobID)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	if err = attachEligibleRecheckCandidatesTx(tx, jobID); err != nil {
		_ = tx.Rollback()
		return 0, err
	}

	candidates, err := loadRecheckCandidatesTx(tx, jobID, batchSize)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	if len(candidates) == 0 {
		if _, err = tx.Exec(`UPDATE silence_recheck_jobs SET status='completed', open_context_fingerprint=NULL, finished_at=?, last_error=NULL WHERE job_id = ?`, nowRaw, jobID); err != nil {
			_ = tx.Rollback()
			return 0, fmt.Errorf("failed to mark recheck job completed: %w", err)
		}
		if err = tx.Commit(); err != nil {
			return 0, fmt.Errorf("failed to commit recheck completion: %w", err)
		}
		logCompletedRecheckJob(ctx, s.db, jobID, time.Since(startedAt))
		return 0, nil
	}

	processed := 0
	promoted := 0
	stale := 0
	missing := 0
	ineligible := 0
	incompleteSources := 0
	lastCursor := cursor
	decisions := make([]runtimeEvent, 0, len(candidates))
	appendDecision := func(event runtimeEvent) { decisions = append(decisions, event) }

	for _, cand := range candidates {
		processed++
		lastCursor = &cand.AlertID
		if _, err = tx.Exec(`UPDATE silence_recheck_job_candidates SET processed_at=? WHERE job_id=? AND alert_id=?`, nowRaw, jobID, cand.AlertID); err != nil {
			_ = tx.Rollback()
			return 0, fmt.Errorf("failed marking recheck candidate processed: %w", err)
		}
		if strings.TrimSpace(cand.MatchLabelsJSON) == "" {
			counter, _ := ClassifyRecheckCandidate(true, RecheckCandidateMissingLabels)
			if counter == "missing_labels" {
				missing++
			}
			appendDecision(recheckCandidateSkippedEvent(jobID, cand.AlertID, "missing_labels"))
			continue
		}
		labels := map[string]string{}
		if err = json.Unmarshal([]byte(cand.MatchLabelsJSON), &labels); err != nil {
			missing++
			appendDecision(recheckCandidateSkippedEvent(jobID, cand.AlertID, "invalid_labels"))
			continue
		}
		relevant, rerr := parseRelevantSourceNames(cand.RelevantSourcesJSON, cand.RelevantUpstreamsJSON)
		if rerr != nil || len(relevant) == 0 {
			ineligible++
			appendDecision(recheckCandidateSkippedEvent(jobID, cand.AlertID, "invalid_sources"))
			continue
		}

		targetRows, terr := readTargetStatesTx(tx, cand.AlertID)
		if terr != nil {
			_ = tx.Rollback()
			return 0, terr
		}
		if len(targetRows) == 0 {
			matched, complete := evaluateRecheckRules(rules, sourceStatuses, nil, relevant, labels)
			if len(matched) == 0 {
				counter, _ := ClassifyRecheckCandidate(complete, RecheckCandidateNonmatch)
				switch counter {
				case "ineligible":
					ineligible++
				case "incomplete_sources":
					incompleteSources++
				}
				reason := "ineligible"
				if !complete {
					reason = "incomplete_sources"
				}
				appendDecision(recheckCandidateSkippedEvent(jobID, cand.AlertID, reason))
				continue
			}
			updated, transitionID, uerr := promoteAlertSuppressedCAS(tx, cand, now)
			if uerr != nil {
				_ = tx.Rollback()
				return 0, uerr
			}
			if !updated {
				counter, _ := ClassifyRecheckCandidate(true, RecheckCandidateStaleCAS)
				if counter == "stale_conflicts" {
					stale++
				}
				appendDecision(recheckCandidateSkippedEvent(jobID, cand.AlertID, "stale_conflict"))
				continue
			}
			promoted++
			appendDecision(runtimeEvent{level: slog.LevelDebug, component: "silence_recheck", event: "candidate_promoted", message: "Silence recheck candidate promoted", attrs: []slog.Attr{slog.Int64("job_id", jobID), slog.String("alert_id", cand.AlertID)}})
			if err = insertSuppressionCauseHistoryTx(tx, cand.AlertID, transitionID, jobID, now, matched); err != nil {
				_ = tx.Rollback()
				return 0, err
			}
			continue
		}

		anyTargetEvidence := false
		anyTargetApplied := false
		allTargetsSuppressed := true
		candidateIncompleteSources := false
		targetCauseHistoryIDs := make([]int64, 0, len(targetRows))

		for _, row := range targetRows {
			if row.RetiredAt != nil {
				continue
			}
			if row.IsLive || row.IsResolved {
				if row.IsLive || !row.IsSuppressed {
					allTargetsSuppressed = false
				}
				continue
			}
			if row.IsSuppressed {
				historyID, found, causeErr := latestTargetSuppressionCauseHistoryTx(tx, cand.AlertID, row.Target)
				if causeErr != nil {
					_ = tx.Rollback()
					return 0, causeErr
				}
				if !found {
					allTargetsSuppressed = false
					continue
				}
				anyTargetEvidence = true
				targetCauseHistoryIDs = append(targetCauseHistoryIDs, historyID)
				continue
			}
			targetSources := sourceSetFromMembers(row.SourceMembers)
			if len(targetSources) == 0 {
				if !row.IsSuppressed {
					allTargetsSuppressed = false
				}
				continue
			}
			matched, complete := evaluateRecheckRules(rules, sourceStatuses, &row.Target, targetSources, labels)
			if len(matched) == 0 {
				if !complete {
					candidateIncompleteSources = true
				}
				if !row.IsSuppressed {
					allTargetsSuppressed = false
				}
				continue
			}
			anyTargetEvidence = true

			applied, eventID, perr := promoteTargetSuppressedCASTx(tx, cand.AlertID, row, now, "silence_match_promoted")
			if perr != nil {
				_ = tx.Rollback()
				return 0, perr
			}
			if applied {
				anyTargetApplied = true
				historyID, causeErr := insertTargetRecheckSuppressionCauseTx(tx, cand.AlertID, row.Target, eventID, jobID, now, matched)
				if causeErr != nil {
					_ = tx.Rollback()
					return 0, causeErr
				}
				targetCauseHistoryIDs = append(targetCauseHistoryIDs, historyID)
				appendDecision(runtimeEvent{level: slog.LevelDebug, component: "silence_recheck", event: "candidate_promoted", message: "Silence recheck target candidate promoted", attrs: []slog.Attr{slog.Int64("job_id", jobID), slog.String("alert_id", cand.AlertID), slog.String("target", row.Target)}})
			} else {
				allTargetsSuppressed = false
			}
		}

		if !anyTargetEvidence {
			if candidateIncompleteSources {
				incompleteSources++
			} else {
				ineligible++
			}
			reason := "ineligible"
			if candidateIncompleteSources {
				reason = "incomplete_sources"
			}
			appendDecision(recheckCandidateSkippedEvent(jobID, cand.AlertID, reason))
			continue
		}
		if !allTargetsSuppressed {
			if anyTargetApplied {
				promoted++
			} else {
				stale++
			}
			if !anyTargetApplied {
				appendDecision(recheckCandidateSkippedEvent(jobID, cand.AlertID, "stale_conflict"))
			}
			continue
		}

		updated, transitionID, uerr := promoteAlertSuppressedCAS(tx, cand, now)
		if uerr != nil {
			_ = tx.Rollback()
			return 0, uerr
		}
		if !updated {
			stale++
			appendDecision(recheckCandidateSkippedEvent(jobID, cand.AlertID, "stale_conflict"))
			continue
		}
		promoted++
		appendDecision(runtimeEvent{level: slog.LevelDebug, component: "silence_recheck", event: "candidate_promoted", message: "Silence recheck candidate promoted", attrs: []slog.Attr{slog.Int64("job_id", jobID), slog.String("alert_id", cand.AlertID)}})
		if err = insertTargetAggregateSuppressionCauseTx(tx, cand.AlertID, transitionID, now, targetCauseHistoryIDs); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}
	if _, err = tx.Exec(`
UPDATE silence_recheck_jobs
SET cursor_alert_id = ?,
    processed_candidates = processed_candidates + ?,
    promoted_alerts = promoted_alerts + ?,
    skipped_missing_labels = skipped_missing_labels + ?,
    skipped_ineligible = skipped_ineligible + ?,
    skipped_incomplete_sources = skipped_incomplete_sources + ?,
    stale_conflicts = stale_conflicts + ?
WHERE job_id = ?
`, lastCursor, processed, promoted, missing, ineligible, incompleteSources, stale, jobID); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("failed to update recheck job progress: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit recheck batch: %w", err)
	}
	if status == "queued" {
		runtimeLog.InfoContext(ctx, "silence_recheck", "job_started", "Silence recheck job started", slog.Int64("job_id", jobID), slog.Int("candidate_count", totalCandidates), slog.Int("pinned_rule_count", len(rules)), slog.Int("source_count", len(sourceStatuses)))
	} else {
		runtimeLog.InfoContext(ctx, "silence_recheck", "job_resumed", "Silence recheck job resumed", slog.Int64("job_id", jobID), slog.Int("prior_processed", priorProcessed), slog.Int("candidate_count", totalCandidates))
	}
	emitRuntimeEventsContext(ctx, decisions)
	level := slog.LevelDebug
	if promoted > 0 {
		level = slog.LevelInfo
	}
	runtimeLog.Event(ctx, level, "silence_recheck", "batch_completed", "Silence recheck batch completed", slog.Int64("job_id", jobID), slog.Int("processed", processed), slog.Int("promoted", promoted), slog.Int("stale", stale), slog.Int("missing_labels", missing), slog.Int("ineligible", ineligible), slog.Int("incomplete_sources", incompleteSources), slog.Duration("duration", time.Since(startedAt)))

	return promoted, nil
}

func recheckCandidateSkippedEvent(jobID int64, alertID, reason string) runtimeEvent {
	return runtimeEvent{level: slog.LevelDebug, component: "silence_recheck", event: "candidate_skipped", message: "Silence recheck candidate skipped", attrs: []slog.Attr{slog.Int64("job_id", jobID), slog.String("alert_id", alertID), slog.String("reason_code", reason)}}
}

func logCompletedRecheckJob(ctx context.Context, db *sql.DB, jobID int64, duration time.Duration) {
	var processed, promoted, missing, ineligible, incomplete, stale int
	err := db.QueryRow(`
SELECT processed_candidates, promoted_alerts, skipped_missing_labels,
       skipped_ineligible, skipped_incomplete_sources, stale_conflicts
FROM silence_recheck_jobs WHERE job_id=?
`, jobID).Scan(&processed, &promoted, &missing, &ineligible, &incomplete, &stale)
	if err != nil {
		runtimeLog.ErrorContext(ctx, "silence_recheck", "job_completion_log_failed", "Failed to load completed silence recheck job counters", slog.Int64("job_id", jobID), slog.Any("error", err))
		return
	}
	runtimeLog.InfoContext(ctx, "silence_recheck", "job_completed", "Silence recheck job completed", slog.Int64("job_id", jobID), slog.Int("processed", processed), slog.Int("promoted", promoted), slog.Int("missing_labels", missing), slog.Int("ineligible", ineligible), slog.Int("incomplete_sources", incomplete), slog.Int("stale", stale), slog.Duration("duration", duration))
}

func attachEligibleRecheckCandidatesTx(tx *sql.Tx, jobID int64) error {
	if _, err := tx.Exec(`
INSERT OR IGNORE INTO silence_recheck_job_candidates(job_id, alert_id)
SELECT ?, alert_id FROM alert_states
WHERE match_labels_json IS NOT NULL AND trim(match_labels_json) != ''
  AND (
    (state='inactive' AND is_live=0 AND is_resolved=0 AND is_suppressed=0)
    OR EXISTS (
      SELECT 1 FROM alert_target_states target
      WHERE target.alert_id=alert_states.alert_id
        AND target.retired_at IS NULL AND target.state='inactive'
        AND target.is_live=0 AND target.is_resolved=0 AND target.is_suppressed=0
    )
  )
`, jobID); err != nil {
		return fmt.Errorf("failed attaching eligible recheck candidates: %w", err)
	}
	if _, err := tx.Exec(`
UPDATE silence_recheck_jobs
SET total_candidates=(SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=?)
WHERE job_id=?
`, jobID, jobID); err != nil {
		return fmt.Errorf("failed counting eligible recheck candidates: %w", err)
	}
	return nil
}

type recheckCandidate struct {
	AlertID               string
	RawState              string
	LastSeenAt            sql.NullString
	MatchLabelsJSON       string
	RelevantUpstreamsJSON string
	RelevantSourcesJSON   string
}

func selectNextRecheckJobTx(tx *sql.Tx) (int64, *string, string, error) {
	var (
		jobID  int64
		cursor sql.NullString
		status string
	)
	err := tx.QueryRow(`
SELECT job_id, cursor_alert_id, status
FROM silence_recheck_jobs
WHERE status IN ('queued','running','failed')
ORDER BY job_id ASC
LIMIT 1
`).Scan(&jobID, &cursor, &status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil, "", nil
		}
		return 0, nil, "", fmt.Errorf("failed to select next recheck job: %w", err)
	}
	if cursor.Valid {
		v := cursor.String
		return jobID, &v, status, nil
	}
	return jobID, nil, status, nil
}

func loadRecheckJobRulesTx(tx *sql.Tx, jobID int64) ([]recheckJobRule, error) {
	rows, err := tx.Query(`
SELECT jr.source_name, jr.cluster_id, jr.rule_id, jr.revision_id, jr.observed_state, sr.matchers_json
FROM silence_recheck_job_rules jr
JOIN silence_revisions sr ON sr.revision_id = jr.revision_id AND sr.rule_id = jr.rule_id
WHERE jr.job_id = ?
`, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to load recheck job rules: %w", err)
	}
	defer rows.Close()

	out := []recheckJobRule{}
	for rows.Next() {
		var r recheckJobRule
		if err = rows.Scan(&r.SourceName, &r.ClusterID, &r.RuleID, &r.RevisionID, &r.ObservedState, &r.MatchersJSON); err != nil {
			return nil, fmt.Errorf("failed to scan recheck job rule: %w", err)
		}
		out = append(out, r)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating recheck job rules: %w", err)
	}
	return out, nil
}

func loadRecheckCandidatesTx(tx *sql.Tx, jobID int64, batchSize int) ([]recheckCandidate, error) {
	rows, err := tx.Query(`
SELECT alerts.alert_id, alerts.raw_state, alerts.last_seen_at, alerts.match_labels_json,
       alerts.relevant_upstreams_json, alerts.relevant_sources_json
FROM silence_recheck_job_candidates candidates
JOIN alert_states alerts ON alerts.alert_id=candidates.alert_id
WHERE (
    (
      alerts.state = 'inactive'
      AND alerts.is_live = 0
      AND alerts.is_resolved = 0
      AND alerts.is_suppressed = 0
    )
    OR EXISTS (
      SELECT 1
      FROM alert_target_states target
      WHERE target.alert_id = alerts.alert_id
        AND target.retired_at IS NULL
        AND target.state = 'inactive'
        AND target.is_live = 0
        AND target.is_resolved = 0
        AND target.is_suppressed = 0
    )
  )
  AND candidates.job_id=?
  AND candidates.processed_at IS NULL
  AND alerts.match_labels_json IS NOT NULL
  AND trim(alerts.match_labels_json) != ''
ORDER BY alerts.alert_id
LIMIT ?
`, jobID, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to load recheck candidates: %w", err)
	}
	defer rows.Close()

	out := []recheckCandidate{}
	for rows.Next() {
		var c recheckCandidate
		if err = rows.Scan(&c.AlertID, &c.RawState, &c.LastSeenAt, &c.MatchLabelsJSON, &c.RelevantUpstreamsJSON, &c.RelevantSourcesJSON); err != nil {
			return nil, fmt.Errorf("failed scanning recheck candidate: %w", err)
		}
		out = append(out, c)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating recheck candidates: %w", err)
	}
	return out, nil
}

func parseRelevantSourceNames(relevantSourcesJSON, relevantUpstreamsJSON string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	if strings.TrimSpace(relevantSourcesJSON) != "" {
		var refs []SourceRef
		if err := json.Unmarshal([]byte(relevantSourcesJSON), &refs); err == nil {
			for _, ref := range refs {
				name := strings.TrimSpace(ref.Name)
				if name != "" {
					out[name] = struct{}{}
				}
			}
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	if strings.TrimSpace(relevantUpstreamsJSON) == "" {
		return nil, errors.New("no relevant source data")
	}
	var names []string
	if err := json.Unmarshal([]byte(relevantUpstreamsJSON), &names); err != nil {
		return nil, err
	}
	for _, n := range names {
		n = strings.TrimSpace(n)
		if n != "" {
			out[n] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("empty relevant source set")
	}
	return out, nil
}

func evaluateRecheckRules(rules []recheckJobRule, sourceStatuses map[string]string, target *string, relevant map[string]struct{}, labels map[string]string) ([]recheckJobRule, bool) {
	if len(relevant) == 0 {
		return nil, false
	}
	for sourceName := range relevant {
		status, found := sourceStatuses[sourceName]
		if !found || (status != "success" && status != "retired") {
			return nil, false
		}
	}
	matched := make([]recheckJobRule, 0)
	matchedBySource := make(map[string]bool, len(relevant))
	for _, r := range rules {
		if r.ObservedState != "active" {
			continue
		}
		if sourceStatuses[r.SourceName] == "retired" {
			continue
		}
		if _, ok := relevant[r.SourceName]; !ok {
			continue
		}
		if target != nil && r.ClusterID != *target {
			continue
		}
		ok, err := matchesSilenceMatchers(r.MatchersJSON, labels)
		if err != nil || !ok {
			continue
		}
		matched = append(matched, r)
		matchedBySource[r.SourceName] = true
	}
	for sourceName := range relevant {
		status := sourceStatuses[sourceName]
		if status == "retired" {
			continue
		}
		if !matchedBySource[sourceName] {
			return nil, true
		}
	}
	return matched, true
}

func loadRecheckJobSourceStatusesTx(tx *sql.Tx, jobID int64) (map[string]string, error) {
	var raw string
	if err := tx.QueryRow(`SELECT source_context_json FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&raw); err != nil {
		return nil, fmt.Errorf("failed loading recheck source context: %w", err)
	}
	var context struct {
		Sources []struct {
			SourceName string `json:"sourceName"`
			SyncStatus string `json:"syncStatus"`
		} `json:"sources"`
	}
	if err := json.Unmarshal([]byte(raw), &context); err != nil {
		return nil, fmt.Errorf("failed decoding recheck source context: %w", err)
	}
	statuses := make(map[string]string, len(context.Sources))
	for _, source := range context.Sources {
		name := strings.TrimSpace(source.SourceName)
		if name != "" {
			statuses[name] = strings.TrimSpace(source.SyncStatus)
		}
	}
	return statuses, nil
}

func sourceSetFromMembers(members []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, member := range members {
		name := strings.TrimSpace(member)
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	return out
}

func matchesSilenceMatchers(matchersJSON string, labels map[string]string) (bool, error) {
	var snapshots []SilenceMatcherSnapshot
	if err := json.Unmarshal([]byte(matchersJSON), &snapshots); err != nil {
		return false, err
	}
	for _, m := range snapshots {
		sm := models.NewSilenceMatcher(m.Name, m.Value, m.IsRegex, m.IsEqual)
		if !sm.IsMatch(labels) {
			return false, nil
		}
	}
	return true, nil
}

func promoteTargetSuppressedCASTx(
	tx *sql.Tx,
	alertID string,
	row targetStateRow,
	now time.Time,
	reasonCode string,
) (bool, int64, error) {
	promotionPlan, eligible := PromoteSuppressed(LifecycleDimensions{
		RawState: row.RawState, IsLive: row.IsLive,
		IsSuppressed: row.IsSuppressed, IsResolved: row.IsResolved,
	})
	if !eligible {
		return false, 0, nil
	}

	nowRaw := now.UTC().Format(time.RFC3339Nano)
	var res sql.Result
	var err error
	if row.LastSeenAt != nil {
		res, err = tx.Exec(`
UPDATE alert_target_states
SET is_suppressed = 1,
    state = 'suppressed',
    updated_at = ?
WHERE alert_id = ?
  AND target = ?
  AND is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND last_seen_at = ?
`, nowRaw, alertID, row.Target, row.LastSeenAt.UTC().Format(time.RFC3339Nano))
	} else {
		res, err = tx.Exec(`
UPDATE alert_target_states
SET is_suppressed = 1,
    state = 'suppressed',
    updated_at = ?
WHERE alert_id = ?
  AND target = ?
  AND is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND last_seen_at IS NULL
`, nowRaw, alertID, row.Target)
	}
	if err != nil {
		return false, 0, fmt.Errorf("failed to promote target state via CAS: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, 0, fmt.Errorf("failed to read target CAS affected rows: %w", err)
	}
	if affected == 0 {
		return false, 0, nil
	}

	oldState := row.State
	newState := promotionPlan.ProjectedState()
	oldRaw := row.RawState
	oldLive := row.IsLive
	newLive := promotionPlan.IsLive
	oldSupp := row.IsSuppressed
	newSupp := promotionPlan.IsSuppressed
	oldResolved := row.IsResolved
	newResolved := promotionPlan.IsResolved

	transitionID, err := insertTargetTransitionTxWithID(tx, alertID, targetStateRow{
		Target:        row.Target,
		State:         newState,
		RawState:      oldRaw,
		IsLive:        newLive,
		IsSuppressed:  newSupp,
		IsResolved:    newResolved,
		StartsAt:      row.StartsAt,
		LastSeenAt:    row.LastSeenAt,
		SourceMembers: row.SourceMembers,
	}, &oldState, &oldRaw, &oldLive, &oldSupp, &oldResolved, reasonCode, now)
	if err != nil {
		return false, 0, err
	}

	return true, transitionID, nil
}

func insertTargetTransitionTxWithID(
	tx *sql.Tx,
	alertID string,
	row targetStateRow,
	oldState *State,
	oldRaw *State,
	oldLive *bool,
	oldSupp *bool,
	oldResolved *bool,
	reasonCode string,
	changedAt time.Time,
) (int64, error) {
	res, err := tx.Exec(`
INSERT INTO alert_target_events(
  alert_id,
  target,
  old_state,
  new_state,
  old_raw_state,
  new_raw_state,
  old_is_live,
  new_is_live,
  old_is_suppressed,
  new_is_suppressed,
  old_is_resolved,
  new_is_resolved,
  changed_at,
  reason_code
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
		alertID,
		row.Target,
		formatNullableState(oldState),
		string(row.State),
		formatNullableState(oldRaw),
		string(row.RawState),
		formatNullableBool(oldLive),
		boolToInt(row.IsLive),
		formatNullableBool(oldSupp),
		boolToInt(row.IsSuppressed),
		formatNullableBool(oldResolved),
		boolToInt(row.IsResolved),
		changedAt.UTC().Format(time.RFC3339Nano),
		sanitizeReasonCode(ptrString(reasonCode)),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert target transition: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to read target transition id: %w", err)
	}
	return id, nil
}

func promoteAlertSuppressedCAS(tx *sql.Tx, cand recheckCandidate, now time.Time) (bool, int64, error) {
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	var res sql.Result
	var err error
	if cand.LastSeenAt.Valid {
		res, err = tx.Exec(`
UPDATE alert_states
SET is_suppressed = 1,
    suppression_observed_at = ?,
    state = 'suppressed',
    updated_at = ?
WHERE alert_id = ?
  AND state = 'inactive'
  AND is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND last_seen_at = ?
`, nowRaw, nowRaw, cand.AlertID, cand.LastSeenAt.String)
	} else {
		res, err = tx.Exec(`
UPDATE alert_states
SET is_suppressed = 1,
    suppression_observed_at = ?,
    state = 'suppressed',
    updated_at = ?
WHERE alert_id = ?
  AND state = 'inactive'
  AND is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND last_seen_at IS NULL
`, nowRaw, nowRaw, cand.AlertID)
	}
	if err != nil {
		return false, 0, fmt.Errorf("failed to promote alert via CAS: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, 0, fmt.Errorf("failed to read CAS affected rows: %w", err)
	}
	if affected == 0 {
		return false, 0, nil
	}

	oldState := StateInactive
	oldRaw := State(strings.TrimSpace(cand.RawState))
	if !oldRaw.IsValid() {
		oldRaw = StateActive
	}
	promotionPlan, _ := PromoteSuppressed(LifecycleDimensions{RawState: oldRaw})
	oldLive := false
	newLive := promotionPlan.IsLive
	oldSupp := false
	newSupp := promotionPlan.IsSuppressed
	oldResolved := false
	newResolved := promotionPlan.IsResolved
	newState := promotionPlan.ProjectedState()
	reason := sanitizeReasonCode(ptrString("silence_match_promoted"))
	transitionID, err := insertTransitionTxWithID(tx, AlertStateTransition{
		AlertID:         cand.AlertID,
		OldState:        &oldState,
		NewState:        newState,
		OldRawState:     &oldRaw,
		NewRawState:     oldRaw,
		OldIsLive:       &oldLive,
		NewIsLive:       newLive,
		OldIsSuppressed: &oldSupp,
		NewIsSuppressed: newSupp,
		OldIsResolved:   &oldResolved,
		NewIsResolved:   newResolved,
		ChangedAt:       now,
		ChangeSource:    ChangeSourceSilence,
		ReasonCode:      reason,
	})
	if err != nil {
		return false, 0, err
	}
	if err := updateTargetStatesSuppressedTx(tx, cand.AlertID, now, "silence_match_promoted"); err != nil {
		return false, 0, err
	}
	return true, transitionID, nil
}

func updateTargetStatesSuppressedTx(tx *sql.Tx, alertID string, now time.Time, reasonCode string) error {
	existing, err := readTargetStatesTx(tx, alertID)
	if err != nil {
		return err
	}
	for _, row := range existing {
		promotionPlan, eligible := PromoteSuppressed(LifecycleDimensions{
			RawState: row.RawState, IsLive: row.IsLive,
			IsSuppressed: row.IsSuppressed, IsResolved: row.IsResolved,
		})
		if !eligible {
			continue
		}
		row.IsSuppressed = promotionPlan.IsSuppressed
		row.State = promotionPlan.ProjectedState()
		if err := upsertTargetStateTx(tx, alertID, row, reasonCode, now); err != nil {
			return err
		}
	}
	return nil
}

func insertSuppressionCauseHistoryTx(tx *sql.Tx, alertID string, transitionEventID, jobID int64, now time.Time, matched []recheckJobRule) error {
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	fingerprint := buildCauseFingerprint(matched)
	res, err := tx.Exec(`
INSERT INTO alert_suppression_cause_history(
  alert_id, transition_event_id, recheck_job_id, source,
  cause_status, cause_fingerprint, observed_at, last_confirmed_at
) VALUES(?, ?, ?, 'silence_recheck', 'known', ?, ?, ?)
`, alertID, transitionEventID, jobID, fingerprint, nowRaw, nowRaw)
	if err != nil {
		return fmt.Errorf("failed to insert suppression cause history: %w", err)
	}
	historyID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to read suppression cause history id: %w", err)
	}

	for _, m := range matched {
		if _, err = tx.Exec(`
INSERT INTO alert_suppression_cause_members(history_id, source_name, rule_id, revision_id, resolution_status)
VALUES(?, ?, ?, ?, 'known')
`, historyID, m.SourceName, m.RuleID, m.RevisionID); err != nil {
			return fmt.Errorf("failed to insert suppression cause member: %w", err)
		}
	}
	return nil
}

func insertTargetRecheckSuppressionCauseTx(tx *sql.Tx, alertID, target string, targetEventID, jobID int64, now time.Time, matched []recheckJobRule) (int64, error) {
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	res, err := tx.Exec(`
INSERT INTO alert_target_suppression_cause_history(
  alert_id, target, target_transition_event_id, recheck_job_id,
  source, cause_status, cause_fingerprint, observed_at, last_confirmed_at
) VALUES(?, ?, ?, ?, 'silence_recheck', 'known', ?, ?, ?)
`, alertID, target, targetEventID, jobID, buildCauseFingerprint(matched), nowRaw, nowRaw)
	if err != nil {
		return 0, fmt.Errorf("failed to insert target suppression cause history: %w", err)
	}
	historyID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to read target suppression cause history id: %w", err)
	}
	for _, matchedRule := range matched {
		if _, err = tx.Exec(`
INSERT INTO alert_target_suppression_cause_members(
  history_id, source_name, rule_id, revision_id, resolution_status
) VALUES(?, ?, ?, ?, 'known')
`, historyID, matchedRule.SourceName, matchedRule.RuleID, matchedRule.RevisionID); err != nil {
			return 0, fmt.Errorf("failed to insert target suppression cause member: %w", err)
		}
	}
	return historyID, nil
}

func latestTargetSuppressionCauseHistoryTx(tx *sql.Tx, alertID, target string) (int64, bool, error) {
	var historyID int64
	err := tx.QueryRow(`
SELECT history_id
FROM alert_target_suppression_cause_history
WHERE alert_id=? AND target=?
ORDER BY history_id DESC LIMIT 1
`, alertID, target).Scan(&historyID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to read effective target suppression cause: %w", err)
	}
	return historyID, true, nil
}

func insertTargetAggregateSuppressionCauseTx(tx *sql.Tx, alertID string, transitionEventID int64, now time.Time, targetHistoryIDs []int64) error {
	if len(targetHistoryIDs) == 0 {
		return errors.New("target aggregate suppression cause requires target histories")
	}
	slices.Sort(targetHistoryIDs)
	uniqueIDs := targetHistoryIDs[:0]
	for _, historyID := range targetHistoryIDs {
		if len(uniqueIDs) == 0 || uniqueIDs[len(uniqueIDs)-1] != historyID {
			uniqueIDs = append(uniqueIDs, historyID)
		}
	}
	raw, _ := json.Marshal(uniqueIDs)
	sum := sha256.Sum256(raw)
	res, err := tx.Exec(`
INSERT INTO alert_target_aggregate_suppression_history(
  alert_id, transition_event_id, cause_fingerprint, observed_at
) VALUES(?, ?, ?, ?)
`, alertID, transitionEventID, fmt.Sprintf("sha256:%x", sum), now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("failed to insert target aggregate suppression history: %w", err)
	}
	aggregateID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to read target aggregate suppression id: %w", err)
	}
	for _, historyID := range uniqueIDs {
		if _, err = tx.Exec(`
INSERT INTO alert_target_aggregate_suppression_links(aggregate_id, target_history_id)
VALUES(?, ?)
`, aggregateID, historyID); err != nil {
			return fmt.Errorf("failed to link target aggregate suppression cause: %w", err)
		}
	}
	return nil
}

func insertLiveSuppressionCauseTx(tx *sql.Tx, alertID string, refs []SourceSilenceRef, now time.Time) error {
	members := make([]suppressionCauseMember, 0)
	unresolvedCount := 0

	for _, ref := range refs {
		sourceName := strings.TrimSpace(ref.SourceName)
		clusterID := strings.TrimSpace(ref.ClusterID)
		if sourceName == "" || clusterID == "" {
			continue
		}
		for _, silenceID := range ref.SilenceIDs {
			silenceID = strings.TrimSpace(silenceID)
			if silenceID == "" {
				continue
			}
			ruleID, revisionID, known, err := resolveLiveSilenceMemberTx(tx, sourceName, clusterID, silenceID, now)
			if err != nil {
				return err
			}
			m := suppressionCauseMember{sourceName: sourceName, ruleID: ruleID, known: known}
			if known {
				m.revisionID = &revisionID
			} else {
				unresolvedCount++
			}
			members = append(members, m)
		}
	}

	var status string
	switch {
	case len(members) == 0:
		status = "none"
	case unresolvedCount > 0:
		status = "partial"
	default:
		status = "known"
	}

	fingerprint := buildLiveCauseFingerprint(status, members)
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	var latestID int64
	var latestFP, latestStatus string
	err := tx.QueryRow(`
SELECT history_id, cause_fingerprint, cause_status
FROM alert_suppression_cause_history
WHERE alert_id = ? AND source = 'live_alertmanager'
ORDER BY history_id DESC
LIMIT 1
`, alertID).Scan(&latestID, &latestFP, &latestStatus)
	if err == nil {
		if latestFP == fingerprint && latestStatus == status {
			if _, err = tx.Exec(`UPDATE alert_suppression_cause_history SET last_confirmed_at = ? WHERE history_id = ?`, nowRaw, latestID); err != nil {
				return fmt.Errorf("failed to refresh live cause timestamp: %w", err)
			}
			return nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query latest live cause history: %w", err)
	}

	res, err := tx.Exec(`
INSERT INTO alert_suppression_cause_history(
  alert_id, transition_event_id, recheck_job_id, source,
  cause_status, cause_fingerprint, observed_at, last_confirmed_at
) VALUES(?, NULL, NULL, 'live_alertmanager', ?, ?, ?, ?)
`, alertID, status, fingerprint, nowRaw, nowRaw)
	if err != nil {
		return fmt.Errorf("failed to insert live suppression cause history: %w", err)
	}
	historyID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to read live suppression cause history id: %w", err)
	}
	for _, m := range members {
		resolution := "unresolved"
		var revision any
		if m.known && m.revisionID != nil {
			resolution = "known"
			revision = *m.revisionID
		}
		if _, err = tx.Exec(`
INSERT INTO alert_suppression_cause_members(history_id, source_name, rule_id, revision_id, resolution_status)
VALUES(?, ?, ?, ?, ?)
`, historyID, m.sourceName, m.ruleID, revision, resolution); err != nil {
			return fmt.Errorf("failed to insert live suppression cause member: %w", err)
		}
	}
	return nil
}

func insertLiveTargetSuppressionCausesTx(tx *sql.Tx, alertID string, targets []LiveTargetSnapshot, refs []SourceSilenceRef, now time.Time) error {
	for _, target := range targets {
		if target.State != StateSuppressed {
			continue
		}
		targetName := strings.TrimSpace(target.Target)
		if targetName == "" {
			continue
		}
		members := make([]suppressionCauseMember, 0)
		unresolvedCount := 0
		for _, ref := range refs {
			if strings.TrimSpace(ref.ClusterID) != targetName {
				continue
			}
			for _, silenceID := range ref.SilenceIDs {
				ruleID, revisionID, known, err := resolveLiveSilenceMemberTx(tx, strings.TrimSpace(ref.SourceName), targetName, strings.TrimSpace(silenceID), now)
				if err != nil {
					return err
				}
				member := suppressionCauseMember{sourceName: strings.TrimSpace(ref.SourceName), ruleID: ruleID, known: known}
				if known {
					member.revisionID = &revisionID
				} else {
					unresolvedCount++
				}
				members = append(members, member)
			}
		}
		status := "none"
		if len(members) > 0 {
			status = "known"
			if unresolvedCount > 0 {
				status = "partial"
			}
		}
		fingerprint := buildLiveCauseFingerprint(status, members)
		nowRaw := now.UTC().Format(time.RFC3339Nano)
		var latestID int64
		var latestFP, latestStatus string
		err := tx.QueryRow(`
SELECT history_id, cause_fingerprint, cause_status
FROM alert_target_suppression_cause_history
WHERE alert_id=? AND target=? AND source='live_alertmanager'
ORDER BY history_id DESC LIMIT 1
`, alertID, targetName).Scan(&latestID, &latestFP, &latestStatus)
		if err == nil && latestFP == fingerprint && latestStatus == status {
			if _, err = tx.Exec(`UPDATE alert_target_suppression_cause_history SET last_confirmed_at=? WHERE history_id=?`, nowRaw, latestID); err != nil {
				return fmt.Errorf("failed to refresh target live cause timestamp: %w", err)
			}
			continue
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to query latest target live cause: %w", err)
		}
		res, err := tx.Exec(`
INSERT INTO alert_target_suppression_cause_history(
  alert_id, target, target_transition_event_id, recheck_job_id,
  source, cause_status, cause_fingerprint, observed_at, last_confirmed_at
) VALUES(?, ?, NULL, NULL, 'live_alertmanager', ?, ?, ?, ?)
`, alertID, targetName, status, fingerprint, nowRaw, nowRaw)
		if err != nil {
			return fmt.Errorf("failed to insert target live suppression cause: %w", err)
		}
		historyID, _ := res.LastInsertId()
		for _, member := range members {
			resolution := "unresolved"
			var revision any
			if member.known && member.revisionID != nil {
				resolution = "known"
				revision = *member.revisionID
			}
			if _, err = tx.Exec(`
INSERT INTO alert_target_suppression_cause_members(history_id, source_name, rule_id, revision_id, resolution_status)
VALUES(?, ?, ?, ?, ?)
`, historyID, member.sourceName, member.ruleID, revision, resolution); err != nil {
				return fmt.Errorf("failed to insert target live suppression cause member: %w", err)
			}
		}
	}
	return nil
}

func resolveLiveSilenceMemberTx(tx *sql.Tx, sourceName, clusterID, silenceID string, now time.Time) (int64, int64, bool, error) {
	var ruleID int64
	err := tx.QueryRow(`SELECT rule_id FROM silence_rules WHERE cluster_id = ? AND silence_id = ?`, clusterID, silenceID).Scan(&ruleID)
	if errors.Is(err, sql.ErrNoRows) {
		nowRaw := now.UTC().Format(time.RFC3339Nano)
		res, ierr := tx.Exec(`INSERT INTO silence_rules(cluster_id, silence_id, first_observed_at, last_observed_at) VALUES(?, ?, ?, ?)`, clusterID, silenceID, nowRaw, nowRaw)
		if ierr != nil {
			return 0, 0, false, fmt.Errorf("failed to insert unresolved silence rule: %w", ierr)
		}
		ruleID, _ = res.LastInsertId()
		return ruleID, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("failed to query silence rule id: %w", err)
	}

	var revisionID int64
	err = tx.QueryRow(`
SELECT ass.revision_id
FROM alertmanager_source_silences ass
WHERE ass.source_name = ? AND ass.rule_id = ?
`, sourceName, ruleID).Scan(&revisionID)
	if errors.Is(err, sql.ErrNoRows) {
		return ruleID, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("failed to query source silence revision: %w", err)
	}
	return ruleID, revisionID, true, nil
}

func buildLiveCauseFingerprint(status string, members []suppressionCauseMember) string {
	keys := make([]string, 0, len(members)+1)
	keys = append(keys, "status:"+status)
	for _, m := range members {
		if m.known && m.revisionID != nil {
			keys = append(keys, fmt.Sprintf("k:%s:%d:%d", m.sourceName, m.ruleID, *m.revisionID))
		} else {
			keys = append(keys, fmt.Sprintf("u:%s:%d", m.sourceName, m.ruleID))
		}
	}
	slices.Sort(keys)
	raw, _ := json.Marshal(keys)
	s := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", s)
}

func buildCauseFingerprint(matched []recheckJobRule) string {
	keys := make([]string, 0, len(matched))
	for _, m := range matched {
		keys = append(keys, fmt.Sprintf("%s:%d:%d", m.SourceName, m.RuleID, m.RevisionID))
	}
	slices.Sort(keys)
	raw, _ := json.Marshal(keys)
	s := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", s)
}

func parseRelevantUpstreams(raw string) ([]string, error) {
	var upstreams []string
	if err := json.Unmarshal([]byte(raw), &upstreams); err != nil {
		return nil, err
	}
	if len(upstreams) == 0 {
		return nil, errors.New("empty relevant upstream list")
	}

	return uniqueSortedStrings(upstreams), nil
}

func marshalRelevantUpstreams(upstreams []string) ([]byte, error) {
	values := uniqueSortedStrings(upstreams)
	if len(values) == 0 {
		return nil, ErrRelevantUpstreamsCorrupt
	}

	out, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal relevant upstreams: %w", err)
	}

	return out, nil
}

func selectStoredRelevantUpstreams(existing, observed []string, allSuccessful bool) []string {
	existing = uniqueSortedStrings(existing)
	observed = uniqueSortedStrings(observed)

	if len(existing) == 0 {
		return observed
	}

	if allSuccessful {
		return observed
	}

	merged := append([]string{}, existing...)
	merged = append(merged, observed...)
	return uniqueSortedStrings(merged)
}

func parseRelevantSources(raw string) ([]SourceRef, error) {
	var refs []SourceRef
	if err := json.Unmarshal([]byte(raw), &refs); err != nil {
		return nil, err
	}
	refs = normalizeSourceRefs(refs)
	if len(refs) == 0 {
		return nil, errors.New("empty relevant source list")
	}
	return refs, nil
}

func marshalRelevantSources(refs []SourceRef) ([]byte, error) {
	refs = normalizeSourceRefs(refs)
	if len(refs) == 0 {
		return nil, ErrRelevantUpstreamsCorrupt
	}
	out, err := json.Marshal(refs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal relevant sources: %w", err)
	}
	return out, nil
}

func selectStoredRelevantSources(existing, observed []SourceRef, allSuccessful bool) []SourceRef {
	existing = normalizeSourceRefs(existing)
	observed = normalizeSourceRefs(observed)

	if len(existing) == 0 {
		return observed
	}
	if allSuccessful {
		return observed
	}
	merged := append([]SourceRef{}, existing...)
	merged = append(merged, observed...)
	return normalizeSourceRefs(merged)
}

func normalizeSourceRefs(refs []SourceRef) []SourceRef {
	if len(refs) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]SourceRef, 0, len(refs))
	for _, ref := range refs {
		name := strings.TrimSpace(ref.Name)
		cluster := strings.TrimSpace(ref.Cluster)
		if name == "" || cluster == "" {
			continue
		}
		k := name + "\x00" + cluster
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, SourceRef{Name: name, Cluster: cluster})
	}
	slices.SortFunc(out, func(a, b SourceRef) int {
		if a.Name == b.Name {
			return strings.Compare(a.Cluster, b.Cluster)
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

func uniqueSortedStrings(values []string) []string {
	uniq := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		k := strings.TrimSpace(v)
		if k == "" {
			continue
		}
		if _, found := uniq[k]; found {
			continue
		}
		uniq[k] = struct{}{}
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func allRelevantUpstreamsSuccessful(relevant []string, successful map[string]bool) bool {
	if len(relevant) == 0 {
		return false
	}
	for _, upstream := range relevant {
		if !successful[upstream] {
			return false
		}
	}
	return true
}

func areAllUpstreamsSuccessful(successful map[string]bool) bool {
	if len(successful) == 0 {
		return false
	}
	for _, ok := range successful {
		if !ok {
			return false
		}
	}
	return true
}

func getCurrentAlertTx(tx *sql.Tx, alertID string) (AlertCurrent, error) {
	row := tx.QueryRow(`
SELECT
  alert_id,
  state,
  raw_state,
  is_live,
  is_suppressed,
  is_resolved,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  match_labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  relevant_sources_json,
  suppression_observed_at,
  resolved_at,
  resolved_by,
  resolved_comment,
  updated_at
FROM alert_states
WHERE alert_id = ?
`, alertID)

	return scanCurrentAlert(row)
}

func sanitizeNullableString(v *string) *string {
	if v == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*v)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func deleteExpiredAlertsTx(tx *sql.Tx, cutoff time.Time) (int, error) {
	res, err := tx.Exec(`
DELETE FROM alert_states
WHERE last_seen_at IS NOT NULL
  AND last_seen_at < ?
`, cutoff.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("failed to delete expired alerts: %w", err)
	}

	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to read expired delete count: %w", err)
	}

	return int(clampInt64ToInt(deleted)), nil
}

type silenceRetentionResult struct {
	recheckJobs    int
	retiredSources int
	revisions      int
	episodes       int
	rules          int
}

func runPhase2RetentionTx(ctx context.Context, tx *sql.Tx, now time.Time, silenceHistoryRetentionTTL time.Duration) (silenceRetentionResult, error) {
	result := silenceRetentionResult{}
	cutoff := now.Add(-silenceHistoryRetentionTTL).UTC().Format(time.RFC3339Nano)

	deleteAndCount := func(query string, args ...any) (int, error) {
		res, err := tx.Exec(query, args...)
		if err != nil {
			return 0, err
		}
		count, err := res.RowsAffected()
		if err != nil {
			runtimeLog.ErrorContext(ctx, "lifecycle_housekeeping", "retention_count_failed", "Failed to read silence retention deletion count", slog.Any("error", err))
			return 0, nil
		}
		return int(clampInt64ToInt(count)), nil
	}
	var err error
	result.recheckJobs, err = deleteAndCount(`
DELETE FROM silence_recheck_jobs
WHERE status = 'completed'
  AND job_id NOT IN (
    SELECT recheck_job_id FROM alert_suppression_cause_history WHERE recheck_job_id IS NOT NULL
    UNION
    SELECT recheck_job_id FROM alert_target_suppression_cause_history WHERE recheck_job_id IS NOT NULL
  )
  AND job_id <> (
    SELECT COALESCE(MAX(job_id), 0) FROM silence_recheck_jobs WHERE status = 'completed'
  )
`)
	if err != nil {
		return silenceRetentionResult{}, fmt.Errorf("failed cleaning completed recheck jobs: %w", err)
	}
	result.retiredSources, err = deleteAndCount(`
DELETE FROM alertmanager_sources
WHERE sync_status = 'retired'
  AND retired_at IS NOT NULL
  AND retired_at < ?
  AND source_name NOT IN (
    SELECT source_name FROM alertmanager_source_silences
    UNION
    SELECT source_name FROM silence_recheck_job_rules
    UNION
    SELECT source_name FROM alert_suppression_cause_members
    UNION
    SELECT source_name FROM alert_target_suppression_cause_members
  )
`, cutoff)
	if err != nil {
		return silenceRetentionResult{}, fmt.Errorf("failed cleaning retired sources: %w", err)
	}

	result.revisions, err = deleteAndCount(`
DELETE FROM silence_revisions
WHERE last_observed_at < ?
  AND revision_id NOT IN (
    SELECT revision_id FROM alertmanager_source_silences
    UNION
    SELECT revision_id FROM silence_recheck_job_rules
    UNION
    SELECT revision_id FROM alert_suppression_cause_members WHERE revision_id IS NOT NULL
    UNION
    SELECT revision_id FROM alert_target_suppression_cause_members WHERE revision_id IS NOT NULL
  )
`, cutoff)
	if err != nil {
		return silenceRetentionResult{}, fmt.Errorf("failed cleaning silence revisions: %w", err)
	}

	result.episodes, err = deleteAndCount(`
DELETE FROM silence_rule_episodes
WHERE open_marker IS NULL
  AND ended_at IS NOT NULL
  AND ended_at < ?
  AND episode_id NOT IN (SELECT episode_id FROM silence_revisions)
`, cutoff)
	if err != nil {
		return silenceRetentionResult{}, fmt.Errorf("failed cleaning silence episodes: %w", err)
	}

	result.rules, err = deleteAndCount(`
DELETE FROM silence_rules
WHERE rule_id NOT IN (SELECT rule_id FROM silence_revisions)
  AND rule_id NOT IN (SELECT rule_id FROM alertmanager_source_silences)
  AND rule_id NOT IN (SELECT rule_id FROM silence_recheck_job_rules)
  AND rule_id NOT IN (SELECT rule_id FROM alert_suppression_cause_members)
  AND rule_id NOT IN (SELECT rule_id FROM alert_target_suppression_cause_members)
  AND last_observed_at < ?
`, cutoff)
	if err != nil {
		return silenceRetentionResult{}, fmt.Errorf("failed cleaning silence rules: %w", err)
	}

	return result, nil
}

func enforceMaxAlertsTx(tx *sql.Tx, maxAlerts int) (int, error) {
	currentCount, err := queryInt(tx, `SELECT COUNT(*) FROM alert_states`)
	if err != nil {
		return 0, fmt.Errorf("failed to count current alerts for max limit: %w", err)
	}

	if currentCount <= maxAlerts {
		return 0, nil
	}

	toDelete := currentCount - maxAlerts
	res, err := tx.Exec(`
DELETE FROM alert_states
WHERE alert_id IN (
  SELECT alert_id
  FROM alert_states
  ORDER BY COALESCE(
      (SELECT MAX(h.changed_at) FROM alert_events h WHERE h.alert_id = alert_states.alert_id),
      updated_at
    ) ASC,
    alert_id ASC
  LIMIT ?
)
`, toDelete)
	if err != nil {
		return 0, fmt.Errorf("failed to enforce max alerts limit: %w", err)
	}

	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to read max limit delete count: %w", err)
	}

	return int(clampInt64ToInt(deleted)), nil
}

func runIncrementalVacuum(db *sql.DB) (int, error) {
	before, err := queryInt(db, `PRAGMA freelist_count`)
	if err != nil {
		return 0, fmt.Errorf("failed to read freelist_count before vacuum: %w", err)
	}

	if _, err = db.Exec(`PRAGMA incremental_vacuum`); err != nil {
		return 0, fmt.Errorf("failed to run incremental vacuum: %w", err)
	}

	after, err := queryInt(db, `PRAGMA freelist_count`)
	if err != nil {
		return 0, fmt.Errorf("failed to read freelist_count after vacuum: %w", err)
	}

	if before < after {
		return 0, nil
	}

	return before - after, nil
}

type silenceReconciliationResult struct {
	queued int
	events []runtimeEvent
}

func reconcileSilenceSourcesWithEventsTx(tx *sql.Tx, sources []SourceSilenceState, now time.Time) (silenceReconciliationResult, error) {
	if sources == nil {
		return silenceReconciliationResult{}, nil
	}
	result := silenceReconciliationResult{}
	previousConsistency, consistencyErr := readClusterConsistencyStatusesTx(tx)
	if consistencyErr != nil {
		result.events = append(result.events, runtimeEvent{level: slog.LevelError, component: "silence_reconciliation", event: "observability_collection_failed", message: "Failed to collect previous HA consistency state for logging", attrs: []slog.Attr{slog.Any("error", consistencyErr)}})
		previousConsistency = map[string]clusterConsistencySnapshot{}
	}
	normalizedSources := make([]SourceSilenceState, len(sources))
	copy(normalizedSources, sources)
	for i := range normalizedSources {
		validCount := len(knownSourceSilences(normalizedSources[i].Silences))
		malformedCount := normalizedSources[i].MalformedRules + len(normalizedSources[i].Silences) - validCount
		normalizedSources[i].Silences = knownSourceSilences(normalizedSources[i].Silences)
		result.events = append(result.events, runtimeEvent{level: slog.LevelDebug, component: "silence_reconciliation", event: "source_snapshot_received", message: "Alertmanager silence source snapshot received", attrs: []slog.Attr{slog.String("source", strings.TrimSpace(normalizedSources[i].SourceName)), slog.String("target", strings.TrimSpace(normalizedSources[i].ClusterID)), slog.String("sync_status", normalizedSourceSyncStatus(normalizedSources[i].SyncStatus)), slog.Int("valid_rule_count", validCount), slog.Int("malformed_rule_count", malformedCount)}})
	}
	sources = normalizedSources
	shouldEnqueueRecheck := false
	configured := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		sourceName := strings.TrimSpace(source.SourceName)
		clusterID := strings.TrimSpace(source.ClusterID)
		if sourceName == "" || clusterID == "" {
			continue
		}
		configured[sourceName] = struct{}{}
		syncStatus := strings.TrimSpace(source.SyncStatus)
		if syncStatus == "" {
			syncStatus = "unknown"
		}
		if syncStatus != "success" && syncStatus != "failed" && syncStatus != "invalid" && syncStatus != "retired" && syncStatus != "unknown" {
			syncStatus = "invalid"
		}
		_, previousStatus, previousCluster, sourceReadErr := readSourceFingerprintAndStatusTx(tx, sourceName)
		if sourceReadErr != nil {
			return silenceReconciliationResult{}, fmt.Errorf("failed reading previous source state for %q: %w", sourceName, sourceReadErr)
		}
		newFP := buildSourceSilenceFingerprint(source)
		promotionCapableChange, promotionErr := sourceSilenceChangeCanPromoteTx(tx, sourceName, source.Silences)
		if promotionErr != nil {
			return silenceReconciliationResult{}, promotionErr
		}
		if previousCluster != "" && previousCluster != clusterID {
			promotionCapableChange = true
		}

		if err := upsertAlertmanagerSourceTx(tx, source, newFP, now); err != nil {
			return silenceReconciliationResult{}, err
		}
		if syncStatus == "success" {
			if err := replaceSourceSilenceRelationsTx(tx, sourceName, clusterID, source.Silences, now); err != nil {
				return silenceReconciliationResult{}, err
			}
		}

		recovered := syncStatus == "success" && (previousStatus == "failed" || previousStatus == "invalid" || previousStatus == "unknown" || previousStatus == "retired")
		if syncStatus == "success" && (promotionCapableChange || recovered) {
			shouldEnqueueRecheck = true
		}
		switch {
		case recovered:
			result.events = append(result.events, runtimeEvent{level: slog.LevelInfo, component: "silence_reconciliation", event: "source_recovered", message: "Alertmanager silence source recovered", attrs: []slog.Attr{slog.String("source", sourceName), slog.String("target", clusterID), slog.String("previous_status", previousStatus)}})
		case syncStatus != "success" && previousStatus != syncStatus:
			result.events = append(result.events, runtimeEvent{level: slog.LevelWarn, component: "silence_reconciliation", event: "source_sync_failed", message: "Alertmanager silence source synchronization failed", attrs: []slog.Attr{slog.String("source", sourceName), slog.String("target", clusterID), slog.String("sync_status", syncStatus)}})
		case syncStatus != "success":
			result.events = append(result.events, runtimeEvent{level: slog.LevelDebug, component: "silence_reconciliation", event: "source_sync_failed_unchanged", message: "Alertmanager silence source remains unavailable", attrs: []slog.Attr{slog.String("source", sourceName), slog.String("target", clusterID), slog.String("sync_status", syncStatus)}})
		}
	}
	retired, retirementErr := retireDeconfiguredSourcesTx(tx, configured, now)
	if retirementErr != nil {
		return silenceReconciliationResult{}, retirementErr
	}
	if len(retired) > 0 {
		shouldEnqueueRecheck = true
		for _, source := range retired {
			result.events = append(result.events, runtimeEvent{level: slog.LevelInfo, component: "silence_reconciliation", event: "source_retired", message: "Alertmanager silence source retired", attrs: []slog.Attr{slog.String("source", source.name), slog.String("target", source.target)}})
		}
	}
	if err := closeConfirmedAbsentSilenceEpisodesTx(tx, sources, now); err != nil {
		return silenceReconciliationResult{}, err
	}
	if err := reconcileClusterConsistencyTx(tx, sources, now); err != nil {
		return silenceReconciliationResult{}, err
	}
	currentConsistency, consistencyErr := readClusterConsistencyStatusesTx(tx)
	if consistencyErr != nil {
		result.events = append(result.events, runtimeEvent{level: slog.LevelError, component: "silence_reconciliation", event: "observability_collection_failed", message: "Failed to collect current HA consistency state for logging", attrs: []slog.Attr{slog.Any("error", consistencyErr)}})
	} else {
		result.events = append(result.events, clusterConsistencyRuntimeEvents(previousConsistency, currentConsistency)...)
	}
	if shouldEnqueueRecheck {
		if _, err := enqueueSilenceRecheckJobTx(tx, now); err != nil {
			return silenceReconciliationResult{}, err
		}
		result.queued = 1
	}
	return result, nil
}

func normalizedSourceSyncStatus(status string) string {
	status = strings.TrimSpace(status)
	if status == "success" || status == "failed" || status == "invalid" || status == "retired" || status == "unknown" {
		return status
	}
	return "invalid"
}

type clusterConsistencySnapshot struct {
	status  string
	members int
}

func readClusterConsistencyStatusesTx(tx *sql.Tx) (map[string]clusterConsistencySnapshot, error) {
	rows, err := tx.Query(`SELECT cluster_id, status, member_count FROM alertmanager_cluster_consistency`)
	if err != nil {
		return nil, fmt.Errorf("failed reading cluster consistency for logging: %w", err)
	}
	defer rows.Close()
	result := map[string]clusterConsistencySnapshot{}
	for rows.Next() {
		var clusterID string
		var snapshot clusterConsistencySnapshot
		if scanErr := rows.Scan(&clusterID, &snapshot.status, &snapshot.members); scanErr != nil {
			return nil, fmt.Errorf("failed scanning cluster consistency for logging: %w", scanErr)
		}
		result[clusterID] = snapshot
	}
	return result, rows.Err()
}

func clusterConsistencyRuntimeEvents(previous, current map[string]clusterConsistencySnapshot) []runtimeEvent {
	events := make([]runtimeEvent, 0)
	for target, snapshot := range current {
		old := previous[target]
		switch {
		case snapshot.status == "inconsistent" && old.status != "inconsistent":
			events = append(events, runtimeEvent{level: slog.LevelWarn, component: "silence_reconciliation", event: "ha_disagreement_detected", message: "Alertmanager silence members disagree", attrs: []slog.Attr{slog.String("target", target), slog.Int("member_count", snapshot.members)}})
		case snapshot.status == "inconsistent":
			events = append(events, runtimeEvent{level: slog.LevelDebug, component: "silence_reconciliation", event: "ha_disagreement_unchanged", message: "Alertmanager silence members still disagree", attrs: []slog.Attr{slog.String("target", target), slog.Int("member_count", snapshot.members)}})
		case snapshot.status == "consistent" && old.status == "inconsistent":
			events = append(events, runtimeEvent{level: slog.LevelInfo, component: "silence_reconciliation", event: "ha_converged", message: "Alertmanager silence members converged", attrs: []slog.Attr{slog.String("target", target), slog.Int("member_count", snapshot.members)}})
		}
	}
	return events
}

func closeConfirmedAbsentSilenceEpisodesTx(tx *sql.Tx, sources []SourceSilenceState, now time.Time) error {
	type coverage struct {
		members       map[string]struct{}
		allSuccessful bool
	}
	byCluster := map[string]*coverage{}
	for _, source := range sources {
		clusterID := strings.TrimSpace(source.ClusterID)
		sourceName := strings.TrimSpace(source.SourceName)
		status := strings.TrimSpace(source.SyncStatus)
		if clusterID == "" || sourceName == "" || status == "retired" {
			continue
		}
		cluster := byCluster[clusterID]
		if cluster == nil {
			cluster = &coverage{members: map[string]struct{}{}, allSuccessful: true}
			byCluster[clusterID] = cluster
		}
		cluster.members[sourceName] = struct{}{}
		if status != "success" {
			cluster.allSuccessful = false
		}
	}
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	for clusterID, cluster := range byCluster {
		if len(cluster.members) == 0 || !cluster.allSuccessful {
			continue
		}
		if _, err := tx.Exec(`
UPDATE silence_rule_episodes
SET ended_at=?, end_reason='absent_confirmed', open_marker=NULL
WHERE open_marker=1
  AND rule_id IN (
    SELECT r.rule_id
    FROM silence_rules r
    WHERE r.cluster_id=?
      AND NOT EXISTS (
        SELECT 1
        FROM alertmanager_source_silences ss
        JOIN alertmanager_sources src ON src.source_name=ss.source_name
        WHERE ss.rule_id=r.rule_id
          AND src.current_cluster_id=?
          AND src.retired_at IS NULL
      )
  )
`, nowRaw, clusterID, clusterID); err != nil {
			return fmt.Errorf("failed closing confirmed-absent silence episodes for cluster %q: %w", clusterID, err)
		}
	}
	return nil
}

type retiredSilenceSource struct {
	name   string
	target string
}

func retireDeconfiguredSourcesTx(tx *sql.Tx, configured map[string]struct{}, now time.Time) ([]retiredSilenceSource, error) {
	rows, err := tx.Query(`SELECT source_name, COALESCE(current_cluster_id, '') FROM alertmanager_sources WHERE retired_at IS NULL ORDER BY source_name`)
	if err != nil {
		return nil, fmt.Errorf("failed to query configured source retirement candidates: %w", err)
	}
	existing := make([]retiredSilenceSource, 0)
	for rows.Next() {
		var source retiredSilenceSource
		if err = rows.Scan(&source.name, &source.target); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan source retirement candidate: %w", err)
		}
		existing = append(existing, source)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("failed to iterate source retirement candidates: %w", err)
	}
	rows.Close()

	retired := make([]retiredSilenceSource, 0)
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	for _, source := range existing {
		if _, ok := configured[source.name]; ok {
			continue
		}
		res, updateErr := tx.Exec(`
UPDATE alertmanager_sources
SET sync_status='retired', retired_at=?, last_configured_at=?
WHERE source_name=? AND retired_at IS NULL
`, nowRaw, nowRaw, source.name)
		if updateErr != nil {
			return nil, fmt.Errorf("failed to retire deconfigured source %q: %w", source.name, updateErr)
		}
		affected, _ := res.RowsAffected()
		if affected > 0 {
			if _, deleteErr := tx.Exec(`DELETE FROM alertmanager_source_silences WHERE source_name=?`, source.name); deleteErr != nil {
				return nil, fmt.Errorf("failed clearing mutable silence relations for retired source %q: %w", source.name, deleteErr)
			}
			retired = append(retired, source)
		}
	}
	return retired, nil
}

func reconcileClusterConsistencyTx(tx *sql.Tx, sources []SourceSilenceState, now time.Time) error {
	byCluster := map[string][]SourceSilenceState{}
	for _, source := range sources {
		clusterID := strings.TrimSpace(source.ClusterID)
		if clusterID != "" {
			byCluster[clusterID] = append(byCluster[clusterID], source)
		}
	}
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	existingRows, err := tx.Query(`SELECT cluster_id FROM alertmanager_cluster_consistency`)
	if err != nil {
		return fmt.Errorf("failed to query cluster consistency retirement candidates: %w", err)
	}
	existingClusters := make([]string, 0)
	for existingRows.Next() {
		var clusterID string
		if err = existingRows.Scan(&clusterID); err != nil {
			existingRows.Close()
			return fmt.Errorf("failed to scan cluster consistency retirement candidate: %w", err)
		}
		existingClusters = append(existingClusters, clusterID)
	}
	existingRows.Close()
	for _, clusterID := range existingClusters {
		if _, ok := byCluster[clusterID]; ok {
			continue
		}
		if _, err = tx.Exec(`DELETE FROM alertmanager_cluster_consistency WHERE cluster_id=?`, clusterID); err != nil {
			return fmt.Errorf("failed to delete deconfigured cluster consistency %q: %w", clusterID, err)
		}
	}
	for clusterID, members := range byCluster {
		status := "unknown"
		allSuccessful := true
		fingerprints := map[string]struct{}{}
		for _, member := range members {
			if strings.TrimSpace(member.SyncStatus) != "success" {
				allSuccessful = false
				break
			}
			fingerprints[buildSourceConsistencyFingerprint(member)] = struct{}{}
		}
		if allSuccessful {
			if len(fingerprints) <= 1 {
				status = "consistent"
			} else {
				status = "inconsistent"
			}
		}
		if status == "unknown" {
			if _, err := tx.Exec(`
INSERT INTO alertmanager_cluster_consistency(cluster_id, status, member_count, last_evaluated_at)
VALUES(?, 'unknown', ?, ?)
ON CONFLICT(cluster_id) DO UPDATE SET member_count=excluded.member_count, last_evaluated_at=excluded.last_evaluated_at
`, clusterID, len(members), nowRaw); err != nil {
				return fmt.Errorf("failed to preserve unknown cluster consistency for %q: %w", clusterID, err)
			}
			continue
		}
		consistentAt := any(nil)
		inconsistentAt := any(nil)
		if status == "consistent" {
			consistentAt = nowRaw
		} else {
			inconsistentAt = nowRaw
		}
		if _, err := tx.Exec(`
INSERT INTO alertmanager_cluster_consistency(
  cluster_id, status, member_count, last_evaluated_at, last_consistent_at, last_inconsistent_at
) VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(cluster_id) DO UPDATE SET
  status=excluded.status,
  member_count=excluded.member_count,
  last_evaluated_at=excluded.last_evaluated_at,
  last_consistent_at=COALESCE(excluded.last_consistent_at, alertmanager_cluster_consistency.last_consistent_at),
  last_inconsistent_at=COALESCE(excluded.last_inconsistent_at, alertmanager_cluster_consistency.last_inconsistent_at)
`, clusterID, status, len(members), nowRaw, consistentAt, inconsistentAt); err != nil {
			return fmt.Errorf("failed to update cluster consistency for %q: %w", clusterID, err)
		}
	}
	return nil
}

func buildSourceConsistencyFingerprint(source SourceSilenceState) string {
	items := make([]string, 0, len(source.Silences))
	for _, silence := range source.Silences {
		contentFP, evaluationFP, _ := buildSilenceRevisionFingerprints(silence)
		items = append(items, strings.Join([]string{
			strings.TrimSpace(silence.SilenceID), contentFP, evaluationFP, strings.TrimSpace(silence.Status),
		}, "\x00"))
	}
	slices.Sort(items)
	raw, _ := json.Marshal(items)
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum)
}

func readSourceFingerprintAndStatusTx(tx *sql.Tx, sourceName string) (string, string, string, error) {
	var fingerprint sql.NullString
	var status string
	var clusterID string
	err := tx.QueryRow(`SELECT silence_set_fingerprint, sync_status, current_cluster_id FROM alertmanager_sources WHERE source_name = ?`, sourceName).Scan(&fingerprint, &status, &clusterID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "unknown", "", nil
	}
	if err != nil {
		return "", "", "", err
	}
	return fingerprint.String, status, clusterID, nil
}

type sourceEvaluationRule struct {
	status                string
	evaluationFingerprint string
}

func sourceSilenceChangeCanPromoteTx(tx *sql.Tx, sourceName string, next []SourceSilenceSnapshot) (bool, error) {
	previous := map[string]sourceEvaluationRule{}
	rows, err := tx.Query(`
SELECT r.silence_id, ss.observed_state, rev.evaluation_fingerprint
FROM alertmanager_source_silences ss
JOIN silence_rules r ON r.rule_id=ss.rule_id
JOIN silence_revisions rev ON rev.revision_id=ss.revision_id
WHERE ss.source_name=?
`, sourceName)
	if err != nil {
		return false, fmt.Errorf("failed loading source evaluation state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var silenceID string
		var rule sourceEvaluationRule
		if err = rows.Scan(&silenceID, &rule.status, &rule.evaluationFingerprint); err != nil {
			return false, fmt.Errorf("failed scanning source evaluation state: %w", err)
		}
		previous[silenceID] = rule
	}
	if err = rows.Err(); err != nil {
		return false, fmt.Errorf("failed iterating source evaluation state: %w", err)
	}
	for _, silence := range next {
		if silence.Status != "active" {
			continue
		}
		_, evaluationFingerprint, _ := buildSilenceRevisionFingerprints(silence)
		old, found := previous[strings.TrimSpace(silence.SilenceID)]
		if !found || old.status != "active" || old.evaluationFingerprint != evaluationFingerprint {
			return true, nil
		}
	}
	return false, nil
}

func buildSourceSilenceFingerprint(source SourceSilenceState) string {
	type silenceFP struct {
		ID                    string `json:"id"`
		Status                string `json:"status"`
		StartsAt              string `json:"startsAt"`
		EndsAt                string `json:"endsAt"`
		UpdatedAt             string `json:"updatedAt"`
		EvaluationFingerprint string `json:"evaluationFingerprint"`
	}
	type sourceFP struct {
		SourceName string      `json:"sourceName"`
		ClusterID  string      `json:"clusterId"`
		Silences   []silenceFP `json:"silences"`
	}
	entries := make([]silenceFP, 0, len(source.Silences))
	for _, s := range source.Silences {
		updatedAt := ""
		if s.UpdatedAt != nil {
			updatedAt = s.UpdatedAt.UTC().Format(time.RFC3339Nano)
		}
		_, evaluationFingerprint, _ := buildSilenceRevisionFingerprints(s)
		entries = append(entries, silenceFP{
			ID:                    strings.TrimSpace(s.SilenceID),
			Status:                strings.TrimSpace(s.Status),
			StartsAt:              s.StartsAt.UTC().Format(time.RFC3339Nano),
			EndsAt:                s.EndsAt.UTC().Format(time.RFC3339Nano),
			UpdatedAt:             updatedAt,
			EvaluationFingerprint: evaluationFingerprint,
		})
	}
	slices.SortFunc(entries, func(a, b silenceFP) int {
		if a.ID == b.ID {
			return strings.Compare(a.UpdatedAt+a.Status, b.UpdatedAt+b.Status)
		}
		return strings.Compare(a.ID, b.ID)
	})
	raw, _ := json.Marshal(sourceFP{
		SourceName: strings.TrimSpace(source.SourceName),
		ClusterID:  strings.TrimSpace(source.ClusterID),
		Silences:   entries,
	})
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum)
}

func upsertAlertmanagerSourceTx(tx *sql.Tx, source SourceSilenceState, sourceFP string, now time.Time) error {
	sourceName := strings.TrimSpace(source.SourceName)
	clusterID := strings.TrimSpace(source.ClusterID)
	syncStatus := strings.TrimSpace(source.SyncStatus)
	if syncStatus == "" {
		syncStatus = "unknown"
	}
	attemptAt := now.UTC().Format(time.RFC3339Nano)
	successAt := any(nil)
	fingerprint := any(nil)
	if syncStatus == "success" {
		successAt = attemptAt
		fingerprint = sourceFP
	}
	_, err := tx.Exec(`
INSERT INTO alertmanager_sources(
  source_name, current_cluster_id, sync_status, silence_set_fingerprint,
  first_configured_at, last_configured_at, last_attempt_at, last_success_at,
  last_confirmed_at, last_error, retired_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source_name) DO UPDATE SET
  current_cluster_id=excluded.current_cluster_id,
  sync_status=excluded.sync_status,
  silence_set_fingerprint=COALESCE(excluded.silence_set_fingerprint, alertmanager_sources.silence_set_fingerprint),
  last_configured_at=excluded.last_configured_at,
  last_attempt_at=excluded.last_attempt_at,
  last_success_at=COALESCE(excluded.last_success_at, alertmanager_sources.last_success_at),
  last_confirmed_at=COALESCE(excluded.last_success_at, alertmanager_sources.last_confirmed_at),
  last_error=excluded.last_error,
  retired_at=CASE WHEN excluded.sync_status='retired' THEN excluded.last_attempt_at ELSE NULL END
`, sourceName, clusterID, syncStatus, fingerprint, attemptAt, attemptAt, attemptAt, successAt, successAt, sanitizeNullableString(ptrString(source.LastError)), nil)
	if err != nil {
		return fmt.Errorf("failed to upsert alertmanager source %q: %w", sourceName, err)
	}
	return nil
}

func replaceSourceSilenceRelationsTx(tx *sql.Tx, sourceName, clusterID string, silences []SourceSilenceSnapshot, now time.Time) error {
	if _, err := tx.Exec(`DELETE FROM alertmanager_source_silences WHERE source_name = ?`, sourceName); err != nil {
		return fmt.Errorf("failed clearing source silence relations for %q: %w", sourceName, err)
	}
	for _, silence := range silences {
		ruleID, revisionID, err := upsertSilenceRuleAndRevisionTx(tx, clusterID, silence, now)
		if err != nil {
			return err
		}
		nowRaw := now.UTC().Format(time.RFC3339Nano)
		_, err = tx.Exec(`
INSERT INTO alertmanager_source_silences(source_name, rule_id, revision_id, observed_state, first_observed_at, last_confirmed_at)
VALUES(?, ?, ?, ?, ?, ?)
		`, sourceName, ruleID, revisionID, silence.Status, nowRaw, nowRaw)
		if err != nil {
			return fmt.Errorf("failed inserting source silence relation: %w", err)
		}
	}
	return nil
}

// knownSourceSilences enforces the mapper's status contract at the storage
// boundary. The complete Alertmanager response validation belongs to the mapper.
func knownSourceSilences(silences []SourceSilenceSnapshot) []SourceSilenceSnapshot {
	known := make([]SourceSilenceSnapshot, 0, len(silences))
	for _, silence := range silences {
		if models.IsKnownSilenceStatus(silence.Status) {
			known = append(known, silence)
		}
	}
	return known
}

func upsertSilenceRuleAndRevisionTx(tx *sql.Tx, clusterID string, silence SourceSilenceSnapshot, now time.Time) (int64, int64, error) {
	silenceID := strings.TrimSpace(silence.SilenceID)
	if silenceID == "" {
		return 0, 0, errors.New("silence ID cannot be empty")
	}
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	_, err := tx.Exec(`
INSERT INTO silence_rules(cluster_id, silence_id, first_observed_at, last_observed_at)
VALUES(?, ?, ?, ?)
ON CONFLICT(cluster_id, silence_id) DO UPDATE SET last_observed_at=excluded.last_observed_at
`, clusterID, silenceID, nowRaw, nowRaw)
	if err != nil {
		return 0, 0, fmt.Errorf("failed upserting silence rule: %w", err)
	}
	var ruleID int64
	if err = tx.QueryRow(`SELECT rule_id FROM silence_rules WHERE cluster_id = ? AND silence_id = ?`, clusterID, silenceID).Scan(&ruleID); err != nil {
		return 0, 0, fmt.Errorf("failed loading silence rule id: %w", err)
	}
	var episodeID int64
	err = tx.QueryRow(`SELECT episode_id FROM silence_rule_episodes WHERE rule_id = ? AND open_marker = 1`, ruleID).Scan(&episodeID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		nextEpisode := 1
		_ = tx.QueryRow(`SELECT COALESCE(MAX(episode_number),0)+1 FROM silence_rule_episodes WHERE rule_id = ?`, ruleID).Scan(&nextEpisode)
		res, ierr := tx.Exec(`
INSERT INTO silence_rule_episodes(rule_id, episode_number, first_observed_at, last_confirmed_at, ended_at, end_reason, open_marker)
VALUES(?, ?, ?, ?, NULL, NULL, 1)
`, ruleID, nextEpisode, nowRaw, nowRaw)
		if ierr != nil {
			return 0, 0, fmt.Errorf("failed creating silence episode: %w", ierr)
		}
		episodeID, _ = res.LastInsertId()
	case err != nil:
		return 0, 0, fmt.Errorf("failed loading silence episode: %w", err)
	default:
		if _, err = tx.Exec(`UPDATE silence_rule_episodes SET last_confirmed_at = ? WHERE episode_id = ?`, nowRaw, episodeID); err != nil {
			return 0, 0, fmt.Errorf("failed updating silence episode: %w", err)
		}
	}

	contentFP, evalFP, matchersJSON := buildSilenceRevisionFingerprints(silence)
	var (
		revisionID            int64
		latestContentFP       string
		latestSourceUpdatedAt sql.NullString
	)
	updatedAtRaw := any(nil)
	updatedAtNormalized := sql.NullString{}
	if silence.UpdatedAt != nil {
		value := silence.UpdatedAt.UTC().Format(time.RFC3339Nano)
		updatedAtRaw = value
		updatedAtNormalized = sql.NullString{String: value, Valid: true}
	}
	err = tx.QueryRow(`
SELECT revision_id, content_fingerprint, source_updated_at
FROM silence_revisions
WHERE episode_id = ?
ORDER BY revision_number DESC
LIMIT 1
	`, episodeID).Scan(&revisionID, &latestContentFP, &latestSourceUpdatedAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, 0, fmt.Errorf("failed loading latest silence revision: %w", err)
	}
	latestMatches := err == nil && latestContentFP == contentFP && latestSourceUpdatedAt == updatedAtNormalized
	if errors.Is(err, sql.ErrNoRows) || !latestMatches {
		nextRevision := 1
		_ = tx.QueryRow(`SELECT COALESCE(MAX(revision_number),0)+1 FROM silence_revisions WHERE episode_id = ?`, episodeID).Scan(&nextRevision)
		annotationsJSON := strings.TrimSpace(silence.AnnotationsJSON)
		if annotationsJSON == "" {
			annotationsJSON = "{}"
		}
		res, ierr := tx.Exec(`
INSERT INTO silence_revisions(
  episode_id, rule_id, revision_number, content_fingerprint, evaluation_fingerprint,
  matchers_json, starts_at, ends_at, source_updated_at, created_by, comment,
  annotations_json, ticket_id, ticket_url, first_observed_at, last_observed_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, episodeID, ruleID, nextRevision, contentFP, evalFP, matchersJSON,
			silence.StartsAt.UTC().Format(time.RFC3339Nano), silence.EndsAt.UTC().Format(time.RFC3339Nano),
			updatedAtRaw, silence.CreatedBy, silence.Comment, annotationsJSON, silence.TicketID, silence.TicketURL,
			nowRaw, nowRaw)
		if ierr != nil {
			return 0, 0, fmt.Errorf("failed creating silence revision: %w", ierr)
		}
		revisionID, _ = res.LastInsertId()
	} else {
		if _, err = tx.Exec(`UPDATE silence_revisions SET last_observed_at = ? WHERE revision_id = ?`, nowRaw, revisionID); err != nil {
			return 0, 0, fmt.Errorf("failed updating silence revision observation time: %w", err)
		}
	}

	return ruleID, revisionID, nil
}

func buildSilenceRevisionFingerprints(silence SourceSilenceSnapshot) (string, string, string) {
	matchers := append([]SilenceMatcherSnapshot(nil), silence.Matchers...)
	slices.SortFunc(matchers, func(a, b SilenceMatcherSnapshot) int {
		if a.Name == b.Name {
			if a.Value == b.Value {
				if a.IsRegex == b.IsRegex {
					if a.IsEqual == b.IsEqual {
						return 0
					}
					if !a.IsEqual {
						return -1
					}
					return 1
				}
				if !a.IsRegex {
					return -1
				}
				return 1
			}
			return strings.Compare(a.Value, b.Value)
		}
		return strings.Compare(a.Name, b.Name)
	})
	matchersJSONRaw, _ := json.Marshal(matchers)
	matchersJSON := string(matchersJSONRaw)
	type fp struct {
		Matchers    []SilenceMatcherSnapshot `json:"matchers"`
		StartsAt    string                   `json:"startsAt"`
		EndsAt      string                   `json:"endsAt"`
		CreatedBy   string                   `json:"createdBy"`
		Comment     string                   `json:"comment"`
		Annotations string                   `json:"annotations"`
	}
	contentRaw, _ := json.Marshal(fp{
		Matchers:    matchers,
		StartsAt:    silence.StartsAt.UTC().Format(time.RFC3339Nano),
		EndsAt:      silence.EndsAt.UTC().Format(time.RFC3339Nano),
		CreatedBy:   silence.CreatedBy,
		Comment:     silence.Comment,
		Annotations: silence.AnnotationsJSON,
	})
	evalRaw, _ := json.Marshal(struct {
		Matchers []SilenceMatcherSnapshot `json:"matchers"`
		StartsAt string                   `json:"startsAt"`
		EndsAt   string                   `json:"endsAt"`
	}{
		Matchers: matchers,
		StartsAt: silence.StartsAt.UTC().Format(time.RFC3339Nano),
		EndsAt:   silence.EndsAt.UTC().Format(time.RFC3339Nano),
	})
	contentSum := sha256.Sum256(contentRaw)
	evalSum := sha256.Sum256(evalRaw)
	return fmt.Sprintf("sha256:%x", contentSum), fmt.Sprintf("sha256:%x", evalSum), matchersJSON
}

func enqueueSilenceRecheckJobTx(tx *sql.Tx, now time.Time) (int64, error) {
	type sourceContext struct {
		SourceName  string `json:"sourceName"`
		ClusterID   string `json:"clusterId"`
		SyncStatus  string `json:"syncStatus"`
		Fingerprint string `json:"fingerprint"`
	}
	rows, err := tx.Query(`
SELECT source_name, COALESCE(current_cluster_id, ''), sync_status, COALESCE(silence_set_fingerprint, '')
FROM alertmanager_sources
ORDER BY source_name ASC
`)
	if err != nil {
		return 0, fmt.Errorf("failed to query source context for recheck job: %w", err)
	}
	contexts := make([]sourceContext, 0)
	for rows.Next() {
		var c sourceContext
		if scanErr := rows.Scan(&c.SourceName, &c.ClusterID, &c.SyncStatus, &c.Fingerprint); scanErr != nil {
			rows.Close()
			return 0, fmt.Errorf("failed to scan source context for recheck job: %w", scanErr)
		}
		contexts = append(contexts, c)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("failed to iterate source context for recheck job: %w", err)
	}
	rows.Close()
	ctxRaw, _ := json.Marshal(struct {
		FormatVersion int             `json:"formatVersion"`
		Sources       []sourceContext `json:"sources"`
	}{FormatVersion: 1, Sources: contexts})
	ctxFingerprintRaw := sha256.Sum256(ctxRaw)
	ctxFingerprint := fmt.Sprintf("sha256:%x", ctxFingerprintRaw)
	nowRaw := now.UTC().Format(time.RFC3339Nano)
	res, err := tx.Exec(`
INSERT INTO silence_recheck_jobs(
  context_fingerprint, open_context_fingerprint, source_context_json,
  trigger_type, status, cursor_alert_id, created_at
) VALUES(?, ?, ?, ?, 'queued', NULL, ?)
ON CONFLICT(open_context_fingerprint) DO NOTHING
`, ctxFingerprint, ctxFingerprint, string(ctxRaw), "source_changed", nowRaw)
	if err != nil {
		return 0, fmt.Errorf("failed to enqueue silence recheck job: %w", err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed checking recheck job insertion: %w", err)
	}
	var jobID int64
	if rowsAffected == 1 {
		jobID, err = res.LastInsertId()
		if err != nil {
			return 0, fmt.Errorf("failed reading inserted recheck job id: %w", err)
		}
	} else {
		if err = tx.QueryRow(`SELECT job_id FROM silence_recheck_jobs WHERE open_context_fingerprint = ?`, ctxFingerprint).Scan(&jobID); err != nil {
			return 0, fmt.Errorf("failed to load existing recheck job id: %w", err)
		}
	}
	if _, err = tx.Exec(`
INSERT OR IGNORE INTO silence_recheck_job_rules(job_id, source_name, cluster_id, rule_id, revision_id, observed_state)
SELECT ?, current.source_name, source.current_cluster_id, current.rule_id, current.revision_id, current.observed_state
FROM alertmanager_source_silences current
JOIN alertmanager_sources source ON source.source_name = current.source_name
WHERE source.retired_at IS NULL
  AND source.sync_status = 'success'
	`, jobID); err != nil {
		return 0, fmt.Errorf("failed to snapshot recheck job rules: %w", err)
	}
	if rowsAffected == 1 {
		if _, err = tx.Exec(`
INSERT OR IGNORE INTO silence_recheck_job_candidates(job_id, alert_id)
SELECT ?, alert_id FROM alert_states
WHERE match_labels_json IS NOT NULL AND trim(match_labels_json) != ''
  AND (
    (state='inactive' AND is_live=0 AND is_resolved=0 AND is_suppressed=0)
    OR EXISTS (
      SELECT 1 FROM alert_target_states target
      WHERE target.alert_id=alert_states.alert_id
        AND target.retired_at IS NULL AND target.state='inactive'
        AND target.is_live=0 AND target.is_resolved=0 AND target.is_suppressed=0
    )
  )
`, jobID); err != nil {
			return 0, fmt.Errorf("failed to snapshot recheck job candidates: %w", err)
		}
		if _, err = tx.Exec(`
UPDATE silence_recheck_jobs
SET total_candidates=(SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=?)
WHERE job_id=?
`, jobID, jobID); err != nil {
			return 0, fmt.Errorf("failed counting recheck job candidates: %w", err)
		}
	}
	return jobID, nil
}

func enqueueAlertForCurrentRecheckContextTx(tx *sql.Tx, alertID string, now time.Time) error {
	res, err := tx.Exec(`
INSERT OR IGNORE INTO silence_recheck_job_candidates(job_id, alert_id)
SELECT job_id, ? FROM silence_recheck_jobs WHERE status IN ('queued','running')
`, alertID)
	if err != nil {
		return fmt.Errorf("failed attaching inactive alert to open recheck jobs: %w", err)
	}
	attached, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed checking inactive alert attachment: %w", err)
	}
	if attached == 0 {
		jobID, enqueueErr := enqueueSilenceRecheckJobTx(tx, now)
		if enqueueErr != nil {
			return enqueueErr
		}
		if jobID == 0 {
			return nil
		}
		if _, err = tx.Exec(`INSERT OR IGNORE INTO silence_recheck_job_candidates(job_id, alert_id) VALUES(?, ?)`, jobID, alertID); err != nil {
			return fmt.Errorf("failed enqueueing inactive alert for recheck: %w", err)
		}
	}
	if _, err = tx.Exec(`
UPDATE silence_recheck_jobs SET total_candidates=(
  SELECT COUNT(*) FROM silence_recheck_job_candidates WHERE job_id=silence_recheck_jobs.job_id
) WHERE status IN ('queued','running')
`); err != nil {
		return fmt.Errorf("failed updating recheck candidate total: %w", err)
	}
	return nil
}

func (s *Store) setMetadata(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO metadata(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("failed to upsert metadata key %q: %w", key, err)
	}
	return nil
}

func setMetadataTx(tx *sql.Tx, key, value string) error {
	_, err := tx.Exec(`INSERT INTO metadata(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("failed to upsert metadata key %q: %w", key, err)
	}
	return nil
}

func (s *Store) getOptionalMetadataTime(key string) (*time.Time, error) {
	var raw sql.NullString
	err := s.db.QueryRow(`SELECT value FROM metadata WHERE key = ?`, key).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read metadata key %q: %w", key, err)
	}
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return nil, nil
	}
	ts, err := time.Parse(time.RFC3339Nano, raw.String)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata key %q: %w", key, err)
	}
	return &ts, nil
}

func queryInt(q interface {
	QueryRow(query string, args ...any) *sql.Row
}, query string, args ...any,
) (int, error) {
	var out int
	if err := q.QueryRow(query, args...).Scan(&out); err != nil {
		return 0, err
	}
	return out, nil
}

func readMetadataStringTx(tx *sql.Tx, key string) (string, error) {
	var value sql.NullString
	err := tx.QueryRow(`SELECT value FROM metadata WHERE key = ?`, key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to read metadata key %q: %w", key, err)
	}
	if !value.Valid {
		return "", nil
	}
	return value.String, nil
}

func readMetadataIntTx(tx *sql.Tx, key string) (int, error) {
	raw, err := readMetadataStringTx(tx, key)
	if err != nil {
		return 0, err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("failed to parse metadata int key %q value %q: %w", key, raw, err)
	}
	if v < 0 {
		return 0, nil
	}
	return v, nil
}

func readMetadataInt64Tx(tx *sql.Tx, key string) (int64, error) {
	raw, err := readMetadataStringTx(tx, key)
	if err != nil {
		return 0, err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse metadata int64 key %q value %q: %w", key, raw, err)
	}
	if v < 0 {
		return 0, nil
	}
	return v, nil
}

func setMetadataIntTx(tx *sql.Tx, key string, value int) error {
	if value < 0 {
		value = 0
	}
	return setMetadataTx(tx, key, strconv.Itoa(value))
}

func setMetadataInt64Tx(tx *sql.Tx, key string, value int64) error {
	if value < 0 {
		value = 0
	}
	return setMetadataTx(tx, key, strconv.FormatInt(value, 10))
}

func setMetadataTimeTx(tx *sql.Tx, key string, ts *time.Time) error {
	if ts == nil {
		return setMetadataTx(tx, key, "")
	}
	v := ts.UTC().Format(time.RFC3339Nano)
	return setMetadataTx(tx, key, v)
}

func (s *Store) getMetadataString(key string) (string, error) {
	var value sql.NullString
	err := s.db.QueryRow(`SELECT value FROM metadata WHERE key = ?`, key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to read metadata key %q: %w", key, err)
	}
	if !value.Valid {
		return "", nil
	}
	return value.String, nil
}

func (s *Store) getMetadataInt(key string) (int, error) {
	raw, err := s.getMetadataString(key)
	if err != nil {
		return 0, err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("failed to parse metadata int key %q value %q: %w", key, raw, err)
	}
	if v < 0 {
		return 0, nil
	}
	return v, nil
}

func clampInt64ToInt(v int64) int64 {
	if v <= 0 {
		return 0
	}
	if v > int64(math.MaxInt) {
		return int64(math.MaxInt)
	}
	return v
}
