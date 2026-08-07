package alertlifecycle

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prymitive/karma/internal/models"
	"github.com/prymitive/karma/internal/regex"
	"github.com/prymitive/karma/internal/transform"

	_ "modernc.org/sqlite"
)

const schemaVersion = "5"

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
)

const reasonCodeMaxLength = 64

const phase2SilenceRetentionTTL = 30 * 24 * time.Hour

const (
	regroupStatusReady   = "ready"
	regroupStatusRunning = "running"
	regroupStatusError   = "error"
)

type Store struct {
	db   *sql.DB
	path string
}

type StoreOptions struct {
	Path string
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

	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=auto_vacuum(INCREMENTAL)", opts.Path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to connect to sqlite database: %w", err)
	}

	store := &Store{db: db, path: opts.Path}
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
  raw_rule_payload_json TEXT,
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

	if _, err = tx.Exec(schemaSQL); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to apply schema: %w", err)
	}

	if err = abortOnLegacyPreviewTables(tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN group_labels_json TEXT NOT NULL DEFAULT '{}'`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (group_labels_json): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN raw_state TEXT NOT NULL DEFAULT 'active'`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (raw_state): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN is_live INTEGER NOT NULL DEFAULT 1`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (is_live): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN is_resolved INTEGER NOT NULL DEFAULT 0`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (is_resolved): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN is_suppressed INTEGER NOT NULL DEFAULT 0`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (is_suppressed): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN match_labels_json TEXT`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (match_labels_json): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN relevant_sources_json TEXT NOT NULL DEFAULT '[]'`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (relevant_sources_json): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN suppression_observed_at TEXT`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (suppression_observed_at): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN resolved_at TEXT`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (resolved_at): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN resolved_by TEXT`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (resolved_by): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_states ADD COLUMN resolved_comment TEXT`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (resolved_comment): %w", err)
		}
	}

	if _, err = tx.Exec(`ALTER TABLE alert_events ADD COLUMN old_is_suppressed INTEGER`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (old_is_suppressed): %w", err)
		}
	}
	if _, err = tx.Exec(`ALTER TABLE alert_events ADD COLUMN new_is_suppressed INTEGER NOT NULL DEFAULT 0`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (new_is_suppressed): %w", err)
		}
	}

	if _, err = tx.Exec(`UPDATE alert_states SET raw_state = state WHERE trim(raw_state) = '' OR raw_state IS NULL`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to backfill raw_state: %w", err)
	}
	if _, err = tx.Exec(`
UPDATE alert_states
SET raw_state = CASE
  WHEN raw_state IN ('active', 'suppressed', 'unprocessed') THEN raw_state
  WHEN state = 'suppressed' THEN 'suppressed'
  WHEN state = 'unprocessed' THEN 'unprocessed'
  ELSE 'active'
END
`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to normalize raw_state values: %w", err)
	}
	if _, err = tx.Exec(`UPDATE alert_states SET is_live = CASE WHEN state IN ('inactive', 'resolved') THEN 0 ELSE 1 END WHERE is_live NOT IN (0,1)`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to normalize is_live: %w", err)
	}
	if _, err = tx.Exec(`UPDATE alert_states SET is_resolved = CASE WHEN state = 'resolved' THEN 1 ELSE 0 END WHERE is_resolved NOT IN (0,1)`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to normalize is_resolved: %w", err)
	}
	if _, err = tx.Exec(`
UPDATE alert_states
SET is_suppressed = CASE
  WHEN is_live = 1 AND raw_state = 'suppressed' THEN 1
  WHEN is_live = 0 AND (state = 'suppressed' OR raw_state = 'suppressed') THEN 1
  ELSE 0
END
WHERE is_suppressed NOT IN (0,1)
   OR (is_live = 1 AND raw_state = 'suppressed' AND is_suppressed != 1)
   OR (is_live = 0 AND (state = 'suppressed' OR raw_state = 'suppressed') AND is_suppressed != 1)
`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to normalize is_suppressed: %w", err)
	}
	if _, err = tx.Exec(`UPDATE alert_events SET new_is_suppressed = CASE WHEN new_state = 'suppressed' THEN 1 ELSE 0 END WHERE new_is_suppressed NOT IN (0,1)`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to backfill new_is_suppressed: %w", err)
	}
	if _, err = tx.Exec(`UPDATE alert_events SET old_is_suppressed = CASE WHEN old_state = 'suppressed' THEN 1 ELSE 0 END WHERE old_state IS NOT NULL AND old_is_suppressed IS NULL`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to backfill old_is_suppressed: %w", err)
	}
	if _, err = tx.Exec(`
UPDATE alert_states
SET state = CASE
  WHEN is_live = 1 THEN
    CASE
      WHEN raw_state = 'active' THEN 'active'
      WHEN raw_state = 'suppressed' THEN 'suppressed'
      ELSE 'unprocessed'
    END
  WHEN is_resolved = 1 THEN 'resolved'
  WHEN is_suppressed = 1 THEN 'suppressed'
  ELSE 'inactive'
END
`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to normalize projected state: %w", err)
	}

	if _, err = tx.Exec(`INSERT INTO metadata(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, metaSchemaVersion, schemaVersion); err != nil {
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

func (s *Store) UpsertCurrentAlert(alert AlertCurrent) error {
	if s == nil || s.db == nil {
		return ErrStoreNotInitialized
	}
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

	return nil
}

func (s *Store) InsertStateTransition(transition AlertStateTransition) error {
	if s == nil || s.db == nil {
		return ErrStoreNotInitialized
	}

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

	return alerts, nil
}

func (s *Store) RunHousekeeping(retentionTTL time.Duration, maxAlerts int) (HousekeepingResult, error) {
	if s == nil || s.db == nil {
		return HousekeepingResult{}, ErrStoreNotInitialized
	}
	if retentionTTL <= 0 {
		return HousekeepingResult{}, errors.New("retentionTTL must be > 0")
	}
	if maxAlerts < 1 {
		return HousekeepingResult{}, errors.New("maxAlerts must be >= 1")
	}

	runAt := time.Now().UTC()
	cutoff := runAt.Add(-retentionTTL)

	ctx := context.Background()
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

	if err = runPhase2RetentionTx(tx, runAt); err != nil {
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
		CurrentRows:                currentRows,
		HistoryRows:                historyRows,
		StateCounts:                stateCounts,
		SuppressedBreakdownCurrent: suppressedCurrent,
		SuppressedBreakdownPast:    suppressedPast,
		GroupingFingerprint:        strings.TrimSpace(groupingFingerprint),
		RegroupTargetFingerprint:   strings.TrimSpace(regroupTargetFingerprint),
		RegroupStatus:              strings.TrimSpace(regroupStatus),
		RegroupError:               strings.TrimSpace(regroupError),
		RegroupProcessed:           regroupProcessed,
		RegroupTotal:               regroupTotal,
		RegroupStartedAt:           regroupStartedAt,
		RegroupFinishedAt:          regroupFinishedAt,
		FileSizeBytes:              fileSize,
		PageCount:                  pageCount,
		PageSizeBytes:              pageSize,
		FreelistCount:              freelistCount,
		LastHousekeepingAt:         lastHousekeepingAt,
		LastIncrementalVacuumAt:    lastVacuumAt,
	}, nil
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

func (s *Store) ResolveAlert(alertID string, changedBy *string, reason *string, changedAt time.Time) (*ResolveResult, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreNotInitialized
	}
	if strings.TrimSpace(alertID) == "" {
		return nil, ErrInvalidAlertID
	}

	ts := changedAt.UTC()
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	ctx := context.Background()
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

	if !current.IsLive && current.IsResolved {
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

	if current.IsLive {
		_ = tx.Rollback()
		return nil, ErrResolveStateConflict
	}

	cleanChangedBy := sanitizeNullableString(changedBy)
	cleanComment := sanitizeNullableString(reason)
	if err = updateManualResolutionTx(tx, alertID, cleanChangedBy, cleanComment, ts); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	newRaw := current.RawState
	newState := projectedState(newRaw, false, current.IsSuppressed, true)
	oldLive := current.IsLive
	newLive := false
	oldResolved := current.IsResolved
	newResolved := true
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

	now := input.Timestamp.UTC()
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

	currentByID, err := readAllCurrentAlertsTx(tx)
	if err != nil {
		_ = tx.Rollback()
		return PollCycleResult{}, err
	}

	for _, inputLive := range input.LiveAlerts {
		live := liveByID[inputLive.AlertID]
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
			if live.State == StateSuppressed {
				if err = insertLiveSuppressionCauseTx(tx, live.AlertID, live.SilencedBySources, now); err != nil {
					_ = tx.Rollback()
					return PollCycleResult{}, err
				}
			}
			continue
		}

		newRawState := live.State
		newIsLive := true
		newIsResolved := false
		newIsSuppressed := live.State == StateSuppressed
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
	}

	for alertID, current := range currentByID {
		if _, exists := liveByID[alertID]; exists {
			continue
		}

		if !current.IsLive {
			continue
		}

		relevant, parseErr := parseRelevantUpstreams(current.RelevantUpstreams)
		if parseErr != nil {
			result.SkippedInactive++
			continue
		}

		if !allRelevantUpstreamsSuccessful(relevant, input.SuccessfulUpstreams) {
			result.SkippedInactive++
			continue
		}

		newRawState := current.RawState
		newIsLive := false
		newIsSuppressed := current.IsSuppressed
		newIsResolved := current.IsResolved
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
		}
		result.Transitions++
	}

	queued, qerr := reconcileSilenceSourcesTx(tx, input.SilenceSources, now)
	if qerr != nil {
		_ = tx.Rollback()
		return PollCycleResult{}, qerr
	}
	result.RecheckJobsQueued = queued

	if err = tx.Commit(); err != nil {
		return PollCycleResult{}, fmt.Errorf("failed to commit poll cycle transaction: %w", err)
	}

	return result, nil
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
	started, total, err := s.beginOrResumeRegroup(targetFingerprint)
	if err != nil {
		return RegroupResult{}, err
	}
	if !started {
		return RegroupResult{Total: total}, nil
	}

	compiled, err := compileGroupingConfig(cfg)
	if err != nil {
		_ = s.failRegroup(targetFingerprint, fmt.Sprintf("invalid grouping config: %v", err))
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

		done, processedInBatch, updatedInBatch, totalRows, batchErr := s.runRegroupBatch(targetFingerprint, compiled, batchSize)
		if batchErr != nil {
			_ = s.failRegroup(targetFingerprint, batchErr.Error())
			return result, batchErr
		}
		result.Processed += processedInBatch
		result.Updated += updatedInBatch
		result.Total = totalRows

		if done {
			return result, nil
		}
	}
}

func (s *Store) beginOrResumeRegroup(targetFingerprint string) (bool, int, error) {
	now := time.Now().UTC()

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return false, 0, fmt.Errorf("failed to begin regroup transaction: %w", err)
	}

	currentFingerprint, err := readMetadataStringTx(tx, metaGroupingFingerprint)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, err
	}
	status, err := readMetadataStringTx(tx, metaRegroupStatus)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, err
	}
	status = strings.TrimSpace(status)
	if status == "" {
		status = regroupStatusReady
	}
	activeTarget, err := readMetadataStringTx(tx, metaRegroupTargetFingerprint)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, err
	}

	totalRows, err := queryInt(tx, `SELECT COUNT(*) FROM alert_states`)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, fmt.Errorf("failed to count rows for regroup: %w", err)
	}

	if strings.TrimSpace(currentFingerprint) == targetFingerprint && status == regroupStatusReady {
		if err = tx.Commit(); err != nil {
			return false, 0, fmt.Errorf("failed to commit regroup readiness check: %w", err)
		}
		return false, totalRows, nil
	}

	resume := status == regroupStatusRunning && strings.TrimSpace(activeTarget) == targetFingerprint
	if !resume {
		if err = setMetadataIntTx(tx, metaRegroupProcessed, 0); err != nil {
			_ = tx.Rollback()
			return false, 0, err
		}
		if err = setMetadataInt64Tx(tx, metaRegroupCursorRowID, 0); err != nil {
			_ = tx.Rollback()
			return false, 0, err
		}
		if err = setMetadataTimeTx(tx, metaRegroupStartedAt, &now); err != nil {
			_ = tx.Rollback()
			return false, 0, err
		}
	}

	if err = setMetadataTx(tx, metaRegroupStatus, regroupStatusRunning); err != nil {
		_ = tx.Rollback()
		return false, 0, err
	}
	if err = setMetadataTx(tx, metaRegroupError, ""); err != nil {
		_ = tx.Rollback()
		return false, 0, err
	}
	if err = setMetadataTx(tx, metaRegroupTargetFingerprint, targetFingerprint); err != nil {
		_ = tx.Rollback()
		return false, 0, err
	}
	if err = setMetadataIntTx(tx, metaRegroupTotal, totalRows); err != nil {
		_ = tx.Rollback()
		return false, 0, err
	}
	if err = setMetadataTimeTx(tx, metaRegroupFinishedAt, nil); err != nil {
		_ = tx.Rollback()
		return false, 0, err
	}

	if err = tx.Commit(); err != nil {
		return false, 0, fmt.Errorf("failed to commit regroup metadata transaction: %w", err)
	}

	return true, totalRows, nil
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

func (s *Store) runRegroupBatch(targetFingerprint string, cfg compiledGroupingConfig, batchSize int) (bool, int, int, int, error) {
	tx, err := s.db.BeginTx(context.Background(), nil)
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

func (s *Store) failRegroup(targetFingerprint, reason string) error {
	tx, err := s.db.BeginTx(context.Background(), nil)
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
		if transition.NewState == StateInactive || transition.NewState == StateResolved {
			transition.NewRawState = StateActive
		} else if transition.NewState.IsValid() {
			transition.NewRawState = transition.NewState
		} else {
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
				if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_') {
					return fmt.Errorf("reason_code must be snake_case")
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

func (s State) IsValid() bool {
	switch s {
	case StateActive, StateSuppressed, StateUnprocessed, StateInactive, StateResolved:
		return true
	default:
		return false
	}
}

func projectedState(rawState State, isLive bool, isSuppressed bool, isResolved bool) State {
	if isLive {
		switch rawState {
		case StateActive:
			return StateActive
		case StateSuppressed:
			return StateSuppressed
		default:
			return StateUnprocessed
		}
	}

	if isResolved {
		return StateResolved
	}

	if isSuppressed {
		return StateSuppressed
	}

	return StateInactive
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

	return nil
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
		current, err := scanCurrentAlert(rows)
		if err != nil {
			return nil, err
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
	rawState := live.State
	isLive := true
	isSuppressed := live.State == StateSuppressed
	isResolved := false
	state := projectedState(rawState, isLive, isSuppressed, isResolved)

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
		1,
		boolToInt(isSuppressed),
		0,
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

func updateCurrentAlertFromLiveTx(tx *sql.Tx, alertID string, live LiveAlertSnapshot, newRawState State, newIsLive bool, newIsSuppressed bool, newIsResolved bool, relevantJSON string, relevantSourcesJSON string, now time.Time) error {
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

func updateProjectedStateTx(tx *sql.Tx, alertID string, rawState State, isLive bool, isSuppressed bool, isResolved bool, now time.Time) error {
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

func updateManualResolutionTx(tx *sql.Tx, alertID string, changedBy *string, comment *string, now time.Time) error {
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

func (s *Store) RunSilenceRecheckBatch(batchSize int) (int, error) {
	if s == nil || s.db == nil {
		return 0, ErrStoreNotInitialized
	}
	if batchSize <= 0 {
		batchSize = 1000
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
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
		return 0, nil
	}

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

	candidates, err := loadRecheckCandidatesTx(tx, cursor, batchSize)
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
		return 0, nil
	}

	processed := 0
	promoted := 0
	stale := 0
	missing := 0
	ineligible := 0
	lastCursor := cursor

	for _, cand := range candidates {
		processed++
		lastCursor = &cand.AlertID
		if strings.TrimSpace(cand.MatchLabelsJSON) == "" {
			missing++
			continue
		}
		labels := map[string]string{}
		if err = json.Unmarshal([]byte(cand.MatchLabelsJSON), &labels); err != nil {
			missing++
			continue
		}
		relevant, rerr := parseRelevantSourceNames(cand.RelevantSourcesJSON, cand.RelevantUpstreamsJSON)
		if rerr != nil || len(relevant) == 0 {
			ineligible++
			continue
		}
		matched := evaluateRecheckRules(rules, relevant, labels)
		if len(matched) == 0 {
			ineligible++
			continue
		}

		updated, transitionID, uerr := promoteAlertSuppressedCAS(tx, cand, now)
		if uerr != nil {
			_ = tx.Rollback()
			return 0, uerr
		}
		if !updated {
			stale++
			continue
		}
		promoted++
		if err = insertSuppressionCauseHistoryTx(tx, cand.AlertID, transitionID, jobID, now, matched); err != nil {
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
    stale_conflicts = stale_conflicts + ?
WHERE job_id = ?
`, lastCursor, processed, promoted, missing, ineligible, stale, jobID); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("failed to update recheck job progress: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit recheck batch: %w", err)
	}

	return promoted, nil
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
WHERE status IN ('queued','running')
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
SELECT jr.source_name, jr.rule_id, jr.revision_id, jr.observed_state, sr.matchers_json
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
		if err = rows.Scan(&r.SourceName, &r.RuleID, &r.RevisionID, &r.ObservedState, &r.MatchersJSON); err != nil {
			return nil, fmt.Errorf("failed to scan recheck job rule: %w", err)
		}
		out = append(out, r)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating recheck job rules: %w", err)
	}
	return out, nil
}

func loadRecheckCandidatesTx(tx *sql.Tx, cursor *string, batchSize int) ([]recheckCandidate, error) {
	rows, err := tx.Query(`
SELECT alert_id, raw_state, last_seen_at, match_labels_json, relevant_upstreams_json, relevant_sources_json
FROM alert_states
WHERE state = 'inactive'
  AND is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND match_labels_json IS NOT NULL
  AND trim(match_labels_json) != ''
  AND (? IS NULL OR alert_id > ?)
ORDER BY alert_id
LIMIT ?
`, cursor, cursor, batchSize)
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

func evaluateRecheckRules(rules []recheckJobRule, relevant map[string]struct{}, labels map[string]string) []recheckJobRule {
	if len(relevant) == 0 {
		return nil
	}
	matched := make([]recheckJobRule, 0)
	matchedBySource := make(map[string]bool, len(relevant))
	for _, r := range rules {
		if r.ObservedState != "active" {
			continue
		}
		if _, ok := relevant[r.SourceName]; !ok {
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
		if !matchedBySource[sourceName] {
			return nil
		}
	}
	return matched
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
	newState := StateSuppressed
	oldRaw := State(strings.TrimSpace(cand.RawState))
	if !oldRaw.IsValid() {
		oldRaw = StateActive
	}
	oldLive := false
	newLive := false
	oldSupp := false
	newSupp := true
	oldResolved := false
	newResolved := false
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
	return true, transitionID, nil
}

func insertSuppressionCauseHistoryTx(tx *sql.Tx, alertID string, transitionEventID int64, jobID int64, now time.Time, matched []recheckJobRule) error {
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

	status := "none"
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
		var revision any = nil
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

func runPhase2RetentionTx(tx *sql.Tx, now time.Time) error {
	cutoff := now.Add(-phase2SilenceRetentionTTL).UTC().Format(time.RFC3339Nano)

	if _, err := tx.Exec(`
DELETE FROM silence_recheck_jobs
WHERE status = 'completed'
  AND job_id NOT IN (
    SELECT recheck_job_id FROM alert_suppression_cause_history WHERE recheck_job_id IS NOT NULL
  )
  AND job_id <> (
    SELECT COALESCE(MAX(job_id), 0) FROM silence_recheck_jobs WHERE status = 'completed'
  )
`); err != nil {
		return fmt.Errorf("failed cleaning completed recheck jobs: %w", err)
	}

	if _, err := tx.Exec(`
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
  )
`, cutoff); err != nil {
		return fmt.Errorf("failed cleaning retired sources: %w", err)
	}

	if _, err := tx.Exec(`
DELETE FROM silence_revisions
WHERE last_observed_at < ?
  AND revision_id NOT IN (
    SELECT revision_id FROM alertmanager_source_silences
    UNION
    SELECT revision_id FROM silence_recheck_job_rules
    UNION
    SELECT revision_id FROM alert_suppression_cause_members WHERE revision_id IS NOT NULL
  )
`, cutoff); err != nil {
		return fmt.Errorf("failed cleaning silence revisions: %w", err)
	}

	if _, err := tx.Exec(`
DELETE FROM silence_rule_episodes
WHERE open_marker IS NULL
  AND ended_at IS NOT NULL
  AND ended_at < ?
  AND episode_id NOT IN (SELECT episode_id FROM silence_revisions)
`, cutoff); err != nil {
		return fmt.Errorf("failed cleaning silence episodes: %w", err)
	}

	if _, err := tx.Exec(`
DELETE FROM silence_rules
WHERE rule_id NOT IN (SELECT rule_id FROM silence_revisions)
  AND rule_id NOT IN (SELECT rule_id FROM alertmanager_source_silences)
  AND rule_id NOT IN (SELECT rule_id FROM silence_recheck_job_rules)
  AND rule_id NOT IN (SELECT rule_id FROM alert_suppression_cause_members)
  AND last_observed_at < ?
`, cutoff); err != nil {
		return fmt.Errorf("failed cleaning silence rules: %w", err)
	}

	return nil
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

func reconcileSilenceSourcesTx(tx *sql.Tx, sources []SourceSilenceState, now time.Time) (int, error) {
	if len(sources) == 0 {
		return 0, nil
	}
	shouldEnqueueRecheck := false
	for _, source := range sources {
		sourceName := strings.TrimSpace(source.SourceName)
		clusterID := strings.TrimSpace(source.ClusterID)
		if sourceName == "" || clusterID == "" {
			continue
		}
		syncStatus := strings.TrimSpace(source.SyncStatus)
		if syncStatus == "" {
			syncStatus = "unknown"
		}
		if syncStatus != "success" && syncStatus != "failed" && syncStatus != "invalid" && syncStatus != "retired" && syncStatus != "unknown" {
			syncStatus = "invalid"
		}

		previousFP, _ := readSourceFingerprintTx(tx, sourceName)
		newFP := buildSourceSilenceFingerprint(source)
		changed := newFP != previousFP

		if err := upsertAlertmanagerSourceTx(tx, source, newFP, now); err != nil {
			return 0, err
		}
		if syncStatus == "success" {
			if err := replaceSourceSilenceRelationsTx(tx, sourceName, clusterID, source.Silences, now); err != nil {
				return 0, err
			}
		}

		if syncStatus == "success" && changed {
			shouldEnqueueRecheck = true
		}
	}
	if shouldEnqueueRecheck {
		if _, err := enqueueSilenceRecheckJobTx(tx, now); err != nil {
			return 0, err
		}
		return 1, nil
	}
	return 0, nil
}

func readSourceFingerprintTx(tx *sql.Tx, sourceName string) (string, error) {
	var fp sql.NullString
	err := tx.QueryRow(`SELECT silence_set_fingerprint FROM alertmanager_sources WHERE source_name = ?`, sourceName).Scan(&fp)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed reading source fingerprint: %w", err)
	}
	if fp.Valid {
		return fp.String, nil
	}
	return "", nil
}

func buildSourceSilenceFingerprint(source SourceSilenceState) string {
	type silenceFP struct {
		ID        string `json:"id"`
		Status    string `json:"status"`
		StartsAt  string `json:"startsAt"`
		EndsAt    string `json:"endsAt"`
		UpdatedAt string `json:"updatedAt"`
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
		entries = append(entries, silenceFP{
			ID:        strings.TrimSpace(s.SilenceID),
			Status:    strings.TrimSpace(s.Status),
			StartsAt:  s.StartsAt.UTC().Format(time.RFC3339Nano),
			EndsAt:    s.EndsAt.UTC().Format(time.RFC3339Nano),
			UpdatedAt: updatedAt,
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
	if syncStatus == "success" {
		successAt = attemptAt
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
  silence_set_fingerprint=excluded.silence_set_fingerprint,
  last_configured_at=excluded.last_configured_at,
  last_attempt_at=excluded.last_attempt_at,
  last_success_at=COALESCE(excluded.last_success_at, alertmanager_sources.last_success_at),
  last_confirmed_at=COALESCE(excluded.last_success_at, alertmanager_sources.last_confirmed_at),
  last_error=excluded.last_error,
  retired_at=CASE WHEN excluded.sync_status='retired' THEN excluded.last_attempt_at ELSE NULL END
`, sourceName, clusterID, syncStatus, sourceFP, attemptAt, attemptAt, attemptAt, successAt, successAt, sanitizeNullableString(ptrString(source.LastError)), nil)
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
		state := strings.TrimSpace(silence.Status)
		if state == "" {
			state = "active"
		}
		if state != "active" && state != "pending" && state != "expired" {
			state = "pending"
		}
		nowRaw := now.UTC().Format(time.RFC3339Nano)
		_, err = tx.Exec(`
INSERT INTO alertmanager_source_silences(source_name, rule_id, revision_id, observed_state, first_observed_at, last_confirmed_at)
VALUES(?, ?, ?, ?, ?, ?)
`, sourceName, ruleID, revisionID, state, nowRaw, nowRaw)
		if err != nil {
			return fmt.Errorf("failed inserting source silence relation: %w", err)
		}
	}
	return nil
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
	if errors.Is(err, sql.ErrNoRows) {
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
	} else if err != nil {
		return 0, 0, fmt.Errorf("failed loading silence episode: %w", err)
	} else {
		if _, err = tx.Exec(`UPDATE silence_rule_episodes SET last_confirmed_at = ? WHERE episode_id = ?`, nowRaw, episodeID); err != nil {
			return 0, 0, fmt.Errorf("failed updating silence episode: %w", err)
		}
	}

	contentFP, evalFP, matchersJSON := buildSilenceRevisionFingerprints(silence)
	var revisionID int64
	err = tx.QueryRow(`SELECT revision_id FROM silence_revisions WHERE episode_id = ? AND content_fingerprint = ?`, episodeID, contentFP).Scan(&revisionID)
	if errors.Is(err, sql.ErrNoRows) {
		nextRevision := 1
		_ = tx.QueryRow(`SELECT COALESCE(MAX(revision_number),0)+1 FROM silence_revisions WHERE episode_id = ?`, episodeID).Scan(&nextRevision)
		updatedAtRaw := any(nil)
		if silence.UpdatedAt != nil {
			updatedAtRaw = silence.UpdatedAt.UTC().Format(time.RFC3339Nano)
		}
		annotationsJSON := strings.TrimSpace(silence.AnnotationsJSON)
		if annotationsJSON == "" {
			annotationsJSON = "{}"
		}
		res, ierr := tx.Exec(`
INSERT INTO silence_revisions(
  episode_id, rule_id, revision_number, content_fingerprint, evaluation_fingerprint,
  matchers_json, starts_at, ends_at, source_updated_at, created_by, comment,
  annotations_json, ticket_id, ticket_url, raw_rule_payload_json, first_observed_at, last_observed_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, episodeID, ruleID, nextRevision, contentFP, evalFP, matchersJSON,
			silence.StartsAt.UTC().Format(time.RFC3339Nano), silence.EndsAt.UTC().Format(time.RFC3339Nano),
			updatedAtRaw, silence.CreatedBy, silence.Comment, annotationsJSON, silence.TicketID, silence.TicketURL,
			sanitizeNullableString(ptrString(silence.RawPayloadJSON)), nowRaw, nowRaw)
		if ierr != nil {
			return 0, 0, fmt.Errorf("failed creating silence revision: %w", ierr)
		}
		revisionID, _ = res.LastInsertId()
	} else if err != nil {
		return 0, 0, fmt.Errorf("failed loading silence revision: %w", err)
	} else {
		if _, err = tx.Exec(`UPDATE silence_revisions SET last_observed_at = ? WHERE revision_id = ?`, nowRaw, revisionID); err != nil {
			return 0, 0, fmt.Errorf("failed updating silence revision observation time: %w", err)
		}
	}

	return ruleID, revisionID, nil
}

func buildSilenceRevisionFingerprints(silence SourceSilenceSnapshot) (string, string, string) {
	matchers := make([]SilenceMatcherSnapshot, 0, len(silence.Matchers))
	for _, m := range silence.Matchers {
		matchers = append(matchers, m)
	}
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
	jobID, _ := res.LastInsertId()
	if jobID == 0 {
		if err = tx.QueryRow(`SELECT job_id FROM silence_recheck_jobs WHERE open_context_fingerprint = ?`, ctxFingerprint).Scan(&jobID); err != nil {
			return 0, fmt.Errorf("failed to load existing recheck job id: %w", err)
		}
	}
	if _, err = tx.Exec(`
INSERT OR IGNORE INTO silence_recheck_job_rules(job_id, source_name, rule_id, revision_id, observed_state)
SELECT ?, source_name, rule_id, revision_id, observed_state
FROM alertmanager_source_silences
`, jobID); err != nil {
		return 0, fmt.Errorf("failed to snapshot recheck job rules: %w", err)
	}
	return jobID, nil
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

func abortOnLegacyPreviewTables(tx *sql.Tx) error {
	legacyTables := []string{"alerts_current", "alert_state_history"}
	for _, table := range legacyTables {
		exists, err := queryInt(tx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table)
		if err != nil {
			return fmt.Errorf("failed to inspect legacy table %q: %w", table, err)
		}
		if exists == 0 {
			continue
		}
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		rows, err := queryInt(tx, query)
		if err != nil {
			return fmt.Errorf("failed to count legacy table %q rows: %w", table, err)
		}
		if rows > 0 {
			return fmt.Errorf("%w: table %s has %d rows", ErrLegacyPreviewTablesNotMigrated, table, rows)
		}
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
}, query string, args ...any) (int, error) {
	var out int
	if err := q.QueryRow(query, args...).Scan(&out); err != nil {
		return 0, err
	}
	return out, nil
}

func queryInt64(q interface {
	QueryRow(query string, args ...any) *sql.Row
}, query string, args ...any) (int64, error) {
	var out int64
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
