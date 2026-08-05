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

const schemaVersion = "3"

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
CREATE TABLE IF NOT EXISTS alerts_current (
  alert_id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  receiver TEXT NOT NULL,
  group_id TEXT NOT NULL,
  group_labels_json TEXT NOT NULL DEFAULT '{}',
  labels_json TEXT NOT NULL,
  annotations_json TEXT NOT NULL,
  starts_at TEXT NOT NULL,
  last_seen_at TEXT,
  relevant_upstreams_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alerts_current_state ON alerts_current(state);
CREATE INDEX IF NOT EXISTS idx_alerts_current_last_seen ON alerts_current(last_seen_at);

CREATE TABLE IF NOT EXISTS alert_state_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  old_state TEXT,
  new_state TEXT NOT NULL,
  changed_at TEXT NOT NULL,
  change_source TEXT NOT NULL,
  changed_by TEXT,
  reason TEXT,
  FOREIGN KEY (alert_id) REFERENCES alerts_current(alert_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_state_history_alert_changed_at
  ON alert_state_history(alert_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_state_history_changed_at
  ON alert_state_history(changed_at DESC);

CREATE TABLE IF NOT EXISTS metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin schema transaction: %w", err)
	}

	if _, err = tx.Exec(schemaSQL); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to apply schema: %w", err)
	}

	if _, err = tx.Exec(`ALTER TABLE alerts_current ADD COLUMN group_labels_json TEXT NOT NULL DEFAULT '{}'`); err != nil {
		errText := strings.ToLower(err.Error())
		if !strings.Contains(errText, "duplicate column") {
			_ = tx.Rollback()
			return fmt.Errorf("failed to migrate schema (group_labels_json): %w", err)
		}
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

	if err := validateCurrentAlert(alert); err != nil {
		return err
	}

	now := time.Now().UTC()
	if alert.UpdatedAt.IsZero() {
		alert.UpdatedAt = now
	}

	_, err := s.db.Exec(`
INSERT INTO alerts_current(
  alert_id,
  state,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(alert_id) DO UPDATE SET
  state=excluded.state,
  receiver=excluded.receiver,
  group_id=excluded.group_id,
  group_labels_json=excluded.group_labels_json,
  labels_json=excluded.labels_json,
  annotations_json=excluded.annotations_json,
  starts_at=excluded.starts_at,
  last_seen_at=excluded.last_seen_at,
  relevant_upstreams_json=excluded.relevant_upstreams_json,
  updated_at=excluded.updated_at
`,
		alert.AlertID,
		string(alert.State),
		alert.Receiver,
		alert.GroupID,
		alert.GroupLabelsJSON,
		alert.LabelsJSON,
		alert.AnnotationsJSON,
		alert.StartsAt.UTC().Format(time.RFC3339Nano),
		formatNullableTime(alert.LastSeenAt),
		alert.RelevantUpstreams,
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

	if err := validateTransition(transition); err != nil {
		return err
	}

	changedAt := transition.ChangedAt
	if changedAt.IsZero() {
		changedAt = time.Now().UTC()
	}

	_, err := s.db.Exec(`
INSERT INTO alert_state_history(
  alert_id,
  old_state,
  new_state,
  changed_at,
  change_source,
  changed_by,
  reason
) VALUES(?, ?, ?, ?, ?, ?, ?)
`,
		transition.AlertID,
		formatNullableState(transition.OldState),
		string(transition.NewState),
		changedAt.UTC().Format(time.RFC3339Nano),
		string(transition.ChangeSource),
		transition.ChangedBy,
		transition.Reason,
	)
	if err != nil {
		return fmt.Errorf("failed to insert state transition: %w", err)
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
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  updated_at
FROM alerts_current
WHERE alert_id = ?
`, alertID)

	var (
		stateRaw     string
		startsAtRaw  string
		lastSeenRaw  sql.NullString
		updatedAtRaw string
		current      AlertCurrent
	)

	err := row.Scan(
		&current.AlertID,
		&stateRaw,
		&current.Receiver,
		&current.GroupID,
		&current.GroupLabelsJSON,
		&current.LabelsJSON,
		&current.AnnotationsJSON,
		&startsAtRaw,
		&lastSeenRaw,
		&current.RelevantUpstreams,
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
	err := s.db.QueryRow(`SELECT COUNT(*) FROM alert_state_history WHERE alert_id = ?`, alertID).Scan(&count)
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
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  updated_at
FROM alerts_current
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

	currentRows, err := queryInt(s.db, `SELECT COUNT(*) FROM alerts_current`)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to count current alerts: %w", err)
	}
	stateCounts, err := s.queryStateCounts()
	if err != nil {
		return Stats{}, err
	}
	historyRows, err := queryInt(s.db, `SELECT COUNT(*) FROM alert_state_history`)
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
		CurrentRows:              currentRows,
		HistoryRows:              historyRows,
		StateCounts:              stateCounts,
		GroupingFingerprint:      strings.TrimSpace(groupingFingerprint),
		RegroupTargetFingerprint: strings.TrimSpace(regroupTargetFingerprint),
		RegroupStatus:            strings.TrimSpace(regroupStatus),
		RegroupError:             strings.TrimSpace(regroupError),
		RegroupProcessed:         regroupProcessed,
		RegroupTotal:             regroupTotal,
		RegroupStartedAt:         regroupStartedAt,
		RegroupFinishedAt:        regroupFinishedAt,
		FileSizeBytes:            fileSize,
		PageCount:                pageCount,
		PageSizeBytes:            pageSize,
		FreelistCount:            freelistCount,
		LastHousekeepingAt:       lastHousekeepingAt,
		LastIncrementalVacuumAt:  lastVacuumAt,
	}, nil
}

func (s *Store) queryStateCounts() (map[string]int, error) {
	out := map[string]int{}

	rows, err := s.db.Query(`
SELECT state, COUNT(*)
FROM alerts_current
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

	currentState, err := getCurrentAlertStateTx(tx, alertID)
	if err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAlertNotFound
		}
		return nil, err
	}

	if currentState == StateResolved {
		_ = tx.Rollback()
		return nil, ErrAlertAlreadyResolved
	}

	if currentState != StateInactive {
		_ = tx.Rollback()
		return nil, ErrResolveStateConflict
	}

	if err = updateStateOnlyTx(tx, alertID, StateResolved, ts); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	cleanChangedBy := sanitizeNullableString(changedBy)
	if err = insertTransitionTx(tx, AlertStateTransition{
		AlertID:      alertID,
		OldState:     &currentState,
		NewState:     StateResolved,
		ChangedAt:    ts,
		ChangeSource: ChangeSourceManual,
		ChangedBy:    cleanChangedBy,
		Reason:       sanitizeNullableString(reason),
	}); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit resolve transaction: %w", err)
	}

	return &ResolveResult{
		AlertID:   alertID,
		OldState:  currentState,
		NewState:  StateResolved,
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

	for _, live := range input.LiveAlerts {
		current, found := currentByID[live.AlertID]
		if !found {
			if err = insertCurrentAlertTx(tx, live, now); err != nil {
				_ = tx.Rollback()
				return PollCycleResult{}, err
			}
			if err = insertTransitionTx(tx, AlertStateTransition{
				AlertID:      live.AlertID,
				OldState:     nil,
				NewState:     live.State,
				ChangedAt:    now,
				ChangeSource: ChangeSourcePoller,
			}); err != nil {
				_ = tx.Rollback()
				return PollCycleResult{}, err
			}
			result.Inserted++
			result.Transitions++
			continue
		}

		newState := live.State
		if current.State == StateResolved {
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

		if err = updateCurrentAlertFromLiveTx(tx, current.AlertID, live, newState, string(relevantJSON), now); err != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, err
		}
		result.Updated++

		if current.State != newState {
			old := current.State
			if err = insertTransitionTx(tx, AlertStateTransition{
				AlertID:      current.AlertID,
				OldState:     &old,
				NewState:     newState,
				ChangedAt:    now,
				ChangeSource: ChangeSourcePoller,
			}); err != nil {
				_ = tx.Rollback()
				return PollCycleResult{}, err
			}
			result.Transitions++
		}
	}

	for alertID, current := range currentByID {
		if _, exists := liveByID[alertID]; exists {
			continue
		}

		if current.State == StateInactive || current.State == StateResolved {
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

		if err = updateStateOnlyTx(tx, alertID, StateInactive, now); err != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, err
		}
		old := current.State
		if err = insertTransitionTx(tx, AlertStateTransition{
			AlertID:      alertID,
			OldState:     &old,
			NewState:     StateInactive,
			ChangedAt:    now,
			ChangeSource: ChangeSourcePoller,
		}); err != nil {
			_ = tx.Rollback()
			return PollCycleResult{}, err
		}
		result.SetInactive++
		result.Transitions++
	}

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

	totalRows, err := queryInt(tx, `SELECT COUNT(*) FROM alerts_current`)
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
	totalRows, err := queryInt(tx, `SELECT COUNT(*) FROM alerts_current`)
	if err != nil {
		_ = tx.Rollback()
		return false, 0, 0, 0, fmt.Errorf("failed to count rows in regroup batch: %w", err)
	}

	rows, err := tx.Query(`
SELECT rowid, alert_id, receiver, labels_json
FROM alerts_current
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
UPDATE alerts_current
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
	if transition.OldState != nil && !transition.OldState.IsValid() {
		return fmt.Errorf("invalid old state: %q", *transition.OldState)
	}
	if !transition.ChangeSource.IsValid() {
		return fmt.Errorf("invalid change source: %q", transition.ChangeSource)
	}

	return nil
}

func formatNullableState(state *State) any {
	if state == nil {
		return nil
	}
	return string(*state)
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

func (s ChangeSource) IsValid() bool {
	switch s {
	case ChangeSourcePoller, ChangeSourceManual:
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
	if len(alert.RelevantUpstreams) == 0 {
		return errors.New("live snapshot relevantUpstreams cannot be empty")
	}
	for _, upstream := range alert.RelevantUpstreams {
		if strings.TrimSpace(upstream) == "" {
			return errors.New("live snapshot relevantUpstreams contains empty name")
		}
	}

	return nil
}

func readAllCurrentAlertsTx(tx *sql.Tx) (map[string]AlertCurrent, error) {
	rows, err := tx.Query(`
SELECT
  alert_id,
  state,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  updated_at
FROM alerts_current
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
		stateRaw     string
		startsAtRaw  string
		lastSeenRaw  sql.NullString
		updatedAtRaw string
		current      AlertCurrent
	)

	err := scanner.Scan(
		&current.AlertID,
		&stateRaw,
		&current.Receiver,
		&current.GroupID,
		&current.GroupLabelsJSON,
		&current.LabelsJSON,
		&current.AnnotationsJSON,
		&startsAtRaw,
		&lastSeenRaw,
		&current.RelevantUpstreams,
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

	_, err = tx.Exec(`
INSERT INTO alerts_current(
  alert_id,
  state,
  receiver,
  group_id,
  group_labels_json,
  labels_json,
  annotations_json,
  starts_at,
  last_seen_at,
  relevant_upstreams_json,
  updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
		live.AlertID,
		string(live.State),
		live.Receiver,
		live.GroupID,
		live.GroupLabelsJSON,
		live.LabelsJSON,
		live.AnnotationsJSON,
		live.StartsAt.UTC().Format(time.RFC3339Nano),
		live.LastSeenAt.UTC().Format(time.RFC3339Nano),
		string(relevantJSON),
		now.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("failed to insert current alert: %w", err)
	}

	return nil
}

func updateCurrentAlertFromLiveTx(tx *sql.Tx, alertID string, live LiveAlertSnapshot, newState State, relevantJSON string, now time.Time) error {
	_, err := tx.Exec(`
UPDATE alerts_current SET
  state = ?,
  receiver = ?,
  group_id = ?,
  group_labels_json = ?,
  labels_json = ?,
  annotations_json = ?,
  starts_at = ?,
  last_seen_at = ?,
  relevant_upstreams_json = ?,
  updated_at = ?
WHERE alert_id = ?
`,
		string(newState),
		live.Receiver,
		live.GroupID,
		live.GroupLabelsJSON,
		live.LabelsJSON,
		live.AnnotationsJSON,
		live.StartsAt.UTC().Format(time.RFC3339Nano),
		live.LastSeenAt.UTC().Format(time.RFC3339Nano),
		relevantJSON,
		now.UTC().Format(time.RFC3339Nano),
		alertID,
	)
	if err != nil {
		return fmt.Errorf("failed to update current alert from live: %w", err)
	}

	return nil
}

func updateStateOnlyTx(tx *sql.Tx, alertID string, newState State, now time.Time) error {
	_, err := tx.Exec(`
UPDATE alerts_current
SET state = ?, updated_at = ?
WHERE alert_id = ?
`, string(newState), now.UTC().Format(time.RFC3339Nano), alertID)
	if err != nil {
		return fmt.Errorf("failed to update alert state: %w", err)
	}

	return nil
}

func insertTransitionTx(tx *sql.Tx, transition AlertStateTransition) error {
	if err := validateTransition(transition); err != nil {
		return err
	}

	changedAt := transition.ChangedAt
	if changedAt.IsZero() {
		changedAt = time.Now().UTC()
	}

	_, err := tx.Exec(`
INSERT INTO alert_state_history(
  alert_id,
  old_state,
  new_state,
  changed_at,
  change_source,
  changed_by,
  reason
) VALUES(?, ?, ?, ?, ?, ?, ?)
`,
		transition.AlertID,
		formatNullableState(transition.OldState),
		string(transition.NewState),
		changedAt.UTC().Format(time.RFC3339Nano),
		string(transition.ChangeSource),
		transition.ChangedBy,
		transition.Reason,
	)
	if err != nil {
		return fmt.Errorf("failed to insert state transition: %w", err)
	}

	return nil
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

func getCurrentAlertStateTx(tx *sql.Tx, alertID string) (State, error) {
	var stateRaw string
	err := tx.QueryRow(`SELECT state FROM alerts_current WHERE alert_id = ?`, alertID).Scan(&stateRaw)
	if err != nil {
		return "", err
	}

	state := State(stateRaw)
	if !state.IsValid() {
		return "", fmt.Errorf("invalid state in database: %q", stateRaw)
	}

	return state, nil
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
DELETE FROM alerts_current
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

func enforceMaxAlertsTx(tx *sql.Tx, maxAlerts int) (int, error) {
	currentCount, err := queryInt(tx, `SELECT COUNT(*) FROM alerts_current`)
	if err != nil {
		return 0, fmt.Errorf("failed to count current alerts for max limit: %w", err)
	}

	if currentCount <= maxAlerts {
		return 0, nil
	}

	toDelete := currentCount - maxAlerts
	res, err := tx.Exec(`
DELETE FROM alerts_current
WHERE alert_id IN (
  SELECT alert_id
  FROM alerts_current
  ORDER BY COALESCE(
      (SELECT MAX(h.changed_at) FROM alert_state_history h WHERE h.alert_id = alerts_current.alert_id),
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
