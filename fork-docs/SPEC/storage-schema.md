# Spec: SQLite Storage Schema

## 1. Goals

- Persist current lifecycle tuple per alert.
- Persist full immutable transition history.
- Support retention, regroup observability, and operator stats.

## 2. Engine mode

- Single-replica local SQLite.
- Pragmas:
  - `journal_mode=WAL`
  - `synchronous=NORMAL`
  - `foreign_keys=ON`
  - `auto_vacuum=INCREMENTAL`
  - `busy_timeout=5000` (milliseconds)

## 3. Tables

### 3.1 `alert_states`

One row per alert identity.

- `alert_id TEXT PRIMARY KEY`
- `state TEXT NOT NULL` (projected state)
- `raw_state TEXT NOT NULL` (`active|suppressed|unprocessed` for live source)
- `is_live INTEGER NOT NULL` (`0|1`)
- `is_suppressed INTEGER NOT NULL` (`0|1`)
- `is_resolved INTEGER NOT NULL` (`0|1`)
- `receiver TEXT NOT NULL`
- `group_id TEXT NOT NULL`
- `group_labels_json TEXT NOT NULL DEFAULT '{}'`
- `labels_json TEXT NOT NULL`
- `match_labels_json TEXT` (nullable for legacy rows)
- `annotations_json TEXT NOT NULL`
- `starts_at TEXT NOT NULL` (RFC3339Nano UTC)
- `last_seen_at TEXT` (nullable)
- `relevant_upstreams_json TEXT NOT NULL`
- `relevant_sources_json TEXT NOT NULL DEFAULT '[]'`
- `suppression_observed_at TEXT` (nullable)
- `resolved_at TEXT` (nullable)
- `resolved_by TEXT` (nullable)
- `resolved_comment TEXT` (nullable)
- `updated_at TEXT NOT NULL`

Indexes:

- `idx_alert_states_state`
- `idx_alert_states_raw_state`
- `idx_alert_states_is_live`
- `idx_alert_states_is_suppressed`
- `idx_alert_states_last_seen`

### 3.2 `alert_events`

Append-only transition log.

- `id INTEGER PRIMARY KEY AUTOINCREMENT`
- `alert_id TEXT NOT NULL`
- `old_state TEXT` (nullable)
- `new_state TEXT NOT NULL`
- `old_raw_state TEXT` (nullable)
- `new_raw_state TEXT NOT NULL`
- `old_is_live INTEGER` (nullable)
- `new_is_live INTEGER NOT NULL`
- `old_is_suppressed INTEGER` (nullable)
- `new_is_suppressed INTEGER NOT NULL`
- `old_is_resolved INTEGER` (nullable)
- `new_is_resolved INTEGER NOT NULL`
- `changed_at TEXT NOT NULL`
- `change_source TEXT NOT NULL`
- `changed_by TEXT` (nullable)
- `reason_code TEXT` (nullable)
- `reason_text TEXT` (nullable)

Indexes:

- `idx_state_history_alert_changed_at` on `(alert_id, changed_at DESC)`
- `idx_state_history_changed_at` on `(changed_at DESC)`

FK:

- `alert_events.alert_id` -> `alert_states.alert_id` (`ON DELETE CASCADE`)

### 3.3 `metadata`

- `key TEXT PRIMARY KEY`
- `value TEXT NOT NULL`

Used for schema version, housekeeping timestamps, regroup status/progress/fingerprints.

## 4. Versioning

Current schema version:

- `5`

Phase 2 is implemented. Design and migration references:

- `fork-docs/SPEC/phase2-silence-awareness-plan.md`
- `fork-docs/SPEC/phase2-silence-awareness-schema.md`
- `fork-docs/SPEC/db-migrations/v4-to-v5.md`

## 5. Phase 2 tables

Implemented Phase 2 tables:

- `alertmanager_sources`
- `silence_rules`
- `silence_rule_episodes`
- `silence_revisions`
- `alertmanager_source_silences`
- `silence_recheck_jobs`
- `silence_recheck_job_rules`
- `alert_suppression_cause_history`
- `alert_suppression_cause_members`

These tables store:

- last-successful silence set per source,
- immutable silence revisions and episodes,
- persistent FIFO recheck jobs with immutable per-job decision input copied from all current source relations,
- append-only suppression cause history and members.

Phase 2 matching inputs:

- `match_labels_json` is sourced from raw Alertmanager labels (pre keep/strip transforms).
- source silence-set fingerprint is content-based; transport `sync_status` does not participate in fingerprinting.

Migration policy and records:

- `fork-docs/SPEC/db-migrations.md`
- `fork-docs/SPEC/db-migrations/`

## 6. Retention inputs

- TTL uses `alert_states.last_seen_at`.
- Max-alert limit eviction ordering uses latest change timestamp from `alert_events` (fallback to `alert_states.updated_at`).

Phase 2 housekeeping also prunes unreferenced completed recheck jobs and stale unreferenced silence history entities under FK constraints.
