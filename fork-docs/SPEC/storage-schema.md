# Spec: SQLite Storage Schema

## 1. Goals

- Persist current lifecycle state per alert.
- Persist full transition history with exact timestamps.
- Support deterministic cleanup and operational stats.

## 2. Database engine mode

Target mode:

- Local SQLite in single-replica deployment.

Recommended pragmas:

- `journal_mode=WAL`
- `synchronous=NORMAL`
- `foreign_keys=ON`
- `auto_vacuum=INCREMENTAL`

## 3. Tables

## 3.1 `alerts_current`

One row per alert identity (same identity semantics as current core/UI).

Suggested fields:

- `alert_id TEXT PRIMARY KEY` - stable alert identity key.
- `state TEXT NOT NULL` - one of `active|suppressed|unprocessed|inactive|resolved`.
- `receiver TEXT NOT NULL`
- `group_id TEXT NOT NULL`
- `labels_json TEXT NOT NULL`
- `annotations_json TEXT NOT NULL`
- `starts_at TEXT NOT NULL` - RFC3339 UTC.
- `last_seen_at TEXT` - nullable for never-seen edge cases; normally set for live-seen alerts.
- `relevant_upstreams_json TEXT NOT NULL` - JSON array of upstream names from latest successful live snapshot.
- `updated_at TEXT NOT NULL` - last row update timestamp UTC.

Indexes:

- `idx_alerts_current_state` on `(state)`.
- `idx_alerts_current_last_seen` on `(last_seen_at)`.

## 3.2 `alert_state_history`

Append-only transition log.

Suggested fields:

- `id INTEGER PRIMARY KEY AUTOINCREMENT`
- `alert_id TEXT NOT NULL`
- `old_state TEXT` - nullable for initial insert.
- `new_state TEXT NOT NULL`
- `changed_at TEXT NOT NULL` - RFC3339 UTC.
- `change_source TEXT NOT NULL` - enum-like values: `poller`, `manual`.
- `changed_by TEXT` - nullable username, `NULL` when unknown/unavailable.
- `reason TEXT` - optional free-text or code for diagnostics.

Indexes:

- `idx_state_history_alert_changed_at` on `(alert_id, changed_at DESC)`.
- `idx_state_history_changed_at` on `(changed_at DESC)`.

Foreign key:

- optional FK from `alert_state_history.alert_id` to `alerts_current.alert_id`.

Note:

- If FK is enabled with delete cascades, ensure retention deletes are intentional.

## 4. Derived values

`last_state_change_at` can be computed from history (`MAX(changed_at)` per alert).
It is optional to denormalize into `alerts_current` for performance.

Current design preference:

- no mandatory denormalized field.
- if query cost becomes high, add denormalized column in a follow-up migration.

## 5. Cleanup metadata

Use an internal metadata table for housekeeping timestamps.

Example `metadata` table:

- `key TEXT PRIMARY KEY`
- `value TEXT NOT NULL`

Suggested keys:

- `last_ttl_cleanup_at`
- `last_limit_cleanup_at`
- `last_incremental_vacuum_at`
- `schema_version`

## 6. Record accounting used by retention

TTL uses:

- `alerts_current.last_seen_at`.

Limit pruning order uses:

- latest transition age from `alert_state_history`.
- oldest state-change-first eviction when over limit after TTL pass.
