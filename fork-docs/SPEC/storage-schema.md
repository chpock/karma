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

## 3. Core tables

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
- `match_labels_json TEXT` (nullable when raw labels are unavailable)
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

- `1`

This preview project supports only schema v1. Startup creates schema v1 in an empty
database and reopens an existing v1 database without changing its shape. Any other
schema version or unrecognized non-empty database is rejected without modification.

Design references:

- `fork-docs/SPEC/phase2-silence-awareness-plan.md`
- `fork-docs/SPEC/phase2-silence-awareness-schema.md`

Schema v1 includes target-scoped lifecycle tracking:

- `alert_target_states` (current per-target state by `(alert_id, target)`)
- `alert_target_events` (append-only target transition history)
- `alert_target_match_labels` (normalized target-scoped raw matcher-label index)
- `alertmanager_cluster_consistency` (last-confirmed HA silence consistency)
- `alert_target_suppression_cause_history` / `_members` (immutable target causes)
- `alert_target_aggregate_suppression_history` / `_links` (global transition evidence)

These tables preserve per-target liveness/state when one logical fingerprint is
present across multiple clusters with divergent visibility.

Before persistence, all deduplicated alert groups sharing a label fingerprint are
merged into one lifecycle input. Target/source membership is the union across
groups; receiver/group metadata is selected deterministically. Group iteration
order can therefore never drop or flap a target.

`alert_target_states.source_members_json` retains previously relevant HA members
while any member has failed/unknown coverage. The set may shrink only after all
still-configured previous members were polled successfully; members removed from
the current source manifest are excluded from completeness checks. This prevents
both false disappearance during partial failure and permanently-live rows after a
topology change. Membership reconciliation and full-target retirement continue
for inactive rows; only the live-to-inactive state transition is liveness-gated.

If every member of a target is removed from the configured source manifest, the
target row receives `retired_at` and is excluded from preview/recheck. Its last
confirmed remote state is preserved rather than inventing an Alertmanager state
transition. A later live observation clears retirement and rebuilds its label
index.

`alert_target_states.match_labels_fingerprint` gates maintenance of
`alert_target_match_labels`. An unchanged raw-label payload performs no delete or
per-label inserts during a poll; the normalized index is rebuilt only when the
payload fingerprint changes.

`alert_target_match_labels` contains only current, non-retired target rows; writers
enforce that materialized-index invariant, so
hot autocomplete queries do not perform one target-state join per label row. Live
rows with a missing/changed fingerprint overlay the index by alert ID.
Silence episodes remain open across partial HA failure and close only after complete
successful cluster coverage confirms absence.

Target suppression provenance is reference-safe under housekeeping: target cause
members protect their revisions/rules/sources, and target cause histories protect
their recheck jobs. A global transition produced after multiple target promotions
links the effective target history IDs rather than copying or re-evaluating rules.

## 5. Silence-awareness tables

Implemented Phase 2 tables:

- `alertmanager_sources`
- `silence_rules`
- `silence_rule_episodes`
- `silence_revisions`
- `alertmanager_source_silences`
- `silence_recheck_jobs`
- `silence_recheck_job_rules`
- `silence_recheck_job_candidates`
- `alert_suppression_cause_history`
- `alert_suppression_cause_members`
- `alertmanager_cluster_consistency`
- `alert_target_suppression_cause_history`
- `alert_target_suppression_cause_members`
- `alert_target_aggregate_suppression_history`
- `alert_target_aggregate_suppression_links`

These tables store:

- last-successful silence set per source,
- immutable silence revisions and episodes,
- persistent FIFO recheck jobs with immutable per-job rules and candidate membership,
- append-only suppression cause history and members.

Phase 2 matching inputs:

- `match_labels_json` is sourced from raw Alertmanager labels (pre keep/strip transforms).
- source silence-set fingerprint is content-based; transport `sync_status` does not participate in fingerprinting.

## 6. Retention inputs

- TTL uses `alert_states.last_seen_at`.
- Max-alert limit eviction ordering uses latest change timestamp from `alert_events` (fallback to `alert_states.updated_at`).

Phase 2 housekeeping also prunes unreferenced completed recheck jobs and stale unreferenced silence history entities under FK constraints.

The complete schema-v1 inventory and relationship contract is maintained in
`phase2-silence-awareness-schema.md`. Literal executable DDL exists only in
`internal/fork/alertlifecycle/store.go` to prevent documentation copies from
becoming an alternate schema definition.
