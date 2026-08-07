# Phase 2 Silence Awareness: Exact SQLite Schema

## 1. Status and ownership

Status: implemented in fork runtime (schema version `5`). This document remains the design + DDL contract reference.

Target schema version: `5` (upgrade from current version `4`).

This document is the exact DDL companion to `phase2-silence-awareness-plan.md`.
The plan owns requirements, rejected alternatives, and D1-D22 rationale. This file
owns table shape, constraints, indexes, transaction order, retention, and migration.

Current runtime remains single-replica SQLite. The schema avoids SQLite triggers,
process-local queues, and poll-history assumptions. Future remote-RDBMS mode uses
the D18 elected-reconciler model and adds lease/fencing infrastructure additively.

## 2. Existing table rebuilds

### 2.1 `alert_states`

```sql
CREATE TABLE alert_states (
  alert_id TEXT PRIMARY KEY,
  state TEXT NOT NULL CONSTRAINT ck_alert_states_state CHECK (
    state IN ('active', 'suppressed', 'unprocessed', 'inactive', 'resolved')
  ),
  raw_state TEXT NOT NULL CONSTRAINT ck_alert_states_raw_state CHECK (
    raw_state IN ('active', 'suppressed', 'unprocessed')
  ),
  is_live INTEGER NOT NULL CONSTRAINT ck_alert_states_is_live CHECK (is_live IN (0, 1)),
  is_suppressed INTEGER NOT NULL CONSTRAINT ck_alert_states_is_suppressed CHECK (is_suppressed IN (0, 1)),
  is_resolved INTEGER NOT NULL CONSTRAINT ck_alert_states_is_resolved CHECK (is_resolved IN (0, 1)),
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
  updated_at TEXT NOT NULL,
  CONSTRAINT ck_alert_states_suppression_time
    CHECK (is_suppressed = 1 OR suppression_observed_at IS NULL),
  CONSTRAINT ck_alert_states_projection CHECK (
    (
      is_live = 1
      AND is_resolved = 0
      AND (
        (raw_state = 'active' AND is_suppressed = 0 AND state = 'active')
        OR (raw_state = 'suppressed' AND is_suppressed = 1 AND state = 'suppressed')
        OR (raw_state = 'unprocessed' AND is_suppressed = 0 AND state = 'unprocessed')
      )
    )
    OR (is_live = 0 AND is_resolved = 1 AND state = 'resolved')
    OR (is_live = 0 AND is_resolved = 0 AND is_suppressed = 1 AND state = 'suppressed')
    OR (is_live = 0 AND is_resolved = 0 AND is_suppressed = 0 AND state = 'inactive')
  )
);

CREATE INDEX idx_alert_states_state
  ON alert_states (state);
CREATE INDEX idx_alert_states_raw_state
  ON alert_states (raw_state);
CREATE INDEX idx_alert_states_is_live
  ON alert_states (is_live);
CREATE INDEX idx_alert_states_is_suppressed
  ON alert_states (is_suppressed);
CREATE INDEX idx_alert_states_last_seen
  ON alert_states (last_seen_at);
CREATE INDEX idx_alert_states_silence_candidates
  ON alert_states (is_live, is_resolved, is_suppressed, alert_id);
```

`match_labels_json=NULL` means original pre-transform source labels are unavailable.
Such rows never enter Phase 2 matching and remain visible in warning/stats.

`relevant_sources_json` canonical shape:

```json
[
  {"name":"am-1","cluster":"production"},
  {"name":"am-2","cluster":"production"}
]
```

Entries sort by `name`, then `cluster` before persistence and CAS comparison.

### 2.2 `alert_events`

```sql
CREATE TABLE alert_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  old_state TEXT,
  new_state TEXT NOT NULL,
  old_raw_state TEXT,
  new_raw_state TEXT NOT NULL,
  old_is_live INTEGER,
  new_is_live INTEGER NOT NULL CONSTRAINT ck_alert_events_new_is_live CHECK (new_is_live IN (0, 1)),
  old_is_suppressed INTEGER CONSTRAINT ck_alert_events_old_is_suppressed CHECK (
    old_is_suppressed IS NULL OR old_is_suppressed IN (0, 1)
  ),
  new_is_suppressed INTEGER NOT NULL CONSTRAINT ck_alert_events_new_is_suppressed CHECK (new_is_suppressed IN (0, 1)),
  old_is_resolved INTEGER,
  new_is_resolved INTEGER NOT NULL CONSTRAINT ck_alert_events_new_is_resolved CHECK (new_is_resolved IN (0, 1)),
  changed_at TEXT NOT NULL,
  change_source TEXT NOT NULL,
  changed_by TEXT,
  reason_code TEXT,
  reason_text TEXT,
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE
);

CREATE INDEX idx_alert_events_alert_changed_at
  ON alert_events (alert_id, changed_at DESC);
CREATE INDEX idx_alert_events_changed_at
  ON alert_events (changed_at DESC);
```

Phase 2 adds `change_source='silence'` and
`reason_code='silence_match_promoted'`. Stable enum validation remains in Go so a
new documented value does not force an SQLite table rebuild.

## 3. Alertmanager source state

### 3.1 `alertmanager_sources`

One mutable row per stable `AlertmanagerConfig.Name`:

```sql
CREATE TABLE alertmanager_sources (
  source_name TEXT PRIMARY KEY,
  current_cluster_id TEXT,
  sync_status TEXT NOT NULL CONSTRAINT ck_alertmanager_sources_sync_status CHECK (
    sync_status IN ('unknown', 'success', 'failed', 'invalid', 'retired')
  ),
  silence_set_fingerprint TEXT,
  first_configured_at TEXT NOT NULL,
  last_configured_at TEXT NOT NULL,
  last_attempt_at TEXT,
  last_success_at TEXT,
  last_confirmed_at TEXT,
  last_error TEXT,
  retired_at TEXT,
  CONSTRAINT ck_alertmanager_sources_active_cluster
    CHECK (sync_status = 'retired' OR current_cluster_id IS NOT NULL),
  CONSTRAINT ck_alertmanager_sources_retired_at
    CHECK (retired_at IS NULL OR sync_status = 'retired')
);

CREATE INDEX idx_alertmanager_sources_status
  ON alertmanager_sources (sync_status, source_name);
CREATE INDEX idx_alertmanager_sources_retired
  ON alertmanager_sources (retired_at);
```

Failure/invalid status never clears the last confirmed set. A migrated retired
source whose cluster cannot be recovered uses NULL rather than an ambiguous empty
string. Re-adding the same Name restores the same identity only after a successful
refresh establishes current cluster/set data.

## 4. Silence identity and history

### 4.1 `silence_rules`

Logical identity `(cluster_id, silence_id)` exists independently of payload so D16
can retain an unresolved `SilencedBy` ID.

```sql
CREATE TABLE silence_rules (
  rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id TEXT NOT NULL,
  silence_id TEXT NOT NULL,
  first_observed_at TEXT NOT NULL,
  last_observed_at TEXT NOT NULL,
  UNIQUE (cluster_id, silence_id)
);

CREATE INDEX idx_silence_rules_silence_id
  ON silence_rules (silence_id);
```

### 4.2 `silence_rule_episodes`

An episode is one continuous observed lifetime of a logical rule. `open_marker=1`
marks the single open episode; closed episodes use NULL. Default UNIQUE semantics
in SQLite/PostgreSQL/MySQL allow multiple NULL closed rows.

```sql
CREATE TABLE silence_rule_episodes (
  episode_id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id INTEGER NOT NULL,
  episode_number INTEGER NOT NULL,
  first_observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  ended_at TEXT,
  end_reason TEXT,
  open_marker INTEGER CONSTRAINT ck_silence_rule_episodes_open_marker CHECK (open_marker IS NULL OR open_marker = 1),
  UNIQUE (rule_id, episode_number),
  UNIQUE (rule_id, open_marker),
  UNIQUE (episode_id, rule_id),
  CONSTRAINT ck_silence_rule_episodes_lifecycle CHECK (
    (open_marker = 1 AND ended_at IS NULL AND end_reason IS NULL)
    OR (open_marker IS NULL AND ended_at IS NOT NULL AND end_reason = 'absent_confirmed')
  ),
  FOREIGN KEY (rule_id) REFERENCES silence_rules(rule_id) ON DELETE CASCADE
);

CREATE INDEX idx_silence_rule_episodes_rule
  ON silence_rule_episodes (rule_id, episode_number DESC);
CREATE INDEX idx_silence_rule_episodes_ended_at
  ON silence_rule_episodes (ended_at);
```

Episode closure requires complete successful absence on every relevant HA member.
Failure/partial/disagreement never closes it.

### 4.3 `silence_revisions`

Source-owned content is immutable. Only observation bookkeeping and karma-derived
ticket display fields may advance without changing source fingerprints.

```sql
CREATE TABLE silence_revisions (
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
  FOREIGN KEY (episode_id, rule_id)
    REFERENCES silence_rule_episodes(episode_id, rule_id) ON DELETE CASCADE
);

CREATE INDEX idx_silence_revisions_episode
  ON silence_revisions (episode_id, revision_number DESC);
CREATE INDEX idx_silence_revisions_content_fingerprint
  ON silence_revisions (content_fingerprint);
CREATE INDEX idx_silence_revisions_evaluation_fingerprint
  ON silence_revisions (evaluation_fingerprint);
CREATE INDEX idx_silence_revisions_last_observed
  ON silence_revisions (last_observed_at);
```

### 4.4 `alertmanager_source_silences`

This is the one mutable last-successful silence set per physical source. It is not
poll history.

```sql
CREATE TABLE alertmanager_source_silences (
  source_name TEXT NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_id INTEGER NOT NULL,
  observed_state TEXT NOT NULL CONSTRAINT ck_source_silences_observed_state CHECK (
    observed_state IN ('pending', 'active', 'expired')
  ),
  first_observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  PRIMARY KEY (source_name, rule_id),
  FOREIGN KEY (source_name)
    REFERENCES alertmanager_sources(source_name) ON DELETE CASCADE,
  FOREIGN KEY (rule_id)
    REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id)
    REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE INDEX idx_source_silences_revision
  ON alertmanager_source_silences (revision_id);
CREATE INDEX idx_source_silences_state
  ON alertmanager_source_silences (source_name, observed_state);
```

Successful reconciliation atomically diffs/replaces these rows and source
fingerprint/health. Failed/invalid refresh changes only source health and preserves
this relation set unchanged.

## 5. Persistent recheck queue and pinned input

### 5.1 `silence_recheck_jobs`

`open_context_fingerprint` enforces at most one equivalent non-completed job while
allowing the same context to enqueue again after a completed recovery cycle.

```sql
CREATE TABLE silence_recheck_jobs (
  job_id INTEGER PRIMARY KEY AUTOINCREMENT,
  context_fingerprint TEXT NOT NULL,
  open_context_fingerprint TEXT,
  source_context_json TEXT NOT NULL,
  trigger_type TEXT NOT NULL,
  status TEXT NOT NULL CONSTRAINT ck_silence_recheck_jobs_status CHECK (
    status IN ('queued', 'running', 'failed', 'completed')
  ),
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
  CONSTRAINT ck_silence_recheck_jobs_nonnegative CHECK (
    total_candidates >= 0
    AND processed_candidates >= 0
    AND promoted_alerts >= 0
    AND skipped_ineligible >= 0
    AND skipped_missing_labels >= 0
    AND skipped_incomplete_sources >= 0
    AND stale_conflicts >= 0
    AND retry_count >= 0
  ),
  CONSTRAINT ck_silence_recheck_jobs_open_context CHECK (
    (status = 'completed' AND open_context_fingerprint IS NULL)
    OR (status != 'completed' AND open_context_fingerprint = context_fingerprint)
  )
);

CREATE INDEX idx_silence_recheck_jobs_fifo
  ON silence_recheck_jobs (status, job_id);
CREATE INDEX idx_silence_recheck_jobs_context
  ON silence_recheck_jobs (context_fingerprint, job_id DESC);
CREATE INDEX idx_silence_recheck_jobs_finished
  ON silence_recheck_jobs (finished_at);
```

`source_context_json` is a canonical immutable manifest of source Name, cluster,
integrity status, set fingerprint, and HA-consistency state. It contains no DB rule
IDs. Rule/revision references live in the normalized child table.

Exact format:

```json
{
  "formatVersion": 1,
  "sources": [
    {
      "sourceName": "am-1",
      "clusterId": "production",
      "integrityStatus": "success",
      "confirmedSetFingerprint": "0123abcd...",
      "consistencyStatus": "consistent"
    },
    {
      "sourceName": "am-2",
      "clusterId": "production",
      "integrityStatus": "failed",
      "confirmedSetFingerprint": "4567efab...",
      "consistencyStatus": "unknown"
    }
  ]
}
```

`integrityStatus` is `unknown|success|failed|invalid|retired`; `consistencyStatus` is
`consistent|disagreement|unknown`. Nullable cluster/fingerprint values are encoded
as JSON null, never omitted. Sources sort by `(sourceName, clusterId)`.

### 5.2 `silence_recheck_job_rules`

One immutable full decision set copied only when a D19 job is enqueued:

```sql
CREATE TABLE silence_recheck_job_rules (
  job_id INTEGER NOT NULL,
  source_name TEXT NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_id INTEGER NOT NULL,
  observed_state TEXT NOT NULL CONSTRAINT ck_recheck_job_rules_observed_state CHECK (
    observed_state IN ('pending', 'active', 'expired')
  ),
  PRIMARY KEY (job_id, source_name, rule_id),
  FOREIGN KEY (job_id)
    REFERENCES silence_recheck_jobs(job_id) ON DELETE CASCADE,
  FOREIGN KEY (source_name)
    REFERENCES alertmanager_sources(source_name) ON DELETE RESTRICT,
  FOREIGN KEY (rule_id)
    REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id)
    REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE INDEX idx_recheck_job_rules_revision
  ON silence_recheck_job_rules (revision_id);
CREATE INDEX idx_recheck_job_rules_source
  ON silence_recheck_job_rules (job_id, source_name, observed_state);
```

The job never reads `alertmanager_source_silences` for matching after enqueue.

## 6. Suppression-cause history

### 6.1 `alert_suppression_cause_history`

One append-only parent represents a complete authoritative cause set. Content is
immutable; only `last_confirmed_at` may advance for the same set within the same
continuous live/suppression episode.

```sql
CREATE TABLE alert_suppression_cause_history (
  history_id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  transition_event_id INTEGER UNIQUE,
  recheck_job_id INTEGER,
  source TEXT NOT NULL CONSTRAINT ck_suppression_cause_source CHECK (
    source IN ('live_alertmanager', 'silence_recheck', 'legacy_unknown')
  ),
  cause_status TEXT NOT NULL CONSTRAINT ck_suppression_cause_status CHECK (
    cause_status IN ('known', 'partial', 'none', 'unknown')
  ),
  cause_fingerprint TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  last_confirmed_at TEXT NOT NULL,
  CONSTRAINT ck_suppression_cause_provenance CHECK (
    (source = 'silence_recheck' AND cause_status = 'known' AND transition_event_id IS NOT NULL AND recheck_job_id IS NOT NULL)
    OR (source = 'live_alertmanager' AND cause_status IN ('known', 'partial', 'none') AND transition_event_id IS NULL AND recheck_job_id IS NULL)
    OR (source = 'legacy_unknown' AND cause_status = 'unknown' AND transition_event_id IS NULL AND recheck_job_id IS NULL)
  ),
  FOREIGN KEY (alert_id) REFERENCES alert_states(alert_id) ON DELETE CASCADE,
  FOREIGN KEY (transition_event_id) REFERENCES alert_events(id) ON DELETE CASCADE,
  FOREIGN KEY (recheck_job_id) REFERENCES silence_recheck_jobs(job_id) ON DELETE RESTRICT
);

CREATE INDEX idx_alert_suppression_cause_latest
  ON alert_suppression_cause_history (alert_id, history_id DESC);
CREATE INDEX idx_alert_suppression_cause_fingerprint
  ON alert_suppression_cause_history (alert_id, cause_fingerprint);
CREATE INDEX idx_alert_suppression_cause_job
  ON alert_suppression_cause_history (recheck_job_id);
```

Application invariants requiring member-count queries:

- `known`: one or more members and all are resolved,
- `partial`: one or more members and at least one is unresolved,
- `none` / `unknown`: zero members,
- a fresh live reappearance appends a new parent even when the set fingerprint equals
  the previous non-live episode.

### 6.2 `alert_suppression_cause_members`

```sql
CREATE TABLE alert_suppression_cause_members (
  member_id INTEGER PRIMARY KEY AUTOINCREMENT,
  history_id INTEGER NOT NULL,
  source_name TEXT NOT NULL,
  rule_id INTEGER NOT NULL,
  revision_id INTEGER,
  resolution_status TEXT NOT NULL CONSTRAINT ck_suppression_member_resolution CHECK (
    resolution_status IN ('known', 'unresolved')
  ),
  UNIQUE (history_id, source_name, rule_id),
  CONSTRAINT ck_suppression_member_revision CHECK (
    (resolution_status = 'known' AND revision_id IS NOT NULL)
    OR (resolution_status = 'unresolved' AND revision_id IS NULL)
  ),
  FOREIGN KEY (history_id)
    REFERENCES alert_suppression_cause_history(history_id) ON DELETE CASCADE,
  FOREIGN KEY (source_name)
    REFERENCES alertmanager_sources(source_name) ON DELETE RESTRICT,
  FOREIGN KEY (rule_id)
    REFERENCES silence_rules(rule_id) ON DELETE RESTRICT,
  FOREIGN KEY (revision_id, rule_id)
    REFERENCES silence_revisions(revision_id, rule_id) ON DELETE RESTRICT
);

CREATE INDEX idx_alert_suppression_members_revision
  ON alert_suppression_cause_members (revision_id);
CREATE INDEX idx_alert_suppression_members_rule
  ON alert_suppression_cause_members (rule_id);
CREATE INDEX idx_alert_suppression_members_source
  ON alert_suppression_cause_members (source_name, history_id);
```

NULL revision preserves exact logical rule/source identity without creating a
synthetic revision. A later resolved observation appends a new parent/member set.

## 7. Fingerprint contracts

All Phase 2 fingerprints are lowercase SHA-256 over UTF-8 deterministic JSON. Each
canonical object includes `formatVersion:1`; canonicalization changes require a new
format version.

General rules:

- typed structures fix object field order,
- arrays sort by documented domain keys,
- duplicate matchers remain duplicates,
- timestamps use UTC RFC3339Nano,
- NULL, empty string, empty array, and absent value differ,
- surrogate DB IDs never participate.

`content_fingerprint` includes matchers sorted by
`(name,value,isRegex,isEqual)`, starts/ends, createdBy, comment, and annotations
sorted by key.

`evaluation_fingerprint` includes only matcher tuples and starts/ends.

Source-set fingerprint includes entries sorted by
`(cluster_id,silence_id,source_updated_at,content_fingerprint,observed_state)`.

Job context fingerprint includes source topology/integrity/HA state, active rule
evaluation fingerprints, and logical episode identity. It is indexed but not a
globally unique lifetime key; only equivalent non-completed jobs conflict.

Cause fingerprint includes `cause_status` and members sorted by
`(source_name,cluster_id,silence_id,resolution_status,revision_content_fingerprint)`.
Unresolved members use literal NULL revision fingerprint while retaining source,
cluster, and silence identity.

## 8. Poll and recheck transactions

### 8.1 Poll reconciliation

Network fetch occurs before the transaction. One short persistence transaction:

1. reconciles configured/retired `alertmanager_sources`,
2. records per-source health without clearing failed source membership,
3. registers logical rules, episodes, revisions,
4. atomically diffs successful `alertmanager_source_silences` and source fingerprint,
5. updates current alerts and all `is_suppressed` transition dimensions,
6. writes live cause parent/members from exact `SilencedBy` refs (D16 partial allowed),
7. writes alert events where tuple changed,
8. if D19 requires, inserts one job and copies the complete current decision set,
9. commits.

No cause/job row references an identity/revision inserted after it.

### 8.2 Recheck batch

Candidate query:

```sql
SELECT alert_id, last_seen_at, match_labels_json, relevant_sources_json
FROM alert_states
WHERE is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND state = 'inactive'
  AND match_labels_json IS NOT NULL
  AND (? IS NULL OR alert_id > ?)
ORDER BY alert_id
LIMIT 1000;
```

The nullable cursor is bound to both placeholders. Therefore the first batch has no
lower bound, while subsequent batches use keyset pagination.

Matching reads only immutable `silence_recheck_job_rules` and source manifest.

Promotion CAS:

```sql
UPDATE alert_states
SET
  state = 'suppressed',
  is_suppressed = 1,
  suppression_observed_at = ?,
  updated_at = ?
WHERE alert_id = ?
  AND last_seen_at = ?
  AND is_live = 0
  AND is_resolved = 0
  AND is_suppressed = 0
  AND state = 'inactive';
```

`RowsAffected=0` is stale/no-op. A successful CAS, event, cause parent/members, job
link, and counters commit in one transaction. Cursor advances only after every
candidate before/at that cursor has a committed terminal outcome.

## 9. Effective-cause read contract

Effective cause is the latest history row only while projected state is suppressed:

```sql
SELECT h.*
FROM alert_states a
JOIN alert_suppression_cause_history h
  ON h.history_id = (
    SELECT MAX(h2.history_id)
    FROM alert_suppression_cause_history h2
    WHERE h2.alert_id = a.alert_id
  )
WHERE a.alert_id = ?
  AND a.state = 'suppressed'
  AND a.is_suppressed = 1;
```

Members load by `history_id`. `none|unknown` intentionally have zero members;
`partial` exposes known silence IDs/source while marking revision details missing.

## 10. Retention and housekeeping

Existing order remains:

1. alert TTL cleanup plus unreferenced silence-history TTL cleanup,
2. max-alert cleanup,
3. reference/orphan/current-set cleanup,
4. incremental vacuum.

Phase 2 rules:

1. Alert deletion cascades cause history/members; event-linked cause also follows its event.
2. Current source relations, pinned jobs, and cause members protect revisions via FKs.
3. Open episodes/current source relations are never TTL-deleted.
4. Unreferenced closed revision/episode/rule history uses the D4 separate silence-history TTL.
5. Queued/running/failed jobs are retained.
6. Completed cause-referenced jobs are retained while referenced.
7. Keep the latest completed job for stats.
8. Delete older completed unreferenced jobs; pinned job rules cascade.
9. After a retired source reaches D4 TTL, first delete its unreferenced current-set
   rows, then eligible unreferenced rule history, and finally the source row; any job
   or cause FK keeps the chain retained.
10. No separate poll-run or job TTL exists.

Housekeeping performs deletions in FK-safe child-to-parent order and reports blocked
reference counts rather than disabling foreign keys.

## 11. Stats and warning fields

Stats add at least:

- source sync status/last attempt/last success/current set size,
- silence rule/episode/revision counts,
- open vs closed episodes,
- current source relation counts by observed state,
- cause-history/member/partial/unresolved counts,
- `missingMatchLabels`,
- HA disagreement count,
- current job/progress/queue depth/oldest pending,
- last error/retry/skipped counters,
- silence-history TTL.

Persistent warning is active for:

- failed FIFO predecessor or pending/running backlog,
- invalid relevant source state,
- unresolved HA disagreement,
- effective partial suppression cause,
- legacy rows with missing matcher labels.

An upstream transport failure remains visible through upstream/source health, keeps
last confirmed state under D3, and cannot clear an existing warning.

## 12. Migration v4 -> v5

Migration is application-controlled, foreign keys stay enabled, and all work is one
transaction:

1. require authoritative schema version exactly `4`,
2. preflight legacy preview tables; if populated `alerts_current` or
   `alert_state_history` data is not represented in canonical v4 tables, abort with
   an explicit historical v3-preview error before changing schema,
3. create temporary rebuilt `alert_states_v5` / `alert_events_v5` (events reference the temporary parent),
4. copy alerts while backfilling `is_suppressed`, deriving `suppression_observed_at` when possible, preserving `relevant_upstreams_json`, populating canonical `relevant_sources_json`, and setting legacy `match_labels_json=NULL`,
5. copy events while backfilling old/new suppression facts when recoverable,
6. drop old child `alert_events`, then old parent `alert_states`,
7. rename temporary parent/child to final names and create final indexes,
8. create the nine Phase 2 tables against final parent names,
9. populate source registry from configured/historical Names; unknown removed Names use NULL cluster and retired status,
10. add one `legacy_unknown` zero-member cause row for each currently suppressed legacy alert,
11. run `PRAGMA foreign_key_check` and abort if it returns any row,
12. update metadata schema version to `5` only as the final migration write,
13. commit; any failure rolls back to effective v4.

No automatic DB deletion/rebuild is allowed. Operators may explicitly recreate the
preview DB if immediate complete pre-transform label coverage is preferred.

## 13. Required tests

1. Fresh v5 bootstrap creates exactly the documented tables/indexes/FKs.
2. Real v4 fixture upgrades atomically and preserves alert/event IDs.
3. Injected migration failure leaves schema/data/version at v4.
4. Projection CHECK rejects invalid raw/live/suppressed/resolved combinations.
5. Legacy transformed labels never populate `match_labels_json`.
6. Pre-transform labels survive keep/strip and populate every new live alert.
7. Configured/retired/renamed/unknown source migration follows D5.
8. Exactly one open episode per logical rule; disappearance/reappearance creates a new episode.
9. Current source set changes only after successful refresh; failure preserves it.
10. Revision/current/job/cause FKs prevent referenced history deletion.
11. Equivalent non-completed job conflicts; same context may enqueue after completion/recovery.
12. Job uses pinned rules after mutable current set changes and resumes from cursor.
13. D19 promotion-capable changes enqueue; reducing/display-only changes do not.
14. Semantic CAS rejects stale `last_seen_at` after live reappearance.
15. One/many/missing `SilencedBy` values create known/partial/none sets without synthetic payload.
16. Unchanged live cause set advances confirmation; fresh reappearance appends a new row.
17. Live suppressed -> non-live preserves `is_suppressed`; fresh active reappearance clears it.
18. Promotion atomically writes state/event/cause/job references and all members.
19. Completed-job reference retention and cleanup follow D21.
20. Housekeeping preserves open/current/referenced rows and required ordering.
21. NULL initial job cursor returns the first candidate batch; later cursors paginate without duplicates.
22. Populated legacy v3 preview tables cause explicit preflight failure rather than silent empty migration.
