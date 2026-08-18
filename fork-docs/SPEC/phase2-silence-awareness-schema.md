# Silence Awareness: Schema v1 Contract

## 1. Authority

Status: fresh v1 schema contract; Problem 2 is implemented and independently accepted.

The literal executable DDL has one source of truth:
`internal/fork/alertlifecycle/store.go` (`bootstrapSchema`). This document describes
the complete table ownership, keys, relationships, and algorithms. It intentionally
does not copy the full SQL because a second DDL copy had already drifted from the
runtime.

The preview project supports only a fresh v1 database or reopening the same v1
database. It has no migration or backfill subsystem. A non-empty unrecognized
database or any schema version other than `1` is rejected without modification.

## 2. Complete table inventory

Schema v1 contains 23 application tables.

### 2.1 Global lifecycle

| Table | Identity | Ownership |
|---|---|---|
| `alert_states` | `alert_id` | Current global dimensions, display/group data, original match labels, source membership, immutable last-observed source links, resolve metadata |
| `alert_events` | auto ID | Append-only old/new global dimensions and transition reason |
| `metadata` | key | Schema version, housekeeping timestamps, grouping/regroup progress |

`alert_states.state` is materialized from `raw_state`, `is_live`, `is_suppressed`,
and `is_resolved`. `match_labels_json` contains original pre-transform labels;
`labels_json` contains display labels. `alert_id` remains the persisted source
identity even when display-label transforms produce a different hash.
`source_links_json` retains source name, logical target, and the alert's original
Prometheus URL so a non-live alert can still navigate to its last observed source.
It intentionally contains no mutable silence or inhibition state.

### 2.2 Per-target lifecycle and autocomplete

| Table | Identity | Ownership |
|---|---|---|
| `alert_target_states` | `(alert_id, target)` | Current target-specific dimensions, member coverage, label fingerprint, retirement |
| `alert_target_events` | auto ID | Append-only target transitions and retirement changes |
| `alert_target_match_labels` | `(alert_id, target, label_name)` | Current eligible raw-label index for autocomplete |

Per-target rows preserve divergent liveness, suppression, start time, and topology
when one global alert fingerprint appears in multiple logical targets. A retired
target keeps its last confirmed dimensions but is excluded from preview,
autocomplete, and recheck. The label index contains only current eligible,
non-retired rows and is refreshed only when `match_labels_fingerprint` changes.

### 2.3 Alertmanager silence source and immutable history

| Table | Identity | Ownership |
|---|---|---|
| `alertmanager_sources` | `source_name` | Physical source identity, target, sync health, last successful fingerprint, retirement |
| `alertmanager_cluster_consistency` | `cluster_id` | Last-confirmed HA consistency state and timestamps |
| `silence_rules` | `(cluster_id, silence_id)` | Logical rule identity independent from payload |
| `silence_rule_episodes` | `(rule_id, episode_number)` | Continuous observed lifetime; one open episode per rule |
| `silence_revisions` | `(episode_id, revision_number)` | Immutable normalized source content and evaluation fingerprints |
| `alertmanager_source_silences` | `(source_name, rule_id)` | One mutable last-successful rule/revision/status relation set per source |

Failed or invalid source refreshes update health but never replace the last
confirmed relation set. An episode closes as `absent_confirmed` only after every
configured, non-retired HA member successfully confirms absence. Reappearance starts
a new episode.

`silence_revisions` stores canonical matchers, declared window, nullable source
`updatedAt`, Author, Comment, annotations, ticket display metadata, and observation
times. A source-content or non-null `updatedAt` change creates the next revision;
status-only or repeated observations do not.

### 2.4 Persistent recheck input and progress

Problem 2 implements one atomic immutable job-creation boundary. Its production
remediation closed exact duplicate integrity, cluster-qualified source completeness,
timestamp validation, post-CAS error propagation, and per-candidate accounting, but
Problem 2 is independently accepted.
The accepted
creator contract stores the complete source context, exact rule revisions, and every
global/target candidate input after poll reconciliation has finished. No worker, alert
upsert, or later transition may insert a candidate into an existing job.

| Table | Identity | Ownership |
|---|---|---|
| `silence_recheck_jobs` | `job_id` | FIFO context, source/job fingerprints, trigger, status, counters, timing, preserved errors |
| `silence_recheck_job_rules` | `(job_id, source_name, target_key, rule_id)` | Immutable successful-source, target-qualified revision, stable silence identity/content fingerprint, and matcher set |
| `silence_recheck_job_candidates` | `(job_id, alert_id, target_key)` | Immutable observation token, labels, source membership, and nullable terminal outcome |
| `silence_recheck_job_attempts` | `attempt_id` | Reserved append-only attempt history for Problem 3 |
| `silence_recheck_job_operator_actions` | `action_id` | Reserved idempotent operator-action history for Problem 3 |

`source_context_fingerprint` identifies the canonical pinned source status/topology and
`job_fingerprint` is the unique SHA-256 identity of the complete source, rule, and
candidate input. The job fingerprint excludes creation time, database IDs, map order,
and batch order. Rule identity uses stable silence identity plus content/evaluation
fingerprints and evaluation fields; local `rule_id`/`revision_id` values remain only
foreign-key/evidence linkage. Same-context eligibility creates a new job fingerprint
and FIFO successor; it never appends to an older job.

Each candidate pins `expected_alert_last_seen_at`, nullable
`expected_target_last_seen_at`, exact raw `match_labels_json`, relevant source
references, and target source members. Candidate rows have no foreign key to mutable
alert or target current-state tables, so alert TTL/max-alert eviction cannot remove
open-job input. A missing current row is a terminal `missing_current_alert` or
`missing_current_target` outcome.

The worker loads unprocessed rows by immutable membership only, evaluates pinned input,
and writes exactly one constrained terminal outcome plus one parent counter in the same
batch transaction. `cursor_alert_id` is only a diagnostic marker; `processed_at` and
`outcome` own resume correctness.

Jobs are strict FIFO. A failed predecessor keeps its first error and blocks later jobs.
Crash/restart resumes the same pinned rules, observations, and remaining candidates.
Retry states, attempt history, scheduler ownership, and operator actions are reserved
by the combined schema and remain Problem 3 behavior.

### 2.5 Global suppression evidence

| Table | Identity | Ownership |
|---|---|---|
| `alert_suppression_cause_history` | `history_id` | Append-only complete global cause set |
| `alert_suppression_cause_members` | member ID; unique cause/source/rule | Exact source/rule/revision member or unresolved member |

A live Alertmanager cause may be `known`, `partial`, or `none`. A recheck promotion
is always `known` and links its transition event and job. Missing live payloads keep
the exact silence identity with a null revision; no synthetic payload is created.

The merge reader selects the latest effective global cause parent (direct or target
aggregate) for each non-live suppressed alert and returns exact identity plus nullable
revision details in one batched query. Older histories remain stored but are not
mixed into the current card explanation.

### 2.6 Target and aggregate suppression evidence

| Table | Identity | Ownership |
|---|---|---|
| `alert_target_suppression_cause_history` | `history_id` | Append-only complete cause set for one target |
| `alert_target_suppression_cause_members` | member ID; unique cause/source/rule | Exact target cause members |
| `alert_target_aggregate_suppression_history` | `aggregate_id` | One global transition produced from effective target promotions |
| `alert_target_aggregate_suppression_links` | `(aggregate_id, target_history_id)` | Links global aggregate evidence to immutable target histories |

Aggregate evidence links target cause histories instead of copying or re-matching
rules. Link deletion cascades so retained aggregate rows cannot block legitimate
alert/evidence housekeeping.

## 3. Fingerprints

All silence-aware fingerprints are lowercase SHA-256 over deterministic typed JSON
with an explicit format version.

- Content fingerprint: sorted matcher tuples, starts/ends, Author, Comment,
  annotations.
- Evaluation fingerprint: only matcher tuples and starts/ends.
- Source-set fingerprint: source rule identity, revision/evaluation identity, and
  observed status.
- Job context: semantic source-context generation, source topology/status/HA
  consistency, and active evaluation identity. Local observation timestamps do not
  participate.
- Cause fingerprint: status plus sorted source/rule/revision member set.

Database surrogate IDs, local observation time, array insertion order, and display
label transformations cannot change semantic fingerprints.

## 4. Poll transaction

Network reads complete before SQL begins. One serialized transaction:

1. reconciles configured, failed, recovered, and retired sources;
2. validates the successful per-source rule sets and reconciles HA consistency;
3. registers rule identities, episodes, revisions, and current source relations;
4. merges the current global and target alert observations;
5. reconciles target topology/retirement and target label indexes;
6. writes global/target current dimensions and append-only events;
7. writes exact live suppression causes;
8. after all state reconciliation, creates one full source-context job or one targeted
   eligibility successor with pinned rules/candidates;
9. commits atomically.

Failed persistence rolls back the complete observation. Runtime transition logs are
emitted only after commit.

## 5. Recheck transaction

Each worker invocation selects the oldest queued/running/failed job. A failed job is
reported and blocks the queue. For a runnable job, one batch:

1. loads immutable pinned source status, rules, and candidate input;
2. loads unprocessed candidate rows by job membership only;
3. compares current global/target state with pinned observation tokens;
4. evaluates Alertmanager matcher semantics using pinned original labels;
5. applies target/global conditional updates using expected `last_seen_at` and state
   predicates;
6. writes events and exact target/global evidence;
7. marks each candidate with one terminal outcome and updates counters;
8. commits.

`RowsAffected=0` is re-read in the same transaction. A successfully observed stale or
ineligible state receives its exact terminal outcome; a re-read failure aborts the
transaction, and an unexplained zero-row result is an invariant error. A live
reappearance, newer lifecycle episode, or manual Resolve cannot be overwritten by an
older job.

## 6. Retention and foreign keys

Housekeeping order is:

1. alert TTL cleanup;
2. max-alert eviction if still over the configured cap;
3. reference-safe cleanup of old unreferenced completed jobs, retired sources,
   revisions, episodes, and rules using the independent silence-history TTL;
4. commit;
5. incremental vacuum.

Alert deletion cascades its events and cause histories. Current source relations,
non-completed jobs, the latest completed job, and any job/evidence references protect
the required source/rule/revision graph. Housekeeping never disables foreign keys.

## 7. Required schema verification

- Fresh bootstrap creates all 23 documented tables and metadata version `1`.
- Reopening v1 preserves rows, indexes, and foreign-key behavior.
- Other/non-empty unknown schemas are rejected without modification.
- Poll, recheck, Resolve, retention, topology retirement/reappearance, HA
  disagreement/convergence, and restart sequences exercise the runtime DDL rather
  than a copied test schema.
