# ADR-006: Immutable Recheck Job Input

- Status: Accepted
- Date: 2026-08-17

Implementation status: Implemented. Problem 2 owns immutable job input; Problem 3 adds the
ADR-007 retry, attempt, scheduler, and operator-recovery behavior on the same schema-v1
objects.

## Context

Before Problem 2, the recheck queue did not have one atomic creation boundary. A job could
be created from a source context and receive candidates later from the worker, an alert
transition, or `UpsertCurrentAlert`. Candidate rows contained only an alert ID, while the
worker read labels, source membership, and observation timestamps from mutable current
rows. This allowed a job to evaluate a later lifecycle episode with the wrong historical
input. Mutable alert retention could also delete candidate rows through a cascading
foreign key.

This decision requires a complete immutable input snapshot. Problem 3 uses that same
snapshot for retries and operator actions, so the final fresh schema-v1 shape is defined
here for both implementations. This preview project has no
deployed lifecycle databases and has no migration or backfill subsystem.

## Decision

### Job identity and provenance

`silence_recheck_jobs` uses these independent concepts:

- `source_context_fingerprint`: the canonical identity of the source status, topology,
  HA consistency, active evaluation, and episode context. A persisted semantic context
  generation distinguishes a later recovery/reassignment context from an earlier
identical rule set without using wall-clock time;
- `job_fingerprint`: a unique SHA-256 identity of the complete immutable job input,
  including source context, trigger, rules, candidates, and candidate observation data;
- `trigger_type`: constrained to `source_context_changed` or
  `alert_became_eligible`.

The generation is stored in the existing `metadata` table under
`recheck_source_context_generation` and increments once per promotion-capable source
context change inside the poll transaction.

`job_fingerprint` is deterministic typed JSON with an explicit format version. It
excludes creation time, database IDs, map iteration order, and batch order. The canonical
input sorts all rules, candidates, targets, source references, source members, silence
IDs, and matcher collections. Source-context fingerprints include the semantic
generation, source/HA status, and member counts, but exclude local observation
timestamps. A duplicate creator request returns the existing job only
after comparing the complete canonical parent, rule, and candidate input.

The old `open_context_fingerprint` uniqueness rule is removed. Multiple open jobs with
the same source context are valid when their trigger or candidate observation differs.

### Immutable rules and candidates

`silence_recheck_job_rules` remains immutable and stores the exact successful-source,
target-qualified silence revision input used by evaluation.

`silence_recheck_job_candidates` has logical identity `(job_id, alert_id, target_key)`.
An empty `target_key` means a global candidate without a non-retired target row. The
candidate stores:

- `expected_alert_last_seen_at`;
- nullable `expected_target_last_seen_at`;
- exact `match_labels_json`;
- exact relevant-source snapshot;
- nullable exact target-source-member snapshot;
- nullable `processed_at`; and
- constrained nullable terminal `outcome`.

Candidate rows have no foreign keys to mutable global or target current-state tables.
They survive alert TTL and max-alert eviction. They may reference the immutable job
parent and, where required by existing history retention, the pinned silence revisions.

The candidate outcome values are exactly:

`promoted`, `stale_alert_observation`, `stale_target_observation`,
`missing_observation_token`, `ineligible_state`, `missing_current_alert`,
`missing_current_target`, `missing_labels`, `invalid_labels`, `no_matching_rule`,
`incomplete_source_coverage`, `retired_target`, `already_suppressed`, and
`already_resolved`.

The parent counters are `promoted_candidates`, `stale_conflicts`,
`skipped_ineligible`, `skipped_missing_labels`, and `skipped_incomplete_sources`.
Every terminal outcome maps to exactly one counter, and:

```text
processed_candidates = promoted_candidates
                     + stale_conflicts
                     + skipped_ineligible
                     + skipped_missing_labels
                     + skipped_incomplete_sources
```

### Combined Problem 3 schema

The same fresh `silence_recheck_jobs` table also reserves the Problem 3 fields:

- explicit status `queued`, `running`, `retry_wait`, `failed`, `completed`, or
  `abandoned`;
- `attempt_count`, total `retry_count`, and `consecutive_retry_count`;
- nullable `next_attempt_at`;
- immutable first-error message, class, stage, and timestamp;
- mutable last-error message, class, stage, and timestamp;
- `recovery_count`; and
- nullable `abandoned_at`, `abandoned_by`, and `abandon_reason`.

Timestamp checks require `retry_wait` to have `next_attempt_at` and no `finished_at`,
terminal states to have `finished_at` and no `next_attempt_at`, and `queued`/`running`
to have neither. `abandoned_at` is non-null only for `abandoned` and equals
`finished_at`.

`silence_recheck_job_attempts` is append-only. Each row has a unique opaque
`attempt_token`, job ID, per-job attempt number, start/end timestamps, constrained
outcome (`committed`, `retry_scheduled`, or `failed`), failure class/stage, and nullable
scheduled retry time. `attempt_count` equals the number of attempt rows for the job.
Pre-selection acquisition failures and shutdown cancellation create no attempt row.

`silence_recheck_job_operator_actions` is append-only with job ID, opaque request ID,
constrained action (`retry` or `abandon`), nullable actor, reason, request/applied
timestamps, applied result, resulting status, canonical request fingerprint, and
`UNIQUE(job_id, request_id)`. The fingerprint is versioned canonical input exactly
`{action, normalizedReason, normalizedActor}`.

Problem 2 created the final job/rule/candidate shape and left the reserved Problem 3
columns at their initial values. ADR-007 now owns the runtime retry states, attempt
writes, scheduler behavior, and operator routes without changing this immutable input.

### Ownership and mutation boundary

One transactional creator is the only production function allowed to insert a job
parent, job rules, or job candidates. It uses a reliable `RETURNING`-style identity
operation and never uses `LastInsertId()` for conflict detection. A duplicate request
must not append missing child rows.

`ApplyPollCycle` is the only production authority that collects eligibility transitions
and invokes the creator. Source-context jobs are created only after all source, global,
and target reconciliation in the poll transaction is complete. If no source context
changed, newly eligible alerts and targets are coalesced into one targeted successor.
An alert or target becoming eligible never appends to an older job.

After creation, the worker may update only terminal candidate outcome fields, progress,
and the corresponding parent counters. It loads candidates by immutable job membership
and terminal outcome, never by current eligibility. It evaluates pinned labels and
pinned source membership, compares pinned observation tokens to current semantic state,
and applies the existing CAS predicates.

### Fresh schema and no migrations

The literal DDL remains in `internal/fork/alertlifecycle/store.go`. The schema version
stays `v1`; the fresh bootstrap is edited directly and the schema-layout fingerprint is
recomputed. No `ALTER TABLE`, migration, backfill, legacy column fallback, or dual-read
path is allowed.

## Consequences

- A recheck job is a reproducible historical input snapshot rather than a mutable query.
- Same-context eligibility creates a FIFO successor instead of changing an older job.
- Alert retention cannot erase an open job's candidate input.
- Restart resumes the same candidate decisions and cannot silently evaluate a newer
  lifecycle episode.
- Problem 3 adds retry and operator recovery without changing the identity or input
  semantics established here.
- Late candidate attachment and mutable correctness-input reads were removed, and the
  Problem 2 verification matrix covers the resulting production invariants.
  Problem 3 runtime behavior is specified by ADR-007, not by this input-identity ADR.

## Verification obligations

Problem 2 must provide deterministic evidence for immutable membership, successor jobs,
canonical input ordering, pinned global and target observations, missing current rows,
terminal outcome accounting, trigger provenance, idempotent duplicate creation,
restart-resume behavior, absence of post-creation candidate inserts, schema constraints,
and fresh-layout fingerprint validation. Problem 3 owns the additional retry, attempt,
scheduler, and operator-action evidence.
