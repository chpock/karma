# Phase 2 Planning: Silence-Aware Lifecycle

## 1. Purpose of this document

This document records the requirements, findings, decisions, and rationale that
produced the implemented silence-aware lifecycle.

The original planning order was:

1. collect requirements and target outcomes,
2. derive required data inputs,
3. design DB schema/storage contracts.

The document contains reviewed requirements, data inputs, and decided architecture
contracts. Exact SQLite DDL lives in `phase2-silence-awareness-schema.md`. The
feature is implemented in `internal/fork/alertlifecycle`; this document remains the
design rationale record.

## 2. Why Phase 2 is needed

Primary goal of Phase 2 (original requirement):

- when silence rules change, the system must re-check stored `inactive` alerts and decide whether some of them should now become `suppressed`.

Before Phase 2, newly created/updated silences could not affect already-stored
inactive alerts. The implemented recheck worker now provides that behavior under the
reliability rules below.

Secondary goal:

- preserve enough silence context for non-live suppressed alerts so operators can understand *why* state is `suppressed`.

## 3. Target outcomes (what we want to achieve)

### 3.1 User-facing outcomes

1. If a new silence rule appears and matches inactive alerts, those alerts are reclassified to suppressed without manual intervention.
2. Operators can distinguish suppressed current/past consistently.
3. Non-live suppressed alerts remain explainable (silence context is visible and not ambiguous).

### 3.2 Domain outcomes

1. Extend the earlier tuple with normalized `is_suppressed` without changing the
   ownership of `raw_state`, `is_live`, or `is_resolved`.
2. Silence-aware logic is additive and deterministic.
3. No state inference when upstream silence data is incomplete/unreliable.

### 3.3 Engineering outcomes

1. Minimize future rework by defining data contracts before schema changes.
2. Keep fork isolation pattern: core logic in fork modules, minimal upstream hooks.
3. Keep transitions auditable and testable with clear acceptance criteria.

## 4. Functional requirements (Phase 2)

### R1. Trigger for re-check

- Reclassification pass is triggered when silence set changes are observed (create/update/expire/delete after successful refresh).
- The trigger must be deterministic and repeatable across poll cycles.

### R2. Reclassification scope

- A new silence match may promote only `inactive -> suppressed`.
- Once promoted, the alert remains suppressed for the rest of that non-live episode even if the matching silence is later changed, deleted, or expired.
- Silence-derived suppression is recalculated only when the alert becomes live again.
- It must not mutate `resolved` alerts.
- It must not redefine Phase 1 state projection rules.

### R3. Decision reliability gate

- Reclassification is allowed only when silence data is fresh and complete for all relevant upstreams required by the decision.
- On partial silence refresh failure, no silence-driven inference is allowed.

### R4. Matching model

- The system must evaluate inactive alerts against current silence matchers using the same label semantics as Alertmanager silence matching.
- The result must be reproducible from stored data + silence snapshot.

### R5. Explainability contract

- For every alert reclassified to suppressed by Phase 2, API/UI must expose enough silence context to explain the decision.
- This applies especially to `suppressed + is_live=false` alerts.
- Practical UI requirement: suppressed non-live alert cards must show silence-rule context (matcher/source/reference) instead of an empty silence section.
- This is part of Phase 2 scope, not a separate future phase.

### R6. Provenance and audit

- Every silence-driven mutation writes explicit reason metadata in `alert_events`.
- Operator/audit view must distinguish poll-driven transitions from silence-driven transitions.

### R7. Non-destructive compatibility

- Phase 2 must preserve all Phase 1 behavior where silence evidence is absent.
- Existing filters (`@state`, `@is_live`) remain valid and stable.

### R8. Safe degradation

- If silence-aware subsystem is unhealthy, app must not fabricate transitions.
- Inconsistency/error state must be visible to operators via persistent warning mechanism.

## 5. Non-goals (explicitly out of this phase)

1. Multi-replica/distributed write model.
2. Full historical silence timeline explorer UI.
3. Identity/dedup redesign.
4. Changes to manual resolve policy.

## 6. Acceptance criteria for requirement stage

Requirements stage is complete when:

1. We agree on exact operator outcomes for current/past suppressed cards.
2. We agree on reliability gates (when silence data is considered usable).
3. We agree on mutation boundaries (`inactive -> suppressed` only).
4. We have an approved list of data fields needed to support those requirements.

After that, this document proceeds to sections:

- "Required data inputs" (step 2)
- "Storage and schema design" (step 3)

## 7. Required data inputs (step 2)

This section defines what data must exist for Phase 2 behavior.
It intentionally does not define table layout yet.

### 7.1 Silence snapshot data (what rules exist now)

For each silence rule we need to track:

1. **Identity and source**
   - silence id
   - upstream/cluster identity
   - fetch timestamp / snapshot version marker

2. **Matcher definition**
   - full matcher list (`name`, `value`, `isRegex`, `isEqual`)
   - normalized matcher form used by evaluation engine

3. **Lifecycle window**
   - `startsAt`
   - `endsAt`
   - whether silence is active at evaluation time

4. **Operator metadata for explainability**
   - `createdBy`
   - `comment`
   - ticket fields (`ticketID`, `ticketURL`) if present

Why this is required:

- reclassification decisions must be reproducible,
- non-live suppressed cards must show silence context.

### 7.2 Silence refresh integrity data (can we trust this cycle)

For each evaluation cycle we need:

1. upstream-level silence refresh success/failure map
2. refresh timestamp per upstream
3. global marker that data is complete enough for decisions

Why this is required:

- Phase 2 must follow "no inference under uncertainty".
- If any relevant silence source is stale/failed, reclassification must be skipped.

### 7.3 Alert candidate data (which inactive alerts can be rechecked)

For each inactive alert candidate we need:

1. alert identity (`alert_id`)
2. labels used for silence matching (already in lifecycle store)
3. relevant upstream set for this alert
4. current dimensions (`state`, `raw_state`, `is_live`, `is_suppressed`, `is_resolved`)

Why this is required:

- matching runs on labels,
- reliability gate runs on relevant upstreams,
- mutation guard requires current tuple state.

### 7.4 Reclassification evidence data (why this alert changed)

When `inactive -> suppressed` is applied, we need to store:

1. matched silence references (id + upstream)
2. evaluation timestamp
3. reason metadata (stable reason code + optional reason text)

Why this is required:

- event log must distinguish silence-driven transition from poller transition,
- API/UI explainability for suppressed non-live cards.

### 7.5 Explainability payload data for API/UI

For non-live suppressed alerts we need a stable payload containing:

1. matched silence id(s)
2. source upstream/cluster of each silence
3. minimal display metadata (`createdBy`, comment/ticket fields)
4. evidence freshness markers (when silence evidence was last confirmed)

Why this is required:

- card must not show an empty silence section,
- operator must understand if context is current snapshot evidence vs historical evidence.

### 7.6 Change detection data (when to run re-check)

To avoid noisy/full rescans every cycle we need a way to detect silence set change:

1. silence-set fingerprint per upstream/cluster
2. last processed fingerprint for Phase 2 evaluator

Why this is required:

- recheck should run when silence set changed,
- repeated no-change cycles should be cheap and deterministic.

### 7.7 Observability and failure-state data

Phase 2 must expose:

1. last evaluation run status (`ready/running/failed/skipped`)
2. counters (`candidates`, `matched`, `reclassified`, `skipped_due_to_incomplete_data`)
3. last error details
4. expected-state flag for persistent warning endpoint

Why this is required:

- operators must see when phase-2 engine is unhealthy,
- warning behavior must match existing persistent warning contract.

## 8. Data stage acceptance checklist

Step 2 is complete when we agree on:

1. exact silence fields required for matching vs for UI explainability,
2. completeness criteria for silence refresh gate,
3. evidence payload shape for `inactive -> suppressed` events,
4. change-detection strategy (fingerprint scope),
5. observability fields required for warning/stats endpoints.

After checklist approval, we move to step 3 (DB schema and indexes).

## 9. Storage and schema design history

The first schema draft was rejected by review. The findings below explain why the
implemented schema differs from that draft; they are historical rationale, not the
current runtime contract.

## 10. Review findings

### F1. Phase 1 tuple cannot represent silence-derived suppression (critical)

Current projection uses only:

- `raw_state`
- `is_live`
- `is_resolved`

An inactive alert has `raw_state=active|unprocessed`, `is_live=false`, `is_resolved=false`.
Changing only projected `state` to `suppressed` violates the projection invariant; changing `raw_state` would incorrectly overwrite upstream-owned state.

Required correction:

- add normalized suppression dimension `is_suppressed` without changing `raw_state` ownership.

Projection extension:

1. if `is_live=true`, project from `raw_state` exactly as Phase 1,
2. else if `is_resolved=true`, project `resolved`,
3. else if `is_suppressed=true`, project `suppressed`,
4. else project `inactive`.

While live, `is_suppressed` must equal whether `raw_state=suppressed`. While non-live, it preserves suppression through the lifecycle episode or can be set by Phase 2. It is intentionally separate from whether a matching silence is currently active.

### F2. Stored labels are not sufficient for Alertmanager-compatible matching (critical)

Current lifecycle `labels_json` is produced after karma label keep/strip transforms.
Silence matchers operate on original Alertmanager alert labels.

Required correction:

- persist an unmodified `match_labels_json` snapshot captured before UI transforms,
- never evaluate silence matchers against display/group labels.

The exact label set that produced the stored alert identity must be used. If future identity logic allows one alert identity to carry different labels per upstream, matching labels must be normalized into a per-upstream child table rather than another JSON column rewrite.

The capture point and missing-label behavior are decided in D9.

### F3. Mutable current snapshot + immutable revisions creates two sources of truth (high)

The rejected draft duplicated silence payload in both a mutable snapshot table and a revision table.
They could diverge and require complex transactional repair.

Required correction:

- immutable revisions own payload,
- mutable current-source and immutable job-input tables contain only references to revisions; they never duplicate payload.

### F4. Per-upstream replacement is not a stable input for background evaluation (high)

A recheck can span multiple batches while the next poll updates current silence data.
Without pinned job rules, different batches may evaluate against different rule sets.

Required correction:

- each evaluator job pins one immutable full decision set in `silence_recheck_job_rules`,
- candidate writes are conditional on the alert tuple still matching the evaluated version.

### F5. Silence identity must distinguish logical cluster and physical upstream (high)

- silence rule identity is cluster-scoped: `(cluster_id, silence_id)`,
- refresh completeness is upstream-instance-scoped: `upstream_id`,
- HA members in one cluster may report the same logical silence.

Conflating these identities causes duplicates or cross-upstream collisions.

### F6. Rule status is temporal, not an immutable revision property (high)

`pending -> active -> expired` can happen only because time crosses `starts_at`/`ends_at`; payload may not change.
Therefore `is_active` must not be stored as immutable revision content.
Effective state belongs to a successful sync/evaluation observation.

### F7. Exact upstream timestamps are currently unavailable (medium)

Current mapper provides `startsAt`, `endsAt`, `createdBy`, `comment`, and matchers.
It does not populate `models.Silence.CreatedAt` and does not expose source `updatedAt`.

Required correction:

- `first_observed_at` is always available,
- source creation/update timestamps must be nullable unless mapper support is explicitly added,
- never present observation time as the exact upstream edit time.

### F8. Evidence must be historical, not deleted when current state changes (high)

Deleting evidence when an alert leaves suppressed state destroys retrospective context.
Evidence must be an append-only history of authoritative suppression-cause sets with immutable revision references. Later rule changes must not rewrite the cause that originally established suppression.

### F9. Multiple matching silences are valid (high)

One alert can match multiple silences and the same logical silence can be observed from multiple HA upstreams.
One cause-history record must preserve the complete simultaneous set; individual silence-member insertion order cannot define the effective cause.

### F10. Rule-content and evaluation fingerprints have different scopes (medium)

- content fingerprint detects any revision change (including comment/ticket metadata),
- evaluation fingerprint includes only matching/effective fields and controls alert recheck.

Matcher arrays must be canonically sorted before hashing; API ordering is not semantic.

### F11. Intermediate revisions between polls cannot be recovered (documented limitation)

Polling can only preserve revisions it observes.
If a rule changes multiple times between successful polls, intermediate revisions are unknowable without an Alertmanager event stream.

### F12. HA members can temporarily disagree (high)

Two upstream instances in one Alertmanager cluster can briefly report different revisions of the same silence.
Without a source update timestamp or consensus rule, choosing one revision would be arbitrary.

Required correction:

- preserve observations per physical upstream,
- detect disagreement at logical cluster level,
- aggregate per-source outcomes under D2 and surface disagreement until a complete snapshot converges.

## 11. Corrected domain model direction

### 11.1 Alert state extension (implemented)

Implemented additions to `alert_states`:

1. `is_suppressed INTEGER NOT NULL DEFAULT 0`
2. `match_labels_json TEXT` (nullable only when original labels are unavailable)
3. `relevant_sources_json TEXT NOT NULL` (upstream name + cluster identity captured with the alert)
4. `suppression_observed_at TEXT` (nullable local time when current suppression episode was first observed/established)

No generic row-version/trigger column is used; D14 defines semantic CAS using existing lifecycle fields.

Implemented additions to `alert_events`:

1. `old_is_suppressed INTEGER` (nullable)
2. `new_is_suppressed INTEGER NOT NULL`
3. `change_source='silence'`
4. reason code for silence-driven promotion (`silence_match_promoted`)

No event is written for no-match or skipped candidates.
When a silence later changes/expires for a non-live suppressed alert, neither state nor its historical causal record is rewritten and no state-transition event is written.

### 11.2 Live alert evidence

Phase 2 must capture exact `SilencedBy` references while an alert is live, not only after it becomes inactive.
Every authoritative change to the live `SilencedBy` cause set appends a history record. The latest record survives the later `is_live=false` transition and is effective while that alert remains suppressed.

`raw_state=suppressed` without `SilencedBy` can represent inhibition; the UI must not invent a silence rule in that case.

### 11.3 Required poll integration DTO

The lifecycle `LiveAlertSnapshot` contract includes:

```text
MatchLabelsJSON string
RelevantSources []{Name, Cluster}
SilenceCauseRefs []{SourceName, ClusterID, SilenceID}
```

Integration rules:

- `MatchLabelsJSON` comes from the D9 pre-transform raw-label index keyed by the existing `LabelsFingerprint`; it never falls back to transformed display labels,
- `RelevantSources` is added as canonical source identity; `RelevantUpstreams` remains for compatibility,
- `SilenceCauseRefs` comes from each deduped `AlertmanagerInstance.SilencedBy`, preserving physical source and logical cluster,
- full revision payloads are resolved through the same pinned silence sync snapshot; missing payload produces D16 unresolved cause member,
- source/silence snapshot rows are persisted before alert cause-history rows in the same poll transaction,
- live alert update remains valid even when one cause reference is unresolved, while Phase 2 promotion requires complete revisions,
- transition code writes `is_suppressed` on every insert/update/disappearance/reappearance and must satisfy the schema projection CHECK.

Required integration tests:

- a label removed by `labels.strip` is still present in `MatchLabelsJSON`,
- same fingerprint with different canonical raw labels produces integrity warning and no Phase 2 candidate,
- one/many/missing live `SilencedBy` payloads create known/partial cause sets correctly,
- live suppressed -> non-live preserves `is_suppressed=true`,
- fresh live active/unprocessed reappearance clears `is_suppressed`,
- all live and non-live update paths satisfy the projection CHECK.

## 12. Reviewed normalized schema

Exact DDL, constraints, indexes, queries, and cleanup rules are specified in
`phase2-silence-awareness-schema.md`. This section describes table ownership and
domain intent.

### 12.1 `alertmanager_sources`

One mutable row per stable upstream Name. It owns configured/retired identity,
cluster, last attempt/success/confirmation, sync status/error, and the fingerprint
of the last successful silence set. Failure never clears confirmed state.

### 12.2 `silence_rules`

Logical `(cluster_id, silence_id)` identity only. It exists independently from a
payload so unresolved live `SilencedBy` IDs can be retained without synthetic data.

### 12.3 `silence_rule_episodes`

One continuous observed lifetime of a rule. It owns first/last confirmation and
confirmed end; a reappearance after confirmed absence creates the next episode.

### 12.4 `silence_revisions`

Immutable source content within an episode: canonical matchers, declared time
window, author/comment/annotations, source update time, and separate
content/evaluation fingerprints. Observation/ticket bookkeeping is non-semantic.

### 12.5 `alertmanager_source_silences`

The one mutable last-successful source relation set. Each `(source, rule)` points to
an exact revision and observed `pending|active|expired` state. Successful polls
atomically diff it; failed/invalid polls leave it unchanged.

### 12.6 `silence_recheck_jobs`

Persistent FIFO work item, cursor, counters/error, trigger, context fingerprint,
and canonical source-integrity manifest. Equivalent non-completed contexts conflict;
the same context may enqueue after completion/recovery.

### 12.7 `silence_recheck_job_rules`

Immutable complete decision set copied from current source relations only when a
D19 job is enqueued. The worker and failover successor never match against mutable
current relations.

### 12.8 `alert_suppression_cause_history`

Append-only complete cause sets. It owns alert, optional direct transition event,
optional recheck job, source/status/fingerprint, observation time, and latest
confirmation. The latest row is effective only while projected alert state is
suppressed.

### 12.9 `alert_suppression_cause_members`

Zero/one/many exact per-source logical rules/revisions in one cause set. A nullable
revision with explicit unresolved status preserves a known ID without invented
payload. No separate event-cause join table exists after D20.

## 13. Reconciliation and transaction contract

1. Poll collects alerts/silences outside the DB transaction.
2. One short transaction reconciles source health/current set, rules, episodes,
   revisions, alert tuple, live cause history, events, and any D19 job + pinned rules.
3. Unchanged successful polls update confirmation/health without history/job rows.
4. Current job completes before a newer job; no enqueued intermediate context is discarded.
5. Each job stores cursor/progress and resumes idempotently after restart/failover.
6. Matching reads immutable job rules plus source manifest and uses canonical
   Alertmanager semantics against `match_labels_json`.
7. State/event/cause/member/job-counter updates commit atomically in batches of 1000.
8. Candidate promotion uses D14 semantic CAS (`last_seen_at` + expected inactive predicates).
9. Poll reappearance/manual resolve winning the race makes stale promotion a no-op.
10. Live `SilencedBy` cause reconciliation runs in poll transaction; suppressed
    non-live alerts are never re-matched.
11. Failed predecessor preserves cursor/error, blocks FIFO, and keeps warning.
12. Future remote mode executes this boundary only under D18/D22 elected-writer lease.

## 14. Fingerprint and trigger contract

Source-set fingerprint includes sorted logical identity, exact revision/evaluation
fingerprint, and authoritative observed state. Job context additionally includes
source topology/integrity/HA consistency and logical episode identity.

D19 enqueues only promotion-capable changes. Display-only, reducing-coverage, and
unchanged changes update current/history/health without full inactive scan.

Fingerprint canonicalization is versioned deterministic JSON + SHA-256 as defined in
the exact schema. Context fingerprint is not globally unique across completed jobs.

## 15. Retention and foreign-key contract

1. Alert cause history/members follow alert retention.
2. Current source relations, pinned jobs, and cause members protect revisions by FK.
3. Open episodes/current source relations are never TTL-deleted.
4. Unreferenced closed rule history uses D4 silence-history TTL.
5. Queued/running/failed and cause-referenced completed jobs are retained.
6. Keep latest completed job; delete older completed unreferenced jobs and child rules.
7. Retired sources delete only after D4 TTL and after every FK reference disappears.
8. Housekeeping never disables FKs or deletes evidence required by events/UI.

## 16. Reviewed design decisions

### D1. Reverse transition when silence stops matching (decided)

Decision:

- do not transition a suppressed non-live alert back to `inactive` when its silence is changed, deleted, or expired,
- this applies both to alerts that were already suppressed while live and to inactive alerts promoted by Phase 2 during a non-live episode,
- suppression remains retained until the alert becomes `is_live=true`,
- on live reappearance, Alertmanager data becomes authoritative again: without suppression the alert becomes `active|unprocessed`; with suppression it remains `suppressed`.

Rationale:

- while the alert is non-live it is not currently firing,
- removing a silence during that absence is not evidence that the historical alert requires renewed attention,
- making it inactive again would create attention solely because a rule changed while the alert itself remained absent,
- the exact silence revision and its end/change remain in evidence history even though projected alert state stays suppressed.

Consequences:

- the historical cause record remains unchanged when its rule later changes/expires, and alert suppression does not follow rule withdrawal during the same non-live episode,
- the state dimension represents a retained lifecycle decision, not merely current rule activity,
- reappearance transaction recomputes `is_suppressed` from fresh live Alertmanager state.

### D2. Multi-upstream aggregation rule (decided)

Decision:

- follow upstream karma `DedupAlerts()` state precedence exactly; do not introduce a fork-specific aggregation rule,
- `active` on any relevant physical source wins over `suppressed`,
- therefore an inactive non-live alert is promoted by Phase 2 only when every relevant source is suppression-compatible in the complete successful snapshot,
- if any relevant source has no active matching silence, the deduplicated alert is not promoted,
- cluster identity is still used to identify/deduplicate logical silence rules for evidence and UI, but it does not replace upstream source-level state precedence.

Example:

```text
relevant source A: matching silence
relevant source B: no matching silence
result: not suppressed (remains inactive)
```

Rationale:

- upstream karma already collects all per-source alert states and gives `active` priority when any source reports it,
- Phase 2 must model the same hypothetical result for a non-live stored alert,
- treating one matching source as sufficient would make the same alert suppressed while non-live and active immediately after reappearance from another source,
- preserving upstream semantics avoids a second fork-specific dedup/state model.

Consequences:

- completeness is checked for every stored relevant upstream source,
- HA replication disagreement naturally prevents promotion when any member would produce an active result,
- logical silence evidence can still be collapsed by `(cluster_id, silence_id)` for display without changing decision semantics.

### D3. Expiration while upstream is unavailable (decided)

Decision:

- the last state confirmed by Alertmanager remains authoritative until Alertmanager successfully provides newer information,
- local time crossing stored `starts_at` or `ends_at` does not activate, expire, or otherwise mutate a silence while its source is unavailable,
- existing alert suppression and causal history remain unchanged during the outage,
- cached silence rules are not used to promote new inactive alerts without a successful current refresh,
- stored time boundaries may be displayed as source metadata, but they do not become state-changing evidence by themselves.

Example:

```text
last confirmed state: suppressed by an active silence
stored ends_at passes while Alertmanager is unreachable
result: alert remains suppressed and silence remains in its last confirmed state
```

Rationale:

- the silence may have been extended, replaced, or changed after the last successful response,
- calculating a new authoritative state from an old payload would be an inference under uncertainty,
- this follows the existing fork rule: when current upstream state is unknown, preserve the last known state and invent nothing.

Consequences:

- freshness/connection health is tracked separately from silence state,
- there is no locally synthesized `expired` transition during an outage,
- after the next successful refresh, the returned snapshot is processed normally and becomes authoritative.

### D4. Historical silence retention (decided)

Decision:

- silence history uses a separate configurable retention TTL,
- default silence-history TTL is 30 days (`720h`),
- current silence revisions are never deleted by TTL,
- revisions referenced by retained alert matches/events are never deleted by TTL,
- unreferenced historical revisions are deleted after the silence-history TTL measured from their last successful observation,
- alert-to-silence match history is retained for exactly as long as its alert lifecycle record and is cascade-deleted with that alert,
- logical rule identity is deleted only after it has no revisions,
- no separate hard `maxSilenceRevisions` cap is introduced initially; revision count and storage size must be observable.

Proposed configuration namespace:

```yaml
alertLifecycle:
  silenceHistory:
    retention:
      ttl: 720h
```

Sync-run retention:

- always retain the latest completed run and latest failed/partial run needed for diagnostics,
- retain run summary/source-integrity rows while referenced by retained evidence,
- delete older unreferenced runs and memberships during housekeeping.

Rationale:

- alert evidence must never outlive its referenced revision,
- silence history and alert history may need different retention periods in the future,
- a separate configuration avoids changing the meaning of alert TTL later,
- TTL bounds unreferenced history without breaking referential integrity,
- a hard row cap cannot safely evict referenced evidence and is therefore omitted.

### D5. Upstream/cluster removal or rename (decided)

Decision:

- follow upstream karma identity semantics: `AlertmanagerConfig.Name` is the stable physical source identity,
- changing URI while preserving Name continues the same source identity,
- removing a Name from configuration is an authoritative local topology change and retires that source from future completeness checks,
- rename is represented as retirement of the old source plus creation of a new source; no URI/cluster heuristic attempts to infer continuity,
- re-adding the same Name restores the same source identity, but it is not used for transitions before its first successful refresh,
- changing `cluster` under the same source Name starts a new logical silence scope while preserving historical revisions under the old cluster identity.

Retirement semantics:

- retirement does not claim that the remote Alertmanager or its silences were deleted/expired,
- last observed remote facts stay historical and source is marked `retired_at`,
- retired source is removed from the operational relevant-source set,
- if other relevant sources remain, decisions use the new configured topology after their next complete successful refresh,
- if no relevant sources remain, no automatic transition is allowed and last alert state is preserved.

Rationale:

- Name already owns source identity in upstream APIs and the Phase 1 relevant-upstream model,
- rename detection from URI/cluster would be heuristic and violate the no-inference rule,
- explicit config removal is authoritative about what this karma deployment observes, without making claims about remote state,
- operators can preserve continuity by keeping Name stable while changing connection details.

### D6. Exact upstream edit timestamps and status (decided)

Decision:

#### AN INVALID SILENCE ENTRY DOES NOT EXIST FOR KARMA

Karma treats every object in a syntactically valid Alertmanager silence-response
array as an independent untrusted entry. Each entry is decoded and validated before
it reaches the Alertmanager model, matching, lifecycle reconciliation, recheck
evaluation, or storage.

An invalid individual entry produces a warning and is skipped. Other valid entries
from the same response continue normally, and the source remains successfully
synchronized. Karma never infers, substitutes, retains, or processes an invalid
field value. This includes a missing identity or required field, unsupported state,
wrong JSON type, malformed date, invalid time range, malformed matcher or regular
expression, unsafe matcher set, and duplicate valid ID.

The skipped entry is absent from the current successful source snapshot. If Karma
knew the rule from an earlier successful snapshot, normal confirmed-absence
handling removes its current source relation. Its open episode closes as
`absent_confirmed` after every configured HA member confirms the absence. A later
valid response is a new appearance and starts a new episode.

Unknown additional JSON fields and absent optional metadata (`updatedAt`,
`annotations`, `createdAt`) are accepted for forward/backward compatibility. Only
a syntactically malformed top-level response, where reliable entry boundaries
cannot be established, fails the whole source refresh and preserves the last
confirmed snapshot.

- extend the typed Alertmanager silence mapper/model to collect `status.state`, `updatedAt`, and `annotations`,
- source `status.state` from a successful response is authoritative (`pending|active|expired`),
- if one silence response object is invalid, ignore only that entry as though it
  was absent from the response; continue processing all valid entries from the same
  Alertmanager response,
- record each ignored rule as a structured non-fatal warning containing the
  Alertmanager instance, silence ID, received status, and reason; log it at warning
  level and expose it through the existing notification UI,
- do not mark the Alertmanager unhealthy for an ignored rule; replace the complete
  warning set on every successful silence poll so a later valid response clears the
  warning,
- apply the same complete response validation when reading an existing rule for
  edit/recreate, while leaving outgoing create/update request payload parsing
  separate because request payloads do not contain response-only fields,
- do not synthesize or expose `createdAt`; Alertmanager API v2 does not provide it,
- store local `first_observed_at` / `last_observed_at` separately from nullable `source_updated_at`,
- unknown/missing **source synchronization status** makes the complete source
  snapshot unreliable and blocks silence-driven transitions; this is distinct
  from one malformed rule in an otherwise successful source response,
- missing `updatedAt` is stored as NULL and does not block matching,
- missing annotations are normalized to an empty object.

Revision fingerprint scope:

- content revision includes matchers, declared time window, author, comment, and annotations,
- observed status is not immutable revision content because it can change with time without a rule edit,
- karma-derived ticket fields are display metadata and do not drive the evaluation fingerprint.

Rationale:

- D3 requires Alertmanager-reported state rather than locally inferred temporal state,
- upstream `updatedAt` and local observation time describe different facts and must not be conflated,
- typed fields provide validation for the data the lifecycle feature uses,
- nullable handling remains honest for malformed or non-standard upstream responses.

### D7. Alertmanager HA disagreement (decided)

Decision:

- preserve every physical upstream observation and revision separately,
- evaluate suppression independently for every relevant source, then aggregate with the upstream-compatible active-first rule from D2,
- if any relevant source has no matching active silence, promotion is blocked,
- if every relevant source is suppression-compatible, promotion is allowed even when members report different revisions,
- never choose a canonical revision by `updatedAt`, source order, or another heuristic,
- group evidence logically by `(cluster_id, silence_id)` while retaining all member observations,
- expose cluster disagreement in API/stats and keep a persistent warning until a later successful snapshot converges,
- a failed refresh cannot clear a previously observed disagreement,
- historical disagreeing revisions remain retained after convergence.

Disagreement exists when successfully polled members in one logical cluster differ by:

- presence/absence of the same silence ID,
- content fingerprint,
- authoritative source status,
- payload validity/completeness.

Different silence IDs with equivalent matchers remain distinct rules and are not disagreement.

Rationale:

- per-source state aggregation follows existing upstream karma semantics,
- blocking an identical final suppressed result would introduce unnecessary fork-specific behavior,
- selecting one revision would invent certainty that the cluster has not provided,
- preserving every observation keeps evidence and audit truthful.

Consequences:

- a promoted alert can reference multiple revisions of one logical silence during HA disagreement,
- UI must show that rule details are inconsistent rather than flattening them into one payload,
- already suppressed non-live alerts remain suppressed under D1; disagreement affects new promotions, evidence, and warning state.

### D8. Earlier retained-suppression naming (superseded by D13)

The intermediate `is_suppression_retained` / `suppression_retained_at` proposal was accepted while the new field was modeled only as a non-live overlay. D13 replaces that model with a normalized suppression axis and is the current contract.

### D9. Raw matching-label capture (decided)

Decision:

- build a fork-side raw-label index from per-upstream `am.Alerts()` after a completed poll,
- key that index by the existing pre-transform `LabelsFingerprint`,
- keep ordinary `DedupAlerts()` as the sole owner of filtering, deduplication, state precedence, and final alert membership,
- enrich only alerts present in the normal deduped result with `match_labels_json` from the raw-label index,
- do not add fork-only raw-label fields to upstream core models,
- never reconstruct matching labels from display-oriented `labels_json`, group labels, or fingerprint,
- canonical raw labels for the same fingerprint must agree; disagreement makes that alert unevaluable and surfaces integrity warning.

Missing-label contract:

- rows without original labels store `match_labels_json=NULL`,
- NULL rows do not participate in Phase 2 matching or reclassification,
- no transformed-label fallback is allowed,
- a later live reappearance fills original labels and makes the row evaluable,
- preview reports the excluded count rather than guessing.

Rationale:

- Alertmanager silence semantics require the original source label set,
- UI keep/strip configuration must not change domain matching,
- fork-side enrichment preserves upstream merge safety and avoids a second dedup implementation,
- NULL explicitly represents missing knowledge and follows the no-inference rule.

### D10. Pinned-input background recheck lifecycle (decided; enqueue scope refined by D19)

Decision:

- every D19 promotion-capable authoritative context is queued as a persistent FIFO recheck job,
- job creation copies the complete current source decision set into immutable job-rule rows,
- a newer poll never cancels or truncates an older queued context,
- enqueued intermediate contexts are not coalesced away because D1 makes every observed matching silence relevant to persistent non-live suppression,
- unchanged/non-promotion-capable contexts do not create full recheck jobs,
- jobs run in background batches of 1000 with immutable candidate membership and
  persisted per-candidate progress, no startup blocking, and no artificial throttle,
- crash/restart resumes the same job idempotently,
- a failed predecessor blocks later jobs and keeps persistent warning,
- per-alert eligibility is revalidated immediately before conditional update,
- only candidate-relevant source failures cause that candidate to be skipped,
- DB expected state requires no running/failed job and an empty queue.

Rationale:

- canceling an old job would make results depend on batch timing,
- skipping enqueued intermediate contexts would lose short-lived but successfully observed matching rules,
- pinning immutable input makes each decision reproducible,
- conditional updates prevent stale rechecks from overwriting live reappearance or manual resolve.

Required observability:

- current job id/context fingerprint,
- processed/total candidates,
- queue depth and oldest pending run,
- skipped candidate counts by reason,
- last error and retry state.

### D11. Original event-evidence linkage decision (superseded by D15/D20)

The original design selected a many-to-many join while evidence consisted of many
independent temporal match episodes. D15 replaced those episodes with one complete
cause-set parent, and D20 therefore replaced the speculative join with nullable
unique `transition_event_id` on that parent. D11 remains in the decision log only
to explain why the join table is intentionally absent from the final schema.

### D12. Silence revision boundaries and fingerprints (decided)

Decision:

- create a new immutable revision when normalized source content changes,
- also create a new revision when non-NULL upstream `updatedAt` changes even if known content is identical,
- create a new revision episode when a rule reappears after a complete successful cluster snapshot confirmed it absent from every relevant member,
- do not create a revision for status-only or repeated poll observations,
- partial failure or member disagreement cannot close a logical rule episode.

Normalized content fingerprint includes:

- canonical matcher array (sorted by name/value/regex/equality; duplicates retained),
- `startsAt` / `endsAt` normalized to UTC RFC3339Nano,
- `createdBy`, comment, and canonical annotations,
- exact string values without cleanup that would alter source semantics.

It excludes:

- authoritative observed status,
- local observation timestamps,
- karma-derived ticket ID/URL.

Evaluation fingerprint includes only matcher/time fields that can affect matching. Observed source status participates in the effective per-source snapshot fingerprint, not immutable revision content.

Storage/time contract:

- keep nullable `source_updated_at` and separate local `first_observed_at` / `last_observed_at`,
- if `updatedAt` is missing, content changes still create revisions and no timestamp is synthesized,
- store observed status in current `alertmanager_source_silences` and copy it into immutable job-rule rows when a recheck is enqueued.

Rationale:

- content/display history remains immutable and complete for observed changes,
- repeated polls do not create duplicate revisions,
- time-driven status changes remain observations,
- a confirmed disappearance/reappearance is a new rule episode even with identical payload,
- matcher ordering cannot create false revisions.

### D13. Normalized suppression axis (decided; supersedes D8)

Decision:

- use `is_suppressed` as the single lifecycle suppression dimension for both live and non-live alerts,
- use nullable `suppression_observed_at` for the local time at which the current suppression episode was first observed/established,
- remove the intermediate `is_suppression_retained` / `suppression_retained_at` proposal,
- keep current/historical silence evidence separate from suppression state.

Live invariant:

```text
is_live=true and raw_state=suppressed
    => is_suppressed=true

is_live=true and raw_state=active|unprocessed
    => is_suppressed=false
```

Non-live behavior:

- a live suppressed alert preserves `is_suppressed=true` after disappearance,
- a live active/unprocessed alert preserves `is_suppressed=false` and projects to inactive,
- Phase 2 promotion sets `is_suppressed=true`,
- silence evidence can become historical while `is_suppressed` remains true under D1,
- live reappearance recalculates `is_suppressed` from fresh `raw_state`,
- manual resolved state overrides projection while the underlying suppression dimension may remain true until reappearance.

Projection:

```text
if is_live:
    state = raw_state
else if is_resolved:
    state = resolved
else if is_suppressed:
    state = suppressed
else:
    state = inactive
```

Ownership:

- `raw_state` is the exact latest upstream state,
- `is_suppressed` is the normalized lifecycle suppression fact,
- `alert_suppression_cause_history` and its members own rule provenance,
- missing silence evidence never causes the UI to invent a rule (raw suppression can be inhibition-derived).

Rationale:

- one suppression axis is simpler than separate live raw-suppressed and non-live retained-overlay mechanisms,
- `is_suppressed` uses direct, common terminology,
- strict live invariant detects raw/normalized state drift,
- evidence remains responsible for explaining why suppression exists.

### D14. Stale background-job protection without generic row versions (decided)

Decision:

- do not add `record_version`, `record_token`, `lifecycle_epoch`, or `last_live_run_id`,
- do not use a SQLite trigger for optimistic concurrency,
- protect Phase 2 promotion with a semantic conditional update using the candidate's expected `last_seen_at` plus state predicates,
- treat `RowsAffected=0` as a harmless stale/no-op result and re-read for diagnostics/counters,
- keep matching inputs immutable or explicitly validated rather than invalidating on unrelated row updates.

Promotion CAS requires:

```text
alert_id matches
last_seen_at equals the value evaluated by the job
is_live=false
is_resolved=false
is_suppressed=false
state=inactive
```

The Store re-reads and validates the row before/after the conditional write as needed. State/event/evidence/link writes remain atomic.

Why this protects the real race:

- a stale job is dangerous only if an alert reappeared (resetting suppression under D1) and then became non-live again before the old job wrote,
- every live observation already updates `last_seen_at`, while disappearance preserves it,
- therefore a new live/non-live episode has a different `last_seen_at` even when booleans return to the same values,
- delete/reinsert also receives a new live observation timestamp,
- original `match_labels_json` is immutable for one alert identity; different source labels imply another `alert_id` (except integrity-error hash collision),
- annotations, receiver, display labels, and grouping metadata do not affect silence matching and should not invalidate a job.

Decision history and rejected alternatives:

1. **Application-managed monotonic version** was rejected because every mutation path would need to remember `+1`, making correctness dependent on developer discipline; it would also retry on irrelevant metadata changes.
2. **SQLite-owned version trigger** was rejected because it introduces database-specific behavior and complicates a future PostgreSQL/MySQL backend.
3. **Trigger-managed lifecycle epoch** was rejected for the same portability reason.
4. **`last_live_run_id`** was considered as a portable episode token but rejected as redundant with existing `last_seen_at` lifecycle data.
5. **Long transaction/row lock during matcher evaluation** was rejected because it can block poll/resolve writes and has database-specific locking semantics.
6. **State predicates alone** were rejected because `non-live -> live -> non-live` can return to identical booleans (ABA); expected `last_seen_at` closes that gap.

Risk assessment:

- default upstream poll interval is one minute, so the ABA scenario normally requires at least two later poll cycles before an old job writes,
- normal matcher execution should usually finish earlier,
- persistent FIFO jobs can nevertheless be delayed by crash/restart, retries, DB errors, or backlog, so the race is rare but real,
- the destructive effect would be a stale historical silence setting sticky `is_suppressed=true` in a newer lifecycle episode, with incorrect event/evidence until another live reappearance,
- semantic CAS removes that effect without introducing a general concurrency subsystem.

Portability:

- the contract requires only ordinary columns, conditional `UPDATE ... WHERE`, transaction atomicity, and `RowsAffected`,
- it is directly implementable in SQLite, PostgreSQL, and MySQL,
- backend-specific row locking can be added later as an optimization, not a correctness dependency.

### D15. Reclassification scope and suppression-cause history (decided)

Decision:

- background silence recheck scans only inactive, non-live, non-resolved, non-suppressed alerts with known matching labels,
- already suppressed non-live alerts are never re-matched against new or changed silence rules,
- resolved alerts are never re-matched,
- live suppression causes come only from authoritative per-source `SilencedBy` payload,
- Phase 2 promotion causes come only from exact revisions that matched in the pinned evaluation run,
- every authoritative change to a live alert's complete silence-cause set appends a history record,
- all past cause sets are retained for future history use,
- the effective silence explanation for an alert whose projected `state=suppressed` is the latest cause-history record,
- if projected state is not `suppressed`, no historical cause record is effective even when an underlying `is_suppressed=true` is overridden by manual resolve.

Set-based history:

- one history record represents the complete simultaneous cause set, not one row per silence ID,
- child members preserve every `(upstream, logical rule, exact revision)` observation,
- member ordering has no priority meaning and is canonicalized for `cause_fingerprint`,
- unchanged authoritative sets do not append duplicates; only `last_confirmed_at` advances,
- a fresh live reappearance always appends a new cause-history record even when its member set equals the prior episode,
- a changed revision under the same silence ID creates a new cause set while the alert is live,
- a Phase-2-promoted non-live alert receives one cause set at promotion and no later re-match records.

No-silence provenance:

- a live suppressed alert with empty `SilencedBy` receives a `cause_status=none` record (for example inhibition-only),
- provenance whose source cannot identify silence IDs uses `cause_status=unknown`,
- neither case invents a silence rule,
- a parent history record can therefore have zero members.

Examples:

```text
10:00 live SilencedBy=[S1]       -> history [S1]
11:00 live SilencedBy=[S1,S2]    -> history [S1,S2]
12:00 live SilencedBy=[S2]       -> history [S2] (effective/latest)
13:00 alert becomes non-live     -> no new re-match; [S2] remains effective
```

```text
inactive alert + pinned run matches A/S1 and B/S2
    -> one promotion history record with members [A/S1 revision X, B/S2 revision Y]
    -> later S1/S2 changes do not rewrite or extend that alert's cause history
```

Rationale:

- the second Phase 2 goal is exact causal explainability, not maintaining a materialized list of all rules that happen to match later,
- retroactively associating a newly created rule with an already suppressed alert would rewrite history,
- a set record handles multiple simultaneous `SilencedBy` IDs without treating array/insert order as priority,
- preserving all live authoritative changes supports future retrospective UI while current UI needs only the latest record.

### D16. Live `SilencedBy` without matching silence payload (decided)

Decision:

- preserve every exact live `SilencedBy` ID even when the same poll result has no corresponding silence payload/revision,
- create/find logical `(cluster_id, silence_id)` rule identity without inventing matchers, status, times, or revision content,
- store an unresolved cause member with `revision_id=NULL`,
- mark the parent cause set `partial`,
- never reject otherwise valid live alert/raw-state data solely because silence details are missing,
- never use unresolved members for Phase 2 promotion,
- if a later live observation resolves the ID to an exact revision, append a new known/partial cause-history record rather than mutating the old partial record.

Status/invariant model:

```text
known   -> one or more members; every member has an exact revision
partial -> one or more members; at least one revision is unresolved
none    -> authoritative SilencedBy set is empty; zero members
unknown -> source provenance cannot identify silence IDs; zero members
```

HA/source behavior:

- known on one member and unresolved/absent on another is D7 disagreement,
- live state remains authoritative,
- inactive promotion requires all relevant sources to provide complete active revision/matcher data,
- effective partial cause produces persistent warning until a later effective known/none record replaces it,
- historical partial records alone do not keep warning active.

Rationale:

- Alertmanager silence and alert requests are separate and can observe different moments/HA replication states,
- dropping the ID loses known evidence,
- rejecting the live alert loses a stronger authoritative fact,
- creating a synthetic revision violates the no-inference rule,
- immutable partial history is honest about exactly what was and was not known at that poll.

### D17. Current source set and immutable per-job input (decided)

Decision:

- do not persist a full silence snapshot for every poll,
- keep one mutable last-successful relation set in `alertmanager_source_silences`,
- unchanged successful polls update confirmation/health metadata without appending history rows,
- failed polls update source health/error but do not mutate the last confirmed relation set,
- when a meaningful recheck job is enqueued, copy the complete decision set into immutable `silence_recheck_job_rules`,
- a running/resumed job never reads mutable current source relations for matching,
- the job parent stores a small canonical source-integrity manifest; rule/revision references stay normalized in the child table with FKs.

Rationale:

- D2 decisions may require a new rule from one source plus already-existing rules from other relevant sources,
- D10 requires a delayed/retried/failover job to use the exact input that caused it,
- storing only the changed rule is insufficient,
- reading the mutable current set is nondeterministic,
- copying every unchanged poll is unnecessary,
- job creation is change-driven and therefore an appropriately sparse point to materialize the full input.

Storage consequence:

- the earlier per-poll `silence_sync_runs`, `silence_sync_sources`, and snapshot-generation design is superseded,
- `alertmanager_sources` owns current sync health/fingerprint,
- `alertmanager_source_silences` owns current last-successful membership,
- `silence_recheck_jobs` + `silence_recheck_job_rules` own pinned evaluation inputs.

### D18. Future multi-replica writer model (decided)

Decision:

- future remote-RDBMS mode uses one elected reconciler/writer per logical karma deployment,
- the elected reconciler alone performs Alertmanager polling, silence reconciliation, recheck jobs, and housekeeping,
- all replicas may serve reads/API and perform transactional manual resolve,
- active-active independent pollers are not supported,
- remote mode introduces DB-backed lease + fencing as additive infrastructure,
- current domain correctness must not depend on process-local memory, SQLite triggers, or SQLite-only locking behavior,
- separate logical deployments use separate databases/schemas rather than sharing unscoped domain tables.

Rationale:

- independent pollers can observe different HA members/times and interleave incompatible current-set/episode transitions,
- elected single-writer preserves the same deterministic reconciliation model as current single-replica SQLite,
- immutable job input and DB cursor allow failover without changing decision context,
- lease/fencing can be added later without rebuilding alert/silence history tables.

### D19. Promotion-capable recheck triggers (decided)

Decision:

- enqueue a recheck only when authoritative context can increase suppression coverage for inactive alerts or retry candidates previously skipped due to uncertainty,
- enqueue for a new active rule, `pending -> active`, active evaluation-fingerprint change, failed/invalid source recovery, blocking HA disagreement resolution, relevant-source retirement, or an evaluable cluster/source context change,
- do not enqueue for unchanged polls, display-only revision changes, rule deletion, `active -> expired`, continued/new failure, or disagreement that only reduces confidence,
- never cancel an already queued job when its rule later changes/expires/deletes,
- D10 FIFO applies to all enqueued promotion-capable contexts, not every storage/history mutation.

Fingerprint contract:

- job context includes source topology, per-source integrity status, active evaluation fingerprints, and HA consistency,
- logical episode identity participates so reappearance of an equivalent rule cannot deduplicate against an already processed old episode,
- context fingerprint is an audit/index value, not a globally unique lifetime key,
- the same silence set may legitimately enqueue again after `failed|invalid -> success`,
- duplicate prevention applies only to an equivalent queued/running job inside the elected-reconciler transaction,
- source-state reconciliation and job enqueue commit atomically, so crash retry cannot enqueue the same transition twice.

Rationale:

- scanning on every storage change wastes work and cannot reverse D1 suppression,
- scanning only for a new silence ID misses matcher/time changes, activation, recovery, and convergence,
- reduction in coverage cannot create a new promotion,
- source recovery can make previously skipped candidates evaluable even when the recovered set fingerprint is unchanged.

### D20. Direct transition-event link from set-based cause history (decided; supersedes D11 linkage table)

Decision:

- remove the proposed `alert_event_suppression_causes` join table,
- add nullable unique `transition_event_id` FK directly to `alert_suppression_cause_history`,
- a Phase 2 promotion creates exactly one state event and one complete cause-set record,
- live Alertmanager cause-history records use NULL because they need no state transition,
- multiple silence causes remain normalized child members of the one cause set.

Rationale:

- D11 was designed when evidence consisted of many independent match episodes,
- D15 replaced those episodes with one complete set parent, making current cardinality one event -> zero/one cause set,
- a join row and relation enum would be speculative overhead,
- if a future feature truly introduces many-to-many event context, its storage
  contract must be designed with that feature.

Transaction/FK contract:

- promotion transaction performs alert CAS, event insert, cause parent insert, and member inserts atomically,
- `UNIQUE(transition_event_id)` rejects two competing cause sets for one event while allowing many NULL live records,
- event-linked cause uses `ON DELETE CASCADE`; live cause history still follows alert retention through its alert FK.

### D21. Reference-based recheck-job retention (decided)

Decision:

- queued/running/failed jobs are never TTL-deleted,
- completed jobs referenced by suppression cause history are retained while any reference exists,
- `alert_suppression_cause_history.recheck_job_id` is non-NULL only for `source=silence_recheck` and uses `ON DELETE RESTRICT`,
- keep the latest completed job for operator stats even when unreferenced,
- delete older completed unreferenced jobs during housekeeping,
- delete pinned `silence_recheck_job_rules` with their job via cascade,
- do not introduce a separate job-retention TTL/config.

Rationale:

- referenced job input is part of the proof that D2 source coverage caused promotion,
- non-completed jobs are required for FIFO/resume/error visibility,
- completed jobs with no promotions are operational execution detail, not user history,
- exact rule/revision/episode history remains after unreferenced job cleanup,
- housekeeping cadence naturally provides short-lived diagnostics without promising another retention contract.

### D22. Defer lease/fencing schema to remote-RDBMS phase (decided)

Decision:

- schema v1 does not add an unused lease table, writer epoch, or fencing columns,
- current SQLite mode remains explicitly single replica,
- every Alertmanager-derived mutation is routed through one reconciler boundary,
- fetch happens outside the DB transaction; persistence is a short atomic transaction,
- jobs/cursors/pinned inputs live entirely in DB rather than process memory,
- manual resolve remains a conditional transaction available from any future API replica,
- future remote-RDBMS support may add DB-backed lease/fencing infrastructure,
- one database/schema represents one logical karma deployment; independent deployments use separate databases/schemas.

Future protocol:

- only the elected reconciler polls, reconciles, runs jobs, and performs housekeeping,
- persistence transaction revalidates/locks the lease using DB server time before domain writes,
- a stale writer cannot commit after leadership changes,
- domain rows do not carry infrastructure writer epochs when transaction-level fencing is sufficient.

Rationale:

- an unused SQLite lease introduces failure modes without providing shared-DB correctness,
- the eventual RDBMS may use different row/advisory locking primitives,
- single-writer transaction boundaries and idempotent keys are the durable architectural choice,
- adding one infrastructure table later is painless compared with prematurely coupling all domain rows to an untested lease design.

## 17. Step 3 acceptance checklist

Schema-stage completion required:

1. D1-D22 are decided (D13 supersedes D8; D20 supersedes D11 linkage shape),
2. normalized suppression and append-only causal history remain separate concepts in schema/API,
3. raw matching-label capture point and missing-label behavior are selected,
4. current source set plus immutable per-job input model is accepted,
5. exact schema v1 DDL, FKs, indexes, and retention rules are written and reviewed.

All five conditions are satisfied and implemented in fresh schema v1.
