# Spec: Relevant Upstreams and Inactive Evaluation

## 1. Problem statement

A single alert identity can be observed from multiple Alertmanager upstreams.
To avoid false transitions, `inactive` must only be assigned when absence is trustworthy.

## 2. Definition

For each alert identity, store canonical `relevant_sources` entries containing the
physical source name and logical target. `relevant_upstreams` remains a compatibility
representation of the same physical membership.

For silence preview and target-specific lifecycle behavior, also store one
`alert_target_states` row per `(alert_id, logical_target)`, including that target's
physical member set and retirement status.

Membership is dynamic, but incomplete polls preserve previously required configured
members. A member is removed only after complete successful coverage or an explicit
configuration change.

## 3. Why this definition

Rejected model: "all upstreams where alert was ever seen".

Reason for rejection:

- It can permanently block `inactive` after topology or routing changes.
- Example: alert used to be seen on A and B; B config changes and never emits this alert anymore; historical B membership would incorrectly remain required forever.

Accepted model (latest successful snapshot) prevents this lock-in.

## 4. Evaluation algorithm

For each cycle:

1. Collect cycle success/failure per upstream.
2. Build current live alert set with upstream membership.
3. For each persisted alert not present in current live set:
   - read its configured, non-retired relevant source set,
   - verify all members were successful in this cycle,
   - only then apply auto-transition to `inactive`.

If any relevant upstream is missing or failed this cycle, skip transition.

## 5. Membership update algorithm

When alert is present in current live snapshot:

- if all previously stored relevant upstreams were successfully polled in this cycle,
  replace stored membership with the current configured observation.
- if coverage is partial for previously stored relevant upstreams,
  keep prior set and union with currently observed upstreams.

This prevents accidental loss of relevant membership during temporary upstream outages.
Once full coverage is restored, stored relevance converges to current snapshot membership.

If a source is removed or reassigned in configuration, it no longer blocks
disappearance. If a logical target loses all configured members, its target row is
marked retired: the last confirmed remote dimensions remain stored, but the target is
excluded from preview, autocomplete, and recheck until it reappears.

## 6. Edge cases

### 6.1 New alert

- Insert with current live membership as `relevant_upstreams`.

### 6.2 Previously resolved alert reappears

- transition to live state,
- refresh `relevant_upstreams` from current snapshot.

### 6.3 No relevant sources stored (data corruption/manual edit)

- do not auto-transition to `inactive` in that cycle,
- log warning and keep state unchanged.

### 6.4 One fingerprint in multiple targets

- Global state keeps the upstream active-first projection.
- Per-target rows preserve each logical target's liveness, suppression, start time,
  and member coverage independently.
- A target can become non-live or retired while another target keeps the same global
  alert live.
