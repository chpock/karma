# Spec: Relevant Upstreams and Inactive Evaluation

## 1. Problem statement

A single alert identity can be observed from multiple Alertmanager upstreams.
To avoid false transitions, `inactive` must only be assigned when absence is trustworthy.

## 2. Definition

For each alert identity, store a `relevant_upstreams` set representing upstreams in which this alert exists in the **latest successful live snapshot** for this alert.

This set is dynamic and replaced by newer successful observations.

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
   - read its `relevant_upstreams` set,
   - verify all members were successful in this cycle,
   - only then apply auto-transition to `inactive`.

If any relevant upstream is missing or failed this cycle, skip transition.

## 5. Update algorithm for relevant_upstreams

When alert is present in current live snapshot:

- if all previously stored relevant upstreams were successfully polled in this cycle,
  overwrite stored `relevant_upstreams` with current observed upstream membership.
- if coverage is partial for previously stored relevant upstreams,
  keep prior set and union with currently observed upstreams.

This prevents accidental loss of relevant membership during temporary upstream outages.
Once full coverage is restored, stored relevance converges to current snapshot membership.

## 6. Edge cases

### 6.1 New alert

- Insert with current live membership as `relevant_upstreams`.

### 6.2 Previously resolved alert reappears

- transition to live state,
- refresh `relevant_upstreams` from current snapshot.

### 6.3 No relevant_upstreams stored (data corruption/manual edit)

- do not auto-transition to `inactive` in that cycle,
- log warning and keep state unchanged.
