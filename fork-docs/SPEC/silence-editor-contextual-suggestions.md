# Contextual Silence-Editor Suggestions

## 1. Status and Purpose

- Status: implemented.
- Scope: matcher label-name and label-value suggestions in the compact silence
  editor.
- Parent specification: [Compact Silence Editor](silence-editor-redesign.md).

The current suggestions endpoints scope results only by logical Alertmanager
target. This feature makes them reflect the alerts selected by the already
completed matcher expressions in the same editor.

For example, after the operator commits `alertname="HostDown"`, adding a
matcher must suggest only label names and values present on `HostDown` alerts
for the selected target.

## 2. Confirmed User Contract

### 2.1 Context matchers

Suggestions are calculated from a **context matcher set**:

- the selected logical target is always part of the context;
- every other completed, syntactically valid matcher is included;
- the matcher field currently being edited is always excluded, even when its
  current text happens to parse successfully;
- empty and syntactically invalid matcher fields are excluded;
- all included matchers use logical AND semantics;
- `=`, `!=`, `=~`, `!~`, absent-label behavior, and regular-expression
  anchoring have exactly the same meaning as preview and submit validation.

`alertname` has no special matching semantics. Its value commonly narrows the
candidate set substantially, but the contract applies equally to every valid
matcher name and operator.

### 2.2 User-visible behavior

1. When an operator opens a matcher field, Karma computes suggestions from
   the target and its context matcher set.
2. While the operator edits `label`, that field does not constrain label-name
   suggestions.
3. After the operator enters `label=""`, that same field still does not
   constrain value suggestions; values are drawn from alerts selected by the
   other context matchers.
4. A matcher only affects suggestions for other fields after it is complete
   and valid and is no longer the currently edited field.
5. A malformed or empty field never hides a suggestion and never causes the
   request to fail.
6. Counts such as `N alerts` and `N values` describe the same context-filtered
   candidate population as the displayed options.
7. For label-name suggestions, labels absent from every currently valid matcher
   are listed first. Labels already used by a valid matcher follow, with
   alphabetical order within each group.

This contract is independent of whether a matcher was initially supplied from
the main Karma filter bar or entered manually in the editor.

## 3. Correctness Boundary

The browser may determine which form fields appear complete, but it is not an
authority for matcher semantics. Every suggestions request carries the
context matcher expressions, and the backend must:

1. parse and validate each supplied matcher with the editor's canonical Go
   parser and compiler;
2. reject a malformed request context as an API request error, rather than
   silently guessing its meaning;
3. use the same compiled-matcher evaluation path as preview;
4. return results only for the requested target.

The UI excludes malformed fields before sending the request so ordinary
in-progress typing remains non-blocking. A malformed context received by the
backend nevertheless remains an invalid client request and is not silently
reinterpreted.

The existing submit safety rule (at least one matcher must not match the empty
string) does not apply to read-only suggestions. A broad but syntactically
valid context is allowed; it simply produces a broad candidate population.

## 4. Data Model and Aggregation Refactor

### 4.1 Problem with the current aggregator

`ListTargetLabelNamesWithLive` and `ListTargetLabelValuesWithLive` aggregate
`alert_target_match_labels` before any form context exists. Once values have
been grouped, the implementation can no longer tell which label/value pairs
belonged to the same alert. Applying a matcher after this aggregation would
produce incorrect combinations.

### 4.2 Required pipeline

The refactor must retain the alert identity until matcher filtering finishes:

1. Build the target-scoped raw-label relation from persisted
   `alert_target_match_labels` and the current live-label overlay.
2. De-duplicate live alerts against persisted rows using the existing alert ID
   and `match_labels_fingerprint` contract.
3. Select candidate alert IDs using the compiled context matcher set.
4. Aggregate label names or values only over those candidate IDs.
5. Apply the requested display-prefix filter, ordering, limit, and exact total
   after candidate selection.

The resulting candidate relation is the only source for suggestion counts.
The implementation must not join separately aggregated label statistics,
because that can combine labels from different alerts.

### 4.3 Source eligibility

The new relation preserves current autocomplete eligibility rules:

- one logical target per query;
- no retired target projections;
- live overlay rows replace stale or missing stored-index rows for the same
  alert ID;
- alerts lacking raw match labels remain absent from suggestions, consistently
  with preview's `excludedNoRawLabels` behavior.

## 5. API and Client Refactor

### 5.1 Request shape

The current GET endpoints cannot safely carry an arbitrary matcher set in a
query string. The implementation will use JSON POST requests while retaining
separate label-name and label-value resources:

```json
{
  "target": "production",
  "contextMatchers": [
    {"name": "alertname", "value": "HostDown", "isRegex": false, "isEqual": true}
  ],
  "term": "ser"
}
```

The label-value request additionally has a required `name` field. `term` is a
display-prefix filter, not a matcher and not part of the context fingerprint.

Both response types retain their current result fields and add a required
opaque `contextFingerprint`. The browser applies a response only when its
generation and fingerprint still match the current editor state.

### 5.2 Client request policy

- The UI builds `contextMatchers` from parsed expressions, excluding
  `editingMatcher` and parse failures.
- Target changes, context changes, closing the editor, and replacement of an
  editor instance abort obsolete requests.
- A response from a prior generation is ignored even if the network request
  could not be aborted.
- No request body or matcher values may be written to runtime or browser
  console logs.

## 6. Performance Contract

The feature must not perform a full alert scan on every typed character.

- A change inside the currently edited matcher does not change the context
  matcher set, so it must not trigger fresh candidate selection.
- Exact equality context such as `alertname="HostDown"` must use the existing
  `(target, label_name, label_value)` index to obtain candidate alert IDs.
- Empty context keeps the current indexed all-target aggregation path.
- Regex and negative matchers must retain the canonical matcher semantics;
  they may not use an SQL approximation that differs from preview.
- Any context-result cache is bounded, time-limited, invalidated by the target
  label-index/live-overlay revision, and stores no raw request payload.
- Candidate selection and prefix filtering are separate work. Prefix typing
  may reuse a valid context result but must never reuse it after source/index
  state changes.

The cache and prefix-query strategy are defined in section 8. The benchmark
matrix validates their budgets and query plans rather than assuming a fixed
data cardinality. On the reference development host, the 100,000-alert exact
`alertname` benchmark completed one contextual value aggregation in about 10ms.

## 7. Required Verification

### 7.1 Storage and backend

- Exact `alertname` context limits both label names and values to the matching
  alerts.
- Multiple valid context matchers use AND semantics.
- The currently edited matcher is absent from the request context.
- Empty and malformed sibling fields are absent from the request context.
- Equality, inequality, positive regex, negative regex, and missing-label
  semantics match preview exactly.
- Stored rows and changed/missing live-overlay rows are merged once per alert
  ID; stale rows never double-count.
- Target isolation, retired targets, and alerts without raw labels retain
  current behavior.
- Old GET endpoints are removed or explicitly rejected; no undocumented
  compatibility path remains.

### 7.2 UI and end-to-end

- Editing `service` after committing `alertname="HostDown"` displays only
  HostDown-derived names and values.
- Editing `alertname` itself does not use its incomplete or current value as a
  self-filter.
- Changing a completed sibling matcher invalidates the old result, aborts its
  request, and ignores a delayed old response.
- Keyboard navigation, mouse selection, scrollbar interaction, ARIA combobox
  state, and modal-width placement continue to work with contextual results.

### 7.3 Performance

Benchmarks run against 100,000 indexed alerts and include:

- no-context label-name and label-value baseline;
- exact `alertname` context with a small candidate set;
- exact `alertname` context with a large candidate set;
- regex and negative context;
- stored-only, changed-live-overlay, and mixed populations;
- repeated prefix input for unchanged context.

The benchmark records SQLite query plans and elapsed time for candidate
selection separately from aggregation/prefix filtering. Exact `alertname`
must show indexed candidate selection. Thresholds are set from measured
baseline results before implementation is accepted.

## 8. Resolved Implementation Decisions

### D1. Exactness under the label-name limit — resolved

The current label-name endpoint caps responses at 1,000 entries. Local prefix
filtering of an already truncated list can hide a valid label that sorts after
the first 1,000 entries.

**Decision:** use a server-prefix query with a bounded context cache. The
backend reuses the selected context population and applies each typed prefix
exactly before the 1,000-entry cap. The browser must not filter a truncated
all-label response locally.

### D2. Context cache budget and invalidation token — resolved

The cache is RAM-only and stores selected candidate alert IDs, never raw
Alertmanager payloads. A cache key contains the logical target, the canonical
order-independent context matcher set, and that target's candidate-relation
revision.

- At most 8 contexts are cached.
- At most 100,000 candidate IDs are cached across every context combined.
- Entries expire after 2 minutes and use least-recently-used eviction.
- Each target has a monotonic stored candidate-relation revision. It changes
  when that target's eligible label index or retirement/reappearance changes.
  The current live overlay is represented by a separate content fingerprint in
  the cache key.
- A result exceeding the individual or global cache budget is computed and
  served exactly but is not cached.
- Cache capacity never truncates candidate selection or displayed suggestions.

### D3. Empty-context request timing — resolved

**Decision:** open label-name suggestions immediately for a new empty matcher
field, as the editor does today. The request uses the current context matcher
set. It is one request for that context; later prefix input follows D1 and D2
and does not repeat candidate selection for each typed character.

## 9. Implementation Evidence

1. D1-D3 are recorded in this document.
2. Storage regressions prove candidate-before-aggregation semantics.
3. The shared target candidate relation replaces the two
   aggregate-first store methods.
4. Canonical context compilation and the POST backend API contract are active.
5. The editor sends only valid non-editing context matchers and keeps its
   generation/abort protection for delayed responses.
6. UI regressions cover context construction, request generation, and delayed
   response protection; the existing browser suite covers editor interaction.
7. The 100k benchmark covers indexed exact `alertname` context alongside the
   existing baseline and live-overlay cases.
8. The editor, schema, and API contracts are synchronized with this behavior.
