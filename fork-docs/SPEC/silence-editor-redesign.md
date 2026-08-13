# Compact Silence Editor

## 1. Status

- Status: implemented.
- POC: [`../POC/silence-editor.html`](../POC/silence-editor.html).
- The POC remains an interaction and layout aid; the React implementation and
  this specification are the runtime contract.
- All product and architecture questions in section 14 are resolved.

## 2. Problem

Karma already provides a modal for creating and editing Alertmanager silences,
but the editor uses screen space inefficiently:

- each matcher occupies a wide multi-control row,
- equality and regex semantics use separate switches,
- time selection expands into large tabbed controls,
- preview is a separate form stage instead of continuous feedback,
- the generated Alertmanager payload occupies additional space,
- the overall flow requires more navigation than the task warrants.

The result is a large editor with substantial empty space and weak visual
priority. Operators should be able to describe a silence, inspect its effect,
and submit it without leaving one compact working surface.

## 3. Objective

Create one new compact silence editor that supports both:

- creating a silence,
- editing or recreating an existing silence.

The editor will use Alertmanager's compact matcher interaction as its baseline.
Each matcher is one directly editable expression input, for example
`environment="production"`. Matcher inputs flow one after another and wrap only
when the available width is exhausted.

All application entry points for adding or editing a silence will open the new
editor. The old editor remains in the repository but is no longer opened by
normal UI actions. Retaining it reduces upstream merge risk and keeps a known
reference implementation during rollout.

## 4. Alertmanager Baseline

The design was compared with the current upstream Alertmanager Elm UI:

- [`Views/SilenceForm/Views.elm`](https://github.com/prometheus/alertmanager/blob/main/ui/app/src/Views/SilenceForm/Views.elm)
- [`Views/FilterBar/Views.elm`](https://github.com/prometheus/alertmanager/blob/main/ui/app/src/Views/FilterBar/Views.elm)

Relevant baseline behavior:

- matchers use compact controls instead of vertically expansive form sections,
- matcher operator semantics remain visible,
- matchers can be added and removed without leaving the form,
- duration and absolute start/end values are related representations of the
  same time range,
- affected alerts can be previewed before submission.

Karma will adopt the compact matcher interaction, not clone Alertmanager's
page layout. Karma-specific requirements include selecting a target
Alertmanager, lifecycle-aware states, automatic preview, current/past
suppressed distinctions, and matched-label highlighting.

## 5. Confirmed Product Requirements

### 5.1 One editor for create and edit

- The same component and data model must support create and edit modes.
- Create mode starts from defaults or matchers supplied by the UI entry point.
- Edit mode loads the complete existing silence into the editor.
- Normal add/edit/recreate links in Karma must open the new editor.
- The existing editor remains in source control and must not be deleted as part
  of this feature.

### 5.2 Target Alertmanager

- One create/edit operation targets exactly one Alertmanager target.
- Target identity is the stable logical cluster name configured in Karma.
- Physical Alertmanager HA members are an internal routing detail and are not
  shown as selectable targets.
- Multi-target batch creation from the legacy editor is intentionally not
  carried into the new editor.
- Preview, validation, submission, and errors are scoped to that one target.
- Read-only targets must not permit submission.
- Edit mode cannot move an existing silence to another target.

### 5.3 Compact matcher editor

- `Matchers` is the most important and information-dense editor section; it
  occupies the full modal width.
- Every matcher is one compact editable expression field containing label,
  operator, and value together.
- One field contains exactly one complete Alertmanager-compatible matcher; a
  comma-separated matcher list is not accepted in one field.
- Supported text syntax follows Alertmanager matcher rules, including all four
  operators, quoted values, escaping, whitespace, and UTF-8 values.
- Existing expressions remain directly editable; changing a label, operator,
  or value does not require deleting and recreating the matcher.
- Matcher fields flow horizontally one after another and wrap as complete units.
- `Add matcher` appends a new editable expression field.
- Resting and editing states must be visually unambiguous. A resting matcher is
  rendered as a compact saved expression; clicking it enters an explicit edit
  state with input focus and a stronger edit treatment.
- When the expression parser expects a label name, the active matcher input
  shows label autocomplete suggestions.
- Value autocomplete is intentionally narrow: it is shown only for an exact
  `<label>=""` expression while the caret is between the two quotes.
- Autocomplete data is target-scoped and comes from exactly the same alert
  dataset that is eligible for automatic preview.
- Alertmanager matcher operators must be representable: `=`, `!=`, `=~`, and
  `!~`.
- The interaction must not split one matcher into a full-width grid row or
  separate equality/regex switches.
- Invalid or incomplete matcher expressions must produce local validation
  feedback.
- Matchers are combined with logical AND, consistent with Alertmanager.
- Submission requires at least one matcher and at least one matcher that does
  not match the empty string, matching Alertmanager's match-all safety rule.
- Multiple matchers may use the same label name, exactly as Alertmanager allows.
  The editor does not attempt heuristic contradiction detection; automatic
  preview shows the actual conjunction result.

### 5.4 Silence time

The operator can select a common duration:

- 1 hour,
- 2 hours,
- 6 hours,
- 12 hours,
- 24 hours,
- 2 days,
- 4 days,
- 1 week,
- 1 month,
- 1 year,
- 100 years.

The operator can alternatively:

- select an absolute date/time using a calendar,
- enter a free-form duration such as `1d`, `1w`, or `1y`.

The selected value always resolves to concrete `startsAt` and `endsAt` values
before submission. Q7-Q10 define calendar, duration, and edit-mode semantics.

### 5.5 Author and comment

- Author is a compact single-line field.
- Comment is a multiline field.
- The last successfully used author is stored in browser local storage.
- A later create operation pre-fills that author.
- With authentication enabled, the authenticated username is the initial author
  value but remains editable. Without authentication, initial precedence is
  local-storage author, configured default, then empty.

### 5.6 Actions

- Primary action is `Create silence` in create mode and `Update silence` in
  edit mode.
- Secondary action is `Cancel`.
- The old explicit `Preview` step is removed from the target flow.
- Submission must remain impossible while required data is invalid.

### 5.7 Automatic affected-alert preview

- Affected alerts are shown below the editor controls.
- The preview refreshes automatically after matcher or target changes.
- Each result shows its projected lifecycle state and labels.
- State representation must distinguish at least:
  - active,
  - inactive,
  - unprocessed,
  - suppressed with `is_live=true`,
  - suppressed with `is_live=false`,
  - resolved.
- Labels involved in matcher evaluation are highlighted separately from other
  labels.
- Loading, empty, stale, and error states must be explicit.

## 6. Information Architecture

The implemented desktop layout is one modal with two vertical regions.

### 6.1 Compact editor region

1. Header with create/edit title and close action.
2. First row: Alertmanager target selector on the left and compact duration
   controls on the right.
3. Second row: full-width editable matcher list.
4. Third row: full-width author field.
5. Fourth row: full-width multiline comment field.
6. Final row: Cancel and create/update actions.

### 6.2 Affected-alert region

1. Result count and refresh state.
2. Scrollable alert list.
3. One compact state badge per alert.
4. Label tokens, with participating labels highlighted.
5. Empty/loading/error feedback in the same stable region.

The preview region must not make form controls jump vertically when its result
count changes. Desktop and mobile behavior must be approved using the POC.

## 7. Interaction Model

### 7.1 Matcher entry

1. The operator edits one complete matcher expression in one input.
2. A valid matcher is normally in a compact resting state; clicking the
   expression enters edit mode.
3. Edit mode is visually distinct but has no separate confirm button.
4. `Add matcher` appends a blank expression input and immediately enters edit
   mode.
5. Each expression is parsed and validated locally while it is edited.
6. When the parser is at the label position, autocomplete filters known label
   names using the typed prefix.
7. Selecting a label suggestion inserts `label=""` and places the caret at the
   value position.
8. At that exact empty-value position, autocomplete offers known values for the
   selected label. It is not shown for operators other than `=` or after value
   text has been entered.
9. Enter or a click outside the matcher confirms a valid edit; Escape restores
   the value from before editing.
10. Removing a matcher deletes that complete expression input.
11. Editing any matcher expression schedules a new preview request.
12. The autocomplete input exposes the WAI-ARIA combobox contract: stable
    input/listbox/option IDs, `aria-controls`, and `aria-activedescendant` track
    the keyboard-selected option.

The serialized matcher remains structured data (`name`, `value`, `isRegex`,
`isEqual`). The editable expression is parsed into that structured model at the
UI/domain boundary.

### 7.2 Matcher visual states

- Resting matcher background matches the normal modal/page control background.
- Editing uses a distinct project-palette background and border, without a glow
  or box-shadow effect.
- Remove uses the project danger border and an `x` icon.
- `Add matcher` uses the project success color.
- The implementation must use existing Karma/Bootswatch theme tokens rather
  than introducing an unrelated matcher palette.
- A valid expression uses clearly distinguishable semantic syntax colors for
  label, operator, quote characters, and value. The palette should remain
  compatible with Karma, but differences must be visible at normal UI size and
  must not collapse into several nearly identical gray tones.
- Regexp operators (`=~` and `!~`) use a distinct operator color. Their values
  also receive a subtle dotted underline so regexp semantics are not
  communicated by color alone.
- A regexp value is tokenized into literal text and RE2 syntax. Every non-literal
  regexp token is highlighted independently, including alternation (`|`),
  anchors, wildcard, quantifiers, grouping, character-class delimiters/ranges,
  brace quantifiers, and escape sequences. Literal fragments keep the normal
  value color.
- Exact and negative-exact operators remain distinguishable from regexp
  operators, while invalid/incomplete expressions keep the existing validation
  treatment instead of receiving misleading valid syntax colors.
- Syntax decoration must not replace the native input model. The actual input
  remains responsible for focus, caret, selection, keyboard behavior, and
  accessibility; a non-interactive synchronized layer may render decoration.

### 7.3 Duration selection

1. Selecting a preset makes it the active duration mode.
2. Selecting `Date` reveals the absolute date/time control.
3. Selecting `Custom` reveals the free-form duration control.
4. The UI shows the resulting end timestamp near the active choice.
5. Only one duration mode is active at a time.

### 7.4 Automatic preview

1. Changes to target or complete matcher expressions mark the visible results stale.
2. A debounce avoids a request for each keystroke/rapid edit.
3. Any older in-flight request is cancelled or ignored.
4. Existing results remain visible but are marked as refreshing.
5. The newest successful response atomically replaces the preview list.

Preview correctness must use backend matcher semantics. The browser must not
become a second, potentially divergent Alertmanager matcher implementation.

## 8. Proposed Preview Data Contract

The current `alertList.json` response returns label sets only. It cannot meet
the new preview contract because it does not include alert identity or
lifecycle dimensions.

Preview uses a dedicated fork endpoint:

```text
POST /silence-editor/preview
```

The request carries one logical target name and structured matcher objects. It
does not carry Karma filter strings or raw comma-separated matcher text. The
backend evaluates Alertmanager matcher semantics and returns, per affected
alert:

- stable alert ID/fingerprint,
- projected `state`,
- `rawState`, `isLive`, `isSuppressed`, and `isResolved`,
- the complete labels used for display,
- names of labels participating in matcher evaluation,
- optional Alertmanager source/cluster context needed for target scoping.

The endpoint is read-only despite using POST; POST is chosen for robust
structured payloads and regexp escaping. Q15-Q16 define population and pagination.

## 9. Local Storage Contract

Author persistence must:

- use one explicit versioned storage key,
- tolerate unavailable or corrupt local storage,
- store only the author string and no silence payload/comment/matchers,
- update only after the operation-specific successful submission defined by Q12,
- define precedence between authenticated identity, stored author, edited
  silence author, and configuration default before implementation.

Author storage is convenience-only. A failed `localStorage` read uses the normal
configured/empty fallback; a failed write leaves the successful remote operation
successful. Both failures emit `console.warn` in the browser, produce no UI
message, and never disable or block Create/Update.

## 10. Entry Points and Compatibility

The integration audit must cover at least:

- navbar `New silence`,
- alert menu `Silence this alert`,
- group menu `Silence this group`,
- managed-silence `Edit`,
- expired managed-silence `Recreate`,
- any shared/deep-link form initialization currently using query parameter
  `m`,
- read-only target handling.

The old editor code stays buildable. New behavior should be isolated in a
fork-specific component directory, with only small integration hooks in these
upstream-owned entry points.

## 11. Accessibility and Responsive Requirements

- All controls must be keyboard reachable.
- Matcher expression inputs must have accessible field and remove labels.
- Enter and outside click confirm a valid matcher edit, Escape restores it, and
  these interactions must not submit the whole form unexpectedly.
- Autocomplete must support keyboard navigation and expose suggestion state to
  assistive technology.
- Escape/backdrop/cancel behavior must follow the decided unsaved-change rule.
- Focus must move into the modal on open and return to its trigger on close.
- State must not be communicated by color alone.
- Matched-label highlighting must remain visible in light and dark themes.
- On narrow screens, controls stack without horizontal page scrolling and the
  alert preview remains usable.

## 12. Implementation Record

### Stage 1: design decisions

- Completed through the POC and the Q1-Q24 decision record in section 14.

### Stage 2: domain and API contract

- Implemented as target-scoped preview, label-name, and label-value endpoints.
- Current live label rows overlay indexed lifecycle rows by alert ID inside the
  indexed SQL query. Stale rows are replaced, live-only alerts are added, and
  exact totals/hit counts are computed without a retained-alert JSON full scan.
- Matcher expressions are parsed into structured matcher objects before any API
  request; raw expression strings are not a wire contract.
- Submission performs a backend preflight through the preview contract before
  contacting Alertmanager, so RE2 validation cannot diverge from browser
  `RegExp` validation.
- Mutation submit is at-most-once per user action: one deterministic writable
  member receives the request (healthy-first, then name order). There is no
  automatic HA retry for create/edit/recreate because Alertmanager create
  semantics are not idempotent for empty silence IDs.
- Submit routing follows upstream proxy mode: `proxy:true` uses backend
  submission through Karma; `proxy:false` returns a direct-submit descriptor and
  the browser executes the POST against the target external/public URI with the
  configured headers and CORS credentials. The descriptor contains the
  backend-normalized payload; the browser never reuses its pre-validation copy.
- Runtime logging follows `runtime-logging-contract.md`. In `proxy:true` mode Karma
  observes and can audit the Alertmanager POST result. In `proxy:false` mode Karma
  can audit only descriptor issuance; the subsequent browser-to-Alertmanager result
  is not observable in the server application log. Browser-routed submission is
  retained by decision; start and terminal result are emitted as structured
  `console.info`/`console.warn`/`console.error` records with the descriptor's
  correlation ID. Descriptor issuance must never be logged as a successful silence
  action, and browser console records are not durable server audit evidence.
- Direct browser mutation has a 30-second abort deadline. Timeout is terminal
  `outcomeUnknown` because Alertmanager may have committed before the response
  was lost.
- Transport or upstream 5xx submission failures are reported as
  `outcomeUnknown=true`; UI blocks retry for the current editor instance
  because upstream mutation may already be committed.
- For direct browser submission, malformed/missing-ID success responses are also
  treated as `outcomeUnknown=true` because mutation may already be committed
  upstream.
- Unknown outcome is terminal only for the currently open editor: the persistent
  error remains visible and retry is disabled, while Cancel/header close/Escape may
  close the modal. A fresh open starts a new editor session. No browser-storage
  marker or Browse navigation participates in mutation handling.
- Logical cluster IDs are the editor target identity; physical HA members are
  resolved only for Alertmanager submission.
- Status returns the deterministic writable member selected by healthy-first/name
  ordering. Submit carries that route token plus explicit operation and original
  silence ID, reselects the current member, and rejects with `requiresReopen` if
  routing or that member's active/expired state changed. Deduped HA state is never
  used to authorize a mutation.
- Edit and recreate use optimistic concurrency control. Immediately before opening
  the form, `/silence-editor/status` reads the silence directly from the selected
  physical Alertmanager member and returns that member's payload plus an opaque
  `revisionToken`. The token is a SHA-256 hash of canonical source content: silence
  ID, sorted structured matchers, starts/ends/created/updated timestamps, author,
  comment, and annotations. Derived and time-varying status (`pending`, `active`,
  `expired`) is excluded from the token.
- The form is populated only from the payload returned by that fresh member read;
  it never combines a deduplicated/list payload from one HA member with a route to
  another member. Submit sends the opaque token, not a client-editable copy of the
  original silence.
- Immediately before any edit/recreate POST or direct-submit descriptor is issued,
  the backend reads the same silence from the same physical member again and
  recomputes the token. A missing silence, changed member, changed operation state,
  or token mismatch returns HTTP 409 with `requiresReopen=true`; no mutation is
  attempted. The UI explains that the silence changed after the editor opened and
  requires closing and reopening the operation. Preserving the user's draft across
  that conflict is intentionally rejected because it could reapply stale fields.
- This prevents silent overwrites caused by another operator, Alertmanager-side
  edits, deletion/recreation, or HA replication lag between opening and saving.
  Alertmanager has no conditional-update primitive, so a narrow race remains
  between the final GET and POST; eliminating that race requires upstream
  Alertmanager support for revision-aware writes.
- Alertmanager silence annotations are intentionally unsupported in the current
  compact editor. They are not displayed, editable, or included in Create payloads,
  and Edit/Recreate may remove or overwrite annotations already stored in
  Alertmanager. This is an explicitly accepted preview limitation. Full annotation
  editing and preservation is the first deferred item in `FUTURE_FEATURES.md`.
- Operation identity is strict: edit payload ID must equal `originalSilenceID`;
  create and recreate payloads carry no ID. Every create entry clears the prior
  route token, and create submits omit it defensively.

### Stage 3: additive UI implementation

- Implemented in the fork-specific `SilenceEditor` component and stylesheet.
- Create, edit, and recreate are explicit operations. Edit preserves exact
  timestamps; recreate omits the old silence ID and preserves its positive
  elapsed duration from a new submit-time start.
- Edit versus recreate is selected from server-provided silence metadata
  (`isExpired`) at entry points; client wall clock is not authoritative. The
  list endpoint and generic alert-response cache hits recompute expiry from the
  request-time server clock. Opening an operation rechecks the selected
  silence through `/silence-editor/status` immediately before filling the form.
  The compact editor opens only after that request succeeds. Status checks share a
  monotonic replacement generation in `SilenceFormStore`; only the latest click
  may populate/remount the editor, and close/replacement invalidates pending
  generations.
- Preview uses a 350 ms debounce, stale-result indication, generation-based
  response ordering, and cursor pagination.
- Preview and autocomplete fail closed on lifecycle store errors (HTTP 500)
  rather than silently degrading to live-only results.
- Preview payload includes `matchedLabels` (name/value pairs from raw labels) so
  evidence remains visible even when rendered labels are stripped by UI label
  configuration.

### Stage 4: entry-point routing

- All normal create/edit/recreate actions route to the compact editor. It has no
  Browse tab or hidden Browser entry point.
- The legacy editor remains in source control and its legacy-only tests continue
  to run, but it has no runtime route or feature flag.
- The old silence-rule Browser also remains in source control with isolated
  component tests, but is not reachable from production UI. Its future scope and
  known risks are documented in `future-silence-browser.md`.

### Stage 5: verification

- Regression coverage includes strict duration grammar, submit-time timing,
  edit/recreate semantics, logical target resolution, defaults, close guards,
  pagination, duplicate-submit prevention, and generated-regexp escaping.
- Backend tests cover validation and target-aware preview responses.
- Production UI build and browser verification are required release gates.

## 13. POC Scope

The standalone HTML POC demonstrates:

- create and edit openings,
- full-width matcher section containing sequential editable expression inputs,
- visually distinct resting/editing matcher states,
- label-name autocomplete while the matcher parser expects a label,
- value autocomplete only for exact `<label>=""` caret position,
- semantic matcher syntax colors with a non-color regexp cue,
- all requested duration presets,
- date and custom duration panels,
- author persistence in local storage,
- automatic in-browser filtering of sample alerts,
- lifecycle state badges,
- matched-label highlighting,
- responsive desktop/mobile layout.

Its sample matcher evaluator exists only to demonstrate interaction. The real
application must evaluate preview matches on the backend.

## 14. Decision Record

### Q1. Target selection cardinality (decided)

One operation targets exactly one Alertmanager target. The target is immutable
in edit mode. The new editor does not support multi-target batch creation. This
keeps preview and submission semantics deterministic and removes partial-success
states from the new flow.

### Q2. Target identity and terminology (decided)

The selector shows the configured logical Alertmanager cluster name. That name
is the target identity used by preview and submission. Physical HA member names
are not shown and cannot be selected; a silence belongs to the logical cluster,
not to whichever member handles the request.

### Q3. Matcher expression grammar (decided)

One input contains exactly one complete Alertmanager-compatible matcher. It
supports `=`, `!=`, `=~`, and `!~`, quoted values, Alertmanager escaping,
permitted whitespace, UTF-8 values, and Alertmanager-compatible unquoted values
such as `severity=critical` and `summary=Disk full`. A comma-separated matcher
list is not
accepted in one field. The UI parses the expression into the structured matcher
model (`name`, `value`, `isRegex`, `isEqual`); the raw editor string is not sent
as the API domain representation.

Preview requests accept at most 100 structured matchers and 64 KiB of JSON.
These server-side limits apply before regexp compilation or retained-alert
evaluation. Submit requests also accept at most 100 structured matchers, but do
not have an additional Karma payload-size limit by default: existing Alertmanager
silences must remain editable regardless of Author or Comment size. Operators may
set `silenceEditor.maxPayloadBytes` to a positive byte count; the same limit then
applies to submit requests and fresh singular silence responses used by
Edit/Recreate verification. It should normally match Alertmanager's
`--silences.max-silence-size-bytes` setting.

### Q4. Duplicate matcher labels (decided)

Multiple matcher inputs may use the same label name. Alertmanager evaluates all
matchers with logical AND, and the editor preserves that behavior. It does not
prohibit duplicates or attempt heuristic contradiction warnings, especially for
regexp combinations; the automatic preview is the source of truth for the
resulting match set.

### Q5. Matcher autocomplete (decided)

Label-name autocomplete at the label position is decided. Narrow value
autocomplete for exact `<label>=""` with the caret between quotes is also
decided. Suggestions come from the same selected-target dataset that is
eligible for automatic preview. Therefore autocomplete cannot suggest data
from another target or from an alert population excluded from preview. Q15
defines the exact live/non-live population shared by both features.

Label names and values use separate backend queries. Label-name results are
alphabetical and normally include every distinct name, with an emergency safety
cap of 1000. Value results are scoped to one selected label, ordered by matching
alert frequency descending with an alphabetical tie-break, and limited to 50.
Both response types include `total` and `truncated`; truncation is never silent.
The UI prefix-filters label names and does not render all returned names into
the DOM at once. Lifecycle-enabled deployments serve these queries from the
normalized `alert_target_match_labels` index. `total` is an exact grouped count
computed independently from the returned page limit; it is never a lower bound.
Typed prefixes are literal strings: SQL wildcard characters `%` and `_` have no
special meaning in autocomplete.

### Q6. Empty and broad matchers (decided)

The editor enforces Alertmanager's match-all safety invariant before submit:
there must be at least one matcher and at least one matcher must not match the
empty string. A set containing only broad matchers such as `service=~".*"` is
invalid, while `service=~".+"` satisfies the non-empty requirement. This is a
blocking validation error, not a bypassable confirmation.

### Q7. Start time (decided)

The approved timing layout exposes `Starts` as `Now` or `At date/time` and
exposes `Ends` as duration or an exact timestamp. In `Now` mode, `startsAt` is
computed immediately before the submission request, not when the modal opened
or when the operator selected Now. If Ends uses duration, `endsAt` is recomputed
from the same submit-time start. The displayed timestamp is therefore a live
estimate until submission. Create and recreate initialize Starts as `Now`. Edit
loads the exact existing `startsAt`, including past or future timestamps, and
does not rewrite it without an operator action.

For an Ends duration, the anchor is `max(startsAt, submitTime)`. Thus an active
silence whose exact start is already in the past keeps that historical start but
`In 2 hours` means two remaining hours from submission. A scheduled future
silence uses its future startsAt as the duration anchor. Selecting duration
never resets Starts as a side effect.

Create initializes `Starts=Now` and `Ends=In 1 hour`, preserving the current
Karma default. Edit loads exact existing startsAt and endsAt because
Alertmanager stores timestamps, not the preset originally used. Recreate
initializes `Starts=Now` and preserves the original positive elapsed interval
(`old endsAt - old startsAt`) relative to the new submit-time start. If the old
interval is non-positive or invalid, recreate falls back to one hour.

### Q8. Preset calendar semantics (decided)

Hour, day, and week presets are fixed elapsed durations (`1d = 24h`, `1w =
7d`). Month and year presets are calendar additions. Calendar addition clamps
to the final valid day of the destination month: January 31 plus one month is
February 28/29, and February 29 plus one year is February 28. Timezone and DST
display/application details remain part of Q10.

### Q9. Custom duration grammar (decided)

Fixed elapsed units are `ms`, `s`, `m`, `h`, `d`, and `w`; integer units may be
combined, for example `1w2d6h`. `m` always means minutes.
Calendar units are `mo` and `y` and must be used alone (`3mo`, `1y`, `100y`),
so calendar and fixed arithmetic are never mixed in one input. Calendar values
follow Q8 clamping rules. Exact lexical normalization is decided separately in
Q9b.

### Q9b. Custom duration lexical rules (decided)

Syntax is canonical and strict. Values are positive integers; units appear once
and in descending order. Whitespace, decimal values, signs, repeated units, and
uppercase units are invalid. For example, `1w2d6h30m` is valid; `30m2h`,
`1.5h`, and `2h 30m` are invalid. The parser does not silently normalize input.

### Q10. Absolute date/time and timezone (decided)

Calendar input is interpreted in the browser's local timezone. The editor always
shows the timezone/UTC offset and the resulting UTC instant next to the control;
the payload uses RFC3339 UTC. A nonexistent local time during a DST forward jump
is a blocking validation error. An ambiguous local time during a DST backward
jump resolves to the earlier offset, and that selected offset is displayed
explicitly before submission. A past end timestamp is invalid.

### Q11. Author precedence (decided)

With authentication enabled, the authenticated username is the highest-priority
initial value, but the Author field remains editable. Therefore Alertmanager
`createdBy` is operator-supplied metadata, not a guaranteed authenticated
identity; server audit logging must treat the authenticated principal
separately. Without authentication, initial precedence is local-storage author,
configured default, then empty.

### Q12. Author persistence during edit (decided)

Create and recreate store the final Author in local storage only after a
successful API response. Edit stores it only when the operator explicitly
changed the initially loaded Author value. Merely saving an existing silence
does not turn its historical author into the browser's next create default.
Failed submissions never update local storage.

### Q13. Author and comment contract (decided)

Author and Comment are both required after a whitespace-only check. Author is
submitted with outer whitespace trimmed. Comment preserves its multiline text,
and edit mode loads the existing comment without rewriting it until the operator
changes it. The fork does not introduce field-specific character caps absent
from Alertmanager, so an existing valid silence cannot become uneditable merely
because its metadata exceeds a new arbitrary UI limit.

### Q14. Preview API shape (decided)

Automatic preview uses dedicated `POST /silence-editor/preview`. Its JSON body
contains the selected logical target and structured matchers. Backend matching
uses Alertmanager semantics. The existing `alerts.json` Karma-filter contract
is not extended, and regexp/matcher arrays are not encoded into a GET URL.

### Q15. Preview population and target scope (decided)

Preview and autocomplete use all retained alert states related to the selected
logical target: live, inactive, suppressed current/past, unprocessed, and
resolved. Live membership comes from the current source snapshot. Non-live
membership uses the last confirmed `relevant_sources`; the editor does not infer
membership from current topology after the alert disappeared. Matching uses raw
`match_labels_json`, not display labels transformed by Karma keep/strip rules.
Records without raw match labels are excluded because correct silence matching
cannot be proven; the response reports their count and the UI shows an explicit
warning rather than silently pretending coverage is complete.
Live instances sharing a label fingerprint are aggregated across every group ID
before target projection, using active-first state precedence and earliest
`startsAt`; map/group iteration order cannot select a partial target view.

### Q16. Preview result limits (decided)

Results are a flat, ungrouped list. The response reports the exact total and
returns at most 100 alerts per page with an opaque backend cursor. The server,
not the client, owns page-size enforcement. Sort order is current states first
(`active`, `unprocessed`, suppressed with `is_live=true`), followed by past
states (`inactive`, suppressed with `is_live=false`, `resolved`). Within each
projected state, alerts sort by `lastSeen` descending and then stable alert ID.
The UI loads further pages without requesting or rendering the full retained set
at once. The opaque cursor references a bounded immutable server-side snapshot,
so polling changes cannot duplicate or skip rows between pages. Expired/evicted
snapshots require preview restart. Submit preflight reuses the current snapshot
instead of repeating the retained-alert scan. One snapshot may contain at most
100,000 matched alerts; a broader result returns HTTP 422 and requires narrower
matchers rather than exceeding the process memory budget.

Snapshot and cursor fingerprints hash canonical JSON containing the target and
structured matcher fields. Delimiter characters inside matcher names or values
cannot alias a different matcher set.

### Q17. Highlight semantics (decided)

For every matcher label, the complete existing `label=value` token is
highlighted. If Alertmanager semantics match because the label is absent and
therefore has an empty value, preview adds a distinct dashed evidence token
`label=<missing>` rather than inventing a real label. A tooltip lists every
matcher expression involving that label, including duplicate-label matchers.
This explains positive, negative, regexp, and missing-label results without
implying data that the alert does not contain.

### Q18. Preview refresh timing (decided)

Every syntactically valid matcher change schedules preview after a 350 ms
debounce, including changes made before leaving matcher edit mode. Invalid input
does not clear the last successful result; that result remains visible with an
explicit stale marker. A newer generation cancels the previous request when
possible and always ignores an older response, preventing out-of-order updates.
Target changes and matcher add/remove use the same generation mechanism.

### Q19. Submission progress and failure (decided)

Multi-target partial success is no longer applicable after Q1. Submission stays
inline in the editor. The primary button shows progress and prevents duplicate
submit; editor controls become temporarily read-only. On a known failure, the modal
remains open, preserves every entered value, and shows a persistent actionable error
with Retry. An unknown mutation outcome is terminal for the current modal and offers
no Retry. Errors are not reduced to an expiring toast or a destructive full-modal
stage transition.

### Q20. Cancel and unsaved changes (decided)

Backdrop clicks never close the modal. Cancel, header close, and Escape close a
pristine form immediately. Once form state differs from its loaded baseline,
those actions require explicit Discard confirmation. While submission is in
flight all close paths are blocked because remote completion may already be
underway. Dirty tracking compares normalized domain state, not incidental focus
or matcher whitespace/quoting presentation state. Confirming replacement from an
external edit/recreate action always mounts a fresh editor instance, even when the
selected silence ID is the same as the discarded draft.

Opening the modal captures the invoking control and moves focus to the first
editor field. Tab and Shift+Tab remain within the dialog, including while lazy
content is loading, and closing restores focus to the invoking control. Every
visible field label is explicitly associated with its control; icon-only close
controls have an accessible name.

### Q21. Post-submit behavior (decided)

After successful create, update, or recreate, the modal briefly shows inline
confirmation containing the logical target and returned silence ID, then closes.
Karma immediately refreshes silence and alert data. A success toast retains the
target/ID confirmation after close. The submitted form cannot be submitted a
second time while confirmation/close is in progress.

### Q22. Generated payload and share link (decided)

The new editor contains neither the generated Alertmanager JSON payload preview
nor a shareable form link. The legacy editor's link encoded target selection,
matchers, duration, and Comment (but not Author) in reversible Base64 URL data;
that behavior is intentionally not carried forward. The old payload/link
components remain with the retained legacy editor source only.

### Q23. Old editor reachability (decided)

The legacy editor remains buildable and tested in the source tree but has no
runtime route, hidden query parameter, UI link, or feature flag. Production has
one supported editor integration path. Rollback uses a normal code release
rather than permanently maintaining two runtime UX contracts.

### Q24. Theme and modal dimensions (decided)

Desktop uses a responsive maximum width approximately matching the POC (around
980 px, finalized with Karma/Bootstrap metrics) and a viewport-bounded maximum
height. Header, compact form, actions, and preview header remain visible while
the affected-alert list consumes remaining space and scrolls independently. If
the form itself cannot fit, the modal provides an outer-scroll fallback rather
than clipping controls. Mobile uses a full-screen modal with one vertical scroll
and sticky header/actions. Exact pixel spacing is implementation polish, not a
standalone-POC contract.
