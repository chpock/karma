# Architecture Conformance Matrix

Status: implemented.

This document is the finite traceability contract for the alert lifecycle and
silence editor work. A decision is not implemented merely because a schema field
or UI control exists. It is implemented only when its authority, writers,
readers, transaction boundary, failure behavior, and executable verification are
identified below.

The executable test coverage for these contracts is summarized in
`architecture-verification.md`.

## 1. Authoritative Boundaries

| Concern | Authoritative source | Writers | Readers | Required boundary |
|---|---|---|---|---|
| Live alert observation | Successful Alertmanager member response | Poll reconciler | Lifecycle engine, live APIs | One poll observation is merged by fingerprint before persistence |
| Historical alert identity | Source alert ID captured before display-label transformations | Lifecycle merge reconstruction | Deduplication, alert cards, manual Resolve | `labels.keep/strip` affect displayed labels but never replace persisted alert identity |
| Global lifecycle projection | `raw_state`, `is_live`, `is_suppressed`, `is_resolved` | Poll, recheck CAS, manual resolve | Alert APIs, grid, stats | Pure projection; state/event/evidence commit atomically |
| Target lifecycle projection | Per-target member observation + configured source manifest | Poll, target recheck CAS, manual resolve | Silence preview/autocomplete | Reconciled for live and non-live rows; retirement is topology, not remote state |
| Silence response validity | Independently decoded and validated entries from the last syntactically valid source response | v017 mapper + silence reconciler | Alertmanager model, recheck, evidence, UI warnings | Invalid entries are absent and cannot block valid neighbors; malformed top-level JSON fails the whole refresh; unknown extra fields and optional metadata are accepted |
| Silence history | Immutable revision/episode rows | Successful silence reconciliation | Job inputs, evidence, history | No per-poll snapshot history |
| Recheck decision | Immutable source/target-qualified rules and candidate membership | Job enqueue + eligibility transaction | FIFO worker | Worker never reads mutable rules; no keyset skip across eligibility changes |
| Suppression evidence | Exact live `SilencedBy` or exact pinned recheck revisions | Poll/recheck transaction | Alerts API `suppressionCauses`, read-only past-alert card | Every suppression transition has immutable cause provenance; unresolved identity is explicit and never synthesized |
| Matcher semantics | Alertmanager Go matcher semantics | Backend normalizer/compiler | Preview, recheck, submit; frontend corpus | Missing label is empty string; frontend cannot redefine RE2 |
| Time | Server request/poll clock; Alertmanager status while unavailable | Backend only | Editor status, lifecycle | Browser wall clock never decides edit/recreate or remote expiry |
| Submit payload | Backend-normalized operation | Submit endpoint | Proxy and direct mutation path | Same payload in both modes; at-most-once mutation |
| Editor navigation | `SilenceEditorSession` | Modal/session coordinator | Compact editor, external edit/recreate entry, close guard | Latest status request wins; unknown mutation blocks retry until the current modal closes |
| Preview pagination | Bounded immutable in-memory result set | Preview endpoint | Cursor pages, submit preflight | No DB snapshot; expiry/eviction returns 409 |
| Runtime observability | `runtime-logging-contract.md` | Runtime wrappers and endpoint handlers | Operator application log | Stable structured events and levels; sensitive payload data is never logged; browser-routed terminal audit is explicitly best-effort |

## 2. Lifecycle Decisions D1-D22

| ID | Invariant | Runtime owner | Verification | Status / required action |
|---|---|---|---|---|
| D1 | Non-live suppression is sticky until live reappearance | `projectedState`, live upsert, recheck worker | `TestApplyPollCycleReactivatesResolved`, `TestRunSilenceRecheckBatchPromotesInactiveAndAddsCause` | Implemented |
| D2 | Active on any relevant member wins; promotion requires every relevant member suppression-compatible | pure target aggregation, effective target cause links | all-source and multi-job target promotion tests | Implemented; global evidence links effective causes from every target/job |
| D3 | Upstream failure preserves last-confirmed silence state | source reconciliation | failed/invalid/unknown/retired recovery sequence | Implemented; fingerprint/current set/revision remain unchanged on failure |
| D4 | Configurable 30-day silence-history TTL; referenced/current rows protected | config -> Store -> housekeeping | config and Store retention tests | Implemented independently from alert retention |
| D5 | Source Name owns identity; removal/reassignment retires old target without inventing remote state | per-target source manifest + pure topology plan | topology restart/removal/reassignment tests | Implemented |
| D6 | Per-entry response validation for identity, state, timestamps, metadata types, matcher set/fields/regexp/safety, and duplicate IDs; invalid entries are absent and never inferred or retained | v017 raw-entry mapper, runtime snapshot filter, revision writer | `TestSilencesSkipsInvalidEntriesIndependently`, `TestSilencesSkipsDuplicateValidIDs`, `TestSilencesAcceptsUnknownExtraFields`, `TestSilencesAcceptsMissingOptionalFields`, `TestSilencesMalformedArrayFailsWholeRefresh`, `TestUnknownSilenceStatusMeansConfirmedAbsence` | Implemented |
| D7 | Preserve HA member disagreement including silence identity; persistent warning until convergence | identity-aware cluster fingerprint, Stats/warning | HA ID/payload/failure/convergence sequence | Implemented |
| D8 | `is_suppression_retained` proposal is superseded | N/A | schema absence | Superseded by D13 |
| D9 | Match only original raw labels; missing labels remain unknown | raw-label index, `match_labels_json` | raw-label exclusion tests | Implemented |
| D10 | Persistent FIFO target-qualified jobs, immutable candidate membership, crash resume, failed predecessor block, complete observability | recheck queue/worker/Stats | exhaustive source matrix, cursor-transition, reassignment, FIFO tests | Implemented |
| D11 | Earlier many-to-many event linkage proposal is superseded | N/A | schema absence | Superseded by D15/D20 |
| D12 | Revision only on normalized content/updatedAt/new episode; not repeated/status-only poll | latest-revision boundary and HA-wide episode closure | revision sequence and `TestSilenceEpisodeClosesOnlyAfterConfirmedHAWideAbsence` | Implemented; A→B→A appends and reappearance starts a new episode |
| D13 | One normalized `is_suppressed` axis with strict live invariant | projection/live validation | projection tests | Implemented |
| D14 | Recheck promotion CAS uses expected `last_seen_at` plus semantic predicates | global and target promotion SQL | `TestGlobalAndTargetPromotionCASRejectStaleObservation` | Implemented |
| D15 | Recheck only eligible inactive states; exact immutable cause set; no later re-match | target cause histories + aggregate links + effective-cause merge reader | target-only/multi-job/restart evidence and retained-card tests | Implemented |
| D16 | Missing live silence payload stores unresolved member, never synthetic revision | live cause writer | `TestLiveSuppressionCauseResolvesLaterWithoutRewritingHistory` | Implemented |
| D17 | One mutable current source set; immutable copy only per meaningful evaluation context | evaluation-aware source fingerprint and pinned job rules | open-predecessor matcher-successor test | Implemented |
| D18 | Future remote DB uses one elected reconciler; domain remains portable | process/store boundary | architecture review | Implemented constraint; no active-active path added |
| D19 | Enqueue only promotion-capable context, including failed/invalid→success recovery | previous/next evaluation-state diff | recovery and `TestRecheckEnqueueUsesPromotionCapableEvaluationChangesOnly` | Implemented |
| D20 | One event links directly to one complete cause set | global/target event-cause transactions | FK, multi-job and restart tests | Implemented |
| D21 | Retain non-completed/referenced/latest completed jobs; clean older unreferenced | housekeeping | `TestRunHousekeepingCleansCompletedUnreferencedJobs` | Implemented |
| D22 | Lease/fencing deferred; all derived writes pass one reconciler transaction boundary | runtime integration | writer-serialization test | Implemented |

## 3. Silence Editor Decisions Q1-Q24

| ID | Contract | Runtime owner | Verification | Status |
|---|---|---|---|---|
| Q1 | One operation targets one Alertmanager target | editor model, submit request | submit/target tests | Implemented |
| Q2 | Logical cluster is target identity; HA members are routing detail | target resolver | cluster-only target tests | Implemented |
| Q3 | One field is one full Alertmanager matcher; structured wire payload; preview is capped at 64 KiB while submit/fresh-read payload limit is independently configurable | frontend parser/backend compiler/request boundaries | shared corpus, preview limit, proxy/direct submit, and fresh-read limit tests | Implemented |
| Q4 | Duplicate label matchers allowed and ANDed | matcher evaluator | matcher-set tests | Implemented |
| Q5 | Preview/autocomplete share target/raw-label population; exact capped counts | lifecycle label index/endpoints | label stats/target tests | Implemented |
| Q6 | Matcher set must not match empty labels universally | backend safety validator | broad-set rejection tests | Implemented |
| Q7 | `Now` computed at submit; edit exact; duration anchor `max(start, submit)` | editor time domain | submit-time/edit tests | Implemented |
| Q8 | Fixed durations are elapsed; month/year calendar-clamped | duration engine | DST/month clamp tests | Implemented |
| Q9 | Strict canonical compound duration grammar | duration parser | valid/invalid table tests | Implemented |
| Q10 | Absolute time uses local zone, exposes UTC, blocks invalid DST time | editor time controls | explicit America/New_York nonexistent/ambiguous tests | Implemented |
| Q11 | Author default priority but editable; auth identity remains separate | initial-state builder/submit audit | author tests | Implemented |
| Q12 | Persist author only after successful operation under operation-specific rules | success handler/local storage | persistence tests | Implemented |
| Q13 | Author/comment required; author trimmed; comment preserved; default payload limit cannot make a valid existing silence uneditable | frontend/backend normalization + `silenceEditor.maxPayloadBytes` | submit validation and large proxy/direct/status payload tests | Implemented |
| Q14 | Structured POST preview; backend owns matching semantics | preview endpoint | API tests | Implemented |
| Q15 | Preview includes target current/history, uses raw labels, reports missing-label exclusions | preview collector/target store | target/raw-label tests | Implemented |
| Q16 | Exact total, page 100, immutable opaque cursor, deterministic order | preview snapshot cache | cursor/cache/order tests | Implemented |
| Q17 | Matched and missing-label evidence is explicit | preview response/renderer | evidence tests | Implemented |
| Q18 | 350 ms valid-input debounce; stale result retained/marked; generation ordering | editor preview coordinator | stale/out-of-order tests | Implemented |
| Q19 | In-form submit progress; preserve data and actionable retry on known failure | editor submit state | submit failure tests | Implemented |
| Q20 | Backdrop ignored; dirty close confirmed; submit blocks navigation; focus lifecycle complete | modal/session guard | guard/focus tests | Implemented |
| Q21 | Success confirms target/ID, refreshes, closes once | success handler | duplicate/success tests | Implemented |
| Q22 | No raw payload preview or shareable link in new runtime | routing/components | App query-param tests | Implemented |
| Q23 | Legacy editor remains buildable/tested but has no runtime route | modal integration/test suites | legacy suites | Implemented |
| Q24 | Desktop fixed form + scrolling result; mobile fullscreen single scroll/sticky actions | editor CSS/modal | five-project visual suite | Implemented |

## 4. Runtime Logging

The normative event, level, field, redaction, noise, and verification requirements
are defined in `runtime-logging-contract.md`.

Status: **Implemented**. Lifecycle startup/poll/transition/resolve, silence
reconciliation and HA edges, recheck jobs/candidates, housekeeping/retention,
regroup, preview/cache/autocomplete, status verification, and both submit routes use
stable structured events and contract levels. Redaction and level behavior have
executable tests. Browser-routed terminal results are intentionally console-only and
best-effort; the correlated server `direct_descriptor_issued` event marks that audit
boundary without claiming success.

## 5. Writer/Reader Inventory

Lifecycle writers:

- `Store.ApplyPollCycle`: live/global/target observation, disappearance, source reconciliation, job enqueue, live causes.
- `Store.RunSilenceRecheckBatch`: target/global promotion CAS, events, recheck causes.
- `Store.ResolveAlert`: global/target manual resolution and events.
- `Store.RunHousekeeping`: lifecycle/silence/job retention and vacuum.
- `Store.RunRegroupBatch`: grouping metadata only; must not change matching/lifecycle dimensions.

Lifecycle readers:

- merged alert endpoints through `dedupedAlertsWithLifecycle`;
- lifecycle stats/warnings;
- silence preview and autocomplete target queries;
- recheck worker pinned-input queries.

Editor writers:

- `SilenceEditorSession` owns operation/navigation generations;
- `SilenceEditor` owns normalized draft and submit generation;
- `/silence-editor/submit` owns normalization, ACL, routing, and mutation outcome;
- success handler owns local author persistence and immediate refresh.

No additional writer or reader may be added without updating this inventory and
adding its invariant/test mapping.

## 6. Transaction And Failure Invariants

1. Poll fetches occur outside the database; one short transaction applies one complete observation.
2. State row, transition event, cause parent, cause members, and job references commit atomically.
3. Failed source fetch updates health only and preserves last-confirmed relations/fingerprint.
4. Stale recheck CAS is a successful no-op, never an error requiring compensating writes.
5. Manual resolve and live reappearance cannot be overwritten by an old recheck job.
6. A mutating silence request is attempted exactly once per user action; ambiguous outcomes block retry in the current modal until close.
7. A failed/stale status request cannot fill or open the compact editor.
8. Process restart may discard preview RAM state, but not job input/progress/domain state.

## 7. Consolidation Evidence

- lifecycle dimensions/topology: `state_machine.go`, table tests and fuzz invariants;
- editor operation/navigation: `SilenceEditorSession.ts`, generation + AbortController;
- D4/D5/D7/D10/D19: config, source reconciliation, consistency/warning and FIFO tests;
- D2/D15/D20: immutable target histories and global aggregate links across jobs/restart;
- parser parity: shared corpus plus isolated Alertmanager v0.33.1 verifier;
- storage lifecycle: fresh schema v1 bootstrap, close/reopen, and foreign-key tests;
- performance: 100k autocomplete ~54 ms/8 KB and preview ~54 ms/44 MB on the
  recorded development CPU (one benchmark iteration, informational budget check);
- replay: committed 1043-group HA traffic artifact passes poll, SQLite target
  projection and label-index verification;
All rows above are implemented or explicitly superseded. Reproducible test,
restart, replay, parser, timezone, performance, build, and browser commands are
recorded in `architecture-verification.md`.
