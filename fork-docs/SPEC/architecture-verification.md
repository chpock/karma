# Architecture Verification

This file records reproducible gates for the lifecycle/silence-editor architecture.
It is evidence, not a replacement for the contracts in the state-machine specs.

## Domain and storage

```sh
go test ./internal/fork/alertlifecycle
go test ./internal/config
go test ./cmd/karma -timeout 600s
```

Coverage includes pure/fuzz lifecycle invariants, target topology, sticky multi-job
causes, FIFO failure blocking, HA inconsistency, fresh schema v1 bootstrap/reopen,
restart, and the recorded traffic replay.

Effective suppression explainability is covered by:

- `TestRunSilenceRecheckBatchPromotesInactiveAndAddsCause` for pinned-rule details;
- `TestListCurrentAlertsForMergeReturnsLatestLiveSuppressionCause` for a live cause
  retained after disappearance;
- `TestListCurrentAlertsForMergeReturnsUnresolvedCauseWithoutInventedDetails` for
  identity-only replication-lag evidence;
- `TestMergeLifecycleStoredAlertsRestoresSuppressionCauseForCard` for the backend
  alert API boundary;
- the retained-evidence case in
  `ui/src/Components/Grid/AlertGrid/AlertGroup/Alert/index.test.tsx` for read-only
  card rendering without Edit/Delete actions.

Invalid silence entries are covered at every decision boundary:

- mapper: `TestSilencesSkipsInvalidEntriesIndependently`, duplicate-ID/field,
  invalid-UTF-8, unknown-extra-field, optional-field, and malformed-array tests;
- lifecycle current-set/history behavior: `TestUnknownSilenceStatusMeansConfirmedAbsence`;
- HA episode closure: `TestSilenceEpisodeClosesOnlyAfterConfirmedHAWideAbsence`.

These tests enforce the normative rule that an invalid individual silence is absent
from a successful source snapshot while valid neighbors continue. A structurally
malformed top-level array fails the complete refresh.

## Runtime logging

```sh
go test ./internal/log
go test ./internal/alertmanager
go test ./internal/fork/alertlifecycle
go test ./cmd/karma -timeout 600s
cd ui
npm test -- --runInBand \
  src/Components/SilenceModal/SilenceEditor.test.tsx \
  src/Components/Toast/AppToasts.test.tsx
```

Coverage includes mandatory component/event fields, level filtering, lifecycle and
silence transitions, source failure/recovery, HA disagreement/convergence, recheck
execution, Resolve, preview/cache, server-routed outcomes, browser-routed correlated
console events, and sensitive-data redaction. The normative event inventory and
field rules are in `runtime-logging-contract.md`.

## Canonical matcher parity

```sh
cd tools/matcher-corpus
go run . ../../fork-docs/SPEC/testdata/alertmanager-matcher-corpus.json
cd ../../ui
npm test -- SilenceEditor.matcher-corpus.test.ts --runInBand
```

The isolated tool pins Alertmanager v0.33.1 without adding it to Karma's runtime
dependency graph.

## Timezone contract

```sh
cd ui
TZ=America/New_York npm test -- SilenceEditor.timezone.test.ts --runInBand
```

This explicitly verifies rejection of a nonexistent spring-forward wall time and the
earlier offset for an ambiguous fall-back wall time.

## Performance budgets

```sh
go test ./internal/fork/alertlifecycle -run '^$' \
  -bench BenchmarkTargetLabelValueStats100k -benchtime=1x -benchmem
go test ./cmd/karma -run '^$' \
  -bench BenchmarkSilenceEditorPreview100k -benchtime=1x -benchmem
```

Reference run on Intel Core Ultra 5 125H:

- target label values, 100k rows: ~54 ms indexed; ~133 ms with one stale live
  alert overlay, both ~8-16 KB allocated in measured query;
- silence preview, 100k candidates/50k matches: ~54 ms, ~44 MB allocated.

The preview cache enforces a 100k-record per-result cap; broader requests fail with
HTTP 422 rather than exceeding the memory budget.

## UI and browser

```sh
cd ui
make lint
npm test -- --runInBand
npm run build
```

Run Playwright in the official image with host user mapping and never concurrently
with host npm commands against the same bind-mounted `ui/node_modules`.

## Production build boundary

`npm run build` uses `ui/tsconfig.build.json`, rooted at `src/index.tsx`, so test,
e2e, fixture, and matcher-corpus files are not production build dependencies. Root
`make lint` uses the full `ui/tsconfig.json` and still type-checks those files.

The Docker Node stage therefore copies only `ui/`; it does not copy
`fork-docs/SPEC/testdata` into the production build context.

## Release gate

Before release, run the current commands rather than copying historical test counts:

```sh
make lint
go test ./... -timeout 600s
npm --prefix ui test -- --runInBand
npm --prefix ui run build
git diff --check
```

During development, a focused Jest command may be run before the full UI suite, for
example `npm --prefix ui test -- --runInBand path/to/file.test.tsx`. It does not
replace the full release command above.

For compact-editor layout/interaction changes, also run the official Playwright
image across Chromium, Firefox, WebKit, iPhone, and Android. Matcher, timezone,
recorded replay, and 100k performance commands above remain specialized gates for
their corresponding contracts.
