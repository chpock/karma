# Silence Editor Session State Machine

## States

`closed`, `checking`, `editing`, `submitting`, `success`, `outcomeUnknown`.

`transitionSilenceEditorSession()` is the navigation/operation contract. React
components may render local progress details, but they cannot independently advance
the operation lifecycle.

## Transition rules

| From | Event | To |
|---|---|---|
| closed | begin create | editing |
| closed/editing | begin external edit/recreate check | checking |
| checking | status success | editing |
| checking | status failure | closed |
| editing | submit | submitting |
| submitting | success | success |
| submitting | known failure | editing |
| submitting | unknown outcome | outcomeUnknown |
| success | close timer/manual close | closed |
| any non-submitting state | close | closed |

Events not listed are rejected by retaining the current state. In particular,
submitting ignores close/replace events. `outcomeUnknown` rejects replacement and
submit events; closing the modal is required before a new mutation.

## Async ownership

`SilenceFormStore` owns one monotonic replacement generation and AbortController.
Starting a newer selection aborts the previous request. Close also aborts and
invalidates it. Only the current generation may fill the form, increment the editor
revision, or open the compact editor.

## Canonical boundaries

- Request-time backend status owns edit versus recreate.
- Backend-normalized payload owns both proxy and direct submission.
- `outcomeUnknown` is terminal for the editor instance; field edits cannot enable a
  second mutation. Closing discards that in-memory terminal state; it is never
  persisted in browser storage.
- Dirty state compares parsed domain matchers and semantic time/metadata fields, not
  presentation whitespace.
- Alertmanager matcher grammar is checked against the shared parser corpus under
  `fork-docs/SPEC/testdata/`.
