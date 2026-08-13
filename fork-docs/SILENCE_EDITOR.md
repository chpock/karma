# Compact Silence Editor

This guide explains how to create, edit, and recreate Alertmanager silence rules
with the compact editor. For alert states, past alerts, Resolve, filters, and
warnings, see the [main user guide](README.md).

The complete automatic preview requires `alertLifecycle.enabled: true`, which is the
default. Deployment settings are documented in
[Fork Configuration](CONFIGURATION.md).

## 1. Creating a silence

Choose **New silence** or use an alert/group silence action. The compact editor opens
with one logical Alertmanager target.

1. Select the target when it is not already fixed by the entry point.
2. Enter one Alertmanager matcher expression per field, for example
   `service=~"api|payments"`.
3. Choose Starts and Ends. Starts defaults to Now and Ends defaults to one hour.
4. Enter the required Author and Comment.
5. Review **Affected alerts**, which updates automatically.
6. Select **Create silence**.

All matchers are combined with AND. The editor supports `=`, `!=`, `=~`, and `!~`,
label/value autocomplete, keyboard selection, and RE2-compatible validation.
Submission requires at least one matcher that does not match an empty value, which
prevents an accidental match-all silence.

Preview includes current and retained alerts for the selected target. It marks
matched labels, shows when a matcher succeeded because a label was missing, and
reports alerts excluded because their original matching labels are unavailable.

## 2. Time controls

Starts can be Now or an exact local date/time. Ends can be a preset duration, a
strict custom duration, or an exact date/time.

Examples of custom duration:

- `90m`
- `1w2d6h30m`
- `3mo`
- `1y`

Whitespace, decimals, signs, repeated units, and out-of-order units are rejected.
Days and weeks are fixed elapsed durations. Months and years are calendar additions
and clamp to the last valid day of the destination month.

The UI displays the local timezone and resulting UTC time. A nonexistent local time
during a daylight-saving jump is rejected; an ambiguous time uses the earlier
offset and displays that choice.

## 3. Editing and recreating a silence

Edit preserves the exact existing start/end timestamps unless you change them.
Recreate is used for an expired silence: it creates a new rule without reusing the
expired ID, starts at submission time, and preserves the original positive duration.

Before opening and again before saving, Karma reads the rule from one deterministic
Alertmanager member. If the rule, route, or active/expired state changed, saving is
blocked and the editor asks you to close and reopen it. This prevents known stale
edits. Alertmanager does not offer an atomic conditional-update API, so a very small
race between Karma's final check and Alertmanager's write remains possible.

Silence annotations introduced by newer Alertmanager versions are not supported by
this preview editor. They are not shown or editable, and Edit/Recreate may remove or
overwrite them. Planned support is recorded in
[Future Features](FUTURE_FEATURES.md).

## 4. Submission results and safe retry behavior

One click causes at most one Alertmanager write attempt. Karma does not retry a
different HA member after a write failure because creating a silence is not
idempotent.

- A known validation or Alertmanager rejection leaves the form open for correction
  and retry.
- If the request may have succeeded but Karma cannot prove the result, the editor
  reports an unknown outcome and disables retry in that modal. Karma does not claim
  either success or failure; close the modal before starting another operation.
- A successful operation shows the target and returned silence ID, refreshes data,
  and closes the modal.

Depending on deployment configuration, Karma either sends the final request through
its backend or gives the validated request to the browser for direct submission.
The user workflow is the same. Browser-routed failures are also written to the
browser developer console with a correlation ID because Karma cannot observe that
final network request server-side.

## 5. Closing and accessibility

Cancel, the header close button, and Escape share the same unsaved-change check.
Closing a changed form asks for confirmation. Closing is blocked while submission is
in progress. Backdrop clicks do not close the modal.

Focus moves into the modal, remains trapped while it is open, and returns to the
control that opened it. Matcher autocomplete supports keyboard navigation and
assistive-technology combobox semantics. Mobile uses a full-screen, single-scroll
layout with sticky header and actions.

## 6. Current limitations

- No production silence-rule Browser; existing external Edit/Recreate actions remain
  available.
- No silence-annotation support in the compact editor.
- Legacy silence share links are ignored.

See [Future Features](FUTURE_FEATURES.md) for deferred work.
