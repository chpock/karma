# Karma History Fork User Guide

This fork extends Karma so users can work with alerts that are currently reported by
Alertmanager and alerts that have disappeared but still need attention. It also adds
a compact editor for creating and updating Alertmanager silence rules.

The default configuration enables these features. Deployment settings are documented
in [Fork Configuration](CONFIGURATION.md).

## 1. Alert states

The grid uses five states:

| State | Meaning |
| --- | --- |
| `active` | Alertmanager currently reports the alert as active. |
| `unprocessed` | Alertmanager currently reports the alert but has not fully processed it. |
| `suppressed` | The alert is silenced now, or it was retained as suppressed after disappearing. |
| `inactive` | Alertmanager no longer reports the alert, but it has not been manually resolved. |
| `resolved` | A user explicitly resolved a past alert. |

The default grid shows active, unprocessed, suppressed, and inactive alerts. Resolved
alerts remain available but are hidden by default so the main grid stays focused on
current and unresolved work.

## 2. Finding current and past alerts

Use the normal filter bar:

- `@state=inactive` shows past alerts that still need attention.
- `@state=resolved` shows manually resolved alerts.
- `@state=suppressed` shows both current and retained suppressed alerts.
- `@is_live=true` shows alerts currently reported by Alertmanager.
- `@is_live=false` shows retained past alerts.

State badges and storage statistics split suppressed alerts into current and past
counts.

## 3. What happens when an alert disappears

Karma retains the alert instead of removing it from the grid. It marks an alert as
past only after all relevant configured Alertmanager sources were checked
successfully and no longer report it. A failed or unavailable source does not make an
alert inactive; Karma keeps the last confirmed state until it has complete data.

The resulting state depends on the last confirmed alert information:

- an active or unprocessed alert becomes `inactive`;
- a suppressed alert remains `suppressed`;
- a previously resolved alert stays resolved until it fires again.

If an inactive alert is later proven to match a complete set of active silence rules,
Karma can change it to suppressed. It never makes this change when required source
data is incomplete or contradictory.

If the same alert fires again, its live Alertmanager state becomes authoritative. A
previous manual resolution is cleared and suppression is recalculated from current
data.

## 4. Resolving a past alert

To resolve an alert:

1. Open the menu on an inactive alert.
2. Select **Resolve this alert**.

Resolve is always a user decision. Karma never automatically marks an alert resolved.
A live alert cannot be resolved. While the request is running, the menu shows
**Resolving...**; a failure remains visible as **Resolve failed**. Resolving an already
resolved past alert again through the backend is a harmless no-op.

Resolved alerts can be found with `@state=resolved`. If a resolved alert fires again,
it returns to the state currently reported by Alertmanager.

## 5. Understanding retained suppression evidence

A past suppressed alert shows **Retained suppression evidence** on its card. This is
a read-only explanation of why Karma considers that historical alert suppressed.

When complete rule details were available, the card shows:

- logical Alertmanager target and physical source;
- silence rule ID;
- time window;
- Author and Comment;
- matchers.

During Alertmanager replication lag, Karma may know the exact silence ID before the
rule details are available. In that case the card shows the known identity and says
that details were unavailable. It does not invent missing matchers or metadata.

This evidence describes the past alert. It does not provide Edit or Delete actions
for a current Alertmanager rule. Editing, expiry, or deletion of the rule does not
rewrite why an already retained alert was suppressed during that past episode.

## 6. Creating and updating silence rules

Every normal silence action opens the compact editor. There is no Browse tab in this
editor.

The basic workflow is:

1. Select an Alertmanager target when the action did not already select one.
2. Enter one matcher expression per field.
3. Choose start and end times.
4. Enter Author and Comment.
5. Review the automatically updated **Affected alerts** preview.
6. Select **Create silence**, **Update silence**, or **Recreate silence**.

On confirmed success, Karma refreshes its data and closes the modal. Editing an
existing rule uses fresh Alertmanager data and refuses to save when the original rule
changed after the editor was opened.

See [Compact Silence Editor](SILENCE_EDITOR.md) for matcher syntax, autocomplete,
time controls, Edit/Recreate behavior, safe retry rules, and editor limitations.

## 7. Warnings and incomplete data

A persistent lifecycle warning means Karma cannot currently prove that its stored
view is fully consistent. Typical causes are:

- background regrouping is still running or failed;
- Alertmanager members disagree about silence rules;
- a silence recheck job failed and blocks later jobs.

Karma keeps serving the last confirmed data and does not invent transitions while
required information is incomplete. The warning disappears after the underlying
condition is resolved.

If one silence object returned by Alertmanager is malformed, Karma reports a warning,
skips that object, and continues processing valid neighboring rules. If the complete
JSON response is malformed, Karma cannot safely separate entries, so the refresh
fails and the previous confirmed snapshot is retained.

## 8. Current limitations

- Lifecycle storage currently supports one Karma replica with local SQLite.
- The compact editor has no production silence-rule Browser.
- Silence annotations from newer Alertmanager versions are not displayed or edited.
- There is no dedicated alert timeline or full history explorer.
- Legacy silence share links are ignored.

Planned work and accepted limitations are listed in
[Future Features](FUTURE_FEATURES.md).

## 9. More documentation

User and operator documentation:

- [Compact Silence Editor](SILENCE_EDITOR.md)
- [Fork Configuration](CONFIGURATION.md)
- [Future Features](FUTURE_FEATURES.md)

Technical documentation for maintainers:

- [Architecture](ARCHITECTURE.md)
- [Implementation plan](IMPLEMENTATION_PLAN.md)
- [State model](SPEC/state-model.md)
- [API contracts](SPEC/api-contracts.md)
- [UI contracts](SPEC/ui-contracts.md)
- [Storage schema](SPEC/storage-schema.md)
- [Silence editor specification](SPEC/silence-editor-redesign.md)
- [Runtime logging contract](SPEC/runtime-logging-contract.md)
- [Architecture conformance](SPEC/architecture-conformance.md)
- [Architecture verification](SPEC/architecture-verification.md)
- [Architecture decisions](ADR/)

The documents in `fork-docs/` are the source of truth for fork-only behavior. A code
change that alters these contracts must update the corresponding documentation.
