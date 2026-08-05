# Spec: Database Migrations Policy

## 1. Purpose

Define a stable, auditable process for evolving the fork SQLite schema over time.

## 2. Versioning model

- `metadata.schema_version` is the authoritative schema version.
- Every schema change increments version by 1 (`v1`, `v2`, ...).
- Version number is tied to the effective schema visible after startup migration.

## 3. Mandatory rules for schema changes

For every DB schema change in this fork:

1. **Code migration step**
   - add startup migration logic that upgrades older DBs to the new version.
2. **Migration document**
   - add one file under `fork-docs/SPEC/db-migrations/` named `vN-to-vN+1.md`.
3. **Schema spec update**
   - update `fork-docs/SPEC/storage-schema.md` to reflect final schema.
4. **Tests**
   - add/adjust tests proving both fresh DB bootstrap and upgrade path work.

No schema-changing PR is complete without all four items.

## 4. Backward compatibility contract

- Running a newer binary against an older DB must auto-upgrade in-place.
- Re-running startup must be idempotent.
- Migration steps must be transactional whenever SQLite supports it.
- If a migration cannot be made safe in-place, it must be documented explicitly
  in the migration file with operator impact and rollback guidance.

## 5. Change categories

### 5.1 Additive changes (preferred)

- new nullable column,
- new column with safe default,
- new index,
- new table.

These should preserve existing reads/writes during upgrade.

### 5.2 Potentially risky changes

- dropping columns/tables,
- type changes,
- semantic reinterpretation of existing data.

These require explicit rationale and operator impact section in migration doc.

## 6. Documentation layout

- Policy (this file): `fork-docs/SPEC/db-migrations.md`
- History entries: `fork-docs/SPEC/db-migrations/vN-to-vN+1.md`

This keeps policy stable while recording every schema step as an append-only log.
