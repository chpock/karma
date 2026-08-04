# Spec: Retention, Limits, and Vacuum

## 1. Defaults

Fork default settings:

- TTL: 30 days
- Max stored alerts in `alerts_current`: 100000

TTL window uses `last_seen_at` semantics.

## 2. Cleanup order

Cleanup order is strict:

1. TTL cleanup
2. Limit cleanup (only if count still exceeds max)

## 3. TTL cleanup policy

Delete alerts whose `last_seen_at` is older than configured TTL.

Related history rows handling:

- If history is tied by FK cascade, rows delete automatically.
- If no FK cascade, explicit history prune must run for deleted alert ids.

## 4. Limit cleanup policy

After TTL pass, if `alerts_current` count exceeds max:

- evict oldest alerts by "state changed long ago" ordering.

Ordering source:

- current implementation orders by latest transition timestamp from history:
  `MAX(alert_state_history.changed_at)` per alert.
- fallback for alerts without transition rows is `alerts_current.updated_at`.

This aligns with requirement to remove records that had older state changes first.

## 5. Vacuum policy

SQLite policy in this phase:

- `journal_mode=WAL`
- `auto_vacuum=INCREMENTAL`
- periodic `PRAGMA incremental_vacuum`

Recommended periodic schedule:

- every 6h or 24h depending write volume.

`VACUUM` full rewrite is not routine; reserve for manual maintenance windows.

## 6. Telemetry requirements

Expose at least:

- DB file size bytes.
- counts for `alerts_current` and `alert_state_history`.
- last housekeeping timestamp.
- last incremental vacuum timestamp.

## 7. Failure behavior

If cleanup or vacuum fails:

- log error with context,
- keep serving API,
- retry next scheduled run.

No hard process crash on housekeeping failure.
