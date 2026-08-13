# Spec: Retention, Limits, and Vacuum

## 1. Defaults

Fork default settings:

- Alert TTL: 30 days
- Max stored alerts in `alert_states`: 100000
- Unreferenced silence-history TTL: 30 days
- Housekeeping interval: 24 hours

TTL window uses `last_seen_at` semantics.

## 2. Cleanup order

Cleanup order is strict:

1. TTL cleanup
2. Limit cleanup (only if count still exceeds max)
3. Reference-safe silence/job history cleanup
4. Incremental vacuum after the deletion transaction commits

## 3. TTL cleanup policy

Delete alerts whose `last_seen_at` is older than configured TTL.

Related history rows handling:

- If history is tied by FK cascade, rows delete automatically.
- If no FK cascade, explicit history prune must run for deleted alert ids.

## 4. Limit cleanup policy

After TTL pass, if `alert_states` count exceeds max:

- evict oldest alerts by "state changed long ago" ordering.

Ordering source:

- current implementation orders by latest transition timestamp from history:
  `MAX(alert_events.changed_at)` per alert.
- fallback for alerts without transition rows is `alert_states.updated_at`.

This aligns with requirement to remove records that had older state changes first.

## 5. Vacuum policy

SQLite policy in this phase:

- `journal_mode=WAL`
- `auto_vacuum=INCREMENTAL`
- periodic `PRAGMA incremental_vacuum`

The configured `alertLifecycle.housekeeping.incrementalVacuumInterval` is the
cadence for the complete housekeeping cycle. One cycle runs immediately when the
worker starts and then at the configured interval (default 24h).

`VACUUM` full rewrite is not routine; reserve for manual maintenance windows.

## 6. Telemetry requirements

Expose at least:

- DB file size bytes.
- counts for `alert_states` and `alert_events`.
- last housekeeping timestamp.
- last incremental vacuum timestamp.
- silence-history TTL and recheck queue diagnostics.

## 7. Failure behavior

If cleanup or vacuum fails:

- log error with context,
- keep serving API,
- retry next scheduled run.

No hard process crash on housekeeping failure.
