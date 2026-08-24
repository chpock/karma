# ADR-003: Storage Mode, Retention Order, and Vacuum Strategy

- Status: Accepted
- Date: 2026-08-03

## Context

Fork needs persistent lifecycle storage with bounded growth and operational simplicity.

## Decision

Storage mode:

- single-replica local SQLite.

Retention policy:

1. TTL cleanup first (default 30 days from `last_seen_at`).
2. apply max-record limit second (default 100000 alerts) if still over cap.
3. retain `queued`, `running`, `retry_wait`, `failed`, and `abandoned` recheck jobs;
   retain completed jobs referenced by global or target suppression-cause history and
   the latest completed job as bounded post-run diagnostic context; delete older
   completed unreferenced jobs during the next housekeeping cycle.
4. prune unreferenced retired sources by `retired_at`, revisions/rules by
   `last_observed_at`, and closed episodes by `ended_at` under the independent
   30-day silence-history TTL.
5. limit eviction order is oldest by state-change age.

Vacuum policy:

- WAL mode,
- incremental auto-vacuum,
- scheduled incremental vacuum,
- the housekeeping/vacuum cycle runs immediately at worker start and then at the
  configured interval (default 24h),
- full vacuum only as manual maintenance.

## Consequences

- Predictable cleanup behavior. Alert rows, older completed unreferenced job graphs,
  and unreferenced silence history are bounded by their existing limit/retention
  mechanisms; `queued`, `running`, `retry_wait`, `failed`, and `abandoned` job graphs
  are deliberately retained without a storage bound in schema v1 for durable queue
  and operator-audit history.
- Any future bound for abandoned-job history requires a separate architecture,
  runtime, and audit-retention decision.
- Simpler operations for initial rollout.
- Multi-replica support explicitly deferred.
