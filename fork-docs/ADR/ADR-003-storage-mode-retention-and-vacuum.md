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
3. prune unreferenced silence/job history under its independent 30-day TTL while
   preserving current/referenced rows.
4. limit eviction order is oldest by state-change age.

Vacuum policy:

- WAL mode,
- incremental auto-vacuum,
- scheduled incremental vacuum,
- the housekeeping/vacuum cycle runs immediately at worker start and then at the
  configured interval (default 24h),
- full vacuum only as manual maintenance.

## Consequences

- Predictable cleanup behavior and bounded disk usage.
- Simpler operations for initial rollout.
- Multi-replica support explicitly deferred.
