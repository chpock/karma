# ADR-002: Inactive Transition and Relevant Upstream Semantics

- Status: Accepted
- Date: 2026-08-03

## Context

A single alert can be seen from multiple upstreams. Missing upstream data can lead to wrong inactivity transitions.
Historical "ever seen" upstream sets create stale dependencies after routing changes.

## Decision

An alert may transition to `inactive` only when:

1. it is absent in the current live snapshot, and
2. all relevant upstreams were successfully polled in the current cycle.

Relevant sources are physical source names plus logical targets from the latest
complete successful observation, not historical all-time membership. Partial
coverage preserves previously required configured members. Explicitly removed or
reassigned sources stop participating; a target with no configured members is
retired without inventing a remote state transition.

If coverage is incomplete, no inactivity transition is performed.

## Consequences

- Stable behavior during partial outages.
- Avoids permanent inactive blocking after topology changes.
- Requires storing and updating relevant upstream membership per alert.
- Requires per-target lifecycle rows because one alert fingerprint may have
  different liveness or suppression in different logical targets.
