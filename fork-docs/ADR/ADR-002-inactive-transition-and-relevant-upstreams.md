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

Relevant upstreams are defined by the latest successful live snapshot membership for that alert, not historical all-time membership.

If coverage is incomplete, no inactivity transition is performed.

## Consequences

- Stable behavior during partial outages.
- Avoids permanent inactive blocking after topology changes.
- Requires storing and updating relevant upstream membership per alert.
