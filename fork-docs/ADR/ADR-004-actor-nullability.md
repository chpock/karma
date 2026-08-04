# ADR-004: Nullable Resolve Actor

- Status: Accepted
- Date: 2026-08-03

## Context

Manual resolve may happen in deployments with authentication disabled.
Using a fixed placeholder (for example "anonymous") can be ambiguous and misleading.

## Decision

Persist `changed_by` as nullable:

- authenticated user available -> store username.
- no authenticated user context -> store `NULL`.

UI may render `NULL` explicitly (for example gray styled) where actor is displayed.

## Consequences

- Clear data semantics without conflating unknown/unauthenticated identities.
- Consumers must handle null actor value.
