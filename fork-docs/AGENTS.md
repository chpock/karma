# Fork Agent Rules

This file defines additional rules for AI/code agents working in this fork.

## 1. Fork-first merge safety

This repository is periodically synchronized with upstream.

Primary rule:

- Minimize changes to original upstream files.

Implementation preference order:

1. Add new fork-specific files/modules.
2. Add small, explicit integration hooks in existing files only when required.
3. Avoid large refactors in upstream-owned files unless explicitly requested.

## 2. Isolate fork behavior

For fork-only features:

- Keep logic in fork-specific packages/directories where possible.
- Avoid mixing unrelated cleanup/refactoring with feature work.
- Keep diffs narrow and intention-revealing.

## 3. Preserve upstream contracts

Unless fork specs explicitly change behavior:

- preserve upstream API contracts,
- preserve existing config semantics,
- preserve existing runtime defaults.

Any intentional contract change must be documented in `fork-docs/SPEC/`.

## 4. Document before broad implementation

For non-trivial fork features:

- update docs in `fork-docs/` first (or in same change),
- record key decisions in `fork-docs/ADR/`.

## 5. Conflict-minimizing edit style

- Prefer additive changes.
- Avoid reformatting unrelated blocks.
- Do not rename/move upstream files without necessity.
- Keep import/order churn minimal unless required.

## 6. Testing focus

- Test new fork behavior directly.
- Avoid touching unrelated tests only for style/cleanup.
- Add focused tests near changed behavior.

## 7. Scope guardrails for this fork phase

Current lifecycle feature scope includes:

- persistent alert lifecycle storage,
- `inactive` and manual `resolved` handling,
- history tracking,
- retention/vacuum operations,
- minimal UI support in existing grid/filter workflows.

Out of scope for current phase (unless explicitly requested):

- multi-replica shared-write architecture,
- advanced dedup identity redesign,
- large UI redesign for retrospective views.
