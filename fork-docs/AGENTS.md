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

### Required verification commands

Run verification from the repository root unless a command explicitly changes
directory:

```bash
# Mandatory after every code change. This runs Go lint/format analysis and then
# the UI ESLint, TypeScript, Stylelint, and Knip gates.
make lint

# Mandatory after Go changes.
go test ./... -timeout 600s

# Mandatory after UI changes.
npm --prefix ui test -- --runInBand <affected test files>
npm --prefix ui run build

# Mandatory patch-integrity checks before reporting completion.
git diff --check
```

Additional rules:

- `make lint` means the root command executed in the repository root. Do not
  report `make -C ui lint` or `make lint` executed inside `ui/` as the root lint
  gate.
- Run root `make lint` after the final code edit, not only before or midway
  through a task.
- A lint failure in changed or accumulated fork code is unfinished work. Fix it
  before starting another feature; do not dismiss it as unrelated without an
  explicit user decision.
- After silence-editor backend or frontend changes, also run the focused backend,
  compact-editor, retained legacy-editor, and lifecycle tests documented by the
  feature specification.
- Browser/visual tests must use local mocks or fixtures only. Agents must not
  access real Alertmanager instances, databases, or other real services.

### UI color review gate

For every UI change that adds or changes a visible color, background, border,
shadow, opacity, badge, button, focus state, or status indicator, agents must
review the rendered result in both the light and dark themes before reporting
completion. Do not assume that a Bootstrap variable is suitable because of its
name: theme overrides, selector specificity, inherited color, opacity, and the
actual surrounding background can change the final result.

The review must cover every affected state that exists in the UI, including as
applicable: resting, hover, focus, active, disabled, loading, selected,
success, warning, and error. For each state, verify and record in the task
summary:

- the actual foreground/background/border colors after rendering;
- contrast status against the real composited background, distinguishing normal
  text, secondary hints, and non-text UI boundaries;
- semantic meaning: neutral, primary action, unavailable, progress, success,
  warning, or error must remain visually unambiguous in both themes.

Use an isolated visual fixture or regression test that renders the relevant
states in both themes. A visual baseline that does not actually contain the
changed state is not evidence. If a design intentionally departs from WCAG
guidance (for example, subdued placeholder text), document the user-approved
target and verify that target instead.

## 7. Scope guardrails for this fork phase

Current lifecycle feature scope includes:

- persistent alert lifecycle storage,
- `inactive` and manual `resolved` handling,
- history tracking,
- silence-aware inactive-to-suppressed reclassification and causal evidence,
- retention/vacuum operations,
- compact create/edit/recreate silence editor with automatic preview,
- structured runtime logging,
- UI support in existing grid/filter workflows.

Out of scope for current phase (unless explicitly requested):

- multi-replica shared-write architecture,
- advanced dedup identity redesign,
- large UI redesign for retrospective views.

## 8. Fork docker publish workflow policy

For branch-level auto-publish on `karma-history`:

- publish linux/amd64 image only,
- do not enable arm64 build in this workflow,
- keep tag format as `<base-version>-ck-<short-sha>`.

Reason:

- arm64 cross-build through QEMU can stall in Go build stage and make branch CI unstable.

## 9. Docker bind-mount user mapping (local development)

When running Docker containers with a bind-mounted project workspace,
always run the container as the host user:

```bash
docker run --rm --user "$(id -u):$(id -g)" -v <host_path>:<container_path> ...
```

Reason:

- running as container `root` creates root-owned files in the mounted tree,
- root-owned artifacts (for example `ui/node_modules/.vite-temp`) later break
  local commands with permission errors (`EACCES`),
- this applies to Playwright/image update runs and any build/test tooling
  executed inside containers.

Never run a host-side UI command concurrently with a container command that runs
`npm ci` against the same bind-mounted `ui/` directory. `npm ci` replaces
`ui/node_modules`; a concurrent host build/lint/test can observe a half-installed
tree and fail with missing binaries. Run the Docker command first, then host-side
verification sequentially.
