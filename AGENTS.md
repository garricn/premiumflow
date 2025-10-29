# Repository Guidelines

## Project Structure & Module Organization

- `src/premiumflow/` holds package code: `core/` (domain logic), `cli/` (entry points), `services/` & `formatters/` (helpers).
- `tests/` mirrors runtime packages (`tests/services/`, `tests/cli/`, etc.); data fixtures live in `tests/fixtures/`.
- `scripts/` contains CLI helpers (e.g., feedback checks).
- Root configs: `pyproject.toml`, `mypy.ini`, `.pre-commit-config.yaml`, `uv.lock`.

## Build, Test, and Development Commands

- `uv sync --extra dev` – install dependencies + editable package respecting `uv.lock`.
- `uv run pytest` – run the full test suite (append paths for subsets, e.g., `uv run pytest tests/services`).
- `uv run black src tests` – format Python sources.
- `uv run ruff check src tests --fix` – apply lint fixes.
- `uv run mypy --config-file mypy.ini src/premiumflow tests` – static type checking.

## Coding Style & Naming Conventions

- Python uses Black (4 spaces, ~100-char lines) and Ruff; keep imports sorted and remove unused symbols.
- Use Decimal for monetary values; prefer dataclasses with explicit types for new services.
- Constants: UPPER_CASE; functions/variables: snake_case; classes: PascalCase.
- Markdown is formatted with mdformat (`uv run mdformat README.md`); shell scripts with shfmt (`shfmt -w scripts`).

## Testing Guidelines

- Tests rely on pytest; name functions `test_<behavior>` and group by module beneath `tests/<area>/`.
- Include edge cases (positive/negative amounts, commission overrides). Add fixtures to `tests/fixtures/` for shared CSVs.
- Run `uv run pytest` locally before opening a PR; CI enforces the same checks.

## Commit & Pull Request Guidelines

- Commit subjects are imperative and concise (e.g., “Add cash-flow aggregation service”). Run pre-commit hooks before committing.
- PRs should link issues (`Resolves #NN`), summarize changes, and list verification commands. Provide CLI output screenshots when UX changes.
- Use focused branches (`feature/<issue>-short-description`) and rebase/sync with `main` frequently.

## Agent-Specific Instructions

- Sign every public comment, review, or PR note with `— Codex`. Example: “LGTM — Codex”.
- When committing on behalf of the agent, append `Signed-off-by: Codex`.
- Before addressing review feedback, inspect outstanding comments via `gh pr view <number> --json reviews,comments`.

## Security & Configuration Tips

- Never commit secrets; store credentials outside the repo.
- Target Python 3.11; use `uv` rather than raw `pip` to ensure environments match CI.
