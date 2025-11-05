# Commit & Pull Request Guidelines

## Commit Guidelines

- Commit subjects are imperative and concise (e.g., "Add cash-flow aggregation service").
- Run pre-commit hooks before committing.
- Use focused branches (`feature/<issue>-short-description`) and rebase/sync with `main` frequently.
- DO NOT force push unless specifically requested to.

## Pull Request Guidelines

- PRs should link issues (`Resolves #NN`).
- Summarize changes clearly.
- List verification commands.
- Provide CLI output screenshots when UX changes.
- Use focused branches (`feature/<issue>-short-description`) and rebase/sync with `main` frequently.

## Branch Naming

- Use descriptive branch names: `feature/<issue>-short-description` or `fix/<issue>-short-description`
- Use `chore/` prefix for maintenance tasks like documentation updates

## Pre-commit Hooks

- Install hooks after syncing dependencies: `uv run pre-commit install --hook-type pre-commit --hook-type pre-push`.
- Run `uv run pre-commit run --all-files` to warm caches and verify the checkout before pushing.
- Hooks call `black`, `ruff`, `mypy`, and `mdformat` via `uv`, plus locally installed `shfmt`, `yamlfmt`, and `taplo` binaries.
- A pre-push hook runs `uv run pytest` so CI failures are caught early.
