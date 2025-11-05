# Repository Guidelines

## Development Guidelines

See [docs/development-guidelines.md](docs/development-guidelines.md) for project structure, coding style, testing, and build commands.

## Code Review Process

See [docs/code-review-guidelines.md](docs/code-review-guidelines.md) for code review process and GitHub commands.

## Commit & Pull Request Guidelines

See [docs/commit-pr-guidelines.md](docs/commit-pr-guidelines.md) for commit and PR guidelines.

## Web UI Development

See [docs/web-ui-guidelines.md](docs/web-ui-guidelines.md) for comprehensive guidelines on creating new HTML pages and templates.

## CLI Development

See [docs/cli-guidelines.md](docs/cli-guidelines.md) for guidelines on creating CLI commands and separating UI logic from business logic.

## Agent-Specific Instructions (Codex)

- Sign every public comment, review, or PR note with `— Codex`. Example: "LGTM — Codex".
- When committing on behalf of the agent, append `Signed-off-by: Codex`.
- Before addressing review feedback, inspect outstanding comments via `gh pr view <number> --json reviews,comments`.
