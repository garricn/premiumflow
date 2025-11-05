# Repository Guidelines

## Development

See [docs/guidelines/development.md](docs/guidelines/development.md) for project structure, coding style, testing, and build commands.
See [docs/guidelines/code-review.md](docs/guidelines/code-review.md) for code review process and GitHub commands.
See [docs/guidelines/commit-pr.md](docs/guidelines/commit-pr.md) for commit and PR guidelines.
See [docs/guidelines/web-ui.md](docs/guidelines/web-ui.md) for comprehensive guidelines on creating new HTML pages and templates.
See [docs/guidelines/cli.md](docs/guidelines/cli.md) for guidelines on creating CLI commands and separating UI logic from business logic.

## Agent-Specific Instructions (Codex)

- Sign every public comment, review, or PR note with `— Codex`. Example: "LGTM — Codex".
- When committing on behalf of the agent, append `Signed-off-by: Codex`.
- Before addressing review feedback, inspect outstanding comments via `gh pr view <number> --json reviews,comments`.
