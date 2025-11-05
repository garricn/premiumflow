# Repository Guidelines

## Development

See [docs/developers/development.md](docs/developers/development.md) for project structure, coding style, testing, and build commands.
See [docs/developers/code-review.md](docs/developers/code-review.md) for code review process and GitHub commands.
See [docs/developers/commit-pr.md](docs/developers/commit-pr.md) for commit and PR guidelines.
See [docs/developers/web-ui.md](docs/developers/web-ui.md) for comprehensive guidelines on creating new HTML pages and templates.
See [docs/developers/cli.md](docs/developers/cli.md) for guidelines on creating CLI commands and separating UI logic from business logic.

## Agent-Specific Instructions (Codex)

- Sign every public comment, review, or PR note with `— Codex`. Example: "LGTM — Codex".
- When committing on behalf of the agent, append `Signed-off-by: Codex`.
- Before addressing review feedback, inspect outstanding comments via `gh pr view <number> --json reviews,comments`.
