# Agent Files Structure Template

This document describes the standard structure for agent-specific configuration files.

## Standard Structure

All agent files should follow this structure:

1. **Title** - Agent-specific name
1. **Quick Rules** (optional) - Important reminders (no force push, ignore other files, etc.)
1. **Identity & Signature** - How this agent should sign its work
1. **Development Guidelines** - References to shared developer documentation
1. **Project Context** (optional) - Project overview for agents that need it

## Section Details

### Title Format

- Use: `# {Agent Name} Agent Guidelines` or `# {Agent Name} Code Assistant Context`
- Examples:
  - `# Codex Agent Guidelines`
  - `# Cursor Agent Guidelines`
  - `# Gemini Code Assistant Context`

### Quick Rules Section

- Use bullet points for important reminders
- Keep concise (2-3 items max)
- Examples:
  - `- DO NOT FORCE PUSH unless specifically asked to`
  - `- IGNORE the AGENTS.md file at project root`

### Identity & Signature Section

Standard format:

```markdown
## Identity & Signature

- **Scope**: Sign PR reviews, issue comments, and automated PR/thread notes. Do not include the signature in PR or issue titles.
- **Signature format**: `— {Agent Name}`
- **Commit footer**: `Signed-off-by: {Agent Name}` (when committing via automation)
- **Example**: "Looks good to merge — {Agent Name}"
```

### Development Guidelines Section

Standard format with consistent order:

```markdown
## Development Guidelines

See [docs/developers/development.md](docs/developers/development.md) for project structure, coding style, testing, and build commands.
See [docs/developers/code-review.md](docs/developers/code-review.md) for code review process and GitHub commands.
See [docs/developers/commit-pr.md](docs/developers/commit-pr.md) for commit and PR guidelines.
See [docs/developers/web-ui.md](docs/developers/web-ui.md) for comprehensive guidelines on creating new HTML pages and templates.
See [docs/developers/cli.md](docs/developers/cli.md) for guidelines on creating CLI commands and separating UI logic from business logic.
```

### Project Context Section (Optional)

Only include if the agent needs project overview context. Use consistent structure:

```markdown
## Project Context

[Project overview, technologies, building/running commands]
```
