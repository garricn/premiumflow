# Gemini Code Assistant Context

This document provides context for the Gemini Code Assistant to understand the `premiumflow` project.

## Project Overview

`premiumflow` is a Python-based toolkit for analyzing options trading data, specifically focusing on "roll chains" from CSV transaction exports. It provides both a command-line interface (CLI) and a web-based UI for importing, analyzing, and visualizing options trading history.

The project is structured as a standard Python package with the main source code located in the `src/premiumflow` directory. It uses a SQLite database for data persistence, storing imported transaction data in `~/.premiumflow/premiumflow.db` by default.

## Key Technologies

- **Backend:** Python 3.11+
- **CLI:** `click`
- **Web Framework:** `fastapi`
- **Dependency Management:** `uv`
- **Testing:** `pytest`
- **Formatting & Linting:** `black`, `ruff`, `mypy`

## Building and Running

The project uses `uv` for dependency management and running commands within the project's virtual environment.

### Environment Setup

1. **Install `uv`:** Follow the instructions in the `README.md` to install `uv`.
1. **Sync dependencies:**
   ```bash
   uv sync --extra dev
   ```

### Running the CLI

The main entry point for the CLI is `premiumflow`.

**Common commands:**

- Analyze transactions: `uv run premiumflow analyze transactions.csv`
- Import data: `uv run premiumflow import --json-output`
- List imports: `uv run premiumflow import list`
- Delete an import: `uv run premiumflow import delete 42 --yes`
- Lookup an option: `uv run premiumflow lookup "TSLA 500C 2025-02-21"`
- Trace an option: `uv run premiumflow trace "TSLA $550 Call" all_transactions.csv`
- View matched legs: `uv run premiumflow legs --status open --lots`

### Running the Web UI

The web application is a FastAPI server.

1. **Start the server:**
   ```bash
   uv run uvicorn premiumflow.web.app:create_app --factory --reload
   ```
1. **Access the UI:** Open a web browser and navigate to `http://127.0.0.1:8000`.

### Running Tests

Tests are run using `pytest`.

```bash
uv run pytest
```

## Development Conventions

See [docs/development-guidelines.md](docs/development-guidelines.md) for comprehensive development guidelines including coding style, testing, and project structure.

See [docs/code-review-guidelines.md](docs/code-review-guidelines.md) for code review process and GitHub commands.

See [docs/commit-pr-guidelines.md](docs/commit-pr-guidelines.md) for commit and PR guidelines.

See [docs/web-ui-guidelines.md](docs/web-ui-guidelines.md) for comprehensive guidelines on creating new HTML pages and templates.
