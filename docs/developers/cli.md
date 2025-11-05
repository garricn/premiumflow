# CLI Development Guidelines

## Separation of Concerns

CLI commands should focus on **user interface and presentation logic only**. All business logic should be implemented in shared code within `core/` or `services/` modules.

### What CLI Commands Should Do

- **Parse command-line arguments** using Click decorators
- **Validate input** (format, file existence, etc.)
- **Call services** from `services/` or `core/` to perform business logic
- **Format output** for display (tables, JSON, etc.)
- **Handle errors** and present user-friendly messages
- **Coordinate flow** between services and formatters

### What CLI Commands Should NOT Do

- **Business logic** (calculations, data transformations, matching algorithms)
- **Data persistence** (database operations should be in services/repository layer)
- **Complex algorithms** (these belong in `core/` or `services/`)
- **Data validation beyond format checks** (business validation belongs in services)

## Architecture Pattern

```
CLI Command (cli/)
  ├── Parse arguments (Click)
  ├── Validate input format
  ├── Call Service (services/) ← Business logic lives here
  │   ├── Uses Core (core/) ← Domain models and logic
  │   └── Uses Repository (persistence/) ← Data access
  ├── Format output (formatters/ or Rich tables)
  └── Display results
```

## Example: Good CLI Command Structure

```python
@click.command()
@click.option("--account-name", required=True)
def my_command(account_name: str) -> None:
    """Command description."""
    console = Console()
    
    # Input validation (format only)
    if not account_name.strip():
        ctx.fail("Account name cannot be empty")
        return
    
    try:
        # Call service for business logic
        result = my_service.process_account(account_name.strip())
        
        # Format and display (presentation logic)
        table = _build_results_table(result)
        console.print(table)
    except MyServiceError as e:
        ctx.fail(f"Error: {e}")
```

## Shared Code Location

- **Domain Logic** → `core/` (models, domain rules, business entities)
- **Business Services** → `services/` (orchestration, complex operations, reusable business logic)
- **Data Access** → `persistence/` (repository pattern, database operations)
- **Formatting** → `formatters/` or inline in CLI for presentation-specific formatting

## Benefits

- **Testability**: Business logic can be tested independently of CLI
- **Reusability**: Same services can be used by CLI, web UI, and other interfaces
- **Maintainability**: Changes to business logic don't require CLI changes
- **Separation of Concerns**: UI logic separate from business logic

## Testing CLI Commands

- Test that CLI commands call the correct services with correct parameters
- Test input validation and error handling
- Test output formatting
- **Do NOT test business logic in CLI tests** - test business logic in service/core tests
- Mock services when testing CLI to focus on CLI-specific behavior
