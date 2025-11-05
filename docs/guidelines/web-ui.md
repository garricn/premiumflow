# Web UI Development Guidelines

When creating new HTML pages or templates for the web UI:

## Always Use Shared CSS

- **Never add inline `<style>` blocks** to HTML templates. All styles must be in `src/premiumflow/web/static/site.css`.
- **Always include the shared stylesheet**: `<link rel="stylesheet" href="{{ url_for('static', path='site.css') }}" />`
- **Add dark mode support**: When adding new CSS classes, include dark mode variants using `@media (prefers-color-scheme: dark)`.

## Template Structure

All HTML templates should follow this structure:

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{{ title }}</title>
    <link rel="stylesheet" href="{{ url_for('static', path='site.css') }}" />
  </head>
  <body>
    <header>
      <h1>{{ title }}</h1>
      <nav class="top-nav">
        <!-- Navigation links -->
      </nav>
    </header>
    <main>
      <!-- Page content -->
    </main>
  </body>
</html>
```

## Adding New Styles

1. **Add styles to `site.css`**, not inline in templates
1. **Include dark mode variants** in the `@media (prefers-color-scheme: dark)` block
1. **Use existing color palette** from the design system (see existing CSS for color values)
1. **Test in both light and dark modes** before submitting PRs

## Example: Adding a New Component

```css
/* In site.css */
.my-new-component {
  background-color: #ffffff;
  border: 1px solid #d1d5db;
  border-radius: 0.5rem;
  padding: 1rem;
}

@media (prefers-color-scheme: dark) {
  .my-new-component {
    background-color: #1f2937;
    border-color: #374151;
  }
}
```

## Why This Matters

- **Consistency**: Shared CSS ensures all pages have consistent styling and dark mode support
- **Maintainability**: Centralized styles are easier to update and maintain
- **User Experience**: Proper dark mode support respects user system preferences
- **Avoid Technical Debt**: Inline styles create maintenance issues and duplicate code (see Issue #173)
