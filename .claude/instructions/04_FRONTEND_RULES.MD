# Frontend Rules (JS/CSS/HTML)

This document defines frontend code quality standards. Read this when working on JavaScript, CSS, or HTML files.

---

## 1. Quality Expectations

Treat JavaScript, CSS, and HTML as first-class code with the same quality expectations as Python.

---

## 2. Toolchain

Use a standard modern toolchain for frontend quality checks:

| Tool | Purpose |
|------|---------|
| ESLint | JavaScript linting and best-practice rules |
| Prettier | Formatting for JavaScript, CSS, and HTML |
| TypeScript (optional) | Static analysis in `allowJs`/`checkJs` mode for `.js` files |

---

## 3. Configuration Files

Ensure the project contains appropriate configuration files:

- `eslint.config.*` or `.eslintrc.*` - ESLint configuration
- `prettier.config.*` or `.prettierrc.*` - Prettier configuration
- `tsconfig.json` - TypeScript configuration (if using TypeScript checks)

---

## 4. ESLint Configuration

Configure ESLint to run on all relevant frontend files:
- Application JavaScript (e.g., `src/**/*.js`)
- Any inline or auxiliary JS files used by the frontend

---

## 5. Prettier Configuration

Configure Prettier to format, at minimum:
- All `.js` files
- All `.css` files
- All `.html` (or templated HTML) files

---

## 6. Pre-Commit Integration

ESLint and Prettier must be part of the check workflow:
- ESLint runs on staged/relevant frontend files; commit is blocked on lint errors.
- Prettier runs on staged/relevant frontend files; formatting issues are auto-fixed where possible.

---

## 7. Rules for Adding/Modifying Files

When adding new frontend files or modifying existing ones:
- Ensure they conform to active ESLint and Prettier rules before they are considered complete.
- Fix linter/formatter problems; do not disable rules or ignore diagnostics unless explicitly instructed.

---

## 8. Configuration Philosophy

When introducing or changing ESLint/Prettier/TypeScript configuration:
- Keep the ruleset "sane default"
- Prefer broadly accepted, community-standard presets (e.g., recommended rulesets)
- Avoid extremely restrictive or unusual rules unless explicitly requested

---

## 9. Design System

When editing HTML/CSS/JS that affects UI, respect the project's design system.

See: [docs/DESIGN_SYSTEM.md](../DESIGN_SYSTEM.md)

---

## 10. Building Frontend Assets

```bash
# Rebuild JS bundles
npm run build
```
