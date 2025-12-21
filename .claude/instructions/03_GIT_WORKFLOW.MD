# Git & Pre-Commit Workflow

This document defines git practices and pre-commit checks. Read this before making commits.

---

## 1. Pre-Commit Checks

This project does not use automated pre-commit hooks. Instead, Claude is responsible for manually running all checks before any commit.

### Python

```
[ ] ruff check .
[ ] ruff format --check .  (run `ruff format .` to fix)
[ ] pytest tests/
```

### JavaScript / TypeScript

```
[ ] npm run lint:js        (ESLint)
[ ] npm run format:check   (Prettier - run `npm run format` to fix)
```

### CSS

```
[ ] npm run lint:css       (Stylelint)
```

### HTML

```
[ ] npm run lint:html      (HTMLHint)
```

### Quick: Run All Linters

```bash
npm run lint               # JS + CSS + HTML linting
npm run format             # Auto-fix formatting (JS, CSS, HTML)
```

**Do not proceed with a commit until all checks pass cleanly.**

---

## 2. Commit Rules

1. **Claude can commit** - Claude may execute `git commit` after running all pre-commit checks successfully.

2. **Claude must NOT push** - Claude must never execute `git push` without explicit user permission. Always ask before pushing.

3. **Commit often** - Make frequent, small commits after completing logical units of work.

4. **Fix failures** - If checks fail, fix the underlying issues instead of disabling or bypassing them.

---

## 3. Commit Message Style

Use commit messages that reference plan steps when applicable:

```
Phase 1.2: input validation helpers
```

Include a clear summary of what changed and why.

---

## 4. Git Best Practices

1. **Create initial commit** - Start with baseline structure.
2. **Commit often** - After every meaningful step or small batch of changes.
3. **Roll back when needed** - If something breaks, use git to roll back to the last known good commit.

---

## 5. CI/CD Expectations

1. CI/CD pipelines are **not** responsible for running the test suite.
2. All tests must be executed and verified **before committing**.
3. Commits pushed to remote should already be in a **green state** (all tests passing).
4. CI/CD may perform building, packaging, deploying, or lightweight health checks, but must not be the primary mechanism for catching failing tests.

---

## 6. Quick Reference

### Fix Python Formatting

```bash
ruff format .
```

### Fix Frontend Formatting

```bash
npm run format
```

### Check Everything (Python)

```bash
ruff check . && ruff format --check . && pytest tests/
```

### Check Everything (Frontend)

```bash
npm run lint && npm run format:check
```
