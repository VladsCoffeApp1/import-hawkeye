# Verified Commands & Paths

This file contains commands and paths that have been tested and verified to work in this project. **Always reference this file before running commands.**

---

## Project Paths

### Root Directory
```
d:\import-hawkeye
```

### Key Directories
```
d:\import-hawkeye\.claude\                    # Claude instructions
d:\import-hawkeye\.claude\instructions\       # Detailed instruction files
d:\import-hawkeye\app\                        # Application code
d:\import-hawkeye\app\schemas\                # Data type schemas
d:\import-hawkeye\tests\                      # Test suite
```

---

## Python Commands

### Running Python Code
```bash
# CORRECT - Always use uv run
uv run python script.py
uv run pytest tests/
uv run ruff check .
uv run ruff format .

# WRONG - Never use these
python script.py          # Don't use
pytest tests/             # Don't use
pip install package       # Don't use
```

### Package Management
```bash
# Add dependency
uv add <package>

# Add dev dependency
uv add --group dev <package>

# Remove dependency
uv remove <package>

# Sync dependencies
uv sync
```

### Linting & Formatting
```bash
# Check code
uv run ruff check .

# Format code (auto-fix)
uv run ruff format .

# Check formatting without fixing
uv run ruff format --check .
```

### Testing
```bash
# Run all tests
uv run pytest tests/

# Run specific test file
uv run pytest tests/test_example.py

# Run with verbose output
uv run pytest tests/ -v
```

---

## Frontend Commands

### Linting
```bash
# Run all linters
npm run lint

# Individual linters
npm run lint:js          # ESLint
npm run lint:css         # Stylelint
npm run lint:html        # HTMLHint
```

### Formatting
```bash
# Auto-fix formatting
npm run format

# Check formatting without fixing
npm run format:check
```

### Building
```bash
# Build frontend assets
npm run build
```

### Playwright E2E Tests
```bash
# Run smoke tests first (fast fail)
npx playwright test --grep @smoke

# Run full test suite
npx playwright test

# Run specific browser
npx playwright test --project=chromium

# Run with UI
npx playwright test --ui

# Run in headed mode
npx playwright test --headed
```

---

## Git Commands

### GitHub CLI
```bash
# CORRECT - Use gh directly
gh pr list
gh issue create --title "Bug fix"
gh pr create --title "Feature" --body "Description"

# WRONG - Don't use full path
"C:\Program Files\GitHub CLI\gh.exe" pr list    # Don't use
```

### Git Workflow
```bash
# Check status
git status

# Stage changes
git add .

# Commit (after running all pre-commit checks)
git commit -m "message"

# DO NOT push without permission
# git push    # Always ask user first
```

---

## Cloud Function Deployment (CI/CD)

Deployment is automated via GitHub Actions. Push to `main` triggers deployment.

### GitHub Secrets Required
```
GCP_SA_KEY - Service account JSON key for deployment
```

### Set up secrets (one-time)
```bash
gh secret set GCP_SA_KEY < path/to/service-account.json
```

### Manual Trigger (if needed)
```bash
# Trigger workflow manually
gh workflow run deploy.yml
```

### Test the Deployed Function
```bash
# Send a test file
curl -X POST https://europe-west3-soldier-tracker.cloudfunctions.net/import-hawkeye \
    -H "Content-Type: application/octet-stream" \
    --data-binary @test.zip
```

### Run Local File Watcher
```bash
# Configure GCF_URL in project.env first
# GCF_URL=https://europe-west3-soldier-tracker.cloudfunctions.net/import-hawkeye

# Run the watcher (no console window)
uv run pythonw watcher.pyw
```

---

## Pre-Commit Checklist

### Python Projects
```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest tests/
```

### Frontend
```bash
npm run lint
npm run format:check
```

### Quick: Run All Checks
```bash
# Python
uv run ruff check . && uv run ruff format --check . && uv run pytest tests/

# Frontend
npm run lint && npm run format:check
```

---

## Common Issues & Solutions

### Issue: "python: command not found"
**Solution:** Use `uv run python` instead of `python`

### Issue: "pytest: command not found"
**Solution:** Use `uv run pytest` instead of `pytest`

### Issue: "Module not found"
**Solution:** Run `uv sync` to sync dependencies

### Issue: "gh: command not found"
**Solution:** Ensure GitHub CLI is installed and in PATH

### Issue: Playwright tests timeout immediately
**Solution:** Run smoke test first with short timeout to verify setup

---

## Path Reference

### Windows Paths (Current System)
```bash
# Use forward slashes or escaped backslashes
cd "d:/claude"                    # Correct
cd d:\\claude                     # Correct
cd d:\claude                      # May cause issues in some contexts
```

### Relative Paths
```bash
# From project root
.claude/CLAUDE.md
.claude/instructions/01_PYTHON_CONVENTIONS.MD
```

---

## Notes

- **Always verify commands in this file before using them**
- **Update this file when you discover new working commands**
- **Document any path changes or corrections here**
- **If a command fails, check this file first before trying alternatives**
