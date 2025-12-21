# Testing Workflow

This document defines testing requirements and TDD practices. Read this when writing or modifying tests.

---

## 1. Pytest Responsibilities

1. For every new feature, function, class, or significant behavior change, generate or update pytest tests.
2. When refactoring existing code, review and update tests to stay aligned with new behavior.
3. Treat pytest as mandatory: code is not "done" until appropriate tests exist and pass.
4. Do not rely on CI/CD to run tests; all tests must pass locally before proposing any commit.
5. If tests fail, do not ignore or work around the failure; fix the tests or implementation.

---

## 2. Test-Driven Development (TDD) Workflow

All work should follow TDD principles:

1. **Write failing tests first** - For each requirement, write tests that define expected behavior before implementation.
2. **Implement minimal code** - Only write production code needed to make tests pass.
3. **Refactor** - Improve structure and readability while keeping tests green.
4. **Cover edge cases** - Every new feature or bugfix needs at least one test covering:
   - Edge cases
   - Error paths
   - Boundary conditions

The test suite is the **authoritative specification** of behavior.

---

## 3. Test Categories

### 3.1 Unit Tests

Cover:
- Core business logic functions
- Data validation and normalization routines
- Non-trivial transformation rules

Requirements:
- Use small, focused test cases
- Avoid external dependencies (network, filesystem) unless required
- Use clear, deterministic inputs and expected outputs

### 3.2 Golden-File / Snapshot Tests

When output structures are complex or sensitive to regression:
- Maintain fixture input files for typical and edge-case scenarios
- Run the full application against these fixtures
- Compare resulting output against stored canonical outputs

### 3.3 Integration / End-to-End Tests

Exercise the full execution path:

**For CLI-style entrypoints:**
- Invoke script with fixture files
- Assert on exit codes, stdout, stderr

**For API-style entrypoints:**
- Send requests to the service
- Assert on status codes, response bodies, headers

### 3.4 Reproducibility & Determinism

- Tests must not depend on network access unless explicitly required
- Avoid reliance on current time, random numbers, or external state
- If time or randomness is required, inject and mock it in tests

---

## 4. Running Tests

### Local Test Commands

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/api/test_rbac.py

# Run with verbose output
pytest tests/ -v

# Run Playwright E2E tests
npx playwright test --project=chromium
```

### Pre-Commit Testing

Before any commit, ensure:
```
[ ] pytest tests/    # All tests pass
```

---

## 4.1 Playwright Testing Strategy: Smoke Test First

**CRITICAL: Always run a quick smoke test before running the full test suite to catch issues early.**

When writing or running Playwright tests, follow this two-phase approach:

### Phase 1: Smoke Test (Quick Validation)
Run a very short test with a brief timeout (2-5 seconds) to verify basic functionality:

```javascript
// Smoke test - verifies page loads and basic elements exist
test('smoke test: page loads', async ({ page }) => {
  test.setTimeout(5000);  // 5 second timeout
  await page.goto('/');
  await expect(page.locator('h1')).toBeVisible();
});
```

**Purpose:**
- Catch configuration errors early (wrong URL, server not running, etc.)
- Verify basic page structure exists
- Fail fast if environment is broken
- Save time by not running full test suite against a broken setup

### Phase 2: Full Test (Complete Validation)
Only after smoke test passes, run the full test with normal timeout:

```javascript
// Full test - runs after smoke test passes
test('complete user flow', async ({ page }) => {
  test.setTimeout(30000);  // 30 second timeout
  await page.goto('/');
  // ... complete test steps ...
});
```

**Test Organization:**
```javascript
test.describe('Feature Name', () => {
  // Always run smoke test first
  test('smoke: basic page load', async ({ page }) => {
    test.setTimeout(5000);
    await page.goto('/feature');
    await expect(page.locator('.main-content')).toBeVisible();
  });

  // Full tests run after smoke test passes
  test('complete workflow', async ({ page }) => {
    test.setTimeout(30000);
    // ... detailed test steps ...
  });
});
```

**Benefits:**
- **Early problem detection** - Know within seconds if environment is broken
- **Time savings** - Don't waste 30+ seconds waiting for a test that will obviously fail
- **Clear failure signals** - Smoke test failures indicate setup issues, not test logic issues
- **Confidence** - Smoke test pass means environment is ready for full testing

**Run smoke tests first:**
```bash
# Run just smoke tests (tag them with @smoke)
npx playwright test --grep @smoke

# Then run full suite if smoke tests pass
npx playwright test
```

---

## 5. Database-Related Projects

For any code that interacts with a database:

1. Clearly define expected database schema in code or migrations.
2. Add startup/initialization logic to create tables if they don't exist.
3. Run integration tests against a controlled database instance.
4. Reset schema as part of test setup.
5. Verify database operations behave correctly with proper schema and fail gracefully when misconfigured.
