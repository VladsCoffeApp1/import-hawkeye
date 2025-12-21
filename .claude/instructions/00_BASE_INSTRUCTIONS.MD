# Universal Pre-Instructions for Structured Applications with Automated Tests

This document defines **pre-instructions** that apply to any project. Project-specific instructions will be provided separately and must be followed **in addition to** this document.

When an LLM (or LLM-based agent system) receives this document on its own (without additional project-specific instructions yet), its first task is **not** to start coding, but to:

1. Carefully read the entire document.
2. Build an explicit understanding of the document's structure (sections, responsibilities, workflows).
3. Write down that understanding in a concise structural summary or outline.

Only after this structural understanding has been documented should the LLM proceed to apply these pre-instructions to any subsequent, project-specific requirements that follow.

This is a **project-agnostic template** that describes how any software project should be specified when you want a model to:

* Take **structured input**
* Apply **deterministic processing and validation**
* Produce **structured output**
* Be backed by a **comprehensive automated test suite** using a **test-driven development (TDD)** approach

It is intentionally neutral with respect to domain (no geometry, no mapping, no specific data format). You can reuse this as a blueprint for any future project.

---

## Autonomous Mode

Do not ask clarifying questions. Make reasonable assumptions and proceed with the task. Only stop to ask if:
- The request is fundamentally ambiguous with no reasonable default
- An action could cause irreversible damage (e.g., deleting production data)

---

## 0. Planning, Orchestration, and Git Workflow (Instructions to the LLM)

Before writing or modifying any code, you must:

1. Generate a clear, multi-step, multi-phase plan for the entire project.

   * Break work into small, explicit phases (analysis, design, implementation, tests, refactors, documentation, CI/CD wiring, etc.).
   * Write this plan into a project file (for example `PLAN.md` or `progress.md`).

2. Act as a conductor/orchestrator of multiple focused agents

   * Break the work into **small, self-contained responsibility areas**, each handled by its own conceptual agent.
   * Treat these agents as **parallel, narrowly scoped workers**, each solving only one well-defined piece of the problem.
   * Give every agent **only the minimal context** required for its task — no full project dumps, no unnecessary surrounding files, no global history.
   * Maintain the **global understanding** yourself; agents operate with restricted local slices.
   * After each agent completes its part, **review, integrate, and unify** all outputs into a consistent state before proceeding.
   * Continuously update the overall plan based on the integrated results, and only then decide which agent to activate next and what context it should receive.

   Additional orchestration rule: Parallel assistance through multiple sessions

   * You may instruct the human to open additional Claude windows or sessions and paste specific instructions into them when parallel execution would materially speed up the work or reduce context pressure.
   * Use this option only when it provides a clear advantage; otherwise continue orchestrating within the current session.

3. Follow the plan step by step.

   * Before starting a new step, briefly restate which step you are on.
   * Update the plan document as steps are completed, adjusted, or split.

4. Use git from the very beginning.

   * Create an initial commit with the baseline structure.
   * Commit early and often, after every meaningful step or small batch of changes.
   * Include commit messages that reference the plan steps (e.g. `Phase 1.2: input validation helpers`).

5. If something breaks or a direction turns out to be wrong, use git to roll back to the last known good commit and adjust the plan accordingly.

6. Use a top-level `docs/` directory for all project documentation.

   * Store high-level design notes, architecture descriptions, progress logs, and any other written artifacts in `docs/`.
   * Maintain a dedicated TODO document inside `docs/` (for example `docs/TODO.md`) listing all tasks to be performed and keeping it up to date as work progresses.

This planning, orchestration, documentation, and git workflow applies to every project using this template.

### 0.1 Plan Structure for Parallel Execution

All implementation plans must be structured for **maximum parallelization**:

1. **Break tasks into small, independent units**
   - Each task should be completable in isolation
   - Tasks should modify different files or non-overlapping sections
   - Aim for tasks that take 5-15 minutes each

2. **Declare dependencies explicitly**
   - Mark tasks with `Dependencies: None` when they can run immediately
   - Mark tasks with `Dependencies: A1, A2` when they require prior work
   - Group independent tasks together for parallel execution

3. **Run multiple agents simultaneously**
   - Launch all independent tasks in parallel using multiple agents
   - Only wait for dependencies when explicitly required
   - Review and integrate results after each parallel batch

4. **Task format in plans**
   ```
   #### TaskID: Short description
   **File:** `path/to/file.ext` (Lines X-Y if applicable)
   **Dependencies:** None | TaskID, TaskID
   **Effort:** Small | Medium | Large

   [Specific instructions]
   ```

---

## 1. Objective

In any project that follows this template, the implementation should:

1. Accept well-defined **input data structures**.
2. Transform or analyze these inputs according to clearly specified **business or domain rules**.
3. Produce a single, well-specified **output structure**.
4. Provide an **execution interface** (CLI, API, worker, etc.) that cleanly separates:

   * Normal output
   * Errors and diagnostics
5. Include a **comprehensive automated test suite**:

   * Unit tests for core logic
   * Integration / end-to-end tests
   * Golden-file / snapshot tests where appropriate
6. Follow a strict **TDD workflow**, where tests define behavior before implementation.

This template does not constrain what the project does; it constrains **how** it is specified, implemented, and validated.

---

## 2. Application Interface Requirements

This section defines generic expectations for how the application is invoked and how it communicates.

### 2.1 Input Interface

For each project, define **how** the application or service receives structured input, without assuming a specific technology:

* Clearly specify the primary entrypoint(s) for data (for example: function calls, API requests, event messages, configuration files, etc.).
* Document the expected structure, types, and constraints of the incoming data.
* Ensure that input handling is well-isolated so it can be exercised in automated tests.

The exact mechanism (CLI, HTTP API, message queue, etc.) can vary by project, but the contract for the input data must always be explicit and validated.

### 2.2 Output and Validation Requirements

* Normal execution must emit **only the primary structured output** on the main channel (stdout, HTTP response body, etc.).
* Do **not** mix logs, debug messages, or human-readable chatter into the structured output.
* All diagnostic, error, or debug messages must go to a **separate channel**, such as:

  * stderr for CLI
  * HTTP error responses for APIs
  * A dedicated logging sink for services

The implementation must reject invalid input and explain why. Examples of fatal validation failures:

* Missing required fields
* Empty collections where at least one item is required
* Wrong data types
* Invalid values (e.g., out-of-range, wrong format)

On validation or parsing error:

* Exit (or respond) with a **non-zero status code** or error status.
* Write a clear, human-readable error message to the error channel.
* Do **not** write partial or invalid structured output to the main output channel.

---

## 3. Processing & Business Logic

This section describes how to specify and implement domain logic in a reusable way.

### 3.1 Domain Model

For each project, define a clear **domain model**:

* Primary input entities (e.g., "items", "records", "events").
* Any derived or intermediate structures (e.g., "aggregates", "summaries", "results").
* Relationships between entities.

The implementation should:

* Represent these entities using appropriate data structures.
* Avoid embedding business rules directly into input/output parsing code.

### 3.2 Processing Rules

Define the processing in terms of deterministic rules, for example:

* How inputs are validated and normalized.
* How mappings, aggregations, or transformations are computed.
* How conflicts or ambiguous cases are resolved.

The implementation must:

* Keep processing logic **pure and testable** where possible.
* Avoid reliance on external state, randomness, or network calls unless explicitly required.

### 3.3 Output Construction

Define the output specification independently of any internal representation, for example:

* Overall structure (e.g., object with `summary`, `details`, `errors` arrays, etc.).
* Required and optional fields in the output.
* How input entities and parameters are reflected back in the output (e.g., IDs and references).

The implementation must:

* Construct outputs that are **consistent, deterministic, and fully specified**.
* Ensure outputs are valid according to the chosen format (for example, valid JSON, valid XML, valid GeoJSON, etc.).

---

## 4. Error Handling Rules

Error handling must be **predictable and deterministic**.

* All malformed inputs or processing failures must produce a well-defined error outcome.
* Error responses must be **structured and human-readable enough** to debug quickly.
* Under no circumstances should normal output be mixed with error diagnostics.

For CLI-based projects:

* Use **exit code 0** for success.
* Use **non-zero exit codes** for any failure.
* Write errors to stderr only.

For API-based projects:

* Use appropriate HTTP status codes.
* Return a structured error payload describing the problem.

### 4.1 Fail-Early / Crash-Hard Rules

These rules govern how errors must be handled in code and by agents:

1. **Never hide errors.** No silent failures.
2. **Avoid try/except** unless a specific, required recovery exists.
3. **Invalid inputs or impossible states** must immediately raise.
4. **All assumptions must be checked;** missing config crashes.
5. **No fallback behavior** masking problems.
6. **Logs give context** but must not swallow exceptions.
7. **Tests must expect failures** for invalid input.
8. **If unsure, fail and ask;** never guess.

---

## 5. Automated Testing Requirements

Every project using this template must come with a comprehensive test suite. Use **pytest** as the primary testing framework and test runner for all automated tests.

### 5.1 Unit Tests

Unit tests should cover:

* Core business logic functions.
* Data validation and normalization routines.
* Any non-trivial transformation rules.

They should:

* Use small, focused test cases.
* Avoid external dependencies (network, filesystem) unless explicitly required.
* Use clear, deterministic inputs and expected outputs.

### 5.2 Golden-File / Snapshot Tests

Where output structures are moderately complex or sensitive to regression, use **golden-file** (snapshot) tests:

* Maintain fixture input files representing typical and edge-case scenarios.
* Run the full application (or main processing entrypoint) against these fixtures.
* Compare the resulting structured output against stored canonical outputs.

These tests should verify:

* Overall structure
* Presence and types of key fields
* Stability of important values

When outputs change intentionally, update the golden files in a controlled way.

### 5.3 Integration / End-to-End Tests

Add tests that exercise the full execution path:

* For CLI-style entrypoints: invoke the script or binary with fixture files or stdin, assert on:

  * Exit codes
  * Content of stdout
  * Content of stderr
* For API-style entrypoints: send requests to the service, assert on:

  * Status codes
  * Response bodies
  * Headers where relevant

The LLM (and its orchestrated agents) should, whenever possible, actually **run** the application and the test suite (for example via `pytest` or equivalent commands) in the local environment to:

* Confirm that the code compiles or imports without errors.
* Ensure that tests pass end-to-end.
* Detect integration issues early (for example, misconfigured paths, environment variables, or missing dependencies).

Include tests for both **success cases** and **failure cases**.

### 5.4 Reproducibility & Determinism

* Tests must not depend on network access unless explicitly part of the requirements.
* Avoid reliance on current time, random numbers, or external mutable state.
* If time or randomness is required, inject it in a way that can be controlled or mocked in tests.

### 5.5 Database-Related Projects

For any project that interacts with a database, the LLM must ensure that:

* The expected database schema (databases, schemas, tables, indexes) is clearly defined in code or migrations.
* The application and tests include logic or setup steps to **create the necessary tables if they do not exist**, or to apply migrations before running.
* Integration and end-to-end tests run against a controlled database instance (for example, a local test database) where the schema is created or reset as part of the test setup.
* Tests verify that database operations behave correctly when the schema is present and fail in a controlled way when misconfigured.

---

## 6. Test-Driven Development Workflow

All work should follow TDD principles:

1. For each functional requirement (parsing, validation, transformation, output, interface behavior), first write one or more **failing tests** that define the expected behavior.
2. Only then implement the minimal production code needed to make those tests **pass**.
3. Refactor to improve structure, readability, and maintainability while keeping the test suite green.
4. Ensure that any new feature or bugfix is accompanied by at least one new or updated test.
5. Explicitly cover:

   * Edge cases
   * Error paths
   * Boundary conditions for parameters and inputs

The test suite is the **authoritative specification** of behavior.

---

## 7. Implementation Expectations

When applying this template to a specific project, the developer (and orchestrated agents) should:

1. Propose a **clean module structure**, typically separating:

   * Input parsing / validation
   * Core domain logic (pure processing functions where possible)
   * Output construction / serialization
   * Execution interface (CLI or API entrypoint)
   * Tests

2. Provide complete source code for:

   * The main application modules
   * The entire test suite
   * Any fixture input and expected-output files used by tests

3. Ensure that:

   * The main output is always valid according to the chosen format.
   * Errors are always reported consistently and separately.
   * The system is portable and does not rely on environment-specific quirks unless explicitly documented.

4. Use structured logging throughout the codebase (when using Python):

   * Import logging as `from loguru import logger as log`.
   * Add extensive debug-level logging during development, especially around input parsing, validation, decision points, and error conditions.
   * Keep logging configurable so that verbose debugging can be enabled during development and reduced in production without changing code paths.

5. Keep the implementation:

   * Deterministic
   * Well-structured
   * Easy to extend and refactor with confidence, thanks to the test suite.

6. For any project that uses a database:

   * Provide a clear definition of the required tables and other schema objects.
   * Add startup or initialization logic (or migrations) that ensures these tables exist before the main application logic runs.
   * Ensure tests cover the schema-creation path so missing tables are detected early rather than failing at runtime in production.

7. For Python projects, manage dependencies with **uv**:

   * Use `uv` as the primary tool for environments and dependency management instead of `pip`.
   * Use `uv add` to add dependencies and `uv remove` to remove them.
   * Prefer `pyproject.toml` (and any `uv` lock files) as the single source of truth for Python dependencies.
   * Avoid introducing `requirements.txt` or raw `pip install` commands unless a specific project explicitly requires them.

---

## 8. CI/CD Expectations

1. CI/CD pipelines must **not** be responsible for running the test suite.
2. All tests (unit, integration, end-to-end, golden-file, etc.) must be executed and verified by the LLM/agents **before committing** any changes.
3. Commits pushed to remote repositories should already be in a **green state** (all tests passing). CI/CD may treat test failures in pushed code as a process violation, not a normal feedback mechanism.
4. CI/CD may perform tasks such as building, packaging, deploying, or running lightweight health checks, but must not be relied upon as the primary mechanism for catching failing tests.
5. The LLM/agents are fully responsible for ensuring that the codebase remains in a tested and consistent state at each commit.

---

## 9. Path and Instruction Maintenance

### 9.1 Self-Updating Paths and Commands

If a command, script, import, or tool fails because of a path problem:

1. Call out the failure explicitly, including the failing path and command.
2. Search for the correct location instead of guessing.
3. Update the canonical instructions to reflect the actual, working path
   (for example `COMMANDS.md` and any affected docs).
4. Retry using the corrected path.

Never silently ignore path errors or keep using known-wrong paths.

### 9.2 Do Not Dodge Issues

When a problem, inconsistency, or failing check is detected:

1. Call out the problem explicitly.
2. Propose a concrete fix or remediation plan.
3. Apply the fix when it is within the scope of the current work and
   consistent with these instructions.
4. If it is out of scope, describe it and propose a follow-up task
   (for example an entry in `docs/todo` or a GitHub issue).

This applies to, for example:

- Path errors and missing files
- Failing tests
- Linter or formatter errors
- Database or schema drift
- Documentation that is out of date with the code

---

## 10. Issue Tracking and Project Management

**All feature plans and bug fixes are tracked in GitHub Issues:**
- Repository: https://github.com/VladsCoffeApp1/vladscoffee1.0/issues
- Plans in `docs/todo/PLAN_*.md` are documented as GitHub issues
- Use GitHub issues for tracking progress, discussions, and milestones

**When creating new features or fixing bugs:**
1. Check if a GitHub issue exists first
2. Create a new issue if needed with proper labels (enhancement, bug, etc.)
3. Reference the issue number in commits and pull requests
4. Link related plan files in the issue description

---

This template is designed to be reused across **any future project**, regardless of domain. You only need to plug in:

* The specific input schema
* The project’s domain rules and processing logic
* The desired output schema

Everything else (interface discipline, validation philosophy, testing strategy, dependency management, orchestration model, and TDD workflow) stays the same.
