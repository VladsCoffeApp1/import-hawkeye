# Python Conventions

This document defines Python-specific conventions for this project. Read this when working on Python code.

---

## 0. Code Design Principles

**Write short, single-responsibility functions that are decoupled for easier testing:**

- **Each function should do ONE thing well** - Break complex operations into smaller, focused functions
- **Keep functions short** - Ideally under 20 lines, definitely under 50
- **Functions should be pure when possible** - No side effects, same input = same output
- **Decouple dependencies** - Pass dependencies as parameters (dependency injection)
- **Avoid tight coupling** - Don't hardcode connections to external services, databases, or global state
- **Design for testability** - If it's hard to test, refactor it into smaller pieces

**Example:**
```python
# BAD: Large function with multiple responsibilities
def process_order(order_id):
    order = db.query(f"SELECT * FROM orders WHERE id={order_id}")
    total = 0
    for item in order['items']:
        total += item['price'] * item['quantity']
    tax = total * 0.1
    final = total + tax
    db.execute(f"UPDATE orders SET total={final} WHERE id={order_id}")
    send_email(order['customer_email'], f"Your total is ${final}")
    return final

# GOOD: Small, focused, testable functions
def calculate_line_total(price: float, quantity: int) -> float:
    """Calculate total for a single line item."""
    return price * quantity

def calculate_order_subtotal(items: list[dict]) -> float:
    """Calculate subtotal for all items."""
    return sum(calculate_line_total(item['price'], item['quantity']) for item in items)

def calculate_tax(subtotal: float, tax_rate: float = 0.1) -> float:
    """Calculate tax amount."""
    return subtotal * tax_rate

def calculate_order_total(subtotal: float, tax: float) -> float:
    """Calculate final total."""
    return subtotal + tax
```

---

## 1. Dependency & Package Management

1. Always use `uv` as the package/dependency manager.
2. To add a dependency, run `uv add <package>`.
3. To remove a dependency, run `uv remove <package>`.
4. Never manually add or remove packages in `pyproject.toml` - use `uv add`/`uv remove`.
5. Tool configuration sections (e.g., `[tool.ruff]`, `[tool.pytest]`) can be edited manually.
6. When you introduce new imports in the code, ensure the required packages are added using `uv add`.
7. When you remove code that uses a package, clean up the dependency with `uv remove` if it is no longer used.

## 1.1 Running Python Code

**CRITICAL: Never use `pip`, `python`, or `pytest` directly. Always use `uv run`.**

- Run Python scripts: `uv run python script.py`
- Run pytest: `uv run pytest tests/`
- Run any Python tool: `uv run <tool>`
- Sync dependencies: `uv sync`

**DO NOT:**
- Use `pip install` - use `uv add` instead
- Use `python script.py` - use `uv run python script.py`
- Use `pytest tests/` - use `uv run pytest tests/`
- Create or use `requirements.txt` - this project uses `pyproject.toml` only

---

## 2. Module Header & Docstring Convention

For standalone scripts and modules, always preserve and use this header pattern:

```python
#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: <filename>.py

"""
```

Rules:
- Replace `<filename>.py` with the actual file name.
- Never remove or significantly change this header unless explicitly instructed.
- Keep the `requires-python` pin and license text exactly as-is.
- Use PEP 287-compliant docstrings throughout the project.

---

## 3. Function Docstring Style

Use reStructuredText/PEP 287-style docstrings with `:param` and `:returns:` fields:

```python
"""
Return the final component for a fully-qualified BigQuery table.

:param full: <project>.<dataset>.<table>
:returns: table
"""
```

Rules:
- Write a short one-line summary in imperative or descriptive form.
- Follow with a blank line, then parameter and return annotations.
- Keep descriptions concise; avoid redundant wording that restates type hints.
- Ensure docstrings stay consistent with actual behavior when functions are modified.

---

## 4. Dev Tooling & `pyproject.toml` Baseline

The project uses this baseline configuration in `pyproject.toml`:

### Dev Dependencies

```toml
[dependency-groups]
dev = [
    "black>=25.1.0",
    "freezegun>=1.5.1",
    "mypy>=1.15.0",
    "pylint>=3.3.6",
    "pytest>=8.3.5",
    "pytest-asyncio>=0.26.0",
    "pytest-html>=4.1.1",
    "pytest-mock>=3.14.0",
    "respx>=0.22.0",
    "ruff>=0.11.4",
]
```

### Tool Configuration

```toml
[tool.black]
line-length = 140
target-version = ["py312"]

[tool.mypy]
python_version = 3.12

[tool.ruff]
target-version = "py312"
line-length = 140

[tool.ruff.lint]
extend-select = ["PL"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

Rules:
- For adding/removing packages, use `uv add`/`uv remove` (with `--group dev` for dev dependencies).
- Tool configuration sections can be edited manually.
- Ensure compatibility with Python 3.12 and line length 140.

---

## 5. Logging

Use structured logging throughout the codebase:

```python
from loguru import logger as log
```

- Add extensive debug-level logging during development.
- Keep logging configurable for verbose debugging in dev vs. reduced output in production.
