# Developer Guide

> Internal reference for contributors working on **sqldrift**.

## Prerequisites

- **Python 3.10+**
- **pip** (with `setuptools ≥ 68.0`)

## Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/sqldrift.git
cd sqldrift

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

## Project Structure

```
sqldrift/
├── src/sqldrift/
│   ├── __init__.py          # Public API exports
│   ├── validator.py         # Stateless validate_query() function
│   └── optimized.py         # SchemaValidator & CachedSchemaValidator classes
├── tests/
│   └── test_validator.py    # pytest suite (20 tests across 3 test classes)
├── benchmarks/
│   └── benchmark.py         # Performance benchmarks (4,000-table scale)
├── examples/
│   └── usage_examples.py    # Runnable usage examples
├── pyproject.toml           # Build config, dependencies, metadata
├── .gitignore
├── LICENSE                  # MIT
└── README.md
```

## Architecture

### Core Modules

| Module | Purpose |
|--------|---------|
| `validator.py` | Stateless `validate_query()` — parses SQL via **sqlglot**, extracts physical table references (skipping CTEs), and checks them against a provided table list. |
| `optimized.py` | `SchemaValidator` — class-based validator that pre-computes a normalized `set` for O(1) lookups. `CachedSchemaValidator` — extends it with `functools.lru_cache` for repeated-query speedups (~282×). |

### How Validation Works

1. Parse the SQL string with `sqlglot.parse_one()`
2. Build a scope tree via `sqlglot.optimizer.scope.build_scope()`
3. Walk all scopes and extract table names, skipping CTE sources
4. Normalize names (lowercase by default, optionally schema-qualified)
5. Diff referenced tables against the live table set
6. Return `(True, "Query is safe to execute.")` or `(False, "Schema Drift Detected: ...")`

### Configuration Options (`SchemaValidator`)

| Option | Default | Effect |
|--------|---------|--------|
| `case_sensitive` | `False` | Case-sensitive table name matching |
| `preserve_schema` | `False` | Match full `schema.table` names instead of base name only |

### Schema Sourcing

sqldrift **does not store or connect to databases** — the caller provides the table list at runtime. This is a deliberate design choice to keep the library database-agnostic.

In production, consumers typically source tables from:

| Source | Method |
|--------|--------|
| PostgreSQL | `SELECT table_name FROM information_schema.tables` |
| MySQL | `SHOW TABLES` |
| SQLAlchemy | `inspect(engine).get_table_names()` |
| Static config | JSON/YAML file with table names |
| Data catalog | API call to metadata service |

**Key design implication:** any new feature should maintain this stateless pattern. The library validates queries — it never manages schema state.

## Running Tests

```bash
pytest                  # run all tests
pytest -v               # verbose output
pytest -x               # stop on first failure
pytest -k "Cached"      # run only CachedSchemaValidator tests
```

All tests live in `tests/test_validator.py` and cover three areas:
- **`TestValidateQuery`** — stateless function (10 tests)
- **`TestSchemaValidator`** — class-based validator (7 tests)
- **`TestCachedSchemaValidator`** — cached variant (3 tests)

## Running Benchmarks

```bash
python benchmarks/benchmark.py
```

Benchmarks compare `validate_query()` vs `SchemaValidator` vs `CachedSchemaValidator` at scale (4,000 tables).

## Adding a New Feature

1. **Write code** in `src/sqldrift/validator.py` or `src/sqldrift/optimized.py`
2. **Export** any new public symbols from `src/sqldrift/__init__.py`
3. **Add tests** in `tests/test_validator.py` (follow the existing `TestXxx` class pattern)
4. **Run** `pytest` to verify nothing is broken
5. **Update** `README.md` if the public API changes

## Code Style

- Type hints on all public function signatures
- Docstrings follow **NumPy/Google hybrid** style with `Args:` / `Returns:` sections
- Private methods prefixed with `_`
- Constants and normalized sets use `frozenset` or `set` as appropriate

## Dependencies

| Package | Role |
|---------|------|
| `sqlglot ≥ 20.0` | SQL parsing and dialect support |
| `pytest ≥ 7.0` | Testing (dev only) |

## Common Tasks

| Task | Command |
|------|---------|
| Install for development | `pip install -e ".[dev]"` |
| Run tests | `pytest` |
| Run benchmarks | `python benchmarks/benchmark.py` |
| Run examples | `python examples/usage_examples.py` |
| Build package | `python -m build` |
