# sqldrift

Detect schema drift before your SQL queries blow up.

**sqldrift** validates SQL queries against a live database schema to catch missing tables *before* execution. Built on [sqlglot](https://github.com/tobymao/sqlglot) for robust, multi-dialect SQL parsing.

## Why?

When AI agents or automated pipelines generate SQL, they may reference tables that have been renamed, dropped, or migrated. Running those queries blindly causes production failures. **sqldrift** catches the problem before it happens.

## Features

- 🔍 **Schema drift detection** — identifies missing tables before query execution
- 🧠 **CTE-aware** — correctly ignores Common Table Expressions
- 🔤 **Case-insensitive matching** (configurable)
- 📛 **Schema-qualified name support** — handles `schema.table` references
- ⚡ **Optimized for scale** — class-based validator with O(1) lookups for 4,000+ tables
- 🗄️ **LRU caching** — up to **282x speedup** for repeated queries
- 🗃️ **Multi-dialect** — PostgreSQL, MySQL, BigQuery, and more via sqlglot

## Installation

```bash
pip install .
```

For development:

```bash
pip install -e ".[dev]"
```

## Quick Start

### Simple one-off validation

```python
from sqldrift import validate_query

live_tables = ["users", "orders", "products"]

success, message = validate_query(
    "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    live_tables,
)
# (True, "Query is safe to execute.")

success, message = validate_query(
    "SELECT * FROM deleted_table",
    live_tables,
)
# (False, "Schema Drift Detected: The following tables were not found: ['deleted_table']")
```

### Reusable validator (recommended for large schemas)

```python
from sqldrift import SchemaValidator

validator = SchemaValidator(live_tables)

# Validate many queries — the table set is built once
success, message = validator.validate("SELECT * FROM users")
```

### Cached validator (best for repeated queries)

```python
from sqldrift import CachedSchemaValidator

cached = CachedSchemaValidator(live_tables, cache_size=256)
success, message = cached.validate(query)  # subsequent identical calls are cached
```

## API Reference

### `validate_query(sql_query, live_tables, *, dialect=None)`

Standalone function for one-off validation.

| Parameter     | Type           | Description                                  |
|---------------|----------------|----------------------------------------------|
| `sql_query`   | `str`          | The SQL query to validate                    |
| `live_tables` | `list[str]`    | Tables that exist in the schema              |
| `dialect`     | `str \| None`  | SQL dialect (`"postgres"`, `"mysql"`, etc.)   |

**Returns:** `tuple[bool, str]`

### `SchemaValidator(live_tables, *, case_sensitive=False, preserve_schema=False)`

Class-based validator with pre-computed table lookups.

| Option            | Default  | Description                                          |
|-------------------|----------|------------------------------------------------------|
| `case_sensitive`  | `False`  | Enable case-sensitive table name matching            |
| `preserve_schema` | `False`  | Match full `schema.table` names instead of base name |

**Methods:**

| Method                          | Description                            |
|---------------------------------|----------------------------------------|
| `validate(sql_query, dialect)`  | Validate a query against the schema    |
| `update_schema(live_tables)`    | Hot-swap the schema at runtime         |
| `table_exists(table_name)`      | Check if a specific table exists       |
| `get_table_count()`             | Return the number of registered tables |

### `CachedSchemaValidator`

Extends `SchemaValidator` with LRU caching. Accepts an additional `cache_size` parameter (default: `128`).

**Additional methods:** `clear_cache()`, `get_cache_info()`

## Project Structure

```
sqldrift/
├── pyproject.toml
├── LICENSE
├── README.md
├── src/
│   └── sqldrift/
│       ├── __init__.py        # Public API
│       ├── validator.py       # Core validate_query function
│       └── optimized.py       # SchemaValidator & CachedSchemaValidator
├── tests/
│   └── test_validator.py      # pytest suite (20 tests)
├── examples/
│   └── usage_examples.py
└── benchmarks/
    └── benchmark.py
```

## Performance

Benchmarked with 4,000 tables:

| Method          | Speedup vs function |
|-----------------|---------------------|
| SchemaValidator | ~2.6x faster        |
| CachedValidator | ~282x faster        |

```bash
python benchmarks/benchmark.py
```

## Testing

```bash
pip install -e ".[dev]"
pytest
```

## License

MIT
