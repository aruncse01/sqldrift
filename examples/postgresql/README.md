# PostgreSQL Integration

Use **sqldrift** with [PostgreSQL](https://www.postgresql.org/) to validate SQL queries before execution.

## Prerequisites

```bash
pip install sqldrift psycopg2-binary
```

- A running PostgreSQL instance
- Database credentials with `SELECT` access to `information_schema`

## Quick Start

### 1. Fetch tables from PostgreSQL

```python
import psycopg2

def get_postgres_tables(
    host: str,
    database: str,
    user: str,
    password: str,
    schema: str = "public",
    port: int = 5432,
) -> list[str]:
    """Fetch all table and view names from a PostgreSQL schema."""
    conn = psycopg2.connect(
        host=host, database=database, user=user, password=password, port=port,
    )
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY table_name
        """,
        (schema,),
    )
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables
```

### 2. Validate before executing

```python
from sqldrift import SchemaValidator

live_tables = get_postgres_tables(
    host="localhost",
    database="mydb",
    user="myuser",
    password="mypassword",
)
validator = SchemaValidator(live_tables)

query = "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
success, msg = validator.validate(query, dialect="postgres")

if success:
    print("Query is safe -- executing...")
else:
    print(f"FAIL: {msg}")
```

### 3. Execute on PostgreSQL (after validation passes)

```python
def execute_on_postgres(conn, query: str) -> list[dict]:
    """Run a validated query and return results as dicts."""
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return results
```

## Full Example

```python
import psycopg2
from sqldrift import CachedSchemaValidator

# --- Configuration ---
PG_CONFIG = {
    "host": "localhost",
    "database": "analytics",
    "user": "myuser",
    "password": "mypassword",
    "port": 5432,
}
SCHEMA = "public"

# --- Connect and fetch tables ---
conn = psycopg2.connect(**PG_CONFIG)
cursor = conn.cursor()
cursor.execute(
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema = %s AND table_type IN ('BASE TABLE', 'VIEW')",
    (SCHEMA,),
)
live_tables = [row[0] for row in cursor.fetchall()]
cursor.close()
print(f"Found {len(live_tables)} tables in {SCHEMA}: {live_tables[:5]}...")

# --- Create a cached validator ---
validator = CachedSchemaValidator(live_tables, cache_size=256)

# --- Validate and execute ---
queries = [
    "SELECT * FROM events WHERE event_date > '2025-01-01'",
    "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
    "SELECT * FROM nonexistent_table",
]

for query in queries:
    success, msg = validator.validate(query, dialect="postgres")

    if success:
        print(f"PASS: {msg}")
        # results = execute_on_postgres(conn, query)
    else:
        print(f"FAIL: {msg}")

conn.close()
```

**Expected output:**
```
Found 12 tables in public: ['events', 'users', 'orders', 'products', 'sessions']...
PASS: Query is safe to execute.
PASS: Query is safe to execute.
FAIL: Schema Drift Detected: The following tables were not found: ['nonexistent_table']
```

## Connection via URL

```python
conn = psycopg2.connect("postgresql://user:password@localhost:5432/mydb")
```

## Multi-Schema Validation

```python
def get_all_tables(conn, schemas: list[str]) -> list[str]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT table_schema || '.' || table_name FROM information_schema.tables "
        "WHERE table_schema = ANY(%s)",
        (schemas,),
    )
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

validator = SchemaValidator(
    get_all_tables(conn, ["public", "staging"]),
    preserve_schema=True,
)
```

## Hot-Swapping Schema

```python
# Refresh when tables change
cursor = conn.cursor()
cursor.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
)
validator.update_schema([row[0] for row in cursor.fetchall()])
cursor.close()
```

## Using with SQLAlchemy

```python
from sqlalchemy import create_engine, inspect

engine = create_engine("postgresql://user:password@localhost:5432/mydb")
live_tables = inspect(engine).get_table_names(schema="public")
validator = SchemaValidator(live_tables)
```

## Tips

- **Use `dialect="postgres"`** -- ensures correct parsing of PostgreSQL-specific syntax (e.g., `::` casts, `ILIKE`)
- **Use `psycopg2-binary`** -- avoids compiling C extensions; use `psycopg2` in production if preferred
- **Connection pooling** -- use `psycopg2.pool` or SQLAlchemy's pool for high-throughput use cases
- **Read-only credentials** -- the table-fetching queries only need `SELECT` on `information_schema`
