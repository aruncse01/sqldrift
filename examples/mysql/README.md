# MySQL Integration

Use **sqldrift** with [MySQL](https://www.mysql.com/) to validate SQL queries before execution.

## Prerequisites

```bash
pip install sqldrift mysql-connector-python
```

- A running MySQL instance (5.7+ or 8.0+)
- Database credentials with `SELECT` access to `information_schema`

## Quick Start

### 1. Fetch tables from MySQL

```python
import mysql.connector

def get_mysql_tables(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int = 3306,
) -> list[str]:
    """Fetch all table and view names from a MySQL database."""
    conn = mysql.connector.connect(
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
        (database,),
    )
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables
```

Alternatively, use `SHOW TABLES` for a simpler approach:

```python
def get_mysql_tables_simple(conn) -> list[str]:
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables
```

### 2. Validate before executing

```python
from sqldrift import SchemaValidator

live_tables = get_mysql_tables(
    host="localhost",
    database="mydb",
    user="myuser",
    password="mypassword",
)
validator = SchemaValidator(live_tables)

query = "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
success, msg = validator.validate(query, dialect="mysql")

if success:
    print("Query is safe -- executing...")
else:
    print(f"FAIL: {msg}")
```

### 3. Execute on MySQL (after validation passes)

```python
def execute_on_mysql(conn, query: str) -> list[dict]:
    """Run a validated query and return results as dicts."""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    return results
```

## Full Example

```python
import mysql.connector
from sqldrift import CachedSchemaValidator

# --- Configuration ---
MYSQL_CONFIG = {
    "host": "localhost",
    "database": "analytics",
    "user": "myuser",
    "password": "mypassword",
    "port": 3306,
}

# --- Connect and fetch tables ---
conn = mysql.connector.connect(**MYSQL_CONFIG)
cursor = conn.cursor()
cursor.execute("SHOW TABLES")
live_tables = [row[0] for row in cursor.fetchall()]
cursor.close()
print(f"Found {len(live_tables)} tables: {live_tables[:5]}...")

# --- Create a cached validator ---
validator = CachedSchemaValidator(live_tables, cache_size=256)

# --- Validate and execute ---
queries = [
    "SELECT * FROM events WHERE event_date > '2025-01-01'",
    "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
    "SELECT * FROM nonexistent_table",
]

for query in queries:
    success, msg = validator.validate(query, dialect="mysql")

    if success:
        print(f"PASS: {msg}")
        # results = execute_on_mysql(conn, query)
    else:
        print(f"FAIL: {msg}")

conn.close()
```

**Expected output:**
```
Found 12 tables: ['events', 'users', 'orders', 'products', 'sessions']...
PASS: Query is safe to execute.
PASS: Query is safe to execute.
FAIL: Schema Drift Detected: The following tables were not found: ['nonexistent_table']
```

## Multi-Database Validation

MySQL uses `database.table` syntax for cross-database queries:

```python
def get_tables_from_databases(conn, databases: list[str]) -> list[str]:
    cursor = conn.cursor()
    placeholders = ", ".join(["%s"] * len(databases))
    cursor.execute(
        f"SELECT table_schema, table_name FROM information_schema.tables "
        f"WHERE table_schema IN ({placeholders})",
        databases,
    )
    tables = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
    cursor.close()
    return tables

validator = SchemaValidator(
    get_tables_from_databases(conn, ["analytics", "warehouse"]),
    preserve_schema=True,
)
```

## Using PyMySQL as an Alternative Driver

```bash
pip install PyMySQL
```

```python
import pymysql

conn = pymysql.connect(
    host="localhost", database="mydb", user="myuser", password="mypassword",
)
cursor = conn.cursor()
cursor.execute("SHOW TABLES")
live_tables = [row[0] for row in cursor.fetchall()]
cursor.close()
```

## Hot-Swapping Schema

```python
cursor = conn.cursor()
cursor.execute("SHOW TABLES")
validator.update_schema([row[0] for row in cursor.fetchall()])
cursor.close()
```

## Using with SQLAlchemy

```python
from sqlalchemy import create_engine, inspect

engine = create_engine("mysql+mysqlconnector://user:password@localhost:3306/mydb")
live_tables = inspect(engine).get_table_names()
validator = SchemaValidator(live_tables)
```

## Tips

- **Use `dialect="mysql"`** -- ensures correct parsing of MySQL-specific syntax (e.g., backtick quoting, `LIMIT` placement)
- **`SHOW TABLES` vs `information_schema`** -- `SHOW TABLES` is simpler; `information_schema` gives more control (filter by type, cross-database)
- **Driver choice** -- `mysql-connector-python` is the official Oracle driver; `PyMySQL` is a pure-Python alternative
- **Connection pooling** -- use SQLAlchemy or `mysql.connector.pooling` for high-throughput use cases
- **Read-only credentials** -- table-fetching queries only need `SELECT` on `information_schema` (or `SHOW TABLES` privilege)
