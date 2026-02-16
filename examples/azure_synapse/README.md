# Azure Synapse Analytics Integration

Use **sqldrift** with [Azure Synapse Analytics](https://learn.microsoft.com/en-us/azure/synapse-analytics/) to validate SQL queries before execution.

## Prerequisites

```bash
pip install sqldrift pyodbc azure-identity
```

- An Azure Synapse workspace with a dedicated or serverless SQL pool
- Azure AD permissions to access the SQL pool
- [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) installed locally

## How It Works

```
┌──────────────┐     ┌─────────────────┐     ┌───────────────┐
│  SQL Query   │────>│    sqldrift      │────>│  Safe to run?  │
└──────────────┘     │  validates against│     └───────┬───────┘
                     │  Synapse tables   │             │
                     └────────┬─────────┘        Yes ──┤── No
                              │                   │         │
                     ┌────────▼─────────┐   ┌─────▼───┐  ┌──▼──────────┐
                     │  Synapse SQL     │   │ Execute  │  │ Reject with │
                     │  Pool (tables)   │   │ query    │  │ drift error │
                     └──────────────────┘   └─────────┘  └─────────────┘
```

## Authentication Options

### Option A: Azure AD Token (recommended)

```python
import pyodbc
from azure.identity import DefaultAzureCredential

def get_synapse_connection(server: str, database: str) -> pyodbc.Connection:
    """Connect to Synapse using Azure AD token authentication."""
    credential = DefaultAzureCredential()
    token = credential.get_token("https://database.windows.net/.default")
    token_bytes = token.token.encode("utf-16-le")

    conn_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
    )

    conn = pyodbc.connect(conn_string, attrs_before={1256: token_bytes})
    return conn
```

### Option B: SQL Authentication

```python
import pyodbc

def get_synapse_connection(server: str, database: str, user: str, password: str) -> pyodbc.Connection:
    """Connect to Synapse using SQL authentication."""
    conn_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_string)
```

## Quick Start

### 1. Fetch tables from Synapse

```python
def get_synapse_tables(conn: pyodbc.Connection, schema: str = "dbo") -> list[str]:
    """Fetch all table and view names from a Synapse SQL pool."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT TABLE_SCHEMA + '.' + TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
        ORDER BY TABLE_NAME
        """,
        (schema,),
    )
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables
```

### 2. Validate before executing

```python
from sqldrift import SchemaValidator

conn = get_synapse_connection(
    server="my-workspace.sql.azuresynapse.net",
    database="my_sql_pool",
)

live_tables = get_synapse_tables(conn)
validator = SchemaValidator(live_tables, preserve_schema=True)

query = "SELECT u.name, o.total FROM dbo.users u JOIN dbo.orders o ON u.id = o.user_id"
success, msg = validator.validate(query)

if success:
    print("Query is safe -- executing on Synapse...")
else:
    print(f"FAIL: {msg}")
```

### 3. Execute on Synapse (after validation passes)

```python
def execute_on_synapse(conn: pyodbc.Connection, query: str) -> list[dict]:
    """Run a validated query on Synapse and return results as dicts."""
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return results
```

## Full Example

```python
import pyodbc
from azure.identity import DefaultAzureCredential
from sqldrift import CachedSchemaValidator

# --- Configuration ---
SYNAPSE_SERVER = "my-workspace.sql.azuresynapse.net"
SYNAPSE_DATABASE = "my_sql_pool"
SYNAPSE_SCHEMA = "dbo"

# --- Connect with Azure AD ---
credential = DefaultAzureCredential()
token = credential.get_token("https://database.windows.net/.default")
token_bytes = token.token.encode("utf-16-le")

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SYNAPSE_SERVER};"
    f"DATABASE={SYNAPSE_DATABASE};",
    attrs_before={1256: token_bytes},
)

# --- Fetch tables ---
cursor = conn.cursor()
cursor.execute(
    "SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?",
    (SYNAPSE_SCHEMA,),
)
live_tables = [row[0] for row in cursor.fetchall()]
cursor.close()
print(f"Found {len(live_tables)} tables in {SYNAPSE_SCHEMA}: {live_tables[:5]}...")

# --- Create a cached validator ---
validator = CachedSchemaValidator(live_tables, preserve_schema=True, cache_size=256)

# --- Validate and execute ---
queries = [
    "SELECT * FROM dbo.events WHERE event_date > '2025-01-01'",
    "SELECT u.name, COUNT(*) FROM dbo.users u JOIN dbo.orders o ON u.id = o.user_id GROUP BY u.name",
    "SELECT * FROM dbo.nonexistent_table",
]

for query in queries:
    success, msg = validator.validate(query)

    if success:
        print(f"PASS: {msg}")
        # results = execute_on_synapse(conn, query)
    else:
        print(f"FAIL: {msg}")

conn.close()
```

**Expected output:**
```
Found 35 tables in dbo: ['dbo.events', 'dbo.users', 'dbo.orders', 'dbo.products', 'dbo.sessions']...
PASS: Query is safe to execute.
PASS: Query is safe to execute.
FAIL: Schema Drift Detected: The following tables were not found: ['dbo.nonexistent_table']
```

## Multi-Schema Validation

Fetch tables across multiple schemas:

```python
def get_all_tables(conn: pyodbc.Connection) -> list[str]:
    """Fetch all tables across all schemas."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
    )
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

validator = SchemaValidator(get_all_tables(conn), preserve_schema=True)
```

## Serverless SQL Pool

For Synapse serverless (on-demand) SQL pools querying external data (e.g., Data Lake):

```python
# Connect to the serverless endpoint
conn = get_synapse_connection(
    server="my-workspace-ondemand.sql.azuresynapse.net",
    database="my_lake_db",
)

# Serverless pools expose external tables and views
live_tables = get_synapse_tables(conn, schema="dbo")
validator = SchemaValidator(live_tables)
```

## Hot-Swapping Schema

Refresh the validator when tables change:

```python
validator.update_schema(get_synapse_tables(conn))
```

## Required Azure Permissions

| Requirement | Details |
|-------------|---------|
| Azure AD role | `Synapse SQL Administrator` or `Synapse Contributor` |
| Database role | `db_datareader` (minimum for listing and querying) |
| Managed Identity | Supported via `ManagedIdentityCredential` |

## Tips

- **Use `preserve_schema=True`** -- Synapse queries typically use `schema.table` syntax (e.g., `dbo.users`)
- **Azure AD auth is preferred** -- avoid storing SQL passwords; use `DefaultAzureCredential` for flexibility
- **Connection pooling** -- reuse the `pyodbc` connection for both table fetching and query execution
- **Serverless vs. dedicated** -- use the correct endpoint (`-ondemand` suffix for serverless)
- **ODBC driver** -- ensure ODBC Driver 18 is installed (`brew install msodbcsql18` on macOS)
