# AWS Glue Catalog + Athena Integration

Use **sqldrift** with [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html) to validate Athena queries before execution.

## Prerequisites

```bash
pip install sqldrift boto3
```

- An AWS account with Glue Data Catalog configured
- IAM permissions: `glue:GetTables`, `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`
- AWS credentials configured (`aws configure`, env vars, or IAM role)

## How It Works

```
┌──────────────┐     ┌─────────────────┐     ┌───────────────┐
│  SQL Query   │────▶│    sqldrift      │────▶│  Safe to run?  │
└──────────────┘     │  validates against│     └───────┬───────┘
                     │  Glue Catalog     │             │
                     └────────┬─────────┘        Yes ──┤── No
                              │                   │         │
                     ┌────────▼─────────┐   ┌─────▼───┐  ┌──▼──────────┐
                     │  Glue Data       │   │ Execute  │  │ Reject with │
                     │  Catalog (tables)│   │ on Athena│  │ drift error │
                     └──────────────────┘   └─────────┘  └─────────────┘
```

## Quick Start

### 1. Fetch tables from Glue Catalog

```python
import boto3

def get_glue_tables(database: str, catalog_id: str | None = None) -> list[str]:
    """Fetch all table names from a Glue Catalog database (handles pagination)."""
    client = boto3.client("glue")
    tables = []
    paginator = client.get_paginator("get_tables")

    params = {"DatabaseName": database}
    if catalog_id:
        params["CatalogId"] = catalog_id

    for page in paginator.paginate(**params):
        tables.extend(t["Name"] for t in page["TableList"])

    return tables
```

### 2. Validate before executing on Athena

```python
from sqldrift import SchemaValidator

# Build validator from Glue Catalog
live_tables = get_glue_tables("my_analytics_db")
validator = SchemaValidator(live_tables)

# Validate a query
query = "SELECT user_id, event_type FROM events JOIN users ON events.user_id = users.id"
success, msg = validator.validate(query)

if success:
    print("Query is safe -- executing on Athena...")
    # execute_on_athena(query)
else:
    print(f"FAIL: {msg}")
```

### 3. Execute on Athena (after validation passes)

```python
import time

def execute_on_athena(query: str, database: str, output_location: str) -> str:
    """Run a validated query on Athena and return the query execution ID."""
    client = boto3.client("athena")

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )

    execution_id = response["QueryExecutionId"]

    # Poll until complete
    while True:
        result = client.get_query_execution(QueryExecutionId=execution_id)
        state = result["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        raise RuntimeError(f"Athena query {state}: {reason}")

    return execution_id
```

## Full Example

```python
import boto3
from sqldrift import CachedSchemaValidator

# --- Configuration ---
GLUE_DATABASE = "my_analytics_db"
ATHENA_OUTPUT = "s3://my-athena-results/output/"

# --- Fetch tables from Glue Catalog ---
def get_glue_tables(database: str) -> list[str]:
    client = boto3.client("glue")
    tables = []
    paginator = client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        tables.extend(t["Name"] for t in page["TableList"])
    return tables

live_tables = get_glue_tables(GLUE_DATABASE)
print(f"Found {len(live_tables)} tables in Glue Catalog: {live_tables[:5]}...")

# --- Create a cached validator (reuse across many queries) ---
validator = CachedSchemaValidator(live_tables, cache_size=256)

# --- Validate and execute ---
queries = [
    "SELECT * FROM events WHERE event_date > '2025-01-01'",
    "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
    "SELECT * FROM nonexistent_table",  # This will fail validation
]

for query in queries:
    success, msg = validator.validate(query)

    if success:
        print(f"PASS: {msg}")
        # execute_on_athena(query, GLUE_DATABASE, ATHENA_OUTPUT)
    else:
        print(f"FAIL: {msg}")
        # Log, alert, or skip — query never hits Athena
```

**Expected output:**
```
Found 42 tables in Glue Catalog: ['events', 'users', 'orders', 'products', 'sessions']...
PASS: Query is safe to execute.
PASS: Query is safe to execute.
FAIL: Schema Drift Detected: The following tables were not found: ['nonexistent_table']
```

## Multi-Database Validation

If your Glue Catalog has multiple databases, create a validator per database or merge them:

```python
# Separate validators
analytics_validator = SchemaValidator(get_glue_tables("analytics"))
warehouse_validator = SchemaValidator(get_glue_tables("warehouse"))

# Or merge into one
all_tables = get_glue_tables("analytics") + get_glue_tables("warehouse")
combined_validator = SchemaValidator(all_tables)
```

## Schema-Qualified Names

If your queries use `database.table` syntax (common in Athena cross-database queries):

```python
# Fetch with database prefix
def get_qualified_tables(database: str) -> list[str]:
    return [f"{database}.{t}" for t in get_glue_tables(database)]

validator = SchemaValidator(
    get_qualified_tables("analytics"),
    preserve_schema=True,  # match full "database.table" names
)

validator.validate("SELECT * FROM analytics.events")
```

## Hot-Swapping Schema

When tables are added/removed in Glue, update the validator without recreating it:

```python
# Periodically refresh (e.g., on a schedule or event trigger)
validator.update_schema(get_glue_tables(GLUE_DATABASE))
```

## IAM Policy (Minimum Required)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["glue:GetTables"],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/my_analytics_db",
        "arn:aws:glue:*:*:table/my_analytics_db/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults"
      ],
      "Resource": "*"
    }
  ]
}
```

## Tips

- **Cache the validator** — Glue API calls cost money; build the validator once and reuse it
- **Use `update_schema()`** — refresh on a schedule instead of recreating the validator
- **Use `CachedSchemaValidator`** — if validating many repeated queries (e.g., from AI agents)
- **Athena dialect** — pass `dialect="presto"` or `dialect="trino"` for Athena-specific SQL syntax
