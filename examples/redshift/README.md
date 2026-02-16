# AWS Redshift Data API Integration

Use **sqldrift** with [Amazon Redshift](https://aws.amazon.com/redshift/) via the **Redshift Data API** -- no direct database connection or VPC networking required.

## Why the Data API?

The Redshift Data API (`boto3` `redshift-data` client) lets you query Redshift without managing connections, drivers, or VPC configuration. It uses IAM authentication and is ideal for Lambda functions, Step Functions, and serverless pipelines.

## Prerequisites

```bash
pip install sqldrift boto3
```

- An AWS account with a Redshift cluster or Redshift Serverless workgroup
- IAM permissions: `redshift-data:ExecuteStatement`, `redshift-data:DescribeStatement`, `redshift-data:GetStatementResult`
- For cluster auth: `redshift:GetClusterCredentialsWithIAM` or a database user
- AWS credentials configured (`aws configure`, env vars, or IAM role)

## How It Works

```
┌──────────────┐     ┌─────────────────┐     ┌───────────────┐
│  SQL Query   │────>│    sqldrift      │────>│  Safe to run?  │
└──────────────┘     │  validates against│     └───────┬───────┘
                     │  Redshift tables  │             │
                     └────────┬─────────┘        Yes ──┤── No
                              │                   │         │
                     ┌────────▼─────────┐   ┌─────▼───┐  ┌──▼──────────┐
                     │  Redshift Data   │   │ Execute  │  │ Reject with │
                     │  API (tables)    │   │ via API  │  │ drift error │
                     └──────────────────┘   └─────────┘  └─────────────┘
```

## Quick Start

### 1. Execute a statement via the Data API

The Data API is asynchronous -- you submit a statement, poll for completion, then fetch results.

```python
import time
import boto3

def execute_redshift_statement(
    sql: str,
    database: str,
    workgroup: str | None = None,
    cluster_id: str | None = None,
    db_user: str | None = None,
    region: str = "us-east-1",
) -> list[list]:
    """Execute a SQL statement via the Redshift Data API and return rows."""
    client = boto3.client("redshift-data", region_name=region)

    # Build request params
    params = {"Database": database, "Sql": sql}

    if workgroup:
        # Redshift Serverless
        params["WorkgroupName"] = workgroup
    elif cluster_id:
        # Provisioned cluster
        params["ClusterIdentifier"] = cluster_id
        if db_user:
            params["DbUser"] = db_user

    # Submit the statement
    response = client.execute_statement(**params)
    statement_id = response["Id"]

    # Poll until finished
    while True:
        desc = client.describe_statement(Id=statement_id)
        status = desc["Status"]

        if status == "FINISHED":
            break
        elif status == "FAILED":
            raise RuntimeError(f"Redshift query failed: {desc.get('Error', 'Unknown')}")
        elif status == "ABORTED":
            raise RuntimeError("Redshift query was aborted")

        time.sleep(0.5)

    # Fetch results
    result = client.get_statement_result(Id=statement_id)
    rows = [[col.get("stringValue", "") for col in record] for record in result["Records"]]
    return rows
```

### 2. Fetch tables from Redshift

```python
def get_redshift_tables(
    database: str,
    schema: str = "public",
    **kwargs,
) -> list[str]:
    """Fetch all table names from a Redshift schema via the Data API."""
    sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
          AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY table_name
    """
    rows = execute_redshift_statement(sql, database=database, **kwargs)
    return [row[0] for row in rows]
```

### 3. Validate before executing

```python
from sqldrift import SchemaValidator

# Fetch tables via the Data API (no VPC/connection needed)
live_tables = get_redshift_tables(
    database="analytics",
    workgroup="my-serverless-workgroup",  # or cluster_id="my-cluster"
)
validator = SchemaValidator(live_tables)

query = "SELECT user_id, event_type FROM events JOIN users ON events.user_id = users.id"
success, msg = validator.validate(query, dialect="postgres")

if success:
    print("Query is safe -- executing on Redshift...")
    execute_redshift_statement(query, database="analytics", workgroup="my-serverless-workgroup")
else:
    print(f"FAIL: {msg}")
```

## Full Example

```python
import time
import boto3
from sqldrift import CachedSchemaValidator

# --- Configuration ---
DATABASE = "analytics"
SCHEMA = "public"
# Use ONE of the following:
WORKGROUP = "my-serverless-workgroup"   # Redshift Serverless
# CLUSTER_ID = "my-redshift-cluster"   # Provisioned cluster
# DB_USER = "admin"                    # Required for provisioned cluster

REGION = "us-east-1"


# --- Data API helper ---
def run_query(sql: str) -> list[list]:
    client = boto3.client("redshift-data", region_name=REGION)

    params = {"Database": DATABASE, "Sql": sql, "WorkgroupName": WORKGROUP}
    # For provisioned clusters, use:
    # params = {"Database": DATABASE, "Sql": sql, "ClusterIdentifier": CLUSTER_ID, "DbUser": DB_USER}

    response = client.execute_statement(**params)
    stmt_id = response["Id"]

    while True:
        desc = client.describe_statement(Id=stmt_id)
        status = desc["Status"]
        if status == "FINISHED":
            break
        elif status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Query {status}: {desc.get('Error', 'Unknown')}")
        time.sleep(0.5)

    result = client.get_statement_result(Id=stmt_id)
    return [[col.get("stringValue", "") for col in record] for record in result["Records"]]


# --- Fetch tables ---
rows = run_query(
    f"SELECT table_name FROM information_schema.tables "
    f"WHERE table_schema = '{SCHEMA}' AND table_type IN ('BASE TABLE', 'VIEW')"
)
live_tables = [row[0] for row in rows]
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
        # results = run_query(query)
    else:
        print(f"FAIL: {msg}")
```

**Expected output:**
```
Found 42 tables in public: ['events', 'users', 'orders', 'products', 'sessions']...
PASS: Query is safe to execute.
PASS: Query is safe to execute.
FAIL: Schema Drift Detected: The following tables were not found: ['nonexistent_table']
```

## Provisioned Cluster vs. Serverless

| Parameter | Serverless | Provisioned Cluster |
|-----------|-----------|---------------------|
| Auth identifier | `WorkgroupName` | `ClusterIdentifier` + `DbUser` |
| IAM auth | Automatic | Requires `redshift:GetClusterCredentialsWithIAM` |
| VPC requirement | None (Data API) | None (Data API) |

```python
# Serverless
get_redshift_tables(database="analytics", workgroup="my-workgroup")

# Provisioned cluster
get_redshift_tables(database="analytics", cluster_id="my-cluster", db_user="admin")
```

## Multi-Schema Validation

```python
# Merge tables across schemas
all_tables = (
    get_redshift_tables(database="analytics", schema="public", workgroup=WORKGROUP)
    + get_redshift_tables(database="analytics", schema="staging", workgroup=WORKGROUP)
)
validator = SchemaValidator(all_tables)
```

## Schema-Qualified Names

If your queries use `schema.table` syntax:

```python
def get_qualified_tables(database: str, schema: str, **kwargs) -> list[str]:
    rows = execute_redshift_statement(
        f"SELECT table_schema || '.' || table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema}'",
        database=database, **kwargs,
    )
    return [row[0] for row in rows]

validator = SchemaValidator(
    get_qualified_tables("analytics", "public", workgroup=WORKGROUP),
    preserve_schema=True,
)
validator.validate("SELECT * FROM public.events", dialect="postgres")
```

## Hot-Swapping Schema

```python
validator.update_schema(
    get_redshift_tables(database=DATABASE, workgroup=WORKGROUP)
)
```

## IAM Policy (Minimum Required)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "redshift-data:ExecuteStatement",
        "redshift-data:DescribeStatement",
        "redshift-data:GetStatementResult"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "redshift-serverless:GetCredentials",
      "Resource": "arn:aws:redshift-serverless:*:*:workgroup/my-serverless-workgroup"
    }
  ]
}
```

For provisioned clusters, add:
```json
{
  "Effect": "Allow",
  "Action": "redshift:GetClusterCredentialsWithIAM",
  "Resource": "arn:aws:redshift:*:*:dbname:my-cluster/analytics"
}
```

## Tips

- **Use `dialect="postgres"`** -- Redshift SQL is PostgreSQL-based; sqlglot parses it correctly with the postgres dialect
- **Data API is async** -- statements are submitted and polled; the helper functions above handle this for you
- **No VPC needed** -- the Data API routes through AWS APIs, unlike JDBC/ODBC which require network access
- **Rate limits** -- the Data API has a soft limit of 200 active statements; cache the validator to minimize API calls
- **Redshift Serverless** -- use `WorkgroupName` instead of `ClusterIdentifier`
