# GCP BigQuery Integration

Use **sqldrift** with [Google BigQuery](https://cloud.google.com/bigquery) to validate queries before execution.

## Prerequisites

```bash
pip install sqldrift google-cloud-bigquery
```

- A GCP project with BigQuery enabled
- IAM permissions: `bigquery.tables.list`, `bigquery.jobs.create`
- Credentials configured (`gcloud auth application-default login`, service account key, or Workload Identity)

## How It Works

```
┌──────────────┐     ┌─────────────────┐     ┌───────────────┐
│  SQL Query   │────>│    sqldrift      │────>│  Safe to run?  │
└──────────────┘     │  validates against│     └───────┬───────┘
                     │  BigQuery tables  │             │
                     └────────┬─────────┘        Yes ──┤── No
                              │                   │         │
                     ┌────────▼─────────┐   ┌─────▼───┐  ┌──▼──────────┐
                     │  BigQuery        │   │ Execute  │  │ Reject with │
                     │  Dataset (tables)│   │ query    │  │ drift error │
                     └──────────────────┘   └─────────┘  └─────────────┘
```

## Quick Start

### 1. Fetch tables from a BigQuery dataset

```python
from google.cloud import bigquery

def get_bigquery_tables(project: str, dataset: str) -> list[str]:
    """Fetch all table names from a BigQuery dataset."""
    client = bigquery.Client(project=project)
    dataset_ref = f"{project}.{dataset}"
    tables = [table.table_id for table in client.list_tables(dataset_ref)]
    return tables
```

### 2. Validate before executing

```python
from sqldrift import SchemaValidator

live_tables = get_bigquery_tables("my-gcp-project", "analytics")
validator = SchemaValidator(live_tables)

query = "SELECT user_id, event_type FROM events JOIN users ON events.user_id = users.id"
success, msg = validator.validate(query, dialect="bigquery")

if success:
    print("Query is safe -- executing on BigQuery...")
else:
    print(f"FAIL: {msg}")
```

### 3. Execute on BigQuery (after validation passes)

```python
def execute_on_bigquery(query: str, project: str) -> list[dict]:
    """Run a validated query on BigQuery and return results."""
    client = bigquery.Client(project=project)
    query_job = client.query(query)
    results = query_job.result()  # blocks until complete
    return [dict(row) for row in results]
```

## Full Example

```python
from google.cloud import bigquery
from sqldrift import CachedSchemaValidator

# --- Configuration ---
GCP_PROJECT = "my-gcp-project"
BQ_DATASET = "analytics"

# --- Fetch tables from BigQuery ---
def get_bigquery_tables(project: str, dataset: str) -> list[str]:
    client = bigquery.Client(project=project)
    return [t.table_id for t in client.list_tables(f"{project}.{dataset}")]

live_tables = get_bigquery_tables(GCP_PROJECT, BQ_DATASET)
print(f"Found {len(live_tables)} tables in {BQ_DATASET}: {live_tables[:5]}...")

# --- Create a cached validator ---
validator = CachedSchemaValidator(live_tables, cache_size=256)

# --- Validate and execute ---
queries = [
    "SELECT * FROM events WHERE event_date > '2025-01-01'",
    "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
    "SELECT * FROM nonexistent_table",
]

for query in queries:
    success, msg = validator.validate(query, dialect="bigquery")

    if success:
        print(f"PASS: {msg}")
        # results = execute_on_bigquery(query, GCP_PROJECT)
    else:
        print(f"FAIL: {msg}")
```

**Expected output:**
```
Found 42 tables in analytics: ['events', 'users', 'orders', 'products', 'sessions']...
PASS: Query is safe to execute.
PASS: Query is safe to execute.
FAIL: Schema Drift Detected: The following tables were not found: ['nonexistent_table']
```

## Multi-Dataset Validation

If your project has multiple datasets, merge tables or use separate validators:

```python
# Merge tables across datasets
all_tables = (
    get_bigquery_tables(GCP_PROJECT, "analytics")
    + get_bigquery_tables(GCP_PROJECT, "warehouse")
)
validator = SchemaValidator(all_tables)
```

## Dataset-Qualified Names

For queries using `dataset.table` syntax:

```python
def get_qualified_tables(project: str, dataset: str) -> list[str]:
    return [f"{dataset}.{t}" for t in get_bigquery_tables(project, dataset)]

validator = SchemaValidator(
    get_qualified_tables(GCP_PROJECT, "analytics"),
    preserve_schema=True,
)

validator.validate("SELECT * FROM analytics.events", dialect="bigquery")
```

## Hot-Swapping Schema

Refresh the validator when tables change without recreating it:

```python
validator.update_schema(get_bigquery_tables(GCP_PROJECT, BQ_DATASET))
```

## Required IAM Roles

| Role | Purpose |
|------|---------|
| `roles/bigquery.dataViewer` | List tables and read metadata |
| `roles/bigquery.jobUser` | Execute queries |

Or at minimum, these permissions:
- `bigquery.tables.list`
- `bigquery.jobs.create`

## Tips

- **Use `dialect="bigquery"`** -- BigQuery has unique SQL syntax that sqlglot handles with the dialect flag
- **Cache the validator** -- BigQuery `list_tables` API calls count against quota; build once, reuse
- **Use `CachedSchemaValidator`** -- for repeated queries from AI agents or pipelines
- **Service accounts** -- use `GOOGLE_APPLICATION_CREDENTIALS` env var for non-interactive auth
