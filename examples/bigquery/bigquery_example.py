"""
GCP BigQuery integration example for sqldrift.

Demonstrates:
  1. Fetching table names from a BigQuery dataset
  2. Validating SQL queries against the dataset using sqldrift
  3. Executing validated queries on BigQuery

Prerequisites:
  pip install sqldrift google-cloud-bigquery

  GCP credentials must be configured (gcloud auth, service account, or
  Workload Identity). Required IAM: bigquery.tables.list, bigquery.jobs.create
"""

from google.cloud import bigquery
from sqldrift import CachedSchemaValidator


# ---------------------------------------------------------------------------
# Configuration -- update these for your environment
# ---------------------------------------------------------------------------
GCP_PROJECT = "my-gcp-project"
BQ_DATASET = "analytics"


# ---------------------------------------------------------------------------
# BigQuery: fetch all tables from a dataset
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# BigQuery: fetch schema (tables + columns)
# ---------------------------------------------------------------------------
def get_bigquery_schema(project: str, dataset: str) -> dict[str, dict[str, list[str]]]:
    """
    Return a schema dictionary compatible with ColumnValidator.
    Format: {table: {"columns": [col1, ...], "types": [type1, ...]}}
    """
    client = bigquery.Client(project=project)
    
    # Query INFORMATION_SCHEMA.COLUMNS
    query = f"""
        SELECT table_name, column_name, data_type
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
        ORDER BY table_name, ordinal_position
    """
    rows = client.query(query).result()

    schema_dict = {}
    for row in rows:
        table = row.table_name
        col = row.column_name
        dtype = row.data_type

        if table not in schema_dict:
            schema_dict[table] = {"columns": [], "types": []}
        schema_dict[table]["columns"].append(col)
        schema_dict[table]["types"].append(dtype)

    return schema_dict


# ---------------------------------------------------------------------------
# BigQuery: execute a query
# ---------------------------------------------------------------------------
def execute_on_bigquery(query: str, project: str) -> list[dict]:
    """Run a validated query on BigQuery and return results as dicts."""
    client = bigquery.Client(project=project)
    query_job = client.query(query)
    results = query_job.result()  # blocks until complete
    return [dict(row) for row in results]


# ---------------------------------------------------------------------------
# Main: validate-then-execute pipeline
# ---------------------------------------------------------------------------
def main():
    # 1. Fetch live schema from BigQuery
    print(f"Fetching schema from BigQuery dataset '{BQ_DATASET}'...")
    schema = get_bigquery_schema(GCP_PROJECT, BQ_DATASET)
    print(f"Found {len(schema)} tables.\n")

    # 2. Create a cached validator
    # Note: CachedColumnValidator handles both table and column checks
    from sqldrift import CachedColumnValidator
    validator = CachedColumnValidator(schema, cache_size=256)

    # 3. Validate queries before execution
    queries = [
        "SELECT * FROM events WHERE event_date > '2025-01-01'",
        "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
        "SELECT tier FROM users",           # Will fail (missing column)
        "SELECT * FROM nonexistent_table",  # Will fail (missing table)
    ]

    for query in queries:
        success, msg = validator.validate(query, dialect="bigquery")

        if success:
            print(f"PASS: {msg}")
            # Uncomment to actually execute:
            # results = execute_on_bigquery(query, GCP_PROJECT)
            # print(f"   Returned {len(results)} rows")
        else:
            print(f"FAIL: {msg}")

    # 4. Show cache stats
    print(f"\nCache info: {validator.get_cache_info()}")


if __name__ == "__main__":
    main()
