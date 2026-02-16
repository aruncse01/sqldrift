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
def get_bigquery_tables(project: str, dataset: str) -> list[str]:
    """Return all table names from a BigQuery dataset."""
    client = bigquery.Client(project=project)
    dataset_ref = f"{project}.{dataset}"
    return [table.table_id for table in client.list_tables(dataset_ref)]


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
    # 1. Fetch live tables from BigQuery
    print(f"Fetching tables from BigQuery dataset '{BQ_DATASET}'...")
    live_tables = get_bigquery_tables(GCP_PROJECT, BQ_DATASET)
    print(f"Found {len(live_tables)} tables: {live_tables[:5]}...\n")

    # 2. Create a cached validator
    validator = CachedSchemaValidator(live_tables, cache_size=256)

    # 3. Validate queries before execution
    queries = [
        "SELECT * FROM events WHERE event_date > '2025-01-01'",
        "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
        "SELECT * FROM nonexistent_table",  # Will fail validation
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
