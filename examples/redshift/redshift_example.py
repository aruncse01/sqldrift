"""
AWS Redshift Data API integration example for sqldrift.

Uses the Redshift Data API (boto3 redshift-data client) instead of a direct
database connection -- no VPC, JDBC driver, or connection management required.

Demonstrates:
  1. Executing SQL via the Data API (async submit + poll + fetch)
  2. Fetching table names from information_schema
  3. Validating SQL queries against the schema using sqldrift
  4. Executing validated queries on Redshift

Prerequisites:
  pip install sqldrift boto3

  AWS credentials must be configured (aws configure, env vars, or IAM role).
  Required IAM: redshift-data:ExecuteStatement, redshift-data:DescribeStatement,
  redshift-data:GetStatementResult
"""

import time
import boto3
from sqldrift import CachedSchemaValidator


# ---------------------------------------------------------------------------
# Configuration -- update these for your environment
# ---------------------------------------------------------------------------
DATABASE = "analytics"
SCHEMA = "public"
REGION = "us-east-1"

# Use ONE of these:
WORKGROUP = "my-serverless-workgroup"   # Redshift Serverless
# CLUSTER_ID = "my-redshift-cluster"   # Provisioned cluster
# DB_USER = "admin"                    # Required for provisioned cluster


# ---------------------------------------------------------------------------
# Redshift Data API: execute a statement and return results
# ---------------------------------------------------------------------------
def run_query(
    sql: str,
    database: str = DATABASE,
    workgroup: str | None = WORKGROUP,
    cluster_id: str | None = None,
    db_user: str | None = None,
) -> list[list[str]]:
    """Execute SQL via the Redshift Data API and return rows."""
    client = boto3.client("redshift-data", region_name=REGION)

    params: dict = {"Database": database, "Sql": sql}
    if workgroup:
        params["WorkgroupName"] = workgroup
    elif cluster_id:
        params["ClusterIdentifier"] = cluster_id
        if db_user:
            params["DbUser"] = db_user

    # Submit
    response = client.execute_statement(**params)
    stmt_id = response["Id"]

    # Poll until finished
    while True:
        desc = client.describe_statement(Id=stmt_id)
        status = desc["Status"]

        if status == "FINISHED":
            break
        elif status in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Redshift query {status}: {desc.get('Error', 'Unknown')}"
            )
        time.sleep(0.5)

    # Fetch results
    result = client.get_statement_result(Id=stmt_id)
    return [
        [col.get("stringValue", "") for col in record]
        for record in result["Records"]
    ]


# ---------------------------------------------------------------------------
# Redshift: fetch all tables from a schema
# ---------------------------------------------------------------------------
def get_redshift_tables(schema: str = SCHEMA, **kwargs) -> list[str]:
    """Return all table names from a Redshift schema via the Data API."""
    rows = run_query(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' "
        f"AND table_type IN ('BASE TABLE', 'VIEW') "
        f"ORDER BY table_name",
        **kwargs,
    )
    return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# Main: validate-then-execute pipeline
# ---------------------------------------------------------------------------
def main():
    # 1. Fetch live tables via the Data API
    print(f"Fetching tables from Redshift schema '{SCHEMA}'...")
    live_tables = get_redshift_tables()
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
        success, msg = validator.validate(query, dialect="postgres")

        if success:
            print(f"PASS: {msg}")
            # Uncomment to actually execute:
            # results = run_query(query)
            # print(f"   Returned {len(results)} rows")
        else:
            print(f"FAIL: {msg}")

    # 4. Show cache stats
    print(f"\nCache info: {validator.get_cache_info()}")


if __name__ == "__main__":
    main()
