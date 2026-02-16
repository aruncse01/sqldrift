"""
AWS Glue Catalog + Athena integration example for sqldrift.

Demonstrates:
  1. Fetching table names from AWS Glue Data Catalog (with pagination)
  2. Validating SQL queries against the catalog using sqldrift
  3. Executing validated queries on Athena

Prerequisites:
  pip install sqldrift boto3

  AWS credentials must be configured (aws configure, env vars, or IAM role).
  Required IAM permissions: glue:GetTables, athena:StartQueryExecution,
  athena:GetQueryExecution, athena:GetQueryResults
"""

import time
import boto3
from sqldrift import CachedSchemaValidator


# ---------------------------------------------------------------------------
# Configuration — update these for your environment
# ---------------------------------------------------------------------------
GLUE_DATABASE = "my_analytics_db"
ATHENA_OUTPUT = "s3://my-athena-results/output/"
AWS_REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Glue Catalog: fetch all tables (paginated)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Glue Catalog: fetch schema (tables + columns)
# ---------------------------------------------------------------------------
def get_glue_schema(
    database: str,
    catalog_id: str | None = None,
    region: str = AWS_REGION,
) -> dict[str, dict[str, list[str]]]:
    """
    Return a schema dictionary from Glue Data Catalog.
    Format: {table: {"columns": [col1, ...], "types": [type1, ...]}}
    """
    client = boto3.client("glue", region_name=region)
    schema_dict = {}
    paginator = client.get_paginator("get_tables")

    params: dict = {"DatabaseName": database}
    if catalog_id:
        params["CatalogId"] = catalog_id

    for page in paginator.paginate(**params):
        for table in page["TableList"]:
            table_name = table["Name"]
            columns = []
            types = []
            
            # Extract columns from StorageDescriptor
            # Note: Partition keys are separate in Glue, check 'PartitionKeys' if needed
            cols_def = table.get("StorageDescriptor", {}).get("Columns", [])
            for col in cols_def:
                columns.append(col["Name"])
                types.append(col["Type"])
                
            # Add partition keys as columns too because they are queryable
            part_keys = table.get("PartitionKeys", [])
            for pk in part_keys:
                columns.append(pk["Name"])
                types.append(pk["Type"])

            schema_dict[table_name] = {"columns": columns, "types": types}

    return schema_dict


# ---------------------------------------------------------------------------
# Athena: execute a query
# ---------------------------------------------------------------------------
def execute_on_athena(
    query: str,
    database: str = GLUE_DATABASE,
    output_location: str = ATHENA_OUTPUT,
    region: str = AWS_REGION,
) -> str:
    """Run a query on Athena and wait for completion. Returns execution ID."""
    client = boto3.client("athena", region_name=region)

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


# ---------------------------------------------------------------------------
# Main: validate-then-execute pipeline
# ---------------------------------------------------------------------------
def main():
    # 1. Fetch live schema from Glue Catalog
    print(f"Fetching schema from Glue database '{GLUE_DATABASE}'...")
    schema = get_glue_schema(GLUE_DATABASE)
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
        success, msg = validator.validate(query)

        if success:
            print(f"PASS: {msg}")
            # Uncomment to actually execute:
            # exec_id = execute_on_athena(query)
            # print(f"   Athena execution ID: {exec_id}")
        else:
            print(f"FAIL: {msg}")
            
            if "Column Drift" in msg:
                # Check for 'tier' specifically for the example
                if "tier" in msg:
                    suggestions = validator.suggest_alternatives("tier")
                    if suggestions:
                        print(f"      Did you mean: {', '.join(suggestions)}?")

    # 4. Show cache stats
    print(f"\nCache info: {validator.get_cache_info()}")


if __name__ == "__main__":
    main()
