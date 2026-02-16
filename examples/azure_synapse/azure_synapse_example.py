"""
Azure Synapse Analytics integration example for sqldrift.

Demonstrates:
  1. Connecting to Synapse SQL pool (Azure AD or SQL auth)
  2. Fetching table names from INFORMATION_SCHEMA
  3. Validating SQL queries against the schema using sqldrift
  4. Executing validated queries on Synapse

Prerequisites:
  pip install sqldrift pyodbc azure-identity

  Microsoft ODBC Driver 18 for SQL Server must be installed.
  Azure credentials must be configured (az login, managed identity, or env vars).
"""

import pyodbc
from azure.identity import DefaultAzureCredential
from sqldrift import CachedSchemaValidator


# ---------------------------------------------------------------------------
# Configuration -- update these for your environment
# ---------------------------------------------------------------------------
SYNAPSE_SERVER = "my-workspace.sql.azuresynapse.net"
SYNAPSE_DATABASE = "my_sql_pool"
SYNAPSE_SCHEMA = "dbo"


# ---------------------------------------------------------------------------
# Synapse: connect with Azure AD token
# ---------------------------------------------------------------------------
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
    return pyodbc.connect(conn_string, attrs_before={1256: token_bytes})


# ---------------------------------------------------------------------------
# Synapse: fetch all tables from a schema
# ---------------------------------------------------------------------------
def get_synapse_tables(
    conn: pyodbc.Connection,
    schema: str = "dbo",
) -> list[str]:
    """Return all table and view names from a Synapse SQL pool schema."""
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


# ---------------------------------------------------------------------------
# Synapse: execute a query
# ---------------------------------------------------------------------------
def execute_on_synapse(conn: pyodbc.Connection, query: str) -> list[dict]:
    """Run a validated query on Synapse and return results as dicts."""
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return results


# ---------------------------------------------------------------------------
# Main: validate-then-execute pipeline
# ---------------------------------------------------------------------------
def main():
    # 1. Connect to Synapse
    print(f"Connecting to {SYNAPSE_SERVER}...")
    conn = get_synapse_connection(SYNAPSE_SERVER, SYNAPSE_DATABASE)

    # 2. Fetch live tables
    print(f"Fetching tables from schema '{SYNAPSE_SCHEMA}'...")
    live_tables = get_synapse_tables(conn, schema=SYNAPSE_SCHEMA)
    print(f"Found {len(live_tables)} tables: {live_tables[:5]}...\n")

    # 3. Create a cached validator (schema-qualified)
    validator = CachedSchemaValidator(
        live_tables,
        preserve_schema=True,
        cache_size=256,
    )

    # 4. Validate queries before execution
    queries = [
        "SELECT * FROM dbo.events WHERE event_date > '2025-01-01'",
        "SELECT u.name, COUNT(*) FROM dbo.users u JOIN dbo.orders o ON u.id = o.user_id GROUP BY u.name",
        "SELECT * FROM dbo.nonexistent_table",  # Will fail validation
    ]

    for query in queries:
        success, msg = validator.validate(query)

        if success:
            print(f"PASS: {msg}")
            # Uncomment to actually execute:
            # results = execute_on_synapse(conn, query)
            # print(f"   Returned {len(results)} rows")
        else:
            print(f"FAIL: {msg}")

    # 5. Show cache stats
    print(f"\nCache info: {validator.get_cache_info()}")

    conn.close()


if __name__ == "__main__":
    main()
