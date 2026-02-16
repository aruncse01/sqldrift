"""
PostgreSQL integration example for sqldrift.

Demonstrates:
  1. Connecting to PostgreSQL via psycopg2
  2. Fetching table names from information_schema
  3. Validating SQL queries against the schema using sqldrift
  4. Executing validated queries

Prerequisites:
  pip install sqldrift psycopg2-binary
"""

import psycopg2
from sqldrift import CachedSchemaValidator


# ---------------------------------------------------------------------------
# Configuration -- update these for your environment
# ---------------------------------------------------------------------------
PG_CONFIG = {
    "host": "localhost",
    "database": "analytics",
    "user": "myuser",
    "password": "mypassword",
    "port": 5432,
}
SCHEMA = "public"


# ---------------------------------------------------------------------------
# PostgreSQL: fetch all tables from a schema
# ---------------------------------------------------------------------------
def get_postgres_tables(conn, schema: str = SCHEMA) -> list[str]:
    """Return all table and view names from a PostgreSQL schema."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY table_name
        """,
        (schema,),
    )
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables


# ---------------------------------------------------------------------------
# PostgreSQL: execute a query
# ---------------------------------------------------------------------------
def execute_query(conn, query: str) -> list[dict]:
    """Run a validated query and return results as dicts."""
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
    # 1. Connect to PostgreSQL
    print(f"Connecting to {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}...")
    conn = psycopg2.connect(**PG_CONFIG)

    # 2. Fetch live tables
    print(f"Fetching tables from schema '{SCHEMA}'...")
    live_tables = get_postgres_tables(conn)
    print(f"Found {len(live_tables)} tables: {live_tables[:5]}...\n")

    # 3. Create a cached validator
    validator = CachedSchemaValidator(live_tables, cache_size=256)

    # 4. Validate queries before execution
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
            # results = execute_query(conn, query)
            # print(f"   Returned {len(results)} rows")
        else:
            print(f"FAIL: {msg}")

    # 5. Show cache stats
    print(f"\nCache info: {validator.get_cache_info()}")

    conn.close()


if __name__ == "__main__":
    main()
