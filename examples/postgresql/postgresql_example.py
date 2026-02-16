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
# ---------------------------------------------------------------------------
# PostgreSQL: fetch schema (tables + columns)
# ---------------------------------------------------------------------------
def get_postgres_schema(conn, schema: str = SCHEMA) -> dict[str, dict[str, list[str]]]:
    """
    Return a schema dictionary compatible with ColumnValidator.
    Format: {table: {"columns": [col1, col2, ...], "types": [type1, ...]}}
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
        """,
        (schema,),
    )
    
    schema_dict = {}
    for table, col, dtype in cursor.fetchall():
        if table not in schema_dict:
            schema_dict[table] = {"columns": [], "types": []}
        schema_dict[table]["columns"].append(col)
        schema_dict[table]["types"].append(dtype)

    cursor.close()
    return schema_dict


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

    # 2. Fetch live schema
    print(f"Fetching schema from '{SCHEMA}'...")
    schema = get_postgres_schema(conn)
    print(f"Found {len(schema)} tables.")

    # 3. Create a cached validator
    # Note: ColumnValidator checks columns; SchemaValidator checks tables.
    # Included ColumnValidator handles both if table exists in schema.
    from sqldrift import CachedColumnValidator
    validator = CachedColumnValidator(schema, cache_size=256)

    # 4. Validate queries before execution
    queries = [
        "SELECT * FROM events WHERE event_date > '2025-01-01'",
        "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
        "SELECT tier FROM users",           # Will fail (missing column)
        "SELECT * FROM nonexistent_table",  # Will fail (missing table)
    ]

    for query in queries:
        # validate() returns (success, message)
        success, msg = validator.validate(query, dialect="postgres")

        if success:
            print(f"PASS: {msg}")
            # Uncomment to actually execute:
            # results = execute_query(conn, query)
            # print(f"   Returned {len(results)} rows")
        else:
            print(f"FAIL: {msg}")
            
            # Suggest alternatives if it's a column error
            if "Column Drift" in msg:
                # Extract the missing column name from the message or query context
                # For demo purposes, we'll just check if 'tier' is missing
                if "tier" in msg:
                    suggestions = validator.suggest_alternatives("tier")
                    if suggestions:
                        print(f"      Did you mean: {', '.join(suggestions)}?")

    # 5. Show cache stats
    print(f"\nCache info: {validator.get_cache_info()}")

    conn.close()


if __name__ == "__main__":
    main()
