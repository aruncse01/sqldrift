"""
MySQL integration example for sqldrift.

Demonstrates:
  1. Connecting to MySQL via mysql-connector-python
  2. Fetching table names using SHOW TABLES
  3. Validating SQL queries against the schema using sqldrift
  4. Executing validated queries

Prerequisites:
  pip install sqldrift mysql-connector-python
"""

import mysql.connector
from sqldrift import CachedSchemaValidator


# ---------------------------------------------------------------------------
# Configuration -- update these for your environment
# ---------------------------------------------------------------------------
MYSQL_CONFIG = {
    "host": "localhost",
    "database": "analytics",
    "user": "myuser",
    "password": "mypassword",
    "port": 3306,
}


# ---------------------------------------------------------------------------
# MySQL: fetch all tables from the current database
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# MySQL: fetch schema (tables + columns)
# ---------------------------------------------------------------------------
def get_mysql_schema(conn) -> dict[str, dict[str, list[str]]]:
    """
    Return a schema dictionary compatible with ColumnValidator.
    Format: {table: {"columns": [col1, ...], "types": [type1, ...]}}
    """
    cursor = conn.cursor()
    # Fetch columns from the current database
    cursor.execute(
        """
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM information_schema.columns
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """
    )
    
    schema_dict = {}
    for row in cursor.fetchall():
        # mysql-connector returns tuples by default
        table, col, dtype = row[0], row[1], row[2]
        
        if table not in schema_dict:
            schema_dict[table] = {"columns": [], "types": []}
        schema_dict[table]["columns"].append(col)
        schema_dict[table]["types"].append(dtype)

    cursor.close()
    return schema_dict


# ---------------------------------------------------------------------------
# MySQL: execute a query
# ---------------------------------------------------------------------------
def execute_query(conn, query: str) -> list[dict]:
    """Run a validated query and return results as dicts."""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    return results


# ---------------------------------------------------------------------------
# Main: validate-then-execute pipeline
# ---------------------------------------------------------------------------
def main():
    # 1. Connect to MySQL
    print(f"Connecting to {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}...")
    conn = mysql.connector.connect(**MYSQL_CONFIG)

    # 2. Fetch live schema
    print("Fetching schema...")
    schema = get_mysql_schema(conn)
    print(f"Found {len(schema)} tables.\n")

    # 3. Create a cached validator
    # Note: CachedColumnValidator handles both table and column checks
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
        success, msg = validator.validate(query, dialect="mysql")

        if success:
            print(f"PASS: {msg}")
            # Uncomment to actually execute:
            # results = execute_query(conn, query)
            # print(f"   Returned {len(results)} rows")
        else:
            print(f"FAIL: {msg}")
            
            if "Column Drift" in msg:
                 # Check for 'tier' specifically for the example
                if "tier" in msg:
                    suggestions = validator.suggest_alternatives("tier")
                    if suggestions:
                        print(f"      Did you mean: {', '.join(suggestions)}?")

    # 5. Show cache stats
    print(f"\nCache info: {validator.get_cache_info()}")

    conn.close()


if __name__ == "__main__":
    main()
