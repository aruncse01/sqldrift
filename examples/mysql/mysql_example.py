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
def get_mysql_tables(conn) -> list[str]:
    """Return all table and view names from the connected MySQL database."""
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables


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

    # 2. Fetch live tables
    print("Fetching tables...")
    live_tables = get_mysql_tables(conn)
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
        success, msg = validator.validate(query, dialect="mysql")

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
