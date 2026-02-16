"""
Core validation function for detecting schema drift in SQL queries.

This module provides a simple, stateless function for one-off query validation.
For better performance with large schemas or repeated queries, use the
class-based validators in sqldrift.optimized.
"""

import sqlglot
from sqlglot.optimizer.scope import build_scope


def validate_query(
    sql_query: str,
    live_tables: list[str],
    *,
    dialect: str | None = None,
) -> tuple[bool, str]:
    """
    Validate that all tables referenced in a SQL query exist in the schema.

    Parses the SQL query, extracts all physical table references (ignoring
    CTEs and subquery aliases), and checks them against the provided list
    of live tables.

    Args:
        sql_query: The SQL query string to validate.
        live_tables: List of table names that currently exist in the schema.
            Supports schema-qualified names (e.g., ``"public.users"``); only
            the base table name is matched by default.
        dialect: Optional SQL dialect for parsing (e.g., ``"postgres"``,
            ``"mysql"``, ``"bigquery"``). Defaults to ``None`` (auto-detect).

    Returns:
        A ``(success, message)`` tuple:

        - ``(True, "Query is safe to execute.")`` — all referenced tables exist.
        - ``(False, "Schema Drift Detected: ...")`` — one or more tables are missing.
        - ``(False, "Invalid SQL syntax: ...")`` — the query could not be parsed.

    Examples:
        >>> validate_query("SELECT * FROM users", ["users", "orders"])
        (True, 'Query is safe to execute.')

        >>> validate_query("SELECT * FROM deleted_table", ["users"])
        (False, "Schema Drift Detected: The following tables were not found: ['deleted_table']")
    """
    try:
        expression = sqlglot.parse_one(sql_query, read=dialect)

        # Normalize live tables: extract base name and lower-case
        live_set = {t.split(".")[-1].lower().strip() for t in live_tables}

        # Use build_scope to distinguish physical tables from CTEs/aliases
        root_scope = build_scope(expression)

        referenced_tables: set[str] = set()

        for scope in root_scope.traverse():
            for table in scope.tables:
                # Skip CTEs defined in the query
                if table.name in scope.cte_sources:
                    continue

                name = table.this.name.lower()
                referenced_tables.add(name)

        missing_tables = referenced_tables - live_set

        if missing_tables:
            parts: list[str] = []
            available = sorted(live_set)

            for table in sorted(missing_tables):
                line = f"- Table '{table}' not found"

                # Suggest similar tables
                suggestions = [
                    c for c in available
                    if table in c or c in table
                    or (len(table) >= 3 and table[:3] == c[:3])
                ]
                if suggestions:
                    line += f". Did you mean: {', '.join(suggestions[:5])}"

                parts.append(line)

            parts.append(f"  Available tables: {', '.join(available)}")

            detail_block = "\n".join(parts)
            return (
                False,
                f"Schema Drift Detected:\n{detail_block}",
            )

        return True, "Query is safe to execute."

    except Exception as e:
        return False, f"Invalid SQL syntax: {e}"
