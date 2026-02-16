"""
Column-level schema drift detection for SQL queries.

While the core sqldrift validators check table existence, this module validates
that columns referenced in SQL queries exist in the current schema. Detects:

- Column renames (e.g., ``customer_name`` -> ``full_name``)
- Column deletions (e.g., removed ``legacy_flag``)
- Qualified column mismatches (e.g., ``table.wrong_col``)

Usage:
    >>> from sqldrift import ColumnValidator
    >>>
    >>> schema = {
    ...     "users": {
    ...         "columns": ["id", "name", "email"],
    ...         "types": ["INTEGER", "VARCHAR", "VARCHAR"],
    ...     },
    ...     "orders": {
    ...         "columns": ["id", "user_id", "total"],
    ...         "types": ["INTEGER", "INTEGER", "DECIMAL"],
    ...     },
    ... }
    >>>
    >>> validator = ColumnValidator(schema)
    >>> validator.validate("SELECT name FROM users")
    (True, 'All columns exist.')
    >>> validator.validate("SELECT customer_name FROM users")
    (False, "Column Drift Detected: ...")
"""

import sqlglot
from typing import Optional
from functools import lru_cache


# ---------------------------------------------------------------------------
# Schema type alias
# ---------------------------------------------------------------------------
# Expected format:
#   {
#       "table_name": {
#           "columns": ["col1", "col2", ...],
#           "types": ["TYPE1", "TYPE2", ...]  # optional
#       }
#   }
SchemaDict = dict[str, dict[str, list[str]]]


class ColumnValidator:
    """
    Validates SQL queries for column-level schema drift.

    Parses SQL queries to extract column references and checks them against
    a provided schema definition. Supports qualified (``table.column``) and
    unqualified column references.

    Args:
        schema: Dictionary mapping table names to column definitions.
        case_sensitive: If ``True``, column name matching is case-sensitive.
            Defaults to ``False``.

    Examples:
        >>> schema = {
        ...     "users": {"columns": ["id", "name", "email"]},
        ...     "orders": {"columns": ["id", "user_id", "total"]},
        ... }
        >>> v = ColumnValidator(schema)
        >>> v.validate("SELECT name FROM users")
        (True, 'All columns exist.')
    """

    def __init__(
        self,
        schema: SchemaDict,
        *,
        case_sensitive: bool = False,
    ):
        self.case_sensitive = case_sensitive
        self._schema_raw = schema
        self._schema, self._column_lookup = self._build_lookups(schema)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _normalize(self, name: str) -> str:
        """Normalize a name based on case sensitivity setting."""
        name = name.strip()
        if not self.case_sensitive:
            name = name.lower()
        return name

    def _build_lookups(
        self, schema: SchemaDict
    ) -> tuple[dict[str, set[str]], dict[str, list[str]]]:
        """
        Build normalized lookup structures from the raw schema.

        Returns:
            A tuple of:
            - table -> set of normalized column names
            - column -> list of tables that contain it
        """
        table_columns: dict[str, set[str]] = {}
        column_to_tables: dict[str, list[str]] = {}

        for table, info in schema.items():
            norm_table = self._normalize(table)
            cols = {self._normalize(c) for c in info.get("columns", [])}
            table_columns[norm_table] = cols

            for col in cols:
                column_to_tables.setdefault(col, []).append(norm_table)

        return table_columns, column_to_tables

    # ------------------------------------------------------------------
    # Column extraction
    # ------------------------------------------------------------------

    def extract_columns(
        self,
        sql_query: str,
        *,
        dialect: Optional[str] = None,
    ) -> list[tuple[Optional[str], str]]:
        """
        Extract column references from a SQL query.

        Args:
            sql_query: The SQL query to parse.
            dialect: Optional SQL dialect for parsing.

        Returns:
            List of ``(table_or_alias, column_name)`` tuples.
            ``table_or_alias`` is ``None`` for unqualified references.
        """
        try:
            expression = sqlglot.parse_one(sql_query, read=dialect)
        except Exception:
            return []

        # Build alias -> real table mapping from FROM/JOIN clauses
        alias_map: dict[str, str] = {}
        for table_node in expression.find_all(sqlglot.exp.Table):
            real_name = table_node.name
            alias = table_node.alias
            if alias:
                alias_map[self._normalize(alias)] = self._normalize(real_name)

        columns: list[tuple[Optional[str], str]] = []
        seen: set[str] = set()

        for col in expression.find_all(sqlglot.exp.Column):
            col_name = col.name
            table_ref = col.table if col.table else None

            # Resolve alias to real table name
            if table_ref:
                norm_ref = self._normalize(table_ref)
                real_table = alias_map.get(norm_ref, norm_ref)
            else:
                real_table = None

            norm_col = self._normalize(col_name)

            # Deduplicate
            key = f"{real_table}.{norm_col}" if real_table else norm_col
            if key not in seen:
                seen.add(key)
                columns.append((real_table, norm_col))

        return columns

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(
        self,
        sql_query: str,
        *,
        dialect: Optional[str] = None,
    ) -> tuple[bool, str]:
        """
        Validate that all columns referenced in a SQL query exist in the schema.

        Args:
            sql_query: The SQL query to validate.
            dialect: Optional SQL dialect for parsing.

        Returns:
            A ``(success, message)`` tuple:

            - ``(True, "All columns exist.")`` -- all columns found.
            - ``(False, "Column Drift Detected: ...")`` -- missing columns.
            - ``(False, "Invalid SQL syntax: ...")`` -- parse failure.
        """
        try:
            columns = self.extract_columns(sql_query, dialect=dialect)
        except Exception as e:
            return False, f"Invalid SQL syntax: {e}"

        if not columns:
            # Could not extract columns (e.g., parse error or SELECT *)
            return True, "All columns exist."

        missing: list[str] = []

        for table, col in columns:
            if table:
                # Qualified reference: check specific table
                if table in self._schema:
                    if col not in self._schema[table]:
                        missing.append(f"{table}.{col}")
                # If table not in schema, skip -- table-level drift is
                # handled by SchemaValidator
            else:
                # Unqualified reference: check if column exists in any table
                if col not in self._column_lookup:
                    missing.append(col)

        if missing:
            return (
                False,
                f"Column Drift Detected: The following columns were not found: "
                f"{sorted(missing)}",
            )

        return True, "All columns exist."

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def update_schema(self, schema: SchemaDict) -> None:
        """
        Update the validator with a new schema definition.

        Args:
            schema: New schema dictionary.
        """
        self._schema_raw = schema
        self._schema, self._column_lookup = self._build_lookups(schema)

    def get_table_count(self) -> int:
        """Return the number of tables in the schema."""
        return len(self._schema)

    def get_column_count(self, table: str) -> int | None:
        """Return the number of columns in a table, or None if not found."""
        norm = self._normalize(table)
        if norm in self._schema:
            return len(self._schema[norm])
        return None

    def column_exists(self, table: str, column: str) -> bool:
        """Check if a specific column exists in a table."""
        norm_table = self._normalize(table)
        norm_col = self._normalize(column)
        if norm_table in self._schema:
            return norm_col in self._schema[norm_table]
        return False

    def get_column_info(
        self, table: str, column: str
    ) -> Optional[dict[str, str]]:
        """
        Get information about a specific column.

        Args:
            table: Table name.
            column: Column name.

        Returns:
            Dictionary with ``table``, ``column``, and optionally ``type``
            keys, or ``None`` if not found.
        """
        norm_table = self._normalize(table)
        norm_col = self._normalize(column)

        if norm_table not in self._schema or norm_col not in self._schema[norm_table]:
            return None

        # Find the original-case names and type from raw schema
        for raw_table, info in self._schema_raw.items():
            if self._normalize(raw_table) == norm_table:
                raw_columns = info.get("columns", [])
                raw_types = info.get("types", [])
                for i, c in enumerate(raw_columns):
                    if self._normalize(c) == norm_col:
                        result: dict[str, str] = {
                            "table": raw_table,
                            "column": c,
                        }
                        if i < len(raw_types):
                            result["type"] = raw_types[i]
                        return result

        return None

    # ------------------------------------------------------------------
    # Suggestions
    # ------------------------------------------------------------------

    def suggest_alternatives(self, column: str) -> list[str]:
        """
        Suggest alternative column names for a missing column.

        Uses substring matching to find similar column names across
        all tables in the schema.

        Args:
            column: The column name that was not found.

        Returns:
            List of suggestions in ``table.column`` format.
        """
        norm_col = self._normalize(column)
        suggestions: list[str] = []

        for raw_table, info in self._schema_raw.items():
            for raw_col in info.get("columns", []):
                norm_raw = self._normalize(raw_col)
                if (
                    norm_col in norm_raw
                    or norm_raw in norm_col
                ):
                    suggestions.append(f"{raw_table}.{raw_col}")

        return suggestions


class CachedColumnValidator(ColumnValidator):
    """
    Extended column validator with LRU caching for repeated queries.

    Args:
        schema: Dictionary mapping table names to column definitions.
        case_sensitive: If ``True``, column name matching is case-sensitive.
        cache_size: Maximum number of queries to cache. Defaults to ``128``.

    Examples:
        >>> cached = CachedColumnValidator(schema, cache_size=256)
        >>> cached.validate("SELECT name FROM users")  # parsed
        (True, 'All columns exist.')
        >>> cached.validate("SELECT name FROM users")  # cached
        (True, 'All columns exist.')
    """

    def __init__(
        self,
        schema: SchemaDict,
        *,
        case_sensitive: bool = False,
        cache_size: int = 128,
    ):
        super().__init__(schema, case_sensitive=case_sensitive)
        self.cache_size = cache_size
        self._validate_cached = lru_cache(maxsize=cache_size)(
            self._validate_internal
        )

    def _validate_internal(
        self,
        sql_query: str,
        dialect: Optional[str] = None,
    ) -> tuple[bool, str]:
        """Internal validation method that gets cached."""
        return super().validate(sql_query, dialect=dialect)

    def validate(
        self,
        sql_query: str,
        *,
        dialect: Optional[str] = None,
    ) -> tuple[bool, str]:
        """Validate with caching support."""
        return self._validate_cached(sql_query, dialect)

    def clear_cache(self) -> None:
        """Clear the validation cache."""
        self._validate_cached.cache_clear()

    def get_cache_info(self) -> dict:
        """
        Get cache statistics.

        Returns:
            A dictionary with keys: ``hits``, ``misses``, ``size``,
            ``maxsize``, ``hit_rate``.
        """
        info = self._validate_cached.cache_info()
        return {
            "hits": info.hits,
            "misses": info.misses,
            "size": info.currsize,
            "maxsize": info.maxsize,
            "hit_rate": (
                info.hits / (info.hits + info.misses)
                if (info.hits + info.misses) > 0
                else 0.0
            ),
        }
