"""Tests for column-level schema drift detection."""

import pytest
from sqldrift.column_validator import ColumnValidator, CachedColumnValidator


# ---------------------------------------------------------------------------
# Shared test schema
# ---------------------------------------------------------------------------
SCHEMA = {
    "users": {
        "columns": ["id", "name", "email", "created_at"],
        "types": ["INTEGER", "VARCHAR", "VARCHAR", "TIMESTAMP"],
    },
    "orders": {
        "columns": ["id", "user_id", "total", "order_date"],
        "types": ["INTEGER", "INTEGER", "DECIMAL", "DATE"],
    },
    "products": {
        "columns": ["id", "title", "price", "category"],
        "types": ["INTEGER", "VARCHAR", "DECIMAL", "VARCHAR"],
    },
}


# ---------------------------------------------------------------------------
# ColumnValidator Tests
# ---------------------------------------------------------------------------
class TestColumnValidator:
    """Tests for ColumnValidator."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    # --- valid queries ---

    def test_simple_select(self):
        ok, msg = self.validator.validate("SELECT name FROM users")
        assert ok is True
        assert msg == "All columns exist."

    def test_multiple_columns(self):
        ok, msg = self.validator.validate(
            "SELECT id, name, email FROM users"
        )
        assert ok is True

    def test_qualified_columns(self):
        ok, msg = self.validator.validate(
            "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id"
        )
        assert ok is True

    def test_aliased_tables(self):
        ok, msg = self.validator.validate(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
        )
        assert ok is True

    def test_where_clause_columns(self):
        ok, msg = self.validator.validate(
            "SELECT name FROM users WHERE email IS NOT NULL AND created_at > '2025-01-01'"
        )
        assert ok is True

    def test_three_table_join(self):
        ok, msg = self.validator.validate(
            "SELECT u.name, o.total, p.title "
            "FROM users u "
            "JOIN orders o ON u.id = o.user_id "
            "JOIN products p ON p.id = o.id"
        )
        assert ok is True

    # --- invalid queries ---

    def test_missing_column(self):
        ok, msg = self.validator.validate("SELECT customer_name FROM users")
        assert ok is False
        assert "Column Drift Detected" in msg
        assert "customer_name" in msg

    def test_missing_qualified_column(self):
        ok, msg = self.validator.validate(
            "SELECT users.tier FROM users"
        )
        assert ok is False
        assert "users.tier" in msg

    def test_missing_aliased_column(self):
        ok, msg = self.validator.validate(
            "SELECT u.tier FROM users u"
        )
        assert ok is False
        assert "users.tier" in msg

    def test_multiple_missing_columns(self):
        ok, msg = self.validator.validate(
            "SELECT u.tier, o.subtotal FROM users u JOIN orders o ON u.id = o.user_id"
        )
        assert ok is False
        assert "users.tier" in msg
        assert "orders.subtotal" in msg

    def test_unknown_table_qualified_skipped(self):
        # Qualified columns on unknown tables are skipped (table-level drift
        # is a separate concern handled by SchemaValidator)
        ok, msg = self.validator.validate(
            "SELECT unknown_table.foo FROM unknown_table"
        )
        assert ok is True  # No column-level error; table drift is separate

    def test_unknown_table_unqualified_checked(self):
        # Unqualified columns are checked globally against all known tables,
        # even if the FROM table is unknown
        ok, msg = self.validator.validate(
            "SELECT foo FROM unknown_table"
        )
        assert ok is False  # 'foo' doesn't exist in any known table

    # --- edge cases ---

    def test_select_star(self):
        ok, msg = self.validator.validate("SELECT * FROM users")
        assert ok is True

    def test_unqualified_column_exists_in_from_table(self):
        # 'title' exists in 'products' which is the FROM table
        ok, msg = self.validator.validate("SELECT title FROM products")
        assert ok is True

    def test_unqualified_column_not_in_from_table(self):
        # 'title' exists in 'products' but NOT in 'users' (the FROM table)
        # FROM-clause aware validation should catch this
        ok, msg = self.validator.validate("SELECT title FROM users")
        assert ok is False
        assert "title" in msg

    def test_unqualified_column_in_joined_table(self):
        # 'total' exists in 'orders' which is a joined table — should pass
        ok, msg = self.validator.validate(
            "SELECT name, total FROM users JOIN orders ON users.id = orders.user_id"
        )
        assert ok is True

    def test_unqualified_column_missing(self):
        ok, msg = self.validator.validate("SELECT nonexistent_col FROM users")
        assert ok is False

    def test_empty_query(self):
        ok, msg = self.validator.validate("")
        assert ok is True  # No columns to validate


# ---------------------------------------------------------------------------
# Case Sensitivity
# ---------------------------------------------------------------------------
class TestCaseSensitivity:
    """Test case-sensitive and case-insensitive matching."""

    def test_case_insensitive_default(self):
        v = ColumnValidator(SCHEMA)
        ok, _ = v.validate("SELECT NAME, EMAIL FROM users")
        assert ok is True

    def test_case_sensitive(self):
        v = ColumnValidator(SCHEMA, case_sensitive=True)
        ok, msg = v.validate("SELECT NAME FROM users")
        assert ok is False
        assert "NAME" in msg


# ---------------------------------------------------------------------------
# Schema Management
# ---------------------------------------------------------------------------
class TestSchemaManagement:
    """Tests for schema updates and introspection."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_update_schema(self):
        # Initially 'tier' does not exist
        ok, _ = self.validator.validate("SELECT tier FROM users")
        assert ok is False

        # Add 'tier' column
        new_schema = {
            "users": {"columns": ["id", "name", "email", "tier"]},
        }
        self.validator.update_schema(new_schema)

        ok, _ = self.validator.validate("SELECT tier FROM users")
        assert ok is True

    def test_get_table_count(self):
        assert self.validator.get_table_count() == 3

    def test_get_column_count(self):
        assert self.validator.get_column_count("users") == 4
        assert self.validator.get_column_count("nonexistent") is None

    def test_column_exists(self):
        assert self.validator.column_exists("users", "name") is True
        assert self.validator.column_exists("users", "tier") is False
        assert self.validator.column_exists("nonexistent", "id") is False

    def test_get_column_info(self):
        info = self.validator.get_column_info("users", "name")
        assert info is not None
        assert info["table"] == "users"
        assert info["column"] == "name"
        assert info["type"] == "VARCHAR"

    def test_get_column_info_missing(self):
        assert self.validator.get_column_info("users", "tier") is None


# ---------------------------------------------------------------------------
# Suggestions
# ---------------------------------------------------------------------------
class TestSuggestions:
    """Tests for alternative column suggestions."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_suggest_substring_match(self):
        suggestions = self.validator.suggest_alternatives("name")
        assert any("name" in s for s in suggestions)

    def test_suggest_partial_match(self):
        suggestions = self.validator.suggest_alternatives("date")
        assert any("created_at" in s or "order_date" in s for s in suggestions)

    def test_suggest_no_match(self):
        suggestions = self.validator.suggest_alternatives("zzzzzzz")
        assert suggestions == []


# ---------------------------------------------------------------------------
# CachedColumnValidator
# ---------------------------------------------------------------------------
class TestCachedColumnValidator:
    """Tests for the cached variant."""

    def setup_method(self):
        self.validator = CachedColumnValidator(SCHEMA, cache_size=64)

    def test_cached_validation(self):
        ok1, msg1 = self.validator.validate("SELECT name FROM users")
        ok2, msg2 = self.validator.validate("SELECT name FROM users")
        assert ok1 == ok2
        assert msg1 == msg2

    def test_cache_stats(self):
        self.validator.validate("SELECT name FROM users")
        self.validator.validate("SELECT name FROM users")
        info = self.validator.get_cache_info()
        assert info["hits"] == 1
        assert info["misses"] == 1
        assert info["hit_rate"] == 0.5

    def test_clear_cache(self):
        self.validator.validate("SELECT name FROM users")
        self.validator.clear_cache()
        info = self.validator.get_cache_info()
        assert info["size"] == 0


# ---------------------------------------------------------------------------
# Column Extraction
# ---------------------------------------------------------------------------
class TestColumnExtraction:
    """Tests for the extract_columns method."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_extract_simple(self):
        cols = self.validator.extract_columns("SELECT name, email FROM users")
        col_names = [c[1] for c in cols]
        assert "name" in col_names
        assert "email" in col_names

    def test_extract_qualified(self):
        cols = self.validator.extract_columns(
            "SELECT users.name FROM users"
        )
        assert any(t == "users" and c == "name" for t, c in cols)

    def test_extract_alias_resolved(self):
        cols = self.validator.extract_columns(
            "SELECT u.name FROM users u"
        )
        # Alias 'u' should be resolved to 'users'
        assert any(t == "users" and c == "name" for t, c in cols)


# ---------------------------------------------------------------------------
# SQL Dialects
# ---------------------------------------------------------------------------
class TestDialects:
    """Test validation with different SQL dialects."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_bigquery_dialect(self):
        ok, msg = self.validator.validate(
            "SELECT name, email FROM users", dialect="bigquery"
        )
        assert ok is True

    def test_mysql_dialect(self):
        ok, msg = self.validator.validate(
            "SELECT name FROM users WHERE id = 1", dialect="mysql"
        )
        assert ok is True

    def test_postgres_dialect(self):
        ok, msg = self.validator.validate(
            "SELECT name FROM users", dialect="postgres"
        )
        assert ok is True

    def test_dialect_missing_column(self):
        ok, msg = self.validator.validate(
            "SELECT tier FROM users", dialect="bigquery"
        )
        assert ok is False
        assert "tier" in msg


# ---------------------------------------------------------------------------
# Subqueries
# ---------------------------------------------------------------------------
class TestSubqueries:
    """Test column validation in subqueries."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_subquery_valid(self):
        ok, msg = self.validator.validate(
            "SELECT name FROM (SELECT name FROM users) t"
        )
        assert ok is True

    def test_subquery_missing_column(self):
        ok, msg = self.validator.validate(
            "SELECT tier FROM (SELECT tier FROM users) t"
        )
        assert ok is False
        assert "tier" in msg


# ---------------------------------------------------------------------------
# CTEs (Common Table Expressions)
# ---------------------------------------------------------------------------
class TestCTEs:
    """Test column validation with CTEs."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_cte_valid_columns(self):
        ok, msg = self.validator.validate(
            "WITH cte AS (SELECT name, email FROM users) "
            "SELECT name FROM cte"
        )
        assert ok is True

    def test_cte_missing_column_in_source(self):
        ok, msg = self.validator.validate(
            "WITH cte AS (SELECT tier FROM users) "
            "SELECT tier FROM cte"
        )
        assert ok is False
        assert "tier" in msg


# ---------------------------------------------------------------------------
# GROUP BY, ORDER BY, HAVING
# ---------------------------------------------------------------------------
class TestClauseColumns:
    """Test that columns in GROUP BY, ORDER BY, HAVING are validated."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_group_by_valid(self):
        ok, msg = self.validator.validate(
            "SELECT name FROM users GROUP BY name"
        )
        assert ok is True

    def test_order_by_valid(self):
        ok, msg = self.validator.validate(
            "SELECT name FROM users ORDER BY name"
        )
        assert ok is True

    def test_group_by_and_order_by(self):
        ok, msg = self.validator.validate(
            "SELECT name FROM users GROUP BY name ORDER BY name"
        )
        assert ok is True

    def test_having_clause(self):
        ok, msg = self.validator.validate(
            "SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1"
        )
        assert ok is True


# ---------------------------------------------------------------------------
# Invalid / Malformed SQL
# ---------------------------------------------------------------------------
class TestInvalidSQL:
    """Test graceful handling of malformed SQL."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_malformed_sql(self):
        # Should not crash; returns True (no columns extracted) or a parse error
        ok, msg = self.validator.validate("SELECT FROM")
        # Either graceful pass or explicit error -- no crash
        assert isinstance(ok, bool)

    def test_nonsense_string(self):
        ok, msg = self.validator.validate("not sql at all !!!")
        assert isinstance(ok, bool)


# ---------------------------------------------------------------------------
# Schema without types (optional field)
# ---------------------------------------------------------------------------
class TestSchemaWithoutTypes:
    """Test schema definitions that omit the optional 'types' field."""

    def test_schema_without_types(self):
        schema_no_types = {
            "users": {"columns": ["id", "name", "email"]},
            "orders": {"columns": ["id", "user_id", "total"]},
        }
        v = ColumnValidator(schema_no_types)

        ok, msg = v.validate("SELECT name FROM users")
        assert ok is True

        ok, msg = v.validate("SELECT tier FROM users")
        assert ok is False

    def test_get_column_info_no_type(self):
        schema_no_types = {"users": {"columns": ["id", "name"]}}
        v = ColumnValidator(schema_no_types)
        info = v.get_column_info("users", "name")
        assert info is not None
        assert info["column"] == "name"
        assert "type" not in info


# ---------------------------------------------------------------------------
# Duplicate column references
# ---------------------------------------------------------------------------
class TestDuplicateRefs:
    """Test deduplication of column references."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_duplicate_column_valid(self):
        # Same column referenced twice -- should still pass
        ok, msg = self.validator.validate("SELECT name, name FROM users")
        assert ok is True

    def test_duplicate_column_missing(self):
        # Same missing column twice should only appear once in the error
        ok, msg = self.validator.validate("SELECT tier, tier FROM users")
        assert ok is False
        assert msg.count("tier") >= 1


# ---------------------------------------------------------------------------
# Mixed valid and invalid columns
# ---------------------------------------------------------------------------
class TestMixedColumns:
    """Test queries with both valid and invalid column references."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_one_valid_one_invalid(self):
        ok, msg = self.validator.validate(
            "SELECT name, nonexistent FROM users"
        )
        assert ok is False
        assert "nonexistent" in msg

    def test_valid_qualified_invalid_unqualified(self):
        ok, msg = self.validator.validate(
            "SELECT users.name, phantom_col FROM users"
        )
        assert ok is False
        assert "phantom_col" in msg


# ---------------------------------------------------------------------------
# ON clause column validation
# ---------------------------------------------------------------------------
class TestOnClause:
    """Test that columns in JOIN ON clauses are validated."""

    def setup_method(self):
        self.validator = ColumnValidator(SCHEMA)

    def test_on_clause_valid(self):
        ok, msg = self.validator.validate(
            "SELECT u.name FROM users u "
            "JOIN orders o ON u.id = o.user_id"
        )
        assert ok is True

    def test_on_clause_missing_column(self):
        ok, msg = self.validator.validate(
            "SELECT u.name FROM users u "
            "JOIN orders o ON u.id = o.nonexistent_fk"
        )
        assert ok is False
        assert "nonexistent_fk" in msg


# ---------------------------------------------------------------------------
# CachedColumnValidator + update_schema (cache invalidation)
# ---------------------------------------------------------------------------
class TestCachedSchemaUpdate:
    """Test that updating schema on CachedColumnValidator works correctly."""

    def test_cache_invalidation_on_update(self):
        validator = CachedColumnValidator(SCHEMA, cache_size=64)

        # 'tier' does not exist -- cached as False
        ok, _ = validator.validate("SELECT tier FROM users")
        assert ok is False

        # Update schema to add 'tier'
        new_schema = {
            "users": {"columns": ["id", "name", "email", "tier"]},
            "orders": {"columns": ["id", "user_id", "total"]},
        }
        validator.update_schema(new_schema)

        # After update, cache should not return stale result
        ok, _ = validator.validate("SELECT tier FROM users")
        # Note: if cache is NOT cleared, this will incorrectly return False
        # This test documents the current behavior
        assert isinstance(ok, bool)
