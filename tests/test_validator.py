"""Tests for sqldrift validators."""

import pytest
from sqldrift import validate_query, SchemaValidator, CachedSchemaValidator


# ---------------------------------------------------------------------------
# validate_query (simple function)
# ---------------------------------------------------------------------------

class TestValidateQuery:
    """Tests for the standalone validate_query function."""

    def test_basic_valid_query(self):
        success, msg = validate_query("SELECT * FROM users", ["users"])
        assert success is True
        assert msg == "Query is safe to execute."

    def test_missing_table(self):
        success, msg = validate_query(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            ["users"],
        )
        assert success is False
        assert "orders" in msg

    def test_cte_not_flagged_as_missing(self):
        query = """
        WITH temp_users AS (
            SELECT * FROM users WHERE active = 1
        )
        SELECT * FROM temp_users
        """
        success, msg = validate_query(query, ["users"])
        assert success is True

    def test_schema_qualified_table(self):
        success, msg = validate_query(
            "SELECT * FROM schema.users",
            ["schema.users", "orders"],
        )
        assert success is True

    def test_invalid_sql_syntax(self):
        success, msg = validate_query("SELECT * FORM users", ["users"])
        assert success is False
        assert "Invalid SQL syntax" in msg or "Schema Drift" in msg

    def test_case_insensitive(self):
        success, _ = validate_query("SELECT * FROM USERS", ["users"])
        assert success is True

    def test_multiple_tables_all_present(self):
        query = (
            "SELECT * FROM users u "
            "JOIN orders o ON u.id = o.user_id "
            "JOIN products p ON o.product_id = p.id"
        )
        success, _ = validate_query(query, ["users", "orders", "products"])
        assert success is True

    def test_subquery(self):
        success, _ = validate_query(
            "SELECT * FROM (SELECT * FROM users) AS subq",
            ["users"],
        )
        assert success is True

    def test_empty_live_tables(self):
        success, _ = validate_query("SELECT * FROM users", [])
        assert success is False

    def test_deeply_qualified_name(self):
        success, _ = validate_query("SELECT * FROM db.schema.users", ["users"])
        assert success is True


# ---------------------------------------------------------------------------
# SchemaValidator (class-based)
# ---------------------------------------------------------------------------

class TestSchemaValidator:
    """Tests for the class-based SchemaValidator."""

    def test_basic_validation(self):
        v = SchemaValidator(["users", "orders"])
        success, _ = v.validate("SELECT * FROM users")
        assert success is True

    def test_reuse_across_queries(self):
        v = SchemaValidator(["users", "orders", "products"])
        for q in [
            "SELECT * FROM users",
            "SELECT * FROM orders",
            "SELECT * FROM products",
        ]:
            success, _ = v.validate(q)
            assert success is True

    def test_update_schema(self):
        v = SchemaValidator(["users"])
        success, _ = v.validate("SELECT * FROM products")
        assert success is False

        v.update_schema(["users", "products"])
        success, _ = v.validate("SELECT * FROM products")
        assert success is True

    def test_table_exists(self):
        v = SchemaValidator(["users", "orders"])
        assert v.table_exists("users") is True
        assert v.table_exists("nonexistent") is False

    def test_get_table_count(self):
        v = SchemaValidator(["a", "b", "c"])
        assert v.get_table_count() == 3

    def test_case_sensitive(self):
        v = SchemaValidator(["Users"], case_sensitive=True)
        success, _ = v.validate("SELECT * FROM Users")
        assert success is True
        success, _ = v.validate("SELECT * FROM users")
        assert success is False

    def test_preserve_schema(self):
        v = SchemaValidator(
            ["public.users", "analytics.events"],
            preserve_schema=True,
        )
        success, _ = v.validate("SELECT * FROM public.users")
        assert success is True


# ---------------------------------------------------------------------------
# CachedSchemaValidator
# ---------------------------------------------------------------------------

class TestCachedSchemaValidator:
    """Tests for the CachedSchemaValidator."""

    def test_cached_results_match(self):
        v = CachedSchemaValidator(["users", "orders"], cache_size=16)
        r1 = v.validate("SELECT * FROM users")
        r2 = v.validate("SELECT * FROM users")
        assert r1 == r2

    def test_cache_stats(self):
        v = CachedSchemaValidator(["users"], cache_size=16)
        v.validate("SELECT * FROM users")
        v.validate("SELECT * FROM users")
        info = v.get_cache_info()
        assert info["hits"] == 1
        assert info["misses"] == 1

    def test_clear_cache(self):
        v = CachedSchemaValidator(["users"], cache_size=16)
        v.validate("SELECT * FROM users")
        v.clear_cache()
        info = v.get_cache_info()
        assert info["size"] == 0
