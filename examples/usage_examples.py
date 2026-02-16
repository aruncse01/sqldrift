"""
Usage examples for sqldrift.

Run with: python examples/usage_examples.py
(Requires: pip install -e . from the project root first)
"""

from sqldrift import validate_query, SchemaValidator, CachedSchemaValidator

# Simulate a schema with 4000+ tables
live_tables = [f"table_{i}" for i in range(4000)]
live_tables.extend(["users", "orders", "products", "customers"])

print("=" * 70)
print("sqldrift — Usage Examples")
print("=" * 70)
print(f"Schema size: {len(live_tables):,} tables\n")

# ----- Example 1: One-off validation ---------------------------------
print("— Example 1: One-off validation (simple function)")
query = "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
success, msg = validate_query(query, live_tables)
print(f"  Query:   {query}")
print(f"  Result:  {success} — {msg}\n")

# ----- Example 2: Reusable validator ----------------------------------
print("— Example 2: Reusable SchemaValidator (recommended)")
validator = SchemaValidator(live_tables)

queries = [
    "SELECT * FROM users",
    "SELECT * FROM table_1999 JOIN table_3500 ON table_1999.id = table_3500.id",
    "SELECT * FROM nonexistent_table",
]
for q in queries:
    ok, m = validator.validate(q)
    status = "✅" if ok else "❌"
    print(f"  {status} {q[:55]:<55} {m[:40]}")
print()

# ----- Example 3: CTE handling ---------------------------------------
print("— Example 3: CTE handling")
cte_query = """
WITH active_users AS (SELECT * FROM users WHERE status = 'active')
SELECT * FROM active_users
"""
ok, m = validator.validate(cte_query)
print(f"  CTEs correctly ignored: {ok}\n")

# ----- Example 4: Cached validator ------------------------------------
print("— Example 4: CachedSchemaValidator (repeated queries)")
cached = CachedSchemaValidator(live_tables, cache_size=256)
for _ in range(10):
    cached.validate("SELECT * FROM users")
info = cached.get_cache_info()
print(f"  Cache hits: {info['hits']}, misses: {info['misses']}, "
      f"hit rate: {info['hit_rate']*100:.0f}%\n")

# ----- Example 5: Dynamic schema updates -----------------------------
print("— Example 5: Dynamic schema updates")
v = SchemaValidator(["users"])
ok1, _ = v.validate("SELECT * FROM products")
v.update_schema(["users", "products"])
ok2, _ = v.validate("SELECT * FROM products")
print(f"  Before update: {ok1}  →  After update: {ok2}\n")

print("=" * 70)
print("Done! See README.md for full API reference.")
print("=" * 70)
