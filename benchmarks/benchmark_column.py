"""
Benchmark for ColumnValidator with large schemas (4000+ tables).
"""
import time
import random
import string
from sqldrift import ColumnValidator, CachedColumnValidator

def generate_schema(num_tables=4000, cols_per_table=20):
    print(f"Generating schema with {num_tables} tables, {cols_per_table} columns each...")
    schema = {}
    
    # Common columns that appear in many tables (for ambiguity testing)
    common_cols = ["id", "created_at", "updated_at", "is_deleted", "name", "description"]
    
    for i in range(num_tables):
        table_name = f"table_{i}"
        columns = common_cols.copy()
        
        # Add unique columns
        for j in range(cols_per_table - len(common_cols)):
            col_name = f"col_{i}_{j}"
            columns.append(col_name)
            
        schema[table_name] = {"columns": columns}
    
    return schema

def run_benchmark():
    # 1. Generate Schema
    schema = generate_schema(4000, 25)
    total_cols = sum(len(t["columns"]) for t in schema.values())
    print(f"Total columns: {total_cols}")
    print("-" * 40)

    # 2. Measure Initialization
    start = time.time()
    validator = ColumnValidator(schema)
    init_time = time.time() - start
    print(f"Init (Cold): {init_time:.4f}s")
    
    # 3. Validation Benchmarks
    queries = [
        # Simple
        "SELECT id, col_100_10 FROM table_100",
        # Qualified
        "SELECT table_200.id, table_200.col_200_5 FROM table_200",
        # Join (2 tables)
        "SELECT t1.id, t2.col_2_5 FROM table_1 t1 JOIN table_2 t2 ON t1.id = t2.id",
        # Complex Join (5 tables)
        "SELECT t1.id, t5.col_5_1 FROM table_1 t1 JOIN table_2 t2 ON t1.id = t2.id JOIN table_3 t3 ON t2.id = t3.id JOIN table_4 t4 ON t3.id = t4.id JOIN table_5 t5 ON t4.id = t5.id",
        # Missing Column (for error consistency)
        "SELECT non_existent_col FROM table_1",
    ]
    
    print("-" * 40)
    print("Validation (uncached):")
    for q in queries:
        start = time.time()
        # Run 100 times
        for _ in range(100):
            validator.validate(q)
        avg = (time.time() - start) / 100
        print(f"  {q[:40]}... : {avg*1000:.3f}ms")

    # 4. Cached Validation
    print("-" * 40)
    print("Validation (CachedColumnValidator, hit rate 100%):")
    cached_validator = CachedColumnValidator(schema, cache_size=1024)
    
    # Warmup
    for q in queries:
        cached_validator.validate(q)
        
    for q in queries:
        start = time.time()
        # Run 1000 times (cached)
        for _ in range(1000):
            cached_validator.validate(q)
        avg = (time.time() - start) / 1000
        print(f"  {q[:40]}... : {avg*1000:.4f}ms")

if __name__ == "__main__":
    run_benchmark()
