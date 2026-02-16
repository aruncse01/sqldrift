"""
Benchmark: compare original function vs class-based vs cached validators.

Run with: python benchmarks/benchmark.py
(Requires: pip install -e . from the project root first)
"""

import time
from sqldrift import validate_query, SchemaValidator, CachedSchemaValidator


def bench(label: str, fn, iterations: int = 100) -> dict:
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    avg = sum(times) / len(times)
    return {"label": label, "avg_ms": avg * 1000, "min_ms": min(times) * 1000}


for scale in (100, 1_000, 4_000, 10_000):
    tables = [f"table_{i}" for i in range(scale)]
    query = (
        f"SELECT * FROM table_{scale//2} "
        f"JOIN table_{scale-1} ON table_{scale//2}.id = table_{scale-1}.id"
    )

    print(f"\n{'='*70}")
    print(f"Scale: {scale:,} tables")
    print(f"{'='*70}")

    r1 = bench("Function", lambda: validate_query(query, tables))
    validator = SchemaValidator(tables)
    r2 = bench("Class", lambda: validator.validate(query))
    cached = CachedSchemaValidator(tables, cache_size=128)
    r3 = bench("Cached", lambda: cached.validate(query))

    print(f"  {'Method':<12} {'Avg (ms)':>10} {'Min (ms)':>10}")
    print(f"  {'-'*34}")
    for r in (r1, r2, r3):
        print(f"  {r['label']:<12} {r['avg_ms']:>10.4f} {r['min_ms']:>10.4f}")

    print(f"\n  Class speedup:  {r1['avg_ms']/r2['avg_ms']:.1f}x")
    print(f"  Cached speedup: {r1['avg_ms']/r3['avg_ms']:.1f}x")
