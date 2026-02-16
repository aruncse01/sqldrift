"""
sqldrift — Detect schema drift before your SQL queries blow up.

Validates SQL queries against a live schema to catch missing tables
before execution. Built for AI agents and automated SQL pipelines.
"""

__version__ = "0.1.0"

from sqldrift.validator import validate_query
from sqldrift.optimized import SchemaValidator, CachedSchemaValidator

__all__ = [
    "validate_query",
    "SchemaValidator",
    "CachedSchemaValidator",
]
