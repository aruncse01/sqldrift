"""
sqldrift — Detect schema drift before your SQL queries blow up.

Validates SQL queries against a live schema to catch missing tables
and columns before execution. Built for AI agents and automated SQL pipelines.
"""

__version__ = "0.1.4"

from sqldrift.validator import validate_query
from sqldrift.optimized import SchemaValidator, CachedSchemaValidator
from sqldrift.column_validator import ColumnValidator, CachedColumnValidator

__all__ = [
    "validate_query",
    "SchemaValidator",
    "CachedSchemaValidator",
    "ColumnValidator",
    "CachedColumnValidator",
]

