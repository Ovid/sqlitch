"""
Database engine implementations for sqlitch.

This module contains database-specific implementations for all supported
database engines including PostgreSQL, MySQL, SQLite, Oracle, and others.
"""

from .base import Engine, EngineRegistry, RegistrySchema, register_engine

# Import engines to register them
try:
    from . import pg
except ImportError:
    pass  # PostgreSQL engine not available

try:
    from . import mysql
except ImportError:
    pass  # MySQL engine not available

try:
    from . import sqlite
except ImportError:
    pass  # SQLite engine not available

try:
    from . import oracle
except ImportError:
    pass  # Oracle engine not available

try:
    from . import snowflake
except ImportError:
    pass  # Snowflake engine not available

__all__ = [
    'Engine',
    'EngineRegistry', 
    'RegistrySchema',
    'register_engine'
]