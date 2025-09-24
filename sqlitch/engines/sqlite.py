"""
SQLite database engine implementation.

This module provides the SQLite-specific implementation of the Engine
base class, handling SQLite connections, registry management, and
SQL execution with proper error handling and transaction management.
"""

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterator, Union
from urllib.parse import urlparse

from ..core.exceptions import (
    EngineError, ConnectionError, DeploymentError
)
from ..core.types import EngineType, Target, sanitize_connection_string
from ..core.change import Change
from ..core.plan import Plan
from .base import Engine, RegistrySchema, register_engine


logger = logging.getLogger(__name__)


class SQLiteRegistrySchema(RegistrySchema):
    """SQLite-specific registry schema."""
    
    @classmethod
    def get_create_statements(cls, engine_type: EngineType) -> List[str]:
        """
        Get SQLite-specific SQL statements to create registry tables.
        
        Args:
            engine_type: Database engine type (should be 'sqlite')
            
        Returns:
            List of SQL CREATE statements for SQLite
        """
        return [
            # Begin transaction
            "BEGIN",
            
            # Releases table
            f"""
            CREATE TABLE {cls.RELEASES_TABLE} (
                version         FLOAT       PRIMARY KEY,
                installed_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                installer_name  TEXT        NOT NULL,
                installer_email TEXT        NOT NULL
            )
            """,
            
            # Projects table
            f"""
            CREATE TABLE {cls.PROJECTS_TABLE} (
                project         TEXT        PRIMARY KEY,
                uri             TEXT            NULL UNIQUE,
                created_at      DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                creator_name    TEXT        NOT NULL,
                creator_email   TEXT        NOT NULL
            )
            """,
            
            # Changes table
            f"""
            CREATE TABLE {cls.CHANGES_TABLE} (
                change_id       TEXT        PRIMARY KEY,
                script_hash     TEXT            NULL,
                change          TEXT        NOT NULL,
                project         TEXT        NOT NULL REFERENCES {cls.PROJECTS_TABLE}(project) ON UPDATE CASCADE,
                note            TEXT        NOT NULL DEFAULT '',
                committed_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                committer_name  TEXT        NOT NULL,
                committer_email TEXT        NOT NULL,
                planned_at      DATETIME    NOT NULL,
                planner_name    TEXT        NOT NULL,
                planner_email   TEXT        NOT NULL,
                UNIQUE(project, script_hash)
            )
            """,
            
            # Tags table
            f"""
            CREATE TABLE {cls.TAGS_TABLE} (
                tag_id          TEXT        PRIMARY KEY,
                tag             TEXT        NOT NULL,
                project         TEXT        NOT NULL REFERENCES {cls.PROJECTS_TABLE}(project) ON UPDATE CASCADE,
                change_id       TEXT        NOT NULL REFERENCES {cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE,
                note            TEXT        NOT NULL DEFAULT '',
                committed_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                committer_name  TEXT        NOT NULL,
                committer_email TEXT        NOT NULL,
                planned_at      DATETIME    NOT NULL,
                planner_name    TEXT        NOT NULL,
                planner_email   TEXT        NOT NULL,
                UNIQUE(project, tag)
            )
            """,
            
            # Dependencies table
            f"""
            CREATE TABLE {cls.DEPENDENCIES_TABLE} (
                change_id       TEXT        NOT NULL REFERENCES {cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE ON DELETE CASCADE,
                type            TEXT        NOT NULL,
                dependency      TEXT        NOT NULL,
                dependency_id   TEXT            NULL REFERENCES {cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE
                                             CONSTRAINT dependencies_check CHECK (
                    (type = 'require'  AND dependency_id IS NOT NULL)
                 OR (type = 'conflict' AND dependency_id IS NULL)
                ),
                PRIMARY KEY (change_id, dependency)
            )
            """,
            
            # Events table
            f"""
            CREATE TABLE {cls.EVENTS_TABLE} (
                event           TEXT        NOT NULL CONSTRAINT events_event_check CHECK (
                    event IN ('deploy', 'revert', 'fail', 'merge')
                ),
                change_id       TEXT        NOT NULL,
                change          TEXT        NOT NULL,
                project         TEXT        NOT NULL REFERENCES {cls.PROJECTS_TABLE}(project) ON UPDATE CASCADE,
                note            TEXT        NOT NULL DEFAULT '',
                requires        TEXT        NOT NULL DEFAULT '',
                conflicts       TEXT        NOT NULL DEFAULT '',
                tags            TEXT        NOT NULL DEFAULT '',
                committed_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                committer_name  TEXT        NOT NULL,
                committer_email TEXT        NOT NULL,
                planned_at      DATETIME    NOT NULL,
                planner_name    TEXT        NOT NULL,
                planner_email   TEXT        NOT NULL,
                PRIMARY KEY (change_id, committed_at)
            )
            """,
            
            # Insert registry version
            f"""
            INSERT INTO {cls.RELEASES_TABLE} (version, installer_name, installer_email)
            VALUES ({cls.REGISTRY_VERSION}, 'sqlitch', 'sqlitch@example.com')
            """,
            
            # Commit transaction
            "COMMIT"
        ]


class SQLiteConnection:
    """Wrapper for SQLite connection with sqitch-specific functionality."""
    
    def __init__(self, connection: sqlite3.Connection):
        """
        Initialize SQLite connection wrapper.
        
        Args:
            connection: sqlite3 connection object
        """
        self._connection = connection
        self._connection.row_factory = sqlite3.Row  # Enable dict-like access
        self._cursor: Optional[sqlite3.Cursor] = None
    
    def execute(self, sql_query: str, params: Optional[Dict[str, Any]] = None) -> None:
        """
        Execute SQL statement.
        
        Args:
            sql_query: SQL query to execute
            params: Optional parameters for query
            
        Raises:
            DeploymentError: If SQL execution fails
        """
        try:
            cursor = self._get_cursor()
            if params:
                # If we have positional placeholders (?) but named parameters,
                # convert to positional parameters in the order they appear
                if isinstance(params, dict) and '?' in sql_query:
                    # Count the number of ? placeholders
                    placeholder_count = sql_query.count('?')
                    if len(params) == placeholder_count:
                        # Convert to list in the order of the SQL statement
                        # This is a simple approach - for production, we'd need more sophisticated parsing
                        param_values = list(params.values())
                        cursor.execute(sql_query, param_values)
                    else:
                        cursor.execute(sql_query, params)
                else:
                    cursor.execute(sql_query, params)
            else:
                cursor.execute(sql_query)
        except sqlite3.Error as e:
            raise DeploymentError(
                f"SQL execution failed: {e}",
                engine_name="sqlite"
            ) from e
    
    def fetchone(self) -> Optional[Dict[str, Any]]:
        """
        Fetch one row from result set.
        
        Returns:
            Dictionary representing the row or None
        """
        cursor = self._get_cursor()
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def fetchall(self) -> List[Dict[str, Any]]:
        """
        Fetch all rows from result set.
        
        Returns:
            List of dictionaries representing the rows
        """
        cursor = self._get_cursor()
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def commit(self) -> None:
        """Commit current transaction."""
        self._connection.commit()
    
    def rollback(self) -> None:
        """Rollback current transaction."""
        self._connection.rollback()
    
    def close(self) -> None:
        """Close the connection."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        self._connection.close()
    
    def _get_cursor(self) -> sqlite3.Cursor:
        """Get cursor for executing queries."""
        if not self._cursor:
            self._cursor = self._connection.cursor()
        return self._cursor


@register_engine('sqlite')
class SQLiteEngine(Engine):
    """
    SQLite database engine implementation.
    
    This engine handles SQLite-specific database operations including
    connection management, registry table creation, and SQL execution
    with proper transaction handling.
    """
    
    def __init__(self, target: Target, plan: Plan) -> None:
        """
        Initialize SQLite engine.
        
        Args:
            target: Target configuration for this engine
            plan: Plan containing changes to manage
            
        Raises:
            EngineError: If SQLite database path is invalid
        """
        super().__init__(target, plan)
        self._registry_schema = SQLiteRegistrySchema()
        
        # Parse database path from URI
        self._db_path = self._parse_database_path(target.uri)
        
        # Ensure parent directory exists
        if self._db_path != ":memory:":
            db_file = Path(self._db_path)
            db_file.parent.mkdir(parents=True, exist_ok=True)
    
    @property
    def engine_type(self) -> EngineType:
        """Get the engine type identifier."""
        return 'sqlite'
    
    @property
    def registry_schema(self) -> RegistrySchema:
        """Get the registry schema for this engine."""
        return self._registry_schema
    
    def _parse_database_path(self, uri: str) -> str:
        """
        Parse database file path from URI.
        
        Args:
            uri: Database URI
            
        Returns:
            Database file path
            
        Raises:
            EngineError: If URI is invalid
        """
        try:
            if uri.startswith('sqlite:'):
                # Handle sqlite:path or sqlite:///path
                if uri.startswith('sqlite:///'):
                    return uri[10:]  # Remove 'sqlite:///'
                elif uri.startswith('sqlite://'):
                    return uri[9:]   # Remove 'sqlite://'
                else:
                    return uri[7:]   # Remove 'sqlite:'
            elif uri.startswith('db:sqlite:'):
                # Handle db:sqlite:path format
                return uri[10:]
            else:
                # Assume it's a direct file path
                return uri
        except Exception as e:
            raise EngineError(
                f"Invalid SQLite URI: {uri}",
                engine_name=self.engine_type
            ) from e
    
    def _create_connection(self) -> SQLiteConnection:
        """
        Create a new SQLite database connection.
        
        Returns:
            SQLite connection wrapper
            
        Raises:
            ConnectionError: If connection cannot be established
        """
        try:
            # Configure SQLite connection
            connection = sqlite3.connect(
                self._db_path,
                timeout=30.0,  # 30 second timeout
                isolation_level=None,  # Autocommit mode off
                check_same_thread=False
            )
            
            # Configure datetime handling to avoid deprecation warnings
            # Use the recommended approach for Python 3.12+
            def adapt_datetime(dt):
                return dt.isoformat()
            
            def convert_datetime(val):
                from datetime import datetime
                return datetime.fromisoformat(val.decode())
            
            sqlite3.register_adapter(datetime, adapt_datetime)
            sqlite3.register_converter("DATETIME", convert_datetime)
            
            # Enable foreign key constraints
            connection.execute("PRAGMA foreign_keys = ON")
            
            # Set SQLite to use immediate transactions for better concurrency
            connection.execute("PRAGMA locking_mode = NORMAL")
            
            # Check SQLite version compatibility
            cursor = connection.cursor()
            cursor.execute("SELECT sqlite_version()")
            version = cursor.fetchone()[0]
            
            # Require SQLite 3.8.6 or later (matching Perl sqitch)
            version_parts = [int(x) for x in version.split('.')]
            if (version_parts[0] < 3 or 
                (version_parts[0] == 3 and version_parts[1] < 8) or
                (version_parts[0] == 3 and version_parts[1] == 8 and version_parts[2] < 6)):
                raise EngineError(
                    f"Sqitch requires SQLite 3.8.6 or later; found {version}",
                    engine_name=self.engine_type
                )
            
            self.logger.debug(f"Connected to SQLite database: {sanitize_connection_string(self._db_path)}")
            return SQLiteConnection(connection)
            
        except sqlite3.Error as e:
            raise ConnectionError(
                f"Failed to connect to SQLite database: {e}",
                connection_string=sanitize_connection_string(self._db_path),
                engine_name=self.engine_type
            ) from e
    
    def _execute_sql_file(self, connection: SQLiteConnection, sql_file: Path, 
                         variables: Optional[Dict[str, Any]] = None) -> None:
        """
        Execute SQL file with optional variable substitution.
        
        Args:
            connection: SQLite connection
            sql_file: Path to SQL file to execute
            variables: Optional variables for substitution
            
        Raises:
            DeploymentError: If SQL execution fails
        """
        try:
            if not sql_file.exists():
                raise DeploymentError(
                    f"SQL file not found: {sql_file}",
                    engine_name=self.engine_type
                )
            
            sql_content = sql_file.read_text(encoding='utf-8')
            
            # Apply variable substitution if provided
            if variables:
                for key, value in variables.items():
                    sql_content = sql_content.replace(f":{key}", str(value))
            
            # Execute SQL content
            # SQLite executescript() handles multiple statements
            connection._connection.executescript(sql_content)
            
            self.logger.debug(f"Executed SQL file: {sql_file}")
            
        except sqlite3.Error as e:
            raise DeploymentError(
                f"Failed to execute SQL file {sql_file}: {e}",
                engine_name=self.engine_type,
                sql_file=str(sql_file)
            ) from e
        except Exception as e:
            raise DeploymentError(
                f"Error reading SQL file {sql_file}: {e}",
                engine_name=self.engine_type,
                sql_file=str(sql_file)
            ) from e
    
    def _get_registry_version(self, connection: SQLiteConnection) -> Optional[str]:
        """
        Get current registry version from database.
        
        Args:
            connection: SQLite connection
            
        Returns:
            Registry version string or None if not found
        """
        try:
            connection.execute(
                f"SELECT CAST(ROUND(MAX(version), 1) AS TEXT) FROM {self.registry_schema.RELEASES_TABLE}"
            )
            row = connection.fetchone()
            return row['CAST(ROUND(MAX(version), 1) AS TEXT)'] if row else None
        except sqlite3.Error:
            return None
    
    def _registry_exists_in_db(self, connection: SQLiteConnection) -> bool:
        """
        Check if registry tables exist in database.
        
        Args:
            connection: SQLite connection
            
        Returns:
            True if registry exists, False otherwise
        """
        try:
            connection.execute(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = :name)",
                {'name': self.registry_schema.CHANGES_TABLE}
            )
            row = connection.fetchone()
            # The column name will be the full expression since we didn't alias it
            key = list(row.keys())[0] if row else None
            return bool(row and row[key]) if key else False
        except sqlite3.Error:
            return False
    
    def run_file(self, file_path: Path) -> None:
        """
        Run SQL file using SQLite command-line client.
        
        Args:
            file_path: Path to SQL file to execute
            
        Raises:
            DeploymentError: If execution fails
        """
        with self.connection() as conn:
            self._execute_sql_file(conn, file_path)
    
    def run_verify(self, file_path: Path) -> None:
        """
        Run verification SQL file.
        
        Args:
            file_path: Path to verification SQL file
            
        Raises:
            DeploymentError: If verification fails
        """
        with self.connection() as conn:
            self._execute_sql_file(conn, file_path)


# Register the SQLite engine