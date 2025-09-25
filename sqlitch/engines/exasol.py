"""
Exasol database engine implementation.

This module provides the Exasol-specific implementation of the Engine
base class, handling Exasol connections, registry management, and
SQL execution with proper error handling and transaction management.
"""

import logging
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union
from urllib.parse import parse_qs, urlparse

from ..core.change import Change
from ..core.exceptions import ConnectionError, DeploymentError, EngineError
from ..core.plan import Plan
from ..core.target import Target
from ..core.types import EngineType, sanitize_connection_string
from .base import Engine, RegistrySchema, register_engine

# Try to import pyexasol
try:
    import pyexasol
except ImportError:
    pyexasol = None


logger = logging.getLogger(__name__)


class ExasolRegistrySchema(RegistrySchema):
    """Exasol-specific registry schema."""

    @classmethod
    def get_create_statements(
        cls, engine_type: EngineType, registry_schema: str = None
    ) -> List[str]:
        """
        Get Exasol-specific SQL statements to create registry tables.

        Args:
            engine_type: Database engine type (should be 'exasol')
            registry_schema: Registry schema name

        Returns:
            List of SQL CREATE statements for Exasol
        """
        schema_prefix = f"{registry_schema}." if registry_schema else ""

        return [
            # Create schema if it doesn't exist
            f"CREATE SCHEMA IF NOT EXISTS {registry_schema}" if registry_schema else "",
            # Releases table
            f"""
            CREATE TABLE {schema_prefix}{cls.RELEASES_TABLE} (
                version           FLOAT                    PRIMARY KEY,
                installed_at      TIMESTAMP WITH LOCAL TIME ZONE DEFAULT current_timestamp NOT NULL,
                installer_name    VARCHAR2(512 CHAR)       NOT NULL,
                installer_email   VARCHAR2(512 CHAR)       NOT NULL
            )
            """,
            # Projects table
            f"""
            CREATE TABLE {schema_prefix}{cls.PROJECTS_TABLE} (
                project         VARCHAR2(512 CHAR)       PRIMARY KEY,
                uri             VARCHAR2(512 CHAR)       NULL,
                created_at      TIMESTAMP WITH LOCAL TIME ZONE DEFAULT current_timestamp NOT NULL,
                creator_name    VARCHAR2(512 CHAR)       NOT NULL,
                creator_email   VARCHAR2(512 CHAR)       NOT NULL
            )
            """,
            # Changes table
            f"""
            CREATE TABLE {schema_prefix}{cls.CHANGES_TABLE} (
                change_id       CHAR(40)                 PRIMARY KEY,
                script_hash     CHAR(40)                     NULL,
                change          VARCHAR2(512 CHAR)       NOT NULL,
                project         VARCHAR2(512 CHAR)       NOT NULL REFERENCES {schema_prefix}{cls.PROJECTS_TABLE}(project),
                note            VARCHAR2(4000 CHAR)      DEFAULT '',
                committed_at    TIMESTAMP WITH LOCAL TIME ZONE DEFAULT current_timestamp NOT NULL,
                committer_name  VARCHAR2(512 CHAR)       NOT NULL,
                committer_email VARCHAR2(512 CHAR)       NOT NULL,
                planned_at      TIMESTAMP WITH LOCAL TIME ZONE NOT NULL,
                planner_name    VARCHAR2(512 CHAR)       NOT NULL,
                planner_email   VARCHAR2(512 CHAR)       NOT NULL
            )
            """,
            # Tags table
            f"""
            CREATE TABLE {schema_prefix}{cls.TAGS_TABLE} (
                tag_id          CHAR(40)                 PRIMARY KEY,
                tag             VARCHAR2(512 CHAR)       NOT NULL,
                project         VARCHAR2(512 CHAR)       NOT NULL REFERENCES {schema_prefix}{cls.PROJECTS_TABLE}(project),
                change_id       CHAR(40)                 NOT NULL REFERENCES {schema_prefix}{cls.CHANGES_TABLE}(change_id),
                note            VARCHAR2(4000 CHAR)      DEFAULT '',
                committed_at    TIMESTAMP WITH LOCAL TIME ZONE DEFAULT current_timestamp NOT NULL,
                committer_name  VARCHAR2(512 CHAR)       NOT NULL,
                committer_email VARCHAR2(512 CHAR)       NOT NULL,
                planned_at      TIMESTAMP WITH LOCAL TIME ZONE NOT NULL,
                planner_name    VARCHAR2(512 CHAR)       NOT NULL,
                planner_email   VARCHAR2(512 CHAR)       NOT NULL
            )
            """,
            # Dependencies table
            f"""
            CREATE TABLE {schema_prefix}{cls.DEPENDENCIES_TABLE} (
                change_id       CHAR(40)                 NOT NULL REFERENCES {schema_prefix}{cls.CHANGES_TABLE}(change_id),
                type            VARCHAR2(8)              NOT NULL,
                dependency      VARCHAR2(1024 CHAR)      NOT NULL,
                dependency_id   CHAR(40)                     NULL REFERENCES {schema_prefix}{cls.CHANGES_TABLE}(change_id),
                PRIMARY KEY (change_id, dependency)
            )
            """,
            # Events table
            f"""
            CREATE TABLE {schema_prefix}{cls.EVENTS_TABLE} (
                event           VARCHAR2(6)                   NOT NULL,
                change_id       CHAR(40)                      NOT NULL,
                change          VARCHAR2(512 CHAR)            NOT NULL,
                project         VARCHAR2(512 CHAR)            NOT NULL REFERENCES {schema_prefix}{cls.PROJECTS_TABLE}(project),
                note            VARCHAR2(4000 CHAR)           DEFAULT '',
                requires        VARCHAR2(4000 CHAR)           DEFAULT '' NOT NULL,
                conflicts       VARCHAR2(4000 CHAR)           DEFAULT '' NOT NULL,
                tags            VARCHAR2(4000 CHAR)           DEFAULT '' NOT NULL,
                committed_at    TIMESTAMP WITH LOCAL TIME ZONE      DEFAULT current_timestamp NOT NULL,
                committer_name  VARCHAR2(512 CHAR)            NOT NULL,
                committer_email VARCHAR2(512 CHAR)            NOT NULL,
                planned_at      TIMESTAMP WITH LOCAL TIME ZONE      NOT NULL,
                planner_name    VARCHAR2(512 CHAR)            NOT NULL,
                planner_email   VARCHAR2(512 CHAR)            NOT NULL
            )
            """,
        ]


class ExasolConnection:
    """Wrapper for pyexasol connection to match our Connection protocol."""

    def __init__(self, connection: "pyexasol.ExaConnection"):
        self._conn = connection
        self._cursor = None

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute SQL statement."""
        try:
            if params:
                # Convert named parameters to positional for pyexasol
                param_values = []
                sql_with_placeholders = sql
                for key, value in params.items():
                    sql_with_placeholders = sql_with_placeholders.replace(
                        f":{key}", "?"
                    )
                    param_values.append(value)
                result = self._conn.execute(sql_with_placeholders, param_values)
            else:
                result = self._conn.execute(sql)
            return result
        except Exception as e:
            logger.error(f"SQL execution failed: {sql}")
            raise

    def fetchone(self) -> Optional[Dict[str, Any]]:
        """Fetch one row from result set."""
        try:
            result = self._conn.fetchone()
            if result:
                # Convert tuple to dict using column names
                columns = [desc[0].lower() for desc in self._conn.description]
                return dict(zip(columns, result))
            return None
        except Exception:
            return None

    def fetchall(self) -> List[Dict[str, Any]]:
        """Fetch all rows from result set."""
        try:
            results = self._conn.fetchall()
            if results:
                columns = [desc[0].lower() for desc in self._conn.description]
                return [dict(zip(columns, row)) for row in results]
            return []
        except Exception:
            return []

    def commit(self) -> None:
        """Commit current transaction."""
        self._conn.commit()

    def rollback(self) -> None:
        """Rollback current transaction."""
        self._conn.rollback()

    def close(self) -> None:
        """Close the connection."""
        self._conn.close()


@register_engine("exasol")
class ExasolEngine(Engine):
    """
    Exasol database engine implementation.

    This engine provides support for Exasol databases using the pyexasol
    driver. It handles Exasol-specific SQL syntax, connection management,
    and registry operations.
    """

    def __init__(self, target: Target, plan: Plan) -> None:
        """
        Initialize Exasol engine.

        Args:
            target: Target configuration for Exasol database
            plan: Plan containing changes to manage

        Raises:
            EngineError: If pyexasol is not available
        """
        if pyexasol is None:
            raise EngineError(
                "pyexasol package is required for Exasol support. "
                "Install it with: pip install pyexasol",
                engine_name="exasol",
            )

        super().__init__(target, plan)
        self._registry_schema_obj = ExasolRegistrySchema()

        # Parse connection parameters from URI
        self._parse_connection_params()

    @property
    def engine_type(self) -> EngineType:
        """Get the engine type identifier."""
        return "exasol"

    @property
    def registry_schema(self) -> RegistrySchema:
        """Get the registry schema for this engine."""
        return self._registry_schema_obj

    def _parse_connection_params(self) -> None:
        """Parse connection parameters from target URI."""
        uri = self.target.uri

        # Extract connection parameters
        self._host = uri.hostname or "localhost"
        self._port = uri.port or 8563
        self._username = uri.username
        self._password = uri.password
        self._database = uri.path.lstrip("/") if uri.path else None

        # Parse query parameters for additional options
        self._connection_options = {}
        if uri.query:
            query_params = parse_qs(uri.query)
            for key, values in query_params.items():
                if values:
                    self._connection_options[key.lower()] = values[0]

        # Set registry schema
        self._registry_schema_name = self.target.registry or "sqitch"

    def _create_connection(self) -> ExasolConnection:
        """
        Create a new Exasol database connection.

        Returns:
            Wrapped Exasol connection object

        Raises:
            ConnectionError: If connection cannot be established
        """
        try:
            # Build connection parameters
            conn_params = {
                "dsn": f"{self._host}:{self._port}",
                "user": self._username,
                "password": self._password,
                "schema": self._database or self._registry_schema_name,
                "autocommit": False,
                "fetch_dict": False,  # We'll handle dict conversion ourselves
            }

            # Add any additional connection options
            conn_params.update(self._connection_options)

            # Create connection
            conn = pyexasol.connect(**conn_params)

            # Set session parameters for compatibility
            conn.execute("ALTER SESSION SET nls_date_format='YYYY-MM-DD HH24:MI:SS'")
            conn.execute(
                "ALTER SESSION SET nls_timestamp_format='YYYY-MM-DD HH24:MI:SS'"
            )
            conn.execute("ALTER SESSION SET TIME_ZONE='UTC'")

            # Open registry schema if specified
            if self._registry_schema_name:
                try:
                    conn.execute(f"OPEN SCHEMA {self._registry_schema_name}")
                except Exception:
                    # Schema might not exist yet, ignore error
                    pass

            return ExasolConnection(conn)

        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Exasol database: {e}",
                connection_string=sanitize_connection_string(str(self.target.uri)),
                engine_name=self.engine_type,
            ) from e

    def _execute_sql_file(
        self,
        connection: ExasolConnection,
        sql_file: Path,
        variables: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Execute SQL file with optional variable substitution.

        Args:
            connection: Exasol database connection
            sql_file: Path to SQL file to execute
            variables: Optional variables for substitution

        Raises:
            DeploymentError: If SQL execution fails
        """
        try:
            # Read SQL file
            sql_content = sql_file.read_text(encoding="utf-8")

            # Perform variable substitution if provided
            if variables:
                for key, value in variables.items():
                    # Exasol uses &variable syntax for substitution
                    sql_content = sql_content.replace(f"&{key}", str(value))

            # Replace registry placeholder
            sql_content = sql_content.replace("&registry", self._registry_schema_name)

            # Split into individual statements and execute
            statements = self._split_sql_statements(sql_content)

            for statement in statements:
                statement = statement.strip()
                if statement and not statement.startswith("--"):
                    self.logger.debug(f"Executing SQL: {statement[:100]}...")
                    connection.execute(statement)

        except Exception as e:
            raise DeploymentError(
                f"Failed to execute SQL file {sql_file}: {e}",
                engine_name=self.engine_type,
            ) from e

    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """
        Split SQL content into individual statements.

        Args:
            sql_content: SQL content to split

        Returns:
            List of SQL statements
        """
        # Remove comments and split on semicolons
        statements = []
        current_statement = []

        for line in sql_content.split("\n"):
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("--"):
                continue

            current_statement.append(line)

            # Check if line ends with semicolon (end of statement)
            if line.endswith(";"):
                statement = " ".join(current_statement)
                if statement.strip():
                    statements.append(statement)
                current_statement = []

        # Add any remaining statement
        if current_statement:
            statement = " ".join(current_statement)
            if statement.strip():
                statements.append(statement)

        return statements

    def _get_registry_version(self, connection: ExasolConnection) -> Optional[str]:
        """
        Get current registry version from database.

        Args:
            connection: Exasol database connection

        Returns:
            Registry version string or None if not found
        """
        try:
            schema_prefix = (
                f"{self._registry_schema_name}." if self._registry_schema_name else ""
            )
            connection.execute(
                f"SELECT MAX(version) as version FROM {schema_prefix}{self.registry_schema.RELEASES_TABLE}"
            )
            row = connection.fetchone()
            if row and row.get("version"):
                return str(row["version"])
            return None
        except Exception:
            return None

    def _registry_exists_in_db(self, connection: ExasolConnection) -> bool:
        """
        Check if registry tables exist in database.

        Args:
            connection: Exasol database connection

        Returns:
            True if registry exists, False otherwise
        """
        try:
            # Check if changes table exists
            connection.execute(
                """
                SELECT EXISTS(
                    SELECT TRUE FROM exa_all_tables
                     WHERE table_schema = ? AND table_name = ?
                )
                """,
                {
                    "table_schema": self._registry_schema_name.upper(),
                    "table_name": "CHANGES",
                },
            )
            row = connection.fetchone()
            return bool(row and row.get("exists"))
        except Exception:
            return False

    def _regex_condition(self, column: str, pattern: str) -> str:
        """
        Get Exasol-specific regex condition.

        Args:
            column: Column name
            pattern: Regular expression pattern

        Returns:
            SQL condition string using REGEXP_LIKE
        """
        # Exasol uses REGEXP_LIKE function
        # Ensure pattern has proper anchors
        if not pattern.startswith("^"):
            pattern = ".*" + pattern
        if not pattern.endswith("$"):
            pattern = pattern + ".*"

        return f"REGEXP_LIKE({column}, ?)"

    def _create_registry(self, connection: ExasolConnection) -> None:
        """
        Create registry tables in database.

        Args:
            connection: Exasol database connection

        Raises:
            EngineError: If registry creation fails
        """
        try:
            statements = self.registry_schema.get_create_statements(
                self.engine_type, self._registry_schema_name
            )

            for statement in statements:
                if statement.strip():  # Skip empty statements
                    self.logger.debug(f"Creating registry: {statement[:100]}...")
                    connection.execute(statement)

            # Insert registry version record
            schema_prefix = (
                f"{self._registry_schema_name}." if self._registry_schema_name else ""
            )
            connection.execute(
                f"""
                INSERT INTO {schema_prefix}{self.registry_schema.RELEASES_TABLE}
                (version, installer_name, installer_email)
                VALUES (?, ?, ?)
                """,
                {
                    "version": float(self.registry_schema.REGISTRY_VERSION),
                    "installer_name": self.plan.creator_name or "Unknown",
                    "installer_email": self.plan.creator_email or "unknown@example.com",
                },
            )

            # Insert initial project record
            self._insert_project_record(connection)

        except Exception as e:
            raise EngineError(
                f"Failed to create Exasol registry: {e}", engine_name=self.engine_type
            ) from e

    def _insert_project_record(self, connection: ExasolConnection) -> None:
        """
        Insert project record into registry.

        Args:
            connection: Exasol database connection
        """
        from datetime import datetime, timezone

        schema_prefix = (
            f"{self._registry_schema_name}." if self._registry_schema_name else ""
        )

        # Check if project already exists
        connection.execute(
            f"SELECT COUNT(*) as count FROM {schema_prefix}{self.registry_schema.PROJECTS_TABLE} WHERE project = ?",
            {"project": self.plan.project_name},
        )
        row = connection.fetchone()

        if not row or row.get("count", 0) == 0:
            connection.execute(
                f"""
                INSERT INTO {schema_prefix}{self.registry_schema.PROJECTS_TABLE}
                (project, uri, created_at, creator_name, creator_email)
                VALUES (?, ?, ?, ?, ?)
                """,
                {
                    "project": self.plan.project_name,
                    "uri": str(self.target.uri),
                    "created_at": datetime.now(timezone.utc),
                    "creator_name": self.plan.creator_name or "Unknown",
                    "creator_email": self.plan.creator_email or "unknown@example.com",
                },
            )

    def get_deployed_changes(self) -> List[str]:
        """
        Get list of deployed change IDs.

        Returns:
            List of deployed change IDs in deployment order

        Raises:
            EngineError: If query fails
        """
        self.ensure_registry()

        with self.connection() as conn:
            try:
                schema_prefix = (
                    f"{self._registry_schema_name}."
                    if self._registry_schema_name
                    else ""
                )
                conn.execute(
                    f"""
                    SELECT change_id FROM {schema_prefix}{self.registry_schema.CHANGES_TABLE}
                    WHERE project = ?
                    ORDER BY committed_at
                    """,
                    {"project": self.plan.project_name},
                )
                rows = conn.fetchall()
                return [row["change_id"] for row in rows]
            except Exception as e:
                raise EngineError(
                    f"Failed to get deployed changes: {e}", engine_name=self.engine_type
                ) from e

    def begin_work(self) -> None:
        """
        Begin a transaction and acquire locks.

        Exasol uses a DELETE FROM changes WHERE FALSE to acquire a lock
        on the changes table, ensuring only one sqitch instance runs at a time.
        """
        # This is handled by the transaction context manager
        pass

    def finish_work(self) -> None:
        """Finish work by committing transaction."""
        # This is handled by the transaction context manager
        pass

    def rollback_work(self) -> None:
        """Rollback work by rolling back transaction."""
        # This is handled by the transaction context manager
        pass
