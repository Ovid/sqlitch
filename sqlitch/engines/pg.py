"""
PostgreSQL database engine implementation.

This module provides the PostgreSQL-specific implementation of the Engine
base class, handling PostgreSQL connections, registry management, and
SQL execution with proper error handling and transaction management.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from ..core.change import Change
from ..core.exceptions import ConnectionError, DeploymentError, EngineError
from ..core.plan import Plan
from ..core.target import Target
from ..core.types import EngineType, sanitize_connection_string
from .base import Engine, RegistrySchema, register_engine

# Try to import psycopg2, fall back to psycopg2-binary
try:
    import psycopg2
    import psycopg2.extensions
    import psycopg2.extras
    from psycopg2 import sql
except ImportError:
    try:
        import psycopg2
        import psycopg2.extensions
        import psycopg2.extras
        from psycopg2 import sql
    except ImportError:
        psycopg2 = None
        sql = None


logger = logging.getLogger(__name__)


class PostgreSQLRegistrySchema(RegistrySchema):
    """PostgreSQL-specific registry schema."""

    @classmethod
    def get_create_statements(cls, engine_type: EngineType) -> List[str]:
        """
        Get PostgreSQL-specific SQL statements to create registry tables.

        Args:
            engine_type: Database engine type (should be 'pg')

        Returns:
            List of SQL CREATE statements for PostgreSQL
        """
        return [
            # Create sqitch schema
            "CREATE SCHEMA IF NOT EXISTS sqitch",
            # Projects table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.PROJECTS_TABLE} (
                project         TEXT PRIMARY KEY,
                uri             TEXT,
                created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                creator_name    TEXT NOT NULL,
                creator_email   TEXT NOT NULL
            )
            """,
            # Releases table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.RELEASES_TABLE} (
                version         REAL PRIMARY KEY,
                installed_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                installer_name  TEXT NOT NULL,
                installer_email TEXT NOT NULL
            )
            """,
            # Changes table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.CHANGES_TABLE} (
                change_id       TEXT PRIMARY KEY,
                script_hash     TEXT,
                change          TEXT NOT NULL,
                project         TEXT NOT NULL,
                note            TEXT NOT NULL DEFAULT '',
                committed_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                committer_name  TEXT NOT NULL,
                committer_email TEXT NOT NULL,
                planned_at      TIMESTAMP WITH TIME ZONE NOT NULL,
                planner_name    TEXT NOT NULL,
                planner_email   TEXT NOT NULL
            )
            """,
            # Tags table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.TAGS_TABLE} (
                tag_id          TEXT PRIMARY KEY,
                tag             TEXT NOT NULL UNIQUE,
                project         TEXT NOT NULL,
                change_id       TEXT NOT NULL REFERENCES sqitch.{cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE,
                note            TEXT NOT NULL DEFAULT '',
                committed_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                committer_name  TEXT NOT NULL,
                committer_email TEXT NOT NULL,
                planned_at      TIMESTAMP WITH TIME ZONE NOT NULL,
                planner_name    TEXT NOT NULL,
                planner_email   TEXT NOT NULL
            )
            """,
            # Dependencies table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.DEPENDENCIES_TABLE} (
                change_id       TEXT NOT NULL REFERENCES sqitch.{cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE ON DELETE CASCADE,
                type            TEXT NOT NULL,
                dependency      TEXT NOT NULL,
                dependency_id   TEXT,
                PRIMARY KEY (change_id, dependency)
            )
            """,
            # Events table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.EVENTS_TABLE} (
                event           TEXT NOT NULL CHECK (event IN ('deploy', 'revert', 'fail', 'merge')),
                change_id       TEXT NOT NULL,
                change          TEXT NOT NULL,
                project         TEXT NOT NULL,
                note            TEXT NOT NULL DEFAULT '',
                requires        TEXT NOT NULL DEFAULT '',
                conflicts       TEXT NOT NULL DEFAULT '',
                tags            TEXT NOT NULL DEFAULT '',
                committed_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                committer_name  TEXT NOT NULL,
                committer_email TEXT NOT NULL,
                planned_at      TIMESTAMP WITH TIME ZONE NOT NULL,
                planner_name    TEXT NOT NULL,
                planner_email   TEXT NOT NULL,
                PRIMARY KEY (change_id, committed_at)
            )
            """,
            # Insert registry version
            f"""
            INSERT INTO sqitch.{cls.RELEASES_TABLE} (version, installer_name, installer_email)
            VALUES ({cls.REGISTRY_VERSION}, 'sqlitch', 'sqlitch@example.com')
            ON CONFLICT (version) DO NOTHING
            """,
        ]


class PostgreSQLConnection:
    """Wrapper for PostgreSQL connection with sqitch-specific functionality."""

    def __init__(self, connection: "psycopg2.extensions.connection"):
        """
        Initialize PostgreSQL connection wrapper.

        Args:
            connection: psycopg2 connection object
        """
        self._connection = connection
        self._cursor: Optional["psycopg2.extensions.cursor"] = None

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
                cursor.execute(sql_query, params)
            else:
                cursor.execute(sql_query)
        except psycopg2.Error as e:
            raise DeploymentError(
                f"SQL execution failed: {e}",
                engine_name="pg",
                sql_state=getattr(e, "pgcode", None),
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
            List of dictionaries representing rows
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

    def _get_cursor(self) -> "psycopg2.extensions.cursor":
        """Get or create cursor with dict row factory."""
        if not self._cursor or self._cursor.closed:
            self._cursor = self._connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            )
        return self._cursor


@register_engine("pg")
class PostgreSQLEngine(Engine):
    """
    PostgreSQL database engine implementation.

    Provides PostgreSQL-specific functionality for sqitch operations
    including connection management, registry operations, and SQL execution.
    """

    def __init__(self, target: Target, plan: Plan) -> None:
        """
        Initialize PostgreSQL engine.

        Args:
            target: Target configuration for PostgreSQL database
            plan: Plan containing changes to manage

        Raises:
            EngineError: If psycopg2 is not available
        """
        if psycopg2 is None:
            raise EngineError(
                "psycopg2 is required for PostgreSQL support. "
                "Install with: pip install psycopg2-binary",
                engine_name="pg",
            )

        super().__init__(target, plan)
        self._connection_params = self._parse_connection_string()
        self._registry_schema_name = target.registry or "sqitch"

    @property
    def engine_type(self) -> EngineType:
        """Get the engine type identifier."""
        return "pg"

    @property
    def registry_schema(self) -> RegistrySchema:
        """Get the registry schema for PostgreSQL."""
        return PostgreSQLRegistrySchema()

    def _parse_connection_string(self) -> Dict[str, Any]:
        """
        Parse PostgreSQL connection string from target URI.

        Returns:
            Dictionary of connection parameters

        Raises:
            ConnectionError: If URI format is invalid
        """
        uri_str = str(self.target.uri)

        # Handle sqitch-style URIs: db:pg://user:pass@host:port/dbname
        if uri_str.startswith("db:pg:"):
            uri_str = uri_str[3:]  # Remove 'db:' prefix
        elif uri_str.startswith("pg:"):
            uri_str = uri_str[3:]  # Remove 'pg:' prefix

        # Add scheme if missing
        if not uri_str.startswith("postgresql://") and not uri_str.startswith(
            "postgres://"
        ):
            if "://" not in uri_str:
                uri_str = "postgresql://" + uri_str

        try:
            parsed = urlparse(uri_str)

            params = {
                "host": parsed.hostname or "localhost",
                "port": parsed.port or 5432,
                "database": parsed.path.lstrip("/") if parsed.path else "postgres",
                "user": parsed.username,
                "password": parsed.password,
            }

            # Handle query parameters
            if parsed.query:
                query_params = parse_qs(parsed.query)
                for key, values in query_params.items():
                    if values:
                        params[key] = values[0]

            # Remove None values
            params = {k: v for k, v in params.items() if v is not None}

            return params

        except Exception as e:
            raise ConnectionError(
                f"Invalid PostgreSQL connection string: {e}",
                connection_string=sanitize_connection_string(uri_str),
                engine_name="pg",
            ) from e

    def _create_connection(self) -> PostgreSQLConnection:
        """
        Create a new PostgreSQL connection.

        Returns:
            PostgreSQL connection wrapper

        Raises:
            ConnectionError: If connection cannot be established
        """
        try:
            self.logger.debug(
                f"Connecting to PostgreSQL: {sanitize_connection_string(str(self.target.uri))}"
            )

            # Create connection with autocommit disabled for transaction control
            conn = psycopg2.connect(**self._connection_params)
            conn.autocommit = False

            # Set search path to include sqitch schema
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SET search_path TO {self._registry_schema_name}, public"
                )

            return PostgreSQLConnection(conn)

        except psycopg2.Error as e:
            raise ConnectionError(
                f"Failed to connect to PostgreSQL database: {e}",
                connection_string=sanitize_connection_string(str(self.target.uri)),
                engine_name="pg",
            ) from e

    def _execute_sql_file(
        self,
        connection: PostgreSQLConnection,
        sql_file: Path,
        variables: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Execute SQL file with optional variable substitution.

        Args:
            connection: PostgreSQL connection
            sql_file: Path to SQL file to execute
            variables: Optional variables for substitution

        Raises:
            DeploymentError: If SQL execution fails
        """
        try:
            if not sql_file.exists():
                raise DeploymentError(
                    f"SQL file not found: {sql_file}",
                    sql_file=str(sql_file),
                    engine_name="pg",
                )

            self.logger.debug(f"Executing SQL file: {sql_file}")

            # Read SQL content
            sql_content = sql_file.read_text(encoding="utf-8")

            # Perform variable substitution if provided
            if variables:
                for var_name, var_value in variables.items():
                    placeholder = f":{var_name}"
                    sql_content = sql_content.replace(placeholder, str(var_value))

            # Split into individual statements and execute
            statements = self._split_sql_statements(sql_content)

            for statement in statements:
                statement = statement.strip()
                if statement and not statement.startswith("--"):
                    connection.execute(statement)

        except Exception as e:
            if isinstance(e, DeploymentError):
                raise
            raise DeploymentError(
                f"Failed to execute SQL file {sql_file}: {e}",
                sql_file=str(sql_file),
                engine_name="pg",
            ) from e

    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """
        Split SQL content into individual statements.

        Args:
            sql_content: SQL content to split

        Returns:
            List of SQL statements
        """
        # Simple statement splitting - in production, this would be more sophisticated
        # to handle strings, comments, and complex cases properly
        statements = []
        current_statement = []

        for line in sql_content.split("\n"):
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("--"):
                continue

            current_statement.append(line)

            # Check if line ends with semicolon (end of statement)
            if line.rstrip().endswith(";"):
                statements.append("\n".join(current_statement))
                current_statement = []

        # Add any remaining statement
        if current_statement:
            statements.append("\n".join(current_statement))

        return statements

    def _get_registry_version(self, connection: PostgreSQLConnection) -> Optional[str]:
        """
        Get current registry version from database.

        Args:
            connection: PostgreSQL connection

        Returns:
            Registry version string or None if not found
        """
        try:
            connection.execute(
                f"SELECT version FROM {self._registry_schema_name}.{self.registry_schema.RELEASES_TABLE} ORDER BY version DESC LIMIT 1"
            )
            row = connection.fetchone()
            return str(row["version"]) if row else None
        except Exception:
            return None

    def _registry_exists_in_db(self, connection: PostgreSQLConnection) -> bool:
        """
        Check if registry tables exist in database.

        Args:
            connection: PostgreSQL connection

        Returns:
            True if registry exists, False otherwise
        """
        try:
            # Check if sqitch schema exists
            connection.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                {"schema_name": self._registry_schema_name},
            )
            if not connection.fetchone():
                return False

            # Check if projects table exists
            connection.execute(
                f"SELECT 1 FROM {self._registry_schema_name}.{self.registry_schema.PROJECTS_TABLE} LIMIT 1"
            )
            return True
        except Exception:
            return False

    def _create_registry(self, connection: PostgreSQLConnection) -> None:
        """
        Create registry tables in PostgreSQL database.

        Args:
            connection: PostgreSQL connection

        Raises:
            EngineError: If registry creation fails
        """
        try:
            statements = self.registry_schema.get_create_statements(self.engine_type)

            # Replace generic schema references with configured schema name
            for statement in statements:
                # Replace 'sqitch.' with configured schema name
                if self._registry_schema_name != "sqitch":
                    statement = statement.replace(
                        "sqitch.", f"{self._registry_schema_name}."
                    )
                    statement = statement.replace("sqitch", self._registry_schema_name)

                connection.execute(statement)

            # Insert initial project record
            self._insert_project_record(connection)

        except Exception as e:
            raise EngineError(
                f"Failed to create PostgreSQL registry: {e}", engine_name="pg"
            ) from e

    def _insert_project_record(self, connection: PostgreSQLConnection) -> None:
        """
        Insert project record into registry.

        Args:
            connection: PostgreSQL connection
        """
        project_name = self.plan.project_name
        project_uri = str(self.target.uri)
        creator_name = self.plan.creator_name or "Unknown"
        creator_email = self.plan.creator_email or "unknown@example.com"

        # Check if project already exists
        connection.execute(
            f"SELECT 1 FROM {self._registry_schema_name}.{self.registry_schema.PROJECTS_TABLE} WHERE project = %s",
            {"project": project_name},
        )

        if not connection.fetchone():
            connection.execute(
                f"""
                INSERT INTO {self._registry_schema_name}.{self.registry_schema.PROJECTS_TABLE}
                (project, uri, creator_name, creator_email)
                VALUES (%s, %s, %s, %s)
                """,
                {
                    "project": project_name,
                    "uri": project_uri,
                    "creator_name": creator_name,
                    "creator_email": creator_email,
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
                conn.execute(
                    f"""
                    SELECT change_id FROM {self._registry_schema_name}.{self.registry_schema.CHANGES_TABLE}
                    WHERE project = %s
                    ORDER BY committed_at
                    """,
                    {"project": self.plan.project_name},
                )
                rows = conn.fetchall()
                return [row["change_id"] for row in rows]
            except Exception as e:
                raise EngineError(
                    f"Failed to get deployed changes: {e}", engine_name="pg"
                ) from e

    def _record_change_deployment(
        self, connection: PostgreSQLConnection, change: Change
    ) -> None:
        """
        Record change deployment in PostgreSQL registry.

        Args:
            connection: PostgreSQL connection
            change: Deployed change
        """
        # Insert change record
        connection.execute(
            f"""
            INSERT INTO {self._registry_schema_name}.{self.registry_schema.CHANGES_TABLE}
            (change_id, script_hash, change, project, note, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            {
                "change_id": change.id,
                "script_hash": self._calculate_script_hash(change),
                "change": change.name,
                "project": self.plan.project_name,
                "note": change.note or "",
                "committer_name": change.planner_name,
                "committer_email": change.planner_email,
                "planned_at": change.timestamp,
                "planner_name": change.planner_name,
                "planner_email": change.planner_email,
            },
        )

        # Insert dependencies
        for dep in change.dependencies:
            connection.execute(
                f"""
                INSERT INTO {self._registry_schema_name}.{self.registry_schema.DEPENDENCIES_TABLE}
                (change_id, type, dependency, dependency_id)
                VALUES (%s, %s, %s, %s)
                """,
                {
                    "change_id": change.id,
                    "type": dep.type,
                    "dependency": dep.change,
                    "dependency_id": self._resolve_dependency_id(dep.change),
                },
            )

        # Insert event record
        connection.execute(
            f"""
            INSERT INTO {self._registry_schema_name}.{self.registry_schema.EVENTS_TABLE}
            (event, change_id, change, project, note, requires, conflicts, tags, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            {
                "event": "deploy",
                "change_id": change.id,
                "change": change.name,
                "project": self.plan.project_name,
                "note": change.note or "",
                "requires": self._format_dependencies(
                    [dep.change for dep in change.dependencies if dep.type == "require"]
                ),
                "conflicts": self._format_dependencies(
                    [
                        dep.change
                        for dep in change.dependencies
                        if dep.type == "conflict"
                    ]
                ),
                "tags": self._format_tags(change.tags),
                "committer_name": change.planner_name,
                "committer_email": change.planner_email,
                "planned_at": change.timestamp,
                "planner_name": change.planner_name,
                "planner_email": change.planner_email,
            },
        )

    def _record_change_revert(
        self, connection: PostgreSQLConnection, change: Change
    ) -> None:
        """
        Record change revert in PostgreSQL registry.

        Args:
            connection: PostgreSQL connection
            change: Reverted change
        """
        # Remove change record
        connection.execute(
            f"DELETE FROM {self._registry_schema_name}.{self.registry_schema.CHANGES_TABLE} WHERE change_id = %s",
            {"change_id": change.id},
        )

        # Dependencies are automatically removed due to CASCADE

        # Insert revert event
        connection.execute(
            f"""
            INSERT INTO {self._registry_schema_name}.{self.registry_schema.EVENTS_TABLE}
            (event, change_id, change, project, note, requires, conflicts, tags, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            {
                "event": "revert",
                "change_id": change.id,
                "change": change.name,
                "project": self.plan.project_name,
                "note": change.note or "",
                "requires": self._format_dependencies(
                    [dep.change for dep in change.dependencies if dep.type == "require"]
                ),
                "conflicts": self._format_dependencies(
                    [
                        dep.change
                        for dep in change.dependencies
                        if dep.type == "conflict"
                    ]
                ),
                "tags": self._format_tags(change.tags),
                "committer_name": change.planner_name,
                "committer_email": change.planner_email,
                "planned_at": change.timestamp,
                "planner_name": change.planner_name,
                "planner_email": change.planner_email,
            },
        )

    def _calculate_script_hash(self, change: Change) -> str:
        """
        Calculate hash of change scripts for integrity checking.

        Args:
            change: Change to calculate hash for

        Returns:
            SHA1 hash of combined scripts
        """
        import hashlib

        hasher = hashlib.sha1()

        # Hash deploy script
        deploy_file = self.plan.get_deploy_file(change)
        if deploy_file.exists():
            hasher.update(deploy_file.read_bytes())

        # Hash revert script
        revert_file = self.plan.get_revert_file(change)
        if revert_file.exists():
            hasher.update(revert_file.read_bytes())

        # Hash verify script
        verify_file = self.plan.get_verify_file(change)
        if verify_file.exists():
            hasher.update(verify_file.read_bytes())

        return hasher.hexdigest()

    def _resolve_dependency_id(self, dependency_name: str) -> Optional[str]:
        """
        Resolve dependency name to change ID.

        Args:
            dependency_name: Name of dependency change

        Returns:
            Change ID of dependency or None if not found
        """
        for change in self.plan.changes:
            if change.name == dependency_name:
                return change.id
        return None

    def _format_dependencies(self, dependencies: List[str]) -> str:
        """
        Format dependencies list for storage.

        Args:
            dependencies: List of dependency names

        Returns:
            Formatted dependency string
        """
        return " ".join(dependencies) if dependencies else ""

    def _format_tags(self, tags: List[str]) -> str:
        """
        Format tags list for storage.

        Args:
            tags: List of tag names

        Returns:
            Formatted tag string
        """
        return " ".join(tags) if tags else ""

    def _regex_condition(self, column: str, pattern: str) -> str:
        """
        Get PostgreSQL-specific regex condition.

        Args:
            column: Column name
            pattern: Regular expression pattern

        Returns:
            SQL condition string
        """
        return f"{column} ~ ?"
