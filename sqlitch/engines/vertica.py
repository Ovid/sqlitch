"""
Vertica database engine implementation.

This module provides the Vertica-specific implementation of the Engine
base class, handling Vertica connections, registry management, and
SQL execution with proper error handling and transaction management.
"""

import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs

from ..core.exceptions import ConnectionError, DeploymentError, EngineError
from ..core.plan import Plan
from ..core.target import Target
from ..core.types import EngineType, sanitize_connection_string
from .base import Engine, RegistrySchema, register_engine

# Try to import vertica-python
try:
    import vertica_python
    from vertica_python.errors import ConnectionError as VerticaConnectionError
    from vertica_python.errors import (
        DatabaseError,
    )
    from vertica_python.errors import Error as VerticaError
    from vertica_python.errors import (
        IntegrityError,
        ProgrammingError,
    )
    from vertica_python.vertica.cursor import Cursor as VerticaCursor
except ImportError:
    vertica_python = None
    VerticaError = Exception
    DatabaseError = Exception
    ProgrammingError = Exception
    IntegrityError = Exception
    VerticaConnectionError = Exception
    VerticaCursor = None


logger = logging.getLogger(__name__)


class VerticaRegistrySchema(RegistrySchema):
    """Vertica-specific registry schema."""

    @classmethod
    def get_create_statements(cls, engine_type: EngineType) -> List[str]:
        """
        Get Vertica-specific SQL statements to create registry tables.

        Args:
            engine_type: Database engine type (should be 'vertica')

        Returns:
            List of SQL CREATE statements for Vertica
        """
        return [
            # Create sqitch schema
            "CREATE SCHEMA IF NOT EXISTS sqitch",
            # Set comment on schema
            "COMMENT ON SCHEMA sqitch IS 'Sqitch database deployment metadata v1.1.'",
            # Releases table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.RELEASES_TABLE} (
                version         FLOAT          PRIMARY KEY ENABLED,
                installed_at    TIMESTAMPTZ    NOT NULL DEFAULT clock_timestamp(),
                installer_name  VARCHAR(1024)  NOT NULL,
                installer_email VARCHAR(1024)  NOT NULL
            )
            """,
            f"COMMENT ON TABLE sqitch.{cls.RELEASES_TABLE} IS 'Sqitch registry releases.'",
            # Projects table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.PROJECTS_TABLE} (
                project         VARCHAR(1024) PRIMARY KEY ENABLED ENCODING AUTO,
                uri             VARCHAR(1024) NULL UNIQUE ENABLED,
                created_at      TIMESTAMPTZ   NOT NULL DEFAULT clock_timestamp(),
                creator_name    VARCHAR(1024) NOT NULL,
                creator_email   VARCHAR(1024) NOT NULL
            )
            """,
            f"COMMENT ON TABLE sqitch.{cls.PROJECTS_TABLE} IS 'Sqitch projects deployed to this database.'",
            # Changes table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.CHANGES_TABLE} (
                change_id       CHAR(40)       PRIMARY KEY ENABLED ENCODING AUTO,
                script_hash     CHAR(40)           NULL UNIQUE ENABLED,
                change          VARCHAR(1024)  NOT NULL,
                project         VARCHAR(1024)  NOT NULL REFERENCES sqitch.{cls.PROJECTS_TABLE}(project),
                note            VARCHAR(65000) NOT NULL DEFAULT '',
                committed_at    TIMESTAMPTZ    NOT NULL DEFAULT clock_timestamp(),
                committer_name  VARCHAR(1024)  NOT NULL,
                committer_email VARCHAR(1024)  NOT NULL,
                planned_at      TIMESTAMPTZ    NOT NULL,
                planner_name    VARCHAR(1024)  NOT NULL,
                planner_email   VARCHAR(1024)  NOT NULL
            )
            """,
            f"COMMENT ON TABLE sqitch.{cls.CHANGES_TABLE} IS 'Tracks the changes currently deployed to the database.'",
            # Tags table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.TAGS_TABLE} (
                tag_id          CHAR(40)       PRIMARY KEY ENABLED ENCODING AUTO,
                tag             VARCHAR(1024)  NOT NULL,
                project         VARCHAR(1024)  NOT NULL REFERENCES sqitch.{cls.PROJECTS_TABLE}(project),
                change_id       CHAR(40)       NOT NULL REFERENCES sqitch.{cls.CHANGES_TABLE}(change_id),
                note            VARCHAR(65000) NOT NULL DEFAULT '',
                committed_at    TIMESTAMPTZ    NOT NULL DEFAULT clock_timestamp(),
                committer_name  VARCHAR(1024)  NOT NULL,
                committer_email VARCHAR(1024)  NOT NULL,
                planned_at      TIMESTAMPTZ    NOT NULL,
                planner_name    VARCHAR(1024)  NOT NULL,
                planner_email   VARCHAR(1024)  NOT NULL,
                UNIQUE(project, tag) ENABLED
            )
            """,
            f"COMMENT ON TABLE sqitch.{cls.TAGS_TABLE} IS 'Tracks the tags currently applied to the database.'",
            # Dependencies table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.DEPENDENCIES_TABLE} (
                change_id       CHAR(40)      NOT NULL REFERENCES sqitch.{cls.CHANGES_TABLE}(change_id),
                type            VARCHAR(8)    NOT NULL ENCODING AUTO,
                dependency      VARCHAR(2048) NOT NULL,
                dependency_id   CHAR(40)      NULL REFERENCES sqitch.{cls.CHANGES_TABLE}(change_id),
                PRIMARY KEY (change_id, dependency) ENABLED
            )
            """,
            f"COMMENT ON TABLE sqitch.{cls.DEPENDENCIES_TABLE} IS 'Tracks the currently satisfied dependencies.'",
            # Events table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.EVENTS_TABLE} (
                event           VARCHAR(6)     NOT NULL ENCODING AUTO,
                change_id       CHAR(40)       NOT NULL,
                change          VARCHAR(1024)  NOT NULL,
                project         VARCHAR(1024)  NOT NULL REFERENCES sqitch.{cls.PROJECTS_TABLE}(project),
                note            VARCHAR(65000) NOT NULL DEFAULT '',
                requires        LONG VARCHAR   NOT NULL DEFAULT '{{}}',
                conflicts       LONG VARCHAR   NOT NULL DEFAULT '{{}}',
                tags            LONG VARCHAR   NOT NULL DEFAULT '{{}}',
                committed_at    TIMESTAMPTZ    NOT NULL DEFAULT clock_timestamp(),
                committer_name  VARCHAR(1024)  NOT NULL,
                committer_email VARCHAR(1024)  NOT NULL,
                planned_at      TIMESTAMPTZ    NOT NULL,
                planner_name    VARCHAR(1024)  NOT NULL,
                planner_email   VARCHAR(1024)  NOT NULL,
                PRIMARY KEY (change_id, committed_at) ENABLED
            )
            """,
            f"COMMENT ON TABLE sqitch.{cls.EVENTS_TABLE} IS 'Contains full history of all deployment events.'",
            # Insert registry version
            f"""
            INSERT INTO sqitch.{cls.RELEASES_TABLE} (version, installer_name, installer_email)
            SELECT {cls.REGISTRY_VERSION}, 'sqlitch', 'sqlitch@example.com'
            WHERE NOT EXISTS (SELECT 1 FROM sqitch.{cls.RELEASES_TABLE} WHERE version = {cls.REGISTRY_VERSION})
            """,
        ]


class VerticaConnection:
    """Wrapper for Vertica connection with sqitch-specific functionality."""

    def __init__(self, connection: "vertica_python.Connection"):
        """
        Initialize Vertica connection wrapper.

        Args:
            connection: vertica-python connection object
        """
        self._connection = connection
        self._cursor: Optional[VerticaCursor] = None

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
                # Convert named parameters to positional for Vertica
                formatted_query, param_values = self._format_query_params(
                    sql_query, params
                )
                cursor.execute(formatted_query, param_values)
            else:
                cursor.execute(sql_query)
        except VerticaError as e:
            raise DeploymentError(
                f"SQL execution failed: {e}",
                engine_name="vertica",
                sql_state=getattr(e, "sqlstate", None),
            ) from e

    def fetchone(self) -> Optional[Dict[str, Any]]:
        """
        Fetch one row from result set.

        Returns:
            Dictionary representing one row, or None if no more rows
        """
        cursor = self._get_cursor()
        row = cursor.fetchone()
        if row is None:
            return None

        # Convert to dictionary using column names
        columns = [desc[0].lower() for desc in cursor.description]
        return dict(zip(columns, row))

    def fetchall(self) -> List[Dict[str, Any]]:
        """
        Fetch all rows from result set.

        Returns:
            List of dictionaries representing rows
        """
        cursor = self._get_cursor()
        rows = cursor.fetchall()
        if not rows:
            return []

        # Convert to list of dictionaries using column names
        columns = [desc[0].lower() for desc in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

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

    def _get_cursor(self) -> VerticaCursor:
        """
        Get cursor for executing queries.

        Returns:
            Vertica cursor object
        """
        if self._cursor is None:
            self._cursor = self._connection.cursor()
        return self._cursor

    def _format_query_params(self, query: str, params: Dict[str, Any]) -> tuple:
        """
        Format query with named parameters for Vertica.

        Args:
            query: SQL query with named parameters
            params: Dictionary of parameter values

        Returns:
            Tuple of (formatted_query, parameter_values)
        """
        # Convert named parameters (:name) to positional (?)
        param_values = []
        formatted_query = query

        # Find all named parameters in the query
        param_pattern = r":(\w+)"
        matches = re.findall(param_pattern, query)

        for param_name in matches:
            if param_name in params:
                param_values.append(params[param_name])
                # Replace first occurrence of :param_name with ?
                formatted_query = formatted_query.replace(f":{param_name}", "?", 1)

        return formatted_query, param_values


@register_engine("vertica")
class VerticaEngine(Engine):
    """
    Vertica database engine implementation.

    This engine provides Vertica-specific functionality for sqitch operations
    including connection management, registry operations, and SQL execution.
    """

    def __init__(self, target: Target, plan: Plan) -> None:
        """
        Initialize Vertica engine.

        Args:
            target: Target configuration for this engine
            plan: Plan containing changes to manage

        Raises:
            EngineError: If vertica-python is not available
        """
        if vertica_python is None:
            raise EngineError(
                "vertica-python is required for Vertica engine. "
                "Install it with: pip install vertica-python"
            )

        super().__init__(target, plan)
        self._registry_schema_name = self._get_registry_schema()

    @property
    def engine_type(self) -> EngineType:
        """Get the engine type identifier."""
        return "vertica"

    @property
    def registry_schema(self) -> RegistrySchema:
        """Get the registry schema for this engine."""
        return VerticaRegistrySchema()

    def _create_connection(self) -> VerticaConnection:  # noqa: C901
        """
        Create a new Vertica database connection.

        Returns:
            Vertica connection wrapper

        Raises:
            ConnectionError: If connection cannot be established
        """
        try:
            # Parse connection parameters from target URI
            uri = self.target.uri

            # Extract connection parameters
            host = self._get_host()
            port = self._get_port()
            user = self._get_user()
            password = self._get_password()
            database = self._get_database()

            # Build connection parameters
            conn_params = {
                "host": host,
                "port": port,
                "user": user,
                "database": database,
                "autocommit": False,  # We manage transactions manually
                "unicode_error": "strict",
                "read_timeout": 600,
                "connection_timeout": 10,
            }

            # Add password if available
            if password:
                conn_params["password"] = password

            # Handle additional connection parameters from query string
            if uri.query:
                query_params = parse_qs(uri.query)
                for key, values in query_params.items():
                    if values and key not in conn_params:
                        # Convert some common parameter names
                        if key == "connection_load_balance":
                            conn_params["connection_load_balance"] = (
                                values[0].lower() == "true"
                            )
                        elif key == "backup_server_node":
                            conn_params["backup_server_node"] = values[0].split(",")
                        else:
                            conn_params[key] = values[0]

            self.logger.debug(f"Connecting to Vertica at {host}:{port}")

            # Create connection
            raw_connection = vertica_python.connect(**conn_params)

            # Set session parameters for sqitch compatibility
            cursor = raw_connection.cursor()
            try:
                # Set search path to include sqitch schema
                cursor.execute(
                    f"SET search_path = {self._registry_schema_name}, public"
                )

                # Set timezone to UTC for consistency
                cursor.execute("SET timezone TO 'UTC'")

                # Set date style for consistency
                cursor.execute("SET datestyle TO 'ISO, YMD'")

            except VerticaError:
                # If schema doesn't exist, that's okay - it will be created later
                pass
            finally:
                cursor.close()

            return VerticaConnection(raw_connection)

        except VerticaError as e:
            raise ConnectionError(
                f"Failed to connect to Vertica: {e}",
                connection_string=sanitize_connection_string(str(self.target.uri)),
                engine_name="vertica",
            ) from e

    def _execute_sql_file(
        self,
        connection: VerticaConnection,
        sql_file: Path,
        variables: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Execute SQL file with optional variable substitution.

        Args:
            connection: Vertica connection
            sql_file: Path to SQL file to execute
            variables: Optional variables for substitution

        Raises:
            DeploymentError: If SQL execution fails
        """
        try:
            # Read SQL file content
            sql_content = sql_file.read_text(encoding="utf-8")

            # Perform variable substitution if variables provided
            if variables:
                for var_name, var_value in variables.items():
                    # Replace &variable_name with actual value
                    sql_content = sql_content.replace(f"&{var_name}", str(var_value))

            # Replace &registry with actual registry schema name
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
                engine_name="vertica",
                sql_file=str(sql_file),
            ) from e

    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """
        Split SQL content into individual statements.

        Args:
            sql_content: SQL content to split

        Returns:
            List of individual SQL statements
        """
        # Simple statement splitting on semicolons
        # This could be enhanced to handle more complex cases
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

    def _get_registry_version(self, connection: VerticaConnection) -> Optional[str]:
        """
        Get current registry version from database.

        Args:
            connection: Vertica connection

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

    def _regex_condition(self, column: str, pattern: str) -> str:
        """
        Get Vertica-specific regex condition.

        Args:
            column: Column name
            pattern: Regular expression pattern

        Returns:
            SQL condition string
        """
        # Vertica uses ~ operator for regex matching
        return f"{column} ~ ?"

    def begin_work(self) -> None:
        """
        Begin transaction with exclusive lock on changes table.

        This ensures that only one instance of Sqitch runs at one time,
        matching the behavior of the Perl implementation.
        """
        with self.connection() as conn:
            # Start transaction
            conn.execute("BEGIN")

            # Lock changes table in exclusive mode
            conn.execute(
                f"LOCK TABLE {self._registry_schema_name}.{self.registry_schema.CHANGES_TABLE} IN EXCLUSIVE MODE"
            )

    def _get_host(self) -> str:
        """
        Get Vertica host.

        Returns:
            Host name or IP address
        """
        uri = self.target.uri

        # Try URI hostname
        if uri.hostname:
            return uri.hostname

        # Try environment variables
        host = os.getenv("VSQL_HOST")
        if host:
            return host

        # Default to localhost
        return "localhost"

    def _get_port(self) -> int:
        """
        Get Vertica port.

        Returns:
            Port number
        """
        uri = self.target.uri

        # Try URI port
        if uri.port:
            return uri.port

        # Try environment variables
        port = os.getenv("VSQL_PORT")
        if port:
            return int(port)

        # Default Vertica port
        return 5433

    def _get_user(self) -> str:
        """
        Get Vertica username.

        Returns:
            Username
        """
        uri = self.target.uri

        # Try URI username
        if uri.username:
            return uri.username

        # Try environment variables
        user = os.getenv("VSQL_USER")
        if user:
            return user

        # Try from URI query parameters
        if uri.query:
            query_params = parse_qs(uri.query)
            if "user" in query_params:
                return query_params["user"][0]

        # Fall back to system user
        return os.getenv("USER", "dbadmin")

    def _get_password(self) -> Optional[str]:
        """
        Get Vertica password.

        Returns:
            Password or None if not available
        """
        uri = self.target.uri

        # Try URI password
        if uri.password:
            return uri.password

        # Try environment variables
        password = os.getenv("VSQL_PASSWORD")
        if password:
            return password

        # Try from URI query parameters
        if uri.query:
            query_params = parse_qs(uri.query)
            for key in ["password", "pwd"]:
                if key in query_params:
                    return query_params[key][0]

        return None

    def _get_database(self) -> str:
        """
        Get Vertica database name.

        Returns:
            Database name
        """
        uri = self.target.uri

        # Try URI path
        if uri.path and len(uri.path) > 1:
            # Remove leading slash
            database = uri.path[1:]
            if database:
                return database

        # Try environment variables
        database = os.getenv("VSQL_DATABASE")
        if database:
            return database

        # Try from URI query parameters
        if uri.query:
            query_params = parse_qs(uri.query)
            if "database" in query_params:
                return query_params["database"][0]

        # Fall back to username
        return self._get_user()

    def _get_registry_schema(self) -> str:
        """
        Get registry schema name.

        Returns:
            Registry schema name
        """
        # Use target registry if specified, otherwise default to 'sqitch'
        return getattr(self.target, "registry", "sqitch")

    def _ts2char_format(self, column: str) -> str:
        """
        Get Vertica-specific timestamp to character format.

        Args:
            column: Column name containing timestamp

        Returns:
            SQL expression to format timestamp
        """
        return f'to_char({column} AT TIME ZONE \'UTC\', \'"year":YYYY:"month":MM:"day":DD:"hour":HH24:"minute":MI:"second":SS:"time_zone":"UTC"\')'

    def _char2ts(self, timestamp_str: str) -> str:
        """
        Convert timestamp string to Vertica format.

        Args:
            timestamp_str: Timestamp string

        Returns:
            Formatted timestamp string for Vertica
        """
        # This would typically convert from ISO format to Vertica's expected format
        return timestamp_str

    def _multi_values(self, count: int, expr: str) -> str:
        """
        Generate multi-value expression for Vertica.

        Args:
            count: Number of value expressions
            expr: Expression template

        Returns:
            Multi-value SQL expression
        """
        return "\nUNION ALL ".join([f"SELECT {expr}"] * count)

    def _dependency_placeholders(self) -> str:
        """
        Get dependency placeholders for Vertica.

        Returns:
            Placeholder string for dependency inserts
        """
        return "CAST(? AS CHAR(40)), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS CHAR(40))"

    def _tag_placeholders(self) -> str:
        """
        Get tag placeholders for Vertica.

        Returns:
            Placeholder string for tag inserts
        """
        return ", ".join(
            [
                "CAST(? AS CHAR(40))",
                "CAST(? AS VARCHAR)",
                "CAST(? AS VARCHAR)",
                "CAST(? AS CHAR(40))",
                "CAST(? AS VARCHAR)",
                "CAST(? AS VARCHAR)",
                "CAST(? AS VARCHAR)",
                "CAST(? AS TIMESTAMPTZ)",
                "CAST(? AS VARCHAR)",
                "CAST(? AS VARCHAR)",
                "clock_timestamp()",
            ]
        )

    def _tag_subselect_columns(self) -> str:
        """
        Get tag subselect columns for Vertica.

        Returns:
            Column list for tag subselects
        """
        return ", ".join(
            [
                "CAST(? AS CHAR(40)) AS tid",
                "CAST(? AS VARCHAR) AS tname",
                "CAST(? AS VARCHAR) AS proj",
                "CAST(? AS CHAR(40)) AS cid",
                "CAST(? AS VARCHAR) AS note",
                "CAST(? AS VARCHAR) AS cuser",
                "CAST(? AS VARCHAR) AS cemail",
                "CAST(? AS TIMESTAMPTZ) AS tts",
                "CAST(? AS VARCHAR) AS puser",
                "CAST(? AS VARCHAR) AS pemail",
                "clock_timestamp()",
            ]
        )
