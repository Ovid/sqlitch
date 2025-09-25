"""
Snowflake database engine implementation.

This module provides the Snowflake-specific implementation of the Engine
base class, handling Snowflake connections, registry management, and
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

# Try to import snowflake-connector-python
try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    from snowflake.connector.errors import (
        DatabaseError,
    )
    from snowflake.connector.errors import Error as SnowflakeError
    from snowflake.connector.errors import (
        IntegrityError,
        ProgrammingError,
    )
except ImportError:
    snowflake = None
    SnowflakeError = Exception
    DatabaseError = Exception
    ProgrammingError = Exception
    IntegrityError = Exception
    DictCursor = None


logger = logging.getLogger(__name__)


class SnowflakeRegistrySchema(RegistrySchema):
    """Snowflake-specific registry schema."""

    @classmethod
    def get_create_statements(cls, engine_type: EngineType) -> List[str]:
        """
        Get Snowflake-specific SQL statements to create registry tables.

        Args:
            engine_type: Database engine type (should be 'snowflake')

        Returns:
            List of SQL CREATE statements for Snowflake
        """
        return [
            # Create sqitch schema
            "CREATE SCHEMA IF NOT EXISTS sqitch",
            # Projects table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.PROJECTS_TABLE} (
                project         TEXT PRIMARY KEY,
                uri             TEXT UNIQUE,
                created_at      TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                creator_name    TEXT NOT NULL,
                creator_email   TEXT NOT NULL
            )
            """,
            # Releases table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.RELEASES_TABLE} (
                version         FLOAT PRIMARY KEY,
                installed_at    TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
                project         TEXT NOT NULL REFERENCES sqitch.{cls.PROJECTS_TABLE}(project) ON UPDATE CASCADE,
                note            TEXT NOT NULL DEFAULT '',
                committed_at    TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                committer_name  TEXT NOT NULL,
                committer_email TEXT NOT NULL,
                planned_at      TIMESTAMP_TZ NOT NULL,
                planner_name    TEXT NOT NULL,
                planner_email   TEXT NOT NULL,
                UNIQUE(project, script_hash)
            )
            """,
            # Tags table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.TAGS_TABLE} (
                tag_id          TEXT PRIMARY KEY,
                tag             TEXT NOT NULL,
                project         TEXT NOT NULL REFERENCES sqitch.{cls.PROJECTS_TABLE}(project) ON UPDATE CASCADE,
                change_id       TEXT NOT NULL REFERENCES sqitch.{cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE,
                note            TEXT NOT NULL DEFAULT '',
                committed_at    TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                committer_name  TEXT NOT NULL,
                committer_email TEXT NOT NULL,
                planned_at      TIMESTAMP_TZ NOT NULL,
                planner_name    TEXT NOT NULL,
                planner_email   TEXT NOT NULL,
                UNIQUE(project, tag)
            )
            """,
            # Dependencies table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.DEPENDENCIES_TABLE} (
                change_id       TEXT NOT NULL REFERENCES sqitch.{cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE ON DELETE CASCADE,
                type            TEXT NOT NULL,
                dependency      TEXT NOT NULL,
                dependency_id   TEXT REFERENCES sqitch.{cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE,
                PRIMARY KEY (change_id, dependency)
            )
            """,
            # Events table
            f"""
            CREATE TABLE IF NOT EXISTS sqitch.{cls.EVENTS_TABLE} (
                event           TEXT NOT NULL,
                change_id       TEXT NOT NULL,
                change          TEXT NOT NULL,
                project         TEXT NOT NULL REFERENCES sqitch.{cls.PROJECTS_TABLE}(project) ON UPDATE CASCADE,
                note            TEXT NOT NULL DEFAULT '',
                requires        TEXT NOT NULL DEFAULT '',
                conflicts       TEXT NOT NULL DEFAULT '',
                tags            TEXT NOT NULL DEFAULT '',
                committed_at    TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                committer_name  TEXT NOT NULL,
                committer_email TEXT NOT NULL,
                planned_at      TIMESTAMP_TZ NOT NULL,
                planner_name    TEXT NOT NULL,
                planner_email   TEXT NOT NULL,
                PRIMARY KEY (change_id, committed_at)
            )
            """,
            # Insert registry version
            f"""
            INSERT INTO sqitch.{cls.RELEASES_TABLE} (version, installer_name, installer_email)
            SELECT {cls.REGISTRY_VERSION}, 'sqlitch', 'sqlitch@example.com'
            WHERE NOT EXISTS (SELECT 1 FROM sqitch.{cls.RELEASES_TABLE} WHERE version = {cls.REGISTRY_VERSION})
            """,
        ]


class SnowflakeConnection:
    """Wrapper for Snowflake connection with sqitch-specific functionality."""

    def __init__(self, connection: "snowflake.connector.SnowflakeConnection"):
        """
        Initialize Snowflake connection wrapper.

        Args:
            connection: snowflake-connector-python connection object
        """
        self._connection = connection
        self._cursor: Optional["snowflake.connector.cursor.SnowflakeCursor"] = None

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
                # Convert named parameters to positional for Snowflake
                formatted_query, param_values = self._format_query_params(
                    sql_query, params
                )
                cursor.execute(formatted_query, param_values)
            else:
                cursor.execute(sql_query)
        except SnowflakeError as e:
            raise DeploymentError(
                f"SQL execution failed: {e}",
                engine_name="snowflake",
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

    def _get_cursor(self) -> "snowflake.connector.cursor.SnowflakeCursor":
        """
        Get cursor for executing queries.

        Returns:
            Snowflake cursor object
        """
        if self._cursor is None:
            self._cursor = self._connection.cursor(DictCursor)
        return self._cursor

    def _format_query_params(self, query: str, params: Dict[str, Any]) -> tuple:
        """
        Format query with named parameters for Snowflake.

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


@register_engine("snowflake")
class SnowflakeEngine(Engine):
    """
    Snowflake database engine implementation.

    This engine provides Snowflake-specific functionality for sqitch operations
    including connection management, registry operations, and SQL execution.
    """

    def __init__(self, target: Target, plan: Plan) -> None:
        """
        Initialize Snowflake engine.

        Args:
            target: Target configuration for this engine
            plan: Plan containing changes to manage

        Raises:
            EngineError: If snowflake-connector-python is not available
        """
        if snowflake is None:
            raise EngineError(
                "snowflake-connector-python is required for Snowflake engine. "
                "Install it with: pip install snowflake-connector-python"
            )

        super().__init__(target, plan)
        self._warehouse = self._get_warehouse()
        self._role = self._get_role()
        self._registry_schema_name = self._get_registry_schema()

    @property
    def engine_type(self) -> EngineType:
        """Get the engine type identifier."""
        return "snowflake"

    @property
    def registry_schema(self) -> RegistrySchema:
        """Get the registry schema for this engine."""
        return SnowflakeRegistrySchema()

    def _create_connection(self) -> SnowflakeConnection:  # noqa: C901
        """
        Create a new Snowflake database connection.

        Returns:
            Snowflake connection wrapper

        Raises:
            ConnectionError: If connection cannot be established
        """
        try:
            # Parse connection parameters from target URI
            uri = self.target.uri

            # Extract connection parameters
            account = self._get_account()
            user = self._get_user()
            password = self._get_password()
            database = self._get_database()
            warehouse = self._warehouse
            role = self._role

            # Build connection parameters
            conn_params = {
                "account": account,
                "user": user,
                "database": database,
                "warehouse": warehouse,
                "autocommit": False,  # We manage transactions manually
            }

            # Add password if available
            if password:
                conn_params["password"] = password

            # Add role if specified
            if role:
                conn_params["role"] = role

            # Add schema if specified
            if uri.path and len(uri.path) > 1:
                # Remove leading slash and use as schema
                schema = uri.path[1:]
                if schema:
                    conn_params["schema"] = schema

            # Handle additional connection parameters from query string
            if uri.query:
                query_params = parse_qs(uri.query)
                for key, values in query_params.items():
                    if values and key not in conn_params:
                        conn_params[key] = values[0]

            self.logger.debug(f"Connecting to Snowflake account: {account}")

            # Create connection
            raw_connection = snowflake.connector.connect(**conn_params)

            # Set session parameters for sqitch compatibility
            cursor = raw_connection.cursor()
            try:
                # Resume warehouse if suspended
                cursor.execute(f"ALTER WAREHOUSE {warehouse} RESUME IF SUSPENDED")

                # Set timezone to UTC for consistency
                cursor.execute("ALTER SESSION SET TIMEZONE='UTC'")

                # Set timestamp format for consistency
                cursor.execute(
                    "ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS'"
                )
                cursor.execute("ALTER SESSION SET TIMESTAMP_TYPE_MAPPING=TIMESTAMP_LTZ")

                # Use sqitch schema
                cursor.execute(f"USE SCHEMA IDENTIFIER('{self._registry_schema_name}')")

            except SnowflakeError:
                # If schema doesn't exist, that's okay - it will be created later
                pass
            finally:
                cursor.close()

            return SnowflakeConnection(raw_connection)

        except SnowflakeError as e:
            raise ConnectionError(
                f"Failed to connect to Snowflake: {e}",
                connection_string=sanitize_connection_string(str(self.target.uri)),
                engine_name="snowflake",
            ) from e

    def _execute_sql_file(
        self,
        connection: SnowflakeConnection,
        sql_file: Path,
        variables: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Execute SQL file with optional variable substitution.

        Args:
            connection: Snowflake connection
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

            # Replace &warehouse with actual warehouse name
            sql_content = sql_content.replace("&warehouse", self._warehouse)

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
                engine_name="snowflake",
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

    def _get_registry_version(self, connection: SnowflakeConnection) -> Optional[str]:
        """
        Get current registry version from database.

        Args:
            connection: Snowflake connection

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
        Get Snowflake-specific regex condition.

        Args:
            column: Column name
            pattern: Regular expression pattern

        Returns:
            SQL condition string
        """
        # Snowflake uses REGEXP_SUBSTR for regex matching
        # Since REGEXP is implicitly anchored, we use REGEXP_SUBSTR which is not
        return f"REGEXP_SUBSTR({column}, ?) IS NOT NULL"

    def _get_account(self) -> str:
        """
        Get Snowflake account name.

        Returns:
            Account name

        Raises:
            EngineError: If account cannot be determined
        """
        uri = self.target.uri

        # Try to get from host
        if uri.hostname:
            host = uri.hostname
            if host.endswith(".snowflakecomputing.com"):
                return host.replace(".snowflakecomputing.com", "")
            return host

        # Try environment variables
        account = os.getenv("SNOWSQL_ACCOUNT")
        if account:
            return account

        # Try from URI query parameters
        if uri.query:
            query_params = parse_qs(uri.query)
            if "account" in query_params:
                return query_params["account"][0]

        raise EngineError("Cannot determine Snowflake account name")

    def _get_user(self) -> str:
        """
        Get Snowflake username.

        Returns:
            Username
        """
        uri = self.target.uri

        # Try URI username
        if uri.username:
            return uri.username

        # Try environment variables
        user = os.getenv("SNOWSQL_USER")
        if user:
            return user

        # Try from URI query parameters
        if uri.query:
            query_params = parse_qs(uri.query)
            if "user" in query_params:
                return query_params["user"][0]

        # Fall back to system user
        return os.getenv("USER", "unknown")

    def _get_password(self) -> Optional[str]:
        """
        Get Snowflake password.

        Returns:
            Password or None if not available
        """
        uri = self.target.uri

        # Try URI password
        if uri.password:
            return uri.password

        # Try environment variables
        password = os.getenv("SNOWSQL_PWD")
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
        Get Snowflake database name.

        Returns:
            Database name
        """
        uri = self.target.uri

        # Try URI path (first part)
        if uri.path and len(uri.path) > 1:
            path_parts = uri.path[1:].split("/")
            if path_parts[0]:
                return path_parts[0]

        # Try environment variables
        database = os.getenv("SNOWSQL_DATABASE")
        if database:
            return database

        # Try from URI query parameters
        if uri.query:
            query_params = parse_qs(uri.query)
            if "database" in query_params:
                return query_params["database"][0]

        # Fall back to username
        return self._get_user()

    def _get_warehouse(self) -> str:
        """
        Get Snowflake warehouse name.

        Returns:
            Warehouse name
        """
        uri = self.target.uri

        # Try from URI query parameters
        if uri.query:
            query_params = parse_qs(uri.query)
            if "warehouse" in query_params:
                return query_params["warehouse"][0]

        # Try environment variables
        warehouse = os.getenv("SNOWSQL_WAREHOUSE")
        if warehouse:
            return warehouse

        # Default to 'sqitch'
        return "sqitch"

    def _get_role(self) -> Optional[str]:
        """
        Get Snowflake role name.

        Returns:
            Role name or None if not specified
        """
        uri = self.target.uri

        # Try from URI query parameters
        if uri.query:
            query_params = parse_qs(uri.query)
            if "role" in query_params:
                return query_params["role"][0]

        # Try environment variables
        role = os.getenv("SNOWSQL_ROLE")
        if role:
            return role

        return None

    def _get_registry_schema(self) -> str:
        """
        Get registry schema name.

        Returns:
            Registry schema name
        """
        # Use target registry if specified, otherwise default to 'sqitch'
        return getattr(self.target, "registry", "sqitch")
