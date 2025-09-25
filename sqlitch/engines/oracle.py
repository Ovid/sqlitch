"""
Oracle database engine implementation.

This module provides the Oracle-specific implementation of the Engine
base class, handling Oracle connections, registry management, and
SQL execution with proper error handling and transaction management.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union
from urllib.parse import urlparse

from ..core.exceptions import ConnectionError, DeploymentError, EngineError
from ..core.plan import Plan
from ..core.target import Target
from ..core.types import EngineType, sanitize_connection_string
from .base import Engine, RegistrySchema, register_engine

# Try to import cx_Oracle
try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None


logger = logging.getLogger(__name__)


class OracleRegistrySchema(RegistrySchema):
    """Oracle-specific registry schema."""

    @classmethod
    def get_create_statements(
        cls, engine_type: EngineType, registry_schema: str = None
    ) -> List[str]:
        """
        Get Oracle-specific SQL statements to create registry tables.

        Args:
            engine_type: Database engine type (should be 'oracle')
            registry_schema: Registry schema name

        Returns:
            List of SQL CREATE statements for Oracle
        """
        schema_prefix = f"{registry_schema}." if registry_schema else ""

        return [
            # Releases table
            f"""
            CREATE TABLE {schema_prefix}{cls.RELEASES_TABLE} (
                version           FLOAT                    PRIMARY KEY,
                installed_at      TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp NOT NULL,
                installer_name    VARCHAR2(512 CHAR)       NOT NULL,
                installer_email   VARCHAR2(512 CHAR)       NOT NULL
            )
            """,
            # Projects table
            f"""
            CREATE TABLE {schema_prefix}{cls.PROJECTS_TABLE} (
                project         VARCHAR2(512 CHAR)       PRIMARY KEY,
                uri             VARCHAR2(512 CHAR)       NULL UNIQUE,
                created_at      TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp NOT NULL,
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
                committed_at    TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp NOT NULL,
                committer_name  VARCHAR2(512 CHAR)       NOT NULL,
                committer_email VARCHAR2(512 CHAR)       NOT NULL,
                planned_at      TIMESTAMP WITH TIME ZONE NOT NULL,
                planner_name    VARCHAR2(512 CHAR)       NOT NULL,
                planner_email   VARCHAR2(512 CHAR)       NOT NULL,
                UNIQUE(project, script_hash)
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
                committed_at    TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp NOT NULL,
                committer_name  VARCHAR2(512 CHAR)       NOT NULL,
                committer_email VARCHAR2(512 CHAR)       NOT NULL,
                planned_at      TIMESTAMP WITH TIME ZONE NOT NULL,
                planner_name    VARCHAR2(512 CHAR)       NOT NULL,
                planner_email   VARCHAR2(512 CHAR)       NOT NULL,
                UNIQUE(project, tag)
            )
            """,
            # Dependencies table
            f"""
            CREATE TABLE {schema_prefix}{cls.DEPENDENCIES_TABLE} (
                change_id       CHAR(40)                 NOT NULL REFERENCES {schema_prefix}{cls.CHANGES_TABLE}(change_id) ON DELETE CASCADE,
                type            VARCHAR2(8)              NOT NULL,
                dependency      VARCHAR2(1024 CHAR)      NOT NULL,
                dependency_id   CHAR(40)                     NULL REFERENCES {schema_prefix}{cls.CHANGES_TABLE}(change_id),
                CONSTRAINT dependencies_check CHECK (
                        (type = 'require'  AND dependency_id IS NOT NULL)
                     OR (type = 'conflict' AND dependency_id IS NULL)
                ),
                PRIMARY KEY (change_id, dependency)
            )
            """,
            # Create sqitch_array type
            f"CREATE TYPE {schema_prefix}sqitch_array AS varray(1024) OF VARCHAR2(512)",
            # Events table
            f"""
            CREATE TABLE {schema_prefix}{cls.EVENTS_TABLE} (
                event           VARCHAR2(6)                   NOT NULL
                CONSTRAINT events_event_check CHECK (
                    event IN ('deploy', 'revert', 'fail', 'merge')
                ),
                change_id       CHAR(40)                      NOT NULL,
                change          VARCHAR2(512 CHAR)            NOT NULL,
                project         VARCHAR2(512 CHAR)            NOT NULL REFERENCES {schema_prefix}{cls.PROJECTS_TABLE}(project),
                note            VARCHAR2(4000 CHAR)           DEFAULT '',
                requires        {schema_prefix}SQITCH_ARRAY   DEFAULT {schema_prefix}SQITCH_ARRAY() NOT NULL,
                conflicts       {schema_prefix}SQITCH_ARRAY   DEFAULT {schema_prefix}SQITCH_ARRAY() NOT NULL,
                tags            {schema_prefix}SQITCH_ARRAY   DEFAULT {schema_prefix}SQITCH_ARRAY() NOT NULL,
                committed_at    TIMESTAMP WITH TIME ZONE      DEFAULT current_timestamp NOT NULL,
                committer_name  VARCHAR2(512 CHAR)            NOT NULL,
                committer_email VARCHAR2(512 CHAR)            NOT NULL,
                planned_at      TIMESTAMP WITH TIME ZONE      NOT NULL,
                planner_name    VARCHAR2(512 CHAR)            NOT NULL,
                planner_email   VARCHAR2(512 CHAR)            NOT NULL
            )
            """,
            # Events index
            f"CREATE UNIQUE INDEX {schema_prefix}events_pkey ON {schema_prefix}{cls.EVENTS_TABLE}(change_id, committed_at)",
            # Comments for releases table
            f"COMMENT ON TABLE  {schema_prefix}{cls.RELEASES_TABLE} IS 'Sqitch registry releases.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.RELEASES_TABLE}.version IS 'Version of the Sqitch registry.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.RELEASES_TABLE}.installed_at IS 'Date the registry release was installed.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.RELEASES_TABLE}.installer_name IS 'Name of the user who installed the registry release.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.RELEASES_TABLE}.installer_email IS 'Email address of the user who installed the registry release.'",
            # Comments for projects table
            f"COMMENT ON TABLE  {schema_prefix}{cls.PROJECTS_TABLE} IS 'Sqitch projects deployed to this database.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.PROJECTS_TABLE}.project IS 'Unique Name of a project.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.PROJECTS_TABLE}.uri IS 'Optional project URI'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.PROJECTS_TABLE}.created_at IS 'Date the project was added to the database.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.PROJECTS_TABLE}.creator_name IS 'Name of the user who added the project.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.PROJECTS_TABLE}.creator_email IS 'Email address of the user who added the project.'",
            # Comments for changes table
            f"COMMENT ON TABLE  {schema_prefix}{cls.CHANGES_TABLE} IS 'Tracks the changes currently deployed to the database.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.change_id IS 'Change primary key.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.script_hash IS 'Deploy script SHA-1 hash.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.change IS 'Name of a deployed change.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.project IS 'Name of the Sqitch project to which the change belongs.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.note IS 'Description of the change.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.committed_at IS 'Date the change was deployed.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.committer_name IS 'Name of the user who deployed the change.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.committer_email IS 'Email address of the user who deployed the change.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.planned_at IS 'Date the change was added to the plan.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.planner_name IS 'Name of the user who planed the change.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.CHANGES_TABLE}.planner_email IS 'Email address of the user who planned the change.'",
            # Comments for tags table
            f"COMMENT ON TABLE  {schema_prefix}{cls.TAGS_TABLE} IS 'Tracks the tags currently applied to the database.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.tag_id IS 'Tag primary key.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.tag IS 'Project-unique tag name.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.project IS 'Name of the Sqitch project to which the tag belongs.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.change_id IS 'ID of last change deployed before the tag was applied.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.note IS 'Description of the tag.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.committed_at IS 'Date the tag was applied to the database.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.committer_name IS 'Name of the user who applied the tag.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.committer_email IS 'Email address of the user who applied the tag.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.planned_at IS 'Date the tag was added to the plan.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.planner_name IS 'Name of the user who planed the tag.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.TAGS_TABLE}.planner_email IS 'Email address of the user who planned the tag.'",
            # Comments for dependencies table
            f"COMMENT ON TABLE  {schema_prefix}{cls.DEPENDENCIES_TABLE} IS 'Tracks the currently satisfied dependencies.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.DEPENDENCIES_TABLE}.change_id IS 'ID of the depending change.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.DEPENDENCIES_TABLE}.type IS 'Type of dependency.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.DEPENDENCIES_TABLE}.dependency IS 'Dependency name.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.DEPENDENCIES_TABLE}.dependency_id IS 'Change ID the dependency resolves to.'",
            # Comments for events table
            f"COMMENT ON TABLE  {schema_prefix}{cls.EVENTS_TABLE} IS 'Contains full history of all deployment events.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.event IS 'Type of event.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.change_id IS 'Change ID.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.change IS 'Change name.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.project IS 'Name of the Sqitch project to which the change belongs.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.note IS 'Description of the change.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.requires IS 'Array of the names of required changes.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.conflicts IS 'Array of the names of conflicting changes.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.tags IS 'Tags associated with the change.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.committed_at IS 'Date the event was committed.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.committer_name IS 'Name of the user who committed the event.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.committer_email IS 'Email address of the user who committed the event.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.planned_at IS 'Date the event was added to the plan.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.planner_name IS 'Name of the user who planed the change.'",
            f"COMMENT ON COLUMN {schema_prefix}{cls.EVENTS_TABLE}.planner_email IS 'Email address of the user who plan planned the change.'",
        ]


class OracleConnection:
    """Wrapper for Oracle database connection."""

    def __init__(self, connection):
        self._connection = connection
        self._cursor = self._connection.cursor()

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute SQL statement."""
        if params:
            # Convert named parameters to positional for Oracle
            param_values = []
            sql_with_positions = sql
            for key, value in params.items():
                sql_with_positions = sql_with_positions.replace(f":{key}", "?")
                param_values.append(value)
            return self._cursor.execute(sql_with_positions, param_values)
        else:
            return self._cursor.execute(sql)

    def fetchone(self) -> Optional[Dict[str, Any]]:
        """Fetch one row from result set."""
        row = self._cursor.fetchone()
        if row is None:
            return None

        # Convert to dictionary using column names
        columns = [desc[0].lower() for desc in self._cursor.description]
        return dict(zip(columns, row))

    def fetchall(self) -> List[Dict[str, Any]]:
        """Fetch all rows from result set."""
        rows = self._cursor.fetchall()
        if not rows:
            return []

        # Convert to list of dictionaries using column names
        columns = [desc[0].lower() for desc in self._cursor.description]
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
        self._connection.close()


@register_engine("oracle")
class OracleEngine(Engine):
    """Oracle database engine implementation."""

    def __init__(self, target: Target, plan: Plan) -> None:
        """
        Initialize Oracle engine.

        Args:
            target: Target configuration for this engine
            plan: Plan containing changes to manage

        Raises:
            EngineError: If cx_Oracle is not available
        """
        if cx_Oracle is None:
            raise EngineError(
                "cx_Oracle package is required for Oracle support. "
                "Install it with: pip install cx_Oracle",
                engine_name="oracle",
            )

        super().__init__(target, plan)

        # Set Oracle environment variables for UTF-8 encoding
        os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
        os.environ["SQLPATH"] = ""  # Disable SQLPATH to prevent start scripts

        # Parse registry schema from target
        self._registry_schema = self._parse_registry_schema()
        self._registry_schema_obj = OracleRegistrySchema()

    @property
    def engine_type(self) -> EngineType:
        """Get the engine type identifier."""
        return "oracle"

    @property
    def registry_schema(self) -> RegistrySchema:
        """Get the registry schema for this engine."""
        return self._registry_schema_obj

    def _parse_registry_schema(self) -> Optional[str]:
        """Parse registry schema from target configuration."""
        # Check if registry is specified in target
        if hasattr(self.target, "registry") and self.target.registry:
            return self.target.registry

        # Default to username if available
        parsed_uri = urlparse(str(self.target.uri))
        if parsed_uri.username:
            return parsed_uri.username.upper()

        return None

    def _create_connection(self) -> OracleConnection:
        """
        Create a new Oracle database connection.

        Returns:
            Oracle database connection wrapper

        Raises:
            ConnectionError: If connection cannot be established
        """
        try:
            parsed_uri = urlparse(str(self.target.uri))

            # Build connection string
            username = parsed_uri.username
            password = parsed_uri.password
            hostname = parsed_uri.hostname or "localhost"
            port = parsed_uri.port or 1521

            # Handle database name/service name
            database = parsed_uri.path.lstrip("/") if parsed_uri.path else None
            if not database:
                # Try environment variables as fallback
                database = (
                    os.environ.get("TWO_TASK")
                    or os.environ.get("LOCAL")
                    or os.environ.get("ORACLE_SID")
                    or username
                )

            if not database:
                raise ConnectionError(
                    "No database/service name specified in connection string",
                    connection_string=sanitize_connection_string(str(self.target.uri)),
                    engine_name=self.engine_type,
                )

            # Create DSN
            dsn = cx_Oracle.makedsn(hostname, port, service_name=database)

            # Connect to Oracle
            connection = cx_Oracle.connect(
                user=username, password=password, dsn=dsn, encoding="UTF-8"
            )

            # Set session parameters
            cursor = connection.cursor()

            # Set date/time formats
            cursor.execute(
                "ALTER SESSION SET nls_date_format='YYYY-MM-DD HH24:MI:SS TZR'"
            )
            cursor.execute(
                "ALTER SESSION SET nls_timestamp_format='YYYY-MM-DD HH24:MI:SS TZR'"
            )
            cursor.execute(
                "ALTER SESSION SET nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS TZR'"
            )

            # Set current schema if registry is specified
            if self._registry_schema:
                try:
                    cursor.execute(
                        f"ALTER SESSION SET CURRENT_SCHEMA = {self._registry_schema}"
                    )
                except cx_Oracle.DatabaseError as e:
                    # Log warning but don't fail - schema might not exist yet
                    self.logger.warning(
                        f"Could not set current schema to {self._registry_schema}: {e}"
                    )

            cursor.close()

            return OracleConnection(connection)

        except cx_Oracle.DatabaseError as e:
            raise ConnectionError(
                f"Failed to connect to Oracle database: {e}",
                connection_string=sanitize_connection_string(str(self.target.uri)),
                engine_name=self.engine_type,
            ) from e
        except Exception as e:
            raise ConnectionError(
                f"Unexpected error connecting to Oracle database: {e}",
                connection_string=sanitize_connection_string(str(self.target.uri)),
                engine_name=self.engine_type,
            ) from e

    def _execute_sql_file(
        self,
        connection: OracleConnection,
        sql_file: Path,
        variables: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Execute SQL file with optional variable substitution.

        Args:
            connection: Oracle database connection
            sql_file: Path to SQL file to execute
            variables: Optional variables for substitution

        Raises:
            DeploymentError: If SQL execution fails
        """
        try:
            # Read SQL file content
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Perform variable substitution if variables provided
            if variables:
                for key, value in variables.items():
                    sql_content = sql_content.replace(f"&{key}", str(value))

            # Split into individual statements (Oracle uses / as statement separator)
            statements = self._split_oracle_statements(sql_content)

            for statement in statements:
                statement = statement.strip()
                if statement and not statement.startswith("--"):
                    try:
                        connection.execute(statement)
                    except Exception as e:
                        raise DeploymentError(
                            f"Failed to execute SQL statement: {e}\nStatement: {statement[:200]}...",
                            change_name=sql_file.stem,
                            operation="execute_sql",
                            engine_name=self.engine_type,
                        ) from e

        except Exception as e:
            if isinstance(e, DeploymentError):
                raise
            raise DeploymentError(
                f"Failed to execute SQL file {sql_file}: {e}",
                change_name=sql_file.stem,
                operation="execute_sql_file",
                engine_name=self.engine_type,
            ) from e

    def _split_oracle_statements(self, sql_content: str) -> List[str]:
        """
        Split Oracle SQL content into individual statements.

        Oracle uses / on its own line as statement separator for PL/SQL blocks,
        and semicolon for regular SQL statements.

        Args:
            sql_content: SQL content to split

        Returns:
            List of individual SQL statements
        """
        statements = []
        current_statement = []
        lines = sql_content.split("\n")

        for line in lines:
            stripped_line = line.strip()

            # Skip empty lines and comments
            if not stripped_line or stripped_line.startswith("--"):
                continue

            # Check for statement separator
            if stripped_line == "/":
                if current_statement:
                    statements.append("\n".join(current_statement))
                    current_statement = []
            else:
                current_statement.append(line)

        # Add final statement if exists
        if current_statement:
            statements.append("\n".join(current_statement))

        return statements

    def _get_registry_version(self, connection: OracleConnection) -> Optional[str]:
        """
        Get current registry version from database.

        Args:
            connection: Oracle database connection

        Returns:
            Registry version string or None if not found
        """
        try:
            schema_prefix = f"{self._registry_schema}." if self._registry_schema else ""
            connection.execute(
                f"SELECT version FROM {schema_prefix}{self.registry_schema.RELEASES_TABLE} ORDER BY version DESC"
            )
            row = connection.fetchone()
            return str(row["version"]) if row else None
        except Exception:
            return None

    def _regex_condition(self, column: str, pattern: str) -> str:
        """
        Get Oracle-specific regex condition.

        Args:
            column: Column name
            pattern: Regular expression pattern

        Returns:
            SQL condition string using REGEXP_LIKE
        """
        return f"REGEXP_LIKE({column}, ?)"

    def _registry_exists_in_db(self, connection: OracleConnection) -> bool:
        """
        Check if registry tables exist in Oracle database.

        Args:
            connection: Oracle database connection

        Returns:
            True if registry exists, False otherwise
        """
        try:
            schema_name = self._registry_schema
            if not schema_name:
                connection.execute(
                    "SELECT SYS_CONTEXT('USERENV', 'SESSION_SCHEMA') FROM DUAL"
                )
                result = connection.fetchone()
                if result:
                    schema_name = result.get(
                        "sys_context('userenv','session_schema')", "UNKNOWN"
                    )
                else:
                    return False

            connection.execute(
                """
                SELECT 1 FROM all_tables
                WHERE owner = UPPER(?) AND table_name = 'CHANGES'
                """,
                {"owner": schema_name},
            )
            row = connection.fetchone()
            return row is not None
        except Exception:
            return False

    def _create_registry(self, connection: OracleConnection) -> None:
        """
        Create registry tables in Oracle database.

        Args:
            connection: Oracle database connection

        Raises:
            EngineError: If registry creation fails
        """
        try:
            statements = self.registry_schema.get_create_statements(
                self.engine_type, self._registry_schema
            )

            for statement in statements:
                try:
                    connection.execute(statement)
                except Exception as e:
                    # Log the statement that failed for debugging
                    self.logger.error(
                        f"Failed to execute statement: {statement[:200]}..."
                    )
                    raise e

            # Insert initial release record
            self._insert_release_record(connection)

            # Insert initial project record
            self._insert_project_record(connection)

        except Exception as e:
            raise EngineError(
                f"Failed to create Oracle registry: {e}", engine_name=self.engine_type
            ) from e

    def _insert_release_record(self, connection: OracleConnection) -> None:
        """
        Insert release record into registry.

        Args:
            connection: Oracle database connection
        """
        schema_prefix = f"{self._registry_schema}." if self._registry_schema else ""

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

    def _insert_project_record(self, connection: OracleConnection) -> None:
        """
        Insert project record into registry.

        Args:
            connection: Oracle database connection
        """
        schema_prefix = f"{self._registry_schema}." if self._registry_schema else ""
        project_name = self.plan.project_name
        project_uri = str(self.target.uri)
        creator_name = self.plan.creator_name or "Unknown"
        creator_email = self.plan.creator_email or "unknown@example.com"

        # Check if project already exists
        connection.execute(
            f"""
            SELECT COUNT(*) as count FROM {schema_prefix}{self.registry_schema.PROJECTS_TABLE}
            WHERE project = ?
            """,
            {"project": project_name},
        )

        row = connection.fetchone()
        if row and row["count"] == 0:
            connection.execute(
                f"""
                INSERT INTO {schema_prefix}{self.registry_schema.PROJECTS_TABLE}
                (project, uri, creator_name, creator_email)
                VALUES (?, ?, ?, ?)
                """,
                {
                    "project": project_name,
                    "uri": project_uri,
                    "creator_name": creator_name,
                    "creator_email": creator_email,
                },
            )
