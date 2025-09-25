"""
Firebird database engine implementation.

This module provides the Firebird-specific implementation of the Engine
base class, handling Firebird connections, registry management, and
SQL execution with proper error handling and transaction management.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlparse

try:
    import fdb
except ImportError:
    fdb = None

from ..core.exceptions import ConnectionError, DeploymentError, EngineError
from ..core.plan import Plan
from ..core.target import Target
from ..core.types import EngineType, sanitize_connection_string
from .base import Engine, RegistrySchema, register_engine

logger = logging.getLogger(__name__)


class FirebirdRegistrySchema(RegistrySchema):
    """Firebird-specific registry schema."""

    @classmethod
    def get_create_statements(cls, engine_type: EngineType) -> List[str]:
        """
        Get Firebird-specific SQL statements to create registry tables.

        Args:
            engine_type: Database engine type (should be 'firebird')

        Returns:
            List of SQL CREATE statements for Firebird
        """
        return [
            # Releases table
            f"""
            CREATE TABLE {cls.RELEASES_TABLE} (
                version         FLOAT         NOT NULL PRIMARY KEY,
                installed_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
                installer_name  VARCHAR(255)  NOT NULL,
                installer_email VARCHAR(255)  NOT NULL
            )
            """,
            # Projects table
            f"""
            CREATE TABLE {cls.PROJECTS_TABLE} (
                project         VARCHAR(255)  NOT NULL PRIMARY KEY,
                uri             VARCHAR(255)  UNIQUE,
                created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
                creator_name    VARCHAR(255)  NOT NULL,
                creator_email   VARCHAR(255)  NOT NULL
            )
            """,
            # Changes table
            f"""
            CREATE TABLE {cls.CHANGES_TABLE} (
                change_id       VARCHAR(40)   NOT NULL PRIMARY KEY,
                script_hash     VARCHAR(40),
                change          VARCHAR(255)  NOT NULL,
                project         VARCHAR(255)  NOT NULL REFERENCES {cls.PROJECTS_TABLE}(project)
                                               ON UPDATE CASCADE,
                note            BLOB SUB_TYPE TEXT DEFAULT '' NOT NULL,
                committed_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
                committer_name  VARCHAR(255)  NOT NULL,
                committer_email VARCHAR(255)  NOT NULL,
                planned_at      TIMESTAMP     NOT NULL,
                planner_name    VARCHAR(255)  NOT NULL,
                planner_email   VARCHAR(255)  NOT NULL,
                UNIQUE(project, script_hash)
            )
            """,
            # Tags table
            f"""
            CREATE TABLE {cls.TAGS_TABLE} (
                tag_id          CHAR(40)      NOT NULL PRIMARY KEY,
                tag             VARCHAR(250)  NOT NULL,
                project         VARCHAR(255)  NOT NULL REFERENCES {cls.PROJECTS_TABLE}(project)
                                                ON UPDATE CASCADE,
                change_id       CHAR(40)      NOT NULL REFERENCES {cls.CHANGES_TABLE}(change_id)
                                                ON UPDATE CASCADE,
                note            BLOB SUB_TYPE TEXT DEFAULT '' NOT NULL,
                committed_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
                committer_name  VARCHAR(512)  NOT NULL,
                committer_email VARCHAR(512)  NOT NULL,
                planned_at      TIMESTAMP     NOT NULL,
                planner_name    VARCHAR(512)  NOT NULL,
                planner_email   VARCHAR(512)  NOT NULL,
                UNIQUE(project, tag)
            )
            """,
            # Dependencies table
            f"""
            CREATE TABLE {cls.DEPENDENCIES_TABLE} (
                change_id       CHAR(40)      NOT NULL REFERENCES {cls.CHANGES_TABLE}(change_id)
                                                ON UPDATE CASCADE ON DELETE CASCADE,
                type            VARCHAR(8)    NOT NULL,
                dependency      VARCHAR(512)  NOT NULL,
                dependency_id   CHAR(40)      REFERENCES {cls.CHANGES_TABLE}(change_id)
                                                ON UPDATE CASCADE,
                PRIMARY KEY (change_id, dependency),
                CONSTRAINT dependencies_check CHECK (
                       (type = 'require'  AND dependency_id IS NOT NULL)
                    OR (type = 'conflict' AND dependency_id IS NULL)
                )
            )
            """,
            # Events table
            f"""
            CREATE TABLE {cls.EVENTS_TABLE} (
                event           VARCHAR(6)    NOT NULL
                CONSTRAINT events_event_check CHECK (
                    event IN ('deploy', 'revert', 'fail', 'merge')
                ),
                change_id       CHAR(40)      NOT NULL,
                change          VARCHAR(512)  NOT NULL,
                project         VARCHAR(255)  NOT NULL REFERENCES {cls.PROJECTS_TABLE}(project)
                                                ON UPDATE CASCADE,
                note            BLOB SUB_TYPE TEXT DEFAULT '' NOT NULL,
                requires        BLOB SUB_TYPE TEXT DEFAULT '' NOT NULL,
                conflicts       BLOB SUB_TYPE TEXT DEFAULT '' NOT NULL,
                tags            BLOB SUB_TYPE TEXT DEFAULT '' NOT NULL,
                committed_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
                committer_name  VARCHAR(512)  NOT NULL,
                committer_email VARCHAR(512)  NOT NULL,
                planned_at      TIMESTAMP     NOT NULL,
                planner_name    VARCHAR(512)  NOT NULL,
                planner_email   VARCHAR(512)  NOT NULL,
                PRIMARY KEY (change_id, committed_at)
            )
            """,
            # Insert initial release record
            f"""
            INSERT INTO {cls.RELEASES_TABLE}
            (version, installer_name, installer_email)
            VALUES (1.1, 'sqlitch', 'sqlitch@example.com')
            """,
            "COMMIT",
        ]


class FirebirdConnection:
    """Wrapper for Firebird database connection."""

    def __init__(self, connection: "fdb.Connection"):
        self._conn = connection
        self._cursor = None

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute SQL statement."""
        if self._cursor is None:
            self._cursor = self._conn.cursor()

        try:
            if params:
                # Convert named parameters to positional for fdb
                param_values = []
                # Replace named placeholders with positional ones
                sql_with_positions = sql
                for key, value in params.items():
                    sql_with_positions = sql_with_positions.replace(f":{key}", "?")
                    param_values.append(value)

                # Also handle ? placeholders directly
                if "?" in sql and not params:
                    self._cursor.execute(sql)
                elif param_values:
                    self._cursor.execute(sql_with_positions, param_values)
                else:
                    # Handle case where we have named params but no ? in SQL
                    param_list = list(params.values())
                    self._cursor.execute(sql, param_list)
            else:
                self._cursor.execute(sql)

            return self._cursor
        except Exception:
            logger.error(f"SQL execution failed: {sql[:100]}...")
            logger.error(f"Parameters: {params}")
            raise

    def fetchone(self) -> Optional[Dict[str, Any]]:
        """Fetch one row from result set."""
        if self._cursor is None:
            return None

        row = self._cursor.fetchone()
        if row is None:
            return None

        # Convert to dictionary using column descriptions
        columns = [desc[0].lower() for desc in self._cursor.description]
        return dict(zip(columns, row))

    def fetchall(self) -> List[Dict[str, Any]]:
        """Fetch all rows from result set."""
        if self._cursor is None:
            return []

        rows = self._cursor.fetchall()
        if not rows:
            return []

        # Convert to list of dictionaries
        columns = [desc[0].lower() for desc in self._cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def commit(self) -> None:
        """Commit current transaction."""
        self._conn.commit()

    def rollback(self) -> None:
        """Rollback current transaction."""
        self._conn.rollback()

    def close(self) -> None:
        """Close the connection."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        self._conn.close()


@register_engine("firebird")
class FirebirdEngine(Engine):
    """
    Firebird database engine implementation.

    This engine supports Firebird databases using the fdb driver.
    It handles Firebird-specific SQL syntax, connection management,
    and registry operations.
    """

    def __init__(self, target: Target, plan: Plan) -> None:
        """
        Initialize Firebird engine.

        Args:
            target: Target configuration for this engine
            plan: Plan containing changes to manage

        Raises:
            EngineError: If fdb driver is not available
        """
        if fdb is None:
            raise EngineError(
                "Firebird support requires the 'fdb' package. "
                "Install it with: pip install fdb",
                engine_name="firebird",
            )

        super().__init__(target, plan)
        self._registry_schema = FirebirdRegistrySchema()

    @property
    def engine_type(self) -> EngineType:
        """Get the engine type identifier."""
        return "firebird"

    @property
    def registry_schema(self) -> RegistrySchema:
        """Get the registry schema for this engine."""
        return self._registry_schema

    def _create_connection(self) -> FirebirdConnection:  # noqa: C901
        """
        Create a new Firebird database connection.

        Returns:
            Firebird connection wrapper

        Raises:
            ConnectionError: If connection cannot be established
        """
        try:
            # Parse connection parameters from URI
            uri_str = str(self.target.uri)

            # Handle sqitch-style URIs: db:firebird://user:pass@host:port/path/to/database
            if uri_str.startswith("db:firebird:"):
                uri_str = uri_str[3:]  # Remove 'db:' prefix
            elif uri_str.startswith("firebird:"):
                uri_str = uri_str  # Keep as is

            # Parse the URI
            parsed = urlparse(uri_str)

            # Extract database path from URI
            if parsed.scheme == "firebird":
                # Handle firebird://host:port/path/to/database
                if parsed.hostname:
                    if parsed.port:
                        dsn = (
                            f"{parsed.hostname}/{parsed.port}:{parsed.path.lstrip('/')}"
                        )
                    else:
                        dsn = f"{parsed.hostname}:{parsed.path.lstrip('/')}"
                else:
                    # Local database file
                    dsn = parsed.path.lstrip("/")
            else:
                # Direct file path
                dsn = uri_str.replace("firebird://", "")

            # Connection parameters
            user = parsed.username or "SYSDBA"
            password = parsed.password or "masterkey"

            # Create database if it doesn't exist
            try:
                # Try to connect first
                conn = fdb.connect(
                    dsn=dsn, user=user, password=password, charset="UTF8"
                )
            except fdb.DatabaseError as e:
                if "No such file or directory" in str(
                    e
                ) or "cannot attach to services manager" in str(e):
                    # Database doesn't exist, create it
                    self.logger.info(f"Creating Firebird database: {dsn}")
                    conn = fdb.create_database(
                        dsn=dsn,
                        user=user,
                        password=password,
                        charset="UTF8",
                        page_size=16384,  # Required for sqitch registry
                    )
                else:
                    raise

            return FirebirdConnection(conn)

        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Firebird database: {e}",
                connection_string=sanitize_connection_string(str(self.target.uri)),
                engine_name=self.engine_type,
            ) from e

    def _execute_sql_file(
        self,
        connection: FirebirdConnection,
        sql_file: Path,
        variables: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Execute SQL file with optional variable substitution.

        Args:
            connection: Firebird connection
            sql_file: Path to SQL file to execute
            variables: Optional variables for substitution

        Raises:
            DeploymentError: If SQL execution fails
        """
        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Perform variable substitution if provided
            if variables:
                for key, value in variables.items():
                    sql_content = sql_content.replace(f"${{{key}}}", str(value))

            # Split SQL content into individual statements
            # Firebird uses semicolon as statement separator
            statements = self._split_sql_statements(sql_content)

            for statement in statements:
                statement = statement.strip()
                if statement and not statement.startswith("--"):
                    self.logger.debug(f"Executing: {statement[:100]}...")
                    connection.execute(statement)

        except Exception as e:
            raise DeploymentError(
                f"Failed to execute SQL file {sql_file}: {e}",
                sql_file=str(sql_file),
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
        # Remove comments and split by semicolon
        # This is a simple implementation - more sophisticated parsing
        # might be needed for complex SQL with embedded semicolons
        statements = []
        current_statement = []

        for line in sql_content.split("\n"):
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("--"):
                continue

            # Remove inline comments
            if "--" in line:
                line = line[: line.index("--")].strip()

            current_statement.append(line)

            # Check if statement ends with semicolon
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

    def _get_registry_version(self, connection: FirebirdConnection) -> Optional[str]:
        """
        Get current registry version from database.

        Args:
            connection: Firebird connection

        Returns:
            Registry version string or None if not found
        """
        try:
            connection.execute(
                f"SELECT CAST(ROUND(MAX(version), 1) AS VARCHAR(24)) AS v FROM {self.registry_schema.RELEASES_TABLE}"
            )
            row = connection.fetchone()
            return str(row["v"]) if row and row["v"] else None
        except Exception:
            return None

    def _regex_condition(self, column: str, pattern: str) -> str:
        """
        Get Firebird-specific regex condition using SIMILAR TO.

        Args:
            column: Column name
            pattern: Regular expression pattern

        Returns:
            SQL condition string
        """
        # Firebird uses SIMILAR TO which is different from standard regex
        # Convert common regex patterns to SIMILAR TO patterns
        self._convert_regex_to_similar(pattern)
        return f"{column} SIMILAR TO ?"

    def _convert_regex_to_similar(self, regex_pattern: str) -> str:
        """
        Convert regex pattern to Firebird SIMILAR TO pattern.

        Args:
            regex_pattern: Regular expression pattern

        Returns:
            SIMILAR TO pattern
        """
        # Basic conversion - this is simplified
        # SIMILAR TO requires the pattern to match the entire string
        pattern = regex_pattern

        # Handle anchors
        if pattern.startswith("^") and pattern.endswith("$"):
            # Remove anchors as SIMILAR TO matches entire string by default
            pattern = pattern[1:-1]
        elif pattern.startswith("^"):
            # Remove start anchor
            pattern = pattern[1:] + "%"
        elif pattern.endswith("$"):
            # Remove end anchor
            pattern = "%" + pattern[:-1]
        else:
            # Add wildcards for partial matching
            pattern = "%" + pattern + "%"

        return pattern

    def _registry_exists_in_db(self, connection: FirebirdConnection) -> bool:
        """
        Check if registry tables exist in Firebird database.

        Args:
            connection: Firebird connection

        Returns:
            True if registry exists, False otherwise
        """
        try:
            # Check if the changes table exists using Firebird system tables
            connection.execute(
                """
                SELECT COUNT(RDB$RELATION_NAME)
                FROM RDB$RELATIONS
                WHERE RDB$SYSTEM_FLAG=0
                      AND RDB$VIEW_BLR IS NULL
                      AND RDB$RELATION_NAME = ?
            """,
                {"table_name": self.registry_schema.CHANGES_TABLE.upper()},
            )

            row = connection.fetchone()
            return row and row.get("count", 0) > 0
        except Exception:
            return False

    def get_current_state(
        self, project: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get current deployment state of the database.

        Args:
            project: Project name (defaults to plan project)

        Returns:
            Dictionary with current state information or None if no changes deployed

        Raises:
            EngineError: If query fails
        """
        self.ensure_registry()

        project_name = project or self.plan.project_name

        with self.connection() as conn:
            try:
                # Get the most recent change with tags using Firebird LIST function
                conn.execute(
                    f"""
                    SELECT FIRST 1 c.change_id, c.script_hash, c.change, c.project, c.note,
                           c.committer_name, c.committer_email, c.committed_at,
                           c.planner_name, c.planner_email, c.planned_at,
                           LIST(t.tag, ' ') as tags
                    FROM {self.registry_schema.CHANGES_TABLE} c
                    LEFT JOIN {self.registry_schema.TAGS_TABLE} t ON c.change_id = t.change_id
                    WHERE c.project = ?
                    GROUP BY c.change_id, c.script_hash, c.change, c.project, c.note,
                             c.committer_name, c.committer_email, c.committed_at,
                             c.planner_name, c.planner_email, c.planned_at
                    ORDER BY c.committed_at DESC
                """,
                    {"project": project_name},
                )

                row = conn.fetchone()

                if not row:
                    return None

                # Parse tags
                tags = []
                if row.get("tags"):
                    tags = [
                        tag.strip() for tag in row["tags"].split(" ") if tag.strip()
                    ]

                return {
                    "change_id": row["change_id"],
                    "script_hash": row["script_hash"],
                    "change": row["change"],
                    "project": row["project"],
                    "note": row["note"] or "",
                    "committer_name": row["committer_name"],
                    "committer_email": row["committer_email"],
                    "committed_at": row["committed_at"],
                    "planner_name": row["planner_name"],
                    "planner_email": row["planner_email"],
                    "planned_at": row["planned_at"],
                    "tags": tags,
                }

            except Exception as e:
                raise EngineError(
                    f"Failed to get current state: {e}", engine_name=self.engine_type
                ) from e

    def search_events(  # noqa: C901
        self,
        event: Optional[List[str]] = None,
        change: Optional[str] = None,
        project: Optional[str] = None,
        committer: Optional[str] = None,
        planner: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        direction: str = "DESC",
    ) -> Iterator[Dict[str, Any]]:
        """
        Search events in the registry using Firebird-specific syntax.

        Args:
            event: List of event types to filter by
            change: Regular expression to match change names
            project: Regular expression to match project names
            committer: Regular expression to match committer names
            planner: Regular expression to match planner names
            limit: Maximum number of events to return
            offset: Number of events to skip
            direction: Sort direction ('ASC' or 'DESC')

        Yields:
            Dictionary with event information

        Raises:
            EngineError: If query fails
        """
        self.ensure_registry()

        # Validate direction
        if direction.upper() not in ("ASC", "DESC"):
            raise EngineError(
                f"Search direction must be either 'ASC' or 'DESC', got '{direction}'"
            )

        direction = direction.upper()

        # Build WHERE clause
        where_conditions = []
        params = {}
        param_counter = 0

        if event:
            placeholders = []
            for evt in event:
                param_name = f"event_{param_counter}"
                placeholders.append(f":{param_name}")
                params[param_name] = evt
                param_counter += 1
            where_conditions.append(f"e.event IN ({', '.join(placeholders)})")

        if change:
            param_name = f"change_{param_counter}"
            where_conditions.append(f"e.change SIMILAR TO :{param_name}")
            params[param_name] = self._convert_regex_to_similar(change)
            param_counter += 1

        if project:
            param_name = f"project_{param_counter}"
            where_conditions.append(f"e.project SIMILAR TO :{param_name}")
            params[param_name] = self._convert_regex_to_similar(project)
            param_counter += 1

        if committer:
            param_name = f"committer_{param_counter}"
            where_conditions.append(f"e.committer_name SIMILAR TO :{param_name}")
            params[param_name] = self._convert_regex_to_similar(committer)
            param_counter += 1

        if planner:
            param_name = f"planner_{param_counter}"
            where_conditions.append(f"e.planner_name SIMILAR TO :{param_name}")
            params[param_name] = self._convert_regex_to_similar(planner)
            param_counter += 1

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        # Build FIRST/SKIP clause (Firebird's LIMIT/OFFSET)
        limit_clause = ""
        if limit is not None:
            limit_clause = f"FIRST {limit}"
            if offset is not None:
                limit_clause += f" SKIP {offset}"
        elif offset is not None:
            limit_clause = f"FIRST 999999999 SKIP {offset}"

        query = f"""
            SELECT {limit_clause} e.event, e.project, e.change_id, e.change, e.note,
                   e.requires, e.conflicts, e.tags,
                   e.committer_name, e.committer_email, e.committed_at,
                   e.planner_name, e.planner_email, e.planned_at
            FROM {self.registry_schema.EVENTS_TABLE} e
            {where_clause}
            ORDER BY e.committed_at {direction}
        """

        with self.connection() as conn:
            try:
                conn.execute(query, params if params else None)

                while True:
                    row = conn.fetchone()
                    if not row:
                        break

                    # Parse array fields (stored as space-delimited strings)
                    requires = self._parse_array_field(row.get("requires", ""))
                    conflicts = self._parse_array_field(row.get("conflicts", ""))
                    tags = self._parse_array_field(row.get("tags", ""))

                    yield {
                        "event": row["event"],
                        "project": row["project"],
                        "change_id": row["change_id"],
                        "change": row["change"],
                        "note": row["note"] or "",
                        "requires": requires,
                        "conflicts": conflicts,
                        "tags": tags,
                        "committer_name": row["committer_name"],
                        "committer_email": row["committer_email"],
                        "committed_at": row["committed_at"],
                        "planner_name": row["planner_name"],
                        "planner_email": row["planner_email"],
                        "planned_at": row["planned_at"],
                    }

            except Exception as e:
                raise EngineError(
                    f"Failed to search events: {e}", engine_name=self.engine_type
                ) from e
