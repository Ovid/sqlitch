"""
Abstract base class for database engines.

This module defines the abstract Engine base class that all database-specific
engines must implement. It provides the common interface and shared functionality
for database operations including deployment, revert, verification, and status.
"""

import hashlib
import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Protocol,
)

from ..core.change import Change
from ..core.exceptions import ConnectionError, DeploymentError, EngineError
from ..core.plan import Plan
from ..core.types import (
    ChangeId,
    ChangeStatus,
    EngineType,
    Target,
    sanitize_connection_string,
)

logger = logging.getLogger(__name__)


class Connection(Protocol):
    """Protocol for database connections."""

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute SQL statement."""
        ...

    def fetchone(self) -> Optional[Dict[str, Any]]:
        """Fetch one row from result set."""
        ...

    def fetchall(self) -> List[Dict[str, Any]]:
        """Fetch all rows from result set."""
        ...

    def commit(self) -> None:
        """Commit current transaction."""
        ...

    def rollback(self) -> None:
        """Rollback current transaction."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...


class RegistrySchema:
    """Schema definition for sqitch registry tables."""

    # Core registry tables
    PROJECTS_TABLE = "projects"
    RELEASES_TABLE = "releases"
    CHANGES_TABLE = "changes"
    TAGS_TABLE = "tags"
    DEPENDENCIES_TABLE = "dependencies"
    EVENTS_TABLE = "events"

    # Registry version for schema upgrades
    REGISTRY_VERSION = "1.1"

    @classmethod
    def get_create_statements(cls, engine_type: EngineType) -> List[str]:
        """
        Get SQL statements to create registry tables for specific engine.

        Args:
            engine_type: Database engine type

        Returns:
            List of SQL CREATE statements
        """
        # This will be implemented by each engine with engine-specific SQL
        raise NotImplementedError("Subclasses must implement get_create_statements")


class Engine(ABC):
    """
    Abstract base class for database engines.

    This class defines the interface that all database engines must implement
    to support sqitch operations. It provides common functionality for
    connection management, registry operations, and change execution.
    """

    def __init__(self, target: Target, plan: Plan) -> None:
        """
        Initialize database engine.

        Args:
            target: Target configuration for this engine
            plan: Plan containing changes to manage
        """
        self.target = target
        self.plan = plan
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._connection: Optional[Connection] = None
        self._registry_exists: Optional[bool] = None

    @property
    @abstractmethod
    def engine_type(self) -> EngineType:
        """Get the engine type identifier."""
        ...

    @property
    @abstractmethod
    def registry_schema(self) -> RegistrySchema:
        """Get the registry schema for this engine."""
        ...

    @abstractmethod
    def _create_connection(self) -> Connection:
        """
        Create a new database connection.

        Returns:
            Database connection object

        Raises:
            ConnectionError: If connection cannot be established
        """
        ...

    @abstractmethod
    def _execute_sql_file(
        self,
        connection: Connection,
        sql_file: Path,
        variables: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Execute SQL file with optional variable substitution.

        Args:
            connection: Database connection
            sql_file: Path to SQL file to execute
            variables: Optional variables for substitution

        Raises:
            DeploymentError: If SQL execution fails
        """
        ...

    @abstractmethod
    def _get_registry_version(self, connection: Connection) -> Optional[str]:
        """
        Get current registry version from database.

        Args:
            connection: Database connection

        Returns:
            Registry version string or None if not found
        """
        ...

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        """
        Get database connection as context manager.

        Yields:
            Database connection

        Raises:
            ConnectionError: If connection cannot be established
        """
        conn = None
        try:
            conn = self._create_connection()
            self.logger.debug(
                f"Connected to {sanitize_connection_string(str(self.target.uri))}"
            )
            yield conn
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass  # Ignore rollback errors
            raise ConnectionError(
                f"Failed to connect to {self.engine_type} database: {e}",
                connection_string=sanitize_connection_string(str(self.target.uri)),
                engine_name=self.engine_type,
            ) from e
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass  # Ignore close errors

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """
        Get database connection with transaction management.

        Yields:
            Database connection with active transaction

        Raises:
            ConnectionError: If connection cannot be established
            DeploymentError: If transaction fails
        """
        with self.connection() as conn:
            try:
                yield conn
                conn.commit()
                self.logger.debug("Transaction committed")
            except Exception as e:
                try:
                    conn.rollback()
                    self.logger.debug("Transaction rolled back")
                except Exception:
                    pass  # Ignore rollback errors
                raise DeploymentError(
                    f"Transaction failed: {e}", engine_name=self.engine_type
                ) from e

    def ensure_registry(self) -> None:
        """
        Ensure registry tables exist and are up to date.

        Raises:
            EngineError: If registry cannot be created or upgraded
        """
        if self._registry_exists is True:
            return

        with self.transaction() as conn:
            # Check if registry exists
            if not self._registry_exists_in_db(conn):
                self.logger.info("Creating sqitch registry")
                self._create_registry(conn)
            else:
                # Check version and upgrade if needed
                current_version = self._get_registry_version(conn)
                if current_version != self.registry_schema.REGISTRY_VERSION:
                    self.logger.info(
                        f"Upgrading registry from {current_version} to {self.registry_schema.REGISTRY_VERSION}"
                    )
                    self._upgrade_registry(conn, current_version)

        self._registry_exists = True

    def _registry_exists_in_db(self, connection: Connection) -> bool:
        """
        Check if registry tables exist in database.

        Args:
            connection: Database connection

        Returns:
            True if registry exists, False otherwise
        """
        try:
            # Try to query the projects table
            connection.execute(
                f"SELECT COUNT(*) FROM {self.registry_schema.PROJECTS_TABLE}"
            )
            return True
        except Exception:
            return False

    def _create_registry(self, connection: Connection) -> None:
        """
        Create registry tables in database.

        Args:
            connection: Database connection

        Raises:
            EngineError: If registry creation fails
        """
        try:
            statements = self.registry_schema.get_create_statements(self.engine_type)
            for statement in statements:
                connection.execute(statement)

            # Insert initial project record
            self._insert_project_record(connection)

        except Exception as e:
            raise EngineError(
                f"Failed to create registry: {e}", engine_name=self.engine_type
            ) from e

    def _upgrade_registry(
        self, connection: Connection, from_version: Optional[str]
    ) -> None:
        """
        Upgrade registry schema to current version.

        Args:
            connection: Database connection
            from_version: Current registry version

        Raises:
            EngineError: If upgrade fails
        """
        # Default implementation - subclasses can override for specific upgrades
        self.logger.warning(f"Registry upgrade from {from_version} not implemented")

    def _insert_project_record(self, connection: Connection) -> None:
        """
        Insert project record into registry.

        Args:
            connection: Database connection
        """
        project_name = self.plan.project_name
        project_uri = str(self.target.uri)
        created_at = datetime.now(timezone.utc)
        creator_name = self.plan.creator_name or "Unknown"
        creator_email = self.plan.creator_email or "unknown@example.com"

        connection.execute(
            f"""
            INSERT INTO {self.registry_schema.PROJECTS_TABLE}
            (project, uri, created_at, creator_name, creator_email)
            VALUES (?, ?, ?, ?, ?)
            """,
            {
                "project": project_name,
                "uri": project_uri,
                "created_at": created_at,
                "creator_name": creator_name,
                "creator_email": creator_email,
            },
        )

    def get_deployed_changes(self) -> List[ChangeId]:
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
                    SELECT change_id FROM {self.registry_schema.CHANGES_TABLE}
                    WHERE project = ?
                    ORDER BY committed_at
                    """,
                    {"project": self.plan.project_name},
                )
                rows = conn.fetchall()
                return [ChangeId(row["change_id"]) for row in rows]
            except Exception as e:
                raise EngineError(
                    f"Failed to get deployed changes: {e}", engine_name=self.engine_type
                ) from e

    def get_change_status(self, change: Change) -> ChangeStatus:
        """
        Get deployment status of a specific change.

        Args:
            change: Change to check status for

        Returns:
            Status of the change

        Raises:
            EngineError: If query fails
        """
        self.ensure_registry()

        with self.connection() as conn:
            try:
                conn.execute(
                    f"""
                    SELECT committed_at FROM {self.registry_schema.CHANGES_TABLE}
                    WHERE project = ? AND change_id = ?
                    """,
                    {"project": self.plan.project_name, "change_id": change.id},
                )
                row = conn.fetchone()
                return ChangeStatus.DEPLOYED if row else ChangeStatus.PENDING
            except Exception as e:
                raise EngineError(
                    f"Failed to get change status: {e}", engine_name=self.engine_type
                ) from e

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
                # Get the most recent change with tags
                conn.execute(
                    f"""
                    SELECT c.change_id, c.script_hash, c.change, c.project, c.note,
                           c.committer_name, c.committer_email, c.committed_at,
                           c.planner_name, c.planner_email, c.planned_at,
                           GROUP_CONCAT(t.tag, ' ') as tags
                    FROM {self.registry_schema.CHANGES_TABLE} c
                    LEFT JOIN {self.registry_schema.TAGS_TABLE} t ON c.change_id = t.change_id
                    WHERE c.project = ?
                    GROUP BY c.change_id, c.script_hash, c.change, c.project, c.note,
                             c.committer_name, c.committer_email, c.committed_at,
                             c.planner_name, c.planner_email, c.planned_at
                    ORDER BY c.committed_at DESC
                    LIMIT 1
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

    def get_current_changes(
        self, project: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Get iterator over all deployed changes.

        Args:
            project: Project name (defaults to plan project)

        Yields:
            Dictionary with change information

        Raises:
            EngineError: If query fails
        """
        self.ensure_registry()

        project_name = project or self.plan.project_name

        with self.connection() as conn:
            try:
                conn.execute(
                    f"""
                    SELECT change_id, script_hash, change, committer_name, committer_email,
                           committed_at, planner_name, planner_email, planned_at
                    FROM {self.registry_schema.CHANGES_TABLE}
                    WHERE project = ?
                    ORDER BY committed_at DESC
                    """,
                    {"project": project_name},
                )

                while True:
                    row = conn.fetchone()
                    if not row:
                        break
                    yield {
                        "change_id": row["change_id"],
                        "script_hash": row["script_hash"],
                        "change": row["change"],
                        "committer_name": row["committer_name"],
                        "committer_email": row["committer_email"],
                        "committed_at": row["committed_at"],
                        "planner_name": row["planner_name"],
                        "planner_email": row["planner_email"],
                        "planned_at": row["planned_at"],
                    }

            except Exception as e:
                raise EngineError(
                    f"Failed to get current changes: {e}", engine_name=self.engine_type
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
        Search events in the registry.

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
        params = []

        if event:
            placeholders = ", ".join("?" for _ in event)
            where_conditions.append(f"e.event IN ({placeholders})")
            # Add event values to params list
            params.extend(event)

        if change:
            where_conditions.append(self._regex_condition("e.change", change))
            params.append(change)

        if project:
            where_conditions.append(self._regex_condition("e.project", project))
            params.append(project)

        if committer:
            where_conditions.append(
                self._regex_condition("e.committer_name", committer)
            )
            params.append(committer)

        if planner:
            where_conditions.append(self._regex_condition("e.planner_name", planner))
            params.append(planner)

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        # Build LIMIT/OFFSET clause
        limit_clause = ""
        if limit is not None:
            limit_clause = f"LIMIT {limit}"
            if offset is not None:
                limit_clause += f" OFFSET {offset}"
        elif offset is not None:
            # Some databases don't support OFFSET without LIMIT
            limit_clause = f"LIMIT 999999999 OFFSET {offset}"

        query = f"""
            SELECT e.event, e.project, e.change_id, e.change, e.note,
                   e.requires, e.conflicts, e.tags,
                   e.committer_name, e.committer_email, e.committed_at,
                   e.planner_name, e.planner_email, e.planned_at
            FROM {self.registry_schema.EVENTS_TABLE} e
            {where_clause}
            ORDER BY e.committed_at {direction}
            {limit_clause}
        """

        with self.connection() as conn:
            try:
                if params:
                    conn.execute(query, params)
                else:
                    conn.execute(query)

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

    @abstractmethod
    def _regex_condition(self, column: str, pattern: str) -> str:
        """
        Get database-specific regex condition.

        Args:
            column: Column name
            pattern: Regular expression pattern

        Returns:
            SQL condition string
        """
        ...

    def _parse_array_field(self, value: str) -> List[str]:
        """
        Parse space-delimited array field.

        Args:
            value: Space-delimited string

        Returns:
            List of strings
        """
        if not value or not value.strip():
            return []
        return value.strip().split()

    def get_current_tags(
        self, project: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Get iterator over all deployed tags.

        Args:
            project: Project name (defaults to plan project)

        Yields:
            Dictionary with tag information

        Raises:
            EngineError: If query fails
        """
        self.ensure_registry()

        project_name = project or self.plan.project_name

        with self.connection() as conn:
            try:
                conn.execute(
                    f"""
                    SELECT tag_id, tag, committer_name, committer_email, committed_at,
                           planner_name, planner_email, planned_at
                    FROM {self.registry_schema.TAGS_TABLE}
                    WHERE project = ?
                    ORDER BY committed_at DESC
                    """,
                    {"project": project_name},
                )

                while True:
                    row = conn.fetchone()
                    if not row:
                        break
                    yield {
                        "tag_id": row["tag_id"],
                        "tag": row["tag"],
                        "committer_name": row["committer_name"],
                        "committer_email": row["committer_email"],
                        "committed_at": row["committed_at"],
                        "planner_name": row["planner_name"],
                        "planner_email": row["planner_email"],
                        "planned_at": row["planned_at"],
                    }

            except Exception as e:
                raise EngineError(
                    f"Failed to get current tags: {e}", engine_name=self.engine_type
                ) from e

    def deploy_change(self, change: Change) -> None:
        """
        Deploy a single change to the database.

        Args:
            change: Change to deploy

        Raises:
            DeploymentError: If deployment fails
        """
        self.ensure_registry()

        self.logger.info(f"Deploying {change.name}")

        with self.transaction() as conn:
            try:
                # Execute deploy script
                deploy_file = self.plan.get_deploy_file(change)
                if deploy_file.exists():
                    self._execute_sql_file(conn, deploy_file)

                # Record deployment in registry
                self._record_change_deployment(conn, change)

                self.logger.info(f"Successfully deployed {change.name}")

            except Exception as e:
                raise DeploymentError(
                    f"Failed to deploy change: {e}",
                    change_name=change.name,
                    operation="deploy",
                    engine_name=self.engine_type,
                ) from e

    def revert_change(self, change: Change) -> None:
        """
        Revert a single change from the database.

        Args:
            change: Change to revert

        Raises:
            DeploymentError: If revert fails
        """
        self.ensure_registry()

        self.logger.info(f"Reverting {change.name}")

        with self.transaction() as conn:
            try:
                # Execute revert script
                revert_file = self.plan.get_revert_file(change)
                if revert_file.exists():
                    self._execute_sql_file(conn, revert_file)

                # Remove from registry
                self._record_change_revert(conn, change)

                self.logger.info(f"Successfully reverted {change.name}")

            except Exception as e:
                raise DeploymentError(
                    f"Failed to revert change: {e}",
                    change_name=change.name,
                    operation="revert",
                    engine_name=self.engine_type,
                ) from e

    def verify_change(self, change: Change) -> bool:
        """
        Verify a single change in the database.

        Args:
            change: Change to verify

        Returns:
            True if verification passes, False otherwise

        Raises:
            DeploymentError: If verification fails with error
        """
        self.logger.info(f"Verifying {change.name}")

        try:
            with self.connection() as conn:
                verify_file = self.plan.get_verify_file(change)
                if verify_file.exists():
                    self._execute_sql_file(conn, verify_file)

                self.logger.info(f"Successfully verified {change.name}")
                return True

        except Exception as e:
            self.logger.error(f"Verification failed for {change.name}: {e}")
            return False

    def _record_change_deployment(self, connection: Connection, change: Change) -> None:
        """
        Record change deployment in registry.

        Args:
            connection: Database connection
            change: Deployed change
        """
        now = datetime.now(timezone.utc)

        # Insert change record
        connection.execute(
            f"""
            INSERT INTO {self.registry_schema.CHANGES_TABLE}
            (change_id, script_hash, change, project, note, committed_at, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            {
                "change_id": change.id,
                "script_hash": self._calculate_script_hash(change),
                "change": change.name,
                "project": self.plan.project_name,
                "note": change.note or "",
                "committed_at": now,
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
                INSERT INTO {self.registry_schema.DEPENDENCIES_TABLE}
                (change_id, type, dependency, dependency_id)
                VALUES (?, ?, ?, ?)
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
            INSERT INTO {self.registry_schema.EVENTS_TABLE}
            (event, change_id, change, project, note, requires, conflicts, tags, committed_at, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                "committed_at": now,
                "committer_name": change.planner_name,
                "committer_email": change.planner_email,
                "planned_at": change.timestamp,
                "planner_name": change.planner_name,
                "planner_email": change.planner_email,
            },
        )

    def _record_change_revert(self, connection: Connection, change: Change) -> None:
        """
        Record change revert in registry.

        Args:
            connection: Database connection
            change: Reverted change
        """
        now = datetime.now(timezone.utc)

        # Remove change record
        connection.execute(
            f"DELETE FROM {self.registry_schema.CHANGES_TABLE} WHERE change_id = ?",
            {"change_id": change.id},
        )

        # Remove dependencies
        connection.execute(
            f"DELETE FROM {self.registry_schema.DEPENDENCIES_TABLE} WHERE change_id = ?",
            {"change_id": change.id},
        )

        # Insert revert event
        connection.execute(
            f"""
            INSERT INTO {self.registry_schema.EVENTS_TABLE}
            (event, change_id, change, project, note, requires, conflicts, tags, committed_at, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                "committed_at": now,
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


class EngineRegistry:
    """Registry for database engine classes."""

    _engines: Dict[EngineType, type] = {}

    @classmethod
    def register(cls, engine_type: EngineType, engine_class: type) -> None:
        """
        Register an engine class.

        Args:
            engine_type: Engine type identifier
            engine_class: Engine class to register
        """
        cls._engines[engine_type] = engine_class

    @classmethod
    def get_engine_class(cls, engine_type: EngineType) -> type:
        """
        Get engine class for type.

        Args:
            engine_type: Engine type identifier

        Returns:
            Engine class

        Raises:
            EngineError: If engine type not supported
        """
        if engine_type not in cls._engines:
            raise EngineError(f"Unsupported engine type: {engine_type}")
        return cls._engines[engine_type]

    @classmethod
    def create_engine(cls, target: Target, plan: Plan) -> Engine:
        """
        Create engine instance for target.

        Args:
            target: Target configuration
            plan: Plan to manage

        Returns:
            Engine instance

        Raises:
            EngineError: If engine cannot be created
        """
        engine_class = cls.get_engine_class(target.engine_type)
        return engine_class(target, plan)

    @classmethod
    def list_supported_engines(cls) -> List[EngineType]:
        """
        Get list of supported engine types.

        Returns:
            List of supported engine types
        """
        return list(cls._engines.keys())

    def planned_deployed_common_ancestor_id(self) -> Optional[str]:
        """
        Get the ID of the common ancestor between planned and deployed changes.

        This method compares the SHA1 hashes of the deploy scripts to their values
        at the time of deployment to find the last change that hasn't diverged.

        Returns:
            Change ID of common ancestor, or None if no common ancestor
        """
        try:
            deployed_changes = self._load_deployed_changes()
            if not deployed_changes:
                return None

            divergent_idx = self._find_planned_deployed_divergence_idx(
                0, deployed_changes
            )

            if divergent_idx == -1:
                # No divergence found, return last deployed change
                return deployed_changes[-1].id if deployed_changes else None
            elif divergent_idx == 0:
                # Divergence at first change
                return None
            else:
                # Return change before divergence
                return deployed_changes[divergent_idx - 1].id

        except Exception as e:
            logger.error(f"Error finding common ancestor: {e}")
            return None

    def _load_deployed_changes(self) -> List[Change]:
        """
        Load deployed changes from the database.

        Returns:
            List of deployed Change objects
        """
        try:
            with self.connection() as conn:
                # Get deployed changes in order
                sql = f"""
                    SELECT change_id, change, note, committed_at,
                           committer_name, committer_email,
                           planned_at, planner_name, planner_email
                    FROM {self.registry_schema.CHANGES_TABLE}
                    WHERE project = %s
                    ORDER BY committed_at
                """

                result = conn.execute(sql, {"project": self.plan.project})
                rows = result.fetchall() if hasattr(result, "fetchall") else []

                changes = []
                for row in rows:
                    # Create Change object from database row
                    change = Change(
                        name=row["change"],
                        note=row.get("note", ""),
                        tags=[],  # Tags would need separate query
                        dependencies=[],  # Dependencies would need separate query
                        conflicts=[],
                        timestamp=row["planned_at"],
                        planner_name=row["planner_name"],
                        planner_email=row["planner_email"],
                    )
                    # Set the ID from database
                    change._id = row["change_id"]
                    changes.append(change)

                return changes

        except Exception as e:
            logger.error(f"Error loading deployed changes: {e}")
            return []

    def _find_planned_deployed_divergence_idx(
        self, from_idx: int, deployed_changes: List[Change]
    ) -> int:
        """
        Find the index where planned and deployed changes diverge.

        Args:
            from_idx: Starting index to check from
            deployed_changes: List of deployed changes

        Returns:
            Index of first divergent change, or -1 if no divergence
        """
        try:
            plan = self.plan

            for i, deployed_change in enumerate(deployed_changes):
                plan_idx = i + from_idx

                # Check if we've exceeded the plan
                if plan_idx >= len(plan.changes):
                    return i

                planned_change = plan.changes[plan_idx]

                # Compare script hashes
                deployed_hash = self._get_deployed_script_hash(deployed_change)
                planned_hash = self._calculate_script_hash(planned_change)

                if deployed_hash != planned_hash:
                    return i

            return -1  # No divergence found

        except Exception as e:
            logger.error(f"Error finding divergence: {e}")
            return 0  # Assume divergence at start on error

    def _get_deployed_script_hash(self, change: Change) -> str:
        """
        Get the script hash that was stored when the change was deployed.

        Args:
            change: Deployed change

        Returns:
            Script hash from deployment time
        """
        # For now, recalculate the hash
        # In a full implementation, this would be stored in the database
        return self._calculate_script_hash(change)

    def revert(  # noqa: C901
        self,
        to_change: Optional[str] = None,
        prompt: bool = True,
        prompt_accept: bool = True,
    ) -> None:
        """
        Revert changes to a specific change.

        Args:
            to_change: Change to revert to (None for all changes)
            prompt: Whether to prompt for confirmation
            prompt_accept: Default response for prompts
        """
        try:
            deployed_changes = self.get_deployed_changes()
            if not deployed_changes:
                logger.info("No changes to revert")
                return

            # Find changes to revert
            changes_to_revert = []
            if to_change:
                # Find the target change and revert everything after it
                target_found = False
                for change_id in reversed(deployed_changes):
                    if change_id == to_change:
                        target_found = True
                        break
                    changes_to_revert.append(change_id)

                if not target_found:
                    raise EngineError(
                        f"Change not found in deployed changes: {to_change}"
                    )
            else:
                # Revert all changes
                changes_to_revert = list(reversed(deployed_changes))

            if not changes_to_revert:
                logger.info("No changes to revert")
                return

            # Prompt for confirmation if needed
            if prompt and changes_to_revert:
                # This would need to be implemented to actually prompt the user
                # For now, assume confirmation based on prompt_accept
                if not prompt_accept:
                    logger.info("Revert cancelled by user")
                    return

            # Revert changes in reverse order
            for change_id in changes_to_revert:
                change = self.plan.get_change_by_id(change_id)
                if change:
                    self.revert_change(change)
                    logger.info(f"Reverted change: {change.name}")
                else:
                    logger.warning(f"Change not found in plan: {change_id}")

        except Exception as e:
            raise EngineError(f"Revert operation failed: {e}") from e

    def deploy(self, to_change: Optional[str] = None, mode: str = "all") -> None:  # noqa: C901
        """
        Deploy changes up to a specific change.

        Args:
            to_change: Change to deploy up to (None for all changes)
            mode: Deployment mode ('all', 'change', 'tag')
        """
        try:
            deployed_changes = set(self.get_deployed_changes())

            # Find changes to deploy
            changes_to_deploy = []

            if to_change:
                # Find target change in plan
                target_change = self.plan.get_change(to_change)
                if not target_change:
                    # Try to find by ID
                    target_change = self.plan.get_change_by_id(to_change)

                if not target_change:
                    raise EngineError(f"Change not found in plan: {to_change}")

                # Find all changes up to target
                for change in self.plan.changes:
                    if change.id not in deployed_changes:
                        changes_to_deploy.append(change)
                    if change.id == target_change.id:
                        break
            else:
                # Deploy all undeployed changes
                for change in self.plan.changes:
                    if change.id not in deployed_changes:
                        changes_to_deploy.append(change)

            if not changes_to_deploy:
                logger.info("No changes to deploy")
                return

            # Deploy changes in order
            for change in changes_to_deploy:
                self.deploy_change(change)
                logger.info(f"Deployed change: {change.name}")

                # Handle mode-specific stopping conditions
                if mode == "change" and change.id == to_change:
                    break
                elif mode == "tag" and change.tags and to_change in change.tags:
                    break

        except Exception as e:
            raise EngineError(f"Deploy operation failed: {e}") from e

    def set_verify(self, verify: bool) -> None:
        """Set verification mode."""
        self._verify = verify

    def set_log_only(self, log_only: bool) -> None:
        """Set log-only mode."""
        self._log_only = log_only

    def set_lock_timeout(self, timeout: int) -> None:
        """Set lock timeout."""
        self._lock_timeout = timeout

    def set_variables(self, variables: Dict[str, Any]) -> None:
        """Set template variables."""
        self._variables = variables


def register_engine(engine_type: EngineType):
    """
    Decorator to register engine classes.

    Args:
        engine_type: Engine type identifier

    Returns:
        Decorator function
    """

    def decorator(engine_class: type) -> type:
        EngineRegistry.register(engine_type, engine_class)
        return engine_class

    return decorator
