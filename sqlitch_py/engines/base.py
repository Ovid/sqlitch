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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import (
    Any, Dict, List, Optional, Tuple, Iterator, Protocol, 
    ContextManager, Union, Set
)

from ..core.exceptions import (
    EngineError, ConnectionError, DeploymentError, LockError
)
from ..core.types import (
    ChangeId, ChangeStatus, Target, EngineType, OperationType,
    ValidatedURI, sanitize_connection_string
)
from ..core.change import Change
from ..core.plan import Plan


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
    def _execute_sql_file(self, connection: Connection, sql_file: Path, 
                         variables: Optional[Dict[str, Any]] = None) -> None:
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
            self.logger.debug(f"Connected to {sanitize_connection_string(str(self.target.uri))}")
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
                engine_name=self.engine_type
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
                    f"Transaction failed: {e}",
                    engine_name=self.engine_type
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
                    self.logger.info(f"Upgrading registry from {current_version} to {self.registry_schema.REGISTRY_VERSION}")
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
            connection.execute(f"SELECT COUNT(*) FROM {self.registry_schema.PROJECTS_TABLE}")
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
                f"Failed to create registry: {e}",
                engine_name=self.engine_type
            ) from e
    
    def _upgrade_registry(self, connection: Connection, from_version: Optional[str]) -> None:
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
        created_at = datetime.utcnow()
        creator_name = self.plan.creator_name or "Unknown"
        creator_email = self.plan.creator_email or "unknown@example.com"
        
        connection.execute(
            f"""
            INSERT INTO {self.registry_schema.PROJECTS_TABLE} 
            (project, uri, created_at, creator_name, creator_email)
            VALUES (?, ?, ?, ?, ?)
            """,
            {
                'project': project_name,
                'uri': project_uri,
                'created_at': created_at,
                'creator_name': creator_name,
                'creator_email': creator_email
            }
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
                    {'project': self.plan.project_name}
                )
                rows = conn.fetchall()
                return [ChangeId(row['change_id']) for row in rows]
            except Exception as e:
                raise EngineError(
                    f"Failed to get deployed changes: {e}",
                    engine_name=self.engine_type
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
                    {
                        'project': self.plan.project_name,
                        'change_id': change.id
                    }
                )
                row = conn.fetchone()
                return ChangeStatus.DEPLOYED if row else ChangeStatus.PENDING
            except Exception as e:
                raise EngineError(
                    f"Failed to get change status: {e}",
                    engine_name=self.engine_type
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
                    engine_name=self.engine_type
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
                    engine_name=self.engine_type
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
        now = datetime.utcnow()
        
        # Insert change record
        connection.execute(
            f"""
            INSERT INTO {self.registry_schema.CHANGES_TABLE}
            (change_id, script_hash, change, project, note, committed_at, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            {
                'change_id': change.id,
                'script_hash': self._calculate_script_hash(change),
                'change': change.name,
                'project': self.plan.project_name,
                'note': change.note or '',
                'committed_at': now,
                'committer_name': change.author_name,
                'committer_email': change.author_email,
                'planned_at': change.timestamp,
                'planner_name': change.author_name,
                'planner_email': change.author_email
            }
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
                    'change_id': change.id,
                    'type': dep.type.value,
                    'dependency': dep.change,
                    'dependency_id': self._resolve_dependency_id(dep.change)
                }
            )
        
        # Insert event record
        connection.execute(
            f"""
            INSERT INTO {self.registry_schema.EVENTS_TABLE}
            (event, change_id, change, project, note, requires, conflicts, tags, committed_at, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            {
                'event': 'deploy',
                'change_id': change.id,
                'change': change.name,
                'project': self.plan.project_name,
                'note': change.note or '',
                'requires': self._format_dependencies(change.requires),
                'conflicts': self._format_dependencies(change.conflicts),
                'tags': self._format_tags(change.tags),
                'committed_at': now,
                'committer_name': change.author_name,
                'committer_email': change.author_email,
                'planned_at': change.timestamp,
                'planner_name': change.author_name,
                'planner_email': change.author_email
            }
        )
    
    def _record_change_revert(self, connection: Connection, change: Change) -> None:
        """
        Record change revert in registry.
        
        Args:
            connection: Database connection
            change: Reverted change
        """
        now = datetime.utcnow()
        
        # Remove change record
        connection.execute(
            f"DELETE FROM {self.registry_schema.CHANGES_TABLE} WHERE change_id = ?",
            {'change_id': change.id}
        )
        
        # Remove dependencies
        connection.execute(
            f"DELETE FROM {self.registry_schema.DEPENDENCIES_TABLE} WHERE change_id = ?",
            {'change_id': change.id}
        )
        
        # Insert revert event
        connection.execute(
            f"""
            INSERT INTO {self.registry_schema.EVENTS_TABLE}
            (event, change_id, change, project, note, requires, conflicts, tags, committed_at, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            {
                'event': 'revert',
                'change_id': change.id,
                'change': change.name,
                'project': self.plan.project_name,
                'note': change.note or '',
                'requires': self._format_dependencies(change.requires),
                'conflicts': self._format_dependencies(change.conflicts),
                'tags': self._format_tags(change.tags),
                'committed_at': now,
                'committer_name': change.author_name,
                'committer_email': change.author_email,
                'planned_at': change.timestamp,
                'planner_name': change.author_name,
                'planner_email': change.author_email
            }
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
        return ' '.join(dependencies) if dependencies else ''
    
    def _format_tags(self, tags: List[str]) -> str:
        """
        Format tags list for storage.
        
        Args:
            tags: List of tag names
            
        Returns:
            Formatted tag string
        """
        return ' '.join(tags) if tags else ''


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