"""
MySQL database engine implementation.

This module provides the MySQL-specific implementation of the Engine
base class, handling MySQL connections, registry management, and
SQL execution with proper error handling and transaction management.
"""

import logging
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterator, Union
from urllib.parse import urlparse, parse_qs

from ..core.exceptions import (
    EngineError, ConnectionError, DeploymentError
)
from ..core.types import EngineType, Target, sanitize_connection_string
from ..core.change import Change
from ..core.plan import Plan
from .base import Engine, RegistrySchema, register_engine

# Try to import PyMySQL
try:
    import pymysql
    import pymysql.cursors
    from pymysql import Error as MySQLError
except ImportError:
    pymysql = None
    MySQLError = None


logger = logging.getLogger(__name__)


class MySQLRegistrySchema(RegistrySchema):
    """MySQL-specific registry schema."""
    
    @classmethod
    def get_create_statements(cls, engine_type: EngineType) -> List[str]:
        """
        Get MySQL-specific SQL statements to create registry tables.
        
        Args:
            engine_type: Database engine type (should be 'mysql')
            
        Returns:
            List of SQL CREATE statements for MySQL
        """
        return [
            # Set SQL mode for ANSI compliance
            "SET SESSION sql_mode = 'ansi,strict_trans_tables,no_auto_value_on_zero,no_zero_date,no_zero_in_date,only_full_group_by,error_for_division_by_zero'",
            
            # Releases table
            f"""
            CREATE TABLE IF NOT EXISTS {cls.RELEASES_TABLE} (
                version         FLOAT(4, 1)   PRIMARY KEY
                                COMMENT 'Version of the Sqitch registry.',
                installed_at    DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                COMMENT 'Date the registry release was installed.',
                installer_name  VARCHAR(255)  NOT NULL
                                COMMENT 'Name of the user who installed the registry release.',
                installer_email VARCHAR(255)  NOT NULL
                                COMMENT 'Email address of the user who installed the registry release.'
            ) ENGINE=InnoDB, CHARACTER SET utf8mb4, COMMENT='Sqitch registry releases.'
            """,
            
            # Projects table
            f"""
            CREATE TABLE IF NOT EXISTS {cls.PROJECTS_TABLE} (
                project         VARCHAR(255) PRIMARY KEY
                                COMMENT 'Unique Name of a project.',
                uri             VARCHAR(255) NULL UNIQUE
                                COMMENT 'Optional project URI',
                created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                COMMENT 'Date the project was added to the database.',
                creator_name    VARCHAR(255) NOT NULL
                                COMMENT 'Name of the user who added the project.',
                creator_email   VARCHAR(255) NOT NULL
                                COMMENT 'Email address of the user who added the project.'
            ) ENGINE=InnoDB, CHARACTER SET utf8mb4, COMMENT='Sqitch projects deployed to this database.'
            """,
            
            # Changes table
            f"""
            CREATE TABLE IF NOT EXISTS {cls.CHANGES_TABLE} (
                change_id       VARCHAR(40)  PRIMARY KEY
                                COMMENT 'Change primary key.',
                script_hash     VARCHAR(40)      NULL
                                COMMENT 'Deploy script SHA-1 hash.',
                `change`        VARCHAR(255) NOT NULL
                                COMMENT 'Name of a deployed change.',
                project         VARCHAR(255) NOT NULL
                                COMMENT 'Name of the Sqitch project to which the change belongs.',
                note            TEXT         NOT NULL DEFAULT ''
                                COMMENT 'Description of the change.',
                committed_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                COMMENT 'Date the change was deployed.',
                committer_name  VARCHAR(255) NOT NULL
                                COMMENT 'Name of the user who deployed the change.',
                committer_email VARCHAR(255) NOT NULL
                                COMMENT 'Email address of the user who deployed the change.',
                planned_at      DATETIME(6)  NOT NULL
                                COMMENT 'Date the change was added to the plan.',
                planner_name    VARCHAR(255) NOT NULL
                                COMMENT 'Name of the user who planed the change.',
                planner_email   VARCHAR(255) NOT NULL
                                COMMENT 'Email address of the user who planned the change.',
                UNIQUE KEY unique_project_script (project, script_hash),
                FOREIGN KEY (project) REFERENCES {cls.PROJECTS_TABLE}(project) ON UPDATE CASCADE
            ) ENGINE=InnoDB, CHARACTER SET utf8mb4, COMMENT='Tracks the changes currently deployed to the database.'
            """,
            
            # Tags table
            f"""
            CREATE TABLE IF NOT EXISTS {cls.TAGS_TABLE} (
                tag_id          VARCHAR(40)  PRIMARY KEY
                                COMMENT 'Tag primary key.',
                tag             VARCHAR(255) NOT NULL
                                COMMENT 'Project-unique tag name.',
                project         VARCHAR(255) NOT NULL
                                COMMENT 'Name of the Sqitch project to which the tag belongs.',
                change_id       VARCHAR(40)  NOT NULL
                                COMMENT 'ID of last change deployed before the tag was applied.',
                note            VARCHAR(255) NOT NULL DEFAULT ''
                                COMMENT 'Description of the tag.',
                committed_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                COMMENT 'Date the tag was applied to the database.',
                committer_name  VARCHAR(255) NOT NULL
                                COMMENT 'Name of the user who applied the tag.',
                committer_email VARCHAR(255) NOT NULL
                                COMMENT 'Email address of the user who applied the tag.',
                planned_at      DATETIME(6)  NOT NULL
                                COMMENT 'Date the tag was added to the plan.',
                planner_name    VARCHAR(255) NOT NULL
                                COMMENT 'Name of the user who planed the tag.',
                planner_email   VARCHAR(255) NOT NULL
                                COMMENT 'Email address of the user who planned the tag.',
                UNIQUE KEY unique_project_tag (project, tag),
                FOREIGN KEY (project) REFERENCES {cls.PROJECTS_TABLE}(project) ON UPDATE CASCADE,
                FOREIGN KEY (change_id) REFERENCES {cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE
            ) ENGINE=InnoDB, CHARACTER SET utf8mb4, COMMENT='Tracks the tags currently applied to the database.'
            """,
            
            # Dependencies table
            f"""
            CREATE TABLE IF NOT EXISTS {cls.DEPENDENCIES_TABLE} (
                change_id       VARCHAR(40)  NOT NULL
                                COMMENT 'ID of the depending change.',
                type            VARCHAR(8)   NOT NULL
                                COMMENT 'Type of dependency.',
                dependency      VARCHAR(255) NOT NULL
                                COMMENT 'Dependency name.',
                dependency_id   VARCHAR(40)      NULL
                                COMMENT 'Change ID the dependency resolves to.',
                PRIMARY KEY (change_id, dependency),
                FOREIGN KEY (change_id) REFERENCES {cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE ON DELETE CASCADE,
                FOREIGN KEY (dependency_id) REFERENCES {cls.CHANGES_TABLE}(change_id) ON UPDATE CASCADE
            ) ENGINE=InnoDB, CHARACTER SET utf8mb4, COMMENT='Tracks the currently satisfied dependencies.'
            """,
            
            # Events table
            f"""
            CREATE TABLE IF NOT EXISTS {cls.EVENTS_TABLE} (
                event           ENUM('deploy', 'fail', 'merge', 'revert') NOT NULL
                                COMMENT 'Type of event.',
                change_id       VARCHAR(40)  NOT NULL
                                COMMENT 'Change ID.',
                `change`        VARCHAR(255) NOT NULL
                                COMMENT 'Change name.',
                project         VARCHAR(255) NOT NULL
                                COMMENT 'Name of the Sqitch project to which the change belongs.',
                note            TEXT         NOT NULL DEFAULT ''
                                COMMENT 'Description of the change.',
                requires        TEXT         NOT NULL DEFAULT ''
                                COMMENT 'List of the names of required changes.',
                conflicts       TEXT         NOT NULL DEFAULT ''
                                COMMENT 'List of the names of conflicting changes.',
                tags            TEXT         NOT NULL DEFAULT ''
                                COMMENT 'List of tags associated with the change.',
                committed_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                COMMENT 'Date the event was committed.',
                committer_name  VARCHAR(255) NOT NULL
                                COMMENT 'Name of the user who committed the event.',
                committer_email VARCHAR(255) NOT NULL
                                COMMENT 'Email address of the user who committed the event.',
                planned_at      DATETIME(6)  NOT NULL
                                COMMENT 'Date the event was added to the plan.',
                planner_name    VARCHAR(255) NOT NULL
                                COMMENT 'Name of the user who planed the change.',
                planner_email   VARCHAR(255) NOT NULL
                                COMMENT 'Email address of the user who plan planned the change.',
                PRIMARY KEY (change_id, committed_at),
                FOREIGN KEY (project) REFERENCES {cls.PROJECTS_TABLE}(project) ON UPDATE CASCADE
            ) ENGINE=InnoDB, CHARACTER SET utf8mb4, COMMENT='Contains full history of all deployment events.'
            """,
            
            # Insert registry version
            f"""
            INSERT INTO {cls.RELEASES_TABLE} (version, installer_name, installer_email)
            VALUES ({cls.REGISTRY_VERSION}, 'sqlitch', 'sqlitch@example.com')
            ON DUPLICATE KEY UPDATE version = VALUES(version)
            """
        ]


class MySQLConnection:
    """Wrapper for MySQL connection with sqitch-specific functionality."""
    
    def __init__(self, connection: 'pymysql.Connection'):
        """
        Initialize MySQL connection wrapper.
        
        Args:
            connection: PyMySQL connection object
        """
        self._connection = connection
        self._cursor: Optional['pymysql.cursors.Cursor'] = None
    
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
        except MySQLError as e:
            raise DeploymentError(
                f"SQL execution failed: {e}",
                engine_name="mysql",
                sql_state=getattr(e, 'args', [None, None])[1] if hasattr(e, 'args') and len(e.args) > 1 else None
            ) from e
    
    def fetchone(self) -> Optional[Dict[str, Any]]:
        """
        Fetch one row from result set.
        
        Returns:
            Dictionary representing the row or None
        """
        cursor = self._get_cursor()
        row = cursor.fetchone()
        return row if row else None
    
    def fetchall(self) -> List[Dict[str, Any]]:
        """
        Fetch all rows from result set.
        
        Returns:
            List of dictionaries representing rows
        """
        cursor = self._get_cursor()
        rows = cursor.fetchall()
        return list(rows) if rows else []
    
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
    
    def _get_cursor(self) -> 'pymysql.cursors.Cursor':
        """Get or create cursor with dict row factory."""
        if not self._cursor:
            self._cursor = self._connection.cursor(pymysql.cursors.DictCursor)
        return self._cursor


@register_engine('mysql')
class MySQLEngine(Engine):
    """
    MySQL database engine implementation.
    
    Provides MySQL-specific functionality for sqitch operations
    including connection management, registry operations, and SQL execution.
    """
    
    def __init__(self, target: Target, plan: Plan) -> None:
        """
        Initialize MySQL engine.
        
        Args:
            target: Target configuration for MySQL database
            plan: Plan containing changes to manage
            
        Raises:
            EngineError: If PyMySQL is not available
        """
        if pymysql is None:
            raise EngineError(
                "PyMySQL is required for MySQL support. "
                "Install with: pip install PyMySQL",
                engine_name="mysql"
            )
        
        super().__init__(target, plan)
        self._connection_params = self._parse_connection_string()
        self._registry_db_name = target.registry or self._connection_params.get('database', 'sqitch')
    
    @property
    def engine_type(self) -> EngineType:
        """Get the engine type identifier."""
        return 'mysql'
    
    @property
    def registry_schema(self) -> RegistrySchema:
        """Get the registry schema for MySQL."""
        return MySQLRegistrySchema()
    
    def _parse_connection_string(self) -> Dict[str, Any]:
        """
        Parse MySQL connection string from target URI.
        
        Returns:
            Dictionary of connection parameters
            
        Raises:
            ConnectionError: If URI format is invalid
        """
        uri_str = str(self.target.uri)
        
        # Handle sqitch-style URIs: db:mysql://user:pass@host:port/dbname
        if uri_str.startswith('db:mysql:'):
            uri_str = uri_str[3:]  # Remove 'db:' prefix
        elif uri_str.startswith('mysql:'):
            uri_str = uri_str[6:]  # Remove 'mysql:' prefix
            if not uri_str.startswith('//'):
                uri_str = '//' + uri_str
            uri_str = 'mysql:' + uri_str
        
        # Add scheme if missing
        if not uri_str.startswith('mysql://'):
            if '://' not in uri_str:
                uri_str = 'mysql://' + uri_str
        
        try:
            parsed = urlparse(uri_str)
            
            params = {
                'host': parsed.hostname or 'localhost',
                'port': parsed.port or 3306,
                'database': parsed.path.lstrip('/') if parsed.path else None,
                'user': parsed.username,
                'password': parsed.password,
                'charset': 'utf8mb4',
                'autocommit': False,
                'cursorclass': pymysql.cursors.DictCursor
            }
            
            # Handle query parameters
            if parsed.query:
                query_params = parse_qs(parsed.query)
                for key, values in query_params.items():
                    if values:
                        # Map common MySQL connection parameters
                        if key == 'charset':
                            params['charset'] = values[0]
                        elif key == 'ssl':
                            params['ssl'] = {'ssl': True} if values[0].lower() in ('1', 'true', 'yes') else None
                        elif key == 'ssl_ca':
                            if 'ssl' not in params:
                                params['ssl'] = {}
                            params['ssl']['ca'] = values[0]
                        elif key == 'ssl_cert':
                            if 'ssl' not in params:
                                params['ssl'] = {}
                            params['ssl']['cert'] = values[0]
                        elif key == 'ssl_key':
                            if 'ssl' not in params:
                                params['ssl'] = {}
                            params['ssl']['key'] = values[0]
                        elif key == 'connect_timeout':
                            params['connect_timeout'] = int(values[0])
            
            # Remove None values
            params = {k: v for k, v in params.items() if v is not None}
            
            return params
            
        except Exception as e:
            raise ConnectionError(
                f"Invalid MySQL connection string: {e}",
                connection_string=sanitize_connection_string(uri_str),
                engine_name="mysql"
            ) from e
    
    def _create_connection(self) -> MySQLConnection:
        """
        Create a new MySQL connection.
        
        Returns:
            MySQL connection wrapper
            
        Raises:
            ConnectionError: If connection cannot be established
        """
        try:
            self.logger.debug(f"Connecting to MySQL: {sanitize_connection_string(str(self.target.uri))}")
            
            # Create connection
            conn = pymysql.connect(**self._connection_params)
            
            # Set MySQL session variables for sqitch compatibility
            with conn.cursor() as cursor:
                cursor.execute("SET SESSION character_set_client = 'utf8mb4'")
                cursor.execute("SET SESSION character_set_server = 'utf8mb4'")
                cursor.execute("SET SESSION time_zone = '+00:00'")
                cursor.execute("SET SESSION group_concat_max_len = 32768")
                cursor.execute("""SET SESSION sql_mode = 'ansi,strict_trans_tables,no_auto_value_on_zero,no_zero_date,no_zero_in_date,only_full_group_by,error_for_division_by_zero'""")
                
                # Check MySQL version compatibility
                cursor.execute("SELECT VERSION()")
                version_info = cursor.fetchone()['VERSION()']
                
                # Parse version to check minimum requirements
                if 'mariadb' in version_info.lower():
                    # MariaDB 5.3.0 or higher required
                    version_match = re.search(r'(\d+)\.(\d+)\.(\d+)', version_info)
                    if version_match:
                        major, minor, patch = map(int, version_match.groups())
                        if (major, minor) < (5, 3):
                            raise EngineError(
                                f"Sqitch requires MariaDB 5.3.0 or higher; this is {version_info}",
                                engine_name="mysql"
                            )
                else:
                    # MySQL 5.1.0 or higher required
                    version_match = re.search(r'(\d+)\.(\d+)\.(\d+)', version_info)
                    if version_match:
                        major, minor, patch = map(int, version_match.groups())
                        if (major, minor) < (5, 1):
                            raise EngineError(
                                f"Sqitch requires MySQL 5.1.0 or higher; this is {version_info}",
                                engine_name="mysql"
                            )
            
            return MySQLConnection(conn)
            
        except MySQLError as e:
            raise ConnectionError(
                f"Failed to connect to MySQL database: {e}",
                connection_string=sanitize_connection_string(str(self.target.uri)),
                engine_name="mysql"
            ) from e
    
    def _execute_sql_file(self, connection: MySQLConnection, sql_file: Path, 
                         variables: Optional[Dict[str, Any]] = None) -> None:
        """
        Execute SQL file with optional variable substitution.
        
        Args:
            connection: MySQL connection
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
                    engine_name="mysql"
                )
            
            self.logger.debug(f"Executing SQL file: {sql_file}")
            
            # Read SQL content
            sql_content = sql_file.read_text(encoding='utf-8')
            
            # Perform variable substitution if provided
            if variables:
                for var_name, var_value in variables.items():
                    placeholder = f":{var_name}"
                    sql_content = sql_content.replace(placeholder, str(var_value))
            
            # Split into individual statements and execute
            statements = self._split_sql_statements(sql_content)
            
            for statement in statements:
                statement = statement.strip()
                if statement and not statement.startswith('--') and not statement.startswith('#'):
                    connection.execute(statement)
            
        except Exception as e:
            if isinstance(e, DeploymentError):
                raise
            raise DeploymentError(
                f"Failed to execute SQL file {sql_file}: {e}",
                sql_file=str(sql_file),
                engine_name="mysql"
            ) from e
    
    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """
        Split SQL content into individual statements.
        
        Args:
            sql_content: SQL content to split
            
        Returns:
            List of SQL statements
        """
        statements = []
        current_statement = []
        in_delimiter_block = False
        custom_delimiter = ';'
        
        for line in sql_content.split('\n'):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('--') or line.startswith('#'):
                continue
            
            # Handle DELIMITER statements (MySQL-specific)
            if line.upper().startswith('DELIMITER'):
                delimiter_match = re.match(r'DELIMITER\s+(.+)', line, re.IGNORECASE)
                if delimiter_match:
                    custom_delimiter = delimiter_match.group(1).strip()
                    in_delimiter_block = custom_delimiter != ';'
                continue
            
            current_statement.append(line)
            
            # Check if line ends with current delimiter
            if line.rstrip().endswith(custom_delimiter):
                # Remove the delimiter from the statement
                if current_statement:
                    last_line = current_statement[-1]
                    current_statement[-1] = last_line[:-len(custom_delimiter)].rstrip()
                
                statement_text = '\n'.join(current_statement).strip()
                if statement_text:
                    statements.append(statement_text)
                current_statement = []
                
                # Reset delimiter if we're ending a delimiter block
                if in_delimiter_block and custom_delimiter != ';':
                    custom_delimiter = ';'
                    in_delimiter_block = False
        
        # Add any remaining statement
        if current_statement:
            statement_text = '\n'.join(current_statement).strip()
            if statement_text:
                statements.append(statement_text)
        
        return statements
    
    def _get_registry_version(self, connection: MySQLConnection) -> Optional[str]:
        """
        Get current registry version from database.
        
        Args:
            connection: MySQL connection
            
        Returns:
            Registry version string or None if not found
        """
        try:
            # First check if we're in the registry database
            connection.execute(f"USE `{self._registry_db_name}`")
            
            connection.execute(
                f"SELECT CAST(ROUND(MAX(version), 1) AS CHAR) as version FROM {self.registry_schema.RELEASES_TABLE}"
            )
            row = connection.fetchone()
            return str(row['version']) if row and row['version'] else None
        except Exception:
            return None
    
    def _registry_exists_in_db(self, connection: MySQLConnection) -> bool:
        """
        Check if registry tables exist in database.
        
        Args:
            connection: MySQL connection
            
        Returns:
            True if registry exists, False otherwise
        """
        try:
            # Check if registry database exists
            connection.execute("SHOW DATABASES LIKE %s", (self._registry_db_name,))
            if not connection.fetchone():
                return False
            
            # Switch to registry database
            connection.execute(f"USE `{self._registry_db_name}`")
            
            # Check if projects table exists
            connection.execute(
                "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (self._registry_db_name, self.registry_schema.PROJECTS_TABLE)
            )
            result = connection.fetchone()
            return result and result['count'] > 0
        except Exception:
            return False
    
    def _create_registry(self, connection: MySQLConnection) -> None:
        """
        Create registry tables in MySQL database.
        
        Args:
            connection: MySQL connection
            
        Raises:
            EngineError: If registry creation fails
        """
        try:
            # Create registry database if it doesn't exist
            connection.execute(f"CREATE DATABASE IF NOT EXISTS `{self._registry_db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            
            # Switch to registry database
            connection.execute(f"USE `{self._registry_db_name}`")
            
            statements = self.registry_schema.get_create_statements(self.engine_type)
            
            for statement in statements:
                connection.execute(statement)
            
            # Insert initial project record
            self._insert_project_record(connection)
            
        except Exception as e:
            raise EngineError(
                f"Failed to create MySQL registry: {e}",
                engine_name="mysql"
            ) from e
    
    def _insert_project_record(self, connection: MySQLConnection) -> None:
        """
        Insert project record into registry.
        
        Args:
            connection: MySQL connection
        """
        project_name = self.plan.project_name
        project_uri = str(self.target.uri)
        creator_name = self.plan.creator_name or "Unknown"
        creator_email = self.plan.creator_email or "unknown@example.com"
        
        # Check if project already exists
        connection.execute(
            f"SELECT 1 FROM {self.registry_schema.PROJECTS_TABLE} WHERE project = %s",
            (project_name,)
        )
        
        if not connection.fetchone():
            connection.execute(
                f"""
                INSERT INTO {self.registry_schema.PROJECTS_TABLE} 
                (project, uri, creator_name, creator_email)
                VALUES (%s, %s, %s, %s)
                """,
                (project_name, project_uri, creator_name, creator_email)
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
                # Switch to registry database
                conn.execute(f"USE `{self._registry_db_name}`")
                
                conn.execute(
                    f"""
                    SELECT change_id FROM {self.registry_schema.CHANGES_TABLE}
                    WHERE project = %s
                    ORDER BY committed_at
                    """,
                    (self.plan.project_name,)
                )
                rows = conn.fetchall()
                return [row['change_id'] for row in rows]
            except Exception as e:
                raise EngineError(
                    f"Failed to get deployed changes: {e}",
                    engine_name="mysql"
                ) from e 
   
    def _record_change_deployment(self, connection: MySQLConnection, change: Change) -> None:
        """
        Record change deployment in MySQL registry.
        
        Args:
            connection: MySQL connection
            change: Deployed change
        """
        # Ensure we're in the registry database
        connection.execute(f"USE `{self._registry_db_name}`")
        
        # Insert change record
        connection.execute(
            f"""
            INSERT INTO {self.registry_schema.CHANGES_TABLE}
            (change_id, script_hash, `change`, project, note, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                change.id,
                self._calculate_script_hash(change),
                change.name,
                self.plan.project_name,
                change.note or '',
                change.planner_name,
                change.planner_email,
                change.timestamp,
                change.planner_name,
                change.planner_email
            )
        )
        
        # Insert dependencies
        for dep in change.dependencies:
            connection.execute(
                f"""
                INSERT INTO {self.registry_schema.DEPENDENCIES_TABLE}
                (change_id, type, dependency, dependency_id)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    change.id,
                    dep.type,
                    dep.change,
                    self._resolve_dependency_id(dep.change)
                )
            )
        
        # Insert event record
        connection.execute(
            f"""
            INSERT INTO {self.registry_schema.EVENTS_TABLE}
            (event, change_id, `change`, project, note, requires, conflicts, tags, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                'deploy',
                change.id,
                change.name,
                self.plan.project_name,
                change.note or '',
                self._format_dependencies([dep.change for dep in change.dependencies if dep.type == 'require']),
                self._format_dependencies([dep.change for dep in change.dependencies if dep.type == 'conflict']),
                self._format_tags(change.tags),
                change.planner_name,
                change.planner_email,
                change.timestamp,
                change.planner_name,
                change.planner_email
            )
        )
    
    def _record_change_revert(self, connection: MySQLConnection, change: Change) -> None:
        """
        Record change revert in MySQL registry.
        
        Args:
            connection: MySQL connection
            change: Reverted change
        """
        # Ensure we're in the registry database
        connection.execute(f"USE `{self._registry_db_name}`")
        
        # Remove change record
        connection.execute(
            f"DELETE FROM {self.registry_schema.CHANGES_TABLE} WHERE change_id = %s",
            (change.id,)
        )
        
        # Dependencies are automatically removed due to CASCADE
        
        # Insert revert event
        connection.execute(
            f"""
            INSERT INTO {self.registry_schema.EVENTS_TABLE}
            (event, change_id, `change`, project, note, requires, conflicts, tags, committer_name, committer_email, planned_at, planner_name, planner_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                'revert',
                change.id,
                change.name,
                self.plan.project_name,
                change.note or '',
                self._format_dependencies([dep.change for dep in change.dependencies if dep.type == 'require']),
                self._format_dependencies([dep.change for dep in change.dependencies if dep.type == 'conflict']),
                self._format_tags(change.tags),
                change.planner_name,
                change.planner_email,
                change.timestamp,
                change.planner_name,
                change.planner_email
            )
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
    
    @contextmanager
    def transaction(self) -> Iterator[MySQLConnection]:
        """
        Get database connection with transaction management and table locking.
        
        MySQL requires table locking for concurrent access control.
        
        Yields:
            Database connection with active transaction
            
        Raises:
            ConnectionError: If connection cannot be established
            DeploymentError: If transaction fails
        """
        with self.connection() as conn:
            try:
                # Switch to registry database for locking
                conn.execute(f"USE `{self._registry_db_name}`")
                
                # Lock all registry tables to prevent concurrent modifications
                # This matches the Perl implementation's locking strategy
                tables_to_lock = [
                    f"{self.registry_schema.RELEASES_TABLE} WRITE",
                    f"{self.registry_schema.CHANGES_TABLE} WRITE", 
                    f"{self.registry_schema.DEPENDENCIES_TABLE} WRITE",
                    f"{self.registry_schema.EVENTS_TABLE} WRITE",
                    f"{self.registry_schema.PROJECTS_TABLE} WRITE",
                    f"{self.registry_schema.TAGS_TABLE} WRITE"
                ]
                
                conn.execute(f"LOCK TABLES {', '.join(tables_to_lock)}")
                
                # Start transaction after locking
                conn.execute("START TRANSACTION")
                
                yield conn
                
                # Commit transaction
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
                    engine_name="mysql"
                ) from e
            finally:
                try:
                    # Always unlock tables
                    conn.execute("UNLOCK TABLES")
                except Exception:
                    pass  # Ignore unlock errors