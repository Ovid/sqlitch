"""
Unit tests for MySQL database engine.

Tests the MySQL-specific implementation of the Engine base class,
including connection handling, registry management, and SQL execution.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timezone
from pathlib import Path

from sqlitch.core.exceptions import EngineError, ConnectionError, DeploymentError
from sqlitch.core.types import Target, URI
from sqlitch.core.change import Change, Dependency
from sqlitch.core.plan import Plan
from sqlitch.engines.mysql import MySQLEngine, MySQLConnection, MySQLRegistrySchema


class TestMySQLRegistrySchema:
    """Test MySQL registry schema."""
    
    def test_get_create_statements(self):
        """Test getting MySQL-specific CREATE statements."""
        statements = MySQLRegistrySchema.get_create_statements('mysql')
        
        assert len(statements) > 0
        assert any('CREATE TABLE IF NOT EXISTS releases' in stmt for stmt in statements)
        assert any('CREATE TABLE IF NOT EXISTS projects' in stmt for stmt in statements)
        assert any('CREATE TABLE IF NOT EXISTS changes' in stmt for stmt in statements)
        assert any('CREATE TABLE IF NOT EXISTS tags' in stmt for stmt in statements)
        assert any('CREATE TABLE IF NOT EXISTS dependencies' in stmt for stmt in statements)
        assert any('CREATE TABLE IF NOT EXISTS events' in stmt for stmt in statements)
        
        # Check MySQL-specific features
        assert any('ENGINE=InnoDB' in stmt for stmt in statements)
        assert any('CHARACTER SET utf8mb4' in stmt for stmt in statements)
        assert any('DATETIME(6)' in stmt for stmt in statements)  # Fractional seconds
        assert any('`change`' in stmt for stmt in statements)  # Quoted reserved word


class TestMySQLConnection:
    """Test MySQL connection wrapper."""
    
    @pytest.fixture
    def mock_pymysql_conn(self):
        """Create mock PyMySQL connection."""
        conn = Mock()
        cursor = Mock()
        cursor.fetchone.return_value = {'test': 'value'}
        cursor.fetchall.return_value = [{'test': 'value1'}, {'test': 'value2'}]
        conn.cursor.return_value = cursor
        return conn
    
    @pytest.fixture
    def mysql_connection(self, mock_pymysql_conn):
        """Create MySQL connection wrapper."""
        return MySQLConnection(mock_pymysql_conn)
    
    def test_execute_without_params(self, mysql_connection, mock_pymysql_conn):
        """Test executing SQL without parameters."""
        mysql_connection.execute("SELECT 1")
        
        cursor = mock_pymysql_conn.cursor.return_value
        cursor.execute.assert_called_once_with("SELECT 1")
    
    def test_execute_with_params(self, mysql_connection, mock_pymysql_conn):
        """Test executing SQL with parameters."""
        params = {'id': 1, 'name': 'test'}
        mysql_connection.execute("SELECT * FROM table WHERE id = %(id)s", params)
        
        cursor = mock_pymysql_conn.cursor.return_value
        cursor.execute.assert_called_once_with("SELECT * FROM table WHERE id = %(id)s", params)
    
    def test_execute_error_handling(self, mysql_connection, mock_pymysql_conn):
        """Test SQL execution error handling."""
        from sqlitch.engines.mysql import MySQLError
        
        cursor = mock_pymysql_conn.cursor.return_value
        cursor.execute.side_effect = MySQLError("SQL error")
        
        with pytest.raises(DeploymentError) as exc_info:
            mysql_connection.execute("INVALID SQL")
        
        assert "SQL execution failed" in str(exc_info.value)
        assert exc_info.value.engine_name == "mysql"
    
    def test_fetchone(self, mysql_connection, mock_pymysql_conn):
        """Test fetching one row."""
        result = mysql_connection.fetchone()
        
        cursor = mock_pymysql_conn.cursor.return_value
        cursor.fetchone.assert_called_once()
        assert result == {'test': 'value'}
    
    def test_fetchall(self, mysql_connection, mock_pymysql_conn):
        """Test fetching all rows."""
        result = mysql_connection.fetchall()
        
        cursor = mock_pymysql_conn.cursor.return_value
        cursor.fetchall.assert_called_once()
        assert result == [{'test': 'value1'}, {'test': 'value2'}]
    
    def test_commit(self, mysql_connection, mock_pymysql_conn):
        """Test committing transaction."""
        mysql_connection.commit()
        mock_pymysql_conn.commit.assert_called_once()
    
    def test_rollback(self, mysql_connection, mock_pymysql_conn):
        """Test rolling back transaction."""
        mysql_connection.rollback()
        mock_pymysql_conn.rollback.assert_called_once()
    
    def test_close(self, mysql_connection, mock_pymysql_conn):
        """Test closing connection."""
        mysql_connection.close()
        mock_pymysql_conn.close.assert_called_once()


class TestMySQLEngine:
    """Test MySQL database engine."""
    
    @pytest.fixture
    def target(self):
        """Create test target."""
        return Target(
            name="test",
            uri=URI("mysql://user:pass@localhost:3306/testdb"),
            registry="sqitch_registry"
        )
    
    @pytest.fixture
    def plan(self):
        """Create test plan."""
        plan = Mock(spec=Plan)
        plan.project_name = "test_project"
        plan.creator_name = "Test User"
        plan.creator_email = "test@example.com"
        plan.changes = []
        return plan
    
    @pytest.fixture
    def mock_pymysql(self):
        """Mock PyMySQL module."""
        with patch('sqlitch.engines.mysql.pymysql') as mock:
            mock.connect.return_value = Mock()
            mock.cursors = Mock()
            mock.cursors.DictCursor = Mock()
            yield mock
    
    @pytest.fixture
    def mysql_engine(self, target, plan, mock_pymysql):
        """Create MySQL engine instance."""
        return MySQLEngine(target, plan)
    
    def test_init_without_pymysql(self, target, plan):
        """Test initialization without PyMySQL available."""
        with patch('sqlitch.engines.mysql.pymysql', None):
            with pytest.raises(EngineError) as exc_info:
                MySQLEngine(target, plan)
            
            assert "PyMySQL is required" in str(exc_info.value)
            assert exc_info.value.engine_name == "mysql"
    
    def test_engine_type(self, mysql_engine):
        """Test engine type property."""
        assert mysql_engine.engine_type == 'mysql'
    
    def test_registry_schema(self, mysql_engine):
        """Test registry schema property."""
        schema = mysql_engine.registry_schema
        assert isinstance(schema, MySQLRegistrySchema)
    
    def test_parse_connection_string_basic(self, target, plan, mock_pymysql):
        """Test parsing basic MySQL connection string."""
        engine = MySQLEngine(target, plan)
        params = engine._connection_params
        
        assert params['host'] == 'localhost'
        assert params['port'] == 3306
        assert params['database'] == 'testdb'
        assert params['user'] == 'user'
        assert params['password'] == 'pass'
        assert params['charset'] == 'utf8mb4'
        assert params['autocommit'] is False
    
    def test_parse_connection_string_with_query_params(self, plan, mock_pymysql):
        """Test parsing connection string with query parameters."""
        target = Target(
            name="test",
            uri=URI("mysql://user:pass@localhost:3306/testdb?charset=latin1&ssl=1&connect_timeout=30"),
            registry="sqitch_registry"
        )
        
        engine = MySQLEngine(target, plan)
        params = engine._connection_params
        
        assert params['charset'] == 'latin1'
        assert params['ssl'] == {'ssl': True}
        assert params['connect_timeout'] == 30
    
    def test_parse_connection_string_sqitch_style(self, plan, mock_pymysql):
        """Test parsing sqitch-style connection string."""
        target = Target(
            name="test",
            uri=URI("db:mysql://user:pass@localhost/testdb"),
            registry="sqitch_registry"
        )
        
        engine = MySQLEngine(target, plan)
        params = engine._connection_params
        
        assert params['host'] == 'localhost'
        assert params['database'] == 'testdb'
    
    @patch('sqlitch.engines.mysql.pymysql.connect')
    def test_parse_connection_string_invalid(self, mock_connect, plan, mock_pymysql):
        """Test parsing invalid connection string."""
        target = Target(
            name="test",
            uri=URI("invalid://connection"),
            registry="sqitch_registry"
        )
        
        # Mock pymysql.connect to raise an exception
        from sqlitch.engines.mysql import MySQLError
        mock_connect.side_effect = MySQLError("Connection failed")
        
        # The error should occur during connection creation, not initialization
        engine = MySQLEngine(target, plan)
        
        with pytest.raises(ConnectionError) as exc_info:
            engine._create_connection()
        
        assert "Failed to connect to MySQL database" in str(exc_info.value)
        assert exc_info.value.engine_name == "mysql"
    
    @patch('sqlitch.engines.mysql.pymysql.connect')
    def test_create_connection_success(self, mock_connect, mysql_engine):
        """Test successful connection creation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {'VERSION()': '8.0.25'}
        
        # Mock cursor context manager
        cursor_context = Mock()
        cursor_context.__enter__ = Mock(return_value=mock_cursor)
        cursor_context.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value = cursor_context
        
        mock_connect.return_value = mock_conn
        
        connection = mysql_engine._create_connection()
        
        assert isinstance(connection, MySQLConnection)
        mock_connect.assert_called_once()
        
        # Verify session setup calls
        cursor_calls = mock_cursor.execute.call_args_list
        assert any("character_set_client" in str(call) for call in cursor_calls)
        assert any("time_zone" in str(call) for call in cursor_calls)
        assert any("sql_mode" in str(call) for call in cursor_calls)
    
    @patch('sqlitch.engines.mysql.pymysql.connect')
    def test_create_connection_version_check_mysql(self, mock_connect, mysql_engine):
        """Test MySQL version compatibility check."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {'VERSION()': '5.0.95'}  # Too old
        
        # Mock cursor context manager
        cursor_context = Mock()
        cursor_context.__enter__ = Mock(return_value=mock_cursor)
        cursor_context.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value = cursor_context
        
        mock_connect.return_value = mock_conn
        
        with pytest.raises(EngineError) as exc_info:
            mysql_engine._create_connection()
        
        assert "MySQL 5.1.0 or higher" in str(exc_info.value)
    
    @patch('sqlitch.engines.mysql.pymysql.connect')
    def test_create_connection_version_check_mariadb(self, mock_connect, mysql_engine):
        """Test MariaDB version compatibility check."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {'VERSION()': '5.2.14-MariaDB'}  # Too old
        
        # Mock cursor context manager
        cursor_context = Mock()
        cursor_context.__enter__ = Mock(return_value=mock_cursor)
        cursor_context.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value = cursor_context
        
        mock_connect.return_value = mock_conn
        
        with pytest.raises(EngineError) as exc_info:
            mysql_engine._create_connection()
        
        assert "MariaDB 5.3.0 or higher" in str(exc_info.value)
    
    @patch('sqlitch.engines.mysql.pymysql.connect')
    def test_create_connection_failure(self, mock_connect, mysql_engine):
        """Test connection creation failure."""
        from sqlitch.engines.mysql import MySQLError
        
        mock_connect.side_effect = MySQLError("Connection failed")
        
        with pytest.raises(ConnectionError) as exc_info:
            mysql_engine._create_connection()
        
        assert "Failed to connect to MySQL database" in str(exc_info.value)
        assert exc_info.value.engine_name == "mysql"
    
    def test_split_sql_statements_basic(self, mysql_engine):
        """Test splitting basic SQL statements."""
        sql = """
        CREATE TABLE test (id INT);
        INSERT INTO test VALUES (1);
        SELECT * FROM test;
        """
        
        statements = mysql_engine._split_sql_statements(sql)
        
        assert len(statements) == 3
        assert "CREATE TABLE test" in statements[0]
        assert "INSERT INTO test" in statements[1]
        assert "SELECT * FROM test" in statements[2]
    
    def test_split_sql_statements_with_delimiter(self, mysql_engine):
        """Test splitting SQL with custom delimiter."""
        sql = """
        DELIMITER $$
        CREATE FUNCTION test() RETURNS INT
        BEGIN
            RETURN 1;
        END$$
        DELIMITER ;
        SELECT test();
        """
        
        statements = mysql_engine._split_sql_statements(sql)
        
        assert len(statements) == 2
        assert "CREATE FUNCTION test()" in statements[0]
        assert "SELECT test()" in statements[1]
    
    def test_split_sql_statements_with_comments(self, mysql_engine):
        """Test splitting SQL with comments."""
        sql = """
        -- This is a comment
        CREATE TABLE test (id INT);
        # MySQL-style comment
        INSERT INTO test VALUES (1);
        """
        
        statements = mysql_engine._split_sql_statements(sql)
        
        assert len(statements) == 2
        assert "CREATE TABLE test" in statements[0]
        assert "INSERT INTO test" in statements[1]
    
    def test_execute_sql_file_success(self, mysql_engine, tmp_path):
        """Test successful SQL file execution."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE test (id INT);\nINSERT INTO test VALUES (1);")
        
        mock_connection = Mock()
        
        mysql_engine._execute_sql_file(mock_connection, sql_file)
        
        # Should execute both statements
        assert mock_connection.execute.call_count == 2
    
    def test_execute_sql_file_with_variables(self, mysql_engine, tmp_path):
        """Test SQL file execution with variable substitution."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE :table_name (id INT);")
        
        mock_connection = Mock()
        variables = {'table_name': 'users'}
        
        mysql_engine._execute_sql_file(mock_connection, sql_file, variables)
        
        # Check that variable was substituted
        call_args = mock_connection.execute.call_args[0][0]
        assert 'users' in call_args
        assert ':table_name' not in call_args
    
    def test_execute_sql_file_not_found(self, mysql_engine):
        """Test SQL file execution with missing file."""
        mock_connection = Mock()
        non_existent_file = Path("/non/existent/file.sql")
        
        with pytest.raises(DeploymentError) as exc_info:
            mysql_engine._execute_sql_file(mock_connection, non_existent_file)
        
        assert "SQL file not found" in str(exc_info.value)
        assert exc_info.value.engine_name == "mysql"
    
    def test_get_registry_version_success(self, mysql_engine):
        """Test getting registry version."""
        mock_connection = Mock()
        mock_connection.fetchone.return_value = {'version': '1.1'}
        
        version = mysql_engine._get_registry_version(mock_connection)
        
        assert version == '1.1'
        mock_connection.execute.assert_called()
    
    def test_get_registry_version_not_found(self, mysql_engine):
        """Test getting registry version when not found."""
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Table doesn't exist")
        
        version = mysql_engine._get_registry_version(mock_connection)
        
        assert version is None
    
    def test_registry_exists_in_db_true(self, mysql_engine):
        """Test registry existence check when registry exists."""
        mock_connection = Mock()
        mock_connection.fetchone.side_effect = [
            {'database': 'sqitch_registry'},  # Database exists
            {'count': 1}  # Table exists
        ]
        
        exists = mysql_engine._registry_exists_in_db(mock_connection)
        
        assert exists is True
    
    def test_registry_exists_in_db_false(self, mysql_engine):
        """Test registry existence check when registry doesn't exist."""
        mock_connection = Mock()
        mock_connection.fetchone.return_value = None  # Database doesn't exist
        
        exists = mysql_engine._registry_exists_in_db(mock_connection)
        
        assert exists is False
    
    def test_create_registry_success(self, mysql_engine):
        """Test successful registry creation."""
        mock_connection = Mock()
        
        mysql_engine._create_registry(mock_connection)
        
        # Should execute multiple CREATE statements
        assert mock_connection.execute.call_count > 5
        
        # Check that database creation and USE statements are called
        execute_calls = [call[0][0] for call in mock_connection.execute.call_args_list]
        assert any("CREATE DATABASE" in call for call in execute_calls)
        assert any("USE" in call for call in execute_calls)
    
    def test_insert_project_record_new_project(self, mysql_engine):
        """Test inserting new project record."""
        mock_connection = Mock()
        mock_connection.fetchone.return_value = None  # Project doesn't exist
        
        mysql_engine._insert_project_record(mock_connection)
        
        # Should check for existing project and insert new one
        assert mock_connection.execute.call_count == 2
        
        # Check INSERT statement
        insert_call = mock_connection.execute.call_args_list[1]
        assert "INSERT INTO" in insert_call[0][0]
        assert mysql_engine.plan.project_name in insert_call[0][1]
    
    def test_insert_project_record_existing_project(self, mysql_engine):
        """Test inserting project record when project already exists."""
        mock_connection = Mock()
        mock_connection.fetchone.return_value = {'project': 'test_project'}  # Project exists
        
        mysql_engine._insert_project_record(mock_connection)
        
        # Should only check for existing project, not insert
        assert mock_connection.execute.call_count == 1
    
    def test_get_deployed_changes_success(self, mysql_engine):
        """Test getting deployed changes."""
        mock_connection = Mock()
        mock_connection.fetchall.return_value = [
            {'change_id': 'change1'},
            {'change_id': 'change2'}
        ]
        
        with patch.object(mysql_engine, 'ensure_registry'):
            with patch.object(mysql_engine, 'connection') as mock_conn_ctx:
                mock_conn_ctx.return_value.__enter__.return_value = mock_connection
                
                changes = mysql_engine.get_deployed_changes()
        
        assert changes == ['change1', 'change2']
    
    def test_calculate_script_hash(self, mysql_engine, tmp_path):
        """Test calculating script hash."""
        # Create mock change
        change = Mock()
        change.name = "test_change"
        
        # Create test files
        deploy_file = tmp_path / "deploy.sql"
        deploy_file.write_text("CREATE TABLE test (id INT);")
        
        revert_file = tmp_path / "revert.sql"
        revert_file.write_text("DROP TABLE test;")
        
        # Mock plan methods
        mysql_engine.plan.get_deploy_file.return_value = deploy_file
        mysql_engine.plan.get_revert_file.return_value = revert_file
        mysql_engine.plan.get_verify_file.return_value = tmp_path / "nonexistent.sql"
        
        hash_value = mysql_engine._calculate_script_hash(change)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) == 40  # SHA1 hash length
    
    def test_resolve_dependency_id_found(self, mysql_engine):
        """Test resolving dependency ID when dependency exists."""
        change1 = Mock()
        change1.name = "dependency_change"
        change1.id = "dep_id_123"
        
        mysql_engine.plan.changes = [change1]
        
        dep_id = mysql_engine._resolve_dependency_id("dependency_change")
        
        assert dep_id == "dep_id_123"
    
    def test_resolve_dependency_id_not_found(self, mysql_engine):
        """Test resolving dependency ID when dependency doesn't exist."""
        mysql_engine.plan.changes = []
        
        dep_id = mysql_engine._resolve_dependency_id("nonexistent_change")
        
        assert dep_id is None
    
    def test_format_dependencies(self, mysql_engine):
        """Test formatting dependencies list."""
        deps = ["dep1", "dep2", "dep3"]
        formatted = mysql_engine._format_dependencies(deps)
        
        assert formatted == "dep1 dep2 dep3"
    
    def test_format_dependencies_empty(self, mysql_engine):
        """Test formatting empty dependencies list."""
        formatted = mysql_engine._format_dependencies([])
        
        assert formatted == ""
    
    def test_format_tags(self, mysql_engine):
        """Test formatting tags list."""
        tags = ["v1.0", "release", "stable"]
        formatted = mysql_engine._format_tags(tags)
        
        assert formatted == "v1.0 release stable"
    
    def test_record_change_deployment(self, mysql_engine, tmp_path):
        """Test recording change deployment."""
        mock_connection = Mock()
        
        # Create test files for script hash calculation
        deploy_file = tmp_path / "deploy.sql"
        deploy_file.write_text("CREATE TABLE test (id INT);")
        
        # Mock plan methods
        mysql_engine.plan.get_deploy_file.return_value = deploy_file
        mysql_engine.plan.get_revert_file.return_value = tmp_path / "nonexistent.sql"
        mysql_engine.plan.get_verify_file.return_value = tmp_path / "nonexistent.sql"
        
        # Create mock change
        change = Mock()
        change.id = "change_123"
        change.name = "test_change"
        change.note = "Test change"
        change.planner_name = "Test User"
        change.planner_email = "test@example.com"
        change.timestamp = datetime.now(timezone.utc)
        change.dependencies = []
        change.tags = []
        
        mysql_engine._record_change_deployment(mock_connection, change)
        
        # Should execute USE, INSERT for change, and INSERT for event
        assert mock_connection.execute.call_count == 3
        
        # Check that change and event records are inserted
        execute_calls = [call[0][0] for call in mock_connection.execute.call_args_list]
        assert any("INSERT INTO changes" in call for call in execute_calls)
        assert any("INSERT INTO events" in call for call in execute_calls)
    
    def test_record_change_revert(self, mysql_engine):
        """Test recording change revert."""
        mock_connection = Mock()
        
        # Create mock change
        change = Mock()
        change.id = "change_123"
        change.name = "test_change"
        change.note = "Test change"
        change.planner_name = "Test User"
        change.planner_email = "test@example.com"
        change.timestamp = datetime.now(timezone.utc)
        change.dependencies = []
        change.tags = []
        
        mysql_engine._record_change_revert(mock_connection, change)
        
        # Should execute USE, DELETE for change, and INSERT for revert event
        assert mock_connection.execute.call_count == 3
        
        # Check that change is deleted and revert event is inserted
        execute_calls = [call[0][0] for call in mock_connection.execute.call_args_list]
        assert any("DELETE FROM changes" in call for call in execute_calls)
        assert any("INSERT INTO events" in call for call in execute_calls)
    
    def test_transaction_context_manager_success(self, mysql_engine):
        """Test transaction context manager with successful transaction."""
        mock_connection = Mock()
        
        with patch.object(mysql_engine, 'connection') as mock_conn_ctx:
            mock_conn_ctx.return_value.__enter__.return_value = mock_connection
            
            with mysql_engine.transaction() as conn:
                assert conn == mock_connection
                conn.execute("SELECT 1")
        
        # Should lock tables, start transaction, and commit
        execute_calls = [call[0][0] for call in mock_connection.execute.call_args_list]
        assert any("LOCK TABLES" in call for call in execute_calls)
        assert any("START TRANSACTION" in call for call in execute_calls)
        assert any("UNLOCK TABLES" in call for call in execute_calls)
        mock_connection.commit.assert_called_once()
    
    def test_transaction_context_manager_failure(self, mysql_engine):
        """Test transaction context manager with failed transaction."""
        mock_connection = Mock()
        mock_connection.execute.side_effect = [None, None, Exception("SQL error")]  # Fail on third call
        
        with patch.object(mysql_engine, 'connection') as mock_conn_ctx:
            mock_conn_ctx.return_value.__enter__.return_value = mock_connection
            
            with pytest.raises(DeploymentError):
                with mysql_engine.transaction() as conn:
                    conn.execute("USE database")
                    conn.execute("LOCK TABLES")
                    conn.execute("INVALID SQL")  # This will fail
        
        # Should attempt rollback
        mock_connection.rollback.assert_called_once()