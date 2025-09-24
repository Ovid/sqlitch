"""
Unit tests for Oracle database engine.

Tests the Oracle-specific implementation including connection handling,
registry management, SQL execution, and change operations.
"""

import pytest
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, Any, List

from sqlitch.engines.oracle import (
    OracleEngine, OracleConnection, OracleRegistrySchema
)
from sqlitch.core.exceptions import (
    EngineError, ConnectionError, DeploymentError
)
from sqlitch.core.types import URI
from sqlitch.core.target import Target
from sqlitch.core.change import Change, Dependency
from sqlitch.core.plan import Plan


class MockCxOracleConnection:
    """Mock cx_Oracle connection for testing."""
    
    def __init__(self):
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.cursors = []
    
    def cursor(self):
        cursor = MockCxOracleCursor()
        self.cursors.append(cursor)
        return cursor
    
    def commit(self):
        self.committed = True
    
    def rollback(self):
        self.rolled_back = True
    
    def close(self):
        self.closed = True


class MockCxOracleCursor:
    """Mock cx_Oracle cursor for testing."""
    
    def __init__(self):
        self.executed_statements = []
        self.executed_params = []
        self.fetchone_results = []
        self.fetchall_results = []
        self.description = []
        self.closed = False
        self.current_result_index = 0
    
    def execute(self, sql, params=None):
        self.executed_statements.append(sql)
        self.executed_params.append(params)
        return self
    
    def fetchone(self):
        if self.fetchone_results:
            result = self.fetchone_results.pop(0)
            # If result is a dict, convert to tuple based on description
            if isinstance(result, dict) and self.description:
                return tuple(result.get(desc[0].lower(), None) for desc in self.description)
            return result
        return None
    
    def fetchall(self):
        if self.fetchall_results:
            return self.fetchall_results.pop(0)
        return []
    
    def close(self):
        self.closed = True


@pytest.fixture
def mock_cx_oracle():
    """Mock cx_Oracle module."""
    with patch('sqlitch.engines.oracle.cx_Oracle') as mock:
        mock.makedsn = Mock(return_value='mock_dsn')
        mock.connect = Mock(return_value=MockCxOracleConnection())
        mock.DatabaseError = Exception
        yield mock


@pytest.fixture
def target():
    """Create test target."""
    return Target(
        name="test_oracle",
        uri=URI("oracle://testuser:testpass@localhost:1521/testdb"),
        registry="testuser"
    )


@pytest.fixture
def plan():
    """Create test plan."""
    return Plan(
        file=Path("sqitch.plan"),
        project="test_project",
        uri="https://github.com/test/project",
        changes=[]
    )


@pytest.fixture
def change():
    """Create test change."""
    return Change(
        id="test_change_id",
        name="test_change",
        note="Test change note",
        timestamp=datetime.now(),
        planner_name="Test Planner",
        planner_email="planner@example.com",
        dependencies=[],
        conflicts=[]
    )


class TestOracleRegistrySchema:
    """Test cases for OracleRegistrySchema."""
    
    def test_get_create_statements_with_schema(self):
        """Test getting create statements with schema prefix."""
        statements = OracleRegistrySchema.get_create_statements('oracle', 'testschema')
        
        assert len(statements) > 0
        assert any('testschema.releases' in stmt for stmt in statements)
        assert any('testschema.projects' in stmt for stmt in statements)
        assert any('testschema.changes' in stmt for stmt in statements)
        assert any('testschema.tags' in stmt for stmt in statements)
        assert any('testschema.dependencies' in stmt for stmt in statements)
        assert any('testschema.events' in stmt for stmt in statements)
    
    def test_get_create_statements_without_schema(self):
        """Test getting create statements without schema prefix."""
        statements = OracleRegistrySchema.get_create_statements('oracle', None)
        
        assert len(statements) > 0
        assert any('releases' in stmt and 'testschema.releases' not in stmt for stmt in statements)
        assert any('projects' in stmt and 'testschema.projects' not in stmt for stmt in statements)
    
    def test_create_statements_include_comments(self):
        """Test that create statements include table and column comments."""
        statements = OracleRegistrySchema.get_create_statements('oracle', 'test')
        
        comment_statements = [stmt for stmt in statements if stmt.strip().startswith('COMMENT ON')]
        assert len(comment_statements) > 0
        assert any('Sqitch registry releases' in stmt for stmt in comment_statements)
        assert any('Sqitch projects deployed' in stmt for stmt in comment_statements)


class TestOracleConnection:
    """Test cases for OracleConnection wrapper."""
    
    def test_execute_with_params(self):
        """Test executing SQL with parameters."""
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        
        oracle_conn.execute("SELECT * FROM test WHERE id = :id", {'id': 123})
        
        cursor = mock_conn.cursors[0]
        assert len(cursor.executed_statements) == 1
        assert "SELECT * FROM test WHERE id = ?" in cursor.executed_statements[0]
        assert cursor.executed_params[0] == [123]
    
    def test_execute_without_params(self):
        """Test executing SQL without parameters."""
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        
        oracle_conn.execute("SELECT * FROM test")
        
        cursor = mock_conn.cursors[0]
        assert len(cursor.executed_statements) == 1
        assert cursor.executed_statements[0] == "SELECT * FROM test"
        assert cursor.executed_params[0] is None
    
    def test_fetchone(self):
        """Test fetching one row."""
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        
        # Setup mock cursor
        oracle_conn.execute("SELECT * FROM test")
        cursor = mock_conn.cursors[0]
        cursor.description = [('id', None), ('name', None)]
        cursor.fetchone_results = [(1, 'test')]
        
        result = oracle_conn.fetchone()
        
        assert result == {'id': 1, 'name': 'test'}
    
    def test_fetchall(self):
        """Test fetching all rows."""
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        
        # Setup mock cursor
        oracle_conn.execute("SELECT * FROM test")
        cursor = mock_conn.cursors[0]
        cursor.description = [('id', None), ('name', None)]
        cursor.fetchall_results = [[(1, 'test1'), (2, 'test2')]]
        
        results = oracle_conn.fetchall()
        
        assert len(results) == 2
        assert results[0] == {'id': 1, 'name': 'test1'}
        assert results[1] == {'id': 2, 'name': 'test2'}
    
    def test_commit(self):
        """Test committing transaction."""
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        
        oracle_conn.commit()
        
        assert mock_conn.committed is True
    
    def test_rollback(self):
        """Test rolling back transaction."""
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        
        oracle_conn.rollback()
        
        assert mock_conn.rolled_back is True
    
    def test_close(self):
        """Test closing connection."""
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        
        # Execute something to create cursor
        oracle_conn.execute("SELECT 1")
        
        oracle_conn.close()
        
        assert mock_conn.closed is True
        assert mock_conn.cursors[0].closed is True


class TestOracleEngine:
    """Test cases for OracleEngine."""
    
    def test_init_without_cx_oracle(self, target, plan):
        """Test initialization fails without cx_Oracle."""
        with patch('sqlitch.engines.oracle.cx_Oracle', None):
            with pytest.raises(EngineError) as exc_info:
                OracleEngine(target, plan)
            
            assert "cx_Oracle package is required" in str(exc_info.value)
    
    def test_init_sets_environment_variables(self, mock_cx_oracle, target, plan):
        """Test initialization sets Oracle environment variables."""
        OracleEngine(target, plan)
        
        assert os.environ['NLS_LANG'] == 'AMERICAN_AMERICA.AL32UTF8'
        assert os.environ['SQLPATH'] == ''
    
    def test_engine_type(self, mock_cx_oracle, target, plan):
        """Test engine type property."""
        engine = OracleEngine(target, plan)
        assert engine.engine_type == 'oracle'
    
    def test_parse_registry_schema_from_target(self, mock_cx_oracle, plan):
        """Test parsing registry schema from target."""
        target = Target(
            name="test",
            uri=URI("oracle://user:pass@host/db"),
            registry="custom_schema"
        )
        
        engine = OracleEngine(target, plan)
        assert engine._registry_schema == "custom_schema"
    
    def test_parse_registry_schema_from_username(self, mock_cx_oracle, plan):
        """Test parsing registry schema from username."""
        target = Target(
            name="test",
            uri=URI("oracle://testuser:pass@host/db")
        )
        
        engine = OracleEngine(target, plan)
        assert engine._registry_schema == "TESTUSER"
    
    def test_create_connection_success(self, mock_cx_oracle, target, plan):
        """Test successful connection creation."""
        engine = OracleEngine(target, plan)
        
        with engine.connection() as conn:
            assert isinstance(conn, OracleConnection)
        
        # Verify cx_Oracle.connect was called with correct parameters
        mock_cx_oracle.connect.assert_called_once()
        call_kwargs = mock_cx_oracle.connect.call_args[1]
        assert call_kwargs['user'] == 'testuser'
        assert call_kwargs['password'] == 'testpass'
        assert call_kwargs['encoding'] == 'UTF-8'
    
    def test_create_connection_failure(self, mock_cx_oracle, target, plan):
        """Test connection creation failure."""
        mock_cx_oracle.connect.side_effect = Exception("Connection failed")
        
        engine = OracleEngine(target, plan)
        
        with pytest.raises(ConnectionError) as exc_info:
            with engine.connection():
                pass
        
        assert "Failed to connect to Oracle database" in str(exc_info.value)
    
    def test_execute_sql_file(self, mock_cx_oracle, target, plan, tmp_path):
        """Test executing SQL file."""
        # Create test SQL file
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("""
        -- Test comment
        CREATE TABLE test (id NUMBER);
        /
        INSERT INTO test VALUES (1);
        """)
        
        engine = OracleEngine(target, plan)
        
        with engine.connection() as conn:
            # The OracleConnection wrapper uses the same cursor
            # Get the cursor from the wrapper
            cursor = conn._cursor
            initial_count = len(cursor.executed_statements)
            
            engine._execute_sql_file(conn, sql_file)
            
            # Check statements executed after the initial setup
            new_statements = cursor.executed_statements[initial_count:]
        
        # Verify statements were executed
        # Should have executed 2 statements (CREATE and INSERT)
        assert len(new_statements) >= 2
        assert any("CREATE TABLE test" in stmt for stmt in new_statements)
        assert any("INSERT INTO test" in stmt for stmt in new_statements)
    
    def test_execute_sql_file_with_variables(self, mock_cx_oracle, target, plan, tmp_path):
        """Test executing SQL file with variable substitution."""
        # Create test SQL file with variables
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE &table_name (id NUMBER);")
        
        engine = OracleEngine(target, plan)
        
        with engine.connection() as conn:
            # The OracleConnection wrapper uses the same cursor
            cursor = conn._cursor
            initial_count = len(cursor.executed_statements)
            
            engine._execute_sql_file(conn, sql_file, {'table_name': 'test_table'})
            
            # Check statements executed after the initial setup
            new_statements = cursor.executed_statements[initial_count:]
        
        # Verify variable substitution
        assert any("CREATE TABLE test_table" in stmt for stmt in new_statements)
    
    def test_split_oracle_statements(self, mock_cx_oracle, target, plan):
        """Test splitting Oracle SQL statements."""
        engine = OracleEngine(target, plan)
        
        sql_content = """
        -- Comment
        CREATE TABLE test (id NUMBER);
        /
        INSERT INTO test VALUES (1);
        INSERT INTO test VALUES (2);
        /
        """
        
        statements = engine._split_oracle_statements(sql_content)
        
        assert len(statements) == 2
        assert "CREATE TABLE test" in statements[0]
        assert "INSERT INTO test VALUES (1)" in statements[1]
        assert "INSERT INTO test VALUES (2)" in statements[1]
    
    def test_registry_exists_in_db(self, mock_cx_oracle, target, plan):
        """Test checking if registry exists."""
        engine = OracleEngine(target, plan)
        
        # Create a mock connection directly
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        cursor = mock_conn.cursors[0]
        
        # Mock the fetchone result for the registry check
        cursor.fetchone_results = [{'table_name': 'CHANGES'}]
        
        exists = engine._registry_exists_in_db(oracle_conn)
        assert exists is True
    
    def test_registry_does_not_exist_in_db(self, mock_cx_oracle, target, plan):
        """Test checking if registry does not exist."""
        engine = OracleEngine(target, plan)
        
        with engine.connection() as conn:
            # Mock query that raises exception (table doesn't exist)
            mock_conn = mock_cx_oracle.connect.return_value
            cursor = mock_conn.cursors[0]
            cursor.execute = Mock(side_effect=Exception("Table not found"))
            
            exists = engine._registry_exists_in_db(conn)
            assert exists is False
    
    def test_create_registry(self, mock_cx_oracle, target, plan):
        """Test creating registry tables."""
        engine = OracleEngine(target, plan)
        
        # Create a mock connection directly
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        cursor = mock_conn.cursors[0]
        cursor.fetchone_results = [{'count': 0}]  # Project doesn't exist
        
        engine._create_registry(oracle_conn)
        
        # Verify multiple statements were executed
        assert len(cursor.executed_statements) > 10  # Should have many CREATE statements
        
        # Verify some key statements
        statements_str = ' '.join(cursor.executed_statements)
        assert 'CREATE TABLE' in statements_str
        assert 'releases' in statements_str
        assert 'projects' in statements_str
        assert 'changes' in statements_str
    
    def test_get_registry_version(self, mock_cx_oracle, target, plan):
        """Test getting registry version."""
        engine = OracleEngine(target, plan)
        
        # Create a mock connection directly
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        cursor = mock_conn.cursors[0]
        cursor.description = [('VERSION', None)]  # Add description for column names (Oracle uses uppercase)
        cursor.fetchone_results = [{'version': 1.1}]
        
        version = engine._get_registry_version(oracle_conn)
        assert version == "1.1"
    
    def test_get_registry_version_not_found(self, mock_cx_oracle, target, plan):
        """Test getting registry version when not found."""
        engine = OracleEngine(target, plan)
        
        with engine.connection() as conn:
            # Mock query that raises exception
            mock_conn = mock_cx_oracle.connect.return_value
            cursor = mock_conn.cursors[0]
            cursor.execute = Mock(side_effect=Exception("Table not found"))
            
            version = engine._get_registry_version(conn)
            assert version is None
    
    def test_regex_condition(self, mock_cx_oracle, target, plan):
        """Test Oracle regex condition generation."""
        engine = OracleEngine(target, plan)
        
        condition = engine._regex_condition('column_name', 'pattern')
        assert condition == "REGEXP_LIKE(column_name, ?)"
    
    def test_insert_release_record(self, mock_cx_oracle, target, plan):
        """Test inserting release record."""
        engine = OracleEngine(target, plan)
        
        # Create a mock connection directly
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        cursor = mock_conn.cursors[0]
        
        engine._insert_release_record(oracle_conn)
        
        # Verify INSERT statement was executed
        insert_statements = [stmt for stmt in cursor.executed_statements if "INSERT INTO" in stmt]
        assert len(insert_statements) == 1
        assert "releases" in insert_statements[0]
    
    def test_insert_project_record(self, mock_cx_oracle, target, plan):
        """Test inserting project record."""
        engine = OracleEngine(target, plan)
        
        # Create a mock connection directly
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        cursor = mock_conn.cursors[0]
        cursor.description = [('COUNT', None)]  # Add description for column names
        cursor.fetchone_results = [{'count': 0}]  # Project doesn't exist
        
        engine._insert_project_record(oracle_conn)
        
        # Check for specific statements
        select_statements = [stmt for stmt in cursor.executed_statements if "SELECT COUNT(*)" in stmt]
        insert_statements = [stmt for stmt in cursor.executed_statements if "INSERT INTO" in stmt and "projects" in stmt]
        
        assert len(select_statements) == 1
        assert len(insert_statements) == 1
    
    def test_insert_project_record_already_exists(self, mock_cx_oracle, target, plan):
        """Test inserting project record when it already exists."""
        engine = OracleEngine(target, plan)
        
        # Create a mock connection directly
        mock_conn = MockCxOracleConnection()
        oracle_conn = OracleConnection(mock_conn)
        cursor = mock_conn.cursors[0]
        cursor.description = [('COUNT', None)]  # Add description for column names
        cursor.fetchone_results = [{'count': 1}]  # Project exists
        
        engine._insert_project_record(oracle_conn)
        
        # Verify only SELECT was executed, no INSERT
        select_statements = [stmt for stmt in cursor.executed_statements if "SELECT COUNT(*)" in stmt]
        insert_statements = [stmt for stmt in cursor.executed_statements if "INSERT INTO" in stmt and "projects" in stmt]
        
        assert len(select_statements) == 1
        assert len(insert_statements) == 0  # No INSERT should happen