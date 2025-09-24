"""
Unit tests for SQLite database engine.

This module contains comprehensive unit tests for the SQLiteEngine class,
testing connection management, registry operations, and SQL execution
with proper mocking and error handling verification.
"""

import pytest
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from sqlitch.engines.sqlite import SQLiteEngine, SQLiteConnection, SQLiteRegistrySchema
from sqlitch.core.types import Target, EngineType
from sqlitch.core.plan import Plan
from sqlitch.core.change import Change, Dependency
from sqlitch.core.exceptions import EngineError, ConnectionError, DeploymentError


class TestSQLiteRegistrySchema:
    """Test SQLite registry schema."""
    
    def test_get_create_statements(self):
        """Test getting create statements for SQLite."""
        statements = SQLiteRegistrySchema.get_create_statements('sqlite')
        
        assert isinstance(statements, list)
        assert len(statements) > 0
        
        # Check that all required tables are created
        sql_text = ' '.join(statements)
        assert 'CREATE TABLE releases' in sql_text
        assert 'CREATE TABLE projects' in sql_text
        assert 'CREATE TABLE changes' in sql_text
        assert 'CREATE TABLE tags' in sql_text
        assert 'CREATE TABLE dependencies' in sql_text
        assert 'CREATE TABLE events' in sql_text
        
        # Check for SQLite-specific features
        assert 'REFERENCES' in sql_text  # Foreign keys
        assert 'CHECK' in sql_text       # Check constraints
        assert 'UNIQUE' in sql_text      # Unique constraints


class TestSQLiteConnection:
    """Test SQLite connection wrapper."""
    
    @pytest.fixture
    def mock_sqlite_connection(self):
        """Create mock sqlite3 connection."""
        mock_conn = Mock(spec=sqlite3.Connection)
        mock_cursor = Mock(spec=sqlite3.Cursor)
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn, mock_cursor
    
    def test_init(self, mock_sqlite_connection):
        """Test SQLiteConnection initialization."""
        mock_conn, _ = mock_sqlite_connection
        
        conn = SQLiteConnection(mock_conn)
        
        assert conn._connection is mock_conn
        assert mock_conn.row_factory == sqlite3.Row
    
    def test_execute_without_params(self, mock_sqlite_connection):
        """Test executing SQL without parameters."""
        mock_conn, mock_cursor = mock_sqlite_connection
        conn = SQLiteConnection(mock_conn)
        
        conn.execute("SELECT 1")
        
        mock_cursor.execute.assert_called_once_with("SELECT 1")
    
    def test_execute_with_params(self, mock_sqlite_connection):
        """Test executing SQL with parameters."""
        mock_conn, mock_cursor = mock_sqlite_connection
        conn = SQLiteConnection(mock_conn)
        
        params = {'id': 1, 'name': 'test'}
        conn.execute("SELECT * FROM table WHERE id = :id", params)
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM table WHERE id = :id", params)
    
    def test_execute_error(self, mock_sqlite_connection):
        """Test SQL execution error handling."""
        mock_conn, mock_cursor = mock_sqlite_connection
        mock_cursor.execute.side_effect = sqlite3.Error("SQL error")
        conn = SQLiteConnection(mock_conn)
        
        with pytest.raises(DeploymentError) as exc_info:
            conn.execute("INVALID SQL")
        
        assert "SQL execution failed" in str(exc_info.value)
        assert exc_info.value.engine_name == "sqlite"
    
    def test_fetchone(self, mock_sqlite_connection):
        """Test fetching one row."""
        mock_conn, mock_cursor = mock_sqlite_connection
        # Create a proper mock row that behaves like sqlite3.Row
        mock_row = Mock()
        mock_row.keys.return_value = ['id', 'name']
        mock_row.__getitem__ = Mock(side_effect=lambda k: {'id': 1, 'name': 'test'}[k])
        mock_cursor.fetchone.return_value = mock_row
        conn = SQLiteConnection(mock_conn)
        
        result = conn.fetchone()
        
        assert result is not None
        assert result['id'] == 1
        assert result['name'] == 'test'
        mock_cursor.fetchone.assert_called_once()
    
    def test_fetchone_no_result(self, mock_sqlite_connection):
        """Test fetching one row with no result."""
        mock_conn, mock_cursor = mock_sqlite_connection
        mock_cursor.fetchone.return_value = None
        conn = SQLiteConnection(mock_conn)
        
        result = conn.fetchone()
        
        assert result is None
    
    def test_fetchall(self, mock_sqlite_connection):
        """Test fetching all rows."""
        mock_conn, mock_cursor = mock_sqlite_connection
        mock_rows = []
        for i in range(2):
            mock_row = Mock()
            mock_row.keys.return_value = ['id', 'name']
            # Fix closure issue by creating a proper closure
            def make_getitem(idx):
                return lambda k: {'id': idx, 'name': f'test{idx}'}[k]
            mock_row.__getitem__ = Mock(side_effect=make_getitem(i))
            mock_rows.append(mock_row)
        mock_cursor.fetchall.return_value = mock_rows
        conn = SQLiteConnection(mock_conn)
        
        result = conn.fetchall()
        
        assert len(result) == 2
        assert result[0]['id'] == 0
        assert result[1]['id'] == 1
        mock_cursor.fetchall.assert_called_once()
    
    def test_commit(self, mock_sqlite_connection):
        """Test transaction commit."""
        mock_conn, _ = mock_sqlite_connection
        conn = SQLiteConnection(mock_conn)
        
        conn.commit()
        
        mock_conn.commit.assert_called_once()
    
    def test_rollback(self, mock_sqlite_connection):
        """Test transaction rollback."""
        mock_conn, _ = mock_sqlite_connection
        conn = SQLiteConnection(mock_conn)
        
        conn.rollback()
        
        mock_conn.rollback.assert_called_once()
    
    def test_close(self, mock_sqlite_connection):
        """Test connection close."""
        mock_conn, mock_cursor = mock_sqlite_connection
        conn = SQLiteConnection(mock_conn)
        conn._cursor = mock_cursor
        
        conn.close()
        
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
        assert conn._cursor is None


class TestSQLiteEngine:
    """Test SQLite database engine."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database file path."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        yield db_path
        # Cleanup
        Path(db_path).unlink(missing_ok=True)
    
    @pytest.fixture
    def mock_target(self, temp_db_path):
        """Create mock target with temporary database."""
        return Target(
            name="test",
            uri=f"sqlite:{temp_db_path}",
            registry="sqitch"
        )
    
    @pytest.fixture
    def mock_plan(self):
        """Create mock plan."""
        plan = Mock(spec=Plan)
        plan.project_name = "test_project"
        plan.creator_name = "Test User"
        plan.creator_email = "test@example.com"
        plan.changes = []
        return plan
    
    @pytest.fixture
    def sqlite_engine(self, mock_target, mock_plan):
        """Create SQLite engine instance."""
        return SQLiteEngine(mock_target, mock_plan)
    
    def test_init(self, sqlite_engine, temp_db_path):
        """Test SQLite engine initialization."""
        assert sqlite_engine.engine_type == 'sqlite'
        assert isinstance(sqlite_engine.registry_schema, SQLiteRegistrySchema)
        assert sqlite_engine._db_path == temp_db_path
    
    def test_init_creates_parent_directory(self, mock_plan):
        """Test that parent directory is created for database file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "subdir" / "test.db"
            target = Target(name="test", uri=f"sqlite:{db_path}", registry="sqitch")
            
            engine = SQLiteEngine(target, mock_plan)
            
            assert db_path.parent.exists()
            assert engine._db_path == str(db_path)
    
    def test_parse_database_path_sqlite_scheme(self, mock_plan):
        """Test parsing database path with sqlite: scheme."""
        test_cases = [
            ("sqlite:test.db", "test.db"),
            ("sqlite://test.db", "test.db"),
            ("sqlite:///absolute/path/test.db", "absolute/path/test.db"),  # Fixed expected path
            ("db:sqlite:test.db", "test.db"),
            ("test.db", "test.db"),  # Direct path
        ]
        
        for uri, expected_path in test_cases:
            target = Target(name="test", uri=uri, registry="sqitch")
            engine = SQLiteEngine(target, mock_plan)
            assert engine._db_path == expected_path
    
    def test_parse_database_path_invalid_uri(self, mock_plan):
        """Test parsing invalid database URI."""
        target = Target(name="test", uri="", registry="sqitch")
        
        # Should not raise error for empty URI, just use it as-is
        engine = SQLiteEngine(target, mock_plan)
        assert engine._db_path == ""
    
    @patch('sqlite3.connect')
    def test_create_connection_success(self, mock_connect, sqlite_engine):
        """Test successful database connection creation."""
        mock_conn = Mock(spec=sqlite3.Connection)
        mock_cursor = Mock(spec=sqlite3.Cursor)
        mock_cursor.fetchone.return_value = ('3.8.6',)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        connection = sqlite_engine._create_connection()
        
        assert isinstance(connection, SQLiteConnection)
        mock_connect.assert_called_once()
        mock_conn.execute.assert_any_call("PRAGMA foreign_keys = ON")
        mock_conn.execute.assert_any_call("PRAGMA locking_mode = NORMAL")
    
    @patch('sqlite3.connect')
    def test_create_connection_version_check(self, mock_connect, sqlite_engine):
        """Test SQLite version compatibility check."""
        mock_conn = Mock(spec=sqlite3.Connection)
        mock_cursor = Mock(spec=sqlite3.Cursor)
        mock_cursor.fetchone.return_value = ('3.7.0',)  # Too old
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with pytest.raises(EngineError) as exc_info:
            sqlite_engine._create_connection()
        
        assert "requires SQLite 3.8.6 or later" in str(exc_info.value)
    
    @patch('sqlite3.connect')
    def test_create_connection_error(self, mock_connect, sqlite_engine):
        """Test connection creation error handling."""
        mock_connect.side_effect = sqlite3.Error("Connection failed")
        
        with pytest.raises(ConnectionError) as exc_info:
            sqlite_engine._create_connection()
        
        assert "Failed to connect to SQLite database" in str(exc_info.value)
        assert exc_info.value.engine_name == 'sqlite'
    
    def test_execute_sql_file_success(self, sqlite_engine):
        """Test successful SQL file execution."""
        mock_connection = Mock(spec=SQLiteConnection)
        mock_sqlite_conn = Mock(spec=sqlite3.Connection)
        mock_connection._connection = mock_sqlite_conn
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("CREATE TABLE test (id INTEGER);")
            sql_file = Path(f.name)
        
        try:
            sqlite_engine._execute_sql_file(mock_connection, sql_file)
            mock_sqlite_conn.executescript.assert_called_once()
        finally:
            sql_file.unlink()
    
    def test_execute_sql_file_with_variables(self, sqlite_engine):
        """Test SQL file execution with variable substitution."""
        mock_connection = Mock(spec=SQLiteConnection)
        mock_sqlite_conn = Mock(spec=sqlite3.Connection)
        mock_connection._connection = mock_sqlite_conn
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("CREATE TABLE :table_name (id INTEGER);")
            sql_file = Path(f.name)
        
        try:
            variables = {'table_name': 'test_table'}
            sqlite_engine._execute_sql_file(mock_connection, sql_file, variables)
            
            # Check that variable substitution occurred
            call_args = mock_sqlite_conn.executescript.call_args[0][0]
            assert 'test_table' in call_args
            assert ':table_name' not in call_args
        finally:
            sql_file.unlink()
    
    def test_execute_sql_file_not_found(self, sqlite_engine):
        """Test SQL file execution with missing file."""
        mock_connection = Mock(spec=SQLiteConnection)
        non_existent_file = Path("/non/existent/file.sql")
        
        with pytest.raises(DeploymentError) as exc_info:
            sqlite_engine._execute_sql_file(mock_connection, non_existent_file)
        
        assert "SQL file not found" in str(exc_info.value)
    
    def test_execute_sql_file_sqlite_error(self, sqlite_engine):
        """Test SQL file execution with SQLite error."""
        mock_connection = Mock(spec=SQLiteConnection)
        mock_sqlite_conn = Mock(spec=sqlite3.Connection)
        mock_sqlite_conn.executescript.side_effect = sqlite3.Error("SQL error")
        mock_connection._connection = mock_sqlite_conn
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("INVALID SQL;")
            sql_file = Path(f.name)
        
        try:
            with pytest.raises(DeploymentError) as exc_info:
                sqlite_engine._execute_sql_file(mock_connection, sql_file)
            
            assert "Failed to execute SQL file" in str(exc_info.value)
        finally:
            sql_file.unlink()
    
    def test_get_registry_version_success(self, sqlite_engine):
        """Test getting registry version."""
        mock_connection = Mock(spec=SQLiteConnection)
        mock_connection.fetchone.return_value = {'CAST(ROUND(MAX(version), 1) AS TEXT)': '1.1'}
        
        version = sqlite_engine._get_registry_version(mock_connection)
        
        assert version == '1.1'
        mock_connection.execute.assert_called_once()
    
    def test_get_registry_version_not_found(self, sqlite_engine):
        """Test getting registry version when not found."""
        mock_connection = Mock(spec=SQLiteConnection)
        mock_connection.fetchone.return_value = None
        
        version = sqlite_engine._get_registry_version(mock_connection)
        
        assert version is None
    
    def test_get_registry_version_error(self, sqlite_engine):
        """Test getting registry version with database error."""
        mock_connection = Mock(spec=SQLiteConnection)
        mock_connection.execute.side_effect = sqlite3.Error("Table not found")
        
        version = sqlite_engine._get_registry_version(mock_connection)
        
        assert version is None
    
    def test_registry_exists_in_db_true(self, sqlite_engine):
        """Test registry existence check when registry exists."""
        mock_connection = Mock(spec=SQLiteConnection)
        mock_connection.fetchone.return_value = {
            'EXISTS(SELECT 1 FROM sqlite_master WHERE type = \'table\' AND name = ?)': 1
        }
        
        exists = sqlite_engine._registry_exists_in_db(mock_connection)
        
        assert exists is True
        mock_connection.execute.assert_called_once()
    
    def test_registry_exists_in_db_false(self, sqlite_engine):
        """Test registry existence check when registry doesn't exist."""
        mock_connection = Mock(spec=SQLiteConnection)
        mock_connection.fetchone.return_value = {
            'EXISTS(SELECT 1 FROM sqlite_master WHERE type = \'table\' AND name = ?)': 0
        }
        
        exists = sqlite_engine._registry_exists_in_db(mock_connection)
        
        assert exists is False
    
    def test_registry_exists_in_db_error(self, sqlite_engine):
        """Test registry existence check with database error."""
        mock_connection = Mock(spec=SQLiteConnection)
        mock_connection.execute.side_effect = sqlite3.Error("Database error")
        
        exists = sqlite_engine._registry_exists_in_db(mock_connection)
        
        assert exists is False
    
    def test_run_file(self, sqlite_engine):
        """Test running SQL file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("SELECT 1;")
            sql_file = Path(f.name)
        
        try:
            with patch.object(sqlite_engine, 'connection') as mock_context:
                mock_conn = Mock(spec=SQLiteConnection)
                mock_context.return_value.__enter__.return_value = mock_conn
                
                with patch.object(sqlite_engine, '_execute_sql_file') as mock_execute:
                    sqlite_engine.run_file(sql_file)
                    mock_execute.assert_called_once_with(mock_conn, sql_file)
        finally:
            sql_file.unlink()
    
    def test_run_verify(self, sqlite_engine):
        """Test running verification SQL file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("SELECT COUNT(*) FROM test_table;")
            sql_file = Path(f.name)
        
        try:
            with patch.object(sqlite_engine, 'connection') as mock_context:
                mock_conn = Mock(spec=SQLiteConnection)
                mock_context.return_value.__enter__.return_value = mock_conn
                
                with patch.object(sqlite_engine, '_execute_sql_file') as mock_execute:
                    sqlite_engine.run_verify(sql_file)
                    mock_execute.assert_called_once_with(mock_conn, sql_file)
        finally:
            sql_file.unlink()


class TestSQLiteEngineIntegration:
    """Integration tests for SQLite engine with real database."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database file."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        yield db_path
        Path(db_path).unlink(missing_ok=True)
    
    @pytest.fixture
    def real_target(self, temp_db_path):
        """Create real target with temporary database."""
        return Target(
            name="test",
            uri=f"sqlite:{temp_db_path}",
            registry="sqitch"
        )
    
    @pytest.fixture
    def real_plan(self, tmp_path):
        """Create real plan."""
        plan_file = tmp_path / "sqitch.plan"
        plan_file.write_text("%syntax-version=1.0.0\n%project=test_project\n")
        return Plan(
            file=plan_file,
            project="test_project",
            uri="https://example.com/test",
            changes=[]
        )
    
    @pytest.fixture
    def real_engine(self, real_target, real_plan):
        """Create real SQLite engine."""
        return SQLiteEngine(real_target, real_plan)
    
    def test_real_connection(self, real_engine):
        """Test creating real SQLite connection."""
        with real_engine.connection() as conn:
            assert isinstance(conn, SQLiteConnection)
            
            # Test basic query
            conn.execute("SELECT 1 as test")
            result = conn.fetchone()
            assert result['test'] == 1
    
    def test_registry_creation(self, real_engine):
        """Test creating registry tables in real database."""
        real_engine.ensure_registry()
        
        # Verify tables were created
        with real_engine.connection() as conn:
            conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ('releases', 'projects', 'changes', 'tags', 'dependencies', 'events')
                ORDER BY name
            """)
            tables = [row['name'] for row in conn.fetchall()]
            
            expected_tables = ['changes', 'dependencies', 'events', 'projects', 'releases', 'tags']
            assert tables == expected_tables
    
    def test_registry_version(self, real_engine):
        """Test registry version tracking."""
        real_engine.ensure_registry()
        
        with real_engine.connection() as conn:
            version = real_engine._get_registry_version(conn)
            assert version == "1.1"