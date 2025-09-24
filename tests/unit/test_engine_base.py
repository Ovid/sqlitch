"""
Unit tests for the abstract engine base class.

Tests the core functionality of the Engine base class including
registry management, connection handling, and the engine registry.
"""

import pytest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

from sqlitch.engines.base import (
    Engine, EngineRegistry, RegistrySchema, register_engine
)
from sqlitch.core.exceptions import EngineError, ConnectionError, DeploymentError
from sqlitch.core.types import URI, EngineType
from sqlitch.core.target import Target
from sqlitch.core.change import Change
from sqlitch.core.plan import Plan


class MockConnection:
    """Mock database connection for testing."""
    
    def __init__(self):
        self.executed_statements = []
        self.fetch_results = []
        self.fetch_index = 0
        self.committed = False
        self.rolled_back = False
        self.closed = False
    
    def execute(self, sql: str, params=None):
        self.executed_statements.append((sql, params))
    
    def fetchone(self):
        if self.fetch_index < len(self.fetch_results):
            result = self.fetch_results[self.fetch_index]
            self.fetch_index += 1
            return result
        return None
    
    def fetchall(self):
        results = self.fetch_results[self.fetch_index:]
        self.fetch_index = len(self.fetch_results)
        return results
    
    def commit(self):
        self.committed = True
    
    def rollback(self):
        self.rolled_back = True
    
    def close(self):
        self.closed = True


class MockEngine(Engine):
    """Test implementation of Engine for testing."""
    
    @property
    def engine_type(self) -> EngineType:
        return 'pg'
    
    @property
    def registry_schema(self) -> RegistrySchema:
        schema = RegistrySchema()
        # Override the method for testing
        schema.get_create_statements = lambda engine_type: ["CREATE TABLE test"]
        return schema
    
    def _create_connection(self):
        return MockConnection()
    
    def _execute_sql_file(self, connection, sql_file, variables=None):
        # Mock implementation
        connection.execute(f"-- Executing {sql_file}")
    
    def _get_registry_version(self, connection):
        return "1.1"
    
    def _regex_condition(self, column: str, pattern: str) -> str:
        """Mock regex condition for testing."""
        return f"{column} ~ ?"


@pytest.fixture
def mock_target():
    """Create mock target for testing."""
    return Target(
        name="test",
        uri=URI("db:pg://localhost/test"),
        registry="sqitch"
    )


@pytest.fixture
def mock_plan():
    """Create mock plan for testing."""
    plan = Mock(spec=Plan)
    plan.project_name = "test_project"
    plan.creator_name = "Test User"
    plan.creator_email = "test@example.com"
    plan.changes = []
    plan.get_deploy_file = Mock(return_value=Path("/fake/deploy.sql"))
    plan.get_revert_file = Mock(return_value=Path("/fake/revert.sql"))
    plan.get_verify_file = Mock(return_value=Path("/fake/verify.sql"))
    return plan


@pytest.fixture
def test_engine(mock_target, mock_plan):
    """Create test engine instance."""
    return MockEngine(mock_target, mock_plan)


class TestEngineBase:
    """Test cases for Engine base class."""
    
    def test_engine_initialization(self, test_engine, mock_target, mock_plan):
        """Test engine initialization."""
        assert test_engine.target == mock_target
        assert test_engine.plan == mock_plan
        assert test_engine.engine_type == 'pg'
        assert test_engine._connection is None
        assert test_engine._registry_exists is None
    
    def test_connection_context_manager(self, test_engine):
        """Test connection context manager."""
        with test_engine.connection() as conn:
            assert isinstance(conn, MockConnection)
            assert not conn.closed
        
        assert conn.closed
    
    def test_connection_error_handling(self, test_engine):
        """Test connection error handling."""
        # Mock _create_connection to raise exception
        test_engine._create_connection = Mock(side_effect=Exception("Connection failed"))
        
        with pytest.raises(ConnectionError) as exc_info:
            with test_engine.connection():
                pass
        
        assert "Failed to connect to pg database" in str(exc_info.value)
        assert exc_info.value.engine_name == 'pg'
    
    def test_transaction_context_manager(self, test_engine):
        """Test transaction context manager."""
        with test_engine.transaction() as conn:
            assert isinstance(conn, MockConnection)
            conn.execute("SELECT 1")
        
        assert conn.committed
        assert conn.closed
    
    def test_transaction_rollback_on_error(self, test_engine):
        """Test transaction rollback on error."""
        # Create a mock connection that we can inspect
        mock_conn = MockConnection()
        test_engine._create_connection = Mock(return_value=mock_conn)
        
        with pytest.raises(ConnectionError):  # The connection context manager re-raises as ConnectionError
            with test_engine.transaction() as conn:
                conn.execute("SELECT 1")
                raise Exception("Test error")
        
        # Connection should be rolled back and closed
        assert mock_conn.rolled_back
        assert mock_conn.closed
    
    def test_registry_exists_check(self, test_engine):
        """Test registry existence check."""
        # Mock connection that returns successful query
        mock_conn = MockConnection()
        test_engine._create_connection = Mock(return_value=mock_conn)
        
        # Should return True when query succeeds
        with test_engine.connection() as conn:
            exists = test_engine._registry_exists_in_db(conn)
            assert exists
    
    def test_registry_not_exists_check(self, test_engine):
        """Test registry non-existence check."""
        # Mock connection that raises exception on query
        mock_conn = MockConnection()
        mock_conn.execute = Mock(side_effect=Exception("Table not found"))
        test_engine._create_connection = Mock(return_value=mock_conn)
        
        with test_engine.connection() as conn:
            exists = test_engine._registry_exists_in_db(conn)
            assert not exists
    
    def test_ensure_registry_creates_when_missing(self, test_engine):
        """Test registry creation when missing."""
        mock_conn = MockConnection()
        
        # First call fails (registry doesn't exist), subsequent calls succeed
        call_count = 0
        def mock_execute(sql, params=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Table not found")
            # Subsequent calls succeed
        
        mock_conn.execute = mock_execute
        test_engine._create_connection = Mock(return_value=mock_conn)
        
        test_engine.ensure_registry()
        
        assert test_engine._registry_exists is True
    
    def test_get_deployed_changes(self, test_engine):
        """Test getting deployed changes."""
        mock_conn = MockConnection()
        mock_conn.fetch_results = [
            {'change_id': 'abc123'},
            {'change_id': 'def456'}
        ]
        test_engine._create_connection = Mock(return_value=mock_conn)
        test_engine._registry_exists = True
        
        changes = test_engine.get_deployed_changes()
        
        assert len(changes) == 2
        assert changes[0] == 'abc123'
        assert changes[1] == 'def456'
    
    def test_deploy_change(self, test_engine, mock_plan):
        """Test change deployment."""
        # Create mock change
        change = Mock(spec=Change)
        change.name = "test_change"
        change.id = "abc123"
        change.note = "Test change"
        change.planner_name = "Test User"
        change.planner_email = "test@example.com"
        change.timestamp = datetime.now(timezone.utc)
        change.dependencies = []
        change.requires = []
        change.conflicts = []
        change.tags = []
        
        # Plan methods are already mocked in fixture
        
        # Mock connection
        mock_conn = MockConnection()
        test_engine._create_connection = Mock(return_value=mock_conn)
        test_engine._registry_exists = True
        
        # Mock the script hash calculation to avoid file I/O
        test_engine._calculate_script_hash = Mock(return_value="abc123hash")
        
        # Mock file existence and Path.read_bytes
        with patch('pathlib.Path.exists', return_value=True):
            test_engine.deploy_change(change)
        
        assert mock_conn.committed
        assert len(mock_conn.executed_statements) > 0
    
    def test_revert_change(self, test_engine, mock_plan):
        """Test change revert."""
        # Create mock change
        change = Mock(spec=Change)
        change.name = "test_change"
        change.id = "abc123"
        change.note = "Test change"
        change.planner_name = "Test User"
        change.planner_email = "test@example.com"
        change.timestamp = datetime.now(timezone.utc)
        change.dependencies = []
        change.requires = []
        change.conflicts = []
        change.tags = []
        
        # Plan methods are already mocked in fixture
        
        # Mock connection
        mock_conn = MockConnection()
        test_engine._create_connection = Mock(return_value=mock_conn)
        test_engine._registry_exists = True
        
        # Mock file existence
        with patch('pathlib.Path.exists', return_value=True):
            test_engine.revert_change(change)
        
        assert mock_conn.committed
        assert len(mock_conn.executed_statements) > 0
    
    def test_verify_change_success(self, test_engine, mock_plan):
        """Test successful change verification."""
        # Create mock change
        change = Mock(spec=Change)
        change.name = "test_change"
        
        # Plan methods are already mocked in fixture
        
        # Mock connection
        mock_conn = MockConnection()
        test_engine._create_connection = Mock(return_value=mock_conn)
        
        # Mock file existence
        with patch('pathlib.Path.exists', return_value=True):
            result = test_engine.verify_change(change)
        
        assert result is True
    
    def test_verify_change_failure(self, test_engine, mock_plan):
        """Test failed change verification."""
        # Create mock change
        change = Mock(spec=Change)
        change.name = "test_change"
        
        # Plan methods are already mocked in fixture
        
        # Mock connection that raises exception
        mock_conn = MockConnection()
        test_engine._create_connection = Mock(return_value=mock_conn)
        test_engine._execute_sql_file = Mock(side_effect=Exception("Verification failed"))
        
        # Mock file existence
        with patch('pathlib.Path.exists', return_value=True):
            result = test_engine.verify_change(change)
        
        assert result is False


class TestEngineRegistry:
    """Test cases for EngineRegistry."""
    
    def test_register_engine(self):
        """Test engine registration."""
        @register_engine('test')
        class TestEngine(Engine):
            pass
        
        assert EngineRegistry.get_engine_class('test') == TestEngine
    
    def test_get_unsupported_engine(self):
        """Test getting unsupported engine type."""
        with pytest.raises(EngineError) as exc_info:
            EngineRegistry.get_engine_class('unsupported')
        
        assert "Unsupported engine type: unsupported" in str(exc_info.value)
    
    def test_create_engine(self, mock_plan):
        """Test engine creation."""
        @register_engine('pg')  # Override the pg engine for this test
        class TestCreateEngine(Engine):
            @property
            def engine_type(self):
                return 'pg'
            
            @property
            def registry_schema(self):
                return RegistrySchema()
            
            def _create_connection(self):
                return MockConnection()
            
            def _execute_sql_file(self, connection, sql_file, variables=None):
                pass
            
            def _get_registry_version(self, connection):
                return "1.1"
            
            def _regex_condition(self, column: str, pattern: str) -> str:
                """Mock regex condition for testing."""
                return f"{column} ~ ?"
        
        # Create new target with pg engine type
        test_target = Target(
            name="test",
            uri=URI("db:pg://localhost/test"),
            registry="sqitch"
        )
        
        engine = EngineRegistry.create_engine(test_target, mock_plan)
        assert isinstance(engine, TestCreateEngine)
    
    def test_list_supported_engines(self):
        """Test listing supported engines."""
        # Register a test engine
        @register_engine('list_test')
        class ListTestEngine(Engine):
            pass
        
        engines = EngineRegistry.list_supported_engines()
        assert 'list_test' in engines


class TestRegistrySchema:
    """Test cases for RegistrySchema."""
    
    def test_registry_constants(self):
        """Test registry schema constants."""
        schema = RegistrySchema()
        
        assert schema.PROJECTS_TABLE == "projects"
        assert schema.RELEASES_TABLE == "releases"
        assert schema.CHANGES_TABLE == "changes"
        assert schema.TAGS_TABLE == "tags"
        assert schema.DEPENDENCIES_TABLE == "dependencies"
        assert schema.EVENTS_TABLE == "events"
        assert schema.REGISTRY_VERSION == "1.1"
    
    def test_get_create_statements_not_implemented(self):
        """Test that get_create_statements raises NotImplementedError."""
        schema = RegistrySchema()
        
        with pytest.raises(NotImplementedError):
            schema.get_create_statements('pg')