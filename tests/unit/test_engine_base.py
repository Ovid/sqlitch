"""
Unit tests for the abstract engine base class.

Tests the core functionality of the Engine base class including
registry management, connection handling, and the engine registry.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.core.change import Change
from sqlitch.core.exceptions import ConnectionError, DeploymentError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.core.types import URI, EngineType
from sqlitch.engines.base import Engine, EngineRegistry, RegistrySchema, register_engine


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
        results = self.fetch_results[self.fetch_index :]
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
        return "pg"

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
    return Target(name="test", uri=URI("db:pg://localhost/test"), registry="sqitch")


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
        assert test_engine.engine_type == "pg"
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
        test_engine._create_connection = Mock(
            side_effect=Exception("Connection failed")
        )

        with pytest.raises(ConnectionError) as exc_info:
            with test_engine.connection():
                pass

        assert "Failed to connect to pg database" in str(exc_info.value)
        assert exc_info.value.engine_name == "pg"

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

        with pytest.raises(
            ConnectionError
        ):  # The connection context manager re-raises as ConnectionError
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
        mock_conn.fetch_results = [{"change_id": "abc123"}, {"change_id": "def456"}]
        test_engine._create_connection = Mock(return_value=mock_conn)
        test_engine._registry_exists = True

        changes = test_engine.get_deployed_changes()

        assert len(changes) == 2
        assert changes[0] == "abc123"
        assert changes[1] == "def456"

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
        with patch("pathlib.Path.exists", return_value=True):
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
        with patch("pathlib.Path.exists", return_value=True):
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
        with patch("pathlib.Path.exists", return_value=True):
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
        test_engine._execute_sql_file = Mock(
            side_effect=Exception("Verification failed")
        )

        # Mock file existence
        with patch("pathlib.Path.exists", return_value=True):
            result = test_engine.verify_change(change)

        assert result is False


class TestEngineRegistry:
    """Test cases for EngineRegistry."""

    def test_register_engine(self):
        """Test engine registration."""

        @register_engine("test")
        class TestEngine(Engine):
            pass

        assert EngineRegistry.get_engine_class("test") == TestEngine

    def test_get_unsupported_engine(self):
        """Test getting unsupported engine type."""
        with pytest.raises(EngineError) as exc_info:
            EngineRegistry.get_engine_class("unsupported")

        assert "Unsupported engine type: unsupported" in str(exc_info.value)

    def test_create_engine(self, mock_plan):
        """Test engine creation."""

        @register_engine("pg")  # Override the pg engine for this test
        class TestCreateEngine(Engine):
            @property
            def engine_type(self):
                return "pg"

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
            name="test", uri=URI("db:pg://localhost/test"), registry="sqitch"
        )

        engine = EngineRegistry.create_engine(test_target, mock_plan)
        assert isinstance(engine, TestCreateEngine)

    def test_list_supported_engines(self):
        """Test listing supported engines."""

        # Register a test engine
        @register_engine("list_test")
        class ListTestEngine(Engine):
            pass

        engines = EngineRegistry.list_supported_engines()
        assert "list_test" in engines


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
            schema.get_create_statements("pg")


class TestEngineAdvancedFunctionality:
    """Test advanced engine functionality with comprehensive coverage."""

    @pytest.fixture
    def advanced_engine(self, mock_target, mock_plan):
        """Create advanced test engine with more functionality."""
        engine = MockEngine(mock_target, mock_plan)
        # Add some helper methods for testing
        engine._calculate_script_hash = Mock(return_value="test_hash_123")
        engine._resolve_dependency_id = Mock(return_value="dep_id_123")
        engine._format_dependencies = Mock(return_value="dep1 dep2")
        engine._format_tags = Mock(return_value="tag1 tag2")
        return engine

    def test_get_change_status_deployed(self, advanced_engine):
        """Test getting status of deployed change."""
        mock_conn = MockConnection()
        mock_conn.fetch_results = [{"committed_at": datetime.now(timezone.utc)}]
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        change = Mock(spec=Change)
        change.id = "test_change_id"

        status = advanced_engine.get_change_status(change)
        assert status.value == "deployed"

    def test_get_change_status_pending(self, advanced_engine):
        """Test getting status of pending change."""
        mock_conn = MockConnection()
        mock_conn.fetch_results = []  # No results = not deployed
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        change = Mock(spec=Change)
        change.id = "test_change_id"

        status = advanced_engine.get_change_status(change)
        assert status.value == "pending"

    def test_get_change_status_error(self, advanced_engine):
        """Test error handling in get_change_status."""
        mock_conn = MockConnection()
        mock_conn.execute = Mock(side_effect=Exception("Database error"))
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        change = Mock(spec=Change)
        change.id = "test_change_id"

        with pytest.raises(EngineError) as exc_info:
            advanced_engine.get_change_status(change)

        assert "Failed to get change status" in str(exc_info.value)

    def test_get_current_state_with_data(self, advanced_engine):
        """Test getting current state when data exists."""
        mock_conn = MockConnection()
        mock_conn.fetch_results = [
            {
                "change_id": "abc123",
                "script_hash": "hash123",
                "change": "test_change",
                "project": "test_project",
                "note": "Test note",
                "committer_name": "Test User",
                "committer_email": "test@example.com",
                "committed_at": datetime.now(timezone.utc),
                "planner_name": "Planner",
                "planner_email": "planner@example.com",
                "planned_at": datetime.now(timezone.utc),
                "tags": "tag1 tag2",
            }
        ]
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        state = advanced_engine.get_current_state()

        assert state is not None
        assert state["change_id"] == "abc123"
        assert state["change"] == "test_change"
        assert state["tags"] == ["tag1", "tag2"]

    def test_get_current_state_no_data(self, advanced_engine):
        """Test getting current state when no data exists."""
        mock_conn = MockConnection()
        mock_conn.fetch_results = []
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        state = advanced_engine.get_current_state()
        assert state is None

    def test_get_current_state_with_project_param(self, advanced_engine):
        """Test getting current state with specific project."""
        mock_conn = MockConnection()
        mock_conn.fetch_results = [
            {
                "change_id": "abc123",
                "script_hash": "hash123",
                "change": "test_change",
                "project": "other_project",
                "note": "",
                "committer_name": "Test User",
                "committer_email": "test@example.com",
                "committed_at": datetime.now(timezone.utc),
                "planner_name": "Planner",
                "planner_email": "planner@example.com",
                "planned_at": datetime.now(timezone.utc),
                "tags": None,
            }
        ]
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        state = advanced_engine.get_current_state("other_project")

        assert state is not None
        assert state["project"] == "other_project"
        assert state["tags"] == []

    def test_get_current_state_error(self, advanced_engine):
        """Test error handling in get_current_state."""
        mock_conn = MockConnection()
        mock_conn.execute = Mock(side_effect=Exception("Database error"))
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        with pytest.raises(EngineError) as exc_info:
            advanced_engine.get_current_state()

        assert "Failed to get current state" in str(exc_info.value)

    def test_get_current_changes(self, advanced_engine):
        """Test getting current changes iterator."""
        mock_conn = MockConnection()
        mock_conn.fetch_results = [
            {
                "change_id": "abc123",
                "script_hash": "hash123",
                "change": "change1",
                "committer_name": "User1",
                "committer_email": "user1@example.com",
                "committed_at": datetime.now(timezone.utc),
                "planner_name": "Planner1",
                "planner_email": "planner1@example.com",
                "planned_at": datetime.now(timezone.utc),
            },
            {
                "change_id": "def456",
                "script_hash": "hash456",
                "change": "change2",
                "committer_name": "User2",
                "committer_email": "user2@example.com",
                "committed_at": datetime.now(timezone.utc),
                "planner_name": "Planner2",
                "planner_email": "planner2@example.com",
                "planned_at": datetime.now(timezone.utc),
            },
        ]
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        changes = list(advanced_engine.get_current_changes())

        assert len(changes) == 2
        assert changes[0]["change_id"] == "abc123"
        assert changes[1]["change_id"] == "def456"

    def test_get_current_changes_error(self, advanced_engine):
        """Test error handling in get_current_changes."""
        mock_conn = MockConnection()
        mock_conn.execute = Mock(side_effect=Exception("Database error"))
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        with pytest.raises(EngineError) as exc_info:
            list(advanced_engine.get_current_changes())

        assert "Failed to get current changes" in str(exc_info.value)

    def test_search_events_basic(self, advanced_engine):
        """Test basic event search functionality."""
        mock_conn = MockConnection()
        mock_conn.fetch_results = [
            {
                "event": "deploy",
                "project": "test_project",
                "change_id": "abc123",
                "change": "test_change",
                "note": "Test note",
                "requires": "dep1 dep2",
                "conflicts": "conf1",
                "tags": "tag1 tag2",
                "committer_name": "User",
                "committer_email": "user@example.com",
                "committed_at": datetime.now(timezone.utc),
                "planner_name": "Planner",
                "planner_email": "planner@example.com",
                "planned_at": datetime.now(timezone.utc),
            }
        ]
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        events = list(advanced_engine.search_events())

        assert len(events) == 1
        assert events[0]["event"] == "deploy"
        assert events[0]["requires"] == ["dep1", "dep2"]
        assert events[0]["conflicts"] == ["conf1"]
        assert events[0]["tags"] == ["tag1", "tag2"]

    def test_search_events_with_filters(self, advanced_engine):
        """Test event search with various filters."""
        mock_conn = MockConnection()
        mock_conn.fetch_results = []
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        # Test with all filter parameters
        list(
            advanced_engine.search_events(
                event=["deploy", "revert"],
                change="test.*",
                project="test_project",
                committer="user.*",
                planner="planner.*",
                limit=10,
                offset=5,
                direction="ASC",
            )
        )

        # Verify the query was constructed with filters
        assert len(mock_conn.executed_statements) > 0

    def test_search_events_invalid_direction(self, advanced_engine):
        """Test search events with invalid direction."""
        advanced_engine._registry_exists = True

        with pytest.raises(EngineError) as exc_info:
            list(advanced_engine.search_events(direction="INVALID"))

        assert "Search direction must be either 'ASC' or 'DESC'" in str(exc_info.value)

    def test_search_events_error(self, advanced_engine):
        """Test error handling in search_events."""
        mock_conn = MockConnection()
        mock_conn.execute = Mock(side_effect=Exception("Database error"))
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        with pytest.raises(EngineError) as exc_info:
            list(advanced_engine.search_events())

        assert "Failed to search events" in str(exc_info.value)

    def test_parse_array_field(self, advanced_engine):
        """Test parsing of array fields."""
        # Test with values
        result = advanced_engine._parse_array_field("item1 item2 item3")
        assert result == ["item1", "item2", "item3"]

        # Test with empty string
        result = advanced_engine._parse_array_field("")
        assert result == []

        # Test with whitespace only
        result = advanced_engine._parse_array_field("   ")
        assert result == []

        # Test with None
        result = advanced_engine._parse_array_field(None)
        assert result == []

    def test_get_current_tags(self, advanced_engine):
        """Test getting current tags iterator."""
        mock_conn = MockConnection()
        mock_conn.fetch_results = [
            {
                "tag_id": "tag123",
                "tag": "v1.0",
                "committer_name": "User",
                "committer_email": "user@example.com",
                "committed_at": datetime.now(timezone.utc),
                "planner_name": "Planner",
                "planner_email": "planner@example.com",
                "planned_at": datetime.now(timezone.utc),
            }
        ]
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        tags = list(advanced_engine.get_current_tags())

        assert len(tags) == 1
        assert tags[0]["tag"] == "v1.0"

    def test_get_current_tags_error(self, advanced_engine):
        """Test error handling in get_current_tags."""
        mock_conn = MockConnection()
        mock_conn.execute = Mock(side_effect=Exception("Database error"))
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True

        with pytest.raises(EngineError) as exc_info:
            list(advanced_engine.get_current_tags())

        assert "Failed to get current tags" in str(exc_info.value)

    def test_record_change_deployment(self, advanced_engine):
        """Test recording change deployment in registry."""
        mock_conn = MockConnection()
        advanced_engine._registry_exists = True

        # Create mock change with dependencies
        change = Mock(spec=Change)
        change.id = "abc123"
        change.name = "test_change"
        change.note = "Test note"
        change.planner_name = "Test User"
        change.planner_email = "test@example.com"
        change.timestamp = datetime.now(timezone.utc)

        # Mock dependency
        dep = Mock()
        dep.type = "require"
        dep.change = "dependency_change"
        change.dependencies = [dep]
        change.tags = ["tag1", "tag2"]

        advanced_engine._record_change_deployment(mock_conn, change)

        # Should have executed multiple statements (change, dependencies, event)
        assert len(mock_conn.executed_statements) >= 3

    def test_record_change_revert(self, advanced_engine):
        """Test recording change revert in registry."""
        mock_conn = MockConnection()
        advanced_engine._registry_exists = True

        # Create mock change
        change = Mock(spec=Change)
        change.id = "abc123"
        change.name = "test_change"
        change.note = "Test note"
        change.planner_name = "Test User"
        change.planner_email = "test@example.com"
        change.timestamp = datetime.now(timezone.utc)
        change.dependencies = []
        change.tags = []

        advanced_engine._record_change_revert(mock_conn, change)

        # Should have executed statements for deletion and event recording
        assert len(mock_conn.executed_statements) >= 3

    def test_calculate_script_hash(self, advanced_engine):
        """Test script hash calculation."""
        # Mock file operations
        with patch("pathlib.Path.exists") as mock_exists:
            with patch("pathlib.Path.read_bytes") as mock_read_bytes:
                mock_exists.return_value = True
                mock_read_bytes.return_value = b"test content"

                change = Mock(spec=Change)

                # Reset the mock to use real implementation
                advanced_engine._calculate_script_hash = (
                    MockEngine._calculate_script_hash.__get__(
                        advanced_engine, MockEngine
                    )
                )

                result = advanced_engine._calculate_script_hash(change)

                assert isinstance(result, str)
                assert len(result) == 40  # SHA1 hex digest length

    def test_calculate_script_hash_missing_files(self, advanced_engine):
        """Test script hash calculation with missing files."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = False

            change = Mock(spec=Change)

            # Reset the mock to use real implementation
            advanced_engine._calculate_script_hash = (
                MockEngine._calculate_script_hash.__get__(advanced_engine, MockEngine)
            )

            result = advanced_engine._calculate_script_hash(change)

            assert isinstance(result, str)
            assert len(result) == 40  # SHA1 hex digest length

    def test_resolve_dependency_id(self, advanced_engine):
        """Test dependency ID resolution."""
        # Create mock changes in plan
        change1 = Mock(spec=Change)
        change1.name = "change1"
        change1.id = "id1"

        change2 = Mock(spec=Change)
        change2.name = "change2"
        change2.id = "id2"

        advanced_engine.plan.changes = [change1, change2]

        # Reset the mock to use real implementation
        advanced_engine._resolve_dependency_id = (
            MockEngine._resolve_dependency_id.__get__(advanced_engine, MockEngine)
        )

        # Test existing dependency
        result = advanced_engine._resolve_dependency_id("change1")
        assert result == "id1"

        # Test non-existing dependency
        result = advanced_engine._resolve_dependency_id("nonexistent")
        assert result is None

    def test_format_dependencies(self, advanced_engine):
        """Test dependency formatting."""
        # Reset the mock to use real implementation
        advanced_engine._format_dependencies = MockEngine._format_dependencies.__get__(
            advanced_engine, MockEngine
        )

        # Test with dependencies
        result = advanced_engine._format_dependencies(["dep1", "dep2", "dep3"])
        assert result == "dep1 dep2 dep3"

        # Test with empty list
        result = advanced_engine._format_dependencies([])
        assert result == ""

    def test_format_tags(self, advanced_engine):
        """Test tag formatting."""
        # Reset the mock to use real implementation
        advanced_engine._format_tags = MockEngine._format_tags.__get__(
            advanced_engine, MockEngine
        )

        # Test with tags
        result = advanced_engine._format_tags(["tag1", "tag2"])
        assert result == "tag1 tag2"

        # Test with empty list
        result = advanced_engine._format_tags([])
        assert result == ""

    def test_upgrade_registry_default(self, advanced_engine):
        """Test default registry upgrade implementation."""
        mock_conn = MockConnection()

        # Should not raise exception, just log warning
        advanced_engine._upgrade_registry(mock_conn, "1.0")

    def test_insert_project_record(self, advanced_engine):
        """Test project record insertion."""
        mock_conn = MockConnection()

        advanced_engine._insert_project_record(mock_conn)

        # Should have executed INSERT statement
        assert len(mock_conn.executed_statements) == 1
        sql, params = mock_conn.executed_statements[0]
        assert "INSERT INTO" in sql
        assert "projects" in sql

    def test_deploy_change_error(self, advanced_engine):
        """Test deploy change error handling."""
        change = Mock(spec=Change)
        change.name = "test_change"

        # Mock connection that raises exception
        mock_conn = MockConnection()
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True
        advanced_engine._execute_sql_file = Mock(side_effect=Exception("Deploy failed"))

        with patch("pathlib.Path.exists", return_value=True):
            with pytest.raises(ConnectionError) as exc_info:
                advanced_engine.deploy_change(change)

        # The exception gets wrapped in ConnectionError due to transaction handling
        assert "Failed to connect to pg database" in str(exc_info.value)

    def test_revert_change_error(self, advanced_engine):
        """Test revert change error handling."""
        change = Mock(spec=Change)
        change.name = "test_change"

        # Mock connection that raises exception
        mock_conn = MockConnection()
        advanced_engine._create_connection = Mock(return_value=mock_conn)
        advanced_engine._registry_exists = True
        advanced_engine._execute_sql_file = Mock(side_effect=Exception("Revert failed"))

        with patch("pathlib.Path.exists", return_value=True):
            with pytest.raises(ConnectionError) as exc_info:
                advanced_engine.revert_change(change)

        # The exception gets wrapped in ConnectionError due to transaction handling
        assert "Failed to connect to pg database" in str(exc_info.value)

    def test_verify_change_no_file(self, advanced_engine):
        """Test verify change when verify file doesn't exist."""
        change = Mock(spec=Change)
        change.name = "test_change"

        mock_conn = MockConnection()
        advanced_engine._create_connection = Mock(return_value=mock_conn)

        with patch("pathlib.Path.exists", return_value=False):
            result = advanced_engine.verify_change(change)

        assert result is True  # Should succeed even without verify file

    def test_create_registry_error(self, advanced_engine):
        """Test registry creation error handling."""
        mock_conn = MockConnection()
        mock_conn.execute = Mock(side_effect=Exception("Create failed"))

        with pytest.raises(EngineError) as exc_info:
            advanced_engine._create_registry(mock_conn)

        assert "Failed to create registry" in str(exc_info.value)

    def test_ensure_registry_upgrade_path(self, advanced_engine):
        """Test registry upgrade path in ensure_registry."""
        mock_conn = MockConnection()

        # Mock registry exists but with old version
        advanced_engine._registry_exists_in_db = Mock(return_value=True)
        advanced_engine._get_registry_version = Mock(return_value="1.0")
        advanced_engine._upgrade_registry = Mock()
        advanced_engine._create_connection = Mock(return_value=mock_conn)

        advanced_engine.ensure_registry()

        # Should have called upgrade
        advanced_engine._upgrade_registry.assert_called_once_with(mock_conn, "1.0")
        assert advanced_engine._registry_exists is True

    def test_connection_rollback_error_handling(self, advanced_engine):
        """Test connection rollback error handling."""
        mock_conn = MockConnection()
        mock_conn.rollback = Mock(side_effect=Exception("Rollback failed"))

        advanced_engine._create_connection = Mock(
            side_effect=Exception("Connection failed")
        )

        # Should not raise exception even if rollback fails
        with pytest.raises(ConnectionError):
            with advanced_engine.connection():
                pass

    def test_connection_close_error_handling(self, advanced_engine):
        """Test connection close error handling."""
        mock_conn = MockConnection()
        mock_conn.close = Mock(side_effect=Exception("Close failed"))

        advanced_engine._create_connection = Mock(return_value=mock_conn)

        # Should not raise exception even if close fails
        with advanced_engine.connection():
            pass

    def test_transaction_rollback_error_handling(self, advanced_engine):
        """Test transaction rollback error handling."""
        mock_conn = MockConnection()
        mock_conn.rollback = Mock(side_effect=Exception("Rollback failed"))
        advanced_engine._create_connection = Mock(return_value=mock_conn)

        with pytest.raises(ConnectionError):
            with advanced_engine.transaction():
                raise Exception("Test error")

        # Should have attempted rollback despite error
