"""
Unit tests for PostgreSQL database engine.

Tests the PostgreSQL-specific implementation including connection handling,
registry management, SQL execution, and change operations.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from sqlitch.core.change import Change, Dependency
from sqlitch.core.exceptions import ConnectionError, DeploymentError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.core.types import URI
from sqlitch.engines.pg import (
    PostgreSQLConnection,
    PostgreSQLEngine,
    PostgreSQLRegistrySchema,
)


class MockPsycopg2Connection:
    """Mock psycopg2 connection for testing."""

    def __init__(self):
        self.autocommit = False
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.cursors = []

    def cursor(self, cursor_factory=None):
        cursor = MockPsycopg2Cursor()
        cursor.cursor_factory = cursor_factory
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class MockPsycopg2Cursor:
    """Mock psycopg2 cursor for testing."""

    def __init__(self):
        self.executed_statements = []
        self.fetch_results = []
        self.fetch_index = 0
        self.closed = False
        self.cursor_factory = None

    def execute(self, sql, params=None):
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

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def mock_psycopg2():
    """Mock psycopg2 module."""
    with patch("sqlitch.engines.pg.psycopg2") as mock_pg:
        mock_pg.connect = Mock(return_value=MockPsycopg2Connection())
        mock_pg.Error = Exception
        mock_pg.extras = Mock()
        mock_pg.extras.RealDictCursor = Mock()
        mock_pg.extensions = Mock()
        yield mock_pg


@pytest.fixture
def pg_target():
    """Create PostgreSQL target for testing."""
    return Target(
        name="test_pg",
        uri=URI("db:pg://user:pass@localhost:5432/testdb"),
        registry="sqitch",
    )


@pytest.fixture
def mock_plan():
    """Create mock plan for testing."""
    plan = Mock(spec=Plan)
    plan.project_name = "test_project"
    plan.creator_name = "Test User"
    plan.creator_email = "test@example.com"
    plan.changes = []

    # Mock file path methods
    def mock_get_deploy_file(change):
        return Path(f"/fake/deploy/{change.name}.sql")

    def mock_get_revert_file(change):
        return Path(f"/fake/revert/{change.name}.sql")

    def mock_get_verify_file(change):
        return Path(f"/fake/verify/{change.name}.sql")

    plan.get_deploy_file = mock_get_deploy_file
    plan.get_revert_file = mock_get_revert_file
    plan.get_verify_file = mock_get_verify_file

    return plan


@pytest.fixture
def pg_engine(mock_psycopg2, pg_target, mock_plan):
    """Create PostgreSQL engine instance."""
    return PostgreSQLEngine(pg_target, mock_plan)


@pytest.fixture
def test_change():
    """Create test change for testing."""
    return Change(
        name="test_change",
        note="Test change for unit tests",
        timestamp=datetime(2023, 1, 15, 10, 30, 0),
        planner_name="Test User",
        planner_email="test@example.com",
        tags=["v1.0"],
        dependencies=[Dependency(type="require", change="initial_schema")],
        conflicts=[],
    )


class TestPostgreSQLRegistrySchema:
    """Test PostgreSQL registry schema."""

    def test_get_create_statements(self):
        """Test PostgreSQL registry creation statements."""
        statements = PostgreSQLRegistrySchema.get_create_statements("pg")

        assert len(statements) > 0
        assert any("CREATE SCHEMA IF NOT EXISTS sqitch" in stmt for stmt in statements)
        assert any(
            "CREATE TABLE IF NOT EXISTS sqitch.projects" in stmt for stmt in statements
        )
        assert any(
            "CREATE TABLE IF NOT EXISTS sqitch.changes" in stmt for stmt in statements
        )
        assert any(
            "CREATE TABLE IF NOT EXISTS sqitch.tags" in stmt for stmt in statements
        )
        assert any(
            "CREATE TABLE IF NOT EXISTS sqitch.dependencies" in stmt
            for stmt in statements
        )
        assert any(
            "CREATE TABLE IF NOT EXISTS sqitch.events" in stmt for stmt in statements
        )
        assert any("INSERT INTO sqitch.releases" in stmt for stmt in statements)


class TestPostgreSQLConnection:
    """Test PostgreSQL connection wrapper."""

    def test_connection_initialization(self, mock_psycopg2):
        """Test connection wrapper initialization."""
        mock_conn = MockPsycopg2Connection()
        pg_conn = PostgreSQLConnection(mock_conn)

        assert pg_conn._connection == mock_conn
        assert pg_conn._cursor is None

    def test_execute_sql(self, mock_psycopg2):
        """Test SQL execution."""
        mock_conn = MockPsycopg2Connection()
        pg_conn = PostgreSQLConnection(mock_conn)

        pg_conn.execute("SELECT 1", {"param": "value"})

        cursor = mock_conn.cursors[0]
        assert len(cursor.executed_statements) == 1
        assert cursor.executed_statements[0] == ("SELECT 1", {"param": "value"})

    def test_execute_sql_without_params(self, mock_psycopg2):
        """Test SQL execution without parameters."""
        mock_conn = MockPsycopg2Connection()
        pg_conn = PostgreSQLConnection(mock_conn)

        pg_conn.execute("SELECT 1")

        cursor = mock_conn.cursors[0]
        assert len(cursor.executed_statements) == 1
        assert cursor.executed_statements[0] == ("SELECT 1", None)

    def test_fetchone(self, mock_psycopg2):
        """Test fetching one row."""
        mock_conn = MockPsycopg2Connection()
        pg_conn = PostgreSQLConnection(mock_conn)

        # Mock cursor with result
        cursor = pg_conn._get_cursor()
        cursor.fetch_results = [{"id": 1, "name": "test"}]

        result = pg_conn.fetchone()
        assert result == {"id": 1, "name": "test"}

    def test_fetchall(self, mock_psycopg2):
        """Test fetching all rows."""
        mock_conn = MockPsycopg2Connection()
        pg_conn = PostgreSQLConnection(mock_conn)

        # Mock cursor with results
        cursor = pg_conn._get_cursor()
        cursor.fetch_results = [{"id": 1}, {"id": 2}]

        results = pg_conn.fetchall()
        assert results == [{"id": 1}, {"id": 2}]

    def test_commit(self, mock_psycopg2):
        """Test transaction commit."""
        mock_conn = MockPsycopg2Connection()
        pg_conn = PostgreSQLConnection(mock_conn)

        pg_conn.commit()
        assert mock_conn.committed

    def test_rollback(self, mock_psycopg2):
        """Test transaction rollback."""
        mock_conn = MockPsycopg2Connection()
        pg_conn = PostgreSQLConnection(mock_conn)

        pg_conn.rollback()
        assert mock_conn.rolled_back

    def test_close(self, mock_psycopg2):
        """Test connection close."""
        mock_conn = MockPsycopg2Connection()
        pg_conn = PostgreSQLConnection(mock_conn)

        # Create cursor first
        cursor = pg_conn._get_cursor()

        pg_conn.close()
        assert mock_conn.closed
        assert cursor.closed
        assert pg_conn._cursor is None


class TestPostgreSQLEngine:
    """Test PostgreSQL engine implementation."""

    def test_engine_initialization(self, mock_psycopg2, pg_target, mock_plan):
        """Test PostgreSQL engine initialization."""
        engine = PostgreSQLEngine(pg_target, mock_plan)

        assert engine.target == pg_target
        assert engine.plan == mock_plan
        assert engine.engine_type == "pg"
        assert engine._registry_schema_name == "sqitch"
        assert "host" in engine._connection_params
        assert "database" in engine._connection_params

    def test_initialization_without_psycopg2(self, pg_target, mock_plan):
        """Test engine initialization fails without psycopg2."""
        with patch("sqlitch.engines.pg.psycopg2", None):
            with pytest.raises(EngineError) as exc_info:
                PostgreSQLEngine(pg_target, mock_plan)

            assert "psycopg2 is required" in str(exc_info.value)
            assert exc_info.value.engine_name == "pg"

    def test_parse_connection_string_full_uri(self, mock_psycopg2, mock_plan):
        """Test parsing full PostgreSQL URI."""
        target = Target(
            name="test",
            uri=URI("db:pg://user:pass@host:5433/mydb?sslmode=require"),
            registry="sqitch",
        )

        engine = PostgreSQLEngine(target, mock_plan)
        params = engine._connection_params

        assert params["host"] == "host"
        assert params["port"] == 5433
        assert params["database"] == "mydb"
        assert params["user"] == "user"
        assert params["password"] == "pass"
        assert params["sslmode"] == "require"

    def test_parse_connection_string_minimal(self, mock_psycopg2, mock_plan):
        """Test parsing minimal PostgreSQL URI."""
        target = Target(name="test", uri=URI("db:pg:///mydb"), registry="sqitch")

        engine = PostgreSQLEngine(target, mock_plan)
        params = engine._connection_params

        assert params["host"] == "localhost"
        assert params["port"] == 5432
        assert params["database"] == "mydb"
        assert "user" not in params
        assert "password" not in params

    def test_create_connection_success(self, pg_engine, mock_psycopg2):
        """Test successful connection creation."""
        mock_conn = MockPsycopg2Connection()
        mock_psycopg2.connect.return_value = mock_conn

        pg_conn = pg_engine._create_connection()

        assert isinstance(pg_conn, PostgreSQLConnection)
        assert not mock_conn.autocommit
        mock_psycopg2.connect.assert_called_once()

    def test_create_connection_failure(self, pg_engine, mock_psycopg2):
        """Test connection creation failure."""
        mock_psycopg2.connect.side_effect = mock_psycopg2.Error("Connection failed")

        with pytest.raises(ConnectionError) as exc_info:
            pg_engine._create_connection()

        assert "Failed to connect to PostgreSQL database" in str(exc_info.value)
        assert exc_info.value.engine_name == "pg"

    def test_execute_sql_file_success(self, pg_engine):
        """Test successful SQL file execution."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        sql_file = Path("/fake/test.sql")

        # Mock file content
        sql_content = """
        -- Comment
        CREATE TABLE test (id INTEGER);

        INSERT INTO test VALUES (1);
        """

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("pathlib.Path.read_text", return_value=sql_content),
        ):

            pg_engine._execute_sql_file(mock_conn, sql_file)

            # Should execute non-comment statements
            assert mock_conn.execute.call_count >= 2

    def test_execute_sql_file_with_variables(self, pg_engine):
        """Test SQL file execution with variable substitution."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        sql_file = Path("/fake/test.sql")
        variables = {"table_name": "users", "schema": "public"}

        sql_content = "CREATE TABLE :schema.:table_name (id INTEGER);"

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("pathlib.Path.read_text", return_value=sql_content),
        ):

            pg_engine._execute_sql_file(mock_conn, sql_file, variables)

            # Check that variables were substituted
            mock_conn.execute.assert_called_with(
                "CREATE TABLE public.users (id INTEGER);"
            )

    def test_execute_sql_file_not_found(self, pg_engine):
        """Test SQL file execution with missing file."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        sql_file = Path("/fake/missing.sql")

        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(DeploymentError) as exc_info:
                pg_engine._execute_sql_file(mock_conn, sql_file)

            assert "SQL file not found" in str(exc_info.value)
            assert exc_info.value.engine_name == "pg"

    def test_split_sql_statements(self, pg_engine):
        """Test SQL statement splitting."""
        sql_content = """
        -- This is a comment
        CREATE TABLE test (id INTEGER);

        INSERT INTO test VALUES (1);
        INSERT INTO test VALUES (2);

        -- Another comment
        SELECT * FROM test;
        """

        statements = pg_engine._split_sql_statements(sql_content)

        # Should have 4 statements (excluding comments and empty lines)
        assert len(statements) == 4
        assert "CREATE TABLE test (id INTEGER);" in statements
        assert "INSERT INTO test VALUES (1);" in statements
        assert "INSERT INTO test VALUES (2);" in statements
        assert "SELECT * FROM test;" in statements

    def test_get_registry_version_success(self, pg_engine):
        """Test getting registry version."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        mock_conn.fetchone.return_value = {"version": "1.1"}

        version = pg_engine._get_registry_version(mock_conn)

        assert version == "1.1"
        mock_conn.execute.assert_called_once()

    def test_get_registry_version_not_found(self, pg_engine):
        """Test getting registry version when not found."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        mock_conn.fetchone.return_value = None

        version = pg_engine._get_registry_version(mock_conn)

        assert version is None

    def test_registry_exists_check_true(self, pg_engine):
        """Test registry existence check returns True."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        mock_conn.fetchone.side_effect = [
            {"schema_name": "sqitch"},  # Schema exists
            {"count": 1},  # Table exists
        ]

        exists = pg_engine._registry_exists_in_db(mock_conn)

        assert exists is True
        assert mock_conn.execute.call_count == 2

    def test_registry_exists_check_false_no_schema(self, pg_engine):
        """Test registry existence check returns False when schema missing."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        mock_conn.fetchone.return_value = None  # Schema doesn't exist

        exists = pg_engine._registry_exists_in_db(mock_conn)

        assert exists is False
        assert mock_conn.execute.call_count == 1

    def test_registry_exists_check_false_exception(self, pg_engine):
        """Test registry existence check returns False on exception."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        mock_conn.execute.side_effect = Exception("Table not found")

        exists = pg_engine._registry_exists_in_db(mock_conn)

        assert exists is False

    def test_create_registry_success(self, pg_engine):
        """Test successful registry creation."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        mock_conn.fetchone.return_value = None  # Project doesn't exist yet

        pg_engine._create_registry(mock_conn)

        # Should execute multiple CREATE statements plus INSERT
        assert mock_conn.execute.call_count > 5

    def test_create_registry_custom_schema(self, mock_psycopg2, mock_plan):
        """Test registry creation with custom schema name."""
        target = Target(
            name="test", uri=URI("db:pg://localhost/test"), registry="custom_schema"
        )

        engine = PostgreSQLEngine(target, mock_plan)
        mock_conn = Mock(spec=PostgreSQLConnection)
        mock_conn.fetchone.return_value = None

        engine._create_registry(mock_conn)

        # Check that custom schema name is used
        executed_sql = [call[0][0] for call in mock_conn.execute.call_args_list]
        assert any("custom_schema" in sql for sql in executed_sql)

    def test_get_deployed_changes(self, pg_engine):
        """Test getting deployed changes."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        mock_conn.fetchall.return_value = [
            {"change_id": "abc123"},
            {"change_id": "def456"},
        ]

        pg_engine._registry_exists = True

        with patch.object(pg_engine, "connection") as mock_context:
            mock_context.return_value.__enter__.return_value = mock_conn

            changes = pg_engine.get_deployed_changes()

        assert changes == ["abc123", "def456"]
        mock_conn.execute.assert_called_once()

    def test_deploy_change_success(self, pg_engine, test_change):
        """Test successful change deployment."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        pg_engine._registry_exists = True

        # Mock file operations
        with (
            patch.object(pg_engine, "transaction") as mock_transaction,
            patch.object(pg_engine, "_execute_sql_file") as mock_execute_file,
            patch.object(pg_engine, "_calculate_script_hash", return_value="hash123"),
            patch.object(pg_engine, "_resolve_dependency_id", return_value="dep123"),
            patch("pathlib.Path.exists", return_value=True),
        ):

            mock_transaction.return_value.__enter__.return_value = mock_conn

            pg_engine.deploy_change(test_change)

            # Should execute SQL file and record deployment
            mock_execute_file.assert_called_once()
            assert mock_conn.execute.call_count >= 3  # change, dependencies, events

    def test_revert_change_success(self, pg_engine, test_change):
        """Test successful change revert."""
        mock_conn = Mock(spec=PostgreSQLConnection)
        pg_engine._registry_exists = True

        with (
            patch.object(pg_engine, "transaction") as mock_transaction,
            patch.object(pg_engine, "_execute_sql_file") as mock_execute_file,
            patch("pathlib.Path.exists", return_value=True),
        ):

            mock_transaction.return_value.__enter__.return_value = mock_conn

            pg_engine.revert_change(test_change)

            # Should execute SQL file and record revert
            mock_execute_file.assert_called_once()
            assert mock_conn.execute.call_count >= 2  # delete change, insert event

    def test_verify_change_success(self, pg_engine, test_change):
        """Test successful change verification."""
        mock_conn = Mock(spec=PostgreSQLConnection)

        with (
            patch.object(pg_engine, "connection") as mock_context,
            patch.object(pg_engine, "_execute_sql_file") as mock_execute_file,
            patch("pathlib.Path.exists", return_value=True),
        ):

            mock_context.return_value.__enter__.return_value = mock_conn

            result = pg_engine.verify_change(test_change)

            assert result is True
            mock_execute_file.assert_called_once()

    def test_verify_change_failure(self, pg_engine, test_change):
        """Test failed change verification."""
        mock_conn = Mock(spec=PostgreSQLConnection)

        with (
            patch.object(pg_engine, "connection") as mock_context,
            patch.object(
                pg_engine,
                "_execute_sql_file",
                side_effect=Exception("Verification failed"),
            ),
            patch("pathlib.Path.exists", return_value=True),
        ):

            mock_context.return_value.__enter__.return_value = mock_conn

            result = pg_engine.verify_change(test_change)

            assert result is False

    def test_record_change_deployment(self, pg_engine, test_change):
        """Test recording change deployment."""
        mock_conn = Mock(spec=PostgreSQLConnection)

        with (
            patch.object(pg_engine, "_calculate_script_hash", return_value="hash123"),
            patch.object(pg_engine, "_resolve_dependency_id", return_value="dep123"),
        ):

            pg_engine._record_change_deployment(mock_conn, test_change)

            # Should insert into changes, dependencies, and events tables
            assert mock_conn.execute.call_count == 3

    def test_record_change_revert(self, pg_engine, test_change):
        """Test recording change revert."""
        mock_conn = Mock(spec=PostgreSQLConnection)

        pg_engine._record_change_revert(mock_conn, test_change)

        # Should delete from changes and insert into events
        assert mock_conn.execute.call_count == 2

        # Check that first call is DELETE
        delete_call = mock_conn.execute.call_args_list[0]
        assert "DELETE FROM" in delete_call[0][0]

        # Check that second call is INSERT into events
        insert_call = mock_conn.execute.call_args_list[1]
        assert "INSERT INTO" in insert_call[0][0]
        assert "events" in insert_call[0][0]


class TestPostgreSQLEngineIntegration:
    """Integration tests for PostgreSQL engine."""

    def test_full_registry_setup(self, pg_engine):
        """Test complete registry setup process."""
        mock_conn = Mock(spec=PostgreSQLConnection)

        # First call to check existence returns False
        # Subsequent calls succeed
        mock_conn.fetchone.side_effect = [
            None,  # Schema doesn't exist
            None,  # Project doesn't exist yet
        ]

        with patch.object(pg_engine, "transaction") as mock_transaction:
            mock_transaction.return_value.__enter__.return_value = mock_conn

            pg_engine.ensure_registry()

            # Should create registry and mark as existing
            assert pg_engine._registry_exists is True
            assert mock_conn.execute.call_count > 5

    def test_registry_already_exists(self, pg_engine):
        """Test registry setup when already exists."""
        pg_engine._registry_exists = True

        # Should not do anything
        pg_engine.ensure_registry()

        # No database calls should be made
        assert pg_engine._registry_exists is True

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.read_text")
    def test_deploy_change_with_real_sql(
        self, mock_read_text, mock_exists, pg_engine, test_change
    ):
        """Test change deployment with realistic SQL content."""
        mock_exists.return_value = True
        mock_read_text.return_value = """
        -- Deploy test_change
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL
        );

        CREATE INDEX idx_users_email ON users(email);
        """

        mock_conn = Mock(spec=PostgreSQLConnection)
        pg_engine._registry_exists = True

        with (
            patch.object(pg_engine, "transaction") as mock_transaction,
            patch.object(pg_engine, "_calculate_script_hash", return_value="hash123"),
            patch.object(pg_engine, "_resolve_dependency_id", return_value="dep123"),
        ):

            mock_transaction.return_value.__enter__.return_value = mock_conn

            pg_engine.deploy_change(test_change)

            # Should execute the SQL statements
            executed_statements = [
                call[0][0] for call in mock_conn.execute.call_args_list
            ]

            # Check that CREATE TABLE and CREATE INDEX were executed
            create_table_executed = any(
                "CREATE TABLE users" in stmt for stmt in executed_statements
            )
            create_index_executed = any(
                "CREATE INDEX idx_users_email" in stmt for stmt in executed_statements
            )

            assert create_table_executed
            assert create_index_executed
