"""
Unit tests for Snowflake database engine.

This module contains comprehensive unit tests for the Snowflake engine
implementation, testing connection management, registry operations,
and SQL execution functionality.
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from sqlitch.core.change import Change
from sqlitch.core.exceptions import ConnectionError, DeploymentError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.core.types import EngineType
from sqlitch.engines.snowflake import (
    SnowflakeConnection,
    SnowflakeEngine,
    SnowflakeRegistrySchema,
)


class TestSnowflakeRegistrySchema:
    """Test Snowflake registry schema."""

    def test_get_create_statements(self):
        """Test getting create statements for Snowflake."""
        statements = SnowflakeRegistrySchema.get_create_statements("snowflake")

        assert len(statements) > 0
        assert any("CREATE SCHEMA IF NOT EXISTS sqitch" in stmt for stmt in statements)
        assert any(
            "CREATE TABLE IF NOT EXISTS sqitch.projects" in stmt for stmt in statements
        )
        assert any(
            "CREATE TABLE IF NOT EXISTS sqitch.releases" in stmt for stmt in statements
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


class TestSnowflakeConnection:
    """Test Snowflake connection wrapper."""

    @pytest.fixture
    def mock_snowflake_connection(self):
        """Create mock Snowflake connection."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn, mock_cursor

    def test_init(self, mock_snowflake_connection):
        """Test connection initialization."""
        mock_conn, _ = mock_snowflake_connection
        conn = SnowflakeConnection(mock_conn)

        assert conn._connection == mock_conn
        assert conn._cursor is None

    def test_execute_without_params(self, mock_snowflake_connection):
        """Test executing SQL without parameters."""
        mock_conn, mock_cursor = mock_snowflake_connection
        conn = SnowflakeConnection(mock_conn)

        sql = "SELECT 1"
        conn.execute(sql)

        mock_cursor.execute.assert_called_once_with(sql)

    def test_execute_with_params(self, mock_snowflake_connection):
        """Test executing SQL with parameters."""
        mock_conn, mock_cursor = mock_snowflake_connection
        conn = SnowflakeConnection(mock_conn)

        sql = "SELECT * FROM table WHERE id = :id AND name = :name"
        params = {"id": 1, "name": "test"}

        conn.execute(sql, params)

        # Should convert named parameters to positional
        expected_sql = "SELECT * FROM table WHERE id = ? AND name = ?"
        expected_params = [1, "test"]
        mock_cursor.execute.assert_called_once_with(expected_sql, expected_params)

    def test_fetchone(self, mock_snowflake_connection):
        """Test fetching one row."""
        mock_conn, mock_cursor = mock_snowflake_connection
        mock_cursor.fetchone.return_value = ("value1", "value2")
        mock_cursor.description = [("COLUMN1",), ("COLUMN2",)]

        conn = SnowflakeConnection(mock_conn)
        result = conn.fetchone()

        assert result == {"column1": "value1", "column2": "value2"}

    def test_fetchone_no_rows(self, mock_snowflake_connection):
        """Test fetching when no rows available."""
        mock_conn, mock_cursor = mock_snowflake_connection
        mock_cursor.fetchone.return_value = None

        conn = SnowflakeConnection(mock_conn)
        result = conn.fetchone()

        assert result is None

    def test_fetchall(self, mock_snowflake_connection):
        """Test fetching all rows."""
        mock_conn, mock_cursor = mock_snowflake_connection
        mock_cursor.fetchall.return_value = [("value1", "value2"), ("value3", "value4")]
        mock_cursor.description = [("COLUMN1",), ("COLUMN2",)]

        conn = SnowflakeConnection(mock_conn)
        result = conn.fetchall()

        expected = [
            {"column1": "value1", "column2": "value2"},
            {"column1": "value3", "column2": "value4"},
        ]
        assert result == expected

    def test_commit(self, mock_snowflake_connection):
        """Test committing transaction."""
        mock_conn, _ = mock_snowflake_connection
        conn = SnowflakeConnection(mock_conn)

        conn.commit()

        mock_conn.commit.assert_called_once()

    def test_rollback(self, mock_snowflake_connection):
        """Test rolling back transaction."""
        mock_conn, _ = mock_snowflake_connection
        conn = SnowflakeConnection(mock_conn)

        conn.rollback()

        mock_conn.rollback.assert_called_once()

    def test_close(self, mock_snowflake_connection):
        """Test closing connection."""
        mock_conn, mock_cursor = mock_snowflake_connection
        conn = SnowflakeConnection(mock_conn)
        conn._cursor = mock_cursor

        conn.close()

        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
        assert conn._cursor is None


class TestSnowflakeEngine:
    """Test Snowflake engine implementation."""

    @pytest.fixture
    def mock_target(self):
        """Create mock target."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.hostname = "test-account.snowflakecomputing.com"
        target.uri.username = "testuser"
        target.uri.password = "testpass"
        target.uri.path = "/testdb/testschema"
        target.uri.query = "warehouse=testwh&role=testrole"
        target.registry = "sqitch"
        return target

    @pytest.fixture
    def mock_plan(self):
        """Create mock plan."""
        plan = Mock(spec=Plan)
        plan.project_name = "testproject"
        plan.creator_name = "Test User"
        plan.creator_email = "test@example.com"
        plan.changes = []
        return plan

    @pytest.fixture
    def engine(self, mock_target, mock_plan):
        """Create Snowflake engine instance."""
        with patch("sqlitch.engines.snowflake.snowflake") as mock_snowflake:
            mock_snowflake.connector = Mock()
            engine = SnowflakeEngine(mock_target, mock_plan)
            return engine

    def test_init_without_snowflake_module(self, mock_target, mock_plan):
        """Test initialization without snowflake-connector-python."""
        with patch("sqlitch.engines.snowflake.snowflake", None):
            with pytest.raises(
                EngineError, match="snowflake-connector-python is required"
            ):
                SnowflakeEngine(mock_target, mock_plan)

    def test_engine_type(self, engine):
        """Test engine type property."""
        assert engine.engine_type == "snowflake"

    def test_registry_schema(self, engine):
        """Test registry schema property."""
        schema = engine.registry_schema
        assert isinstance(schema, SnowflakeRegistrySchema)

    @patch("sqlitch.engines.snowflake.snowflake")
    def test_create_connection_success(self, mock_snowflake, engine):
        """Test successful connection creation."""
        # Setup mocks
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake.connector.connect.return_value = mock_conn

        # Create connection
        connection = engine._create_connection()

        # Verify connection was created
        assert isinstance(connection, SnowflakeConnection)
        mock_snowflake.connector.connect.assert_called_once()

        # Verify session setup
        expected_calls = [
            call("ALTER WAREHOUSE testwh RESUME IF SUSPENDED"),
            call("ALTER SESSION SET TIMEZONE='UTC'"),
            call("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS'"),
            call("ALTER SESSION SET TIMESTAMP_TYPE_MAPPING=TIMESTAMP_LTZ"),
            call("USE SCHEMA IDENTIFIER('sqitch')"),
        ]
        mock_cursor.execute.assert_has_calls(expected_calls)

    @patch("sqlitch.engines.snowflake.snowflake")
    def test_create_connection_failure(self, mock_snowflake, engine):
        """Test connection creation failure."""
        # Setup mock to raise exception
        from sqlitch.engines.snowflake import SnowflakeError

        mock_snowflake.connector.connect.side_effect = SnowflakeError(
            "Connection failed"
        )

        # Test connection failure
        with pytest.raises(ConnectionError, match="Failed to connect to Snowflake"):
            engine._create_connection()

    def test_execute_sql_file(self, engine):
        """Test executing SQL file."""
        # Create temporary SQL file
        sql_content = """
        -- Test comment
        CREATE TABLE test (id INT);

        INSERT INTO test VALUES (1);
        """

        with patch("pathlib.Path.read_text", return_value=sql_content):
            mock_connection = Mock()
            sql_file = Path("test.sql")

            engine._execute_sql_file(mock_connection, sql_file)

            # Verify SQL statements were executed
            assert mock_connection.execute.call_count == 2
            mock_connection.execute.assert_any_call("CREATE TABLE test (id INT);")
            mock_connection.execute.assert_any_call("INSERT INTO test VALUES (1);")

    def test_execute_sql_file_with_variables(self, engine):
        """Test executing SQL file with variable substitution."""
        sql_content = "CREATE SCHEMA &registry;\nUSE WAREHOUSE &warehouse;"

        with patch("pathlib.Path.read_text", return_value=sql_content):
            mock_connection = Mock()
            sql_file = Path("test.sql")
            variables = {"custom_var": "custom_value"}

            engine._execute_sql_file(mock_connection, sql_file, variables)

            # Verify variable substitution occurred (statements are split by semicolon)
            mock_connection.execute.assert_any_call("CREATE SCHEMA sqitch;")
            mock_connection.execute.assert_any_call("USE WAREHOUSE testwh;")

    def test_split_sql_statements(self, engine):
        """Test splitting SQL content into statements."""
        sql_content = """
        -- Comment line
        CREATE TABLE test (id INT);

        INSERT INTO test VALUES (1);
        INSERT INTO test VALUES (2);

        -- Another comment
        DROP TABLE test;
        """

        statements = engine._split_sql_statements(sql_content)

        expected = [
            "CREATE TABLE test (id INT);",
            "INSERT INTO test VALUES (1);",
            "INSERT INTO test VALUES (2);",
            "DROP TABLE test;",
        ]
        assert statements == expected

    def test_get_registry_version(self, engine):
        """Test getting registry version."""
        mock_connection = Mock()
        mock_connection.fetchone.return_value = {"version": 1.1}

        version = engine._get_registry_version(mock_connection)

        assert version == "1.1"
        mock_connection.execute.assert_called_once()

    def test_get_registry_version_not_found(self, engine):
        """Test getting registry version when not found."""
        mock_connection = Mock()
        mock_connection.fetchone.return_value = None

        version = engine._get_registry_version(mock_connection)

        assert version is None

    def test_regex_condition(self, engine):
        """Test regex condition generation."""
        condition = engine._regex_condition("column_name", "pattern")

        assert condition == "REGEXP_SUBSTR(column_name, ?) IS NOT NULL"

    def test_get_account_from_hostname(self, engine):
        """Test getting account from hostname."""
        account = engine._get_account()

        assert account == "test-account"

    def test_get_account_from_env(self, mock_target, mock_plan):
        """Test getting account from environment variable."""
        mock_target.uri.hostname = None

        with patch.dict(os.environ, {"SNOWSQL_ACCOUNT": "env-account"}):
            with patch("sqlitch.engines.snowflake.snowflake") as mock_snowflake:
                mock_snowflake.connector = Mock()
                engine = SnowflakeEngine(mock_target, mock_plan)
                account = engine._get_account()

                assert account == "env-account"

    def test_get_account_failure(self, mock_target, mock_plan):
        """Test account determination failure."""
        mock_target.uri.hostname = None

        with patch.dict(os.environ, {}, clear=True):
            with patch("sqlitch.engines.snowflake.snowflake") as mock_snowflake:
                mock_snowflake.connector = Mock()
                engine = SnowflakeEngine(mock_target, mock_plan)

                with pytest.raises(
                    EngineError, match="Cannot determine Snowflake account name"
                ):
                    engine._get_account()

    def test_get_user_from_uri(self, engine):
        """Test getting user from URI."""
        user = engine._get_user()

        assert user == "testuser"

    def test_get_user_from_env(self, mock_target, mock_plan):
        """Test getting user from environment variable."""
        mock_target.uri.username = None

        with patch.dict(os.environ, {"SNOWSQL_USER": "env-user"}):
            with patch("sqlitch.engines.snowflake.snowflake") as mock_snowflake:
                mock_snowflake.connector = Mock()
                engine = SnowflakeEngine(mock_target, mock_plan)
                user = engine._get_user()

                assert user == "env-user"

    def test_get_password_from_uri(self, engine):
        """Test getting password from URI."""
        password = engine._get_password()

        assert password == "testpass"

    def test_get_password_from_env(self, mock_target, mock_plan):
        """Test getting password from environment variable."""
        mock_target.uri.password = None

        with patch.dict(os.environ, {"SNOWSQL_PWD": "env-password"}):
            with patch("sqlitch.engines.snowflake.snowflake") as mock_snowflake:
                mock_snowflake.connector = Mock()
                engine = SnowflakeEngine(mock_target, mock_plan)
                password = engine._get_password()

                assert password == "env-password"

    def test_get_database_from_path(self, engine):
        """Test getting database from URI path."""
        database = engine._get_database()

        assert database == "testdb"

    def test_get_warehouse_from_query(self, engine):
        """Test getting warehouse from query parameters."""
        warehouse = engine._warehouse

        assert warehouse == "testwh"

    def test_get_warehouse_default(self, mock_target, mock_plan):
        """Test getting default warehouse."""
        mock_target.uri.query = None

        with patch.dict(os.environ, {}, clear=True):
            with patch("sqlitch.engines.snowflake.snowflake") as mock_snowflake:
                mock_snowflake.connector = Mock()
                engine = SnowflakeEngine(mock_target, mock_plan)

                assert engine._warehouse == "sqitch"

    def test_get_role_from_query(self, engine):
        """Test getting role from query parameters."""
        role = engine._role

        assert role == "testrole"

    def test_get_role_none(self, mock_target, mock_plan):
        """Test getting role when not specified."""
        mock_target.uri.query = None

        with patch.dict(os.environ, {}, clear=True):
            with patch("sqlitch.engines.snowflake.snowflake") as mock_snowflake:
                mock_snowflake.connector = Mock()
                engine = SnowflakeEngine(mock_target, mock_plan)

                assert engine._role is None

    def test_get_registry_schema_default(self, engine):
        """Test getting default registry schema."""
        schema = engine._get_registry_schema()

        assert schema == "sqitch"

    def test_get_registry_schema_custom(self, mock_target, mock_plan):
        """Test getting custom registry schema."""
        mock_target.registry = "custom_schema"

        with patch("sqlitch.engines.snowflake.snowflake") as mock_snowflake:
            mock_snowflake.connector = Mock()
            engine = SnowflakeEngine(mock_target, mock_plan)
            schema = engine._get_registry_schema()

            assert schema == "custom_schema"


@pytest.fixture
def sample_change():
    """Create a sample change for testing."""
    return Change(
        name="test_change",
        note="Test change",
        tags=["v1.0"],
        dependencies=[],
        conflicts=[],
        timestamp=datetime.now(timezone.utc),
        planner_name="Test User",
        planner_email="test@example.com",
    )


class TestSnowflakeEngineIntegration:
    """Integration tests for Snowflake engine operations."""

    @pytest.fixture
    def mock_target_for_integration(self):
        """Create mock target for integration tests."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.hostname = "test-account.snowflakecomputing.com"
        target.uri.username = "testuser"
        target.uri.password = "testpass"
        target.uri.path = "/testdb/testschema"
        target.uri.query = "warehouse=testwh&role=testrole"
        target.registry = "sqitch"
        return target

    @pytest.fixture
    def mock_plan_for_integration(self):
        """Create mock plan for integration tests."""
        plan = Mock(spec=Plan)
        plan.project_name = "testproject"
        plan.creator_name = "Test User"
        plan.creator_email = "test@example.com"
        plan.changes = []
        return plan

    @pytest.fixture
    def engine_with_mocks(self, mock_target_for_integration, mock_plan_for_integration):
        """Create engine with mocked dependencies."""
        with patch("sqlitch.engines.snowflake.snowflake") as mock_snowflake:
            mock_snowflake.connector = Mock()
            engine = SnowflakeEngine(
                mock_target_for_integration, mock_plan_for_integration
            )
            return engine

    def test_deploy_change_success(self, engine_with_mocks, sample_change):
        """Test successful change deployment."""
        engine = engine_with_mocks

        # Mock plan methods
        engine.plan.get_deploy_file = Mock(return_value=Path("deploy.sql"))
        engine.plan.get_revert_file = Mock(return_value=Path("revert.sql"))
        engine.plan.get_verify_file = Mock(return_value=Path("verify.sql"))

        # Mock file operations
        with patch("pathlib.Path.exists", return_value=True):
            with patch(
                "pathlib.Path.read_text", return_value="CREATE TABLE test (id INT);"
            ):
                with patch(
                    "pathlib.Path.read_bytes",
                    return_value=b"CREATE TABLE test (id INT);",
                ):
                    with patch.object(engine, "ensure_registry"):
                        with patch.object(engine, "transaction") as mock_transaction:
                            mock_conn = Mock()
                            mock_transaction.return_value.__enter__.return_value = (
                                mock_conn
                            )

                            # Execute deployment
                            engine.deploy_change(sample_change)

                            # Verify SQL execution
                            mock_conn.execute.assert_called()

    def test_revert_change_success(self, engine_with_mocks, sample_change):
        """Test successful change revert."""
        engine = engine_with_mocks

        # Mock plan methods
        engine.plan.get_revert_file = Mock(return_value=Path("revert.sql"))

        # Mock file operations
        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text", return_value="DROP TABLE test;"):
                with patch.object(engine, "ensure_registry"):
                    with patch.object(engine, "transaction") as mock_transaction:
                        mock_conn = Mock()
                        mock_transaction.return_value.__enter__.return_value = mock_conn

                        # Execute revert
                        engine.revert_change(sample_change)

                        # Verify SQL execution
                        mock_conn.execute.assert_called()

    def test_verify_change_success(self, engine_with_mocks, sample_change):
        """Test successful change verification."""
        engine = engine_with_mocks

        # Mock plan methods
        engine.plan.get_verify_file = Mock(return_value=Path("verify.sql"))

        # Mock file operations
        with patch("pathlib.Path.exists", return_value=True):
            with patch(
                "pathlib.Path.read_text", return_value="SELECT COUNT(*) FROM test;"
            ):
                with patch.object(engine, "connection") as mock_connection:
                    mock_conn = Mock()
                    mock_connection.return_value.__enter__.return_value = mock_conn

                    # Execute verification
                    result = engine.verify_change(sample_change)

                    # Verify result
                    assert result is True
                    mock_conn.execute.assert_called()

    def test_verify_change_failure(self, engine_with_mocks, sample_change):
        """Test change verification failure."""
        engine = engine_with_mocks

        # Mock plan methods
        engine.plan.get_verify_file = Mock(return_value=Path("verify.sql"))

        # Mock file operations to raise exception
        with patch("pathlib.Path.exists", return_value=True):
            with patch(
                "pathlib.Path.read_text", return_value="SELECT COUNT(*) FROM test;"
            ):
                with patch.object(engine, "connection") as mock_connection:
                    mock_conn = Mock()
                    mock_conn.execute.side_effect = Exception("Verification failed")
                    mock_connection.return_value.__enter__.return_value = mock_conn

                    # Execute verification
                    result = engine.verify_change(sample_change)

                    # Verify result
                    assert result is False
