"""
Unit tests for Vertica database engine.

This module contains unit tests for the VerticaEngine class,
testing connection management, registry operations, and SQL execution.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.core.change import Change
from sqlitch.core.exceptions import ConnectionError, DeploymentError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.engines.vertica import (
    VerticaConnection,
    VerticaEngine,
    VerticaRegistrySchema,
)


class TestVerticaRegistrySchema:
    """Test Vertica registry schema."""

    def test_get_create_statements(self):
        """Test getting create statements for Vertica."""
        statements = VerticaRegistrySchema.get_create_statements("vertica")

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
        assert any(
            "CREATE TABLE IF NOT EXISTS sqitch.releases" in stmt for stmt in statements
        )


class TestVerticaConnection:
    """Test Vertica connection wrapper."""

    @pytest.fixture
    def mock_vertica_connection(self):
        """Create mock Vertica connection."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn, mock_cursor

    def test_init(self, mock_vertica_connection):
        """Test connection initialization."""
        mock_conn, _ = mock_vertica_connection
        conn = VerticaConnection(mock_conn)

        assert conn._connection == mock_conn
        assert conn._cursor is None

    def test_execute_without_params(self, mock_vertica_connection):
        """Test executing SQL without parameters."""
        mock_conn, mock_cursor = mock_vertica_connection
        conn = VerticaConnection(mock_conn)

        conn.execute("SELECT 1")

        mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_execute_with_params(self, mock_vertica_connection):
        """Test executing SQL with parameters."""
        mock_conn, mock_cursor = mock_vertica_connection
        conn = VerticaConnection(mock_conn)

        conn.execute("SELECT * FROM table WHERE id = :id", {"id": 123})

        # Should convert named parameters to positional
        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM table WHERE id = ?", [123]
        )

    def test_fetchone(self, mock_vertica_connection):
        """Test fetching one row."""
        mock_conn, mock_cursor = mock_vertica_connection
        mock_cursor.fetchone.return_value = ("value1", "value2")
        mock_cursor.description = [("col1",), ("col2",)]

        conn = VerticaConnection(mock_conn)
        result = conn.fetchone()

        assert result == {"col1": "value1", "col2": "value2"}

    def test_fetchone_no_rows(self, mock_vertica_connection):
        """Test fetching one row when no rows available."""
        mock_conn, mock_cursor = mock_vertica_connection
        mock_cursor.fetchone.return_value = None

        conn = VerticaConnection(mock_conn)
        result = conn.fetchone()

        assert result is None

    def test_fetchall(self, mock_vertica_connection):
        """Test fetching all rows."""
        mock_conn, mock_cursor = mock_vertica_connection
        mock_cursor.fetchall.return_value = [("value1", "value2"), ("value3", "value4")]
        mock_cursor.description = [("col1",), ("col2",)]

        conn = VerticaConnection(mock_conn)
        result = conn.fetchall()

        assert result == [
            {"col1": "value1", "col2": "value2"},
            {"col1": "value3", "col2": "value4"},
        ]

    def test_commit(self, mock_vertica_connection):
        """Test committing transaction."""
        mock_conn, _ = mock_vertica_connection
        conn = VerticaConnection(mock_conn)

        conn.commit()

        mock_conn.commit.assert_called_once()

    def test_rollback(self, mock_vertica_connection):
        """Test rolling back transaction."""
        mock_conn, _ = mock_vertica_connection
        conn = VerticaConnection(mock_conn)

        conn.rollback()

        mock_conn.rollback.assert_called_once()

    def test_close(self, mock_vertica_connection):
        """Test closing connection."""
        mock_conn, mock_cursor = mock_vertica_connection
        conn = VerticaConnection(mock_conn)
        conn._cursor = mock_cursor

        conn.close()

        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()


class TestVerticaEngine:
    """Test Vertica database engine."""

    @pytest.fixture
    def mock_target(self):
        """Create mock target."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.hostname = "localhost"
        target.uri.port = 5433
        target.uri.username = "user"
        target.uri.password = "pass"
        target.uri.path = "/testdb"
        target.uri.query = None
        target.registry = "sqitch"
        return target

    @pytest.fixture
    def mock_plan(self):
        """Create mock plan."""
        plan = Mock(spec=Plan)
        plan.project_name = "test_project"
        plan.creator_name = "Test User"
        plan.creator_email = "test@example.com"
        return plan

    @pytest.fixture
    def mock_change(self):
        """Create mock change."""
        change = Mock(spec=Change)
        change.id = "abc123"
        change.name = "test_change"
        change.note = "Test change"
        change.planner_name = "Test User"
        change.planner_email = "test@example.com"
        change.timestamp = datetime.now(timezone.utc)
        change.dependencies = []
        change.conflicts = []
        change.tags = []
        return change

    def test_init_without_vertica_python(self, mock_target, mock_plan):
        """Test initialization without vertica-python installed."""
        with patch("sqlitch.engines.vertica.vertica_python", None):
            with pytest.raises(EngineError, match="vertica-python is required"):
                VerticaEngine(mock_target, mock_plan)

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_init_with_vertica_python(
        self, mock_vertica_python, mock_target, mock_plan
    ):
        """Test initialization with vertica-python available."""
        engine = VerticaEngine(mock_target, mock_plan)

        assert engine.target == mock_target
        assert engine.plan == mock_plan
        assert engine.engine_type == "vertica"
        assert isinstance(engine.registry_schema, VerticaRegistrySchema)

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_create_connection_success(
        self, mock_vertica_python, mock_target, mock_plan
    ):
        """Test successful connection creation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_vertica_python.connect.return_value = mock_conn

        engine = VerticaEngine(mock_target, mock_plan)
        connection = engine._create_connection()

        assert isinstance(connection, VerticaConnection)
        mock_vertica_python.connect.assert_called_once()

        # Verify connection parameters
        call_args = mock_vertica_python.connect.call_args[1]
        assert call_args["host"] == "localhost"
        assert call_args["port"] == 5433
        assert call_args["user"] == "user"
        assert call_args["password"] == "pass"
        assert call_args["database"] == "testdb"
        assert call_args["autocommit"] is False

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_create_connection_failure(
        self, mock_vertica_python, mock_target, mock_plan
    ):
        """Test connection creation failure."""
        from sqlitch.engines.vertica import VerticaError

        mock_vertica_python.connect.side_effect = VerticaError("Connection failed")

        engine = VerticaEngine(mock_target, mock_plan)

        with pytest.raises(ConnectionError, match="Failed to connect to Vertica"):
            engine._create_connection()

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_execute_sql_file(
        self, mock_vertica_python, mock_target, mock_plan, tmp_path
    ):
        """Test executing SQL file."""
        # Create test SQL file
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1;\nSELECT 2;")

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        engine = VerticaEngine(mock_target, mock_plan)
        connection = VerticaConnection(mock_conn)

        engine._execute_sql_file(connection, sql_file)

        # Should execute both statements
        assert mock_cursor.execute.call_count == 2

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_execute_sql_file_with_variables(
        self, mock_vertica_python, mock_target, mock_plan, tmp_path
    ):
        """Test executing SQL file with variable substitution."""
        # Create test SQL file with variables
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT '&var1';\nCREATE SCHEMA &registry;")

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        engine = VerticaEngine(mock_target, mock_plan)
        connection = VerticaConnection(mock_conn)

        engine._execute_sql_file(connection, sql_file, {"var1": "test_value"})

        # Should execute statements with substituted variables
        assert mock_cursor.execute.call_count == 2
        # First call should have substituted var1
        first_call = mock_cursor.execute.call_args_list[0][0][0]
        assert "test_value" in first_call
        # Second call should have substituted registry
        second_call = mock_cursor.execute.call_args_list[1][0][0]
        assert "sqitch" in second_call

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_registry_version(self, mock_vertica_python, mock_target, mock_plan):
        """Test getting registry version."""
        engine = VerticaEngine(mock_target, mock_plan)

        # Mock the connection to return version data
        mock_connection = Mock(spec=VerticaConnection)
        mock_connection.fetchone.return_value = {
            "version": 1.1
        }  # Vertica returns float

        version = engine._get_registry_version(mock_connection)

        assert version == "1.1"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_registry_version_not_found(
        self, mock_vertica_python, mock_target, mock_plan
    ):
        """Test getting registry version when not found."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor

        engine = VerticaEngine(mock_target, mock_plan)
        connection = VerticaConnection(mock_conn)

        version = engine._get_registry_version(connection)

        assert version is None

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_regex_condition(self, mock_vertica_python, mock_target, mock_plan):
        """Test regex condition generation."""
        engine = VerticaEngine(mock_target, mock_plan)

        condition = engine._regex_condition("column_name", "pattern")

        assert condition == "column_name ~ ?"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_host_from_uri(self, mock_vertica_python, mock_plan):
        """Test getting host from URI."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.hostname = "example.com"
        target.registry = "sqitch"

        engine = VerticaEngine(target, mock_plan)
        host = engine._get_host()

        assert host == "example.com"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_host_from_env(self, mock_vertica_python, mock_plan):
        """Test getting host from environment variable."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.hostname = None
        target.registry = "sqitch"

        with patch.dict("os.environ", {"VSQL_HOST": "env-host"}):
            engine = VerticaEngine(target, mock_plan)
            host = engine._get_host()

        assert host == "env-host"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_host_default(self, mock_vertica_python, mock_plan):
        """Test getting default host."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.hostname = None
        target.registry = "sqitch"

        engine = VerticaEngine(target, mock_plan)
        host = engine._get_host()

        assert host == "localhost"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_port_from_uri(self, mock_vertica_python, mock_plan):
        """Test getting port from URI."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.port = 9999
        target.registry = "sqitch"

        engine = VerticaEngine(target, mock_plan)
        port = engine._get_port()

        assert port == 9999

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_port_from_env(self, mock_vertica_python, mock_plan):
        """Test getting port from environment variable."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.port = None
        target.registry = "sqitch"

        with patch.dict("os.environ", {"VSQL_PORT": "8888"}):
            engine = VerticaEngine(target, mock_plan)
            port = engine._get_port()

        assert port == 8888

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_port_default(self, mock_vertica_python, mock_plan):
        """Test getting default port."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.port = None
        target.registry = "sqitch"

        engine = VerticaEngine(target, mock_plan)
        port = engine._get_port()

        assert port == 5433

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_user_from_uri(self, mock_vertica_python, mock_plan):
        """Test getting user from URI."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.username = "testuser"
        target.registry = "sqitch"

        engine = VerticaEngine(target, mock_plan)
        user = engine._get_user()

        assert user == "testuser"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_user_from_env(self, mock_vertica_python, mock_plan):
        """Test getting user from environment variable."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.username = None
        target.registry = "sqitch"

        with patch.dict("os.environ", {"VSQL_USER": "envuser"}):
            engine = VerticaEngine(target, mock_plan)
            user = engine._get_user()

        assert user == "envuser"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_user_default(self, mock_vertica_python, mock_plan):
        """Test getting default user."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.username = None
        target.uri.query = None
        target.registry = "sqitch"

        with patch.dict("os.environ", {"USER": "systemuser"}, clear=True):
            engine = VerticaEngine(target, mock_plan)
            user = engine._get_user()

        assert user == "systemuser"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_password_from_uri(self, mock_vertica_python, mock_plan):
        """Test getting password from URI."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.password = "secret"
        target.registry = "sqitch"

        engine = VerticaEngine(target, mock_plan)
        password = engine._get_password()

        assert password == "secret"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_password_from_env(self, mock_vertica_python, mock_plan):
        """Test getting password from environment variable."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.password = None
        target.uri.query = None
        target.registry = "sqitch"

        with patch.dict("os.environ", {"VSQL_PASSWORD": "envpass"}):
            engine = VerticaEngine(target, mock_plan)
            password = engine._get_password()

        assert password == "envpass"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_password_none(self, mock_vertica_python, mock_plan):
        """Test getting password when not available."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.password = None
        target.uri.query = None
        target.registry = "sqitch"

        engine = VerticaEngine(target, mock_plan)
        password = engine._get_password()

        assert password is None

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_database_from_uri(self, mock_vertica_python, mock_plan):
        """Test getting database from URI."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.path = "/testdb"
        target.registry = "sqitch"

        engine = VerticaEngine(target, mock_plan)
        database = engine._get_database()

        assert database == "testdb"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_database_from_env(self, mock_vertica_python, mock_plan):
        """Test getting database from environment variable."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.path = "/"
        target.uri.query = None
        target.registry = "sqitch"

        with patch.dict("os.environ", {"VSQL_DATABASE": "envdb"}):
            engine = VerticaEngine(target, mock_plan)
            database = engine._get_database()

        assert database == "envdb"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_database_default(self, mock_vertica_python, mock_plan):
        """Test getting default database."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.path = "/"
        target.uri.query = None
        target.uri.username = "testuser"
        target.registry = "sqitch"

        engine = VerticaEngine(target, mock_plan)
        database = engine._get_database()

        assert database == "testuser"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_registry_schema_from_target(self, mock_vertica_python, mock_plan):
        """Test getting registry schema from target."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.registry = "custom_schema"

        engine = VerticaEngine(target, mock_plan)
        schema = engine._get_registry_schema()

        assert schema == "custom_schema"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_get_registry_schema_default(self, mock_vertica_python, mock_plan):
        """Test getting default registry schema."""
        target = Mock(spec=Target)
        target.uri = Mock()
        # Remove registry attribute to test default
        del target.registry

        engine = VerticaEngine(target, mock_plan)
        schema = engine._get_registry_schema()

        assert schema == "sqitch"

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_split_sql_statements(self, mock_vertica_python, mock_target, mock_plan):
        """Test splitting SQL content into statements."""
        engine = VerticaEngine(mock_target, mock_plan)

        sql_content = """
        -- Comment line
        SELECT 1;

        INSERT INTO table VALUES (1, 2);
        -- Another comment
        UPDATE table SET col = 'value';
        """

        statements = engine._split_sql_statements(sql_content)

        assert len(statements) == 3
        assert "SELECT 1;" in statements[0]
        assert "INSERT INTO table VALUES (1, 2);" in statements[1]
        assert "UPDATE table SET col = 'value';" in statements[2]

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_ts2char_format(self, mock_vertica_python, mock_target, mock_plan):
        """Test timestamp to character format."""
        engine = VerticaEngine(mock_target, mock_plan)

        result = engine._ts2char_format("timestamp_column")

        expected = 'to_char(timestamp_column AT TIME ZONE \'UTC\', \'"year":YYYY:"month":MM:"day":DD:"hour":HH24:"minute":MI:"second":SS:"time_zone":"UTC"\')'
        assert result == expected

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_multi_values(self, mock_vertica_python, mock_target, mock_plan):
        """Test multi-value expression generation."""
        engine = VerticaEngine(mock_target, mock_plan)

        result = engine._multi_values(3, "?, ?")

        expected = "SELECT ?, ?\nUNION ALL SELECT ?, ?\nUNION ALL SELECT ?, ?"
        assert result == expected

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_dependency_placeholders(self, mock_vertica_python, mock_target, mock_plan):
        """Test dependency placeholders."""
        engine = VerticaEngine(mock_target, mock_plan)

        result = engine._dependency_placeholders()

        expected = "CAST(? AS CHAR(40)), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS CHAR(40))"
        assert result == expected

    @patch("sqlitch.engines.vertica.vertica_python")
    def test_tag_placeholders(self, mock_vertica_python, mock_target, mock_plan):
        """Test tag placeholders."""
        engine = VerticaEngine(mock_target, mock_plan)

        result = engine._tag_placeholders()

        assert "CAST(? AS CHAR(40))" in result
        assert "CAST(? AS VARCHAR)" in result
        assert "CAST(? AS TIMESTAMPTZ)" in result
        assert "clock_timestamp()" in result
