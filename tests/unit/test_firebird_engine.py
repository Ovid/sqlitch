"""
Unit tests for Firebird database engine.

This module contains comprehensive unit tests for the FirebirdEngine class,
testing connection management, registry operations, SQL execution, and
error handling specific to Firebird databases.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from sqlitch.core.change import Change
from sqlitch.core.exceptions import ConnectionError, DeploymentError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.core.types import EngineType
from sqlitch.engines.firebird import (
    FirebirdConnection,
    FirebirdEngine,
    FirebirdRegistrySchema,
)


class TestFirebirdRegistrySchema:
    """Test Firebird registry schema."""

    def test_get_create_statements(self):
        """Test getting Firebird-specific CREATE statements."""
        statements = FirebirdRegistrySchema.get_create_statements("firebird")

        assert isinstance(statements, list)
        assert len(statements) > 0

        # Check that all required tables are created
        statement_text = " ".join(statements)
        assert "CREATE TABLE releases" in statement_text
        assert "CREATE TABLE projects" in statement_text
        assert "CREATE TABLE changes" in statement_text
        assert "CREATE TABLE tags" in statement_text
        assert "CREATE TABLE dependencies" in statement_text
        assert "CREATE TABLE events" in statement_text

        # Check Firebird-specific syntax
        assert "VARCHAR(" in statement_text
        assert "TIMESTAMP" in statement_text
        assert "BLOB SUB_TYPE TEXT" in statement_text
        assert "CHAR(40)" in statement_text


class TestFirebirdConnection:
    """Test Firebird connection wrapper."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_fdb_conn = Mock()
        self.mock_cursor = Mock()
        self.mock_fdb_conn.cursor.return_value = self.mock_cursor

        self.connection = FirebirdConnection(self.mock_fdb_conn)

    def test_execute_with_params(self):
        """Test executing SQL with named parameters."""
        sql = "SELECT * FROM table WHERE id = :id AND name = :name"
        params = {"id": 1, "name": "test"}

        self.connection.execute(sql, params)

        # Should create cursor and execute with converted parameters
        self.mock_fdb_conn.cursor.assert_called_once()
        self.mock_cursor.execute.assert_called_once()

    def test_execute_without_params(self):
        """Test executing SQL without parameters."""
        sql = "SELECT * FROM table"

        self.connection.execute(sql)

        self.mock_cursor.execute.assert_called_once_with(sql)

    def test_fetchone(self):
        """Test fetching one row."""
        # Mock cursor description and fetchone
        self.mock_cursor.description = [("id", None), ("name", None)]
        self.mock_cursor.fetchone.return_value = (1, "test")

        # Execute a query first to set up cursor
        self.connection.execute("SELECT * FROM table")

        result = self.connection.fetchone()

        assert result == {"id": 1, "name": "test"}

    def test_fetchone_no_result(self):
        """Test fetchone when no results."""
        self.mock_cursor.fetchone.return_value = None

        self.connection.execute("SELECT * FROM table")
        result = self.connection.fetchone()

        assert result is None

    def test_fetchall(self):
        """Test fetching all rows."""
        self.mock_cursor.description = [("id", None), ("name", None)]
        self.mock_cursor.fetchall.return_value = [(1, "test1"), (2, "test2")]

        self.connection.execute("SELECT * FROM table")

        result = self.connection.fetchall()

        assert result == [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

    def test_commit(self):
        """Test committing transaction."""
        self.connection.commit()
        self.mock_fdb_conn.commit.assert_called_once()

    def test_rollback(self):
        """Test rolling back transaction."""
        self.connection.rollback()
        self.mock_fdb_conn.rollback.assert_called_once()

    def test_close(self):
        """Test closing connection."""
        # Set up cursor
        self.connection.execute("SELECT 1")

        self.connection.close()

        self.mock_cursor.close.assert_called_once()
        self.mock_fdb_conn.close.assert_called_once()


class TestFirebirdEngine:
    """Test Firebird database engine."""

    def setup_method(self):
        """Set up test fixtures."""
        self.target = Target(
            name="test",
            uri="firebird://user:pass@localhost/test.fdb",
            registry="sqitch",
        )

        self.plan = Plan(
            file=Path("/tmp/sqitch.plan"),
            project="test_project",
            uri="https://example.com/test",
            changes=[],
        )

    def test_init_without_fdb(self):
        """Test initialization when fdb is not available."""
        with patch("sqlitch.engines.firebird.fdb", None):
            with pytest.raises(EngineError) as exc_info:
                FirebirdEngine(self.target, self.plan)

            assert "fdb" in str(exc_info.value)
            assert "pip install fdb" in str(exc_info.value)

    @patch("sqlitch.engines.firebird.fdb")
    def test_init_with_fdb(self, mock_fdb):
        """Test successful initialization with fdb available."""
        engine = FirebirdEngine(self.target, self.plan)

        assert engine.engine_type == "firebird"
        assert isinstance(engine.registry_schema, FirebirdRegistrySchema)

    @patch("sqlitch.engines.firebird.fdb")
    def test_create_connection_success(self, mock_fdb):
        """Test successful database connection."""
        mock_conn = Mock()
        mock_fdb.connect.return_value = mock_conn

        engine = FirebirdEngine(self.target, self.plan)
        connection = engine._create_connection()

        assert isinstance(connection, FirebirdConnection)
        mock_fdb.connect.assert_called_once_with(
            dsn="localhost:test.fdb", user="user", password="pass", charset="UTF8"
        )

    @patch("sqlitch.engines.firebird.fdb")
    def test_create_connection_create_database(self, mock_fdb):
        """Test creating database when it doesn't exist."""

        # Create a mock DatabaseError class
        class MockDatabaseError(Exception):
            pass

        mock_fdb.DatabaseError = MockDatabaseError

        # First connection attempt fails
        mock_fdb.connect.side_effect = MockDatabaseError("No such file or directory")

        mock_created_conn = Mock()
        mock_fdb.create_database.return_value = mock_created_conn

        engine = FirebirdEngine(self.target, self.plan)
        connection = engine._create_connection()

        assert isinstance(connection, FirebirdConnection)
        mock_fdb.create_database.assert_called_once_with(
            dsn="localhost:test.fdb",
            user="user",
            password="pass",
            charset="UTF8",
            page_size=16384,
        )

    @patch("sqlitch.engines.firebird.fdb")
    def test_create_connection_local_file(self, mock_fdb):
        """Test connection to local database file."""
        mock_conn = Mock()
        mock_fdb.connect.return_value = mock_conn

        # Use local file URI
        target = Target(
            name="test", uri="firebird:///path/to/test.fdb", registry="sqitch"
        )

        engine = FirebirdEngine(target, self.plan)
        engine._create_connection()

        mock_fdb.connect.assert_called_once_with(
            dsn="path/to/test.fdb", user="SYSDBA", password="masterkey", charset="UTF8"
        )

    @patch("sqlitch.engines.firebird.fdb")
    def test_create_connection_failure(self, mock_fdb):
        """Test connection failure."""
        mock_fdb.connect.side_effect = Exception("Connection failed")

        engine = FirebirdEngine(self.target, self.plan)

        with pytest.raises(ConnectionError) as exc_info:
            engine._create_connection()

        assert "Failed to connect to Firebird database" in str(exc_info.value)

    @patch("sqlitch.engines.firebird.fdb")
    def test_execute_sql_file(self, mock_fdb):
        """Test executing SQL file."""
        mock_conn = Mock()
        mock_fdb.connect.return_value = mock_conn

        engine = FirebirdEngine(self.target, self.plan)
        connection = FirebirdConnection(mock_conn)

        # Create temporary SQL file
        sql_content = """
        -- Comment line
        CREATE TABLE test (id INTEGER);
        INSERT INTO test VALUES (1);
        -- Another comment
        INSERT INTO test VALUES (2);
        """

        sql_file = Path("/tmp/test.sql")

        with patch("builtins.open", mock_open(read_data=sql_content)):
            with patch.object(connection, "execute") as mock_execute:
                engine._execute_sql_file(connection, sql_file)

        # Should execute non-comment statements
        expected_calls = [
            call("CREATE TABLE test (id INTEGER);"),
            call("INSERT INTO test VALUES (1);"),
            call("INSERT INTO test VALUES (2);"),
        ]
        mock_execute.assert_has_calls(expected_calls)

    @patch("sqlitch.engines.firebird.fdb")
    def test_execute_sql_file_with_variables(self, mock_fdb):
        """Test executing SQL file with variable substitution."""
        mock_conn = Mock()
        mock_fdb.connect.return_value = mock_conn

        engine = FirebirdEngine(self.target, self.plan)
        connection = FirebirdConnection(mock_conn)

        sql_content = "CREATE TABLE ${table_name} (id INTEGER);"
        sql_file = Path("/tmp/test.sql")
        variables = {"table_name": "users"}

        with patch("builtins.open", mock_open(read_data=sql_content)):
            with patch.object(connection, "execute") as mock_execute:
                engine._execute_sql_file(connection, sql_file, variables)

        mock_execute.assert_called_once_with("CREATE TABLE users (id INTEGER);")

    @patch("sqlitch.engines.firebird.fdb")
    def test_split_sql_statements(self, mock_fdb):
        """Test splitting SQL content into statements."""
        engine = FirebirdEngine(self.target, self.plan)

        sql_content = """
        -- Comment
        CREATE TABLE test (id INTEGER);

        INSERT INTO test VALUES (1); -- Inline comment
        INSERT INTO test VALUES (2);

        -- Another comment
        """

        statements = engine._split_sql_statements(sql_content)

        expected = [
            "CREATE TABLE test (id INTEGER);",
            "INSERT INTO test VALUES (1);",
            "INSERT INTO test VALUES (2);",
        ]
        assert statements == expected

    @patch("sqlitch.engines.firebird.fdb")
    def test_get_registry_version(self, mock_fdb):
        """Test getting registry version."""
        mock_conn = Mock()
        mock_fdb.connect.return_value = mock_conn

        engine = FirebirdEngine(self.target, self.plan)
        connection = FirebirdConnection(mock_conn)

        with patch.object(connection, "execute") as mock_execute:
            with patch.object(connection, "fetchone", return_value={"v": "1.1"}):
                version = engine._get_registry_version(connection)

        assert version == "1.1"
        mock_execute.assert_called_once()

    @patch("sqlitch.engines.firebird.fdb")
    def test_get_registry_version_not_found(self, mock_fdb):
        """Test getting registry version when not found."""
        mock_conn = Mock()
        mock_fdb.connect.return_value = mock_conn

        engine = FirebirdEngine(self.target, self.plan)
        connection = FirebirdConnection(mock_conn)

        with patch.object(
            connection, "execute", side_effect=Exception("Table not found")
        ):
            version = engine._get_registry_version(connection)

        assert version is None

    @patch("sqlitch.engines.firebird.fdb")
    def test_regex_condition(self, mock_fdb):
        """Test creating regex condition for Firebird."""
        engine = FirebirdEngine(self.target, self.plan)

        condition = engine._regex_condition("column_name", "pattern")

        assert condition == "column_name SIMILAR TO ?"

    @patch("sqlitch.engines.firebird.fdb")
    def test_convert_regex_to_similar(self, mock_fdb):
        """Test converting regex patterns to SIMILAR TO patterns."""
        engine = FirebirdEngine(self.target, self.plan)

        # Test anchored pattern
        result = engine._convert_regex_to_similar("^test$")
        assert result == "test"

        # Test start anchor
        result = engine._convert_regex_to_similar("^test")
        assert result == "test%"

        # Test end anchor
        result = engine._convert_regex_to_similar("test$")
        assert result == "%test"

        # Test no anchors
        result = engine._convert_regex_to_similar("test")
        assert result == "%test%"

    @patch("sqlitch.engines.firebird.fdb")
    def test_registry_exists_in_db(self, mock_fdb):
        """Test checking if registry exists."""
        mock_conn = Mock()
        mock_fdb.connect.return_value = mock_conn

        engine = FirebirdEngine(self.target, self.plan)
        connection = FirebirdConnection(mock_conn)

        with patch.object(connection, "execute") as mock_execute:
            with patch.object(connection, "fetchone", return_value={"count": 1}):
                exists = engine._registry_exists_in_db(connection)

        assert exists is True
        mock_execute.assert_called_once()

    @patch("sqlitch.engines.firebird.fdb")
    def test_registry_exists_in_db_not_found(self, mock_fdb):
        """Test checking registry when it doesn't exist."""
        mock_conn = Mock()
        mock_fdb.connect.return_value = mock_conn

        engine = FirebirdEngine(self.target, self.plan)
        connection = FirebirdConnection(mock_conn)

        with patch.object(
            connection, "execute", side_effect=Exception("Table not found")
        ):
            exists = engine._registry_exists_in_db(connection)

        assert exists is False


def mock_open(read_data=""):
    """Create a mock for the open builtin."""
    mock_file = MagicMock()
    mock_file.read.return_value = read_data
    mock_file.__enter__.return_value = mock_file
    return MagicMock(return_value=mock_file)
