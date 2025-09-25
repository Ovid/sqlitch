"""
Unit tests for Exasol database engine.

This module contains comprehensive unit tests for the ExasolEngine class,
testing connection management, registry operations, and SQL execution
with proper mocking of the pyexasol dependency.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch
from urllib.parse import urlparse

import pytest

from sqlitch.core.change import Change
from sqlitch.core.exceptions import ConnectionError, DeploymentError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.core.types import EngineType
from sqlitch.engines.exasol import ExasolConnection, ExasolEngine, ExasolRegistrySchema


class TestExasolRegistrySchema:
    """Test ExasolRegistrySchema class."""

    def test_get_create_statements_with_schema(self):
        """Test getting create statements with schema prefix."""
        statements = ExasolRegistrySchema.get_create_statements(
            "exasol", registry_schema="test_schema"
        )

        assert len(statements) == 7  # Schema + 6 tables
        assert "CREATE SCHEMA IF NOT EXISTS test_schema" in statements[0]
        assert "test_schema.releases" in statements[1]
        assert "test_schema.projects" in statements[2]
        assert "test_schema.changes" in statements[3]
        assert "test_schema.tags" in statements[4]
        assert "test_schema.dependencies" in statements[5]
        assert "test_schema.events" in statements[6]

    def test_get_create_statements_without_schema(self):
        """Test getting create statements without schema prefix."""
        statements = ExasolRegistrySchema.get_create_statements(
            "exasol", registry_schema=None
        )

        assert len(statements) == 7
        assert statements[0] == ""  # Empty schema creation
        assert "releases" in statements[1]
        assert "projects" in statements[2]


class TestExasolConnection:
    """Test ExasolConnection wrapper class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_conn = Mock()
        self.connection = ExasolConnection(self.mock_conn)

    def test_execute_without_params(self):
        """Test executing SQL without parameters."""
        sql = "SELECT * FROM test_table"
        self.mock_conn.execute.return_value = "result"

        result = self.connection.execute(sql)

        self.mock_conn.execute.assert_called_once_with(sql)
        assert result == "result"

    def test_execute_with_params(self):
        """Test executing SQL with named parameters."""
        sql = "SELECT * FROM test_table WHERE id = :id AND name = :name"
        params = {"id": 1, "name": "test"}
        self.mock_conn.execute.return_value = "result"

        result = self.connection.execute(sql, params)

        # Should convert named params to positional
        expected_sql = "SELECT * FROM test_table WHERE id = ? AND name = ?"
        self.mock_conn.execute.assert_called_once_with(expected_sql, [1, "test"])
        assert result == "result"

    def test_fetchone_with_result(self):
        """Test fetching one row with result."""
        self.mock_conn.fetchone.return_value = (1, "test", "value")
        self.mock_conn.description = [("id",), ("name",), ("value",)]

        result = self.connection.fetchone()

        assert result == {"id": 1, "name": "test", "value": "value"}

    def test_fetchone_no_result(self):
        """Test fetching one row with no result."""
        self.mock_conn.fetchone.return_value = None

        result = self.connection.fetchone()

        assert result is None

    def test_fetchall_with_results(self):
        """Test fetching all rows with results."""
        self.mock_conn.fetchall.return_value = [(1, "test"), (2, "test2")]
        self.mock_conn.description = [("id",), ("name",)]

        result = self.connection.fetchall()

        assert result == [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]

    def test_fetchall_no_results(self):
        """Test fetching all rows with no results."""
        self.mock_conn.fetchall.return_value = []

        result = self.connection.fetchall()

        assert result == []

    def test_commit(self):
        """Test committing transaction."""
        self.connection.commit()
        self.mock_conn.commit.assert_called_once()

    def test_rollback(self):
        """Test rolling back transaction."""
        self.connection.rollback()
        self.mock_conn.rollback.assert_called_once()

    def test_close(self):
        """Test closing connection."""
        self.connection.close()
        self.mock_conn.close.assert_called_once()


class TestExasolEngine:
    """Test ExasolEngine class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.target = Target(
            name="test_target",
            uri=urlparse(
                "exasol://user:pass@localhost:8563/test_db?schema=test_schema"
            ),
            registry="test_registry",
        )

        self.plan = Plan(
            file=Path("sqitch.plan"),
            project="test_project",
            uri="https://example.com/test_project",
        )

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_init_success(self, mock_pyexasol):
        """Test successful engine initialization."""
        engine = ExasolEngine(self.target, self.plan)

        assert engine.engine_type == "exasol"
        assert isinstance(engine.registry_schema, ExasolRegistrySchema)
        assert engine._host == "localhost"
        assert engine._port == 8563
        assert engine._username == "user"
        assert engine._password == "pass"
        assert engine._database == "test_db"
        assert engine._registry_schema_name == "test_registry"

    def test_init_no_pyexasol(self):
        """Test engine initialization without pyexasol."""
        with patch("sqlitch.engines.exasol.pyexasol", None):
            with pytest.raises(EngineError) as exc_info:
                ExasolEngine(self.target, self.plan)

            assert "pyexasol package is required" in str(exc_info.value)

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_parse_connection_params_defaults(self, mock_pyexasol):
        """Test parsing connection parameters with defaults."""
        target = Target(
            name="test", uri=urlparse("exasol://user@host/db"), registry=None
        )

        engine = ExasolEngine(target, self.plan)

        assert engine._host == "host"
        assert engine._port == 8563  # Default port
        assert engine._username == "user"
        assert engine._password is None
        assert engine._database == "db"
        assert engine._registry_schema_name == "sqitch"  # Default registry

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_parse_connection_params_with_query(self, mock_pyexasol):
        """Test parsing connection parameters with query string."""
        target = Target(
            name="test",
            uri=urlparse("exasol://user:pass@host:9999/db?timeout=30&ssl=true"),
            registry="custom_registry",
        )

        engine = ExasolEngine(target, self.plan)

        assert engine._host == "host"
        assert engine._port == 9999
        assert engine._connection_options == {"timeout": "30", "ssl": "true"}
        assert engine._registry_schema_name == "custom_registry"

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_create_connection_success(self, mock_pyexasol):
        """Test successful connection creation."""
        mock_conn = Mock()
        mock_pyexasol.connect.return_value = mock_conn

        engine = ExasolEngine(self.target, self.plan)
        connection = engine._create_connection()

        # Verify connection parameters
        mock_pyexasol.connect.assert_called_once_with(
            dsn="localhost:8563",
            user="user",
            password="pass",
            schema="test_schema",  # From query params
            autocommit=False,
            fetch_dict=False,
        )

        # Verify session setup
        expected_calls = [
            call("ALTER SESSION SET nls_date_format='YYYY-MM-DD HH24:MI:SS'"),
            call("ALTER SESSION SET nls_timestamp_format='YYYY-MM-DD HH24:MI:SS'"),
            call("ALTER SESSION SET TIME_ZONE='UTC'"),
            call("OPEN SCHEMA test_registry"),
        ]
        mock_conn.execute.assert_has_calls(expected_calls)

        assert isinstance(connection, ExasolConnection)

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_create_connection_failure(self, mock_pyexasol):
        """Test connection creation failure."""
        mock_pyexasol.connect.side_effect = Exception("Connection failed")

        engine = ExasolEngine(self.target, self.plan)

        with pytest.raises(ConnectionError) as exc_info:
            engine._create_connection()

        assert "Failed to connect to Exasol database" in str(exc_info.value)

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_execute_sql_file_success(self, mock_pyexasol):
        """Test successful SQL file execution."""
        engine = ExasolEngine(self.target, self.plan)

        # Create mock connection
        mock_conn = Mock()
        connection = ExasolConnection(mock_conn)

        # Create temporary SQL file
        sql_content = """
        -- Comment line
        CREATE TABLE test (id INT);

        INSERT INTO test VALUES (1);
        INSERT INTO test VALUES (2);
        """

        with patch("pathlib.Path.read_text", return_value=sql_content):
            sql_file = Path("test.sql")
            engine._execute_sql_file(connection, sql_file)

        # Verify SQL statements were executed
        expected_calls = [
            call("CREATE TABLE test (id INT);"),
            call("INSERT INTO test VALUES (1);"),
            call("INSERT INTO test VALUES (2);"),
        ]
        mock_conn.execute.assert_has_calls(expected_calls)

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_execute_sql_file_with_variables(self, mock_pyexasol):
        """Test SQL file execution with variable substitution."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        connection = ExasolConnection(mock_conn)

        sql_content = (
            "CREATE SCHEMA &registry; CREATE TABLE &registry.&table_name (id INT);"
        )
        variables = {"table_name": "test_table"}

        with patch("pathlib.Path.read_text", return_value=sql_content):
            sql_file = Path("test.sql")
            engine._execute_sql_file(connection, sql_file, variables)

        # Verify variable substitution - should be called once with combined statement
        mock_conn.execute.assert_called_once_with(
            "CREATE SCHEMA test_registry; CREATE TABLE test_registry.test_table (id INT);"
        )

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_execute_sql_file_failure(self, mock_pyexasol):
        """Test SQL file execution failure."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("SQL error")
        connection = ExasolConnection(mock_conn)

        with patch("pathlib.Path.read_text", return_value="SELECT 1;"):
            sql_file = Path("test.sql")

            with pytest.raises(DeploymentError) as exc_info:
                engine._execute_sql_file(connection, sql_file)

            assert "Failed to execute SQL file" in str(exc_info.value)

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_get_registry_version_exists(self, mock_pyexasol):
        """Test getting registry version when it exists."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        # Mock the ExasolConnection wrapper behavior
        connection = ExasolConnection(mock_conn)

        # Mock fetchone to return the expected result
        with patch.object(connection, "fetchone", return_value={"version": 1.1}):
            version = engine._get_registry_version(connection)

        assert version == "1.1"

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_get_registry_version_not_exists(self, mock_pyexasol):
        """Test getting registry version when it doesn't exist."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Table not found")
        connection = ExasolConnection(mock_conn)

        version = engine._get_registry_version(connection)

        assert version is None

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_registry_exists_in_db_true(self, mock_pyexasol):
        """Test registry existence check when registry exists."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        connection = ExasolConnection(mock_conn)

        # Mock fetchone to return the expected result
        with patch.object(connection, "fetchone", return_value={"exists": True}):
            exists = engine._registry_exists_in_db(connection)

        assert exists is True

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_registry_exists_in_db_false(self, mock_pyexasol):
        """Test registry existence check when registry doesn't exist."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Table not found")
        connection = ExasolConnection(mock_conn)

        exists = engine._registry_exists_in_db(connection)

        assert exists is False

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_regex_condition(self, mock_pyexasol):
        """Test regex condition generation."""
        engine = ExasolEngine(self.target, self.plan)

        condition = engine._regex_condition("column_name", "pattern")

        assert condition == "REGEXP_LIKE(column_name, ?)"

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_create_registry_success(self, mock_pyexasol):
        """Test successful registry creation."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        connection = ExasolConnection(mock_conn)

        engine._create_registry(connection)

        # Verify registry creation statements were executed
        assert (
            mock_conn.execute.call_count >= 7
        )  # Schema + 6 tables + version + project

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_create_registry_failure(self, mock_pyexasol):
        """Test registry creation failure."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Creation failed")
        connection = ExasolConnection(mock_conn)

        with pytest.raises(EngineError) as exc_info:
            engine._create_registry(connection)

        assert "Failed to create Exasol registry" in str(exc_info.value)

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_insert_project_record_new_project(self, mock_pyexasol):
        """Test inserting new project record."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        mock_conn.fetchone.return_value = {"count": 0}  # Project doesn't exist
        connection = ExasolConnection(mock_conn)

        engine._insert_project_record(connection)

        # Verify project was inserted
        assert mock_conn.execute.call_count == 2  # Check + insert

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_insert_project_record_existing_project(self, mock_pyexasol):
        """Test inserting project record when project already exists."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        connection = ExasolConnection(mock_conn)

        # Mock fetchone to return project exists
        with patch.object(connection, "fetchone", return_value={"count": 1}):
            engine._insert_project_record(connection)

        # Verify only check was performed via execute, no insert
        assert mock_conn.execute.call_count == 1

    def test_split_sql_statements(self):
        """Test splitting SQL content into statements."""
        with patch("sqlitch.engines.exasol.pyexasol"):
            engine = ExasolEngine(self.target, self.plan)

        sql_content = """
        -- This is a comment
        CREATE TABLE test (id INT);

        INSERT INTO test VALUES (1);
        INSERT INTO test VALUES (2);

        -- Another comment
        SELECT * FROM test;
        """

        statements = engine._split_sql_statements(sql_content)

        expected = [
            "CREATE TABLE test (id INT);",
            "INSERT INTO test VALUES (1);",
            "INSERT INTO test VALUES (2);",
            "SELECT * FROM test;",
        ]

        assert statements == expected

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_get_deployed_changes_success(self, mock_pyexasol):
        """Test getting deployed changes successfully."""
        engine = ExasolEngine(self.target, self.plan)

        # Mock the connection context manager and registry check
        mock_conn = Mock()
        connection = ExasolConnection(mock_conn)

        with patch.object(engine, "ensure_registry"):
            with patch.object(engine, "connection") as mock_context:
                mock_context.return_value.__enter__.return_value = connection
                # Mock fetchall to return the expected results
                with patch.object(
                    connection,
                    "fetchall",
                    return_value=[{"change_id": "change1"}, {"change_id": "change2"}],
                ):
                    changes = engine.get_deployed_changes()

        assert changes == ["change1", "change2"]

    @patch("sqlitch.engines.exasol.pyexasol")
    def test_get_deployed_changes_failure(self, mock_pyexasol):
        """Test getting deployed changes with failure."""
        engine = ExasolEngine(self.target, self.plan)

        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Query failed")

        with patch.object(engine, "ensure_registry"):
            with patch.object(engine, "connection") as mock_context:
                mock_context.return_value.__enter__.return_value = ExasolConnection(
                    mock_conn
                )

                with pytest.raises(EngineError) as exc_info:
                    engine.get_deployed_changes()

        assert "Failed to get deployed changes" in str(exc_info.value)
