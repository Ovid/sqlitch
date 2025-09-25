"""
Integration tests for Exasol database engine.

This module contains integration tests for the ExasolEngine class,
testing real database operations with a test Exasol database.
These tests require a running Exasol database instance.
"""

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pytest

from sqlitch.core.change import Change
from sqlitch.core.exceptions import ConnectionError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.engines.exasol import ExasolEngine

# Skip integration tests if pyexasol is not available or no test database
pytest_plugins = []

try:
    import pyexasol

    EXASOL_AVAILABLE = True
except ImportError:
    EXASOL_AVAILABLE = False

# Test database configuration from environment
EXASOL_TEST_DSN = os.getenv("EXASOL_TEST_DSN", "localhost:8563")
EXASOL_TEST_USER = os.getenv("EXASOL_TEST_USER", "sys")
EXASOL_TEST_PASSWORD = os.getenv("EXASOL_TEST_PASSWORD", "exasol")
EXASOL_TEST_SCHEMA = os.getenv("EXASOL_TEST_SCHEMA", "test_schema")

# Skip all tests if Exasol is not available or not configured
pytestmark = pytest.mark.skipif(
    not EXASOL_AVAILABLE
    or not all([EXASOL_TEST_DSN, EXASOL_TEST_USER, EXASOL_TEST_PASSWORD]),
    reason="Exasol not available or not configured for testing",
)


@pytest.fixture(scope="session")
def exasol_test_connection():
    """
    Provide a test Exasol connection for integration tests.

    This fixture creates a connection to a test Exasol database
    and ensures it's available for testing.
    """
    if not EXASOL_AVAILABLE:
        pytest.skip("pyexasol not available")

    try:
        conn = pyexasol.connect(
            dsn=EXASOL_TEST_DSN,
            user=EXASOL_TEST_USER,
            password=EXASOL_TEST_PASSWORD,
            autocommit=True,
        )

        # Test connection
        conn.execute("SELECT 1")

        yield conn

        conn.close()

    except Exception as e:
        pytest.skip(f"Cannot connect to test Exasol database: {e}")


@pytest.fixture
def test_target():
    """Create a test target configuration."""
    uri_string = f"exasol://{EXASOL_TEST_USER}:{EXASOL_TEST_PASSWORD}@{EXASOL_TEST_DSN}/{EXASOL_TEST_SCHEMA}"
    return Target(name="exasol_test", uri=urlparse(uri_string), registry="sqlitch_test")


@pytest.fixture
def test_plan():
    """Create a test plan."""
    return Plan(
        file=Path("sqitch.plan"),
        project="test_project",
        uri="https://example.com/test_project",
    )


@pytest.fixture
def test_engine(test_target, test_plan):
    """Create a test Exasol engine."""
    return ExasolEngine(test_target, test_plan)


@pytest.fixture
def clean_database(exasol_test_connection, test_target):
    """
    Ensure clean database state for each test.

    This fixture drops and recreates the test schema to ensure
    each test starts with a clean state.
    """
    conn = exasol_test_connection
    registry_schema = test_target.registry

    # Clean up before test
    try:
        conn.execute(f"DROP SCHEMA {registry_schema} CASCADE")
    except Exception:
        pass  # Schema might not exist

    yield

    # Clean up after test
    try:
        conn.execute(f"DROP SCHEMA {registry_schema} CASCADE")
    except Exception:
        pass  # Ignore cleanup errors


class TestExasolEngineIntegration:
    """Integration tests for ExasolEngine."""

    def test_connection_creation(self, test_engine, clean_database):
        """Test creating a database connection."""
        with test_engine.connection() as conn:
            # Test basic query
            conn.execute("SELECT 1 as test_value")
            result = conn.fetchone()
            assert result["test_value"] == 1

    def test_connection_failure_invalid_credentials(self, test_plan):
        """Test connection failure with invalid credentials."""
        invalid_target = Target(
            name="invalid_test",
            uri=urlparse(f"exasol://invalid:invalid@{EXASOL_TEST_DSN}/test"),
            registry="test_registry",
        )

        engine = ExasolEngine(invalid_target, test_plan)

        with pytest.raises(ConnectionError):
            with engine.connection():
                pass

    def test_registry_creation(self, test_engine, clean_database):
        """Test creating registry tables."""
        # Ensure registry doesn't exist initially
        with test_engine.connection() as conn:
            assert not test_engine._registry_exists_in_db(conn)

        # Create registry
        test_engine.ensure_registry()

        # Verify registry exists
        with test_engine.connection() as conn:
            assert test_engine._registry_exists_in_db(conn)

            # Verify all tables exist
            tables = [
                "releases",
                "projects",
                "changes",
                "tags",
                "dependencies",
                "events",
            ]
            for table in tables:
                conn.execute(
                    f"SELECT COUNT(*) as count FROM {test_engine._registry_schema_name}.{table}"
                )
                result = conn.fetchone()
                assert "count" in result  # Table exists and is queryable

    def test_registry_version_tracking(self, test_engine, clean_database):
        """Test registry version tracking."""
        # Create registry
        test_engine.ensure_registry()

        # Check version
        with test_engine.connection() as conn:
            version = test_engine._get_registry_version(conn)
            assert version == test_engine.registry_schema.REGISTRY_VERSION

    def test_project_record_insertion(self, test_engine, clean_database):
        """Test project record insertion."""
        # Create registry
        test_engine.ensure_registry()

        # Verify project record exists
        with test_engine.connection() as conn:
            conn.execute(
                f"SELECT * FROM {test_engine._registry_schema_name}.projects WHERE project = ?",
                {"project": test_engine.plan.project_name},
            )
            result = conn.fetchone()

            assert result is not None
            assert result["project"] == test_engine.plan.project_name
            assert result["creator_name"] == test_engine.plan.creator_name
            assert result["creator_email"] == test_engine.plan.creator_email

    def test_sql_file_execution(self, test_engine, clean_database):
        """Test executing SQL files."""
        # Create registry first
        test_engine.ensure_registry()

        # Create temporary SQL file
        sql_content = f"""
        CREATE TABLE {test_engine._registry_schema_name}.test_table (
            id INTEGER,
            name VARCHAR(100)
        );

        INSERT INTO {test_engine._registry_schema_name}.test_table VALUES (1, 'test1');
        INSERT INTO {test_engine._registry_schema_name}.test_table VALUES (2, 'test2');
        """

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(sql_content)
            sql_file = Path(f.name)

        try:
            # Execute SQL file
            with test_engine.connection() as conn:
                test_engine._execute_sql_file(conn, sql_file)

                # Verify results
                conn.execute(
                    f"SELECT COUNT(*) as count FROM {test_engine._registry_schema_name}.test_table"
                )
                result = conn.fetchone()
                assert result["count"] == 2

                conn.execute(
                    f"SELECT * FROM {test_engine._registry_schema_name}.test_table ORDER BY id"
                )
                results = conn.fetchall()
                assert len(results) == 2
                assert results[0]["name"] == "test1"
                assert results[1]["name"] == "test2"

        finally:
            # Clean up temporary file
            sql_file.unlink()

    def test_sql_file_execution_with_variables(self, test_engine, clean_database):
        """Test executing SQL files with variable substitution."""
        # Create registry first
        test_engine.ensure_registry()

        # Create SQL file with variables
        sql_content = """
        CREATE TABLE &registry.&table_name (
            id INTEGER,
            value VARCHAR(&max_length)
        );

        INSERT INTO &registry.&table_name VALUES (1, '&test_value');
        """

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(sql_content)
            sql_file = Path(f.name)

        try:
            variables = {
                "table_name": "variable_test",
                "max_length": "50",
                "test_value": "hello",
            }

            # Execute SQL file with variables
            with test_engine.connection() as conn:
                test_engine._execute_sql_file(conn, sql_file, variables)

                # Verify results
                conn.execute(
                    f"SELECT * FROM {test_engine._registry_schema_name}.variable_test"
                )
                result = conn.fetchone()
                assert result["id"] == 1
                assert result["value"] == "hello"

        finally:
            # Clean up temporary file
            sql_file.unlink()

    def test_deployed_changes_tracking(self, test_engine, clean_database):
        """Test tracking deployed changes."""
        # Create registry
        test_engine.ensure_registry()

        # Initially no changes
        changes = test_engine.get_deployed_changes()
        assert changes == []

        # Add a change record manually
        change_id = "test_change_123"
        with test_engine.connection() as conn:
            conn.execute(
                f"""
                INSERT INTO {test_engine._registry_schema_name}.changes
                (change_id, script_hash, change, project, note, committed_at,
                 committer_name, committer_email, planned_at, planner_name, planner_email)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                {
                    "change_id": change_id,
                    "script_hash": "abc123",
                    "change": "test_change",
                    "project": test_engine.plan.project_name,
                    "note": "Test change",
                    "committed_at": datetime.now(timezone.utc),
                    "committer_name": "Test User",
                    "committer_email": "test@example.com",
                    "planned_at": datetime.now(timezone.utc),
                    "planner_name": "Test User",
                    "planner_email": "test@example.com",
                },
            )

        # Verify change is tracked
        changes = test_engine.get_deployed_changes()
        assert changes == [change_id]

    def test_regex_condition_functionality(self, test_engine, clean_database):
        """Test regex condition functionality."""
        # Create registry and test data
        test_engine.ensure_registry()

        with test_engine.connection() as conn:
            # Insert test data
            conn.execute(
                f"""
                INSERT INTO {test_engine._registry_schema_name}.changes
                (change_id, script_hash, change, project, note, committed_at,
                 committer_name, committer_email, planned_at, planner_name, planner_email)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                {
                    "change_id": "test_123",
                    "script_hash": "abc123",
                    "change": "add_users_table",
                    "project": test_engine.plan.project_name,
                    "note": "Add users table",
                    "committed_at": datetime.now(timezone.utc),
                    "committer_name": "Test User",
                    "committer_email": "test@example.com",
                    "planned_at": datetime.now(timezone.utc),
                    "planner_name": "Test User",
                    "planner_email": "test@example.com",
                },
            )

            # Test regex condition
            regex_condition = test_engine._regex_condition("change", "users")
            query = f"""
                SELECT change FROM {test_engine._registry_schema_name}.changes
                WHERE {regex_condition}
            """

            conn.execute(query, [".*users.*"])
            result = conn.fetchone()
            assert result is not None
            assert result["change"] == "add_users_table"

    def test_transaction_management(self, test_engine, clean_database):
        """Test transaction management with rollback."""
        # Create registry
        test_engine.ensure_registry()

        # Test successful transaction
        with test_engine.transaction() as conn:
            conn.execute(
                f"CREATE TABLE {test_engine._registry_schema_name}.transaction_test (id INTEGER)"
            )

        # Verify table exists
        with test_engine.connection() as conn:
            conn.execute(
                f"SELECT COUNT(*) as count FROM {test_engine._registry_schema_name}.transaction_test"
            )
            result = conn.fetchone()
            assert "count" in result  # Table exists

        # Test transaction rollback
        try:
            with test_engine.transaction() as conn:
                conn.execute(
                    f"CREATE TABLE {test_engine._registry_schema_name}.rollback_test (id INTEGER)"
                )
                # Force an error to trigger rollback
                raise Exception("Forced error")
        except Exception:
            pass  # Expected

        # Verify rollback_test table doesn't exist
        with test_engine.connection() as conn:
            try:
                conn.execute(
                    f"SELECT COUNT(*) FROM {test_engine._registry_schema_name}.rollback_test"
                )
                assert False, "Table should not exist after rollback"
            except Exception:
                pass  # Expected - table should not exist

    def test_multiple_connections(self, test_engine, clean_database):
        """Test handling multiple connections."""
        # Create registry
        test_engine.ensure_registry()

        # Test multiple concurrent connections
        connections = []
        try:
            for i in range(3):
                conn_context = test_engine.connection()
                conn = conn_context.__enter__()
                connections.append((conn_context, conn))

                # Test each connection
                conn.execute("SELECT ? as connection_id", {"connection_id": i})
                result = conn.fetchone()
                assert result["connection_id"] == i

        finally:
            # Clean up connections
            for conn_context, conn in connections:
                try:
                    conn_context.__exit__(None, None, None)
                except Exception:
                    pass

    def test_schema_operations(self, test_engine, clean_database):
        """Test schema-specific operations."""
        # Create registry
        test_engine.ensure_registry()

        with test_engine.connection() as conn:
            # Test schema exists
            conn.execute(
                "SELECT schema_name FROM exa_all_schemas WHERE schema_name = ?",
                {"schema_name": test_engine._registry_schema_name.upper()},
            )
            result = conn.fetchone()
            assert result is not None
            assert result["schema_name"] == test_engine._registry_schema_name.upper()

            # Test opening schema
            conn.execute(f"OPEN SCHEMA {test_engine._registry_schema_name}")

            # Test creating objects in schema
            conn.execute("CREATE TABLE test_schema_table (id INTEGER)")
            conn.execute("INSERT INTO test_schema_table VALUES (42)")

            # Verify object exists in correct schema
            conn.execute("SELECT id FROM test_schema_table")
            result = conn.fetchone()
            assert result["id"] == 42
