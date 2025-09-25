"""
Integration tests for Vertica database engine.

This module contains integration tests for the VerticaEngine class,
testing real database operations with a Vertica test database.
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlitch.core.change import Change
from sqlitch.core.exceptions import ConnectionError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.engines.vertica import VerticaEngine, VerticaRegistrySchema

# Skip all tests if vertica-python is not available
vertica_python = pytest.importorskip(
    "vertica_python", reason="vertica-python not available"
)


@pytest.fixture(scope="session")
def vertica_container():
    """
    Provide Vertica container for testing.

    Note: This is a placeholder for actual Vertica container setup.
    In a real test environment, you would use Docker or similar to
    start a Vertica container for testing.
    """
    # Check if we have a test Vertica instance available
    test_host = os.getenv("VERTICA_TEST_HOST", "localhost")
    test_port = int(os.getenv("VERTICA_TEST_PORT", "5433"))
    test_user = os.getenv("VERTICA_TEST_USER", "dbadmin")
    test_password = os.getenv("VERTICA_TEST_PASSWORD", "")
    test_database = os.getenv("VERTICA_TEST_DATABASE", "test")

    # Try to connect to verify Vertica is available
    try:
        conn = vertica_python.connect(
            host=test_host,
            port=test_port,
            user=test_user,
            password=test_password,
            database=test_database,
            connection_timeout=5,
        )
        conn.close()

        return {
            "host": test_host,
            "port": test_port,
            "user": test_user,
            "password": test_password,
            "database": test_database,
        }
    except Exception:
        pytest.skip("Vertica test database not available")


@pytest.fixture
def vertica_target(vertica_container):
    """Create Vertica target for testing."""
    container = vertica_container

    target = Mock(spec=Target)
    target.uri = Mock()
    target.uri.hostname = container["host"]
    target.uri.port = container["port"]
    target.uri.username = container["user"]
    target.uri.password = container["password"]
    target.uri.path = f"/{container['database']}"
    target.uri.query = None
    target.registry = "sqitch_test"
    return target


@pytest.fixture
def test_plan():
    """Create test plan."""
    plan = Mock(spec=Plan)
    plan.project_name = "test_project"
    plan.creator_name = "Test User"
    plan.creator_email = "test@example.com"
    return plan


@pytest.fixture
def test_change():
    """Create test change."""
    change = Mock(spec=Change)
    change.id = "abc123def456"
    change.name = "test_change"
    change.note = "Test change for integration testing"
    change.planner_name = "Test User"
    change.planner_email = "test@example.com"
    change.timestamp = datetime.now(timezone.utc)
    change.dependencies = []
    change.conflicts = []
    change.tags = []
    return change


@pytest.fixture
def vertica_engine(vertica_target, test_plan):
    """Create Vertica engine for testing."""
    return VerticaEngine(vertica_target, test_plan)


@pytest.fixture
def clean_registry(vertica_engine):
    """Clean up registry before and after tests."""
    # Clean up before test
    try:
        with vertica_engine.connection() as conn:
            conn.execute(
                f"DROP SCHEMA IF EXISTS {vertica_engine._registry_schema_name} CASCADE"
            )
    except Exception:
        pass  # Schema might not exist

    yield

    # Clean up after test
    try:
        with vertica_engine.connection() as conn:
            conn.execute(
                f"DROP SCHEMA IF EXISTS {vertica_engine._registry_schema_name} CASCADE"
            )
    except Exception:
        pass  # Ignore cleanup errors


class TestVerticaEngineIntegration:
    """Integration tests for Vertica engine."""

    def test_engine_initialization(self, vertica_target, test_plan):
        """Test engine initialization."""
        engine = VerticaEngine(vertica_target, test_plan)

        assert engine.engine_type == "vertica"
        assert engine.target == vertica_target
        assert engine.plan == test_plan
        assert isinstance(engine.registry_schema, VerticaRegistrySchema)

    def test_connection_creation(self, vertica_engine):
        """Test creating database connection."""
        with vertica_engine.connection() as conn:
            # Test basic query
            conn.execute("SELECT 1 as test_col")
            result = conn.fetchone()
            assert result["test_col"] == 1

    def test_connection_failure_invalid_host(self, test_plan):
        """Test connection failure with invalid host."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.hostname = "invalid-host"
        target.uri.port = 5433
        target.uri.username = "user"
        target.uri.password = None
        target.uri.path = "/db"
        target.uri.query = None
        target.registry = "sqitch"

        engine = VerticaEngine(target, test_plan)

        with pytest.raises(ConnectionError):
            with engine.connection():
                pass

    def test_registry_creation(self, vertica_engine, clean_registry):
        """Test creating registry tables."""
        vertica_engine.ensure_registry()

        # Verify registry tables exist
        with vertica_engine.connection() as conn:
            # Check projects table
            conn.execute(
                f"""
                SELECT COUNT(*) as count 
                FROM v_catalog.tables 
                WHERE schema_name = '{vertica_engine._registry_schema_name}' 
                AND table_name = 'projects'
            """
            )
            result = conn.fetchone()
            assert result["count"] == 1

            # Check changes table
            conn.execute(
                f"""
                SELECT COUNT(*) as count 
                FROM v_catalog.tables 
                WHERE schema_name = '{vertica_engine._registry_schema_name}' 
                AND table_name = 'changes'
            """
            )
            result = conn.fetchone()
            assert result["count"] == 1

            # Check tags table
            conn.execute(
                f"""
                SELECT COUNT(*) as count 
                FROM v_catalog.tables 
                WHERE schema_name = '{vertica_engine._registry_schema_name}' 
                AND table_name = 'tags'
            """
            )
            result = conn.fetchone()
            assert result["count"] == 1

    def test_registry_version(self, vertica_engine, clean_registry):
        """Test registry version tracking."""
        vertica_engine.ensure_registry()

        with vertica_engine.connection() as conn:
            version = vertica_engine._get_registry_version(conn)
            assert version == VerticaRegistrySchema.REGISTRY_VERSION

    def test_sql_file_execution(self, vertica_engine, clean_registry, tmp_path):
        """Test executing SQL file."""
        vertica_engine.ensure_registry()

        # Create test SQL file
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            f"""
            CREATE TABLE {vertica_engine._registry_schema_name}.test_table (
                id INTEGER,
                name VARCHAR(100)
            );
            
            INSERT INTO {vertica_engine._registry_schema_name}.test_table VALUES (1, 'test');
        """
        )

        with vertica_engine.connection() as conn:
            vertica_engine._execute_sql_file(conn, sql_file)

            # Verify table was created and data inserted
            conn.execute(
                f"SELECT COUNT(*) as count FROM {vertica_engine._registry_schema_name}.test_table"
            )
            result = conn.fetchone()
            assert result["count"] == 1

    def test_sql_file_execution_with_variables(
        self, vertica_engine, clean_registry, tmp_path
    ):
        """Test executing SQL file with variable substitution."""
        vertica_engine.ensure_registry()

        # Create test SQL file with variables
        sql_file = tmp_path / "test_vars.sql"
        sql_file.write_text(
            """
            CREATE TABLE &registry.test_vars (
                id INTEGER,
                value VARCHAR(100) DEFAULT '&test_var'
            );
        """
        )

        with vertica_engine.connection() as conn:
            vertica_engine._execute_sql_file(
                conn, sql_file, {"test_var": "substituted_value"}
            )

            # Verify table was created with substituted values
            conn.execute(
                f"""
                SELECT column_default 
                FROM v_catalog.columns 
                WHERE schema_name = '{vertica_engine._registry_schema_name}' 
                AND table_name = 'test_vars' 
                AND column_name = 'value'
            """
            )
            result = conn.fetchone()
            assert "substituted_value" in result["column_default"]

    def test_transaction_management(self, vertica_engine, clean_registry):
        """Test transaction management."""
        vertica_engine.ensure_registry()

        # Test successful transaction
        with vertica_engine.transaction() as conn:
            conn.execute(
                f"""
                CREATE TABLE {vertica_engine._registry_schema_name}.transaction_test (
                    id INTEGER
                )
            """
            )
            conn.execute(
                f"INSERT INTO {vertica_engine._registry_schema_name}.transaction_test VALUES (1)"
            )

        # Verify data was committed
        with vertica_engine.connection() as conn:
            conn.execute(
                f"SELECT COUNT(*) as count FROM {vertica_engine._registry_schema_name}.transaction_test"
            )
            result = conn.fetchone()
            assert result["count"] == 1

        # Test transaction rollback on error
        try:
            with vertica_engine.transaction() as conn:
                conn.execute(
                    f"INSERT INTO {vertica_engine._registry_schema_name}.transaction_test VALUES (2)"
                )
                # Force an error
                conn.execute("SELECT * FROM non_existent_table")
        except Exception:
            pass  # Expected to fail

        # Verify rollback occurred - should still have only 1 row
        with vertica_engine.connection() as conn:
            conn.execute(
                f"SELECT COUNT(*) as count FROM {vertica_engine._registry_schema_name}.transaction_test"
            )
            result = conn.fetchone()
            assert result["count"] == 1

    def test_regex_condition_functionality(self, vertica_engine, clean_registry):
        """Test regex condition in actual database."""
        vertica_engine.ensure_registry()

        with vertica_engine.connection() as conn:
            # Create test data
            conn.execute(
                f"""
                CREATE TABLE {vertica_engine._registry_schema_name}.regex_test (
                    name VARCHAR(100)
                )
            """
            )
            conn.execute(
                f"INSERT INTO {vertica_engine._registry_schema_name}.regex_test VALUES ('test123')"
            )
            conn.execute(
                f"INSERT INTO {vertica_engine._registry_schema_name}.regex_test VALUES ('abc456')"
            )
            conn.execute(
                f"INSERT INTO {vertica_engine._registry_schema_name}.regex_test VALUES ('xyz789')"
            )

            # Test regex condition
            condition = vertica_engine._regex_condition("name", r"test\d+")
            query = f"SELECT COUNT(*) as count FROM {vertica_engine._registry_schema_name}.regex_test WHERE {condition}"

            conn.execute(query, [r"test\d+"])
            result = conn.fetchone()
            assert result["count"] == 1

    def test_connection_parameters_from_environment(self, test_plan):
        """Test connection parameter extraction from environment variables."""
        # Test with environment variables
        env_vars = {
            "VSQL_HOST": "env-host",
            "VSQL_PORT": "9999",
            "VSQL_USER": "env-user",
            "VSQL_PASSWORD": "env-pass",
            "VSQL_DATABASE": "env-db",
        }

        target = Mock(spec=Target)
        target.uri = ValidatedURI("vertica:///db")  # Minimal URI
        target.registry = "sqitch"

        with patch.dict("os.environ", env_vars):
            engine = VerticaEngine(target, test_plan)

            assert engine._get_host() == "env-host"
            assert engine._get_port() == 9999
            assert engine._get_user() == "env-user"
            assert engine._get_password() == "env-pass"
            assert engine._get_database() == "env-db"

    def test_connection_parameters_from_query_string(self, test_plan):
        """Test connection parameter extraction from URI query string."""
        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.hostname = "host"
        target.uri.username = None
        target.uri.password = None
        target.uri.path = "/db"
        target.uri.query = (
            "user=query-user&password=query-pass&connection_load_balance=true"
        )
        target.registry = "sqitch"

        engine = VerticaEngine(target, test_plan)

        assert engine._get_user() == "query-user"
        assert engine._get_password() == "query-pass"

    def test_registry_schema_customization(self, vertica_container, test_plan):
        """Test using custom registry schema."""
        container = vertica_container

        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.hostname = container["host"]
        target.uri.port = container["port"]
        target.uri.username = container["user"]
        target.uri.password = container["password"]
        target.uri.path = f"/{container['database']}"
        target.uri.query = None
        target.registry = "custom_sqitch_schema"

        engine = VerticaEngine(target, test_plan)

        assert engine._get_registry_schema() == "custom_sqitch_schema"
        assert engine._registry_schema_name == "custom_sqitch_schema"

    @pytest.mark.slow
    def test_large_sql_file_execution(self, vertica_engine, clean_registry, tmp_path):
        """Test executing large SQL file."""
        vertica_engine.ensure_registry()

        # Create large SQL file with many statements
        sql_file = tmp_path / "large_test.sql"
        statements = []
        for i in range(100):
            statements.append(
                f"INSERT INTO {vertica_engine._registry_schema_name}.projects (project, creator_name, creator_email) VALUES ('project_{i}', 'user_{i}', 'user_{i}@example.com');"
            )

        sql_file.write_text("\n".join(statements))

        with vertica_engine.connection() as conn:
            vertica_engine._execute_sql_file(conn, sql_file)

            # Verify all statements were executed
            conn.execute(
                f"SELECT COUNT(*) as count FROM {vertica_engine._registry_schema_name}.projects"
            )
            result = conn.fetchone()
            assert (
                result["count"] >= 100
            )  # At least 100 (might have initial project record)

    def test_concurrent_connections(self, vertica_engine, clean_registry):
        """Test multiple concurrent connections."""
        vertica_engine.ensure_registry()

        # Test that multiple connections can be created
        connections = []
        try:
            for i in range(5):
                conn = vertica_engine._create_connection()
                connections.append(conn)

                # Test each connection works
                conn.execute("SELECT 1 as test")
                result = conn.fetchone()
                assert result["test"] == 1
        finally:
            # Clean up connections
            for conn in connections:
                try:
                    conn.close()
                except Exception:
                    pass

    def test_error_handling_in_sql_execution(
        self, vertica_engine, clean_registry, tmp_path
    ):
        """Test error handling during SQL execution."""
        vertica_engine.ensure_registry()

        # Create SQL file with invalid SQL
        sql_file = tmp_path / "invalid.sql"
        sql_file.write_text("SELECT * FROM non_existent_table;")

        with vertica_engine.connection() as conn:
            with pytest.raises(Exception):  # Should raise some kind of database error
                vertica_engine._execute_sql_file(conn, sql_file)
