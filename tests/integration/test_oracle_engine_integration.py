"""
Integration tests for Oracle database engine.

These tests require a running Oracle database instance and test the full
integration of the Oracle engine with a real database.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pytest

from sqlitch.core.change import Change
from sqlitch.core.exceptions import ConnectionError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.core.types import URI
from sqlitch.engines.base import EngineRegistry
from sqlitch.engines.oracle import OracleEngine

# Skip all tests if cx_Oracle is not available
cx_Oracle = pytest.importorskip("cx_Oracle", reason="cx_Oracle not available")


@pytest.fixture(scope="session")
def oracle_container():
    """
    Provide Oracle container for testing.

    This fixture attempts to use a running Oracle container or skips tests
    if Oracle is not available.
    """
    # Check if Oracle connection details are provided via environment
    oracle_host = os.environ.get("ORACLE_TEST_HOST", "localhost")
    oracle_port = os.environ.get("ORACLE_TEST_PORT", "1521")
    oracle_service = os.environ.get("ORACLE_TEST_SERVICE", "XE")
    oracle_user = os.environ.get("ORACLE_TEST_USER", "system")
    oracle_password = os.environ.get("ORACLE_TEST_PASSWORD", "oracle")

    # Try to connect to Oracle to verify it's available
    try:
        dsn = cx_Oracle.makedsn(oracle_host, oracle_port, service_name=oracle_service)
        connection = cx_Oracle.connect(
            user=oracle_user, password=oracle_password, dsn=dsn
        )
        connection.close()

        return {
            "host": oracle_host,
            "port": oracle_port,
            "service": oracle_service,
            "user": oracle_user,
            "password": oracle_password,
        }
    except Exception as e:
        pytest.skip(f"Oracle database not available: {e}")


@pytest.fixture
def oracle_target(oracle_container):
    """Create Oracle target for testing."""
    return Target(
        name="test_oracle",
        uri=URI(
            f"oracle://{oracle_container['user']}:{oracle_container['password']}@"
            f"{oracle_container['host']}:{oracle_container['port']}/{oracle_container['service']}"
        ),
        registry=oracle_container["user"].upper(),
    )


@pytest.fixture
def test_plan():
    """Create test plan."""
    return Plan(
        file=Path("sqitch.plan"),
        project="test_oracle_project",
        uri="https://github.com/test/oracle-project",
        changes=[],
    )


@pytest.fixture
def oracle_engine(oracle_target, test_plan):
    """Create Oracle engine instance."""
    return OracleEngine(oracle_target, test_plan)


@pytest.fixture
def test_change():
    """Create test change."""
    return Change(
        id="test_change_123",
        name="test_change",
        note="Test change for Oracle integration",
        timestamp=datetime.now(),
        planner_name="Test Planner",
        planner_email="planner@example.com",
        dependencies=[],
        conflicts=[],
    )


class TestOracleEngineIntegration:
    """Integration tests for Oracle engine."""

    def test_engine_registration(self):
        """Test that Oracle engine is properly registered."""
        supported_engines = EngineRegistry.list_supported_engines()
        assert "oracle" in supported_engines

        # Test engine creation
        target = Target(name="test", uri=URI("oracle://user:pass@host/db"))
        plan = Plan(file=Path("sqitch.plan"), project="test", uri="", changes=[])

        engine = EngineRegistry.create_engine("oracle", target, plan)
        assert isinstance(engine, OracleEngine)

    def test_connection_creation(self, oracle_engine):
        """Test creating Oracle database connection."""
        with oracle_engine.connection() as conn:
            assert conn is not None

            # Test basic query
            conn.execute("SELECT 1 FROM dual")
            result = conn.fetchone()
            assert result is not None

    def test_connection_failure_invalid_credentials(self, oracle_container, test_plan):
        """Test connection failure with invalid credentials."""
        target = Target(
            name="test_invalid",
            uri=URI(
                f"oracle://invalid:invalid@{oracle_container['host']}:{oracle_container['port']}/{oracle_container['service']}"
            ),
            registry="invalid",
        )

        engine = OracleEngine(target, test_plan)

        with pytest.raises(ConnectionError):
            with engine.connection():
                pass

    def test_registry_creation(self, oracle_engine):
        """Test creating registry tables."""
        # Clean up any existing registry first
        with oracle_engine.connection() as conn:
            try:
                # Drop tables if they exist (in reverse dependency order)
                tables = [
                    "events",
                    "dependencies",
                    "tags",
                    "changes",
                    "projects",
                    "releases",
                ]
                for table in tables:
                    try:
                        conn.execute(
                            f"DROP TABLE {oracle_engine._registry_schema}.{table} CASCADE CONSTRAINTS"
                        )
                    except Exception:
                        pass  # Table might not exist

                # Drop type if it exists
                try:
                    conn.execute(
                        f"DROP TYPE {oracle_engine._registry_schema}.sqitch_array"
                    )
                except Exception:
                    pass  # Type might not exist

            except Exception:
                pass  # Ignore cleanup errors

        # Test registry creation
        oracle_engine.ensure_registry()

        # Verify tables were created
        with oracle_engine.connection() as conn:
            # Check that all required tables exist
            conn.execute(
                """
                SELECT table_name FROM all_tables
                WHERE owner = ? AND table_name IN ('RELEASES', 'PROJECTS', 'CHANGES', 'TAGS', 'DEPENDENCIES', 'EVENTS')
            """,
                {"owner": oracle_engine._registry_schema},
            )

            tables = conn.fetchall()
            table_names = [row["table_name"] for row in tables]

            assert "RELEASES" in table_names
            assert "PROJECTS" in table_names
            assert "CHANGES" in table_names
            assert "TAGS" in table_names
            assert "DEPENDENCIES" in table_names
            assert "EVENTS" in table_names

    def test_registry_already_exists(self, oracle_engine):
        """Test handling when registry already exists."""
        # Ensure registry exists
        oracle_engine.ensure_registry()

        # Call again - should not fail
        oracle_engine.ensure_registry()

        # Verify registry is still functional
        with oracle_engine.connection() as conn:
            conn.execute(
                f"SELECT COUNT(*) as count FROM {oracle_engine._registry_schema}.projects"
            )
            result = conn.fetchone()
            assert result["count"] >= 0

    def test_get_registry_version(self, oracle_engine):
        """Test getting registry version."""
        oracle_engine.ensure_registry()

        with oracle_engine.connection() as conn:
            version = oracle_engine._get_registry_version(conn)
            assert version is not None
            assert float(version) > 0

    def test_deploy_change(self, oracle_engine, test_change, tmp_path):
        """Test deploying a change."""
        oracle_engine.ensure_registry()

        # Create deploy script
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        deploy_file = deploy_dir / f"{test_change.name}.sql"
        deploy_file.write_text(
            "CREATE TABLE test_deploy_table (id NUMBER PRIMARY KEY);"
        )

        # Mock plan to return our deploy file
        oracle_engine.plan.get_deploy_file = lambda change: deploy_file

        # Deploy the change
        oracle_engine.deploy_change(test_change)

        # Verify change was recorded in registry
        with oracle_engine.connection() as conn:
            conn.execute(
                f"SELECT COUNT(*) as count FROM {oracle_engine._registry_schema}.changes WHERE change_id = ?",
                {"change_id": test_change.id},
            )
            result = conn.fetchone()
            assert result["count"] == 1

        # Verify table was created
        with oracle_engine.connection() as conn:
            conn.execute("SELECT COUNT(*) as count FROM test_deploy_table")
            result = conn.fetchone()
            assert result["count"] == 0  # Table exists but is empty

    def test_revert_change(self, oracle_engine, test_change, tmp_path):
        """Test reverting a change."""
        oracle_engine.ensure_registry()

        # First deploy the change
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        deploy_file = deploy_dir / f"{test_change.name}.sql"
        deploy_file.write_text(
            "CREATE TABLE test_revert_table (id NUMBER PRIMARY KEY);"
        )

        # Create revert script
        revert_dir = tmp_path / "revert"
        revert_dir.mkdir()
        revert_file = revert_dir / f"{test_change.name}.sql"
        revert_file.write_text("DROP TABLE test_revert_table;")

        # Mock plan methods
        oracle_engine.plan.get_deploy_file = lambda change: deploy_file
        oracle_engine.plan.get_revert_file = lambda change: revert_file

        # Deploy then revert
        oracle_engine.deploy_change(test_change)
        oracle_engine.revert_change(test_change)

        # Verify change was removed from registry
        with oracle_engine.connection() as conn:
            conn.execute(
                f"SELECT COUNT(*) as count FROM {oracle_engine._registry_schema}.changes WHERE change_id = ?",
                {"change_id": test_change.id},
            )
            result = conn.fetchone()
            assert result["count"] == 0

        # Verify table was dropped
        with oracle_engine.connection() as conn:
            try:
                conn.execute("SELECT COUNT(*) FROM test_revert_table")
                assert False, "Table should have been dropped"
            except Exception:
                pass  # Expected - table should not exist

    def test_verify_change(self, oracle_engine, test_change, tmp_path):
        """Test verifying a change."""
        oracle_engine.ensure_registry()

        # Create and deploy a change first
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        deploy_file = deploy_dir / f"{test_change.name}.sql"
        deploy_file.write_text(
            "CREATE TABLE test_verify_table (id NUMBER PRIMARY KEY);"
        )

        # Create verify script
        verify_dir = tmp_path / "verify"
        verify_dir.mkdir()
        verify_file = verify_dir / f"{test_change.name}.sql"
        verify_file.write_text("SELECT COUNT(*) FROM test_verify_table;")

        # Mock plan methods
        oracle_engine.plan.get_deploy_file = lambda change: deploy_file
        oracle_engine.plan.get_verify_file = lambda change: verify_file

        # Deploy the change
        oracle_engine.deploy_change(test_change)

        # Verify the change
        result = oracle_engine.verify_change(test_change)
        assert result is True

    def test_verify_change_failure(self, oracle_engine, test_change, tmp_path):
        """Test verification failure."""
        oracle_engine.ensure_registry()

        # Create verify script that will fail
        verify_dir = tmp_path / "verify"
        verify_dir.mkdir()
        verify_file = verify_dir / f"{test_change.name}.sql"
        verify_file.write_text("SELECT COUNT(*) FROM nonexistent_table;")

        # Mock plan method
        oracle_engine.plan.get_verify_file = lambda change: verify_file

        # Verify should fail
        result = oracle_engine.verify_change(test_change)
        assert result is False

    def test_get_current_state(self, oracle_engine, test_change, tmp_path):
        """Test getting current database state."""
        oracle_engine.ensure_registry()

        # Deploy a change
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        deploy_file = deploy_dir / f"{test_change.name}.sql"
        deploy_file.write_text("CREATE TABLE test_state_table (id NUMBER PRIMARY KEY);")

        oracle_engine.plan.get_deploy_file = lambda change: deploy_file
        oracle_engine.deploy_change(test_change)

        # Get current state
        state = oracle_engine.get_current_state()

        assert state is not None
        assert state["change_id"] == test_change.id
        assert state["change"] == test_change.name
        assert state["project"] == oracle_engine.plan.project_name

    def test_get_deployed_changes(self, oracle_engine, test_change, tmp_path):
        """Test getting list of deployed changes."""
        oracle_engine.ensure_registry()

        # Initially no changes
        changes = oracle_engine.get_deployed_changes()
        assert len(changes) == 0

        # Deploy a change
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        deploy_file = deploy_dir / f"{test_change.name}.sql"
        deploy_file.write_text(
            "CREATE TABLE test_deployed_table (id NUMBER PRIMARY KEY);"
        )

        oracle_engine.plan.get_deploy_file = lambda change: deploy_file
        oracle_engine.deploy_change(test_change)

        # Should now have one change
        changes = oracle_engine.get_deployed_changes()
        assert len(changes) == 1
        assert changes[0] == test_change.id

    def test_sql_file_execution_with_variables(self, oracle_engine, tmp_path):
        """Test SQL file execution with variable substitution."""
        oracle_engine.ensure_registry()

        # Create SQL file with variables
        sql_file = tmp_path / "test_vars.sql"
        sql_file.write_text(
            "CREATE TABLE &table_name (id NUMBER, name VARCHAR2(&name_length));"
        )

        with oracle_engine.connection() as conn:
            oracle_engine._execute_sql_file(
                conn,
                sql_file,
                {"table_name": "test_variables_table", "name_length": "100"},
            )

        # Verify table was created with correct structure
        with oracle_engine.connection() as conn:
            conn.execute(
                """
                SELECT column_name, data_type, data_length
                FROM all_tab_columns
                WHERE owner = ? AND table_name = 'TEST_VARIABLES_TABLE'
                ORDER BY column_id
            """,
                {"owner": oracle_engine._registry_schema},
            )

            columns = conn.fetchall()
            assert len(columns) == 2
            assert columns[0]["column_name"] == "ID"
            assert columns[0]["data_type"] == "NUMBER"
            assert columns[1]["column_name"] == "NAME"
            assert columns[1]["data_type"] == "VARCHAR2"
            assert columns[1]["data_length"] == 100

    def test_transaction_rollback_on_error(self, oracle_engine, test_change, tmp_path):
        """Test that transactions are rolled back on error."""
        oracle_engine.ensure_registry()

        # Create deploy script with error
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        deploy_file = deploy_dir / f"{test_change.name}.sql"
        deploy_file.write_text(
            """
            CREATE TABLE test_rollback_table (id NUMBER PRIMARY KEY);
            INSERT INTO test_rollback_table VALUES (1);
            INSERT INTO nonexistent_table VALUES (1);  -- This will fail
        """
        )

        oracle_engine.plan.get_deploy_file = lambda change: deploy_file

        # Deploy should fail
        with pytest.raises(Exception):
            oracle_engine.deploy_change(test_change)

        # Verify change was not recorded in registry
        with oracle_engine.connection() as conn:
            conn.execute(
                f"SELECT COUNT(*) as count FROM {oracle_engine._registry_schema}.changes WHERE change_id = ?",
                {"change_id": test_change.id},
            )
            result = conn.fetchone()
            assert result["count"] == 0

        # Verify table was not created (transaction rolled back)
        with oracle_engine.connection() as conn:
            try:
                conn.execute("SELECT COUNT(*) FROM test_rollback_table")
                assert False, "Table should not exist due to rollback"
            except Exception:
                pass  # Expected - table should not exist
