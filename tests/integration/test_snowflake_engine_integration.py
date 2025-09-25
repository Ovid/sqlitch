"""
Integration tests for Snowflake database engine.

This module contains integration tests that verify the Snowflake engine
works correctly with actual database operations. These tests require
a Snowflake database connection to be available.
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
from sqlitch.core.types import EngineType
from sqlitch.engines.snowflake import SnowflakeEngine

# Skip all tests if snowflake-connector-python is not available
pytest_plugins = []

try:
    import snowflake.connector

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


@pytest.mark.skipif(
    not SNOWFLAKE_AVAILABLE, reason="snowflake-connector-python not available"
)
class TestSnowflakeEngineIntegration:
    """Integration tests for Snowflake engine."""

    @pytest.fixture(scope="class")
    def snowflake_config(self):
        """Get Snowflake configuration from environment variables."""
        config = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "SQLITCH_TEST"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }

        # Skip tests if required config is missing
        if not all([config["account"], config["user"], config["password"]]):
            pytest.skip("Snowflake connection configuration not available")

        return config

    @pytest.fixture
    def target(self, snowflake_config):
        """Create target for Snowflake connection."""
        # Build URI from config
        uri_parts = [
            f"snowflake://{snowflake_config['user']}:{snowflake_config['password']}",
            f"@{snowflake_config['account']}.snowflakecomputing.com",
            f"/{snowflake_config['database']}",
        ]

        query_params = []
        if snowflake_config["warehouse"]:
            query_params.append(f"warehouse={snowflake_config['warehouse']}")
        if snowflake_config["role"]:
            query_params.append(f"role={snowflake_config['role']}")

        if query_params:
            uri_parts.append(f"?{'&'.join(query_params)}")

        target = Mock(spec=Target)
        target.uri = Mock()
        target.uri.hostname = f"{snowflake_config['account']}.snowflakecomputing.com"
        target.uri.username = snowflake_config["user"]
        target.uri.password = snowflake_config["password"]
        target.uri.path = f"/{snowflake_config['database']}"
        target.uri.query = "&".join(query_params) if query_params else None
        target.registry = "sqitch_test"
        target.engine_type = "snowflake"

        return target

    @pytest.fixture
    def plan(self):
        """Create test plan."""
        plan = Mock(spec=Plan)
        plan.project_name = "test_project"
        plan.creator_name = "Test User"
        plan.creator_email = "test@example.com"
        plan.changes = []

        # Mock file path methods
        def get_deploy_file(change):
            return Path(f"deploy/{change.name}.sql")

        def get_revert_file(change):
            return Path(f"revert/{change.name}.sql")

        def get_verify_file(change):
            return Path(f"verify/{change.name}.sql")

        plan.get_deploy_file = get_deploy_file
        plan.get_revert_file = get_revert_file
        plan.get_verify_file = get_verify_file

        return plan

    @pytest.fixture
    def engine(self, target, plan):
        """Create Snowflake engine instance."""
        return SnowflakeEngine(target, plan)

    @pytest.fixture
    def sample_change(self):
        """Create a sample change for testing."""
        return Change(
            name="test_table",
            note="Create test table",
            tags=[],
            dependencies=[],
            conflicts=[],
            timestamp=datetime.now(timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com",
        )

    def test_connection_creation(self, engine):
        """Test that we can create a connection to Snowflake."""
        try:
            with engine.connection() as conn:
                # Execute a simple query to verify connection
                conn.execute("SELECT CURRENT_VERSION()")
                result = conn.fetchone()
                assert result is not None
                assert "current_version()" in result
        except ConnectionError:
            pytest.skip("Could not connect to Snowflake database")

    def test_registry_creation(self, engine):
        """Test registry table creation."""
        try:
            # Clean up any existing registry
            with engine.connection() as conn:
                try:
                    conn.execute("DROP SCHEMA IF EXISTS sqitch_test CASCADE")
                    conn.commit()
                except Exception:
                    pass  # Schema might not exist

            # Ensure registry is created
            engine.ensure_registry()

            # Verify registry tables exist
            with engine.connection() as conn:
                # Check projects table
                conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'SQITCH_TEST'
                    AND table_name = 'PROJECTS'
                """
                )
                result = conn.fetchone()
                assert result is not None

                # Check releases table
                conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'SQITCH_TEST'
                    AND table_name = 'RELEASES'
                """
                )
                result = conn.fetchone()
                assert result is not None

        except ConnectionError:
            pytest.skip("Could not connect to Snowflake database")

    def test_registry_version(self, engine):
        """Test registry version detection."""
        try:
            # Ensure registry exists
            engine.ensure_registry()

            # Check version
            with engine.connection() as conn:
                version = engine._get_registry_version(conn)
                assert version == "1.1"

        except ConnectionError:
            pytest.skip("Could not connect to Snowflake database")

    def test_sql_file_execution(self, engine, sample_change, tmp_path):
        """Test executing SQL files."""
        try:
            # Ensure registry exists
            engine.ensure_registry()

            # Create test SQL file
            sql_file = tmp_path / "test.sql"
            sql_content = """
            CREATE OR REPLACE TABLE test_execution (
                id INTEGER,
                name VARCHAR(100)
            );

            INSERT INTO test_execution VALUES (1, 'test');
            """
            sql_file.write_text(sql_content)

            # Execute SQL file
            with engine.connection() as conn:
                engine._execute_sql_file(conn, sql_file)
                conn.commit()

                # Verify table was created and data inserted
                conn.execute("SELECT COUNT(*) as count FROM test_execution")
                result = conn.fetchone()
                assert result["count"] == 1

                # Clean up
                conn.execute("DROP TABLE IF EXISTS test_execution")
                conn.commit()

        except ConnectionError:
            pytest.skip("Could not connect to Snowflake database")

    def test_variable_substitution(self, engine, tmp_path):
        """Test SQL variable substitution."""
        try:
            # Create SQL file with variables
            sql_file = tmp_path / "variables.sql"
            sql_content = """
            CREATE OR REPLACE TABLE &registry.test_vars (
                id INTEGER
            );

            USE WAREHOUSE &warehouse;
            """
            sql_file.write_text(sql_content)

            # Execute with variable substitution
            with engine.connection() as conn:
                engine._execute_sql_file(conn, sql_file)
                conn.commit()

                # Verify table was created in correct schema
                conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'SQITCH_TEST'
                    AND table_name = 'TEST_VARS'
                """
                )
                result = conn.fetchone()
                assert result is not None

                # Clean up
                conn.execute("DROP TABLE IF EXISTS sqitch_test.test_vars")
                conn.commit()

        except ConnectionError:
            pytest.skip("Could not connect to Snowflake database")

    def test_regex_condition(self, engine):
        """Test regex condition functionality."""
        try:
            with engine.connection() as conn:
                # Test regex condition
                condition = engine._regex_condition("test_column", r"test.*")

                # Create test query
                query = f"SELECT 'test123' as test_column WHERE {condition}"
                conn.execute(query, ["test.*"])
                result = conn.fetchone()

                assert result is not None
                assert result["test_column"] == "test123"

        except ConnectionError:
            pytest.skip("Could not connect to Snowflake database")

    def test_change_deployment_cycle(self, engine, sample_change, tmp_path):
        """Test complete change deployment and revert cycle."""
        try:
            # Ensure registry exists
            engine.ensure_registry()

            # Create deploy script
            deploy_file = tmp_path / "deploy" / f"{sample_change.name}.sql"
            deploy_file.parent.mkdir(exist_ok=True)
            deploy_file.write_text(
                """
            CREATE OR REPLACE TABLE test_deployment (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100)
            );
            """
            )

            # Create revert script
            revert_file = tmp_path / "revert" / f"{sample_change.name}.sql"
            revert_file.parent.mkdir(exist_ok=True)
            revert_file.write_text("DROP TABLE IF EXISTS test_deployment;")

            # Create verify script
            verify_file = tmp_path / "verify" / f"{sample_change.name}.sql"
            verify_file.parent.mkdir(exist_ok=True)
            verify_file.write_text(
                """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'TEST_DEPLOYMENT';
            """
            )

            # Mock plan file methods to return our test files
            engine.plan.get_deploy_file = lambda change: deploy_file
            engine.plan.get_revert_file = lambda change: revert_file
            engine.plan.get_verify_file = lambda change: verify_file

            # Deploy change
            engine.deploy_change(sample_change)

            # Verify deployment
            assert engine.verify_change(sample_change) is True

            # Check that table exists
            with engine.connection() as conn:
                conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_name = 'TEST_DEPLOYMENT'
                """
                )
                result = conn.fetchone()
                assert result is not None

            # Revert change
            engine.revert_change(sample_change)

            # Verify table is gone
            with engine.connection() as conn:
                conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_name = 'TEST_DEPLOYMENT'
                """
                )
                result = conn.fetchone()
                assert result is None

        except ConnectionError:
            pytest.skip("Could not connect to Snowflake database")

    def test_connection_error_handling(self, target, plan):
        """Test connection error handling."""
        # Create target with invalid credentials
        bad_target = Mock(spec=Target)
        bad_target.uri = Mock()
        bad_target.uri.hostname = "invalid-account.snowflakecomputing.com"
        bad_target.uri.username = "invalid_user"
        bad_target.uri.password = "invalid_password"
        bad_target.uri.path = "/invalid_db"
        bad_target.uri.query = "warehouse=invalid_wh"
        bad_target.registry = "sqitch"

        engine = SnowflakeEngine(bad_target, plan)

        # Should raise ConnectionError
        with pytest.raises(ConnectionError):
            with engine.connection():
                pass

    @pytest.fixture(autouse=True)
    def cleanup_test_schema(self, engine):
        """Clean up test schema after each test."""
        yield

        # Clean up test schema
        try:
            with engine.connection() as conn:
                conn.execute("DROP SCHEMA IF EXISTS sqitch_test CASCADE")
                conn.commit()
        except Exception:
            pass  # Ignore cleanup errors


@pytest.mark.skipif(SNOWFLAKE_AVAILABLE, reason="Testing import error handling")
class TestSnowflakeEngineWithoutModule:
    """Test Snowflake engine behavior when module is not available."""

    def test_engine_creation_without_module(self):
        """Test that engine creation fails gracefully without snowflake module."""
        target = Mock(spec=Target)
        plan = Mock(spec=Plan)

        with patch("sqlitch.engines.snowflake.snowflake", None):
            with pytest.raises(
                EngineError, match="snowflake-connector-python is required"
            ):
                SnowflakeEngine(target, plan)
