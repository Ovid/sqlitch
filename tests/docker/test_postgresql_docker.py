"""
Docker-based PostgreSQL integration tests.

These tests use real PostgreSQL Docker containers to verify full deployment
cycles, registry creation, change deployment, revert, and verification.
"""

import os
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from sqlitch.core.change import Change
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.core.types import URI
from sqlitch.engines.pg import PostgreSQLEngine


@pytest.mark.integration
class TestPostgreSQLDockerIntegration:
    """Docker-based integration tests for PostgreSQL engine."""

    @pytest.fixture(scope="class")
    def postgres_uri(self, postgresql_container):
        """Get PostgreSQL connection URI from Docker container."""
        if not postgresql_container:
            pytest.skip("PostgreSQL container not available")

        # Get container port mapping
        port = postgresql_container.ports.get("5432/tcp")
        if port:
            host_port = port[0]["HostPort"]
        else:
            host_port = "5432"

        return f"db:pg://sqlitch:test@localhost:{host_port}/sqlitch_test"

    @pytest.fixture
    def postgres_target(self, postgres_uri):
        """Create PostgreSQL target for testing."""
        return Target(
            name="test_pg_docker",
            uri=URI(postgres_uri),
            registry="sqitch",
        )

    @pytest.fixture
    def test_plan(self, tmp_path):
        """Create a test plan with sample changes."""
        plan = Mock(spec=Plan)
        plan.project_name = "docker_test_project"
        plan.creator_name = "Docker Test User"
        plan.creator_email = "docker.test@example.com"

        # Create sample changes
        from datetime import datetime

        change1 = Change(
            name="create_users_table",
            note="Create users table for testing",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="Docker Test User",
            planner_email="docker.test@example.com",
        )

        change2 = Change(
            name="add_user_indexes",
            note="Add indexes to users table",
            timestamp=datetime(2023, 1, 16, 14, 20, 0),
            planner_name="Docker Test User",
            planner_email="docker.test@example.com",
        )

        plan.changes = [change1, change2]

        # Create SQL files for changes
        deploy_dir = tmp_path / "deploy"
        revert_dir = tmp_path / "revert"
        verify_dir = tmp_path / "verify"

        for dir_path in [deploy_dir, revert_dir, verify_dir]:
            dir_path.mkdir(exist_ok=True)

        # Create deploy scripts
        (deploy_dir / "create_users_table.sql").write_text(
            """
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
        )

        (deploy_dir / "add_user_indexes.sql").write_text(
            """
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_created_at ON users(created_at);
"""
        )

        # Create revert scripts
        (revert_dir / "add_user_indexes.sql").write_text(
            """
DROP INDEX IF EXISTS idx_users_created_at;
DROP INDEX IF EXISTS idx_users_email;
DROP INDEX IF EXISTS idx_users_username;
"""
        )

        (revert_dir / "create_users_table.sql").write_text(
            """
DROP TABLE IF EXISTS users;
"""
        )

        # Create verify scripts
        (verify_dir / "create_users_table.sql").write_text(
            """
SELECT 1/COUNT(*) FROM information_schema.tables
WHERE table_name = 'users' AND table_schema = 'public';
"""
        )

        (verify_dir / "add_user_indexes.sql").write_text(
            """
SELECT 1/COUNT(*) FROM pg_indexes
WHERE tablename = 'users' AND indexname = 'idx_users_username';

SELECT 1/COUNT(*) FROM pg_indexes
WHERE tablename = 'users' AND indexname = 'idx_users_email';

SELECT 1/COUNT(*) FROM pg_indexes
WHERE tablename = 'users' AND indexname = 'idx_users_created_at';
"""
        )

        # Mock file path methods
        def get_deploy_file(change):
            return deploy_dir / f"{change.name}.sql"

        def get_revert_file(change):
            return revert_dir / f"{change.name}.sql"

        def get_verify_file(change):
            return verify_dir / f"{change.name}.sql"

        plan.get_deploy_file = get_deploy_file
        plan.get_revert_file = get_revert_file
        plan.get_verify_file = get_verify_file

        return plan

    def test_registry_creation_and_initialization(
        self, postgres_target, test_plan, skip_if_no_docker
    ):
        """Test creating and initializing the sqitch registry."""
        engine = PostgreSQLEngine(postgres_target, test_plan)

        # Test registry creation
        with engine.connection() as conn:
            # Registry should not exist initially
            assert not engine._registry_exists_in_db(conn)

            # Create registry
            engine._create_registry(conn)

            # Registry should now exist
            assert engine._registry_exists_in_db(conn)

            # Check that all required tables exist
            tables_query = """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'sqitch'
            ORDER BY table_name
            """
            conn.execute(tables_query)
            tables = [row[0] for row in conn.fetchall()]

            expected_tables = [
                "changes",
                "dependencies",
                "events",
                "projects",
                "releases",
                "tags",
            ]
            assert all(table in tables for table in expected_tables)

            # Check registry version
            version = engine._get_registry_version(conn)
            assert version is not None
            assert version.startswith("1.")

    def test_full_deployment_cycle(self, postgres_target, test_plan, skip_if_no_docker):
        """Test complete deployment, verification, and revert cycle."""
        engine = PostgreSQLEngine(postgres_target, test_plan)

        with engine.connection() as conn:
            # Ensure clean state
            if engine._registry_exists_in_db(conn):
                conn.execute("DROP SCHEMA IF EXISTS sqitch CASCADE")
                conn.commit()

            # Create registry
            engine._create_registry(conn)

            # Deploy first change
            change1 = test_plan.changes[0]
            engine.deploy_change(change1)

            # Verify deployment was recorded
            conn.execute(
                "SELECT COUNT(*) FROM sqitch.changes WHERE change_id = %s",
                (change1.id,),
            )
            assert conn.fetchone()[0] == 1

            # Verify table was created
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'users' AND table_schema = 'public'
            """
            )
            assert conn.fetchone()[0] == 1

            # Deploy second change
            change2 = test_plan.changes[1]
            engine.deploy_change(change2)

            # Verify indexes were created
            conn.execute(
                """
                SELECT COUNT(*) FROM pg_indexes
                WHERE tablename = 'users' AND indexname LIKE 'idx_users_%'
            """
            )
            assert conn.fetchone()[0] == 3

            # Test verification
            result = engine.verify_change(change1)
            assert result.success

            result = engine.verify_change(change2)
            assert result.success

            # Test revert
            engine.revert_change(change2)

            # Verify indexes were removed
            conn.execute(
                """
                SELECT COUNT(*) FROM pg_indexes
                WHERE tablename = 'users' AND indexname LIKE 'idx_users_%'
            """
            )
            assert conn.fetchone()[0] == 0

            # Verify change was removed from registry
            conn.execute(
                "SELECT COUNT(*) FROM sqitch.changes WHERE change_id = %s",
                (change2.id,),
            )
            assert conn.fetchone()[0] == 0

            # Revert first change
            engine.revert_change(change1)

            # Verify table was removed
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'users' AND table_schema = 'public'
            """
            )
            assert conn.fetchone()[0] == 0

    def test_concurrent_deployments(
        self, postgres_target, test_plan, skip_if_no_docker
    ):
        """Test that concurrent deployments are handled properly."""
        engine1 = PostgreSQLEngine(postgres_target, test_plan)
        engine2 = PostgreSQLEngine(postgres_target, test_plan)

        with engine1.connection() as conn1:
            # Ensure clean state
            if engine1._registry_exists_in_db(conn1):
                conn1.execute("DROP SCHEMA IF EXISTS sqitch CASCADE")
                conn1.commit()

            # Create registry
            engine1.create_registry(conn1)

            with engine2.connection() as conn2:
                # Both engines should see the registry
                assert engine1._registry_exists_in_db(conn1)
                assert engine2._registry_exists_in_db(conn2)

                # Deploy change with first engine
                change1 = test_plan.changes[0]
                engine1.deploy_change(change1)

                # Second engine should see the deployed change
                conn2.execute(
                    "SELECT COUNT(*) FROM sqitch.changes WHERE change_id = %s",
                    (change1.id,),
                )
                assert conn2.fetchone()[0] == 1

    def test_registry_schema_isolation(
        self, postgres_target, test_plan, skip_if_no_docker
    ):
        """Test that sqitch registry is properly isolated in its own schema."""
        engine = PostgreSQLEngine(postgres_target, test_plan)

        with engine.connection() as conn:
            # Ensure clean state
            if engine._registry_exists_in_db(conn):
                conn.execute("DROP SCHEMA IF EXISTS sqitch CASCADE")
                conn.commit()

            # Create registry
            engine._create_registry(conn)

            # Verify sqitch schema exists
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.schemata
                WHERE schema_name = 'sqitch'
            """
            )
            assert conn.fetchone()[0] == 1

            # Deploy a change that creates objects in public schema
            change1 = test_plan.changes[0]
            engine.deploy_change(change1)

            # Verify user table is in public schema, not sqitch schema
            conn.execute(
                """
                SELECT table_schema FROM information_schema.tables
                WHERE table_name = 'users'
            """
            )
            schema = conn.fetchone()[0]
            assert schema == "public"

            # Verify sqitch tables are in sqitch schema
            conn.execute(
                """
                SELECT DISTINCT table_schema FROM information_schema.tables
                WHERE table_name IN ('changes', 'events', 'projects')
            """
            )
            schemas = [row[0] for row in conn.fetchall()]
            assert schemas == ["sqitch"]

    def test_error_handling_and_rollback(
        self, postgres_target, test_plan, skip_if_no_docker
    ):
        """Test error handling and transaction rollback."""
        engine = PostgreSQLEngine(postgres_target, test_plan)

        # Create a change with invalid SQL
        from datetime import datetime

        bad_change = Change(
            name="bad_change",
            note="Change with invalid SQL",
            timestamp=datetime(2023, 2, 1, 12, 0, 0),
            planner_name="Test User",
            planner_email="test@example.com",
        )

        # Create temporary SQL file with invalid syntax
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("CREATE TABLE invalid_syntax_table ( invalid syntax here")
            bad_sql_file = Path(f.name)

        try:
            with engine.connection() as conn:
                # Ensure clean state
                if engine._registry_exists_in_db(conn):
                    conn.execute("DROP SCHEMA IF EXISTS sqitch CASCADE")
                    conn.commit()

                # Create registry
                engine._create_registry(conn)

                # Mock the plan to return our bad SQL file
                original_get_deploy_file = test_plan.get_deploy_file
                test_plan.get_deploy_file = lambda change: (
                    bad_sql_file
                    if change == bad_change
                    else original_get_deploy_file(change)
                )

                # Attempt to deploy bad change - should fail
                with pytest.raises(Exception):
                    engine.deploy_change(bad_change)

                # Verify the change was not recorded in registry
                conn.execute(
                    "SELECT COUNT(*) FROM sqitch.changes WHERE change_id = %s",
                    (bad_change.id,),
                )
                assert conn.fetchone()[0] == 0

                # Verify no partial state was left behind
                conn.execute(
                    """
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_name = 'invalid_syntax_table'
                """
                )
                assert conn.fetchone()[0] == 0

        finally:
            # Clean up temporary file
            bad_sql_file.unlink(missing_ok=True)

    def test_performance_with_large_changes(
        self, postgres_target, test_plan, skip_if_no_docker
    ):
        """Test performance with larger SQL scripts."""
        engine = PostgreSQLEngine(postgres_target, test_plan)

        # Create a change with a large SQL script
        from datetime import datetime

        large_change = Change(
            name="large_data_insert",
            note="Insert large amount of test data",
            timestamp=datetime(2023, 2, 2, 15, 30, 0),
            planner_name="Test User",
            planner_email="test@example.com",
        )

        # Create temporary SQL file with many INSERT statements
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("CREATE TABLE large_test_table (id INTEGER, data TEXT);\n")
            # Insert 1000 rows
            for i in range(1000):
                f.write(
                    f"INSERT INTO large_test_table (id, data) VALUES ({i}, 'test_data_{i}');\n"
                )
            large_sql_file = Path(f.name)

        try:
            with engine.connection() as conn:
                # Ensure clean state
                if engine._registry_exists_in_db(conn):
                    conn.execute("DROP SCHEMA IF EXISTS sqitch CASCADE")
                    conn.commit()

                # Create registry
                engine._create_registry(conn)

                # Mock the plan to return our large SQL file
                original_get_deploy_file = test_plan.get_deploy_file
                test_plan.get_deploy_file = lambda change: (
                    large_sql_file
                    if change == large_change
                    else original_get_deploy_file(change)
                )

                # Time the deployment
                start_time = time.time()
                engine.deploy_change(large_change)
                end_time = time.time()

                # Should complete in reasonable time (less than 10 seconds)
                assert end_time - start_time < 10.0

                # Verify all data was inserted
                conn.execute("SELECT COUNT(*) FROM large_test_table")
                assert conn.fetchone()[0] == 1000

                # Verify change was recorded
                conn.execute(
                    "SELECT COUNT(*) FROM sqitch.changes WHERE change_id = %s",
                    (large_change.id,),
                )
                assert conn.fetchone()[0] == 1

        finally:
            # Clean up temporary file
            large_sql_file.unlink(missing_ok=True)
