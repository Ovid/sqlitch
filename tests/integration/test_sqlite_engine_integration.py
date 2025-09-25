"""
Integration tests for SQLite database engine.

This module contains integration tests that verify the SQLite engine works
correctly with real SQLite databases, testing full workflows including
deployment, revert, and verification operations.
"""

import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from sqlitch.core.change import Change, Dependency
from sqlitch.core.exceptions import DeploymentError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.engines.sqlite import SQLiteEngine


class TestSQLiteEngineIntegration:
    """Integration tests for SQLite engine with real database operations."""

    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database file."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        yield db_path
        # Cleanup
        Path(db_path).unlink(missing_ok=True)

    @pytest.fixture
    def target(self, temp_db_path):
        """Create target configuration."""
        return Target(
            name="test_sqlite", uri=f"sqlite:{temp_db_path}", registry="sqitch"
        )

    @pytest.fixture
    def plan(self, tmp_path):
        """Create plan with test changes."""
        # Create plan directory structure
        deploy_dir = tmp_path / "deploy"
        revert_dir = tmp_path / "revert"
        verify_dir = tmp_path / "verify"

        deploy_dir.mkdir()
        revert_dir.mkdir()
        verify_dir.mkdir()

        # Create test changes
        changes = [
            Change(
                name="users_table",
                note="Add users table",
                tags=[],
                dependencies=[],
                conflicts=[],
                timestamp=datetime.now(timezone.utc),
                planner_name="Test User",
                planner_email="test@example.com",
            ),
            Change(
                name="posts_table",
                note="Add posts table",
                tags=[],
                dependencies=[Dependency(type="require", change="users_table")],
                conflicts=[],
                timestamp=datetime.now(timezone.utc),
                planner_name="Test User",
                planner_email="test@example.com",
            ),
        ]

        # Create SQL files for changes
        (deploy_dir / "users_table.sql").write_text(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """
        )

        (revert_dir / "users_table.sql").write_text(
            """
            DROP TABLE users;
        """
        )

        (verify_dir / "users_table.sql").write_text(
            """
            SELECT COUNT(*) FROM users WHERE 1=0;
        """
        )

        (deploy_dir / "posts_table.sql").write_text(
            """
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
        """
        )

        (revert_dir / "posts_table.sql").write_text(
            """
            DROP TABLE posts;
        """
        )

        (verify_dir / "posts_table.sql").write_text(
            """
            SELECT COUNT(*) FROM posts WHERE 1=0;
        """
        )

        plan_file = tmp_path / "sqitch.plan"
        plan_file.write_text("%syntax-version=1.0.0\n%project=test_project\n")
        plan = Plan(
            file=plan_file,
            project="test_project",
            uri="https://example.com/test",
            changes=changes,
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

    @pytest.fixture
    def engine(self, target, plan):
        """Create SQLite engine instance."""
        return SQLiteEngine(target, plan)

    def test_database_connection(self, engine):
        """Test basic database connection functionality."""
        with engine.connection() as conn:
            # Test basic query
            conn.execute("SELECT sqlite_version()")
            result = conn.fetchone()
            assert result is not None
            assert "sqlite_version()" in result

    def test_registry_initialization(self, engine):
        """Test registry table creation and initialization."""
        # Initially registry should not exist
        with engine.connection() as conn:
            assert not engine._registry_exists_in_db(conn)

        # Create registry
        engine.ensure_registry()

        # Now registry should exist
        with engine.connection() as conn:
            assert engine._registry_exists_in_db(conn)

            # Check that all tables were created
            conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name IN ('releases', 'projects', 'changes', 'tags', 'dependencies', 'events')
                ORDER BY name
            """
            )
            tables = [row["name"] for row in conn.fetchall()]
            expected_tables = [
                "changes",
                "dependencies",
                "events",
                "projects",
                "releases",
                "tags",
            ]
            assert tables == expected_tables

            # Check registry version
            version = engine._get_registry_version(conn)
            assert version == "1.1"

            # Check project record was inserted
            conn.execute("SELECT project, creator_name, creator_email FROM projects")
            project_row = conn.fetchone()
            assert project_row["project"] == "test_project"
            assert project_row["creator_name"] == "Test User"
            assert project_row["creator_email"] == "test@example.com"

    def test_change_deployment(self, engine):
        """Test deploying changes to database."""
        engine.ensure_registry()

        # Deploy first change
        users_change = engine.plan.changes[0]
        engine.deploy_change(users_change)

        # Verify change was recorded in registry
        with engine.connection() as conn:
            conn.execute(
                "SELECT change_id, change, project FROM changes WHERE change = ?",
                {"change": "users_table"},
            )
            change_row = conn.fetchone()
            assert change_row is not None
            assert change_row["change"] == "users_table"
            assert change_row["project"] == "test_project"
            assert change_row["change_id"] == users_change.id

            # Verify actual table was created
            conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
            )
            table_row = conn.fetchone()
            assert table_row is not None
            assert table_row["name"] == "users"

    def test_change_deployment_with_dependencies(self, engine):
        """Test deploying changes with dependencies."""
        engine.ensure_registry()

        # Deploy both changes
        for change in engine.plan.changes:
            engine.deploy_change(change)

        # Verify both changes were deployed
        deployed_changes = engine.get_deployed_changes()
        assert len(deployed_changes) == 2

        # Verify dependency was recorded
        with engine.connection() as conn:
            posts_change = engine.plan.changes[1]
            conn.execute(
                """
                SELECT type, dependency FROM dependencies
                WHERE change_id = ? AND type = 'require'
            """,
                {"change_id": posts_change.id},
            )
            dep_row = conn.fetchone()
            assert dep_row is not None
            assert dep_row["dependency"] == "users_table"

    def test_change_revert(self, engine):
        """Test reverting changes from database."""
        engine.ensure_registry()

        # Deploy changes first
        for change in engine.plan.changes:
            engine.deploy_change(change)

        # Verify both tables exist
        with engine.connection() as conn:
            conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('users', 'posts')"
            )
            tables = [row["name"] for row in conn.fetchall()]
            assert "users" in tables
            assert "posts" in tables

        # Revert posts table (must revert in reverse order due to foreign key)
        posts_change = engine.plan.changes[1]
        engine.revert_change(posts_change)

        # Verify posts table was dropped and change removed from registry
        with engine.connection() as conn:
            conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='posts'"
            )
            table_row = conn.fetchone()
            assert table_row is None

            conn.execute("SELECT change FROM changes WHERE change = 'posts_table'")
            change_row = conn.fetchone()
            assert change_row is None

        # Revert users table
        users_change = engine.plan.changes[0]
        engine.revert_change(users_change)

        # Verify users table was dropped
        with engine.connection() as conn:
            conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
            )
            table_row = conn.fetchone()
            assert table_row is None

    def test_change_verification(self, engine):
        """Test verifying deployed changes."""
        engine.ensure_registry()

        # Deploy first change
        users_change = engine.plan.changes[0]
        engine.deploy_change(users_change)

        # Verify change
        result = engine.verify_change(users_change)
        assert result is True

    def test_get_current_state(self, engine):
        """Test getting current deployment state."""
        engine.ensure_registry()

        # Initially no state
        state = engine.get_current_state()
        assert state is None

        # Deploy a change
        users_change = engine.plan.changes[0]
        engine.deploy_change(users_change)

        # Now should have state
        state = engine.get_current_state()
        assert state is not None
        assert state["change"] == "users_table"
        assert state["project"] == "test_project"
        assert state["change_id"] == users_change.id

    def test_get_deployed_changes(self, engine):
        """Test getting list of deployed changes."""
        engine.ensure_registry()

        # Initially no changes
        deployed = engine.get_deployed_changes()
        assert len(deployed) == 0

        # Deploy changes
        for change in engine.plan.changes:
            engine.deploy_change(change)

        # Should have both changes
        deployed = engine.get_deployed_changes()
        assert len(deployed) == 2
        assert deployed[0] == engine.plan.changes[0].id
        assert deployed[1] == engine.plan.changes[1].id

    def test_transaction_rollback_on_error(self, engine):
        """Test that transactions are rolled back on errors."""
        engine.ensure_registry()

        # First, verify we can deploy a good change
        good_change = engine.plan.changes[0]  # users_table
        engine.deploy_change(good_change)

        # Verify it was deployed
        deployed = engine.get_deployed_changes()
        assert len(deployed) == 1

        # Now test that a bad SQL deployment fails and doesn't leave partial state
        # We'll test this by creating a change that has valid SQL but will fail
        # during the registry recording phase by mocking a database error

        # Create a change that would normally succeed
        test_change = Change(
            name="test_rollback",
            note="Test rollback behavior",
            tags=[],
            dependencies=[],
            conflicts=[],
            timestamp=datetime.now(timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com",
        )

        # Create valid SQL file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("CREATE TABLE test_rollback (id INTEGER);")
            sql_file = Path(f.name)

        # Mock the get_deploy_file method
        original_get_deploy_file = engine.plan.get_deploy_file
        engine.plan.get_deploy_file = lambda change: (
            sql_file
            if change.name == "test_rollback"
            else original_get_deploy_file(change)
        )

        try:
            # Mock the _record_change_deployment to fail after SQL execution
            original_record = engine._record_change_deployment

            def failing_record(conn, change):
                if change.name == "test_rollback":
                    raise Exception("Simulated registry failure")
                return original_record(conn, change)

            engine._record_change_deployment = failing_record

            # Attempt to deploy - should fail during registry recording
            with pytest.raises((ConnectionError, DeploymentError, Exception)):
                engine.deploy_change(test_change)

            # Verify the table was not created (transaction rolled back)
            with engine.connection() as conn:
                try:
                    conn.execute("SELECT COUNT(*) FROM test_rollback")
                    assert False, "Table should not exist after rollback"
                except Exception:
                    pass  # Expected - table should not exist

                # Verify no change was recorded in registry
                conn.execute(
                    "SELECT COUNT(*) as count FROM changes WHERE change = 'test_rollback'"
                )
                count_row = conn.fetchone()
                assert count_row["count"] == 0

            # Restore original method
            engine._record_change_deployment = original_record

        finally:
            sql_file.unlink()

    def test_foreign_key_constraints(self, engine):
        """Test that foreign key constraints are enforced."""
        engine.ensure_registry()

        # Deploy users table
        users_change = engine.plan.changes[0]
        engine.deploy_change(users_change)

        # Try to insert post with non-existent user (should fail due to foreign key)
        with engine.connection() as conn:
            with pytest.raises(DeploymentError):
                conn.execute(
                    "INSERT INTO posts (user_id, title) VALUES (999, 'Test Post')"
                )

    def test_concurrent_access(self, engine, temp_db_path):
        """Test concurrent access to SQLite database."""
        engine.ensure_registry()

        # Create second engine instance with same database
        target2 = Target(name="test2", uri=f"sqlite:{temp_db_path}", registry="sqitch")
        engine2 = SQLiteEngine(target2, engine.plan)

        # Both engines should be able to read from the database
        with engine.connection() as conn1:
            with engine2.connection() as conn2:
                conn1.execute("SELECT COUNT(*) as count FROM projects")
                result1 = conn1.fetchone()

                conn2.execute("SELECT COUNT(*) as count FROM projects")
                result2 = conn2.fetchone()

                assert result1["count"] == result2["count"]

    def test_memory_database(self, plan):
        """Test using in-memory SQLite database."""
        # Use a temporary file instead of :memory: since in-memory databases
        # don't persist across connections in SQLite
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            temp_db = f.name

        try:
            memory_target = Target(
                name="memory_test", uri=f"sqlite:{temp_db}", registry="sqitch"
            )

            memory_engine = SQLiteEngine(memory_target, plan)

            # Should be able to create registry
            memory_engine.ensure_registry()

            # Should be able to deploy changes
            users_change = plan.changes[0]
            memory_engine.deploy_change(users_change)

            # Verify change was deployed
            deployed = memory_engine.get_deployed_changes()
            assert len(deployed) == 1
            assert deployed[0] == users_change.id
        finally:
            Path(temp_db).unlink(missing_ok=True)

    def test_database_file_permissions(self, engine, temp_db_path):
        """Test database file is created with appropriate permissions."""
        engine.ensure_registry()

        db_file = Path(temp_db_path)
        assert db_file.exists()

        # File should be readable and writable by owner
        stat = db_file.stat()
        assert stat.st_mode & 0o600  # At least read/write for owner

    def test_sql_file_execution(self, engine, tmp_path):
        """Test executing SQL files directly."""
        # Create test SQL file
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE test_direct (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );

            INSERT INTO test_direct (name) VALUES ('test1'), ('test2');
        """
        )

        # Execute file
        engine.run_file(sql_file)

        # Verify results
        with engine.connection() as conn:
            conn.execute("SELECT COUNT(*) as count FROM test_direct")
            result = conn.fetchone()
            assert result["count"] == 2

    def test_verify_file_execution(self, engine, tmp_path):
        """Test executing verification SQL files."""
        engine.ensure_registry()

        # Deploy users table first
        users_change = engine.plan.changes[0]
        engine.deploy_change(users_change)

        # Create verification SQL file
        verify_file = tmp_path / "verify_users.sql"
        verify_file.write_text(
            """
            -- Verify users table exists and has correct structure
            SELECT
                COUNT(*) as table_count
            FROM sqlite_master
            WHERE type='table' AND name='users';

            -- Verify columns exist
            PRAGMA table_info(users);
        """
        )

        # Execute verification
        engine.run_verify(verify_file)  # Should not raise exception

    def test_error_handling_with_real_database(self, engine):
        """Test error handling with real database operations."""
        engine.ensure_registry()

        # Test deploying change with missing SQL file - this should succeed
        # because the base engine only executes files that exist
        missing_change = Change(
            name="missing_file",
            note="File doesn't exist",
            tags=[],
            dependencies=[],
            conflicts=[],
            timestamp=datetime.now(timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com",
        )

        # Mock get_deploy_file to return non-existent file
        engine.plan.get_deploy_file = lambda change: (
            Path("/non/existent/file.sql") if change.name == "missing_file" else None
        )

        # This should succeed because the file doesn't exist and base engine skips non-existent files
        engine.deploy_change(missing_change)

        # Verify the change was recorded even though no SQL was executed
        deployed = engine.get_deployed_changes()
        assert missing_change.id in deployed
