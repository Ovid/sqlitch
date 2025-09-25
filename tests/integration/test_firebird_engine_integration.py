"""
Integration tests for Firebird database engine.

This module contains integration tests that verify the FirebirdEngine works
correctly with actual Firebird database operations, including registry
creation, change deployment, and state management.
"""

import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlitch.core.change import Change
from sqlitch.core.exceptions import ConnectionError, EngineError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.core.types import EngineType
from sqlitch.engines.firebird import FirebirdEngine

# Skip all tests if fdb is not available
fdb = pytest.importorskip("fdb", reason="fdb package not available")


class TestFirebirdEngineIntegration:
    """Integration tests for Firebird engine."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create temporary directory for test database
        self.temp_dir = Path(tempfile.mkdtemp())
        self.db_path = self.temp_dir / "test.fdb"

        self.target = Target(
            name="test",
            uri=f"firebird://SYSDBA:masterkey@/{self.db_path}",
            registry="sqitch",
        )

        self.plan = Plan(
            file=Path("/tmp/sqitch.plan"),
            project="test_project",
            uri="https://example.com/test",
            changes=[],
        )

        # Create test changes
        self.change1 = Change(
            name="initial_schema",
            id="abc123",
            note="Initial schema setup",
            timestamp=datetime.now(timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com",
            dependencies=[],
            conflicts=[],
            tags=[],
        )

        self.change2 = Change(
            name="add_users_table",
            id="def456",
            note="Add users table",
            timestamp=datetime.now(timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com",
            dependencies=[],
            conflicts=[],
            tags=[],
        )

    def teardown_method(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_engine_initialization(self):
        """Test engine initialization."""
        engine = FirebirdEngine(self.target, self.plan)

        assert engine.engine_type == "firebird"
        assert engine.target == self.target
        assert engine.plan == self.plan

    def test_connection_creation(self):
        """Test database connection creation."""
        engine = FirebirdEngine(self.target, self.plan)

        # Test connection context manager
        with engine.connection() as conn:
            assert conn is not None
            # Execute a simple query to verify connection works
            conn.execute("SELECT 1 FROM RDB$DATABASE")
            result = conn.fetchone()
            assert result is not None

    def test_database_creation(self):
        """Test automatic database creation."""
        # Ensure database doesn't exist
        if self.db_path.exists():
            self.db_path.unlink()

        engine = FirebirdEngine(self.target, self.plan)

        # Connection should create the database
        with engine.connection() as conn:
            conn.execute("SELECT 1 FROM RDB$DATABASE")
            result = conn.fetchone()
            assert result is not None

        # Database file should now exist
        assert self.db_path.exists()

    def test_registry_creation(self):
        """Test registry table creation."""
        engine = FirebirdEngine(self.target, self.plan)

        # Ensure registry
        engine.ensure_registry()

        # Verify tables exist
        with engine.connection() as conn:
            # Check that all registry tables exist
            tables = [
                "RELEASES",
                "PROJECTS",
                "CHANGES",
                "TAGS",
                "DEPENDENCIES",
                "EVENTS",
            ]

            for table in tables:
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM RDB$RELATIONS
                    WHERE RDB$SYSTEM_FLAG=0
                          AND RDB$VIEW_BLR IS NULL
                          AND RDB$RELATION_NAME = ?
                """,
                    {"table_name": table},
                )

                result = conn.fetchone()
                assert (
                    result and list(result.values())[0] > 0
                ), f"Table {table} not found"

    def test_registry_version(self):
        """Test registry version tracking."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        # Check version
        with engine.connection() as conn:
            version = engine._get_registry_version(conn)
            assert version == "1.1"

    def test_project_insertion(self):
        """Test project record insertion."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        # Check project was inserted
        with engine.connection() as conn:
            conn.execute(
                "SELECT project, creator_name, creator_email FROM PROJECTS WHERE project = ?",
                {"project": self.plan.project_name},
            )
            result = conn.fetchone()

            assert result is not None
            assert result["project"] == self.plan.project_name
            assert result["creator_name"] == self.plan.creator_name
            assert result["creator_email"] == self.plan.creator_email

    def test_get_deployed_changes_empty(self):
        """Test getting deployed changes when none exist."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        # Should return empty list
        changes = engine.get_deployed_changes()
        assert changes == []

    def test_get_current_state_empty(self):
        """Test getting current state when no changes deployed."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        # Should return None
        state = engine.get_current_state()
        assert state is None

    def test_change_deployment_simulation(self):
        """Test simulating change deployment (without actual SQL files)."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        # Mock the SQL file execution to avoid needing actual files
        with patch.object(engine, "_execute_sql_file"):
            with engine.transaction() as conn:
                engine._record_change_deployment(conn, self.change1)

        # Verify change was recorded
        changes = engine.get_deployed_changes()
        assert len(changes) == 1
        assert changes[0] == self.change1.id

        # Check current state
        state = engine.get_current_state()
        assert state is not None
        assert state["change_id"] == self.change1.id
        assert state["change"] == self.change1.name
        assert state["project"] == self.plan.project_name

    def test_multiple_change_deployment(self):
        """Test deploying multiple changes."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        # Mock SQL file execution
        with patch.object(engine, "_execute_sql_file"):
            # Deploy first change
            with engine.transaction() as conn:
                engine._record_change_deployment(conn, self.change1)

            # Deploy second change
            with engine.transaction() as conn:
                engine._record_change_deployment(conn, self.change2)

        # Verify both changes were recorded
        changes = engine.get_deployed_changes()
        assert len(changes) == 2
        assert self.change1.id in changes
        assert self.change2.id in changes

        # Current state should be the most recent change
        state = engine.get_current_state()
        assert state is not None
        assert state["change_id"] == self.change2.id

    def test_search_events_empty(self):
        """Test searching events when none exist."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        # Should return empty iterator
        events = list(engine.search_events())
        assert events == []

    def test_search_events_with_filters(self):
        """Test searching events with various filters."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        # Test different filter combinations (should not raise errors)
        list(engine.search_events(event=["deploy"]))
        list(engine.search_events(change="test"))
        list(engine.search_events(project="test_project"))
        list(engine.search_events(committer="Test User"))
        list(engine.search_events(planner="Test User"))
        list(engine.search_events(limit=10))
        list(engine.search_events(offset=5))
        list(engine.search_events(direction="ASC"))

    def test_search_events_invalid_direction(self):
        """Test search events with invalid direction."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        with pytest.raises(EngineError) as exc_info:
            list(engine.search_events(direction="INVALID"))

        assert "Search direction must be either 'ASC' or 'DESC'" in str(exc_info.value)

    def test_regex_pattern_conversion(self):
        """Test regex pattern conversion for SIMILAR TO."""
        engine = FirebirdEngine(self.target, self.plan)

        # Test various regex patterns
        assert engine._convert_regex_to_similar("^test$") == "test"
        assert engine._convert_regex_to_similar("^test") == "test%"
        assert engine._convert_regex_to_similar("test$") == "%test"
        assert engine._convert_regex_to_similar("test") == "%test%"

    def test_sql_statement_splitting(self):
        """Test SQL statement splitting functionality."""
        engine = FirebirdEngine(self.target, self.plan)

        sql_content = """
        -- This is a comment
        CREATE TABLE test (
            id INTEGER PRIMARY KEY
        );

        INSERT INTO test VALUES (1); -- Inline comment
        INSERT INTO test VALUES (2);

        -- Another comment
        SELECT * FROM test;
        """

        statements = engine._split_sql_statements(sql_content)

        expected = [
            "CREATE TABLE test ( id INTEGER PRIMARY KEY );",
            "INSERT INTO test VALUES (1);",
            "INSERT INTO test VALUES (2);",
            "SELECT * FROM test;",
        ]

        assert statements == expected

    def test_connection_error_handling(self):
        """Test connection error handling."""
        # Use invalid connection parameters
        bad_target = Target(
            name="test",
            uri="firebird://invalid:invalid@nonexistent/invalid.fdb",
            registry="sqitch",
        )

        engine = FirebirdEngine(bad_target, self.plan)

        # Should raise ConnectionError
        with pytest.raises(ConnectionError) as exc_info:
            with engine.connection():
                pass

        assert "Failed to connect to Firebird database" in str(exc_info.value)

    def test_transaction_rollback_on_error(self):
        """Test transaction rollback on error."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        # Test transaction rollback
        with pytest.raises(Exception):
            with engine.transaction() as conn:
                # Execute valid SQL
                conn.execute("SELECT 1 FROM RDB$DATABASE")
                # Then raise an error to trigger rollback
                raise Exception("Test error")

        # Connection should still work after rollback
        with engine.connection() as conn:
            conn.execute("SELECT 1 FROM RDB$DATABASE")
            result = conn.fetchone()
            assert result is not None

    @pytest.mark.skipif(not fdb, reason="fdb package not available")
    def test_firebird_specific_features(self):
        """Test Firebird-specific features and syntax."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        with engine.connection() as conn:
            # Test Firebird system tables
            conn.execute(
                """
                SELECT COUNT(*)
                FROM RDB$RELATIONS
                WHERE RDB$SYSTEM_FLAG = 0
            """
            )
            result = conn.fetchone()
            assert result is not None

            # Test Firebird generators (sequences)
            conn.execute("CREATE GENERATOR test_gen")
            conn.execute("SET GENERATOR test_gen TO 100")

            conn.execute("SELECT GEN_ID(test_gen, 1) FROM RDB$DATABASE")
            result = conn.fetchone()
            assert result is not None
            assert list(result.values())[0] == 101

    def test_blob_text_handling(self):
        """Test handling of BLOB SUB_TYPE TEXT fields."""
        engine = FirebirdEngine(self.target, self.plan)

        # Create registry
        engine.ensure_registry()

        # Test inserting and retrieving text in BLOB fields
        with engine.connection() as conn:
            # Insert a change with a note (stored in BLOB SUB_TYPE TEXT)
            test_note = "This is a test note with special characters: àáâãäå"

            conn.execute(
                """
                INSERT INTO CHANGES
                (change_id, change, project, note, committed_at, committer_name,
                 committer_email, planned_at, planner_name, planner_email)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                {
                    "change_id": "test123",
                    "change": "test_change",
                    "project": self.plan.project_name,
                    "note": test_note,
                    "committed_at": datetime.now(timezone.utc),
                    "committer_name": "Test User",
                    "committer_email": "test@example.com",
                    "planned_at": datetime.now(timezone.utc),
                    "planner_name": "Test User",
                    "planner_email": "test@example.com",
                },
            )

            # Retrieve and verify
            conn.execute(
                "SELECT note FROM CHANGES WHERE change_id = ?", {"change_id": "test123"}
            )
            result = conn.fetchone()

            assert result is not None
            assert result["note"] == test_note
