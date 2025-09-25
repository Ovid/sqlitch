"""
Integration tests for PostgreSQL engine.

These tests verify that the PostgreSQL engine can be properly instantiated
and integrated with the rest of the system.
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlitch.core.change import Change, Dependency
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.core.types import URI
from sqlitch.engines.base import EngineRegistry
from sqlitch.engines.pg import PostgreSQLEngine


class TestPostgreSQLEngineIntegration:
    """Integration tests for PostgreSQL engine."""

    def test_engine_registration(self):
        """Test that PostgreSQL engine is properly registered."""
        # The engine should be registered via the @register_engine decorator
        supported_engines = EngineRegistry.list_supported_engines()
        assert "pg" in supported_engines

        # Should be able to get the engine class
        engine_class = EngineRegistry.get_engine_class("pg")
        assert engine_class == PostgreSQLEngine

    def test_engine_creation_via_registry(self):
        """Test creating PostgreSQL engine via registry."""
        target = Target(
            name="test_pg",
            uri=URI("db:pg://user:pass@localhost:5432/testdb"),
            registry="sqitch",
        )

        plan = Mock(spec=Plan)
        plan.project_name = "test_project"
        plan.creator_name = "Test User"
        plan.creator_email = "test@example.com"

        with patch("sqlitch.engines.pg.psycopg2") as mock_pg:
            mock_pg.connect = Mock()
            mock_pg.Error = Exception
            mock_pg.extras = Mock()
            mock_pg.extensions = Mock()

            # Create engine via registry
            engine = EngineRegistry.create_engine(target, plan)

            assert isinstance(engine, PostgreSQLEngine)
            assert engine.target == target
            assert engine.plan == plan
            assert engine.engine_type == "pg"

    def test_engine_with_real_change_objects(self):
        """Test engine with real Change objects."""
        target = Target(
            name="test_pg",
            uri=URI("db:pg://user:pass@localhost:5432/testdb"),
            registry="sqitch",
        )

        # Create a real Plan object
        plan = Mock(spec=Plan)
        plan.project_name = "test_project"
        plan.creator_name = "Test User"
        plan.creator_email = "test@example.com"

        # Create real Change objects
        change1 = Change(
            name="initial_schema",
            note="Initial database schema",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="Test User",
            planner_email="test@example.com",
            tags=["v1.0"],
            dependencies=[],
            conflicts=[],
        )

        change2 = Change(
            name="add_users_table",
            note="Add users table",
            timestamp=datetime(2023, 1, 16, 14, 20, 0),
            planner_name="Test User",
            planner_email="test@example.com",
            tags=[],
            dependencies=[Dependency(type="require", change="initial_schema")],
            conflicts=[],
        )

        plan.changes = [change1, change2]

        # Mock file path methods
        def mock_get_deploy_file(change):
            return Path(f"/fake/deploy/{change.name}.sql")

        def mock_get_revert_file(change):
            return Path(f"/fake/revert/{change.name}.sql")

        def mock_get_verify_file(change):
            return Path(f"/fake/verify/{change.name}.sql")

        plan.get_deploy_file = mock_get_deploy_file
        plan.get_revert_file = mock_get_revert_file
        plan.get_verify_file = mock_get_verify_file

        with patch("sqlitch.engines.pg.psycopg2") as mock_pg:
            mock_pg.connect = Mock()
            mock_pg.Error = Exception
            mock_pg.extras = Mock()
            mock_pg.extensions = Mock()

            # Create engine
            engine = PostgreSQLEngine(target, plan)

            # Test that helper methods work with real Change objects
            # Mock file existence and content for hash calculation
            def mock_read_bytes(path_self):
                if "initial_schema" in str(path_self):
                    return b"CREATE TABLE initial_schema_table;"
                elif "add_users_table" in str(path_self):
                    return b"CREATE TABLE users;"
                else:
                    return b"-- empty"

            with (
                patch("pathlib.Path.exists", return_value=True),
                patch("pathlib.Path.read_bytes", mock_read_bytes),
            ):

                hash1 = engine._calculate_script_hash(change1)
                hash2 = engine._calculate_script_hash(change2)

                # Hashes should be different for different changes
                assert hash1 != hash2
                assert len(hash1) == 40  # SHA1 hex digest length
                assert len(hash2) == 40

            # Test dependency resolution
            dep_id = engine._resolve_dependency_id("initial_schema")
            assert dep_id == change1.id

            dep_id_missing = engine._resolve_dependency_id("nonexistent")
            assert dep_id_missing is None

            # Test dependency formatting
            requires = engine._format_dependencies(["initial_schema", "other_change"])
            assert requires == "initial_schema other_change"

            empty_requires = engine._format_dependencies([])
            assert empty_requires == ""

            # Test tag formatting
            tags = engine._format_tags(["v1.0", "release"])
            assert tags == "v1.0 release"

            empty_tags = engine._format_tags([])
            assert empty_tags == ""

    def test_connection_string_parsing_variations(self):
        """Test various PostgreSQL connection string formats."""
        plan = Mock(spec=Plan)
        plan.project_name = "test_project"

        test_cases = [
            # (input_uri, expected_params)
            (
                "db:pg://user:pass@host:5433/mydb",
                {
                    "host": "host",
                    "port": 5433,
                    "database": "mydb",
                    "user": "user",
                    "password": "pass",
                },
            ),
            ("db:pg:///mydb", {"host": "localhost", "port": 5432, "database": "mydb"}),
            (
                "db:pg://localhost/mydb?sslmode=require",
                {
                    "host": "localhost",
                    "port": 5432,
                    "database": "mydb",
                    "sslmode": "require",
                },
            ),
            (
                "postgresql://user@host/db",
                {"host": "host", "port": 5432, "database": "db", "user": "user"},
            ),
        ]

        with patch("sqlitch.engines.pg.psycopg2") as mock_pg:
            mock_pg.connect = Mock()
            mock_pg.Error = Exception
            mock_pg.extras = Mock()
            mock_pg.extensions = Mock()

            for uri_str, expected_params in test_cases:
                target = Target(name="test", uri=URI(uri_str), registry="sqitch")

                engine = PostgreSQLEngine(target, plan)

                # Check that all expected parameters are present
                for key, value in expected_params.items():
                    assert (
                        engine._connection_params.get(key) == value
                    ), f"For URI {uri_str}, expected {key}={value}, got {engine._connection_params.get(key)}"

    def test_registry_schema_sql_generation(self):
        """Test that registry schema generates valid SQL."""
        from sqlitch.engines.pg import PostgreSQLRegistrySchema

        statements = PostgreSQLRegistrySchema.get_create_statements("pg")

        # Should have multiple statements
        assert len(statements) > 5

        # Check for key SQL elements
        sql_text = " ".join(statements)

        # Should create schema
        assert "CREATE SCHEMA IF NOT EXISTS sqitch" in sql_text

        # Should create all required tables
        assert "CREATE TABLE IF NOT EXISTS sqitch.projects" in sql_text
        assert "CREATE TABLE IF NOT EXISTS sqitch.releases" in sql_text
        assert "CREATE TABLE IF NOT EXISTS sqitch.changes" in sql_text
        assert "CREATE TABLE IF NOT EXISTS sqitch.tags" in sql_text
        assert "CREATE TABLE IF NOT EXISTS sqitch.dependencies" in sql_text
        assert "CREATE TABLE IF NOT EXISTS sqitch.events" in sql_text

        # Should insert registry version
        assert "INSERT INTO sqitch.releases" in sql_text

        # Should have proper foreign key constraints
        assert "REFERENCES sqitch.changes(change_id)" in sql_text

        # Should have proper check constraints
        assert "CHECK (event IN ('deploy', 'revert', 'fail', 'merge'))" in sql_text
