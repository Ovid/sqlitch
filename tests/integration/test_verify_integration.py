"""
Integration tests for the verify command.

These tests verify that the verify command works correctly with real
database connections and file system operations.
"""

import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from sqlitch.commands.verify import VerifyCommand
from sqlitch.core.change import Change
from sqlitch.core.config import Config
from sqlitch.core.exceptions import SqlitchError
from sqlitch.core.plan import Plan
from sqlitch.core.sqitch import Sqitch
from sqlitch.core.target import Target


@pytest.fixture
def temp_project():
    """Create temporary project directory."""
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create project structure
        (temp_dir / "deploy").mkdir()
        (temp_dir / "revert").mkdir()
        (temp_dir / "verify").mkdir()

        # Create sqitch.conf
        config_content = """
[core]
    engine = pg
    top_dir = .
    plan_file = sqitch.plan

[engine "pg"]
    target = db:pg://test@localhost/test_db
    registry = sqitch
"""
        (temp_dir / "sqitch.conf").write_text(config_content)

        # Create sqitch.plan
        plan_content = """%syntax-version=1.0.0
%project=test_project
%uri=https://github.com/example/test_project

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
users [initial_schema] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
posts [users] 2023-01-17T09:15:00Z Jane Smith <jane@example.com> # Add posts table
"""
        (temp_dir / "sqitch.plan").write_text(plan_content)

        yield temp_dir

    finally:
        shutil.rmtree(temp_dir)


@pytest.fixture
def temp_project_with_scripts(temp_project):
    """Create temporary project with SQL scripts."""
    # Create deploy scripts
    deploy_dir = temp_project / "deploy"
    revert_dir = temp_project / "revert"
    verify_dir = temp_project / "verify"

    # Initial schema
    (deploy_dir / "initial_schema.sql").write_text(
        """
-- Deploy initial_schema

CREATE SCHEMA IF NOT EXISTS app;
"""
    )

    (revert_dir / "initial_schema.sql").write_text(
        """
-- Revert initial_schema

DROP SCHEMA IF EXISTS app CASCADE;
"""
    )

    (verify_dir / "initial_schema.sql").write_text(
        """
-- Verify initial_schema

SELECT 1/COUNT(*) FROM information_schema.schemata WHERE schema_name = 'app';
"""
    )

    # Users table
    (deploy_dir / "users.sql").write_text(
        """
-- Deploy users

CREATE TABLE app.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
"""
    )

    (revert_dir / "users.sql").write_text(
        """
-- Revert users

DROP TABLE IF EXISTS app.users;
"""
    )

    (verify_dir / "users.sql").write_text(
        """
-- Verify users

SELECT 1/COUNT(*) FROM information_schema.tables
WHERE table_schema = 'app' AND table_name = 'users';
"""
    )

    # Posts table
    (deploy_dir / "posts.sql").write_text(
        """
-- Deploy posts

CREATE TABLE app.posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES app.users(id),
    title VARCHAR(200) NOT NULL,
    content TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
"""
    )

    (revert_dir / "posts.sql").write_text(
        """
-- Revert posts

DROP TABLE IF EXISTS app.posts;
"""
    )

    (verify_dir / "posts.sql").write_text(
        """
-- Verify posts

SELECT 1/COUNT(*) FROM information_schema.tables
WHERE table_schema = 'app' AND table_name = 'posts';
"""
    )

    return temp_project


class TestVerifyIntegration:
    """Integration tests for verify command."""

    def test_verify_command_creation(self, temp_project):
        """Test creating verify command with real configuration."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            assert command.sqitch == sqitch
            assert command.config == config
        finally:
            os.chdir(original_cwd)

    def test_parse_args_integration(self, temp_project):
        """Test argument parsing in real environment."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            # Test various argument combinations
            options = command._parse_args(["--target", "prod", "--from", "users"])
            assert options["target"] == "prod"
            assert options["from_change"] == "users"

            options = command._parse_args(["users", "posts", "--set", "var=value"])
            assert options["from_change"] == "users"
            assert options["to_change"] == "posts"
            assert options["variables"] == {"var": "value"}
        finally:
            os.chdir(original_cwd)

    def test_load_plan_integration(self, temp_project):
        """Test loading real plan file."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            plan = command._load_plan()

            assert plan.project_name == "test_project"
            assert len(plan.changes) == 3
            assert plan.changes[0].name == "initial_schema"
            assert plan.changes[1].name == "users"
            assert plan.changes[2].name == "posts"
        finally:
            os.chdir(original_cwd)

    def test_load_plan_custom_file(self, temp_project):
        """Test loading custom plan file."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            # Create custom plan file
            custom_plan = temp_project / "custom.plan"
            custom_plan.write_text(
                """%syntax-version=1.0.0
%project=custom_project

test_change 2023-01-15T10:30:00Z Test User <test@example.com> # Test change
"""
            )

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            plan = command._load_plan(custom_plan)

            assert plan.project_name == "custom_project"
            assert len(plan.changes) == 1
            assert plan.changes[0].name == "test_change"
        finally:
            os.chdir(original_cwd)

    def test_load_plan_file_not_found(self, temp_project):
        """Test loading non-existent plan file."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            with pytest.raises(SqlitchError, match="Plan file not found"):
                command._load_plan(Path("nonexistent.plan"))
        finally:
            os.chdir(original_cwd)

    def test_determine_verification_range_integration(self, temp_project):
        """Test determining verification range with real plan."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            plan = command._load_plan()
            deployed_changes = plan.changes[:2]  # First two changes deployed

            # Test default range
            from_idx, to_idx = command._determine_verification_range(
                plan, deployed_changes, {}
            )
            assert from_idx == 0
            assert to_idx == 1

            # Test with from_change
            from_idx, to_idx = command._determine_verification_range(
                plan, deployed_changes, {"from_change": "users"}
            )
            assert from_idx == 1
            assert to_idx == 1
        finally:
            os.chdir(original_cwd)

    def test_determine_verification_range_change_not_deployed(self, temp_project):
        """Test error when change not deployed."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            plan = command._load_plan()
            deployed_changes = plan.changes[:1]  # Only first change deployed

            with pytest.raises(
                SqlitchError, match='Change "posts" has not been deployed'
            ):
                command._determine_verification_range(
                    plan, deployed_changes, {"from_change": "posts"}
                )
        finally:
            os.chdir(original_cwd)

    def test_determine_verification_range_change_not_found(self, temp_project):
        """Test error when change not found."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            plan = command._load_plan()
            deployed_changes = plan.changes

            with pytest.raises(SqlitchError, match='Cannot find "nonexistent"'):
                command._determine_verification_range(
                    plan, deployed_changes, {"from_change": "nonexistent"}
                )
        finally:
            os.chdir(original_cwd)

    def test_verify_changes_no_deployed(self, temp_project):
        """Test verification with no deployed changes."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            # Mock engine with no deployed changes
            from unittest.mock import Mock

            mock_engine = Mock()
            mock_engine.target.name = "test_db"
            mock_engine.get_deployed_changes.return_value = []
            mock_engine.ensure_registry.return_value = None

            plan = command._load_plan()

            result = command._verify_changes(mock_engine, plan, {})
            assert result == 0
        finally:
            os.chdir(original_cwd)

    def test_verify_changes_no_plan(self, temp_project):
        """Test verification with deployed changes but no plan."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            # Mock engine with deployed changes
            from unittest.mock import Mock

            mock_engine = Mock()
            mock_engine.target.name = "test_db"
            mock_engine.get_deployed_changes.return_value = ["change1"]
            mock_engine.ensure_registry.return_value = None

            # Create empty plan
            empty_plan = Plan(
                file=Path("sqitch.plan"), project="test", uri=None, changes=[], tags=[]
            )

            with pytest.raises(
                SqlitchError, match="There are deployed changes, but none planned"
            ):
                command._verify_changes(mock_engine, empty_plan, {})
        finally:
            os.chdir(original_cwd)

    def test_execute_not_initialized(self, temp_project):
        """Test execution in non-initialized directory."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            # Remove sqitch.conf to simulate non-initialized project
            (temp_project / "sqitch.conf").unlink()

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            # Mock require_initialized to raise error
            from unittest.mock import patch

            with patch.object(
                sqitch,
                "require_initialized",
                side_effect=SqlitchError("Not initialized"),
            ):
                result = command.execute([])
                assert result == 1
        finally:
            os.chdir(original_cwd)

    def test_execute_with_mock_engine(self, temp_project):
        """Test full execution with mocked engine."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            from unittest.mock import Mock, patch

            # Mock engine
            mock_engine = Mock()
            mock_engine.target.name = "test_db"
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = []

            # Mock target
            mock_target = Mock()

            with patch.object(command, "get_target", return_value=mock_target):
                with patch(
                    "sqlitch.engines.base.EngineRegistry.create_engine",
                    return_value=mock_engine,
                ):
                    result = command.execute([])
                    assert result == 0
        finally:
            os.chdir(original_cwd)

    def test_execute_with_arguments(self, temp_project):
        """Test execution with various arguments."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            from unittest.mock import Mock, patch

            # Mock engine
            mock_engine = Mock()
            mock_engine.target.name = "test_db"
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = []

            # Mock target
            mock_target = Mock()

            with patch.object(command, "get_target", return_value=mock_target):
                with patch(
                    "sqlitch.engines.base.EngineRegistry.create_engine",
                    return_value=mock_engine,
                ):
                    # Test with various argument combinations
                    result = command.execute(["--target", "prod"])
                    assert result == 0

                    result = command.execute(["--from", "users", "--to", "posts"])
                    assert result == 0

                    result = command.execute(["--set", "var=value", "--no-parallel"])
                    assert result == 0
        finally:
            os.chdir(original_cwd)

    def test_file_operations(self, temp_project_with_scripts):
        """Test that verify command can access SQL files."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project_with_scripts)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            plan = command._load_plan()

            # Check that verify files exist
            for change in plan.changes:
                verify_file = (
                    temp_project_with_scripts / "verify" / f"{change.name}.sql"
                )
                assert verify_file.exists()

                # Check file content
                content = verify_file.read_text()
                assert f"-- Verify {change.name}" in content
        finally:
            os.chdir(original_cwd)

    def test_error_handling_integration(self, temp_project):
        """Test error handling in integration context."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            # Test with invalid arguments
            result = command.execute(["--invalid-option"])
            assert result == 1

            # Test with missing plan file
            (temp_project / "sqitch.plan").unlink()
            result = command.execute([])
            assert result == 1
        finally:
            os.chdir(original_cwd)

    def test_verbosity_integration(self, temp_project):
        """Test command with different verbosity levels."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            # Test with different verbosity levels
            for verbosity in [0, 1, 2]:
                config = Config()
                sqitch = Sqitch(config=config, options={"verbosity": verbosity})
                command = VerifyCommand(sqitch)

                assert command.sqitch.verbosity == verbosity
        finally:
            os.chdir(original_cwd)

    def test_parallel_vs_sequential(self, temp_project):
        """Test parallel vs sequential execution modes."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            from unittest.mock import Mock, patch

            # Mock engine with multiple deployed changes
            mock_engine = Mock()
            mock_engine.target.name = "test_db"
            mock_engine.ensure_registry.return_value = None

            plan = command._load_plan()
            deployed_change_ids = [change.id for change in plan.changes]
            mock_engine.get_deployed_changes.return_value = deployed_change_ids
            mock_engine.verify_change.return_value = True

            mock_target = Mock()

            with patch.object(command, "get_target", return_value=mock_target):
                with patch(
                    "sqlitch.engines.base.EngineRegistry.create_engine",
                    return_value=mock_engine,
                ):
                    # Test parallel execution
                    result = command.execute(["--parallel"])
                    assert result == 0

                    # Test sequential execution
                    result = command.execute(["--no-parallel"])
                    assert result == 0
        finally:
            os.chdir(original_cwd)

    def test_max_workers_option(self, temp_project):
        """Test max workers option."""
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project)

            config = Config()
            sqitch = Sqitch(config=config)
            command = VerifyCommand(sqitch)

            # Test parsing max workers
            options = command._parse_args(["--max-workers", "8"])
            assert options["max_workers"] == 8

            # Test invalid max workers
            result = command.execute(["--max-workers", "invalid"])
            assert result == 1
        finally:
            os.chdir(original_cwd)
