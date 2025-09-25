"""
Integration tests for the revert command.

This module tests the revert command functionality in realistic scenarios
with actual database operations and file system interactions.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from sqlitch.commands.revert import RevertCommand
from sqlitch.core.config import Config
from sqlitch.core.exceptions import SqlitchError
from sqlitch.core.sqitch import Sqitch


@pytest.fixture
def temp_project():
    """Create a temporary project directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create project structure
        (temp_path / "deploy").mkdir()
        (temp_path / "revert").mkdir()
        (temp_path / "verify").mkdir()

        # Create sqitch.conf
        config_content = """[core]
    engine = pg
    top_dir = .
    plan_file = sqitch.plan

[engine "pg"]
    target = db:pg://test@localhost/test_db
    registry = sqitch

[user]
    name = Test User
    email = test@example.com
"""
        (temp_path / "sqitch.conf").write_text(config_content)

        # Create sqitch.plan
        plan_content = """%syntax-version=1.0.0
%project=test_project
%uri=https://github.com/example/test_project

initial_schema 2023-01-01T12:00:00Z Test User <test@example.com> # Initial schema
users_table [initial_schema] 2023-01-02T12:00:00Z Test User <test@example.com> # Add users table
posts_table [users_table] 2023-01-03T12:00:00Z Test User <test@example.com> # Add posts table
"""
        (temp_path / "sqitch.plan").write_text(plan_content)

        # Create SQL files
        deploy_dir = temp_path / "deploy"
        revert_dir = temp_path / "revert"
        verify_dir = temp_path / "verify"

        # Initial schema
        (deploy_dir / "initial_schema.sql").write_text(
            """
-- Deploy initial_schema

CREATE SCHEMA IF NOT EXISTS app;
CREATE TABLE app.metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""
        )

        (revert_dir / "initial_schema.sql").write_text(
            """
-- Revert initial_schema

DROP TABLE IF EXISTS app.metadata;
DROP SCHEMA IF EXISTS app;
"""
        )

        (verify_dir / "initial_schema.sql").write_text(
            """
-- Verify initial_schema

SELECT 1/COUNT(*) FROM information_schema.tables
WHERE table_schema = 'app' AND table_name = 'metadata';
"""
        )

        # Users table
        (deploy_dir / "users_table.sql").write_text(
            """
-- Deploy users_table

CREATE TABLE app.users (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
"""
        )

        (revert_dir / "users_table.sql").write_text(
            """
-- Revert users_table

DROP TABLE IF EXISTS app.users;
"""
        )

        (verify_dir / "users_table.sql").write_text(
            """
-- Verify users_table

SELECT 1/COUNT(*) FROM information_schema.tables
WHERE table_schema = 'app' AND table_name = 'users';
"""
        )

        # Posts table
        (deploy_dir / "posts_table.sql").write_text(
            """
-- Deploy posts_table

CREATE TABLE app.posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES app.users(id),
    title TEXT NOT NULL,
    content TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
"""
        )

        (revert_dir / "posts_table.sql").write_text(
            """
-- Revert posts_table

DROP TABLE IF EXISTS app.posts;
"""
        )

        (verify_dir / "posts_table.sql").write_text(
            """
-- Verify posts_table

SELECT 1/COUNT(*) FROM information_schema.tables
WHERE table_schema = 'app' AND table_name = 'posts';
"""
        )

        # Change to project directory
        original_cwd = os.getcwd()
        os.chdir(temp_path)

        try:
            yield temp_path
        finally:
            os.chdir(original_cwd)


@pytest.fixture
def sqitch_instance(temp_project):
    """Create Sqitch instance for testing."""
    config = Config()
    return Sqitch(config=config)


@pytest.fixture
def revert_command(sqitch_instance):
    """Create RevertCommand instance."""
    return RevertCommand(sqitch_instance)


class TestRevertCommandIntegration:
    """Integration tests for revert command."""

    def test_revert_command_help(self, revert_command):
        """Test revert command help display."""
        with pytest.raises(SystemExit):
            revert_command.execute(["--help"])

    def test_revert_command_log_only_all_changes(self, revert_command):
        """Test log-only mode for all changes."""
        result = revert_command.execute(["--log-only"])
        assert result == 0

    def test_revert_command_log_only_to_change(self, revert_command):
        """Test log-only mode to specific change."""
        result = revert_command.execute(["--log-only", "--to-change", "users_table"])
        assert result == 0

    def test_revert_command_log_only_to_tag(self, revert_command):
        """Test log-only mode to specific tag."""
        # This will fail because there are no tags in our test plan
        result = revert_command.execute(["--log-only", "--to-tag", "v1.0"])
        assert result == 1  # Should fail because tag doesn't exist

    def test_revert_command_no_prompt_no_deployed_changes(self, revert_command):
        """Test revert with no deployed changes."""
        with patch(
            "sqlitch.engines.base.EngineRegistry.create_engine"
        ) as mock_create_engine:
            mock_engine = mock_create_engine.return_value
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = []

            result = revert_command.execute(["-y"])
            assert result == 0

    def test_revert_command_with_deployed_changes(self, revert_command):
        """Test revert with deployed changes."""
        # Load the actual plan to get real change IDs
        from sqlitch.core.plan import Plan

        plan = Plan.from_file(Path("sqitch.plan"))
        actual_change_ids = [change.id for change in plan.changes]

        with patch(
            "sqlitch.engines.base.EngineRegistry.create_engine"
        ) as mock_create_engine:
            mock_engine = mock_create_engine.return_value
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = actual_change_ids
            mock_engine.revert_change.return_value = None

            result = revert_command.execute(["-y"])
            assert result == 0

            # Should have called revert_change for each deployed change
            assert mock_engine.revert_change.call_count == len(actual_change_ids)

    def test_revert_command_to_specific_change(self, revert_command):
        """Test revert to specific change."""
        # Load the actual plan to get real change IDs
        from sqlitch.core.plan import Plan

        plan = Plan.from_file(Path("sqitch.plan"))
        actual_change_ids = [change.id for change in plan.changes]

        with patch(
            "sqlitch.engines.base.EngineRegistry.create_engine"
        ) as mock_create_engine:
            mock_engine = mock_create_engine.return_value
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = actual_change_ids
            mock_engine.revert_change.return_value = None

            result = revert_command.execute(["-y", "--to-change", "users_table"])
            assert result == 0

            # Should only revert changes after users_table (posts_table)
            assert mock_engine.revert_change.call_count == 1

    def test_revert_command_failure_handling(self, revert_command):
        """Test revert failure handling."""
        # Load the actual plan to get real change IDs
        from sqlitch.core.plan import Plan

        plan = Plan.from_file(Path("sqitch.plan"))
        actual_change_ids = [plan.changes[0].id]  # Just use first change

        with patch(
            "sqlitch.engines.base.EngineRegistry.create_engine"
        ) as mock_create_engine:
            mock_engine = mock_create_engine.return_value
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = actual_change_ids
            mock_engine.revert_change.side_effect = Exception("Revert failed")

            result = revert_command.execute(["-y"])
            assert result == 1

    def test_revert_command_invalid_target_change(self, revert_command):
        """Test revert to non-existent change."""
        # Load the actual plan to get real change IDs
        from sqlitch.core.plan import Plan

        plan = Plan.from_file(Path("sqitch.plan"))
        actual_change_ids = [plan.changes[0].id]  # Just use first change

        with patch(
            "sqlitch.engines.base.EngineRegistry.create_engine"
        ) as mock_create_engine:
            mock_engine = mock_create_engine.return_value
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = actual_change_ids

            result = revert_command.execute(["-y", "--to-change", "nonexistent"])
            assert result == 1

    def test_revert_command_strict_mode_without_target(self, revert_command):
        """Test strict mode without specifying target."""
        # Load the actual plan to get real change IDs
        from sqlitch.core.plan import Plan

        plan = Plan.from_file(Path("sqitch.plan"))
        actual_change_ids = [plan.changes[0].id]  # Just use first change

        with patch(
            "sqlitch.engines.base.EngineRegistry.create_engine"
        ) as mock_create_engine:
            mock_engine = mock_create_engine.return_value
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = actual_change_ids

            result = revert_command.execute(["--strict", "-y"])
            assert result == 1

    def test_revert_command_strict_mode_with_target(self, revert_command):
        """Test strict mode with target specified."""
        # Load the actual plan to get real change IDs
        from sqlitch.core.plan import Plan

        plan = Plan.from_file(Path("sqitch.plan"))
        actual_change_ids = [change.id for change in plan.changes]

        with patch(
            "sqlitch.engines.base.EngineRegistry.create_engine"
        ) as mock_create_engine:
            mock_engine = mock_create_engine.return_value
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = actual_change_ids
            mock_engine.revert_change.return_value = None

            result = revert_command.execute(
                ["--strict", "-y", "--to-change", "users_table"]
            )
            assert result == 0

    def test_revert_command_with_confirmation_prompt(self, revert_command):
        """Test revert with confirmation prompt."""
        # Load the actual plan to get real change IDs
        from sqlitch.core.plan import Plan

        plan = Plan.from_file(Path("sqitch.plan"))
        actual_change_ids = [plan.changes[0].id]  # Just use first change

        with (
            patch(
                "sqlitch.engines.base.EngineRegistry.create_engine"
            ) as mock_create_engine,
            patch("builtins.input", return_value="y"),
        ):

            mock_engine = mock_create_engine.return_value
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = actual_change_ids
            mock_engine.revert_change.return_value = None

            result = revert_command.execute([])
            assert result == 0

    def test_revert_command_confirmation_declined(self, revert_command):
        """Test revert when confirmation is declined."""
        # Load the actual plan to get real change IDs
        from sqlitch.core.plan import Plan

        plan = Plan.from_file(Path("sqitch.plan"))
        actual_change_ids = [plan.changes[0].id]  # Just use first change

        with (
            patch(
                "sqlitch.engines.base.EngineRegistry.create_engine"
            ) as mock_create_engine,
            patch("builtins.input", return_value="n"),
        ):

            mock_engine = mock_create_engine.return_value
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = actual_change_ids

            result = revert_command.execute([])
            assert result == 0

            # Should not have called revert_change
            mock_engine.revert_change.assert_not_called()

    def test_revert_command_invalid_arguments(self, revert_command):
        """Test revert with invalid arguments."""
        result = revert_command.execute(["--invalid-option"])
        assert result == 1

    def test_revert_command_missing_plan_file(self, revert_command):
        """Test revert when plan file is missing."""
        # Remove the plan file
        os.remove("sqitch.plan")

        result = revert_command.execute(["-y"])
        assert result == 1

    def test_revert_command_not_initialized(self, revert_command):
        """Test revert when project is not initialized."""
        # Remove config file to simulate uninitialized project
        os.remove("sqitch.conf")

        result = revert_command.execute(["-y"])
        assert result == 1


class TestRevertCommandCLIIntegration:
    """Test CLI integration for revert command."""

    def test_revert_command_cli_registration(self):
        """Test that revert command is registered in CLI."""
        from sqlitch.cli import cli

        # Check that revert command is registered
        assert "revert" in [cmd.name for cmd in cli.commands.values()]

    def test_revert_command_click_wrapper(self, temp_project):
        """Test Click command wrapper."""
        from click.testing import CliRunner

        from sqlitch.cli import CliContext
        from sqlitch.commands.revert import revert_command

        runner = CliRunner()

        # Test help
        result = runner.invoke(revert_command, ["--help"])
        assert result.exit_code == 0
        assert "Revert database changes" in result.output

        # Test log-only mode with proper context setup
        cli_ctx = CliContext()
        with patch(
            "sqlitch.engines.base.EngineRegistry.create_engine"
        ) as mock_create_engine:
            mock_engine = mock_create_engine.return_value
            mock_engine.ensure_registry.return_value = None
            mock_engine.get_deployed_changes.return_value = []

            result = runner.invoke(revert_command, ["--log-only"], obj=cli_ctx)
            assert result.exit_code == 0
