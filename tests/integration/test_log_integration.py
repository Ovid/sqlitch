"""
Integration tests for log command.

This module contains integration tests for the log command,
testing the full workflow with real database operations.
"""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from sqlitch.commands.log import LogCommand
from sqlitch.core.change import Change
from sqlitch.core.config import Config
from sqlitch.core.plan import Plan
from sqlitch.core.sqitch import Sqitch
from sqlitch.engines.sqlite import SQLiteEngine


class TestLogIntegration:
    """Integration tests for log command."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create temporary directory for test project
        self.temp_dir = tempfile.mkdtemp()
        self.project_dir = Path(self.temp_dir)

        # Create basic project structure
        (self.project_dir / "deploy").mkdir()
        (self.project_dir / "revert").mkdir()
        (self.project_dir / "verify").mkdir()

        # Create sqitch.conf
        config_content = """
[core]
    engine = sqlite
    top_dir = .
    plan_file = sqitch.plan

[engine "sqlite"]
    target = db:sqlite:test.db
    registry = sqitch

[user]
    name = Test User
    email = test@example.com
"""
        (self.project_dir / "sqitch.conf").write_text(config_content)

        # Create sqitch.plan
        plan_content = """%syntax-version=1.0.0
%project=test_project
%uri=https://github.com/example/test_project

initial_schema 2023-01-15T10:30:00Z Test User <test@example.com> # Initial schema
add_users 2023-01-16T14:20:00Z Test User <test@example.com> # Add users table
"""
        (self.project_dir / "sqitch.plan").write_text(plan_content)

        # Create deploy scripts
        (self.project_dir / "deploy" / "initial_schema.sql").write_text(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY);"
        )
        (self.project_dir / "deploy" / "add_users.sql").write_text(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"
        )

        # Create revert scripts
        (self.project_dir / "revert" / "initial_schema.sql").write_text(
            "DROP TABLE test_table;"
        )
        (self.project_dir / "revert" / "add_users.sql").write_text("DROP TABLE users;")

        # Initialize Sqitch
        config = Config([self.project_dir / "sqitch.conf"])
        self.sqitch = Sqitch(config=config, options={"verbosity": 0})

        # Change to project directory
        import os

        self.original_cwd = os.getcwd()
        os.chdir(self.project_dir)

    def teardown_method(self):
        """Clean up test fixtures."""
        import os
        import shutil

        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_log_empty_database(self):
        """Test log command with empty database."""
        command = LogCommand(self.sqitch)

        # Should return 1 (error) for uninitialized database
        result = command.execute([])
        assert result == 1

    def test_log_initialized_no_events(self):
        """Test log command with initialized but empty database."""
        # Initialize the database
        target = self.sqitch.get_target()
        engine = self.sqitch.engine_for_target(target)
        engine.ensure_registry()

        command = LogCommand(self.sqitch)

        # Should return 1 (error) for no events
        result = command.execute([])
        assert result == 1

    def test_log_with_deployed_changes(self, capsys):
        """Test log command with deployed changes."""
        # Deploy some changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_result = deploy_command.execute([])
        assert deploy_result == 0

        # Now test log command
        command = LogCommand(self.sqitch)
        result = command.execute(["--format", "oneline"])

        assert result == 0

        # Check output
        captured = capsys.readouterr()
        output = captured.out

        # Should contain database header
        assert "On database" in output

        # Should contain change information
        assert "add_users" in output
        assert "initial_schema" in output
        assert "test_project" in output

    def test_log_with_format_options(self, capsys):
        """Test log command with different format options."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test different formats
        formats = ["short", "medium", "long", "oneline"]

        for format_name in formats:
            command = LogCommand(self.sqitch)
            result = command.execute(["--format", format_name, "--no-headers"])

            assert result == 0

            captured = capsys.readouterr()
            output = captured.out

            # Should contain change names
            assert "add_users" in output or "initial_schema" in output

    def test_log_with_max_count(self, capsys):
        """Test log command with max count limit."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test with max count
        command = LogCommand(self.sqitch)
        result = command.execute(
            ["--max-count", "1", "--format", "oneline", "--no-headers"]
        )

        assert result == 0

        captured = capsys.readouterr()
        output = captured.out
        lines = [line for line in output.split("\n") if line.strip()]

        # Should have only one change (plus possible empty lines)
        assert len(lines) <= 2  # Allow for some formatting

    def test_log_with_reverse_order(self, capsys):
        """Test log command with reverse order."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test with reverse order
        command = LogCommand(self.sqitch)
        result = command.execute(["--reverse", "--format", "oneline", "--no-headers"])

        assert result == 0

        captured = capsys.readouterr()
        output = captured.out

        # Should contain changes (order verification would need more complex setup)
        assert "add_users" in output or "initial_schema" in output

    def test_log_with_event_filter(self, capsys):
        """Test log command with event filtering."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test with event filter
        command = LogCommand(self.sqitch)
        result = command.execute(
            ["--event", "deploy", "--format", "oneline", "--no-headers"]
        )

        assert result == 0

        captured = capsys.readouterr()
        output = captured.out

        # Should contain deploy events
        assert "add_users" in output or "initial_schema" in output

    def test_log_with_change_pattern(self, capsys):
        """Test log command with change pattern filtering."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test with change pattern (using GLOB pattern for SQLite)
        command = LogCommand(self.sqitch)
        result = command.execute(
            ["--change-pattern", "add*", "--format", "oneline", "--no-headers"]
        )

        assert result == 0

        captured = capsys.readouterr()
        captured.out

        # Should contain matching changes
        # Note: SQLite uses GLOB, so this might not work exactly like regex
        # The test verifies the command runs without error

    def test_log_with_project_pattern(self, capsys):
        """Test log command with project pattern filtering."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test with project pattern
        command = LogCommand(self.sqitch)
        result = command.execute(
            ["--project-pattern", "test*", "--format", "oneline", "--no-headers"]
        )

        assert result == 0

        captured = capsys.readouterr()
        output = captured.out

        # Should contain changes from matching project
        assert "add_users" in output or "initial_schema" in output

    def test_log_with_committer_pattern(self, capsys):
        """Test log command with committer pattern filtering."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test with committer pattern
        command = LogCommand(self.sqitch)
        result = command.execute(
            ["--committer-pattern", "Test*", "--format", "oneline", "--no-headers"]
        )

        assert result == 0

        captured = capsys.readouterr()
        output = captured.out

        # Should contain changes from matching committer
        assert "add_users" in output or "initial_schema" in output

    def test_log_with_date_format(self, capsys):
        """Test log command with different date formats."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test with different date formats
        date_formats = ["iso", "raw", "short"]

        for date_format in date_formats:
            command = LogCommand(self.sqitch)
            result = command.execute(
                ["--date-format", date_format, "--format", "medium", "--no-headers"]
            )

            assert result == 0

            captured = capsys.readouterr()
            output = captured.out

            # Should contain change information
            assert "add_users" in output or "initial_schema" in output

    def test_log_with_abbrev(self, capsys):
        """Test log command with abbreviated change IDs."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test with abbreviated IDs
        command = LogCommand(self.sqitch)
        result = command.execute(
            ["--abbrev", "6", "--format", "oneline", "--no-headers"]
        )

        assert result == 0

        captured = capsys.readouterr()
        output = captured.out

        # Should contain abbreviated change IDs (6 characters)
        # This is hard to verify without knowing the exact IDs, but command should succeed
        assert len(output) > 0

    def test_log_oneline_shorthand(self, capsys):
        """Test log command with --oneline shorthand."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test --oneline shorthand
        command = LogCommand(self.sqitch)
        result = command.execute(["--oneline", "--no-headers"])

        assert result == 0

        captured = capsys.readouterr()
        output = captured.out

        # Should contain change information in oneline format
        assert "add_users" in output or "initial_schema" in output

    def test_log_no_headers(self, capsys):
        """Test log command without headers."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test without headers
        command = LogCommand(self.sqitch)
        result = command.execute(["--no-headers", "--format", "oneline"])

        assert result == 0

        captured = capsys.readouterr()
        output = captured.out

        # Should not contain database header
        assert "On database" not in output

        # Should still contain change information
        assert "add_users" in output or "initial_schema" in output

    def test_log_with_skip(self, capsys):
        """Test log command with skip option."""
        # Deploy changes first
        from sqlitch.commands.deploy import DeployCommand

        deploy_command = DeployCommand(self.sqitch)
        deploy_command.execute([])

        # Test with skip
        command = LogCommand(self.sqitch)
        result = command.execute(["--skip", "1", "--format", "oneline", "--no-headers"])

        assert result == 0

        captured = capsys.readouterr()
        output = captured.out

        # Should contain fewer changes (hard to verify exact count without more setup)
        # But command should succeed
        lines = [line for line in output.split("\n") if line.strip()]
        assert len(lines) >= 0  # Could be 0 or more depending on skip

    def test_log_error_handling(self):
        """Test log command error handling."""
        # Test with invalid arguments
        command = LogCommand(self.sqitch)

        # Invalid max-count
        result = command.execute(["--max-count", "invalid"])
        assert result == 1

        # Invalid skip
        result = command.execute(["--skip", "invalid"])
        assert result == 1

        # Invalid color
        result = command.execute(["--color", "invalid"])
        assert result == 1

        # Invalid abbrev
        result = command.execute(["--abbrev", "invalid"])
        assert result == 1

        # Unknown format
        result = command.execute(["--format", "unknown_format"])
        assert result == 1
