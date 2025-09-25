"""Basic CLI integration tests."""

import pytest
from click.testing import CliRunner

from sqlitch.cli import cli


class TestCLIIntegration:
    """Test CLI integration and basic command functionality."""

    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()

    def test_cli_help(self, runner):
        """Test that CLI help works."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Sqlitch database change management" in result.output
        assert "Commands:" in result.output

    def test_cli_version(self, runner):
        """Test that CLI version works."""
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "sqlitch" in result.output.lower()

    def test_command_help_pages(self, runner):
        """Test that all commands have help pages."""
        commands = [
            "init",
            "add",
            "deploy",
            "revert",
            "verify",
            "status",
            "log",
            "tag",
            "show",
            "bundle",
            "checkout",
            "rebase",
        ]

        for command in commands:
            result = runner.invoke(cli, [command, "--help"])
            assert result.exit_code == 0, f"Help for {command} command failed"
            assert "Usage:" in result.output, f"Help for {command} missing usage"

    def test_init_command_basic(self, runner):
        """Test basic init command functionality."""
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["init", "sqlite"])
            assert result.exit_code == 0

            # Check that files were created
            import os

            assert os.path.exists("sqitch.conf")
            assert os.path.exists("sqitch.plan")
            assert os.path.exists("deploy")
            assert os.path.exists("revert")
            assert os.path.exists("verify")

    def test_status_command_uninitialized(self, runner):
        """Test status command on uninitialized project."""
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["status"])
            # Should fail gracefully for uninitialized project
            assert result.exit_code != 0

    def test_invalid_command(self, runner):
        """Test handling of invalid commands."""
        result = runner.invoke(cli, ["nonexistent"])
        assert result.exit_code != 0
        assert "No such command" in result.output

    def test_global_options(self, runner):
        """Test global CLI options."""
        # Test verbose option
        result = runner.invoke(cli, ["-v", "--help"])
        assert result.exit_code == 0

        # Test quiet option
        result = runner.invoke(cli, ["-q", "--help"])
        assert result.exit_code == 0

        # Test config option (should not fail even with non-existent file)
        result = runner.invoke(cli, ["-c", "nonexistent.conf", "--help"])
        assert result.exit_code == 0
