"""Unit tests for the CLI module."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import click
import pytest
from click.testing import CliRunner

from sqlitch.cli import (
    CliContext,
    cli,
    create_command_wrapper,
    format_command_error,
    get_sqitch_from_context,
    handle_keyboard_interrupt,
    handle_sqlitch_error,
    handle_unexpected_error,
    main,
    suggest_command_help,
)
from sqlitch.core.config import Config
from sqlitch.core.exceptions import ConfigurationError, SqlitchError
from sqlitch.core.sqitch import Sqitch


class TestCliContext:
    """Test the CliContext class."""

    def test_init(self):
        """Test CliContext initialization."""
        ctx = CliContext()

        assert ctx.config_files == []
        assert ctx.verbosity == 0
        assert ctx.sqitch is None

    def test_create_sqitch_success(self):
        """Test successful Sqitch creation."""
        ctx = CliContext()
        ctx.config_files = [Path("test.conf")]
        ctx.verbosity = 1

        with patch("sqlitch.cli.Config") as mock_config_class:
            with patch("sqlitch.cli.Sqitch") as mock_sqitch_class:
                mock_config = Mock(spec=Config)
                mock_sqitch = Mock(spec=Sqitch)
                mock_config_class.return_value = mock_config
                mock_sqitch_class.return_value = mock_sqitch

                result = ctx.create_sqitch()

                assert result == mock_sqitch
                assert ctx.sqitch == mock_sqitch
                mock_config_class.assert_called_with([Path("test.conf")])
                mock_sqitch_class.assert_called_with(
                    config=mock_config, options={"verbosity": 1}
                )

    def test_create_sqitch_no_config_files(self):
        """Test Sqitch creation with no config files."""
        ctx = CliContext()

        with patch("sqlitch.cli.Config") as mock_config_class:
            with patch("sqlitch.cli.Sqitch") as mock_sqitch_class:
                mock_config = Mock(spec=Config)
                mock_sqitch = Mock(spec=Sqitch)
                mock_config_class.return_value = mock_config
                mock_sqitch_class.return_value = mock_sqitch

                result = ctx.create_sqitch()

                assert result == mock_sqitch
                mock_config_class.assert_called_with(None)

    def test_create_sqitch_cached(self):
        """Test that Sqitch instance is cached."""
        ctx = CliContext()

        with patch("sqlitch.cli.Config") as mock_config_class:
            with patch("sqlitch.cli.Sqitch") as mock_sqitch_class:
                mock_sqitch = Mock(spec=Sqitch)
                mock_sqitch_class.return_value = mock_sqitch

                # First call
                result1 = ctx.create_sqitch()
                # Second call
                result2 = ctx.create_sqitch()

                assert result1 == result2 == mock_sqitch
                # Should only be called once due to caching
                mock_config_class.assert_called_once()
                mock_sqitch_class.assert_called_once()

    def test_create_sqitch_exception(self):
        """Test Sqitch creation with exception."""
        ctx = CliContext()

        with patch("sqlitch.cli.Config", side_effect=Exception("Config error")):
            with pytest.raises(
                ConfigurationError, match="Failed to initialize sqlitch: Config error"
            ):
                ctx.create_sqitch()


class TestCliCommand:
    """Test the main CLI command."""

    def test_cli_with_no_subcommand(self):
        """Test CLI with no subcommand shows help."""
        runner = CliRunner()

        result = runner.invoke(cli, [])

        assert result.exit_code == 0
        assert "Sqlitch database change management" in result.output
        assert "Usage:" in result.output

    def test_cli_with_config_option(self, tmp_path):
        """Test CLI with config option."""
        config_file = tmp_path / "test.conf"
        config_file.write_text("[core]\nengine = pg\n")

        runner = CliRunner()

        # Mock the context to avoid actual command execution
        with patch("sqlitch.cli.CliContext") as mock_ctx_class:
            mock_ctx = Mock()
            mock_ctx_class.return_value = mock_ctx

            result = runner.invoke(cli, ["--config", str(config_file)])

            assert result.exit_code == 0
            assert mock_ctx.config_files == [str(config_file)]

    def test_cli_with_multiple_config_files(self, tmp_path):
        """Test CLI with multiple config files."""
        config1 = tmp_path / "test1.conf"
        config2 = tmp_path / "test2.conf"
        config1.write_text("[core]\nengine = pg\n")
        config2.write_text("[user]\nname = test\n")

        runner = CliRunner()

        with patch("sqlitch.cli.CliContext") as mock_ctx_class:
            mock_ctx = Mock()
            mock_ctx_class.return_value = mock_ctx

            result = runner.invoke(
                cli, ["--config", str(config1), "--config", str(config2)]
            )

            assert result.exit_code == 0
            assert mock_ctx.config_files == [str(config1), str(config2)]

    def test_cli_with_verbose_option(self):
        """Test CLI with verbose option."""
        runner = CliRunner()

        with patch("sqlitch.cli.CliContext") as mock_ctx_class:
            mock_ctx = Mock()
            mock_ctx_class.return_value = mock_ctx

            result = runner.invoke(cli, ["-v", "-v"])

            assert result.exit_code == 0
            assert mock_ctx.verbosity == 2

    def test_cli_with_quiet_option(self):
        """Test CLI with quiet option."""
        runner = CliRunner()

        with patch("sqlitch.cli.CliContext") as mock_ctx_class:
            mock_ctx = Mock()
            mock_ctx_class.return_value = mock_ctx

            result = runner.invoke(cli, ["-q", "-q"])

            assert result.exit_code == 0
            assert mock_ctx.verbosity == -2

    def test_cli_with_verbose_and_quiet(self):
        """Test CLI with both verbose and quiet options."""
        runner = CliRunner()

        with patch("sqlitch.cli.CliContext") as mock_ctx_class:
            mock_ctx = Mock()
            mock_ctx_class.return_value = mock_ctx

            result = runner.invoke(cli, ["-v", "-v", "-q"])

            assert result.exit_code == 0
            assert mock_ctx.verbosity == 1  # 2 verbose - 1 quiet

    def test_cli_version_option(self):
        """Test CLI version option."""
        runner = CliRunner()

        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert "sqlitch, version 1.0.0" in result.output

    def test_cli_nonexistent_config_file(self, tmp_path):
        """Test CLI with nonexistent config file."""
        nonexistent = tmp_path / "nonexistent.conf"

        runner = CliRunner()

        result = runner.invoke(cli, ["--config", str(nonexistent)])

        assert result.exit_code != 0
        assert "does not exist" in result.output


class TestErrorHandlers:
    """Test error handling functions."""

    def test_handle_sqlitch_error(self):
        """Test handling SqlitchError."""
        error = SqlitchError("Test error")
        mock_sqitch = Mock()

        with patch(
            "sqlitch.core.exceptions.handle_exception", return_value=1
        ) as mock_handle:
            result = handle_sqlitch_error(error, mock_sqitch)

        assert result == 1
        mock_handle.assert_called_once_with(error, mock_sqitch)

    def test_handle_sqlitch_error_no_sqitch(self):
        """Test handling SqlitchError without sqitch instance."""
        error = SqlitchError("Test error")

        with patch(
            "sqlitch.core.exceptions.handle_exception", return_value=1
        ) as mock_handle:
            result = handle_sqlitch_error(error, None)

        assert result == 1
        mock_handle.assert_called_once_with(error, None)

    def test_handle_keyboard_interrupt_with_sqitch(self):
        """Test handling KeyboardInterrupt with sqitch instance."""
        mock_sqitch = Mock()

        result = handle_keyboard_interrupt(mock_sqitch)

        assert result == 130
        mock_sqitch.vent.assert_called_once_with(
            "\nsqlitch: Operation cancelled by user"
        )

    def test_handle_keyboard_interrupt_without_sqitch(self):
        """Test handling KeyboardInterrupt without sqitch instance."""
        with patch("click.echo") as mock_echo:
            result = handle_keyboard_interrupt(None)

        assert result == 130
        mock_echo.assert_called_once_with(
            "\nsqlitch: Operation cancelled by user", err=True
        )

    def test_handle_unexpected_error_with_sqitch(self):
        """Test handling unexpected error with sqitch instance."""
        error = Exception("Unexpected error")
        mock_sqitch = Mock()
        mock_sqitch.verbosity = 1

        result = handle_unexpected_error(error, mock_sqitch)

        assert result == 2
        mock_sqitch.vent.assert_called_once_with(
            "sqlitch: Unexpected error: Unexpected error"
        )

    def test_handle_unexpected_error_with_sqitch_verbose(self):
        """Test handling unexpected error with verbose sqitch instance."""
        error = Exception("Unexpected error")
        mock_sqitch = Mock()
        mock_sqitch.verbosity = 2

        with patch("traceback.format_exc", return_value="Traceback..."):
            result = handle_unexpected_error(error, mock_sqitch)

        assert result == 2
        mock_sqitch.vent.assert_called_once_with(
            "sqlitch: Unexpected error: Unexpected error"
        )
        mock_sqitch.trace.assert_called_once_with("Traceback...")

    def test_handle_unexpected_error_without_sqitch(self):
        """Test handling unexpected error without sqitch instance."""
        error = Exception("Unexpected error")

        with patch("click.echo") as mock_echo:
            result = handle_unexpected_error(error, None)

        assert result == 2
        mock_echo.assert_called_once_with(
            "sqlitch: Unexpected error: Unexpected error", err=True
        )


class TestUtilityFunctions:
    """Test utility functions."""

    def test_format_command_error_basic(self):
        """Test basic command error formatting."""
        result = format_command_error("deploy", "Database connection failed")

        assert result == "sqlitch deploy: Database connection failed"

    def test_format_command_error_with_suggestion(self):
        """Test command error formatting with suggestion."""
        result = format_command_error(
            "deploy", "Database connection failed", "Check your database configuration"
        )

        expected = "sqlitch deploy: Database connection failed\nCheck your database configuration"
        assert result == expected

    def test_suggest_command_help_similar_commands(self):
        """Test command suggestion with similar commands."""
        available = ["deploy", "revert", "status", "verify"]

        result = suggest_command_help("dep", available)

        assert "deploy" in result
        assert "Did you mean one of:" in result
        assert 'Try "sqlitch help"' in result

    def test_suggest_command_help_no_similar_commands(self):
        """Test command suggestion with no similar commands."""
        available = ["deploy", "revert", "status", "verify"]

        result = suggest_command_help("xyz", available)

        assert '"xyz" is not a valid command' in result
        assert 'Try "sqlitch help"' in result

    def test_suggest_command_help_partial_match(self):
        """Test command suggestion with partial match."""
        available = ["deploy", "revert", "status", "verify"]

        result = suggest_command_help("stat", available)

        assert "status" in result
        assert "Did you mean one of:" in result

    def test_get_sqitch_from_context(self):
        """Test getting Sqitch from Click context."""
        mock_ctx = Mock()
        mock_cli_ctx = Mock()
        mock_sqitch = Mock()

        mock_ctx.obj = mock_cli_ctx
        mock_cli_ctx.create_sqitch.return_value = mock_sqitch

        result = get_sqitch_from_context(mock_ctx)

        assert result == mock_sqitch
        mock_cli_ctx.create_sqitch.assert_called_once()


class TestMainFunction:
    """Test the main entry point function."""

    def test_main_success(self):
        """Test successful main execution."""
        with patch("sqlitch.cli.cli") as mock_cli:
            result = main()

        assert result == 0
        mock_cli.assert_called_once_with(standalone_mode=False)

    def test_main_sqlitch_error(self):
        """Test main with SqlitchError."""
        error = SqlitchError("Test error")

        with patch("sqlitch.cli.cli", side_effect=error):
            with patch(
                "sqlitch.cli.handle_sqlitch_error", return_value=1
            ) as mock_handle:
                result = main()

        assert result == 1
        mock_handle.assert_called_once_with(error, None)

    def test_main_keyboard_interrupt(self):
        """Test main with KeyboardInterrupt."""
        with patch("sqlitch.cli.cli", side_effect=KeyboardInterrupt):
            with patch(
                "sqlitch.cli.handle_keyboard_interrupt", return_value=130
            ) as mock_handle:
                result = main()

        assert result == 130
        mock_handle.assert_called_once_with(None)

    def test_main_click_exception(self):
        """Test main with Click exception."""
        click_error = click.ClickException("Click error")
        click_error.exit_code = 2

        with patch("sqlitch.cli.cli", side_effect=click_error):
            result = main()

        assert result == 2

    def test_main_click_abort(self):
        """Test main with Click abort."""
        with patch("sqlitch.cli.cli", side_effect=click.Abort):
            with patch("click.echo") as mock_echo:
                result = main()

        assert result == 130
        mock_echo.assert_called_once_with(
            "\nsqlitch: Operation cancelled by user", err=True
        )

    def test_main_unexpected_error(self):
        """Test main with unexpected error."""
        error = Exception("Unexpected error")

        with patch("sqlitch.cli.cli", side_effect=error):
            with patch(
                "sqlitch.cli.handle_unexpected_error", return_value=2
            ) as mock_handle:
                result = main()

        assert result == 2
        mock_handle.assert_called_once_with(error, None)


class TestCommandWrapper:
    """Test the command wrapper functionality."""

    def test_create_command_wrapper_basic(self):
        """Test basic command wrapper creation."""
        mock_command_class = Mock()

        wrapper = create_command_wrapper(mock_command_class)

        # Test that wrapper is callable
        assert callable(wrapper)


class TestCommandRegistration:
    """Test command registration functionality."""

    def test_command_imports_handled_gracefully(self):
        """Test that import errors for commands are handled gracefully."""
        # This test verifies that the CLI module can be imported even if
        # some command modules are missing or have import errors

        # The CLI module should import successfully even with missing commands
        # This is tested by the fact that we can import and use the CLI module
        # in these tests

        assert cli is not None
        assert callable(cli)

    def test_cli_has_registered_commands(self):
        """Test that CLI has some registered commands."""
        # Check that the CLI group has commands registered
        assert len(cli.commands) > 0

        # Check for some expected commands (these should be available)
        expected_commands = ["init", "deploy", "revert", "status", "config"]

        for cmd_name in expected_commands:
            if cmd_name in cli.commands:
                assert isinstance(cli.commands[cmd_name], click.Command)


class TestCliIntegration:
    """Integration tests for CLI functionality."""

    def test_cli_context_flow(self, tmp_path):
        """Test the complete CLI context flow."""
        config_file = tmp_path / "test.conf"
        config_file.write_text("[core]\nengine = pg\n")

        # Create a CLI context
        ctx = CliContext()
        ctx.config_files = [config_file]
        ctx.verbosity = 1

        # Test that we can create a Sqitch instance
        with patch("sqlitch.cli.Config") as mock_config_class:
            with patch("sqlitch.cli.Sqitch") as mock_sqitch_class:
                mock_config = Mock()
                mock_sqitch = Mock()
                mock_config_class.return_value = mock_config
                mock_sqitch_class.return_value = mock_sqitch

                sqitch = ctx.create_sqitch()

                assert sqitch == mock_sqitch
                mock_config_class.assert_called_with([config_file])
                mock_sqitch_class.assert_called_with(
                    config=mock_config, options={"verbosity": 1}
                )

    def test_error_handling_integration(self):
        """Test integrated error handling."""
        # Test that all error handlers return appropriate exit codes

        # SqlitchError
        error = SqlitchError("Test error")
        with patch("sqlitch.core.exceptions.handle_exception", return_value=1):
            assert handle_sqlitch_error(error) == 1

        # KeyboardInterrupt
        assert handle_keyboard_interrupt() == 130

        # Unexpected error
        error = Exception("Test error")
        assert handle_unexpected_error(error) == 2

    def test_main_function_integration(self):
        """Test main function integration."""
        # Test that main function properly delegates to CLI
        with patch("sqlitch.cli.cli") as mock_cli:
            result = main()

        assert result == 0
        mock_cli.assert_called_once_with(standalone_mode=False)
