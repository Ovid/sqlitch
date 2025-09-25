"""
Tests for sqlitch exception handling system.

This module tests the custom exception hierarchy and error handling
functionality to ensure it matches Perl sqitch behavior.
"""

import sys
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

from sqlitch.core.exceptions import (
    ConfigurationError,
    ConnectionError,
    DeploymentError,
    EngineError,
    IOError,
    LockError,
    PlanError,
    SqlitchError,
    TemplateError,
    UsageError,
    UserError,
    ValidationError,
    VCSError,
    format_database_error,
    format_error_message,
    format_file_error,
    format_validation_error,
    handle_exception,
    hurl,
    suggest_help,
)


class TestSqlitchError:
    """Test base SqlitchError class."""

    def test_basic_error(self):
        """Test basic error creation."""
        error = SqlitchError("Test error")
        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert error.ident == "sqitch"
        assert error.exitval == 2

    def test_error_with_ident(self):
        """Test error with custom identifier."""
        error = SqlitchError("Test error", ident="test", exitval=1)
        assert error.ident == "test"
        assert error.exitval == 1

    def test_error_with_context(self):
        """Test error with additional context."""
        error = SqlitchError("Test error", file="test.txt", line=42)
        assert error.context["file"] == "test.txt"
        assert error.context["line"] == 42

    def test_as_string_basic(self):
        """Test as_string method for basic error."""
        error = SqlitchError("Test error")
        assert error.as_string() == "Test error"

    def test_as_string_dev_error(self):
        """Test as_string method for DEV error includes stack trace."""
        error = SqlitchError("Test error", ident="DEV")
        result = error.as_string()
        assert "Test error" in result
        # Should include stack trace for DEV errors
        assert "Traceback" in result or "test_as_string_dev_error" in result

    def test_details_string(self):
        """Test details_string method."""
        error = SqlitchError("Test error")
        assert error.details_string() == ""

        # DEV error should include stack trace
        dev_error = SqlitchError("Test error", ident="DEV")
        details = dev_error.details_string()
        assert details != ""


class TestSpecificErrors:
    """Test specific error types."""

    def test_configuration_error(self):
        """Test ConfigurationError."""
        error = ConfigurationError(
            "Invalid config", config_file="test.conf", config_key="core.engine"
        )
        assert error.ident == "config"
        assert error.config_file == "test.conf"
        assert error.config_key == "core.engine"

    def test_plan_error(self):
        """Test PlanError."""
        error = PlanError("Invalid syntax", plan_file="sqitch.plan", line_number=42)
        assert error.ident == "plan"
        assert error.plan_file == "sqitch.plan"
        assert error.line_number == 42
        assert "at sqitch.plan:42" in str(error)

    def test_plan_error_without_line(self):
        """Test PlanError without line number."""
        error = PlanError("Invalid syntax", plan_file="sqitch.plan")
        assert "in sqitch.plan" in str(error)

    def test_engine_error(self):
        """Test EngineError."""
        error = EngineError("Database error", engine_name="pg", sql_state="42P01")
        assert error.ident == "engine"
        assert error.engine_name == "pg"
        assert error.sql_state == "42P01"

    def test_connection_error(self):
        """Test ConnectionError."""
        error = ConnectionError(
            "Cannot connect", connection_string="postgresql://user@host/db"
        )
        assert error.ident == "connection"
        assert error.connection_string == "postgresql://user@host/db"

    def test_deployment_error(self):
        """Test DeploymentError."""
        error = DeploymentError(
            "SQL failed", change_name="add_users", operation="deploy"
        )
        assert error.ident == "deploy"
        assert error.change_name == "add_users"
        assert error.operation == "deploy"
        assert "during deploy of add_users" in str(error)

    def test_deployment_error_operation_only(self):
        """Test DeploymentError with operation only."""
        error = DeploymentError("SQL failed", operation="deploy")
        assert "during deploy" in str(error)

    def test_validation_error(self):
        """Test ValidationError."""
        error = ValidationError(
            "Invalid name", field_name="change", field_value="bad name"
        )
        assert error.ident == "validation"
        assert error.field_name == "change"
        assert error.field_value == "bad name"

    def test_template_error(self):
        """Test TemplateError."""
        error = TemplateError(
            "Missing variable", template_file="deploy.tmpl", template_var="project"
        )
        assert error.ident == "template"
        assert error.template_file == "deploy.tmpl"
        assert error.template_var == "project"

    def test_vcs_error(self):
        """Test VCSError."""
        error = VCSError(
            "Git failed", vcs_command="git status", repository_path="/path/to/repo"
        )
        assert error.ident == "vcs"
        assert error.vcs_command == "git status"
        assert error.repository_path == "/path/to/repo"

    def test_lock_error(self):
        """Test LockError."""
        error = LockError("Cannot acquire lock", lock_name="deploy_lock")
        assert error.ident == "lock"
        assert error.lock_name == "deploy_lock"

    def test_user_error(self):
        """Test UserError."""
        error = UserError("Cannot determine user name")
        assert error.ident == "user"

    def test_io_error(self):
        """Test IOError."""
        error = IOError("Cannot read file", file_path="/path/to/file", command="cat")
        assert error.ident == "io"
        assert error.file_path == "/path/to/file"
        assert error.command == "cat"

    def test_usage_error(self):
        """Test UsageError."""
        error = UsageError("Invalid arguments", command="deploy")
        assert error.ident == "usage"
        assert error.exitval == 1  # Usage errors have exitval 1
        assert error.command == "deploy"


class TestHurlFunction:
    """Test hurl function."""

    def test_hurl_single_argument(self):
        """Test hurl with single argument (DEV error)."""
        with pytest.raises(SqlitchError) as exc_info:
            hurl("Something went wrong")

        error = exc_info.value
        assert error.message == "Something went wrong"
        assert error.ident == "DEV"
        assert error.exitval == 2

    def test_hurl_two_arguments(self):
        """Test hurl with identifier and message."""
        with pytest.raises(SqlitchError) as exc_info:
            hurl("config", "Invalid configuration")

        error = exc_info.value
        assert error.message == "Invalid configuration"
        assert error.ident == "config"
        assert error.exitval == 2

    def test_hurl_with_exitval(self):
        """Test hurl with custom exit value."""
        with pytest.raises(SqlitchError) as exc_info:
            hurl("usage", "Invalid arguments", exitval=1)

        error = exc_info.value
        assert error.exitval == 1

    def test_hurl_with_context(self):
        """Test hurl with additional context."""
        with pytest.raises(SqlitchError) as exc_info:
            hurl("io", "Cannot read file", file="/path/to/file")

        error = exc_info.value
        assert error.context["file"] == "/path/to/file"


class TestHandleException:
    """Test exception handling function."""

    def test_handle_sqlitch_error_without_sqitch(self):
        """Test handling SqlitchError without sqitch instance."""
        error = SqlitchError("Test error", exitval=3)

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            exit_code = handle_exception(error)

        assert exit_code == 3
        assert "sqlitch: Test error" in mock_stderr.getvalue()

    def test_handle_sqlitch_error_with_sqitch(self):
        """Test handling SqlitchError with sqitch instance."""
        error = SqlitchError("Test error", exitval=2)
        mock_sqitch = MagicMock()

        exit_code = handle_exception(error, mock_sqitch)

        assert exit_code == 2
        mock_sqitch.vent.assert_called_once_with("Test error")
        mock_sqitch.trace.assert_called_once()

    def test_handle_non_fatal_error(self):
        """Test handling non-fatal error (exitval=1)."""
        error = SqlitchError("Non-fatal error", exitval=1)
        mock_sqitch = MagicMock()

        exit_code = handle_exception(error, mock_sqitch)

        assert exit_code == 1
        mock_sqitch.info.assert_called_once_with("Non-fatal error")

    def test_handle_dev_error(self):
        """Test handling DEV error."""
        error = SqlitchError("Dev error", ident="DEV")
        mock_sqitch = MagicMock()

        exit_code = handle_exception(error, mock_sqitch)

        assert exit_code == 2
        mock_sqitch.vent.assert_called_once()
        # Should call vent with full error string for DEV errors
        call_args = mock_sqitch.vent.call_args[0][0]
        assert "Dev error" in call_args

    def test_handle_unexpected_error(self):
        """Test handling unexpected (non-SqlitchError) exception."""
        error = ValueError("Unexpected error")

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            exit_code = handle_exception(error)

        assert exit_code == 2
        assert "sqlitch: unexpected error: Unexpected error" in mock_stderr.getvalue()

    def test_handle_unexpected_error_with_sqitch(self):
        """Test handling unexpected error with sqitch instance."""
        error = ValueError("Unexpected error")
        mock_sqitch = MagicMock()

        exit_code = handle_exception(error, mock_sqitch)

        assert exit_code == 2
        mock_sqitch.vent.assert_called_once_with(
            "sqlitch: unexpected error: Unexpected error"
        )


class TestUtilityFunctions:
    """Test utility functions."""

    def test_format_error_message(self):
        """Test format_error_message function."""
        result = format_error_message("config", "Invalid value")
        assert result == "sqlitch: config: Invalid value"

    def test_suggest_help(self):
        """Test suggest_help function."""
        result = suggest_help()
        assert result == "Try 'sqlitch help' for more information."

        result = suggest_help("deploy")
        assert result == "Try 'sqlitch help deploy' for more information."

    def test_format_validation_error(self):
        """Test format_validation_error function."""
        result = format_validation_error("name", "bad-name", "must not contain hyphens")
        assert result == 'Invalid name "bad-name": must not contain hyphens'

    def test_format_file_error(self):
        """Test format_file_error function."""
        result = format_file_error("read", "/path/to/file", "Permission denied")
        assert result == "Cannot read /path/to/file: Permission denied"

    def test_format_database_error(self):
        """Test format_database_error function."""
        result = format_database_error(
            "connect to", "postgresql://localhost/test", "Connection refused"
        )
        assert (
            result
            == "Cannot connect to postgresql://localhost/test: Connection refused"
        )


class TestErrorIntegration:
    """Test error handling integration."""

    def test_error_chain(self):
        """Test error chaining with previous exceptions."""
        try:
            raise ValueError("Original error")
        except ValueError as e:
            error = SqlitchError("Wrapped error", previous_exception=e)
            assert error.previous_exception is e

    def test_error_context_preservation(self):
        """Test that error context is preserved."""
        error = ConfigurationError(
            "Invalid config",
            config_file="test.conf",
            config_key="core.engine",
            additional_info="extra context",
        )

        assert error.config_file == "test.conf"
        assert error.config_key == "core.engine"
        assert error.context["additional_info"] == "extra context"

    def test_error_message_consistency(self):
        """Test that error messages are consistent with Perl sqitch."""
        # Test various error types to ensure message format consistency
        errors = [
            ConfigurationError("Invalid engine"),
            PlanError("Syntax error"),
            DeploymentError("SQL failed"),
            ConnectionError("Cannot connect"),
        ]

        for error in errors:
            # All errors should have consistent string representation
            error_str = str(error)
            assert isinstance(error_str, str)
            assert len(error_str) > 0
            # Should not include "sqlitch:" prefix in __str__ (that's added by CLI)
            assert not error_str.startswith("sqlitch:")
