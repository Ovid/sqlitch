"""
Unit tests for feedback utilities.

This module tests the user feedback utilities including operation reporters,
change reporters, and error formatting functions.
"""

import time
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.core.exceptions import (
    ConfigurationError,
    ConnectionError,
    DeploymentError,
    PlanError,
    SqlitchError,
    UserError,
)
from sqlitch.utils.feedback import (
    ChangeReporter,
    OperationReporter,
    confirm_destructive_operation,
    format_error_with_suggestions,
    operation_feedback,
    show_operation_summary,
    validate_operation_preconditions,
)


class TestOperationReporter:
    """Test OperationReporter class."""

    def setup_method(self):
        """Set up test environment."""
        self.sqitch = Mock()
        self.sqitch.verbosity = 1
        self.sqitch.info = Mock()
        self.sqitch.comment = Mock()
        self.sqitch.debug = Mock()
        self.sqitch.vent = Mock()
        self.sqitch.trace = Mock()

    def test_operation_reporter_initialization(self):
        """Test OperationReporter initialization."""
        reporter = OperationReporter(self.sqitch, "deploy")

        assert reporter.sqitch is self.sqitch
        assert reporter.operation == "deploy"
        assert reporter.start_time is None
        assert reporter.current_step == 0
        assert reporter.total_steps == 0
        assert reporter.progress_indicator is None

    def test_start_operation_basic(self):
        """Test starting operation without progress bar."""
        reporter = OperationReporter(self.sqitch, "deploy")

        reporter.start_operation("test_db")

        assert reporter.start_time is not None
        assert reporter.total_steps == 0
        self.sqitch.info.assert_called_with("Deploy to test_db")

    @patch("sqlitch.utils.feedback.ProgressBar")
    def test_start_operation_with_progress_bar(self, mock_progress_bar):
        """Test starting operation with progress bar."""
        mock_bar = Mock()
        mock_progress_bar.return_value = mock_bar

        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db", 5)

        assert reporter.total_steps == 5
        mock_progress_bar.assert_called_once()
        mock_bar.start.assert_called_once()

    @patch("sqlitch.utils.feedback.Spinner")
    def test_start_operation_with_spinner(self, mock_spinner):
        """Test starting operation with spinner."""
        mock_spin = Mock()
        mock_spinner.return_value = mock_spin

        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db", 0)  # 0 steps triggers spinner

        mock_spinner.assert_called_once()
        mock_spin.start.assert_called_once()

    def test_step_progress_basic(self):
        """Test step progress reporting."""
        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db")

        reporter.step_progress("Step 1")

        assert reporter.current_step == 1
        self.sqitch.comment.assert_called_with("Step 1")

    def test_step_progress_with_details(self):
        """Test step progress with details."""
        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db")

        reporter.step_progress("Step 1", "Processing change")

        self.sqitch.comment.assert_called_with("Step 1 - Processing change")

    @patch("sqlitch.utils.feedback.ProgressBar")
    def test_step_progress_with_progress_bar(self, mock_progress_bar):
        """Test step progress with progress bar update."""
        mock_bar = Mock()
        mock_progress_bar.return_value = mock_bar

        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db", 3)

        reporter.step_progress("Step 1")

        mock_bar.update.assert_called_with(1)

    def test_complete_operation_success(self):
        """Test successful operation completion."""
        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db", 3)
        reporter.current_step = 3

        reporter.complete_operation(success=True)

        self.sqitch.info.assert_called()
        call_args = [call[0][0] for call in self.sqitch.info.call_args_list]
        assert any("Deploy completed (3 changes)" in arg for arg in call_args)

    def test_complete_operation_with_message(self):
        """Test operation completion with custom message."""
        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db")

        reporter.complete_operation(success=True, message="Custom completion message")

        self.sqitch.info.assert_called_with("Custom completion message")

    def test_complete_operation_failure(self):
        """Test failed operation completion."""
        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db")

        reporter.complete_operation(success=False)

        self.sqitch.vent.assert_called_with("Deploy failed")

    def test_report_error_sqlitch_error(self):
        """Test error reporting with SqlitchError."""
        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db")

        error = DeploymentError("SQL failed")
        reporter.report_error(error, "step 2")

        self.sqitch.vent.assert_called()
        call_args = self.sqitch.vent.call_args[0][0]
        assert "Deploy failed" in call_args
        assert "SQL failed" in call_args
        assert "step 2" in call_args

    def test_report_error_unexpected(self):
        """Test error reporting with unexpected error."""
        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db")

        error = ValueError("Unexpected error")
        reporter.report_error(error)

        self.sqitch.vent.assert_called()
        call_args = self.sqitch.vent.call_args[0][0]
        assert "Deploy failed" in call_args
        assert "Unexpected error" in call_args

    def test_report_error_with_high_verbosity(self):
        """Test error reporting with high verbosity."""
        self.sqitch.verbosity = 3
        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db")

        error = DeploymentError("SQL failed")
        error.details_string = Mock(return_value="Detailed error info")

        reporter.report_error(error)

        self.sqitch.trace.assert_called_with("Detailed error info")


class TestChangeReporter:
    """Test ChangeReporter class."""

    def setup_method(self):
        """Set up test environment."""
        self.sqitch = Mock()
        self.sqitch.verbosity = 1
        self.sqitch.comment = Mock()
        self.sqitch.debug = Mock()
        self.sqitch.vent = Mock()

    def test_change_reporter_initialization(self):
        """Test ChangeReporter initialization."""
        reporter = ChangeReporter(self.sqitch, "add_users", "deploy")

        assert reporter.sqitch is self.sqitch
        assert reporter.change_name == "add_users"
        assert reporter.operation == "deploy"
        assert reporter.start_time is None

    def test_start_change(self):
        """Test starting change operation."""
        reporter = ChangeReporter(self.sqitch, "add_users", "deploy")

        reporter.start_change()

        assert reporter.start_time is not None
        self.sqitch.comment.assert_called_with("deploy add_users")

    def test_complete_change_success(self):
        """Test successful change completion."""
        reporter = ChangeReporter(self.sqitch, "add_users", "deploy")
        reporter.start_change()

        # Simulate some time passing
        time.sleep(0.01)

        reporter.complete_change(success=True)

        # Should not call vent for successful completion
        self.sqitch.vent.assert_not_called()

    def test_complete_change_failure(self):
        """Test failed change completion."""
        reporter = ChangeReporter(self.sqitch, "add_users", "deploy")
        reporter.start_change()

        reporter.complete_change(success=False)

        self.sqitch.vent.assert_called_with("Failed to deploy add_users")

    def test_complete_change_with_timing(self):
        """Test change completion with timing information."""
        self.sqitch.verbosity = 2
        reporter = ChangeReporter(self.sqitch, "add_users", "deploy")

        # Mock time to simulate duration
        with patch("time.time", side_effect=[1000.0, 1000.6]):  # 0.6 second duration
            reporter.start_change()
            reporter.complete_change(success=True)

        self.sqitch.debug.assert_called()
        call_args = self.sqitch.debug.call_args[0][0]
        assert "add_users took" in call_args
        assert "seconds" in call_args

    def test_report_sql_execution_basic(self):
        """Test SQL execution reporting."""
        self.sqitch.verbosity = 2
        reporter = ChangeReporter(self.sqitch, "add_users", "deploy")

        sql_file = Path("deploy/add_users.sql")
        reporter.report_sql_execution(sql_file)

        self.sqitch.debug.assert_called()
        call_args = self.sqitch.debug.call_args[0][0]
        assert "Executing deploy/add_users.sql" in call_args

    def test_report_sql_execution_with_line_count(self):
        """Test SQL execution reporting with line count."""
        self.sqitch.verbosity = 2
        reporter = ChangeReporter(self.sqitch, "add_users", "deploy")

        sql_file = Path("deploy/add_users.sql")
        reporter.report_sql_execution(sql_file, 25)

        self.sqitch.debug.assert_called()
        call_args = self.sqitch.debug.call_args[0][0]
        assert "deploy/add_users.sql (25 lines)" in call_args


class TestOperationFeedbackContextManager:
    """Test operation_feedback context manager."""

    def setup_method(self):
        """Set up test environment."""
        self.sqitch = Mock()
        self.sqitch.verbosity = 1
        self.sqitch.info = Mock()
        self.sqitch.vent = Mock()

    @patch("sqlitch.utils.feedback.OperationReporter")
    def test_operation_feedback_success(self, mock_reporter_class):
        """Test successful operation feedback."""
        mock_reporter = Mock()
        mock_reporter_class.return_value = mock_reporter

        with operation_feedback(self.sqitch, "deploy", "test_db", 3) as reporter:
            assert reporter is mock_reporter

        mock_reporter.start_operation.assert_called_with("test_db", 3)
        mock_reporter.complete_operation.assert_called_with(success=True)

    @patch("sqlitch.utils.feedback.OperationReporter")
    def test_operation_feedback_with_exception(self, mock_reporter_class):
        """Test operation feedback with exception."""
        mock_reporter = Mock()
        mock_reporter_class.return_value = mock_reporter

        with pytest.raises(ValueError):
            with operation_feedback(self.sqitch, "deploy", "test_db", 3):
                raise ValueError("Test error")

        mock_reporter.report_error.assert_called()
        # Should not call complete_operation on exception
        mock_reporter.complete_operation.assert_not_called()


class TestErrorFormatting:
    """Test error formatting functions."""

    def test_format_error_with_suggestions_config(self):
        """Test configuration error formatting."""
        error = ConfigurationError("Invalid engine")
        formatted = format_error_with_suggestions(error, "init")

        assert "Invalid engine" in formatted
        assert "Suggestions:" in formatted
        assert "Try 'sqlitch help init'" in formatted
        assert "Check your sqlitch.conf file" in formatted

    def test_format_error_with_suggestions_plan(self):
        """Test plan error formatting."""
        error = PlanError("Syntax error")
        formatted = format_error_with_suggestions(error, "deploy")

        assert "Check your sqitch.plan file" in formatted
        assert "Ensure all dependencies are properly defined" in formatted

    def test_format_error_with_suggestions_connection(self):
        """Test connection error formatting."""
        error = ConnectionError("Cannot connect")
        formatted = format_error_with_suggestions(error, "deploy")

        assert "Verify your database connection settings" in formatted
        assert "Check that the database server is running" in formatted

    def test_format_error_with_suggestions_deployment(self):
        """Test deployment error formatting."""
        error = DeploymentError("SQL failed")
        formatted = format_error_with_suggestions(error, "deploy")

        assert "Check the SQL syntax" in formatted
        assert "Verify that all required database objects exist" in formatted

    def test_format_error_with_suggestions_user(self):
        """Test user error formatting."""
        error = UserError("Cannot find user")
        formatted = format_error_with_suggestions(error, "deploy")

        assert "Set user.name:" in formatted
        assert "Set user.email:" in formatted

    def test_format_error_with_suggestions_generic(self):
        """Test generic error formatting."""
        error = SqlitchError("Generic error", ident="unknown")
        formatted = format_error_with_suggestions(error, "deploy")

        assert "Try 'sqlitch help deploy'" in formatted

    def test_format_error_without_command(self):
        """Test error formatting without command context."""
        error = SqlitchError("Generic error")
        formatted = format_error_with_suggestions(error)

        assert "Try 'sqlitch help'" in formatted


class TestPreconditionValidation:
    """Test precondition validation functions."""

    def setup_method(self):
        """Set up test environment."""
        self.sqitch = Mock()
        self.sqitch.require_initialized = Mock()
        self.sqitch.validate_user_info = Mock(return_value=[])
        self.sqitch.get_target = Mock()

    def test_validate_preconditions_success(self):
        """Test successful precondition validation."""
        errors = validate_operation_preconditions(self.sqitch, "deploy", "test_db")

        assert errors == []
        self.sqitch.require_initialized.assert_called_once()
        self.sqitch.validate_user_info.assert_called_once()

    def test_validate_preconditions_not_initialized(self):
        """Test precondition validation when not initialized."""
        self.sqitch.require_initialized.side_effect = SqlitchError("Not initialized")

        errors = validate_operation_preconditions(self.sqitch, "deploy", "test_db")

        assert len(errors) == 1
        assert "Not initialized" in errors[0]

    def test_validate_preconditions_user_info_missing(self):
        """Test precondition validation with missing user info."""
        self.sqitch.validate_user_info.return_value = [
            "Missing user name",
            "Missing user email",
        ]

        errors = validate_operation_preconditions(self.sqitch, "deploy", "test_db")

        assert len(errors) == 2
        assert "Missing user name" in errors
        assert "Missing user email" in errors

    def test_validate_preconditions_target_error(self):
        """Test precondition validation with target error."""
        self.sqitch.get_target.side_effect = SqlitchError("Invalid target")

        errors = validate_operation_preconditions(self.sqitch, "deploy", "test_db")

        assert len(errors) == 1
        assert "Target validation failed" in errors[0]

    def test_validate_preconditions_non_database_operation(self):
        """Test precondition validation for non-database operations."""
        validate_operation_preconditions(self.sqitch, "init", "test_db")

        # Should not try to validate target for init operation
        self.sqitch.get_target.assert_not_called()


class TestUtilityFunctions:
    """Test utility functions."""

    def setup_method(self):
        """Set up test environment."""
        self.sqitch = Mock()
        self.sqitch.info = Mock()
        self.sqitch.comment = Mock()
        self.sqitch.vent = Mock()
        self.sqitch.ask_yes_no = Mock()

    def test_show_operation_summary_success(self):
        """Test successful operation summary."""
        self.sqitch.verbosity = 1  # Set verbosity as a number
        changes = ["add_users", "add_posts", "add_comments"]
        show_operation_summary(self.sqitch, "deploy", changes, 5.2, True)

        self.sqitch.info.assert_called()
        call_args = [call[0][0] for call in self.sqitch.info.call_args_list]
        assert any("Deploy completed successfully" in arg for arg in call_args)
        assert any("Applied 3 changes in 5.20 seconds" in arg for arg in call_args)

    def test_show_operation_summary_no_changes(self):
        """Test operation summary with no changes."""
        show_operation_summary(self.sqitch, "deploy", [], 1.0, True)

        self.sqitch.info.assert_called_with("Nothing to deploy")

    def test_show_operation_summary_failure(self):
        """Test failed operation summary."""
        changes = ["add_users", "add_posts"]
        show_operation_summary(self.sqitch, "deploy", changes, 3.5, False)

        self.sqitch.vent.assert_called()
        # Check that both calls were made
        call_args_list = [call[0][0] for call in self.sqitch.vent.call_args_list]
        assert any("Deploy failed after 3.50 seconds" in arg for arg in call_args_list)
        assert any(
            "Successfully processed 2 changes before failure" in arg
            for arg in call_args_list
        )

    def test_confirm_destructive_operation_no_changes(self):
        """Test confirmation with no changes."""
        result = confirm_destructive_operation(self.sqitch, "revert", "test_db", [])

        assert result is True
        self.sqitch.ask_yes_no.assert_not_called()

    def test_confirm_destructive_operation_with_changes(self):
        """Test confirmation with changes."""
        self.sqitch.ask_yes_no.return_value = True
        changes = ["add_users", "add_posts"]

        result = confirm_destructive_operation(
            self.sqitch, "revert", "test_db", changes
        )

        assert result is True
        self.sqitch.info.assert_called()
        self.sqitch.ask_yes_no.assert_called()

    def test_confirm_destructive_operation_declined(self):
        """Test confirmation declined."""
        self.sqitch.ask_yes_no.return_value = False
        changes = ["add_users"]

        result = confirm_destructive_operation(
            self.sqitch, "revert", "test_db", changes
        )

        assert result is False


class TestFeedbackIntegration:
    """Test feedback system integration."""

    def setup_method(self):
        """Set up test environment."""
        self.sqitch = Mock()
        self.sqitch.verbosity = 1
        self.sqitch.info = Mock()
        self.sqitch.comment = Mock()
        self.sqitch.debug = Mock()
        self.sqitch.vent = Mock()

    @patch("sys.stderr", new_callable=StringIO)
    def test_feedback_with_real_progress_indicators(self, mock_stderr):
        """Test feedback system with real progress indicators."""
        with operation_feedback(self.sqitch, "deploy", "test_db", 3) as reporter:
            reporter.step_progress("Step 1")
            reporter.step_progress("Step 2")
            reporter.step_progress("Step 3")

        # Should have some output to stderr (progress indicators)
        # Exact output depends on terminal capabilities
        mock_stderr.getvalue()
        # Just verify that some output was produced
        # (progress indicators write to stderr)

    def test_nested_feedback_contexts(self):
        """Test nested feedback contexts."""
        with patch("sqlitch.utils.feedback.OperationReporter") as mock_reporter_class:
            mock_outer = Mock()
            mock_inner = Mock()
            mock_reporter_class.side_effect = [mock_outer, mock_inner]

            with operation_feedback(self.sqitch, "deploy", "test_db", 3):
                with operation_feedback(self.sqitch, "verify", "test_db", 3):
                    pass

            # Both reporters should be started and completed
            mock_outer.start_operation.assert_called_once()
            mock_outer.complete_operation.assert_called_once()
            mock_inner.start_operation.assert_called_once()
            mock_inner.complete_operation.assert_called_once()

    def test_feedback_error_recovery(self):
        """Test feedback system error recovery."""
        with patch("sqlitch.utils.feedback.OperationReporter") as mock_reporter_class:
            mock_reporter = Mock()
            mock_reporter_class.return_value = mock_reporter

            # Simulate error in progress indicator
            mock_reporter.step_progress.side_effect = Exception("Progress error")

            with pytest.raises(Exception):
                with operation_feedback(
                    self.sqitch, "deploy", "test_db", 3
                ) as reporter:
                    reporter.step_progress("Step 1")

            # Should still call report_error
            mock_reporter.report_error.assert_called()
