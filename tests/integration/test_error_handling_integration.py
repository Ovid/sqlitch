"""
Integration tests for error handling and user feedback.

This module tests the comprehensive error handling system including
progress indicators, error formatting, and user feedback mechanisms.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from io import StringIO
import sys

from sqlitch.core.sqitch import Sqitch
from sqlitch.core.config import Config
from sqlitch.core.exceptions import (
    SqlitchError, ConfigurationError, PlanError, DeploymentError,
    ConnectionError, ValidationError, hurl
)
from sqlitch.utils.feedback import (
    OperationReporter, ChangeReporter, operation_feedback,
    format_error_with_suggestions, validate_operation_preconditions
)
from sqlitch.commands.deploy import DeployCommand


class TestErrorHandlingIntegration:
    """Test error handling integration across the system."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.config = Config()
        self.sqitch = Sqitch(config=self.config, options={'verbosity': 1})
    
    def teardown_method(self):
        """Clean up test environment."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_configuration_error_handling(self):
        """Test configuration error handling with suggestions."""
        error = ConfigurationError("Invalid engine type", config_key="core.engine")
        
        formatted = format_error_with_suggestions(error, "init")
        
        assert "Invalid engine type" in formatted
        assert "Try 'sqlitch help init'" in formatted
        assert "Check your sqlitch.conf file" in formatted
    
    def test_plan_error_handling(self):
        """Test plan error handling with line numbers."""
        error = PlanError("Syntax error in plan", plan_file="sqitch.plan", line_number=5)
        
        formatted = format_error_with_suggestions(error, "deploy")
        
        assert "Syntax error in plan at sqitch.plan:5" in str(error)
        assert "Check your sqitch.plan file" in formatted
    
    def test_deployment_error_handling(self):
        """Test deployment error handling with context."""
        error = DeploymentError(
            "SQL syntax error",
            change_name="add_users",
            operation="deploy",
            sql_file="deploy/add_users.sql"
        )
        
        formatted = format_error_with_suggestions(error, "deploy")
        
        assert "SQL syntax error during deploy of add_users" in str(error)
        assert "Check the SQL syntax" in formatted
    
    def test_connection_error_handling(self):
        """Test connection error handling with suggestions."""
        error = ConnectionError("Connection refused", connection_string="postgresql://user@localhost/db")
        
        formatted = format_error_with_suggestions(error, "deploy")
        
        assert "Connection refused" in str(error)
        assert "Verify your database connection settings" in formatted
        assert "Check that the database server is running" in formatted
    
    def test_user_error_handling(self):
        """Test user configuration error handling."""
        from sqlitch.core.exceptions import UserError
        error = UserError("Cannot find user name")
        
        formatted = format_error_with_suggestions(error, "deploy")
        
        assert "Set user.name:" in formatted
        assert "Set user.email:" in formatted
    
    def test_hurl_function_compatibility(self):
        """Test hurl function matches Perl sqitch behavior."""
        # Single argument (DEV error)
        with pytest.raises(SqlitchError) as exc_info:
            hurl("This is a development error")
        
        assert exc_info.value.ident == "DEV"
        assert exc_info.value.exitval == 2
        
        # Two argument form
        with pytest.raises(SqlitchError) as exc_info:
            hurl("config", "Invalid configuration")
        
        assert exc_info.value.ident == "config"
        assert exc_info.value.message == "Invalid configuration"
    
    def test_error_chain_preservation(self):
        """Test that error chains are preserved."""
        original_error = ValueError("Original error")
        
        try:
            raise original_error
        except ValueError as e:
            sqitch_error = SqlitchError("Wrapped error", previous_exception=e)
        
        assert sqitch_error.previous_exception is original_error
        assert "Original error" in sqitch_error.as_string()


class TestOperationReporter:
    """Test operation reporter functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.sqitch = Mock()
        self.sqitch.verbosity = 1
        self.sqitch.info = Mock()
        self.sqitch.comment = Mock()
        self.sqitch.debug = Mock()
        self.sqitch.vent = Mock()
        self.sqitch.trace = Mock()
    
    def test_operation_reporter_basic(self):
        """Test basic operation reporter functionality."""
        reporter = OperationReporter(self.sqitch, "deploy")
        
        reporter.start_operation("test_db", 3)
        assert self.sqitch.info.called
        
        reporter.step_progress("Step 1")
        assert self.sqitch.comment.called
        
        reporter.complete_operation(success=True)
        assert self.sqitch.info.call_count >= 2
    
    def test_operation_reporter_with_progress_bar(self):
        """Test operation reporter with progress bar."""
        with patch('sqlitch.utils.feedback.ProgressBar') as mock_bar:
            mock_progress = Mock()
            mock_bar.return_value = mock_progress
            
            reporter = OperationReporter(self.sqitch, "deploy")
            reporter.start_operation("test_db", 5)
            
            mock_bar.assert_called_once()
            mock_progress.start.assert_called_once()
            
            reporter.step_progress("Step 1")
            mock_progress.update.assert_called_with(1)
            
            reporter.complete_operation()
            mock_progress.stop.assert_called_once()
    
    def test_operation_reporter_error_handling(self):
        """Test operation reporter error handling."""
        reporter = OperationReporter(self.sqitch, "deploy")
        reporter.start_operation("test_db", 3)
        
        error = DeploymentError("SQL failed")
        reporter.report_error(error, "step 2")
        
        self.sqitch.vent.assert_called()
        call_args = self.sqitch.vent.call_args[0][0]
        assert "Deploy failed" in call_args
        assert "SQL failed" in call_args
    
    def test_operation_feedback_context_manager(self):
        """Test operation feedback context manager."""
        with patch('sqlitch.utils.feedback.OperationReporter') as mock_reporter:
            mock_instance = Mock()
            mock_reporter.return_value = mock_instance
            
            with operation_feedback(self.sqitch, "deploy", "test_db", 3) as reporter:
                assert reporter is mock_instance
                mock_instance.start_operation.assert_called_with("test_db", 3)
            
            mock_instance.complete_operation.assert_called_with(success=True)
    
    def test_operation_feedback_with_exception(self):
        """Test operation feedback context manager with exception."""
        with patch('sqlitch.utils.feedback.OperationReporter') as mock_reporter:
            mock_instance = Mock()
            mock_reporter.return_value = mock_instance
            
            with pytest.raises(ValueError):
                with operation_feedback(self.sqitch, "deploy", "test_db", 3):
                    raise ValueError("Test error")
            
            mock_instance.report_error.assert_called()


class TestChangeReporter:
    """Test change reporter functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.sqitch = Mock()
        self.sqitch.verbosity = 1
        self.sqitch.comment = Mock()
        self.sqitch.debug = Mock()
        self.sqitch.vent = Mock()
    
    def test_change_reporter_basic(self):
        """Test basic change reporter functionality."""
        reporter = ChangeReporter(self.sqitch, "add_users", "deploy")
        
        reporter.start_change()
        self.sqitch.comment.assert_called_with("deploy add_users")
        
        reporter.complete_change(success=True)
        # Should not call vent for successful completion
        self.sqitch.vent.assert_not_called()
    
    def test_change_reporter_failure(self):
        """Test change reporter failure handling."""
        reporter = ChangeReporter(self.sqitch, "add_users", "deploy")
        
        reporter.start_change()
        reporter.complete_change(success=False)
        
        self.sqitch.vent.assert_called_with("Failed to deploy add_users")
    
    def test_change_reporter_sql_execution(self):
        """Test change reporter SQL execution reporting."""
        reporter = ChangeReporter(self.sqitch, "add_users", "deploy")
        
        sql_file = Path("deploy/add_users.sql")
        reporter.report_sql_execution(sql_file, 25)
        
        if self.sqitch.verbosity >= 2:
            self.sqitch.debug.assert_called()


class TestPreconditionValidation:
    """Test operation precondition validation."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.original_cwd = Path.cwd()
        
        # Create a mock sqitch instance
        self.sqitch = Mock()
        self.sqitch.require_initialized = Mock()
        self.sqitch.validate_user_info = Mock(return_value=[])
        self.sqitch.get_target = Mock()
    
    def teardown_method(self):
        """Clean up test environment."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
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
        self.sqitch.validate_user_info.return_value = ["Missing user name", "Missing user email"]
        
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


class TestCommandErrorHandling:
    """Test command-level error handling."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.config = Config()
        self.sqitch = Sqitch(config=self.config, options={'verbosity': 1})
    
    def teardown_method(self):
        """Clean up test environment."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_deploy_command_error_handling(self):
        """Test deploy command error handling."""
        command = DeployCommand(self.sqitch)
        
        # Mock the validation to fail
        with patch.object(command, 'validate_preconditions') as mock_validate:
            mock_validate.side_effect = SqlitchError("Validation failed")
            
            exit_code = command.execute([])
            
            assert exit_code == 1  # Enhanced error handling returns 1 for most cases
    
    def test_command_handle_error_method(self):
        """Test base command handle_error method."""
        from sqlitch.commands.base import BaseCommand
        
        class TestCommand(BaseCommand):
            def execute(self, args):
                return 0
        
        command = TestCommand(self.sqitch)
        
        # Test SqlitchError handling
        error = ConfigurationError("Test error")
        exit_code = command.handle_error(error)
        assert exit_code == 1  # Enhanced error handling returns 1 for most cases
        
        # Test unexpected error handling
        error = ValueError("Unexpected error")
        exit_code = command.handle_error(error)
        assert exit_code == 2


class TestErrorMessageFormatting:
    """Test error message formatting and suggestions."""
    
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
        from sqlitch.core.exceptions import UserError
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


class TestProgressIntegration:
    """Test progress indicator integration."""
    
    def setup_method(self):
        """Set up test environment."""
        self.sqitch = Mock()
        self.sqitch.verbosity = 1
        self.sqitch.info = Mock()
        self.sqitch.comment = Mock()
        self.sqitch.debug = Mock()
    
    def test_progress_with_operation_feedback(self):
        """Test progress indicators with operation feedback."""
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            with operation_feedback(self.sqitch, "deploy", "test_db", 3) as reporter:
                reporter.step_progress("Step 1")
                reporter.step_progress("Step 2")
                reporter.step_progress("Step 3")
            
            # Should have progress output
            output = mock_stderr.getvalue()
            # Progress bar or spinner should produce some output
            # (exact output depends on terminal capabilities)
    
    def test_progress_cancellation(self):
        """Test progress indicator cancellation."""
        with patch('sys.stderr', new_callable=StringIO):
            reporter = OperationReporter(self.sqitch, "deploy")
            reporter.start_operation("test_db", 3)
            
            # Simulate cancellation
            reporter.report_error(KeyboardInterrupt(), "step 2")
            
            self.sqitch.vent.assert_called()