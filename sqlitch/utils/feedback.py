"""
User feedback utilities for sqlitch operations.

This module provides enhanced user feedback mechanisms including operation
status reporting, progress tracking, and error presentation that matches
the Perl sqitch behavior.
"""

import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Optional

from ..core.exceptions import SqlitchError
from .progress import ProgressBar, ProgressIndicator, Spinner


class OperationReporter:
    """
    Enhanced operation reporter for sqlitch commands.

    Provides comprehensive feedback during database operations including
    progress indicators, status messages, and error reporting.
    """

    def __init__(self, sqitch, operation: str = "operation"):
        """
        Initialize operation reporter.

        Args:
            sqitch: Sqitch instance for logging
            operation: Name of the operation being performed
        """
        self.sqitch = sqitch
        self.operation = operation
        self.start_time = None
        self.current_step = 0
        self.total_steps = 0
        self.progress_indicator: Optional[ProgressIndicator] = None

    def start_operation(self, target: str, total_steps: int = 0) -> None:
        """
        Start operation reporting.

        Args:
            target: Target database or description
            total_steps: Total number of steps (0 for unknown)
        """
        self.start_time = time.time()
        self.total_steps = total_steps
        self.current_step = 0

        # Report operation start
        self.sqitch.info(f"{self.operation.capitalize()} to {target}")

        # Start progress indicator for long operations
        if total_steps > 1:
            self.progress_indicator = ProgressBar(
                total=total_steps,
                message=f"{self.operation.capitalize()}",
                file=sys.stderr,
            )
            self.progress_indicator.start()
        elif self.sqitch.verbosity >= 1:
            self.progress_indicator = Spinner(
                message=f"{self.operation.capitalize()}", file=sys.stderr
            )
            self.progress_indicator.start()

    def step_progress(self, step_name: str, details: Optional[str] = None) -> None:
        """
        Report progress on a step.

        Args:
            step_name: Name of the current step
            details: Optional details about the step
        """
        self.current_step += 1

        # Update progress indicator
        if self.progress_indicator and hasattr(self.progress_indicator, "update"):
            self.progress_indicator.update(self.current_step)

        # Report step details
        if self.sqitch.verbosity >= 1:
            if details:
                self.sqitch.comment(f"{step_name} - {details}")
            else:
                self.sqitch.comment(step_name)

    def complete_operation(
        self, success: bool = True, message: Optional[str] = None
    ) -> None:
        """
        Complete operation reporting.

        Args:
            success: Whether operation was successful
            message: Optional completion message
        """
        # Stop progress indicator
        if self.progress_indicator:
            self.progress_indicator.stop()
            self.progress_indicator = None

        # Calculate duration
        duration = time.time() - self.start_time if self.start_time else 0

        # Report completion
        if success:
            if message:
                self.sqitch.info(message)
            elif self.total_steps > 0:
                self.sqitch.info(
                    f"{self.operation.capitalize()} completed ({self.current_step} changes)"
                )
            else:
                self.sqitch.info(f"{self.operation.capitalize()} completed")

            if self.sqitch.verbosity >= 2 and duration > 1:
                self.sqitch.debug(f"Operation took {duration:.2f} seconds")
        else:
            self.sqitch.vent(f"{self.operation.capitalize()} failed")

    def report_error(self, error: Exception, context: Optional[str] = None) -> None:
        """
        Report operation error.

        Args:
            error: Exception that occurred
            context: Optional context about when error occurred
        """
        # Stop progress indicator
        if self.progress_indicator:
            self.progress_indicator.stop()
            self.progress_indicator = None

        # Format error message
        if isinstance(error, SqlitchError):
            error_msg = str(error)
        else:
            error_msg = f"Unexpected error: {error}"

        if context:
            error_msg = f"{error_msg} (during {context})"

        self.sqitch.vent(f"{self.operation.capitalize()} failed: {error_msg}")

        # Show additional details for high verbosity
        if self.sqitch.verbosity >= 2:
            if isinstance(error, SqlitchError) and hasattr(error, "details_string"):
                details = error.details_string()
                if details:
                    self.sqitch.trace(details)
            else:
                import traceback

                self.sqitch.trace(traceback.format_exc())


@contextmanager
def operation_feedback(
    sqitch, operation: str, target: str, total_steps: int = 0
) -> Iterator[OperationReporter]:
    """
    Context manager for operation feedback.

    Args:
        sqitch: Sqitch instance
        operation: Operation name
        target: Target description
        total_steps: Total number of steps

    Yields:
        OperationReporter instance

    Example:
        with operation_feedback(sqitch, "deploy", "mydb", 5) as reporter:
            for change in changes:
                reporter.step_progress(f"Deploying {change.name}")
                deploy_change(change)
    """
    reporter = OperationReporter(sqitch, operation)
    try:
        reporter.start_operation(target, total_steps)
        yield reporter
        reporter.complete_operation(success=True)
    except Exception as e:
        reporter.report_error(e)
        raise


class ChangeReporter:
    """
    Reporter for individual change operations.

    Provides detailed feedback for deploy, revert, and verify operations
    on individual database changes.
    """

    def __init__(self, sqitch, change_name: str, operation: str):
        """
        Initialize change reporter.

        Args:
            sqitch: Sqitch instance
            change_name: Name of the change
            operation: Operation being performed
        """
        self.sqitch = sqitch
        self.change_name = change_name
        self.operation = operation
        self.start_time = None

    def start_change(self) -> None:
        """Start change operation."""
        self.start_time = time.time()
        if self.sqitch.verbosity >= 1:
            self.sqitch.comment(f"{self.operation} {self.change_name}")

    def complete_change(self, success: bool = True) -> None:
        """
        Complete change operation.

        Args:
            success: Whether operation was successful
        """
        duration = time.time() - self.start_time if self.start_time else 0

        if success:
            if (
                hasattr(self.sqitch, "verbosity")
                and self.sqitch.verbosity >= 2
                and duration > 0.5
            ):
                self.sqitch.debug(f"{self.change_name} took {duration:.2f} seconds")
        else:
            self.sqitch.vent(f"Failed to {self.operation} {self.change_name}")

    def report_sql_execution(self, sql_file: Path, line_count: int = 0) -> None:
        """
        Report SQL file execution.

        Args:
            sql_file: Path to SQL file being executed
            line_count: Number of lines in the file
        """
        if hasattr(self.sqitch, "verbosity") and self.sqitch.verbosity >= 2:
            if line_count > 0:
                self.sqitch.debug(f"Executing {sql_file} ({line_count} lines)")
            else:
                self.sqitch.debug(f"Executing {sql_file}")


def format_error_with_suggestions(  # noqa: C901
    error: SqlitchError, command: Optional[str] = None
) -> str:
    """
    Format error message with helpful suggestions.

    Args:
        error: SqlitchError to format
        command: Command that failed (optional)

    Returns:
        Formatted error message with suggestions
    """
    message = str(error)
    suggestions = []

    # Add command-specific suggestions
    if command:
        if error.ident == "config":
            suggestions.append(
                f"Try 'sqlitch help {command}' for configuration options"
            )
            suggestions.append("Check your sqlitch.conf file for syntax errors")
        elif error.ident == "plan":
            suggestions.append("Check your sqitch.plan file for syntax errors")
            suggestions.append("Ensure all dependencies are properly defined")
        elif error.ident == "connection":
            suggestions.append("Verify your database connection settings")
            suggestions.append("Check that the database server is running")
        elif error.ident == "deploy":
            suggestions.append("Check the SQL syntax in your change files")
            suggestions.append("Verify that all required database objects exist")
        elif error.ident == "user":
            suggestions.append(
                "Set user.name: sqlitch config --user user.name 'Your Name'"
            )
            suggestions.append(
                "Set user.email: sqlitch config --user user.email 'you@example.com'"
            )

    # Add general suggestions
    if not suggestions:
        if command:
            suggestions.append(f"Try 'sqlitch help {command}' for more information")
        else:
            suggestions.append("Try 'sqlitch help' for more information")

    # Format final message
    if suggestions:
        message += "\n\nSuggestions:"
        for suggestion in suggestions:
            message += f"\n  • {suggestion}"

    return message


def show_operation_summary(
    sqitch, operation: str, changes: List[str], duration: float, success: bool = True
) -> None:
    """
    Show summary of operation results.

    Args:
        sqitch: Sqitch instance
        operation: Operation name
        changes: List of change names processed
        duration: Operation duration in seconds
        success: Whether operation was successful
    """
    if success:
        if changes:
            sqitch.info(f"{operation.capitalize()} completed successfully")
            sqitch.info(f"Applied {len(changes)} changes in {duration:.2f} seconds")

            if hasattr(sqitch, "verbosity") and sqitch.verbosity >= 1:
                sqitch.comment("Changes applied:")
                for change in changes:
                    sqitch.comment(f"  + {change}")
        else:
            sqitch.info(f"Nothing to {operation}")
    else:
        sqitch.vent(f"{operation.capitalize()} failed after {duration:.2f} seconds")
        if changes:
            sqitch.vent(f"Successfully processed {len(changes)} changes before failure")


def confirm_destructive_operation(
    sqitch, operation: str, target: str, changes: List[str]
) -> bool:
    """
    Confirm destructive operations with user.

    Args:
        sqitch: Sqitch instance
        operation: Operation name (e.g., "revert")
        target: Target database
        changes: List of changes that will be affected

    Returns:
        True if user confirms, False otherwise
    """
    if not changes:
        return True

    # Show what will be affected
    sqitch.info(f"The following changes will be {operation}ed from {target}:")
    for change in changes:
        sqitch.info(f"  - {change}")

    # Ask for confirmation
    return sqitch.ask_yes_no(
        f"Are you sure you want to {operation} these changes?", False
    )


def validate_operation_preconditions(sqitch, operation: str, target: str) -> List[str]:
    """
    Validate preconditions for operations.

    Args:
        sqitch: Sqitch instance
        operation: Operation name
        target: Target database

    Returns:
        List of validation error messages
    """
    errors = []

    # Check if project is initialized
    try:
        sqitch.require_initialized()
    except SqlitchError as e:
        errors.append(str(e))

    # Check user configuration
    user_errors = sqitch.validate_user_info()
    errors.extend(user_errors)

    # Operation-specific validations
    if operation in ["deploy", "revert", "verify"]:
        # Check if target is accessible
        try:
            sqitch.get_target(target)
            # Basic validation - more detailed checks happen in engine
        except SqlitchError as e:
            errors.append(f"Target validation failed: {e}")

    return errors
