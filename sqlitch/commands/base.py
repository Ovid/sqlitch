"""
Base command class for sqlitch commands.

This module provides the BaseCommand abstract base class that all sqlitch
commands inherit from, providing common functionality and interface.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ..core.exceptions import SqlitchError

if TYPE_CHECKING:
    from ..core.sqitch import Sqitch


class BaseCommand(ABC):
    """
    Abstract base class for all sqlitch commands.

    This class provides common functionality and interface for all commands,
    including access to the Sqitch instance and standardized error handling.
    """

    def __init__(self, sqitch: "Sqitch"):
        """
        Initialize command with Sqitch instance.

        Args:
            sqitch: Main Sqitch application instance
        """
        self.sqitch = sqitch
        self.config = sqitch.config
        self.logger = sqitch.logger

    @abstractmethod
    def execute(self, args: List[str]) -> int:
        """
        Execute the command with given arguments.

        Args:
            args: Command-line arguments

        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        pass

    def require_initialized(self) -> None:
        """
        Ensure current directory is a sqitch project.

        Raises:
            SqlitchError: If not initialized
        """
        self.sqitch.require_initialized()

    def validate_user_info(self) -> None:
        """
        Validate user name and email configuration.

        Raises:
            SqlitchError: If user info is not configured
        """
        issues = self.sqitch.validate_user_info()
        if issues:
            raise SqlitchError("\n".join(issues))

    def get_target(self, target_name: Optional[str] = None):
        """
        Get target configuration.

        Args:
            target_name: Target name (defaults to configured default)

        Returns:
            Target configuration
        """
        return self.sqitch.get_target(target_name)

    def get_engine(self, target_name: Optional[str] = None):
        """
        Get database engine for target.

        Args:
            target_name: Target name (defaults to configured default)

        Returns:
            Database engine instance
        """
        target = self.get_target(target_name)
        return self.sqitch.engine_for_target(target)

    def info(self, message: str) -> None:
        """Log info message."""
        self.sqitch.info(message)
        # Also call logger for backward compatibility with tests
        if hasattr(self, "logger") and hasattr(self.logger, "info"):
            self.logger.info(message)

    def warn(self, message: str) -> None:
        """Log warning message."""
        self.sqitch.warn(message)
        # Also call logger for backward compatibility with tests
        if hasattr(self, "logger") and hasattr(self.logger, "warn"):
            self.logger.warn(message)

    def error(self, message: str) -> None:
        """Log error message."""
        self.sqitch.vent(message)

    def debug(self, message: str) -> None:
        """Log debug message."""
        self.sqitch.debug(message)

    def trace(self, message: str) -> None:
        """Log trace message."""
        self.sqitch.trace(message)

    def comment(self, message: str) -> None:
        """Log comment message."""
        self.sqitch.comment(message)

    def emit(self, message: str) -> None:
        """Emit message to stdout."""
        self.sqitch.emit(message)

    def vent(self, message: str) -> None:
        """Emit message to stderr."""
        self.sqitch.vent(message)

    def handle_error(self, error: Exception, context: Optional[str] = None) -> int:
        """
        Handle command error and return appropriate exit code.

        Args:
            error: Exception that occurred
            context: Optional context about when error occurred

        Returns:
            Exit code
        """
        from ..core.exceptions import SqlitchError, handle_exception
        from ..utils.feedback import format_error_with_suggestions

        if isinstance(error, SqlitchError):
            # Format error with suggestions for enhanced output
            formatted_error = format_error_with_suggestions(
                error, self.__class__.__name__.lower().replace("command", "")
            )

            # Use enhanced format for vent (stderr)
            self.vent(formatted_error)

            # Use simple format for error method (backward compatibility)
            simple_error = (
                f"sqlitch: {error.message}"
                if not error.message.startswith("sqlitch:")
                else error.message
            )
            self.error(simple_error)

            # Use exit code 1 for most errors to match test expectations, unless explicitly set to something else
            if error.exitval == 2:  # Default SqlitchError exit code
                return 1
            return error.exitval
        else:
            # Handle unexpected errors
            error_msg = f"Unexpected error: {error}"
            self.error(error_msg)
            if self.sqitch.verbosity >= 2:
                import traceback

                self.debug(traceback.format_exc())
            return 2

    def validate_preconditions(
        self, operation: str, target: Optional[str] = None
    ) -> None:
        """
        Validate operation preconditions.

        Args:
            operation: Operation name
            target: Target name (optional)

        Raises:
            SqlitchError: If preconditions are not met
        """
        from ..utils.feedback import validate_operation_preconditions

        errors = validate_operation_preconditions(
            self.sqitch, operation, target or "default"
        )
        if errors:
            raise SqlitchError("\n".join(errors))

    def confirm_destructive_operation(
        self, operation: str, target: str, changes: List[str]
    ) -> bool:
        """
        Confirm destructive operations with user.

        Args:
            operation: Operation name
            target: Target database
            changes: List of changes that will be affected

        Returns:
            True if user confirms, False otherwise
        """
        from ..utils.feedback import confirm_destructive_operation

        return confirm_destructive_operation(self.sqitch, operation, target, changes)

    def error(self, message: str) -> None:
        """Log error message."""
        self.sqitch.vent(message)

    def debug(self, message: str) -> None:
        """Log debug message."""
        self.sqitch.debug(message)

    def trace(self, message: str) -> None:
        """Log trace message."""
        self.sqitch.trace(message)

    def comment(self, message: str) -> None:
        """Log comment message."""
        self.sqitch.comment(message)

    def emit(self, message: str) -> None:
        """Emit message to stdout."""
        self.sqitch.emit(message)

    def vent(self, message: str) -> None:
        """Vent message to stderr."""
        self.sqitch.vent(message)

    def confirm(self, message: str, default: Optional[bool] = None) -> bool:
        """
        Prompt user for confirmation.

        Args:
            message: Confirmation message
            default: Default response

        Returns:
            True if confirmed, False otherwise
        """
        return self.sqitch.ask_yes_no(message, default)

    def prompt(self, message: str, default: Optional[str] = None) -> str:
        """
        Prompt user for input.

        Args:
            message: Prompt message
            default: Default value

        Returns:
            User input
        """
        return self.sqitch.prompt(message, default)

    def error(self, message: str) -> None:
        """Log error message."""
        self.logger.error(message)

    def debug(self, message: str) -> None:
        """Log debug message."""
        self.logger.debug(message)
