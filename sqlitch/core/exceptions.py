"""
Custom exception hierarchy for sqitch.

This module defines all custom exceptions used throughout the sqitch application,
providing clear error categorization and consistent error handling that matches
the Perl sqitch error format and behavior.
"""

import sys
import traceback
from typing import Any, Optional


class SqlitchError(Exception):
    """
    Base exception for all sqitch errors.

    This is the root exception class that all other sqitch-specific
    exceptions inherit from. It provides consistent error formatting
    and optional error codes matching Perl sqitch behavior.
    """

    def __init__(
        self, message: str, ident: str = "sqitch", exitval: int = 2, **kwargs: Any
    ) -> None:
        """
        Initialize sqitch error.

        Args:
            message: Human-readable error message
            ident: Error identifier (matches Perl sqitch ident system)
            exitval: Exit value to use when this error causes program termination
            **kwargs: Additional error context
        """
        super().__init__(message)
        self.message = message
        self.ident = ident
        self.exitval = exitval
        self.context = kwargs
        self.previous_exception = kwargs.get("previous_exception")

    def __str__(self) -> str:
        """Format error message for display."""
        return self.message

    def as_string(self) -> str:
        """
        Return full string representation including stack trace.

        Returns:
            Complete error string with message, previous exception, and stack trace
        """
        parts = [self.message]

        if self.previous_exception:
            parts.append(str(self.previous_exception))

        # Add stack trace for DEV errors or when verbosity is high
        if self.ident == "DEV":
            # Get current stack trace
            stack_trace = "".join(
                traceback.format_stack()[:-1]
            )  # Exclude current frame
            if stack_trace.strip():
                parts.append(stack_trace)

        return "\n".join(filter(None, parts))

    def details_string(self) -> str:
        """
        Return details string (previous exception and stack trace).

        Returns:
            Details without the main message
        """
        parts = []

        if self.previous_exception:
            parts.append(str(self.previous_exception))

        if self.ident == "DEV":
            stack_trace = "".join(
                traceback.format_stack()[:-1]
            )  # Exclude current frame
            if stack_trace.strip():
                parts.append(stack_trace)

        return "\n".join(filter(None, parts))


class ConfigurationError(SqlitchError):
    """
    Configuration-related errors.

    Raised when there are issues with configuration file parsing,
    invalid configuration values, or missing required configuration.
    """

    def __init__(
        self,
        message: str,
        config_file: Optional[str] = None,
        config_key: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize configuration error.

        Args:
            message: Error description
            config_file: Path to problematic config file
            config_key: Specific configuration key that caused the error
            **kwargs: Additional context
        """
        super().__init__(message, ident="config", exitval=2, **kwargs)
        self.config_file = config_file
        self.config_key = config_key


class PlanError(SqlitchError):
    """
    Plan file parsing or validation errors.

    Raised when there are issues with plan file syntax, invalid change
    definitions, dependency conflicts, or other plan-related problems.
    """

    def __init__(
        self,
        message: str,
        plan_file: Optional[str] = None,
        line_number: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize plan error.

        Args:
            message: Error description
            plan_file: Path to problematic plan file
            line_number: Line number where error occurred
            **kwargs: Additional context
        """
        super().__init__(message, ident="plan", exitval=2, **kwargs)
        self.plan_file = plan_file
        self.line_number = line_number

    def __str__(self) -> str:
        """Format plan error with file and line information."""
        base_msg = self.message
        if self.plan_file and self.line_number:
            return f"{base_msg} at {self.plan_file}:{self.line_number}"
        elif self.plan_file:
            return f"{base_msg} in {self.plan_file}"
        return base_msg


class EngineError(SqlitchError):
    """
    Database engine errors.

    Base class for all database engine-related errors including
    connection issues, SQL execution errors, and engine-specific problems.
    """

    def __init__(
        self,
        message: str,
        engine_name: Optional[str] = None,
        sql_state: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize engine error.

        Args:
            message: Error description
            engine_name: Name of the database engine
            sql_state: SQL state code if applicable
            **kwargs: Additional context
        """
        super().__init__(message, ident="engine", exitval=2, **kwargs)
        self.engine_name = engine_name
        self.sql_state = sql_state


class ConnectionError(EngineError):
    """
    Database connection errors.

    Raised when unable to establish or maintain a database connection,
    including authentication failures and network issues.
    """

    def __init__(
        self, message: str, connection_string: Optional[str] = None, **kwargs: Any
    ) -> None:
        """
        Initialize connection error.

        Args:
            message: Error description
            connection_string: Sanitized connection string (no passwords)
            **kwargs: Additional context
        """
        # Call SqlitchError directly to override ident
        SqlitchError.__init__(self, message, ident="connection", exitval=2, **kwargs)
        self.connection_string = connection_string
        self.engine_name = kwargs.get("engine_name")


class DeploymentError(EngineError):
    """
    Deployment operation errors.

    Raised when deployment, revert, or verify operations fail,
    including SQL execution errors and transaction failures.
    """

    def __init__(
        self,
        message: str,
        change_name: Optional[str] = None,
        operation: Optional[str] = None,
        sql_file: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize deployment error.

        Args:
            message: Error description
            change_name: Name of the change being processed
            operation: Type of operation (deploy, revert, verify)
            sql_file: Path to SQL file being executed
            **kwargs: Additional context
        """
        # Call SqlitchError directly to override ident
        SqlitchError.__init__(self, message, ident="deploy", exitval=2, **kwargs)
        self.change_name = change_name
        self.operation = operation
        self.sql_file = sql_file
        self.engine_name = kwargs.get("engine_name")

    def __str__(self) -> str:
        """Format deployment error with operation context."""
        base_msg = self.message
        if self.operation and self.change_name:
            return f"{base_msg} during {self.operation} of {self.change_name}"
        elif self.operation:
            return f"{base_msg} during {self.operation}"
        return base_msg


class ValidationError(SqlitchError):
    """
    Data validation errors.

    Raised when input data fails validation checks,
    including invalid change names, malformed dependencies, etc.
    """

    def __init__(
        self,
        message: str,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize validation error.

        Args:
            message: Error description
            field_name: Name of the field that failed validation
            field_value: The invalid value
            **kwargs: Additional context
        """
        super().__init__(message, ident="validation", exitval=2, **kwargs)
        self.field_name = field_name
        self.field_value = field_value


class TemplateError(SqlitchError):
    """
    Template processing errors.

    Raised when template files cannot be found, parsed, or processed,
    including variable substitution errors.
    """

    def __init__(
        self,
        message: str,
        template_file: Optional[str] = None,
        template_var: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize template error.

        Args:
            message: Error description
            template_file: Path to problematic template file
            template_var: Template variable that caused the error
            **kwargs: Additional context
        """
        super().__init__(message, ident="template", exitval=2, **kwargs)
        self.template_file = template_file
        self.template_var = template_var


class VCSError(SqlitchError):
    """
    Version control system errors.

    Raised when VCS operations fail, including Git integration
    issues and repository state problems.
    """

    def __init__(
        self,
        message: str,
        vcs_command: Optional[str] = None,
        repository_path: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize VCS error.

        Args:
            message: Error description
            vcs_command: VCS command that failed
            repository_path: Path to the repository
            **kwargs: Additional context
        """
        super().__init__(message, ident="vcs", exitval=2, **kwargs)
        self.vcs_command = vcs_command
        self.repository_path = repository_path


class LockError(SqlitchError):
    """
    Database locking errors.

    Raised when unable to acquire or release database locks,
    indicating concurrent sqitch operations.
    """

    def __init__(
        self, message: str, lock_name: Optional[str] = None, **kwargs: Any
    ) -> None:
        """
        Initialize lock error.

        Args:
            message: Error description
            lock_name: Name of the lock that couldn't be acquired
            **kwargs: Additional context
        """
        super().__init__(message, ident="lock", exitval=2, **kwargs)
        self.lock_name = lock_name


class UserError(SqlitchError):
    """
    User configuration errors.

    Raised when user name or email cannot be determined or are invalid.
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        """
        Initialize user error.

        Args:
            message: Error description
            **kwargs: Additional context
        """
        super().__init__(message, ident="user", exitval=2, **kwargs)


class IOError(SqlitchError):
    """
    Input/output errors.

    Raised when file operations fail or when there are issues with
    reading/writing files or executing external commands.
    """

    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        command: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize IO error.

        Args:
            message: Error description
            file_path: Path to problematic file
            command: Command that failed
            **kwargs: Additional context
        """
        super().__init__(message, ident="io", exitval=2, **kwargs)
        self.file_path = file_path
        self.command = command


class UsageError(SqlitchError):
    """
    Command usage errors.

    Raised when commands are invoked with invalid arguments or
    in inappropriate contexts.
    """

    def __init__(
        self, message: str, command: Optional[str] = None, **kwargs: Any
    ) -> None:
        """
        Initialize usage error.

        Args:
            message: Error description
            command: Command that was used incorrectly
            **kwargs: Additional context
        """
        super().__init__(message, ident="usage", exitval=1, **kwargs)
        self.command = command


def hurl(
    ident_or_message: str,
    message: Optional[str] = None,
    exitval: int = 2,
    **kwargs: Any,
) -> None:
    """
    Throw a SqlitchError exception.

    This function mimics the Perl sqitch hurl() function for consistent
    error throwing patterns.

    Args:
        ident_or_message: Either error identifier + message, or just message for DEV errors
        message: Error message (if first arg is identifier)
        exitval: Exit value for the error
        **kwargs: Additional error context

    Raises:
        SqlitchError: Always raises an exception

    Examples:
        hurl("config", "Invalid configuration value")
        hurl("Cannot parse plan file")  # DEV error
        hurl("io", "Cannot open file: {file}", file="test.txt")
    """
    if message is None:
        # Single argument form - treat as DEV error
        raise SqlitchError(ident_or_message, ident="DEV", exitval=exitval, **kwargs)
    else:
        # Two argument form - first is ident, second is message
        raise SqlitchError(message, ident=ident_or_message, exitval=exitval, **kwargs)


def format_error_message(error_type: str, details: str, **context: Any) -> str:
    """
    Format error messages to match Perl sqitch output.

    Args:
        error_type: Type of error (e.g., "configuration", "plan")
        details: Detailed error description
        **context: Additional context for error formatting

    Returns:
        Formatted error message string
    """
    return f"sqlitch: {error_type}: {details}"


def handle_exception(exc: Exception, sqitch=None) -> int:
    """
    Handle exceptions and return appropriate exit codes.

    This function mimics the Perl sqitch exception handling behavior,
    including proper output routing and verbosity handling.

    Args:
        exc: Exception to handle
        sqitch: Optional Sqitch instance for logging

    Returns:
        Exit code for the application
    """
    if isinstance(exc, SqlitchError):
        if sqitch:
            if exc.exitval == 1:
                # Non-fatal exception; just send the message to info
                sqitch.info(exc.message)
            elif exc.ident == "DEV":
                # Vent complete details of fatal DEV error
                sqitch.vent(exc.as_string())
            else:
                # Vent fatal error message, trace details
                sqitch.vent(exc.message)
                sqitch.trace(exc.details_string())
        else:
            # No sqitch instance, print to stderr
            print(f"sqlitch: {exc.message}", file=sys.stderr)

        return exc.exitval
    else:
        error_msg = f"sqlitch: unexpected error: {exc}"
        if sqitch:
            sqitch.vent(error_msg)
        else:
            print(error_msg, file=sys.stderr)
        return 2


def suggest_help(command: Optional[str] = None) -> str:
    """
    Generate helpful suggestions for common errors.

    Args:
        command: Command that failed (optional)

    Returns:
        Helpful suggestion text
    """
    if command:
        return f"Try 'sqlitch help {command}' for more information."
    else:
        return "Try 'sqlitch help' for more information."


def format_validation_error(field: str, value: Any, expected: str) -> str:
    """
    Format validation error messages consistently.

    Args:
        field: Field name that failed validation
        value: Invalid value
        expected: Description of expected format

    Returns:
        Formatted validation error message
    """
    return f'Invalid {field} "{value}": {expected}'


def format_file_error(operation: str, file_path: str, error: str) -> str:
    """
    Format file operation error messages.

    Args:
        operation: Operation that failed (e.g., "read", "write")
        file_path: Path to the file
        error: Error description

    Returns:
        Formatted file error message
    """
    return f"Cannot {operation} {file_path}: {error}"


def format_database_error(operation: str, target: str, error: str) -> str:
    """
    Format database operation error messages.

    Args:
        operation: Database operation (e.g., "connect to", "deploy to")
        target: Database target
        error: Error description

    Returns:
        Formatted database error message
    """
    return f"Cannot {operation} {target}: {error}"
