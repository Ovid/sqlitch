"""
Custom exception hierarchy for sqitch.

This module defines all custom exceptions used throughout the sqitch application,
providing clear error categorization and consistent error handling.
"""

from typing import Optional, Any, Dict


class SqlitchError(Exception):
    """
    Base exception for all sqitch errors.
    
    This is the root exception class that all other sqitch-specific
    exceptions inherit from. It provides consistent error formatting
    and optional error codes.
    """
    
    def __init__(self, message: str, error_code: Optional[int] = None, **kwargs: Any) -> None:
        """
        Initialize sqitch error.
        
        Args:
            message: Human-readable error message
            error_code: Optional numeric error code
            **kwargs: Additional error context
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = kwargs
    
    def __str__(self) -> str:
        """Format error message for display."""
        return f"sqlitch: {self.message}"


class ConfigurationError(SqlitchError):
    """
    Configuration-related errors.
    
    Raised when there are issues with configuration file parsing,
    invalid configuration values, or missing required configuration.
    """
    
    def __init__(self, message: str, config_file: Optional[str] = None, 
                 config_key: Optional[str] = None, **kwargs: Any) -> None:
        """
        Initialize configuration error.
        
        Args:
            message: Error description
            config_file: Path to problematic config file
            config_key: Specific configuration key that caused the error
            **kwargs: Additional context
        """
        super().__init__(message, **kwargs)
        self.config_file = config_file
        self.config_key = config_key


class PlanError(SqlitchError):
    """
    Plan file parsing or validation errors.
    
    Raised when there are issues with plan file syntax, invalid change
    definitions, dependency conflicts, or other plan-related problems.
    """
    
    def __init__(self, message: str, plan_file: Optional[str] = None,
                 line_number: Optional[int] = None, **kwargs: Any) -> None:
        """
        Initialize plan error.
        
        Args:
            message: Error description
            plan_file: Path to problematic plan file
            line_number: Line number where error occurred
            **kwargs: Additional context
        """
        super().__init__(message, **kwargs)
        self.plan_file = plan_file
        self.line_number = line_number
    
    def __str__(self) -> str:
        """Format plan error with file and line information."""
        base_msg = super().__str__()
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
    
    def __init__(self, message: str, engine_name: Optional[str] = None,
                 sql_state: Optional[str] = None, **kwargs: Any) -> None:
        """
        Initialize engine error.
        
        Args:
            message: Error description
            engine_name: Name of the database engine
            sql_state: SQL state code if applicable
            **kwargs: Additional context
        """
        super().__init__(message, **kwargs)
        self.engine_name = engine_name
        self.sql_state = sql_state


class ConnectionError(EngineError):
    """
    Database connection errors.
    
    Raised when unable to establish or maintain a database connection,
    including authentication failures and network issues.
    """
    
    def __init__(self, message: str, connection_string: Optional[str] = None,
                 **kwargs: Any) -> None:
        """
        Initialize connection error.
        
        Args:
            message: Error description
            connection_string: Sanitized connection string (no passwords)
            **kwargs: Additional context
        """
        super().__init__(message, **kwargs)
        self.connection_string = connection_string


class DeploymentError(EngineError):
    """
    Deployment operation errors.
    
    Raised when deployment, revert, or verify operations fail,
    including SQL execution errors and transaction failures.
    """
    
    def __init__(self, message: str, change_name: Optional[str] = None,
                 operation: Optional[str] = None, sql_file: Optional[str] = None,
                 **kwargs: Any) -> None:
        """
        Initialize deployment error.
        
        Args:
            message: Error description
            change_name: Name of the change being processed
            operation: Type of operation (deploy, revert, verify)
            sql_file: Path to SQL file being executed
            **kwargs: Additional context
        """
        super().__init__(message, **kwargs)
        self.change_name = change_name
        self.operation = operation
        self.sql_file = sql_file
    
    def __str__(self) -> str:
        """Format deployment error with operation context."""
        base_msg = super().__str__()
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
    
    def __init__(self, message: str, field_name: Optional[str] = None,
                 field_value: Optional[Any] = None, **kwargs: Any) -> None:
        """
        Initialize validation error.
        
        Args:
            message: Error description
            field_name: Name of the field that failed validation
            field_value: The invalid value
            **kwargs: Additional context
        """
        super().__init__(message, **kwargs)
        self.field_name = field_name
        self.field_value = field_value


class TemplateError(SqlitchError):
    """
    Template processing errors.
    
    Raised when template files cannot be found, parsed, or processed,
    including variable substitution errors.
    """
    
    def __init__(self, message: str, template_file: Optional[str] = None,
                 template_var: Optional[str] = None, **kwargs: Any) -> None:
        """
        Initialize template error.
        
        Args:
            message: Error description
            template_file: Path to problematic template file
            template_var: Template variable that caused the error
            **kwargs: Additional context
        """
        super().__init__(message, **kwargs)
        self.template_file = template_file
        self.template_var = template_var


class VCSError(SqlitchError):
    """
    Version control system errors.
    
    Raised when VCS operations fail, including Git integration
    issues and repository state problems.
    """
    
    def __init__(self, message: str, vcs_command: Optional[str] = None,
                 repository_path: Optional[str] = None, **kwargs: Any) -> None:
        """
        Initialize VCS error.
        
        Args:
            message: Error description
            vcs_command: VCS command that failed
            repository_path: Path to the repository
            **kwargs: Additional context
        """
        super().__init__(message, **kwargs)
        self.vcs_command = vcs_command
        self.repository_path = repository_path


class LockError(SqlitchError):
    """
    Database locking errors.
    
    Raised when unable to acquire or release database locks,
    indicating concurrent sqitch operations.
    """
    
    def __init__(self, message: str, lock_name: Optional[str] = None,
                 **kwargs: Any) -> None:
        """
        Initialize lock error.
        
        Args:
            message: Error description
            lock_name: Name of the lock that couldn't be acquired
            **kwargs: Additional context
        """
        super().__init__(message, **kwargs)
        self.lock_name = lock_name


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


def handle_exception(exc: Exception) -> int:
    """
    Handle exceptions and return appropriate exit codes.
    
    Args:
        exc: Exception to handle
    
    Returns:
        Exit code for the application
    """
    if isinstance(exc, SqlitchError):
        print(str(exc), file=sys.stderr)
        return exc.error_code or 1
    else:
        print(f"sqlitch: unexpected error: {exc}", file=sys.stderr)
        return 2


# Import sys for stderr usage in handle_exception
import sys