"""
Logging configuration and utilities for sqlitch.

This module provides centralized logging configuration and utilities
to ensure consistent logging behavior across the application.
"""

import logging
import sys
from datetime import datetime
from enum import IntEnum
from pathlib import Path
from typing import Any, Dict, Optional

from sqlitch.core.types import VerbosityLevel


class LogLevel(IntEnum):
    """Custom log levels matching sqlitch verbosity."""

    TRACE = 5  # Most verbose (-vv)
    DEBUG = 10  # Debug info (-v)
    INFO = 20  # Normal output (default)
    WARN = 30  # Warnings
    ERROR = 40  # Errors
    FATAL = 50  # Fatal errors


class SqlitchFormatter(logging.Formatter):
    """
    Custom formatter for sqlitch log messages.

    Formats log messages to match the style and format of the original
    Perl sqitch tool for consistency.
    """

    def __init__(self, show_timestamps: bool = False, show_level: bool = False) -> None:
        """
        Initialize formatter.

        Args:
            show_timestamps: Whether to include timestamps in output
            show_level: Whether to include log level in output
        """
        self.show_timestamps = show_timestamps
        self.show_level = show_level
        super().__init__()

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record.

        Args:
            record: Log record to format

        Returns:
            Formatted log message
        """
        parts = []

        if self.show_timestamps:
            timestamp = datetime.fromtimestamp(record.created).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            parts.append(f"[{timestamp}]")

        if self.show_level and record.levelno >= logging.WARNING:
            level_name = record.levelname.lower()
            parts.append(f"{level_name}:")

        parts.append(record.getMessage())

        return " ".join(parts)


class ColoredFormatter(SqlitchFormatter):
    """
    Colored formatter for terminal output.

    Adds ANSI color codes to log messages based on their level
    for better visual distinction in terminal output.
    """

    # ANSI color codes
    COLORS = {
        LogLevel.TRACE: "\033[90m",  # Dark gray
        LogLevel.DEBUG: "\033[36m",  # Cyan
        LogLevel.INFO: "\033[0m",  # Default
        LogLevel.WARN: "\033[33m",  # Yellow
        LogLevel.ERROR: "\033[31m",  # Red
        LogLevel.FATAL: "\033[91m",  # Bright red
    }
    RESET = "\033[0m"

    def __init__(self, use_colors: bool = True, **kwargs: Any) -> None:
        """
        Initialize colored formatter.

        Args:
            use_colors: Whether to use colors (auto-detected if None)
            **kwargs: Additional arguments for parent formatter
        """
        super().__init__(**kwargs)
        self.use_colors = use_colors and self._supports_color()

    def _supports_color(self) -> bool:
        """Check if terminal supports colors."""
        return (
            hasattr(sys.stdout, "isatty")
            and sys.stdout.isatty()
            and hasattr(sys.stderr, "isatty")
            and sys.stderr.isatty()
        )

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record with colors.

        Args:
            record: Log record to format

        Returns:
            Formatted and colored log message
        """
        message = super().format(record)

        if self.use_colors and record.levelno in self.COLORS:
            color = self.COLORS[record.levelno]
            return f"{color}{message}{self.RESET}"

        return message


class SqlitchLogger:
    """
    Main logger class for sqlitch operations.

    Provides a centralized logging interface with verbosity control
    and consistent formatting across the application.
    """

    def __init__(self, name: str = "sqlitch", verbosity: VerbosityLevel = 0) -> None:
        """
        Initialize sqlitch logger.

        Args:
            name: Logger name
            verbosity: Verbosity level (-2 to 3)
        """
        self.logger = logging.getLogger(name)
        self.verbosity = verbosity
        self._setup_logger()

    def _setup_logger(self) -> None:
        """Set up logger configuration based on verbosity."""
        # Clear any existing handlers
        self.logger.handlers.clear()

        # Set log level based on verbosity
        level = self._verbosity_to_level(self.verbosity)
        self.logger.setLevel(level)

        # Create console handler
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)

        # Create formatter
        show_level = self.verbosity >= 1
        formatter = ColoredFormatter(show_level=show_level)
        handler.setFormatter(formatter)

        self.logger.addHandler(handler)

        # Prevent propagation to root logger
        self.logger.propagate = False

    def _verbosity_to_level(self, verbosity: VerbosityLevel) -> int:
        """
        Convert verbosity level to logging level.

        Args:
            verbosity: Verbosity level

        Returns:
            Corresponding logging level
        """
        mapping = {
            -2: LogLevel.FATAL,  # Only fatal errors
            -1: LogLevel.ERROR,  # Errors and fatal
            0: LogLevel.WARN,  # Warnings, errors, and fatal (default)
            1: LogLevel.INFO,  # Info and above
            2: LogLevel.DEBUG,  # Debug and above
            3: LogLevel.TRACE,  # Everything
        }
        return mapping.get(verbosity, LogLevel.WARN)

    def set_verbosity(self, verbosity: VerbosityLevel) -> None:
        """
        Update logger verbosity.

        Args:
            verbosity: New verbosity level
        """
        self.verbosity = verbosity
        level = self._verbosity_to_level(verbosity)
        self.logger.setLevel(level)

        for handler in self.logger.handlers:
            handler.setLevel(level)

    def trace(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log trace message (most verbose)."""
        self.logger.log(LogLevel.TRACE, message, *args, **kwargs)

    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log debug message."""
        self.logger.debug(message, *args, **kwargs)

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log info message."""
        self.logger.info(message, *args, **kwargs)

    def warn(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log warning message."""
        self.logger.warning(message, *args, **kwargs)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log error message."""
        self.logger.error(message, *args, **kwargs)

    def fatal(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log fatal error message."""
        self.logger.log(LogLevel.FATAL, message, *args, **kwargs)

    def comment(self, message: str) -> None:
        """Log a comment (always shown regardless of verbosity)."""
        # Comments are always printed to stdout, not stderr
        print(f"# {message}")

    def emit(self, message: str) -> None:
        """Emit a message directly to stdout (for command output)."""
        print(message)


# Global logger instance
_global_logger: Optional[SqlitchLogger] = None


def get_logger(name: str = "sqlitch") -> SqlitchLogger:
    """
    Get or create global logger instance.

    Args:
        name: Logger name

    Returns:
        Global logger instance
    """
    global _global_logger
    if _global_logger is None:
        _global_logger = SqlitchLogger(name)
    return _global_logger


def configure_logging(
    verbosity: VerbosityLevel = 0, log_file: Optional[Path] = None
) -> SqlitchLogger:
    """
    Configure global logging settings.

    Args:
        verbosity: Verbosity level
        log_file: Optional log file path

    Returns:
        Configured logger instance
    """
    global _global_logger
    _global_logger = SqlitchLogger("sqlitch", verbosity)

    # Add file handler if log file specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(LogLevel.TRACE)  # Log everything to file

        # Use detailed format for file logging
        file_formatter = SqlitchFormatter(show_timestamps=True, show_level=True)
        file_handler.setFormatter(file_formatter)

        _global_logger.logger.addHandler(file_handler)

    return _global_logger


def log_sql_execution(sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    """
    Log SQL execution for debugging.

    Args:
        sql: SQL statement being executed
        params: Optional parameters for the SQL
    """
    logger = get_logger()
    logger.debug("Executing SQL: %s", sql)
    if params:
        logger.trace("SQL parameters: %s", params)


def log_file_operation(operation: str, file_path: Path) -> None:
    """
    Log file operations.

    Args:
        operation: Type of operation (read, write, create, etc.)
        file_path: Path to the file
    """
    logger = get_logger()
    logger.debug("%s file: %s", operation.capitalize(), file_path)


def log_command_execution(command: str, args: list) -> None:
    """
    Log command execution.

    Args:
        command: Command being executed
        args: Command arguments
    """
    logger = get_logger()
    logger.debug("Executing command: %s %s", command, " ".join(args))


def log_database_operation(
    operation: str, target: str, change: Optional[str] = None
) -> None:
    """
    Log database operations.

    Args:
        operation: Type of operation (deploy, revert, verify)
        target: Database target
        change: Optional change name
    """
    logger = get_logger()
    if change:
        logger.info("%s %s to %s", operation.capitalize(), change, target)
    else:
        logger.info("%s to %s", operation.capitalize(), target)


class LogContext:
    """
    Context manager for temporary logging configuration.

    Allows temporarily changing logging settings within a specific
    context and restoring them afterwards.
    """

    def __init__(
        self, verbosity: Optional[VerbosityLevel] = None, suppress_output: bool = False
    ) -> None:
        """
        Initialize log context.

        Args:
            verbosity: Temporary verbosity level
            suppress_output: Whether to suppress all output
        """
        self.new_verbosity = verbosity
        self.suppress_output = suppress_output
        self.original_verbosity: Optional[VerbosityLevel] = None
        self.original_handlers: list = []

    def __enter__(self) -> SqlitchLogger:
        """Enter context and apply temporary settings."""
        logger = get_logger()

        # Save original settings
        self.original_verbosity = logger.verbosity
        self.original_handlers = logger.logger.handlers.copy()

        # Apply temporary settings
        if self.new_verbosity is not None:
            logger.set_verbosity(self.new_verbosity)

        if self.suppress_output:
            # Remove all handlers to suppress output
            logger.logger.handlers.clear()

        return logger

    def __exit__(self, *args: Any) -> None:
        """Exit context and restore original settings."""
        logger = get_logger()

        # Restore original settings
        if self.original_verbosity is not None:
            logger.set_verbosity(self.original_verbosity)

        if self.suppress_output:
            # Restore original handlers
            logger.logger.handlers = self.original_handlers
