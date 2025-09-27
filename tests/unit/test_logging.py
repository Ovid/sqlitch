"""
Unit tests for the logging utilities.

This module tests the logging configuration, formatters, and utilities
used throughout the sqlitch application.
"""

import logging
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlitch.utils.logging import (
    ColoredFormatter,
    LogContext,
    LogLevel,
    SqlitchFormatter,
    SqlitchLogger,
    configure_logging,
    get_logger,
    log_command_execution,
    log_database_operation,
    log_file_operation,
    log_sql_execution,
)


class TestLogLevel:
    """Test LogLevel enum."""

    def test_log_level_values(self):
        """Test that log levels have correct values."""
        assert LogLevel.TRACE == 5
        assert LogLevel.DEBUG == 10
        assert LogLevel.INFO == 20
        assert LogLevel.WARN == 30
        assert LogLevel.ERROR == 40
        assert LogLevel.FATAL == 50

    def test_log_level_ordering(self):
        """Test that log levels are properly ordered."""
        assert LogLevel.TRACE < LogLevel.DEBUG
        assert LogLevel.DEBUG < LogLevel.INFO
        assert LogLevel.INFO < LogLevel.WARN
        assert LogLevel.WARN < LogLevel.ERROR
        assert LogLevel.ERROR < LogLevel.FATAL


class TestSqlitchFormatter:
    """Test SqlitchFormatter class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        self.record.created = datetime(2023, 12, 25, 15, 30, 45).timestamp()

    def test_formatter_init_defaults(self):
        """Test formatter initialization with defaults."""
        formatter = SqlitchFormatter()
        assert formatter.show_timestamps is False
        assert formatter.show_level is False

    def test_formatter_init_with_options(self):
        """Test formatter initialization with options."""
        formatter = SqlitchFormatter(show_timestamps=True, show_level=True)
        assert formatter.show_timestamps is True
        assert formatter.show_level is True

    def test_format_basic_message(self):
        """Test formatting basic message."""
        formatter = SqlitchFormatter()
        result = formatter.format(self.record)
        assert result == "Test message"

    def test_format_with_timestamps(self):
        """Test formatting with timestamps."""
        formatter = SqlitchFormatter(show_timestamps=True)
        result = formatter.format(self.record)
        assert "[2023-12-25 15:30:45]" in result
        assert "Test message" in result

    def test_format_with_level_info(self):
        """Test formatting with level for INFO (should not show level)."""
        formatter = SqlitchFormatter(show_level=True)
        result = formatter.format(self.record)
        assert result == "Test message"  # INFO level not shown

    def test_format_with_level_warning(self):
        """Test formatting with level for WARNING."""
        formatter = SqlitchFormatter(show_level=True)
        self.record.levelno = logging.WARNING
        self.record.levelname = "WARNING"
        result = formatter.format(self.record)
        assert result == "warning: Test message"

    def test_format_with_level_error(self):
        """Test formatting with level for ERROR."""
        formatter = SqlitchFormatter(show_level=True)
        self.record.levelno = logging.ERROR
        self.record.levelname = "ERROR"
        result = formatter.format(self.record)
        assert result == "error: Test message"

    def test_format_with_timestamps_and_level(self):
        """Test formatting with both timestamps and level."""
        formatter = SqlitchFormatter(show_timestamps=True, show_level=True)
        self.record.levelno = logging.ERROR
        self.record.levelname = "ERROR"
        result = formatter.format(self.record)
        assert "[2023-12-25 15:30:45]" in result
        assert "error:" in result
        assert "Test message" in result


class TestColoredFormatter:
    """Test ColoredFormatter class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

    def test_colored_formatter_init_with_colors(self):
        """Test colored formatter initialization with colors enabled."""
        with patch.object(ColoredFormatter, "_supports_color", return_value=True):
            formatter = ColoredFormatter(use_colors=True)
            assert formatter.use_colors is True

    def test_colored_formatter_init_without_colors(self):
        """Test colored formatter initialization with colors disabled."""
        formatter = ColoredFormatter(use_colors=False)
        assert formatter.use_colors is False

    def test_supports_color_with_tty(self):
        """Test color support detection with TTY."""
        formatter = ColoredFormatter(use_colors=False)

        # Mock stdout and stderr to have isatty method returning True
        mock_stdout = Mock()
        mock_stdout.isatty.return_value = True
        mock_stderr = Mock()
        mock_stderr.isatty.return_value = True

        with patch.object(sys, "stdout", mock_stdout):
            with patch.object(sys, "stderr", mock_stderr):
                assert formatter._supports_color() is True

    def test_supports_color_without_tty(self):
        """Test color support detection without TTY."""
        formatter = ColoredFormatter(use_colors=False)

        # Mock stdout and stderr to have isatty method returning False
        mock_stdout = Mock()
        mock_stdout.isatty.return_value = False
        mock_stderr = Mock()
        mock_stderr.isatty.return_value = False

        with patch.object(sys, "stdout", mock_stdout):
            with patch.object(sys, "stderr", mock_stderr):
                assert formatter._supports_color() is False

    def test_supports_color_no_isatty(self):
        """Test color support detection when isatty is not available."""
        formatter = ColoredFormatter(use_colors=False)

        # Mock stdout and stderr without isatty method
        mock_stdout = Mock(spec=[])  # No isatty method
        mock_stderr = Mock(spec=[])  # No isatty method

        with patch.object(sys, "stdout", mock_stdout):
            with patch.object(sys, "stderr", mock_stderr):
                assert formatter._supports_color() is False

    def test_format_with_colors_disabled(self):
        """Test formatting with colors disabled."""
        formatter = ColoredFormatter(use_colors=False)
        result = formatter.format(self.record)
        assert result == "Test message"
        assert "\033[" not in result  # No ANSI codes

    def test_format_with_colors_enabled_info(self):
        """Test formatting with colors enabled for INFO level."""
        formatter = ColoredFormatter(use_colors=True)
        formatter.use_colors = True  # Force enable for testing
        self.record.levelno = LogLevel.INFO
        result = formatter.format(self.record)
        assert "\033[0m" in result  # Default color for INFO
        assert "Test message" in result

    def test_format_with_colors_enabled_error(self):
        """Test formatting with colors enabled for ERROR level."""
        formatter = ColoredFormatter(use_colors=True)
        formatter.use_colors = True  # Force enable for testing
        self.record.levelno = LogLevel.ERROR
        result = formatter.format(self.record)
        assert "\033[31m" in result  # Red color for ERROR
        assert "\033[0m" in result  # Reset code
        assert "Test message" in result

    def test_format_with_colors_enabled_unknown_level(self):
        """Test formatting with colors enabled for unknown level."""
        formatter = ColoredFormatter(use_colors=True)
        formatter.use_colors = True  # Force enable for testing
        self.record.levelno = 999  # Unknown level
        result = formatter.format(self.record)
        assert result == "Test message"  # No color codes for unknown level

    def test_color_codes_exist(self):
        """Test that all expected color codes are defined."""
        assert LogLevel.TRACE in ColoredFormatter.COLORS
        assert LogLevel.DEBUG in ColoredFormatter.COLORS
        assert LogLevel.INFO in ColoredFormatter.COLORS
        assert LogLevel.WARN in ColoredFormatter.COLORS
        assert LogLevel.ERROR in ColoredFormatter.COLORS
        assert LogLevel.FATAL in ColoredFormatter.COLORS


class TestSqlitchLogger:
    """Test SqlitchLogger class."""

    def setup_method(self):
        """Set up test fixtures."""
        # Clear any existing handlers to avoid interference
        logging.getLogger("test_logger").handlers.clear()

    def test_logger_init_defaults(self):
        """Test logger initialization with defaults."""
        logger = SqlitchLogger("test_logger")
        assert logger.logger.name == "test_logger"
        assert logger.verbosity == 0
        assert len(logger.logger.handlers) == 1
        assert logger.logger.propagate is False

    def test_logger_init_with_verbosity(self):
        """Test logger initialization with verbosity."""
        logger = SqlitchLogger("test_logger", verbosity=2)
        assert logger.verbosity == 2
        assert logger.logger.level == LogLevel.DEBUG

    def test_verbosity_to_level_mapping(self):
        """Test verbosity to log level mapping."""
        logger = SqlitchLogger("test_logger")

        assert logger._verbosity_to_level(-2) == LogLevel.FATAL
        assert logger._verbosity_to_level(-1) == LogLevel.ERROR
        assert logger._verbosity_to_level(0) == LogLevel.WARN
        assert logger._verbosity_to_level(1) == LogLevel.INFO
        assert logger._verbosity_to_level(2) == LogLevel.DEBUG
        assert logger._verbosity_to_level(3) == LogLevel.TRACE
        assert logger._verbosity_to_level(999) == LogLevel.WARN  # Default

    def test_set_verbosity(self):
        """Test setting verbosity."""
        logger = SqlitchLogger("test_logger", verbosity=0)
        assert logger.logger.level == LogLevel.WARN

        logger.set_verbosity(2)
        assert logger.verbosity == 2
        assert logger.logger.level == LogLevel.DEBUG

        # Check that handlers are also updated
        for handler in logger.logger.handlers:
            assert handler.level == LogLevel.DEBUG

    def test_logging_methods(self):
        """Test all logging methods."""
        logger = SqlitchLogger("test_logger", verbosity=3)  # Enable all levels

        with patch.object(logger.logger, "log") as mock_log:
            with patch.object(logger.logger, "debug") as mock_debug:
                with patch.object(logger.logger, "info") as mock_info:
                    with patch.object(logger.logger, "warning") as mock_warning:
                        with patch.object(logger.logger, "error") as mock_error:

                            logger.trace("trace message")
                            mock_log.assert_called_with(LogLevel.TRACE, "trace message")

                            logger.debug("debug message")
                            mock_debug.assert_called_with("debug message")

                            logger.info("info message")
                            mock_info.assert_called_with("info message")

                            logger.warn("warn message")
                            mock_warning.assert_called_with("warn message")

                            logger.error("error message")
                            mock_error.assert_called_with("error message")

                            logger.fatal("fatal message")
                            mock_log.assert_called_with(LogLevel.FATAL, "fatal message")

    def test_logging_methods_with_args(self):
        """Test logging methods with arguments."""
        logger = SqlitchLogger("test_logger", verbosity=3)

        with patch.object(logger.logger, "info") as mock_info:
            logger.info("Message with %s", "argument")
            mock_info.assert_called_with("Message with %s", "argument")

    def test_logging_methods_with_kwargs(self):
        """Test logging methods with keyword arguments."""
        logger = SqlitchLogger("test_logger", verbosity=3)

        with patch.object(logger.logger, "error") as mock_error:
            logger.error("Error message", exc_info=True)
            mock_error.assert_called_with("Error message", exc_info=True)

    def test_comment_method(self):
        """Test comment method."""
        logger = SqlitchLogger("test_logger")

        with patch("builtins.print") as mock_print:
            logger.comment("This is a comment")
            mock_print.assert_called_once_with("# This is a comment")

    def test_emit_method(self):
        """Test emit method."""
        logger = SqlitchLogger("test_logger")

        with patch("builtins.print") as mock_print:
            logger.emit("Direct output")
            mock_print.assert_called_once_with("Direct output")

    def test_setup_logger_clears_handlers(self):
        """Test that setup_logger clears existing handlers."""
        logger = SqlitchLogger("test_logger")
        initial_handler_count = len(logger.logger.handlers)

        # Add an extra handler
        extra_handler = logging.StreamHandler()
        logger.logger.addHandler(extra_handler)
        assert len(logger.logger.handlers) == initial_handler_count + 1

        # Setup logger again
        logger._setup_logger()
        assert len(logger.logger.handlers) == 1  # Should be back to 1

    def test_formatter_configuration(self):
        """Test that formatter is configured correctly based on verbosity."""
        # Test with verbosity >= 1 (should show level)
        logger = SqlitchLogger("test_logger", verbosity=1)
        handler = logger.logger.handlers[0]
        formatter = handler.formatter
        assert isinstance(formatter, ColoredFormatter)
        assert formatter.show_level is True

        # Test with verbosity < 1 (should not show level)
        logger = SqlitchLogger("test_logger", verbosity=0)
        handler = logger.logger.handlers[0]
        formatter = handler.formatter
        assert isinstance(formatter, ColoredFormatter)
        assert formatter.show_level is False


class TestGlobalLoggerFunctions:
    """Test global logger functions."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset global logger
        import sqlitch.utils.logging

        sqlitch.utils.logging._global_logger = None

    def test_get_logger_creates_new(self):
        """Test that get_logger creates new logger when none exists."""
        logger = get_logger("test")
        assert isinstance(logger, SqlitchLogger)
        assert logger.logger.name == "test"

    def test_get_logger_returns_existing(self):
        """Test that get_logger returns existing logger."""
        logger1 = get_logger("test")
        logger2 = get_logger("different_name")  # Name is ignored for global logger
        assert logger1 is logger2

    def test_configure_logging_basic(self):
        """Test basic logging configuration."""
        logger = configure_logging(verbosity=1)
        assert isinstance(logger, SqlitchLogger)
        assert logger.verbosity == 1
        assert logger.logger.name == "sqlitch"

    def test_configure_logging_with_file(self, tmp_path):
        """Test logging configuration with file."""
        log_file = tmp_path / "test.log"
        logger = configure_logging(verbosity=2, log_file=log_file)

        assert len(logger.logger.handlers) == 2  # Console + file handler

        # Find the file handler
        file_handler = None
        for handler in logger.logger.handlers:
            if isinstance(handler, logging.FileHandler):
                file_handler = handler
                break

        assert file_handler is not None
        assert file_handler.level == LogLevel.TRACE
        assert isinstance(file_handler.formatter, SqlitchFormatter)
        assert file_handler.formatter.show_timestamps is True
        assert file_handler.formatter.show_level is True


class TestUtilityFunctions:
    """Test utility logging functions."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset global logger
        import sqlitch.utils.logging

        sqlitch.utils.logging._global_logger = None

    def test_log_sql_execution_basic(self):
        """Test SQL execution logging without parameters."""
        with patch("sqlitch.utils.logging.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            log_sql_execution("SELECT * FROM users")

            mock_logger.debug.assert_called_once_with(
                "Executing SQL: %s", "SELECT * FROM users"
            )
            mock_logger.trace.assert_not_called()

    def test_log_sql_execution_with_params(self):
        """Test SQL execution logging with parameters."""
        with patch("sqlitch.utils.logging.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            params = {"user_id": 123, "name": "test"}
            log_sql_execution("SELECT * FROM users WHERE id = %(user_id)s", params)

            mock_logger.debug.assert_called_once_with(
                "Executing SQL: %s", "SELECT * FROM users WHERE id = %(user_id)s"
            )
            mock_logger.trace.assert_called_once_with("SQL parameters: %s", params)

    def test_log_file_operation(self):
        """Test file operation logging."""
        with patch("sqlitch.utils.logging.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            file_path = Path("/path/to/file.txt")
            log_file_operation("read", file_path)

            mock_logger.debug.assert_called_once_with("%s file: %s", "Read", file_path)

    def test_log_command_execution(self):
        """Test command execution logging."""
        with patch("sqlitch.utils.logging.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            log_command_execution("git", ["status", "--porcelain"])

            mock_logger.debug.assert_called_once_with(
                "Executing command: %s %s", "git", "status --porcelain"
            )

    def test_log_database_operation_with_change(self):
        """Test database operation logging with change."""
        with patch("sqlitch.utils.logging.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            log_database_operation(
                "deploy", "pg:db://localhost/test", "add_users_table"
            )

            mock_logger.info.assert_called_once_with(
                "%s %s to %s", "Deploy", "add_users_table", "pg:db://localhost/test"
            )

    def test_log_database_operation_without_change(self):
        """Test database operation logging without change."""
        with patch("sqlitch.utils.logging.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            log_database_operation("status", "pg:db://localhost/test")

            mock_logger.info.assert_called_once_with(
                "%s to %s", "Status", "pg:db://localhost/test"
            )


class TestLogContext:
    """Test LogContext context manager."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset global logger
        import sqlitch.utils.logging

        sqlitch.utils.logging._global_logger = None

    def test_log_context_init(self):
        """Test LogContext initialization."""
        context = LogContext(verbosity=2, suppress_output=True)
        assert context.new_verbosity == 2
        assert context.suppress_output is True
        assert context.original_verbosity is None
        assert context.original_handlers == []

    def test_log_context_with_verbosity_change(self):
        """Test LogContext with verbosity change."""
        # Set up initial logger
        logger = get_logger()
        original_verbosity = logger.verbosity

        with LogContext(verbosity=3) as context_logger:
            assert context_logger.verbosity == 3
            assert context_logger is logger

        # Should restore original verbosity
        assert logger.verbosity == original_verbosity

    def test_log_context_with_output_suppression(self):
        """Test LogContext with output suppression."""
        logger = get_logger()
        original_handlers = logger.logger.handlers.copy()

        with LogContext(suppress_output=True) as context_logger:
            assert len(context_logger.logger.handlers) == 0
            assert context_logger is logger

        # Should restore original handlers
        assert logger.logger.handlers == original_handlers

    def test_log_context_with_both_options(self):
        """Test LogContext with both verbosity and suppression."""
        logger = get_logger()
        original_verbosity = logger.verbosity
        original_handlers = logger.logger.handlers.copy()

        with LogContext(verbosity=1, suppress_output=True) as context_logger:
            assert context_logger.verbosity == 1
            assert len(context_logger.logger.handlers) == 0

        # Should restore both
        assert logger.verbosity == original_verbosity
        assert logger.logger.handlers == original_handlers

    def test_log_context_no_options(self):
        """Test LogContext with no options."""
        logger = get_logger()
        original_verbosity = logger.verbosity
        original_handlers = logger.logger.handlers.copy()

        with LogContext() as context_logger:
            assert context_logger.verbosity == original_verbosity
            assert context_logger.logger.handlers == original_handlers

        # Nothing should change
        assert logger.verbosity == original_verbosity
        assert logger.logger.handlers == original_handlers

    def test_log_context_exception_handling(self):
        """Test LogContext restores settings even when exception occurs."""
        logger = get_logger()
        original_verbosity = logger.verbosity
        original_handlers = logger.logger.handlers.copy()

        try:
            with LogContext(verbosity=3, suppress_output=True):
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Should still restore original settings
        assert logger.verbosity == original_verbosity
        assert logger.logger.handlers == original_handlers

    def test_log_context_verbosity_only(self):
        """Test LogContext with only verbosity change (no output suppression)."""
        logger = get_logger()
        original_verbosity = logger.verbosity
        original_handlers = logger.logger.handlers.copy()

        with LogContext(verbosity=2, suppress_output=False) as context_logger:
            assert context_logger.verbosity == 2
            # Handlers should remain the same
            assert len(context_logger.logger.handlers) == len(original_handlers)

        # Should restore original verbosity, handlers unchanged
        assert logger.verbosity == original_verbosity
        assert logger.logger.handlers == original_handlers

    def test_log_context_no_suppression_branch(self):
        """Test LogContext exit path when suppress_output is False."""
        logger = get_logger()
        original_verbosity = logger.verbosity

        # Create context with suppress_output=False (default)
        context = LogContext(verbosity=2)

        # Manually set up the context state
        context.original_verbosity = original_verbosity
        context.suppress_output = False  # Explicitly set to False
        context.original_handlers = logger.logger.handlers.copy()

        # Enter and exit the context manually to test the branch
        context.__enter__()
        context.__exit__(None, None, None)

        # Should restore verbosity but not touch handlers
        assert logger.verbosity == original_verbosity

    def test_log_context_no_original_verbosity(self):
        """Test LogContext exit when original_verbosity is None."""
        logger = get_logger()

        # Create context that doesn't change verbosity
        context = LogContext(suppress_output=True)

        # Manually set up the context state
        context.original_verbosity = None  # No verbosity change
        context.suppress_output = True
        context.original_handlers = logger.logger.handlers.copy()

        # Enter and exit the context manually to test the branch
        context.__enter__()
        context.__exit__(None, None, None)

        # Should not try to restore verbosity (since original_verbosity is None)


class TestIntegrationScenarios:
    """Test integration scenarios and real-world usage."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset global logger
        import sqlitch.utils.logging

        sqlitch.utils.logging._global_logger = None

    def test_end_to_end_logging_workflow(self, tmp_path):
        """Test complete logging workflow."""
        log_file = tmp_path / "sqlitch.log"

        # Configure logging with file
        logger = configure_logging(verbosity=2, log_file=log_file)

        # Log various types of messages
        logger.info("Starting operation")
        logger.debug("Debug information")
        logger.warn("Warning message")
        logger.error("Error occurred")

        # Use utility functions
        log_sql_execution("SELECT 1", {"param": "value"})
        log_file_operation("write", Path("test.sql"))
        log_command_execution("psql", ["-c", "SELECT 1"])
        log_database_operation("deploy", "localhost", "test_change")

        # Check that log file was created and contains content
        assert log_file.exists()
        content = log_file.read_text()
        assert "Starting operation" in content
        assert "Debug information" in content
        assert "Warning message" in content
        assert "Error occurred" in content

    def test_verbosity_filtering(self):
        """Test that verbosity properly filters messages."""
        # Capture stderr to check what gets logged
        stderr_capture = StringIO()

        with patch("sys.stderr", stderr_capture):
            logger = SqlitchLogger("test", verbosity=0)  # Only WARN and above

            logger.trace("Trace message")  # Should not appear
            logger.debug("Debug message")  # Should not appear
            logger.info("Info message")  # Should not appear
            logger.warn("Warn message")  # Should appear
            logger.error("Error message")  # Should appear

        output = stderr_capture.getvalue()
        assert "Trace message" not in output
        assert "Debug message" not in output
        assert "Info message" not in output
        assert "Warn message" in output
        assert "Error message" in output

    def test_formatter_integration(self):
        """Test formatter integration with real logging."""
        stderr_capture = StringIO()

        with patch("sys.stderr", stderr_capture):
            logger = SqlitchLogger("test", verbosity=1)  # Show levels
            logger.warn("Test warning")
            logger.error("Test error")

        output = stderr_capture.getvalue()
        assert "warning: Test warning" in output
        assert "error: Test error" in output

    def test_multiple_loggers_isolation(self):
        """Test that multiple logger instances don't interfere."""
        logger1 = SqlitchLogger("logger1", verbosity=1)
        logger2 = SqlitchLogger("logger2", verbosity=2)

        assert logger1.verbosity == 1
        assert logger2.verbosity == 2
        assert logger1.logger.name == "logger1"
        assert logger2.logger.name == "logger2"

        # Changing one shouldn't affect the other
        logger1.set_verbosity(3)
        assert logger1.verbosity == 3
        assert logger2.verbosity == 2


if __name__ == "__main__":
    pytest.main([__file__])
