"""
Tests for progress indicators and user feedback utilities.

This module tests the progress indicators, status reporting, and user
interaction functionality.
"""

import sys
import threading
import time
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

from sqlitch.core.exceptions import IOError
from sqlitch.utils.progress import (
    Dots,
    ProgressBar,
    ProgressIndicator,
    Spinner,
    StatusReporter,
    confirm_action,
    progress_indicator,
    prompt_for_input,
    show_progress,
)


class TestProgressIndicator:
    """Test base ProgressIndicator class."""

    def test_basic_functionality(self):
        """Test basic progress indicator functionality."""
        output = StringIO()
        indicator = ProgressIndicator("Testing", file=output)

        assert not indicator.active
        assert indicator.message == "Testing"
        assert indicator.file is output

    def test_context_manager(self):
        """Test progress indicator as context manager."""
        output = StringIO()

        with ProgressIndicator("Testing", file=output) as indicator:
            assert indicator.active

        assert not indicator.active


class TestSpinner:
    """Test Spinner progress indicator."""

    def test_spinner_creation(self):
        """Test spinner creation."""
        output = StringIO()
        spinner = Spinner("Loading", file=output, interval=0.01)

        assert spinner.message == "Loading"
        assert spinner.interval == 0.01
        assert spinner.char_index == 0

    def test_spinner_animation(self):
        """Test spinner animation."""
        output = StringIO()
        spinner = Spinner("Loading", file=output, interval=0.01)

        spinner.start()
        time.sleep(0.05)  # Let it spin a few times
        spinner.stop()

        output_text = output.getvalue()
        assert "Loading" in output_text
        # Should contain spinner characters
        assert any(char in output_text for char in Spinner.CHARS)

    def test_spinner_cleanup(self):
        """Test spinner cleanup."""
        output = StringIO()
        spinner = Spinner("Loading", file=output, interval=0.01)

        with spinner:
            time.sleep(0.02)

        output_text = output.getvalue()
        assert output_text.endswith("Loading\n")


class TestDots:
    """Test Dots progress indicator."""

    def test_dots_creation(self):
        """Test dots creation."""
        output = StringIO()
        dots = Dots("Processing", file=output, interval=0.01, max_dots=3)

        assert dots.message == "Processing"
        assert dots.interval == 0.01
        assert dots.max_dots == 3
        assert dots.dot_count == 0

    def test_dots_animation(self):
        """Test dots animation."""
        output = StringIO()
        dots = Dots("Processing", file=output, interval=0.01)

        dots.start()
        time.sleep(0.05)  # Let it animate
        dots.stop()

        output_text = output.getvalue()
        assert "Processing" in output_text
        assert "." in output_text


class TestProgressBar:
    """Test ProgressBar progress indicator."""

    def test_progress_bar_creation(self):
        """Test progress bar creation."""
        output = StringIO()
        bar = ProgressBar(100, "Downloading", file=output, width=20)

        assert bar.total == 100
        assert bar.message == "Downloading"
        assert bar.width == 20
        assert bar.current == 0

    def test_progress_bar_update(self):
        """Test progress bar updates."""
        output = StringIO()
        bar = ProgressBar(100, "Downloading", file=output, width=20)

        bar.start()
        bar.update(25)
        bar.update(50)
        bar.update(100)
        bar.stop()

        output_text = output.getvalue()
        assert "Downloading" in output_text
        assert "25%" in output_text or "50%" in output_text or "100%" in output_text
        assert "█" in output_text or "░" in output_text

    def test_progress_bar_increment(self):
        """Test progress bar increment."""
        output = StringIO()
        bar = ProgressBar(10, "Processing", file=output)

        bar.start()
        for i in range(5):
            bar.increment(2)
        bar.stop()

        assert bar.current == 10

    def test_progress_bar_zero_total(self):
        """Test progress bar with zero total."""
        output = StringIO()
        bar = ProgressBar(0, "Processing", file=output)

        bar.start()
        bar.update(0)
        bar.stop()

        output_text = output.getvalue()
        assert "100%" in output_text


class TestProgressIndicatorContextManager:
    """Test progress_indicator context manager."""

    def test_spinner_context(self):
        """Test spinner context manager."""
        output = StringIO()

        with progress_indicator("Loading", "spinner", file=output) as indicator:
            assert isinstance(indicator, Spinner)
            assert indicator.active
            time.sleep(0.01)

        assert not indicator.active

    def test_dots_context(self):
        """Test dots context manager."""
        output = StringIO()

        with progress_indicator("Processing", "dots", file=output) as indicator:
            assert isinstance(indicator, Dots)
            assert indicator.active
            time.sleep(0.01)

        assert not indicator.active

    def test_bar_context(self):
        """Test progress bar context manager."""
        output = StringIO()

        with progress_indicator(
            "Downloading", "bar", file=output, total=100
        ) as indicator:
            assert isinstance(indicator, ProgressBar)
            assert indicator.active
            indicator.update(50)

        assert not indicator.active

    def test_invalid_indicator_type(self):
        """Test invalid indicator type."""
        with pytest.raises(ValueError, match="Unknown indicator type"):
            with progress_indicator("Test", "invalid"):
                pass


class TestShowProgress:
    """Test show_progress function."""

    def test_show_progress_with_bar(self):
        """Test show_progress with progress bar."""
        items = list(range(5))
        results = []

        # Capture the progress output
        with patch("sys.stderr", new_callable=StringIO):
            for item in show_progress(items, "Processing items"):
                results.append(item * 2)

        assert results == [0, 2, 4, 6, 8]

    def test_show_progress_single_item(self):
        """Test show_progress with single item (uses spinner)."""
        items = [42]
        results = []

        with patch("sys.stderr", new_callable=StringIO):
            for item in show_progress(items, "Processing item"):
                results.append(item)

        assert results == [42]

    def test_show_progress_no_bar(self):
        """Test show_progress with bar disabled."""
        items = list(range(3))
        results = []

        with patch("sys.stderr", new_callable=StringIO):
            for item in show_progress(items, "Processing", show_bar=False):
                results.append(item)

        assert results == [0, 1, 2]


class TestStatusReporter:
    """Test StatusReporter class."""

    def test_status_reporter_creation(self):
        """Test status reporter creation."""
        output = StringIO()
        reporter = StatusReporter(verbosity=1, file=output)

        assert reporter.verbosity == 1
        assert reporter.file is output

    def test_status_messages(self):
        """Test status message reporting."""
        output = StringIO()
        reporter = StatusReporter(verbosity=2, file=output)

        reporter.status("Test message", level=0)
        reporter.status("Verbose message", level=1)
        reporter.status("Debug message", level=2)
        reporter.status("Trace message", level=3)  # Should not appear

        output_text = output.getvalue()
        assert "Test message" in output_text
        assert "Verbose message" in output_text
        assert "Debug message" in output_text
        assert "Trace message" not in output_text

    def test_info_verbose_debug_trace(self):
        """Test info, verbose, debug, and trace methods."""
        output = StringIO()
        reporter = StatusReporter(verbosity=3, file=output)

        reporter.info("Info message")
        reporter.verbose("Verbose message")
        reporter.debug("Debug message")
        reporter.trace("Trace message")

        output_text = output.getvalue()
        assert "Info message" in output_text
        assert "# Verbose message" in output_text
        assert "debug: Debug message" in output_text
        assert "trace: Trace message" in output_text

    def test_warning_and_error(self):
        """Test warning and error messages (always shown)."""
        output = StringIO()
        reporter = StatusReporter(verbosity=-1, file=output)  # Very low verbosity

        reporter.warning("Warning message")
        reporter.error("Error message")

        output_text = output.getvalue()
        assert "warning: Warning message" in output_text
        assert "error: Error message" in output_text

    def test_operation_methods(self):
        """Test operation-specific methods."""
        output = StringIO()
        reporter = StatusReporter(verbosity=1, file=output)

        reporter.operation_start("deploy", "postgresql://localhost/test")
        reporter.change_status("deploy", "add_users")
        reporter.operation_complete("deploy", 3)

        output_text = output.getvalue()
        assert "Deploy to postgresql://localhost/test" in output_text
        assert "Deploy completed (3 changes)" in output_text


class TestConfirmAction:
    """Test confirm_action function."""

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", return_value="y")
    def test_confirm_yes(self, mock_input, mock_isatty):
        """Test confirmation with yes response."""
        result = confirm_action("Continue?")
        assert result is True
        mock_input.assert_called_once_with("Continue? [y/n] ")

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", return_value="n")
    def test_confirm_no(self, mock_input, mock_isatty):
        """Test confirmation with no response."""
        result = confirm_action("Continue?")
        assert result is False

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", return_value="")
    def test_confirm_default_yes(self, mock_input, mock_isatty):
        """Test confirmation with default yes."""
        result = confirm_action("Continue?", default=True)
        assert result is True
        mock_input.assert_called_once_with("Continue? [Y/n] ")

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", return_value="")
    def test_confirm_default_no(self, mock_input, mock_isatty):
        """Test confirmation with default no."""
        result = confirm_action("Continue?", default=False)
        assert result is False
        mock_input.assert_called_once_with("Continue? [y/N] ")

    @patch("sys.stdin.isatty", return_value=False)
    def test_confirm_unattended_with_default(self, mock_isatty):
        """Test confirmation when unattended with default."""
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            result = confirm_action("Continue?", default=True)

        assert result is True
        assert "Continue? [Y/n] Y" in mock_stdout.getvalue()

    @patch("sys.stdin.isatty", return_value=False)
    def test_confirm_unattended_no_default(self, mock_isatty):
        """Test confirmation when unattended without default."""
        with pytest.raises(IOError, match="unattended"):
            confirm_action("Continue?")

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", side_effect=["invalid", "maybe", "y"])
    def test_confirm_invalid_responses(self, mock_input, mock_isatty):
        """Test confirmation with invalid responses."""
        with patch("builtins.print") as mock_print:
            result = confirm_action("Continue?")

        assert result is True
        assert mock_input.call_count == 3
        assert mock_print.call_count == 2  # Two "Please answer" messages

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", side_effect=["invalid"] * 3)
    def test_confirm_max_attempts(self, mock_input, mock_isatty):
        """Test confirmation with max attempts exceeded."""
        with pytest.raises(IOError, match="No valid answer after 3 attempts"):
            confirm_action("Continue?")

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", side_effect=KeyboardInterrupt())
    def test_confirm_keyboard_interrupt(self, mock_input, mock_isatty):
        """Test confirmation with keyboard interrupt."""
        result = confirm_action("Continue?")
        assert result is False


class TestPromptForInput:
    """Test prompt_for_input function."""

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", return_value="test input")
    def test_prompt_basic(self, mock_input, mock_isatty):
        """Test basic input prompt."""
        result = prompt_for_input("Enter value:")
        assert result == "test input"
        mock_input.assert_called_once_with("Enter value: ")

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", return_value="")
    def test_prompt_with_default(self, mock_input, mock_isatty):
        """Test input prompt with default value."""
        result = prompt_for_input("Enter value:", default="default")
        assert result == "default"
        mock_input.assert_called_once_with("Enter value: [default] ")

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", return_value="user input")
    def test_prompt_override_default(self, mock_input, mock_isatty):
        """Test input prompt overriding default."""
        result = prompt_for_input("Enter value:", default="default")
        assert result == "user input"

    @patch("sys.stdin.isatty", return_value=False)
    def test_prompt_unattended_with_default(self, mock_isatty):
        """Test input prompt when unattended with default."""
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            result = prompt_for_input("Enter value:", default="default")

        assert result == "default"
        assert "Enter value: [default] default" in mock_stdout.getvalue()

    @patch("sys.stdin.isatty", return_value=False)
    def test_prompt_unattended_no_default(self, mock_isatty):
        """Test input prompt when unattended without default."""
        with pytest.raises(IOError, match="unattended"):
            prompt_for_input("Enter value:")

    @patch("sys.stdin.isatty", return_value=True)
    @patch("builtins.input", side_effect=KeyboardInterrupt())
    def test_prompt_keyboard_interrupt(self, mock_input, mock_isatty):
        """Test input prompt with keyboard interrupt."""
        with pytest.raises(IOError, match="Operation cancelled"):
            prompt_for_input("Enter value:")


class TestProgressIntegration:
    """Test progress indicator integration."""

    def test_multiple_indicators(self):
        """Test using multiple progress indicators."""
        output1 = StringIO()
        output2 = StringIO()

        with Spinner("Task 1", file=output1, interval=0.01):
            time.sleep(0.02)
            with Dots("Task 2", file=output2, interval=0.01):
                time.sleep(0.02)

        assert "Task 1" in output1.getvalue()
        assert "Task 2" in output2.getvalue()

    def test_nested_progress_contexts(self):
        """Test nested progress contexts."""
        output = StringIO()

        with progress_indicator("Outer task", "spinner", file=output, interval=0.01):
            time.sleep(0.01)
            with progress_indicator("Inner task", "dots", file=output, interval=0.01):
                time.sleep(0.01)

        output_text = output.getvalue()
        # Both tasks should appear in output
        assert "Outer task" in output_text
        assert "Inner task" in output_text

    def test_progress_with_exception(self):
        """Test progress indicator cleanup on exception."""
        output = StringIO()

        with pytest.raises(ValueError):
            with progress_indicator("Task", "spinner", file=output, interval=0.01):
                time.sleep(0.01)
                raise ValueError("Test exception")

        # Progress indicator should still clean up properly
        output_text = output.getvalue()
        assert "Task" in output_text
