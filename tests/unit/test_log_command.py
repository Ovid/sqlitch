"""
Unit tests for log command.

This module contains unit tests for the LogCommand class,
testing argument parsing, formatting, and event filtering.
"""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.commands.log import LogCommand
from sqlitch.core.exceptions import SqlitchError
from sqlitch.utils.formatter import FORMATS, ItemFormatter


class TestLogCommand:
    """Test cases for LogCommand."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_sqitch = Mock()
        self.mock_config = Mock()
        self.mock_logger = Mock()

        self.mock_sqitch.config = self.mock_config
        self.mock_sqitch.logger = self.mock_logger

        self.command = LogCommand(self.mock_sqitch)

    def test_init(self):
        """Test LogCommand initialization."""
        assert self.command.sqitch == self.mock_sqitch
        assert self.command.config == self.mock_config
        assert self.command.logger == self.mock_logger

    def test_parse_args_basic(self):
        """Test basic argument parsing."""
        args = ["--target", "test_db", "--format", "short"]
        options = self.command._parse_args(args)

        assert options["target"] == "test_db"
        assert options["format"] == "short"
        assert options["headers"] is True
        assert options["reverse"] is False

    def test_parse_args_oneline(self):
        """Test --oneline option sets format and abbrev."""
        args = ["--oneline"]
        options = self.command._parse_args(args)

        assert options["format"] == "oneline"
        assert options["abbrev"] == 6

    def test_parse_args_max_count(self):
        """Test --max-count option parsing."""
        args = ["--max-count", "10"]
        options = self.command._parse_args(args)

        assert options["max_count"] == 10

    def test_parse_args_max_count_invalid(self):
        """Test --max-count with invalid value."""
        args = ["--max-count", "invalid"]

        with pytest.raises(SqlitchError, match="Invalid max-count value"):
            self.command._parse_args(args)

    def test_parse_args_skip(self):
        """Test --skip option parsing."""
        args = ["--skip", "5"]
        options = self.command._parse_args(args)

        assert options["skip"] == 5

    def test_parse_args_skip_invalid(self):
        """Test --skip with invalid value."""
        args = ["--skip", "invalid"]

        with pytest.raises(SqlitchError, match="Invalid skip value"):
            self.command._parse_args(args)

    def test_parse_args_reverse(self):
        """Test --reverse option."""
        args = ["--reverse"]
        options = self.command._parse_args(args)

        assert options["reverse"] is True

    def test_parse_args_no_reverse(self):
        """Test --no-reverse option."""
        args = ["--no-reverse"]
        options = self.command._parse_args(args)

        assert options["reverse"] is False

    def test_parse_args_headers(self):
        """Test --headers and --no-headers options."""
        args = ["--no-headers"]
        options = self.command._parse_args(args)
        assert options["headers"] is False

        args = ["--headers"]
        options = self.command._parse_args(args)
        assert options["headers"] is True

    def test_parse_args_color(self):
        """Test color options."""
        args = ["--color", "always"]
        options = self.command._parse_args(args)
        assert options["color"] == "always"

        args = ["--no-color"]
        options = self.command._parse_args(args)
        assert options["color"] == "never"

    def test_parse_args_color_invalid(self):
        """Test invalid color value."""
        args = ["--color", "invalid"]

        with pytest.raises(SqlitchError, match="Invalid color value"):
            self.command._parse_args(args)

    def test_parse_args_abbrev(self):
        """Test --abbrev option."""
        args = ["--abbrev", "8"]
        options = self.command._parse_args(args)

        assert options["abbrev"] == 8

    def test_parse_args_abbrev_invalid(self):
        """Test --abbrev with invalid value."""
        args = ["--abbrev", "invalid"]

        with pytest.raises(SqlitchError, match="Invalid abbrev value"):
            self.command._parse_args(args)

    def test_parse_args_date_format(self):
        """Test date format options."""
        args = ["--date-format", "raw"]
        options = self.command._parse_args(args)
        assert options["date_format"] == "raw"

        args = ["--date", "iso"]
        options = self.command._parse_args(args)
        assert options["date_format"] == "iso"

    def test_parse_args_event_multiple(self):
        """Test multiple --event options."""
        args = ["--event", "deploy", "--event", "revert"]
        options = self.command._parse_args(args)

        assert options["event"] == ["deploy", "revert"]

    def test_parse_args_patterns(self):
        """Test pattern options."""
        args = [
            "--change-pattern",
            "user.*",
            "--project-pattern",
            "myproject",
            "--committer-pattern",
            "john.*",
            "--planner-pattern",
            "jane.*",
        ]
        options = self.command._parse_args(args)

        assert options["change_pattern"] == "user.*"
        assert options["project_pattern"] == "myproject"
        assert options["committer_pattern"] == "john.*"
        assert options["planner_pattern"] == "jane.*"

    def test_parse_args_unknown_option(self):
        """Test unknown option raises error."""
        args = ["--unknown-option"]

        with pytest.raises(SqlitchError, match="Unknown option"):
            self.command._parse_args(args)

    def test_get_format_template_predefined(self):
        """Test getting predefined format templates."""
        options = {"format": "short"}
        template = self.command._get_format_template(options)

        assert template == FORMATS["short"]

    def test_get_format_template_custom(self):
        """Test getting custom format template."""
        custom_format = "%h %n"
        options = {"format": f"format:{custom_format}"}
        template = self.command._get_format_template(options)

        assert template == custom_format

    def test_get_format_template_direct(self):
        """Test direct format string."""
        format_string = "%h %n %s"
        options = {"format": format_string}
        template = self.command._get_format_template(options)

        assert template == format_string

    def test_get_format_template_unknown(self):
        """Test unknown format raises error."""
        options = {"format": "unknown_format"}

        with pytest.raises(SqlitchError, match="Unknown log format"):
            self.command._get_format_template(options)

    @patch("sqlitch.commands.log.LogCommand.get_target")
    @patch("sqlitch.commands.log.LogCommand._is_database_initialized")
    @patch("sqlitch.commands.log.LogCommand._has_events")
    def test_execute_success(self, mock_has_events, mock_initialized, mock_get_target):
        """Test successful command execution."""
        # Setup mocks
        mock_target = Mock()
        mock_target.uri = "test://db"
        mock_get_target.return_value = mock_target

        mock_engine = Mock()
        mock_events = [
            {
                "event": "deploy",
                "change_id": "abc123",
                "change": "test_change",
                "project": "test_project",
                "note": "Test change",
                "requires": [],
                "conflicts": [],
                "tags": [],
                "committer_name": "John Doe",
                "committer_email": "john@example.com",
                "committed_at": datetime.now(),
                "planner_name": "Jane Smith",
                "planner_email": "jane@example.com",
                "planned_at": datetime.now(),
            }
        ]
        mock_engine.search_events.return_value = iter(mock_events)

        self.mock_sqitch.engine_for_target.return_value = mock_engine
        mock_initialized.return_value = True
        mock_has_events.return_value = True

        # Execute command
        with patch("builtins.print") as mock_print:
            result = self.command.execute(["--format", "oneline"])

        assert result == 0
        mock_print.assert_called()

    @patch("sqlitch.commands.log.LogCommand.get_target")
    @patch("sqlitch.commands.log.LogCommand._is_database_initialized")
    def test_execute_not_initialized(self, mock_initialized, mock_get_target):
        """Test execution with uninitialized database."""
        mock_target = Mock()
        mock_target.uri = "test://db"
        mock_get_target.return_value = mock_target
        mock_initialized.return_value = False

        result = self.command.execute([])

        assert result == 1
        self.mock_logger.error.assert_called()

    @patch("sqlitch.commands.log.LogCommand.get_target")
    @patch("sqlitch.commands.log.LogCommand._is_database_initialized")
    @patch("sqlitch.commands.log.LogCommand._has_events")
    def test_execute_no_events(
        self, mock_has_events, mock_initialized, mock_get_target
    ):
        """Test execution with no events."""
        mock_target = Mock()
        mock_target.uri = "test://db"
        mock_get_target.return_value = mock_target
        mock_initialized.return_value = True
        mock_has_events.return_value = False

        result = self.command.execute([])

        assert result == 1
        self.mock_logger.error.assert_called()

    def test_is_database_initialized_true(self):
        """Test database initialization check returns True."""
        mock_engine = Mock()
        mock_engine.ensure_registry.return_value = None

        result = self.command._is_database_initialized(mock_engine)

        assert result is True

    def test_is_database_initialized_false(self):
        """Test database initialization check returns False."""
        from sqlitch.core.exceptions import EngineError

        mock_engine = Mock()
        mock_engine.ensure_registry.side_effect = EngineError("Not initialized")

        result = self.command._is_database_initialized(mock_engine)

        assert result is False

    def test_has_events_true(self):
        """Test has events check returns True."""
        mock_engine = Mock()
        mock_engine.search_events.return_value = iter([{"event": "deploy"}])

        result = self.command._has_events(mock_engine)

        assert result is True

    def test_has_events_false(self):
        """Test has events check returns False."""
        mock_engine = Mock()
        mock_engine.search_events.return_value = iter([])

        result = self.command._has_events(mock_engine)

        assert result is False

    def test_has_events_error(self):
        """Test has events check with engine error."""
        from sqlitch.core.exceptions import EngineError

        mock_engine = Mock()
        mock_engine.search_events.side_effect = EngineError("Database error")

        result = self.command._has_events(mock_engine)

        assert result is False

    @patch("builtins.print")
    def test_display_header(self, mock_print):
        """Test header display."""
        mock_target = Mock()
        mock_target.uri = "test://database"

        self.command._display_header(mock_target)

        mock_print.assert_called_once_with("On database test://database")


class TestItemFormatter:
    """Test cases for ItemFormatter."""

    def setup_method(self):
        """Set up test fixtures."""
        self.formatter = ItemFormatter()
        self.sample_event = {
            "event": "deploy",
            "change_id": "abc123def456",
            "change": "add_users_table",
            "project": "myproject",
            "note": "Add users table\n\nThis adds the main users table.",
            "requires": ["initial_schema"],
            "conflicts": [],
            "tags": ["v1.0"],
            "committer_name": "John Doe",
            "committer_email": "john@example.com",
            "committed_at": datetime(2023, 1, 15, 10, 30, 0),
            "planner_name": "Jane Smith",
            "planner_email": "jane@example.com",
            "planned_at": datetime(2023, 1, 14, 15, 45, 0),
        }

    def test_init_defaults(self):
        """Test ItemFormatter initialization with defaults."""
        formatter = ItemFormatter()

        assert formatter.date_format == "iso"
        assert formatter.color == "auto"
        assert formatter.abbrev == 0

    def test_init_custom(self):
        """Test ItemFormatter initialization with custom values."""
        formatter = ItemFormatter(date_format="raw", color="always", abbrev=8)

        assert formatter.date_format == "raw"
        assert formatter.color == "always"
        assert formatter.abbrev == 8

    def test_format_basic_codes(self):
        """Test basic format code replacement."""
        template = "%e %n %o"
        result = self.formatter.format(template, self.sample_event)

        assert "deploy" in result
        assert "add_users_table" in result
        assert "myproject" in result

    def test_format_change_id_codes(self):
        """Test change ID format codes."""
        template = "%H %h"
        result = self.formatter.format(template, self.sample_event)

        assert "abc123def456" in result
        assert "abc123def456" in result  # No abbreviation by default

    def test_format_change_id_abbreviated(self):
        """Test abbreviated change ID."""
        formatter = ItemFormatter(abbrev=6)
        template = "%h"
        result = formatter.format(template, self.sample_event)

        assert result == "abc123"

    def test_format_note_codes(self):
        """Test note format codes."""
        template = "%s|%b|%B"
        result = self.formatter.format(template, self.sample_event)

        parts = result.split("|")
        assert parts[0] == "Add users table"  # Subject
        assert "This adds the main users table." in parts[1]  # Body
        assert parts[2] == self.sample_event["note"]  # Full note

    def test_format_person_codes(self):
        """Test person format codes."""
        template = "%c %p"
        result = self.formatter.format(template, self.sample_event)

        assert "John Doe <john@example.com>" in result
        assert "Jane Smith <jane@example.com>" in result

    def test_format_array_codes(self):
        """Test array format codes."""
        template = "%t %r %x"
        result = self.formatter.format(template, self.sample_event)

        assert " v1.0" in result  # Tags
        assert " initial_schema" in result  # Requires
        # Conflicts should be empty

    def test_format_date_codes(self):
        """Test date format codes."""
        template = "%{date}c %{date}p"
        result = self.formatter.format(template, self.sample_event)

        assert "2023-01-15 10:30:00" in result
        assert "2023-01-14 15:45:00" in result

    def test_format_color_codes_no_color(self):
        """Test color codes with no color."""
        formatter = ItemFormatter(color="never")
        template = "%{:event}C%e%{reset}C"
        result = formatter.format(template, self.sample_event)

        assert result == "deploy"

    def test_format_color_codes_with_color(self):
        """Test color codes with color enabled."""
        formatter = ItemFormatter(color="always")
        template = "%{:event}C%e%{reset}C"
        result = formatter.format(template, self.sample_event)

        # Should contain ANSI color codes
        assert "\033[32m" in result  # Green for deploy
        assert "\033[0m" in result  # Reset
        assert "deploy" in result

    def test_get_event_label(self):
        """Test event label generation."""
        assert self.formatter._get_event_label("deploy") == "Deploy"
        assert self.formatter._get_event_label("revert") == "Revert"
        assert self.formatter._get_event_label("fail") == "Fail"

    def test_get_event_label_lower(self):
        """Test lowercase event label generation."""
        assert self.formatter._get_event_label_lower("DEPLOY") == "deploy"
        assert self.formatter._get_event_label_lower("Revert") == "revert"

    def test_get_subject_empty_note(self):
        """Test subject extraction from empty note."""
        assert self.formatter._get_subject("") == ""
        assert self.formatter._get_subject(None) == ""

    def test_get_subject_single_line(self):
        """Test subject extraction from single line note."""
        assert self.formatter._get_subject("Single line") == "Single line"

    def test_get_subject_multi_line(self):
        """Test subject extraction from multi-line note."""
        note = "First line\nSecond line\nThird line"
        assert self.formatter._get_subject(note) == "First line"

    def test_get_body_empty_note(self):
        """Test body extraction from empty note."""
        assert self.formatter._get_body("") == ""
        assert self.formatter._get_body(None) == ""

    def test_get_body_single_line(self):
        """Test body extraction from single line note."""
        assert self.formatter._get_body("Single line") == ""

    def test_get_body_multi_line(self):
        """Test body extraction from multi-line note."""
        note = "First line\nSecond line\nThird line"
        expected = "Second line\nThird line"
        assert self.formatter._get_body(note) == expected

    def test_format_date_iso(self):
        """Test ISO date formatting."""
        date = datetime(2023, 1, 15, 10, 30, 0)
        result = self.formatter._format_date(date, "iso")

        assert result == "2023-01-15 10:30:00"

    def test_format_date_raw(self):
        """Test raw date formatting."""
        date = datetime(2023, 1, 15, 10, 30, 0)
        result = self.formatter._format_date(date, "raw")

        assert "2023-01-15 10:30:00" in result

    def test_format_date_short(self):
        """Test short date formatting."""
        date = datetime(2023, 1, 15, 10, 30, 0)
        result = self.formatter._format_date(date, "short")

        assert result == "2023-01-15"

    def test_format_date_custom(self):
        """Test custom date formatting."""
        date = datetime(2023, 1, 15, 10, 30, 0)
        result = self.formatter._format_date(date, "%Y-%m")

        assert result == "2023-01"

    def test_format_date_string_input(self):
        """Test date formatting with string input."""
        date_str = "2023-01-15T10:30:00"
        result = self.formatter._format_date(date_str, "iso")

        assert "2023-01-15 10:30:00" in result

    def test_format_date_invalid_input(self):
        """Test date formatting with invalid input."""
        result = self.formatter._format_date("invalid", "iso")

        assert result == "invalid"

    def test_predefined_formats_exist(self):
        """Test that all predefined formats exist."""
        expected_formats = ["raw", "full", "long", "medium", "short", "oneline"]

        for format_name in expected_formats:
            assert format_name in FORMATS
            assert isinstance(FORMATS[format_name], str)
            assert len(FORMATS[format_name]) > 0
