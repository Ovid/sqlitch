"""
Unit tests for the status command.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.commands.status import StatusCommand
from sqlitch.core.change import Change
from sqlitch.core.config import Config
from sqlitch.core.exceptions import EngineError, PlanError, SqlitchError
from sqlitch.core.plan import Plan
from sqlitch.core.sqitch import Sqitch


@pytest.fixture
def mock_sqitch():
    """Create a mock Sqitch instance."""
    sqitch = Mock(spec=Sqitch)
    sqitch.config = Mock(spec=Config)
    sqitch.logger = Mock()
    sqitch.verbosity = 0
    sqitch.get_plan_file.return_value = Path("sqitch.plan")
    sqitch.require_initialized.return_value = None
    return sqitch


@pytest.fixture
def status_command(mock_sqitch):
    """Create a StatusCommand instance."""
    command = StatusCommand(mock_sqitch)
    # Mock the logging methods
    command.info = Mock()
    command.warn = Mock()
    command.error = Mock()
    command.debug = Mock()
    # Mock other methods that will be called
    command.require_initialized = Mock()
    command.get_target = Mock()
    command._load_plan = Mock()
    command._get_current_state = Mock()
    return command


@pytest.fixture
def mock_plan():
    """Create a mock plan."""
    plan = Mock(spec=Plan)
    plan.project_name = "test_project"
    plan.file = Path("sqitch.plan")

    # Create mock changes
    change1 = Mock(spec=Change)
    change1.id = "change1_id"
    change1.name = "change1"
    change1.tags = []

    change2 = Mock(spec=Change)
    change2.id = "change2_id"
    change2.name = "change2"
    change2.tags = ["v1.0"]

    change3 = Mock(spec=Change)
    change3.id = "change3_id"
    change3.name = "change3"
    change3.tags = []

    plan.changes = [change1, change2, change3]
    return plan


@pytest.fixture
def mock_engine():
    """Create a mock engine."""
    engine = Mock()
    engine.ensure_registry.return_value = None
    return engine


@pytest.fixture
def mock_target():
    """Create a mock target."""
    target = Mock()
    target.uri = "postgresql://user@localhost/test"
    return target


class TestStatusCommand:
    """Test cases for StatusCommand."""

    def test_parse_args_default(self, status_command):
        """Test parsing default arguments."""
        status_command.config.get.side_effect = lambda key, default=None: {
            "status.show_changes": False,
            "status.show_tags": False,
            "status.date_format": "iso",
        }.get(key, default)

        options = status_command._parse_args([])

        assert options["target"] is None
        assert options["plan_file"] is None
        assert options["project"] is None
        assert options["show_changes"] is False
        assert options["show_tags"] is False
        assert options["date_format"] == "iso"

    def test_parse_args_with_options(self, status_command):
        """Test parsing arguments with options."""
        status_command.config.get.side_effect = lambda key, default=None: default

        args = [
            "--target",
            "prod",
            "--plan-file",
            "custom.plan",
            "--project",
            "myproject",
            "--show-changes",
            "--show-tags",
            "--date-format",
            "rfc",
        ]

        options = status_command._parse_args(args)

        assert options["target"] == "prod"
        assert options["plan_file"] == Path("custom.plan")
        assert options["project"] == "myproject"
        assert options["show_changes"] is True
        assert options["show_tags"] is True
        assert options["date_format"] == "rfc"

    def test_parse_args_help(self, status_command):
        """Test help option."""
        with patch("builtins.print") as mock_print:
            with pytest.raises(SystemExit):
                status_command._parse_args(["--help"])
            mock_print.assert_called_once()

    def test_parse_args_unknown_option(self, status_command):
        """Test unknown option raises error."""
        with pytest.raises(SqlitchError, match="Unknown option: --unknown"):
            status_command._parse_args(["--unknown"])

    def test_parse_args_unexpected_argument(self, status_command):
        """Test unexpected positional argument raises error."""
        with pytest.raises(SqlitchError, match="Unexpected argument: unexpected"):
            status_command._parse_args(["unexpected"])

    def test_load_plan_default(self, mock_sqitch, mock_plan):
        """Test loading default plan file."""
        # Create command without mocked _load_plan
        command = StatusCommand(mock_sqitch)
        command.info = Mock()
        command.warn = Mock()
        command.error = Mock()
        command.debug = Mock()

        with patch(
            "sqlitch.commands.status.Plan.from_file", return_value=mock_plan
        ) as mock_from_file:
            with patch.object(Path, "exists", return_value=True):
                plan = command._load_plan()

                mock_from_file.assert_called_once_with(mock_sqitch.get_plan_file())
                assert plan == mock_plan

    def test_load_plan_custom_file(self, mock_sqitch, mock_plan):
        """Test loading custom plan file."""
        # Create command without mocked _load_plan
        command = StatusCommand(mock_sqitch)
        command.info = Mock()
        command.warn = Mock()
        command.error = Mock()
        command.debug = Mock()

        custom_file = Path("custom.plan")

        with patch(
            "sqlitch.commands.status.Plan.from_file", return_value=mock_plan
        ) as mock_from_file:
            with patch.object(Path, "exists", return_value=True):
                plan = command._load_plan(custom_file)

                mock_from_file.assert_called_once_with(custom_file)
                assert plan == mock_plan

    def test_load_plan_file_not_found(self, mock_sqitch):
        """Test loading non-existent plan file raises error."""
        # Create command without mocked _load_plan
        command = StatusCommand(mock_sqitch)
        command.info = Mock()
        command.warn = Mock()
        command.error = Mock()
        command.debug = Mock()

        with patch.object(Path, "exists", return_value=False):
            with pytest.raises(PlanError, match="Plan file not found"):
                command._load_plan()

    def test_load_plan_parse_error(self, mock_sqitch):
        """Test plan parsing error."""
        # Create command without mocked _load_plan
        command = StatusCommand(mock_sqitch)
        command.info = Mock()
        command.warn = Mock()
        command.error = Mock()
        command.debug = Mock()

        with patch.object(Path, "exists", return_value=True):
            with patch(
                "sqlitch.commands.status.Plan.from_file",
                side_effect=Exception("Parse error"),
            ):
                with pytest.raises(PlanError, match="Failed to load plan file"):
                    command._load_plan()

    def test_get_current_state_success(self, mock_sqitch, mock_engine):
        """Test getting current state successfully."""
        # Create command without mocked _get_current_state
        command = StatusCommand(mock_sqitch)
        command.info = Mock()
        command.warn = Mock()
        command.error = Mock()
        command.debug = Mock()

        expected_state = {
            "change_id": "test_id",
            "change": "test_change",
            "project": "test_project",
        }
        mock_engine.get_current_state.return_value = expected_state

        state = command._get_current_state(mock_engine)

        assert state == expected_state
        mock_engine.get_current_state.assert_called_once_with(None)

    def test_get_current_state_with_project(self, mock_sqitch, mock_engine):
        """Test getting current state with specific project."""
        # Create command without mocked _get_current_state
        command = StatusCommand(mock_sqitch)
        command.info = Mock()
        command.warn = Mock()
        command.error = Mock()
        command.debug = Mock()

        expected_state = {
            "change_id": "test_id",
            "change": "test_change",
            "project": "custom_project",
        }
        mock_engine.get_current_state.return_value = expected_state

        state = command._get_current_state(mock_engine, "custom_project")

        assert state == expected_state
        mock_engine.get_current_state.assert_called_once_with("custom_project")

    def test_get_current_state_not_initialized(self, mock_sqitch, mock_engine):
        """Test getting current state when database not initialized."""
        # Create command without mocked _get_current_state
        command = StatusCommand(mock_sqitch)
        command.info = Mock()
        command.warn = Mock()
        command.error = Mock()
        command.debug = Mock()

        mock_engine.get_current_state.side_effect = Exception("Not initialized")
        mock_engine._registry_exists_in_db.return_value = False
        mock_engine._create_connection.return_value = Mock()

        with pytest.raises(SqlitchError, match="Database has not been initialized"):
            command._get_current_state(mock_engine)

    def test_emit_state_basic(self, status_command):
        """Test emitting basic state information."""
        state = {
            "project": "test_project",
            "change_id": "abc123",
            "change": "test_change",
            "tags": [],
            "committed_at": datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "committer_name": "John Doe",
            "committer_email": "john@example.com",
        }
        options = {"date_format": "iso"}

        status_command._emit_state(state, options)

        # Verify info calls
        expected_calls = [
            "Project:  test_project",
            "Change:   abc123",
            "Name:     test_change",
            "Deployed: 2023-01-15T10:30:00+00:00",
            "By:       John Doe <john@example.com>",
        ]

        for expected_call in expected_calls:
            status_command.info.assert_any_call(expected_call)

    def test_emit_state_with_tags(self, status_command):
        """Test emitting state with tags."""
        state = {
            "project": "test_project",
            "change_id": "abc123",
            "change": "test_change",
            "tags": ["v1.0", "stable"],
            "committed_at": datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "committer_name": "John Doe",
            "committer_email": "john@example.com",
        }
        options = {"date_format": "iso"}

        status_command._emit_state(state, options)

        status_command.info.assert_any_call("Tags:     v1.0, stable")

    def test_emit_state_single_tag(self, status_command):
        """Test emitting state with single tag."""
        state = {
            "project": "test_project",
            "change_id": "abc123",
            "change": "test_change",
            "tags": ["v1.0"],
            "committed_at": datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "committer_name": "John Doe",
            "committer_email": "john@example.com",
        }
        options = {"date_format": "iso"}

        status_command._emit_state(state, options)

        status_command.info.assert_any_call("Tag:     v1.0")

    def test_emit_state_rfc_date_format(self, status_command):
        """Test emitting state with RFC date format."""
        state = {
            "project": "test_project",
            "change_id": "abc123",
            "change": "test_change",
            "tags": [],
            "committed_at": datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "committer_name": "John Doe",
            "committer_email": "john@example.com",
        }
        options = {"date_format": "rfc"}

        status_command._emit_state(state, options)

        status_command.info.assert_any_call("Deployed: Sun, 15 Jan 2023 10:30:00 +0000")

    def test_emit_changes_none(self, status_command, mock_engine):
        """Test emitting changes when none exist."""
        mock_engine.get_current_changes.return_value = iter([])

        status_command._emit_changes(mock_engine, None, {})

        status_command.info.assert_any_call("")
        status_command.info.assert_any_call("Changes: None.")

    def test_emit_changes_single(self, status_command, mock_engine):
        """Test emitting single change."""
        changes = [
            {
                "change": "test_change",
                "committed_at": datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
                "committer_name": "John Doe",
                "committer_email": "john@example.com",
            }
        ]
        mock_engine.get_current_changes.return_value = iter(changes)

        status_command._emit_changes(mock_engine, None, {"date_format": "iso"})

        status_command.info.assert_any_call("Change:")
        status_command.info.assert_any_call(
            "  test_change - 2023-01-15T10:30:00+00:00 - John Doe <john@example.com>"
        )

    def test_emit_changes_multiple(self, status_command, mock_engine):
        """Test emitting multiple changes."""
        changes = [
            {
                "change": "change1",
                "committed_at": datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
                "committer_name": "John Doe",
                "committer_email": "john@example.com",
            },
            {
                "change": "longer_change_name",
                "committed_at": datetime(2023, 1, 16, 11, 30, 0, tzinfo=timezone.utc),
                "committer_name": "Jane Smith",
                "committer_email": "jane@example.com",
            },
        ]
        mock_engine.get_current_changes.return_value = iter(changes)

        status_command._emit_changes(mock_engine, None, {"date_format": "iso"})

        status_command.info.assert_any_call("Changes:")
        # Check alignment padding
        status_command.info.assert_any_call(
            "  change1            - 2023-01-15T10:30:00+00:00 - John Doe <john@example.com>"
        )
        status_command.info.assert_any_call(
            "  longer_change_name - 2023-01-16T11:30:00+00:00 - Jane Smith <jane@example.com>"
        )

    def test_emit_tags_none(self, status_command, mock_engine):
        """Test emitting tags when none exist."""
        mock_engine.get_current_tags.return_value = iter([])

        status_command._emit_tags(mock_engine, None, {})

        status_command.info.assert_any_call("")
        status_command.info.assert_any_call("Tags: None.")

    def test_emit_tags_single(self, status_command, mock_engine):
        """Test emitting single tag."""
        tags = [
            {
                "tag": "v1.0",
                "committed_at": datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
                "committer_name": "John Doe",
                "committer_email": "john@example.com",
            }
        ]
        mock_engine.get_current_tags.return_value = iter(tags)

        status_command._emit_tags(mock_engine, None, {"date_format": "iso"})

        status_command.info.assert_any_call("Tag:")
        status_command.info.assert_any_call(
            "  v1.0 - 2023-01-15T10:30:00+00:00 - John Doe <john@example.com>"
        )

    def test_emit_status_up_to_date(self, status_command, mock_plan):
        """Test emitting status when up to date."""
        state = {"change_id": "change3_id"}  # Last change in plan

        status_command._emit_status(state, mock_plan, {})

        status_command.info.assert_any_call("")
        status_command.info.assert_any_call("Nothing to deploy (up-to-date)")

    def test_emit_status_undeployed_changes(self, status_command, mock_plan):
        """Test emitting status with undeployed changes."""
        state = {"change_id": "change1_id"}  # First change in plan

        status_command._emit_status(state, mock_plan, {})

        status_command.info.assert_any_call("")
        status_command.info.assert_any_call("Undeployed changes:")
        status_command.info.assert_any_call("  * change2 @v1.0")
        status_command.info.assert_any_call("  * change3")

    def test_emit_status_single_undeployed_change(self, status_command, mock_plan):
        """Test emitting status with single undeployed change."""
        state = {"change_id": "change2_id"}  # Second change in plan

        status_command._emit_status(state, mock_plan, {})

        status_command.info.assert_any_call("Undeployed change:")
        status_command.info.assert_any_call("  * change3")

    def test_emit_status_change_not_found(self, status_command, mock_plan):
        """Test emitting status when current change not found in plan."""
        state = {"change_id": "unknown_change_id"}

        status_command._emit_status(state, mock_plan, {})

        status_command.warn.assert_called_once_with(
            f"Cannot find this change in {mock_plan.file}"
        )
        status_command.error.assert_called_once_with(
            "Make sure you are connected to the proper database for this project."
        )

    def test_format_change_name_with_tags(self, status_command):
        """Test formatting change name with tags."""
        change = Mock(spec=Change)
        change.name = "test_change"
        change.tags = ["v1.0", "stable"]

        result = status_command._format_change_name_with_tags(change)

        assert result == "test_change @v1.0 @stable"

    def test_format_change_name_without_tags(self, status_command):
        """Test formatting change name without tags."""
        change = Mock(spec=Change)
        change.name = "test_change"
        change.tags = []

        result = status_command._format_change_name_with_tags(change)

        assert result == "test_change"

    def test_execute_success(self, status_command, mock_plan, mock_engine, mock_target):
        """Test successful execution."""
        with patch("sqlitch.commands.status.EngineRegistry") as mock_registry:
            # Setup mocks
            status_command.get_target.return_value = mock_target
            status_command._load_plan.return_value = mock_plan
            mock_registry.create_engine.return_value = mock_engine

            current_state = {
                "change_id": "change1_id",
                "change": "change1",
                "project": "test_project",
                "tags": [],
                "committed_at": datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
                "committer_name": "John Doe",
                "committer_email": "john@example.com",
            }
            status_command._get_current_state.return_value = current_state

            # Mock other methods
            status_command._emit_state = Mock()
            status_command._emit_changes = Mock()
            status_command._emit_tags = Mock()
            status_command._emit_status = Mock()

            result = status_command.execute([])

            assert result == 0
            status_command.require_initialized.assert_called_once()
            mock_engine.ensure_registry.assert_called_once()
            status_command._emit_state.assert_called_once()
            status_command._emit_status.assert_called_once()

    def test_execute_no_changes_deployed(
        self, status_command, mock_plan, mock_engine, mock_target
    ):
        """Test execution when no changes are deployed."""
        with patch("sqlitch.commands.status.EngineRegistry") as mock_registry:
            # Setup mocks
            status_command.get_target.return_value = mock_target
            status_command._load_plan.return_value = mock_plan
            status_command._get_current_state.return_value = None
            mock_registry.create_engine.return_value = mock_engine

            result = status_command.execute([])

            assert result == 1
            status_command.error.assert_called_once_with("No changes deployed")

    def test_execute_sqlitch_error(self, status_command):
        """Test execution with SqlitchError."""
        status_command.require_initialized.side_effect = SqlitchError("Test error")

        result = status_command.execute([])

        assert result == 1
        status_command.error.assert_called_once_with("sqlitch: Test error")

    def test_execute_unexpected_error(self, status_command):
        """Test execution with unexpected error."""
        status_command.require_initialized.side_effect = Exception("Unexpected error")

        result = status_command.execute([])

        assert result == 2
        status_command.error.assert_called_once_with(
            "Unexpected error: Unexpected error"
        )
