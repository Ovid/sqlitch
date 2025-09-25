"""
Unit tests for the revert command.

This module tests the RevertCommand class functionality including argument parsing,
change determination, confirmation prompts, and revert execution.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.commands.revert import RevertCommand
from sqlitch.core.change import Change, Dependency
from sqlitch.core.config import Config
from sqlitch.core.exceptions import DeploymentError, PlanError, SqlitchError
from sqlitch.core.plan import Plan, Tag
from sqlitch.core.sqitch import Sqitch


@pytest.fixture
def mock_sqitch():
    """Create mock Sqitch instance."""
    sqitch = Mock(spec=Sqitch)
    sqitch.verbosity = 0
    sqitch.get_plan_file.return_value = Path("sqitch.plan")
    sqitch.config = Mock()
    sqitch.logger = Mock()
    return sqitch


@pytest.fixture
def revert_command(mock_sqitch):
    """Create RevertCommand instance."""
    return RevertCommand(mock_sqitch)


@pytest.fixture
def sample_changes():
    """Create sample changes for testing."""
    changes = []

    # Create changes with proper timestamps
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    for i, name in enumerate(["initial", "users", "posts"]):
        change = Change(
            name=name,
            note=f"Add {name}",
            timestamp=base_time.replace(day=i + 1),
            planner_name="Test User",
            planner_email="test@example.com",
            dependencies=[],
            tags=[],
        )
        changes.append(change)

    return changes


@pytest.fixture
def sample_plan(sample_changes):
    """Create sample plan for testing."""
    plan = Mock(spec=Plan)
    plan.changes = sample_changes
    plan.project_name = "test_project"

    # Mock tag lookup
    tag = Tag(
        name="v1.0",
        timestamp=datetime(2023, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
        note="Version 1.0",
        planner_name="Test User",
        planner_email="test@example.com",
        change=sample_changes[1] if len(sample_changes) > 1 else None,
    )
    plan.get_tag.return_value = tag

    return plan


class TestRevertCommandArgumentParsing:
    """Test argument parsing functionality."""

    def test_parse_args_defaults(self, revert_command):
        """Test default argument parsing."""
        options = revert_command._parse_args([])

        assert options["target"] is None
        assert options["plan_file"] is None
        assert options["to_change"] is None
        assert options["mode"] == "all"
        assert options["no_prompt"] is False
        assert options["prompt_accept"] is True
        assert options["log_only"] is False
        assert options["lock_timeout"] is None
        assert options["revert_dir"] is None
        assert options["modified"] is False
        assert options["strict"] is False

    def test_parse_args_target(self, revert_command):
        """Test target argument parsing."""
        options = revert_command._parse_args(["--target", "production"])
        assert options["target"] == "production"

    def test_parse_args_plan_file(self, revert_command):
        """Test plan file argument parsing."""
        options = revert_command._parse_args(["--plan-file", "custom.plan"])
        assert options["plan_file"] == Path("custom.plan")

    def test_parse_args_to_change(self, revert_command):
        """Test to-change argument parsing."""
        options = revert_command._parse_args(["--to-change", "users"])
        assert options["to_change"] == "users"
        assert options["mode"] == "change"

        # Test alias
        options = revert_command._parse_args(["--to", "posts"])
        assert options["to_change"] == "posts"
        assert options["mode"] == "change"

    def test_parse_args_to_tag(self, revert_command):
        """Test to-tag argument parsing."""
        options = revert_command._parse_args(["--to-tag", "v1.0"])
        assert options["to_change"] == "v1.0"
        assert options["mode"] == "tag"

    def test_parse_args_no_prompt(self, revert_command):
        """Test no-prompt argument parsing."""
        options = revert_command._parse_args(["-y"])
        assert options["no_prompt"] is True

        options = revert_command._parse_args(["--no-prompt"])
        assert options["no_prompt"] is True

    def test_parse_args_log_only(self, revert_command):
        """Test log-only argument parsing."""
        options = revert_command._parse_args(["--log-only"])
        assert options["log_only"] is True

    def test_parse_args_lock_timeout(self, revert_command):
        """Test lock timeout argument parsing."""
        options = revert_command._parse_args(["--lock-timeout", "30"])
        assert options["lock_timeout"] == 30

    def test_parse_args_modified(self, revert_command):
        """Test modified argument parsing."""
        options = revert_command._parse_args(["-m"])
        assert options["modified"] is True

        options = revert_command._parse_args(["--modified"])
        assert options["modified"] is True

    def test_parse_args_strict(self, revert_command):
        """Test strict argument parsing."""
        options = revert_command._parse_args(["--strict"])
        assert options["strict"] is True

    def test_parse_args_positional_change(self, revert_command):
        """Test positional change argument."""
        options = revert_command._parse_args(["users"])
        assert options["to_change"] == "users"
        assert options["mode"] == "change"

    def test_parse_args_invalid_option(self, revert_command):
        """Test invalid option handling."""
        with pytest.raises(SqlitchError, match="Unknown option: --invalid"):
            revert_command._parse_args(["--invalid"])

    def test_parse_args_missing_value(self, revert_command):
        """Test missing value handling."""
        with pytest.raises(SqlitchError, match="--target requires a value"):
            revert_command._parse_args(["--target"])

    def test_parse_args_invalid_lock_timeout(self, revert_command):
        """Test invalid lock timeout handling."""
        with pytest.raises(SqlitchError, match="--lock-timeout must be an integer"):
            revert_command._parse_args(["--lock-timeout", "invalid"])


class TestRevertCommandPlanLoading:
    """Test plan loading functionality."""

    def test_load_plan_default(self, revert_command, mock_sqitch):
        """Test loading default plan file."""
        plan_file = Path("sqitch.plan")
        mock_sqitch.get_plan_file.return_value = plan_file

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("sqlitch.core.plan.Plan.from_file") as mock_from_file,
        ):

            mock_plan = Mock(spec=Plan)
            mock_from_file.return_value = mock_plan

            result = revert_command._load_plan()

            assert result == mock_plan
            mock_from_file.assert_called_once_with(plan_file)

    def test_load_plan_custom_file(self, revert_command):
        """Test loading custom plan file."""
        plan_file = Path("custom.plan")

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("sqlitch.core.plan.Plan.from_file") as mock_from_file,
        ):

            mock_plan = Mock(spec=Plan)
            mock_from_file.return_value = mock_plan

            result = revert_command._load_plan(plan_file)

            assert result == mock_plan
            mock_from_file.assert_called_once_with(plan_file)

    def test_load_plan_file_not_found(self, revert_command):
        """Test plan file not found error."""
        plan_file = Path("missing.plan")

        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(PlanError, match="Plan file not found: missing.plan"):
                revert_command._load_plan(plan_file)

    def test_load_plan_parse_error(self, revert_command):
        """Test plan file parse error."""
        plan_file = Path("sqitch.plan")

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch(
                "sqlitch.core.plan.Plan.from_file", side_effect=Exception("Parse error")
            ),
        ):

            with pytest.raises(
                PlanError, match="Failed to load plan file.*Parse error"
            ):
                revert_command._load_plan(plan_file)


class TestRevertCommandChangeDetermination:
    """Test change determination functionality."""

    def test_determine_changes_no_deployed(self, revert_command, sample_plan):
        """Test when no changes are deployed."""
        mock_engine = Mock()
        mock_engine.get_deployed_changes.return_value = []

        options = {"log_only": False}

        changes = revert_command._determine_changes_to_revert(
            mock_engine, sample_plan, options
        )

        assert changes == []

    def test_determine_changes_all_deployed(
        self, revert_command, sample_plan, sample_changes
    ):
        """Test reverting all deployed changes."""
        mock_engine = Mock()
        mock_engine.get_deployed_changes.return_value = [
            change.id for change in sample_changes
        ]

        options = {"log_only": False}

        changes = revert_command._determine_changes_to_revert(
            mock_engine, sample_plan, options
        )

        # Should return changes in reverse order
        expected = list(reversed(sample_changes))
        assert changes == expected

    def test_determine_changes_to_specific_change(
        self, revert_command, sample_plan, sample_changes
    ):
        """Test reverting to specific change."""
        mock_engine = Mock()
        mock_engine.get_deployed_changes.return_value = [
            change.id for change in sample_changes
        ]

        options = {"log_only": False, "to_change": "users", "mode": "change"}

        changes = revert_command._determine_changes_to_revert(
            mock_engine, sample_plan, options
        )

        # Should revert only 'posts' (keeping 'users' and earlier)
        assert len(changes) == 1
        assert changes[0].name == "posts"

    def test_determine_changes_to_tag(
        self, revert_command, sample_plan, sample_changes
    ):
        """Test reverting to specific tag."""
        mock_engine = Mock()
        mock_engine.get_deployed_changes.return_value = [
            change.id for change in sample_changes
        ]

        options = {"log_only": False, "to_change": "v1.0", "mode": "tag"}

        changes = revert_command._determine_changes_to_revert(
            mock_engine, sample_plan, options
        )

        # Should revert 'posts' (keeping changes up to tag)
        assert len(changes) == 1
        assert changes[0].name == "posts"

    def test_determine_changes_log_only_mode(
        self, revert_command, sample_plan, sample_changes
    ):
        """Test log-only mode."""
        mock_engine = Mock()
        # Engine should not be called in log-only mode

        options = {"log_only": True}

        changes = revert_command._determine_changes_to_revert(
            mock_engine, sample_plan, options
        )

        # Should assume all changes are deployed and return in reverse order
        expected = list(reversed(sample_changes))
        assert changes == expected
        mock_engine.get_deployed_changes.assert_not_called()

    def test_determine_changes_strict_mode_no_target(
        self, revert_command, sample_plan, sample_changes
    ):
        """Test strict mode without target."""
        mock_engine = Mock()
        mock_engine.get_deployed_changes.return_value = [
            change.id for change in sample_changes
        ]

        options = {"log_only": False, "strict": True}

        with pytest.raises(
            SqlitchError, match="Must specify a target revision in strict mode"
        ):
            revert_command._determine_changes_to_revert(
                mock_engine, sample_plan, options
            )

    def test_get_changes_to_revert_to_change_not_found(
        self, revert_command, sample_plan, sample_changes
    ):
        """Test reverting to non-existent change."""
        with pytest.raises(SqlitchError, match="Change not found in plan: nonexistent"):
            revert_command._get_changes_to_revert_to_change(
                sample_plan, sample_changes, "nonexistent"
            )

    def test_get_changes_to_revert_to_change_not_deployed(
        self, revert_command, sample_plan, sample_changes
    ):
        """Test reverting to non-deployed change."""
        # Only first change is deployed
        deployed_changes = [sample_changes[0]]

        with pytest.raises(SqlitchError, match="Target change is not deployed: users"):
            revert_command._get_changes_to_revert_to_change(
                sample_plan, deployed_changes, "users"
            )

    def test_get_changes_to_revert_to_tag_not_found(
        self, revert_command, sample_plan, sample_changes
    ):
        """Test reverting to non-existent tag."""
        sample_plan.get_tag.return_value = None

        with pytest.raises(SqlitchError, match="Tag not found in plan: nonexistent"):
            revert_command._get_changes_to_revert_to_tag(
                sample_plan, sample_changes, "nonexistent"
            )


class TestRevertCommandExecution:
    """Test revert execution functionality."""

    def test_revert_changes_success(self, revert_command, sample_changes):
        """Test successful revert execution."""
        mock_engine = Mock()
        mock_engine.revert_change.return_value = None

        options = {"log_only": False, "no_prompt": True}

        # Take only first change for simplicity
        changes_to_revert = [sample_changes[0]]

        result = revert_command._revert_changes(mock_engine, changes_to_revert, options)

        assert result == 0
        mock_engine.revert_change.assert_called_once_with(sample_changes[0])

    def test_revert_changes_log_only(self, revert_command, sample_changes):
        """Test log-only revert execution."""
        mock_engine = Mock()

        options = {"log_only": True}
        changes_to_revert = [sample_changes[0]]

        with patch.object(
            revert_command, "_log_revert_plan", return_value=0
        ) as mock_log:
            result = revert_command._revert_changes(
                mock_engine, changes_to_revert, options
            )

            assert result == 0
            mock_log.assert_called_once_with(changes_to_revert)
            mock_engine.revert_change.assert_not_called()

    def test_revert_changes_empty_list(self, revert_command):
        """Test reverting empty change list."""
        mock_engine = Mock()
        options = {"log_only": False}

        result = revert_command._revert_changes(mock_engine, [], options)

        assert result == 0
        mock_engine.revert_change.assert_not_called()

    def test_revert_changes_with_confirmation(self, revert_command, sample_changes):
        """Test revert with user confirmation."""
        mock_engine = Mock()

        options = {"log_only": False, "no_prompt": False}

        changes_to_revert = [sample_changes[0]]

        with patch.object(revert_command, "_confirm_revert", return_value=True):
            result = revert_command._revert_changes(
                mock_engine, changes_to_revert, options
            )

            assert result == 0
            mock_engine.revert_change.assert_called_once()

    def test_revert_changes_confirmation_declined(self, revert_command, sample_changes):
        """Test revert when user declines confirmation."""
        mock_engine = Mock()

        options = {"log_only": False, "no_prompt": False}

        changes_to_revert = [sample_changes[0]]

        with patch.object(revert_command, "_confirm_revert", return_value=False):
            result = revert_command._revert_changes(
                mock_engine, changes_to_revert, options
            )

            assert result == 0
            mock_engine.revert_change.assert_not_called()

    def test_revert_changes_failure(self, revert_command, sample_changes):
        """Test revert failure handling."""
        mock_engine = Mock()
        mock_engine.revert_change.side_effect = DeploymentError("Revert failed")

        options = {"log_only": False, "no_prompt": True}

        changes_to_revert = [sample_changes[0]]

        result = revert_command._revert_changes(mock_engine, changes_to_revert, options)

        assert result == 1

    def test_revert_changes_keyboard_interrupt(self, revert_command, sample_changes):
        """Test keyboard interrupt handling."""
        mock_engine = Mock()
        mock_engine.revert_change.side_effect = KeyboardInterrupt()

        options = {"log_only": False, "no_prompt": True}

        changes_to_revert = [sample_changes[0]]

        result = revert_command._revert_changes(mock_engine, changes_to_revert, options)

        assert result == 130


class TestRevertCommandConfirmation:
    """Test confirmation functionality."""

    def test_confirm_revert_single_change(self, revert_command, sample_changes):
        """Test confirmation for single change."""
        with patch("builtins.input", return_value="y"):
            result = revert_command._confirm_revert([sample_changes[0]], {})
            assert result is True

        with patch("builtins.input", return_value="n"):
            result = revert_command._confirm_revert([sample_changes[0]], {})
            assert result is False

    def test_confirm_revert_multiple_changes(self, revert_command, sample_changes):
        """Test confirmation for multiple changes."""
        with patch("builtins.input", return_value="yes"):
            result = revert_command._confirm_revert(sample_changes, {})
            assert result is True

        with patch("builtins.input", return_value="no"):
            result = revert_command._confirm_revert(sample_changes, {})
            assert result is False

    def test_confirm_revert_eof_error(self, revert_command, sample_changes):
        """Test confirmation with EOF error."""
        with patch("builtins.input", side_effect=EOFError()):
            result = revert_command._confirm_revert([sample_changes[0]], {})
            assert result is False

    def test_confirm_revert_keyboard_interrupt(self, revert_command, sample_changes):
        """Test confirmation with keyboard interrupt."""
        with patch("builtins.input", side_effect=KeyboardInterrupt()):
            result = revert_command._confirm_revert([sample_changes[0]], {})
            assert result is False


class TestRevertCommandLogPlan:
    """Test log plan functionality."""

    def test_log_revert_plan_empty(self, revert_command):
        """Test logging empty revert plan."""
        result = revert_command._log_revert_plan([])
        assert result == 0

    def test_log_revert_plan_single_change(self, revert_command, sample_changes):
        """Test logging single change revert plan."""
        result = revert_command._log_revert_plan([sample_changes[0]])
        assert result == 0

    def test_log_revert_plan_multiple_changes(self, revert_command, sample_changes):
        """Test logging multiple changes revert plan."""
        result = revert_command._log_revert_plan(sample_changes)
        assert result == 0


class TestRevertCommandIntegration:
    """Test full command integration."""

    def test_execute_success(
        self, revert_command, mock_sqitch, sample_plan, sample_changes
    ):
        """Test successful command execution."""
        # Mock all dependencies
        mock_sqitch.get_plan_file.return_value = Path("sqitch.plan")

        mock_engine = Mock()
        mock_engine.get_deployed_changes.return_value = [
            change.id for change in sample_changes
        ]
        mock_engine.revert_change.return_value = None

        with (
            patch.object(revert_command, "require_initialized"),
            patch.object(revert_command, "validate_user_info"),
            patch.object(revert_command, "_load_plan", return_value=sample_plan),
            patch.object(revert_command, "get_target"),
            patch(
                "sqlitch.engines.base.EngineRegistry.create_engine",
                return_value=mock_engine,
            ),
        ):

            result = revert_command.execute(["-y"])  # No prompt

            assert result == 0

    def test_execute_sqlitch_error(self, revert_command):
        """Test SqlitchError handling."""
        with patch.object(
            revert_command,
            "require_initialized",
            side_effect=SqlitchError("Not initialized"),
        ):
            result = revert_command.execute([])
            assert result == 1

    def test_execute_unexpected_error(self, revert_command):
        """Test unexpected error handling."""
        with patch.object(
            revert_command, "require_initialized", side_effect=Exception("Unexpected")
        ):
            result = revert_command.execute([])
            assert result == 2

    def test_show_help(self, revert_command):
        """Test help display."""
        with pytest.raises(SystemExit):
            revert_command._parse_args(["--help"])
