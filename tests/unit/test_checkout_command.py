"""
Unit tests for checkout command.
"""

import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.commands.checkout import CheckoutCommand
from sqlitch.core.change import Change
from sqlitch.core.config import Config
from sqlitch.core.exceptions import PlanError, SqlitchError
from sqlitch.core.plan import Plan
from sqlitch.core.sqitch import Sqitch
from sqlitch.utils.git import VCSError


@pytest.fixture
def mock_sqitch():
    """Create mock Sqitch instance."""
    sqitch = Mock(spec=Sqitch)
    sqitch.config = Mock(spec=Config)
    sqitch.config.get.return_value = None
    sqitch.logger = Mock()
    sqitch.verbosity = 0
    return sqitch


@pytest.fixture
def checkout_command(mock_sqitch):
    """Create CheckoutCommand instance."""
    return CheckoutCommand(mock_sqitch)


@pytest.fixture
def sample_change():
    """Create sample change."""
    return Change(
        name="users",
        note="Add users table",
        timestamp=datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
        planner_name="John Doe",
        planner_email="john@example.com",
        tags=["alpha"],
    )


@pytest.fixture
def sample_plan(sample_change):
    """Create sample plan."""
    plan = Plan(file=Path("sqitch.plan"), project="test_project")
    plan.changes = [sample_change]
    plan._build_indexes()
    return plan


class TestCheckoutCommand:
    """Test CheckoutCommand class."""

    def test_init(self, mock_sqitch):
        """Test command initialization."""
        with patch.object(CheckoutCommand, "_get_git_client", return_value="git"):
            command = CheckoutCommand(mock_sqitch)
            assert command.sqitch == mock_sqitch
            assert command.git_client == "git"

    def test_get_git_client_default(self, checkout_command):
        """Test default git client detection."""
        checkout_command.config.get.return_value = None

        client = checkout_command._get_git_client()
        # Should return the actual git executable path
        import shutil

        expected_git = shutil.which("git")
        assert client == expected_git

    def test_get_git_client_windows(self, checkout_command):
        """Test git client detection on Windows."""
        checkout_command.config.get.return_value = None

        # The method now uses shutil.which() regardless of platform
        client = checkout_command._get_git_client()
        import shutil

        expected_git = shutil.which("git")
        assert client == expected_git

    def test_get_git_client_configured(self, checkout_command):
        """Test configured git client."""
        checkout_command.config.get.return_value = "/usr/local/bin/git"

        client = checkout_command._get_git_client()
        assert client == "/usr/local/bin/git"

    def test_parse_args_basic(self, checkout_command):
        """Test basic argument parsing."""
        args = ["main"]
        options = checkout_command._parse_args(args)

        assert options["branch"] == "main"
        assert options["target"] is None
        assert options["mode"] == "all"
        assert not options["verify"]
        assert not options["no_prompt"]
        assert options["prompt_accept"]

    def test_parse_args_with_options(self, checkout_command):
        """Test argument parsing with options."""
        args = [
            "--target",
            "db:pg://localhost/test",
            "--mode",
            "tag",
            "--verify",
            "--set",
            "foo=bar",
            "--set-deploy",
            "deploy_var=value",
            "--set-revert",
            "revert_var=other",
            "--log-only",
            "--lock-timeout",
            "120",
            "-y",
            "feature-branch",
        ]

        options = checkout_command._parse_args(args)

        assert options["branch"] == "feature-branch"
        assert options["target"] == "db:pg://localhost/test"
        assert options["mode"] == "tag"
        assert options["verify"]
        assert options["no_prompt"]
        assert options["log_only"]
        assert options["lock_timeout"] == 120
        assert options["deploy_variables"] == {"foo": "bar", "deploy_var": "value"}
        assert options["revert_variables"] == {"foo": "bar", "revert_var": "other"}

    def test_parse_args_invalid_mode(self, checkout_command):
        """Test parsing with invalid mode."""
        args = ["--mode", "invalid", "main"]

        with pytest.raises(SqlitchError, match="Invalid mode: invalid"):
            checkout_command._parse_args(args)

    def test_parse_args_invalid_variable(self, checkout_command):
        """Test parsing with invalid variable format."""
        args = ["--set", "invalid_format", "main"]

        with pytest.raises(SqlitchError, match="Invalid variable format"):
            checkout_command._parse_args(args)

    def test_parse_args_missing_branch(self, checkout_command):
        """Test parsing without branch argument."""
        args = []
        options = checkout_command._parse_args(args)

        assert options["branch"] is None

    def test_parse_args_multiple_targets_warning(self, checkout_command):
        """Test warning for multiple targets."""
        args = ["main", "target1", "target2"]

        with patch.object(checkout_command, "warn") as mock_warn:
            options = checkout_command._parse_args(args)

            assert options["branch"] == "main"
            assert options["target"] == "target1"
            mock_warn.assert_called_once_with(
                "Too many targets specified; connecting to target1"
            )

    def test_get_config_defaults(self, checkout_command):
        """Test configuration defaults."""

        def mock_get(key, as_bool=False, **kwargs):
            values = {
                "checkout.verify": True,
                "checkout.mode": "change",
                "checkout.no_prompt": True,
                "checkout.prompt_accept": False,
            }
            return values.get(key)

        checkout_command.config.get.side_effect = mock_get

        defaults = checkout_command._get_config_defaults()

        assert defaults["verify"]
        assert defaults["mode"] == "change"
        assert defaults["no_prompt"]
        assert not defaults["prompt_accept"]

    def test_get_config_defaults_strict_mode(self, checkout_command):
        """Test strict mode configuration."""

        def mock_get(key, as_bool=False, **kwargs):
            if as_bool and key == "checkout.strict":
                return True
            return None

        checkout_command.config.get.side_effect = mock_get

        with pytest.raises(SqlitchError, match="cannot be used in strict mode"):
            checkout_command._get_config_defaults()

    def test_get_current_branch(self, checkout_command):
        """Test getting current branch."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.stdout = "main\n"
            mock_run.return_value.returncode = 0

            branch = checkout_command._get_current_branch()
            assert branch == "main"

            git_exe = shutil.which("git")
            mock_run.assert_called_once_with(
                [git_exe, "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )

    def test_get_current_branch_error(self, checkout_command):
        """Test error getting current branch."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "git")

            with pytest.raises(VCSError, match="Failed to get current branch"):
                checkout_command._get_current_branch()

    def test_load_plan_default(self, checkout_command):
        """Test loading default plan file."""
        checkout_command.config.get.return_value = "sqitch.plan"

        with (
            patch("pathlib.Path.exists", return_value=True),
            patch.object(Plan, "from_file") as mock_from_file,
        ):
            mock_plan = Mock(spec=Plan)
            mock_from_file.return_value = mock_plan

            plan = checkout_command._load_plan()

            assert plan == mock_plan
            mock_from_file.assert_called_once()

    def test_load_plan_not_found(self, checkout_command):
        """Test loading non-existent plan file."""
        checkout_command.config.get.return_value = "sqitch.plan"

        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(PlanError, match="Plan file not found"):
                checkout_command._load_plan()

    def test_load_branch_plan(self, checkout_command):
        """Test loading plan from branch."""
        target = Mock()
        target.plan_file = Path("sqitch.plan")

        with (
            patch("subprocess.run") as mock_run,
            patch.object(Plan, "from_string") as mock_from_string,
        ):
            mock_run.return_value.stdout = "%project=test\nusers 2023-01-01T00:00:00Z Test <test@example.com> # Test"
            mock_run.return_value.returncode = 0
            mock_plan = Mock(spec=Plan)
            mock_from_string.return_value = mock_plan

            plan = checkout_command._load_branch_plan("feature", target)

            assert plan == mock_plan
            git_exe = shutil.which("git")
            mock_run.assert_called_once_with(
                [git_exe, "show", "feature:sqitch.plan"],
                capture_output=True,
                text=True,
                check=True,
            )

    def test_load_branch_plan_error(self, checkout_command):
        """Test error loading plan from branch."""
        target = Mock()
        target.plan_file = Path("sqitch.plan")

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "git")

            with pytest.raises(VCSError, match="Failed to load plan from branch"):
                checkout_command._load_branch_plan("feature", target)

    def test_find_last_common_change(self, checkout_command):
        """Test finding last common change."""
        # Create changes
        change1 = Change(
            name="change1",
            note="",
            timestamp=datetime.now(timezone.utc),
            planner_name="Test",
            planner_email="test@example.com",
        )
        change2 = Change(
            name="change2",
            note="",
            timestamp=datetime.now(timezone.utc),
            planner_name="Test",
            planner_email="test@example.com",
        )
        change3 = Change(
            name="change3",
            note="",
            timestamp=datetime.now(timezone.utc),
            planner_name="Test",
            planner_email="test@example.com",
        )

        # Current plan has change1 and change2
        current_plan = Mock(spec=Plan)
        current_plan.get_change.side_effect = lambda id: {
            change1.id: change1,
            change2.id: change2,
        }.get(id)

        # Target plan has change1, change2, and change3
        target_plan = Mock(spec=Plan)
        target_plan.changes = [change1, change2, change3]

        last_common = checkout_command._find_last_common_change(
            current_plan, target_plan
        )

        assert last_common == change2

    def test_find_last_common_change_none(self, checkout_command):
        """Test finding last common change when none exist."""
        change1 = Change(
            name="change1",
            note="",
            timestamp=datetime.now(timezone.utc),
            planner_name="Test",
            planner_email="test@example.com",
        )

        current_plan = Mock(spec=Plan)
        current_plan.get_change.return_value = None

        target_plan = Mock(spec=Plan)
        target_plan.changes = [change1]

        last_common = checkout_command._find_last_common_change(
            current_plan, target_plan
        )

        assert last_common is None

    def test_configure_engine(self, checkout_command):
        """Test engine configuration."""
        engine = Mock()
        options = {"verify": True, "log_only": True, "lock_timeout": 120}

        checkout_command._configure_engine(engine, options)

        assert engine.with_verify is True
        assert engine.log_only is True
        assert engine.lock_timeout == 120

    def test_checkout_branch(self, checkout_command):
        """Test checking out branch."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            checkout_command._checkout_branch("feature")

            git_exe = shutil.which("git")
            mock_run.assert_called_once_with(
                [git_exe, "checkout", "feature"], check=True
            )

    def test_checkout_branch_error(self, checkout_command):
        """Test error checking out branch."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "git")

            with pytest.raises(VCSError, match="Failed to checkout branch"):
                checkout_command._checkout_branch("feature")

    def test_execute_no_branch(self, checkout_command):
        """Test execute without branch argument."""
        with (
            patch.object(checkout_command, "require_initialized"),
            patch.object(checkout_command, "validate_user_info"),
            patch.object(checkout_command, "_show_usage") as mock_usage,
        ):

            result = checkout_command.execute([])

            assert result == 1
            mock_usage.assert_called_once()

    def test_execute_already_on_branch(self, checkout_command):
        """Test execute when already on target branch."""
        with (
            patch.object(checkout_command, "require_initialized"),
            patch.object(checkout_command, "validate_user_info"),
            patch.object(checkout_command, "_get_current_branch", return_value="main"),
        ):

            result = checkout_command.execute(["main"])

            assert result == 1  # Error handled by handle_error

    @patch("subprocess.run")
    def test_execute_success(
        self, mock_run, checkout_command, sample_plan, sample_change
    ):
        """Test successful checkout execution."""
        # Mock subprocess calls
        mock_run.side_effect = [
            # Get current branch
            Mock(stdout="current\n", returncode=0),
            # Load branch plan
            Mock(
                stdout="%project=test\nusers 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table",
                returncode=0,
            ),
            # Checkout branch
            Mock(returncode=0),
        ]

        # Mock other dependencies
        with (
            patch.object(checkout_command, "require_initialized"),
            patch.object(checkout_command, "validate_user_info"),
            patch.object(checkout_command, "get_target") as mock_get_target,
            patch.object(checkout_command, "_load_plan", return_value=sample_plan),
            patch.object(checkout_command, "info"),
            patch(
                "sqlitch.engines.base.EngineRegistry.create_engine"
            ) as mock_create_engine,
        ):

            # Setup mocks
            mock_target = Mock()
            mock_target.plan_file = Path("sqitch.plan")
            mock_get_target.return_value = mock_target

            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock plan parsing
            with patch.object(Plan, "from_string", return_value=sample_plan):
                result = checkout_command.execute(["main"])

            assert result == 0
            mock_engine.ensure_registry.assert_called()
            mock_engine.revert.assert_called()
            mock_engine.deploy.assert_called()


class TestCheckoutCommandIntegration:
    """Integration tests for checkout command."""

    def test_show_usage(self, checkout_command):
        """Test usage display."""
        with patch.object(checkout_command, "emit") as mock_emit:
            checkout_command._show_usage()

            mock_emit.assert_called_once()
            usage_text = mock_emit.call_args[0][0]
            assert "Usage: sqlitch checkout" in usage_text
            assert "--target" in usage_text
            assert "--mode" in usage_text

    def test_show_help(self, checkout_command):
        """Test help display."""
        with patch.object(checkout_command, "emit") as mock_emit:
            checkout_command._show_help()

            mock_emit.assert_called_once()
            help_text = mock_emit.call_args[0][0]
            assert "sqlitch-checkout" in help_text
            assert "SYNOPSIS" in help_text
            assert "DESCRIPTION" in help_text
            assert "OPTIONS" in help_text
