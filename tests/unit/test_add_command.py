"""Unit tests for the add command."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, mock_open, patch

import pytest

from sqlitch.commands.add import AddCommand
from sqlitch.core.change import Change, Dependency
from sqlitch.core.config import Config
from sqlitch.core.exceptions import PlanError, SqlitchError
from sqlitch.core.plan import Plan
from sqlitch.core.sqitch import Sqitch
from sqlitch.core.target import Target


@pytest.fixture
def mock_sqitch():
    """Create a mock Sqitch instance."""
    sqitch = Mock(spec=Sqitch)
    sqitch.config = Mock(spec=Config)
    sqitch.logger = Mock()
    sqitch.user_name = "Test User"
    sqitch.user_email = "test@example.com"
    sqitch.editor = "vim"

    # Mock config methods
    sqitch.config.get_section.return_value = {}
    sqitch.config.get.return_value = None

    return sqitch


@pytest.fixture
def mock_target():
    """Create a mock target."""
    target = Mock(spec=Target)
    target.engine = "pg"
    target.plan_file = Path("sqitch.plan")
    target.top_dir = Path(".")
    target.deploy_dir = Path("deploy")
    target.revert_dir = Path("revert")
    target.verify_dir = Path("verify")

    # Mock plan
    plan = Mock(spec=Plan)
    plan.project = "test_project"
    plan.file = Path("sqitch.plan")
    plan._change_index = {}
    target.plan = plan

    return target


@pytest.fixture
def add_command(mock_sqitch):
    """Create AddCommand instance."""
    return AddCommand(mock_sqitch)


class TestAddCommand:
    """Test cases for AddCommand."""

    def test_init(self, mock_sqitch):
        """Test command initialization."""
        command = AddCommand(mock_sqitch)
        assert command.sqitch is mock_sqitch
        assert command.config is mock_sqitch.config
        assert command.logger is mock_sqitch.logger

    def test_parse_args_basic(self, add_command):
        """Test basic argument parsing."""
        change_name, options = add_command._parse_args(["test_change"])

        assert change_name == "test_change"
        assert options["requires"] == []
        assert options["conflicts"] == []
        assert options["note"] == []
        assert options["all"] is False
        assert options["template_name"] is None
        assert options["with_scripts"] == {
            "deploy": True,
            "revert": True,
            "verify": True,
        }
        assert options["variables"] == {}
        assert options["open_editor"] is False

    def test_parse_args_with_options(self, add_command):
        """Test argument parsing with various options."""
        args = [
            "test_change",
            "--requires",
            "dep1",
            "--conflicts",
            "conflict1",
            "--note",
            "Test note",
            "--all",
            "--template",
            "custom",
            "--set",
            "var1=value1",
            "--open-editor",
        ]

        change_name, options = add_command._parse_args(args)

        assert change_name == "test_change"
        assert options["requires"] == ["dep1"]
        assert options["conflicts"] == ["conflict1"]
        assert options["note"] == ["Test note"]
        assert options["all"] is True
        assert options["template_name"] == "custom"
        assert options["variables"] == {"var1": "value1"}
        assert options["open_editor"] is True

    def test_parse_args_change_name_option(self, add_command):
        """Test parsing change name from option."""
        change_name, options = add_command._parse_args(["--change", "test_change"])

        assert change_name == "test_change"

    def test_parse_args_multiple_requires(self, add_command):
        """Test parsing multiple requires."""
        args = ["test_change", "--requires", "dep1", "--requires", "dep2"]
        change_name, options = add_command._parse_args(args)

        assert options["requires"] == ["dep1", "dep2"]

    def test_parse_args_with_without_scripts(self, add_command):
        """Test parsing with/without script options."""
        args = ["test_change", "--with", "deploy", "--without", "verify"]

        change_name, options = add_command._parse_args(args)

        assert options["with_scripts"]["deploy"] is True
        assert options["with_scripts"]["verify"] is False

    def test_parse_args_invalid_option(self, add_command):
        """Test parsing with invalid option."""
        with pytest.raises(SqlitchError, match="Unknown option"):
            add_command._parse_args(["--invalid-option"])

    def test_parse_args_missing_value(self, add_command):
        """Test parsing with missing option value."""
        with pytest.raises(SqlitchError, match="requires a value"):
            add_command._parse_args(["--requires"])

    def test_create_change(self, add_command):
        """Test creating a change object."""
        options = {
            "requires": ["dep1", "dep2"],
            "conflicts": ["conflict1"],
            "note": ["Line 1", "Line 2"],
        }

        test_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with patch("sqlitch.commands.add.datetime") as mock_datetime:
            mock_datetime.now.return_value = test_time
            mock_datetime.timezone = timezone

            change = add_command._create_change("test_change", options)

        assert change.name == "test_change"
        assert change.note == "Line 1\n\nLine 2"
        assert change.timestamp == test_time
        assert change.planner_name == "Test User"
        assert change.planner_email == "test@example.com"

        # Check dependencies
        assert len(change.dependencies) == 3

        # Check requires
        require_deps = [dep for dep in change.dependencies if dep.type == "require"]
        assert len(require_deps) == 2
        assert require_deps[0].change == "dep1"
        assert require_deps[1].change == "dep2"

        # Check conflicts
        conflict_deps = [dep for dep in change.dependencies if dep.type == "conflict"]
        assert len(conflict_deps) == 1
        assert conflict_deps[0].change == "conflict1"

    def test_create_change_no_user_info(self, add_command):
        """Test creating change without user info."""
        add_command.sqitch.user_name = None

        with pytest.raises(
            SqlitchError, match="User name and email must be configured"
        ):
            add_command._create_change("test_change", {})

    def test_get_targets_default(self, add_command, mock_target):
        """Test getting default target."""
        add_command.get_target = Mock(return_value=mock_target)

        targets = add_command._get_targets({"all": False})

        assert len(targets) == 1
        assert targets[0] is mock_target
        add_command.get_target.assert_called_once_with()

    def test_get_targets_all(self, add_command, mock_target):
        """Test getting all targets."""
        add_command.get_target = Mock(return_value=mock_target)
        add_command.config.get_section.return_value = {"prod": {}, "dev": {}}

        targets = add_command._get_targets({"all": True})

        assert len(targets) == 2
        add_command.get_target.assert_any_call("prod")
        add_command.get_target.assert_any_call("dev")

    def test_get_targets_all_no_targets(self, add_command, mock_target):
        """Test getting all targets when none configured."""
        add_command.get_target = Mock(return_value=mock_target)
        add_command.config.get_section.return_value = {}

        targets = add_command._get_targets({"all": True})

        assert len(targets) == 1
        assert targets[0] is mock_target
        add_command.get_target.assert_called_once_with()

    @patch("sqlitch.commands.add.create_template_engine")
    @patch("sqlitch.commands.add.TemplateContext")
    def test_create_script_files(
        self, mock_template_context, mock_create_engine, add_command, mock_target
    ):
        """Test creating script files."""
        # Setup mocks
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        mock_engine.render_template.return_value = "-- Test content"

        mock_context = Mock()
        mock_template_context.return_value = mock_context
        mock_context.to_dict.return_value = {"project": "test", "change": "test_change"}

        # Create change
        change = Change(
            name="test_change",
            note="Test note",
            timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
            planner_name="Test User",
            planner_email="test@example.com",
        )

        # Mock file paths
        change.deploy_file = Mock(return_value=Path("deploy/test_change.sql"))
        change.revert_file = Mock(return_value=Path("revert/test_change.sql"))
        change.verify_file = Mock(return_value=Path("verify/test_change.sql"))

        options = {
            "with_scripts": {"deploy": True, "revert": True, "verify": True},
            "template_directory": None,
            "template_name": None,
            "variables": {},
        }

        with (
            patch("pathlib.Path.exists", return_value=False),
            patch("pathlib.Path.mkdir"),
            patch("pathlib.Path.write_text") as mock_write,
        ):

            files = add_command._create_script_files(change, mock_target, options)

        assert len(files) == 3
        assert mock_write.call_count == 3
        mock_engine.render_template.assert_called()

    def test_create_script_files_skip_existing(self, add_command, mock_target):
        """Test skipping existing script files."""
        change = Change(
            name="test_change",
            note="Test note",
            timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
            planner_name="Test User",
            planner_email="test@example.com",
        )

        # Mock file paths
        change.deploy_file = Mock(return_value=Path("deploy/test_change.sql"))
        change.revert_file = Mock(return_value=Path("revert/test_change.sql"))
        change.verify_file = Mock(return_value=Path("verify/test_change.sql"))

        options = {
            "with_scripts": {"deploy": True, "revert": True, "verify": True},
            "template_directory": None,
            "template_name": None,
            "variables": {},
        }

        with patch("pathlib.Path.exists", return_value=True):
            files = add_command._create_script_files(change, mock_target, options)

        assert len(files) == 0
        add_command.logger.emit.assert_called()

    def test_has_double_extension(self, add_command):
        """Test double extension detection."""
        assert add_command._has_double_extension(Path("test.sql.sql"))
        assert add_command._has_double_extension(Path("test.py.py"))
        assert not add_command._has_double_extension(Path("test.sql"))
        assert not add_command._has_double_extension(Path("test.sql.bak"))

    @patch("subprocess.run")
    def test_open_editor(self, mock_run, add_command):
        """Test opening editor."""
        files = [Path("deploy/test.sql"), Path("revert/test.sql")]

        add_command._open_editor(files)

        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert args[0] == "vim"
        # Use os.path.join or Path to handle platform-specific separators
        import os

        assert os.path.join("deploy", "test.sql") in args
        assert os.path.join("revert", "test.sql") in args

    def test_open_editor_no_editor(self, add_command):
        """Test opening editor when none configured."""
        add_command.sqitch.editor = None
        files = [Path("deploy/test.sql")]

        add_command._open_editor(files)

        add_command.logger.warn.assert_called_with("No editor configured")

    @patch("subprocess.run", side_effect=Exception("Editor failed"))
    def test_open_editor_failure(self, mock_run, add_command):
        """Test editor failure."""
        files = [Path("deploy/test.sql")]

        add_command._open_editor(files)

        add_command.logger.warn.assert_called_with(
            "Failed to open editor: Editor failed"
        )

    @patch("sqlitch.commands.add.Plan.from_file")
    @patch("sqlitch.commands.add.validate_change_name")
    def test_execute_success(
        self, mock_validate, mock_plan_from_file, add_command, mock_target
    ):
        """Test successful command execution."""
        # Setup mocks
        mock_validate.return_value = True

        mock_plan = Mock(spec=Plan)
        mock_plan.file = Path("sqitch.plan")
        mock_plan._change_index = {}
        mock_plan_from_file.return_value = mock_plan

        add_command.require_initialized = Mock()
        add_command.validate_user_info = Mock()
        add_command._get_targets = Mock(return_value=[mock_target])
        add_command._create_change = Mock(return_value=Mock(spec=Change))
        add_command._create_script_files = Mock(return_value=[])

        # Mock change methods
        change = add_command._create_change.return_value
        change.format_name_with_tags.return_value = "test_change"

        result = add_command.execute(["test_change"])

        assert result == 0
        add_command.require_initialized.assert_called_once()
        add_command.validate_user_info.assert_called_once()
        mock_plan.add_change.assert_called_once()
        mock_plan.save.assert_called_once()

    def test_execute_no_change_name(self, add_command):
        """Test execution without change name."""
        add_command.require_initialized = Mock()
        add_command.validate_user_info = Mock()

        result = add_command.execute([])

        assert result == 1
        add_command.logger.error.assert_called()

    def test_execute_invalid_change_name(self, add_command):
        """Test execution with invalid change name."""
        add_command.require_initialized = Mock()
        add_command.validate_user_info = Mock()

        with patch(
            "sqlitch.commands.add.validate_change_name",
            side_effect=SqlitchError("Invalid name"),
        ):
            result = add_command.execute(["invalid-name"])

        assert result == 1
        add_command.logger.error.assert_called()

    @patch("sqlitch.commands.add.Plan.from_file")
    def test_execute_change_exists(self, mock_plan_from_file, add_command, mock_target):
        """Test execution when change already exists."""
        # Setup mocks
        mock_plan = Mock(spec=Plan)
        mock_plan.file = Path("sqitch.plan")
        mock_plan._change_index = {"test_change": Mock()}
        mock_plan_from_file.return_value = mock_plan

        add_command.require_initialized = Mock()
        add_command.validate_user_info = Mock()
        add_command._get_targets = Mock(return_value=[mock_target])

        with patch("sqlitch.commands.add.validate_change_name"):
            result = add_command.execute(["test_change"])

        assert result == 0
        add_command.logger.warn.assert_called()
        mock_plan.add_change.assert_not_called()

    def test_execute_exception(self, add_command):
        """Test execution with exception."""
        add_command.require_initialized = Mock(
            side_effect=SqlitchError("Not initialized")
        )

        result = add_command.execute(["test_change"])

        assert result == 1
        add_command.logger.error.assert_called()


class TestAddCommandIntegration:
    """Integration tests for AddCommand."""

    @patch("sqlitch.commands.add.Plan.from_file")
    @patch("sqlitch.commands.add.create_template_engine")
    @patch("sqlitch.commands.add.validate_change_name")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.write_text")
    def test_full_workflow(
        self,
        mock_write,
        mock_mkdir,
        mock_exists,
        mock_validate,
        mock_create_engine,
        mock_plan_from_file,
        mock_sqitch,
    ):
        """Test full add workflow."""
        # Setup mocks
        mock_exists.return_value = False
        mock_validate.return_value = True

        mock_plan = Mock(spec=Plan)
        mock_plan.file = Path("sqitch.plan")
        mock_plan.project = "test_project"
        mock_plan._change_index = {}
        mock_plan_from_file.return_value = mock_plan

        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        mock_engine.render_template.return_value = "-- Test content"

        mock_target = Mock(spec=Target)
        mock_target.engine = "pg"
        mock_target.plan_file = Path("sqitch.plan")
        mock_target.plan = mock_plan

        # Mock change file methods
        mock_change = Mock()
        mock_change.name = "test_change"
        mock_change.dependencies = []  # Empty list of dependencies
        mock_change.deploy_file.return_value = Path("deploy/test_change.sql")
        mock_change.revert_file.return_value = Path("revert/test_change.sql")
        mock_change.verify_file.return_value = Path("verify/test_change.sql")
        mock_change.format_name_with_tags.return_value = "test_change"

        # Create command
        command = AddCommand(mock_sqitch)
        command.require_initialized = Mock()
        command.validate_user_info = Mock()
        command.get_target = Mock(return_value=mock_target)
        command._get_targets = Mock(
            return_value=[mock_target]
        )  # Return list of targets
        command._create_change = Mock(return_value=mock_change)
        command.error = Mock()  # Mock error method to capture error messages

        # Execute
        with patch("sqlitch.commands.add.TemplateContext") as mock_context_class:
            mock_context = Mock()
            mock_context.to_dict.return_value = {
                "project": "test_project",
                "change": "test_change",
            }
            mock_context_class.return_value = mock_context

            result = command.execute(["test_change", "--note", "Test note"])

        if result != 0:
            print(f"Command failed with exit code: {result}")
            if command.error.called:
                print(f"Error message: {command.error.call_args}")
        assert result == 0
        mock_plan.add_change.assert_called_once()
        mock_plan.save.assert_called_once()
        assert mock_write.call_count == 3  # deploy, revert, verify
