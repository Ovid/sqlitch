"""Unit tests for the show command."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, mock_open, patch

import pytest

from sqlitch.commands.show import ShowCommand
from sqlitch.core.change import Change, Dependency, Tag
from sqlitch.core.exceptions import SqlitchError
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target


class TestShowCommand:
    """Test cases for ShowCommand."""

    @pytest.fixture
    def mock_sqitch(self):
        """Create mock Sqitch instance."""
        sqitch = Mock()
        sqitch.config = Mock()
        sqitch.logger = Mock()
        sqitch.verbosity = 1
        sqitch.emit = Mock()
        sqitch.vent = Mock()
        return sqitch

    @pytest.fixture
    def show_command(self, mock_sqitch):
        """Create ShowCommand instance."""
        return ShowCommand(mock_sqitch)

    @pytest.fixture
    def sample_change(self):
        """Create sample change."""
        return Change(
            name="test_change",
            note="Test change note",
            timestamp=datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com",
            dependencies=[
                Dependency(type="require", change="initial_schema"),
                Dependency(type="conflict", change="conflicting_change"),
            ],
        )

    @pytest.fixture
    def sample_tag(self, sample_change):
        """Create sample tag."""
        return Tag(
            name="v1.0",
            note="Version 1.0 release",
            timestamp=datetime(2023, 1, 20, 9, 0, 0, tzinfo=timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com",
            change=sample_change,
        )

    @pytest.fixture
    def sample_plan(self, sample_change, sample_tag):
        """Create sample plan."""
        plan = Plan(
            file=Path("sqitch.plan"),
            project="test_project",
            uri="https://example.com/test",
            changes=[sample_change],
            tags=[sample_tag],
        )
        # Manually build indexes since we're not using from_file
        plan._build_indexes()
        return plan

    @pytest.fixture
    def mock_target(self, sample_plan):
        """Create mock target."""
        target = Mock(spec=Target)
        target.plan = sample_plan
        target.top_dir = Path("/test/project")
        target.deploy_dir = "deploy"
        target.revert_dir = "revert"
        target.verify_dir = "verify"
        return target

    def test_show_change_info(self, show_command, mock_target, sample_change):
        """Test showing change information."""
        show_command.get_target = Mock(return_value=mock_target)

        exit_code = show_command.execute(["change", "test_change"])

        assert exit_code == 0
        show_command.sqitch.emit.assert_called_once()

        # Check that the emitted content contains expected information
        emitted_content = show_command.sqitch.emit.call_args[0][0]
        assert "project test_project" in emitted_content
        assert "uri https://example.com/test" in emitted_content
        assert "change test_change" in emitted_content
        assert "planner Test User <test@example.com>" in emitted_content
        assert "requires" in emitted_content
        assert "  + initial_schema" in emitted_content
        assert "conflicts" in emitted_content
        assert "  - !conflicting_change" in emitted_content
        assert "Test change note" in emitted_content

    def test_show_tag_info(self, show_command, mock_target, sample_tag):
        """Test showing tag information."""
        show_command.get_target = Mock(return_value=mock_target)

        exit_code = show_command.execute(["tag", "v1.0"])

        assert exit_code == 0
        show_command.sqitch.emit.assert_called_once()

        # Check that the emitted content contains expected information
        emitted_content = show_command.sqitch.emit.call_args[0][0]
        assert "project test_project" in emitted_content
        assert "uri https://example.com/test" in emitted_content
        assert "tag @v1.0" in emitted_content
        assert "planner Test User <test@example.com>" in emitted_content
        assert "Version 1.0 release" in emitted_content

    def test_show_tag_with_at_prefix(self, show_command, mock_target):
        """Test showing tag with @ prefix."""
        show_command.get_target = Mock(return_value=mock_target)

        exit_code = show_command.execute(["tag", "@v1.0"])

        assert exit_code == 0
        show_command.sqitch.emit.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data=b"CREATE TABLE test;")
    @patch("sys.stdout")
    def test_show_deploy_script(
        self, mock_stdout, mock_file, show_command, mock_target, sample_change
    ):
        """Test showing deploy script contents."""
        show_command.get_target = Mock(return_value=mock_target)

        # Mock the file path
        deploy_path = Path("/test/project/deploy/test_change.sql")
        sample_change.deploy_file = Mock(return_value=deploy_path)

        # Mock path existence
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "is_dir", return_value=False),
        ):

            exit_code = show_command.execute(["deploy", "test_change"])

        assert exit_code == 0
        # Check that file was opened and content written to stdout
        mock_file.assert_called_once_with(deploy_path, "rb")
        mock_stdout.buffer.write.assert_called_once_with(b"CREATE TABLE test;")

    @patch("builtins.open", new_callable=mock_open, read_data=b"DROP TABLE test;")
    @patch("sys.stdout")
    def test_show_revert_script(
        self, mock_stdout, mock_file, show_command, mock_target, sample_change
    ):
        """Test showing revert script contents."""
        show_command.get_target = Mock(return_value=mock_target)

        # Mock the file path
        revert_path = Path("/test/project/revert/test_change.sql")
        sample_change.revert_file = Mock(return_value=revert_path)

        # Mock path existence
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "is_dir", return_value=False),
        ):

            exit_code = show_command.execute(["revert", "test_change"])

        assert exit_code == 0
        # Check that file was opened and content written to stdout
        mock_file.assert_called_once_with(revert_path, "rb")
        mock_stdout.buffer.write.assert_called_once_with(b"DROP TABLE test;")

    @patch("builtins.open", new_callable=mock_open, read_data=b"SELECT 1 FROM test;")
    @patch("sys.stdout")
    def test_show_verify_script(
        self, mock_stdout, mock_file, show_command, mock_target, sample_change
    ):
        """Test showing verify script contents."""
        show_command.get_target = Mock(return_value=mock_target)

        # Mock the file path
        verify_path = Path("/test/project/verify/test_change.sql")
        sample_change.verify_file = Mock(return_value=verify_path)

        # Mock path existence
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "is_dir", return_value=False),
        ):

            exit_code = show_command.execute(["verify", "test_change"])

        assert exit_code == 0
        # Check that file was opened and content written to stdout
        mock_file.assert_called_once_with(verify_path, "rb")
        mock_stdout.buffer.write.assert_called_once_with(b"SELECT 1 FROM test;")

    def test_show_with_target_option(self, show_command, mock_target):
        """Test show command with target option."""
        show_command.get_target = Mock(return_value=mock_target)

        exit_code = show_command.execute(
            ["--target", "production", "change", "test_change"]
        )

        assert exit_code == 0
        show_command.get_target.assert_called_once_with("production")

    def test_show_exists_only_existing_change(self, show_command, mock_target):
        """Test show command with --exists flag for existing change."""
        show_command.get_target = Mock(return_value=mock_target)

        exit_code = show_command.execute(["--exists", "change", "test_change"])

        assert exit_code == 0
        # Should not emit anything when using --exists
        show_command.sqitch.emit.assert_not_called()

    def test_show_exists_only_nonexistent_change(self, show_command, mock_target):
        """Test show command with --exists flag for non-existent change."""
        show_command.get_target = Mock(return_value=mock_target)

        exit_code = show_command.execute(["--exists", "change", "nonexistent"])

        assert exit_code == 1
        # Should not emit anything when using --exists
        show_command.sqitch.emit.assert_not_called()

    def test_show_unknown_change(self, show_command, mock_target):
        """Test showing unknown change."""
        show_command.get_target = Mock(return_value=mock_target)

        exit_code = show_command.execute(["change", "unknown_change"])

        assert exit_code != 0

    def test_show_unknown_tag(self, show_command, mock_target):
        """Test showing unknown tag."""
        show_command.get_target = Mock(return_value=mock_target)

        exit_code = show_command.execute(["tag", "unknown_tag"])

        assert exit_code != 0

    def test_show_nonexistent_script_file(
        self, show_command, mock_target, sample_change
    ):
        """Test showing script file that doesn't exist."""
        show_command.get_target = Mock(return_value=mock_target)

        # Mock the file path to not exist
        deploy_path = Path("/test/project/deploy/test_change.sql")
        sample_change.deploy_file = Mock(return_value=deploy_path)

        with patch.object(Path, "exists", return_value=False):
            exit_code = show_command.execute(["deploy", "test_change"])

        assert exit_code != 0

    def test_show_script_file_is_directory(
        self, show_command, mock_target, sample_change
    ):
        """Test showing script file that is actually a directory."""
        show_command.get_target = Mock(return_value=mock_target)

        # Mock the file path
        deploy_path = Path("/test/project/deploy/test_change.sql")
        sample_change.deploy_file = Mock(return_value=deploy_path)

        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "is_dir", return_value=True),
        ):

            exit_code = show_command.execute(["deploy", "test_change"])

        assert exit_code != 0

    def test_show_invalid_object_type(self, show_command, mock_target):
        """Test showing invalid object type."""
        show_command.get_target = Mock(return_value=mock_target)

        exit_code = show_command.execute(["invalid_type", "test_change"])

        assert exit_code != 0

    def test_show_missing_arguments(self, show_command):
        """Test show command with missing arguments."""
        exit_code = show_command.execute([])

        assert exit_code == 2
        show_command.sqitch.vent.assert_called()

    def test_show_missing_object_key(self, show_command):
        """Test show command with missing object key."""
        exit_code = show_command.execute(["change"])

        assert exit_code == 2
        show_command.sqitch.vent.assert_called()

    def test_show_too_many_arguments(self, show_command):
        """Test show command with too many arguments."""
        exit_code = show_command.execute(["change", "test_change", "extra_arg"])

        assert exit_code != 0

    def test_show_unknown_option(self, show_command):
        """Test show command with unknown option."""
        exit_code = show_command.execute(["--unknown-option", "change", "test_change"])

        assert exit_code != 0

    def test_show_target_option_missing_value(self, show_command):
        """Test show command with --target option missing value."""
        exit_code = show_command.execute(["--target"])

        assert exit_code != 0
