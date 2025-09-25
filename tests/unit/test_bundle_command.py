"""Unit tests for bundle command."""

import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.commands.bundle import BundleCommand
from sqlitch.core.change import Change
from sqlitch.core.config import Config
from sqlitch.core.exceptions import SqlitchError
from sqlitch.core.plan import Plan
from sqlitch.core.sqitch import Sqitch
from sqlitch.core.target import Target


@pytest.fixture
def mock_sqitch():
    """Create a mock Sqitch instance."""
    sqitch = Mock(spec=Sqitch)
    sqitch.config = Mock(spec=Config)
    sqitch.logger = Mock()
    sqitch.verbosity = 1
    sqitch.user_name = "Test User"
    sqitch.user_email = "test@example.com"

    # Mock methods
    sqitch.info = Mock()
    sqitch.warn = Mock()
    sqitch.debug = Mock()
    sqitch.vent = Mock()
    sqitch.require_initialized = Mock()

    return sqitch


@pytest.fixture
def mock_target():
    """Create a mock Target instance."""
    target = Target(
        name="test",
        uri="db:pg://localhost/test",
        engine="pg",
        top_dir=Path("."),
        deploy_dir=Path("deploy"),
        revert_dir=Path("revert"),
        verify_dir=Path("verify"),
        plan_file=Path("sqitch.plan"),
    )
    return target


@pytest.fixture
def mock_plan():
    """Create a mock Plan instance."""
    plan = Mock(spec=Plan)
    plan.project = "test_project"
    plan.uri = "https://example.com/test"
    plan.syntax_version = "1.0.0"

    # Create mock changes
    change1 = Mock(spec=Change)
    change1.name = "initial"
    change1.tags = []
    change1.is_reworked = False
    change1.path_segments = ["initial.sql"]
    change1.format_name_with_tags.return_value = "initial"
    change1.deploy_file = Path("deploy/initial.sql")
    change1.revert_file = Path("revert/initial.sql")
    change1.verify_file = Path("verify/initial.sql")

    change2 = Mock(spec=Change)
    change2.name = "users"
    change2.tags = ["v1.0"]
    change2.is_reworked = False
    change2.path_segments = ["users.sql"]
    change2.format_name_with_tags.return_value = "users @v1.0"
    change2.deploy_file = Path("deploy/users.sql")
    change2.revert_file = Path("revert/users.sql")
    change2.verify_file = Path("verify/users.sql")

    plan.changes = [change1, change2]
    plan.tags = []

    return plan


@pytest.fixture
def bundle_command(mock_sqitch):
    """Create a BundleCommand instance."""
    return BundleCommand(mock_sqitch)


@pytest.fixture
def temp_project_dir():
    """Create a temporary project directory."""
    temp_dir = Path(tempfile.mkdtemp())

    # Create project structure
    (temp_dir / "deploy").mkdir()
    (temp_dir / "revert").mkdir()
    (temp_dir / "verify").mkdir()

    # Create config file
    config_content = """[core]
    engine = pg
    
[engine "pg"]
    target = db:pg://localhost/test
"""
    (temp_dir / "sqitch.conf").write_text(config_content)

    # Create plan file
    plan_content = """%syntax-version=1.0.0
%project=test_project
%uri=https://example.com/test

initial 2023-01-01T10:00:00Z Test User <test@example.com> # Initial change
users 2023-01-02T10:00:00Z Test User <test@example.com> # Add users table
"""
    (temp_dir / "sqitch.plan").write_text(plan_content)

    # Create change files
    (temp_dir / "deploy" / "initial.sql").write_text("CREATE SCHEMA test;")
    (temp_dir / "revert" / "initial.sql").write_text("DROP SCHEMA test;")
    (temp_dir / "verify" / "initial.sql").write_text("SELECT 1;")

    (temp_dir / "deploy" / "users.sql").write_text("CREATE TABLE users (id INT);")
    (temp_dir / "revert" / "users.sql").write_text("DROP TABLE users;")
    (temp_dir / "verify" / "users.sql").write_text("SELECT 1 FROM users LIMIT 1;")

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir)


class TestBundleCommand:
    """Test cases for BundleCommand."""

    def test_init(self, bundle_command, mock_sqitch):
        """Test command initialization."""
        assert bundle_command.sqitch == mock_sqitch
        assert bundle_command.config == mock_sqitch.config

    def test_parse_args_basic(self, bundle_command):
        """Test basic argument parsing."""
        options, targets, changes = bundle_command._parse_args([])

        assert options["dest_dir"] == Path("bundle")
        assert options["all"] is False
        assert options["from"] is None
        assert options["to"] is None
        assert targets == []
        assert changes == []

    def test_parse_args_with_dest_dir(self, bundle_command):
        """Test parsing with destination directory."""
        options, targets, changes = bundle_command._parse_args(
            ["--dest-dir", "my-bundle"]
        )

        assert options["dest_dir"] == Path("my-bundle")

    def test_parse_args_with_all(self, bundle_command):
        """Test parsing with --all flag."""
        options, targets, changes = bundle_command._parse_args(["--all"])

        assert options["all"] is True

    def test_parse_args_with_from_to(self, bundle_command):
        """Test parsing with --from and --to options."""
        options, targets, changes = bundle_command._parse_args(
            ["--from", "initial", "--to", "@v1.0"]
        )

        assert options["from"] == "initial"
        assert options["to"] == "@v1.0"

    def test_parse_args_with_targets(self, bundle_command):
        """Test parsing with target arguments."""
        options, targets, changes = bundle_command._parse_args(["pg", "mysql"])

        assert targets == ["pg", "mysql"]

    def test_parse_args_with_changes(self, bundle_command):
        """Test parsing with change arguments."""
        options, targets, changes = bundle_command._parse_args(
            ["pg", "initial", "@v1.0"]
        )

        assert targets == ["pg"]
        assert changes == ["initial", "@v1.0"]

    def test_parse_args_invalid_option(self, bundle_command):
        """Test parsing with invalid option."""
        with pytest.raises(SqlitchError, match="Unknown option: --invalid"):
            bundle_command._parse_args(["--invalid"])

    def test_validate_args_all_with_targets(self, bundle_command):
        """Test validation error when using --all with targets."""
        options = {"all": True}
        targets = ["pg"]
        changes = []

        with pytest.raises(
            SqlitchError, match="Cannot specify both --all and target arguments"
        ):
            bundle_command._validate_args(options, targets, changes)

    def test_validate_args_from_to_with_changes(self, bundle_command):
        """Test validation error when using --from/--to with changes."""
        options = {"from": "initial", "to": "@v1.0"}
        targets = []
        changes = ["change1"]

        with pytest.raises(
            SqlitchError,
            match="Cannot specify both --from or --to and change arguments",
        ):
            bundle_command._validate_args(options, targets, changes)

    def test_validate_args_multiple_targets_with_from_to(
        self, bundle_command, mock_sqitch
    ):
        """Test warning when using --from/--to with multiple targets."""
        options = {"from": "initial", "to": "@v1.0"}
        targets = ["pg", "mysql"]
        changes = []

        # Should not raise but should warn
        bundle_command._validate_args(options, targets, changes)
        mock_sqitch.warn.assert_called_once()

    def test_looks_like_change_spec(self, bundle_command):
        """Test change specification detection."""
        assert bundle_command._looks_like_change_spec("@v1.0") is True
        assert bundle_command._looks_like_change_spec("HEAD") is True
        assert bundle_command._looks_like_change_spec("@ROOT") is True
        assert bundle_command._looks_like_change_spec("change:tag") is True
        assert bundle_command._looks_like_change_spec("pg") is False
        assert bundle_command._looks_like_change_spec("mysql") is False

    @patch("sqlitch.commands.bundle.BundleCommand.get_target")
    def test_get_targets_to_bundle_default(
        self, mock_get_target, bundle_command, mock_target
    ):
        """Test getting default target."""
        mock_get_target.return_value = mock_target

        targets = bundle_command._get_targets_to_bundle([], {"all": False})

        assert len(targets) == 1
        assert targets[0] == mock_target
        mock_get_target.assert_called_once_with()

    @patch("sqlitch.commands.bundle.BundleCommand.get_target")
    def test_get_targets_to_bundle_specified(
        self, mock_get_target, bundle_command, mock_target
    ):
        """Test getting specified targets."""
        mock_get_target.return_value = mock_target

        targets = bundle_command._get_targets_to_bundle(["pg"], {"all": False})

        assert len(targets) == 1
        assert targets[0] == mock_target
        mock_get_target.assert_called_once_with("pg")

    def test_dest_top_dir_current(self, bundle_command, mock_target):
        """Test destination top directory for current directory."""
        mock_target.top_dir = Path(".")
        dest_dir = Path("bundle")

        result = bundle_command._dest_top_dir(mock_target, dest_dir)

        assert result == dest_dir

    def test_dest_top_dir_subdirectory(self, bundle_command, mock_target):
        """Test destination top directory for subdirectory."""
        mock_target.top_dir = Path("subdir")
        dest_dir = Path("bundle")

        result = bundle_command._dest_top_dir(mock_target, dest_dir)

        assert result == dest_dir / "subdir"

    def test_dest_dirs_for(self, bundle_command, mock_target):
        """Test destination directories for target."""
        dest_dir = Path("bundle")

        result = bundle_command._dest_dirs_for(mock_target, dest_dir)

        expected = {
            "deploy": dest_dir / "deploy",
            "revert": dest_dir / "revert",
            "verify": dest_dir / "verify",
            "reworked_deploy": dest_dir / "deploy",
            "reworked_revert": dest_dir / "revert",
            "reworked_verify": dest_dir / "verify",
        }

        assert result == expected

    def test_find_change_index_root(self, bundle_command, mock_plan):
        """Test finding ROOT change index."""
        result = bundle_command._find_change_index(mock_plan, "@ROOT")
        assert result == 0

        result = bundle_command._find_change_index(mock_plan, "ROOT")
        assert result == 0

    def test_find_change_index_head(self, bundle_command, mock_plan):
        """Test finding HEAD change index."""
        result = bundle_command._find_change_index(mock_plan, "@HEAD")
        assert result == 1  # len(changes) - 1

        result = bundle_command._find_change_index(mock_plan, "HEAD")
        assert result == 1

    def test_find_change_index_by_name(self, bundle_command, mock_plan):
        """Test finding change by name."""
        result = bundle_command._find_change_index(mock_plan, "initial")
        assert result == 0

        result = bundle_command._find_change_index(mock_plan, "users")
        assert result == 1

    def test_find_change_index_by_tag(self, bundle_command, mock_plan):
        """Test finding change by tag."""
        result = bundle_command._find_change_index(mock_plan, "@v1.0")
        assert result == 1  # users has tag v1.0

    def test_find_change_index_not_found(self, bundle_command, mock_plan):
        """Test finding non-existent change."""
        result = bundle_command._find_change_index(mock_plan, "nonexistent")
        assert result is None

    @patch("shutil.copy2")
    def test_copy_if_modified_new_file(
        self, mock_copy, bundle_command, temp_project_dir
    ):
        """Test copying when destination doesn't exist."""
        # Create a real source file
        src = temp_project_dir / "source.txt"
        src.write_text("test content")

        dest = temp_project_dir / "dest.txt"

        bundle_command._copy_if_modified(src, dest)

        mock_copy.assert_called_once_with(src, dest)

    @patch("pathlib.Path.exists")
    def test_copy_if_modified_source_missing(self, mock_exists, bundle_command):
        """Test error when source file doesn't exist."""
        src = Path("missing.txt")
        dest = Path("dest.txt")

        mock_exists.return_value = False

        with pytest.raises(SqlitchError, match="Cannot copy .* does not exist"):
            bundle_command._copy_if_modified(src, dest)

    @patch("sqlitch.commands.bundle.BundleCommand._bundle_config")
    @patch("sqlitch.commands.bundle.BundleCommand._bundle_plan")
    @patch("sqlitch.commands.bundle.BundleCommand._bundle_scripts")
    @patch("sqlitch.commands.bundle.BundleCommand._get_targets_to_bundle")
    def test_bundle_project_basic(
        self,
        mock_get_targets,
        mock_bundle_scripts,
        mock_bundle_plan,
        mock_bundle_config,
        bundle_command,
        mock_target,
        mock_sqitch,
    ):
        """Test basic project bundling."""
        mock_get_targets.return_value = [mock_target]

        options = {"dest_dir": Path("bundle"), "from": None, "to": None}
        targets = []
        changes = []

        result = bundle_command._bundle_project([mock_target], changes, options)

        assert result == 0
        mock_bundle_config.assert_called_once_with(Path("bundle"))
        mock_bundle_plan.assert_called_once_with(
            mock_target, Path("bundle"), None, None
        )
        mock_bundle_scripts.assert_called_once_with(
            mock_target, Path("bundle"), None, None
        )
        mock_sqitch.info.assert_called_with("Bundling into bundle")

    def test_execute_not_initialized(self, bundle_command, mock_sqitch):
        """Test execute when not in initialized project."""
        mock_sqitch.require_initialized.side_effect = SqlitchError("Not initialized")

        result = bundle_command.execute([])

        assert result == 1  # Error exit code

    @patch("sqlitch.commands.bundle.BundleCommand._bundle_project")
    @patch("sqlitch.commands.bundle.BundleCommand._get_targets_to_bundle")
    def test_execute_success(
        self, mock_get_targets, mock_bundle_project, bundle_command, mock_target
    ):
        """Test successful execution."""
        mock_get_targets.return_value = [mock_target]
        mock_bundle_project.return_value = 0

        result = bundle_command.execute([])

        assert result == 0
        mock_bundle_project.assert_called_once()


class TestBundleCommandIntegration:
    """Integration tests for bundle command."""

    def test_bundle_config_file_exists(self, bundle_command, temp_project_dir):
        """Test bundling configuration file."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_project_dir)

            # Mock the config to return the local file
            bundle_command.config.local_file = temp_project_dir / "sqitch.conf"

            dest_dir = temp_project_dir / "bundle"
            bundle_command._bundle_config(dest_dir)

            # Check that config was copied
            assert (dest_dir / "sqitch.conf").exists()

            # Check content
            original_content = (temp_project_dir / "sqitch.conf").read_text()
            bundled_content = (dest_dir / "sqitch.conf").read_text()
            assert original_content == bundled_content
        finally:
            os.chdir(original_cwd)

    def test_write_partial_plan(self, bundle_command, mock_plan, temp_project_dir):
        """Test writing partial plan file."""
        dest_file = temp_project_dir / "test_plan.plan"

        bundle_command._write_partial_plan(mock_plan, dest_file, "initial", "users")

        assert dest_file.exists()
        content = dest_file.read_text()

        # Should contain pragmas
        assert "%syntax-version=1.0.0" in content
        assert "%project=test_project" in content
        assert "%uri=https://example.com/test" in content
