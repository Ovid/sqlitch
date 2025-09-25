"""Unit tests for the init command."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.commands.init import InitCommand
from sqlitch.core.config import Config
from sqlitch.core.exceptions import SqlitchError
from sqlitch.core.sqitch import Sqitch


class TestInitCommand:
    """Test cases for InitCommand."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        temp_dir = Path(tempfile.mkdtemp())
        original_cwd = Path.cwd()

        # Change to temp directory
        import os

        os.chdir(temp_dir)

        yield temp_dir

        # Cleanup
        os.chdir(original_cwd)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_sqitch(self, temp_dir):
        """Create mock Sqitch instance."""
        config = Mock(spec=Config)
        config.get.return_value = None
        config.get_engine_config.return_value = {}

        sqitch = Mock(spec=Sqitch)
        sqitch.config = config
        sqitch.get_plan_file.return_value = temp_dir / "sqitch.plan"
        sqitch.logger = Mock()
        sqitch.logger.info = Mock()
        sqitch.logger.warn = Mock()
        sqitch.logger.error = Mock()
        sqitch.logger.debug = Mock()

        return sqitch

    @pytest.fixture
    def init_command(self, mock_sqitch):
        """Create InitCommand instance."""
        return InitCommand(mock_sqitch)

    def test_validate_project_name_valid(self, init_command):
        """Test project name validation with valid names."""
        valid_names = [
            "myproject",
            "my_project",
            "my-project",
            "my.project",
            "project123",
            "a",
            "A",
        ]

        for name in valid_names:
            # Should not raise exception
            init_command._validate_project_name(name)

    def test_validate_project_name_invalid(self, init_command):
        """Test project name validation with invalid names."""
        invalid_names = [
            "",  # Empty
            "123project",  # Starts with number
            "_project",  # Starts with underscore
            "-project",  # Starts with dash
            ".project",  # Starts with dot
            "project@",  # Contains @
            "project:name",  # Contains :
            "project#tag",  # Contains #
            "project[0]",  # Contains brackets
            "project name",  # Contains space
        ]

        for name in invalid_names:
            with pytest.raises(SqlitchError, match="Invalid project name"):
                init_command._validate_project_name(name)

    def test_parse_args_basic(self, init_command):
        """Test basic argument parsing."""
        project_name, options = init_command._parse_args(["myproject"])

        assert project_name == "myproject"
        assert options["engine"] is None
        assert options["vcs"] is True

    def test_parse_args_with_engine(self, init_command):
        """Test argument parsing with engine option."""
        project_name, options = init_command._parse_args(
            ["--engine", "pg", "myproject"]
        )

        assert project_name == "myproject"
        assert options["engine"] == "pg"

    def test_parse_args_with_uri(self, init_command):
        """Test argument parsing with URI option."""
        project_name, options = init_command._parse_args(
            ["--uri", "db:pg://localhost/test", "myproject"]
        )

        assert project_name == "myproject"
        assert options["uri"] == "db:pg://localhost/test"

    def test_parse_args_no_project(self, init_command):
        """Test argument parsing without project name."""
        project_name, options = init_command._parse_args([])
        assert project_name is None
        assert isinstance(options, dict)

    def test_parse_args_unknown_option(self, init_command):
        """Test argument parsing with unknown option."""
        with pytest.raises(SqlitchError, match="Unknown option"):
            init_command._parse_args(["--unknown", "myproject"])

    def test_determine_engine_from_option(self, init_command):
        """Test engine determination from command option."""
        options = {"engine": "pg"}
        engine = init_command._determine_engine(options)
        assert engine == "pg"

    def test_determine_engine_from_uri(self, init_command):
        """Test engine determination from URI."""
        options = {"uri": "db:mysql://localhost/test"}
        engine = init_command._determine_engine(options)
        assert engine == "mysql"

    def test_determine_engine_from_config(self, init_command):
        """Test engine determination from configuration."""
        init_command.config.get.return_value = "sqlite"
        options = {}
        engine = init_command._determine_engine(options)
        assert engine == "sqlite"

    def test_determine_target_uri_from_target_option(self, init_command):
        """Test target URI determination from target option."""
        # The --uri option is for project URI, not target URI
        # Target URI should come from --target option or config
        options = {"target": "test_target"}

        # Mock the config to return a target
        with patch.object(init_command.config, "get_target") as mock_get_target:
            mock_target = Mock()
            mock_target.uri = "db:pg://localhost/test"
            mock_get_target.return_value = mock_target

            uri = init_command._determine_target_uri("pg", options)
            assert uri == "db:pg://localhost/test"

    def test_determine_target_uri_default(self, init_command):
        """Test default target URI determination."""
        options = {}
        uri = init_command._determine_target_uri("pg", options)
        assert uri == "db:pg:"

    @patch("sqlitch.commands.init.detect_vcs")
    def test_init_vcs_not_present(self, mock_detect_vcs, init_command, temp_dir):
        """Test VCS initialization when not present."""
        mock_detect_vcs.return_value = None

        with patch("sqlitch.commands.init.GitRepository") as mock_git_repo:
            mock_repo = Mock()
            mock_git_repo.return_value = mock_repo

            init_command._init_vcs()

            mock_repo.init_repository.assert_called_once()

    @patch("sqlitch.commands.init.detect_vcs")
    def test_init_vcs_already_present(self, mock_detect_vcs, init_command):
        """Test VCS initialization when already present."""
        mock_vcs = Mock()
        mock_detect_vcs.return_value = mock_vcs

        init_command._init_vcs()

        # Should not try to initialize
        init_command.sqitch.logger.debug.assert_called_with("VCS already initialized")

    def test_write_config_basic(self, init_command, temp_dir):
        """Test basic configuration file writing."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            options = {"engine": "pg"}

            init_command._write_config(options)

            config_file = Path("sqitch.conf")
            assert config_file.exists()

            content = config_file.read_text()
            assert "[core]" in content
            assert "engine = pg" in content
            assert '[engine "pg"]' in content
        finally:
            os.chdir(original_cwd)

    def test_write_config_already_exists(self, init_command, temp_dir):
        """Test configuration writing when file already exists."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            config_file = Path("sqitch.conf")
            config_file.write_text("[core]\nengine = mysql\n")

            init_command._write_config({"engine": "pg"})

            # Should not overwrite
            content = config_file.read_text()
            assert "engine = mysql" in content
            assert "engine = pg" not in content
        finally:
            os.chdir(original_cwd)

    def test_write_plan_basic(self, init_command, temp_dir):
        """Test basic plan file writing."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            options = {}

            init_command._write_plan("myproject", options)

            plan_file = Path("sqitch.plan")
            assert plan_file.exists()

            content = plan_file.read_text()
            assert "%syntax-version=1.0.0" in content
            assert "%project=myproject" in content
        finally:
            os.chdir(original_cwd)

    def test_write_plan_with_uri(self, init_command, temp_dir):
        """Test plan file writing with URI."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            options = {"uri": "https://github.com/user/project"}

            init_command._write_plan("myproject", options)

            plan_file = Path("sqitch.plan")
            content = plan_file.read_text()
            assert "%uri=https://github.com/user/project" in content
        finally:
            os.chdir(original_cwd)

    def test_write_plan_already_exists_same_project(self, init_command, temp_dir):
        """Test plan file writing when file exists for same project."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            plan_file = Path("sqitch.plan")
            plan_file.write_text("%syntax-version=1.0.0\n%project=myproject\n")

            # Should not raise error
            init_command._write_plan("myproject", {})
        finally:
            os.chdir(original_cwd)

    def test_write_plan_already_exists_different_project(self, init_command, temp_dir):
        """Test plan file writing when file exists for different project."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            plan_file = Path("sqitch.plan")
            plan_file.write_text("%syntax-version=1.0.0\n%project=otherproject\n")

            # This should now be caught by _is_already_initialized, not _write_plan
            with pytest.raises(SqlitchError, match="already initialized"):
                init_command._is_already_initialized("myproject")
        finally:
            os.chdir(original_cwd)

    def test_create_directories(self, init_command, temp_dir):
        """Test directory creation."""
        options = {}

        init_command._create_directories(options)

        assert (temp_dir / "deploy").exists()
        assert (temp_dir / "revert").exists()
        assert (temp_dir / "verify").exists()

    def test_create_directories_custom(self, init_command, temp_dir):
        """Test directory creation with custom paths."""
        options = {
            "top_dir": Path("custom"),
            "deploy_dir": "migrations",
            "revert_dir": "rollbacks",
            "verify_dir": "tests",
        }

        init_command._create_directories(options)

        assert (temp_dir / "custom" / "migrations").exists()
        assert (temp_dir / "custom" / "rollbacks").exists()
        assert (temp_dir / "custom" / "tests").exists()

    def test_execute_success(self, init_command, temp_dir):
        """Test successful execution."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            with patch.object(init_command, "_init_vcs"):
                exit_code = init_command.execute(["myproject"])

            assert exit_code == 0
            assert Path("sqitch.conf").exists()
            assert Path("sqitch.plan").exists()
            assert Path("deploy").exists()
            assert Path("revert").exists()
            assert Path("verify").exists()
        finally:
            os.chdir(original_cwd)

    def test_execute_already_initialized(self, init_command, temp_dir):
        """Test execution when already initialized."""
        # Create plan file
        plan_file = Path("sqitch.plan")
        plan_file.write_text("%syntax-version=1.0.0\n%project=myproject\n")

        init_command.sqitch.get_plan_file.return_value = plan_file

        exit_code = init_command.execute(["myproject"])

        assert exit_code == 0
        init_command.sqitch.logger.info.assert_called_with(
            "Project already initialized"
        )

    def test_execute_invalid_project_name(self, init_command, temp_dir):
        """Test execution with invalid project name."""
        exit_code = init_command.execute(["123invalid"])

        assert exit_code == 1
        init_command.sqitch.logger.error.assert_called()

    def test_is_already_initialized_same_project(self, init_command, temp_dir):
        """Test _is_already_initialized with same project."""
        plan_file = Path("sqitch.plan")
        plan_file.write_text("%syntax-version=1.0.0\n%project=myproject\n")

        init_command.sqitch.get_plan_file.return_value = plan_file

        result = init_command._is_already_initialized("myproject")
        assert result is True

    def test_is_already_initialized_different_project(self, init_command, temp_dir):
        """Test _is_already_initialized with different project."""
        plan_file = Path("sqitch.plan")
        plan_file.write_text("%syntax-version=1.0.0\n%project=otherproject\n")

        init_command.sqitch.get_plan_file.return_value = plan_file

        with pytest.raises(SqlitchError, match="already initialized"):
            init_command._is_already_initialized("myproject")

    def test_is_already_initialized_no_plan_file(self, init_command, temp_dir):
        """Test _is_already_initialized when no plan file exists."""
        plan_file = Path("sqitch.plan")
        init_command.sqitch.get_plan_file.return_value = plan_file

        result = init_command._is_already_initialized("myproject")
        assert result is False

    def test_get_default_gitignore(self, init_command):
        """Test default .gitignore content."""
        content = init_command._get_default_gitignore()

        assert "# Sqlitch" in content
        assert "*.log" in content
        assert ".sqlitch/" in content
        assert "__pycache__/" in content
