"""Unit tests for the main Sqitch application class."""

import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.core.config import Config
from sqlitch.core.exceptions import ConfigurationError, EngineError, SqlitchError
from sqlitch.core.sqitch import Sqitch, create_sqitch
from sqlitch.core.target import Target
from sqlitch.core.types import URI


class TestSqitch:
    """Test cases for the Sqitch class."""

    def test_init_with_minimal_config(self):
        """Test Sqitch initialization with minimal configuration."""
        config = Config()
        sqitch = Sqitch(config=config)

        assert sqitch.config is config
        assert sqitch.options == {}
        assert sqitch.verbosity == 0
        assert sqitch.logger is not None

    def test_init_with_options(self):
        """Test Sqitch initialization with options."""
        config = Config()
        options = {"verbosity": 2, "log_file": "/tmp/test.log"}

        sqitch = Sqitch(config=config, options=options)

        assert sqitch.options == options
        assert sqitch.verbosity == 2

    def test_verbosity_clamping(self):
        """Test verbosity level is clamped to valid range."""
        config = Config()

        # Test upper bound
        sqitch = Sqitch(config=config, options={"verbosity": 10})
        assert sqitch.verbosity == 3

        # Test lower bound
        sqitch = Sqitch(config=config, options={"verbosity": -10})
        assert sqitch.verbosity == -2

    @patch.dict(
        os.environ,
        {
            "SQITCH_USER_NAME": "",
            "USER": "",
            "USERNAME": "",
            "EMAIL": "",
            "SQITCH_USER_EMAIL": "",
        },
        clear=False,
    )
    @patch("sqlitch.core.sqitch.subprocess.run")
    @patch("pwd.getpwuid", side_effect=KeyError())  # Mock system user lookup failure
    def test_user_name_detection_from_git(self, mock_pwd, mock_run):
        """Test user name detection from Git configuration."""

        # Mock different responses for name and email calls
        def side_effect(cmd, **kwargs):
            if "user.name" in cmd:
                return Mock(returncode=0, stdout="John Doe\n")
            elif "user.email" in cmd:
                return Mock(returncode=1)  # Fail email lookup
            return Mock(returncode=1)

        mock_run.side_effect = side_effect

        config = Config()
        sqitch = Sqitch(config=config)

        assert sqitch.user_name == "John Doe"

    @patch("subprocess.run")
    def test_user_email_detection_from_git(self, mock_run):
        """Test user email detection from Git configuration."""
        mock_run.return_value = Mock(returncode=0, stdout="john@example.com\n")

        config = Config()
        sqitch = Sqitch(config=config)

        assert sqitch.user_email == "john@example.com"
        mock_run.assert_called_with(
            ["git", "config", "--get", "user.email"],
            capture_output=True,
            text=True,
            timeout=5,
        )

    @patch.dict(os.environ, {"SQITCH_USER_NAME": "Jane Doe"})
    def test_user_name_from_environment(self):
        """Test user name detection from environment variable."""
        config = Config()
        sqitch = Sqitch(config=config)

        assert sqitch.user_name == "Jane Doe"

    @patch.dict(os.environ, {"SQITCH_USER_EMAIL": "jane@example.com"})
    def test_user_email_from_environment(self):
        """Test user email detection from environment variable."""
        config = Config()
        sqitch = Sqitch(config=config)

        assert sqitch.user_email == "jane@example.com"

    @patch.dict(os.environ, {"USER": "testuser"})
    @patch("subprocess.run")
    def test_user_name_fallback_to_user_env(self, mock_run):
        """Test user name fallback to USER environment variable."""
        mock_run.return_value = Mock(returncode=1)  # Git command fails

        config = Config()
        sqitch = Sqitch(config=config)

        assert sqitch.user_name == "testuser"

    @patch.dict(
        os.environ,
        {
            "SQITCH_USER_NAME": "",
            "USER": "",
            "USERNAME": "",
            "EMAIL": "",
            "SQITCH_USER_EMAIL": "",
        },
        clear=False,
    )
    @patch("sqlitch.core.sqitch.subprocess.run")
    @patch("pwd.getpwuid", side_effect=KeyError())  # Mock system user lookup failure
    def test_user_detection_git_timeout(self, mock_pwd, mock_run):
        """Test user detection when Git command times out."""
        mock_run.side_effect = subprocess.TimeoutExpired(["git"], 5)

        config = Config()
        sqitch = Sqitch(config=config)

        # Should not raise exception, just return None
        assert sqitch.user_name is None
        assert sqitch.user_email is None

    def test_user_name_from_config(self):
        """Test user name from configuration takes precedence."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write("[user]\nname = Config User\n")
            config_file = Path(f.name)

        try:
            config = Config([config_file])
            sqitch = Sqitch(config=config)

            assert sqitch.user_name == "Config User"
        finally:
            config_file.unlink()

    def test_user_email_from_config(self):
        """Test user email from configuration takes precedence."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write("[user]\nemail = config@example.com\n")
            config_file = Path(f.name)

        try:
            config = Config([config_file])
            sqitch = Sqitch(config=config)

            assert sqitch.user_email == "config@example.com"
        finally:
            config_file.unlink()

    def test_engine_for_target_pg(self):
        """Test engine creation for PostgreSQL target."""
        config = Config()
        sqitch = Sqitch(config=config)

        target = Target(name="test", uri=URI("db:pg://user@localhost/test"))

        # Mock Plan.from_file to avoid file system dependency
        mock_plan = Mock()

        with (
            patch.object(sqitch, "_get_engine_class") as mock_get_class,
            patch("sqlitch.core.plan.Plan.from_file", return_value=mock_plan),
        ):
            mock_engine_class = Mock()
            mock_engine = Mock()
            mock_engine_class.return_value = mock_engine
            mock_get_class.return_value = mock_engine_class

            engine = sqitch.engine_for_target(target)

            assert engine is mock_engine
            mock_get_class.assert_called_once_with("pg")
            mock_engine_class.assert_called_once_with(target, mock_plan)

    def test_engine_for_target_unsupported(self):
        """Test engine creation for unsupported target."""
        config = Config()
        sqitch = Sqitch(config=config)

        target = Target(name="test", uri=URI("db:unsupported://localhost/test"))

        with pytest.raises(EngineError, match="Unsupported engine type in URI"):
            sqitch.engine_for_target(target)

    def test_engine_for_target_creation_failure(self):
        """Test engine creation failure handling."""
        config = Config()
        sqitch = Sqitch(config=config)

        target = Target(name="test", uri=URI("db:pg://user@localhost/test"))

        with patch.object(sqitch, "_get_engine_class") as mock_get_class:
            mock_engine_class = Mock()
            mock_engine_class.side_effect = Exception("Connection failed")
            mock_get_class.return_value = mock_engine_class

            with pytest.raises(EngineError, match="Failed to create pg engine"):
                sqitch.engine_for_target(target)

    def test_get_engine_class_pg(self):
        """Test getting PostgreSQL engine class."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch("importlib.import_module") as mock_import:
            mock_module = Mock()
            mock_engine_class = Mock()
            mock_module.PostgreSQLEngine = mock_engine_class
            mock_import.return_value = mock_module

            engine_class = sqitch._get_engine_class("pg")

            assert engine_class is mock_engine_class
            mock_import.assert_called_once_with("sqlitch.engines.pg")

    def test_get_engine_class_unknown(self):
        """Test getting unknown engine class."""
        config = Config()
        sqitch = Sqitch(config=config)

        engine_class = sqitch._get_engine_class("unknown")
        assert engine_class is None

    def test_get_engine_class_import_error(self):
        """Test engine class import error handling."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch("importlib.import_module") as mock_import:
            mock_import.side_effect = ImportError("Module not found")

            engine_class = sqitch._get_engine_class("pg")
            assert engine_class is None

    @pytest.mark.compatibility
    def test_get_target_default(self):
        """Test getting default target."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write(
                '[core]\nengine = pg\n[engine "pg"]\ntarget = db:pg://localhost/test\n'
            )
            config_file = Path(f.name)

        try:
            config = Config([config_file])
            sqitch = Sqitch(config=config)

            target = sqitch.get_target()

            assert target.name == "db:pg://localhost/test"
            assert target.uri == "db:pg://localhost/test"
        finally:
            config_file.unlink()

    def test_get_target_named(self):
        """Test getting named target."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write('[target "prod"]\nuri = db:pg://prod.example.com/mydb\n')
            config_file = Path(f.name)

        try:
            config = Config([config_file])
            sqitch = Sqitch(config=config)

            target = sqitch.get_target("prod")

            assert target.name == "prod"
            assert target.uri == "db:pg://prod.example.com/mydb"
        finally:
            config_file.unlink()

    def test_run_command_success(self):
        """Test successful command execution."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch.object(sqitch, "_get_command_class") as mock_get_class:
            mock_command_class = Mock()
            mock_command = Mock()
            mock_command.execute.return_value = 0
            mock_command_class.return_value = mock_command
            mock_get_class.return_value = mock_command_class

            exit_code = sqitch.run_command("init", ["pg"])

            assert exit_code == 0
            mock_get_class.assert_called_once_with("init")
            mock_command_class.assert_called_once_with(sqitch)
            mock_command.execute.assert_called_once_with(["pg"])

    def test_run_command_unknown(self):
        """Test running unknown command."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch.object(sqitch, "_get_command_class", return_value=None):
            exit_code = sqitch.run_command("unknown", [])

            assert exit_code == 1

    def test_run_command_sqitch_error(self):
        """Test command execution with SqlitchError."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch.object(sqitch, "_get_command_class") as mock_get_class:
            mock_command_class = Mock()
            mock_command = Mock()
            mock_command.execute.side_effect = SqlitchError("Test error", exitval=5)
            mock_command_class.return_value = mock_command
            mock_get_class.return_value = mock_command_class

            exit_code = sqitch.run_command("init", [])

            assert exit_code == 5

    def test_run_command_keyboard_interrupt(self):
        """Test command execution with keyboard interrupt."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch.object(sqitch, "_get_command_class") as mock_get_class:
            mock_command_class = Mock()
            mock_command = Mock()
            mock_command.execute.side_effect = KeyboardInterrupt()
            mock_command_class.return_value = mock_command
            mock_get_class.return_value = mock_command_class

            exit_code = sqitch.run_command("init", [])

            assert exit_code == 130

    def test_run_command_unexpected_error(self):
        """Test command execution with unexpected error."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch.object(sqitch, "_get_command_class") as mock_get_class:
            mock_command_class = Mock()
            mock_command = Mock()
            mock_command.execute.side_effect = RuntimeError("Unexpected error")
            mock_command_class.return_value = mock_command
            mock_get_class.return_value = mock_command_class

            exit_code = sqitch.run_command("init", [])

            assert exit_code == 2

    def test_validate_user_info_complete(self):
        """Test user info validation when complete."""
        config = Config()
        sqitch = Sqitch(config=config)
        sqitch.user_name = "John Doe"
        sqitch.user_email = "john@example.com"

        issues = sqitch.validate_user_info()
        assert issues == []

    def test_validate_user_info_missing_name(self):
        """Test user info validation with missing name."""
        config = Config()
        sqitch = Sqitch(config=config)
        sqitch.user_name = None
        sqitch.user_email = "john@example.com"

        issues = sqitch.validate_user_info()
        assert len(issues) == 1
        assert "Cannot find your name" in issues[0]

    def test_validate_user_info_missing_email(self):
        """Test user info validation with missing email."""
        config = Config()
        sqitch = Sqitch(config=config)
        sqitch.user_name = "John Doe"
        sqitch.user_email = None

        issues = sqitch.validate_user_info()
        assert len(issues) == 1
        assert "Cannot infer your email address" in issues[0]

    def test_validate_user_info_missing_both(self):
        """Test user info validation with missing name and email."""
        config = Config()
        sqitch = Sqitch(config=config)
        sqitch.user_name = None
        sqitch.user_email = None

        issues = sqitch.validate_user_info()
        assert len(issues) == 2

    def test_get_plan_file_default(self):
        """Test getting default plan file path."""
        config = Config()
        sqitch = Sqitch(config=config)

        plan_file = sqitch.get_plan_file()
        assert plan_file == Path("./sqitch.plan")

    def test_get_plan_file_explicit(self):
        """Test getting explicit plan file path."""
        config = Config()
        sqitch = Sqitch(config=config)

        explicit_path = Path("/tmp/custom.plan")
        plan_file = sqitch.get_plan_file(explicit_path)
        assert plan_file == explicit_path

    def test_get_plan_file_from_config(self):
        """Test getting plan file path from configuration."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write("[core]\nplan_file = custom.plan\ntop_dir = /project\n")
            config_file = Path(f.name)

        try:
            config = Config([config_file])
            sqitch = Sqitch(config=config)

            plan_file = sqitch.get_plan_file()
            assert plan_file == Path("/project/custom.plan")
        finally:
            config_file.unlink()

    def test_get_directories(self):
        """Test getting project directories."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write(
                """[core]
top_dir = /project
deploy_dir = sql/deploy
revert_dir = sql/revert
verify_dir = sql/verify
"""
            )
            config_file = Path(f.name)

        try:
            config = Config([config_file])
            sqitch = Sqitch(config=config)

            assert sqitch.get_top_dir() == Path("/project")
            assert sqitch.get_deploy_dir() == Path("/project/sql/deploy")
            assert sqitch.get_revert_dir() == Path("/project/sql/revert")
            assert sqitch.get_verify_dir() == Path("/project/sql/verify")
        finally:
            config_file.unlink()

    def test_set_verbosity(self):
        """Test setting verbosity level."""
        config = Config()
        sqitch = Sqitch(config=config)

        sqitch.set_verbosity(2)
        assert sqitch.verbosity == 2

        # Test clamping
        sqitch.set_verbosity(10)
        assert sqitch.verbosity == 3

        sqitch.set_verbosity(-10)
        assert sqitch.verbosity == -2

    def test_is_initialized_true(self):
        """Test is_initialized when plan file exists."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch.object(sqitch, "get_plan_file") as mock_get_plan:
            mock_plan_file = Mock()
            mock_plan_file.exists.return_value = True
            mock_get_plan.return_value = mock_plan_file

            assert sqitch.is_initialized() is True

    def test_is_initialized_false(self):
        """Test is_initialized when plan file doesn't exist."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch.object(sqitch, "get_plan_file") as mock_get_plan:
            mock_plan_file = Mock()
            mock_plan_file.exists.return_value = False
            mock_get_plan.return_value = mock_plan_file

            assert sqitch.is_initialized() is False

    def test_require_initialized_success(self):
        """Test require_initialized when initialized."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch.object(sqitch, "is_initialized", return_value=True):
            sqitch.require_initialized()  # Should not raise

    def test_require_initialized_failure(self):
        """Test require_initialized when not initialized."""
        config = Config()
        sqitch = Sqitch(config=config)

        with patch.object(sqitch, "is_initialized", return_value=False):
            with pytest.raises(SqlitchError, match="No project configuration found"):
                sqitch.require_initialized()

    def test_repr(self):
        """Test string representation."""
        config = Config()
        sqitch = Sqitch(config=config)
        sqitch.user_name = "John Doe"
        sqitch.user_email = "john@example.com"

        repr_str = repr(sqitch)
        assert "Sqitch(" in repr_str
        assert "verbosity=0" in repr_str
        assert "user_name='John Doe'" in repr_str
        assert "user_email='john@example.com'" in repr_str


class TestCreateSqitch:
    """Test cases for the create_sqitch function."""

    def test_create_sqitch_minimal(self):
        """Test creating Sqitch with minimal parameters."""
        sqitch = create_sqitch()

        assert isinstance(sqitch, Sqitch)
        assert isinstance(sqitch.config, Config)
        assert sqitch.options == {}

    def test_create_sqitch_with_config_files(self):
        """Test creating Sqitch with config files."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write("[user]\nname = Test User\n")
            config_file = Path(f.name)

        try:
            sqitch = create_sqitch([config_file])

            assert sqitch.user_name == "Test User"
        finally:
            config_file.unlink()

    def test_create_sqitch_with_cli_options(self):
        """Test creating Sqitch with CLI options."""
        cli_options = {"verbosity": 1, "config_override": "test"}

        sqitch = create_sqitch(cli_options=cli_options)

        assert sqitch.options == cli_options
        assert sqitch.verbosity == 1

    def test_create_sqitch_configuration_error(self):
        """Test creating Sqitch with configuration error."""
        with patch("sqlitch.core.sqitch.Config") as mock_config:
            mock_config.side_effect = Exception("Config error")

            with pytest.raises(
                ConfigurationError, match="Failed to create sqitch instance"
            ):
                create_sqitch()
