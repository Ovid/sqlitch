"""
Unit tests for the rebase command.

This module tests the RebaseCommand class functionality including argument parsing,
configuration handling, and rebase operation logic.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.commands.rebase import RebaseCommand
from sqlitch.core.change import Change
from sqlitch.core.config import Config
from sqlitch.core.exceptions import PlanError, SqlitchError
from sqlitch.core.plan import Plan


class TestRebaseCommand:
    """Test cases for RebaseCommand."""

    @pytest.fixture
    def mock_sqitch(self):
        """Create mock Sqitch instance."""
        sqitch = Mock()
        sqitch.config = Mock(spec=Config)
        sqitch.logger = Mock()
        sqitch.require_initialized = Mock()
        sqitch.validate_user_info = Mock(return_value=[])
        sqitch.get_target = Mock()
        sqitch.engine_for_target = Mock()
        sqitch.info = Mock()
        sqitch.warn = Mock()
        sqitch.vent = Mock()
        sqitch.debug = Mock()
        sqitch.trace = Mock()
        sqitch.comment = Mock()
        sqitch.emit = Mock()
        sqitch.ask_yes_no = Mock(return_value=True)
        sqitch.prompt = Mock(return_value="test")
        return sqitch

    @pytest.fixture
    def rebase_command(self, mock_sqitch):
        """Create RebaseCommand instance."""
        return RebaseCommand(mock_sqitch)

    @pytest.fixture
    def sample_plan(self, tmp_path):
        """Create sample plan for testing."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=test_project
%uri=https://example.com/test

change1 2023-01-01T10:00:00Z Test User <test@example.com> # First change
change2 [change1] 2023-01-02T10:00:00Z Test User <test@example.com> # Second change
@v1.0 2023-01-03T10:00:00Z Test User <test@example.com> # Version 1.0
change3 [change2] 2023-01-04T10:00:00Z Test User <test@example.com> # Third change
"""
        plan_file.write_text(plan_content)
        return Plan.from_file(plan_file)

    def test_parse_args_basic(self, rebase_command):
        """Test basic argument parsing."""
        # Mock config to return non-strict mode
        rebase_command.config.get = Mock(return_value=False)

        args = ["change1", "change2"]
        options = rebase_command._parse_args(args)

        assert options["onto_change"] is None
        assert options["upto_change"] is None
        assert options["modified"] is False
        assert options["verify"] is False
        assert options["no_prompt"] is False

    def test_parse_args_with_options(self, rebase_command):
        """Test argument parsing with options."""
        # Mock config to return non-strict mode
        rebase_command.config.get = Mock(return_value=False)

        args = [
            "--target",
            "test_target",
            "--onto-change",
            "change1",
            "--upto-change",
            "change3",
            "--modified",
            "--verify",
            "--log-only",
            "--lock-timeout",
            "30",
            "-y",
            "--mode",
            "tag",
            "--set",
            "var1=value1",
            "--set-deploy",
            "deploy_var=deploy_value",
            "--set-revert",
            "revert_var=revert_value",
        ]

        options = rebase_command._parse_args(args)

        assert options["target"] == "test_target"
        assert options["onto_change"] == "change1"
        assert options["upto_change"] == "change3"
        assert options["modified"] is True
        assert options["verify"] is True
        assert options["log_only"] is True
        assert options["lock_timeout"] == 30
        assert options["no_prompt"] is True
        assert options["mode"] == "tag"
        assert options["deploy_variables"]["var1"] == "value1"
        assert options["deploy_variables"]["deploy_var"] == "deploy_value"
        assert options["revert_variables"]["var1"] == "value1"
        assert options["revert_variables"]["revert_var"] == "revert_value"

    def test_parse_args_invalid_mode(self, rebase_command):
        """Test argument parsing with invalid mode."""
        args = ["--mode", "invalid"]

        with pytest.raises(SqlitchError, match="Invalid mode: invalid"):
            rebase_command._parse_args(args)

    def test_parse_args_invalid_lock_timeout(self, rebase_command):
        """Test argument parsing with invalid lock timeout."""
        args = ["--lock-timeout", "invalid"]

        with pytest.raises(SqlitchError, match="Invalid lock timeout: invalid"):
            rebase_command._parse_args(args)

    def test_parse_args_missing_option_value(self, rebase_command):
        """Test argument parsing with missing option value."""
        args = ["--target"]

        with pytest.raises(SqlitchError, match="Option --target requires a value"):
            rebase_command._parse_args(args)

    def test_parse_args_unknown_option(self, rebase_command):
        """Test argument parsing with unknown option."""
        args = ["--unknown-option"]

        with pytest.raises(SqlitchError, match="Unknown option: --unknown-option"):
            rebase_command._parse_args(args)

    def test_apply_config_defaults(self, rebase_command):
        """Test applying configuration defaults."""
        rebase_command.config.get = Mock(
            side_effect=lambda key, as_bool=False: {
                "rebase.verify": True,
                "rebase.mode": "change",
                "rebase.no_prompt": True,
                "rebase.prompt_accept": False,
            }.get(key, False if as_bool else None)
        )

        options = {
            "verify": None,
            "mode": "all",
            "no_prompt": False,
            "prompt_accept": True,
        }

        rebase_command._apply_config_defaults(options)

        assert options["verify"] is True
        assert options["mode"] == "change"
        assert options["no_prompt"] is True
        assert (
            options["prompt_accept"] is True
        )  # Should remain True since condition is not met

    def test_apply_config_defaults_strict_mode(self, rebase_command):
        """Test strict mode configuration."""
        rebase_command.config.get = Mock(
            side_effect=lambda key, as_bool=False: {"rebase.strict": True}.get(
                key, False if as_bool else None
            )
        )

        options = {
            "verify": False,
            "mode": "all",
            "no_prompt": False,
            "prompt_accept": True,
        }

        with pytest.raises(SqlitchError, match="cannot be used in strict mode"):
            rebase_command._apply_config_defaults(options)

    def test_load_plan_default(self, rebase_command, tmp_path):
        """Test loading plan with default file."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=test_project

change1 2023-01-01T10:00:00Z Test User <test@example.com> # First change
"""
        plan_file.write_text(plan_content)

        rebase_command.config.get = Mock(return_value=str(plan_file))

        with patch("pathlib.Path.cwd", return_value=tmp_path):
            plan = rebase_command._load_plan()
            assert plan.project == "test_project"
            assert len(plan.changes) == 1

    def test_load_plan_not_found(self, rebase_command):
        """Test loading non-existent plan file."""
        with pytest.raises(PlanError, match="Plan file not found"):
            rebase_command._load_plan(Path("nonexistent.plan"))

    def test_determine_onto_change_modified(self, rebase_command):
        """Test determining onto change in modified mode."""
        options = {"modified": True, "onto_change": None}
        engine = Mock()
        engine.planned_deployed_common_ancestor_id = Mock(return_value="change1")
        plan = Mock()
        args = []

        result = rebase_command._determine_onto_change(options, engine, plan, args)
        assert result == "change1"
        engine.planned_deployed_common_ancestor_id.assert_called_once()

    def test_determine_onto_change_explicit(self, rebase_command):
        """Test determining onto change with explicit option."""
        options = {"modified": False, "onto_change": "change2"}
        engine = Mock()
        plan = Mock()
        args = []

        result = rebase_command._determine_onto_change(options, engine, plan, args)
        assert result == "change2"

    def test_determine_onto_change_from_args(self, rebase_command):
        """Test determining onto change from arguments."""
        options = {"modified": False, "onto_change": None}
        engine = Mock()
        plan = Mock()
        args = ["change1", "change2"]

        result = rebase_command._determine_onto_change(options, engine, plan, args)
        assert result == "change1"

    def test_determine_upto_change_explicit(self, rebase_command):
        """Test determining upto change with explicit option."""
        options = {"upto_change": "change3", "onto_change": None, "modified": False}
        engine = Mock()
        plan = Mock()
        args = ["change1", "change2"]
        onto_change = "change1"

        result = rebase_command._determine_upto_change(
            options, engine, plan, args, onto_change
        )
        assert result == "change3"

    def test_determine_upto_change_from_args(self, rebase_command):
        """Test determining upto change from arguments."""
        options = {"upto_change": None, "onto_change": None, "modified": False}
        engine = Mock()
        plan = Mock()
        args = ["change1", "change2"]
        onto_change = "change1"

        result = rebase_command._determine_upto_change(
            options, engine, plan, args, onto_change
        )
        assert result == "change2"

    def test_warn_about_extra_args(self, rebase_command):
        """Test warning about extra arguments."""
        options = {"onto_change": None, "upto_change": None, "modified": False}
        args = ["change1", "change2", "change3"]
        onto_change = "change1"
        upto_change = "change2"

        rebase_command._warn_about_extra_args(options, args, onto_change, upto_change)
        rebase_command.sqitch.warn.assert_called_once()
        assert (
            "Too many changes specified" in rebase_command.sqitch.warn.call_args[0][0]
        )

    def test_collect_revert_vars(self, rebase_command):
        """Test collecting revert variables."""
        rebase_command.config.get_section = Mock(
            side_effect=lambda section: {
                "core.variables": {"core_var": "core_value"},
                "deploy.variables": {"deploy_var": "deploy_value"},
                "revert.variables": {"revert_var": "revert_value"},
            }.get(section, {})
        )

        target = Mock()
        target.variables = {"target_var": "target_value"}

        options = {"revert_variables": {"cmd_var": "cmd_value"}}

        result = rebase_command._collect_revert_vars(target, options)

        assert result["core_var"] == "core_value"
        assert result["deploy_var"] == "deploy_value"
        assert result["revert_var"] == "revert_value"
        assert result["target_var"] == "target_value"
        assert result["cmd_var"] == "cmd_value"

    def test_collect_deploy_vars(self, rebase_command):
        """Test collecting deploy variables."""
        rebase_command.config.get_section = Mock(
            side_effect=lambda section: {
                "core.variables": {"core_var": "core_value"},
                "deploy.variables": {"deploy_var": "deploy_value"},
            }.get(section, {})
        )

        target = Mock()
        target.variables = {"target_var": "target_value"}

        options = {"deploy_variables": {"cmd_var": "cmd_value"}}

        result = rebase_command._collect_deploy_vars(target, options)

        assert result["core_var"] == "core_value"
        assert result["deploy_var"] == "deploy_value"
        assert result["target_var"] == "target_value"
        assert result["cmd_var"] == "cmd_value"

    def test_execute_success(self, rebase_command, sample_plan, tmp_path):
        """Test successful rebase execution."""
        # Setup mocks
        mock_engine = Mock()
        mock_engine.planned_deployed_common_ancestor_id = Mock(return_value="change1")
        mock_engine.revert = Mock()
        mock_engine.deploy = Mock()
        mock_engine.set_variables = Mock()
        rebase_command.get_engine = Mock(return_value=mock_engine)

        target = Mock()
        target.variables = {}
        rebase_command.get_target = Mock(return_value=target)

        # Mock config to return non-strict mode
        rebase_command.config.get = Mock(return_value=False)
        rebase_command.config.get_section = Mock(return_value={})

        # Setup plan loading
        plan_file = tmp_path / "sqitch.plan"
        sample_plan.save = Mock()
        sample_plan.file = plan_file

        with patch.object(rebase_command, "_load_plan", return_value=sample_plan):
            result = rebase_command.execute(
                ["--onto-change", "change1", "--upto-change", "change2"]
            )

        assert result == 0
        mock_engine.revert.assert_called_once()
        mock_engine.deploy.assert_called_once()

    def test_execute_revert_error(self, rebase_command, sample_plan):
        """Test rebase execution with revert error."""
        # Setup mocks
        mock_engine = Mock()
        mock_engine.revert = Mock(side_effect=SqlitchError("Revert failed", exitval=2))
        rebase_command.get_engine = Mock(return_value=mock_engine)

        target = Mock()
        target.variables = {}
        rebase_command.get_target = Mock(return_value=target)

        with patch.object(rebase_command, "_load_plan", return_value=sample_plan):
            result = rebase_command.execute(["--onto-change", "change1"])

        assert result == 1  # Error handled

    def test_execute_plan_not_found(self, rebase_command):
        """Test rebase execution with missing plan file."""
        with patch.object(
            rebase_command, "_load_plan", side_effect=PlanError("Plan not found")
        ):
            result = rebase_command.execute([])

        assert result == 1  # Error handled

    def test_execute_not_initialized(self, rebase_command):
        """Test rebase execution in uninitialized project."""
        rebase_command.require_initialized = Mock(
            side_effect=SqlitchError("Not initialized")
        )

        result = rebase_command.execute([])
        assert result == 1  # Error handled

    def test_execute_invalid_user_info(self, rebase_command):
        """Test rebase execution with invalid user info."""
        rebase_command.validate_user_info = Mock(
            side_effect=SqlitchError("Invalid user info")
        )

        result = rebase_command.execute([])
        assert result == 1  # Error handled


class TestRebaseCommandIntegration:
    """Integration tests for rebase command with real components."""

    @pytest.fixture
    def config_file(self, tmp_path):
        """Create test configuration file."""
        config_file = tmp_path / "sqitch.conf"
        config_content = """[core]
    engine = pg
    plan_file = sqitch.plan
    
[engine "pg"]
    target = db:pg://user@localhost/test
    
[rebase]
    verify = true
    mode = all
"""
        config_file.write_text(config_content)
        return config_file

    @pytest.fixture
    def plan_file(self, tmp_path):
        """Create test plan file."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=test_rebase
%uri=https://example.com/test_rebase

initial 2023-01-01T10:00:00Z Test User <test@example.com> # Initial schema
users [initial] 2023-01-02T10:00:00Z Test User <test@example.com> # Add users table
@v1.0 2023-01-03T10:00:00Z Test User <test@example.com> # Version 1.0
posts [users] 2023-01-04T10:00:00Z Test User <test@example.com> # Add posts table
"""
        plan_file.write_text(plan_content)
        return plan_file

    def test_rebase_with_real_plan(self, tmp_path, config_file, plan_file):
        """Test rebase command with real plan file."""
        # Change to test directory
        import os

        from sqlitch.core.config import Config
        from sqlitch.core.sqitch import Sqitch

        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)

            config = Config([config_file])
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)

            # Mock the engine creation and operations
            mock_engine = Mock()
            mock_engine.planned_deployed_common_ancestor_id = Mock(
                return_value="initial"
            )
            mock_engine.revert = Mock()
            mock_engine.deploy = Mock()
            mock_engine.set_variables = Mock()
            command.get_engine = Mock(return_value=mock_engine)

            result = command.execute(
                ["--onto-change", "initial", "--upto-change", "users"]
            )

            assert result == 0
            mock_engine.revert.assert_called_once()
            mock_engine.deploy.assert_called_once()

        finally:
            os.chdir(old_cwd)
