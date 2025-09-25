"""Unit tests for the config command."""

from pathlib import Path
from unittest.mock import Mock, PropertyMock, patch

import pytest

from sqlitch.commands.config import ConfigCommand
from sqlitch.core.config import Config
from sqlitch.core.exceptions import SqlitchError
from sqlitch.core.sqitch import Sqitch


@pytest.fixture
def mock_sqitch():
    """Create a mock Sqitch instance."""
    sqitch = Mock(spec=Sqitch)
    sqitch.config = Mock(spec=Config)
    sqitch.logger = Mock()

    # Mock config methods
    sqitch.config.get.return_value = None
    sqitch.config.set.return_value = None
    sqitch.config._merged_config = {}

    return sqitch


@pytest.fixture
def config_command(mock_sqitch):
    """Create a ConfigCommand instance."""
    return ConfigCommand(mock_sqitch)


class TestConfigCommand:
    """Test the ConfigCommand class."""

    def test_init(self, mock_sqitch):
        """Test ConfigCommand initialization."""
        command = ConfigCommand(mock_sqitch)
        assert command.sqitch == mock_sqitch
        assert command.config == mock_sqitch.config

    def test_execute_list_action(self, config_command, mock_sqitch):
        """Test execute with list action."""
        mock_sqitch.config._merged_config = {
            "core": {"engine": "pg"},
            "user": {"name": "Test User"},
        }

        with patch.object(config_command, "emit") as mock_emit:
            result = config_command.execute(["--list"])

        assert result == 0
        # Should emit config values
        mock_emit.assert_called()

    def test_execute_get_action(self, config_command, mock_sqitch):
        """Test execute with get action."""
        mock_sqitch.config.get.return_value = "pg"

        with patch.object(config_command, "emit") as mock_emit:
            result = config_command.execute(["core.engine"])

        assert result == 0
        mock_sqitch.config.get.assert_called_with("core.engine")
        mock_emit.assert_called_with("pg")

    def test_execute_get_action_not_found(self, config_command, mock_sqitch):
        """Test execute with get action when key not found."""
        mock_sqitch.config.get.return_value = None

        result = config_command.execute(["nonexistent.key"])

        assert result == 1
        mock_sqitch.config.get.assert_called_with("nonexistent.key")

    def test_execute_set_action(self, config_command, mock_sqitch):
        """Test execute with set action."""
        result = config_command.execute(["core.engine", "mysql"])

        assert result == 0
        mock_sqitch.config.set.assert_called_with("core.engine", "mysql")

    def test_execute_unknown_action(self, config_command):
        """Test execute with unknown action."""
        with patch.object(config_command, "error"):
            result = config_command.execute([])

        assert result == 0  # List is default action

    def test_execute_exception_handling(self, config_command):
        """Test execute with exception handling."""
        with patch.object(
            config_command, "_parse_args", side_effect=Exception("Test error")
        ):
            with patch.object(
                config_command, "handle_error", return_value=1
            ) as mock_handle:
                result = config_command.execute(["test"])

        assert result == 1
        mock_handle.assert_called_once()


class TestConfigCommandArgumentParsing:
    """Test argument parsing for ConfigCommand."""

    def test_parse_args_list_flag(self, config_command):
        """Test parsing list flag."""
        action, key, value, options = config_command._parse_args(["--list"])

        assert action == "list"
        assert key is None
        assert value is None
        assert options["list"] is True

    def test_parse_args_list_short_flag(self, config_command):
        """Test parsing list short flag."""
        action, key, value, options = config_command._parse_args(["-l"])

        assert action == "list"
        assert options["list"] is True

    def test_parse_args_local_flag(self, config_command):
        """Test parsing local flag."""
        action, key, value, options = config_command._parse_args(["--local", "key"])

        assert action == "get"
        assert key == "key"
        assert options["local"] is True

    def test_parse_args_user_flag(self, config_command):
        """Test parsing user flag."""
        action, key, value, options = config_command._parse_args(["--user", "key"])

        assert action == "get"
        assert options["user"] is True

    def test_parse_args_global_flag(self, config_command):
        """Test parsing global flag (alias for user)."""
        action, key, value, options = config_command._parse_args(["--global", "key"])

        assert action == "get"
        assert options["user"] is True

    def test_parse_args_system_flag(self, config_command):
        """Test parsing system flag."""
        action, key, value, options = config_command._parse_args(["--system", "key"])

        assert action == "get"
        assert options["system"] is True

    def test_parse_args_get_key(self, config_command):
        """Test parsing get action with key."""
        action, key, value, options = config_command._parse_args(["core.engine"])

        assert action == "get"
        assert key == "core.engine"
        assert value is None

    def test_parse_args_set_key_value(self, config_command):
        """Test parsing set action with key and value."""
        action, key, value, options = config_command._parse_args(
            ["core.engine", "mysql"]
        )

        assert action == "set"
        assert key == "core.engine"
        assert value == "mysql"

    def test_parse_args_no_args(self, config_command):
        """Test parsing with no arguments."""
        action, key, value, options = config_command._parse_args([])

        assert action == "list"
        assert key is None
        assert value is None

    def test_parse_args_unexpected_argument(self, config_command):
        """Test parsing with unexpected argument."""
        with patch.object(config_command, "error") as mock_error:
            action, key, value, options = config_command._parse_args(
                ["key", "value", "extra"]
            )

        mock_error.assert_called_with("Unexpected argument: extra")


class TestConfigCommandListConfig:
    """Test list configuration functionality."""

    def test_list_config_empty(self, config_command, mock_sqitch):
        """Test listing empty configuration."""
        mock_sqitch.config._merged_config = {}

        result = config_command._list_config({})

        assert result == 0

    def test_list_config_simple_values(self, config_command, mock_sqitch):
        """Test listing simple configuration values."""
        mock_sqitch.config._merged_config = {
            "core": {"engine": "pg"},
            "user": {"name": "Test User", "email": "test@example.com"},
        }

        with patch.object(config_command, "emit") as mock_emit:
            result = config_command._list_config({})

        assert result == 0
        # Should emit all config values
        expected_calls = [
            "core.engine=pg",
            "user.name=Test User",
            "user.email=test@example.com",
        ]
        for expected in expected_calls:
            assert any(expected in str(call) for call in mock_emit.call_args_list)

    def test_list_config_list_values(self, config_command, mock_sqitch):
        """Test listing configuration with list values."""
        mock_sqitch.config._merged_config = {
            "core": {"variables": ["var1=value1", "var2=value2"]}
        }

        with patch.object(config_command, "emit") as mock_emit:
            result = config_command._list_config({})

        assert result == 0
        # Should emit each list item separately
        mock_emit.assert_any_call("core.variables=var1=value1")
        mock_emit.assert_any_call("core.variables=var2=value2")

    def test_list_config_exception(self, config_command, mock_sqitch):
        """Test list config with exception."""
        # Make _merged_config access raise an exception
        type(mock_sqitch.config)._merged_config = PropertyMock(
            side_effect=Exception("Config error")
        )

        with patch.object(config_command, "error") as mock_error:
            result = config_command._list_config({})

        assert result == 1
        mock_error.assert_called()


class TestConfigCommandGetConfig:
    """Test get configuration functionality."""

    def test_get_config_no_key(self, config_command):
        """Test get config without key."""
        with patch.object(config_command, "error") as mock_error:
            result = config_command._get_config(None, {})

        assert result == 1
        mock_error.assert_called_with("Configuration key is required")

    def test_get_config_string_value(self, config_command, mock_sqitch):
        """Test get config with string value."""
        mock_sqitch.config.get.return_value = "pg"

        with patch.object(config_command, "emit") as mock_emit:
            result = config_command._get_config("core.engine", {})

        assert result == 0
        mock_emit.assert_called_with("pg")

    def test_get_config_list_value(self, config_command, mock_sqitch):
        """Test get config with list value."""
        mock_sqitch.config.get.return_value = ["value1", "value2"]

        with patch.object(config_command, "emit") as mock_emit:
            result = config_command._get_config("core.variables", {})

        assert result == 0
        mock_emit.assert_any_call("value1")
        mock_emit.assert_any_call("value2")

    def test_get_config_not_found(self, config_command, mock_sqitch):
        """Test get config when key not found."""
        mock_sqitch.config.get.return_value = None

        result = config_command._get_config("nonexistent.key", {})

        assert result == 1

    def test_get_config_exception(self, config_command, mock_sqitch):
        """Test get config with exception."""
        mock_sqitch.config.get.side_effect = Exception("Config error")

        with patch.object(config_command, "error") as mock_error:
            result = config_command._get_config("core.engine", {})

        assert result == 1
        mock_error.assert_called()


class TestConfigCommandSetConfig:
    """Test set configuration functionality."""

    def test_set_config_no_key(self, config_command):
        """Test set config without key."""
        with patch.object(config_command, "error") as mock_error:
            result = config_command._set_config(None, "value", {})

        assert result == 1
        mock_error.assert_called_with(
            "Both key and value are required for setting configuration"
        )

    def test_set_config_no_value(self, config_command):
        """Test set config without value."""
        with patch.object(config_command, "error") as mock_error:
            result = config_command._set_config("key", None, {})

        assert result == 1
        mock_error.assert_called_with(
            "Both key and value are required for setting configuration"
        )

    def test_set_config_success(self, config_command, mock_sqitch):
        """Test successful set config."""
        result = config_command._set_config("core.engine", "mysql", {})

        assert result == 0
        mock_sqitch.config.set.assert_called_with("core.engine", "mysql")

    def test_set_config_exception(self, config_command, mock_sqitch):
        """Test set config with exception."""
        mock_sqitch.config.set.side_effect = Exception("Config error")

        with patch.object(config_command, "error") as mock_error:
            result = config_command._set_config("core.engine", "mysql", {})

        assert result == 1
        mock_error.assert_called()


class TestConfigCommandEmitConfigSection:
    """Test configuration section emission."""

    def test_emit_config_section_dict(self, config_command):
        """Test emitting dictionary configuration."""
        data = {"engine": "pg", "user": {"name": "Test", "email": "test@example.com"}}

        with patch.object(config_command, "emit") as mock_emit:
            config_command._emit_config_section(data, "")

        mock_emit.assert_any_call("engine=pg")
        mock_emit.assert_any_call("user.name=Test")
        mock_emit.assert_any_call("user.email=test@example.com")

    def test_emit_config_section_list(self, config_command):
        """Test emitting list configuration."""
        data = {"variables": ["var1=value1", "var2=value2"]}

        with patch.object(config_command, "emit") as mock_emit:
            config_command._emit_config_section(data, "")

        mock_emit.assert_any_call("variables=var1=value1")
        mock_emit.assert_any_call("variables=var2=value2")

    def test_emit_config_section_simple_value(self, config_command):
        """Test emitting simple value."""
        with patch.object(config_command, "emit") as mock_emit:
            config_command._emit_config_section("pg", "core.engine")

        mock_emit.assert_called_with("core.engine=pg")

    def test_emit_config_section_nested_dict(self, config_command):
        """Test emitting nested dictionary."""
        data = {
            "target": {
                "dev": {"uri": "db:pg://localhost/dev"},
                "prod": {"uri": "db:pg://localhost/prod"},
            }
        }

        with patch.object(config_command, "emit") as mock_emit:
            config_command._emit_config_section(data, "")

        mock_emit.assert_any_call("target.dev.uri=db:pg://localhost/dev")
        mock_emit.assert_any_call("target.prod.uri=db:pg://localhost/prod")


class TestConfigCommandIntegration:
    """Integration tests for ConfigCommand."""

    def test_full_workflow_list(self, mock_sqitch):
        """Test full workflow for listing configuration."""
        mock_sqitch.config._merged_config = {
            "core": {"engine": "pg", "plan_file": "sqitch.plan"},
            "user": {"name": "Test User"},
        }

        command = ConfigCommand(mock_sqitch)

        with patch.object(command, "emit") as mock_emit:
            result = command.execute(["--list"])

        assert result == 0
        assert mock_emit.call_count >= 3  # Should emit all config values

    def test_full_workflow_get_set(self, mock_sqitch):
        """Test full workflow for get and set operations."""
        command = ConfigCommand(mock_sqitch)

        # Test set
        result = command.execute(["core.engine", "mysql"])
        assert result == 0
        mock_sqitch.config.set.assert_called_with("core.engine", "mysql")

        # Test get
        mock_sqitch.config.get.return_value = "mysql"
        with patch.object(command, "emit") as mock_emit:
            result = command.execute(["core.engine"])

        assert result == 0
        mock_emit.assert_called_with("mysql")

    def test_error_handling_integration(self, mock_sqitch):
        """Test error handling in integration scenario."""
        # Make the _parse_args method raise an exception to trigger error handling
        command = ConfigCommand(mock_sqitch)

        with patch.object(command, "_parse_args", side_effect=Exception("Parse error")):
            with patch.object(command, "handle_error", return_value=1) as mock_handle:
                result = command.execute(["core.engine"])

        assert result == 1
        mock_handle.assert_called_once()
