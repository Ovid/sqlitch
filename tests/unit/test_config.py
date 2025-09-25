"""
Unit tests for configuration management.

Tests the Config class functionality including file parsing,
hierarchy management, validation, and type coercion.
"""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import mock_open, patch

import pytest

from sqlitch.core.config import Config, ConfigSource
from sqlitch.core.exceptions import ConfigurationError
from sqlitch.core.target import Target
from sqlitch.core.types import URI


class TestConfig:
    """Test cases for Config class."""

    def test_init_empty_config(self):
        """Test initialization with no configuration files."""
        with patch.object(Config, "_load_default_configs"):
            config = Config()
            assert config._cli_options == {}
            assert config._sources == []

    def test_init_with_explicit_files(self, tmp_path):
        """Test initialization with explicit configuration files."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[core]
engine = pg
"""
        )

        config = Config(config_files=[config_file])
        assert len(config._sources) == 1
        assert config._sources[0].path == config_file
        assert config._sources[0].source_type == "explicit"

    def test_init_with_cli_options(self):
        """Test initialization with command-line options."""
        cli_options = {"core.engine": "mysql", "user.name": "test"}
        with patch.object(Config, "_load_default_configs"):
            config = Config(cli_options=cli_options)
            assert config._cli_options == cli_options

    def test_load_config_file_valid(self, tmp_path):
        """Test loading a valid configuration file."""
        config_file = tmp_path / "valid.conf"
        config_file.write_text(
            """
[core]
engine = pg
top_dir = .

[user]
name = John Doe
email = john@example.com

[engine "pg"]
target = db:pg://localhost/test
registry = sqitch
"""
        )

        config = Config()
        parser = config._load_config_file(config_file)

        assert parser.has_section("core")
        assert parser.get("core", "engine") == "pg"
        assert parser.get("user", "name") == "John Doe"

    def test_load_config_file_invalid_syntax(self, tmp_path):
        """Test loading configuration file with invalid syntax."""
        config_file = tmp_path / "invalid.conf"
        config_file.write_text(
            """
[core
engine = pg
"""
        )

        config = Config()
        with pytest.raises(ConfigurationError) as exc_info:
            config._load_config_file(config_file)

        assert "Invalid configuration syntax" in str(exc_info.value)
        assert config_file.name in str(exc_info.value)

    def test_load_config_file_not_found(self, tmp_path):
        """Test loading non-existent configuration file."""
        config_file = tmp_path / "nonexistent.conf"

        config = Config()
        with pytest.raises(ConfigurationError) as exc_info:
            config._load_config_file(config_file)

        assert "Cannot read configuration file" in str(exc_info.value)

    @pytest.mark.compatibility
    def test_get_system_config_paths_unix(self):
        """Test system configuration paths on Unix-like systems."""
        config = Config()

        with patch("sys.platform", "linux"):
            paths = config._get_system_config_paths()

            expected_paths = [
                Path("/etc/sqitch/sqitch.conf"),
                Path("/usr/local/etc/sqitch/sqitch.conf"),
            ]
            assert paths == expected_paths

    def test_get_system_config_paths_windows(self):
        """Test system configuration paths on Windows."""
        config = Config()

        with (
            patch("sys.platform", "win32"),
            patch.dict(os.environ, {"PROGRAMFILES": "C:\\Program Files"}),
        ):
            paths = config._get_system_config_paths()

            # Check that the path contains the expected components
            assert len(paths) > 0
            path_str = str(paths[0])
            assert "Program Files" in path_str
            assert "Sqlitch" in path_str
            assert "sqitch.conf" in path_str

    @pytest.mark.compatibility
    def test_get_global_config_path_unix(self):
        """Test global configuration path on Unix-like systems."""
        with (
            patch("sys.platform", "linux"),
            patch("sqlitch.core.config.Path.home", return_value=Path("/home/user")),
        ):
            config = Config()
            path = config._get_global_config_path()

            expected_path = Path("/home/user/.config/sqlitch/sqitch.conf")
            assert path == expected_path

    def test_get_global_config_path_unix_xdg(self):
        """Test global configuration path with XDG_CONFIG_HOME."""
        config = Config()

        with (
            patch("sys.platform", "linux"),
            patch.dict(os.environ, {"XDG_CONFIG_HOME": "/custom/config"}),
        ):
            path = config._get_global_config_path()

            expected_path = Path("/custom/config/sqlitch/sqitch.conf")
            assert path == expected_path

    def test_get_global_config_path_windows(self):
        """Test global configuration path on Windows."""
        config = Config()

        with (
            patch("sys.platform", "win32"),
            patch("sqlitch.core.config.Path.home", return_value=Path("C:/Users/user")),
        ):
            path = config._get_global_config_path()

            # On non-Windows systems, Path normalizes to forward slashes
            expected_path = Path("C:/Users/user/.sqlitch/sqitch.conf")
            assert path == expected_path

    def test_get_local_config_paths(self, tmp_path):
        """Test local configuration path discovery."""
        # Create nested directory structure
        project_dir = tmp_path / "project" / "subdir"
        project_dir.mkdir(parents=True)

        # Create config file in parent directory
        config_file = tmp_path / "project" / "sqitch.conf"
        config_file.write_text("[core]\nengine = pg")

        config = Config()

        with patch("pathlib.Path.cwd", return_value=project_dir):
            paths = config._get_local_config_paths()

            assert len(paths) == 1
            assert paths[0] == config_file

    def test_parse_subsection(self):
        """Test parsing of subsection names."""
        config = Config()

        main, sub = config._parse_subsection('engine "pg"')
        assert main == "engine"
        assert sub == "pg"

        main, sub = config._parse_subsection('target "production"')
        assert main == "target"
        assert sub == "production"

        main, sub = config._parse_subsection("core")
        assert main == "core"
        assert sub == ""

    def test_merge_configurations_priority(self, tmp_path):
        """Test configuration merging respects priority order."""
        # Create multiple config files
        system_config = tmp_path / "system.conf"
        system_config.write_text(
            """
[core]
engine = pg
verbosity = 0
"""
        )

        local_config = tmp_path / "local.conf"
        local_config.write_text(
            """
[core]
engine = mysql
top_dir = /custom
"""
        )

        # Mock the config loading to use our test files
        with (
            patch.object(
                Config, "_get_system_config_paths", return_value=[system_config]
            ),
            patch.object(Config, "_get_global_config_path", return_value=None),
            patch.object(
                Config, "_get_local_config_paths", return_value=[local_config]
            ),
        ):

            config = Config()

            # Local should override system
            assert config.get("core.engine") == "mysql"
            assert config.get("core.verbosity") == "0"  # From system
            assert config.get("core.top_dir") == "/custom"  # From local

    def test_cli_overrides(self, tmp_path):
        """Test command-line options override configuration files."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[core]
engine = pg
verbosity = 1
"""
        )

        cli_options = {"core.engine": "mysql", "user.name": "CLI User"}

        config = Config(config_files=[config_file], cli_options=cli_options)

        assert config.get("core.engine") == "mysql"  # CLI override
        assert config.get("core.verbosity") == "1"  # From file
        assert config.get("user.name") == "CLI User"  # CLI only

    def test_get_simple_key(self, tmp_path):
        """Test getting simple configuration values."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[core]
engine = pg
verbosity = 2
"""
        )

        config = Config(config_files=[config_file])

        assert config.get("core.engine") == "pg"
        assert config.get("core.verbosity") == "2"
        assert config.get("core.nonexistent") is None
        assert config.get("core.nonexistent", "default") == "default"

    def test_get_with_type_coercion(self, tmp_path):
        """Test getting values with type coercion."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[core]
verbosity = 2
debug = true
ratio = 3.14
tags = tag1,tag2,tag3
"""
        )

        config = Config(config_files=[config_file])

        assert config.get("core.verbosity", expected_type=int) == 2
        assert config.get("core.debug", expected_type=bool) is True
        assert config.get("core.ratio", expected_type=float) == 3.14
        assert config.get("core.tags", expected_type=list) == ["tag1", "tag2", "tag3"]

    def test_get_invalid_type_coercion(self, tmp_path):
        """Test type coercion with invalid values."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[core]
verbosity = not_a_number
"""
        )

        config = Config(config_files=[config_file])

        with pytest.raises(ConfigurationError) as exc_info:
            config.get("core.verbosity", expected_type=int)

        assert "Cannot convert" in str(exc_info.value)
        assert "core.verbosity" in str(exc_info.value)

    def test_get_invalid_key(self):
        """Test getting value with invalid key."""
        config = Config()

        with pytest.raises(ConfigurationError) as exc_info:
            config.get("invalid key with spaces")

        assert "Invalid configuration key" in str(exc_info.value)

    def test_set_value(self, tmp_path):
        """Test setting configuration values."""
        config_file = tmp_path / "sqitch.conf"
        config = Config()

        config.set("core.engine", "mysql", filename=config_file)
        config.set("user.name", "Test User", filename=config_file)

        assert config.get("core.engine") == "mysql"
        assert config.get("user.name") == "Test User"

        # Verify file was written correctly
        assert config_file.exists()
        content = config_file.read_text()
        assert "engine = mysql" in content
        assert "name = Test User" in content

    def test_set_invalid_key(self, tmp_path):
        """Test setting value with invalid key."""
        config_file = tmp_path / "sqitch.conf"
        config = Config()

        with pytest.raises(ConfigurationError) as exc_info:
            config.set("invalid key", "value", filename=config_file)

        assert "Invalid configuration key" in str(exc_info.value)

    def test_get_target_explicit(self, tmp_path):
        """Test getting explicitly configured target."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[target "production"]
uri = db:pg://prod.example.com/mydb
registry = sqitch_prod
client = psql
"""
        )

        config = Config(config_files=[config_file])
        target = config.get_target("production")

        assert target.name == "production"
        assert target.uri == "db:pg://prod.example.com/mydb"
        assert target.registry == "sqitch_prod"
        assert target.client == "psql"

    @pytest.mark.compatibility
    def test_get_target_default_from_engine(self, tmp_path):
        """Test getting default target from engine configuration."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[core]
engine = pg

[engine "pg"]
target = db:pg://localhost/testdb
registry = sqitch
"""
        )

        config = Config(config_files=[config_file])
        target = config.get_target("default")

        assert target.name == "default"
        assert target.uri == "db:pg://localhost/testdb"
        assert target.registry == "sqitch"

    def test_get_target_not_found(self):
        """Test getting non-existent target."""
        config = Config()

        with pytest.raises(ConfigurationError) as exc_info:
            config.get_target("nonexistent")

        assert "Target 'nonexistent' not found" in str(exc_info.value)

    def test_get_target_missing_uri(self, tmp_path):
        """Test getting target with missing URI."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[target "incomplete"]
registry = sqitch
"""
        )

        config = Config(config_files=[config_file])

        with pytest.raises(ConfigurationError) as exc_info:
            config.get_target("incomplete")

        assert "missing required 'uri' field" in str(exc_info.value)

    def test_get_target_invalid_uri(self, tmp_path):
        """Test getting target with invalid URI."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[target "invalid"]
uri = invalid://not-a-db-uri
"""
        )

        config = Config(config_files=[config_file])

        with pytest.raises(ConfigurationError) as exc_info:
            config.get_target("invalid")

        assert "Invalid URI" in str(exc_info.value)

    def test_get_engine_config(self, tmp_path):
        """Test getting engine-specific configuration."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[engine "pg"]
target = db:pg://localhost/test
registry = sqitch
client = psql

[engine "mysql"]
target = db:mysql://localhost/test
registry = sqitch_mysql
"""
        )

        config = Config(config_files=[config_file])

        pg_config = config.get_engine_config("pg")
        assert pg_config["target"] == "db:pg://localhost/test"
        assert pg_config["registry"] == "sqitch"
        assert pg_config["client"] == "psql"

        mysql_config = config.get_engine_config("mysql")
        assert mysql_config["target"] == "db:mysql://localhost/test"
        assert mysql_config["registry"] == "sqitch_mysql"

        # Non-existent engine returns empty dict
        empty_config = config.get_engine_config("oracle")
        assert empty_config == {}

    def test_get_user_info(self, tmp_path):
        """Test getting user name and email."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[user]
name = John Doe
email = john@example.com
"""
        )

        config = Config(config_files=[config_file])

        assert config.get_user_name() == "John Doe"
        assert config.get_user_email() == "john@example.com"

    def test_get_user_invalid_email(self, tmp_path):
        """Test getting invalid user email."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[user]
email = invalid-email
"""
        )

        config = Config(config_files=[config_file])

        with pytest.raises(ConfigurationError) as exc_info:
            config.get_user_email()

        assert "Invalid email address" in str(exc_info.value)

    def test_get_core_config(self, tmp_path):
        """Test getting core configuration section."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[core]
engine = pg
top_dir = /custom
verbosity = 2
"""
        )

        config = Config(config_files=[config_file])
        core_config = config.get_core_config()

        assert core_config["engine"] == "pg"
        assert core_config["top_dir"] == "/custom"
        assert core_config["verbosity"] == "2"

    def test_list_targets(self, tmp_path):
        """Test listing configured targets."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[core]
engine = pg

[engine "pg"]
target = db:pg://localhost/test

[target "production"]
uri = db:pg://prod.example.com/db

[target "staging"]
uri = db:pg://staging.example.com/db
"""
        )

        config = Config(config_files=[config_file])
        targets = config.list_targets()

        # Should include explicit targets plus default
        expected_targets = ["default", "production", "staging"]
        assert sorted(targets) == expected_targets

    def test_list_engines(self, tmp_path):
        """Test listing configured engines."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[engine "pg"]
target = db:pg://localhost/test

[engine "mysql"]
target = db:mysql://localhost/test

[engine "sqlite"]
target = db:sqlite:test.db
"""
        )

        config = Config(config_files=[config_file])
        engines = config.list_engines()

        expected_engines = ["mysql", "pg", "sqlite"]
        assert engines == expected_engines

    def test_validate_valid_config(self, tmp_path):
        """Test validation of valid configuration."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[user]
name = John Doe
email = john@example.com

[target "test"]
uri = db:pg://localhost/test
"""
        )

        config = Config(config_files=[config_file])
        issues = config.validate()

        assert issues == []

    def test_validate_invalid_config(self, tmp_path):
        """Test validation of invalid configuration."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[user]
email = invalid-email

[target "test"]
uri = invalid://not-a-db-uri
"""
        )

        config = Config(config_files=[config_file])
        issues = config.validate()

        assert len(issues) >= 1
        assert any("Invalid" in issue for issue in issues)

    def test_to_dict(self, tmp_path):
        """Test converting configuration to dictionary."""
        config_file = tmp_path / "test.conf"
        config_file.write_text(
            """
[core]
engine = pg

[user]
name = Test User
"""
        )

        config = Config(config_files=[config_file])
        config_dict = config.to_dict()

        assert "core" in config_dict
        assert "user" in config_dict
        assert config_dict["core"]["engine"] == "pg"
        assert config_dict["user"]["name"] == "Test User"

    def test_get_config_sources(self, tmp_path):
        """Test getting configuration sources."""
        config_file = tmp_path / "test.conf"
        config_file.write_text("[core]\nengine = pg")

        config = Config(config_files=[config_file])
        sources = config.get_config_sources()

        assert len(sources) == 1
        assert sources[0].path == config_file
        assert sources[0].source_type == "explicit"

    def test_repr(self):
        """Test string representation."""
        with patch.object(Config, "_load_default_configs"):
            config = Config()
            config._sources = [
                ConfigSource(None, 10, "system"),
                ConfigSource(None, 20, "global"),
            ]

            repr_str = repr(config)
            assert "Config" in repr_str
            assert "system" in repr_str
            assert "global" in repr_str


class TestConfigSource:
    """Test cases for ConfigSource dataclass."""

    def test_config_source_creation(self):
        """Test creating ConfigSource instances."""
        source = ConfigSource(
            path=Path("/etc/sqitch.conf"), priority=10, source_type="system"
        )

        assert source.path == Path("/etc/sqitch.conf")
        assert source.priority == 10
        assert source.source_type == "system"
        assert source.parser is None

    def test_config_source_with_parser(self):
        """Test ConfigSource with parser."""
        import configparser

        parser = configparser.ConfigParser()

        source = ConfigSource(
            path=Path("/test.conf"), priority=20, source_type="local", parser=parser
        )

        assert source.parser is parser
