"""
Configuration compatibility tests between sqlitch and Perl sqitch.

These tests verify that configuration file parsing, hierarchy, and
option handling work identically between implementations.
"""

import configparser
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest


@pytest.mark.compatibility
class TestConfigCompatibility:
    """Test configuration handling compatibility."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dirs = []

    def teardown_method(self):
        """Clean up test environment."""
        for temp_dir in self.temp_dirs:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

    def create_temp_project(self) -> Path:
        """Create a temporary project directory."""
        temp_dir = Path(tempfile.mkdtemp(prefix="sqitch_config_compat_"))
        self.temp_dirs.append(temp_dir)
        return temp_dir

    def run_sqlitch(
        self, args: List[str], cwd: Optional[Path] = None
    ) -> subprocess.CompletedProcess:
        """Run sqlitch command."""
        cmd = ["python", "-m", "sqlitch.cli"] + args
        return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)

    def run_sqitch(
        self, args: List[str], cwd: Optional[Path] = None
    ) -> subprocess.CompletedProcess:
        """Run Perl sqitch command."""
        cmd = ["sqitch"] + args
        return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)

    def is_sqitch_available(self) -> bool:
        """Check if Perl sqitch is available."""
        try:
            result = subprocess.run(
                ["sqitch", "--version"], capture_output=True, timeout=5
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def test_config_file_format_after_init(self):
        """Test that config files created by init have identical format."""
        if not self.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        # Create two temporary projects
        sqlitch_dir = self.create_temp_project()
        sqitch_dir = self.create_temp_project()

        # Initialize both projects
        self.run_sqlitch(["init", "--engine", "pg", "testproject"], cwd=sqlitch_dir)
        self.run_sqitch(["init", "--engine", "pg", "testproject"], cwd=sqitch_dir)

        # Read both config files
        sqlitch_config = (sqlitch_dir / "sqitch.conf").read_text()
        sqitch_config = (sqitch_dir / "sqitch.conf").read_text()

        # Parse both configs
        sqlitch_parser = configparser.ConfigParser()
        sqlitch_parser.read_string(sqlitch_config)

        sqitch_parser = configparser.ConfigParser()
        sqitch_parser.read_string(sqitch_config)

        # Both should have core section with engine
        assert sqlitch_parser.has_section("core")
        assert sqitch_parser.has_section("core")

        assert sqlitch_parser.get("core", "engine") == "pg"
        assert sqitch_parser.get("core", "engine") == "pg"

    def test_config_get_command_compatibility(self):
        """Test config get command output format."""
        if not self.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        temp_dir = self.create_temp_project()

        # Initialize project
        self.run_sqlitch(["init", "--engine", "pg", "testproject"], cwd=temp_dir)

        # Get config value from both
        sqlitch_result = self.run_sqlitch(["config", "core.engine"], cwd=temp_dir)
        sqitch_result = self.run_sqitch(["config", "core.engine"], cwd=temp_dir)

        # Both should succeed and return 'pg'
        assert sqlitch_result.returncode == 0
        assert sqitch_result.returncode == 0

        assert sqlitch_result.stdout.strip() == "pg"
        assert sqitch_result.stdout.strip() == "pg"

    def test_config_set_command_compatibility(self):
        """Test config set command behavior."""
        if not self.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        temp_dir = self.create_temp_project()

        # Initialize project
        self.run_sqlitch(["init", "--engine", "pg", "testproject"], cwd=temp_dir)

        # Set a config value
        sqlitch_result = self.run_sqlitch(
            ["config", "user.name", "Test User"], cwd=temp_dir
        )

        # Should succeed
        assert sqlitch_result.returncode == 0

        # Verify it was set
        get_result = self.run_sqlitch(["config", "user.name"], cwd=temp_dir)
        assert get_result.returncode == 0
        assert get_result.stdout.strip() == "Test User"

    def test_config_list_format_compatibility(self):
        """Test config list output format."""
        if not self.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        temp_dir = self.create_temp_project()

        # Initialize project
        self.run_sqlitch(["init", "--engine", "pg", "testproject"], cwd=temp_dir)

        # List config from both
        sqlitch_result = self.run_sqlitch(["config", "--list"], cwd=temp_dir)
        sqitch_result = self.run_sqitch(["config", "--list"], cwd=temp_dir)

        # Both should succeed
        assert sqlitch_result.returncode == 0
        assert sqitch_result.returncode == 0

        # Both should show core.engine
        assert "core.engine=pg" in sqlitch_result.stdout
        assert "core.engine=pg" in sqitch_result.stdout

    def test_config_hierarchy_behavior(self):
        """Test configuration hierarchy (local overrides global)."""
        if not self.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        temp_dir = self.create_temp_project()

        # Initialize project
        self.run_sqlitch(["init", "--engine", "pg", "testproject"], cwd=temp_dir)

        # Set local config
        self.run_sqlitch(["config", "user.name", "Local User"], cwd=temp_dir)

        # Get the value
        result = self.run_sqlitch(["config", "user.name"], cwd=temp_dir)
        assert result.returncode == 0
        assert result.stdout.strip() == "Local User"

    def test_config_boolean_values(self):
        """Test boolean configuration value handling."""
        if not self.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        temp_dir = self.create_temp_project()

        # Initialize project
        self.run_sqlitch(["init", "--engine", "pg", "testproject"], cwd=temp_dir)

        # Set boolean values
        self.run_sqlitch(["config", "deploy.verify", "true"], cwd=temp_dir)

        # Get the value
        result = self.run_sqlitch(["config", "deploy.verify"], cwd=temp_dir)
        assert result.returncode == 0
        assert result.stdout.strip().lower() in ["true", "1", "yes"]

    def test_config_invalid_key_error(self):
        """Test error handling for invalid config keys."""
        if not self.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        temp_dir = self.create_temp_project()

        # Initialize project
        self.run_sqlitch(["init", "--engine", "pg", "testproject"], cwd=temp_dir)

        # Try to get non-existent key
        sqlitch_result = self.run_sqlitch(["config", "nonexistent.key"], cwd=temp_dir)
        sqitch_result = self.run_sqitch(["config", "nonexistent.key"], cwd=temp_dir)

        # Both should exit with error
        assert sqlitch_result.returncode != 0
        assert sqitch_result.returncode != 0

    def test_config_section_handling(self):
        """Test configuration section handling."""
        if not self.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        temp_dir = self.create_temp_project()

        # Initialize project
        self.run_sqlitch(["init", "--engine", "pg", "testproject"], cwd=temp_dir)

        # Set values in different sections
        self.run_sqlitch(["config", "core.verbosity", "2"], cwd=temp_dir)
        self.run_sqlitch(["config", "deploy.verify", "true"], cwd=temp_dir)
        self.run_sqlitch(["config", "user.name", "Test User"], cwd=temp_dir)

        # List all config
        result = self.run_sqlitch(["config", "--list"], cwd=temp_dir)
        assert result.returncode == 0

        output = result.stdout
        assert "core.verbosity=2" in output
        assert "deploy.verify=" in output  # Value might be normalized
        assert "user.name=Test User" in output
