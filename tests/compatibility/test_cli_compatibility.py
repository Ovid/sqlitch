"""
CLI compatibility tests between sqlitch and Perl sqitch.

These tests compare command-line interface behavior, argument parsing,
and output formatting between the Python and Perl implementations.
"""

import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest


class SquitchCompatibilityTester:
    """Helper class for running compatibility tests between sqlitch and sqitch."""

    def __init__(self):
        self.temp_dirs = []

    def cleanup(self):
        """Clean up temporary directories."""
        for temp_dir in self.temp_dirs:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
        self.temp_dirs.clear()

    def create_temp_project(self) -> Path:
        """Create a temporary project directory."""
        temp_dir = Path(tempfile.mkdtemp(prefix="sqitch_compat_"))
        self.temp_dirs.append(temp_dir)
        return temp_dir

    def run_sqlitch(
        self,
        args: List[str],
        cwd: Optional[Path] = None,
        input_text: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        """Run sqlitch command and return result."""
        cmd = ["python", "-m", "sqlitch.cli"] + args
        return subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, input=input_text
        )

    def run_sqitch(
        self,
        args: List[str],
        cwd: Optional[Path] = None,
        input_text: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        """Run Perl sqitch command and return result."""
        cmd = ["sqitch"] + args
        return subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, input=input_text
        )

    def is_sqitch_available(self) -> bool:
        """Check if Perl sqitch is available."""
        try:
            result = subprocess.run(
                ["sqitch", "--version"], capture_output=True, text=True, timeout=5
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def normalize_output(self, output: str) -> str:
        """Normalize output for comparison by removing timestamps and paths."""
        # Remove timestamps
        output = re.sub(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "TIMESTAMP", output)

        # Remove absolute paths, keep relative structure
        output = re.sub(r"/[^\s]+/([^/\s]+)", r"PATH/\1", output)

        # Normalize whitespace
        output = re.sub(r"\s+", " ", output.strip())

        return output

    def compare_outputs(
        self,
        sqlitch_result: subprocess.CompletedProcess,
        sqitch_result: subprocess.CompletedProcess,
        ignore_exit_code: bool = False,
    ) -> Dict[str, Any]:
        """Compare outputs from sqlitch and sqitch commands."""
        comparison = {
            "exit_codes_match": sqlitch_result.returncode == sqitch_result.returncode,
            "stdout_match": False,
            "stderr_match": False,
            "sqlitch_stdout": sqlitch_result.stdout,
            "sqitch_stdout": sqitch_result.stdout,
            "sqlitch_stderr": sqlitch_result.stderr,
            "sqitch_stderr": sqitch_result.stderr,
            "sqlitch_exit_code": sqlitch_result.returncode,
            "sqitch_exit_code": sqitch_result.returncode,
        }

        # Normalize outputs for comparison
        sqlitch_stdout_norm = self.normalize_output(sqlitch_result.stdout)
        sqitch_stdout_norm = self.normalize_output(sqitch_result.stdout)
        sqlitch_stderr_norm = self.normalize_output(sqlitch_result.stderr)
        sqitch_stderr_norm = self.normalize_output(sqitch_result.stderr)

        comparison["stdout_match"] = sqlitch_stdout_norm == sqitch_stdout_norm
        comparison["stderr_match"] = sqlitch_stderr_norm == sqitch_stderr_norm

        return comparison


@pytest.fixture
def compat_tester():
    """Fixture providing compatibility tester."""
    tester = SquitchCompatibilityTester()
    yield tester
    tester.cleanup()


@pytest.mark.compatibility
class TestCLICompatibility:
    """Test CLI compatibility between sqlitch and Perl sqitch."""

    def test_version_output(self, compat_tester):
        """Test that version output format is compatible."""
        if not compat_tester.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        sqlitch_result = compat_tester.run_sqlitch(["--version"])
        sqitch_result = compat_tester.run_sqitch(["--version"])

        # Both should exit successfully
        assert sqlitch_result.returncode == 0
        assert sqitch_result.returncode == 0

        # Both should output version information
        assert "sqlitch" in sqlitch_result.stdout.lower()
        assert "sqitch" in sqitch_result.stdout.lower()

    def test_help_output_structure(self, compat_tester):
        """Test that help output has similar structure."""
        if not compat_tester.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        sqlitch_result = compat_tester.run_sqlitch(["--help"])
        sqitch_result = compat_tester.run_sqitch(["--help"])

        # Both should exit successfully
        assert sqlitch_result.returncode == 0
        assert sqitch_result.returncode == 0

        # Both should mention common commands
        common_commands = ["init", "deploy", "revert", "verify", "status"]
        for cmd in common_commands:
            assert cmd in sqlitch_result.stdout
            assert cmd in sqitch_result.stdout

    def test_invalid_command_error(self, compat_tester):
        """Test that invalid commands produce similar errors."""
        if not compat_tester.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        invalid_cmd = "nonexistent_command_xyz"
        sqlitch_result = compat_tester.run_sqlitch([invalid_cmd])
        sqitch_result = compat_tester.run_sqitch([invalid_cmd])

        # Both should exit with error
        assert sqlitch_result.returncode != 0
        assert sqitch_result.returncode != 0

        # Both should mention the invalid command
        assert (
            invalid_cmd in sqlitch_result.stderr or invalid_cmd in sqlitch_result.stdout
        )
        assert (
            invalid_cmd in sqitch_result.stderr or invalid_cmd in sqitch_result.stdout
        )

    def test_global_options_parsing(self, compat_tester):
        """Test that global options are parsed consistently."""
        if not compat_tester.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        # Test verbose option
        sqlitch_result = compat_tester.run_sqlitch(["--verbose", "--help"])
        sqitch_result = compat_tester.run_sqitch(["--verbose", "--help"])

        assert sqlitch_result.returncode == 0
        assert sqitch_result.returncode == 0

        # Test quiet option
        sqlitch_result = compat_tester.run_sqlitch(["--quiet", "--help"])
        sqitch_result = compat_tester.run_sqitch(["--quiet", "--help"])

        assert sqlitch_result.returncode == 0
        assert sqitch_result.returncode == 0


@pytest.mark.compatibility
class TestInitCommandCompatibility:
    """Test init command compatibility."""

    def test_init_help(self, compat_tester):
        """Test init command help output."""
        if not compat_tester.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        sqlitch_result = compat_tester.run_sqlitch(["init", "--help"])
        sqitch_result = compat_tester.run_sqitch(["init", "--help"])

        assert sqlitch_result.returncode == 0
        assert sqitch_result.returncode == 0

        # Both should mention engine option
        assert "engine" in sqlitch_result.stdout.lower()
        assert "engine" in sqitch_result.stdout.lower()

    def test_init_without_engine_error(self, compat_tester):
        """Test that init without engine produces similar error."""
        if not compat_tester.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        temp_dir = compat_tester.create_temp_project()

        sqlitch_result = compat_tester.run_sqlitch(["init"], cwd=temp_dir)
        sqitch_result = compat_tester.run_sqitch(["init"], cwd=temp_dir)

        # Both should exit with error
        assert sqlitch_result.returncode != 0
        assert sqitch_result.returncode != 0

        # Both should mention engine requirement
        error_output = sqlitch_result.stderr + sqlitch_result.stdout
        assert "engine" in error_output.lower()


@pytest.mark.compatibility
class TestStatusCommandCompatibility:
    """Test status command compatibility."""

    def test_status_no_project_error(self, compat_tester):
        """Test status command error when no project exists."""
        if not compat_tester.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        temp_dir = compat_tester.create_temp_project()

        sqlitch_result = compat_tester.run_sqlitch(["status"], cwd=temp_dir)
        sqitch_result = compat_tester.run_sqitch(["status"], cwd=temp_dir)

        # Both should exit with error
        assert sqlitch_result.returncode != 0
        assert sqitch_result.returncode != 0


@pytest.mark.compatibility
class TestConfigCompatibility:
    """Test configuration handling compatibility."""

    def test_config_list_format(self, compat_tester):
        """Test config list output format."""
        if not compat_tester.is_sqitch_available():
            pytest.skip("Perl sqitch not available")

        temp_dir = compat_tester.create_temp_project()

        # Initialize a project first
        compat_tester.run_sqlitch(["init", "--engine", "pg", "test"], cwd=temp_dir)

        sqlitch_result = compat_tester.run_sqlitch(["config", "--list"], cwd=temp_dir)

        # Should exit successfully and show config
        assert sqlitch_result.returncode == 0
        assert "core.engine" in sqlitch_result.stdout
