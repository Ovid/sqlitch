"""
Framework validation tests for compatibility testing.

These tests validate that the compatibility testing framework itself
works correctly, without requiring Perl sqitch to be installed.
"""
import pytest
import subprocess
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional


@pytest.mark.compatibility
class TestCompatibilityFramework:
    """Test the compatibility testing framework itself."""
    
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
        temp_dir = Path(tempfile.mkdtemp(prefix="sqitch_framework_test_"))
        self.temp_dirs.append(temp_dir)
        return temp_dir
    
    def run_sqlitch(self, args: List[str], cwd: Optional[Path] = None) -> subprocess.CompletedProcess:
        """Run sqlitch command."""
        cmd = ["python", "-m", "sqlitch.cli"] + args
        return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    
    def test_sqlitch_version_command(self):
        """Test that sqlitch version command works."""
        result = self.run_sqlitch(["--version"])
        
        # Should exit successfully
        assert result.returncode == 0
        
        # Should output version information
        assert "sqlitch" in result.stdout.lower()
    
    def test_sqlitch_help_command(self):
        """Test that sqlitch help command works."""
        result = self.run_sqlitch(["--help"])
        
        # Should exit successfully
        assert result.returncode == 0
        
        # Should mention common commands
        common_commands = ["init", "deploy", "revert", "verify", "status"]
        for cmd in common_commands:
            assert cmd in result.stdout
    
    def test_sqlitch_invalid_command(self):
        """Test that sqlitch handles invalid commands properly."""
        result = self.run_sqlitch(["nonexistent_command_xyz"])
        
        # Should exit with error
        assert result.returncode != 0
        
        # Should mention the invalid command
        assert "nonexistent_command_xyz" in result.stderr or "nonexistent_command_xyz" in result.stdout
    
    def test_sqlitch_init_basic_functionality(self):
        """Test basic sqlitch init functionality."""
        temp_dir = self.create_temp_project()
        
        # Initialize project
        result = self.run_sqlitch(["init", "--engine", "pg", "testproject"], cwd=temp_dir)
        
        # Should succeed
        assert result.returncode == 0
        
        # Should create expected files
        assert (temp_dir / "sqitch.plan").exists()
        assert (temp_dir / "sqitch.conf").exists()
        
        # Config should contain engine setting
        config_content = (temp_dir / "sqitch.conf").read_text()
        assert "engine = pg" in config_content
    
    def test_sqlitch_config_operations(self):
        """Test basic sqlitch config operations."""
        temp_dir = self.create_temp_project()
        
        # Initialize project
        result = self.run_sqlitch(["init", "--engine", "pg", "testproject"], cwd=temp_dir)
        assert result.returncode == 0
        
        # Check that config file was created with correct content
        config_file = temp_dir / "sqitch.conf"
        assert config_file.exists()
        
        config_content = config_file.read_text()
        assert "engine = pg" in config_content
        
        # Test that status command recognizes the project
        result = self.run_sqlitch(["status"], cwd=temp_dir)
        # Status should fail because no database is configured, but it should recognize the project
        assert result.returncode != 0
        # Should not complain about missing sqitch.conf
        assert "sqitch.conf" not in result.stderr
    
    def test_compatibility_test_runner_import(self):
        """Test that compatibility test runner can be imported."""
        from tests.compatibility.test_runner import CompatibilityTestRunner
        
        runner = CompatibilityTestRunner()
        
        # Should detect that sqitch is not available
        assert not runner.sqitch_available
        
        # Should generate appropriate report
        report = runner.generate_compatibility_report()
        assert "SKIPPED" in report
        assert "Perl sqitch not available" in report