"""Integration tests for the init command."""

import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlitch.commands.init import InitCommand
from sqlitch.core.plan import Plan
from sqlitch.core.sqitch import create_sqitch


class TestInitIntegration:
    """Integration tests for init command."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        temp_dir = Path(tempfile.mkdtemp())
        original_cwd = Path.cwd()

        # Change to temp directory
        os.chdir(temp_dir)

        yield temp_dir

        # Cleanup
        os.chdir(original_cwd)
        shutil.rmtree(temp_dir)

    def test_init_basic_project(self, temp_dir):
        """Test initializing a basic project."""
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        with patch.object(command, "_init_vcs"):  # Skip VCS init for test
            exit_code = command.execute(["myproject"])

        assert exit_code == 0

        # Check files were created
        assert (temp_dir / "sqitch.conf").exists()
        assert (temp_dir / "sqitch.plan").exists()
        assert (temp_dir / "deploy").is_dir()
        assert (temp_dir / "revert").is_dir()
        assert (temp_dir / "verify").is_dir()

        # Check configuration content
        config_content = (temp_dir / "sqitch.conf").read_text()
        assert "[core]" in config_content
        assert "top_dir = ." in config_content
        assert "plan_file = sqitch.plan" in config_content

        # Check plan content
        plan_content = (temp_dir / "sqitch.plan").read_text()
        assert "%syntax-version=1.0.0" in plan_content
        assert "%project=myproject" in plan_content

        # Verify plan can be parsed
        plan = Plan.from_file(temp_dir / "sqitch.plan")
        assert plan.project == "myproject"
        assert plan.syntax_version == "1.0.0"

    def test_init_with_engine(self, temp_dir):
        """Test initializing project with specific engine."""
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        with patch.object(command, "_init_vcs"):
            exit_code = command.execute(["--engine", "pg", "myproject"])

        assert exit_code == 0

        # Check configuration includes engine
        config_content = (temp_dir / "sqitch.conf").read_text()
        assert "engine = pg" in config_content
        assert '[engine "pg"]' in config_content
        assert "target = db:pg:" in config_content

    def test_init_with_uri(self, temp_dir):
        """Test initializing project with database URI."""
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        uri = "db:pg://user@localhost/testdb"

        with patch.object(command, "_init_vcs"):
            exit_code = command.execute(["--uri", uri, "myproject"])

        assert exit_code == 0

        # Check plan includes URI
        plan_content = (temp_dir / "sqitch.plan").read_text()
        assert f"%uri={uri}" in plan_content

        # Check configuration
        config_content = (temp_dir / "sqitch.conf").read_text()
        assert "engine = pg" in config_content
        assert f"target = {uri}" in config_content

    def test_init_custom_directories(self, temp_dir):
        """Test initializing project with custom directories."""
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        with patch.object(command, "_init_vcs"):
            exit_code = command.execute(
                [
                    "--top-dir",
                    "custom",
                    "--deploy-dir",
                    "migrations",
                    "--revert-dir",
                    "rollbacks",
                    "--verify-dir",
                    "tests",
                    "myproject",
                ]
            )

        assert exit_code == 0

        # Check custom directories were created
        assert (temp_dir / "custom" / "migrations").is_dir()
        assert (temp_dir / "custom" / "rollbacks").is_dir()
        assert (temp_dir / "custom" / "tests").is_dir()

        # Check configuration
        config_content = (temp_dir / "sqitch.conf").read_text()
        assert "top_dir = custom" in config_content
        assert "deploy_dir = migrations" in config_content
        assert "revert_dir = rollbacks" in config_content
        assert "verify_dir = tests" in config_content

    def test_init_custom_plan_file(self, temp_dir):
        """Test initializing project with custom plan file."""
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        with patch.object(command, "_init_vcs"):
            exit_code = command.execute(["--plan-file", "custom.plan", "myproject"])

        assert exit_code == 0

        # Check custom plan file was created
        assert (temp_dir / "custom.plan").exists()
        assert not (temp_dir / "sqitch.plan").exists()

        # Check configuration
        config_content = (temp_dir / "sqitch.conf").read_text()
        assert "plan_file = custom.plan" in config_content

    def test_init_with_registry_and_client(self, temp_dir):
        """Test initializing project with registry and client options."""
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        with patch.object(command, "_init_vcs"):
            exit_code = command.execute(
                [
                    "--engine",
                    "pg",
                    "--registry",
                    "sqitch_meta",
                    "--client",
                    "psql",
                    "myproject",
                ]
            )

        assert exit_code == 0

        # Check configuration
        config_content = (temp_dir / "sqitch.conf").read_text()
        assert "registry = sqitch_meta" in config_content
        assert "client = psql" in config_content

    def test_init_already_initialized(self, temp_dir):
        """Test initializing when project is already initialized."""
        # First initialization
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        with patch.object(command, "_init_vcs"):
            exit_code = command.execute(["myproject"])
        assert exit_code == 0

        # Second initialization should succeed but not overwrite
        original_config = (temp_dir / "sqitch.conf").read_text()

        exit_code = command.execute(["myproject"])
        assert exit_code == 0

        # Configuration should be unchanged
        new_config = (temp_dir / "sqitch.conf").read_text()
        assert new_config == original_config

    def test_init_different_project_same_directory(self, temp_dir):
        """Test initializing different project in same directory."""
        # First initialization
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        with patch.object(command, "_init_vcs"):
            exit_code = command.execute(["project1"])
        assert exit_code == 0

        # Second initialization with different project should fail
        exit_code = command.execute(["project2"])
        assert exit_code == 1

    def test_init_invalid_project_name(self, temp_dir):
        """Test initializing with invalid project name."""
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        exit_code = command.execute(["123invalid"])
        assert exit_code == 1

        # No files should be created
        assert not (temp_dir / "sqitch.conf").exists()
        assert not (temp_dir / "sqitch.plan").exists()

    def test_init_missing_project_name(self, temp_dir):
        """Test initializing without project name."""
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        exit_code = command.execute([])
        assert exit_code == 1

        # No files should be created
        assert not (temp_dir / "sqitch.conf").exists()
        assert not (temp_dir / "sqitch.plan").exists()

    @patch("sqlitch.commands.init.GitRepository")
    def test_init_with_vcs(self, mock_git_repo, temp_dir):
        """Test initializing with VCS integration."""
        mock_repo = Mock()
        mock_repo.init_repository = Mock()
        mock_git_repo.return_value = mock_repo

        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        exit_code = command.execute(["myproject"])
        assert exit_code == 0

        # Check that Git initialization was attempted
        mock_repo.init_repository.assert_called_once()

        # Check .gitignore was created
        assert (temp_dir / ".gitignore").exists()
        gitignore_content = (temp_dir / ".gitignore").read_text()
        assert "# Sqlitch" in gitignore_content
        assert "*.log" in gitignore_content

    def test_init_no_vcs(self, temp_dir):
        """Test initializing without VCS integration."""
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        exit_code = command.execute(["--no-vcs", "myproject"])
        assert exit_code == 0

        # VCS files should not be created
        assert not (temp_dir / ".git").exists()
        assert not (temp_dir / ".gitignore").exists()

    def test_init_creates_valid_project_structure(self, temp_dir):
        """Test that init creates a valid project structure."""
        sqitch = create_sqitch()
        command = InitCommand(sqitch)

        with patch.object(command, "_init_vcs"):
            exit_code = command.execute(["--engine", "pg", "testproject"])

        assert exit_code == 0

        # Verify we can create a new sqitch instance from the initialized project
        config_files = (
            [temp_dir / "sqitch.conf"] if (temp_dir / "sqitch.conf").exists() else None
        )
        new_sqitch = create_sqitch(config_files=config_files)

        # Should be able to detect it's initialized
        assert new_sqitch.is_initialized()

        # Should be able to parse the plan
        plan_file = new_sqitch.get_plan_file()
        plan = Plan.from_file(plan_file)
        assert plan.project == "testproject"

        # Should be able to get target
        target = new_sqitch.get_target()
        assert target.engine_type == "pg"
