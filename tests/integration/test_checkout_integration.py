"""
Integration tests for checkout command.
"""

import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlitch.commands.checkout import CheckoutCommand
from sqlitch.core.config import Config
from sqlitch.core.exceptions import SqlitchError
from sqlitch.core.sqitch import Sqitch
from sqlitch.utils.git import VCSError


@pytest.fixture
def temp_git_repo():
    """Create temporary git repository for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        repo_path = Path(temp_dir)

        # Initialize git repo with main as default branch
        subprocess.run(["git", "init", "-b", "main"], cwd=repo_path, check=True, capture_output=True)
        subprocess.run(
            ["git", "config", "user.name", "Test User"], cwd=repo_path, check=True
        )
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"],
            cwd=repo_path,
            check=True,
        )

        # Create initial sqitch.plan
        plan_content = """%syntax-version=1.0.0
%project=test_project

users 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table
"""
        plan_file = repo_path / "sqitch.plan"
        plan_file.write_text(plan_content)

        # Create initial commit
        subprocess.run(["git", "add", "."], cwd=repo_path, check=True)
        subprocess.run(
            ["git", "commit", "-m", "Initial commit"], cwd=repo_path, check=True
        )

        # Create feature branch with additional change
        subprocess.run(["git", "checkout", "-b", "feature"], cwd=repo_path, check=True)

        feature_plan_content = """%syntax-version=1.0.0
%project=test_project

users 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table
posts [users] 2023-01-16T14:20:00Z Jane Smith <jane@example.com> # Add posts table
"""
        plan_file.write_text(feature_plan_content)

        subprocess.run(["git", "add", "."], cwd=repo_path, check=True)
        subprocess.run(
            ["git", "commit", "-m", "Add posts table"], cwd=repo_path, check=True
        )

        # Go back to main branch
        subprocess.run(["git", "checkout", "main"], cwd=repo_path, check=True)

        yield repo_path


@pytest.fixture
def sqitch_config(temp_git_repo):
    """Create Sqitch configuration for testing."""
    config_content = f"""[core]
    engine = sqlite
    top_dir = {temp_git_repo}
    plan_file = sqitch.plan

[engine "sqlite"]
    target = db:sqlite:{temp_git_repo}/test.db
    registry = sqitch
"""

    config_file = temp_git_repo / "sqitch.conf"
    config_file.write_text(config_content)

    return Config([config_file])


@pytest.fixture
def sqitch_instance(sqitch_config):
    """Create Sqitch instance for testing."""
    return Sqitch(config=sqitch_config)


@pytest.fixture
def checkout_command(sqitch_instance):
    """Create CheckoutCommand instance."""
    return CheckoutCommand(sqitch_instance)


class TestCheckoutIntegration:
    """Integration tests for checkout command."""

    def test_checkout_with_git_repo(self, checkout_command, temp_git_repo):
        """Test checkout command with real git repository."""
        # Change to repo directory
        import os

        original_cwd = os.getcwd()
        os.chdir(temp_git_repo)

        try:
            # Mock database operations since we don't have a real database
            with patch(
                "sqlitch.engines.base.EngineRegistry.create_engine"
            ) as mock_create_engine:
                mock_engine = Mock()
                mock_engine.ensure_registry.return_value = None
                mock_engine.revert.return_value = None
                mock_engine.deploy.return_value = None
                mock_create_engine.return_value = mock_engine

                # Mock user validation
                with patch.object(checkout_command, "validate_user_info"):
                    # Execute checkout to feature branch
                    result = checkout_command.execute(["feature"])

                    assert result == 0

                    # Verify we're on the feature branch
                    current_branch = subprocess.run(
                        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                        capture_output=True,
                        text=True,
                        check=True,
                    ).stdout.strip()

                    assert current_branch == "feature"

                    # Verify engine operations were called
                    mock_engine.ensure_registry.assert_called()
                    mock_engine.revert.assert_called()
                    mock_engine.deploy.assert_called()

        finally:
            os.chdir(original_cwd)

    def test_checkout_already_on_branch(self, checkout_command, temp_git_repo):
        """Test checkout when already on target branch."""
        import os

        original_cwd = os.getcwd()
        os.chdir(temp_git_repo)

        try:
            # We're already on main branch
            with patch.object(
                checkout_command, "handle_error", return_value=1
            ) as mock_handle_error:
                result = checkout_command.execute(["main"])

                assert result == 1
                mock_handle_error.assert_called_once()

                # Verify the error is about already being on the branch
                error_arg = mock_handle_error.call_args[0][0]
                assert isinstance(error_arg, SqlitchError)
                assert "Already on branch main" in str(error_arg)

        finally:
            os.chdir(original_cwd)

    def test_checkout_nonexistent_branch(self, checkout_command, temp_git_repo):
        """Test checkout with non-existent branch."""
        import os

        original_cwd = os.getcwd()
        os.chdir(temp_git_repo)

        try:
            with patch.object(
                checkout_command, "handle_error", return_value=1
            ) as mock_handle_error:
                result = checkout_command.execute(["nonexistent"])

                assert result == 1
                mock_handle_error.assert_called_once()

                # Verify the error is about VCS failure
                error_arg = mock_handle_error.call_args[0][0]
                assert isinstance(error_arg, VCSError)

        finally:
            os.chdir(original_cwd)

    def test_checkout_no_common_changes(self, checkout_command, temp_git_repo):
        """Test checkout when branches have no common changes."""
        import os

        original_cwd = os.getcwd()
        os.chdir(temp_git_repo)

        try:
            # Create a branch with completely different changes
            subprocess.run(
                ["git", "checkout", "-b", "different"], cwd=temp_git_repo, check=True
            )

            different_plan_content = """%syntax-version=1.0.0
%project=test_project

widgets 2023-01-20T09:00:00Z Bob Smith <bob@example.com> # Add widgets table
"""
            plan_file = temp_git_repo / "sqitch.plan"
            plan_file.write_text(different_plan_content)

            subprocess.run(["git", "add", "."], cwd=temp_git_repo, check=True)
            subprocess.run(
                ["git", "commit", "-m", "Add widgets table"],
                cwd=temp_git_repo,
                check=True,
            )

            # Go back to main
            subprocess.run(["git", "checkout", "main"], cwd=temp_git_repo, check=True)

            with patch.object(
                checkout_command, "handle_error", return_value=1
            ) as mock_handle_error:
                result = checkout_command.execute(["different"])

                assert result == 1
                mock_handle_error.assert_called_once()

                # Verify the error is about no common changes
                error_arg = mock_handle_error.call_args[0][0]
                assert isinstance(error_arg, SqlitchError)
                assert "has no changes in common" in str(error_arg)

        finally:
            os.chdir(original_cwd)

    def test_checkout_with_options(self, checkout_command, temp_git_repo):
        """Test checkout command with various options."""
        import os

        original_cwd = os.getcwd()
        os.chdir(temp_git_repo)

        try:
            with patch(
                "sqlitch.engines.base.EngineRegistry.create_engine"
            ) as mock_create_engine:
                mock_engine = Mock()
                mock_engine.ensure_registry.return_value = None
                mock_engine.revert.return_value = None
                mock_engine.deploy.return_value = None
                mock_create_engine.return_value = mock_engine

                with patch.object(checkout_command, "validate_user_info"):
                    # Execute checkout with options
                    result = checkout_command.execute(
                        [
                            "--mode",
                            "tag",
                            "--verify",
                            "--set",
                            "test_var=test_value",
                            "--log-only",
                            "-y",
                            "feature",
                        ]
                    )

                    assert result == 0

                    # Verify engine configuration
                    assert mock_engine.with_verify is True
                    assert mock_engine.log_only is True

                    # Verify variables were set
                    mock_engine.set_variables.assert_called()

        finally:
            os.chdir(original_cwd)

    def test_checkout_plan_parsing_error(self, checkout_command, temp_git_repo):
        """Test checkout with invalid plan file in target branch."""
        import os

        original_cwd = os.getcwd()
        os.chdir(temp_git_repo)

        try:
            # Create branch with invalid plan
            subprocess.run(
                ["git", "checkout", "-b", "invalid"], cwd=temp_git_repo, check=True
            )

            invalid_plan_content = "invalid plan content"
            plan_file = temp_git_repo / "sqitch.plan"
            plan_file.write_text(invalid_plan_content)

            subprocess.run(["git", "add", "."], cwd=temp_git_repo, check=True)
            subprocess.run(
                ["git", "commit", "-m", "Invalid plan"], cwd=temp_git_repo, check=True
            )

            # Go back to main
            subprocess.run(["git", "checkout", "main"], cwd=temp_git_repo, check=True)

            with patch.object(
                checkout_command, "handle_error", return_value=1
            ) as mock_handle_error:
                result = checkout_command.execute(["invalid"])

                assert result == 1
                mock_handle_error.assert_called_once()

        finally:
            os.chdir(original_cwd)

    def test_checkout_engine_error(self, checkout_command, temp_git_repo):
        """Test checkout with engine error during revert."""
        import os

        original_cwd = os.getcwd()
        os.chdir(temp_git_repo)

        try:
            with patch(
                "sqlitch.engines.base.EngineRegistry.create_engine"
            ) as mock_create_engine:
                mock_engine = Mock()
                mock_engine.ensure_registry.return_value = None
                mock_engine.revert.side_effect = SqlitchError("Revert failed")
                mock_create_engine.return_value = mock_engine

                with (
                    patch.object(checkout_command, "validate_user_info"),
                    patch.object(
                        checkout_command, "handle_error", return_value=1
                    ) as mock_handle_error,
                ):

                    result = checkout_command.execute(["feature"])

                    assert result == 1
                    mock_handle_error.assert_called_once()

        finally:
            os.chdir(original_cwd)


class TestCheckoutCommandCLI:
    """Test checkout command CLI integration."""

    def test_checkout_help(self, checkout_command):
        """Test checkout help display."""
        with patch.object(checkout_command, "_show_help") as mock_help:
            with pytest.raises(SystemExit):
                checkout_command.execute(["--help"])

            mock_help.assert_called_once()

    def test_checkout_usage_no_args(self, checkout_command):
        """Test checkout usage when no arguments provided."""
        with (
            patch.object(checkout_command, "require_initialized"),
            patch.object(checkout_command, "validate_user_info"),
            patch.object(checkout_command, "_show_usage") as mock_usage,
        ):

            result = checkout_command.execute([])

            assert result == 1
            mock_usage.assert_called_once()
