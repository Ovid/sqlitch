"""
Integration tests for Git utilities.

These tests use real Git repositories to verify functionality.
"""

import os
import subprocess
from pathlib import Path

import pytest

from sqlitch.utils.git import (
    GitRepository,
    VCSError,
    detect_vcs,
    get_vcs_user_info,
    is_vcs_clean,
    suggest_change_name,
)


@pytest.fixture
def git_repo(tmp_path):
    """Create a real Git repository for testing."""
    # Initialize repository
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)

    # Configure user (required for commits)
    subprocess.run(
        ["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True
    )
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True
    )

    return tmp_path


@pytest.fixture
def git_repo_with_commits(git_repo):
    """Create a Git repository with some commits."""
    # Create and commit initial file
    test_file = git_repo / "test.txt"
    test_file.write_text("Initial content")

    subprocess.run(["git", "add", "test.txt"], cwd=git_repo, check=True)
    subprocess.run(["git", "commit", "-m", "Initial commit"], cwd=git_repo, check=True)

    # Create and commit second file
    test_file2 = git_repo / "test2.txt"
    test_file2.write_text("Second content")

    subprocess.run(["git", "add", "test2.txt"], cwd=git_repo, check=True)
    subprocess.run(["git", "commit", "-m", "Second commit"], cwd=git_repo, check=True)

    return git_repo


class TestGitRepositoryIntegration:
    """Integration tests for GitRepository class."""

    def test_detect_real_repository(self, git_repo):
        """Test detecting a real Git repository."""
        repo = GitRepository(git_repo)

        assert repo.is_repository
        assert repo.root_path == git_repo
        assert repo._git_dir == git_repo / ".git"

    def test_detect_repository_in_subdirectory(self, git_repo):
        """Test detecting repository from subdirectory."""
        subdir = git_repo / "subdir"
        subdir.mkdir()

        repo = GitRepository(subdir)

        assert repo.is_repository
        assert repo.root_path == git_repo

    def test_get_status_clean_repository(self, git_repo_with_commits):
        """Test getting status of clean repository."""
        repo = GitRepository(git_repo_with_commits)
        status = repo.get_status()

        assert status.is_repo
        assert status.is_clean
        assert status.current_branch in ["main", "master"]  # Depends on Git version
        assert status.current_commit is not None
        assert len(status.current_commit) == 40  # SHA1 hash length
        assert not status.has_staged_changes
        assert not status.has_unstaged_changes
        assert status.untracked_files == []

    def test_get_status_with_modifications(self, git_repo_with_commits):
        """Test getting status with modifications."""
        # Modify existing file
        test_file = git_repo_with_commits / "test.txt"
        test_file.write_text("Modified content")

        # Create untracked file
        untracked_file = git_repo_with_commits / "untracked.txt"
        untracked_file.write_text("Untracked content")

        repo = GitRepository(git_repo_with_commits)
        status = repo.get_status()

        assert status.is_repo
        assert not status.is_clean
        assert status.has_unstaged_changes
        assert not status.has_staged_changes
        assert "untracked.txt" in status.untracked_files

    def test_get_status_with_staged_changes(self, git_repo_with_commits):
        """Test getting status with staged changes."""
        # Modify and stage file
        test_file = git_repo_with_commits / "test.txt"
        test_file.write_text("Staged content")

        subprocess.run(
            ["git", "add", "test.txt"], cwd=git_repo_with_commits, check=True
        )

        repo = GitRepository(git_repo_with_commits)
        status = repo.get_status()

        assert status.is_repo
        assert not status.is_clean
        assert status.has_staged_changes
        assert not status.has_unstaged_changes

    def test_get_user_info(self, git_repo):
        """Test getting user information from Git config."""
        repo = GitRepository(git_repo)

        name = repo.get_user_name()
        email = repo.get_user_email()

        assert name == "Test User"
        assert email == "test@example.com"

    def test_init_repository(self, tmp_path):
        """Test initializing a new repository."""
        repo = GitRepository(tmp_path)

        assert not repo.is_repository

        repo.init_repository()

        assert repo.is_repository
        assert (tmp_path / ".git").exists()

    def test_add_and_commit_files(self, git_repo):
        """Test adding and committing files."""
        # Create test files
        file1 = git_repo / "file1.txt"
        file2 = git_repo / "file2.txt"
        file1.write_text("Content 1")
        file2.write_text("Content 2")

        repo = GitRepository(git_repo)

        # Add files
        repo.add_files([file1, file2])

        # Check status
        status = repo.get_status()
        assert status.has_staged_changes

        # Commit files
        commit_hash = repo.commit("Add test files")

        assert len(commit_hash) == 40  # SHA1 hash length

        # Check status after commit
        status = repo.get_status()
        assert status.is_clean

    def test_commit_with_author(self, git_repo):
        """Test committing with custom author."""
        # Create test file
        test_file = git_repo / "test.txt"
        test_file.write_text("Test content")

        repo = GitRepository(git_repo)
        repo.add_files([test_file])

        commit_hash = repo.commit("Test commit", "Custom Author <custom@example.com>")

        assert len(commit_hash) == 40

        # Verify author in commit
        result = subprocess.run(
            ["git", "log", "-1", "--format=%an <%ae>"],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        )
        assert result.stdout.strip() == "Custom Author <custom@example.com>"

    def test_get_file_history(self, git_repo_with_commits):
        """Test getting file history."""
        repo = GitRepository(git_repo_with_commits)
        history = repo.get_file_history(Path("test.txt"))

        assert len(history) >= 1
        assert "hash" in history[0]
        assert "message" in history[0]
        assert history[0]["message"] == "Initial commit"

    def test_is_file_tracked(self, git_repo_with_commits):
        """Test checking if files are tracked."""
        repo = GitRepository(git_repo_with_commits)

        # Tracked file
        assert repo.is_file_tracked(Path("test.txt"))

        # Untracked file
        untracked_file = git_repo_with_commits / "untracked.txt"
        untracked_file.write_text("Untracked")
        assert not repo.is_file_tracked(Path("untracked.txt"))

    def test_get_relative_path(self, git_repo):
        """Test getting relative paths."""
        subdir = git_repo / "subdir"
        subdir.mkdir()

        repo = GitRepository(git_repo)

        # File in root
        root_file = git_repo / "root.txt"
        assert repo.get_relative_path(root_file) == Path("root.txt")

        # File in subdirectory
        sub_file = subdir / "sub.txt"
        assert repo.get_relative_path(sub_file) == Path("subdir/sub.txt")

    def test_branch_operations(self, git_repo_with_commits):
        """Test branch-related operations."""
        # Create and switch to feature branch
        subprocess.run(
            ["git", "checkout", "-b", "feature/test"],
            cwd=git_repo_with_commits,
            check=True,
        )

        repo = GitRepository(git_repo_with_commits)
        status = repo.get_status()

        assert status.current_branch == "feature/test"


class TestUtilityFunctionsIntegration:
    """Integration tests for utility functions."""

    def test_detect_vcs_real_repo(self, git_repo):
        """Test VCS detection with real repository."""
        vcs = detect_vcs(git_repo)

        assert vcs is not None
        assert isinstance(vcs, GitRepository)
        assert vcs.is_repository

    def test_detect_vcs_no_repo(self, tmp_path):
        """Test VCS detection without repository."""
        vcs = detect_vcs(tmp_path)

        assert vcs is None

    def test_get_vcs_user_info_real_repo(self, git_repo):
        """Test getting VCS user info from real repository."""
        name, email = get_vcs_user_info(git_repo)

        assert name == "Test User"
        assert email == "test@example.com"

    def test_get_vcs_user_info_no_repo(self, tmp_path):
        """Test getting VCS user info without repository."""
        name, email = get_vcs_user_info(tmp_path)

        assert name is None
        assert email is None

    def test_is_vcs_clean_clean_repo(self, git_repo_with_commits):
        """Test VCS clean check with clean repository."""
        assert is_vcs_clean(git_repo_with_commits)

    def test_is_vcs_clean_dirty_repo(self, git_repo_with_commits):
        """Test VCS clean check with dirty repository."""
        # Modify file
        test_file = git_repo_with_commits / "test.txt"
        test_file.write_text("Modified")

        assert not is_vcs_clean(git_repo_with_commits)

    def test_is_vcs_clean_no_repo(self, tmp_path):
        """Test VCS clean check without repository."""
        assert is_vcs_clean(tmp_path)  # No VCS means clean

    def test_suggest_change_name_main_branch(self, git_repo_with_commits):
        """Test change name suggestion on main branch."""
        name = suggest_change_name("my_change", git_repo_with_commits)
        assert name == "my_change"

    def test_suggest_change_name_feature_branch(self, git_repo_with_commits):
        """Test change name suggestion on feature branch."""
        # Create and switch to feature branch
        subprocess.run(
            ["git", "checkout", "-b", "feature/user-auth"],
            cwd=git_repo_with_commits,
            check=True,
        )

        name = suggest_change_name("my_change", git_repo_with_commits)
        assert name == "my_change_feature_user_auth"

    def test_suggest_change_name_no_repo(self, tmp_path):
        """Test change name suggestion without repository."""
        name = suggest_change_name("my_change", tmp_path)
        assert name == "my_change"


class TestErrorHandling:
    """Test error handling in Git operations."""

    def test_git_command_not_found(self, tmp_path, monkeypatch):
        """Test handling when git command is not found."""
        # Mock PATH to not include git
        monkeypatch.setenv("PATH", "")

        repo = GitRepository(tmp_path)

        with pytest.raises(VCSError, match="Git command not found"):
            repo._run_git_command(["status"])

    def test_invalid_git_operation(self, tmp_path):
        """Test handling invalid git operations."""
        repo = GitRepository(tmp_path)

        # Try to commit without repository
        with pytest.raises(VCSError, match="Not a Git repository"):
            repo.commit("Test commit")

        # Try to add files without repository
        with pytest.raises(VCSError, match="Not a Git repository"):
            repo.add_files([Path("test.txt")])

    def test_git_command_failure(self, git_repo):
        """Test handling git command failures."""
        repo = GitRepository(git_repo)

        # Try to commit without staged changes (should fail)
        with pytest.raises(VCSError, match="Git command failed"):
            repo.commit("Empty commit")


@pytest.mark.skipif(
    subprocess.run(["which", "git"], capture_output=True).returncode != 0,
    reason="Git not available",
)
class TestRealGitIntegration:
    """Tests that require real Git installation."""

    def test_full_workflow(self, tmp_path):
        """Test complete Git workflow."""
        repo = GitRepository(tmp_path)

        # Initialize repository
        repo.init_repository()
        assert repo.is_repository

        # Configure user
        subprocess.run(
            ["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True
        )
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"],
            cwd=tmp_path,
            check=True,
        )

        # Create files
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("Content 1")
        file2.write_text("Content 2")

        # Check initial status
        status = repo.get_status()
        assert not status.is_clean
        assert len(status.untracked_files) == 2

        # Add files
        repo.add_files([file1, file2])

        # Check status after add
        status = repo.get_status()
        assert status.has_staged_changes

        # Commit files
        commit_hash = repo.commit("Initial commit")
        assert len(commit_hash) == 40

        # Check final status
        status = repo.get_status()
        assert status.is_clean

        # Verify files are tracked
        assert repo.is_file_tracked(file1)
        assert repo.is_file_tracked(file2)

        # Check history
        history = repo.get_file_history(file1)
        assert len(history) == 1
        assert history[0]["message"] == "Initial commit"
