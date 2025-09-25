"""
Git integration utilities for sqlitch.

This module provides Git repository detection, status checking,
and integration for change file naming and commit tracking.
"""

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..core.exceptions import SqlitchError


class VCSError(SqlitchError):
    """Version control system error."""

    pass


@dataclass
class GitStatus:
    """Git repository status information."""

    is_repo: bool
    is_clean: bool
    current_branch: Optional[str]
    current_commit: Optional[str]
    has_staged_changes: bool
    has_unstaged_changes: bool
    untracked_files: List[str]


class GitRepository:
    """Git repository interface."""

    def __init__(self, path: Optional[Path] = None):
        """
        Initialize Git repository interface.

        Args:
            path: Repository path (defaults to current directory)
        """
        self.path = path or Path.cwd()
        self._git_dir = self._find_git_dir()
        self._git_executable = self._find_git_executable()

    def _find_git_executable(self) -> str:
        """Find the git executable (handles git vs git.exe on Windows)."""
        git_exe = shutil.which("git")
        if git_exe is None:
            raise VCSError("Git command not found. Please install Git.")
        return git_exe

    def _find_git_dir(self) -> Optional[Path]:
        """Find .git directory by walking up the directory tree."""
        current = self.path.resolve()

        while current != current.parent:
            git_dir = current / ".git"
            if git_dir.exists():
                return git_dir
            current = current.parent

        return None

    @property
    def is_repository(self) -> bool:
        """Check if current path is in a Git repository."""
        return self._git_dir is not None

    @property
    def root_path(self) -> Optional[Path]:
        """Get repository root path."""
        if self._git_dir:
            return self._git_dir.parent
        return None

    def _run_git_command(
        self, args: List[str], check: bool = True
    ) -> subprocess.CompletedProcess:
        """
        Run git command.

        Args:
            args: Git command arguments
            check: Whether to check return code

        Returns:
            Completed process

        Raises:
            VCSError: If command fails and check=True
        """
        try:
            result = subprocess.run(
                [self._git_executable] + args,
                cwd=self.path,
                capture_output=True,
                text=True,
                timeout=30,
            )

            if check and result.returncode != 0:
                raise VCSError(f"Git command failed: {' '.join(args)}\n{result.stderr}")

            return result

        except subprocess.TimeoutExpired:
            raise VCSError(f"Git command timed out: {' '.join(args)}")
        except FileNotFoundError:
            raise VCSError("Git command not found. Please install Git.")

    def get_status(self) -> GitStatus:
        """
        Get repository status.

        Returns:
            Git status information
        """
        if not self.is_repository:
            return GitStatus(
                is_repo=False,
                is_clean=True,
                current_branch=None,
                current_commit=None,
                has_staged_changes=False,
                has_unstaged_changes=False,
                untracked_files=[],
            )

        try:
            # Get current branch
            branch_result = self._run_git_command(
                ["rev-parse", "--abbrev-ref", "HEAD"], check=False
            )
            current_branch = (
                branch_result.stdout.strip() if branch_result.returncode == 0 else None
            )

            # Get current commit
            commit_result = self._run_git_command(["rev-parse", "HEAD"], check=False)
            current_commit = (
                commit_result.stdout.strip() if commit_result.returncode == 0 else None
            )

            # Get status
            status_result = self._run_git_command(
                ["status", "--porcelain"], check=False
            )
            status_lines = (
                status_result.stdout.rstrip().split("\n")
                if status_result.stdout.rstrip()
                else []
            )

            has_staged_changes = False
            has_unstaged_changes = False
            untracked_files = []

            for line in status_lines:
                if len(line) >= 2:
                    staged_status = line[0]
                    unstaged_status = line[1]
                    filename = line[3:] if len(line) > 3 else ""

                    # Staged changes: first character is not space or ?
                    if staged_status != " " and staged_status != "?":
                        has_staged_changes = True

                    # Unstaged changes: second character is not space
                    if unstaged_status != " ":
                        has_unstaged_changes = True

                    # Untracked files: both characters are ?
                    if staged_status == "?" and unstaged_status == "?":
                        untracked_files.append(filename)

            is_clean = (
                not has_staged_changes
                and not has_unstaged_changes
                and not untracked_files
            )

            return GitStatus(
                is_repo=True,
                is_clean=is_clean,
                current_branch=current_branch,
                current_commit=current_commit,
                has_staged_changes=has_staged_changes,
                has_unstaged_changes=has_unstaged_changes,
                untracked_files=untracked_files,
            )

        except VCSError:
            # If git commands fail, assume clean state
            return GitStatus(
                is_repo=True,
                is_clean=True,
                current_branch=None,
                current_commit=None,
                has_staged_changes=False,
                has_unstaged_changes=False,
                untracked_files=[],
            )

    def get_user_name(self) -> Optional[str]:
        """
        Get Git user name.

        Returns:
            Git user name or None if not configured
        """
        try:
            result = self._run_git_command(
                ["config", "--get", "user.name"], check=False
            )
            return result.stdout.strip() if result.returncode == 0 else None
        except VCSError:
            return None

    def get_user_email(self) -> Optional[str]:
        """
        Get Git user email.

        Returns:
            Git user email or None if not configured
        """
        try:
            result = self._run_git_command(
                ["config", "--get", "user.email"], check=False
            )
            return result.stdout.strip() if result.returncode == 0 else None
        except VCSError:
            return None

    def init_repository(self) -> None:
        """
        Initialize Git repository.

        Raises:
            VCSError: If initialization fails
        """
        if self.is_repository:
            return  # Already a repository

        self._run_git_command(["init"])
        self._git_dir = self.path / ".git"

    def add_files(self, files: List[Path]) -> None:
        """
        Add files to Git staging area.

        Args:
            files: List of files to add

        Raises:
            VCSError: If add fails
        """
        if not self.is_repository:
            raise VCSError("Not a Git repository")

        file_paths = [str(f) for f in files]
        self._run_git_command(["add"] + file_paths)

    def commit(self, message: str, author: Optional[str] = None) -> str:
        """
        Commit staged changes.

        Args:
            message: Commit message
            author: Optional author override

        Returns:
            Commit hash

        Raises:
            VCSError: If commit fails
        """
        if not self.is_repository:
            raise VCSError("Not a Git repository")

        args = ["commit", "-m", message]
        if author:
            args.extend(["--author", author])

        self._run_git_command(args)

        # Get the commit hash
        result = self._run_git_command(["rev-parse", "HEAD"])
        return result.stdout.strip()

    def get_file_history(
        self, file_path: Path, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get file commit history.

        Args:
            file_path: File path
            limit: Maximum number of commits to return

        Returns:
            List of commit information
        """
        if not self.is_repository:
            return []

        try:
            result = self._run_git_command(
                ["log", "--oneline", f"-{limit}", "--", str(file_path)], check=False
            )

            if result.returncode != 0:
                return []

            commits = []
            for line in result.stdout.strip().split("\n"):
                if line:
                    parts = line.split(" ", 1)
                    if len(parts) == 2:
                        commits.append({"hash": parts[0], "message": parts[1]})

            return commits

        except VCSError:
            return []

    def is_file_tracked(self, file_path: Path) -> bool:
        """
        Check if file is tracked by Git.

        Args:
            file_path: File path to check

        Returns:
            True if file is tracked
        """
        if not self.is_repository:
            return False

        try:
            result = self._run_git_command(
                ["ls-files", "--error-unmatch", str(file_path)], check=False
            )
            return result.returncode == 0
        except VCSError:
            return False

    def get_relative_path(self, file_path: Path) -> Optional[Path]:
        """
        Get path relative to repository root.

        Args:
            file_path: Absolute file path

        Returns:
            Relative path or None if not in repository
        """
        if not self.is_repository or not self.root_path:
            return None

        try:
            return file_path.relative_to(self.root_path)
        except ValueError:
            return None


def detect_vcs(path: Optional[Path] = None) -> Optional[GitRepository]:
    """
    Detect version control system in path.

    Args:
        path: Path to check (defaults to current directory)

    Returns:
        VCS repository instance or None if not found
    """
    git_repo = GitRepository(path)
    if git_repo.is_repository:
        return git_repo

    return None


def get_vcs_user_info(
    path: Optional[Path] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Get user name and email from VCS configuration.

    Args:
        path: Repository path

    Returns:
        Tuple of (name, email) or (None, None) if not available
    """
    vcs = detect_vcs(path)
    if vcs:
        return vcs.get_user_name(), vcs.get_user_email()

    return None, None


def is_vcs_clean(path: Optional[Path] = None) -> bool:
    """
    Check if VCS working directory is clean.

    Args:
        path: Repository path

    Returns:
        True if clean or no VCS, False if dirty
    """
    vcs = detect_vcs(path)
    if vcs:
        status = vcs.get_status()
        return status.is_clean

    return True  # No VCS means "clean"


def suggest_change_name(base_name: str, path: Optional[Path] = None) -> str:
    """
    Suggest change name based on VCS state.

    Args:
        base_name: Base change name
        path: Repository path

    Returns:
        Suggested change name
    """
    vcs = detect_vcs(path)
    if not vcs:
        return base_name

    status = vcs.get_status()

    # If on a feature branch, include branch name
    if status.current_branch and status.current_branch not in [
        "main",
        "master",
        "develop",
    ]:
        # Clean up branch name for use in change name
        branch_suffix = status.current_branch.replace("/", "_").replace("-", "_")
        return f"{base_name}_{branch_suffix}"

    return base_name
