"""
Unit tests for code quality checks.

This module contains tests that verify code formatting, import ordering,
and other code quality standards are maintained across the codebase.
"""

import subprocess
import sys
from pathlib import Path

import pytest


class TestCodeQuality:
    """Test code quality standards."""

    def test_black_formatting(self):
        """Test that all Python files pass Black formatting checks."""
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent

        # Run black --check on sqlitch and tests directories
        result = subprocess.run(
            [sys.executable, "-m", "black", "--check", "--diff", "sqlitch", "tests"],
            cwd=project_root,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            pytest.fail(
                f"Black formatting check failed:\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}\n"
                f"Run 'black sqlitch tests' to fix formatting issues."
            )

    def test_isort_import_ordering(self):
        """Test that all Python files pass isort import ordering checks."""
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent

        # Run isort --check-only on sqlitch and tests directories
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "isort",
                "--check-only",
                "--diff",
                "sqlitch",
                "tests",
            ],
            cwd=project_root,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            pytest.fail(
                f"isort import ordering check failed:\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}\n"
                f"Run 'isort sqlitch tests' to fix import ordering issues."
            )

    def test_no_syntax_errors(self):
        """Test that all Python files can be compiled without syntax errors."""
        project_root = Path(__file__).parent.parent.parent

        # Find all Python files in sqlitch and tests directories
        python_files = []
        for directory in ["sqlitch", "tests"]:
            dir_path = project_root / directory
            if dir_path.exists():
                python_files.extend(dir_path.rglob("*.py"))

        syntax_errors = []

        for py_file in python_files:
            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    compile(f.read(), str(py_file), "exec")
            except SyntaxError as e:
                syntax_errors.append(f"{py_file}: {e}")
            except Exception:
                # Skip files that can't be read (e.g., binary files misnamed as .py)
                continue

        if syntax_errors:
            pytest.fail(
                "Syntax errors found in Python files:\n" + "\n".join(syntax_errors)
            )

    @pytest.mark.slow
    def test_flake8_critical_issues(self):
        """Test that there are no critical flake8 issues (syntax errors, undefined names)."""
        project_root = Path(__file__).parent.parent.parent

        # Run flake8 with only critical error codes
        # E9xx: syntax errors
        # F821: undefined name
        # F822: undefined name in __all__
        # F823: local variable referenced before assignment
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "flake8",
                "--select=E9,F821,F822,F823",
                "sqlitch",
                "tests",
            ],
            cwd=project_root,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            pytest.fail(
                f"Critical flake8 issues found:\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}\n"
                f"These are critical issues that must be fixed."
            )
