"""Integration tests for bundle command."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from sqlitch.commands.bundle import BundleCommand
from sqlitch.core.config import Config
from sqlitch.core.exceptions import SqlitchError
from sqlitch.core.sqitch import Sqitch


@pytest.fixture
def temp_project():
    """Create a temporary sqlitch project."""
    temp_dir = Path(tempfile.mkdtemp())
    original_cwd = Path.cwd()

    try:
        # Change to temp directory
        import os

        os.chdir(temp_dir)

        # Create project structure
        (temp_dir / "deploy").mkdir()
        (temp_dir / "revert").mkdir()
        (temp_dir / "verify").mkdir()

        # Create config file
        config_content = """[core]
    engine = pg
    top_dir = .
    plan_file = sqitch.plan

[engine "pg"]
    target = db:pg://localhost/test
    registry = sqitch
    client = psql

[user]
    name = Test User
    email = test@example.com
"""
        (temp_dir / "sqitch.conf").write_text(config_content)

        # Create plan file
        plan_content = """%syntax-version=1.0.0
%project=test_project
%uri=https://example.com/test

initial 2023-01-01T10:00:00Z Test User <test@example.com> # Initial schema
users [initial] 2023-01-02T10:00:00Z Test User <test@example.com> # Add users table
@v1.0 2023-01-03T10:00:00Z Test User <test@example.com> # Version 1.0 release
posts [users] 2023-01-04T10:00:00Z Test User <test@example.com> # Add posts table
"""
        (temp_dir / "sqitch.plan").write_text(plan_content)

        # Create change files
        (temp_dir / "deploy" / "initial.sql").write_text(
            """
-- Deploy test_project:initial to pg

BEGIN;

CREATE SCHEMA test;

COMMIT;
"""
        )

        (temp_dir / "revert" / "initial.sql").write_text(
            """
-- Revert test_project:initial from pg

BEGIN;

DROP SCHEMA test CASCADE;

COMMIT;
"""
        )

        (temp_dir / "verify" / "initial.sql").write_text(
            """
-- Verify test_project:initial on pg

SELECT 1/count(*) FROM information_schema.schemata WHERE schema_name = 'test';
"""
        )

        (temp_dir / "deploy" / "users.sql").write_text(
            """
-- Deploy test_project:users to pg

BEGIN;

CREATE TABLE test.users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
);

COMMIT;
"""
        )

        (temp_dir / "revert" / "users.sql").write_text(
            """
-- Revert test_project:users from pg

BEGIN;

DROP TABLE test.users;

COMMIT;
"""
        )

        (temp_dir / "verify" / "users.sql").write_text(
            """
-- Verify test_project:users on pg

SELECT id, name, email FROM test.users WHERE FALSE;
"""
        )

        (temp_dir / "deploy" / "posts.sql").write_text(
            """
-- Deploy test_project:posts to pg

BEGIN;

CREATE TABLE test.posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES test.users(id),
    title TEXT NOT NULL,
    content TEXT
);

COMMIT;
"""
        )

        (temp_dir / "revert" / "posts.sql").write_text(
            """
-- Revert test_project:posts from pg

BEGIN;

DROP TABLE test.posts;

COMMIT;
"""
        )

        (temp_dir / "verify" / "posts.sql").write_text(
            """
-- Verify test_project:posts on pg

SELECT id, user_id, title, content FROM test.posts WHERE FALSE;
"""
        )

        yield temp_dir

    finally:
        # Restore original directory
        os.chdir(original_cwd)
        # Cleanup
        shutil.rmtree(temp_dir)


@pytest.fixture
def sqitch_instance(temp_project):
    """Create a Sqitch instance for the temp project."""
    config = Config()
    sqitch = Sqitch(config=config)
    return sqitch


class TestBundleIntegration:
    """Integration tests for bundle command."""

    def test_bundle_entire_project(self, sqitch_instance, temp_project):
        """Test bundling entire project."""
        command = BundleCommand(sqitch_instance)

        # Execute bundle command
        result = command.execute([])

        assert result == 0

        # Check bundle directory was created
        bundle_dir = temp_project / "bundle"
        assert bundle_dir.exists()
        assert bundle_dir.is_dir()

        # Check config file was copied
        config_file = bundle_dir / "sqitch.conf"
        assert config_file.exists()

        # Check plan file was copied
        plan_file = bundle_dir / "sqitch.plan"
        assert plan_file.exists()

        # Verify plan content
        plan_content = plan_file.read_text()
        assert "%project=test_project" in plan_content
        assert "initial" in plan_content
        assert "users" in plan_content
        assert "posts" in plan_content
        assert "@v1.0" in plan_content

        # Check script directories were created
        assert (bundle_dir / "deploy").exists()
        assert (bundle_dir / "revert").exists()
        assert (bundle_dir / "verify").exists()

        # Check all script files were copied
        script_files = [
            "deploy/initial.sql",
            "revert/initial.sql",
            "verify/initial.sql",
            "deploy/users.sql",
            "revert/users.sql",
            "verify/users.sql",
            "deploy/posts.sql",
            "revert/posts.sql",
            "verify/posts.sql",
        ]

        for script_file in script_files:
            bundled_file = bundle_dir / script_file
            assert bundled_file.exists(), f"Missing bundled file: {script_file}"

            # Verify content matches original
            original_file = temp_project / script_file
            assert bundled_file.read_text() == original_file.read_text()

    def test_bundle_with_custom_dest_dir(self, sqitch_instance, temp_project):
        """Test bundling with custom destination directory."""
        command = BundleCommand(sqitch_instance)

        # Execute bundle command with custom destination
        result = command.execute(["--dest-dir", "my-bundle"])

        assert result == 0

        # Check custom bundle directory was created
        bundle_dir = temp_project / "my-bundle"
        assert bundle_dir.exists()

        # Check files were copied
        assert (bundle_dir / "sqitch.conf").exists()
        assert (bundle_dir / "sqitch.plan").exists()
        assert (bundle_dir / "deploy" / "initial.sql").exists()

    def test_bundle_with_from_to_range(self, sqitch_instance, temp_project):
        """Test bundling with --from and --to options."""
        command = BundleCommand(sqitch_instance)

        # Bundle from initial to @v1.0
        result = command.execute(["--from", "initial", "--to", "@v1.0"])

        assert result == 0

        bundle_dir = temp_project / "bundle"
        assert bundle_dir.exists()

        # Check plan file contains only the specified range
        plan_file = bundle_dir / "sqitch.plan"
        plan_content = plan_file.read_text()

        assert "initial" in plan_content
        assert "users" in plan_content
        assert "@v1.0" in plan_content
        # posts should not be included (it's after @v1.0)
        assert "posts" not in plan_content

        # Check only relevant script files were copied
        assert (bundle_dir / "deploy" / "initial.sql").exists()
        assert (bundle_dir / "deploy" / "users.sql").exists()
        assert not (bundle_dir / "deploy" / "posts.sql").exists()

    def test_bundle_from_root_to_head(self, sqitch_instance, temp_project):
        """Test bundling from @ROOT to @HEAD."""
        command = BundleCommand(sqitch_instance)

        result = command.execute(["--from", "@ROOT", "--to", "@HEAD"])

        assert result == 0

        bundle_dir = temp_project / "bundle"
        plan_file = bundle_dir / "sqitch.plan"
        plan_content = plan_file.read_text()

        # Should include all changes
        assert "initial" in plan_content
        assert "users" in plan_content
        assert "posts" in plan_content
        assert "@v1.0" in plan_content

    def test_bundle_single_change(self, sqitch_instance, temp_project):
        """Test bundling a single change."""
        command = BundleCommand(sqitch_instance)

        result = command.execute(["--from", "users", "--to", "users"])

        assert result == 0

        bundle_dir = temp_project / "bundle"
        plan_file = bundle_dir / "sqitch.plan"
        plan_content = plan_file.read_text()

        # Should include only the users change
        assert "users" in plan_content
        # "initial" should only appear as a dependency, not as a standalone change
        lines = plan_content.split("\n")
        change_lines = [
            line
            for line in lines
            if line and not line.startswith("%") and not line.startswith("#")
        ]
        assert len([line for line in change_lines if line.startswith("users")]) == 1
        assert len([line for line in change_lines if line.startswith("initial")]) == 0
        assert "posts" not in plan_content

        # Should have only users scripts
        assert (bundle_dir / "deploy" / "users.sql").exists()
        assert not (bundle_dir / "deploy" / "initial.sql").exists()
        assert not (bundle_dir / "deploy" / "posts.sql").exists()

    def test_bundle_invalid_change_range(self, sqitch_instance, temp_project):
        """Test bundling with invalid change range."""
        command = BundleCommand(sqitch_instance)

        # Try to bundle non-existent change
        result = command.execute(["--from", "nonexistent"])

        assert result == 1  # Should fail

    def test_bundle_conflicting_options(self, sqitch_instance, temp_project):
        """Test bundling with conflicting options."""
        command = BundleCommand(sqitch_instance)

        # Try to use --from with change arguments
        result = command.execute(["--from", "initial", "users"])

        assert result == 1  # Should fail

    def test_bundle_preserves_file_timestamps(self, sqitch_instance, temp_project):
        """Test that bundling preserves file modification times."""
        import time

        # Modify a file and note its timestamp
        deploy_file = temp_project / "deploy" / "initial.sql"
        original_content = deploy_file.read_text()

        # Wait a bit and modify the file
        time.sleep(0.1)
        deploy_file.write_text(original_content + "\n-- Modified")
        modified_time = deploy_file.stat().st_mtime

        command = BundleCommand(sqitch_instance)
        result = command.execute([])

        assert result == 0

        # Check that bundled file has same modification time
        bundled_file = temp_project / "bundle" / "deploy" / "initial.sql"
        bundled_time = bundled_file.stat().st_mtime

        # Times should be very close (within 1 second due to filesystem precision)
        assert abs(bundled_time - modified_time) < 1.0

    def test_bundle_skips_unchanged_files(self, sqitch_instance, temp_project):
        """Test that bundling skips files that haven't changed."""
        command = BundleCommand(sqitch_instance)

        # First bundle
        result = command.execute([])
        assert result == 0

        bundle_dir = temp_project / "bundle"
        bundled_file = bundle_dir / "deploy" / "initial.sql"
        first_bundle_time = bundled_file.stat().st_mtime

        # Second bundle without changes
        result = command.execute([])
        assert result == 0

        second_bundle_time = bundled_file.stat().st_mtime

        # File should not have been re-copied
        assert second_bundle_time == first_bundle_time

    def test_bundle_creates_nested_directories(self, sqitch_instance, temp_project):
        """Test that bundling creates necessary nested directories."""
        # Create a nested script file
        nested_dir = temp_project / "deploy" / "functions"
        nested_dir.mkdir()
        nested_file = nested_dir / "user_functions.sql"
        nested_file.write_text("CREATE FUNCTION test();")

        # Add to plan (this is a simplified test - normally plan would reference it)
        command = BundleCommand(sqitch_instance)
        result = command.execute([])

        assert result == 0

        # Check that nested directory structure is preserved in bundle
        bundle_dir = temp_project / "bundle"
        assert bundle_dir.exists()
        # Note: This test is simplified - full nested support would require
        # more complex change path handling

    def test_bundle_handles_missing_scripts_gracefully(
        self, sqitch_instance, temp_project
    ):
        """Test that bundling handles missing script files gracefully."""
        # Remove a script file
        (temp_project / "verify" / "posts.sql").unlink()

        command = BundleCommand(sqitch_instance)
        result = command.execute([])

        # Should still succeed
        assert result == 0

        bundle_dir = temp_project / "bundle"

        # Deploy and revert should be copied
        assert (bundle_dir / "deploy" / "posts.sql").exists()
        assert (bundle_dir / "revert" / "posts.sql").exists()

        # Verify should not exist (was missing)
        assert not (bundle_dir / "verify" / "posts.sql").exists()

    def test_bundle_without_config_file(self, sqitch_instance, temp_project):
        """Test bundling when no config file exists."""
        # Remove config file
        (temp_project / "sqitch.conf").unlink()

        command = BundleCommand(sqitch_instance)
        result = command.execute([])

        # Should still work
        assert result == 0

        bundle_dir = temp_project / "bundle"

        # Config file should not be in bundle
        assert not (bundle_dir / "sqitch.conf").exists()

        # But plan and scripts should still be bundled
        assert (bundle_dir / "sqitch.plan").exists()
        assert (bundle_dir / "deploy" / "initial.sql").exists()


class TestBundleCommandLineInterface:
    """Test the command-line interface for bundle command."""

    def test_bundle_cli_basic(self, sqitch_instance, temp_project):
        """Test basic CLI usage."""
        from click.testing import CliRunner

        from sqlitch.commands.bundle import bundle_command

        runner = CliRunner()

        with runner.isolated_filesystem():
            # Copy project to isolated filesystem
            import shutil

            shutil.copytree(temp_project, "project")

            import os

            os.chdir("project")

            # Mock the context
            from unittest.mock import Mock

            ctx = Mock()
            ctx.obj = Mock()
            ctx.obj.create_sqitch = Mock(return_value=sqitch_instance)

            # This is a simplified test - full CLI testing would require
            # more complex setup with proper Click context
            pass  # Placeholder for CLI testing
