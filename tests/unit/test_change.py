"""Unit tests for change, tag, and dependency classes."""

from datetime import datetime
from pathlib import Path

import pytest

from sqlitch.core.change import Change, Dependency, Tag
from sqlitch.core.target import Target


class TestDependency:
    """Test Dependency class functionality."""

    def test_create_simple_dependency(self):
        """Test creating a simple dependency."""
        dep = Dependency(type="require", change="users")
        assert dep.type == "require"
        assert dep.change == "users"
        assert dep.project is None

    def test_create_project_dependency(self):
        """Test creating a dependency with project."""
        dep = Dependency(type="require", change="users", project="myproject")
        assert dep.type == "require"
        assert dep.change == "users"
        assert dep.project == "myproject"

    def test_from_string_simple(self):
        """Test parsing simple dependency from string."""
        dep = Dependency.from_string("users")
        assert dep.type == "require"
        assert dep.change == "users"
        assert dep.project is None

    def test_from_string_with_project(self):
        """Test parsing dependency with project from string."""
        dep = Dependency.from_string("users@myproject")
        assert dep.type == "require"
        assert dep.change == "users"
        assert dep.project == "myproject"

    def test_from_string_conflict(self):
        """Test parsing conflict dependency from string."""
        dep = Dependency.from_string("!users")
        assert dep.type == "conflict"
        assert dep.change == "users"
        assert dep.project is None

    def test_from_string_conflict_with_project(self):
        """Test parsing conflict dependency with project from string."""
        dep = Dependency.from_string("!users@myproject")
        assert dep.type == "conflict"
        assert dep.change == "users"
        assert dep.project == "myproject"

    def test_str_simple(self):
        """Test string representation of simple dependency."""
        dep = Dependency(type="require", change="users")
        assert str(dep) == "users"

    def test_str_with_project(self):
        """Test string representation of dependency with project."""
        dep = Dependency(type="require", change="users", project="myproject")
        assert str(dep) == "users@myproject"


class TestTag:
    """Test Tag class functionality."""

    def test_create_tag(self):
        """Test creating a tag."""
        tag = Tag(
            name="v1.0",
            note="Release v1.0",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        assert tag.name == "v1.0"
        assert tag.note == "Release v1.0"
        assert tag.planner_name == "John Doe"
        assert tag.planner_email == "john@example.com"

    def test_id_generation(self):
        """Test tag ID generation."""
        tag = Tag(
            name="v1.0",
            note="Release v1.0",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        tag_id = tag.id
        assert isinstance(tag_id, str)
        assert len(tag_id) == 40  # SHA1 hash length

        # ID should be consistent
        assert tag.id == tag_id

    def test_id_uniqueness(self):
        """Test that different tags have different IDs."""
        tag1 = Tag(
            name="v1.0",
            note="Release v1.0",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        tag2 = Tag(
            name="v2.0",
            note="Release v2.0",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        assert tag1.id != tag2.id

    def test_str_representation(self):
        """Test string representation for plan file format."""
        tag = Tag(
            name="v1.0",
            note="Release v1.0",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        expected = (
            "@v1.0 2023-01-15T10:30:00Z John Doe <john@example.com> # Release v1.0"
        )
        assert str(tag) == expected

    def test_str_representation_empty_note(self):
        """Test string representation with empty note."""
        tag = Tag(
            name="v1.0",
            note="",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        expected = "@v1.0 2023-01-15T10:30:00Z John Doe <john@example.com> # "
        assert str(tag) == expected


class TestChange:
    """Test Change class functionality."""

    def test_create_change(self):
        """Test creating a change."""
        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        assert change.name == "users"
        assert change.note == "Add users table"
        assert change.planner_name == "John Doe"
        assert change.planner_email == "john@example.com"
        assert change.tags == []
        assert change.dependencies == []
        assert change.conflicts == []

    def test_create_change_with_dependencies(self):
        """Test creating a change with dependencies."""
        dep = Dependency(type="require", change="initial_schema")
        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
            dependencies=[dep],
        )

        assert len(change.dependencies) == 1
        assert change.dependencies[0].change == "initial_schema"

    def test_id_generation(self):
        """Test change ID generation."""
        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        change_id = change.id
        assert isinstance(change_id, str)
        assert len(change_id) == 40  # SHA1 hash length

        # ID should be consistent
        assert change.id == change_id

    def test_id_with_dependencies(self):
        """Test change ID generation includes dependencies."""
        dep = Dependency(type="require", change="initial_schema")

        change1 = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        change2 = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
            dependencies=[dep],
        )

        # Changes with different dependencies should have different IDs
        assert change1.id != change2.id

    def test_id_uniqueness(self):
        """Test that different changes have different IDs."""
        change1 = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        change2 = Change(
            name="posts",
            note="Add posts table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        assert change1.id != change2.id

    def test_file_paths(self):
        """Test file path generation."""
        target = Target(
            name="test",
            uri="db:pg://localhost/test",
            top_dir=Path("/project"),
            deploy_dir=Path("deploy"),
            revert_dir=Path("revert"),
            verify_dir=Path("verify"),
        )

        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        assert change.deploy_file(target) == Path("/project/deploy/users.sql")
        assert change.revert_file(target) == Path("/project/revert/users.sql")
        assert change.verify_file(target) == Path("/project/verify/users.sql")

    def test_str_representation_simple(self):
        """Test string representation without dependencies."""
        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        expected = (
            "users 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table"
        )
        assert str(change) == expected

    def test_str_representation_with_dependencies(self):
        """Test string representation with dependencies."""
        dep = Dependency(type="require", change="initial_schema")
        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
            dependencies=[dep],
        )

        expected = "users [initial_schema] 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table"
        assert str(change) == expected

    def test_str_representation_multiple_dependencies(self):
        """Test string representation with multiple dependencies."""
        dep1 = Dependency(type="require", change="initial_schema")
        dep2 = Dependency(type="require", change="permissions")
        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
            dependencies=[dep1, dep2],
        )

        expected = "users [initial_schema permissions] 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table"
        assert str(change) == expected

    def test_str_representation_empty_note(self):
        """Test string representation with empty note."""
        change = Change(
            name="users",
            note="",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
        )

        expected = "users 2023-01-15T10:30:00Z John Doe <john@example.com>"
        assert str(change) == expected
