"""Unit tests for plan file parsing and management."""

import pytest
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

from sqlitch.core.plan import Plan
from sqlitch.core.change import Change, Dependency, Tag
from sqlitch.core.exceptions import PlanError


class TestDependency:
    """Test Dependency dataclass."""
    
    def test_from_string_simple(self):
        """Test parsing simple dependency."""
        dep = Dependency.from_string("users")
        assert dep.type == "require"
        assert dep.change == "users"
        assert dep.project is None
    
    def test_from_string_with_project(self):
        """Test parsing dependency with project."""
        dep = Dependency.from_string("users@myproject")
        assert dep.type == "require"
        assert dep.change == "users"
        assert dep.project == "myproject"
    
    def test_from_string_conflict(self):
        """Test parsing conflict dependency."""
        dep = Dependency.from_string("!users")
        assert dep.type == "conflict"
        assert dep.change == "users"
        assert dep.project is None
    
    def test_str_representation(self):
        """Test string representation."""
        dep = Dependency(type="require", change="users")
        assert str(dep) == "users"
        
        dep_with_project = Dependency(type="require", change="users", project="myproject")
        assert str(dep_with_project) == "users@myproject"


class TestTag:
    """Test Tag dataclass."""
    
    def test_id_generation(self):
        """Test tag ID generation."""
        tag = Tag(
            name="v1.0",
            note="Release v1.0",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com"
        )
        
        # ID should be consistent
        id1 = tag.id
        id2 = tag.id
        assert id1 == id2
        assert len(id1) == 40  # SHA1 hash length
    
    def test_str_representation(self):
        """Test string representation for plan file."""
        tag = Tag(
            name="v1.0",
            note="Release v1.0",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com"
        )
        
        expected = "@v1.0 2023-01-15T10:30:00Z John Doe <john@example.com> # Release v1.0"
        assert str(tag) == expected


class TestChange:
    """Test Change dataclass."""
    
    def test_id_generation(self):
        """Test change ID generation."""
        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com"
        )
        
        # ID should be consistent
        id1 = change.id
        id2 = change.id
        assert id1 == id2
        assert len(id1) == 40  # SHA1 hash length
    
    def test_id_with_dependencies(self):
        """Test change ID generation with dependencies."""
        dep = Dependency(type="require", change="initial_schema")
        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com",
            dependencies=[dep]
        )
        
        # ID should include dependency information
        assert len(change.id) == 40
    
    def test_str_representation_simple(self):
        """Test string representation without dependencies."""
        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com"
        )
        
        expected = "users 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table"
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
            dependencies=[dep]
        )
        
        expected = "users [[initial_schema]] 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table"
        assert str(change) == expected


class TestPlan:
    """Test Plan class."""
    
    def test_parse_minimal_plan(self, tmp_path):
        """Test parsing minimal valid plan."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        assert plan.project == "myproject"
        assert plan.syntax_version == "1.0.0"
        assert plan.uri is None
        assert len(plan.changes) == 1
        assert len(plan.tags) == 0
        
        change = plan.changes[0]
        assert change.name == "initial_schema"
        assert change.note == "Initial schema"
        assert change.planner_name == "John Doe"
        assert change.planner_email == "john@example.com"
    
    def test_parse_plan_with_uri(self, tmp_path):
        """Test parsing plan with URI."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject
%uri=https://github.com/example/myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        assert plan.uri == "https://github.com/example/myproject"
    
    def test_parse_plan_with_dependencies(self, tmp_path):
        """Test parsing plan with dependencies."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
users [initial_schema] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 2
        users_change = plan.changes[1]
        assert users_change.name == "users"
        assert len(users_change.dependencies) == 1
        assert users_change.dependencies[0].change == "initial_schema"
        assert users_change.dependencies[0].type == "require"
    
    def test_parse_plan_with_tags(self, tmp_path):
        """Test parsing plan with tags."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
@v1.0 2023-01-20T09:00:00Z John Doe <john@example.com> # Release v1.0
users [initial_schema] 2023-01-25T11:15:00Z Jane Smith <jane@example.com> # Add users table
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 2
        assert len(plan.tags) == 1
        
        tag = plan.tags[0]
        assert tag.name == "v1.0"
        assert tag.note == "Release v1.0"
        assert tag.planner_name == "John Doe"
    
    def test_parse_plan_with_comments_and_empty_lines(self, tmp_path):
        """Test parsing plan with comments and empty lines."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

# This is a comment

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema

# Another comment
users [initial_schema] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 2
        assert plan.changes[0].name == "initial_schema"
        assert plan.changes[1].name == "users"
    
    def test_parse_plan_missing_project(self, tmp_path):
        """Test parsing plan without project pragma."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
"""
        plan_file.write_text(plan_content)
        
        with pytest.raises(PlanError, match="Missing %project pragma"):
            Plan.from_file(plan_file)
    
    def test_parse_plan_invalid_change_format(self, tmp_path):
        """Test parsing plan with invalid change format."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

invalid_change
"""
        plan_file.write_text(plan_content)
        
        with pytest.raises(PlanError, match="Invalid change format"):
            Plan.from_file(plan_file)
    
    def test_parse_plan_invalid_tag_format(self, tmp_path):
        """Test parsing plan with invalid tag format."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

@invalid_tag
"""
        plan_file.write_text(plan_content)
        
        with pytest.raises(PlanError, match="Invalid tag format"):
            Plan.from_file(plan_file)
    
    def test_parse_plan_invalid_timestamp(self, tmp_path):
        """Test parsing plan with invalid timestamp."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema invalid-timestamp John Doe <john@example.com> # Initial schema
"""
        plan_file.write_text(plan_content)
        
        with pytest.raises(PlanError, match="Invalid timestamp"):
            Plan.from_file(plan_file)
    
    def test_parse_plan_missing_email(self, tmp_path):
        """Test parsing plan with missing email."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe # Initial schema
"""
        plan_file.write_text(plan_content)
        
        with pytest.raises(PlanError, match="Missing or invalid email format"):
            Plan.from_file(plan_file)
    
    def test_parse_nonexistent_file(self):
        """Test parsing nonexistent file."""
        with pytest.raises(PlanError, match="Plan file not found"):
            Plan.from_file(Path("nonexistent.plan"))
    
    def test_validate_duplicate_changes(self, tmp_path):
        """Test validation of duplicate change names."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

users 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table
users 2023-01-16T14:20:00Z John Doe <john@example.com> # Duplicate users table
"""
        plan_file.write_text(plan_content)
        
        with pytest.raises(PlanError, match="Duplicate change name: users"):
            Plan.from_file(plan_file)
    
    def test_validate_duplicate_tags(self, tmp_path):
        """Test validation of duplicate tag names."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
@v1.0 2023-01-20T09:00:00Z John Doe <john@example.com> # Release v1.0
@v1.0 2023-01-21T09:00:00Z John Doe <john@example.com> # Duplicate release
"""
        plan_file.write_text(plan_content)
        
        with pytest.raises(PlanError, match="Duplicate tag name: v1.0"):
            Plan.from_file(plan_file)
    
    def test_validate_missing_dependency(self, tmp_path):
        """Test validation of missing dependencies."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

users [nonexistent] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
"""
        plan_file.write_text(plan_content)
        
        with pytest.raises(PlanError, match="depends on unknown change: nonexistent"):
            Plan.from_file(plan_file)
    
    def test_changes_since(self, tmp_path):
        """Test getting changes since a specific change."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
users [initial_schema] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
posts [users] 2023-01-17T16:45:00Z Jane Smith <jane@example.com> # Add posts table
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        # Get changes since initial_schema
        changes = plan.changes_since("initial_schema")
        assert len(changes) == 2
        assert changes[0].name == "users"
        assert changes[1].name == "posts"
        
        # Get changes since users
        changes = plan.changes_since("users")
        assert len(changes) == 1
        assert changes[0].name == "posts"
        
        # Get changes since last change
        changes = plan.changes_since("posts")
        assert len(changes) == 0
    
    def test_changes_since_nonexistent(self, tmp_path):
        """Test getting changes since nonexistent change."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        with pytest.raises(PlanError, match="Change not found: nonexistent"):
            plan.changes_since("nonexistent")
    
    def test_get_change(self, tmp_path):
        """Test getting change by name."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

users 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        change = plan.get_change("users")
        assert change is not None
        assert change.name == "users"
        
        assert plan.get_change("nonexistent") is None
    
    def test_get_tag(self, tmp_path):
        """Test getting tag by name."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
@v1.0 2023-01-20T09:00:00Z John Doe <john@example.com> # Release v1.0
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        tag = plan.get_tag("v1.0")
        assert tag is not None
        assert tag.name == "v1.0"
        
        assert plan.get_tag("nonexistent") is None
    
    def test_add_change(self, tmp_path):
        """Test adding change to plan."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        new_change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 16, 14, 20, 0),
            planner_name="John Doe",
            planner_email="john@example.com"
        )
        
        plan.add_change(new_change)
        
        assert len(plan.changes) == 2
        assert plan.get_change("users") == new_change
    
    def test_add_duplicate_change(self, tmp_path):
        """Test adding duplicate change to plan."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

users 2023-01-15T10:30:00Z John Doe <john@example.com> # Add users table
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        duplicate_change = Change(
            name="users",
            note="Duplicate users table",
            timestamp=datetime(2023, 1, 16, 14, 20, 0),
            planner_name="John Doe",
            planner_email="john@example.com"
        )
        
        with pytest.raises(PlanError, match="Change users already exists"):
            plan.add_change(duplicate_change)
    
    def test_save_plan(self, tmp_path):
        """Test saving plan to file."""
        plan_file = tmp_path / "sqitch.plan"
        
        # Create plan programmatically
        plan = Plan(
            file=plan_file,
            project="myproject",
            uri="https://github.com/example/myproject",
            syntax_version="1.0.0"
        )
        
        change = Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="John Doe",
            planner_email="john@example.com"
        )
        plan.add_change(change)
        
        tag = Tag(
            name="v1.0",
            note="Release v1.0",
            timestamp=datetime(2023, 1, 20, 9, 0, 0),
            planner_name="John Doe",
            planner_email="john@example.com"
        )
        plan.add_tag(tag)
        
        # Save and reload
        plan.save()
        
        reloaded_plan = Plan.from_file(plan_file)
        
        assert reloaded_plan.project == "myproject"
        assert reloaded_plan.uri == "https://github.com/example/myproject"
        assert len(reloaded_plan.changes) == 1
        assert len(reloaded_plan.tags) == 1
        assert reloaded_plan.changes[0].name == "users"
        assert reloaded_plan.tags[0].name == "v1.0"
    
    def test_parse_plan_complex_dependencies(self, tmp_path):
        """Test parsing plan with complex dependency formats."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
users [initial_schema !old_users] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
posts [users external@otherproject] 2023-01-17T16:45:00Z Jane Smith <jane@example.com> # Add posts table
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 3
        
        # Check users change dependencies
        users_change = plan.changes[1]
        assert len(users_change.dependencies) == 2
        assert users_change.dependencies[0].type == "require"
        assert users_change.dependencies[0].change == "initial_schema"
        assert users_change.dependencies[1].type == "conflict"
        assert users_change.dependencies[1].change == "old_users"
        
        # Check posts change dependencies
        posts_change = plan.changes[2]
        assert len(posts_change.dependencies) == 2
        assert posts_change.dependencies[0].type == "require"
        assert posts_change.dependencies[0].change == "users"
        assert posts_change.dependencies[1].type == "require"
        assert posts_change.dependencies[1].change == "external"
        assert posts_change.dependencies[1].project == "otherproject"
    
    def test_parse_plan_unicode_content(self, tmp_path):
        """Test parsing plan with unicode characters."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject
%uri=https://github.com/example/myproject

initial_schema 2023-01-15T10:30:00Z José García <jose@example.com> # Schéma initial
users [initial_schema] 2023-01-16T14:20:00Z 张三 <zhang@example.com> # 添加用户表
"""
        plan_file.write_text(plan_content, encoding='utf-8')
        
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 2
        assert plan.changes[0].planner_name == "José García"
        assert plan.changes[0].note == "Schéma initial"
        assert plan.changes[1].planner_name == "张三"
        assert plan.changes[1].note == "添加用户表"
    
    def test_parse_plan_malformed_encoding(self, tmp_path):
        """Test parsing plan with invalid encoding."""
        plan_file = tmp_path / "sqitch.plan"
        # Write invalid UTF-8 bytes
        plan_file.write_bytes(b'%project=test\n\xff\xfe invalid utf-8')
        
        with pytest.raises(PlanError, match="Invalid encoding"):
            Plan.from_file(plan_file)
    
    def test_validate_chronological_order(self, tmp_path):
        """Test validation of chronological order."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

users 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema (out of order)
"""
        plan_file.write_text(plan_content)
        
        with pytest.raises(PlanError, match="has earlier timestamp than previous change"):
            Plan.from_file(plan_file)
    
    def test_parse_plan_with_cross_project_dependencies(self, tmp_path):
        """Test parsing plan with cross-project dependencies (should not error)."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

users [external_change@otherproject] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
"""
        plan_file.write_text(plan_content)
        
        # Cross-project dependencies should not cause validation errors
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 1
        assert len(plan.changes[0].dependencies) == 1
        assert plan.changes[0].dependencies[0].project == "otherproject"
    
    def test_parse_plan_empty_dependency_brackets(self, tmp_path):
        """Test parsing plan with empty dependency brackets."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

users [] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 1
        assert len(plan.changes[0].dependencies) == 0
    
    def test_parse_plan_complex_dependencies(self, tmp_path):
        """Test parsing plan with complex dependency formats."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema
users [initial_schema !old_users] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
posts [users external@otherproject] 2023-01-17T16:45:00Z Jane Smith <jane@example.com> # Add posts table
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 3
        
        # Check users change dependencies
        users_change = plan.changes[1]
        assert len(users_change.dependencies) == 2
        assert users_change.dependencies[0].type == "require"
        assert users_change.dependencies[0].change == "initial_schema"
        assert users_change.dependencies[1].type == "conflict"
        assert users_change.dependencies[1].change == "old_users"
        
        # Check posts change dependencies
        posts_change = plan.changes[2]
        assert len(posts_change.dependencies) == 2
        assert posts_change.dependencies[0].type == "require"
        assert posts_change.dependencies[0].change == "users"
        assert posts_change.dependencies[1].type == "require"
        assert posts_change.dependencies[1].change == "external"
        assert posts_change.dependencies[1].project == "otherproject"
    
    def test_parse_plan_unicode_content(self, tmp_path):
        """Test parsing plan with unicode characters."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject
%uri=https://github.com/example/myproject

initial_schema 2023-01-15T10:30:00Z José García <jose@example.com> # Schéma initial
users [initial_schema] 2023-01-16T14:20:00Z 张三 <zhang@example.com> # 添加用户表
"""
        plan_file.write_text(plan_content, encoding='utf-8')
        
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 2
        assert plan.changes[0].planner_name == "José García"
        assert plan.changes[0].note == "Schéma initial"
        assert plan.changes[1].planner_name == "张三"
        assert plan.changes[1].note == "添加用户表"
    
    def test_parse_plan_malformed_encoding(self, tmp_path):
        """Test parsing plan with invalid encoding."""
        plan_file = tmp_path / "sqitch.plan"
        # Write invalid UTF-8 bytes
        plan_file.write_bytes(b'%project=test\n\xff\xfe invalid utf-8')
        
        with pytest.raises(PlanError, match="Invalid encoding"):
            Plan.from_file(plan_file)
    
    def test_validate_chronological_order(self, tmp_path):
        """Test validation of chronological order."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

users 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
initial_schema 2023-01-15T10:30:00Z John Doe <john@example.com> # Initial schema (out of order)
"""
        plan_file.write_text(plan_content)
        
        with pytest.raises(PlanError, match="has earlier timestamp than previous change"):
            Plan.from_file(plan_file)
    
    def test_parse_plan_with_cross_project_dependencies(self, tmp_path):
        """Test parsing plan with cross-project dependencies (should not error)."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

users [external_change@otherproject] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
"""
        plan_file.write_text(plan_content)
        
        # Cross-project dependencies should not cause validation errors
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 1
        assert len(plan.changes[0].dependencies) == 1
        assert plan.changes[0].dependencies[0].project == "otherproject"
    
    def test_parse_plan_empty_dependency_brackets(self, tmp_path):
        """Test parsing plan with empty dependency brackets."""
        plan_file = tmp_path / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=myproject

users [] 2023-01-16T14:20:00Z John Doe <john@example.com> # Add users table
"""
        plan_file.write_text(plan_content)
        
        plan = Plan.from_file(plan_file)
        
        assert len(plan.changes) == 1
        assert len(plan.changes[0].dependencies) == 0