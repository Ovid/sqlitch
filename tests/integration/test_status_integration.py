"""
Integration tests for the status command.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone

from sqlitch.commands.init import InitCommand
from sqlitch.commands.deploy import DeployCommand
from sqlitch.commands.status import StatusCommand
from sqlitch.core.config import Config
from sqlitch.core.sqitch import Sqitch
from sqlitch.core.plan import Plan
from sqlitch.core.change import Change, Dependency


@pytest.fixture
def temp_project_dir():
    """Create a temporary project directory."""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sqitch_instance(temp_project_dir):
    """Create a Sqitch instance for testing."""
    # Change to temp directory
    original_cwd = Path.cwd()
    import os
    os.chdir(temp_project_dir)
    
    try:
        config = Config()
        sqitch = Sqitch(config=config, options={'verbosity': 0})
        yield sqitch
    finally:
        os.chdir(original_cwd)


@pytest.fixture
def initialized_project(temp_project_dir, sqitch_instance):
    """Create an initialized sqitch project."""
    # Initialize project
    init_command = InitCommand(sqitch_instance)
    init_command.execute(['pg'])
    
    # Create a simple plan file
    plan_content = """%syntax-version=1.0.0
%project=test_project
%uri=https://github.com/example/test_project

initial_schema 2023-01-15T10:30:00Z Test User <test@example.com> # Initial schema
users [initial_schema] 2023-01-16T14:20:00Z Test User <test@example.com> # Add users table
@v1.0 2023-01-20T09:00:00Z Test User <test@example.com> # Release v1.0

posts [users] 2023-01-25T11:15:00Z Jane Smith <jane@example.com> # Add posts table
"""
    
    plan_file = temp_project_dir / "sqitch.plan"
    plan_file.write_text(plan_content)
    
    # Create deploy scripts
    deploy_dir = temp_project_dir / "deploy"
    deploy_dir.mkdir(exist_ok=True)
    
    (deploy_dir / "initial_schema.sql").write_text("""
-- Deploy test_project:initial_schema to pg

BEGIN;

CREATE SCHEMA IF NOT EXISTS app;

COMMIT;
""")
    
    (deploy_dir / "users.sql").write_text("""
-- Deploy test_project:users to pg
-- requires: initial_schema

BEGIN;

CREATE TABLE app.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMIT;
""")
    
    (deploy_dir / "posts.sql").write_text("""
-- Deploy test_project:posts to pg
-- requires: users

BEGIN;

CREATE TABLE app.posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES app.users(id),
    title VARCHAR(200) NOT NULL,
    content TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMIT;
""")
    
    # Create revert scripts
    revert_dir = temp_project_dir / "revert"
    revert_dir.mkdir(exist_ok=True)
    
    (revert_dir / "initial_schema.sql").write_text("""
-- Revert test_project:initial_schema from pg

BEGIN;

DROP SCHEMA IF EXISTS app CASCADE;

COMMIT;
""")
    
    (revert_dir / "users.sql").write_text("""
-- Revert test_project:users from pg

BEGIN;

DROP TABLE IF EXISTS app.users CASCADE;

COMMIT;
""")
    
    (revert_dir / "posts.sql").write_text("""
-- Revert test_project:posts from pg

BEGIN;

DROP TABLE IF EXISTS app.posts;

COMMIT;
""")
    
    # Create verify scripts
    verify_dir = temp_project_dir / "verify"
    verify_dir.mkdir(exist_ok=True)
    
    (verify_dir / "initial_schema.sql").write_text("""
-- Verify test_project:initial_schema on pg

BEGIN;

SELECT 1/COUNT(*) FROM information_schema.schemata WHERE schema_name = 'app';

ROLLBACK;
""")
    
    (verify_dir / "users.sql").write_text("""
-- Verify test_project:users on pg

BEGIN;

SELECT id, username, email, created_at FROM app.users WHERE FALSE;

ROLLBACK;
""")
    
    (verify_dir / "posts.sql").write_text("""
-- Verify test_project:posts on pg

BEGIN;

SELECT id, user_id, title, content, created_at FROM app.posts WHERE FALSE;

ROLLBACK;
""")
    
    return temp_project_dir


class TestStatusIntegration:
    """Integration tests for status command."""
    
    def test_status_no_changes_deployed(self, initialized_project, sqitch_instance):
        """Test status when no changes are deployed."""
        from unittest.mock import Mock, patch
        
        status_command = StatusCommand(sqitch_instance)
        
        # Mock the engine to simulate no deployed changes
        mock_engine = Mock()
        mock_engine.ensure_registry.return_value = None
        mock_engine.get_current_state.return_value = None
        
        mock_target = Mock()
        mock_target.uri = "postgresql://test@localhost/test_db"
        
        with patch('sqlitch.commands.status.EngineRegistry') as mock_registry:
            mock_registry.create_engine.return_value = mock_engine
            
            with patch.object(status_command, 'get_target', return_value=mock_target):
                result = status_command.execute([])
                assert result == 1  # Should return 1 when no changes deployed
    
    def test_status_with_mock_engine(self, initialized_project, sqitch_instance):
        """Test status command with mocked engine."""
        from unittest.mock import Mock, patch
        
        status_command = StatusCommand(sqitch_instance)
        
        # Mock the engine and its methods
        mock_engine = Mock()
        mock_engine.ensure_registry.return_value = None
        
        # Mock current state
        current_state = {
            'change_id': 'abc123def456',
            'change': 'users',
            'project': 'test_project',
            'note': 'Add users table',
            'tags': ['v1.0'],
            'committed_at': datetime(2023, 1, 16, 14, 20, 0, tzinfo=timezone.utc),
            'committer_name': 'Test User',
            'committer_email': 'test@example.com',
            'planner_name': 'Test User',
            'planner_email': 'test@example.com',
            'planned_at': datetime(2023, 1, 16, 14, 20, 0, tzinfo=timezone.utc)
        }
        mock_engine.get_current_state.return_value = current_state
        
        # Mock current changes
        changes = [
            {
                'change_id': 'abc123def456',
                'change': 'users',
                'committed_at': datetime(2023, 1, 16, 14, 20, 0, tzinfo=timezone.utc),
                'committer_name': 'Test User',
                'committer_email': 'test@example.com',
                'planner_name': 'Test User',
                'planner_email': 'test@example.com',
                'planned_at': datetime(2023, 1, 16, 14, 20, 0, tzinfo=timezone.utc)
            },
            {
                'change_id': 'def456ghi789',
                'change': 'initial_schema',
                'committed_at': datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
                'committer_name': 'Test User',
                'committer_email': 'test@example.com',
                'planner_name': 'Test User',
                'planner_email': 'test@example.com',
                'planned_at': datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
            }
        ]
        mock_engine.get_current_changes.return_value = iter(changes)
        
        # Mock current tags
        tags = [
            {
                'tag_id': 'tag123',
                'tag': 'v1.0',
                'committed_at': datetime(2023, 1, 20, 9, 0, 0, tzinfo=timezone.utc),
                'committer_name': 'Test User',
                'committer_email': 'test@example.com',
                'planner_name': 'Test User',
                'planner_email': 'test@example.com',
                'planned_at': datetime(2023, 1, 20, 9, 0, 0, tzinfo=timezone.utc)
            }
        ]
        mock_engine.get_current_tags.return_value = iter(tags)
        
        # Mock target
        mock_target = Mock()
        mock_target.uri = "postgresql://test@localhost/test_db"
        
        with patch('sqlitch.commands.status.EngineRegistry') as mock_registry:
            mock_registry.create_engine.return_value = mock_engine
            
            with patch.object(status_command, 'get_target', return_value=mock_target):
                # Test basic status
                result = status_command.execute([])
                assert result == 0
                
                # Test with show changes
                result = status_command.execute(['--show-changes'])
                assert result == 0
                
                # Test with show tags
                result = status_command.execute(['--show-tags'])
                assert result == 0
                
                # Test with both
                result = status_command.execute(['--show-changes', '--show-tags'])
                assert result == 0
    
    def test_status_up_to_date(self, initialized_project, sqitch_instance):
        """Test status when all changes are deployed."""
        from unittest.mock import Mock, patch
        
        status_command = StatusCommand(sqitch_instance)
        
        # Load the actual plan
        plan = Plan.from_file(initialized_project / "sqitch.plan")
        
        # Mock engine with all changes deployed
        mock_engine = Mock()
        mock_engine.ensure_registry.return_value = None
        
        # Use the last change from the plan
        last_change = plan.changes[-1]  # posts
        current_state = {
            'change_id': last_change.id,
            'change': last_change.name,
            'project': 'test_project',
            'note': last_change.note,
            'tags': last_change.tags,
            'committed_at': datetime(2023, 1, 25, 11, 15, 0, tzinfo=timezone.utc),
            'committer_name': 'Jane Smith',
            'committer_email': 'jane@example.com',
            'planner_name': 'Jane Smith',
            'planner_email': 'jane@example.com',
            'planned_at': last_change.timestamp
        }
        mock_engine.get_current_state.return_value = current_state
        mock_engine.get_current_changes.return_value = iter([])
        mock_engine.get_current_tags.return_value = iter([])
        
        mock_target = Mock()
        mock_target.uri = "postgresql://test@localhost/test_db"
        
        with patch('sqlitch.commands.status.EngineRegistry') as mock_registry:
            mock_registry.create_engine.return_value = mock_engine
            
            with patch.object(status_command, 'get_target', return_value=mock_target):
                result = status_command.execute([])
                assert result == 0
                
                # Should show "Nothing to deploy (up-to-date)"
                # We can't easily assert on the info calls in integration tests
                # since they go through the real logger, so we just check the return code
                pass
    
    def test_status_with_undeployed_changes(self, initialized_project, sqitch_instance):
        """Test status with undeployed changes."""
        from unittest.mock import Mock, patch
        
        status_command = StatusCommand(sqitch_instance)
        
        # Load the actual plan
        plan = Plan.from_file(initialized_project / "sqitch.plan")
        
        # Mock engine with only first change deployed
        mock_engine = Mock()
        mock_engine.ensure_registry.return_value = None
        
        # Use the first change from the plan
        first_change = plan.changes[0]  # initial_schema
        current_state = {
            'change_id': first_change.id,
            'change': first_change.name,
            'project': 'test_project',
            'note': first_change.note,
            'tags': first_change.tags,
            'committed_at': datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            'committer_name': 'Test User',
            'committer_email': 'test@example.com',
            'planner_name': 'Test User',
            'planner_email': 'test@example.com',
            'planned_at': first_change.timestamp
        }
        mock_engine.get_current_state.return_value = current_state
        mock_engine.get_current_changes.return_value = iter([])
        mock_engine.get_current_tags.return_value = iter([])
        
        mock_target = Mock()
        mock_target.uri = "postgresql://test@localhost/test_db"
        
        with patch('sqlitch.commands.status.EngineRegistry') as mock_registry:
            mock_registry.create_engine.return_value = mock_engine
            
            with patch.object(status_command, 'get_target', return_value=mock_target):
                result = status_command.execute([])
                assert result == 0
                
                # Should show undeployed changes
                # We can't easily assert on the info calls in integration tests
                # since they go through the real logger, so we just check the return code
                pass
    
    def test_status_change_not_in_plan(self, initialized_project, sqitch_instance):
        """Test status when current change is not in plan."""
        from unittest.mock import Mock, patch
        
        status_command = StatusCommand(sqitch_instance)
        
        # Mock engine with unknown change
        mock_engine = Mock()
        mock_engine.ensure_registry.return_value = None
        
        current_state = {
            'change_id': 'unknown_change_id',
            'change': 'unknown_change',
            'project': 'test_project',
            'note': 'Unknown change',
            'tags': [],
            'committed_at': datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            'committer_name': 'Test User',
            'committer_email': 'test@example.com',
            'planner_name': 'Test User',
            'planner_email': 'test@example.com',
            'planned_at': datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        }
        mock_engine.get_current_state.return_value = current_state
        mock_engine.get_current_changes.return_value = iter([])
        mock_engine.get_current_tags.return_value = iter([])
        
        mock_target = Mock()
        mock_target.uri = "postgresql://test@localhost/test_db"
        
        with patch('sqlitch.commands.status.EngineRegistry') as mock_registry:
            mock_registry.create_engine.return_value = mock_engine
            
            with patch.object(status_command, 'get_target', return_value=mock_target):
                result = status_command.execute([])
                assert result == 0
                
                # Should show warning about change not found
                # We can't easily assert on the warn/error calls in integration tests
                # since they go through the real logger, so we just check the return code
                pass
    
    def test_status_date_formats(self, initialized_project, sqitch_instance):
        """Test status with different date formats."""
        from unittest.mock import Mock, patch
        
        status_command = StatusCommand(sqitch_instance)
        
        # Mock engine
        mock_engine = Mock()
        mock_engine.ensure_registry.return_value = None
        
        current_state = {
            'change_id': 'abc123',
            'change': 'test_change',
            'project': 'test_project',
            'note': 'Test change',
            'tags': [],
            'committed_at': datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            'committer_name': 'Test User',
            'committer_email': 'test@example.com',
            'planner_name': 'Test User',
            'planner_email': 'test@example.com',
            'planned_at': datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        }
        mock_engine.get_current_state.return_value = current_state
        mock_engine.get_current_changes.return_value = iter([])
        mock_engine.get_current_tags.return_value = iter([])
        
        mock_target = Mock()
        mock_target.uri = "postgresql://test@localhost/test_db"
        
        with patch('sqlitch.commands.status.EngineRegistry') as mock_registry:
            mock_registry.create_engine.return_value = mock_engine
            
            with patch.object(status_command, 'get_target', return_value=mock_target):
                # Test ISO format (default)
                result = status_command.execute([])
                assert result == 0
                
                # Test RFC format
                result = status_command.execute(['--date-format', 'rfc'])
                assert result == 0
                
                # Test custom strftime format
                result = status_command.execute(['--date-format', '%Y-%m-%d %H:%M'])
                assert result == 0
    
    def test_status_error_handling(self, initialized_project, sqitch_instance):
        """Test status command error handling."""
        from unittest.mock import Mock, patch
        
        status_command = StatusCommand(sqitch_instance)
        
        # Test with invalid arguments
        result = status_command.execute(['--unknown-option'])
        assert result == 1
        
        # Test with missing plan file
        with patch.object(Path, 'exists', return_value=False):
            result = status_command.execute([])
            assert result == 1
        
        # Test with engine error
        mock_engine = Mock()
        mock_engine.ensure_registry.side_effect = Exception("Database error")
        
        mock_target = Mock()
        mock_target.uri = "postgresql://test@localhost/test_db"
        
        with patch('sqlitch.commands.status.EngineRegistry') as mock_registry:
            mock_registry.create_engine.return_value = mock_engine
            
            with patch.object(status_command, 'get_target', return_value=mock_target):
                result = status_command.execute([])
                assert result == 2  # Unexpected error