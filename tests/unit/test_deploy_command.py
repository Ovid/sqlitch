"""
Unit tests for deploy command.

Tests the deploy command implementation including argument parsing,
change determination, dependency validation, and deployment execution.
"""

import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, call
from typing import List, Dict, Any

from sqlitch.commands.deploy import DeployCommand
from sqlitch.core.exceptions import SqlitchError, DeploymentError, PlanError
from sqlitch.core.sqitch import Sqitch
from sqlitch.core.config import Config
from sqlitch.core.plan import Plan
from sqlitch.core.change import Change, Dependency
from sqlitch.core.types import Target, URI, ChangeStatus


@pytest.fixture
def mock_sqitch():
    """Create mock Sqitch instance."""
    sqitch = Mock(spec=Sqitch)
    sqitch.verbosity = 0
    sqitch.logger = Mock()
    sqitch.config = Mock(spec=Config)
    
    # Mock methods
    sqitch.require_initialized = Mock()
    sqitch.validate_user_info = Mock(return_value=[])
    sqitch.get_plan_file = Mock(return_value=Path('sqitch.plan'))
    sqitch.get_target = Mock()
    sqitch.engine_for_target = Mock()
    
    return sqitch


@pytest.fixture
def deploy_command(mock_sqitch):
    """Create deploy command instance."""
    return DeployCommand(mock_sqitch)


@pytest.fixture
def sample_plan():
    """Create sample plan with changes."""
    plan = Mock(spec=Plan)
    plan.project_name = "test_project"
    plan.changes = [
        Change(
            name="initial_schema",
            note="Initial database schema",
            timestamp=datetime(2023, 1, 15, 10, 0, 0),
            planner_name="Test User",
            planner_email="test@example.com",
            dependencies=[]
        ),
        Change(
            name="users_table",
            note="Add users table",
            timestamp=datetime(2023, 1, 16, 11, 0, 0),
            planner_name="Test User",
            planner_email="test@example.com",
            dependencies=[
                Dependency(type="require", change="initial_schema")
            ]
        ),
        Change(
            name="posts_table",
            note="Add posts table",
            timestamp=datetime(2023, 1, 17, 12, 0, 0),
            planner_name="Test User",
            planner_email="test@example.com",
            dependencies=[
                Dependency(type="require", change="users_table")
            ]
        )
    ]
    
    # Mock tag
    from sqlitch.core.change import Tag
    plan.tags = [
        Tag(
            name="v1.0",
            note="Version 1.0 release",
            timestamp=datetime(2023, 1, 17, 15, 0, 0),
            planner_name="Test User",
            planner_email="test@example.com"
        )
    ]
    
    plan.get_tag = Mock(return_value=plan.tags[0])
    
    return plan


@pytest.fixture
def mock_engine():
    """Create mock database engine."""
    engine = Mock()
    engine.ensure_registry = Mock()
    engine.get_deployed_changes = Mock(return_value=[])
    engine.deploy_change = Mock()
    engine.verify_change = Mock(return_value=True)
    return engine


@pytest.fixture
def mock_target():
    """Create mock target."""
    return Mock(spec=Target)


class TestDeployCommandArgumentParsing:
    """Test deploy command argument parsing."""
    
    def test_parse_args_empty(self, deploy_command):
        """Test parsing empty arguments."""
        options = deploy_command._parse_args([])
        
        assert options['target'] is None
        assert options['plan_file'] is None
        assert options['to_change'] is None
        assert options['mode'] == 'all'
        assert options['verify'] is True
        assert options['log_only'] is False
    
    def test_parse_args_target(self, deploy_command):
        """Test parsing target argument."""
        options = deploy_command._parse_args(['--target', 'production'])
        
        assert options['target'] == 'production'
    
    def test_parse_args_plan_file(self, deploy_command):
        """Test parsing plan file argument."""
        options = deploy_command._parse_args(['--plan-file', 'custom.plan'])
        
        assert options['plan_file'] == Path('custom.plan')
    
    def test_parse_args_to_change(self, deploy_command):
        """Test parsing to-change argument."""
        options = deploy_command._parse_args(['--to-change', 'users_table'])
        
        assert options['to_change'] == 'users_table'
        assert options['mode'] == 'change'
    
    def test_parse_args_to_tag(self, deploy_command):
        """Test parsing to-tag argument."""
        options = deploy_command._parse_args(['--to-tag', 'v1.0'])
        
        assert options['to_change'] == 'v1.0'
        assert options['mode'] == 'tag'
    
    def test_parse_args_positional_change(self, deploy_command):
        """Test parsing positional change argument."""
        options = deploy_command._parse_args(['users_table'])
        
        assert options['to_change'] == 'users_table'
        assert options['mode'] == 'change'
    
    def test_parse_args_no_verify(self, deploy_command):
        """Test parsing no-verify flag."""
        options = deploy_command._parse_args(['--no-verify'])
        
        assert options['verify'] is False
    
    def test_parse_args_log_only(self, deploy_command):
        """Test parsing log-only flag."""
        options = deploy_command._parse_args(['--log-only'])
        
        assert options['log_only'] is True
    
    def test_parse_args_lock_timeout(self, deploy_command):
        """Test parsing lock timeout."""
        options = deploy_command._parse_args(['--lock-timeout', '30'])
        
        assert options['lock_timeout'] == 30
    
    def test_parse_args_invalid_lock_timeout(self, deploy_command):
        """Test parsing invalid lock timeout."""
        with pytest.raises(SqlitchError, match="must be an integer"):
            deploy_command._parse_args(['--lock-timeout', 'invalid'])
    
    def test_parse_args_unknown_option(self, deploy_command):
        """Test parsing unknown option."""
        with pytest.raises(SqlitchError, match="Unknown option"):
            deploy_command._parse_args(['--unknown'])
    
    def test_parse_args_missing_value(self, deploy_command):
        """Test parsing option with missing value."""
        with pytest.raises(SqlitchError, match="requires a value"):
            deploy_command._parse_args(['--target'])
    
    def test_parse_args_help(self, deploy_command):
        """Test parsing help option."""
        with patch.object(deploy_command, '_show_help') as mock_help:
            with pytest.raises(SystemExit):
                deploy_command._parse_args(['--help'])
            mock_help.assert_called_once()


class TestDeployCommandPlanLoading:
    """Test deploy command plan loading."""
    
    def test_load_plan_default(self, deploy_command, mock_sqitch):
        """Test loading plan with default path."""
        mock_plan = Mock(spec=Plan)
        
        with patch('sqlitch.commands.deploy.Plan.from_file', return_value=mock_plan) as mock_from_file, \
             patch('pathlib.Path.exists', return_value=True):
            
            plan = deploy_command._load_plan()
            
            assert plan == mock_plan
            mock_from_file.assert_called_once_with(mock_sqitch.get_plan_file.return_value)
    
    def test_load_plan_custom_path(self, deploy_command):
        """Test loading plan with custom path."""
        custom_path = Path('custom.plan')
        mock_plan = Mock(spec=Plan)
        
        with patch('sqlitch.commands.deploy.Plan.from_file', return_value=mock_plan) as mock_from_file, \
             patch('pathlib.Path.exists', return_value=True):
            
            plan = deploy_command._load_plan(custom_path)
            
            assert plan == mock_plan
            mock_from_file.assert_called_once_with(custom_path)
    
    def test_load_plan_not_found(self, deploy_command):
        """Test loading non-existent plan file."""
        with patch('pathlib.Path.exists', return_value=False):
            with pytest.raises(PlanError, match="Plan file not found"):
                deploy_command._load_plan(Path('missing.plan'))
    
    def test_load_plan_parse_error(self, deploy_command):
        """Test loading plan with parse error."""
        with patch('sqlitch.commands.deploy.Plan.from_file', side_effect=Exception("Parse error")), \
             patch('pathlib.Path.exists', return_value=True):
            
            with pytest.raises(PlanError, match="Failed to load plan file"):
                deploy_command._load_plan(Path('bad.plan'))


class TestDeployCommandChangeSelection:
    """Test deploy command change selection logic."""
    
    def test_determine_changes_all_pending(self, deploy_command, sample_plan, mock_engine):
        """Test determining changes when all are pending."""
        mock_engine.get_deployed_changes.return_value = []
        
        changes = deploy_command._determine_changes_to_deploy(
            mock_engine, sample_plan, {}
        )
        
        assert len(changes) == 3
        assert changes[0].name == "initial_schema"
        assert changes[1].name == "users_table"
        assert changes[2].name == "posts_table"
    
    def test_determine_changes_some_deployed(self, deploy_command, sample_plan, mock_engine):
        """Test determining changes when some are already deployed."""
        # Mock first change as already deployed
        deployed_id = sample_plan.changes[0].id
        mock_engine.get_deployed_changes.return_value = [deployed_id]
        
        changes = deploy_command._determine_changes_to_deploy(
            mock_engine, sample_plan, {}
        )
        
        assert len(changes) == 2
        assert changes[0].name == "users_table"
        assert changes[1].name == "posts_table"
    
    def test_determine_changes_all_deployed(self, deploy_command, sample_plan, mock_engine):
        """Test determining changes when all are deployed."""
        deployed_ids = [change.id for change in sample_plan.changes]
        mock_engine.get_deployed_changes.return_value = deployed_ids
        
        changes = deploy_command._determine_changes_to_deploy(
            mock_engine, sample_plan, {}
        )
        
        assert len(changes) == 0
    
    def test_determine_changes_to_specific_change(self, deploy_command, sample_plan, mock_engine):
        """Test determining changes up to specific change."""
        mock_engine.get_deployed_changes.return_value = []
        options = {'to_change': 'users_table', 'mode': 'change'}
        
        changes = deploy_command._determine_changes_to_deploy(
            mock_engine, sample_plan, options
        )
        
        assert len(changes) == 2
        assert changes[0].name == "initial_schema"
        assert changes[1].name == "users_table"
    
    def test_determine_changes_to_tag(self, deploy_command, sample_plan, mock_engine):
        """Test determining changes up to tag."""
        mock_engine.get_deployed_changes.return_value = []
        options = {'to_change': 'v1.0', 'mode': 'tag'}
        
        changes = deploy_command._determine_changes_to_deploy(
            mock_engine, sample_plan, options
        )
        
        # Should include all changes up to the tag timestamp
        assert len(changes) == 3
    
    def test_get_changes_up_to_change_by_name(self, deploy_command, sample_plan):
        """Test getting changes up to specific change by name."""
        changes = deploy_command._get_changes_up_to_change(sample_plan, 'users_table')
        
        assert len(changes) == 2
        assert changes[0].name == "initial_schema"
        assert changes[1].name == "users_table"
    
    def test_get_changes_up_to_change_by_id(self, deploy_command, sample_plan):
        """Test getting changes up to specific change by ID."""
        target_id = sample_plan.changes[1].id
        changes = deploy_command._get_changes_up_to_change(sample_plan, target_id)
        
        assert len(changes) == 2
        assert changes[0].name == "initial_schema"
        assert changes[1].name == "users_table"
    
    def test_get_changes_up_to_change_not_found(self, deploy_command, sample_plan):
        """Test getting changes up to non-existent change."""
        with pytest.raises(SqlitchError, match="Change not found in plan"):
            deploy_command._get_changes_up_to_change(sample_plan, 'nonexistent')
    
    def test_get_changes_up_to_tag_found(self, deploy_command, sample_plan):
        """Test getting changes up to tag."""
        changes = deploy_command._get_changes_up_to_tag(sample_plan, 'v1.0')
        
        # Should include all changes up to tag timestamp
        assert len(changes) == 3
    
    def test_get_changes_up_to_tag_not_found(self, deploy_command, sample_plan):
        """Test getting changes up to non-existent tag."""
        sample_plan.get_tag.return_value = None
        
        with pytest.raises(SqlitchError, match="Tag not found in plan"):
            deploy_command._get_changes_up_to_tag(sample_plan, 'nonexistent')


class TestDeployCommandDependencyValidation:
    """Test deploy command dependency validation."""
    
    def test_validate_dependencies_satisfied(self, deploy_command):
        """Test dependency validation when all dependencies are satisfied."""
        # Create mock plan with the dependency change
        initial_change = Change(
            name="initial_schema",
            note="Initial schema",
            timestamp=datetime(2023, 1, 15, 10, 0, 0),
            planner_name="Test User",
            planner_email="test@example.com",
            dependencies=[]
        )
        
        changes = [
            Change(
                name="users_table",
                note="Add users table",
                timestamp=datetime(2023, 1, 16, 11, 0, 0),
                planner_name="Test User",
                planner_email="test@example.com",
                dependencies=[
                    Dependency(type="require", change="initial_schema")
                ]
            )
        ]
        
        # Mock plan with both changes
        mock_plan = Mock()
        mock_plan.changes = [initial_change] + changes
        deploy_command._current_plan = mock_plan
        
        # Mock that initial_schema is already deployed
        deployed_ids = {initial_change.id}
        
        # Should not raise exception
        deploy_command._validate_dependencies(changes, deployed_ids)
    
    def test_validate_dependencies_in_batch(self, deploy_command):
        """Test dependency validation when dependency is in same batch."""
        changes = [
            Change(
                name="initial_schema",
                note="Initial schema",
                timestamp=datetime(2023, 1, 15, 10, 0, 0),
                planner_name="Test User",
                planner_email="test@example.com",
                dependencies=[]
            ),
            Change(
                name="users_table",
                note="Add users table",
                timestamp=datetime(2023, 1, 16, 11, 0, 0),
                planner_name="Test User",
                planner_email="test@example.com",
                dependencies=[
                    Dependency(type="require", change="initial_schema")
                ]
            )
        ]
        
        deployed_ids = set()
        
        # Should not raise exception since initial_schema is in the batch
        deploy_command._validate_dependencies(changes, deployed_ids)
    
    def test_validate_dependencies_missing(self, deploy_command):
        """Test dependency validation when dependency is missing."""
        changes = [
            Change(
                name="users_table",
                note="Add users table",
                timestamp=datetime(2023, 1, 16, 11, 0, 0),
                planner_name="Test User",
                planner_email="test@example.com",
                dependencies=[
                    Dependency(type="require", change="missing_change")
                ]
            )
        ]
        
        deployed_ids = set()
        
        with pytest.raises(SqlitchError, match="requires missing_change"):
            deploy_command._validate_dependencies(changes, deployed_ids)
    
    def test_validate_dependencies_cross_project(self, deploy_command):
        """Test dependency validation for cross-project dependencies."""
        changes = [
            Change(
                name="users_table",
                note="Add users table",
                timestamp=datetime(2023, 1, 16, 11, 0, 0),
                planner_name="Test User",
                planner_email="test@example.com",
                dependencies=[
                    Dependency(type="require", change="external_change", project="other_project")
                ]
            )
        ]
        
        deployed_ids = set()
        
        # Should not raise exception for cross-project dependencies
        deploy_command._validate_dependencies(changes, deployed_ids)


class TestDeployCommandExecution:
    """Test deploy command execution."""
    
    def test_deploy_changes_success(self, deploy_command, sample_plan, mock_engine):
        """Test successful deployment of changes."""
        changes = sample_plan.changes[:2]  # Deploy first two changes
        options = {'verify': True}
        
        result = deploy_command._deploy_changes(mock_engine, changes, options)
        
        assert result == 0
        assert mock_engine.deploy_change.call_count == 2
        assert mock_engine.verify_change.call_count == 2
    
    def test_deploy_changes_no_verify(self, deploy_command, sample_plan, mock_engine):
        """Test deployment without verification."""
        changes = sample_plan.changes[:1]
        options = {'verify': False}
        
        result = deploy_command._deploy_changes(mock_engine, changes, options)
        
        assert result == 0
        assert mock_engine.deploy_change.call_count == 1
        assert mock_engine.verify_change.call_count == 0
    
    def test_deploy_changes_deployment_failure(self, deploy_command, sample_plan, mock_engine):
        """Test deployment failure."""
        changes = sample_plan.changes[:2]
        options = {'verify': True}
        
        # Mock deployment failure on second change
        mock_engine.deploy_change.side_effect = [None, Exception("Deployment failed")]
        
        result = deploy_command._deploy_changes(mock_engine, changes, options)
        
        assert result == 1
        assert mock_engine.deploy_change.call_count == 2
    
    def test_deploy_changes_verification_failure(self, deploy_command, sample_plan, mock_engine):
        """Test verification failure."""
        changes = sample_plan.changes[:1]
        options = {'verify': True}
        
        # Mock verification failure
        mock_engine.verify_change.return_value = False
        
        result = deploy_command._deploy_changes(mock_engine, changes, options)
        
        assert result == 1
        assert mock_engine.deploy_change.call_count == 1
        assert mock_engine.verify_change.call_count == 1
    
    def test_deploy_changes_keyboard_interrupt(self, deploy_command, sample_plan, mock_engine):
        """Test deployment interrupted by user."""
        changes = sample_plan.changes[:2]
        options = {'verify': True}
        
        # Mock keyboard interrupt on second change
        mock_engine.deploy_change.side_effect = [None, KeyboardInterrupt()]
        
        result = deploy_command._deploy_changes(mock_engine, changes, options)
        
        assert result == 130
    
    def test_log_deployment_plan(self, deploy_command, sample_plan):
        """Test logging deployment plan."""
        changes = sample_plan.changes[:2]
        
        result = deploy_command._log_deployment_plan(changes)
        
        assert result == 0
        # Should have logged the changes
        assert deploy_command.sqitch.logger.info.called
    
    def test_log_deployment_plan_empty(self, deploy_command):
        """Test logging empty deployment plan."""
        changes = []
        
        result = deploy_command._log_deployment_plan(changes)
        
        assert result == 0


class TestDeployCommandIntegration:
    """Test deploy command integration."""
    
    def test_execute_success(self, deploy_command, mock_sqitch, sample_plan, mock_engine, mock_target):
        """Test successful command execution."""
        # Setup mocks
        mock_sqitch.get_target.return_value = mock_target
        mock_engine.get_deployed_changes.return_value = []
        
        with patch.object(deploy_command, '_load_plan', return_value=sample_plan), \
             patch('sqlitch.engines.base.EngineRegistry.create_engine', return_value=mock_engine):
            result = deploy_command.execute([])
        
        assert result == 0
        mock_sqitch.require_initialized.assert_called_once()
        mock_sqitch.validate_user_info.assert_called_once()
        mock_engine.ensure_registry.assert_called_once()
    
    def test_execute_not_initialized(self, deploy_command, mock_sqitch):
        """Test execution when project not initialized."""
        mock_sqitch.require_initialized.side_effect = SqlitchError("Not initialized")
        
        result = deploy_command.execute([])
        
        assert result == 1
    
    def test_execute_invalid_user_info(self, deploy_command, mock_sqitch):
        """Test execution with invalid user info."""
        mock_sqitch.validate_user_info.side_effect = SqlitchError("User info invalid")
        
        result = deploy_command.execute([])
        
        assert result == 1
    
    def test_execute_plan_load_error(self, deploy_command, mock_sqitch):
        """Test execution with plan loading error."""
        with patch.object(deploy_command, '_load_plan', side_effect=PlanError("Plan error")):
            result = deploy_command.execute([])
        
        assert result == 1
    
    def test_execute_unexpected_error(self, deploy_command, mock_sqitch):
        """Test execution with unexpected error."""
        mock_sqitch.require_initialized.side_effect = Exception("Unexpected error")
        
        result = deploy_command.execute([])
        
        assert result == 2
    
    def test_execute_with_verbosity(self, deploy_command, mock_sqitch, sample_plan, mock_engine, mock_target):
        """Test execution with verbose output."""
        mock_sqitch.verbosity = 2
        mock_sqitch.get_target.return_value = mock_target
        mock_engine.get_deployed_changes.return_value = []
        
        with patch.object(deploy_command, '_load_plan', return_value=sample_plan), \
             patch('sqlitch.engines.base.EngineRegistry.create_engine', return_value=mock_engine):
            result = deploy_command.execute([])
        
        assert result == 0
        # Should have made verbose logging calls
        assert deploy_command.sqitch.logger.info.called


class TestDeployCommandHelp:
    """Test deploy command help functionality."""
    
    def test_show_help(self, deploy_command, capsys):
        """Test showing help message."""
        deploy_command._show_help()
        
        captured = capsys.readouterr()
        assert "Usage: sqlitch deploy" in captured.out
        assert "Deploy database changes" in captured.out
        assert "--target" in captured.out
        assert "--verify" in captured.out