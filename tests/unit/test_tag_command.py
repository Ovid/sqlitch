"""Unit tests for the tag command."""

import pytest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from sqlitch.commands.tag import TagCommand
from sqlitch.core.change import Change, Tag
from sqlitch.core.exceptions import SqlitchError, PlanError
from sqlitch.core.plan import Plan
from sqlitch.core.sqitch import Sqitch
from sqlitch.core.config import Config


@pytest.fixture
def mock_sqitch():
    """Create a mock Sqitch instance."""
    sqitch = Mock(spec=Sqitch)
    sqitch.config = Mock(spec=Config)
    sqitch.user_name = "Test User"
    sqitch.user_email = "test@example.com"
    sqitch.verbosity = 1
    sqitch.logger = Mock()
    sqitch.info = Mock()
    sqitch.warn = Mock()
    sqitch.vent = Mock()
    sqitch.emit = Mock()
    sqitch.debug = Mock()
    sqitch.trace = Mock()
    sqitch.comment = Mock()
    sqitch.ask_yes_no = Mock(return_value=True)
    sqitch.prompt = Mock(return_value="")
    sqitch.request_note_for = Mock(return_value="Test note")
    sqitch.validate_user_info = Mock(return_value=[])  # Return empty list for no issues
    return sqitch


@pytest.fixture
def tag_command(mock_sqitch):
    """Create a TagCommand instance."""
    return TagCommand(mock_sqitch)


@pytest.fixture
def sample_change():
    """Create a sample change."""
    return Change(
        name="test_change",
        note="Test change",
        timestamp=datetime.now(timezone.utc),
        planner_name="Test User",
        planner_email="test@example.com"
    )


@pytest.fixture
def sample_plan(sample_change):
    """Create a sample plan with a change."""
    plan = Plan(
        file=Path("sqitch.plan"),
        project="test_project"
    )
    plan.changes = [sample_change]
    plan._build_indexes()
    return plan


class TestTagCommand:
    """Test cases for TagCommand."""
    
    def test_init(self, mock_sqitch):
        """Test TagCommand initialization."""
        command = TagCommand(mock_sqitch)
        assert command.sqitch is mock_sqitch
        assert command.config is mock_sqitch.config
    
    def test_parse_args_tag_name_only(self, tag_command):
        """Test parsing arguments with tag name only."""
        tag_name, change_name, options = tag_command._parse_args(['v1.0'])
        
        assert tag_name == 'v1.0'
        assert change_name is None
        assert options['note'] == []
        assert options['all'] is False
    
    def test_parse_args_tag_and_change(self, tag_command):
        """Test parsing arguments with tag and change names."""
        tag_name, change_name, options = tag_command._parse_args(['v1.0', 'my_change'])
        
        assert tag_name == 'v1.0'
        assert change_name == 'my_change'
        assert options['note'] == []
        assert options['all'] is False
    
    def test_parse_args_with_options(self, tag_command):
        """Test parsing arguments with options."""
        args = ['--tag', 'v1.0', '--change', 'my_change', '--note', 'Release note', '--all']
        tag_name, change_name, options = tag_command._parse_args(args)
        
        assert tag_name == 'v1.0'
        assert change_name == 'my_change'
        assert options['note'] == ['Release note']
        assert options['all'] is True
    
    def test_parse_args_short_options(self, tag_command):
        """Test parsing arguments with short options."""
        args = ['-t', 'v1.0', '-c', 'my_change', '-n', 'Release note', '-a']
        tag_name, change_name, options = tag_command._parse_args(args)
        
        assert tag_name == 'v1.0'
        assert change_name == 'my_change'
        assert options['note'] == ['Release note']
        assert options['all'] is True
    
    def test_parse_args_multiple_notes(self, tag_command):
        """Test parsing arguments with multiple notes."""
        args = ['v1.0', '-n', 'First note', '-n', 'Second note']
        tag_name, change_name, options = tag_command._parse_args(args)
        
        assert tag_name == 'v1.0'
        assert change_name is None
        assert options['note'] == ['First note', 'Second note']
    
    def test_parse_args_unknown_option(self, tag_command):
        """Test parsing arguments with unknown option."""
        with pytest.raises(SqlitchError, match="Unknown option: --unknown"):
            tag_command._parse_args(['--unknown'])
    
    def test_parse_args_missing_value(self, tag_command):
        """Test parsing arguments with missing option value."""
        with pytest.raises(SqlitchError, match="Option --tag requires a value"):
            tag_command._parse_args(['--tag'])
    
    def test_parse_args_too_many_positional(self, tag_command):
        """Test parsing arguments with too many positional arguments."""
        with pytest.raises(SqlitchError, match="Unexpected argument: extra"):
            tag_command._parse_args(['tag', 'change', 'extra'])
    
    @patch('sqlitch.commands.tag.validate_tag_name')
    def test_create_tag(self, mock_validate, tag_command, sample_change):
        """Test creating a tag."""
        mock_validate.return_value = True
        
        options = {'note': ['Test note']}
        tag = tag_command._create_tag('v1.0', sample_change, options)
        
        assert tag.name == 'v1.0'
        assert tag.note == 'Test note'
        assert tag.planner_name == 'Test User'
        assert tag.planner_email == 'test@example.com'
        assert tag.change is sample_change
        assert isinstance(tag.timestamp, datetime)
    
    def test_create_tag_multiple_notes(self, tag_command, sample_change):
        """Test creating a tag with multiple notes."""
        options = {'note': ['First note', 'Second note']}
        tag = tag_command._create_tag('v1.0', sample_change, options)
        
        assert tag.note == 'First note\n\nSecond note'
    
    def test_create_tag_no_user_info(self, tag_command, sample_change):
        """Test creating a tag without user info."""
        tag_command.sqitch.user_name = None
        tag_command.sqitch.user_email = None
        
        options = {'note': []}
        with pytest.raises(SqlitchError, match="User name and email must be configured"):
            tag_command._create_tag('v1.0', sample_change, options)
    
    @patch('sqlitch.commands.tag.TagCommand._get_targets')
    @patch('sqlitch.core.plan.Plan.from_file')
    def test_add_tag_success(self, mock_plan_from_file, mock_get_targets, tag_command, sample_plan):
        """Test successfully adding a tag."""
        # Setup mocks
        mock_target = Mock()
        mock_target.plan_file = Path("sqitch.plan")
        mock_get_targets.return_value = [mock_target]
        mock_plan_from_file.return_value = sample_plan
        
        # Mock plan methods
        sample_plan.add_tag = Mock()
        sample_plan.save = Mock()
        
        # Execute
        result = tag_command._add_tag('v1.0', None, {'note': ['Test note']})
        
        # Verify
        assert result == 0
        mock_plan_from_file.assert_called_once_with(mock_target.plan_file)
        sample_plan.add_tag.assert_called_once()
        sample_plan.save.assert_called_once()
        tag_command.sqitch.info.assert_called()
    
    @patch('sqlitch.commands.tag.TagCommand._get_targets')
    @patch('sqlitch.core.plan.Plan.from_file')
    def test_add_tag_already_exists(self, mock_plan_from_file, mock_get_targets, tag_command, sample_plan):
        """Test adding a tag that already exists."""
        # Setup mocks
        mock_target = Mock()
        mock_target.plan_file = Path("sqitch.plan")
        mock_get_targets.return_value = [mock_target]
        
        # Add existing tag to plan
        sample_plan._tag_index['v1.0'] = Mock()
        mock_plan_from_file.return_value = sample_plan
        
        # Execute and verify
        with pytest.raises(SqlitchError, match='Tag "@v1.0" already exists'):
            tag_command._add_tag('v1.0', None, {'note': []})
    
    @patch('sqlitch.commands.tag.TagCommand._get_targets')
    @patch('sqlitch.core.plan.Plan.from_file')
    def test_add_tag_unknown_change(self, mock_plan_from_file, mock_get_targets, tag_command, sample_plan):
        """Test adding a tag to an unknown change."""
        # Setup mocks
        mock_target = Mock()
        mock_target.plan_file = Path("sqitch.plan")
        mock_get_targets.return_value = [mock_target]
        mock_plan_from_file.return_value = sample_plan
        
        # Execute and verify
        with pytest.raises(SqlitchError, match='Unknown change: "nonexistent"'):
            tag_command._add_tag('v1.0', 'nonexistent', {'note': []})
    
    @patch('sqlitch.commands.tag.TagCommand._get_targets')
    @patch('sqlitch.core.plan.Plan.from_file')
    def test_add_tag_no_changes(self, mock_plan_from_file, mock_get_targets, tag_command):
        """Test adding a tag to a plan with no changes."""
        # Setup mocks
        mock_target = Mock()
        mock_target.plan_file = Path("sqitch.plan")
        mock_get_targets.return_value = [mock_target]
        
        empty_plan = Plan(file=Path("sqitch.plan"), project="test_project")
        mock_plan_from_file.return_value = empty_plan
        
        # Execute and verify
        with pytest.raises(SqlitchError, match='Cannot apply tag "@v1.0" to a plan with no changes'):
            tag_command._add_tag('v1.0', None, {'note': []})
    
    @patch('sqlitch.commands.tag.TagCommand._get_targets')
    @patch('sqlitch.core.plan.Plan.from_file')
    def test_add_tag_with_change_name(self, mock_plan_from_file, mock_get_targets, tag_command, sample_plan):
        """Test adding a tag to a specific change."""
        # Setup mocks
        mock_target = Mock()
        mock_target.plan_file = Path("sqitch.plan")
        mock_get_targets.return_value = [mock_target]
        mock_plan_from_file.return_value = sample_plan
        
        # Mock plan methods
        sample_plan.add_tag = Mock()
        sample_plan.save = Mock()
        
        # Execute
        result = tag_command._add_tag('v1.0', 'test_change', {'note': ['Test note']})
        
        # Verify
        assert result == 0
        sample_plan.add_tag.assert_called_once()
        sample_plan.save.assert_called_once()
    
    @patch('sqlitch.commands.tag.TagCommand._get_targets')
    @patch('sqlitch.core.plan.Plan.from_file')
    def test_list_tags(self, mock_plan_from_file, mock_get_targets, tag_command):
        """Test listing tags."""
        # Setup mocks
        mock_target = Mock()
        mock_target.plan_file = Path("sqitch.plan")
        mock_get_targets.return_value = [mock_target]
        
        # Create plan with tags
        plan = Plan(file=Path("sqitch.plan"), project="test_project")
        tag1 = Tag(
            name="v1.0",
            note="First release",
            timestamp=datetime.now(timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com"
        )
        tag2 = Tag(
            name="v2.0",
            note="Second release",
            timestamp=datetime.now(timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com"
        )
        plan.tags = [tag1, tag2]
        mock_plan_from_file.return_value = plan
        
        # Execute
        result = tag_command._list_tags({'all': False})
        
        # Verify
        assert result == 0
        tag_command.sqitch.emit.assert_any_call("@v1.0")
        tag_command.sqitch.emit.assert_any_call("@v2.0")
    
    def test_get_targets_default(self, tag_command):
        """Test getting default target."""
        mock_target = Mock()
        tag_command.get_target = Mock(return_value=mock_target)
        
        targets = tag_command._get_targets({'all': False})
        
        assert targets == [mock_target]
        tag_command.get_target.assert_called_once_with()
    
    def test_get_targets_all(self, tag_command):
        """Test getting all targets."""
        mock_target1 = Mock()
        mock_target2 = Mock()
        tag_command.get_target = Mock(side_effect=[mock_target1, mock_target2])
        tag_command.config.get_section = Mock(return_value={'target1': {}, 'target2': {}})
        
        targets = tag_command._get_targets({'all': True})
        
        assert len(targets) == 2
        assert mock_target1 in targets
        assert mock_target2 in targets
    
    def test_get_targets_all_no_targets(self, tag_command):
        """Test getting all targets when none configured."""
        mock_target = Mock()
        tag_command.get_target = Mock(return_value=mock_target)
        tag_command.config.get_section = Mock(return_value=None)
        
        targets = tag_command._get_targets({'all': True})
        
        assert targets == [mock_target]
        tag_command.get_target.assert_called_once_with()
    
    @patch('sqlitch.commands.tag.TagCommand.require_initialized')
    @patch('sqlitch.commands.tag.TagCommand.validate_user_info')
    @patch('sqlitch.commands.tag.TagCommand._add_tag')
    def test_execute_add_tag(self, mock_add_tag, mock_validate_user, mock_require_init, tag_command):
        """Test executing tag command to add a tag."""
        mock_add_tag.return_value = 0
        
        result = tag_command.execute(['v1.0'])
        
        assert result == 0
        mock_require_init.assert_called_once()
        mock_add_tag.assert_called_once_with('v1.0', None, {'note': [], 'all': False})
    
    @patch('sqlitch.commands.tag.TagCommand.require_initialized')
    @patch('sqlitch.commands.tag.TagCommand._list_tags')
    def test_execute_list_tags(self, mock_list_tags, mock_require_init, tag_command):
        """Test executing tag command to list tags."""
        mock_list_tags.return_value = 0
        
        result = tag_command.execute([])
        
        assert result == 0
        mock_require_init.assert_called_once()
        mock_list_tags.assert_called_once_with({'note': [], 'all': False})
    
    @patch('sqlitch.commands.tag.TagCommand.require_initialized')
    def test_execute_error_handling(self, mock_require_init, tag_command):
        """Test error handling in execute method."""
        mock_require_init.side_effect = SqlitchError("Not initialized")
        tag_command.handle_error = Mock(return_value=1)
        
        result = tag_command.execute(['v1.0'])
        
        assert result == 1
        tag_command.handle_error.assert_called_once()
    
    def test_request_note_for_tag(self, tag_command):
        """Test requesting note for tag."""
        tag_command.sqitch.request_note_for.return_value = "Test note from editor"
        
        note = tag_command._request_note_for_tag()
        
        assert note == "Test note from editor"
        tag_command.sqitch.request_note_for.assert_called_once_with("tag")
    
    def test_request_note_for_tag_fallback(self, tag_command):
        """Test requesting note for tag with fallback to prompt."""
        tag_command.sqitch.request_note_for.side_effect = Exception("Editor failed")
        tag_command.prompt = Mock(return_value="Prompt note")
        
        note = tag_command._request_note_for_tag()
        
        assert note == "Prompt note"
        tag_command.prompt.assert_called_once_with("Tag note (optional): ", default="")


class TestTagCommandIntegration:
    """Integration tests for TagCommand."""
    
    @patch('sqlitch.commands.tag.TagCommand.require_initialized')
    @patch('sqlitch.commands.tag.TagCommand.validate_user_info')
    @patch('sqlitch.commands.tag.TagCommand._get_targets')
    @patch('sqlitch.core.plan.Plan.from_file')
    def test_full_add_tag_workflow(self, mock_plan_from_file, mock_get_targets, 
                                  mock_validate_user, mock_require_init, tag_command, sample_plan):
        """Test full workflow of adding a tag."""
        # Setup mocks
        mock_target = Mock()
        mock_target.plan_file = Path("sqitch.plan")
        mock_get_targets.return_value = [mock_target]
        mock_plan_from_file.return_value = sample_plan
        
        # Mock plan methods
        sample_plan.add_tag = Mock()
        sample_plan.save = Mock()
        
        # Execute
        result = tag_command.execute(['--tag', 'v1.0', '--note', 'Release note'])
        
        # Verify
        assert result == 0
        mock_require_init.assert_called_once()
        mock_validate_user.assert_called_once()
        mock_get_targets.assert_called_once()
        mock_plan_from_file.assert_called_once()
        sample_plan.add_tag.assert_called_once()
        sample_plan.save.assert_called_once()
        tag_command.sqitch.info.assert_called()