"""Integration tests for the show command."""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime, timezone

from sqlitch.commands.init import InitCommand
from sqlitch.commands.add import AddCommand
from sqlitch.commands.tag import TagCommand
from sqlitch.commands.show import ShowCommand
from sqlitch.core.config import Config
from sqlitch.core.sqitch import Sqitch


class TestShowIntegration:
    """Integration tests for show command."""
    
    @pytest.fixture
    def temp_project(self):
        """Create temporary project directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            project_dir = Path(temp_dir)
            yield project_dir
    
    @pytest.fixture
    def initialized_project(self, temp_project):
        """Create initialized sqitch project."""
        # Change to project directory
        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(temp_project)
            
            # Create config and sqitch instance
            config = Config()
            config.set('user.name', 'Test User')
            config.set('user.email', 'test@example.com')
            config.set('core.engine', 'pg')
            config.set('engine.pg.target', 'db:pg://test@localhost/test')
            
            sqitch = Sqitch(config=config)
            
            # Initialize project
            init_cmd = InitCommand(sqitch)
            init_cmd.execute(['pg'])
            
            yield temp_project, sqitch
            
        finally:
            os.chdir(original_cwd)
    
    def test_show_change_after_add(self, initialized_project):
        """Test showing change information after adding a change."""
        project_dir, sqitch = initialized_project
        
        # Add a change
        add_cmd = AddCommand(sqitch)
        add_cmd.execute(['test_change', '-n', 'Test change for show command'])
        
        # Show the change
        show_cmd = ShowCommand(sqitch)
        exit_code = show_cmd.execute(['change', 'test_change'])
        
        assert exit_code == 0
        
        # Verify change files exist
        assert (project_dir / 'deploy' / 'test_change.sql').exists()
        assert (project_dir / 'revert' / 'test_change.sql').exists()
        assert (project_dir / 'verify' / 'test_change.sql').exists()
    
    def test_show_deploy_script_content(self, initialized_project):
        """Test showing deploy script content."""
        project_dir, sqitch = initialized_project
        
        # Add a change
        add_cmd = AddCommand(sqitch)
        add_cmd.execute(['test_table', '-n', 'Add test table'])
        
        # Modify deploy script
        deploy_file = project_dir / 'deploy' / 'test_table.sql'
        deploy_file.write_text('CREATE TABLE test_table (id INTEGER PRIMARY KEY);')
        
        # Show deploy script
        show_cmd = ShowCommand(sqitch)
        
        # For integration test, just verify it doesn't crash
        # The actual output testing is done in unit tests
        exit_code = show_cmd.execute(['deploy', 'test_table'])
        
        assert exit_code == 0
    
    def test_show_revert_script_content(self, initialized_project):
        """Test showing revert script content."""
        project_dir, sqitch = initialized_project
        
        # Add a change
        add_cmd = AddCommand(sqitch)
        add_cmd.execute(['test_table', '-n', 'Add test table'])
        
        # Modify revert script
        revert_file = project_dir / 'revert' / 'test_table.sql'
        revert_file.write_text('DROP TABLE test_table;')
        
        # Show revert script
        show_cmd = ShowCommand(sqitch)
        
        # For integration test, just verify it doesn't crash
        # The actual output testing is done in unit tests
        exit_code = show_cmd.execute(['revert', 'test_table'])
        
        assert exit_code == 0
    
    def test_show_verify_script_content(self, initialized_project):
        """Test showing verify script content."""
        project_dir, sqitch = initialized_project
        
        # Add a change
        add_cmd = AddCommand(sqitch)
        add_cmd.execute(['test_table', '-n', 'Add test table'])
        
        # Modify verify script
        verify_file = project_dir / 'verify' / 'test_table.sql'
        verify_file.write_text('SELECT 1/count(*) FROM test_table WHERE FALSE;')
        
        # Show verify script
        show_cmd = ShowCommand(sqitch)
        
        # For integration test, just verify it doesn't crash
        # The actual output testing is done in unit tests
        exit_code = show_cmd.execute(['verify', 'test_table'])
        
        assert exit_code == 0
    
    def test_show_tag_after_tagging(self, initialized_project):
        """Test showing tag information after creating a tag."""
        project_dir, sqitch = initialized_project
        
        # Add a change
        add_cmd = AddCommand(sqitch)
        add_cmd.execute(['initial_schema', '-n', 'Initial database schema'])
        
        # Create a tag
        tag_cmd = TagCommand(sqitch)
        tag_cmd.execute(['v1.0', '-n', 'Version 1.0 release'])
        
        # Show the tag
        show_cmd = ShowCommand(sqitch)
        exit_code = show_cmd.execute(['tag', 'v1.0'])
        
        assert exit_code == 0
    
    def test_show_tag_with_at_prefix(self, initialized_project):
        """Test showing tag with @ prefix."""
        project_dir, sqitch = initialized_project
        
        # Add a change
        add_cmd = AddCommand(sqitch)
        add_cmd.execute(['initial_schema', '-n', 'Initial database schema'])
        
        # Create a tag
        tag_cmd = TagCommand(sqitch)
        tag_cmd.execute(['v1.0', '-n', 'Version 1.0 release'])
        
        # Show the tag with @ prefix
        show_cmd = ShowCommand(sqitch)
        exit_code = show_cmd.execute(['tag', '@v1.0'])
        
        assert exit_code == 0
    
    def test_show_exists_flag(self, initialized_project):
        """Test show command with --exists flag."""
        project_dir, sqitch = initialized_project
        
        # Add a change
        add_cmd = AddCommand(sqitch)
        add_cmd.execute(['test_change', '-n', 'Test change'])
        
        # Test existing change
        show_cmd = ShowCommand(sqitch)
        exit_code = show_cmd.execute(['--exists', 'change', 'test_change'])
        assert exit_code == 0
        
        # Test non-existing change
        exit_code = show_cmd.execute(['--exists', 'change', 'nonexistent'])
        assert exit_code == 1
        
        # Test existing script file
        exit_code = show_cmd.execute(['--exists', 'deploy', 'test_change'])
        assert exit_code == 0
        
        # Test non-existing script file
        exit_code = show_cmd.execute(['--exists', 'deploy', 'nonexistent'])
        assert exit_code == 1
    
    def test_show_change_with_dependencies(self, initialized_project):
        """Test showing change with dependencies."""
        project_dir, sqitch = initialized_project
        
        # Add first change
        add_cmd = AddCommand(sqitch)
        add_cmd.execute(['users_table', '-n', 'Add users table'])
        
        # Add second change with dependency
        add_cmd.execute(['posts_table', '-r', 'users_table', '-n', 'Add posts table'])
        
        # Show the dependent change
        show_cmd = ShowCommand(sqitch)
        exit_code = show_cmd.execute(['change', 'posts_table'])
        
        assert exit_code == 0
    
    def test_show_nonexistent_change(self, initialized_project):
        """Test showing non-existent change."""
        project_dir, sqitch = initialized_project
        
        show_cmd = ShowCommand(sqitch)
        exit_code = show_cmd.execute(['change', 'nonexistent'])
        
        assert exit_code != 0
    
    def test_show_nonexistent_tag(self, initialized_project):
        """Test showing non-existent tag."""
        project_dir, sqitch = initialized_project
        
        show_cmd = ShowCommand(sqitch)
        exit_code = show_cmd.execute(['tag', 'nonexistent'])
        
        assert exit_code != 0
    
    def test_show_nonexistent_script_file(self, initialized_project):
        """Test showing non-existent script file."""
        project_dir, sqitch = initialized_project
        
        # Add a change but remove the deploy file
        add_cmd = AddCommand(sqitch)
        add_cmd.execute(['test_change', '-n', 'Test change'])
        
        # Remove deploy file if it exists
        deploy_file = project_dir / 'deploy' / 'test_change.sql'
        if deploy_file.exists():
            deploy_file.unlink()
        
        # Try to show deploy script
        show_cmd = ShowCommand(sqitch)
        exit_code = show_cmd.execute(['deploy', 'test_change'])
        
        assert exit_code != 0
    
    def test_show_invalid_arguments(self, initialized_project):
        """Test show command with invalid arguments."""
        project_dir, sqitch = initialized_project
        
        show_cmd = ShowCommand(sqitch)
        
        # Missing arguments
        exit_code = show_cmd.execute([])
        assert exit_code == 2
        
        # Invalid object type
        exit_code = show_cmd.execute(['invalid', 'test'])
        assert exit_code != 0
        
        # Too many arguments
        exit_code = show_cmd.execute(['change', 'test', 'extra'])
        assert exit_code != 0