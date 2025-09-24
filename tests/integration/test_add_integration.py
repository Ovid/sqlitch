"""Integration tests for the add command."""

import os
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch

from sqlitch.cli import cli
from sqlitch.core.config import Config
from sqlitch.core.plan import Plan
from sqlitch.core.sqitch import Sqitch
from click.testing import CliRunner


@pytest.fixture
def temp_project():
    """Create a temporary sqlitch project."""
    temp_dir = Path(tempfile.mkdtemp())
    
    try:
        # Create project structure
        (temp_dir / 'deploy').mkdir()
        (temp_dir / 'revert').mkdir()
        (temp_dir / 'verify').mkdir()
        
        # Create config file
        config_content = """[core]
    engine = pg
    top_dir = .
    plan_file = sqitch.plan

[engine "pg"]
    target = db:pg://test@localhost/test
    registry = sqitch

[user]
    name = Test User
    email = test@example.com
"""
        (temp_dir / 'sqitch.conf').write_text(config_content)
        
        # Create plan file
        plan_content = """%syntax-version=1.0.0
%project=test_project
%uri=https://github.com/example/test_project

"""
        (temp_dir / 'sqitch.plan').write_text(plan_content)
        
        yield temp_dir
        
    finally:
        shutil.rmtree(temp_dir)


class TestAddIntegration:
    """Integration tests for add command."""
    
    def test_add_basic_change(self, temp_project):
        """Test adding a basic change."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            # Copy project to isolated filesystem
            shutil.copytree(temp_project, 'project')
            
            # Change to project directory
            import os
            os.chdir('project')
            
            # Run add command
            result = runner.invoke(cli, ['add', 'users'])
            
            if result.exit_code != 0:
                print(f"Error output: {result.output}")
                print(f"Exception: {result.exception}")
            assert result.exit_code == 0
            assert 'Added' in result.output
            
            # Check plan file was updated
            plan = Plan.from_file(Path('sqitch.plan'))
            assert 'users' in plan._change_index
            
            # Check script files were created
            assert (Path('deploy') / 'users.sql').exists()
            assert (Path('revert') / 'users.sql').exists()
            assert (Path('verify') / 'users.sql').exists()
            
            # Check script content
            deploy_content = (Path('deploy') / 'users.sql').read_text()
            assert 'Deploy test_project:users to pg' in deploy_content
            assert 'XXX Add DDLs here' in deploy_content
    
    def test_add_change_with_dependencies(self, temp_project):
        """Test adding a change with dependencies."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # First add a base change
            result = runner.invoke(cli, ['add', 'users'])
            assert result.exit_code == 0
            
            # Add change with dependency
            result = runner.invoke(cli, [
                'add', 'posts',
                '--requires', 'users',
                '--note', 'Add posts table'
            ])
            
            if result.exit_code != 0:
                print(f"Error output: {result.output}")
                print(f"Exception: {result.exception}")
            assert result.exit_code == 0
            
            # Check plan file
            plan = Plan.from_file(Path('sqitch.plan'))
            posts_change = plan._change_index['posts']
            
            assert posts_change.note == 'Add posts table'
            assert len(posts_change.dependencies) == 1
            assert posts_change.dependencies[0].change == 'users'
            assert posts_change.dependencies[0].type == 'require'
            
            # Check script content includes dependency
            deploy_content = (Path('deploy') / 'posts.sql').read_text()
            assert 'requires: users' in deploy_content
    
    def test_add_change_with_conflicts(self, temp_project):
        """Test adding a change with conflicts."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # Add change with conflict
            result = runner.invoke(cli, [
                'add', 'new_feature',
                '--conflicts', 'old_feature'
            ])
            
            assert result.exit_code == 0
            
            # Check plan file
            plan = Plan.from_file(Path('sqitch.plan'))
            change = plan._change_index['new_feature']
            
            assert len(change.dependencies) == 1
            assert change.dependencies[0].change == 'old_feature'
            assert change.dependencies[0].type == 'conflict'
            
            # Check script content includes conflict
            deploy_content = (Path('deploy') / 'new_feature.sql').read_text()
            assert 'conflicts: old_feature' in deploy_content
    
    def test_add_change_with_custom_template(self, temp_project):
        """Test adding a change with custom template."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # Create custom template directory
            template_dir = Path('templates')
            template_dir.mkdir()
            (template_dir / 'deploy').mkdir()
            
            # Create custom template
            custom_template = """-- Custom Deploy {{ project }}:{{ change }} to {{ engine }}
{% for req in requires %}
-- requires: {{ req }}
{% endfor %}

-- Custom template content
BEGIN;

-- TODO: Add your DDL here

COMMIT;
"""
            (template_dir / 'deploy' / 'pg.tmpl').write_text(custom_template)
            
            # Add change with custom template
            result = runner.invoke(cli, [
                'add', 'custom_change',
                '--template-directory', str(template_dir)
            ])
            
            assert result.exit_code == 0
            
            # Check script content uses custom template
            deploy_content = (Path('deploy') / 'custom_change.sql').read_text()
            assert 'Custom Deploy test_project:custom_change to pg' in deploy_content
            assert 'Custom template content' in deploy_content
    
    def test_add_change_without_scripts(self, temp_project):
        """Test adding a change without certain scripts."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # Add change without verify script
            result = runner.invoke(cli, [
                'add', 'no_verify',
                '--without', 'verify'
            ])
            
            assert result.exit_code == 0
            
            # Check only deploy and revert scripts were created
            assert (Path('deploy') / 'no_verify.sql').exists()
            assert (Path('revert') / 'no_verify.sql').exists()
            assert not (Path('verify') / 'no_verify.sql').exists()
    
    def test_add_change_with_variables(self, temp_project):
        """Test adding a change with template variables."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # Add change with variables
            result = runner.invoke(cli, [
                'add', 'with_vars',
                '--set', 'table_name=users',
                '--set', 'schema=public'
            ])
            
            assert result.exit_code == 0
            
            # Variables should be available to templates
            # (This would require custom templates to test properly)
            assert (Path('deploy') / 'with_vars.sql').exists()
    
    def test_add_duplicate_change(self, temp_project):
        """Test adding a duplicate change."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # Add change first time
            result = runner.invoke(cli, ['add', 'users'])
            assert result.exit_code == 0
            
            # Try to add same change again
            result = runner.invoke(cli, ['add', 'users'])
            assert result.exit_code == 0  # Should succeed but warn
            assert 'already exists' in result.output
    
    def test_add_change_invalid_name(self, temp_project):
        """Test adding a change with invalid name."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # Try to add change with invalid name
            result = runner.invoke(cli, ['add', ''])
            assert result.exit_code != 0
    
    def test_add_change_not_initialized(self):
        """Test adding a change in non-initialized directory."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ['add', 'test'])
            assert result.exit_code != 0
            assert 'No project configuration found' in result.output
    
    @patch.dict(os.environ, {}, clear=True)
    @patch('sqlitch.core.sqitch.Sqitch._get_user_name', return_value=None)
    @patch('sqlitch.core.sqitch.Sqitch._get_user_email', return_value=None)
    def test_add_change_no_user_config(self, mock_email, mock_name, temp_project):
        """Test adding a change without user configuration."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # Remove user config
            config_content = """[core]
    engine = pg
    top_dir = .
    plan_file = sqitch.plan

[engine "pg"]
    target = db:pg://test@localhost/test
    registry = sqitch
"""
            Path('sqitch.conf').write_text(config_content)
            
            result = runner.invoke(cli, ['add', 'test'])
            assert result.exit_code != 0
            assert 'User name' in result.output or 'email' in result.output
    
    def test_add_change_multiple_notes(self, temp_project):
        """Test adding a change with multiple note lines."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            result = runner.invoke(cli, [
                'add', 'multi_note',
                '--note', 'First line',
                '--note', 'Second line'
            ])
            
            assert result.exit_code == 0
            
            # Check plan file
            plan = Plan.from_file(Path('sqitch.plan'))
            change = plan._change_index['multi_note']
            
            # Note should be stored as single line in plan file format
            assert change.note == 'First line Second line'
    
    def test_add_change_existing_files(self, temp_project):
        """Test adding a change when script files already exist."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # Create existing script file
            (Path('deploy') / 'existing.sql').write_text('-- Existing content')
            
            result = runner.invoke(cli, ['add', 'existing'])
            
            assert result.exit_code == 0
            assert 'Skipped' in result.output
            
            # Check existing file wasn't overwritten
            content = (Path('deploy') / 'existing.sql').read_text()
            assert content == '-- Existing content'
    
    @patch('subprocess.run')
    def test_add_change_with_editor(self, mock_run, temp_project):
        """Test adding a change and opening editor."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # Set editor in environment
            env = os.environ.copy()
            env['EDITOR'] = 'vim'
            
            result = runner.invoke(cli, ['add', 'edit_test', '--edit'], env=env)
            
            assert result.exit_code == 0
            mock_run.assert_called_once()
    
    def test_add_change_help(self):
        """Test add command help."""
        runner = CliRunner()
        
        result = runner.invoke(cli, ['add', '--help'])
        
        assert result.exit_code == 0
        assert 'Add a new change' in result.output
        assert '--requires' in result.output
        assert '--conflicts' in result.output
        assert '--note' in result.output


class TestAddCommandOptions:
    """Test add command option parsing."""
    
    def test_add_short_options(self, temp_project):
        """Test add command with short options."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # First add the dependency
            result = runner.invoke(cli, ['add', 'dep1'])
            assert result.exit_code == 0
            
            # Set editor for -e option
            env = os.environ.copy()
            env['EDITOR'] = 'vim'
            
            result = runner.invoke(cli, [
                'add', 'short_opts',
                '-r', 'dep1',
                '-x', 'conflict1',
                '-n', 'Short note',
                '-a',
                '-t', 'pg',
                '-e'
            ], env=env)
            
            if result.exit_code != 0:
                print(f"Error output: {result.output}")
                print(f"Exception: {result.exception}")
            assert result.exit_code == 0
            
            # Check plan file
            plan = Plan.from_file(Path('sqitch.plan'))
            change = plan._change_index['short_opts']
            
            assert change.note == 'Short note'
            assert len(change.dependencies) == 2
    
    def test_add_change_name_option(self, temp_project):
        """Test specifying change name via option."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            result = runner.invoke(cli, ['add', '--change', 'named_change'])
            
            assert result.exit_code == 0
            
            # Check plan file
            plan = Plan.from_file(Path('sqitch.plan'))
            assert 'named_change' in plan._change_index
    
    def test_add_multiple_dependencies(self, temp_project):
        """Test adding multiple dependencies."""
        runner = CliRunner()
        
        with runner.isolated_filesystem():
            shutil.copytree(temp_project, 'project')
            import os
            os.chdir('project')
            
            # First add the dependencies
            result = runner.invoke(cli, ['add', 'dep1'])
            assert result.exit_code == 0
            result = runner.invoke(cli, ['add', 'dep2'])
            assert result.exit_code == 0
            
            result = runner.invoke(cli, [
                'add', 'multi_deps',
                '--requires', 'dep1',
                '--requires', 'dep2',
                '--conflicts', 'conflict1',
                '--conflicts', 'conflict2'
            ])
            
            assert result.exit_code == 0
            
            # Check plan file
            plan = Plan.from_file(Path('sqitch.plan'))
            change = plan._change_index['multi_deps']
            
            assert len(change.dependencies) == 4
            
            requires = [dep for dep in change.dependencies if dep.type == 'require']
            conflicts = [dep for dep in change.dependencies if dep.type == 'conflict']
            
            assert len(requires) == 2
            assert len(conflicts) == 2
            
            assert requires[0].change == 'dep1'
            assert requires[1].change == 'dep2'
            assert conflicts[0].change == 'conflict1'
            assert conflicts[1].change == 'conflict2'