"""
Integration tests for the rebase command.

This module tests the rebase command end-to-end functionality including
CLI integration, database operations, and real workflow scenarios.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from sqlitch.cli import cli
from sqlitch.core.config import Config
from sqlitch.core.sqitch import Sqitch
from sqlitch.commands.rebase import RebaseCommand
from sqlitch.core.plan import Plan
from sqlitch.core.change import Change


def create_mock_engine():
    """Create a mock engine for testing."""
    mock_engine = Mock()
    mock_engine.planned_deployed_common_ancestor_id = Mock(return_value='initial')
    mock_engine.revert = Mock()
    mock_engine.deploy = Mock()
    mock_engine.set_variables = Mock()
    mock_engine.set_verify = Mock()
    mock_engine.set_log_only = Mock()
    mock_engine.set_lock_timeout = Mock()
    return mock_engine


class TestRebaseIntegration:
    """Integration tests for rebase command."""
    
    @pytest.fixture
    def temp_project(self):
        """Create temporary project directory."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def initialized_project(self, temp_project):
        """Create initialized sqlitch project."""
        # Create config file
        config_file = temp_project / "sqitch.conf"
        config_content = """[core]
    engine = pg
    plan_file = sqitch.plan
    
[engine "pg"]
    target = db:pg://test@localhost/test_rebase
    registry = sqitch
    
[user]
    name = Test User
    email = test@example.com
    
[rebase]
    verify = true
    no_prompt = false
"""
        config_file.write_text(config_content)
        
        # Create plan file
        plan_file = temp_project / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=test_rebase
%uri=https://example.com/test_rebase

initial 2023-01-01T10:00:00Z Test User <test@example.com> # Initial schema
users [initial] 2023-01-02T10:00:00Z Test User <test@example.com> # Add users table
@v1.0 2023-01-03T10:00:00Z Test User <test@example.com> # Version 1.0
posts [users] 2023-01-04T10:00:00Z Test User <test@example.com> # Add posts table
comments [posts] 2023-01-05T10:00:00Z Test User <test@example.com> # Add comments table
"""
        plan_file.write_text(plan_content)
        
        # Create directories
        (temp_project / "deploy").mkdir()
        (temp_project / "revert").mkdir()
        (temp_project / "verify").mkdir()
        
        # Create SQL files
        (temp_project / "deploy" / "initial.sql").write_text("CREATE TABLE initial_table (id INT);")
        (temp_project / "deploy" / "users.sql").write_text("CREATE TABLE users (id INT, name TEXT);")
        (temp_project / "deploy" / "posts.sql").write_text("CREATE TABLE posts (id INT, user_id INT, title TEXT);")
        (temp_project / "deploy" / "comments.sql").write_text("CREATE TABLE comments (id INT, post_id INT, content TEXT);")
        
        (temp_project / "revert" / "initial.sql").write_text("DROP TABLE initial_table;")
        (temp_project / "revert" / "users.sql").write_text("DROP TABLE users;")
        (temp_project / "revert" / "posts.sql").write_text("DROP TABLE posts;")
        (temp_project / "revert" / "comments.sql").write_text("DROP TABLE comments;")
        
        (temp_project / "verify" / "initial.sql").write_text("SELECT 1 FROM initial_table LIMIT 1;")
        (temp_project / "verify" / "users.sql").write_text("SELECT 1 FROM users LIMIT 1;")
        (temp_project / "verify" / "posts.sql").write_text("SELECT 1 FROM posts LIMIT 1;")
        (temp_project / "verify" / "comments.sql").write_text("SELECT 1 FROM comments LIMIT 1;")
        
        return temp_project
    
    def test_rebase_command_creation(self, initialized_project):
        """Test creating rebase command instance."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            
            assert command is not None
            assert command.sqitch is sqitch
            assert command.config is config
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_basic_execution(self, initialized_project):
        """Test basic rebase execution."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            # Setup mock engine
            mock_engine = create_mock_engine()
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            command.get_engine = Mock(return_value=mock_engine)
            
            result = command.execute(['--onto-change', 'initial', '--upto-change', 'users'])
            
            assert result == 0
            mock_engine.revert.assert_called_once()
            mock_engine.deploy.assert_called_once()
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_modified_mode(self, initialized_project):
        """Test rebase in modified mode."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            # Setup mock engine
            mock_engine = create_mock_engine()
            mock_engine.planned_deployed_common_ancestor_id = Mock(return_value='users')
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            command.get_engine = Mock(return_value=mock_engine)
            
            result = command.execute(['--modified', '--upto-change', 'posts'])
            
            assert result == 0
            mock_engine.planned_deployed_common_ancestor_id.assert_called_once()
            mock_engine.revert.assert_called_once_with('users', True, True)
            mock_engine.deploy.assert_called_once_with('posts', 'all')
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_with_variables(self, initialized_project):
        """Test rebase with variable substitution."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            # Setup mock engine
            mock_engine = create_mock_engine()
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            command.get_engine = Mock(return_value=mock_engine)
            
            result = command.execute([
                '--onto-change', 'initial',
                '--upto-change', 'users',
                '--set', 'env=test',
                '--set-deploy', 'deploy_var=deploy_value',
                '--set-revert', 'revert_var=revert_value'
            ])
            
            assert result == 0
            
            # Check that variables were set correctly
            assert mock_engine.set_variables.call_count == 2
            
            # First call should be for revert variables
            revert_call_args = mock_engine.set_variables.call_args_list[0][0][0]
            assert 'env' in revert_call_args
            assert 'revert_var' in revert_call_args
            assert revert_call_args['env'] == 'test'
            assert revert_call_args['revert_var'] == 'revert_value'
            
            # Second call should be for deploy variables
            deploy_call_args = mock_engine.set_variables.call_args_list[1][0][0]
            assert 'env' in deploy_call_args
            assert 'deploy_var' in deploy_call_args
            assert deploy_call_args['env'] == 'test'
            assert deploy_call_args['deploy_var'] == 'deploy_value'
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_with_verify(self, initialized_project):
        """Test rebase with verification enabled."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            # Setup mock engine
            mock_engine = create_mock_engine()
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            command.get_engine = Mock(return_value=mock_engine)
            
            result = command.execute(['--onto-change', 'initial', '--upto-change', 'users', '--verify'])
            
            assert result == 0
            mock_engine.set_verify.assert_called_once_with(True)
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_log_only_mode(self, initialized_project):
        """Test rebase in log-only mode."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            # Setup mock engine
            mock_engine = create_mock_engine()
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            command.get_engine = Mock(return_value=mock_engine)
            
            result = command.execute(['--onto-change', 'initial', '--upto-change', 'users', '--log-only'])
            
            assert result == 0
            mock_engine.set_log_only.assert_called_once_with(True)
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_no_prompt_mode(self, initialized_project):
        """Test rebase with no prompt mode."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            # Setup mock engine
            mock_engine = create_mock_engine()
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            command.get_engine = Mock(return_value=mock_engine)
            
            result = command.execute(['--onto-change', 'initial', '--upto-change', 'users', '-y'])
            
            assert result == 0
            mock_engine.revert.assert_called_once_with('initial', False, True)  # no_prompt=False (inverted)
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_different_modes(self, initialized_project):
        """Test rebase with different deployment modes."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            # Setup mock engine
            mock_engine = create_mock_engine()
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            command.get_engine = Mock(return_value=mock_engine)
            
            # Test change mode
            result = command.execute(['--onto-change', 'initial', '--upto-change', 'users', '--mode', 'change'])
            assert result == 0
            mock_engine.deploy.assert_called_with('users', 'change')
            
            # Test tag mode
            mock_engine.reset_mock()
            result = command.execute(['--onto-change', 'initial', '--upto-change', 'v1.0', '--mode', 'tag'])
            assert result == 0
            mock_engine.deploy.assert_called_with('v1.0', 'tag')
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_error_handling(self, initialized_project):
        """Test rebase error handling."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            
            # Test with non-existent plan file
            result = command.execute(['--plan-file', 'nonexistent.plan'])
            assert result == 1  # Error exit code
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_argument_validation(self, initialized_project):
        """Test rebase argument validation."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            
            # Test invalid mode
            result = command.execute(['--mode', 'invalid'])
            assert result == 1  # Error exit code
            
            # Test invalid lock timeout
            result = command.execute(['--lock-timeout', 'invalid'])
            assert result == 1  # Error exit code
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_engine_error_handling(self, initialized_project):
        """Test rebase engine error handling."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            # Setup mock engine that raises errors
            mock_engine = create_mock_engine()
            mock_engine.revert = Mock(side_effect=Exception("Revert failed"))
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            command.get_engine = Mock(return_value=mock_engine)
            
            result = command.execute(['--onto-change', 'initial', '--upto-change', 'users'])
            assert result != 0  # Error exit code
        
        finally:
            os.chdir(old_cwd)
    
    def test_rebase_strict_mode_error(self, initialized_project):
        """Test rebase error in strict mode."""
        import os
        old_cwd = os.getcwd()
        try:
            os.chdir(initialized_project)
            
            # Modify config to enable strict mode
            config_file = initialized_project / "sqitch.conf"
            config_content = config_file.read_text()
            config_content += "\n[rebase]\n    strict = true\n"
            config_file.write_text(config_content)
            
            config = Config()
            sqitch = Sqitch(config=config, options={})
            command = RebaseCommand(sqitch)
            
            result = command.execute(['--onto-change', 'initial'])
            assert result == 1  # Error exit code
        
        finally:
            os.chdir(old_cwd)


class TestRebaseCliIntegration:
    """Test rebase command CLI integration."""
    
    @pytest.fixture
    def temp_project(self):
        """Create temporary project directory."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def cli_project(self, temp_project):
        """Create CLI test project."""
        # Create minimal config and plan
        config_file = temp_project / "sqitch.conf"
        config_content = """[core]
    engine = pg
    plan_file = sqitch.plan
    
[engine "pg"]
    target = db:pg://test@localhost/test_cli
    registry = sqitch
    
[user]
    name = Test User
    email = test@example.com
"""
        config_file.write_text(config_content)
        
        plan_file = temp_project / "sqitch.plan"
        plan_content = """%syntax-version=1.0.0
%project=cli_test

change1 2023-01-01T10:00:00Z Test User <test@example.com> # First change
change2 [change1] 2023-01-02T10:00:00Z Test User <test@example.com> # Second change
"""
        plan_file.write_text(plan_content)
        
        return temp_project
    
    def test_cli_rebase_command(self, cli_project):
        """Test rebase command through CLI."""
        import os
        from click.testing import CliRunner
        
        old_cwd = os.getcwd()
        try:
            os.chdir(cli_project)
            
            # Setup mock engine
            mock_engine = create_mock_engine()
            
            # Mock the get_engine method at the command level
            with patch.object(RebaseCommand, 'get_engine', return_value=mock_engine):
                runner = CliRunner()
                result = runner.invoke(cli, ['rebase', '--onto-change', 'change1', '--upto-change', 'change2'])
                
                if result.exit_code != 0:
                    print(f"CLI output: {result.output}")
                    print(f"Exception: {result.exception}")
                
                assert result.exit_code == 0
                mock_engine.revert.assert_called_once()
                mock_engine.deploy.assert_called_once()
        
        finally:
            os.chdir(old_cwd)
    
    def test_cli_rebase_help(self, cli_project):
        """Test rebase command help."""
        import os
        from click.testing import CliRunner
        
        old_cwd = os.getcwd()
        try:
            os.chdir(cli_project)
            
            runner = CliRunner()
            result = runner.invoke(cli, ['rebase', '--help'])
            
            assert result.exit_code == 0
            assert 'Revert and redeploy database changes' in result.output
            assert '--onto-change' in result.output
            assert '--upto-change' in result.output
            assert '--modified' in result.output
        
        finally:
            os.chdir(old_cwd)
    
    def test_cli_rebase_error(self, cli_project):
        """Test rebase command error through CLI."""
        import os
        from click.testing import CliRunner
        
        old_cwd = os.getcwd()
        try:
            os.chdir(cli_project)
            
            runner = CliRunner()
            result = runner.invoke(cli, ['rebase', '--invalid-option'])
            
            assert result.exit_code != 0
        
        finally:
            os.chdir(old_cwd)