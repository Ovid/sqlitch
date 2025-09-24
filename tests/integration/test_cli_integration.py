"""
Integration tests for CLI framework.

This module tests the Click-based CLI framework including global options,
command discovery, and command execution.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from sqlitch_py.cli import cli, main
from sqlitch_py.core.exceptions import SqlitchError, ConfigurationError


class TestCliFramework:
    """Test CLI framework functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_cli_help(self):
        """Test CLI help output."""
        result = self.runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        assert 'Sqlitch database change management' in result.output
        assert '--config' in result.output
        assert '--verbose' in result.output
        assert '--quiet' in result.output
        assert '--version' in result.output
    
    def test_cli_version(self):
        """Test CLI version output."""
        result = self.runner.invoke(cli, ['--version'])
        
        assert result.exit_code == 0
        assert '1.0.0' in result.output
    
    def test_cli_no_command_shows_help(self):
        """Test that CLI shows help when no command is provided."""
        result = self.runner.invoke(cli, [])
        
        assert result.exit_code == 0
        assert 'Sqlitch database change management' in result.output
        assert 'Commands:' in result.output
    
    def test_global_verbose_option(self):
        """Test global verbose option."""
        with patch('sqlitch_py.cli.get_sqitch_from_context') as mock_get_sqitch:
            mock_sqitch = MagicMock()
            mock_sqitch.user_name = 'Test User'
            mock_sqitch.user_email = 'test@example.com'
            mock_sqitch.verbosity = 1
            mock_get_sqitch.return_value = mock_sqitch
            
            result = self.runner.invoke(cli, ['-v', 'test-command'])
            
            # Should create sqitch with verbosity = 1
            mock_get_sqitch.assert_called_once()
    
    def test_global_quiet_option(self):
        """Test global quiet option."""
        with patch('sqlitch_py.cli.get_sqitch_from_context') as mock_get_sqitch:
            mock_sqitch = MagicMock()
            mock_sqitch.user_name = 'Test User'
            mock_sqitch.user_email = 'test@example.com'
            mock_sqitch.verbosity = -1
            mock_get_sqitch.return_value = mock_sqitch
            
            result = self.runner.invoke(cli, ['-q', 'test-command'])
            
            # Should create sqitch with verbosity = -1
            mock_get_sqitch.assert_called_once()
    
    def test_multiple_verbose_options(self):
        """Test multiple verbose options."""
        with patch('sqlitch_py.cli.get_sqitch_from_context') as mock_get_sqitch:
            mock_sqitch = MagicMock()
            mock_sqitch.user_name = 'Test User'
            mock_sqitch.user_email = 'test@example.com'
            mock_sqitch.verbosity = 2
            mock_get_sqitch.return_value = mock_sqitch
            
            result = self.runner.invoke(cli, ['-vv', 'test-command'])
            
            mock_get_sqitch.assert_called_once()
    
    def test_config_file_option(self):
        """Test config file option."""
        # Create a temporary config file
        config_file = self.temp_path / 'test.conf'
        config_file.write_text('[core]\nengine = pg\n')
        
        with patch('sqlitch_py.cli.get_sqitch_from_context') as mock_get_sqitch:
            mock_sqitch = MagicMock()
            mock_get_sqitch.return_value = mock_sqitch
            
            result = self.runner.invoke(cli, ['-c', str(config_file), 'test-command'])
            
            mock_get_sqitch.assert_called_once()
    
    def test_invalid_config_file(self):
        """Test invalid config file path."""
        result = self.runner.invoke(cli, ['-c', '/nonexistent/config.conf', 'test-command'])
        
        assert result.exit_code == 2
        assert 'Configuration file does not exist' in result.output
    
    def test_config_file_not_file(self):
        """Test config file path that is not a file."""
        result = self.runner.invoke(cli, ['-c', str(self.temp_path), 'test-command'])
        
        assert result.exit_code == 2
        assert 'Configuration path is not a file' in result.output
    
    def test_multiple_config_files(self):
        """Test multiple config files."""
        # Create temporary config files
        config1 = self.temp_path / 'config1.conf'
        config2 = self.temp_path / 'config2.conf'
        config1.write_text('[core]\nengine = pg\n')
        config2.write_text('[user]\nname = Test User\n')
        
        with patch('sqlitch_py.cli.get_sqitch_from_context') as mock_get_sqitch:
            mock_sqitch = MagicMock()
            mock_get_sqitch.return_value = mock_sqitch
            
            result = self.runner.invoke(cli, [
                '-c', str(config1),
                '-c', str(config2),
                'test-command'
            ])
            
            mock_get_sqitch.assert_called_once()
    
    def test_init_command_available(self):
        """Test that init command is available."""
        # Test that the init command function exists and can be imported
        from sqlitch_py.commands.init import init_command
        
        # Test that it's a Click command
        assert hasattr(init_command, 'callback')
        
        # Test help directly
        result = self.runner.invoke(init_command, ['--help'])
        assert result.exit_code == 0
        assert 'Initialize a sqlitch project' in result.output
    
    def test_init_command_execution(self):
        """Test init command execution."""
        from sqlitch_py.commands.init import init_command
        
        with patch('sqlitch_py.commands.init.InitCommand') as mock_command_class:
            mock_command = MagicMock()
            mock_command.execute.return_value = 0
            mock_command_class.return_value = mock_command
            
            with patch('sqlitch_py.cli.get_sqitch_from_context') as mock_get_sqitch:
                mock_sqitch = MagicMock()
                mock_get_sqitch.return_value = mock_sqitch
                
                result = self.runner.invoke(init_command, ['pg'])
                
                assert result.exit_code == 0
                mock_command_class.assert_called_once_with(mock_sqitch)
                mock_command.execute.assert_called_once()
    
    def test_unknown_command(self):
        """Test unknown command handling."""
        result = self.runner.invoke(cli, ['nonexistent-command'])
        
        assert result.exit_code == 2
        assert 'No such command' in result.output
    
    def test_command_error_handling(self):
        """Test command error handling."""
        from sqlitch_py.commands.init import init_command
        
        with patch('sqlitch_py.commands.init.InitCommand') as mock_command_class:
            mock_command = MagicMock()
            mock_command.execute.side_effect = SqlitchError("Test error")
            mock_command_class.return_value = mock_command
            
            with patch('sqlitch_py.cli.get_sqitch_from_context') as mock_get_sqitch:
                mock_sqitch = MagicMock()
                mock_get_sqitch.return_value = mock_sqitch
                
                result = self.runner.invoke(init_command, [])
                
                assert result.exit_code == 1
                assert 'Test error' in result.output
    
    def test_keyboard_interrupt_handling(self):
        """Test keyboard interrupt handling."""
        from sqlitch_py.commands.init import init_command
        
        with patch('sqlitch_py.commands.init.InitCommand') as mock_command_class:
            mock_command = MagicMock()
            mock_command.execute.side_effect = KeyboardInterrupt()
            mock_command_class.return_value = mock_command
            
            with patch('sqlitch_py.cli.get_sqitch_from_context') as mock_get_sqitch:
                mock_sqitch = MagicMock()
                mock_get_sqitch.return_value = mock_sqitch
                
                result = self.runner.invoke(init_command, [])
                
                assert result.exit_code == 1
                assert 'Aborted' in result.output


class TestMainFunction:
    """Test main function and entry point."""
    
    def test_main_function_success(self):
        """Test main function success case."""
        with patch('sqlitch_py.cli.cli') as mock_cli:
            mock_cli.return_value = None
            
            exit_code = main()
            
            assert exit_code == 0
            mock_cli.assert_called_once_with(standalone_mode=False)
    
    def test_main_function_sqlitch_error(self):
        """Test main function with SqlitchError."""
        with patch('sqlitch_py.cli.cli') as mock_cli:
            mock_cli.side_effect = SqlitchError("Test error")
            
            exit_code = main()
            
            assert exit_code == 1
    
    def test_main_function_keyboard_interrupt(self):
        """Test main function with KeyboardInterrupt."""
        with patch('sqlitch_py.cli.cli') as mock_cli:
            mock_cli.side_effect = KeyboardInterrupt()
            
            with patch('sqlitch_py.cli.register_commands'):
                exit_code = main()
                
                assert exit_code == 130
    
    def test_main_function_unexpected_error(self):
        """Test main function with unexpected error."""
        with patch('sqlitch_py.cli.cli') as mock_cli:
            mock_cli.side_effect = RuntimeError("Unexpected error")
            
            with patch('sqlitch_py.cli.register_commands'):
                exit_code = main()
                
                assert exit_code == 2


class TestCliContext:
    """Test CLI context functionality."""
    
    def test_cli_context_creation(self):
        """Test CLI context creation."""
        from sqlitch_py.cli import CliContext
        
        ctx = CliContext()
        
        assert ctx.config_files == []
        assert ctx.verbosity == 0
        assert ctx.sqitch is None
    
    def test_cli_context_create_sqitch(self):
        """Test CLI context sqitch creation."""
        from sqlitch_py.cli import CliContext
        
        ctx = CliContext()
        
        with patch('sqlitch_py.cli.Config') as mock_config_class:
            mock_config = MagicMock()
            mock_config_class.return_value = mock_config
            
            with patch('sqlitch_py.cli.Sqitch') as mock_sqitch_class:
                mock_sqitch = MagicMock()
                mock_sqitch_class.return_value = mock_sqitch
                
                sqitch = ctx.create_sqitch()
                
                assert sqitch is mock_sqitch
                assert ctx.sqitch is mock_sqitch
                mock_config_class.assert_called_once_with(None)
                mock_sqitch_class.assert_called_once_with(
                    config=mock_config,
                    options={'verbosity': 0}
                )
    
    def test_cli_context_create_sqitch_with_config_files(self):
        """Test CLI context sqitch creation with config files."""
        from sqlitch_py.cli import CliContext
        
        ctx = CliContext()
        ctx.config_files = [Path('/test/config.conf')]
        
        with patch('sqlitch_py.cli.Config') as mock_config_class:
            mock_config = MagicMock()
            mock_config_class.return_value = mock_config
            
            with patch('sqlitch_py.cli.Sqitch') as mock_sqitch_class:
                mock_sqitch = MagicMock()
                mock_sqitch_class.return_value = mock_sqitch
                
                sqitch = ctx.create_sqitch()
                
                mock_config_class.assert_called_once_with([Path('/test/config.conf')])
    
    def test_cli_context_create_sqitch_error(self):
        """Test CLI context sqitch creation error."""
        from sqlitch_py.cli import CliContext
        
        ctx = CliContext()
        
        with patch('sqlitch_py.cli.Config') as mock_config_class:
            mock_config_class.side_effect = Exception("Config error")
            
            with pytest.raises(ConfigurationError) as exc_info:
                ctx.create_sqitch()
            
            assert "Failed to initialize sqlitch" in str(exc_info.value)
    
    def test_cli_context_reuse_sqitch(self):
        """Test CLI context reuses sqitch instance."""
        from sqlitch_py.cli import CliContext
        
        ctx = CliContext()
        
        with patch('sqlitch_py.cli.Config') as mock_config_class:
            mock_config = MagicMock()
            mock_config_class.return_value = mock_config
            
            with patch('sqlitch_py.cli.Sqitch') as mock_sqitch_class:
                mock_sqitch = MagicMock()
                mock_sqitch_class.return_value = mock_sqitch
                
                sqitch1 = ctx.create_sqitch()
                sqitch2 = ctx.create_sqitch()
                
                assert sqitch1 is sqitch2
                mock_sqitch_class.assert_called_once()


class TestCommandRegistration:
    """Test command registration functionality."""
    
    def test_register_commands_function_exists(self):
        """Test that register_commands function exists."""
        from sqlitch_py.cli import register_commands
        
        # Should not raise an exception
        register_commands()
    
    def test_command_wrapper_creation(self):
        """Test command wrapper creation."""
        from sqlitch_py.cli import create_command_wrapper
        from sqlitch_py.commands.base import BaseCommand
        
        class TestCommand(BaseCommand):
            def execute(self, args):
                return 0
        
        wrapper = create_command_wrapper(TestCommand)
        
        # Should return a callable
        assert callable(wrapper)