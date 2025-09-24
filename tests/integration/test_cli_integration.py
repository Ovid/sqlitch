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

from sqlitch.cli import cli, main
from sqlitch.core.exceptions import SqlitchError, ConfigurationError


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
        # No commands implemented yet, so no Commands: section
        assert 'Options:' in result.output
    
    def test_global_verbose_option(self):
        """Test global verbose option."""
        # Test that verbose option is parsed correctly
        result = self.runner.invoke(cli, ['-v', 'nonexistent'])
        
        # Should fail with unknown command, but verbose option should be parsed
        assert result.exit_code == 2
        assert 'No such command' in result.output
    
    def test_global_quiet_option(self):
        """Test global quiet option."""
        # Test that quiet option is parsed correctly
        result = self.runner.invoke(cli, ['-q', 'nonexistent'])
        
        # Should fail with unknown command, but quiet option should be parsed
        assert result.exit_code == 2
        assert 'No such command' in result.output
    
    def test_multiple_verbose_options(self):
        """Test multiple verbose options."""
        # Test that multiple verbose options are parsed correctly
        result = self.runner.invoke(cli, ['-vv', 'nonexistent'])
        
        # Should fail with unknown command, but verbose options should be parsed
        assert result.exit_code == 2
        assert 'No such command' in result.output
    
    def test_config_file_option(self):
        """Test config file option."""
        # Create a temporary config file
        config_file = self.temp_path / 'test.conf'
        config_file.write_text('[core]\nengine = pg\n')
        
        # Test that config file option is parsed correctly
        result = self.runner.invoke(cli, ['-c', str(config_file), 'nonexistent'])
        
        # Should fail with unknown command, but config option should be parsed
        assert result.exit_code == 2
        assert 'No such command' in result.output
    
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
        
        # Test that multiple config files are parsed correctly
        result = self.runner.invoke(cli, [
            '-c', str(config1),
            '-c', str(config2),
            'nonexistent'
        ])
        
        # Should fail with unknown command, but config options should be parsed
        assert result.exit_code == 2
        assert 'No such command' in result.output
    
    def test_init_command_available(self):
        """Test that init command will be available when implemented."""
        # This test will pass when init command is implemented in task 8
        # For now, just test that the CLI framework can handle missing commands
        result = self.runner.invoke(cli, ['init', '--help'])
        
        # Should get "No such command" error since init is not implemented yet
        assert result.exit_code == 2
        assert 'No such command' in result.output
    
    def test_init_command_execution(self):
        """Test init command execution framework (will be implemented in task 8)."""
        # This test verifies the CLI framework can handle command execution
        # The actual init command will be implemented in task 8
        
        # Test that unknown commands are handled gracefully
        result = self.runner.invoke(cli, ['init', 'pg'])
        
        assert result.exit_code == 2
        assert 'No such command' in result.output
    
    def test_unknown_command(self):
        """Test unknown command handling."""
        result = self.runner.invoke(cli, ['nonexistent-command'])
        
        assert result.exit_code == 2
        assert 'No such command' in result.output
    
    def test_command_error_handling(self):
        """Test command error handling framework."""
        # Test that the CLI framework handles errors properly
        # This will be more thoroughly tested when commands are implemented
        
        # Test unknown command error handling
        result = self.runner.invoke(cli, ['nonexistent'])
        
        assert result.exit_code == 2
        assert 'No such command' in result.output
    
    def test_keyboard_interrupt_handling(self):
        """Test keyboard interrupt handling in main function."""
        # Test that the main function handles KeyboardInterrupt properly
        with patch('sqlitch.cli.cli') as mock_cli:
            mock_cli.side_effect = KeyboardInterrupt()
            
            exit_code = main()
            
            assert exit_code == 130


class TestMainFunction:
    """Test main function and entry point."""
    
    def test_main_function_success(self):
        """Test main function success case."""
        with patch('sqlitch.cli.cli') as mock_cli:
            mock_cli.return_value = None
            
            exit_code = main()
            
            assert exit_code == 0
            mock_cli.assert_called_once_with(standalone_mode=False)
    
    def test_main_function_sqlitch_error(self):
        """Test main function with SqlitchError."""
        with patch('sqlitch.cli.cli') as mock_cli:
            mock_cli.side_effect = SqlitchError("Test error")
            
            exit_code = main()
            
            assert exit_code == 1
    
    def test_main_function_keyboard_interrupt(self):
        """Test main function with KeyboardInterrupt."""
        with patch('sqlitch.cli.cli') as mock_cli:
            mock_cli.side_effect = KeyboardInterrupt()
            
            with patch('sqlitch.cli.register_commands'):
                exit_code = main()
                
                assert exit_code == 130
    
    def test_main_function_unexpected_error(self):
        """Test main function with unexpected error."""
        with patch('sqlitch.cli.cli') as mock_cli:
            mock_cli.side_effect = RuntimeError("Unexpected error")
            
            with patch('sqlitch.cli.register_commands'):
                exit_code = main()
                
                assert exit_code == 2


class TestCliContext:
    """Test CLI context functionality."""
    
    def test_cli_context_creation(self):
        """Test CLI context creation."""
        from sqlitch.cli import CliContext
        
        ctx = CliContext()
        
        assert ctx.config_files == []
        assert ctx.verbosity == 0
        assert ctx.sqitch is None
    
    def test_cli_context_create_sqitch(self):
        """Test CLI context sqitch creation."""
        from sqlitch.cli import CliContext
        
        ctx = CliContext()
        
        with patch('sqlitch.cli.Config') as mock_config_class:
            mock_config = MagicMock()
            mock_config_class.return_value = mock_config
            
            with patch('sqlitch.cli.Sqitch') as mock_sqitch_class:
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
        from sqlitch.cli import CliContext
        
        ctx = CliContext()
        ctx.config_files = [Path('/test/config.conf')]
        
        with patch('sqlitch.cli.Config') as mock_config_class:
            mock_config = MagicMock()
            mock_config_class.return_value = mock_config
            
            with patch('sqlitch.cli.Sqitch') as mock_sqitch_class:
                mock_sqitch = MagicMock()
                mock_sqitch_class.return_value = mock_sqitch
                
                sqitch = ctx.create_sqitch()
                
                mock_config_class.assert_called_once_with([Path('/test/config.conf')])
    
    def test_cli_context_create_sqitch_error(self):
        """Test CLI context sqitch creation error."""
        from sqlitch.cli import CliContext
        
        ctx = CliContext()
        
        with patch('sqlitch.cli.Config') as mock_config_class:
            mock_config_class.side_effect = Exception("Config error")
            
            with pytest.raises(ConfigurationError) as exc_info:
                ctx.create_sqitch()
            
            assert "Failed to initialize sqlitch" in str(exc_info.value)
    
    def test_cli_context_reuse_sqitch(self):
        """Test CLI context reuses sqitch instance."""
        from sqlitch.cli import CliContext
        
        ctx = CliContext()
        
        with patch('sqlitch.cli.Config') as mock_config_class:
            mock_config = MagicMock()
            mock_config_class.return_value = mock_config
            
            with patch('sqlitch.cli.Sqitch') as mock_sqitch_class:
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
        from sqlitch.cli import register_commands
        
        # Should not raise an exception
        register_commands()
    
    def test_command_wrapper_creation(self):
        """Test command wrapper creation."""
        from sqlitch.cli import create_command_wrapper
        from sqlitch.commands.base import BaseCommand
        
        class TestCommand(BaseCommand):
            def execute(self, args):
                return 0
        
        wrapper = create_command_wrapper(TestCommand)
        
        # Should return a callable
        assert callable(wrapper)