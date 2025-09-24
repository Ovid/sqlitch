"""
Main CLI entry point for sqlitch.

This module provides the Click-based command-line interface for sqlitch,
including global options, command discovery, and command execution.
"""

import sys
from pathlib import Path
from typing import List, Optional, Tuple

import click

from .core.config import Config
from .core.sqitch import Sqitch
from .core.exceptions import SqlitchError, ConfigurationError


# Global context for passing data between commands
class CliContext:
    """Context object for CLI commands."""
    
    def __init__(self):
        self.config_files: List[Path] = []
        self.verbosity: int = 0
        self.sqitch: Optional[Sqitch] = None
    
    def create_sqitch(self) -> Sqitch:
        """Create Sqitch instance if not already created."""
        if self.sqitch is None:
            try:
                config = Config(self.config_files if self.config_files else None)
                options = {'verbosity': self.verbosity}
                self.sqitch = Sqitch(config=config, options=options)
            except Exception as e:
                raise ConfigurationError(f"Failed to initialize sqlitch: {e}")
        return self.sqitch


def validate_config_file(ctx, param, value):
    """Validate config file paths."""
    if not value:
        return []
    
    config_files = []
    for path_str in value:
        path = Path(path_str)
        if not path.exists():
            raise click.BadParameter(f"Configuration file does not exist: {path}")
        if not path.is_file():
            raise click.BadParameter(f"Configuration path is not a file: {path}")
        config_files.append(path)
    
    return config_files


@click.group(invoke_without_command=True)
@click.option(
    '--config', '-c',
    multiple=True,
    callback=validate_config_file,
    help='Configuration file to read (can be used multiple times)'
)
@click.option(
    '--verbose', '-v',
    count=True,
    help='Increase verbosity (can be used multiple times)'
)
@click.option(
    '--quiet', '-q',
    count=True,
    help='Decrease verbosity (can be used multiple times)'
)
@click.version_option(version='1.0.0', prog_name='sqlitch')
@click.pass_context
def cli(ctx: click.Context, config: Tuple[Path, ...], verbose: int, quiet: int) -> None:
    """
    Sqlitch database change management.
    
    Sqlitch is a database change management application. It provides a
    consistent way to deploy and revert database schema changes.
    """
    # Create context object
    cli_ctx = CliContext()
    cli_ctx.config_files = list(config)
    cli_ctx.verbosity = verbose - quiet
    
    # Store in Click context
    ctx.ensure_object(dict)
    ctx.obj = cli_ctx
    
    # If no command specified, show help
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# Register commands immediately
try:
    from .commands.init import init_command
    cli.add_command(init_command, name='init')
except ImportError:
    pass  # Command not available

try:
    from .commands.deploy import deploy_command
    cli.add_command(deploy_command, name='deploy')
except ImportError:
    pass  # Command not available

try:
    from .commands.revert import revert_command
    cli.add_command(revert_command, name='revert')
except ImportError:
    pass  # Command not available

try:
    from .commands.verify import verify_command
    cli.add_command(verify_command, name='verify')
except ImportError:
    pass  # Command not available

try:
    from .commands.status import status_command
    cli.add_command(status_command, name='status')
except ImportError:
    pass  # Command not available


# Commands will be registered when CLI is invoked


# Error handling
def handle_sqlitch_error(e: SqlitchError) -> int:
    """Handle SqlitchError and return appropriate exit code."""
    click.echo(f"sqlitch: {e}", err=True)
    error_code = getattr(e, 'error_code', None)
    return error_code if error_code is not None else 1


def handle_keyboard_interrupt() -> int:
    """Handle KeyboardInterrupt."""
    click.echo("\nsqlitch: Operation cancelled by user", err=True)
    return 130


def handle_unexpected_error(e: Exception) -> int:
    """Handle unexpected errors."""
    click.echo(f"sqlitch: Unexpected error: {e}", err=True)
    return 2


# Main entry point
def main() -> int:
    """Main entry point for the CLI."""
    try:
        # Run CLI (commands are already registered at module level)
        cli(standalone_mode=False)
        return 0
        
    except SqlitchError as e:
        return handle_sqlitch_error(e)
    except KeyboardInterrupt:
        return handle_keyboard_interrupt()
    except Exception as e:
        return handle_unexpected_error(e)


if __name__ == '__main__':
    sys.exit(main())


# Helper functions for command implementations
def get_sqitch_from_context(ctx: click.Context) -> Sqitch:
    """Get Sqitch instance from Click context."""
    cli_ctx: CliContext = ctx.obj
    return cli_ctx.create_sqitch()


def create_command_wrapper(command_class):
    """
    Create a Click command wrapper for a BaseCommand subclass.
    
    This function creates a Click command that instantiates the given
    command class and executes it with the provided arguments.
    """
    def wrapper(**kwargs):
        @click.pass_context
        def command_func(ctx: click.Context, **cmd_kwargs):
            try:
                sqitch = get_sqitch_from_context(ctx)
                command = command_class(sqitch)
                
                # Convert Click arguments to list format
                args = []
                for key, value in cmd_kwargs.items():
                    if value is not None:
                        if isinstance(value, bool) and value:
                            args.append(f'--{key.replace("_", "-")}')
                        elif not isinstance(value, bool):
                            args.extend([f'--{key.replace("_", "-")}', str(value)])
                
                exit_code = command.execute(args)
                if exit_code != 0:
                    sys.exit(exit_code)
                    
            except SqlitchError as e:
                sys.exit(handle_sqlitch_error(e))
            except KeyboardInterrupt:
                sys.exit(handle_keyboard_interrupt())
            except Exception as e:
                sys.exit(handle_unexpected_error(e))
        
        return command_func(**kwargs)
    
    return wrapper


# Placeholder commands removed - using dynamic registration