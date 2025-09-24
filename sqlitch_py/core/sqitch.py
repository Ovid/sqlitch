"""
Main Sqitch application class.

This module provides the central Sqitch class that coordinates all operations,
manages configuration, handles user detection, and provides engine factory methods.
"""

import os
import sys
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING

from .config import Config
from .exceptions import SqlitchError, ConfigurationError, EngineError
from .types import EngineType, VerbosityLevel, Target
from ..utils.logging import SqlitchLogger, configure_logging, get_logger

if TYPE_CHECKING:
    from ..engines.base import Engine


@dataclass
class Sqitch:
    """
    Main Sqitch application class.
    
    This class serves as the central coordinator for all sqitch operations,
    managing configuration, user detection, logging, and engine instantiation.
    """
    
    config: Config
    options: Dict[str, Any] = field(default_factory=dict)
    verbosity: VerbosityLevel = field(init=False)
    user_name: Optional[str] = field(init=False)
    user_email: Optional[str] = field(init=False)
    logger: SqlitchLogger = field(init=False)
    
    def __post_init__(self) -> None:
        """Initialize computed fields after dataclass initialization."""
        self.verbosity = self._compute_verbosity()
        self.user_name = self._get_user_name()
        self.user_email = self._get_user_email()
        self.logger = self._setup_logging()
    
    def _compute_verbosity(self) -> VerbosityLevel:
        """
        Compute verbosity level from options and configuration.
        
        Returns:
            Computed verbosity level
        """
        # Start with command-line verbosity if provided
        verbosity = self.options.get('verbosity', 0)
        
        # Clamp to valid range
        return max(-2, min(3, verbosity))
    
    def _get_user_name(self) -> Optional[str]:
        """
        Detect user name from configuration, environment, or system.
        
        Returns:
            User name or None if not found
        """
        # Try configuration first
        user_name = self.config.get_user_name()
        if user_name:
            return user_name
        
        # Try environment variables
        user_name = os.environ.get('SQITCH_USER_NAME')
        if user_name:
            return user_name
        
        user_name = os.environ.get('USER')
        if user_name:
            return user_name
        
        user_name = os.environ.get('USERNAME')  # Windows
        if user_name:
            return user_name
        
        # Try Git configuration
        try:
            result = subprocess.run(
                ['git', 'config', '--get', 'user.name'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                git_name = result.stdout.strip()
                if git_name:
                    return git_name
        except (subprocess.TimeoutExpired, subprocess.SubprocessError, FileNotFoundError):
            pass
        
        # Try system user info
        try:
            import pwd
            return pwd.getpwuid(os.getuid()).pw_gecos.split(',')[0]
        except (ImportError, KeyError, OSError):
            pass
        
        return None
    
    def _get_user_email(self) -> Optional[str]:
        """
        Detect user email from configuration, environment, or system.
        
        Returns:
            User email or None if not found
        """
        # Try configuration first
        user_email = self.config.get_user_email()
        if user_email:
            return user_email
        
        # Try environment variables
        user_email = os.environ.get('SQITCH_USER_EMAIL')
        if user_email:
            return user_email
        
        user_email = os.environ.get('EMAIL')
        if user_email:
            return user_email
        
        # Try Git configuration
        try:
            result = subprocess.run(
                ['git', 'config', '--get', 'user.email'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                git_email = result.stdout.strip()
                if git_email:
                    return git_email
        except (subprocess.TimeoutExpired, subprocess.SubprocessError, FileNotFoundError):
            pass
        
        return None
    
    def _setup_logging(self) -> SqlitchLogger:
        """
        Set up logging configuration.
        
        Returns:
            Configured logger instance
        """
        log_file = self.options.get('log_file')
        log_file_path = Path(log_file) if log_file else None
        
        return configure_logging(
            verbosity=self.verbosity,
            log_file=log_file_path
        )
    
    def engine_for_target(self, target: Target) -> 'Engine':
        """
        Create appropriate engine for target.
        
        Args:
            target: Target configuration
            
        Returns:
            Database engine instance
            
        Raises:
            EngineError: If engine cannot be created
        """
        try:
            engine_type = target.engine_type
        except ValueError as e:
            raise EngineError(f"Unsupported engine type in URI: {target.uri}")
        
        # Import engine classes dynamically to avoid circular imports
        engine_class = self._get_engine_class(engine_type)
        
        if not engine_class:
            raise EngineError(f"Unsupported engine type: {engine_type}")
        
        try:
            return engine_class(self, target)
        except Exception as e:
            raise EngineError(f"Failed to create {engine_type} engine: {e}")
    
    def _get_engine_class(self, engine_type: EngineType) -> Optional[Type['Engine']]:
        """
        Get engine class for the specified engine type.
        
        Args:
            engine_type: Database engine type
            
        Returns:
            Engine class or None if not found
        """
        engine_modules = {
            'pg': 'sqlitch_py.engines.pg',
            'mysql': 'sqlitch_py.engines.mysql',
            'sqlite': 'sqlitch_py.engines.sqlite',
            'oracle': 'sqlitch_py.engines.oracle',
            'snowflake': 'sqlitch_py.engines.snowflake',
            'vertica': 'sqlitch_py.engines.vertica',
            'exasol': 'sqlitch_py.engines.exasol',
            'firebird': 'sqlitch_py.engines.firebird',
            'cockroach': 'sqlitch_py.engines.cockroach',
        }
        
        engine_classes = {
            'pg': 'PostgreSQLEngine',
            'mysql': 'MySQLEngine',
            'sqlite': 'SQLiteEngine',
            'oracle': 'OracleEngine',
            'snowflake': 'SnowflakeEngine',
            'vertica': 'VerticaEngine',
            'exasol': 'ExasolEngine',
            'firebird': 'FirebirdEngine',
            'cockroach': 'CockroachEngine',
        }
        
        module_name = engine_modules.get(engine_type)
        class_name = engine_classes.get(engine_type)
        
        if not module_name or not class_name:
            return None
        
        try:
            import importlib
            module = importlib.import_module(module_name)
            return getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            self.logger.debug(f"Failed to import engine {engine_type}: {e}")
            return None
    
    def get_target(self, target_name: Optional[str] = None) -> Target:
        """
        Get target configuration by name.
        
        Args:
            target_name: Target name (defaults to configured default)
            
        Returns:
            Target configuration
            
        Raises:
            ConfigurationError: If target not found or invalid
        """
        if not target_name:
            # Use default target
            target_name = self.config.get('core.target', 'default')
        
        return self.config.get_target(target_name)
    
    def run_command(self, command_name: str, args: List[str]) -> int:
        """
        Execute a sqitch command.
        
        Args:
            command_name: Name of the command to execute
            args: Command arguments
            
        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        try:
            # Import command class dynamically
            command_class = self._get_command_class(command_name)
            
            if not command_class:
                self.logger.error(f"Unknown command: {command_name}")
                return 1
            
            # Create and execute command
            command = command_class(self)
            return command.execute(args)
            
        except SqlitchError as e:
            self.logger.error(str(e))
            return e.error_code or 1
        except KeyboardInterrupt:
            self.logger.error("Operation cancelled by user")
            return 130  # Standard exit code for SIGINT
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            if self.verbosity >= 2:
                import traceback
                self.logger.debug(traceback.format_exc())
            return 2
    
    def _get_command_class(self, command_name: str) -> Optional[Type]:
        """
        Get command class for the specified command name.
        
        Args:
            command_name: Command name
            
        Returns:
            Command class or None if not found
        """
        command_modules = {
            'init': 'sqlitch_py.commands.init',
            'add': 'sqlitch_py.commands.add',
            'deploy': 'sqlitch_py.commands.deploy',
            'revert': 'sqlitch_py.commands.revert',
            'verify': 'sqlitch_py.commands.verify',
            'status': 'sqlitch_py.commands.status',
            'log': 'sqlitch_py.commands.log',
            'tag': 'sqlitch_py.commands.tag',
            'bundle': 'sqlitch_py.commands.bundle',
            'checkout': 'sqlitch_py.commands.checkout',
            'rebase': 'sqlitch_py.commands.rebase',
            'show': 'sqlitch_py.commands.show',
            'config': 'sqlitch_py.commands.config',
            'engine': 'sqlitch_py.commands.engine',
            'target': 'sqlitch_py.commands.target',
            'plan': 'sqlitch_py.commands.plan',
            'help': 'sqlitch_py.commands.help',
        }
        
        command_classes = {
            'init': 'InitCommand',
            'add': 'AddCommand',
            'deploy': 'DeployCommand',
            'revert': 'RevertCommand',
            'verify': 'VerifyCommand',
            'status': 'StatusCommand',
            'log': 'LogCommand',
            'tag': 'TagCommand',
            'bundle': 'BundleCommand',
            'checkout': 'CheckoutCommand',
            'rebase': 'RebaseCommand',
            'show': 'ShowCommand',
            'config': 'ConfigCommand',
            'engine': 'EngineCommand',
            'target': 'TargetCommand',
            'plan': 'PlanCommand',
            'help': 'HelpCommand',
        }
        
        module_name = command_modules.get(command_name)
        class_name = command_classes.get(command_name)
        
        if not module_name or not class_name:
            return None
        
        try:
            import importlib
            module = importlib.import_module(module_name)
            return getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            self.logger.debug(f"Failed to import command {command_name}: {e}")
            return None
    
    def validate_user_info(self) -> List[str]:
        """
        Validate user name and email configuration.
        
        Returns:
            List of validation issues
        """
        issues = []
        
        if not self.user_name:
            issues.append("User name not configured. Set user.name in configuration or SQITCH_USER_NAME environment variable.")
        
        if not self.user_email:
            issues.append("User email not configured. Set user.email in configuration or SQITCH_USER_EMAIL environment variable.")
        
        return issues
    
    def get_plan_file(self, plan_file: Optional[Path] = None) -> Path:
        """
        Get plan file path.
        
        Args:
            plan_file: Explicit plan file path
            
        Returns:
            Path to plan file
        """
        if plan_file:
            return plan_file
        
        # Get from configuration
        plan_file_config = self.config.get('core.plan_file', 'sqitch.plan')
        
        # Make relative to top directory
        top_dir = Path(self.config.get('core.top_dir', '.'))
        return top_dir / plan_file_config
    
    def get_top_dir(self) -> Path:
        """
        Get project top directory.
        
        Returns:
            Path to project top directory
        """
        return Path(self.config.get('core.top_dir', '.'))
    
    def get_deploy_dir(self) -> Path:
        """
        Get deploy scripts directory.
        
        Returns:
            Path to deploy directory
        """
        top_dir = self.get_top_dir()
        deploy_dir = self.config.get('core.deploy_dir', 'deploy')
        return top_dir / deploy_dir
    
    def get_revert_dir(self) -> Path:
        """
        Get revert scripts directory.
        
        Returns:
            Path to revert directory
        """
        top_dir = self.get_top_dir()
        revert_dir = self.config.get('core.revert_dir', 'revert')
        return top_dir / revert_dir
    
    def get_verify_dir(self) -> Path:
        """
        Get verify scripts directory.
        
        Returns:
            Path to verify directory
        """
        top_dir = self.get_top_dir()
        verify_dir = self.config.get('core.verify_dir', 'verify')
        return top_dir / verify_dir
    
    def set_verbosity(self, verbosity: VerbosityLevel) -> None:
        """
        Update verbosity level.
        
        Args:
            verbosity: New verbosity level
        """
        self.verbosity = max(-2, min(3, verbosity))
        self.logger.set_verbosity(self.verbosity)
    
    def is_initialized(self) -> bool:
        """
        Check if current directory is a sqitch project.
        
        Returns:
            True if initialized, False otherwise
        """
        plan_file = self.get_plan_file()
        return plan_file.exists()
    
    def require_initialized(self) -> None:
        """
        Ensure current directory is a sqitch project.
        
        Raises:
            SqlitchError: If not initialized
        """
        if not self.is_initialized():
            raise SqlitchError(
                "Not a sqitch project. Run 'sqitch init' to initialize one."
            )
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"Sqitch(verbosity={self.verbosity}, "
            f"user_name={self.user_name!r}, "
            f"user_email={self.user_email!r})"
        )


def create_sqitch(config_files: Optional[List[Path]] = None,
                 cli_options: Optional[Dict[str, Any]] = None) -> Sqitch:
    """
    Create Sqitch instance with configuration.
    
    Args:
        config_files: Optional list of configuration files
        cli_options: Optional command-line options
        
    Returns:
        Configured Sqitch instance
        
    Raises:
        ConfigurationError: If configuration is invalid
    """
    try:
        config = Config(config_files, cli_options)
        return Sqitch(config=config, options=cli_options or {})
    except Exception as e:
        raise ConfigurationError(f"Failed to create sqitch instance: {e}")