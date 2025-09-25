"""
Main Sqitch application class.

This module provides the central Sqitch class that coordinates all operations,
manages configuration, handles user detection, and provides engine factory methods.
"""

import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type

from .. import i18n
from ..utils.logging import SqlitchLogger, configure_logging
from .config import Config
from .exceptions import ConfigurationError, EngineError, SqlitchError
from .target import Target
from .types import EngineType, VerbosityLevel

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
        verbosity = self.options.get("verbosity", 0)

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
        user_name = os.environ.get("SQITCH_USER_NAME")
        if user_name:
            return user_name

        user_name = os.environ.get("USER")
        if user_name:
            return user_name

        user_name = os.environ.get("USERNAME")  # Windows
        if user_name:
            return user_name

        # Try Git configuration
        try:
            result = subprocess.run(
                ["git", "config", "--get", "user.name"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                git_name = result.stdout.strip()
                if git_name:
                    return git_name
        except (
            subprocess.TimeoutExpired,
            subprocess.SubprocessError,
            FileNotFoundError,
        ):
            pass

        # Try system user info
        try:
            import pwd

            return pwd.getpwuid(os.getuid()).pw_gecos.split(",")[0]
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
        user_email = os.environ.get("SQITCH_USER_EMAIL")
        if user_email:
            return user_email

        user_email = os.environ.get("EMAIL")
        if user_email:
            return user_email

        # Try Git configuration
        try:
            result = subprocess.run(
                ["git", "config", "--get", "user.email"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                git_email = result.stdout.strip()
                if git_email:
                    return git_email
        except (
            subprocess.TimeoutExpired,
            subprocess.SubprocessError,
            FileNotFoundError,
        ):
            pass

        return None

    def _setup_logging(self) -> SqlitchLogger:
        """
        Set up logging configuration.

        Returns:
            Configured logger instance
        """
        return configure_logging(self.verbosity)

    def info(self, message: str) -> None:
        """Send informational message to stdout if verbosity >= 1."""
        if self.verbosity >= 1:
            self.emit(message)

    def debug(self, message: str) -> None:
        """Send debug message to stderr if verbosity >= 2."""
        if self.verbosity >= 2:
            self.vent(f"debug: {message}")

    def trace(self, message: str) -> None:
        """Send trace message to stderr if verbosity >= 3."""
        if self.verbosity >= 3:
            self.vent(f"trace: {message}")

    def comment(self, message: str) -> None:
        """Send comment to stdout (always shown)."""
        lines = message.split("\n")
        for line in lines:
            print(f"# {line}")

    def emit(self, message: str) -> None:
        """Send message to stdout."""
        print(message)

    def vent(self, message: str) -> None:
        """Send message to stderr."""
        print(message, file=sys.stderr)

    def warn(self, message: str) -> None:
        """Send warning message to stderr."""
        lines = message.split("\n")
        for line in lines:
            self.vent(f"warning: {line}")

    def ask_yes_no(self, message: str, default: Optional[bool] = None) -> bool:
        """
        Prompt user for yes/no confirmation.

        Args:
            message: Confirmation message
            default: Default response (True for yes, False for no, None for no default)

        Returns:
            True if user confirms, False otherwise

        Raises:
            IOError: If running unattended with no default
        """
        from ..utils.progress import confirm_action

        return confirm_action(message, default)

    def prompt(self, message: str, default: Optional[str] = None) -> str:
        """
        Prompt user for input.

        Args:
            message: Prompt message
            default: Default value

        Returns:
            User input or default value

        Raises:
            IOError: If running unattended with no default
        """
        from ..utils.progress import prompt_for_input

        return prompt_for_input(message, default)

    def is_interactive(self) -> bool:
        """Check if running in interactive mode."""
        return sys.stdin.isatty() and (
            sys.stdout.isatty()
            or not (hasattr(sys.stdout, "mode") and "w" in sys.stdout.mode)
        )

    def is_unattended(self) -> bool:
        """Check if running unattended."""
        return not self.is_interactive() and sys.stdin.isatty()

    def validate_user_info(self) -> List[str]:
        """
        Validate user name and email configuration.

        Returns:
            List of validation error messages
        """
        issues = []

        if not self.user_name:
            issues.append(
                getattr(i18n, "__")(
                    'Cannot find your name; run sqlitch config --user user.name "YOUR NAME"'
                )
            )

        if not self.user_email:
            issues.append(
                getattr(i18n, "__")(
                    "Cannot infer your email address; run sqlitch config --user user.email you@host.com"
                )
            )

        return issues

    def require_initialized(self) -> None:
        """
        Ensure current directory is a sqitch project.

        Raises:
            SqlitchError: If not initialized
        """
        if not self.is_initialized():
            from .exceptions import hurl

            hurl(
                "init",
                'No project configuration found. Run the "init" command to initialize a project',
            )

    def get_target(self, target_name: Optional[str] = None) -> "Target":
        """
        Get target configuration.

        Args:
            target_name: Target name (defaults to configured default)

        Returns:
            Target configuration
        """
        if not target_name:
            target_name = self.config.get("core.target")

        if not target_name:
            # Use default target based on engine
            engine = self.config.get("core.engine")
            if not engine:
                from .exceptions import hurl

                hurl(
                    "config",
                    getattr(i18n, "__")(
                        "No engine specified; specify via target or core.engine"
                    ),
                )
            target_name = engine

        return Target.from_config(self.config, target_name)

    def engine_for_target(self, target: Target) -> "Engine":
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
        except ValueError:
            raise EngineError(
                getattr(i18n, "__x")(
                    "Unsupported engine type in URI: {uri}", uri=target.uri
                )
            )

        # Import engine classes dynamically to avoid circular imports
        engine_class = self._get_engine_class(engine_type)

        if not engine_class:
            raise EngineError(
                getattr(i18n, "__x")("Unknown engine: {engine}", engine=engine_type)
            )

        try:
            # Get the plan for this target
            plan = target.plan
            return engine_class(target, plan)
        except Exception as e:
            raise EngineError(
                getattr(i18n, "__x")(
                    "Failed to create {engine} engine: {error}",
                    engine=engine_type,
                    error=str(e),
                )
            )

    def _get_engine_class(self, engine_type: EngineType) -> Optional[Type["Engine"]]:
        """
        Get engine class for the specified engine type.

        Args:
            engine_type: Database engine type

        Returns:
            Engine class or None if not found
        """
        engine_modules = {
            "pg": "sqlitch.engines.pg",
            "mysql": "sqlitch.engines.mysql",
            "sqlite": "sqlitch.engines.sqlite",
            "oracle": "sqlitch.engines.oracle",
            "snowflake": "sqlitch.engines.snowflake",
            "vertica": "sqlitch.engines.vertica",
            "exasol": "sqlitch.engines.exasol",
            "firebird": "sqlitch.engines.firebird",
            "cockroach": "sqlitch.engines.cockroach",
        }

        engine_classes = {
            "pg": "PostgreSQLEngine",
            "mysql": "MySQLEngine",
            "sqlite": "SQLiteEngine",
            "oracle": "OracleEngine",
            "snowflake": "SnowflakeEngine",
            "vertica": "VerticaEngine",
            "exasol": "ExasolEngine",
            "firebird": "FirebirdEngine",
            "cockroach": "CockroachEngine",
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
                self.logger.error(
                    getattr(i18n, "__x")(
                        '"{command}" is not a valid command', command=command_name
                    )
                )
                return 1

            # Create and execute command
            command = command_class(self)
            return command.execute(args)

        except SqlitchError as e:
            self.logger.error(str(e))
            return e.exitval or 1
        except KeyboardInterrupt:
            self.logger.error(getattr(i18n, "__")("Operation cancelled by user"))
            return 130  # Standard exit code for SIGINT
        except Exception as e:
            self.logger.error(
                getattr(i18n, "__x")("Unexpected error: {error}", error=str(e))
            )
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
            "init": "sqlitch.commands.init",
            "add": "sqlitch.commands.add",
            "deploy": "sqlitch.commands.deploy",
            "revert": "sqlitch.commands.revert",
            "verify": "sqlitch.commands.verify",
            "status": "sqlitch.commands.status",
            "log": "sqlitch.commands.log",
            "tag": "sqlitch.commands.tag",
            "bundle": "sqlitch.commands.bundle",
            "checkout": "sqlitch.commands.checkout",
            "rebase": "sqlitch.commands.rebase",
            "show": "sqlitch.commands.show",
            "config": "sqlitch.commands.config",
            "engine": "sqlitch.commands.engine",
            "target": "sqlitch.commands.target",
            "plan": "sqlitch.commands.plan",
            "help": "sqlitch.commands.help",
        }

        command_classes = {
            "init": "InitCommand",
            "add": "AddCommand",
            "deploy": "DeployCommand",
            "revert": "RevertCommand",
            "verify": "VerifyCommand",
            "status": "StatusCommand",
            "log": "LogCommand",
            "tag": "TagCommand",
            "bundle": "BundleCommand",
            "checkout": "CheckoutCommand",
            "rebase": "RebaseCommand",
            "show": "ShowCommand",
            "config": "ConfigCommand",
            "engine": "EngineCommand",
            "target": "TargetCommand",
            "plan": "PlanCommand",
            "help": "HelpCommand",
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
        plan_file_config = self.config.get("core.plan_file", "sqitch.plan")

        # Make relative to top directory
        top_dir = Path(self.config.get("core.top_dir", "."))
        return top_dir / plan_file_config

    def get_top_dir(self) -> Path:
        """
        Get project top directory.

        Returns:
            Path to project top directory
        """
        return Path(self.config.get("core.top_dir", "."))

    def get_deploy_dir(self) -> Path:
        """
        Get deploy scripts directory.

        Returns:
            Path to deploy directory
        """
        top_dir = self.get_top_dir()
        deploy_dir = self.config.get("core.deploy_dir", "deploy")
        return top_dir / deploy_dir

    def get_revert_dir(self) -> Path:
        """
        Get revert scripts directory.

        Returns:
            Path to revert directory
        """
        top_dir = self.get_top_dir()
        revert_dir = self.config.get("core.revert_dir", "revert")
        return top_dir / revert_dir

    def get_verify_dir(self) -> Path:
        """
        Get verify scripts directory.

        Returns:
            Path to verify directory
        """
        top_dir = self.get_top_dir()
        verify_dir = self.config.get("core.verify_dir", "verify")
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

    @property
    def editor(self) -> Optional[str]:
        """
        Get configured editor command.

        Returns:
            Editor command or None if not configured
        """
        # Check configuration
        editor = self.config.get("core.editor")
        if editor:
            return editor

        # Check environment variables
        for env_var in ["SQITCH_EDITOR", "VISUAL", "EDITOR"]:
            editor = os.environ.get(env_var)
            if editor:
                return editor

        return None

    def request_note_for(self, object_type: str) -> str:
        """
        Request a note from the user for the specified object type.

        Args:
            object_type: Type of object (e.g., 'tag', 'change')

        Returns:
            Note text from user
        """
        import subprocess
        import tempfile

        editor = self.editor
        if not editor:
            # Fall back to simple prompt
            return self.prompt(
                f"{object_type.capitalize()} note (optional): ", default=""
            )

        # Create temporary file for note editing
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".txt", delete=False) as f:
            temp_file = Path(f.name)
            f.write(f"\n# Please enter the note for the {object_type}.\n")
            f.write("# Lines starting with '#' will be ignored.\n")

        try:
            # Open editor
            if " " in editor:
                import shlex

                cmd = shlex.split(editor) + [str(temp_file)]
            else:
                cmd = [editor, str(temp_file)]

            result = subprocess.run(cmd, check=False)
            if result.returncode != 0:
                self.warn(f"Editor exited with code {result.returncode}")

            # Read the note
            content = temp_file.read_text(encoding="utf-8")

            # Filter out comment lines and empty lines
            lines = []
            for line in content.splitlines():
                line = line.rstrip()
                if line and not line.startswith("#"):
                    lines.append(line)

            # Join lines and strip whitespace
            note = "\n".join(lines).strip()
            return note

        except Exception as e:
            self.warn(f"Failed to open editor: {e}")
            return self.prompt(
                f"{object_type.capitalize()} note (optional): ", default=""
            )
        finally:
            # Clean up temporary file
            try:
                temp_file.unlink()
            except OSError:
                pass

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"Sqitch(verbosity={self.verbosity}, "
            f"user_name={self.user_name!r}, "
            f"user_email={self.user_email!r})"
        )


def create_sqitch(
    config_files: Optional[List[Path]] = None,
    cli_options: Optional[Dict[str, Any]] = None,
) -> Sqitch:
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
        raise ConfigurationError(f"Failed to create sqitch instance: {e}")
