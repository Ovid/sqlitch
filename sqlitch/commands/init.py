"""
Init command implementation for sqlitch.

This module implements the 'init' command which initializes a new sqlitch project,
creating the necessary configuration files, directories, and plan file.
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import click

from ..core.exceptions import ConfigurationError, SqlitchError
from ..core.target import Target
from ..core.types import URI, EngineType, validate_project_name
from ..utils.git import GitRepository, detect_vcs
from ..utils.template import create_template_engine
from .base import BaseCommand


class InitCommand(BaseCommand):
    """Initialize a sqlitch project."""

    def execute(self, args: List[str]) -> int:
        """
        Execute the init command.

        Args:
            args: Command arguments

        Returns:
            Exit code (0 for success)
        """
        try:
            # Parse arguments
            project_name, options = self._parse_args(args)

            # Validate project name
            self._validate_project_name(project_name)

            # Check if already initialized
            if self._is_already_initialized(project_name):
                self.info("Project already initialized")
                return 0

            # Create configuration
            self._write_config(options)

            # Create plan file
            self._write_plan(project_name, options)

            # Create directories
            self._create_directories(options)

            # Initialize VCS if requested
            if options.get("vcs", True):
                self._init_vcs()

            self.logger.info(f"Initialized sqlitch project '{project_name}'")
            return 0

        except SqlitchError as e:
            self.logger.error(str(e))
            return 1
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return 2

    def _parse_args(self, args: List[str]) -> tuple[str, Dict[str, Any]]:
        """
        Parse command arguments.

        Args:
            args: Raw command arguments

        Returns:
            Tuple of (project_name, options)
        """
        options = {
            "engine": None,
            "uri": None,
            "target": None,
            "top_dir": None,
            "plan_file": None,
            "registry": None,
            "client": None,
            "extension": None,
            "deploy_dir": None,
            "revert_dir": None,
            "verify_dir": None,
            "vcs": True,
        }

        project_name = None
        i = 0

        while i < len(args):
            arg = args[i]

            if arg in ["--help", "-h"]:
                self._show_help()
                raise SystemExit(0)
            elif arg == "--engine":
                if i + 1 >= len(args):
                    raise SqlitchError("--engine requires a value")
                options["engine"] = args[i + 1]
                i += 2
            elif arg == "--uri":
                if i + 1 >= len(args):
                    raise SqlitchError("--uri requires a value")
                options["uri"] = args[i + 1]
                i += 2
            elif arg == "--target":
                if i + 1 >= len(args):
                    raise SqlitchError("--target requires a value")
                options["target"] = args[i + 1]
                i += 2
            elif arg == "--top-dir":
                if i + 1 >= len(args):
                    raise SqlitchError("--top-dir requires a value")
                options["top_dir"] = Path(args[i + 1])
                i += 2
            elif arg == "--plan-file":
                if i + 1 >= len(args):
                    raise SqlitchError("--plan-file requires a value")
                options["plan_file"] = Path(args[i + 1])
                i += 2
            elif arg == "--registry":
                if i + 1 >= len(args):
                    raise SqlitchError("--registry requires a value")
                options["registry"] = args[i + 1]
                i += 2
            elif arg == "--client":
                if i + 1 >= len(args):
                    raise SqlitchError("--client requires a value")
                options["client"] = args[i + 1]
                i += 2
            elif arg == "--extension":
                if i + 1 >= len(args):
                    raise SqlitchError("--extension requires a value")
                options["extension"] = args[i + 1]
                i += 2
            elif arg == "--deploy-dir":
                if i + 1 >= len(args):
                    raise SqlitchError("--deploy-dir requires a value")
                options["deploy_dir"] = args[i + 1]
                i += 2
            elif arg == "--revert-dir":
                if i + 1 >= len(args):
                    raise SqlitchError("--revert-dir requires a value")
                options["revert_dir"] = args[i + 1]
                i += 2
            elif arg == "--verify-dir":
                if i + 1 >= len(args):
                    raise SqlitchError("--verify-dir requires a value")
                options["verify_dir"] = args[i + 1]
                i += 2
            elif arg == "--no-vcs":
                options["vcs"] = False
                i += 1
            elif arg.startswith("-"):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                if project_name is None:
                    project_name = arg
                else:
                    raise SqlitchError(f"Unexpected argument: {arg}")
                i += 1

        if project_name is None:
            raise SqlitchError("Project name is required")

        return project_name, options

    def _validate_project_name(self, project_name: str) -> None:
        """
        Validate project name.

        Args:
            project_name: Project name to validate

        Raises:
            SqlitchError: If project name is invalid
        """
        if not project_name:
            raise SqlitchError(
                'Invalid project name "": project names must not be empty'
            )

        # Use the same validation as Perl sqitch
        name_pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9._-]*[a-zA-Z0-9]$|^[a-zA-Z]$")

        if not name_pattern.match(project_name):
            raise SqlitchError(
                f'Invalid project name "{project_name}": project names must not '
                'begin with punctuation, contain "@", ":", "#", "[", "]", or blanks, '
                "or end in punctuation or digits following punctuation"
            )

        # Additional checks for problematic characters
        if any(char in project_name for char in "@:#[] "):
            raise SqlitchError(
                f'Invalid project name "{project_name}": contains prohibited characters'
            )

    def _is_already_initialized(self, project_name: str) -> bool:
        """
        Check if project is already initialized.

        Args:
            project_name: Project name to check

        Returns:
            True if already initialized for the same project

        Raises:
            SqlitchError: If initialized for a different project
        """
        plan_file = self.sqitch.get_plan_file()
        if not plan_file.exists():
            return False

        # Check if it's a valid plan file for this project
        try:
            from ..core.plan import Plan

            existing_plan = Plan.from_file(plan_file)
            if existing_plan.project == project_name:
                return True
            else:
                raise SqlitchError(
                    f'Cannot initialize because project "{existing_plan.project}" '
                    f"already initialized in {plan_file}"
                )
        except Exception as e:
            if "already initialized" in str(e):
                raise
            # If we can't parse it, assume it's invalid
            raise SqlitchError(
                f"Cannot initialize because {plan_file} already exists "
                "and is not a valid plan file"
            )

    def _write_config(self, options: Dict[str, Any]) -> None:
        """
        Write configuration file.

        Args:
            options: Command options
        """
        config_file = Path("sqitch.conf")

        if config_file.exists():
            self.logger.info(f"Configuration file {config_file} already exists")
            return

        # Determine engine
        engine = self._determine_engine(options)

        # Build configuration content
        config_lines = []
        config_lines.append("[core]")

        if engine:
            config_lines.append(f"\tengine = {engine}")
        else:
            config_lines.append("\t# engine = ")

        # Add core properties
        top_dir = options.get("top_dir") or "."
        plan_file = options.get("plan_file") or "sqitch.plan"

        config_lines.append(f"\ttop_dir = {top_dir}")
        config_lines.append(f"\tplan_file = {plan_file}")

        # Add optional core properties
        for prop in ["extension", "deploy_dir", "revert_dir", "verify_dir"]:
            if options.get(prop):
                config_lines.append(f"\t{prop} = {options[prop]}")

        config_lines.append("")

        # Add engine section if engine is specified
        if engine:
            config_lines.append(f'[engine "{engine}"]')

            # Add engine properties
            target_uri = self._determine_target_uri(engine, options)
            if target_uri:
                config_lines.append(f"\ttarget = {target_uri}")
            else:
                config_lines.append("\t# target = ")

            registry = options.get("registry")
            if registry:
                config_lines.append(f"\tregistry = {registry}")
            else:
                config_lines.append("\t# registry = ")

            client = options.get("client")
            if client:
                config_lines.append(f"\tclient = {client}")
            else:
                config_lines.append("\t# client = ")

            config_lines.append("")

        # Write configuration file
        config_content = "\n".join(config_lines)
        config_file.write_text(config_content, encoding="utf-8")

        self.logger.info(f"Created {config_file}")

    def _determine_engine(self, options: Dict[str, Any]) -> Optional[str]:
        """
        Determine database engine from options or configuration.

        Args:
            options: Command options

        Returns:
            Engine name or None
        """
        # Check command-line option
        if options.get("engine"):
            return options["engine"]

        # Check URI
        uri = options.get("uri")
        if uri:
            try:
                target = Target(name="temp", uri=URI(uri))
                return target.engine_type
            except ValueError:
                pass

        # Check existing configuration
        try:
            return self.config.get("core.engine")
        except Exception:
            pass

        return None

    def _determine_target_uri(self, engine: str, options: Dict[str, Any]) -> str:
        """
        Determine target URI for engine.

        Args:
            engine: Database engine
            options: Command options

        Returns:
            Target URI
        """
        # Check explicit URI
        if options.get("uri"):
            return options["uri"]

        # Check target name
        target_name = options.get("target")
        if target_name:
            try:
                target = self.config.get_target(target_name)
                return target.uri
            except Exception:
                pass

        # Check engine configuration
        try:
            engine_config = self.config.get_engine_config(engine)
            target_uri = engine_config.get("target")
            if target_uri:
                return target_uri
        except Exception:
            pass

        # Return default URI for engine
        return f"db:{engine}:"

    def _write_plan(self, project_name: str, options: Dict[str, Any]) -> None:
        """
        Write plan file.

        Args:
            project_name: Project name
            options: Command options
        """
        plan_file = options.get("plan_file")
        if plan_file is None:
            plan_file = Path("sqitch.plan")
        elif isinstance(plan_file, str):
            plan_file = Path(plan_file)

        if plan_file.exists():
            self.logger.info(
                f"Plan file {plan_file} already exists for project '{project_name}'"
            )
            return

        # Create plan file directory if needed
        plan_file.parent.mkdir(parents=True, exist_ok=True)

        # Build plan content
        plan_lines = []
        plan_lines.append("%syntax-version=1.0.0")
        plan_lines.append(f"%project={project_name}")

        # Add URI if available
        uri = options.get("uri")
        if uri:
            plan_lines.append(f"%uri={uri}")

        plan_lines.append("")  # Empty line after pragmas

        # Write plan file
        plan_content = "\n".join(plan_lines)
        plan_file.write_text(plan_content, encoding="utf-8")

        self.logger.info(f"Created {plan_file}")

    def _create_directories(self, options: Dict[str, Any]) -> None:
        """
        Create project directories.

        Args:
            options: Command options
        """
        top_dir_option = options.get("top_dir")
        top_dir = Path(top_dir_option) if top_dir_option is not None else Path(".")

        # Standard directories
        directories = [
            top_dir / (options.get("deploy_dir") or "deploy"),
            top_dir / (options.get("revert_dir") or "revert"),
            top_dir / (options.get("verify_dir") or "verify"),
        ]

        # Create directories
        for directory in directories:
            if not directory.exists():
                directory.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Created {directory}/")

    def _init_vcs(self) -> None:
        """Initialize version control system if not already present."""
        vcs = detect_vcs()

        if not vcs:
            # Initialize Git repository
            try:
                git_repo = GitRepository()
                git_repo.init_repository()
                self.logger.info("Initialized Git repository")

                # Create .gitignore if it doesn't exist
                gitignore_path = Path(".gitignore")
                if not gitignore_path.exists():
                    gitignore_content = self._get_default_gitignore()
                    gitignore_path.write_text(gitignore_content, encoding="utf-8")
                    self.logger.info("Created .gitignore")

            except Exception as e:
                self.logger.warn(f"Failed to initialize Git repository: {e}")
        else:
            self.logger.debug("VCS already initialized")

    def _get_default_gitignore(self) -> str:
        """Get default .gitignore content for sqlitch projects."""
        return """# Sqlitch
*.log
.sqlitch/

# Database dumps
*.sql.gz
*.dump

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Python
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
env/
venv/
.env
.venv

# Temporary files
*.tmp
*.temp
"""

    def _show_help(self) -> None:
        """Show command help."""
        help_text = """Usage: sqlitch init [options] <project>

Initialize a sqlitch project in the current directory.

Arguments:
  <project>              Project name

Options:
  --engine <engine>      Database engine (pg, mysql, sqlite, oracle, etc.)
  --uri <uri>           Database URI
  --target <target>     Target name
  --top-dir <dir>       Top directory for the project (default: .)
  --plan-file <file>    Plan file name (default: sqitch.plan)
  --registry <name>     Registry schema/database name
  --client <client>     Database client command
  --extension <ext>     SQL file extension
  --deploy-dir <dir>    Deploy scripts directory (default: deploy)
  --revert-dir <dir>    Revert scripts directory (default: revert)
  --verify-dir <dir>    Verify scripts directory (default: verify)
  --no-vcs             Don't initialize version control
  -h, --help           Show this help message

Examples:
  sqlitch init myproject
  sqlitch init --engine pg myproject
  sqlitch init --uri db:pg://user@localhost/mydb myproject
"""
        print(help_text)


# Click command wrapper for CLI integration
@click.command("init")
@click.argument("project", required=False)
@click.option("--engine", help="Database engine")
@click.option("--uri", help="Database URI")
@click.option("--target", help="Target name")
@click.option("--top-dir", help="Top directory for the project")
@click.option("--plan-file", help="Plan file name")
@click.option("--registry", help="Registry schema/database name")
@click.option("--client", help="Database client command")
@click.option("--extension", help="SQL file extension")
@click.option("--deploy-dir", help="Deploy scripts directory")
@click.option("--revert-dir", help="Revert scripts directory")
@click.option("--verify-dir", help="Verify scripts directory")
@click.option("--no-vcs", is_flag=True, help="Don't initialize version control")
@click.pass_context
def init_command(ctx: click.Context, project: Optional[str], **kwargs) -> None:
    """Initialize a sqlitch project."""
    from ..cli import get_sqitch_from_context

    sqitch = get_sqitch_from_context(ctx)
    command = InitCommand(sqitch)

    # Build arguments list
    args = []

    if project:
        args.append(project)

    for key, value in kwargs.items():
        if value is not None:
            if isinstance(value, bool) and value:
                args.append(f'--{key.replace("_", "-")}')
            elif not isinstance(value, bool):
                args.extend([f'--{key.replace("_", "-")}', str(value)])

    exit_code = command.execute(args)
    if exit_code != 0:
        raise click.ClickException(f"Init command failed with exit code {exit_code}")
