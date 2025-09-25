"""
Add command implementation for sqlitch.

This module implements the 'add' command which adds a new change to the sqlitch plan,
creating the necessary deploy, revert, and verify script files from templates.
"""

import re
import shlex
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from ..core.change import Change, Dependency
from ..core.exceptions import SqlitchError
from ..core.plan import Plan
from ..core.types import validate_change_name
from ..utils.template import TemplateContext, create_template_engine
from .base import BaseCommand


class AddCommand(BaseCommand):
    """Add a new change to sqlitch plans."""

    def execute(self, args: List[str]) -> int:
        """
        Execute the add command.

        Args:
            args: Command arguments

        Returns:
            Exit code (0 for success)
        """
        try:
            # Ensure we're in a sqlitch project
            self.require_initialized()

            # Validate user info
            self.validate_user_info()

            # Parse arguments
            change_name, options = self._parse_args(args)

            if not change_name:
                raise SqlitchError("Change name is required")

            # Validate change name
            validate_change_name(change_name)

            # Get target(s)
            targets = self._get_targets(options)

            # Process each target
            files_created = []
            plans_updated = []

            for target in targets:
                plan = Plan.from_file(target.plan_file)

                # Check if change already exists
                if change_name in plan._change_index:
                    self.warn(f"Change '{change_name}' already exists in {plan.file}")
                    continue

                # Create change
                change = self._create_change(change_name, options)

                # Add to plan
                plan.add_change(change)

                # Create script files
                created_files = self._create_script_files(change, target, options)
                files_created.extend(created_files)

                # Save plan
                plan.save()
                plans_updated.append(plan.file)

                self.logger.emit(
                    f"Added '{change.format_name_with_tags()}' to {plan.file}"
                )

            # Open editor if requested
            if options.get("open_editor", False) and files_created:
                self._open_editor(files_created)

            return 0

        except Exception as e:
            self.error(f"Failed to add change: {e}")
            return 1

    def _parse_args(
        self, args: List[str]
    ) -> Tuple[Optional[str], Dict[str, Any]]:  # noqa: C901
        """
        Parse command arguments.

        Args:
            args: Command arguments

        Returns:
            Tuple of (change_name, options)
        """
        options = {
            "requires": [],
            "conflicts": [],
            "note": [],
            "all": False,
            "template_name": None,
            "template_directory": None,
            "with_scripts": {"deploy": True, "revert": True, "verify": True},
            "variables": {},
            "open_editor": False,
        }

        change_name = None
        i = 0

        while i < len(args):
            arg = args[i]

            if arg in ("-c", "--change", "--change-name"):
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                change_name = args[i + 1]
                i += 2
            elif arg in ("-r", "--requires"):
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options["requires"].append(args[i + 1])
                i += 2
            elif arg in ("-x", "--conflicts"):
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options["conflicts"].append(args[i + 1])
                i += 2
            elif arg in ("-n", "-m", "--note"):
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options["note"].append(args[i + 1])
                i += 2
            elif arg in ("-a", "--all"):
                options["all"] = True
                i += 1
            elif arg in ("-t", "--template", "--template-name"):
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options["template_name"] = args[i + 1]
                i += 2
            elif arg == "--template-directory":
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options["template_directory"] = Path(args[i + 1])
                i += 2
            elif arg == "--with":
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                script_type = args[i + 1]
                if script_type not in ("deploy", "revert", "verify"):
                    raise SqlitchError(f"Invalid script type: {script_type}")
                options["with_scripts"][script_type] = True
                i += 2
            elif arg == "--without":
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                script_type = args[i + 1]
                if script_type not in ("deploy", "revert", "verify"):
                    raise SqlitchError(f"Invalid script type: {script_type}")
                options["with_scripts"][script_type] = False
                i += 2
            elif arg.startswith("--set=") or arg in ("-s", "--set"):
                if arg.startswith("--set="):
                    var_assignment = arg[6:]
                else:
                    if i + 1 >= len(args):
                        raise SqlitchError(f"Option {arg} requires a value")
                    var_assignment = args[i + 1]
                    i += 1

                if "=" not in var_assignment:
                    raise SqlitchError(f"Invalid variable assignment: {var_assignment}")

                key, value = var_assignment.split("=", 1)
                options["variables"][key] = value
                i += 1
            elif arg in ("-e", "--edit", "--open-editor"):
                options["open_editor"] = True
                i += 1
            elif arg.startswith("-"):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                # Positional argument - change name
                if change_name is None:
                    change_name = arg
                else:
                    raise SqlitchError(f"Unexpected argument: {arg}")
                i += 1

        # Get configuration defaults
        config_vars = self.config.get_section("add.variables") or {}
        options["variables"] = {**config_vars, **options["variables"]}

        template_dir = self.config.get("add.template_directory")
        if template_dir and not options["template_directory"]:
            options["template_directory"] = Path(template_dir)

        template_name = self.config.get("add.template_name")
        if template_name and not options["template_name"]:
            options["template_name"] = template_name

        open_editor = self.config.get("add.open_editor", as_bool=True)
        if open_editor and not options["open_editor"]:
            options["open_editor"] = open_editor

        return change_name, options

    def _get_targets(self, options: Dict[str, Any]) -> List[Any]:
        """
        Get list of targets to process.

        Args:
            options: Command options

        Returns:
            List of targets
        """
        if options.get("all", False):
            # Get all targets from configuration
            targets = []
            target_names = self.config.get_section("target") or {}

            if not target_names:
                # Use default target
                targets.append(self.get_target())
            else:
                for target_name in target_names.keys():
                    targets.append(self.get_target(target_name))

            return targets
        else:
            # Use default target
            return [self.get_target()]

    def _create_change(self, name: str, options: Dict[str, Any]) -> Change:
        """
        Create a new Change object.

        Args:
            name: Change name
            options: Command options

        Returns:
            New Change object
        """
        # Parse dependencies
        dependencies = []
        for req in options.get("requires", []):
            dependencies.append(Dependency.from_string(req))

        for conflict in options.get("conflicts", []):
            dep = Dependency.from_string(conflict)
            dep.type = "conflict"
            dependencies.append(dep)

        # Create note
        note_parts = options.get("note", [])
        note = "\n\n".join(note_parts) if note_parts else ""

        # Get user info
        user_name = self.sqitch.user_name
        user_email = self.sqitch.user_email

        if not user_name or not user_email:
            raise SqlitchError("User name and email must be configured")

        return Change(
            name=name,
            note=note,
            timestamp=datetime.now(timezone.utc),
            planner_name=user_name,
            planner_email=user_email,
            dependencies=dependencies,
        )

    def _create_script_files(
        self, change: Change, target: Any, options: Dict[str, Any]
    ) -> List[Path]:
        """
        Create script files for the change.

        Args:
            change: Change object
            target: Target configuration
            options: Command options

        Returns:
            List of created file paths
        """
        created_files = []
        with_scripts = options.get("with_scripts", {})

        # Get template engine
        template_dirs = []
        if options.get("template_directory"):
            template_dirs.append(options["template_directory"])

        template_engine = create_template_engine(template_dirs)

        # Get template name (engine type or custom)
        template_name = options.get("template_name") or target.engine

        # Create template context
        context = TemplateContext(
            project=target.plan.project,
            change=change.name,
            engine=target.engine,
            requires=[
                dep.change for dep in change.dependencies if dep.type == "require"
            ],
            conflicts=[
                dep.change for dep in change.dependencies if dep.type == "conflict"
            ],
        )

        # Add custom variables
        context_dict = context.to_dict()
        context_dict.update(options.get("variables", {}))

        # Create each script type
        for script_type in ["deploy", "revert", "verify"]:
            if not with_scripts.get(script_type, True):
                continue

            # Get file path
            if script_type == "deploy":
                file_path = change.deploy_file(target)
            elif script_type == "revert":
                file_path = change.revert_file(target)
            else:  # verify
                file_path = change.verify_file(target)

            # Skip if file already exists
            if file_path.exists():
                self.logger.emit(f"Skipped {file_path}: already exists")
                continue

            # Create directory if needed
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Get template path
            template_path = f"{script_type}/{template_name}.tmpl"

            try:
                # Render template
                content = template_engine.render_template(template_path, context)

                # Write file
                file_path.write_text(content, encoding="utf-8")

                # Check for double extension warning
                if self._has_double_extension(file_path):
                    ext = file_path.suffix[1:]  # Remove the dot
                    self.warn(f"File {file_path} has a double extension of {ext}")

                self.logger.emit(f"Created {file_path}")
                created_files.append(file_path)

            except Exception as e:
                raise SqlitchError(f"Failed to create {file_path}: {e}")

        return created_files

    def _has_double_extension(self, file_path: Path) -> bool:
        """
        Check if file has a double extension (e.g., .sql.sql).

        Args:
            file_path: File path to check

        Returns:
            True if file has double extension
        """
        name = file_path.name
        match = re.search(r"\.(\w+)\.\1+$", name)
        return match is not None

    def _open_editor(self, files: List[Path]) -> None:
        """
        Open files in editor.

        Args:
            files: List of files to open
        """
        try:
            editor = self.sqitch.editor
            if not editor:
                self.warn("No editor configured")
                return

            # Build command
            file_args = [str(f) for f in files]

            if " " in editor:
                # Editor command has arguments
                cmd_parts = shlex.split(editor)
                cmd = cmd_parts + file_args
            else:
                cmd = [editor] + file_args

            # Execute editor
            subprocess.run(cmd, check=False)

        except Exception as e:
            self.warn(f"Failed to open editor: {e}")


# Click command wrapper for CLI integration
@click.command("add")
@click.argument("change_name", required=False)
@click.option("-c", "--change", "--change-name", help="Change name")
@click.option("-r", "--requires", multiple=True, help="Required change")
@click.option("-x", "--conflicts", multiple=True, help="Conflicting change")
@click.option("-n", "-m", "--note", multiple=True, help="Change note")
@click.option("-a", "--all", is_flag=True, help="Add to all plans")
@click.option("-t", "--template", "--template-name", help="Template name")
@click.option(
    "--template-directory",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Template directory",
)
@click.option(
    "--with",
    "with_scripts",
    multiple=True,
    type=click.Choice(["deploy", "revert", "verify"]),
    help="Include script type",
)
@click.option(
    "--without",
    "without_scripts",
    multiple=True,
    type=click.Choice(["deploy", "revert", "verify"]),
    help="Exclude script type",
)
@click.option(
    "-s", "--set", "variables", multiple=True, help="Set template variable (key=value)"
)
@click.option(
    "-e", "--edit", "--open-editor", is_flag=True, help="Open files in editor"
)
@click.pass_context
def add_command(
    ctx: click.Context, change_name: Optional[str], **kwargs
) -> None:  # noqa: C901
    """Add a new change to sqlitch plans."""
    from ..cli import get_sqitch_from_context

    sqitch = get_sqitch_from_context(ctx)
    command = AddCommand(sqitch)

    # Build arguments list
    args = []

    if change_name:
        args.append(change_name)

    # Handle options
    if kwargs.get("change"):
        args.extend(["--change", kwargs["change"]])

    for req in kwargs.get("requires", []):
        args.extend(["--requires", req])

    for conflict in kwargs.get("conflicts", []):
        args.extend(["--conflicts", conflict])

    for note in kwargs.get("note", []):
        args.extend(["--note", note])

    if kwargs.get("all"):
        args.append("--all")

    if kwargs.get("template"):
        args.extend(["--template", kwargs["template"]])

    if kwargs.get("template_directory"):
        args.extend(["--template-directory", str(kwargs["template_directory"])])

    # Handle with/without scripts
    with_scripts = set(kwargs.get("with_scripts", []))
    without_scripts = set(kwargs.get("without_scripts", []))

    for script_type in with_scripts:
        args.extend(["--with", script_type])

    for script_type in without_scripts:
        args.extend(["--without", script_type])

    # Handle variables
    for var in kwargs.get("variables", []):
        args.extend(["--set", var])

    if kwargs.get("edit"):
        args.append("--open-editor")

    exit_code = command.execute(args)
    if exit_code != 0:
        raise click.ClickException(f"Add command failed with exit code {exit_code}")
