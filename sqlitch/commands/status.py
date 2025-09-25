"""
Status command implementation for sqlitch.

This module implements the 'status' command which displays the current
deployment state of the target database, including deployed changes,
tags, and comparison with the plan file.
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import click

from ..core.change import Change
from ..core.exceptions import EngineError, PlanError, SqlitchError
from ..core.plan import Plan
from ..core.types import ChangeStatus
from ..engines.base import EngineRegistry
from .base import BaseCommand


class StatusCommand(BaseCommand):
    """Display status information about Sqitch deployment."""

    def execute(self, args: List[str]) -> int:
        """
        Execute the status command.

        Args:
            args: Command arguments

        Returns:
            Exit code (0 for success, 1 for no changes deployed)
        """
        try:
            # Parse arguments
            options = self._parse_args(args)

            # Ensure project is initialized
            self.require_initialized()

            # Get target
            target = self.get_target(options.get("target"))

            # Load plan
            plan = self._load_plan(options.get("plan_file"))

            # Create engine with plan
            engine = EngineRegistry.create_engine(target, plan)

            # Ensure registry exists
            engine.ensure_registry()

            # Get current state
            current_state = self._get_current_state(engine, options.get("project"))

            if not current_state:
                self.error("No changes deployed")
                return 1

            # Display database info
            self.info(f"On database {target.uri}")

            # Display current state
            self._emit_state(current_state, options)

            # Display changes if requested
            if options.get("show_changes"):
                self._emit_changes(engine, options.get("project"), options)

            # Display tags if requested
            if options.get("show_tags"):
                self._emit_tags(engine, options.get("project"), options)

            # Display status comparison with plan
            self._emit_status(current_state, plan, options)

            return 0

        except Exception as e:
            return self.handle_error(e, "status")

    def _parse_args(self, args: List[str]) -> Dict[str, Any]:
        """
        Parse command arguments.

        Args:
            args: Raw command arguments

        Returns:
            Parsed options dictionary
        """
        options = {
            "target": None,
            "plan_file": None,
            "project": None,
            "show_changes": self.config.get("status.show_changes", False),
            "show_tags": self.config.get("status.show_tags", False),
            "date_format": self.config.get("status.date_format", "iso"),
        }

        i = 0
        while i < len(args):
            arg = args[i]

            if arg in ["--help", "-h"]:
                self._show_help()
                raise SystemExit(0)
            elif arg == "--target":
                if i + 1 >= len(args):
                    raise SqlitchError("--target requires a value")
                options["target"] = args[i + 1]
                i += 2
            elif arg == "--plan-file":
                if i + 1 >= len(args):
                    raise SqlitchError("--plan-file requires a value")
                options["plan_file"] = Path(args[i + 1])
                i += 2
            elif arg == "--project":
                if i + 1 >= len(args):
                    raise SqlitchError("--project requires a value")
                options["project"] = args[i + 1]
                i += 2
            elif arg == "--show-changes":
                options["show_changes"] = True
                i += 1
            elif arg == "--show-tags":
                options["show_tags"] = True
                i += 1
            elif arg == "--date-format":
                if i + 1 >= len(args):
                    raise SqlitchError("--date-format requires a value")
                options["date_format"] = args[i + 1]
                i += 2
            elif arg.startswith("-"):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                raise SqlitchError(f"Unexpected argument: {arg}")

        return options

    def _load_plan(self, plan_file: Optional[Path] = None) -> Plan:
        """
        Load plan file.

        Args:
            plan_file: Optional plan file path

        Returns:
            Loaded plan

        Raises:
            PlanError: If plan cannot be loaded
        """
        if plan_file is None:
            plan_file = self.sqitch.get_plan_file()

        if not plan_file.exists():
            raise PlanError(f"Plan file not found: {plan_file}")

        try:
            return Plan.from_file(plan_file)
        except Exception as e:
            raise PlanError(f"Failed to load plan file {plan_file}: {e}")

    def _get_current_state(
        self, engine, project: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get current deployment state from database.

        Args:
            engine: Database engine
            project: Optional project name

        Returns:
            Current state dictionary or None if no changes deployed
        """
        try:
            return engine.get_current_state(project)
        except Exception as e:
            if not engine._registry_exists_in_db(engine._create_connection()):
                raise SqlitchError("Database has not been initialized for Sqitch")
            raise EngineError(f"Failed to get current state: {e}")

    def _emit_state(self, state: Dict[str, Any], options: Dict[str, Any]) -> None:
        """
        Emit current state information.

        Args:
            state: Current state dictionary
            options: Command options
        """
        self.info(f"Project:  {state['project']}")
        self.info(f"Change:   {state['change_id']}")
        self.info(f"Name:     {state['change']}")

        if state.get("tags"):
            tags = state["tags"]
            if isinstance(tags, list) and tags:
                tag_word = "Tag" if len(tags) == 1 else "Tags"
                self.info(f"{tag_word}:     {', '.join(tags)}")

        # Format date according to date_format option
        committed_at = state["committed_at"]
        if isinstance(committed_at, datetime):
            date_format = options.get("date_format", "iso")
            if date_format == "iso":
                date_str = committed_at.isoformat()
            elif date_format == "rfc":
                date_str = committed_at.strftime("%a, %d %b %Y %H:%M:%S %z")
            else:
                # Use strftime format
                date_str = committed_at.strftime(date_format)
        else:
            date_str = str(committed_at)

        self.info(f"Deployed: {date_str}")
        self.info(f"By:       {state['committer_name']} <{state['committer_email']}>")

    def _emit_changes(
        self, engine, project: Optional[str], options: Dict[str, Any]
    ) -> None:
        """
        Emit list of deployed changes.

        Args:
            engine: Database engine
            project: Project name
            options: Command options
        """
        try:
            changes = list(engine.get_current_changes(project))

            if not changes:
                self.info("")
                self.info("Changes: None.")
                return

            self.info("")
            change_word = "Change" if len(changes) == 1 else "Changes"
            self.info(f"{change_word}:")

            # Find the longest change name for alignment
            max_name_len = (
                max(len(change["change"]) for change in changes) if changes else 0
            )

            # Format and display each change
            date_format = options.get("date_format", "iso")
            for change in changes:
                # Format date
                committed_at = change["committed_at"]
                if isinstance(committed_at, datetime):
                    if date_format == "iso":
                        date_str = committed_at.isoformat()
                    elif date_format == "rfc":
                        date_str = committed_at.strftime("%a, %d %b %Y %H:%M:%S %z")
                    else:
                        date_str = committed_at.strftime(date_format)
                else:
                    date_str = str(committed_at)

                # Pad change name for alignment
                name_padding = " " * (max_name_len - len(change["change"]))

                self.info(
                    f"  {change['change']}{name_padding} - {date_str} - {change['committer_name']} <{change['committer_email']}>"
                )

        except Exception as e:
            self.warn(f"Failed to get deployed changes: {e}")

    def _emit_tags(
        self, engine, project: Optional[str], options: Dict[str, Any]
    ) -> None:
        """
        Emit list of deployed tags.

        Args:
            engine: Database engine
            project: Project name
            options: Command options
        """
        try:
            tags = list(engine.get_current_tags(project))

            self.info("")

            if not tags:
                self.info("Tags: None.")
                return

            tag_word = "Tag" if len(tags) == 1 else "Tags"
            self.info(f"{tag_word}:")

            # Find the longest tag name for alignment
            max_name_len = max(len(tag["tag"]) for tag in tags) if tags else 0

            # Format and display each tag
            date_format = options.get("date_format", "iso")
            for tag in tags:
                # Format date
                committed_at = tag["committed_at"]
                if isinstance(committed_at, datetime):
                    if date_format == "iso":
                        date_str = committed_at.isoformat()
                    elif date_format == "rfc":
                        date_str = committed_at.strftime("%a, %d %b %Y %H:%M:%S %z")
                    else:
                        date_str = committed_at.strftime(date_format)
                else:
                    date_str = str(committed_at)

                # Pad tag name for alignment
                name_padding = " " * (max_name_len - len(tag["tag"]))

                self.info(
                    f"  {tag['tag']}{name_padding} - {date_str} - {tag['committer_name']} <{tag['committer_email']}>"
                )

        except Exception as e:
            self.warn(f"Failed to get deployed tags: {e}")

    def _emit_status(
        self, state: Dict[str, Any], plan: Plan, options: Dict[str, Any]
    ) -> None:
        """
        Emit status comparison with plan.

        Args:
            state: Current state dictionary
            plan: Deployment plan
            options: Command options
        """
        self.info("")

        # Find the current change in the plan
        current_change_id = state["change_id"]
        current_index = None

        for i, change in enumerate(plan.changes):
            if change.id == current_change_id:
                current_index = i
                break

        if current_index is None:
            self.warn(f"Cannot find this change in {plan.file}")
            self.error(
                "Make sure you are connected to the proper database for this project."
            )
            return

        # Check if we're up to date
        total_changes = len(plan.changes)
        if current_index == total_changes - 1:
            self.info("Nothing to deploy (up-to-date)")
        else:
            # Show undeployed changes
            undeployed_count = total_changes - (current_index + 1)
            change_word = "change" if undeployed_count == 1 else "changes"
            self.info(f"Undeployed {change_word}:")

            # List undeployed changes
            for i in range(current_index + 1, total_changes):
                change = plan.changes[i]
                # Format change name with tags if any
                name_with_tags = self._format_change_name_with_tags(change)
                self.info(f"  * {name_with_tags}")

    def _format_change_name_with_tags(self, change: Change) -> str:
        """
        Format change name with associated tags.

        Args:
            change: Change object

        Returns:
            Formatted change name with tags
        """
        name = change.name
        if change.tags:
            tags_str = " ".join(f"@{tag}" for tag in change.tags)
            return f"{name} {tags_str}"
        return name

    def _show_help(self) -> None:
        """Show command help."""
        help_text = """Usage: sqlitch status [options]

Display status information about Sqitch deployment.

Options:
  --target <target>       Target database to check status for
  --plan-file <file>      Plan file to read (default: sqitch.plan)
  --project <project>     Project name to check status for
  --show-changes          Show list of deployed changes
  --show-tags             Show list of deployed tags
  --date-format <format>  Date format (iso, rfc, or strftime format)
  -h, --help             Show this help message

Examples:
  sqlitch status                    # Show basic status
  sqlitch status --show-changes     # Show status with deployed changes
  sqlitch status --show-tags        # Show status with deployed tags
  sqlitch status --target prod      # Show status for 'prod' target
  sqlitch status --date-format rfc  # Use RFC date format
"""
        print(help_text)


# Click command wrapper for CLI integration
@click.command("status")
@click.option("--target", help="Target database to check status for")
@click.option("--plan-file", help="Plan file to read")
@click.option("--project", help="Project name to check status for")
@click.option("--show-changes", is_flag=True, help="Show list of deployed changes")
@click.option("--show-tags", is_flag=True, help="Show list of deployed tags")
@click.option("--date-format", help="Date format (iso, rfc, or strftime format)")
@click.pass_context
def status_command(ctx: click.Context, **kwargs) -> None:
    """Display status information about Sqitch deployment."""
    from ..cli import get_sqitch_from_context

    sqitch = get_sqitch_from_context(ctx)
    command = StatusCommand(sqitch)

    # Build arguments list
    args = []

    for key, value in kwargs.items():
        if value is not None:
            if isinstance(value, bool) and value:
                args.append(f'--{key.replace("_", "-")}')
            elif not isinstance(value, bool):
                args.extend([f'--{key.replace("_", "-")}', str(value)])

    exit_code = command.execute(args)
    if exit_code != 0:
        raise click.ClickException(f"Status command failed with exit code {exit_code}")
