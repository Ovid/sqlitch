"""
Log command implementation.

This module provides the LogCommand class that displays the change history
from the database event log with various formatting and filtering options.
"""

from typing import Any, Dict, List, Optional

import click

from ..core.exceptions import EngineError, SqlitchError
from ..utils.formatter import FORMATS, ItemFormatter
from .base import BaseCommand


class LogCommand(BaseCommand):
    """
    Command to display database change history.

    This command searches the event log and displays change history
    with various formatting and filtering options.
    """

    def execute(self, args: List[str]) -> int:
        """
        Execute the log command.

        Args:
            args: Command-line arguments

        Returns:
            Exit code (0 for success, 1 for failure)
        """
        try:
            # Parse arguments
            options = self._parse_args(args)

            # Get target and engine
            target = self.get_target(options.get("target"))
            engine = self.sqitch.engine_for_target(target)

            # Check if database is initialized
            if not self._is_database_initialized(engine):
                self.error(f"Database {target.uri} has not been initialized for Sqitch")
                return 1

            # Check if there are any events
            if not self._has_events(engine):
                self.error(f"No events logged for {target.uri}")
                return 1

            # Create formatter
            formatter = ItemFormatter(
                date_format=options.get("date_format", "iso"),
                color=options.get("color", "auto"),
                abbrev=options.get("abbrev", 0),
            )

            # Get format template
            format_template = self._get_format_template(options)

            # Display header if requested
            if options.get("headers", True):
                self._display_header(target)

            # Search and display events
            event_count = 0
            for event in engine.search_events(
                event=options.get("event"),
                change=options.get("change_pattern"),
                project=options.get("project_pattern"),
                committer=options.get("committer_pattern"),
                planner=options.get("planner_pattern"),
                limit=options.get("max_count"),
                offset=options.get("skip"),
                direction="ASC" if options.get("reverse") else "DESC",
            ):
                formatted_output = formatter.format(format_template, event)
                print(formatted_output)
                event_count += 1

            if event_count == 0:
                self.info("No matching events found")

            return 0

        except SqlitchError as e:
            self.error(str(e))
            return 1
        except Exception as e:
            self.error(f"Unexpected error: {e}")
            return 1

    def _parse_args(self, args: List[str]) -> Dict[str, Any]:
        """
        Parse command-line arguments.

        Args:
            args: Command-line arguments

        Returns:
            Dictionary of parsed options
        """
        options = {
            "headers": True,
            "reverse": False,
            "abbrev": 0,
            "color": "auto",
            "date_format": "iso",
            "format": "medium",
        }

        i = 0
        while i < len(args):
            arg = args[i]

            if arg in ("-t", "--target"):
                if i + 1 >= len(args):
                    raise SqlitchError("Option --target requires a value")
                options["target"] = args[i + 1]
                i += 2
            elif arg.startswith("--target="):
                options["target"] = arg.split("=", 1)[1]
                i += 1
            elif arg in ("-f", "--format"):
                if i + 1 >= len(args):
                    raise SqlitchError("Option --format requires a value")
                options["format"] = args[i + 1]
                i += 2
            elif arg.startswith("--format="):
                options["format"] = arg.split("=", 1)[1]
                i += 1
            elif arg == "--oneline":
                options["format"] = "oneline"
                options["abbrev"] = 6
                i += 1
            elif arg in ("-n", "--max-count"):
                if i + 1 >= len(args):
                    raise SqlitchError("Option --max-count requires a value")
                try:
                    options["max_count"] = int(args[i + 1])
                except ValueError:
                    raise SqlitchError(f"Invalid max-count value: {args[i + 1]}")
                i += 2
            elif arg.startswith("--max-count="):
                try:
                    options["max_count"] = int(arg.split("=", 1)[1])
                except ValueError:
                    raise SqlitchError(
                        f"Invalid max-count value: {arg.split('=', 1)[1]}"
                    )
                i += 1
            elif arg == "--skip":
                if i + 1 >= len(args):
                    raise SqlitchError("Option --skip requires a value")
                try:
                    options["skip"] = int(args[i + 1])
                except ValueError:
                    raise SqlitchError(f"Invalid skip value: {args[i + 1]}")
                i += 2
            elif arg.startswith("--skip="):
                try:
                    options["skip"] = int(arg.split("=", 1)[1])
                except ValueError:
                    raise SqlitchError(f"Invalid skip value: {arg.split('=', 1)[1]}")
                i += 1
            elif arg == "--reverse":
                options["reverse"] = True
                i += 1
            elif arg == "--no-reverse":
                options["reverse"] = False
                i += 1
            elif arg == "--headers":
                options["headers"] = True
                i += 1
            elif arg == "--no-headers":
                options["headers"] = False
                i += 1
            elif arg == "--color":
                if i + 1 >= len(args):
                    raise SqlitchError("Option --color requires a value")
                color_value = args[i + 1]
                if color_value not in ("always", "never", "auto"):
                    raise SqlitchError(f"Invalid color value: {color_value}")
                options["color"] = color_value
                i += 2
            elif arg.startswith("--color="):
                color_value = arg.split("=", 1)[1]
                if color_value not in ("always", "never", "auto"):
                    raise SqlitchError(f"Invalid color value: {color_value}")
                options["color"] = color_value
                i += 1
            elif arg == "--no-color":
                options["color"] = "never"
                i += 1
            elif arg == "--abbrev":
                if i + 1 >= len(args):
                    raise SqlitchError("Option --abbrev requires a value")
                try:
                    options["abbrev"] = int(args[i + 1])
                except ValueError:
                    raise SqlitchError(f"Invalid abbrev value: {args[i + 1]}")
                i += 2
            elif arg.startswith("--abbrev="):
                try:
                    options["abbrev"] = int(arg.split("=", 1)[1])
                except ValueError:
                    raise SqlitchError(f"Invalid abbrev value: {arg.split('=', 1)[1]}")
                i += 1
            elif arg == "--date-format" or arg == "--date":
                if i + 1 >= len(args):
                    raise SqlitchError("Option --date-format requires a value")
                options["date_format"] = args[i + 1]
                i += 2
            elif arg.startswith("--date-format=") or arg.startswith("--date="):
                options["date_format"] = arg.split("=", 1)[1]
                i += 1
            elif arg == "--event":
                if i + 1 >= len(args):
                    raise SqlitchError("Option --event requires a value")
                if "event" not in options:
                    options["event"] = []
                options["event"].append(args[i + 1])
                i += 2
            elif arg.startswith("--event="):
                if "event" not in options:
                    options["event"] = []
                options["event"].append(arg.split("=", 1)[1])
                i += 1
            elif arg == "--change-pattern" or arg == "--change":
                if i + 1 >= len(args):
                    raise SqlitchError("Option --change-pattern requires a value")
                options["change_pattern"] = args[i + 1]
                i += 2
            elif arg.startswith("--change-pattern=") or arg.startswith("--change="):
                options["change_pattern"] = arg.split("=", 1)[1]
                i += 1
            elif arg == "--project-pattern" or arg == "--project":
                if i + 1 >= len(args):
                    raise SqlitchError("Option --project-pattern requires a value")
                options["project_pattern"] = args[i + 1]
                i += 2
            elif arg.startswith("--project-pattern=") or arg.startswith("--project="):
                options["project_pattern"] = arg.split("=", 1)[1]
                i += 1
            elif arg == "--committer-pattern" or arg == "--committer":
                if i + 1 >= len(args):
                    raise SqlitchError("Option --committer-pattern requires a value")
                options["committer_pattern"] = args[i + 1]
                i += 2
            elif arg.startswith("--committer-pattern=") or arg.startswith(
                "--committer="
            ):
                options["committer_pattern"] = arg.split("=", 1)[1]
                i += 1
            elif arg == "--planner-pattern" or arg == "--planner":
                if i + 1 >= len(args):
                    raise SqlitchError("Option --planner-pattern requires a value")
                options["planner_pattern"] = args[i + 1]
                i += 2
            elif arg.startswith("--planner-pattern=") or arg.startswith("--planner="):
                options["planner_pattern"] = arg.split("=", 1)[1]
                i += 1
            elif arg.startswith("-"):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                # Positional arguments (if any)
                i += 1

        return options

    def _get_format_template(self, options: Dict[str, Any]) -> str:
        """
        Get format template from options.

        Args:
            options: Parsed options

        Returns:
            Format template string
        """
        format_name = options.get("format", "medium")

        # Check if it's a predefined format
        if format_name in FORMATS:
            return FORMATS[format_name]

        # Check if it's a custom format (starts with 'format:')
        if format_name.startswith("format:"):
            return format_name[7:]  # Remove 'format:' prefix

        # Check if it's a direct format string
        if "%" in format_name:
            return format_name

        # Unknown format
        raise SqlitchError(f'Unknown log format "{format_name}"')

    def _is_database_initialized(self, engine) -> bool:
        """
        Check if database is initialized for Sqitch.

        Args:
            engine: Database engine

        Returns:
            True if initialized, False otherwise
        """
        try:
            engine.ensure_registry()
            return True
        except EngineError:
            return False

    def _has_events(self, engine) -> bool:
        """
        Check if database has any events.

        Args:
            engine: Database engine

        Returns:
            True if events exist, False otherwise
        """
        try:
            # Try to get one event
            for _ in engine.search_events(limit=1):
                return True
            return False
        except EngineError:
            return False

    def _display_header(self, target) -> None:
        """
        Display header information.

        Args:
            target: Database target
        """
        print(f"On database {target.uri}")


# Click command wrapper for CLI integration


@click.command("log")
@click.option("--target", "-t", help="Target database to read log from")
@click.option(
    "--event",
    multiple=True,
    help="Event type to filter by (can be used multiple times)",
)
@click.option(
    "--change-pattern", "--change", help="Regular expression to match change names"
)
@click.option(
    "--project-pattern", "--project", help="Regular expression to match project names"
)
@click.option(
    "--committer-pattern",
    "--committer",
    help="Regular expression to match committer names",
)
@click.option(
    "--planner-pattern", "--planner", help="Regular expression to match planner names"
)
@click.option(
    "--format",
    "-f",
    default="medium",
    help="Output format (raw, full, long, medium, short, oneline)",
)
@click.option(
    "--date-format",
    "--date",
    default="iso",
    help="Date format (iso, raw, short, or strftime format)",
)
@click.option(
    "--max-count", "-n", type=int, help="Maximum number of entries to display"
)
@click.option("--skip", type=int, help="Number of entries to skip")
@click.option(
    "--reverse/--no-reverse", default=False, help="Reverse the order of entries"
)
@click.option(
    "--color",
    type=click.Choice(["always", "never", "auto"]),
    default="auto",
    help="When to use color",
)
@click.option(
    "--no-color", is_flag=True, help="Never use color (same as --color=never)"
)
@click.option(
    "--abbrev",
    type=int,
    default=0,
    help="Abbreviate change IDs to this many characters",
)
@click.option(
    "--oneline", is_flag=True, help="Shorthand for --format=oneline --abbrev=6"
)
@click.option("--headers/--no-headers", default=True, help="Show/hide headers")
@click.pass_context
def log_command(ctx: click.Context, **kwargs) -> None:
    """Display database change history."""
    from ..cli import get_sqitch_from_context

    try:
        sqitch = get_sqitch_from_context(ctx)
        command = LogCommand(sqitch)

        # Convert Click arguments to list format
        args = []

        # Handle special cases
        if kwargs.get("no_color"):
            kwargs["color"] = "never"

        if kwargs.get("oneline"):
            kwargs["format"] = "oneline"
            kwargs["abbrev"] = 6

        # Convert kwargs to command line arguments
        for key, value in kwargs.items():
            if value is None or key in ("no_color", "oneline"):
                continue

            arg_name = key.replace("_", "-")

            if isinstance(value, bool):
                if value:
                    args.append(f"--{arg_name}")
            elif isinstance(value, (list, tuple)):
                for item in value:
                    args.extend([f"--{arg_name}", str(item)])
            else:
                args.extend([f"--{arg_name}", str(value)])

        exit_code = command.execute(args)
        if exit_code != 0:
            ctx.exit(exit_code)

    except Exception as e:
        click.echo(f"sqlitch: {e}", err=True)
        ctx.exit(1)
