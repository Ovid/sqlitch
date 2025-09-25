"""
Config command for sqlitch.

This module provides the config command for getting and setting
configuration values in sqlitch projects.
"""

from typing import Any, Dict, List, Optional

import click

from ..core.exceptions import SqlitchError
from .base import BaseCommand


class ConfigCommand(BaseCommand):
    """Command for managing configuration."""

    def execute(self, args: List[str]) -> int:
        """Execute the config command."""
        try:
            action, key, value, options = self._parse_args(args)

            # Handle different actions
            if action == "list":
                return self._list_config(options)
            elif action == "get":
                return self._get_config(key, options)
            elif action == "set":
                return self._set_config(key, value, options)
            else:
                self.error(f"Unknown config action: {action}")
                return 1

        except Exception as e:
            return self.handle_error(e, "config")

    def _parse_args(
        self, args: List[str]
    ) -> tuple[str, Optional[str], Optional[str], Dict[str, Any]]:  # noqa: C901
        """Parse command arguments."""
        options = {
            "list": False,
            "get": False,
            "set": False,
            "local": False,
            "user": False,
            "system": False,
        }

        action = None
        key = None
        value = None

        i = 0
        while i < len(args):
            arg = args[i]

            if arg in ["--list", "-l"]:
                options["list"] = True
                action = "list"
            elif arg == "--local":
                options["local"] = True
            elif arg == "--user" or arg == "--global":
                options["user"] = True
            elif arg == "--system":
                options["system"] = True
            elif not key:
                key = arg
                action = "get"
            elif not value:
                value = arg
                action = "set"
            else:
                self.error(f"Unexpected argument: {arg}")
                return action, key, value, options

            i += 1

        # If no explicit action and we have key/value, it's a set
        if not action and key and value:
            action = "set"
        elif not action and key:
            action = "get"
        elif not action:
            action = "list"

        return action, key, value, options

    def _list_config(self, options: Dict[str, Any]) -> int:
        """List configuration values."""
        try:
            config = self.config

            # For now, just use the merged config (TODO: implement scope-specific listing)
            config_data = config._merged_config

            # Format and display config
            if config_data:
                self._emit_config_section(config_data, "")

            return 0

        except Exception as e:
            self.error(f"Failed to list configuration: {e}")
            return 1

    def _get_config(self, key: str, options: Dict[str, Any]) -> int:
        """Get a configuration value."""
        if not key:
            self.error("Configuration key is required")
            return 1

        try:
            config = self.config
            value = config.get(key)

            if value is not None:
                if isinstance(value, list):
                    for v in value:
                        self.emit(str(v))
                else:
                    self.emit(str(value))
                return 0
            else:
                # Exit with code 1 if key not found (matches Perl sqitch)
                return 1

        except Exception as e:
            self.error(f"Failed to get configuration value: {e}")
            return 1

    def _set_config(self, key: str, value: str, options: Dict[str, Any]) -> int:
        """Set a configuration value."""
        if not key or value is None:
            self.error("Both key and value are required for setting configuration")
            return 1

        try:
            config = self.config
            config.set(key, value)
            return 0

        except Exception as e:
            self.error(f"Failed to set configuration value: {e}")
            return 1

    def _emit_config_section(self, data: Any, prefix: str) -> None:
        """Recursively emit configuration values."""
        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key
                if isinstance(value, dict):
                    self._emit_config_section(value, full_key)
                elif isinstance(value, list):
                    for v in value:
                        self.emit(f"{full_key}={v}")
                else:
                    self.emit(f"{full_key}={value}")
        else:
            self.emit(f"{prefix}={data}")


# Click command for CLI integration
@click.command()
@click.option("--list", "-l", is_flag=True, help="List all configuration values")
@click.option("--local", is_flag=True, help="Use local configuration file")
@click.option(
    "--user", "--global", "user", is_flag=True, help="Use user configuration file"
)
@click.option("--system", is_flag=True, help="Use system configuration file")
@click.argument("key", required=False)
@click.argument("value", required=False)
@click.pass_context
def config_command(
    ctx: click.Context,
    list: bool,
    local: bool,
    user: bool,
    system: bool,
    key: Optional[str],
    value: Optional[str],
) -> None:  # noqa: C901
    """Get and set configuration values."""
    from ..cli import get_sqitch_from_context

    try:
        sqitch = get_sqitch_from_context(ctx)
        command = ConfigCommand(sqitch)

        # Build args list from options
        args = []
        if list:
            args.append("--list")
        if local:
            args.append("--local")
        if user:
            args.append("--user")
        if system:
            args.append("--system")
        if key:
            args.append(key)
        if value:
            args.append(value)

        exit_code = command.execute(args)
        if exit_code != 0:
            import sys

            sys.exit(exit_code)

    except SqlitchError as e:
        click.echo(f"Error: {e}", err=True)
        import sys

        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        import sys

        sys.exit(1)
