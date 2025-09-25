"""
Checkout command implementation for sqlitch.

This module implements the 'checkout' command which reverts to a common change,
checks out a VCS branch, and redeploys changes from the new branch.
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import click

from ..core.change import Change
from ..core.exceptions import PlanError, SqlitchError
from ..core.plan import Plan
from ..utils.git import VCSError
from .base import BaseCommand


class CheckoutCommand(BaseCommand):
    """Checkout VCS branch and update database state."""

    def __init__(self, sqitch):
        """Initialize checkout command."""
        super().__init__(sqitch)
        self.git_client = self._get_git_client()

    def _get_git_client(self) -> str:
        """Get git client command."""
        client = self.config.get("core.vcs.client")
        if client:
            return client

        # Default to git with .exe on Windows
        import platform

        if platform.system() == "Windows":
            return "git.exe"
        return "git"

    def execute(self, args: List[str]) -> int:
        """
        Execute the checkout command.

        Args:
            args: Command arguments

        Returns:
            Exit code (0 for success)
        """
        try:
            # Parse arguments
            options = self._parse_args(args)

            # Ensure project is initialized
            self.require_initialized()

            # Validate user info
            self.validate_user_info()

            # Get branch name
            branch = options.get("branch")
            if not branch:
                self._show_usage()
                return 1

            # Check if we're already on the target branch
            current_branch = self._get_current_branch()
            if current_branch == branch:
                raise SqlitchError(
                    f"Already on branch {branch}", ident="checkout", exitval=1
                )

            # Get target
            target = self.get_target(options.get("target"))

            # Load current plan
            current_plan = self._load_plan(options.get("plan_file"))

            # Load target branch plan
            target_plan = self._load_branch_plan(branch, target)

            # Find last common change
            last_common_change = self._find_last_common_change(
                current_plan, target_plan
            )

            if not last_common_change:
                raise SqlitchError(
                    f"Branch {branch} has no changes in common with current branch {current_branch}",
                    ident="checkout",
                )

            self.info(
                f"Last change before the branches diverged: {last_common_change.format_name_with_tags()}"
            )

            # Create engine with current plan
            from ..engines.base import EngineRegistry

            engine = EngineRegistry.create_engine(target, current_plan)

            # Configure engine options
            self._configure_engine(engine, options)

            # Ensure registry exists
            if not options.get("log_only"):
                engine.ensure_registry()

            # Revert to last common change
            self._revert_to_common_change(engine, last_common_change, options)

            # Checkout the new branch
            self._checkout_branch(branch)

            # Create engine with target plan
            engine = EngineRegistry.create_engine(target, target_plan)
            self._configure_engine(engine, options)

            # Deploy changes from target branch
            self._deploy_target_changes(engine, options)

            return 0

        except Exception as e:
            return self.handle_error(e, "checkout")

    def _parse_args(self, args: List[str]) -> Dict[str, Any]:  # noqa: C901
        """
        Parse command arguments.

        Args:
            args: Raw command arguments

        Returns:
            Parsed options dictionary
        """
        options = {
            "branch": None,
            "target": None,
            "plan_file": None,
            "mode": "all",  # 'all', 'change', 'tag'
            "verify": False,
            "no_prompt": False,
            "prompt_accept": True,
            "log_only": False,
            "lock_timeout": None,
            "deploy_variables": {},
            "revert_variables": {},
        }

        # Apply configuration defaults
        options.update(self._get_config_defaults())

        i = 0
        while i < len(args):
            arg = args[i]

            if arg in ["--help", "-h"]:
                self._show_help()
                raise SystemExit(0)
            elif arg in ["--target", "-t"]:
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options["target"] = args[i + 1]
                i += 1
            elif arg == "--mode":
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                mode = args[i + 1]
                if mode not in ["all", "change", "tag"]:
                    raise SqlitchError(f"Invalid mode: {mode}")
                options["mode"] = mode
                i += 1
            elif arg == "--verify":
                options["verify"] = True
            elif arg == "--no-verify":
                options["verify"] = False
            elif arg in ["--set", "-s"]:
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                key, value = self._parse_variable(args[i + 1])
                options["deploy_variables"][key] = value
                options["revert_variables"][key] = value
                i += 1
            elif arg in ["--set-deploy", "-e"]:
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                key, value = self._parse_variable(args[i + 1])
                options["deploy_variables"][key] = value
                i += 1
            elif arg in ["--set-revert", "-r"]:
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                key, value = self._parse_variable(args[i + 1])
                options["revert_variables"][key] = value
                i += 1
            elif arg == "--log-only":
                options["log_only"] = True
            elif arg == "--lock-timeout":
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                try:
                    options["lock_timeout"] = int(args[i + 1])
                except ValueError:
                    raise SqlitchError(f"Invalid lock timeout: {args[i + 1]}")
                i += 1
            elif arg == "-y":
                options["no_prompt"] = True
            elif arg in ["--plan-file", "-f"]:
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options["plan_file"] = Path(args[i + 1])
                i += 1
            elif arg.startswith("-"):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                # First non-option argument is the branch
                if not options["branch"]:
                    options["branch"] = arg
                else:
                    # Additional arguments are targets (warn about multiple)
                    if not options["target"]:
                        options["target"] = arg
                    else:
                        self.warn(
                            f"Too many targets specified; connecting to {options['target']}"
                        )

            i += 1

        return options

    def _get_config_defaults(self) -> Dict[str, Any]:
        """Get configuration defaults."""
        defaults = {}

        # Checkout-specific config takes precedence
        if self.config.get("checkout.verify") is not None:
            defaults["verify"] = self.config.get("checkout.verify", as_bool=True)
        elif self.config.get("deploy.verify") is not None:
            defaults["verify"] = self.config.get("deploy.verify", as_bool=True)

        if self.config.get("checkout.mode"):
            defaults["mode"] = self.config.get("checkout.mode")
        elif self.config.get("deploy.mode"):
            defaults["mode"] = self.config.get("deploy.mode")

        if self.config.get("checkout.no_prompt") is not None:
            defaults["no_prompt"] = self.config.get("checkout.no_prompt", as_bool=True)
        elif self.config.get("revert.no_prompt") is not None:
            defaults["no_prompt"] = self.config.get("revert.no_prompt", as_bool=True)

        if self.config.get("checkout.prompt_accept") is not None:
            defaults["prompt_accept"] = self.config.get(
                "checkout.prompt_accept", as_bool=True
            )
        elif self.config.get("revert.prompt_accept") is not None:
            defaults["prompt_accept"] = self.config.get(
                "revert.prompt_accept", as_bool=True
            )

        # Check for strict mode
        if self.config.get("checkout.strict", as_bool=True) or self.config.get(
            "revert.strict", as_bool=True
        ):
            raise SqlitchError(
                '"checkout" cannot be used in strict mode.\n'
                "Use explicit revert and deploy commands instead.",
                ident="checkout",
            )

        return defaults

    def _parse_variable(self, var_str: str) -> tuple[str, str]:
        """Parse variable assignment string."""
        if "=" not in var_str:
            raise SqlitchError(f"Invalid variable format: {var_str}")
        key, value = var_str.split("=", 1)
        return key.strip(), value.strip()

    def _get_current_branch(self) -> str:
        """Get current Git branch."""
        import subprocess

        try:
            result = subprocess.run(
                [self.git_client, "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            raise VCSError(f"Failed to get current branch: {e}")

    def _load_plan(self, plan_file: Optional[Path] = None) -> Plan:
        """Load plan file."""
        if plan_file:
            return Plan.from_file(plan_file)

        # Use default plan file
        plan_path = Path(self.config.get("core.plan_file", "sqitch.plan"))
        if not plan_path.exists():
            raise PlanError(f"Plan file not found: {plan_path}")

        return Plan.from_file(plan_path)

    def _load_branch_plan(self, branch: str, target) -> Plan:
        """Load plan file from specified branch."""
        import subprocess

        # Get plan file path - use just the filename, not absolute path
        plan_file = target.plan_file or Path(
            self.config.get("core.plan_file", "sqitch.plan")
        )

        # For git show, we need just the filename relative to repo root
        # If plan_file is absolute, get just the name
        if plan_file.is_absolute():
            relative_path = plan_file.name
        else:
            relative_path = str(plan_file)

        try:
            result = subprocess.run(
                [self.git_client, "show", f"{branch}:{relative_path}"],
                capture_output=True,
                text=True,
                check=True,
            )

            # Parse plan content directly
            return Plan.from_string(result.stdout, plan_file)

        except subprocess.CalledProcessError as e:
            raise VCSError(f"Failed to load plan from branch {branch}: {e}")

    def _find_last_common_change(
        self, current_plan: Plan, target_plan: Plan
    ) -> Optional[Change]:
        """Find the last change common to both plans."""
        last_common = None

        for change in target_plan.changes:
            # Check if this change exists in current plan
            current_change = current_plan.get_change(change.id)
            if current_change:
                last_common = change
            else:
                break

        return last_common

    def _configure_engine(self, engine, options: Dict[str, Any]) -> None:
        """Configure engine with options."""
        if options.get("verify") is not None:
            engine.with_verify = options["verify"]

        if options.get("log_only"):
            engine.log_only = options["log_only"]

        if options.get("lock_timeout") is not None:
            engine.lock_timeout = options["lock_timeout"]

    def _revert_to_common_change(
        self, engine, last_common_change: Change, options: Dict[str, Any]
    ) -> None:
        """Revert to the last common change."""
        try:
            # Set revert variables
            if options.get("revert_variables"):
                engine.set_variables(options["revert_variables"])

            # Revert to the common change
            engine.revert(
                last_common_change.id,
                not options.get("no_prompt", False),
                options.get("prompt_accept", True),
            )
        except SqlitchError as e:
            # Handle non-fatal errors (e.g., nothing to revert)
            if e.exitval <= 1 and e.ident != "revert:confirm":
                self.info(e.message)
            else:
                raise

    def _checkout_branch(self, branch: str) -> None:
        """Checkout the specified branch."""
        import subprocess

        try:
            subprocess.run([self.git_client, "checkout", branch], check=True)
        except subprocess.CalledProcessError as e:
            raise VCSError(f"Failed to checkout branch {branch}: {e}")

    def _deploy_target_changes(self, engine, options: Dict[str, Any]) -> None:
        """Deploy changes from target branch."""
        # Set deploy variables
        if options.get("deploy_variables"):
            engine.set_variables(options["deploy_variables"])

        # Deploy all changes
        engine.deploy(None, options.get("mode", "all"))

    def _show_usage(self) -> None:
        """Show command usage."""
        usage = """Usage: sqlitch checkout [options] [<database>] <branch>

Options:
    -t --target <target>         database to which to connect
       --mode <mode>             deploy failure reversion mode (all, tag, or change)
       --verify                  run verify scripts after deploying each change
       --no-verify               do not run verify scripts
    -s --set        <key=value>  set a database client variable
    -r --set-revert <key=value>  set a database client revert variable
    -e --set-deploy <key=value>  set a database client deploy variable
       --log-only                log changes without running them
       --lock-timeout <timeout>  seconds to wait for target lock
    -y                           disable the prompt before reverting
    -f --plan-file  <file>       path to a deployment plan file
"""
        self.emit(usage)

    def _show_help(self) -> None:
        """Show command help."""
        help_text = """sqlitch-checkout - Revert, checkout another VCS branch, and re-deploy changes

SYNOPSIS
    sqlitch checkout [options] [<database>] <branch>

DESCRIPTION
    Checkout another branch in your project's VCS (such as git), while performing
    the necessary database changes to update your database for the new branch.

    More specifically, the checkout command compares the plan in the current
    branch to that in the branch to check out, identifies the last common changes
    between them, reverts to that change, checks out the new branch, and then
    deploys all changes.

    If the VCS is already on the specified branch, nothing will be done.

OPTIONS
    -t --target <target>
        The target database to which to connect. This option can be either a URI
        or the name of a target in the configuration.

    --mode <mode>
        Specify the reversion mode to use in case of failure. Possible values are:

        all     In the event of failure, revert all deployed changes, back to the
                point at which deployment started. This is the default.

        tag     In the event of failure, revert all deployed changes to the last
                successfully-applied tag.

        change  In the event of failure, no changes will be reverted.

    --verify
        Verify each change by running its verify script, if there is one.

    --no-verify
        Don't verify each change. This is the default.

    -s --set <key=value>
        Set a variable name and value for use by the database engine client.

    -e --set-deploy <key=value>
        Set a variable name and value for use by the database engine client when
        deploying.

    -r --set-revert <key=value>
        Set a variable name and value for use by the database engine client when
        reverting.

    --log-only
        Log the changes as if they were deployed, but without actually running
        the deploy scripts.

    --lock-timeout <timeout>
        Set the number of seconds for Sqlitch to wait to get an exclusive advisory
        lock on the target database. Defaults to 60.

    -y
        Disable the prompt that normally asks whether or not to execute the revert.

    -f --plan-file <file>
        Path to the deployment plan file. Defaults to sqitch.plan.
"""
        self.emit(help_text)


# Click command for CLI integration
@click.command("checkout")
@click.argument("args", nargs=-1)
@click.pass_context
def checkout_command(ctx: click.Context, args: tuple) -> None:
    """Revert, checkout another VCS branch, and re-deploy changes."""
    cli_ctx = ctx.obj
    sqitch = cli_ctx.create_sqitch()

    command = CheckoutCommand(sqitch)
    exit_code = command.execute(list(args))

    if exit_code != 0:
        sys.exit(exit_code)
