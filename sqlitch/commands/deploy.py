"""
Deploy command implementation for sqlitch.

This module implements the 'deploy' command which deploys database changes
from the plan file to the target database, with transaction management,
progress reporting, and rollback on failure.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import click

from ..core.change import Change
from ..core.exceptions import DeploymentError, PlanError, SqlitchError
from ..core.plan import Plan
from .base import BaseCommand


class DeployCommand(BaseCommand):
    """Deploy database changes."""

    def execute(self, args: List[str]) -> int:
        """
        Execute the deploy command.

        Args:
            args: Command arguments

        Returns:
            Exit code (0 for success)
        """
        try:
            # Parse arguments
            options = self._parse_args(args)

            # Validate preconditions
            self.validate_preconditions("deploy", options.get("target"))

            # Load plan
            plan = self._load_plan(options.get("plan_file"))

            # Get target
            target = self.get_target(options.get("target"))

            # Create engine with plan
            from ..engines.base import EngineRegistry

            engine = EngineRegistry.create_engine(target, plan)

            # For log-only mode, we don't need to connect to the database
            if not options.get("log_only"):
                # Ensure registry exists
                engine.ensure_registry()

            # Determine changes to deploy
            changes_to_deploy = self._determine_changes_to_deploy(engine, plan, options)

            if not changes_to_deploy:
                self.info("Nothing to deploy")
                return 0

            # Deploy changes
            return self._deploy_changes(engine, changes_to_deploy, options)

        except Exception as e:
            return self.handle_error(e, "deploy")

    def _parse_args(self, args: List[str]) -> Dict[str, Any]:  # noqa: C901
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
            "to_change": None,
            "mode": "all",  # 'all', 'change', 'tag'
            "verify": True,
            "log_only": False,
            "lock_timeout": None,
            "deploy_dir": None,
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
            elif arg == "--to-change":
                if i + 1 >= len(args):
                    raise SqlitchError("--to-change requires a value")
                options["to_change"] = args[i + 1]
                options["mode"] = "change"
                i += 2
            elif arg == "--to-tag":
                if i + 1 >= len(args):
                    raise SqlitchError("--to-tag requires a value")
                options["to_change"] = args[i + 1]
                options["mode"] = "tag"
                i += 2
            elif arg == "--no-verify":
                options["verify"] = False
                i += 1
            elif arg == "--verify":
                options["verify"] = True
                i += 1
            elif arg == "--log-only":
                options["log_only"] = True
                i += 1
            elif arg == "--lock-timeout":
                if i + 1 >= len(args):
                    raise SqlitchError("--lock-timeout requires a value")
                try:
                    options["lock_timeout"] = int(args[i + 1])
                except ValueError:
                    raise SqlitchError("--lock-timeout must be an integer")
                i += 2
            elif arg == "--deploy-dir":
                if i + 1 >= len(args):
                    raise SqlitchError("--deploy-dir requires a value")
                options["deploy_dir"] = Path(args[i + 1])
                i += 2
            elif arg.startswith("-"):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                # Positional argument - treat as target change/tag
                if options["to_change"] is None:
                    options["to_change"] = arg
                    options["mode"] = "change"  # Default to change mode
                else:
                    raise SqlitchError(f"Unexpected argument: {arg}")
                i += 1

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

    def _determine_changes_to_deploy(
        self, engine, plan: Plan, options: Dict[str, Any]
    ) -> List[Change]:
        """
        Determine which changes need to be deployed.

        Args:
            engine: Database engine
            plan: Deployment plan
            options: Command options

        Returns:
            List of changes to deploy
        """
        # Get currently deployed changes (skip for log-only mode)
        if options.get("log_only"):
            deployed_change_ids = set()
        else:
            deployed_change_ids = set(engine.get_deployed_changes())

        # Get all changes from plan
        all_changes = plan.changes.copy()

        # Filter to changes that haven't been deployed yet
        pending_changes = [
            change for change in all_changes if change.id not in deployed_change_ids
        ]

        if not pending_changes:
            return []

        # Apply target filtering if specified
        if options.get("to_change"):
            target_change = options["to_change"]
            mode = options.get("mode", "change")

            if mode == "tag":
                # Deploy up to and including the specified tag
                target_changes = self._get_changes_up_to_tag(plan, target_change)
            else:
                # Deploy up to and including the specified change
                target_changes = self._get_changes_up_to_change(plan, target_change)

            # Filter pending changes to only include those up to target
            target_change_ids = {change.id for change in target_changes}
            pending_changes = [
                change for change in pending_changes if change.id in target_change_ids
            ]

        # Store plan for dependency validation
        self._current_plan = plan

        # Validate dependencies
        self._validate_dependencies(pending_changes, deployed_change_ids)

        return pending_changes

    def _get_changes_up_to_change(self, plan: Plan, target_change: str) -> List[Change]:
        """
        Get all changes up to and including the specified change.

        Args:
            plan: Deployment plan
            target_change: Target change name or ID

        Returns:
            List of changes up to target

        Raises:
            SqlitchError: If target change not found
        """
        target_index = None

        # Find target change by name or ID
        for i, change in enumerate(plan.changes):
            if change.name == target_change or change.id == target_change:
                target_index = i
                break

        if target_index is None:
            raise SqlitchError(f"Change not found in plan: {target_change}")

        return plan.changes[: target_index + 1]

    def _get_changes_up_to_tag(self, plan: Plan, target_tag: str) -> List[Change]:
        """
        Get all changes up to and including the specified tag.

        Args:
            plan: Deployment plan
            target_tag: Target tag name

        Returns:
            List of changes up to tag

        Raises:
            SqlitchError: If target tag not found
        """
        # Find the tag
        target_tag_obj = plan.get_tag(target_tag)
        if target_tag_obj is None:
            raise SqlitchError(f"Tag not found in plan: {target_tag}")

        # Find the change associated with this tag
        # Tags are typically placed after their associated change
        tag_change_index = None

        for i, change in enumerate(plan.changes):
            if change.timestamp <= target_tag_obj.timestamp:
                tag_change_index = i
            else:
                break

        if tag_change_index is None:
            return []

        return plan.changes[: tag_change_index + 1]

    def _validate_dependencies(
        self, changes: List[Change], deployed_change_ids: Set[str]
    ) -> None:
        """
        Validate that all dependencies are satisfied.

        Args:
            changes: Changes to validate
            deployed_change_ids: Set of already deployed change IDs

        Raises:
            SqlitchError: If dependencies are not satisfied
        """
        # Build mapping of change names to IDs for all changes in the plan
        all_plan_changes = getattr(self, "_current_plan", None)
        change_name_to_id = {}
        if all_plan_changes:
            change_name_to_id = {
                change.name: change.id for change in all_plan_changes.changes
            }

        # Build set of change names that are available (deployed + in current batch)
        available_change_names = set()

        # Add deployed changes by mapping IDs back to names
        for deployed_id in deployed_change_ids:
            for name, change_id in change_name_to_id.items():
                if change_id == deployed_id:
                    available_change_names.add(name)
                    break

        # Add changes that will be deployed in this batch
        for change in changes:
            available_change_names.add(change.name)

        # Validate dependencies
        for change in changes:
            for dep in change.dependencies:
                if dep.type == "require" and dep.project is None:
                    if dep.change not in available_change_names:
                        raise SqlitchError(
                            f"Change {change.name} requires {dep.change}, "
                            "but it is not deployed and not in the current deployment"
                        )

    def _deploy_changes(  # noqa: C901
        self, engine, changes: List[Change], options: Dict[str, Any]
    ) -> int:
        """
        Deploy the specified changes.

        Args:
            engine: Database engine
            changes: Changes to deploy
            options: Command options

        Returns:
            Exit code (0 for success)
        """
        if options.get("log_only"):
            return self._log_deployment_plan(changes)

        total_changes = len(changes)
        deployed_count = 0

        self.info(
            f"Deploying {total_changes} change{'s' if total_changes != 1 else ''}"
        )

        try:
            for i, change in enumerate(changes, 1):
                # Progress reporting
                if self.sqitch.verbosity >= 0:
                    progress = f"[{i}/{total_changes}]"
                    self.info(f"{progress} Deploying {change.name}")

                # Verbose logging
                if self.sqitch.verbosity >= 1:
                    self.info(f"  Change ID: {change.id}")
                    self.info(f"  Note: {change.note}")
                    if change.dependencies:
                        deps = ", ".join(str(dep) for dep in change.dependencies)
                        self.info(f"  Dependencies: {deps}")

                # Deploy the change
                try:
                    engine.deploy_change(change)
                    deployed_count += 1

                    # Verify if requested
                    if options.get("verify", True):
                        if self.sqitch.verbosity >= 1:
                            self.info(f"  Verifying {change.name}")

                        if not engine.verify_change(change):
                            raise DeploymentError(
                                f"Verification failed for change {change.name}",
                                change_name=change.name,
                                operation="verify",
                            )

                    if self.sqitch.verbosity >= 0:
                        self.info(f"  + {change.name}")

                except Exception as e:
                    # Deployment failed - report error and exit
                    self.error(f"Deployment failed at change {change.name}: {e}")

                    if deployed_count > 0:
                        self.info(
                            f"Successfully deployed {deployed_count} change{'s' if deployed_count != 1 else ''}"
                        )

                    return 1

            # All changes deployed successfully
            self.info(
                f"Successfully deployed {deployed_count} change{'s' if deployed_count != 1 else ''}"
            )
            return 0

        except KeyboardInterrupt:
            self.error("Deployment cancelled by user")
            if deployed_count > 0:
                self.info(
                    f"Successfully deployed {deployed_count} change{'s' if deployed_count != 1 else ''} before cancellation"
                )
            return 130

    def _log_deployment_plan(self, changes: List[Change]) -> int:
        """
        Log the deployment plan without executing.

        Args:
            changes: Changes that would be deployed

        Returns:
            Exit code (always 0)
        """
        if not changes:
            self.info("Nothing to deploy")
            return 0

        self.info(
            f"Would deploy {len(changes)} change{'s' if len(changes) != 1 else ''}:"
        )

        for change in changes:
            self.info(f"  + {change.name}")
            if self.sqitch.verbosity >= 1:
                self.info(f"    ID: {change.id}")
                self.info(f"    Note: {change.note}")
                if change.dependencies:
                    deps = ", ".join(str(dep) for dep in change.dependencies)
                    self.info(f"    Dependencies: {deps}")

        return 0

    def _deploy_changes_with_feedback(  # noqa: C901
        self, engine, changes: List[Change], options: Dict[str, Any], target
    ) -> int:
        """
        Deploy changes with enhanced user feedback.

        Args:
            engine: Database engine
            changes: Changes to deploy
            options: Command options
            target: Target configuration

        Returns:
            Exit code (0 for success)
        """
        if options.get("log_only"):
            return self._log_deployment_plan(changes)

        import time

        from ..utils.feedback import ChangeReporter, operation_feedback

        start_time = time.time()
        change_names = [change.name for change in changes]

        try:
            with operation_feedback(
                self.sqitch, "deploy", str(target.uri), len(changes)
            ) as reporter:
                deployed_count = 0

                for change in changes:
                    change_reporter = ChangeReporter(self.sqitch, change.name, "deploy")

                    try:
                        change_reporter.start_change()
                        reporter.step_progress(f"Deploying {change.name}")

                        # Deploy the change
                        engine.deploy_change(change)
                        deployed_count += 1

                        # Verify if requested
                        if options.get("verify", True):
                            reporter.step_progress(f"Verifying {change.name}")
                            if not engine.verify_change(change):
                                raise DeploymentError(
                                    f"Verification failed for change {change.name}",
                                    change_name=change.name,
                                    operation="verify",
                                )

                        change_reporter.complete_change(success=True)

                    except Exception as e:
                        change_reporter.complete_change(success=False)
                        raise DeploymentError(
                            f"Failed to deploy {change.name}: {e}",
                            change_name=change.name,
                            operation="deploy",
                        )

                # Show summary
                duration = time.time() - start_time
                from ..utils.feedback import show_operation_summary

                show_operation_summary(
                    self.sqitch, "deploy", change_names, duration, True
                )

                return 0

        except KeyboardInterrupt:
            duration = time.time() - start_time
            self.vent("Deploy cancelled by user")
            if deployed_count > 0:
                self.info(
                    f"Successfully deployed {deployed_count} changes before cancellation"
                )
            return 130
        except Exception:
            duration = time.time() - start_time
            from ..utils.feedback import show_operation_summary

            show_operation_summary(
                self.sqitch, "deploy", change_names[:deployed_count], duration, False
            )
            raise

    def _show_help(self) -> None:
        """Show command help."""
        help_text = """Usage: sqlitch deploy [options] [<change>]

Deploy database changes from the plan to the target database.

Arguments:
  <change>              Deploy up to and including this change

Options:
  --target <target>     Target database to deploy to
  --plan-file <file>    Plan file to read (default: sqitch.plan)
  --to-change <change>  Deploy up to and including this change
  --to-tag <tag>        Deploy up to and including this tag
  --verify              Verify each change after deployment (default)
  --no-verify           Skip verification after deployment
  --log-only            Show what would be deployed without executing
  --lock-timeout <sec>  Lock timeout in seconds
  --deploy-dir <dir>    Directory containing deploy scripts
  -h, --help           Show this help message

Examples:
  sqlitch deploy                    # Deploy all pending changes
  sqlitch deploy users              # Deploy up to 'users' change
  sqlitch deploy --to-tag v1.0      # Deploy up to 'v1.0' tag
  sqlitch deploy --target prod      # Deploy to 'prod' target
  sqlitch deploy --log-only         # Show deployment plan
  sqlitch deploy --no-verify        # Skip verification
"""
        print(help_text)


# Click command wrapper for CLI integration
@click.command("deploy")
@click.argument("change", required=False)
@click.option("--target", help="Target database to deploy to")
@click.option("--plan-file", help="Plan file to read")
@click.option("--to-change", help="Deploy up to and including this change")
@click.option("--to-tag", help="Deploy up to and including this tag")
@click.option(
    "--verify/--no-verify", default=True, help="Verify each change after deployment"
)
@click.option(
    "--log-only", is_flag=True, help="Show what would be deployed without executing"
)
@click.option("--lock-timeout", type=int, help="Lock timeout in seconds")
@click.option("--deploy-dir", help="Directory containing deploy scripts")
@click.pass_context
def deploy_command(ctx: click.Context, change: Optional[str], **kwargs) -> None:
    """Deploy database changes from the plan to the target database."""
    from ..cli import get_sqitch_from_context

    sqitch = get_sqitch_from_context(ctx)
    command = DeployCommand(sqitch)

    # Build arguments list
    args = []

    if change:
        args.append(change)

    for key, value in kwargs.items():
        if value is not None:
            if isinstance(value, bool) and value and key != "verify":
                args.append(f'--{key.replace("_", "-")}')
            elif isinstance(value, bool) and not value and key == "verify":
                args.append("--no-verify")
            elif not isinstance(value, bool):
                args.extend([f'--{key.replace("_", "-")}', str(value)])

    exit_code = command.execute(args)
    if exit_code != 0:
        raise click.ClickException(f"Deploy command failed with exit code {exit_code}")
