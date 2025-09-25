"""
Verify command implementation for sqlitch.

This module implements the 'verify' command which verifies deployed database
changes by running verification scripts, with support for parallel verification,
detailed error reporting, and range-based verification.
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import click

from ..core.change import Change
from ..core.exceptions import PlanError, SqlitchError
from ..core.plan import Plan
from .base import BaseCommand


class VerificationResult:
    """Result of verifying a single change."""

    def __init__(
        self,
        change: Change,
        success: bool,
        error: Optional[str] = None,
        out_of_order: bool = False,
        not_in_plan: bool = False,
        not_deployed: bool = False,
        reworked: bool = False,
    ):
        self.change = change
        self.success = success
        self.error = error
        self.out_of_order = out_of_order
        self.not_in_plan = not_in_plan
        self.not_deployed = not_deployed
        self.reworked = reworked

    @property
    def has_errors(self) -> bool:
        """Check if this result has any errors."""
        return (
            not self.success
            or self.out_of_order
            or self.not_in_plan
            or self.not_deployed
        )


class VerifyCommand(BaseCommand):
    """Verify deployed database changes."""

    def __init__(self, sqitch):
        super().__init__(sqitch)
        self._lock = threading.Lock()

    def execute(self, args: List[str]) -> int:
        """
        Execute the verify command.

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

            # Load plan
            plan = self._load_plan(options.get("plan_file"))

            # Get target
            target = self.get_target(options.get("target"))

            # Create engine with plan
            from ..engines.base import EngineRegistry

            engine = EngineRegistry.create_engine(target, plan)

            # Ensure registry exists
            engine.ensure_registry()

            # Perform verification
            return self._verify_changes(engine, plan, options)

        except SqlitchError as e:
            self.error(str(e))
            return 1
        except Exception as e:
            self.error(f"Unexpected error: {e}")
            if self.sqitch.verbosity >= 2:
                import traceback

                self.debug(traceback.format_exc())
            return 2

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
            "from_change": None,
            "to_change": None,
            "variables": {},
            "parallel": True,
            "max_workers": None,
        }

        i = 0
        while i < len(args):
            arg = args[i]

            if arg in ["--help", "-h"]:
                self._show_help()
                raise SystemExit(0)
            elif arg == "--target" or arg == "-t":
                if i + 1 >= len(args):
                    raise SqlitchError("--target requires a value")
                options["target"] = args[i + 1]
                i += 2
            elif arg == "--plan-file":
                if i + 1 >= len(args):
                    raise SqlitchError("--plan-file requires a value")
                options["plan_file"] = Path(args[i + 1])
                i += 2
            elif arg == "--from-change" or arg == "--from":
                if i + 1 >= len(args):
                    raise SqlitchError("--from-change requires a value")
                options["from_change"] = args[i + 1]
                i += 2
            elif arg == "--to-change" or arg == "--to":
                if i + 1 >= len(args):
                    raise SqlitchError("--to-change requires a value")
                options["to_change"] = args[i + 1]
                i += 2
            elif arg == "--set" or arg == "-s":
                if i + 1 >= len(args):
                    raise SqlitchError("--set requires a value")
                var_assignment = args[i + 1]
                if "=" not in var_assignment:
                    raise SqlitchError("--set requires format key=value")
                key, value = var_assignment.split("=", 1)
                options["variables"][key] = value
                i += 2
            elif arg == "--no-parallel":
                options["parallel"] = False
                i += 1
            elif arg == "--parallel":
                options["parallel"] = True
                i += 1
            elif arg == "--max-workers":
                if i + 1 >= len(args):
                    raise SqlitchError("--max-workers requires a value")
                try:
                    options["max_workers"] = int(args[i + 1])
                except ValueError:
                    raise SqlitchError("--max-workers must be an integer")
                i += 2
            elif arg.startswith("-"):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                # Positional arguments - from and to changes
                if options["from_change"] is None:
                    options["from_change"] = arg
                elif options["to_change"] is None:
                    options["to_change"] = arg
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

    def _verify_changes(self, engine, plan: Plan, options: Dict[str, Any]) -> int:
        """
        Verify deployed changes.

        Args:
            engine: Database engine
            plan: Deployment plan
            options: Command options

        Returns:
            Exit code (0 for success)
        """
        self.info(f"Verifying {engine.target.name}")

        # Get deployed changes
        deployed_change_ids = set(engine.get_deployed_changes())

        if not deployed_change_ids:
            if plan.changes:
                self.info("No changes deployed")
            else:
                self.info("Nothing to verify (no planned or deployed changes)")
            return 0

        if not plan.changes:
            raise SqlitchError("There are deployed changes, but none planned!")

        # Get deployed changes as Change objects
        deployed_changes = [
            change for change in plan.changes if change.id in deployed_change_ids
        ]

        if not deployed_changes:
            self.info("No deployed changes found in plan")
            return 0

        # Determine verification range
        from_idx, to_idx = self._determine_verification_range(
            plan, deployed_changes, options
        )

        # Get changes to verify
        changes_to_verify = deployed_changes[from_idx : to_idx + 1]

        if not changes_to_verify:
            self.info("No changes to verify in specified range")
            return 0

        # Perform verification
        results = self._run_verifications(engine, plan, changes_to_verify, options)

        # Check for undeployed changes in range
        undeployed_results = self._check_undeployed_changes(
            plan, deployed_change_ids, from_idx, to_idx
        )
        results.extend(undeployed_results)

        # Report results
        return self._report_results(results, plan, from_idx, to_idx, options)

    def _determine_verification_range(
        self, plan: Plan, deployed_changes: List[Change], options: Dict[str, Any]
    ) -> Tuple[int, int]:
        """
        Determine the range of changes to verify.

        Args:
            plan: Deployment plan
            deployed_changes: List of deployed changes
            options: Command options

        Returns:
            Tuple of (from_index, to_index) in deployed_changes list
        """
        from_idx = 0
        to_idx = len(deployed_changes) - 1

        # Handle from_change
        if options.get("from_change"):
            from_change = options["from_change"]
            for i, change in enumerate(deployed_changes):
                if change.name == from_change or change.id == from_change:
                    from_idx = i
                    break
            else:
                # Check if it's in plan but not deployed
                if any(
                    c.name == from_change or c.id == from_change for c in plan.changes
                ):
                    raise SqlitchError(f'Change "{from_change}" has not been deployed')
                else:
                    raise SqlitchError(
                        f'Cannot find "{from_change}" in the database or the plan'
                    )

        # Handle to_change
        if options.get("to_change"):
            to_change = options["to_change"]
            for i, change in enumerate(deployed_changes):
                if change.name == to_change or change.id == to_change:
                    to_idx = i
                    break
            else:
                # Check if it's in plan but not deployed
                if any(c.name == to_change or c.id == to_change for c in plan.changes):
                    raise SqlitchError(f'Change "{to_change}" has not been deployed')
                else:
                    raise SqlitchError(
                        f'Cannot find "{to_change}" in the database or the plan'
                    )

        return from_idx, to_idx

    def _run_verifications(
        self, engine, plan: Plan, changes: List[Change], options: Dict[str, Any]
    ) -> List[VerificationResult]:
        """
        Run verification for all changes.

        Args:
            engine: Database engine
            plan: Deployment plan
            changes: Changes to verify
            options: Command options

        Returns:
            List of verification results
        """
        results = []

        # Calculate max name length for formatting
        max_name_len = max(len(change.format_name_with_tags()) for change in changes)

        if options.get("parallel", True) and len(changes) > 1:
            # Parallel verification
            results = self._run_parallel_verifications(
                engine, plan, changes, options, max_name_len
            )
        else:
            # Sequential verification
            results = self._run_sequential_verifications(
                engine, plan, changes, options, max_name_len
            )

        return results

    def _run_sequential_verifications(
        self,
        engine,
        plan: Plan,
        changes: List[Change],
        options: Dict[str, Any],
        max_name_len: int,
    ) -> List[VerificationResult]:
        """
        Run verifications sequentially.

        Args:
            engine: Database engine
            plan: Deployment plan
            changes: Changes to verify
            options: Command options
            max_name_len: Maximum name length for formatting

        Returns:
            List of verification results
        """
        results = []

        for i, change in enumerate(changes):
            result = self._verify_single_change(engine, plan, change, i, options)
            self._emit_verification_result(result, max_name_len)
            results.append(result)

        return results

    def _run_parallel_verifications(
        self,
        engine,
        plan: Plan,
        changes: List[Change],
        options: Dict[str, Any],
        max_name_len: int,
    ) -> List[VerificationResult]:
        """
        Run verifications in parallel.

        Args:
            engine: Database engine
            plan: Deployment plan
            changes: Changes to verify
            options: Command options
            max_name_len: Maximum name length for formatting

        Returns:
            List of verification results
        """
        results = [None] * len(changes)  # Pre-allocate to maintain order
        max_workers = options.get("max_workers") or min(len(changes), 4)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all verification tasks
            future_to_index = {
                executor.submit(
                    self._verify_single_change, engine, plan, change, i, options
                ): i
                for i, change in enumerate(changes)
            }

            # Collect results as they complete
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    result = future.result()
                    results[index] = result

                    # Thread-safe output
                    with self._lock:
                        self._emit_verification_result(result, max_name_len)

                except Exception as e:
                    # Create error result
                    change = changes[index]
                    result = VerificationResult(change, False, str(e))
                    results[index] = result

                    with self._lock:
                        self._emit_verification_result(result, max_name_len)

        return results

    def _verify_single_change(
        self,
        engine,
        plan: Plan,
        change: Change,
        expected_index: int,
        options: Dict[str, Any],
    ) -> VerificationResult:
        """
        Verify a single change.

        Args:
            engine: Database engine
            plan: Deployment plan
            change: Change to verify
            expected_index: Expected index in plan
            options: Command options

        Returns:
            Verification result
        """
        # Check if change is in plan
        plan_index = None
        for i, plan_change in enumerate(plan.changes):
            if plan_change.id == change.id:
                plan_index = i
                break

        out_of_order = False
        not_in_plan = False
        reworked = False

        if plan_index is None:
            not_in_plan = True
        else:
            # Check if it's out of order (this is a simplified check)
            # In a real implementation, we'd need to track the expected sequence
            if plan_index != expected_index:
                out_of_order = True

            # Check if it's reworked
            plan_change = plan.changes[plan_index]
            reworked = getattr(plan_change, "is_reworked", False)

        # Run verification script (unless reworked)
        success = True
        error = None

        if not reworked:
            try:
                success = engine.verify_change(change)
                if not success:
                    error = "Verification script failed"
            except Exception as e:
                success = False
                error = str(e)

        return VerificationResult(
            change=change,
            success=success,
            error=error,
            out_of_order=out_of_order,
            not_in_plan=not_in_plan,
            reworked=reworked,
        )

    def _emit_verification_result(
        self, result: VerificationResult, max_name_len: int
    ) -> None:
        """
        Emit verification result to output.

        Args:
            result: Verification result
            max_name_len: Maximum name length for formatting
        """
        name = result.change.format_name_with_tags()
        padding = "." * (max_name_len - len(name))

        # Build status line
        status_parts = []

        if result.out_of_order:
            status_parts.append("Out of order")
        if result.not_in_plan:
            status_parts.append("Not present in the plan")
        if result.error:
            status_parts.append(result.error)

        status = " ".join(status_parts) if status_parts else ""
        result_text = "ok" if not result.has_errors else "not ok"

        # Emit the line
        if status:
            self.info(f"  * {name} {padding} {result_text} ({status})")
        else:
            self.info(f"  * {name} {padding} {result_text}")

    def _check_undeployed_changes(
        self, plan: Plan, deployed_change_ids: Set[str], from_idx: int, to_idx: int
    ) -> List[VerificationResult]:
        """
        Check for undeployed changes in the verification range.

        Args:
            plan: Deployment plan
            deployed_change_ids: Set of deployed change IDs
            from_idx: Start index in plan
            to_idx: End index in plan

        Returns:
            List of results for undeployed changes
        """
        results = []

        # Find plan indices corresponding to the verification range
        # This is a simplified approach - in reality we'd need more sophisticated mapping
        plan_from_idx = from_idx
        plan_to_idx = min(to_idx, len(plan.changes) - 1)

        for i in range(plan_from_idx, plan_to_idx + 1):
            if i < len(plan.changes):
                change = plan.changes[i]
                if change.id not in deployed_change_ids:
                    result = VerificationResult(
                        change=change, success=False, not_deployed=True
                    )
                    results.append(result)

        return results

    def _report_results(
        self,
        results: List[VerificationResult],
        plan: Plan,
        from_idx: int,
        to_idx: int,
        options: Dict[str, Any],
    ) -> int:
        """
        Report verification results and return exit code.

        Args:
            results: Verification results
            plan: Deployment plan
            from_idx: Start index
            to_idx: End index
            options: Command options

        Returns:
            Exit code (0 for success)
        """
        # Count errors
        error_count = sum(1 for result in results if result.has_errors)

        if error_count > 0:
            # Emit summary report
            self.info("")
            self.info("Verify Summary Report")
            self.info("-" * len("Verify Summary Report"))

            num_changes = len(results)
            self.info(f"Changes: {num_changes}")
            self.info(f"Errors:  {error_count}")

            # Check for pending changes
            if to_idx < len(plan.changes) - 1:
                pending_changes = plan.changes[to_idx + 1 :]
                if pending_changes:
                    count = len(pending_changes)
                    label = (
                        "Undeployed change:" if count == 1 else "Undeployed changes:"
                    )
                    self.info(label)
                    for change in pending_changes:
                        self.info(f"  * {change.format_name_with_tags()}")

            raise SqlitchError("Verify failed")

        # Success!
        self.info("Verify successful")
        return 0

    def _show_help(self) -> None:
        """Show command help."""
        help_text = """Usage: sqlitch verify [options] [<from-change>] [<to-change>]

Verify deployed database changes by running verification scripts.

Arguments:
  <from-change>         Verify changes from this change (inclusive)
  <to-change>           Verify changes up to this change (inclusive)

Options:
  --target <target>     Target database to verify
  -t <target>           Alias for --target
  --plan-file <file>    Plan file to read (default: sqitch.plan)
  --from-change <change> Verify changes from this change (inclusive)
  --from <change>       Alias for --from-change
  --to-change <change>  Verify changes up to this change (inclusive)
  --to <change>         Alias for --to-change
  --set <key=value>     Set variable for verification scripts
  -s <key=value>        Alias for --set
  --parallel            Run verifications in parallel (default)
  --no-parallel         Run verifications sequentially
  --max-workers <n>     Maximum number of parallel workers
  -h, --help           Show this help message

Examples:
  sqlitch verify                    # Verify all deployed changes
  sqlitch verify users              # Verify from 'users' change to end
  sqlitch verify users posts       # Verify from 'users' to 'posts'
  sqlitch verify --target prod      # Verify on 'prod' target
  sqlitch verify --no-parallel      # Run sequentially
  sqlitch verify --set var=value    # Set variable for scripts
"""
        print(help_text)


# Click command wrapper for CLI integration
@click.command("verify")
@click.argument("from_change", required=False)
@click.argument("to_change", required=False)
@click.option("--target", "-t", help="Target database to verify")
@click.option("--plan-file", help="Plan file to read")
@click.option(
    "--from-change", "--from", help="Verify changes from this change (inclusive)"
)
@click.option(
    "--to-change", "--to", help="Verify changes up to this change (inclusive)"
)
@click.option(
    "--set",
    "-s",
    "variables",
    multiple=True,
    help="Set variable for verification scripts (key=value)",
)
@click.option(
    "--parallel/--no-parallel", default=True, help="Run verifications in parallel"
)
@click.option("--max-workers", type=int, help="Maximum number of parallel workers")
@click.pass_context
def verify_command(
    ctx: click.Context, from_change: Optional[str], to_change: Optional[str], **kwargs
) -> None:
    """Verify deployed database changes by running verification scripts."""
    from ..cli import get_sqitch_from_context

    sqitch = get_sqitch_from_context(ctx)
    command = VerifyCommand(sqitch)

    # Build arguments list
    args = []

    # Handle positional arguments
    if from_change:
        args.append(from_change)
    if to_change:
        args.append(to_change)

    # Handle options
    for key, value in kwargs.items():
        if value is not None:
            if key == "variables":
                for var in value:
                    args.extend(["--set", var])
            elif isinstance(value, bool) and value and key != "parallel":
                args.append(f'--{key.replace("_", "-")}')
            elif isinstance(value, bool) and not value and key == "parallel":
                args.append("--no-parallel")
            elif not isinstance(value, bool):
                args.extend([f'--{key.replace("_", "-")}', str(value)])

    exit_code = command.execute(args)
    if exit_code != 0:
        raise click.ClickException(f"Verify command failed with exit code {exit_code}")
