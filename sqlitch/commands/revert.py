"""
Revert command implementation for sqlitch.

This module implements the 'revert' command which reverts database changes
from the target database, with confirmation prompts, safety checks, and
support for reverting to specific changes or tags.
"""

import sys
from pathlib import Path
from typing import List, Optional, Dict, Any, Set
from datetime import datetime

import click

from .base import BaseCommand
from ..core.exceptions import SqlitchError, DeploymentError, PlanError
from ..core.plan import Plan
from ..core.change import Change
from ..core.types import ChangeStatus


class RevertCommand(BaseCommand):
    """Revert database changes."""
    
    def execute(self, args: List[str]) -> int:
        """
        Execute the revert command.
        
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
            
            # Load plan
            plan = self._load_plan(options.get('plan_file'))
            
            # Get target
            target = self.get_target(options.get('target'))
            
            # Create engine with plan
            from ..engines.base import EngineRegistry
            engine = EngineRegistry.create_engine(target, plan)
            
            # For log-only mode, we don't need to connect to the database
            if not options.get('log_only'):
                # Ensure registry exists
                engine.ensure_registry()
            
            # Determine changes to revert
            changes_to_revert = self._determine_changes_to_revert(
                engine, plan, options
            )
            
            if not changes_to_revert:
                self.info("Nothing to revert")
                return 0
            
            # Revert changes
            return self._revert_changes(engine, changes_to_revert, options)
            
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
            'target': None,
            'plan_file': None,
            'to_change': None,
            'mode': 'all',  # 'all', 'change', 'tag'
            'no_prompt': False,
            'prompt_accept': True,
            'log_only': False,
            'lock_timeout': None,
            'revert_dir': None,
            'modified': False,
            'strict': False,
        }
        
        i = 0
        while i < len(args):
            arg = args[i]
            
            if arg in ['--help', '-h']:
                self._show_help()
                raise SystemExit(0)
            elif arg == '--target':
                if i + 1 >= len(args):
                    raise SqlitchError("--target requires a value")
                options['target'] = args[i + 1]
                i += 2
            elif arg == '--plan-file':
                if i + 1 >= len(args):
                    raise SqlitchError("--plan-file requires a value")
                options['plan_file'] = Path(args[i + 1])
                i += 2
            elif arg == '--to-change' or arg == '--to':
                if i + 1 >= len(args):
                    raise SqlitchError("--to-change requires a value")
                options['to_change'] = args[i + 1]
                options['mode'] = 'change'
                i += 2
            elif arg == '--to-tag':
                if i + 1 >= len(args):
                    raise SqlitchError("--to-tag requires a value")
                options['to_change'] = args[i + 1]
                options['mode'] = 'tag'
                i += 2
            elif arg == '-y' or arg == '--no-prompt':
                options['no_prompt'] = True
                i += 1
            elif arg == '--prompt':
                options['no_prompt'] = False
                i += 1
            elif arg == '--log-only':
                options['log_only'] = True
                i += 1
            elif arg == '--lock-timeout':
                if i + 1 >= len(args):
                    raise SqlitchError("--lock-timeout requires a value")
                try:
                    options['lock_timeout'] = int(args[i + 1])
                except ValueError:
                    raise SqlitchError("--lock-timeout must be an integer")
                i += 2
            elif arg == '--revert-dir':
                if i + 1 >= len(args):
                    raise SqlitchError("--revert-dir requires a value")
                options['revert_dir'] = Path(args[i + 1])
                i += 2
            elif arg == '--modified' or arg == '-m':
                options['modified'] = True
                i += 1
            elif arg == '--strict':
                options['strict'] = True
                i += 1
            elif arg.startswith('-'):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                # Positional argument - treat as target change/tag
                if options['to_change'] is None:
                    options['to_change'] = arg
                    options['mode'] = 'change'  # Default to change mode
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
    
    def _determine_changes_to_revert(self, engine, plan: Plan, 
                                   options: Dict[str, Any]) -> List[Change]:
        """
        Determine which changes need to be reverted.
        
        Args:
            engine: Database engine
            plan: Deployment plan
            options: Command options
            
        Returns:
            List of changes to revert (in reverse order)
        """
        # Get currently deployed changes (skip for log-only mode)
        if options.get('log_only'):
            # For log-only mode, assume all changes are deployed
            deployed_change_ids = set(change.id for change in plan.changes)
        else:
            deployed_change_ids = set(engine.get_deployed_changes())
        
        if not deployed_change_ids:
            return []
        
        # Get all changes from plan that are currently deployed
        deployed_changes = [
            change for change in plan.changes 
            if change.id in deployed_change_ids
        ]
        
        if not deployed_changes:
            return []
        
        # Handle modified mode - revert to common ancestor
        if options.get('modified'):
            # This would require VCS integration to find common ancestor
            # For now, just revert all changes
            self.warn("Modified mode not fully implemented, reverting all changes")
            changes_to_revert = deployed_changes
        elif options.get('to_change'):
            # Revert to specific change or tag
            target_change = options['to_change']
            mode = options.get('mode', 'change')
            
            if mode == 'tag':
                # Revert to the specified tag (keep changes up to and including tag)
                changes_to_revert = self._get_changes_to_revert_to_tag(
                    plan, deployed_changes, target_change
                )
            else:
                # Revert to the specified change (keep changes up to and including change)
                changes_to_revert = self._get_changes_to_revert_to_change(
                    plan, deployed_changes, target_change
                )
        else:
            # Revert all deployed changes
            changes_to_revert = deployed_changes
        
        # Validate strict mode requirements
        if options.get('strict') and not options.get('to_change') and changes_to_revert:
            raise SqlitchError("Must specify a target revision in strict mode")
        
        # Return changes in reverse order (most recent first)
        return list(reversed(changes_to_revert))
    
    def _get_changes_to_revert_to_change(self, plan: Plan, deployed_changes: List[Change], 
                                       target_change: str) -> List[Change]:
        """
        Get changes to revert when reverting to a specific change.
        
        Args:
            plan: Deployment plan
            deployed_changes: Currently deployed changes
            target_change: Target change name or ID
            
        Returns:
            List of changes to revert
            
        Raises:
            SqlitchError: If target change not found or not deployed
        """
        # Find target change in plan
        target_change_obj = None
        target_index = None
        
        for i, change in enumerate(plan.changes):
            if change.name == target_change or change.id == target_change:
                target_change_obj = change
                target_index = i
                break
        
        if target_change_obj is None:
            raise SqlitchError(f"Change not found in plan: {target_change}")
        
        # Check if target change is deployed
        deployed_change_ids = {change.id for change in deployed_changes}
        if target_change_obj.id not in deployed_change_ids:
            raise SqlitchError(f"Target change is not deployed: {target_change}")
        
        # Get changes that come after the target change
        changes_to_revert = []
        for change in plan.changes[target_index + 1:]:
            if change.id in deployed_change_ids:
                changes_to_revert.append(change)
        
        return changes_to_revert
    
    def _get_changes_to_revert_to_tag(self, plan: Plan, deployed_changes: List[Change], 
                                    target_tag: str) -> List[Change]:
        """
        Get changes to revert when reverting to a specific tag.
        
        Args:
            plan: Deployment plan
            deployed_changes: Currently deployed changes
            target_tag: Target tag name
            
        Returns:
            List of changes to revert
            
        Raises:
            SqlitchError: If target tag not found
        """
        # Find the tag
        target_tag_obj = plan.get_tag(target_tag)
        if target_tag_obj is None:
            raise SqlitchError(f"Tag not found in plan: {target_tag}")
        
        # Find the change associated with this tag
        tag_change_index = None
        
        for i, change in enumerate(plan.changes):
            if change.timestamp <= target_tag_obj.timestamp:
                tag_change_index = i
            else:
                break
        
        if tag_change_index is None:
            return deployed_changes  # Revert everything
        
        # Get changes that come after the tag
        deployed_change_ids = {change.id for change in deployed_changes}
        changes_to_revert = []
        
        for change in plan.changes[tag_change_index + 1:]:
            if change.id in deployed_change_ids:
                changes_to_revert.append(change)
        
        return changes_to_revert
    
    def _revert_changes(self, engine, changes: List[Change], 
                       options: Dict[str, Any]) -> int:
        """
        Revert the specified changes.
        
        Args:
            engine: Database engine
            changes: Changes to revert (in reverse order)
            options: Command options
            
        Returns:
            Exit code (0 for success)
        """
        if options.get('log_only'):
            return self._log_revert_plan(changes)
        
        if not changes:
            self.info("Nothing to revert")
            return 0
        
        # Confirmation prompt unless disabled
        if not options.get('no_prompt'):
            if not self._confirm_revert(changes, options):
                self.info("Revert cancelled")
                return 0
        
        total_changes = len(changes)
        reverted_count = 0
        
        self.info(f"Reverting {total_changes} change{'s' if total_changes != 1 else ''}")
        
        try:
            for i, change in enumerate(changes, 1):
                # Progress reporting
                if self.sqitch.verbosity >= 0:
                    progress = f"[{i}/{total_changes}]"
                    self.info(f"{progress} Reverting {change.name}")
                
                # Verbose logging
                if self.sqitch.verbosity >= 1:
                    self.info(f"  Change ID: {change.id}")
                    self.info(f"  Note: {change.note}")
                
                # Revert the change
                try:
                    engine.revert_change(change)
                    reverted_count += 1
                    
                    if self.sqitch.verbosity >= 0:
                        self.info(f"  - {change.name}")
                
                except Exception as e:
                    # Revert failed - report error and exit
                    self.error(f"Revert failed at change {change.name}: {e}")
                    
                    if reverted_count > 0:
                        self.info(f"Successfully reverted {reverted_count} change{'s' if reverted_count != 1 else ''}")
                    
                    return 1
            
            # All changes reverted successfully
            self.info(f"Successfully reverted {reverted_count} change{'s' if reverted_count != 1 else ''}")
            return 0
            
        except KeyboardInterrupt:
            self.error("Revert cancelled by user")
            if reverted_count > 0:
                self.info(f"Successfully reverted {reverted_count} change{'s' if reverted_count != 1 else ''} before cancellation")
            return 130
    
    def _confirm_revert(self, changes: List[Change], options: Dict[str, Any]) -> bool:
        """
        Confirm revert operation with user.
        
        Args:
            changes: Changes to be reverted
            options: Command options
            
        Returns:
            True if user confirms, False otherwise
        """
        if len(changes) == 1:
            message = f"Revert change {changes[0].name}?"
        else:
            message = f"Revert {len(changes)} changes?"
        
        # Show what will be reverted
        self.info("The following changes will be reverted:")
        for change in changes:
            self.info(f"  - {change.name}")
        
        # Get user confirmation
        try:
            response = input(f"{message} [y/N] ").strip().lower()
            return response in ['y', 'yes']
        except (EOFError, KeyboardInterrupt):
            return False
    
    def _log_revert_plan(self, changes: List[Change]) -> int:
        """
        Log the revert plan without executing.
        
        Args:
            changes: Changes that would be reverted
            
        Returns:
            Exit code (always 0)
        """
        if not changes:
            self.info("Nothing to revert")
            return 0
        
        self.info(f"Would revert {len(changes)} change{'s' if len(changes) != 1 else ''}:")
        
        for change in changes:
            self.info(f"  - {change.name}")
            if self.sqitch.verbosity >= 1:
                self.info(f"    ID: {change.id}")
                self.info(f"    Note: {change.note}")
        
        return 0
    
    def _show_help(self) -> None:
        """Show command help."""
        help_text = """Usage: sqlitch revert [options] [<change>]

Revert database changes from the target database.

Arguments:
  <change>              Revert to this change (keeping it deployed)

Options:
  --target <target>     Target database to revert from
  --plan-file <file>    Plan file to read (default: sqitch.plan)
  --to-change <change>  Revert to this change (keeping it deployed)
  --to <change>         Alias for --to-change
  --to-tag <tag>        Revert to this tag (keeping changes up to tag)
  -y, --no-prompt       Do not prompt for confirmation
  --prompt              Prompt for confirmation (default)
  --log-only            Show what would be reverted without executing
  --lock-timeout <sec>  Lock timeout in seconds
  --revert-dir <dir>    Directory containing revert scripts
  -m, --modified        Revert to common ancestor with VCS
  --strict              Require target revision in strict mode
  -h, --help           Show this help message

Examples:
  sqlitch revert                    # Revert all changes (with confirmation)
  sqlitch revert users              # Revert to 'users' change (keep it)
  sqlitch revert --to-tag v1.0      # Revert to 'v1.0' tag
  sqlitch revert --target prod      # Revert from 'prod' target
  sqlitch revert --log-only         # Show revert plan
  sqlitch revert -y                 # Revert without confirmation
"""
        print(help_text)


# Click command wrapper for CLI integration
@click.command('revert')
@click.argument('change', required=False)
@click.option('--target', help='Target database to revert from')
@click.option('--plan-file', help='Plan file to read')
@click.option('--to-change', '--to', help='Revert to this change (keeping it deployed)')
@click.option('--to-tag', help='Revert to this tag (keeping changes up to tag)')
@click.option('-y', '--no-prompt', is_flag=True, help='Do not prompt for confirmation')
@click.option('--prompt', is_flag=True, help='Prompt for confirmation (default)')
@click.option('--log-only', is_flag=True, help='Show what would be reverted without executing')
@click.option('--lock-timeout', type=int, help='Lock timeout in seconds')
@click.option('--revert-dir', help='Directory containing revert scripts')
@click.option('-m', '--modified', is_flag=True, help='Revert to common ancestor with VCS')
@click.option('--strict', is_flag=True, help='Require target revision in strict mode')
@click.pass_context
def revert_command(ctx: click.Context, change: Optional[str], **kwargs) -> None:
    """Revert database changes from the target database."""
    from ..cli import get_sqitch_from_context
    
    sqitch = get_sqitch_from_context(ctx)
    command = RevertCommand(sqitch)
    
    # Build arguments list
    args = []
    
    if change:
        args.append(change)
    
    for key, value in kwargs.items():
        if value is not None:
            if isinstance(value, bool) and value:
                if key == 'no_prompt':
                    args.append('-y')
                elif key == 'modified':
                    args.append('-m')
                else:
                    args.append(f'--{key.replace("_", "-")}')
            elif not isinstance(value, bool):
                args.extend([f'--{key.replace("_", "-")}', str(value)])
    
    exit_code = command.execute(args)
    if exit_code != 0:
        raise click.ClickException(f"Revert command failed with exit code {exit_code}")