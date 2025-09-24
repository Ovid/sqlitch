"""
Bundle command implementation for sqlitch.

This module implements the 'bundle' command which bundles a sqlitch project
for distribution by copying configuration, plan files, and change scripts
to a destination directory.
"""

import shutil
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

import click

from .base import BaseCommand
from ..core.exceptions import SqlitchError, PlanError
from ..core.plan import Plan
from ..core.target import Target


class BundleCommand(BaseCommand):
    """Bundle sqlitch projects for distribution."""
    
    def execute(self, args: List[str]) -> int:
        """
        Execute the bundle command.
        
        Args:
            args: Command arguments
            
        Returns:
            Exit code (0 for success)
        """
        try:
            # Ensure we're in a sqlitch project
            self.require_initialized()
            
            # Parse arguments
            options, targets, changes = self._parse_args(args)
            
            # Validate arguments
            self._validate_args(options, targets, changes)
            
            # Get targets to bundle
            targets_to_bundle = self._get_targets_to_bundle(targets, options)
            
            # Bundle the project
            return self._bundle_project(targets_to_bundle, changes, options)
            
        except Exception as e:
            return self.handle_error(e)
    
    def _parse_args(self, args: List[str]) -> Tuple[Dict[str, Any], List[str], List[str]]:
        """
        Parse command arguments.
        
        Args:
            args: Command arguments
            
        Returns:
            Tuple of (options, targets, changes)
        """
        options = {
            'dest_dir': Path('bundle'),
            'all': False,
            'from': None,
            'to': None,
        }
        
        targets = []
        changes = []
        i = 0
        
        while i < len(args):
            arg = args[i]
            
            if arg in ('--dest-dir', '--dir'):
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options['dest_dir'] = Path(args[i + 1])
                i += 2
            elif arg in ('-a', '--all'):
                options['all'] = True
                i += 1
            elif arg == '--from':
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options['from'] = args[i + 1]
                i += 2
            elif arg == '--to':
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options['to'] = args[i + 1]
                i += 2
            elif arg.startswith('-'):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                # Positional argument - could be target or change
                if self._looks_like_change_spec(arg):
                    # Definitely a change spec
                    changes.append(arg)
                elif changes:
                    # We're already collecting changes, so this is also a change
                    changes.append(arg)
                else:
                    # No changes yet - could be target or change
                    # Check if this looks like a known target/engine name
                    if arg in ('pg', 'mysql', 'sqlite', 'oracle', 'snowflake', 'vertica', 'exasol', 'firebird') or arg.endswith('.plan'):
                        targets.append(arg)
                    else:
                        # Ambiguous - if we have targets, treat as change; otherwise as target
                        if targets:
                            changes.append(arg)
                        else:
                            targets.append(arg)
                i += 1
        
        return options, targets, changes
    
    def _looks_like_change_spec(self, arg: str) -> bool:
        """
        Check if argument looks like a change specification.
        
        Args:
            arg: Argument to check
            
        Returns:
            True if it looks like a change spec
        """
        # Change specs can start with @, contain :, or be HEAD/ROOT
        return (arg.startswith('@') or 
                ':' in arg or 
                arg in ('HEAD', 'ROOT', '@HEAD', '@ROOT'))
    
    def _validate_args(self, options: Dict[str, Any], targets: List[str], changes: List[str]) -> None:
        """
        Validate command arguments.
        
        Args:
            options: Parsed options
            targets: Target list
            changes: Change list
        """
        # Check for conflicting options
        if options.get('all', False) and targets:
            raise SqlitchError("Cannot specify both --all and target arguments")
        
        if len(targets) > 1 and (options.get('from') or options.get('to')):
            self.warn(
                "Use of --to or --from to bundle multiple targets is not recommended.\n"
                "Pass them as arguments after each target argument, instead."
            )
        
        if changes and (options.get('from') or options.get('to')):
            raise SqlitchError("Cannot specify both --from or --to and change arguments")
    
    def _get_targets_to_bundle(self, target_names: List[str], options: Dict[str, Any]) -> List[Target]:
        """
        Get list of targets to bundle.
        
        Args:
            target_names: List of target names
            options: Command options
            
        Returns:
            List of Target objects
        """
        if options['all']:
            # Get all targets from configuration
            targets = []
            target_configs = self.config.get_section('target') or {}
            
            if not target_configs:
                # Use default target
                targets.append(self.get_target())
            else:
                for target_name in target_configs.keys():
                    targets.append(self.get_target(target_name))
            
            return targets
        elif target_names:
            # Use specified targets
            targets = []
            for target_name in target_names:
                targets.append(self.get_target(target_name))
            return targets
        else:
            # Use default target
            return [self.get_target()]
    
    def _bundle_project(self, targets: List[Target], changes: List[str], options: Dict[str, Any]) -> int:
        """
        Bundle the project.
        
        Args:
            targets: List of targets to bundle
            changes: List of change specifications
            options: Command options
            
        Returns:
            Exit code
        """
        dest_dir = options['dest_dir']
        
        self.info(f"Bundling into {dest_dir}")
        
        # Bundle configuration
        self._bundle_config(dest_dir)
        
        # Process targets
        if options['from'] or options['to']:
            # One set of from/to options for all targets
            from_change = options['from']
            to_change = options['to']
            
            for target in targets:
                self._bundle_plan(target, dest_dir, from_change, to_change)
                self._bundle_scripts(target, dest_dir, from_change, to_change)
        else:
            # Separate from/to options for each target
            change_iter = iter(changes)
            
            for target in targets:
                try:
                    from_change = next(change_iter, None)
                    to_change = next(change_iter, None)
                except StopIteration:
                    from_change = to_change = None
                
                self._bundle_plan(target, dest_dir, from_change, to_change)
                self._bundle_scripts(target, dest_dir, from_change, to_change)
        
        return 0
    
    def _bundle_config(self, dest_dir: Path) -> None:
        """
        Bundle configuration file.
        
        Args:
            dest_dir: Destination directory
        """
        self.info("Writing config")
        
        # Find local config file
        config_file = self.config.local_file
        if not config_file or not config_file.exists():
            # Look for sqitch.conf in current directory
            config_file = Path("sqitch.conf")
            if not config_file.exists():
                self.debug("No local configuration file found")
                return
        
        dest_config = dest_dir / config_file.name
        self._copy_if_modified(config_file, dest_config)
    
    def _bundle_plan(self, target: Target, dest_dir: Path, from_change: Optional[str], to_change: Optional[str]) -> None:
        """
        Bundle plan file for target.
        
        Args:
            target: Target to bundle
            dest_dir: Destination directory
            from_change: Starting change (optional)
            to_change: Ending change (optional)
        """
        target_dest_dir = self._dest_top_dir(target, dest_dir)
        
        if from_change is None and to_change is None:
            # Copy entire plan
            self.info("Writing plan")
            plan_file = target.plan_file
            dest_plan = target_dest_dir / plan_file.name
            self._copy_if_modified(plan_file, dest_plan)
        else:
            # Write partial plan
            from_display = from_change or '@ROOT'
            to_display = to_change or '@HEAD'
            self.info(f"Writing plan from {from_display} to {to_display}")
            
            # Create destination directory
            target_dest_dir.mkdir(parents=True, exist_ok=True)
            
            # Write partial plan
            plan = target.plan
            dest_plan = target_dest_dir / target.plan_file.name
            self._write_partial_plan(plan, dest_plan, from_change, to_change)
    
    def _bundle_scripts(self, target: Target, dest_dir: Path, from_change: Optional[str], to_change: Optional[str]) -> None:
        """
        Bundle change scripts for target.
        
        Args:
            target: Target to bundle
            dest_dir: Destination directory
            from_change: Starting change (optional)
            to_change: Ending change (optional)
        """
        plan = target.plan
        
        # Find change range
        from_index = self._find_change_index(plan, from_change or '@ROOT')
        to_index = self._find_change_index(plan, to_change or '@HEAD')
        
        if from_index is None:
            raise SqlitchError(f"Cannot find change {from_change}")
        if to_index is None:
            raise SqlitchError(f"Cannot find change {to_change}")
        
        self.info("Writing scripts")
        
        # Get destination directories
        dest_dirs = self._dest_dirs_for(target, dest_dir)
        
        # Copy scripts for changes in range
        for i in range(from_index, to_index + 1):
            if i >= len(plan.changes):
                break
                
            change = plan.changes[i]
            self.info(f"  + {change.format_name_with_tags()}")
            
            # Determine prefix for reworked changes
            prefix = 'reworked_' if change.is_reworked else ''
            
            # Get path segments for nested directories
            path_segments = change.path_segments
            
            # Copy deploy script
            deploy_file = change.deploy_file(target)
            if deploy_file and deploy_file.exists():
                dest_deploy = dest_dirs[f'{prefix}deploy'] / Path(*path_segments)
                self._copy_if_modified(deploy_file, dest_deploy)
            
            # Copy revert script
            revert_file = change.revert_file(target)
            if revert_file and revert_file.exists():
                dest_revert = dest_dirs[f'{prefix}revert'] / Path(*path_segments)
                self._copy_if_modified(revert_file, dest_revert)
            
            # Copy verify script
            verify_file = change.verify_file(target)
            if verify_file and verify_file.exists():
                dest_verify = dest_dirs[f'{prefix}verify'] / Path(*path_segments)
                self._copy_if_modified(verify_file, dest_verify)
    
    def _dest_top_dir(self, target: Target, dest_dir: Path) -> Path:
        """
        Get destination top directory for target.
        
        Args:
            target: Target
            dest_dir: Base destination directory
            
        Returns:
            Destination top directory
        """
        if target.top_dir == Path('.'):
            return dest_dir
        else:
            return dest_dir / target.top_dir
    
    def _dest_dirs_for(self, target: Target, dest_dir: Path) -> Dict[str, Path]:
        """
        Get destination directories for target scripts.
        
        Args:
            target: Target
            dest_dir: Base destination directory
            
        Returns:
            Dictionary of script type to destination path
        """
        base_dest = dest_dir
        
        return {
            'deploy': base_dest / target.deploy_dir,
            'revert': base_dest / target.revert_dir,
            'verify': base_dest / target.verify_dir,
            'reworked_deploy': base_dest / target.deploy_dir,  # TODO: Handle reworked dirs
            'reworked_revert': base_dest / target.revert_dir,
            'reworked_verify': base_dest / target.verify_dir,
        }
    
    def _copy_if_modified(self, src: Path, dest: Path) -> None:
        """
        Copy file if source is newer than destination.
        
        Args:
            src: Source file
            dest: Destination file
        """
        if not src.exists():
            raise SqlitchError(f"Cannot copy {src}: does not exist")
        
        # Check if we need to copy
        if dest.exists():
            # Skip if destination is newer
            if dest.stat().st_mtime >= src.stat().st_mtime:
                return
        else:
            # Create destination directory
            dest.parent.mkdir(parents=True, exist_ok=True)
        
        self.debug(f"    Copying {src} -> {dest}")
        
        try:
            shutil.copy2(src, dest)
        except Exception as e:
            raise SqlitchError(f'Cannot copy "{src}" to "{dest}": {e}')
    
    def _write_partial_plan(self, plan: Plan, dest_file: Path, from_change: Optional[str], to_change: Optional[str]) -> None:
        """
        Write partial plan to destination file.
        
        Args:
            plan: Source plan
            dest_file: Destination file
            from_change: Starting change (optional)
            to_change: Ending change (optional)
        """
        # Find change range
        from_index = self._find_change_index(plan, from_change or '@ROOT')
        to_index = self._find_change_index(plan, to_change or '@HEAD')
        
        if from_index is None:
            raise SqlitchError(f"Cannot find change {from_change}")
        if to_index is None:
            raise SqlitchError(f"Cannot find change {to_change}")
        
        # Build partial plan content
        lines = []
        
        # Add pragmas
        lines.append(f"%syntax-version={plan.syntax_version}")
        lines.append(f"%project={plan.project}")
        if plan.uri:
            lines.append(f"%uri={plan.uri}")
        lines.append("")  # Empty line after pragmas
        
        # Add changes in range
        for i in range(from_index, to_index + 1):
            if i >= len(plan.changes):
                break
            change = plan.changes[i]
            lines.append(str(change))
            
            # Add any tags for this change
            for tag in plan.tags:
                if tag.change == change:
                    lines.append(str(tag))
        
        # Write to file
        content = '\n'.join(lines) + '\n'
        dest_file.write_text(content, encoding='utf-8')
    
    def _find_change_index(self, plan: Plan, change_spec: str) -> Optional[int]:
        """
        Find index of change in plan.
        
        Args:
            plan: Plan to search
            change_spec: Change specification
            
        Returns:
            Change index or None if not found
        """
        if change_spec in ('@ROOT', 'ROOT'):
            return 0
        elif change_spec in ('@HEAD', 'HEAD'):
            return len(plan.changes) - 1
        elif change_spec.startswith('@'):
            # Tag reference
            tag_name = change_spec[1:]
            # Look for the tag in the plan's tags
            for tag in plan.tags:
                if tag.name == tag_name:
                    # Find the change this tag is associated with
                    if tag.change:
                        for i, change in enumerate(plan.changes):
                            if change == tag.change:
                                return i
            # Also check if any change has this tag name in its tags list
            for i, change in enumerate(plan.changes):
                if hasattr(change, 'tags') and tag_name in change.tags:
                    return i
            return None
        else:
            # Change name
            for i, change in enumerate(plan.changes):
                if change.name == change_spec:
                    return i
            return None


# Click command wrapper for CLI integration
@click.command('bundle')
@click.option('--dest-dir', '--dir', type=click.Path(), help='Destination directory')
@click.option('-a', '--all', is_flag=True, help='Bundle all plans')
@click.option('--from', 'from_change', help='Starting change')
@click.option('--to', 'to_change', help='Ending change')
@click.argument('targets', nargs=-1)
@click.pass_context
def bundle_command(ctx: click.Context, dest_dir: Optional[str], all: bool, 
                  from_change: Optional[str], to_change: Optional[str], 
                  targets: Tuple[str, ...]) -> None:
    """Bundle sqlitch project for distribution."""
    from ..cli import get_sqitch_from_context
    
    sqitch = get_sqitch_from_context(ctx)
    command = BundleCommand(sqitch)
    
    # Build arguments list
    args = []
    
    if dest_dir:
        args.extend(['--dest-dir', dest_dir])
    
    if all:
        args.append('--all')
    
    if from_change:
        args.extend(['--from', from_change])
    
    if to_change:
        args.extend(['--to', to_change])
    
    # Add target arguments
    args.extend(targets)
    
    exit_code = command.execute(args)
    if exit_code != 0:
        raise click.ClickException(f"Bundle command failed with exit code {exit_code}")