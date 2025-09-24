"""
Show command implementation for sqlitch.

This module provides the ShowCommand class for displaying information about
sqitch objects including changes, tags, and script contents.
"""

import sys
from pathlib import Path
from typing import List, Optional

import click

from ..core.exceptions import SqlitchError
from .base import BaseCommand


class ShowCommand(BaseCommand):
    """
    Command to show information about sqitch objects.
    
    Supports showing:
    - change: Change metadata and information
    - tag: Tag metadata and information  
    - deploy: Deploy script contents
    - revert: Revert script contents
    - verify: Verify script contents
    """
    
    def execute(self, args: List[str]) -> int:
        """
        Execute the show command.
        
        Args:
            args: Command arguments [type, object_key, options...]
            
        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        try:
            # Parse arguments
            target_name = None
            exists_only = False
            object_type = None
            object_key = None
            
            i = 0
            while i < len(args):
                arg = args[i]
                
                if arg in ('-t', '--target'):
                    if i + 1 >= len(args):
                        raise SqlitchError("Option --target requires a value")
                    target_name = args[i + 1]
                    i += 2
                elif arg in ('-e', '--exists'):
                    exists_only = True
                    i += 1
                elif arg.startswith('-'):
                    raise SqlitchError(f"Unknown option: {arg}")
                else:
                    # Positional arguments
                    if object_type is None:
                        object_type = arg
                    elif object_key is None:
                        object_key = arg
                    else:
                        raise SqlitchError("Too many arguments")
                    i += 1
            
            # Validate required arguments
            if not object_type or not object_key:
                self.vent("Usage: sqlitch show [options] <type> <object>")
                self.vent("")
                self.vent("Types:")
                self.vent("  change   Show change information")
                self.vent("  tag      Show tag information")
                self.vent("  deploy   Show deploy script contents")
                self.vent("  revert   Show revert script contents")
                self.vent("  verify   Show verify script contents")
                self.vent("")
                self.vent("Options:")
                self.vent("  -t, --target <target>  Target database")
                self.vent("  -e, --exists          Check existence only")
                return 2
            
            # Validate object type
            valid_types = ['change', 'tag', 'deploy', 'revert', 'verify']
            if object_type not in valid_types:
                raise SqlitchError(f'Unknown object type "{object_type}"')
            
            # Get target and plan
            target = self.get_target(target_name)
            plan = target.plan
            
            # Handle tags
            if object_type == 'tag':
                return self._show_tag(plan, object_key, exists_only)
            
            # Handle changes and scripts
            return self._show_change_or_script(plan, target, object_type, object_key, exists_only)
            
        except SqlitchError as e:
            return self.handle_error(e)
        except Exception as e:
            return self.handle_error(e, "showing object")
    
    def _show_tag(self, plan, tag_key: str, exists_only: bool) -> int:
        """Show tag information."""
        # Determine if this is a tag ID or name
        is_id = len(tag_key) == 40 and all(c in '0123456789abcdef' for c in tag_key.lower())
        
        # Find the tag
        tag = None
        if is_id:
            # Look for tag by ID in the plan's tags list
            for t in plan.tags:
                if hasattr(t, 'id') and t.id == tag_key:
                    tag = t
                    break
        else:
            # Look for tag by name (remove @ prefix if present)
            tag_name = tag_key.lstrip('@')
            for t in plan.tags:
                if t.name == tag_name:
                    tag = t
                    break
        
        if not tag:
            if exists_only:
                return 1
            raise SqlitchError(f'Unknown tag "{tag_key}"')
        
        if not exists_only:
            self.emit(tag.info(plan))
        
        return 0
    
    def _show_change_or_script(self, plan, target, object_type: str, object_key: str, exists_only: bool) -> int:
        """Show change information or script contents."""
        # Find the change
        change = plan.get(object_key)
        if not change:
            if exists_only:
                return 1
            raise SqlitchError(f'Unknown change "{object_key}"')
        
        if object_type == 'change':
            # Show change information
            if not exists_only:
                self.emit(change.info(plan))
            return 0
        
        # Show script contents
        if object_type == 'deploy':
            script_path = change.deploy_file(target)
        elif object_type == 'revert':
            script_path = change.revert_file(target)
        elif object_type == 'verify':
            script_path = change.verify_file(target)
        else:
            raise SqlitchError(f'Unknown object type "{object_type}"')
        
        # Check if file exists
        if not script_path.exists():
            if exists_only:
                return 1
            raise SqlitchError(f'File "{script_path}" does not exist')
        
        if script_path.is_dir():
            raise SqlitchError(f'"{script_path}" is not a file')
        
        if exists_only:
            return 0
        
        # Read and output file contents
        try:
            # Read as binary to avoid encoding issues, matching Perl behavior
            with open(script_path, 'rb') as f:
                content = f.read()
            
            # Write to stdout as binary
            sys.stdout.buffer.write(content)
            
        except IOError as e:
            raise SqlitchError(f'Cannot read file "{script_path}": {e}')
        
        return 0


@click.command()
@click.argument('object_type')
@click.argument('object_key')
@click.option('-t', '--target', help='Target database')
@click.option('-e', '--exists', is_flag=True, help='Check existence only')
@click.pass_context
def show_command(ctx: click.Context, object_type: str, object_key: str, 
                target: Optional[str], exists: bool) -> None:
    """
    Show object information or script contents.
    
    OBJECT_TYPE can be: change, tag, deploy, revert, verify
    OBJECT_KEY is the change name, tag name, or change ID
    """
    from ..cli import get_sqitch_from_context
    
    sqitch = get_sqitch_from_context(ctx)
    command = ShowCommand(sqitch)
    
    # Build arguments list
    args = [object_type, object_key]
    if target:
        args.extend(['--target', target])
    if exists:
        args.append('--exists')
    
    exit_code = command.execute(args)
    if exit_code != 0:
        ctx.exit(exit_code)