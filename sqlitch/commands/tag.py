"""
Tag command implementation for sqlitch.

This module implements the 'tag' command which adds tags to changes in the sqlitch plan
or lists existing tags.
"""

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

import click

from .base import BaseCommand
from ..core.change import Tag
from ..core.exceptions import SqlitchError, PlanError
from ..core.plan import Plan
from ..core.types import validate_tag_name


class TagCommand(BaseCommand):
    """Add or list tags in sqlitch plans."""
    
    def execute(self, args: List[str]) -> int:
        """
        Execute the tag command.
        
        Args:
            args: Command arguments
            
        Returns:
            Exit code (0 for success)
        """
        try:
            # Ensure we're in a sqlitch project
            self.require_initialized()
            
            # Parse arguments
            tag_name, change_name, options = self._parse_args(args)
            
            if tag_name:
                # Add tag mode
                return self._add_tag(tag_name, change_name, options)
            else:
                # List tags mode
                return self._list_tags(options)
            
        except Exception as e:
            return self.handle_error(e)
    
    def _parse_args(self, args: List[str]) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
        """
        Parse command arguments.
        
        Args:
            args: Command arguments
            
        Returns:
            Tuple of (tag_name, change_name, options)
        """
        options = {
            'note': [],
            'all': False,
        }
        
        tag_name = None
        change_name = None
        i = 0
        
        while i < len(args):
            arg = args[i]
            
            if arg in ('-t', '--tag', '--tag-name'):
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                tag_name = args[i + 1]
                i += 2
            elif arg in ('-c', '--change', '--change-name'):
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                change_name = args[i + 1]
                i += 2
            elif arg in ('-n', '-m', '--note'):
                if i + 1 >= len(args):
                    raise SqlitchError(f"Option {arg} requires a value")
                options['note'].append(args[i + 1])
                i += 2
            elif arg in ('-a', '--all'):
                options['all'] = True
                i += 1
            elif arg.startswith('-'):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                # Positional argument
                if tag_name is None:
                    tag_name = arg
                elif change_name is None:
                    change_name = arg
                else:
                    raise SqlitchError(f"Unexpected argument: {arg}")
                i += 1
        
        return tag_name, change_name, options
    
    def _add_tag(self, tag_name: str, change_name: Optional[str], options: Dict[str, Any]) -> int:
        """
        Add a tag to the plan.
        
        Args:
            tag_name: Name of the tag to add
            change_name: Name of the change to tag (optional)
            options: Command options
            
        Returns:
            Exit code
        """
        # Validate user info
        self.validate_user_info()
        
        # Remove @ prefix if present
        if tag_name.startswith('@'):
            tag_name = tag_name[1:]
        
        # Validate tag name
        validate_tag_name(tag_name)
        
        # Get targets
        targets = self._get_targets(options)
        
        # Process each target
        tags_created = []
        plans_updated = []
        
        for target in targets:
            plan = Plan.from_file(target.plan_file)
            
            # Check if tag already exists
            if tag_name in plan._tag_index:
                raise SqlitchError(f'Tag "@{tag_name}" already exists')
            
            # Find the change to tag
            change = None
            if change_name:
                change = plan.get_change(change_name)
                if not change:
                    raise SqlitchError(f'Unknown change: "{change_name}"')
            else:
                # Tag the last change
                if not plan.changes:
                    raise SqlitchError(f'Cannot apply tag "@{tag_name}" to a plan with no changes')
                change = plan.changes[-1]
            
            # Create tag
            tag = self._create_tag(tag_name, change, options)
            tags_created.append(tag)
            
            # Add to plan
            plan.add_tag(tag)
            
            # Associate tag with change
            if tag_name not in change.tags:
                change.tags.append(tag_name)
            
            # Save plan
            plan.save()
            plans_updated.append(plan.file)
        
        # Request note if not provided and we have tags
        if tags_created and not options.get('note'):
            note = self._request_note_for_tag()
            if note:
                # Update all tags with the note
                for tag in tags_created:
                    tag.note = note
                
                # Re-save all plans
                for target in targets:
                    plan = Plan.from_file(target.plan_file)
                    plan.save()
        
        # Report success
        for i, target in enumerate(targets):
            tag = tags_created[i]
            plan_file = plans_updated[i]
            self.info(f'Tagged "{tag.change.name}" with @{tag.name} in {plan_file}')
        
        return 0
    
    def _list_tags(self, options: Dict[str, Any]) -> int:
        """
        List tags in the plan.
        
        Args:
            options: Command options
            
        Returns:
            Exit code
        """
        # Get targets
        targets = self._get_targets(options)
        
        # Collect unique tags
        seen_tags = set()
        
        for target in targets:
            plan = Plan.from_file(target.plan_file)
            
            for tag in plan.tags:
                tag_name = f"@{tag.name}"
                if tag_name not in seen_tags:
                    self.emit(tag_name)
                    seen_tags.add(tag_name)
        
        return 0
    
    def _get_targets(self, options: Dict[str, Any]) -> List[Any]:
        """
        Get list of targets to process.
        
        Args:
            options: Command options
            
        Returns:
            List of targets
        """
        if options.get('all', False):
            # Get all targets from configuration
            targets = []
            target_names = self.config.get_section('target') or {}
            
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
    
    def _create_tag(self, name: str, change, options: Dict[str, Any]) -> Tag:
        """
        Create a new Tag object.
        
        Args:
            name: Tag name
            change: Change object to tag
            options: Command options
            
        Returns:
            New Tag object
        """
        # Create note
        note_parts = options.get('note', [])
        note = '\n\n'.join(note_parts) if note_parts else ''
        
        # Get user info
        user_name = self.sqitch.user_name
        user_email = self.sqitch.user_email
        
        if not user_name or not user_email:
            raise SqlitchError("User name and email must be configured")
        
        return Tag(
            name=name,
            note=note,
            timestamp=datetime.now(timezone.utc),
            planner_name=user_name,
            planner_email=user_email,
            change=change
        )
    
    def _request_note_for_tag(self) -> str:
        """
        Request a note for the tag from the user.
        
        Returns:
            Tag note
        """
        try:
            # Try to get note from editor
            return self.sqitch.request_note_for("tag")
        except Exception:
            # Fall back to simple prompt
            return self.prompt("Tag note (optional): ", default="")


# Click command wrapper for CLI integration
@click.command('tag')
@click.argument('tag_name', required=False)
@click.argument('change_name', required=False)
@click.option('-t', '--tag', '--tag-name', help='Tag name')
@click.option('-c', '--change', '--change-name', help='Change name to tag')
@click.option('-n', '-m', '--note', multiple=True, help='Tag note')
@click.option('-a', '--all', is_flag=True, help='Tag in all plans')
@click.pass_context
def tag_command(ctx: click.Context, tag_name: Optional[str], change_name: Optional[str], **kwargs) -> None:
    """Add or list tags in sqlitch plans."""
    from ..cli import get_sqitch_from_context
    
    sqitch = get_sqitch_from_context(ctx)
    command = TagCommand(sqitch)
    
    # Build arguments list
    args = []
    
    if tag_name:
        args.append(tag_name)
    
    if change_name:
        args.append(change_name)
    
    # Handle options
    if kwargs.get('tag'):
        args.extend(['--tag', kwargs['tag']])
    
    if kwargs.get('change'):
        args.extend(['--change', kwargs['change']])
    
    for note in kwargs.get('note', []):
        args.extend(['--note', note])
    
    if kwargs.get('all'):
        args.append('--all')
    
    exit_code = command.execute(args)
    if exit_code != 0:
        raise click.ClickException(f"Tag command failed with exit code {exit_code}")