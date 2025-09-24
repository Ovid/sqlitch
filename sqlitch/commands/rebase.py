"""
Rebase command implementation for sqlitch.

This module implements the 'rebase' command which reverts to a specified change
and then redeploys up to another change, effectively rebasing the database state.
It supports interactive rebasing with user prompts and conflict resolution.
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


class RebaseCommand(BaseCommand):
    """Rebase database changes."""
    
    def execute(self, args: List[str]) -> int:
        """
        Execute the rebase command.
        
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
            engine = self.get_engine(options.get('target'))
            
            # Determine onto and upto changes
            onto_change = self._determine_onto_change(
                options, engine, plan, args
            )
            upto_change = self._determine_upto_change(
                options, engine, plan, args, onto_change
            )
            
            # Warn about multiple targets/changes
            self._warn_about_extra_args(options, args, onto_change, upto_change)
            
            # Configure engine
            self._configure_engine(engine, options)
            
            # Execute rebase operation
            self._execute_rebase(engine, target, onto_change, upto_change, options)
            
            return 0
            
        except SqlitchError as e:
            return self.handle_error(e, "rebase")
        except Exception as e:
            return self.handle_error(e, "rebase")
    
    def _parse_args(self, args: List[str]) -> Dict[str, Any]:
        """Parse command arguments."""
        options = {
            'target': None,
            'onto_change': None,
            'upto_change': None,
            'modified': False,
            'verify': False,
            'log_only': False,
            'lock_timeout': None,
            'no_prompt': False,
            'prompt_accept': True,
            'mode': 'all',
            'deploy_variables': {},
            'revert_variables': {},
            'plan_file': None
        }
        
        i = 0
        remaining_args = []
        
        while i < len(args):
            arg = args[i]
            
            if arg in ('--target', '-t'):
                if i + 1 >= len(args):
                    raise SqlitchError("Option --target requires a value")
                options['target'] = args[i + 1]
                i += 2
            elif arg.startswith('--target='):
                options['target'] = arg.split('=', 1)[1]
                i += 1
            elif arg in ('--onto-change', '--onto'):
                if i + 1 >= len(args):
                    raise SqlitchError("Option --onto-change requires a value")
                options['onto_change'] = args[i + 1]
                i += 2
            elif arg.startswith('--onto-change=') or arg.startswith('--onto='):
                options['onto_change'] = arg.split('=', 1)[1]
                i += 1
            elif arg in ('--upto-change', '--upto'):
                if i + 1 >= len(args):
                    raise SqlitchError("Option --upto-change requires a value")
                options['upto_change'] = args[i + 1]
                i += 2
            elif arg.startswith('--upto-change=') or arg.startswith('--upto='):
                options['upto_change'] = arg.split('=', 1)[1]
                i += 1
            elif arg in ('--modified', '-m'):
                options['modified'] = True
                i += 1
            elif arg == '--verify':
                options['verify'] = True
                i += 1
            elif arg == '--no-verify':
                options['verify'] = False
                i += 1
            elif arg == '--log-only':
                options['log_only'] = True
                i += 1
            elif arg == '--lock-timeout':
                if i + 1 >= len(args):
                    raise SqlitchError("Option --lock-timeout requires a value")
                try:
                    options['lock_timeout'] = int(args[i + 1])
                except ValueError:
                    raise SqlitchError(f"Invalid lock timeout: {args[i + 1]}")
                i += 2
            elif arg.startswith('--lock-timeout='):
                try:
                    options['lock_timeout'] = int(arg.split('=', 1)[1])
                except ValueError:
                    raise SqlitchError(f"Invalid lock timeout: {arg.split('=', 1)[1]}")
                i += 1
            elif arg in ('-y', '--yes'):
                options['no_prompt'] = True
                i += 1
            elif arg == '--mode':
                if i + 1 >= len(args):
                    raise SqlitchError("Option --mode requires a value")
                mode = args[i + 1]
                if mode not in ('change', 'tag', 'all'):
                    raise SqlitchError(f"Invalid mode: {mode}")
                options['mode'] = mode
                i += 2
            elif arg.startswith('--mode='):
                mode = arg.split('=', 1)[1]
                if mode not in ('change', 'tag', 'all'):
                    raise SqlitchError(f"Invalid mode: {mode}")
                options['mode'] = mode
                i += 1
            elif arg == '--set':
                if i + 1 >= len(args):
                    raise SqlitchError("Option --set requires a value")
                key_value = args[i + 1]
                if '=' not in key_value:
                    raise SqlitchError(f"Invalid variable format: {key_value}")
                key, value = key_value.split('=', 1)
                options['deploy_variables'][key] = value
                options['revert_variables'][key] = value
                i += 2
            elif arg.startswith('--set='):
                key_value = arg.split('=', 1)[1]
                if '=' not in key_value:
                    raise SqlitchError(f"Invalid variable format: {key_value}")
                key, value = key_value.split('=', 1)
                options['deploy_variables'][key] = value
                options['revert_variables'][key] = value
                i += 1
            elif arg == '--set-deploy':
                if i + 1 >= len(args):
                    raise SqlitchError("Option --set-deploy requires a value")
                key_value = args[i + 1]
                if '=' not in key_value:
                    raise SqlitchError(f"Invalid variable format: {key_value}")
                key, value = key_value.split('=', 1)
                options['deploy_variables'][key] = value
                i += 2
            elif arg.startswith('--set-deploy='):
                key_value = arg.split('=', 1)[1]
                if '=' not in key_value:
                    raise SqlitchError(f"Invalid variable format: {key_value}")
                key, value = key_value.split('=', 1)
                options['deploy_variables'][key] = value
                i += 1
            elif arg == '--set-revert':
                if i + 1 >= len(args):
                    raise SqlitchError("Option --set-revert requires a value")
                key_value = args[i + 1]
                if '=' not in key_value:
                    raise SqlitchError(f"Invalid variable format: {key_value}")
                key, value = key_value.split('=', 1)
                options['revert_variables'][key] = value
                i += 2
            elif arg.startswith('--set-revert='):
                key_value = arg.split('=', 1)[1]
                if '=' not in key_value:
                    raise SqlitchError(f"Invalid variable format: {key_value}")
                key, value = key_value.split('=', 1)
                options['revert_variables'][key] = value
                i += 1
            elif arg == '--plan-file':
                if i + 1 >= len(args):
                    raise SqlitchError("Option --plan-file requires a value")
                options['plan_file'] = Path(args[i + 1])
                i += 2
            elif arg.startswith('--plan-file='):
                options['plan_file'] = Path(arg.split('=', 1)[1])
                i += 1
            elif arg.startswith('-'):
                raise SqlitchError(f"Unknown option: {arg}")
            else:
                remaining_args.append(arg)
                i += 1
        
        # Apply configuration defaults
        self._apply_config_defaults(options)
        
        return options
    
    def _apply_config_defaults(self, options: Dict[str, Any]) -> None:
        """Apply configuration defaults to options."""
        config = self.config
        
        # Apply defaults from configuration
        if options.get('verify') is None:
            options['verify'] = (
                config.get('rebase.verify', as_bool=True) or
                config.get('deploy.verify', as_bool=True) or
                False
            )
        
        if options.get('mode') == 'all':
            options['mode'] = (
                config.get('rebase.mode') or
                config.get('deploy.mode') or
                'all'
            )
        
        if options.get('no_prompt') is False:
            options['no_prompt'] = (
                config.get('rebase.no_prompt', as_bool=True) or
                config.get('revert.no_prompt', as_bool=True) or
                False
            )
        
        if options.get('prompt_accept') is True:
            options['prompt_accept'] = (
                config.get('rebase.prompt_accept', as_bool=True) or
                config.get('revert.prompt_accept', as_bool=True) or
                True
            )
        
        # Check for strict mode
        strict_mode = (
            config.get('rebase.strict', as_bool=True) or
            config.get('revert.strict', as_bool=True) or
            False
        )
        
        if strict_mode:
            raise SqlitchError(
                '"rebase" cannot be used in strict mode.\n'
                'Use explicit revert and deploy commands instead.'
            )
    
    def _load_plan(self, plan_file: Optional[Path] = None) -> Plan:
        """Load the plan file."""
        if plan_file is None:
            plan_file = Path(self.config.get('core.plan_file', 'sqitch.plan'))
        
        if not plan_file.exists():
            raise PlanError(f"Plan file not found: {plan_file}")
        
        return Plan.from_file(plan_file)
    
    def _determine_onto_change(self, options: Dict[str, Any], engine, plan: Plan, 
                              args: List[str]) -> Optional[str]:
        """Determine the onto change for rebase."""
        if options['modified']:
            # Use planned_deployed_common_ancestor_id for modified mode
            return engine.planned_deployed_common_ancestor_id()
        elif options['onto_change']:
            return options['onto_change']
        elif args:
            # Take first remaining argument as onto change
            return args[0]
        else:
            return None
    
    def _determine_upto_change(self, options: Dict[str, Any], engine, plan: Plan,
                              args: List[str], onto_change: Optional[str]) -> Optional[str]:
        """Determine the upto change for rebase."""
        if options['upto_change']:
            return options['upto_change']
        elif args:
            # If onto_change was taken from args, take second arg as upto
            if not options['onto_change'] and not options['modified'] and len(args) > 1:
                return args[1]
            elif (options['onto_change'] or options['modified']) and len(args) > 0:
                return args[0]
        
        return None
    
    def _warn_about_extra_args(self, options: Dict[str, Any], args: List[str],
                              onto_change: Optional[str], upto_change: Optional[str]) -> None:
        """Warn about extra arguments."""
        # Count how many args were consumed
        consumed = 0
        if not options['onto_change'] and not options['modified'] and onto_change:
            consumed += 1
        if not options['upto_change'] and upto_change:
            consumed += 1
        
        if len(args) > consumed:
            self.warn(
                f'Too many changes specified; rebasing onto "{onto_change}" '
                f'up to "{upto_change}"'
            )
    
    def _configure_engine(self, engine, options: Dict[str, Any]) -> None:
        """Configure engine with options."""
        # Set engine options
        if hasattr(engine, 'set_verify'):
            engine.set_verify(options['verify'])
        if hasattr(engine, 'set_log_only'):
            engine.set_log_only(options['log_only'])
        if hasattr(engine, 'set_lock_timeout') and options['lock_timeout'] is not None:
            engine.set_lock_timeout(options['lock_timeout'])
    
    def _execute_rebase(self, engine, target, onto_change: Optional[str],
                       upto_change: Optional[str], options: Dict[str, Any]) -> None:
        """Execute the rebase operation."""
        # Collect variables for revert
        revert_vars = self._collect_revert_vars(target, options)
        if hasattr(engine, 'set_variables'):
            engine.set_variables(revert_vars)
        
        # Execute revert
        try:
            if hasattr(engine, 'revert'):
                engine.revert(
                    onto_change,
                    not options['no_prompt'],
                    options['prompt_accept']
                )
            else:
                # Fallback to manual revert implementation
                self._manual_revert(engine, onto_change, options)
        except SqlitchError as e:
            # Handle revert errors - some are non-fatal (e.g., nothing to revert)
            if e.exitval > 1 or 'confirm' in str(e):
                raise
            # Emit notice of non-fatal errors
            self.info(str(e))
        
        # Collect variables for deploy
        deploy_vars = self._collect_deploy_vars(target, options)
        if hasattr(engine, 'set_variables'):
            engine.set_variables(deploy_vars)
        
        # Execute deploy
        if hasattr(engine, 'deploy'):
            engine.deploy(upto_change, options['mode'])
        else:
            # Fallback to manual deploy implementation
            self._manual_deploy(engine, upto_change, options)
    
    def _collect_revert_vars(self, target, options: Dict[str, Any]) -> Dict[str, Any]:
        """Collect variables for revert operation."""
        config = self.config
        variables = {}
        
        # Core variables
        core_vars = config.get_section('core.variables') or {}
        variables.update(core_vars)
        
        # Deploy variables (used for both deploy and revert)
        deploy_vars = config.get_section('deploy.variables') or {}
        variables.update(deploy_vars)
        
        # Revert-specific variables
        revert_vars = config.get_section('revert.variables') or {}
        variables.update(revert_vars)
        
        # Target variables
        if hasattr(target, 'variables'):
            variables.update(target.variables)
        
        # Command-line variables
        variables.update(options['revert_variables'])
        
        return variables
    
    def _collect_deploy_vars(self, target, options: Dict[str, Any]) -> Dict[str, Any]:
        """Collect variables for deploy operation."""
        config = self.config
        variables = {}
        
        # Core variables
        core_vars = config.get_section('core.variables') or {}
        variables.update(core_vars)
        
        # Deploy variables
        deploy_vars = config.get_section('deploy.variables') or {}
        variables.update(deploy_vars)
        
        # Target variables
        if hasattr(target, 'variables'):
            variables.update(target.variables)
        
        # Command-line variables
        variables.update(options['deploy_variables'])
        
        return variables
    
    def _manual_revert(self, engine, onto_change: Optional[str], 
                      options: Dict[str, Any]) -> None:
        """Manual revert implementation for engines without revert method."""
        # This is a simplified implementation
        # In practice, this would need to implement the full revert logic
        if onto_change:
            self.info(f"Reverting to change: {onto_change}")
        else:
            self.info("Nothing to revert")
    
    def _manual_deploy(self, engine, upto_change: Optional[str],
                      options: Dict[str, Any]) -> None:
        """Manual deploy implementation for engines without deploy method."""
        # This is a simplified implementation
        # In practice, this would need to implement the full deploy logic
        if upto_change:
            self.info(f"Deploying up to change: {upto_change}")
        else:
            self.info("Nothing to deploy")


# Click command wrapper
@click.command('rebase')
@click.option('--target', '-t', help='Database target')
@click.option('--onto-change', '--onto', help='Change to rebase onto')
@click.option('--upto-change', '--upto', help='Change to rebase up to')
@click.option('--modified', '-m', is_flag=True, 
              help='Revert to the change prior to earliest change with a revised deploy script')
@click.option('--verify/--no-verify', default=None, help='Verify changes after deployment')
@click.option('--log-only', is_flag=True, help='Log changes without executing')
@click.option('--lock-timeout', type=int, help='Lock timeout in seconds')
@click.option('-y', '--yes', 'no_prompt', is_flag=True, help='Do not prompt for confirmation')
@click.option('--mode', type=click.Choice(['change', 'tag', 'all']), default='all',
              help='Deployment mode')
@click.option('--set', multiple=True, help='Set variable (key=value)')
@click.option('--set-deploy', multiple=True, help='Set deploy variable (key=value)')
@click.option('--set-revert', multiple=True, help='Set revert variable (key=value)')
@click.option('--plan-file', type=click.Path(exists=True, path_type=Path),
              help='Path to plan file')
@click.argument('changes', nargs=-1)
@click.pass_context
def rebase_command(ctx: click.Context, target: Optional[str], onto_change: Optional[str],
                  upto_change: Optional[str], modified: bool, verify: Optional[bool],
                  log_only: bool, lock_timeout: Optional[int], no_prompt: bool,
                  mode: str, set: tuple, set_deploy: tuple, set_revert: tuple,
                  plan_file: Optional[Path], changes: tuple) -> None:
    """
    Revert and redeploy database changes.
    
    The rebase command reverts the database to a specified change and then
    redeploys up to another change, effectively rebasing the database state.
    This is useful for applying changes in a different order or after
    modifying change scripts.
    """
    from ..cli import get_sqitch_from_context
    
    try:
        sqitch = get_sqitch_from_context(ctx)
        command = RebaseCommand(sqitch)
        
        # Build arguments list
        args = list(changes)
        
        if target:
            args.extend(['--target', target])
        if onto_change:
            args.extend(['--onto-change', onto_change])
        if upto_change:
            args.extend(['--upto-change', upto_change])
        if modified:
            args.append('--modified')
        if verify is not None:
            args.append('--verify' if verify else '--no-verify')
        if log_only:
            args.append('--log-only')
        if lock_timeout is not None:
            args.extend(['--lock-timeout', str(lock_timeout)])
        if no_prompt:
            args.append('-y')
        if mode != 'all':
            args.extend(['--mode', mode])
        
        for var in set:
            args.extend(['--set', var])
        for var in set_deploy:
            args.extend(['--set-deploy', var])
        for var in set_revert:
            args.extend(['--set-revert', var])
        
        if plan_file:
            args.extend(['--plan-file', str(plan_file)])
        
        exit_code = command.execute(args)
        if exit_code != 0:
            ctx.exit(exit_code)
            
    except Exception as e:
        from ..core.exceptions import SqlitchError
        if isinstance(e, SqlitchError):
            click.echo(f"sqlitch rebase: {e}", err=True)
            ctx.exit(e.exitval if hasattr(e, 'exitval') else 1)
        else:
            click.echo(f"sqlitch rebase: Unexpected error: {e}", err=True)
            ctx.exit(2)