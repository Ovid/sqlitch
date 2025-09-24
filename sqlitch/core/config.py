"""
Configuration management for sqlitch.

This module provides configuration file parsing, hierarchy management,
and validation for sqitch configuration files. It supports the same
INI-style format as Perl sqitch with proper type coercion and validation.
"""

import configparser
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple, Set
from dataclasses import dataclass, field

from .exceptions import ConfigurationError
from .types import (
    ConfigValue, ConfigDict, EngineType, URI, 
    validate_config_key, validate_uri, validate_email,
    coerce_config_value, sanitize_connection_string
)
from .target import Target


@dataclass
class ConfigSource:
    """Represents a configuration source with its priority and path."""
    path: Optional[Path]
    priority: int
    source_type: str  # 'system', 'global', 'local', 'command-line'
    parser: Optional[configparser.ConfigParser] = None


class Config:
    """
    Configuration management for sqlitch.
    
    Handles loading and merging configuration from multiple sources
    in the correct priority order: system < global < local < command-line.
    """
    
    def __init__(self, config_files: Optional[List[Path]] = None, 
                 cli_options: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize configuration manager.
        
        Args:
            config_files: Explicit list of config files to load
            cli_options: Command-line options to override config values
        """
        self._sources: List[ConfigSource] = []
        self._merged_config: Dict[str, Any] = {}
        self._cli_options = cli_options or {}
        
        # Load configuration sources in priority order
        if config_files:
            self._load_explicit_configs(config_files)
        else:
            self._load_default_configs()
        
        # Merge all configurations
        self._merge_configurations()
    
    def _load_explicit_configs(self, config_files: List[Path]) -> None:
        """Load explicitly specified configuration files."""
        for i, config_file in enumerate(config_files):
            if config_file.exists():
                parser = self._load_config_file(config_file)
                source = ConfigSource(
                    path=config_file,
                    priority=100 + i,  # Higher priority than defaults
                    source_type='explicit',
                    parser=parser
                )
                self._sources.append(source)
    
    def _load_default_configs(self) -> None:
        """Load configuration files from default locations."""
        # System-wide configuration
        system_paths = self._get_system_config_paths()
        for path in system_paths:
            if path.exists():
                parser = self._load_config_file(path)
                source = ConfigSource(
                    path=path,
                    priority=10,
                    source_type='system',
                    parser=parser
                )
                self._sources.append(source)
        
        # Global user configuration
        global_path = self._get_global_config_path()
        if global_path and global_path.exists():
            parser = self._load_config_file(global_path)
            source = ConfigSource(
                path=global_path,
                priority=20,
                source_type='global',
                parser=parser
            )
            self._sources.append(source)
        
        # Local project configuration
        local_paths = self._get_local_config_paths()
        for path in local_paths:
            if path.exists():
                parser = self._load_config_file(path)
                source = ConfigSource(
                    path=path,
                    priority=30,
                    source_type='local',
                    parser=parser
                )
                self._sources.append(source)
    
    def _get_system_config_paths(self) -> List[Path]:
        """Get system-wide configuration file paths."""
        paths = []
        
        if sys.platform.startswith('win'):
            # Windows system paths
            if 'PROGRAMFILES' in os.environ:
                paths.append(Path(os.environ['PROGRAMFILES']) / 'Sqlitch' / 'etc' / 'sqitch.conf')
        else:
            # Unix-like system paths
            paths.extend([
                Path('/etc/sqitch/sqitch.conf'),
                Path('/usr/local/etc/sqitch/sqitch.conf'),
            ])
        
        return paths
    
    def _get_global_config_path(self) -> Optional[Path]:
        """Get global user configuration file path."""
        home = Path.home()
        
        if sys.platform.startswith('win'):
            # Windows user config
            return home / '.sqlitch' / 'sqitch.conf'
        else:
            # Unix-like user config
            xdg_config = os.environ.get('XDG_CONFIG_HOME')
            if xdg_config:
                return Path(xdg_config) / 'sqlitch' / 'sqitch.conf'
            else:
                return home / '.config' / 'sqlitch' / 'sqitch.conf'
    
    def _get_local_config_paths(self) -> List[Path]:
        """Get local project configuration file paths."""
        paths = []
        current = Path.cwd()
        
        # Look for sqitch.conf in current directory and parent directories
        while current != current.parent:
            config_path = current / 'sqitch.conf'
            if config_path.exists():
                paths.append(config_path)
                break  # Only use the first one found
            current = current.parent
        
        return paths
    
    def _load_config_file(self, config_path: Path) -> configparser.ConfigParser:
        """
        Load and parse a configuration file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Parsed configuration
            
        Raises:
            ConfigurationError: If file cannot be parsed
        """
        parser = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation(),
            allow_no_value=True,
            delimiters=('=',),
            comment_prefixes=('#', ';'),
            # Allow section names with spaces and quotes
            strict=False
        )
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Pre-process content to handle quoted section names
                content = self._preprocess_config_content(content)
                parser.read_string(content, source=str(config_path))
        except (OSError, IOError) as e:
            raise ConfigurationError(
                f"Cannot read configuration file: {e}",
                config_file=str(config_path)
            )
        except configparser.Error as e:
            raise ConfigurationError(
                f"Invalid configuration syntax: {e}",
                config_file=str(config_path)
            )
        
        return parser
    
    def _preprocess_config_content(self, content: str) -> str:
        """
        Preprocess configuration content to handle quoted section names.
        
        Converts [engine "pg"] to [engine "pg"] format that ConfigParser can handle.
        """
        import re
        
        # Pattern to match section headers with quoted subsections
        pattern = r'^\[([a-zA-Z][a-zA-Z0-9._]*)\s+"([^"]+)"\]'
        
        def replace_section(match):
            main_section = match.group(1)
            sub_section = match.group(2)
            return f'[{main_section} "{sub_section}"]'
        
        return re.sub(pattern, replace_section, content, flags=re.MULTILINE)
    
    def _merge_configurations(self) -> None:
        """Merge all configuration sources in priority order."""
        # Sort sources by priority
        self._sources.sort(key=lambda s: s.priority)
        
        merged = {}
        
        # Merge each source
        for source in self._sources:
            if source.parser:
                self._merge_parser_into_dict(source.parser, merged)
        
        # Apply command-line overrides
        if self._cli_options:
            self._apply_cli_overrides(merged)
        
        self._merged_config = merged
    
    def _merge_parser_into_dict(self, parser: configparser.ConfigParser, 
                               target: Dict[str, Any]) -> None:
        """Merge a ConfigParser into a dictionary."""
        for section_name in parser.sections():
            # Handle quoted section names like [engine "pg"]
            if ' ' in section_name and '"' in section_name:
                # This is a subsection like [engine "pg"]
                main_section, sub_section = self._parse_subsection(section_name)
                if main_section not in target:
                    target[main_section] = {}
                if sub_section not in target[main_section]:
                    target[main_section][sub_section] = {}
                
                for key, value in parser.items(section_name):
                    target[main_section][sub_section][key] = value
            else:
                # Regular section
                if section_name not in target:
                    target[section_name] = {}
                
                for key, value in parser.items(section_name):
                    target[section_name][key] = value
    
    def _parse_subsection(self, section_name: str) -> Tuple[str, str]:
        """Parse section name like 'engine "pg"' into main and sub sections."""
        parts = section_name.split(' ', 1)
        if len(parts) == 2:
            main = parts[0]
            sub = parts[1].strip('"')
            return main, sub
        return section_name, ''
    
    def _apply_cli_overrides(self, config: Dict[str, Any]) -> None:
        """Apply command-line option overrides to configuration."""
        for key, value in self._cli_options.items():
            self._set_nested_value(config, key, value)
    
    def _set_nested_value(self, config: Dict[str, Any], key: str, value: Any) -> None:
        """Set a nested configuration value using dot notation."""
        parts = key.split('.')
        current = config
        
        # Navigate to the parent of the target key
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        
        # Set the final value
        current[parts[-1]] = value
    
    def get(self, key: str, default: Any = None, expected_type: Optional[type] = None, as_bool: bool = False) -> Any:
        """
        Get configuration value with dot notation.
        
        Args:
            key: Configuration key in dot notation (e.g., 'core.engine')
            default: Default value if key not found
            expected_type: Expected type for value coercion
            as_bool: Whether to coerce value to boolean
            
        Returns:
            Configuration value, coerced to expected type if specified
            
        Raises:
            ConfigurationError: If key is invalid or type coercion fails
        """
        if not validate_config_key(key):
            raise ConfigurationError(f"Invalid configuration key: {key}")
        
        value = self._get_nested_value(self._merged_config, key)
        
        if value is None:
            return default
        
        # Boolean coercion if requested
        if as_bool:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        
        # Type coercion if requested
        if expected_type and isinstance(value, str):
            try:
                return coerce_config_value(value, expected_type)
            except (ValueError, TypeError) as e:
                raise ConfigurationError(
                    f"Cannot convert '{value}' to {expected_type.__name__} for key '{key}': {e}",
                    config_key=key
                )
        
        return value
    
    def _get_nested_value(self, config: Dict[str, Any], key: str) -> Any:
        """Get nested value using dot notation."""
        parts = key.split('.')
        current = config
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        
        return current
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value.
        
        Args:
            key: Configuration key in dot notation
            value: Value to set
            
        Raises:
            ConfigurationError: If key is invalid
        """
        if not validate_config_key(key):
            raise ConfigurationError(f"Invalid configuration key: {key}")
        
        self._set_nested_value(self._merged_config, key, value)
    
    def get_target(self, name: str) -> Target:
        """
        Get target configuration.
        
        Args:
            name: Target name
            
        Returns:
            Target configuration object
            
        Raises:
            ConfigurationError: If target not found or invalid
        """
        from pathlib import Path
        
        # Look for target in targets section
        target_config = self._get_nested_value(self._merged_config, f'target.{name}')
        
        if not target_config:
            # Check if it's the default target
            if name == 'default':
                engine = self.get('core.engine')
                if not engine:
                    raise ConfigurationError("No default engine configured")
                
                # Use engine-specific configuration
                engine_config = self._get_nested_value(self._merged_config, f'engine.{engine}')
                if not engine_config:
                    raise ConfigurationError(f"No configuration found for engine '{engine}'")
                
                uri = engine_config.get('target')
                if not uri:
                    raise ConfigurationError(f"No target URI configured for engine '{engine}'")
                
                # Get directory configuration
                top_dir = Path(self.get('core.top_dir', '.'))
                deploy_dir = Path(self.get('core.deploy_dir', 'deploy'))
                revert_dir = Path(self.get('core.revert_dir', 'revert'))
                verify_dir = Path(self.get('core.verify_dir', 'verify'))
                plan_file = Path(self.get('core.plan_file', 'sqitch.plan'))
                
                return Target(
                    name='default',
                    uri=uri,
                    engine=engine,
                    registry=engine_config.get('registry'),
                    client=engine_config.get('client'),
                    top_dir=top_dir,
                    deploy_dir=deploy_dir,
                    revert_dir=revert_dir,
                    verify_dir=verify_dir,
                    plan_file=plan_file
                )
            else:
                raise ConfigurationError(f"Target '{name}' not found")
        
        # Validate required fields
        uri = target_config.get('uri')
        if not uri:
            raise ConfigurationError(f"Target '{name}' missing required 'uri' field")
        
        if not validate_uri(uri):
            raise ConfigurationError(f"Invalid URI for target '{name}': {uri}")
        
        # Extract engine from URI or use configured engine
        engine = target_config.get('engine')
        if not engine:
            if uri.startswith('db:pg:'):
                engine = 'pg'
            elif uri.startswith('db:mysql:'):
                engine = 'mysql'
            elif uri.startswith('db:sqlite:'):
                engine = 'sqlite'
            else:
                engine = self.get('core.engine', 'pg')
        
        # Get directory configuration
        top_dir = Path(target_config.get('top_dir', self.get('core.top_dir', '.')))
        deploy_dir = Path(target_config.get('deploy_dir', self.get('core.deploy_dir', 'deploy')))
        revert_dir = Path(target_config.get('revert_dir', self.get('core.revert_dir', 'revert')))
        verify_dir = Path(target_config.get('verify_dir', self.get('core.verify_dir', 'verify')))
        plan_file = Path(target_config.get('plan_file', self.get('core.plan_file', 'sqitch.plan')))
        
        return Target(
            name=name,
            uri=uri,
            engine=engine,
            registry=target_config.get('registry'),
            client=target_config.get('client'),
            top_dir=top_dir,
            deploy_dir=deploy_dir,
            revert_dir=revert_dir,
            verify_dir=verify_dir,
            plan_file=plan_file
        )
    
    def get_engine_config(self, engine_type: EngineType) -> Dict[str, Any]:
        """
        Get engine-specific configuration.
        
        Args:
            engine_type: Database engine type
            
        Returns:
            Engine configuration dictionary
        """
        engine_config = self._get_nested_value(self._merged_config, f'engine.{engine_type}')
        return engine_config or {}
    
    def get_user_name(self) -> Optional[str]:
        """Get configured user name."""
        return self.get('user.name')
    
    def get_user_email(self) -> Optional[str]:
        """Get configured user email."""
        email = self.get('user.email')
        if email and not validate_email(email):
            raise ConfigurationError(f"Invalid email address: {email}")
        return email
    
    def get_core_config(self) -> Dict[str, Any]:
        """Get core configuration section."""
        return self._get_nested_value(self._merged_config, 'core') or {}
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Get configuration section.
        
        Args:
            section: Section name (e.g., 'add.variables')
            
        Returns:
            Dictionary of section values
        """
        return self._get_nested_value(self._merged_config, section) or {}
    
    def list_targets(self) -> List[str]:
        """List all configured targets."""
        targets = []
        
        # Get explicit targets
        target_section = self._get_nested_value(self._merged_config, 'target')
        if target_section:
            targets.extend(target_section.keys())
        
        # Add default target if engine is configured
        if self.get('core.engine') and 'default' not in targets:
            targets.append('default')
        
        return sorted(targets)
    
    def list_engines(self) -> List[str]:
        """List all configured engines."""
        engine_section = self._get_nested_value(self._merged_config, 'engine')
        if engine_section:
            return sorted(engine_section.keys())
        return []
    
    def validate(self) -> List[str]:
        """
        Validate configuration and return list of issues.
        
        Returns:
            List of validation error messages
        """
        issues = []
        
        # Validate user email if present
        try:
            email = self.get_user_email()
            if email and not validate_email(email):
                issues.append(f"Invalid user email: {email}")
        except ConfigurationError as e:
            issues.append(str(e))
        
        # Validate target URIs
        for target_name in self.list_targets():
            try:
                target = self.get_target(target_name)
                if not validate_uri(target.uri):
                    issues.append(f"Invalid URI for target '{target_name}': {target.uri}")
            except ConfigurationError as e:
                issues.append(str(e))
        
        return issues
    
    def to_dict(self) -> Dict[str, Any]:
        """Get configuration as dictionary."""
        return self._merged_config.copy()
    
    def get_config_sources(self) -> List[ConfigSource]:
        """Get list of configuration sources in priority order."""
        return self._sources.copy()
    
    @property
    def local_file(self) -> Optional[Path]:
        """Get the local configuration file path."""
        # Find the local configuration source
        for source in self._sources:
            if source.source_type == 'local' and source.path:
                return source.path
        
        # If no local config found, check for sqitch.conf in current directory
        local_config = Path('sqitch.conf')
        if local_config.exists():
            return local_config
        
        return None
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        sources = [s.source_type for s in self._sources]
        return f"Config(sources={sources})"