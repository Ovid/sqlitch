"""
Template processing utilities for sqlitch.

This module provides template processing functionality using Jinja2,
including built-in templates for different database engines and
support for custom template directories.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

try:
    from jinja2 import Environment, FileSystemLoader, BaseLoader, Template
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

from ..core.exceptions import SqlitchError
from ..core.types import EngineType, OperationType


class TemplateError(SqlitchError):
    """Template processing error."""
    pass


@dataclass
class TemplateContext:
    """Context for template rendering."""
    project: str
    change: str
    engine: str
    requires: List[str] = None
    conflicts: List[str] = None
    
    def __post_init__(self):
        if self.requires is None:
            self.requires = []
        if self.conflicts is None:
            self.conflicts = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for template rendering."""
        return {
            'project': self.project,
            'change': self.change,
            'engine': self.engine,
            'requires': self.requires,
            'conflicts': self.conflicts,
        }


class BuiltinTemplateLoader(BaseLoader):
    """Loader for built-in templates."""
    
    def __init__(self):
        self.templates = self._load_builtin_templates()
    
    def _load_builtin_templates(self) -> Dict[str, str]:
        """Load built-in template content."""
        return {
            # PostgreSQL templates
            'deploy/pg.tmpl': '''-- Deploy [% project %]:[% change %] to [% engine %]
[% FOREACH item IN requires -%]
-- requires: [% item %]
[% END -%]
[% FOREACH item IN conflicts -%]
-- conflicts: [% item %]
[% END -%]

BEGIN;

-- XXX Add DDLs here.

COMMIT;
''',
            'revert/pg.tmpl': '''-- Revert [% project %]:[% change %] from [% engine %]

BEGIN;

-- XXX Add DDLs here.

COMMIT;
''',
            'verify/pg.tmpl': '''-- Verify [% project %]:[% change %] on [% engine %]

BEGIN;

-- XXX Add verifications here.

ROLLBACK;
''',
            
            # MySQL templates
            'deploy/mysql.tmpl': '''-- Deploy [% project %]:[% change %] to [% engine %]
[% FOREACH item IN requires -%]
-- requires: [% item %]
[% END -%]
[% FOREACH item IN conflicts -%]
-- conflicts: [% item %]
[% END -%]

-- XXX Add DDLs here.
''',
            'revert/mysql.tmpl': '''-- Revert [% project %]:[% change %] from [% engine %]

-- XXX Add DDLs here.
''',
            'verify/mysql.tmpl': '''-- Verify [% project %]:[% change %] on [% engine %]

-- XXX Add verifications here.
''',
            
            # SQLite templates
            'deploy/sqlite.tmpl': '''-- Deploy [% project %]:[% change %] to [% engine %]
[% FOREACH item IN requires -%]
-- requires: [% item %]
[% END -%]
[% FOREACH item IN conflicts -%]
-- conflicts: [% item %]
[% END -%]

-- XXX Add DDLs here.
''',
            'revert/sqlite.tmpl': '''-- Revert [% project %]:[% change %] from [% engine %]

-- XXX Add DDLs here.
''',
            'verify/sqlite.tmpl': '''-- Verify [% project %]:[% change %] on [% engine %]

-- XXX Add verifications here.
''',
        }
    
    def get_source(self, environment, template):
        """Get template source."""
        if template in self.templates:
            source = self.templates[template]
            # Convert Template Toolkit syntax to Jinja2
            source = self._convert_tt_to_jinja2(source)
            return source, None, lambda: True
        raise TemplateError(f"Template not found: {template}")
    
    def _convert_tt_to_jinja2(self, content: str) -> str:
        """Convert Template Toolkit syntax to Jinja2."""
        import re
        
        # Convert [% variable %] to {{ variable }}
        content = re.sub(r'\[%\s*(\w+)\s*%\]', r'{{ \1 }}', content)
        
        # Convert [% FOREACH item IN list %] to {% for item in list %}
        content = re.sub(r'\[%\s*FOREACH\s+(\w+)\s+IN\s+(\w+)\s*-%\]', 
                        r'{% for \1 in \2 %}', content)
        
        # Convert [% END %] to {% endfor %}
        content = re.sub(r'\[%\s*END\s*-%\]', r'{% endfor %}', content)
        
        return content


class TemplateEngine:
    """Template processing engine."""
    
    def __init__(self, template_dirs: Optional[List[Path]] = None):
        """
        Initialize template engine.
        
        Args:
            template_dirs: Optional list of custom template directories
        """
        if not JINJA2_AVAILABLE:
            raise TemplateError("Jinja2 is required for template processing")
        
        self.template_dirs = template_dirs or []
        self.env = self._create_environment()
    
    def _create_environment(self) -> Environment:
        """Create Jinja2 environment with appropriate loaders."""
        loaders = []
        
        # Add custom template directories
        for template_dir in self.template_dirs:
            if template_dir.exists():
                loaders.append(FileSystemLoader(str(template_dir)))
        
        # Add built-in template loader
        loaders.append(BuiltinTemplateLoader())
        
        # Create environment with choice loader
        from jinja2 import ChoiceLoader
        loader = ChoiceLoader(loaders) if loaders else BuiltinTemplateLoader()
        
        return Environment(
            loader=loader,
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True
        )
    
    def render_template(self, template_name: str, context: TemplateContext) -> str:
        """
        Render template with context.
        
        Args:
            template_name: Template name (e.g., 'deploy/pg.tmpl')
            context: Template context
            
        Returns:
            Rendered template content
            
        Raises:
            TemplateError: If template cannot be rendered
        """
        try:
            template = self.env.get_template(template_name)
            return template.render(context.to_dict())
        except Exception as e:
            raise TemplateError(f"Failed to render template {template_name}: {e}")
    
    def get_template_path(self, operation: OperationType, engine: EngineType) -> str:
        """
        Get template path for operation and engine.
        
        Args:
            operation: Operation type (deploy, revert, verify)
            engine: Database engine type
            
        Returns:
            Template path
        """
        return f"{operation}/{engine}.tmpl"
    
    def template_exists(self, template_name: str) -> bool:
        """
        Check if template exists.
        
        Args:
            template_name: Template name
            
        Returns:
            True if template exists
        """
        try:
            self.env.get_template(template_name)
            return True
        except:
            return False
    
    def list_templates(self) -> List[str]:
        """
        List available templates.
        
        Returns:
            List of template names
        """
        templates = []
        
        # Get templates from built-in loader
        builtin_loader = BuiltinTemplateLoader()
        templates.extend(builtin_loader.templates.keys())
        
        # Get templates from custom directories
        for template_dir in self.template_dirs:
            if template_dir.exists():
                for template_file in template_dir.rglob('*.tmpl'):
                    rel_path = template_file.relative_to(template_dir)
                    templates.append(str(rel_path))
        
        return sorted(set(templates))


def create_template_engine(template_dirs: Optional[List[Path]] = None) -> TemplateEngine:
    """
    Create template engine instance.
    
    Args:
        template_dirs: Optional list of custom template directories
        
    Returns:
        Template engine instance
        
    Raises:
        TemplateError: If template engine cannot be created
    """
    return TemplateEngine(template_dirs)


def render_change_template(operation: OperationType, engine: EngineType,
                          project: str, change: str,
                          requires: Optional[List[str]] = None,
                          conflicts: Optional[List[str]] = None,
                          template_dirs: Optional[List[Path]] = None) -> str:
    """
    Render change template.
    
    Args:
        operation: Operation type
        engine: Database engine type
        project: Project name
        change: Change name
        requires: List of required changes
        conflicts: List of conflicting changes
        template_dirs: Optional custom template directories
        
    Returns:
        Rendered template content
        
    Raises:
        TemplateError: If template cannot be rendered
    """
    template_engine = create_template_engine(template_dirs)
    template_name = template_engine.get_template_path(operation, engine)
    
    context = TemplateContext(
        project=project,
        change=change,
        engine=engine,
        requires=requires or [],
        conflicts=conflicts or []
    )
    
    return template_engine.render_template(template_name, context)