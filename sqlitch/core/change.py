"""Change, Tag, and Dependency dataclasses for sqlitch plan management."""

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .target import Target


@dataclass
class Dependency:
    """Represents a change dependency."""
    
    type: str  # 'require' or 'conflict'
    change: str
    project: Optional[str] = None
    
    def __str__(self) -> str:
        """String representation for plan file format."""
        if self.project:
            return f"{self.change}@{self.project}"
        return self.change
    
    @classmethod
    def from_string(cls, dep_str: str) -> 'Dependency':
        """Parse dependency from string format."""
        dep_type = 'require'  # Default type
        
        # Handle conflict dependencies (prefixed with !)
        if dep_str.startswith('!'):
            dep_type = 'conflict'
            dep_str = dep_str[1:]
        
        # Split project from change name
        if '@' in dep_str:
            change, project = dep_str.rsplit('@', 1)
        else:
            change = dep_str
            project = None
            
        return cls(type=dep_type, change=change, project=project)


@dataclass
class Tag:
    """Represents a plan tag."""
    
    name: str
    note: str
    timestamp: datetime
    planner_name: str
    planner_email: str
    change: Optional['Change'] = None
    
    @property
    def id(self) -> str:
        """Generate tag ID (SHA1 hash)."""
        content = f"{self.name} {self.timestamp.isoformat()} {self.planner_name} {self.planner_email} {self.note}"
        return hashlib.sha1(content.encode('utf-8')).hexdigest()
    
    def __str__(self) -> str:
        """String representation for plan file format."""
        timestamp_str = self.timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
        return f"@{self.name} {timestamp_str} {self.planner_name} <{self.planner_email}> # {self.note}"


@dataclass
class Change:
    """Represents a single database change."""
    
    name: str
    note: str
    timestamp: datetime
    planner_name: str
    planner_email: str
    tags: List[str] = field(default_factory=list)
    dependencies: List[Dependency] = field(default_factory=list)
    conflicts: List[str] = field(default_factory=list)
    
    @property
    def id(self) -> str:
        """Generate change ID (SHA1 hash)."""
        # Create content string matching Perl sqitch format
        deps_str = ' '.join(str(dep) for dep in self.dependencies) if self.dependencies else ''
        content = f"{self.name} {deps_str} {self.timestamp.isoformat()} {self.planner_name} {self.planner_email} {self.note}"
        return hashlib.sha1(content.encode('utf-8')).hexdigest()
    
    def deploy_file(self, target: 'Target') -> Path:
        """Get deploy script path."""
        return Path(target.top_dir) / target.deploy_dir / f"{self.name}.sql"
    
    def revert_file(self, target: 'Target') -> Path:
        """Get revert script path."""
        return Path(target.top_dir) / target.revert_dir / f"{self.name}.sql"
    
    def verify_file(self, target: 'Target') -> Path:
        """Get verify script path."""
        return Path(target.top_dir) / target.verify_dir / f"{self.name}.sql"
    
    def format_name_with_tags(self) -> str:
        """Format change name with tags for display."""
        if self.tags:
            tags_str = ' '.join(f"@{tag}" for tag in self.tags)
            return f"{self.name} {tags_str}"
        return self.name
    
    def __str__(self) -> str:
        """String representation for plan file format."""
        result = self.name
        
        # Add dependencies
        if self.dependencies:
            deps = ' '.join(f"[{dep}]" for dep in self.dependencies)
            result += f" [{deps}]"
        
        # Add timestamp and planner info
        timestamp_str = self.timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
        result += f" {timestamp_str} {self.planner_name} <{self.planner_email}>"
        
        # Add note
        if self.note:
            result += f" # {self.note}"
            
        return result