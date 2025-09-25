"""Change, Tag, and Dependency dataclasses for sqlitch plan management."""

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

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
        result = self.change
        if self.project:
            result = f"{result}@{self.project}"

        # Add conflict prefix
        if self.type == "conflict":
            result = f"!{result}"

        return result

    @classmethod
    def from_string(cls, dep_str: str) -> "Dependency":
        """Parse dependency from string format."""
        dep_type = "require"  # Default type

        # Handle conflict dependencies (prefixed with !)
        if dep_str.startswith("!"):
            dep_type = "conflict"
            dep_str = dep_str[1:]

        # Split project from change name
        if "@" in dep_str:
            change, project = dep_str.rsplit("@", 1)
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
    change: Optional["Change"] = None

    @property
    def id(self) -> str:
        """Generate tag ID (SHA1 hash)."""
        content = f"{self.name} {self.timestamp.isoformat()} {self.planner_name} {self.planner_email} {self.note}"
        return hashlib.sha1(content.encode("utf-8")).hexdigest()

    def info(self, plan=None) -> str:
        """
        Return information about the tag for display.

        This matches the Perl sqitch format for tag information.

        Args:
            plan: Optional plan object to get project/uri info from
        """
        lines = []

        # Add project info
        project = plan.project if plan else getattr(self, "project", "unknown")
        lines.append(f"project {project}")

        # Add URI if available
        uri = plan.uri if plan else getattr(self, "uri", None)
        if uri:
            lines.append(f"uri {uri}")

        # Add tag name
        lines.append(f"tag @{self.name}")

        # Add associated change ID
        if self.change:
            lines.append(f"change {self.change.id}")

        # Add planner info
        lines.append(f"planner {self.planner_name} <{self.planner_email}>")

        # Add date
        lines.append(f"date {self.timestamp.strftime('%Y-%m-%d %H:%M:%S %z')}")

        # Add note if present
        if self.note:
            lines.append("")
            lines.append(self.note)

        return "\n".join(lines)

    def __str__(self) -> str:
        """String representation for plan file format."""
        timestamp_str = self.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
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
        deps_str = (
            " ".join(str(dep) for dep in self.dependencies) if self.dependencies else ""
        )
        content = f"{self.name} {deps_str} {self.timestamp.isoformat()} {self.planner_name} {self.planner_email} {self.note}"
        return hashlib.sha1(content.encode("utf-8")).hexdigest()

    def deploy_file(self, target: "Target") -> Path:
        """Get deploy script path."""
        return Path(target.top_dir) / target.deploy_dir / f"{self.name}.sql"

    def revert_file(self, target: "Target") -> Path:
        """Get revert script path."""
        return Path(target.top_dir) / target.revert_dir / f"{self.name}.sql"

    def verify_file(self, target: "Target") -> Path:
        """Get verify script path."""
        return Path(target.top_dir) / target.verify_dir / f"{self.name}.sql"

    def format_name_with_tags(self) -> str:
        """Format change name with tags for display."""
        if self.tags:
            tags_str = " ".join(f"@{tag}" for tag in self.tags)
            return f"{self.name} {tags_str}"
        return self.name

    @property
    def is_reworked(self) -> bool:
        """Check if this is a reworked change."""
        # For now, assume no reworked changes
        # This would be determined by the presence of @tag in the change name
        return "@" in self.name

    @property
    def path_segments(self) -> List[str]:
        """Get path segments for nested directory structure."""
        # For simple changes, just return the filename
        # For nested changes, this would split on directory separators
        return [f"{self.name}.sql"]

    def info(self, plan=None) -> str:
        """
        Return information about the change for display.

        This matches the Perl sqitch format for change information.

        Args:
            plan: Optional plan object to get project/uri info from
        """
        lines = []

        # Add project info
        project = plan.project if plan else getattr(self, "project", "unknown")
        lines.append(f"project {project}")

        # Add URI if available
        uri = plan.uri if plan else getattr(self, "uri", None)
        if uri:
            lines.append(f"uri {uri}")

        # Add change name
        lines.append(f"change {self.name}")

        # Add parent if this is a reworked change
        if hasattr(self, "parent") and self.parent:
            lines.append(f"parent {self.parent.id}")

        # Add planner info
        lines.append(f"planner {self.planner_name} <{self.planner_email}>")

        # Add date
        lines.append(f"date {self.timestamp.strftime('%Y-%m-%d %H:%M:%S %z')}")

        # Add requirements
        if self.dependencies:
            requires = [dep for dep in self.dependencies if dep.type == "require"]
            if requires:
                lines.append("requires")
                for req in requires:
                    lines.append(f"  + {req}")

        # Add conflicts
        conflicts = [dep for dep in self.dependencies if dep.type == "conflict"]
        if conflicts:
            lines.append("conflicts")
            for conf in conflicts:
                lines.append(f"  - {conf}")

        # Add note if present
        if self.note:
            lines.append("")
            lines.append(self.note)

        return "\n".join(lines)

    def __str__(self) -> str:
        """String representation for plan file format."""
        result = self.name

        # Add dependencies
        if self.dependencies:
            deps = " ".join(str(dep) for dep in self.dependencies)
            result += f" [{deps}]"

        # Add timestamp and planner info
        timestamp_str = self.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        result += f" {timestamp_str} {self.planner_name} <{self.planner_email}>"

        # Add note
        if self.note:
            # Convert multi-line notes to single line for plan file format
            note_single_line = (
                self.note.replace("\n\n", " ").replace("\n", " ").replace("\r", " ")
            )
            # Collapse multiple spaces
            import re

            note_single_line = re.sub(r"\s+", " ", note_single_line).strip()
            result += f" # {note_single_line}"

        return result
