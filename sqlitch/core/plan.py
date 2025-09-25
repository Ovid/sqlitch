"""Plan file parsing and management for sqitch."""

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from .change import Change, Dependency, Tag
from .exceptions import PlanError


@dataclass
class Plan:
    """Represents a sqitch deployment plan."""

    file: Path
    project: str
    uri: Optional[str] = None
    syntax_version: str = "1.0.0"
    changes: List[Change] = field(default_factory=list)
    tags: List[Tag] = field(default_factory=list)
    _change_index: Dict[str, Change] = field(default_factory=dict, init=False)
    _tag_index: Dict[str, Tag] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        """Build indexes after initialization."""
        self._build_indexes()

    @property
    def project_name(self) -> str:
        """Get project name (alias for project attribute)."""
        return self.project

    @property
    def creator_name(self) -> Optional[str]:
        """Get creator name from first change or None."""
        if self.changes:
            return self.changes[0].planner_name
        return None

    @property
    def creator_email(self) -> Optional[str]:
        """Get creator email from first change or None."""
        if self.changes:
            return self.changes[0].planner_email
        return None

    def get_deploy_file(self, change: "Change") -> Path:
        """Get deploy script path for a change."""
        # For now, assume standard directory structure
        # This should be configurable based on target
        return Path("deploy") / f"{change.name}.sql"

    def get_revert_file(self, change: "Change") -> Path:
        """Get revert script path for a change."""
        return Path("revert") / f"{change.name}.sql"

    def get_verify_file(self, change: "Change") -> Path:
        """Get verify script path for a change."""
        return Path("verify") / f"{change.name}.sql"

    def _build_indexes(self) -> None:
        """Build internal indexes for fast lookups."""
        self._change_index = {change.name: change for change in self.changes}
        # Also index by ID for checkout command
        for change in self.changes:
            self._change_index[change.id] = change
        self._tag_index = {tag.name: tag for tag in self.tags}

    def get_change(self, identifier: str) -> Optional[Change]:
        """Get change by name or ID."""
        return self._change_index.get(identifier)

    @classmethod
    def from_file(cls, file_path: Path) -> "Plan":
        """Parse plan from file."""
        if not file_path.exists():
            raise PlanError(f"Plan file not found: {file_path}")

        try:
            content = file_path.read_text(encoding="utf-8")
        except UnicodeDecodeError as e:
            raise PlanError(f"Invalid encoding in plan file {file_path}: {e}")

        return cls._parse_content(file_path, content)

    @classmethod
    def from_string(cls, content: str, file_path: Optional[Path] = None) -> "Plan":
        """Parse plan from string content."""
        if file_path is None:
            file_path = Path("sqitch.plan")

        return cls._parse_content(file_path, content)

    @classmethod
    def _parse_content(cls, file_path: Path, content: str) -> "Plan":  # noqa: C901
        """Parse plan content."""
        lines = content.splitlines()

        # Initialize plan with defaults
        plan = cls(
            file=file_path,
            project="",  # Will be set from %project pragma
            uri=None,
            syntax_version="1.0.0",
        )

        # Parse line by line
        for line_num, line in enumerate(lines, 1):
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue

            try:
                if line.startswith("%"):
                    plan._parse_pragma(line)
                elif line.startswith("@"):
                    tag = plan._parse_tag(line)
                    # Associate tag with the most recent change
                    if plan.changes:
                        tag.change = plan.changes[-1]
                        # Also add tag name to the change's tags list
                        if not hasattr(plan.changes[-1], "tags"):
                            plan.changes[-1].tags = []
                        plan.changes[-1].tags.append(tag.name)
                    plan.tags.append(tag)
                else:
                    change = plan._parse_change(line)
                    plan.changes.append(change)
            except Exception as e:
                raise PlanError(f"Error parsing line {line_num} in {file_path}: {e}")

        # Validate required pragmas
        if not plan.project:
            raise PlanError(f"Missing %project pragma in {file_path}")

        # Build indexes and validate
        plan._build_indexes()
        validation_errors = plan.validate()
        if validation_errors:
            raise PlanError(f"Plan validation failed: {'; '.join(validation_errors)}")

        return plan

    def _parse_pragma(self, line: str) -> None:
        """Parse pragma line."""
        if "=" not in line:
            raise PlanError(f"Invalid pragma format: {line}")

        pragma, value = line[1:].split("=", 1)
        pragma = pragma.strip()
        value = value.strip()

        if pragma == "syntax-version":
            self.syntax_version = value
        elif pragma == "project":
            self.project = value
        elif pragma == "uri":
            self.uri = value
        else:
            # Unknown pragmas are ignored for forward compatibility
            pass

    def _parse_tag(self, line: str) -> Tag:
        """Parse tag line."""
        # Format: @tag_name timestamp planner_name <planner_email> # note
        match = re.match(r"^@(\S+)\s+(\S+)\s+(.+?)\s+<([^>]+)>\s*(?:#\s*(.*))?$", line)

        if not match:
            raise PlanError(f"Invalid tag format: {line}")

        tag_name, timestamp_str, planner_name, planner_email, note = match.groups()

        # Parse timestamp
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except ValueError as e:
            raise PlanError(f"Invalid timestamp in tag {tag_name}: {e}")

        return Tag(
            name=tag_name,
            note=note or "",
            timestamp=timestamp,
            planner_name=planner_name,
            planner_email=planner_email,
        )

    def _parse_change(self, line: str) -> Change:  # noqa: C901
        """Parse change line."""
        # Format: change_name [dependencies] timestamp planner_name <planner_email> # note

        # Extract note first (everything after #)
        note = ""
        if "#" in line:
            line, note = line.split("#", 1)
            note = note.strip()
            line = line.strip()

        # Parse the rest
        parts = line.split()
        if len(parts) < 4:
            raise PlanError(f"Invalid change format: {line}")

        change_name = parts[0]

        # Look for dependencies in square brackets
        dependencies = []
        part_idx = 1

        if part_idx < len(parts) and parts[part_idx].startswith("["):
            # Find the complete dependency section (may span multiple parts)
            dep_parts = []
            bracket_count = 0

            while part_idx < len(parts):
                part = parts[part_idx]
                dep_parts.append(part)

                # Count brackets to handle nested or multiple dependency groups
                bracket_count += part.count("[") - part.count("]")

                part_idx += 1

                # Stop when we've closed all brackets
                if bracket_count == 0:
                    break

            # Parse all dependencies from the collected parts
            dep_text = " ".join(dep_parts)

            # Extract content between brackets
            bracket_matches = re.findall(r"\[([^\]]*)\]", dep_text)

            for bracket_content in bracket_matches:
                if bracket_content.strip():
                    for dep in bracket_content.split():
                        dependencies.append(Dependency.from_string(dep))

        # Remaining parts should be: timestamp planner_name <planner_email>
        remaining_parts = parts[part_idx:]
        if len(remaining_parts) < 3:
            raise PlanError(f"Missing timestamp or planner info in change: {line}")

        timestamp_str = remaining_parts[0]

        # Extract email from angle brackets first
        remaining_text = " ".join(remaining_parts[1:])
        email_match = re.search(r"<([^>]+)>", remaining_text)
        if not email_match:
            raise PlanError(f"Missing or invalid email format in change: {line}")

        planner_email = email_match.group(1)

        # Extract planner name (everything before the email)
        planner_name = remaining_text[: email_match.start()].strip()

        # Parse timestamp
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except ValueError as e:
            raise PlanError(f"Invalid timestamp in change {change_name}: {e}")

        return Change(
            name=change_name,
            note=note,
            timestamp=timestamp,
            planner_name=planner_name,
            planner_email=planner_email,
            dependencies=dependencies,
        )

    def changes_since(self, change_id: str) -> List[Change]:
        """Get changes since specified change."""
        # Find the change by ID or name
        start_change = None
        for change in self.changes:
            if change.id == change_id or change.name == change_id:
                start_change = change
                break

        if start_change is None:
            raise PlanError(f"Change not found: {change_id}")

        # Find index and return subsequent changes
        try:
            start_idx = self.changes.index(start_change)
            return self.changes[start_idx + 1 :]
        except ValueError:
            return []

    def get_tag(self, name: str) -> Optional[Tag]:
        """Get tag by name."""
        return self._tag_index.get(name)

    def validate(self) -> List[str]:
        """Validate plan consistency."""
        errors = []

        # Check for duplicate change names
        change_names = [change.name for change in self.changes]
        duplicates = set(
            [name for name in change_names if change_names.count(name) > 1]
        )
        for duplicate in duplicates:
            errors.append(f"Duplicate change name: {duplicate}")

        # Check for duplicate tag names
        tag_names = [tag.name for tag in self.tags]
        duplicates = set([name for name in tag_names if tag_names.count(name) > 1])
        for duplicate in duplicates:
            errors.append(f"Duplicate tag name: {duplicate}")

        # Validate dependencies
        available_changes = set(change.name for change in self.changes)
        for change in self.changes:
            for dep in change.dependencies:
                if dep.type == "require" and dep.change not in available_changes:
                    # Only error if it's not a cross-project dependency
                    if dep.project is None:
                        errors.append(
                            f"Change {change.name} depends on unknown change: {dep.change}"
                        )

        # Check chronological order
        for i in range(1, len(self.changes)):
            if self.changes[i].timestamp < self.changes[i - 1].timestamp:
                errors.append(
                    f"Change {self.changes[i].name} has earlier timestamp than previous change"
                )

        return errors

    def add_change(self, change: Change) -> None:
        """Add a change to the plan."""
        if change.name in self._change_index:
            raise PlanError(f"Change {change.name} already exists in plan")

        self.changes.append(change)
        self._change_index[change.name] = change

    def add_tag(self, tag: Tag) -> None:
        """Add a tag to the plan."""
        if tag.name in self._tag_index:
            raise PlanError(f"Tag {tag.name} already exists in plan")

        self.tags.append(tag)
        self._tag_index[tag.name] = tag

    def create_tag(
        self,
        name: str,
        change_name: Optional[str] = None,
        note: str = "",
        planner_name: str = "",
        planner_email: str = "",
    ) -> Tag:
        """
        Create a new tag in the plan.

        Args:
            name: Tag name (without @ prefix)
            change_name: Name of change to tag (defaults to last change)
            note: Tag note
            planner_name: Name of person creating the tag
            planner_email: Email of person creating the tag

        Returns:
            Created Tag object

        Raises:
            PlanError: If tag already exists or change not found
        """
        # Remove @ prefix if present
        if name.startswith("@"):
            name = name[1:]

        # Check if tag already exists
        if name in self._tag_index:
            raise PlanError(f'Tag "@{name}" already exists')

        # Find the change to tag
        change = None
        if change_name:
            change = self.get_change(change_name)
            if not change:
                raise PlanError(f'Unknown change: "{change_name}"')
        else:
            # Tag the last change
            if not self.changes:
                raise PlanError(f'Cannot apply tag "@{name}" to a plan with no changes')
            change = self.changes[-1]

        # Create tag
        tag = Tag(
            name=name,
            note=note,
            timestamp=datetime.now(timezone.utc),
            planner_name=planner_name,
            planner_email=planner_email,
            change=change,
        )

        # Add to plan
        self.add_tag(tag)

        # Associate tag with change
        if name not in change.tags:
            change.tags.append(name)

        return tag

    def save(self) -> None:
        """Save plan to file."""
        lines = []

        # Add pragmas
        lines.append(f"%syntax-version={self.syntax_version}")
        lines.append(f"%project={self.project}")
        if self.uri:
            lines.append(f"%uri={self.uri}")
        lines.append("")  # Empty line after pragmas

        # Add changes and tags in chronological order
        all_items = []
        all_items.extend(("change", change) for change in self.changes)
        all_items.extend(("tag", tag) for tag in self.tags)

        # Sort by timestamp
        all_items.sort(key=lambda x: x[1].timestamp)

        for item_type, item in all_items:
            lines.append(str(item))

        # Write to file
        content = "\n".join(lines) + "\n"
        self.file.write_text(content, encoding="utf-8")

    @property
    def count(self) -> int:
        """Get the number of changes in the plan."""
        return len(self.changes)

    def change_at(self, index: int) -> Optional[Change]:
        """
        Get change at specific index.

        Args:
            index: Index of change to retrieve

        Returns:
            Change at index, or None if index is out of range
        """
        if 0 <= index < len(self.changes):
            return self.changes[index]
        return None

    def get_change_by_id(self, change_id: str) -> Optional[Change]:
        """
        Get change by ID.

        Args:
            change_id: Change ID to find

        Returns:
            Change with matching ID, or None if not found
        """
        for change in self.changes:
            if change.id == change_id:
                return change
        return None

    def get(self, identifier: str) -> Optional[Change]:
        """
        Get change by name or ID.

        This method provides compatibility with the Perl sqitch interface.

        Args:
            identifier: Change name or ID to find

        Returns:
            Change with matching name or ID, or None if not found
        """
        # First try by name
        change = self.get_change(identifier)
        if change:
            return change

        # Then try by ID
        return self.get_change_by_id(identifier)
