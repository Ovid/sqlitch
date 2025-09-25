"""
Item formatter for sqlitch log output.

This module provides the ItemFormatter class that handles formatting of
log entries with various format templates, matching the Perl sqitch behavior.
"""

import re
from datetime import datetime
from typing import Any, Dict


class ItemFormatter:
    """
    Formatter for sqlitch log entries.

    This class provides formatting capabilities for log entries using
    format templates similar to git log formatting.
    """

    def __init__(self, date_format: str = "iso", color: str = "auto", abbrev: int = 0):
        """
        Initialize formatter.

        Args:
            date_format: Date format to use ('iso', 'raw', etc.)
            color: Color mode ('always', 'never', 'auto')
            abbrev: Number of characters to abbreviate change IDs (0 = no abbreviation)
        """
        self.date_format = date_format
        self.color = color
        self.abbrev = abbrev

        # Color codes for different event types
        self._event_colors = {
            "deploy": "\033[32m",  # Green
            "revert": "\033[34m",  # Blue
            "fail": "\033[31m",  # Red
        }
        self._reset_color = "\033[0m"

        # Check if we should use colors
        self._use_colors = self._should_use_colors()

    def _should_use_colors(self) -> bool:
        """Determine if colors should be used."""
        if self.color == "always":
            return True
        elif self.color == "never":
            return False
        else:  # auto
            import sys

            return sys.stdout.isatty()

    def format(self, template: str, event: Dict[str, Any]) -> str:
        """
        Format event using template.

        Args:
            template: Format template string
            event: Event data dictionary

        Returns:
            Formatted string
        """
        result = template

        # Replace format codes
        result = self._replace_format_codes(result, event)

        return result

    def _replace_format_codes(self, template: str, event: Dict[str, Any]) -> str:
        """Replace format codes in template with event data."""
        result = template

        # Simple format codes (no parameters)
        replacements = {
            "%e": event.get("event", ""),
            "%L": self._get_event_label(event.get("event", "")),
            "%l": self._get_event_label_lower(event.get("event", "")),
            "%H": event.get("change_id", ""),
            "%h": self._abbreviate_id(event.get("change_id", "")),
            "%n": event.get("change", ""),
            "%o": event.get("project", ""),
            "%s": self._get_subject(event.get("note", "")),
            "%b": self._get_body(event.get("note", "")),
            "%B": event.get("note", ""),
            "%v": "\n",
        }

        for code, value in replacements.items():
            result = result.replace(code, str(value))

        # Complex format codes with parameters
        result = self._replace_complex_codes(result, event)

        return result

    def _replace_complex_codes(self, template: str, event: Dict[str, Any]) -> str:
        """Replace complex format codes that may have parameters."""
        result = template

        # Color codes
        result = self._replace_color_codes(result, event)

        # Date codes
        result = self._replace_date_codes(result, event)

        # Person codes
        result = self._replace_person_codes(result, event)

        # Array codes
        result = self._replace_array_codes(result, event)

        # Label codes
        result = self._replace_label_codes(result, event)

        return result

    def _replace_color_codes(self, template: str, event: Dict[str, Any]) -> str:
        """Replace color format codes."""
        result = template

        # %{:event}C - color based on event type
        if "%{:event}C" in result:
            if self._use_colors:
                color = self._event_colors.get(event.get("event", ""), "")
                result = result.replace("%{:event}C", color)
            else:
                result = result.replace("%{:event}C", "")

        # %{reset}C - reset color
        if "%{reset}C" in result:
            if self._use_colors:
                result = result.replace("%{reset}C", self._reset_color)
            else:
                result = result.replace("%{reset}C", "")

        # Other color codes like %{red}C, %{green}C, etc.
        color_pattern = r"%\{(\w+)\}C"
        matches = re.findall(color_pattern, result)
        for color_name in matches:
            if self._use_colors:
                color_code = self._get_ansi_color(color_name)
                result = result.replace(f"%{{{color_name}}}C", color_code)
            else:
                result = result.replace(f"%{{{color_name}}}C", "")

        return result

    def _replace_date_codes(self, template: str, event: Dict[str, Any]) -> str:
        """Replace date format codes."""
        result = template

        # %{date}c - committed date
        if "%{date}c" in result:
            date_str = self._format_date(event.get("committed_at"), self.date_format)
            result = result.replace("%{date}c", date_str)

        # %{date}p - planned date
        if "%{date}p" in result:
            date_str = self._format_date(event.get("planned_at"), self.date_format)
            result = result.replace("%{date}p", date_str)

        # %{date:format}c - committed date with specific format
        date_pattern = r"%\{date:([^}]+)\}c"
        matches = re.findall(date_pattern, result)
        for date_format in matches:
            date_str = self._format_date(event.get("committed_at"), date_format)
            result = result.replace(f"%{{date:{date_format}}}c", date_str)

        # %{date:format}p - planned date with specific format
        date_pattern = r"%\{date:([^}]+)\}p"
        matches = re.findall(date_pattern, result)
        for date_format in matches:
            date_str = self._format_date(event.get("planned_at"), date_format)
            result = result.replace(f"%{{date:{date_format}}}p", date_str)

        return result

    def _replace_person_codes(self, template: str, event: Dict[str, Any]) -> str:
        """Replace person format codes."""
        result = template

        # Committer codes
        result = result.replace(
            "%c",
            f"{event.get('committer_name', '')} <{event.get('committer_email', '')}>",
        )
        result = result.replace("%{name}c", event.get("committer_name", ""))
        result = result.replace("%{email}c", event.get("committer_email", ""))

        # Planner codes
        result = result.replace(
            "%p", f"{event.get('planner_name', '')} <{event.get('planner_email', '')}>"
        )
        result = result.replace("%{name}p", event.get("planner_name", ""))
        result = result.replace("%{email}p", event.get("planner_email", ""))

        return result

    def _replace_array_codes(self, template: str, event: Dict[str, Any]) -> str:
        """Replace array format codes."""
        result = template

        # Tags
        tags = event.get("tags", [])
        if tags:
            result = result.replace("%t", " " + ", ".join(tags))
            result = result.replace("%T", " (" + ", ".join(tags) + ")")
        else:
            result = result.replace("%t", "")
            result = result.replace("%T", "")

        # Requirements
        requires = event.get("requires", [])
        if requires:
            result = result.replace("%r", " " + ", ".join(requires))
            result = result.replace("%R", "Requires:  " + ", ".join(requires) + "\n")
        else:
            result = result.replace("%r", "")
            result = result.replace("%R", "")

        # Conflicts
        conflicts = event.get("conflicts", [])
        if conflicts:
            result = result.replace("%x", " " + ", ".join(conflicts))
            result = result.replace("%X", "Conflicts: " + ", ".join(conflicts) + "\n")
        else:
            result = result.replace("%x", "")
            result = result.replace("%X", "")

        return result

    def _replace_label_codes(self, template: str, event: Dict[str, Any]) -> str:
        """Replace label format codes."""
        result = template

        # Label codes like %{name}_
        label_pattern = r"%\{(\w+)\}_"
        matches = re.findall(label_pattern, result)
        for label in matches:
            label_text = self._get_label_text(label)
            result = result.replace(f"%{{{label}}}_", label_text)

        return result

    def _get_event_label(self, event: str) -> str:
        """Get capitalized event label."""
        labels = {"deploy": "Deploy", "revert": "Revert", "fail": "Fail"}
        return labels.get(event, event.capitalize())

    def _get_event_label_lower(self, event: str) -> str:
        """Get lowercase event label."""
        return event.lower()

    def _abbreviate_id(self, change_id: str) -> str:
        """Abbreviate change ID if abbrev is set."""
        if self.abbrev > 0 and len(change_id) > self.abbrev:
            return change_id[: self.abbrev]
        return change_id

    def _get_subject(self, note: str) -> str:
        """Get subject (first line) from note."""
        if not note:
            return ""
        lines = note.split("\n")
        return lines[0] if lines else ""

    def _get_body(self, note: str) -> str:
        """Get body (everything after first line) from note."""
        if not note:
            return ""
        lines = note.split("\n")
        if len(lines) <= 1:
            return ""
        return "\n".join(lines[1:])

    def _format_date(self, date_value: Any, format_type: str) -> str:  # noqa: C901
        """Format date according to format type."""
        if not date_value:
            return ""

        # Convert to datetime if it's a string
        if isinstance(date_value, str):
            try:
                # Try parsing ISO format
                if "T" in date_value:
                    date_value = datetime.fromisoformat(
                        date_value.replace("Z", "+00:00")
                    )
                else:
                    # Try other common formats
                    date_value = datetime.strptime(date_value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return str(date_value)

        if not isinstance(date_value, datetime):
            return str(date_value)

        if format_type == "iso":
            return date_value.strftime("%Y-%m-%d %H:%M:%S")
        elif format_type == "raw":
            return date_value.strftime("%Y-%m-%d %H:%M:%S %z")
        elif format_type == "short":
            return date_value.strftime("%Y-%m-%d")
        else:
            # Custom format
            try:
                return date_value.strftime(format_type)
            except ValueError:
                return str(date_value)

    def _get_ansi_color(self, color_name: str) -> str:
        """Get ANSI color code for color name."""
        colors = {
            "black": "\033[30m",
            "red": "\033[31m",
            "green": "\033[32m",
            "yellow": "\033[33m",
            "blue": "\033[34m",
            "magenta": "\033[35m",
            "cyan": "\033[36m",
            "white": "\033[37m",
            "reset": "\033[0m",
        }
        return colors.get(color_name.lower(), "")

    def _get_label_text(self, label: str) -> str:
        """Get label text for format codes."""
        labels = {
            "event": "Event:    ",
            "change": "Change:   ",
            "committer": "Committer:",
            "planner": "Planner:  ",
            "by": "By:       ",
            "date": "Date:     ",
            "committed": "Committed:",
            "planned": "Planned:  ",
            "name": "Name:     ",
            "project": "Project:  ",
            "email": "Email:    ",
            "requires": "Requires: ",
            "conflicts": "Conflicts:",
        }
        return labels.get(label, f"{label.capitalize()}:")


# Predefined format templates matching Perl sqitch
FORMATS = {
    "raw": """%(:{event}C%e %H%{reset}C%T
name      %n
project   %o
%{requires}a%{conflicts}aplanner   %{name}p <%{email}p>
planned   %{date:raw}p
committer %{name}c <%{email}c>
committed %{date:raw}c

%{    }B""",
    "full": """%(:{event}C%L %h%{reset}C%T
%{name}_ %n
%{project}_ %o
%R%X%{planner}_ %p
%{planned}_ %{date}p
%{committer}_ %c
%{committed}_ %{date}c

%{    }B""",
    "long": """%(:{event}C%L %h%{reset}C%T
%{name}_ %n
%{project}_ %o
%{planner}_ %p
%{committer}_ %c

%{    }B""",
    "medium": """%(:{event}C%L %h%{reset}C
%{name}_ %n
%{committer}_ %c
%{date}_ %{date}c

%{    }B""",
    "short": """%(:{event}C%L %h%{reset}C
%{name}_ %n
%{committer}_ %c

%{    }s""",
    "oneline": "%{:event}C%h %l%{reset}C %o:%n %s",
}
