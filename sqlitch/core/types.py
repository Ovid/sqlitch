"""
Type system and validators for sqitch.

This module defines custom types, type aliases, and validation functions
used throughout the sqitch application to ensure type safety and data integrity.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    NewType,
    Optional,
    Pattern,
    Protocol,
    Tuple,
    TypeVar,
    Union,
)

# Type aliases for common types
ConfigValue = Union[str, int, float, bool, List[str]]
ConfigDict = Dict[str, ConfigValue]
ChangeId = NewType("ChangeId", str)
TagName = NewType("TagName", str)
ProjectName = NewType("ProjectName", str)
URI = NewType("URI", str)

# Engine types
EngineType = Literal[
    "pg",
    "mysql",
    "sqlite",
    "oracle",
    "snowflake",
    "vertica",
    "exasol",
    "firebird",
    "cockroach",
]

# Operation types
OperationType = Literal["deploy", "revert", "verify"]

# Verbosity levels
VerbosityLevel = Literal[-2, -1, 0, 1, 2, 3]


class DependencyType(Enum):
    """Types of change dependencies."""

    REQUIRE = "require"
    CONFLICT = "conflict"


class ChangeStatus(Enum):
    """Status of a change in the database."""

    DEPLOYED = "deployed"
    PENDING = "pending"
    FAILED = "failed"


@dataclass(frozen=True)
class Dependency:
    """Represents a change dependency."""

    type: DependencyType
    change: str
    project: Optional[str] = None

    def __str__(self) -> str:
        """String representation of dependency."""
        if self.project:
            return f"{self.project}:{self.change}"
        return self.change


@dataclass(frozen=True)
class Target:
    """Represents a deployment target configuration."""

    name: str
    uri: URI
    registry: Optional[str] = None
    client: Optional[str] = None

    @property
    def engine_type(self) -> EngineType:
        """Extract engine type from URI."""
        if self.uri.startswith("db:pg:"):
            return "pg"
        elif self.uri.startswith("db:mysql:"):
            return "mysql"
        elif self.uri.startswith("db:sqlite:"):
            return "sqlite"
        elif self.uri.startswith("db:oracle:"):
            return "oracle"
        elif self.uri.startswith("db:snowflake:"):
            return "snowflake"
        elif self.uri.startswith("db:vertica:"):
            return "vertica"
        elif self.uri.startswith("db:exasol:"):
            return "exasol"
        elif self.uri.startswith("db:firebird:"):
            return "firebird"
        elif self.uri.startswith("db:cockroach:"):
            return "cockroach"
        else:
            raise ValueError(f"Unknown engine type in URI: {self.uri}")


# Validation patterns
CHANGE_NAME_PATTERN: Pattern[str] = re.compile(r"^[a-zA-Z0-9_-]+$")
TAG_NAME_PATTERN: Pattern[str] = re.compile(r"^[a-zA-Z0-9._-]+$")
PROJECT_NAME_PATTERN: Pattern[str] = re.compile(r"^[a-zA-Z0-9._-]+$")
EMAIL_PATTERN: Pattern[str] = re.compile(
    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
)
SHA1_PATTERN: Pattern[str] = re.compile(r"^[a-f0-9]{40}$")


class Validator(Protocol):
    """Protocol for validation functions."""

    def __call__(self, value: Any) -> bool:
        """Validate a value."""
        ...


def validate_change_name(name: str) -> bool:
    """
    Validate change name format.

    Args:
        name: Change name to validate

    Returns:
        True if valid, False otherwise
    """
    return bool(CHANGE_NAME_PATTERN.match(name)) and len(name) <= 255


def validate_tag_name(name: str) -> bool:
    """
    Validate tag name format.

    Args:
        name: Tag name to validate

    Returns:
        True if valid, False otherwise
    """
    return bool(TAG_NAME_PATTERN.match(name)) and len(name) <= 255


def validate_project_name(name: str) -> bool:
    """
    Validate project name format.

    Args:
        name: Project name to validate

    Returns:
        True if valid, False otherwise
    """
    return bool(PROJECT_NAME_PATTERN.match(name)) and len(name) <= 255


def validate_email(email: str) -> bool:
    """
    Validate email address format.

    Args:
        email: Email address to validate

    Returns:
        True if valid, False otherwise
    """
    return bool(EMAIL_PATTERN.match(email)) and len(email) <= 320


def validate_sha1(sha1: str) -> bool:
    """
    Validate SHA1 hash format.

    Args:
        sha1: SHA1 hash to validate

    Returns:
        True if valid, False otherwise
    """
    return bool(SHA1_PATTERN.match(sha1))


def validate_uri(uri: str) -> bool:
    """
    Validate database URI format.

    Args:
        uri: Database URI to validate

    Returns:
        True if valid, False otherwise
    """
    # Basic validation - starts with db: and has engine type
    if not uri.startswith("db:"):
        return False

    # Extract engine part
    parts = uri.split(":", 2)
    if len(parts) < 2:
        return False

    engine = parts[1]
    return engine in [
        "pg",
        "mysql",
        "sqlite",
        "oracle",
        "snowflake",
        "vertica",
        "exasol",
        "firebird",
        "cockroach",
    ]


def validate_path(path: Union[str, Path]) -> bool:
    """
    Validate file path.

    Args:
        path: File path to validate

    Returns:
        True if valid, False otherwise
    """
    try:
        Path(path)
        return True
    except (TypeError, ValueError):
        return False


def validate_datetime_iso(dt_str: str) -> bool:
    """
    Validate ISO datetime format.

    Args:
        dt_str: Datetime string to validate

    Returns:
        True if valid, False otherwise
    """
    try:
        datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def validate_verbosity(level: int) -> bool:
    """
    Validate verbosity level.

    Args:
        level: Verbosity level to validate

    Returns:
        True if valid, False otherwise
    """
    return -2 <= level <= 3


def validate_config_key(key: str) -> bool:
    """
    Validate configuration key format.

    Args:
        key: Configuration key to validate

    Returns:
        True if valid, False otherwise
    """
    # Config keys can contain letters, numbers, dots, and underscores
    pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9._]*$")
    return bool(pattern.match(key)) and len(key) <= 255


T = TypeVar("T")


class ValidatedType(Generic[T]):
    """
    Generic validated type wrapper.

    Wraps a value with validation to ensure it meets specific criteria.
    """

    def __init__(self, value: T, validator: Callable[[T], bool]) -> None:
        """
        Initialize validated type.

        Args:
            value: Value to validate and wrap
            validator: Validation function

        Raises:
            ValueError: If validation fails
        """
        if not validator(value):
            raise ValueError(f"Invalid value: {value}")
        self._value = value
        self._validator = validator

    @property
    def value(self) -> T:
        """Get the validated value."""
        return self._value

    def __str__(self) -> str:
        """String representation."""
        return str(self._value)

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"{self.__class__.__name__}({self._value!r})"

    def __eq__(self, other: Any) -> bool:
        """Equality comparison."""
        if isinstance(other, ValidatedType):
            return self._value == other._value
        return self._value == other

    def __hash__(self) -> int:
        """Hash for use in sets and dicts."""
        return hash(self._value)


# Validated type aliases
ValidatedChangeName = ValidatedType[str]
ValidatedTagName = ValidatedType[str]
ValidatedProjectName = ValidatedType[str]
ValidatedEmail = ValidatedType[str]
ValidatedURI = ValidatedType[str]


def create_change_name(name: str) -> ValidatedChangeName:
    """Create validated change name."""
    return ValidatedChangeName(name, validate_change_name)


def create_tag_name(name: str) -> ValidatedTagName:
    """Create validated tag name."""
    return ValidatedTagName(name, validate_tag_name)


def create_project_name(name: str) -> ValidatedProjectName:
    """Create validated project name."""
    return ValidatedProjectName(name, validate_project_name)


def create_email(email: str) -> ValidatedEmail:
    """Create validated email."""
    return ValidatedEmail(email, validate_email)


def create_uri(uri: str) -> ValidatedURI:
    """Create validated URI."""
    return ValidatedURI(uri, validate_uri)


# Type guards for runtime type checking
def is_change_id(value: Any) -> bool:
    """Check if value is a valid change ID (SHA1)."""
    return isinstance(value, str) and validate_sha1(value)


def is_engine_type(value: Any) -> bool:
    """Check if value is a valid engine type."""
    return value in [
        "pg",
        "mysql",
        "sqlite",
        "oracle",
        "snowflake",
        "vertica",
        "exasol",
        "firebird",
        "cockroach",
    ]


def is_operation_type(value: Any) -> bool:
    """Check if value is a valid operation type."""
    return value in ["deploy", "revert", "verify"]


def coerce_config_value(value: str, expected_type: type) -> ConfigValue:
    """
    Coerce string configuration value to expected type.

    Args:
        value: String value from configuration
        expected_type: Expected Python type

    Returns:
        Coerced value

    Raises:
        ValueError: If coercion fails
    """
    if expected_type == bool:
        return value.lower() in ("true", "1", "yes", "on")
    elif expected_type == int:
        return int(value)
    elif expected_type == float:
        return float(value)
    elif expected_type == list:
        # Split on commas and strip whitespace
        return [item.strip() for item in value.split(",") if item.strip()]
    else:
        return value


def normalize_line_endings(text: str) -> str:
    """
    Normalize line endings to Unix format.

    Args:
        text: Text with potentially mixed line endings

    Returns:
        Text with normalized line endings
    """
    return text.replace("\r\n", "\n").replace("\r", "\n")


def sanitize_connection_string(connection_string: str) -> str:
    """
    Sanitize connection string for logging (remove passwords).

    Args:
        connection_string: Database connection string

    Returns:
        Sanitized connection string
    """
    # Remove password from connection strings
    # This is a simple implementation - real implementation would be more robust
    import re

    # Pattern to match password in various formats
    patterns = [
        r"password=[^;]+",
        r"pwd=[^;]+",
        r"://[^:]+:[^@]+@",  # user:password@host format
    ]

    sanitized = connection_string
    for pattern in patterns:
        sanitized = re.sub(
            pattern,
            lambda m: (
                m.group(0).split("=")[0] + "=***"
                if "=" in m.group(0)
                else "://***:***@"
            ),
            sanitized,
            flags=re.IGNORECASE,
        )

    return sanitized
