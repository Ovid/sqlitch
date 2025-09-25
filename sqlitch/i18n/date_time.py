"""
Locale-aware date/time formatting for sqlitch.

This module provides date/time formatting that respects the user's locale
settings, similar to the original Perl sqitch implementation.
"""

import locale
import platform
from datetime import datetime
from typing import Optional


class LocaleAwareDateTimeFormatter:
    """
    Formatter for date/time values that respects locale settings.
    """

    def __init__(self):
        """Initialize the formatter."""
        self._locale_set = False
        self._setup_locale()

    def _setup_locale(self) -> None:
        """Setup locale for date/time formatting."""
        try:
            if platform.system() == "Windows":
                # On Windows, try to get locale from environment
                import os

                user_locale = os.environ.get("LANG", "en_US.UTF-8")
                locale.setlocale(locale.LC_TIME, user_locale)
            else:
                # On Unix-like systems, use system default
                locale.setlocale(locale.LC_TIME, "")
            self._locale_set = True
        except locale.Error:
            # Fallback to C locale if system locale is not available
            try:
                locale.setlocale(locale.LC_TIME, "C")
                self._locale_set = True
            except locale.Error:
                # If even C locale fails, we'll use default formatting
                self._locale_set = False

    def format_datetime(
        self, dt: datetime, format_type: str = "default"
    ) -> str:  # noqa: C901
        """
        Format a datetime according to the specified format type.

        Args:
            dt: DateTime to format
            format_type: Format type ('iso', 'rfc', 'rfc2822', 'default', or custom format)

        Returns:
            Formatted datetime string
        """
        if format_type == "iso":
            # ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        elif format_type in ("rfc", "rfc2822"):
            # RFC 2822 format: Day, DD Mon YYYY HH:MM:SS +0000
            # Force English locale for RFC format
            old_locale = None
            if self._locale_set:
                try:
                    old_locale = locale.getlocale(locale.LC_TIME)
                    locale.setlocale(locale.LC_TIME, "C")
                except locale.Error:
                    pass

            try:
                formatted = dt.strftime("%a, %d %b %Y %H:%M:%S %z")
                # Fix timezone format for RFC compliance
                if formatted.endswith("+0000"):
                    formatted = formatted[:-5] + "-0000"
                return formatted
            finally:
                # Restore original locale
                if old_locale and self._locale_set:
                    try:
                        locale.setlocale(locale.LC_TIME, old_locale)
                    except locale.Error:
                        pass

        elif format_type.startswith("cldr:"):
            # CLDR format (simplified implementation)
            cldr_format = format_type[5:]
            return self._format_cldr(dt, cldr_format)

        elif format_type.startswith("strftime:"):
            # Direct strftime format
            strftime_format = format_type[9:]
            return dt.strftime(strftime_format)

        else:
            # Default locale-aware format or fallback to ISO
            if format_type == "default":
                if self._locale_set:
                    try:
                        return dt.strftime(
                            "%c"
                        )  # Locale's appropriate date and time representation
                    except (ValueError, OSError):
                        pass

                # Fallback to ISO format
                return self.format_datetime(dt, "iso")
            else:
                # Unknown format, fallback to ISO
                return self.format_datetime(dt, "iso")

    def _format_cldr(self, dt: datetime, cldr_format: str) -> str:
        """
        Format datetime using simplified CLDR patterns.

        Args:
            dt: DateTime to format
            cldr_format: CLDR format pattern

        Returns:
            Formatted datetime string
        """
        # Simplified CLDR pattern mapping
        cldr_mapping = {
            "short": "%x %X",  # Short date and time
            "medium": "%x %X",  # Medium date and time
            "long": "%A, %B %d, %Y %X",  # Long date and time
            "full": "%A, %B %d, %Y %X %Z",  # Full date and time
        }

        pattern = cldr_mapping.get(cldr_format, cldr_format)

        try:
            return dt.strftime(pattern)
        except (ValueError, OSError):
            # Fallback to ISO format
            return self.format_datetime(dt, "iso")


# Global formatter instance
_formatter: Optional[LocaleAwareDateTimeFormatter] = None


def get_datetime_formatter() -> LocaleAwareDateTimeFormatter:
    """
    Get the global datetime formatter instance.

    Returns:
        LocaleAwareDateTimeFormatter instance
    """
    global _formatter
    if _formatter is None:
        _formatter = LocaleAwareDateTimeFormatter()
    return _formatter


def format_datetime(dt: datetime, format_type: str = "default") -> str:
    """
    Format a datetime using locale-aware formatting.

    Args:
        dt: DateTime to format
        format_type: Format type

    Returns:
        Formatted datetime string
    """
    return get_datetime_formatter().format_datetime(dt, format_type)


def format_timestamp(timestamp: float, format_type: str = "default") -> str:
    """
    Format a Unix timestamp using locale-aware formatting.

    Args:
        timestamp: Unix timestamp
        format_type: Format type

    Returns:
        Formatted datetime string
    """
    dt = datetime.fromtimestamp(timestamp)
    return format_datetime(dt, format_type)
