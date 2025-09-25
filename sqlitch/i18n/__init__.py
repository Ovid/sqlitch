"""
Internationalization support for sqlitch.

This module provides gettext-based message translation functionality
compatible with the original Perl sqitch implementation.
"""

import gettext
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Global translation function
_: Optional[gettext.GNUTranslations] = None


def setup_i18n(locale_dir: Optional[Path] = None, domain: str = "sqlitch") -> None:
    """
    Setup internationalization for sqlitch.

    Args:
        locale_dir: Directory containing translation files
        domain: Translation domain name
    """
    global _

    if locale_dir is None:
        # Default to package locale directory
        locale_dir = Path(__file__).parent / "locale"

    try:
        # Try to get translation for current locale
        translation = gettext.translation(
            domain, localedir=str(locale_dir), fallback=True
        )
        _ = translation.gettext
    except Exception:
        # Fallback to identity function
        def _(x):
            return x


def __(message: str) -> str:
    """
    Translate a message.

    Args:
        message: Message to translate

    Returns:
        Translated message or original if no translation available
    """
    global _
    if _ is None:
        setup_i18n()
    return _(message)


def __x(message: str, **kwargs: Any) -> str:
    """
    Translate a message with parameter substitution.

    Args:
        message: Message template to translate
        **kwargs: Parameters for substitution

    Returns:
        Translated and formatted message
    """
    translated = __(message)
    try:
        return translated.format(**kwargs)
    except (KeyError, ValueError):
        # Fallback to original message if formatting fails
        return message.format(**kwargs)


def __n(singular: str, plural: str, count: int, **kwargs: Any) -> str:
    """
    Translate a message with plural forms.

    Args:
        singular: Singular form message
        plural: Plural form message
        count: Count to determine plural form
        **kwargs: Parameters for substitution

    Returns:
        Translated message in appropriate plural form
    """
    global _
    if _ is None:
        setup_i18n()

    # For now, simple English pluralization
    # TODO: Implement proper ngettext support
    message = singular if count == 1 else plural
    translated = _(message)

    try:
        return translated.format(count=count, **kwargs)
    except (KeyError, ValueError):
        return message.format(count=count, **kwargs)


# Initialize on import
setup_i18n()
