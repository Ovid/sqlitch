"""
Sqitch Python Port - Database change management tool.

A complete Python port of the Perl-based sqitch database change management tool,
maintaining full CLI compatibility and feature parity.
"""

__version__ = "1.0.0"
__author__ = "Sqitch Python Port Team"
__email__ = "sqitch-py@example.com"

from sqitch_py.core.exceptions import (
    SqitchError,
    ConfigurationError,
    PlanError,
    EngineError,
    DeploymentError,
    ConnectionError,
)

__all__ = [
    "__version__",
    "__author__",
    "__email__",
    "SqitchError",
    "ConfigurationError", 
    "PlanError",
    "EngineError",
    "DeploymentError",
    "ConnectionError",
]