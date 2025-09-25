"""
Core application logic for sqlitch.

This module contains the main application classes and core functionality
including configuration management, plan parsing, and change representation.
"""

from sqlitch.core.exceptions import (
    ConfigurationError,
    ConnectionError,
    DeploymentError,
    EngineError,
    PlanError,
    SqlitchError,
)

__all__ = [
    "SqlitchError",
    "ConfigurationError",
    "PlanError",
    "EngineError",
    "DeploymentError",
    "ConnectionError",
]
