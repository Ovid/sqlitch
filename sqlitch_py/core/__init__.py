"""
Core application logic for sqlitch.

This module contains the main application classes and core functionality
including configuration management, plan parsing, and change representation.
"""

from sqlitch_py.core.exceptions import (
    SqlitchError,
    ConfigurationError,
    PlanError,
    EngineError,
    DeploymentError,
    ConnectionError,
)

__all__ = [
    "SqlitchError",
    "ConfigurationError",
    "PlanError", 
    "EngineError",
    "DeploymentError",
    "ConnectionError",
]