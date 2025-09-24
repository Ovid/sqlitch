"""
Core application logic for sqitch.

This module contains the main application classes and core functionality
including configuration management, plan parsing, and change representation.
"""

from sqitch_py.core.exceptions import (
    SqitchError,
    ConfigurationError,
    PlanError,
    EngineError,
    DeploymentError,
    ConnectionError,
)

__all__ = [
    "SqitchError",
    "ConfigurationError",
    "PlanError", 
    "EngineError",
    "DeploymentError",
    "ConnectionError",
]