"""
Base command class for sqlitch commands.

This module provides the BaseCommand abstract base class that all sqlitch
commands inherit from, providing common functionality and interface.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..core.exceptions import SqlitchError

if TYPE_CHECKING:
    from ..core.sqitch import Sqitch


class BaseCommand(ABC):
    """
    Abstract base class for all sqlitch commands.
    
    This class provides common functionality and interface for all commands,
    including access to the Sqitch instance and standardized error handling.
    """
    
    def __init__(self, sqitch: 'Sqitch'):
        """
        Initialize command with Sqitch instance.
        
        Args:
            sqitch: Main Sqitch application instance
        """
        self.sqitch = sqitch
        self.config = sqitch.config
        self.logger = sqitch.logger
    
    @abstractmethod
    def execute(self, args: List[str]) -> int:
        """
        Execute the command with given arguments.
        
        Args:
            args: Command-line arguments
            
        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        pass
    
    def require_initialized(self) -> None:
        """
        Ensure current directory is a sqitch project.
        
        Raises:
            SqlitchError: If not initialized
        """
        self.sqitch.require_initialized()
    
    def validate_user_info(self) -> None:
        """
        Validate user name and email configuration.
        
        Raises:
            SqlitchError: If user info is not configured
        """
        issues = self.sqitch.validate_user_info()
        if issues:
            raise SqlitchError("\n".join(issues))
    
    def get_target(self, target_name: Optional[str] = None):
        """
        Get target configuration.
        
        Args:
            target_name: Target name (defaults to configured default)
            
        Returns:
            Target configuration
        """
        return self.sqitch.get_target(target_name)
    
    def get_engine(self, target_name: Optional[str] = None):
        """
        Get database engine for target.
        
        Args:
            target_name: Target name (defaults to configured default)
            
        Returns:
            Database engine instance
        """
        target = self.get_target(target_name)
        return self.sqitch.engine_for_target(target)
    
    def info(self, message: str) -> None:
        """Log info message."""
        self.logger.info(message)
    
    def warn(self, message: str) -> None:
        """Log warning message."""
        self.logger.warn(message)
    
    def error(self, message: str) -> None:
        """Log error message."""
        self.logger.error(message)
    
    def debug(self, message: str) -> None:
        """Log debug message."""
        self.logger.debug(message)
    
    def verbose(self, message: str) -> None:
        """Log verbose message."""
        self.logger.verbose(message)