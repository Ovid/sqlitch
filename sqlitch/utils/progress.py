"""
Progress indicators and user feedback utilities for sqlitch.

This module provides progress indicators, spinners, and other user feedback
mechanisms for long-running operations, matching the behavior of Perl sqitch.
"""

import sys
import time
import threading
from typing import Optional, TextIO, Iterator, Any
from contextlib import contextmanager


class ProgressIndicator:
    """
    Base class for progress indicators.
    
    Provides common functionality for showing progress during long-running
    operations with proper cleanup and thread safety.
    """
    
    def __init__(self, message: str = "", file: Optional[TextIO] = None) -> None:
        """
        Initialize progress indicator.
        
        Args:
            message: Message to display with progress
            file: Output file (defaults to stderr)
        """
        self.message = message
        self.file = file or sys.stderr
        self.active = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
    
    def start(self) -> None:
        """Start the progress indicator."""
        if self.active:
            return
        
        self.active = True
        self._stop_event.clear()
        
        if self.message:
            self.file.write(f"{self.message} ")
            self.file.flush()
        
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop the progress indicator."""
        if not self.active:
            return
        
        self.active = False
        self._stop_event.set()
        
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        
        self._cleanup()
    
    def _run(self) -> None:
        """Run the progress indicator (override in subclasses)."""
        pass
    
    def _cleanup(self) -> None:
        """Clean up after stopping (override in subclasses)."""
        pass
    
    def __enter__(self) -> 'ProgressIndicator':
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.stop()


class Spinner(ProgressIndicator):
    """
    Spinning progress indicator.
    
    Shows a rotating character to indicate ongoing activity.
    """
    
    CHARS = ['|', '/', '-', '\\']
    
    def __init__(self, message: str = "", interval: float = 0.1, **kwargs: Any) -> None:
        """
        Initialize spinner.
        
        Args:
            message: Message to display with spinner
            interval: Time between spinner updates
            **kwargs: Additional arguments for parent class
        """
        super().__init__(message, **kwargs)
        self.interval = interval
        self.char_index = 0
    
    def _run(self) -> None:
        """Run the spinner animation."""
        while not self._stop_event.wait(self.interval):
            if not self.active:
                break
            
            try:
                char = self.CHARS[self.char_index % len(self.CHARS)]
                self.file.write(f'\r{self.message} {char}')
                self.file.flush()
                self.char_index += 1
            except (ValueError, OSError):
                # File handle closed or other I/O error
                break
    
    def _cleanup(self) -> None:
        """Clean up spinner display."""
        # Clear the spinner character
        self.file.write(f'\r{self.message}   \r{self.message}\n')
        self.file.flush()


class Dots(ProgressIndicator):
    """
    Dots progress indicator.
    
    Shows accumulating dots to indicate ongoing activity.
    """
    
    def __init__(self, message: str = "", interval: float = 0.5, max_dots: int = 3, **kwargs: Any) -> None:
        """
        Initialize dots indicator.
        
        Args:
            message: Message to display with dots
            interval: Time between dot updates
            max_dots: Maximum number of dots before cycling
            **kwargs: Additional arguments for parent class
        """
        super().__init__(message, **kwargs)
        self.interval = interval
        self.max_dots = max_dots
        self.dot_count = 0
    
    def _run(self) -> None:
        """Run the dots animation."""
        while not self._stop_event.wait(self.interval):
            if not self.active:
                break
            
            try:
                self.dot_count = (self.dot_count % self.max_dots) + 1
                dots = '.' * self.dot_count + ' ' * (self.max_dots - self.dot_count)
                self.file.write(f'\r{self.message}{dots}')
                self.file.flush()
            except (ValueError, OSError):
                # File handle closed or other I/O error
                break
    
    def _cleanup(self) -> None:
        """Clean up dots display."""
        self.file.write(f'\r{self.message}\n')
        self.file.flush()


class ProgressBar(ProgressIndicator):
    """
    Progress bar for operations with known total.
    
    Shows a visual progress bar with percentage completion.
    """
    
    def __init__(self, total: int, message: str = "", width: int = 40, **kwargs: Any) -> None:
        """
        Initialize progress bar.
        
        Args:
            total: Total number of items to process
            message: Message to display with progress bar
            width: Width of the progress bar in characters
            **kwargs: Additional arguments for parent class
        """
        super().__init__(message, **kwargs)
        self.total = total
        self.width = width
        self.current = 0
    
    def update(self, current: int) -> None:
        """
        Update progress bar position.
        
        Args:
            current: Current progress value
        """
        self.current = min(current, self.total)
        self._draw()
    
    def increment(self, amount: int = 1) -> None:
        """
        Increment progress by specified amount.
        
        Args:
            amount: Amount to increment by
        """
        self.update(self.current + amount)
    
    def _draw(self) -> None:
        """Draw the progress bar."""
        if self.total == 0:
            percent = 100
            filled = self.width
        else:
            percent = int((self.current / self.total) * 100)
            filled = int((self.current / self.total) * self.width)
        
        bar = '█' * filled + '░' * (self.width - filled)
        
        display = f'\r{self.message} [{bar}] {percent}% ({self.current}/{self.total})'
        self.file.write(display)
        self.file.flush()
    
    def _run(self) -> None:
        """Progress bar doesn't need continuous updates."""
        pass
    
    def start(self) -> None:
        """Start progress bar."""
        self.active = True
        if self.message:
            self._draw()
    
    def _cleanup(self) -> None:
        """Clean up progress bar display."""
        self.file.write('\n')
        self.file.flush()


@contextmanager
def progress_indicator(message: str = "", 
                      indicator_type: str = "spinner",
                      file: Optional[TextIO] = None,
                      **kwargs: Any) -> Iterator[ProgressIndicator]:
    """
    Context manager for progress indicators.
    
    Args:
        message: Message to display
        indicator_type: Type of indicator ("spinner", "dots", "bar")
        file: Output file
        **kwargs: Additional arguments for the indicator
    
    Yields:
        Progress indicator instance
    
    Example:
        with progress_indicator("Processing changes", "spinner") as spinner:
            # Long running operation
            time.sleep(5)
    """
    if indicator_type == "spinner":
        indicator = Spinner(message, file=file, **kwargs)
    elif indicator_type == "dots":
        indicator = Dots(message, file=file, **kwargs)
    elif indicator_type == "bar":
        indicator = ProgressBar(message=message, file=file, **kwargs)
    else:
        raise ValueError(f"Unknown indicator type: {indicator_type}")
    
    try:
        indicator.start()
        yield indicator
    finally:
        indicator.stop()


def show_progress(items: list, message: str = "Processing", 
                 show_bar: bool = True) -> Iterator[Any]:
    """
    Show progress while iterating over items.
    
    Args:
        items: Items to iterate over
        message: Progress message
        show_bar: Whether to show progress bar
    
    Yields:
        Items from the input list
    
    Example:
        for item in show_progress(items, "Processing items"):
            process(item)
    """
    if show_bar and len(items) > 1:
        with ProgressBar(len(items), message) as bar:
            for i, item in enumerate(items):
                bar.update(i + 1)
                yield item
    else:
        # For single items or when bar is disabled, just use spinner
        with progress_indicator(message, "spinner") as spinner:
            for item in items:
                yield item


class StatusReporter:
    """
    Status reporter for providing user feedback during operations.
    
    Provides methods for reporting status, warnings, and errors
    in a consistent format matching Perl sqitch behavior.
    """
    
    def __init__(self, verbosity: int = 0, file: Optional[TextIO] = None) -> None:
        """
        Initialize status reporter.
        
        Args:
            verbosity: Verbosity level
            file: Output file (defaults to stderr)
        """
        self.verbosity = verbosity
        self.file = file or sys.stderr
    
    def status(self, message: str, level: int = 0) -> None:
        """
        Report status message.
        
        Args:
            message: Status message
            level: Minimum verbosity level required to show message
        """
        if self.verbosity >= level:
            self.file.write(f"{message}\n")
            self.file.flush()
    
    def info(self, message: str) -> None:
        """Report info message (verbosity >= 0)."""
        self.status(message, 0)
    
    def verbose(self, message: str) -> None:
        """Report verbose message (verbosity >= 1)."""
        self.status(f"# {message}", 1)
    
    def debug(self, message: str) -> None:
        """Report debug message (verbosity >= 2)."""
        self.status(f"debug: {message}", 2)
    
    def trace(self, message: str) -> None:
        """Report trace message (verbosity >= 3)."""
        self.status(f"trace: {message}", 3)
    
    def warning(self, message: str) -> None:
        """Report warning message (always shown)."""
        self.file.write(f"warning: {message}\n")
        self.file.flush()
    
    def error(self, message: str) -> None:
        """Report error message (always shown)."""
        self.file.write(f"error: {message}\n")
        self.file.flush()
    
    def success(self, message: str) -> None:
        """Report success message."""
        self.info(message)
    
    def operation_start(self, operation: str, target: str) -> None:
        """Report start of operation."""
        self.info(f"{operation.capitalize()} to {target}")
    
    def operation_complete(self, operation: str, count: int = 0) -> None:
        """Report completion of operation."""
        if count > 0:
            self.info(f"{operation.capitalize()} completed ({count} changes)")
        else:
            self.info(f"{operation.capitalize()} completed")
    
    def change_status(self, operation: str, change_name: str) -> None:
        """Report status of individual change."""
        self.verbose(f"{operation} {change_name}")


def confirm_action(message: str, default: Optional[bool] = None) -> bool:
    """
    Prompt user for confirmation.
    
    Args:
        message: Confirmation message
        default: Default response (True for yes, False for no, None for no default)
    
    Returns:
        True if user confirms, False otherwise
    
    Raises:
        IOError: If running unattended with no default
    """
    if default is True:
        prompt = f"{message} [Y/n] "
    elif default is False:
        prompt = f"{message} [y/N] "
    else:
        prompt = f"{message} [y/n] "
    
    # Check if running unattended
    if not sys.stdin.isatty():
        if default is not None:
            print(prompt + ("Y" if default else "N"))
            return default
        else:
            from ..core.exceptions import IOError
            raise IOError("Sqitch seems to be unattended and there is no default value for this question")
    
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            response = input(prompt).strip().lower()
            
            if not response:
                if default is not None:
                    return default
                else:
                    print('Please answer "y" or "n".')
                    continue
            
            if response in ('y', 'yes'):
                return True
            elif response in ('n', 'no'):
                return False
            else:
                print('Please answer "y" or "n".')
        
        except (EOFError, KeyboardInterrupt):
            print()  # New line after ^C
            return False
    
    from ..core.exceptions import IOError
    raise IOError("No valid answer after 3 attempts; aborting")


def prompt_for_input(message: str, default: Optional[str] = None) -> str:
    """
    Prompt user for input.
    
    Args:
        message: Prompt message
        default: Default value
    
    Returns:
        User input or default value
    
    Raises:
        IOError: If running unattended with no default
    """
    if default:
        prompt = f"{message} [{default}] "
    else:
        prompt = f"{message} "
    
    # Check if running unattended
    if not sys.stdin.isatty():
        if default is not None:
            print(prompt + default)
            return default
        else:
            from ..core.exceptions import IOError
            raise IOError("Sqitch seems to be unattended and there is no default value for this question")
    
    try:
        response = input(prompt).strip()
        return response if response else (default or "")
    except (EOFError, KeyboardInterrupt):
        print()  # New line after ^C
        from ..core.exceptions import IOError
        raise IOError("Operation cancelled by user")