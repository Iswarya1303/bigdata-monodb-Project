"""Utility functions for the MongoDB pipeline."""

import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Generator, TypeVar

from loguru import logger

T = TypeVar("T")


@contextmanager
def timer(operation_name: str) -> Generator[None, None, None]:
    """Context manager to time operations.
    
    Args:
        operation_name: Name of the operation being timed
        
    Yields:
        None
        
    Example:
        with timer("data loading"):
            load_data()
    """
    start = time.perf_counter()
    logger.info(f"Starting: {operation_name}")
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.info(f"Completed: {operation_name} in {elapsed:.2f}s")


def retry(
    max_attempts: int = 3, 
    delay: float = 1.0, 
    backoff: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,)
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to retry a function on failure.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exceptions to catch
        
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            current_delay = delay
            last_exception: Exception | None = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt}/{max_attempts}): {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
            
            raise last_exception  # type: ignore[misc]
        return wrapper
    return decorator


def format_bytes(size_bytes: int) -> str:
    """Format bytes to human-readable string.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Human-readable size string
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024  # type: ignore[assignment]
    return f"{size_bytes:.2f} PB"


def format_number(num: int | float) -> str:
    """Format large numbers with commas.
    
    Args:
        num: Number to format
        
    Returns:
        Formatted number string
    """
    return f"{num:,.0f}" if isinstance(num, (int, float)) else str(num)


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Human-readable duration string
    """
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{int(minutes)}m {remaining_seconds:.0f}s"
    else:
        hours = seconds // 3600
        remaining = seconds % 3600
        minutes = remaining // 60
        return f"{int(hours)}h {int(minutes)}m"


def chunk_list(lst: list[T], chunk_size: int) -> Generator[list[T], None, None]:
    """Split a list into chunks of specified size.
    
    Args:
        lst: List to split
        chunk_size: Size of each chunk
        
    Yields:
        Chunks of the list
    """
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, returning default on division by zero.
    
    Args:
        numerator: The dividend
        denominator: The divisor
        default: Value to return if denominator is zero
        
    Returns:
        Result of division or default
    """
    return numerator / denominator if denominator != 0 else default

