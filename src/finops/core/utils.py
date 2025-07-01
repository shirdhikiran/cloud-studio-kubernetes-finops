"""Utility functions and decorators."""

import asyncio
import functools
import logging.config
import structlog
import yaml
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar, Union
from tenacity import retry, stop_after_attempt, wait_exponential

T = TypeVar('T')


def retry_with_backoff(
    max_retries: int = 3,
    backoff_factor: float = 1.5,
    max_wait: float = 60.0
):
    """Decorator for retry with exponential backoff."""
    return retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=backoff_factor, max=max_wait),
        reraise=True
    )


def setup_logging(config_path: Optional[Union[str, Path]] = None, log_level: str = "INFO") -> None:
    """Setup structured logging configuration."""
    if config_path and Path(config_path).exists():
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    else:
        # Default configuration
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if config_path else structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def safe_get(dictionary: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Safely get a value from a nested dictionary using dot notation."""
    keys = key.split('.')
    value = dictionary
    
    try:
        for k in keys:
            value = value[k]
        return value
    except (KeyError, TypeError):
        return default


def parse_resource_string(resource_str: str, resource_type: str = "memory") -> float:
    """Parse Kubernetes resource strings (e.g., '100Mi', '2Gi') to bytes or millicores."""
    if not resource_str:
        return 0.0
    
    resource_str = resource_str.strip().upper()
    
    if resource_type.lower() == "cpu":
        # Parse CPU (millicores)
        if resource_str.endswith('M'):
            return float(resource_str[:-1])
        return float(resource_str) * 1000  # Convert cores to millicores
    
    elif resource_type.lower() == "memory":
        # Parse memory to bytes
        units = {
            'KI': 1024,
            'MI': 1024**2,
            'GI': 1024**3,
            'TI': 1024**4,
            'K': 1000,
            'M': 1000**2,
            'G': 1000**3,
            'T': 1000**4
        }
        
        for unit, multiplier in units.items():
            if resource_str.endswith(unit):
                return float(resource_str[:-len(unit)]) * multiplier
        
        # Assume bytes if no unit
        try:
            return float(resource_str)
        except ValueError:
            return 0.0
    
    return 0.0


def format_bytes(bytes_value: float, unit: str = "auto") -> str:
    """Format bytes to human readable format."""
    if unit == "auto":
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} PB"
    
    units = {'B': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4}
    if unit.upper() in units:
        return f"{bytes_value / units[unit.upper()]:.2f} {unit.upper()}"
    
    return str(bytes_value)


def format_cpu(millicores: float) -> str:
    """Format millicores to human readable format."""
    if millicores >= 1000:
        return f"{millicores / 1000:.2f} cores"
    return f"{millicores:.0f}m"


async def gather_with_concurrency(
    coros: list,
    max_concurrency: int = 10,
    return_exceptions: bool = True
) -> list:
    """Execute coroutines with limited concurrency."""
    semaphore = asyncio.Semaphore(max_concurrency)
    
    async def limited_coro(coro):
        async with semaphore:
            return await coro
    
    limited_coros = [limited_coro(coro) for coro in coros]
    return await asyncio.gather(*limited_coros, return_exceptions=return_exceptions)

