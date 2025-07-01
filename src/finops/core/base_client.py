"""Base client interface for all external service clients."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import structlog

logger = structlog.get_logger(__name__)


class BaseClient(ABC):
    """Abstract base class for all external service clients."""
    
    def __init__(self, config: Dict[str, Any], name: Optional[str] = None):
        self.config = config
        self.name = name or self.__class__.__name__
        self._connected = False
        self.logger = logger.bind(client=self.name)
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the external service."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the external service."""
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the client connection is healthy."""
        pass
    
    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._connected
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
