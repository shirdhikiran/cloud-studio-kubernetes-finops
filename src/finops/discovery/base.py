"""Base discovery service interface."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import structlog
from datetime import datetime, timezone 

from finops.core.exceptions import DiscoveryException

logger = structlog.get_logger(__name__)


class BaseDiscoveryService(ABC):
    """Abstract base class for all discovery services."""
    
    def __init__(self, client, config: Dict[str, Any]):
        self.client = client
        self.config = config
        self.logger = logger.bind(service=self.__class__.__name__)
        self._discovery_metadata = {
            'start_time': None,
            'end_time': None,
            'duration_seconds': None,
            'resources_discovered': 0,
            'errors': []
        }
    
    @abstractmethod
    async def discover(self) -> List[Dict[str, Any]]:
        """Perform discovery operation."""
        pass
    
    @abstractmethod
    def get_discovery_type(self) -> str:
        """Get the type of discovery this service performs."""
        pass
    
    async def discover_with_metadata(self) -> Dict[str, Any]:
        """Perform discovery with metadata tracking."""
        self._discovery_metadata['start_time'] = datetime.now(timezone.utc)
        
        try:
            results = await self.discover()
            self._discovery_metadata['resources_discovered'] = len(results) if results else 0
            status = 'success'
            error = None
            
        except Exception as e:
            self.logger.error(f"Discovery failed for {self.get_discovery_type()}", error=str(e))
            results = []
            status = 'failed'
            error = str(e)
            self._discovery_metadata['errors'].append(error)
        
        finally:
            self._discovery_metadata['end_time'] = datetime.now(timezone.utc)
            if self._discovery_metadata['start_time']:
                duration = self._discovery_metadata['end_time'] - self._discovery_metadata['start_time']
                self._discovery_metadata['duration_seconds'] = duration.total_seconds()
        
        return {
            'type': self.get_discovery_type(),
            'status': status,
            'data': results,
            'error': error,
            'metadata': self._discovery_metadata.copy()
        }
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get discovery metadata."""
        return self._discovery_metadata.copy()