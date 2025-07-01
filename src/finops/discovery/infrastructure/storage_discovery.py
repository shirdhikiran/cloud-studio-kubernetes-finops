"""Storage resource discovery service."""

from typing import Dict, Any, List
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.azure.resource_client import ResourceClient

logger = structlog.get_logger(__name__)


class StorageDiscoveryService(BaseDiscoveryService):
    """Discovery service for storage resources."""
    
    def __init__(self, resource_client: ResourceClient, config: Dict[str, Any]):
        super().__init__(resource_client, config)
        self.resource_group = config.get("resource_group")
    
    async def discover(self) -> List[Dict[str, Any]]:
        """Discover storage resources."""
        self.logger.info("Starting storage resource discovery")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        # Discover storage accounts
        storage_accounts = await self.client.discover_storage_accounts(self.resource_group)
        
        # Add resource type to each item
        for storage_account in storage_accounts:
            storage_account['resource_type'] = 'storage_account'
        
        self.logger.info(f"Discovered {len(storage_accounts)} storage resources")
        return storage_accounts
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "storage_resources"