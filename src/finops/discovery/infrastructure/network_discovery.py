"""Network resource discovery service."""

from typing import Dict, Any, List
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.azure.resource_client import ResourceClient

logger = structlog.get_logger(__name__)


class NetworkDiscoveryService(BaseDiscoveryService):
    """Discovery service for network resources."""
    
    def __init__(self, resource_client: ResourceClient, config: Dict[str, Any]):
        super().__init__(resource_client, config)
        self.resource_group = config.get("resource_group")
    
    async def discover(self) -> List[Dict[str, Any]]:
        """Discover network resources."""
        self.logger.info("Starting network resource discovery")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        network_resources = await self.client.discover_network_resources(self.resource_group)
        
        # Flatten the network resources structure
        all_resources = []
        for resource_type, resources in network_resources.items():
            for resource in resources:
                resource['resource_type'] = resource_type
                all_resources.append(resource)
        
        self.logger.info(f"Discovered {len(all_resources)} network resources")
        return all_resources
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "network_resources"