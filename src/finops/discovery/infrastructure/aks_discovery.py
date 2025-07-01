"""AKS cluster discovery service."""

from typing import Dict, Any, List
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.azure.aks_client import AKSClient

logger = structlog.get_logger(__name__)


class AKSDiscoveryService(BaseDiscoveryService):
    """Discovery service for AKS clusters."""
    
    def __init__(self, aks_client: AKSClient, config: Dict[str, Any]):
        super().__init__(aks_client, config)
        self.resource_group = config.get("resource_group")
    
    async def discover(self) -> List[Dict[str, Any]]:
        """Discover AKS clusters."""
        self.logger.info("Starting AKS cluster discovery")
        
        # Ensure client is connected
        if not self.client.is_connected:
            await self.client.connect()
        
        clusters = await self.client.discover_clusters(self.resource_group)
        
        self.logger.info(f"Discovered {len(clusters)} AKS clusters")
        return clusters
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "aks_clusters"