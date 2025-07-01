"""Node pool discovery service."""

from typing import Dict, Any, List
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.azure.aks_client import AKSClient

logger = structlog.get_logger(__name__)


class NodePoolDiscoveryService(BaseDiscoveryService):
    """Discovery service for AKS node pools."""
    
    def __init__(self, aks_client: AKSClient, config: Dict[str, Any]):
        super().__init__(aks_client, config)
        self.cluster_name = config.get("cluster_name")
        self.resource_group = config.get("resource_group")
    
    async def discover(self) -> List[Dict[str, Any]]:
        """Discover node pools for clusters."""
        self.logger.info("Starting node pool discovery")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        all_node_pools = []
        
        if self.cluster_name and self.resource_group:
            # Discover for specific cluster
            node_pools = await self.client.discover_node_pools(
                self.cluster_name, self.resource_group
            )
            all_node_pools.extend(node_pools)
        else:
            # Discover for all clusters
            clusters = await self.client.discover_clusters(self.resource_group)
            
            for cluster in clusters:
                try:
                    node_pools = await self.client.discover_node_pools(
                        cluster['name'], cluster['resource_group']
                    )
                    all_node_pools.extend(node_pools)
                except Exception as e:
                    self.logger.error(
                        f"Failed to discover node pools for cluster {cluster['name']}", 
                        error=str(e)
                    )
        
        self.logger.info(f"Discovered {len(all_node_pools)} node pools")
        return all_node_pools
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "node_pools"