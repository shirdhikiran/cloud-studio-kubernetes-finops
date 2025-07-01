"""Kubernetes cluster resource discovery service."""

from typing import Dict, Any, List
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.kubernetes.k8s_client import KubernetesClient

logger = structlog.get_logger(__name__)


class ClusterDiscoveryService(BaseDiscoveryService):
    """Discovery service for Kubernetes cluster resources."""
    
    def __init__(self, k8s_client: KubernetesClient, config: Dict[str, Any]):
        super().__init__(k8s_client, config)
        self.namespace = config.get("namespace")
    
    async def discover(self) -> List[Dict[str, Any]]:
        """Discover Kubernetes cluster resources."""
        self.logger.info("Starting Kubernetes cluster discovery")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        cluster_resources = {
            'namespaces': await self.client.discover_namespaces(),
            'nodes': await self.client.discover_nodes(),
        }
        
        # Flatten structure for consistent return format
        all_resources = []
        for resource_type, resources in cluster_resources.items():
            for resource in resources:
                resource['resource_type'] = resource_type
                all_resources.append(resource)
        
        self.logger.info(f"Discovered {len(all_resources)} cluster resources")
        return all_resources
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "kubernetes_cluster"