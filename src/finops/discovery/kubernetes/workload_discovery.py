"""Kubernetes workload discovery service."""

from typing import Dict, Any, List
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.kubernetes.k8s_client import KubernetesClient

logger = structlog.get_logger(__name__)


class WorkloadDiscoveryService(BaseDiscoveryService):
    """Discovery service for Kubernetes workloads."""
    
    def __init__(self, k8s_client: KubernetesClient, config: Dict[str, Any]):
        super().__init__(k8s_client, config)
        self.namespace = config.get("namespace")
    
    async def discover(self) -> List[Dict[str, Any]]:
        """Discover Kubernetes workloads."""
        self.logger.info("Starting Kubernetes workload discovery")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        workload_resources = {
            'pods': await self.client.discover_pods(self.namespace),
            'deployments': await self.client.discover_deployments(self.namespace),
            'services': await self.client.discover_services(self.namespace),
        }
        
        # Flatten structure
        all_workloads = []
        for resource_type, resources in workload_resources.items():
            for resource in resources:
                resource['resource_type'] = resource_type
                all_workloads.append(resource)
        
        self.logger.info(f"Discovered {len(all_workloads)} workload resources")
        return all_workloads
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "kubernetes_workloads"