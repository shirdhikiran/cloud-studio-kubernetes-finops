"""Azure Kubernetes Service client."""

from typing import Dict, Any, List, Optional
import structlog
from azure.mgmt.containerservice import ContainerServiceClient
from azure.core.exceptions import AzureError

from finops.core.base_client import BaseClient
from finops.core.exceptions import ClientConnectionException, DiscoveryException
from finops.core.utils import retry_with_backoff

logger = structlog.get_logger(__name__)


class AKSClient(BaseClient):
    """Client for Azure Kubernetes Service operations."""
    
    def __init__(self, credential, subscription_id: str, config: Dict[str, Any]):
        super().__init__(config, "AKSClient")
        self.credential = credential
        self.subscription_id = subscription_id
        self.resource_group = config.get("resource_group")
        self._client = None
    
    async def connect(self) -> None:
        """Connect to AKS service."""
        try:
            self._client = ContainerServiceClient(
                credential=self.credential,
                subscription_id=self.subscription_id
            )
            self._connected = True
            self.logger.info("AKS client connected successfully")
        except Exception as e:
            raise ClientConnectionException("AKS", f"Connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from AKS service."""
        if self._client:
            self._client.close()
            self._connected = False
            self.logger.info("AKS client disconnected")
    
    async def health_check(self) -> bool:
        """Check AKS client health."""
        try:
            if not self._connected or not self._client:
                return False
            
            # Try to list clusters as a health check
            list(self._client.managed_clusters.list())
            return True
        except Exception as e:
            self.logger.warning("AKS health check failed", error=str(e))
            return False
    
    @retry_with_backoff(max_retries=3)
    async def discover_clusters(self, resource_group: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover AKS clusters."""
        if not self._connected:
            raise DiscoveryException("AKS", "Client not connected")
        
        clusters = []
        target_rg = resource_group or self.resource_group
        
        try:
            if target_rg:
                cluster_list = self._client.managed_clusters.list_by_resource_group(target_rg)
                self.logger.info(f"Discovering clusters in resource group: {target_rg}")
            else:
                cluster_list = self._client.managed_clusters.list()
                self.logger.info("Discovering clusters in all resource groups")
            
            for cluster in cluster_list:
                cluster_data = await self._extract_cluster_data(cluster)
                clusters.append(cluster_data)
            
            self.logger.info(f"Discovered {len(clusters)} AKS clusters")
            return clusters
            
        except AzureError as e:
            raise DiscoveryException("AKS", f"Failed to discover clusters: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_node_pools(self, cluster_name: str, resource_group: str) -> List[Dict[str, Any]]:
        """Discover node pools for a cluster."""
        if not self._connected:
            raise DiscoveryException("AKS", "Client not connected")
        
        try:
            pools = self._client.agent_pools.list(resource_group, cluster_name)
            node_pools = []
            
            for pool in pools:
                pool_data = await self._extract_node_pool_data(pool, cluster_name)
                node_pools.append(pool_data)
            
            self.logger.info(f"Discovered {len(node_pools)} node pools for cluster {cluster_name}")
            return node_pools
            
        except AzureError as e:
            raise DiscoveryException("AKS", f"Failed to discover node pools: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def get_cluster_credentials(self, cluster_name: str, resource_group: str) -> bytes:
        """Get cluster credentials."""
        if not self._connected:
            raise DiscoveryException("AKS", "Client not connected")
        
        try:
            credentials = self._client.managed_clusters.list_cluster_user_credentials(
                resource_group, cluster_name
            )
            return credentials.kubeconfigs[0].value
            
        except AzureError as e:
            raise DiscoveryException("AKS", f"Failed to get cluster credentials: {e}")
    
    async def _extract_cluster_data(self, cluster) -> Dict[str, Any]:
        """Extract cluster data from Azure response."""
        return {
            'id': cluster.id,
            'name': cluster.name,
            'resource_group': cluster.id.split('/')[4],
            'location': cluster.location,
            'kubernetes_version': cluster.kubernetes_version,
            'provisioning_state': cluster.provisioning_state,
            'fqdn': cluster.fqdn,
            'node_resource_group': cluster.node_resource_group,
            'dns_prefix': cluster.dns_prefix,
            'enable_rbac': cluster.enable_rbac,
            'network_profile': {
                'network_plugin': cluster.network_profile.network_plugin if cluster.network_profile else None,
                'service_cidr': cluster.network_profile.service_cidr if cluster.network_profile else None,
                'dns_service_ip': cluster.network_profile.dns_service_ip if cluster.network_profile else None,
                'pod_cidr': cluster.network_profile.pod_cidr if cluster.network_profile else None,
                'load_balancer_sku': cluster.network_profile.load_balancer_sku if cluster.network_profile else None,
            },
            'addon_profiles': self._process_addon_profiles(cluster.addon_profiles),
            'sku': {
                'name': cluster.sku.name if cluster.sku else None,
                'tier': cluster.sku.tier if cluster.sku else None
            },
            'tags': cluster.tags or {},
            'created_time': cluster.system_data.created_at if cluster.system_data else None,
            'last_modified': cluster.system_data.last_modified_at if cluster.system_data else None,
        }
    
    async def _extract_node_pool_data(self, pool, cluster_name: str) -> Dict[str, Any]:
        """Extract node pool data from Azure response."""
        return {
            'name': pool.name,
            'cluster_name': cluster_name,
            'vm_size': pool.vm_size,
            'os_type': pool.os_type,
            'os_disk_size_gb': pool.os_disk_size_gb,
            'count': pool.count,
            'min_count': pool.min_count,
            'max_count': pool.max_count,
            'auto_scaling_enabled': pool.enable_auto_scaling,
            'node_taints': pool.node_taints or [],
            'node_labels': pool.node_labels or {},
            'availability_zones': pool.availability_zones or [],
            'mode': pool.mode,
            'orchestrator_version': pool.orchestrator_version,
            'provisioning_state': pool.provisioning_state,
            'power_state': pool.power_state.code if pool.power_state else None,
            'max_pods': pool.max_pods,
            'os_disk_type': pool.os_disk_type,
            'scale_set_priority': pool.scale_set_priority,
            'spot_max_price': pool.spot_max_price,
            'upgrade_settings': {
                'max_surge': pool.upgrade_settings.max_surge if pool.upgrade_settings else None
            },
            'tags': pool.tags or {}
        }
    
    def _process_addon_profiles(self, addon_profiles: Optional[Dict]) -> Dict[str, Any]:
        """Process addon profiles to extract relevant information."""
        processed_addons = {}
        if addon_profiles:
            for addon_name, addon_config in addon_profiles.items():
                processed_addons[addon_name] = {
                    'enabled': addon_config.enabled if hasattr(addon_config, 'enabled') else False,
                    'config': addon_config.config if hasattr(addon_config, 'config') else {}
                }
        return processed_addons
