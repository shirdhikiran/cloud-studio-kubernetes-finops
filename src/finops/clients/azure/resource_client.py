"""Azure Resource Management client."""

from typing import Dict, Any, List, Optional
import structlog
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.core.exceptions import AzureError

from finops.core.base_client import BaseClient
from finops.core.exceptions import ClientConnectionException, DiscoveryException
from finops.core.utils import retry_with_backoff

logger = structlog.get_logger(__name__)


class ResourceClient(BaseClient):
    """Client for Azure Resource Management operations."""
    
    def __init__(self, credential, subscription_id: str, config: Dict[str, Any]):
        super().__init__(config, "ResourceClient")
        self.credential = credential
        self.subscription_id = subscription_id
        
        self._resource_client = None
        self._compute_client = None
        self._network_client = None
    
    async def connect(self) -> None:
        """Connect to Azure Resource Management services."""
        try:
            self._resource_client = ResourceManagementClient(
                credential=self.credential,
                subscription_id=self.subscription_id
            )
            
            self._compute_client = ComputeManagementClient(
                credential=self.credential,
                subscription_id=self.subscription_id
            )
            
            self._network_client = NetworkManagementClient(
                credential=self.credential,
                subscription_id=self.subscription_id
            )
            
            self._connected = True
            self.logger.info("Azure Resource Management clients connected successfully")
            
        except Exception as e:
            raise ClientConnectionException("AzureResource", f"Connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from Azure Resource Management services."""
        for client in [self._resource_client, self._compute_client, self._network_client]:
            if client:
                client.close()
        
        self._connected = False
        self.logger.info("Azure Resource Management clients disconnected")
    
    async def health_check(self) -> bool:
        """Check Resource Management client health."""
        try:
            if not self._connected or not self._resource_client:
                return False
            
            # Try to list resource groups as health check
            list(self._resource_client.resource_groups.list())
            return True
            
        except Exception as e:
            self.logger.warning("Resource Management health check failed", error=str(e))
            return False
    
    @retry_with_backoff(max_retries=3)
    async def discover_resource_groups(self) -> List[Dict[str, Any]]:
        """Discover resource groups."""
        if not self._connected:
            raise DiscoveryException("ResourceManagement", "Client not connected")
        
        try:
            resource_groups = []
            
            for rg in self._resource_client.resource_groups.list():
                rg_data = {
                    'name': rg.name,
                    'location': rg.location,
                    'tags': rg.tags or {},
                    'provisioning_state': rg.properties.provisioning_state if rg.properties else None,
                    'managed_by': rg.managed_by
                }
                resource_groups.append(rg_data)
            
            self.logger.info(f"Discovered {len(resource_groups)} resource groups")
            return resource_groups
            
        except AzureError as e:
            raise DiscoveryException("ResourceManagement", f"Failed to discover resource groups: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_virtual_machines(self, resource_group: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover virtual machines."""
        if not self._connected:
            raise DiscoveryException("ComputeManagement", "Client not connected")
        
        try:
            vms = []
            
            if resource_group:
                vm_list = self._compute_client.virtual_machines.list(resource_group)
            else:
                vm_list = self._compute_client.virtual_machines.list_all()
            
            for vm in vm_list:
                vm_data = await self._extract_vm_data(vm)
                vms.append(vm_data)
            
            self.logger.info(f"Discovered {len(vms)} virtual machines")
            return vms
            
        except AzureError as e:
            raise DiscoveryException("ComputeManagement", f"Failed to discover VMs: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_storage_accounts(self, resource_group: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover storage accounts."""
        if not self._connected:
            raise DiscoveryException("ResourceManagement", "Client not connected")
        
        try:
            storage_accounts = []
            
            # Filter for storage account resources
            filter_str = "resourceType eq 'Microsoft.Storage/storageAccounts'"
            if resource_group:
                resources = self._resource_client.resources.list_by_resource_group(
                    resource_group, filter=filter_str
                )
            else:
                resources = self._resource_client.resources.list(filter=filter_str)
            
            for resource in resources:
                storage_data = {
                    'name': resource.name,
                    'resource_group': resource.id.split('/')[4],
                    'location': resource.location,
                    'kind': resource.kind,
                    'sku': resource.sku.name if resource.sku else None,
                    'tags': resource.tags or {}
                }
                storage_accounts.append(storage_data)
            
            self.logger.info(f"Discovered {len(storage_accounts)} storage accounts")
            return storage_accounts
            
        except AzureError as e:
            raise DiscoveryException("ResourceManagement", f"Failed to discover storage accounts: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_network_resources(self, resource_group: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Discover network resources."""
        if not self._connected:
            raise DiscoveryException("NetworkManagement", "Client not connected")
        
        try:
            network_resources = {
                'load_balancers': [],
                'public_ips': [],
                'virtual_networks': [],
                'network_security_groups': []
            }
            
            # Load Balancers
            if resource_group:
                lb_list = self._network_client.load_balancers.list(resource_group)
            else:
                lb_list = self._network_client.load_balancers.list_all()
            
            for lb in lb_list:
                lb_data = {
                    'name': lb.name,
                    'resource_group': lb.id.split('/')[4],
                    'location': lb.location,
                    'sku': lb.sku.name if lb.sku else None,
                    'frontend_ip_configurations': len(lb.frontend_ip_configurations or []),
                    'backend_address_pools': len(lb.backend_address_pools or []),
                    'load_balancing_rules': len(lb.load_balancing_rules or []),
                    'tags': lb.tags or {}
                }
                network_resources['load_balancers'].append(lb_data)
            
            # Public IPs
            if resource_group:
                pip_list = self._network_client.public_ip_addresses.list(resource_group)
            else:
                pip_list = self._network_client.public_ip_addresses.list_all()
            
            for pip in pip_list:
                pip_data = {
                    'name': pip.name,
                    'resource_group': pip.id.split('/')[4],
                    'location': pip.location,
                    'ip_address': pip.ip_address,
                    'allocation_method': pip.public_ip_allocation_method,
                    'sku': pip.sku.name if pip.sku else None,
                    'tags': pip.tags or {}
                }
                network_resources['public_ips'].append(pip_data)
            
            # Virtual Networks
            if resource_group:
                vnet_list = self._network_client.virtual_networks.list(resource_group)
            else:
                vnet_list = self._network_client.virtual_networks.list_all()
            
            for vnet in vnet_list:
                vnet_data = {
                    'name': vnet.name,
                    'resource_group': vnet.id.split('/')[4],
                    'location': vnet.location,
                    'address_space': vnet.address_space.address_prefixes if vnet.address_space else [],
                    'subnets': len(vnet.subnets or []),
                    'tags': vnet.tags or {}
                }
                network_resources['virtual_networks'].append(vnet_data)
            
            total_resources = sum(len(resources) for resources in network_resources.values())
            self.logger.info(f"Discovered {total_resources} network resources")
            return network_resources
            
        except AzureError as e:
            raise DiscoveryException("NetworkManagement", f"Failed to discover network resources: {e}")
    
    async def _extract_vm_data(self, vm) -> Dict[str, Any]:
        """Extract VM data from Azure response."""
        return {
            'name': vm.name,
            'resource_group': vm.id.split('/')[4],
            'location': vm.location,
            'vm_size': vm.hardware_profile.vm_size if vm.hardware_profile else None,
            'os_type': vm.storage_profile.os_disk.os_type if vm.storage_profile and vm.storage_profile.os_disk else None,
            'provisioning_state': vm.provisioning_state,
            'power_state': None,  # Would need additional call to get power state
            'availability_set': vm.availability_set.id.split('/')[-1] if vm.availability_set else None,
            'tags': vm.tags or {},
            'zones': vm.zones or []
        }
