# src/finops/discovery/discovery_client.py
"""Complete discovery client for comprehensive data collection."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
import structlog
import asyncio

from finops.clients.azure.aks_client import AKSClient
from finops.clients.azure.cost_client import CostClient
from finops.clients.azure.monitor_client import MonitorClient
from finops.clients.kubernetes.k8s_client import KubernetesClient
from finops.core.base_client import BaseClient
from finops.core.exceptions import DiscoveryException

logger = structlog.get_logger(__name__)


class DiscoveryClient(BaseClient):
    """Complete discovery client for comprehensive discovery."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config, "DiscoveryClient")
        self.subscription_id = config.get("subscription_id")
        self.resource_group = config.get("resource_group")
        self.cost_analysis_days = config.get("cost_analysis_days", 30)
        self.metrics_hours = config.get("metrics_hours", 24)
        
        # Clients
        self.aks_client: Optional[AKSClient] = None
        self.cost_client: Optional[CostClient] = None
        self.monitor_client: Optional[MonitorClient] = None
        self.k8s_client: Optional[KubernetesClient] = None
        
    def set_clients(self, aks_client: AKSClient, cost_client: CostClient, 
                   monitor_client: MonitorClient, k8s_client: Optional[KubernetesClient] = None):
        """Set the required clients."""
        self.aks_client = aks_client
        self.cost_client = cost_client
        self.monitor_client = monitor_client
        self.k8s_client = k8s_client    
        
    async def connect(self) -> None:
        """Connect all clients."""
        clients = [self.aks_client, self.cost_client, self.monitor_client]
        if self.k8s_client:
            clients.append(self.k8s_client)
            
        for client in clients:
            if client and not client.is_connected:
                await client.connect()
                
        self._connected = True
        self.logger.info("Discovery client connected")
        
    async def disconnect(self) -> None:
        """Disconnect all clients."""
        clients = [self.aks_client, self.cost_client, self.monitor_client, self.k8s_client]
        for client in clients:
            if client:
                await client.disconnect()
                
        self._connected = False
        self.logger.info("Discovery client disconnected")
        
    async def health_check(self) -> bool:
        """Check health of all clients."""
        if not self._connected:
            return False
            
        clients = [self.aks_client, self.cost_client, self.monitor_client]
        if self.k8s_client:
            clients.append(self.k8s_client)
            
        for client in clients:
            if client and not await client.health_check():
                return False
                
        return True
    
    async def discover_comprehensive_data(self) -> Dict[str, Any]:
        """Discover comprehensive data for resource group."""
        if not self._connected:
            raise DiscoveryException("DiscoveryClient", "Client not connected")
            
        self.logger.info(f"Starting comprehensive discovery for resource group: {self.resource_group}")
        
        discovery_data = {
            "discovery_metadata": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "resource_group": self.resource_group,
                "subscription_id": self.subscription_id,
                "cost_analysis_days": self.cost_analysis_days,
                "metrics_hours": self.metrics_hours
            },
            "resource_group_costs": {},
            "clusters": [],
            "summary": {
                "total_clusters": 0,
                "total_nodes": 0,
                "total_pods": 0,
                "total_cost": 0.0,
                "currency": "USD"
            }
        }
        
        try:
            # Get resource group costs first
            discovery_data["resource_group_costs"] = await self._get_resource_group_costs()
            
            # Discover clusters
            clusters = await self.aks_client.discover_clusters(self.resource_group)
            discovery_data["summary"]["total_clusters"] = len(clusters)
            
            # Process each cluster
            for cluster in clusters:
                cluster_data = await self._discover_cluster_comprehensive(cluster)
                discovery_data["clusters"].append(cluster_data)
                
                # Update summary
                discovery_data["summary"]["total_nodes"] += cluster_data.get("node_count", 0)
                discovery_data["summary"]["total_pods"] += cluster_data.get("pod_count", 0)
                discovery_data["summary"]["total_cost"] += cluster_data.get("cost_allocation", {}).get("total_cost", 0.0)
            
            self.logger.info(
                f"Comprehensive discovery completed",
                clusters=len(clusters),
                total_cost=discovery_data["summary"]["total_cost"]
            )
            
            return discovery_data
            
        except Exception as e:
            self.logger.error(f"Comprehensive discovery failed: {e}")
            raise DiscoveryException("DiscoveryClient", f"Discovery failed: {e}")
    
    async def _get_resource_group_costs(self) -> Dict[str, Any]:
        """Get comprehensive resource group costs."""
        try:
            # Get overall resource group costs
            rg_costs = await self.cost_client.get_resource_group_costs(
                self.resource_group, self.cost_analysis_days
            )
            
            # Get detailed resource-level costs
            detailed_costs = await self.cost_client.get_detailed_resource_costs(
                self.resource_group, self.cost_analysis_days
            )
            
            return {
                "overall_costs": rg_costs,
                "detailed_resource_costs": detailed_costs,
                "cost_summary": {
                    "total_cost": rg_costs.get("total_cost", 0.0),
                    "by_service": rg_costs.get("by_service", {}),
                    "by_resource_type": rg_costs.get("by_resource_type", {}),
                    "by_meter_category": rg_costs.get("by_meter_category", {}),
                    "daily_breakdown": rg_costs.get("daily_breakdown", []),
                    "currency": rg_costs.get("currency", "USD")
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get resource group costs: {e}")
            return {
                "error": str(e),
                "overall_costs": {},
                "detailed_resource_costs": {},
                "cost_summary": {"total_cost": 0.0, "currency": "USD"}
            }
    
    async def _discover_cluster_comprehensive(self, cluster: Dict[str, Any]) -> Dict[str, Any]:
        """Discover comprehensive data for a single cluster."""
        cluster_name = cluster["name"]
        resource_group = cluster["resource_group"]
        cluster_id = cluster["id"]
        
        self.logger.info(f"Processing cluster: {cluster_name}")
        
        cluster_data = {
            "cluster_info": cluster,
            "node_pools": [],
            "kubernetes_resources": {},
            "metrics": {},
            "cost_allocation": {},
            "utilization_summary": {},
            "node_count": 0,
            "pod_count": 0,
            "discovery_status": "in_progress"
        }
        
        try:
            # Discover node pools
            node_pools = await self.aks_client.discover_node_pools(cluster_name, resource_group)
            cluster_data["node_pools"] = node_pools
            cluster_data["node_count"] = sum(pool.get("count", 0) for pool in node_pools)
            
            # Get cluster metrics
            metrics = await self.monitor_client.get_cluster_metrics(
                cluster_id, cluster_name, self.metrics_hours
            )
            cluster_data["metrics"] = metrics
            cluster_data["utilization_summary"] = metrics.get("utilization_summary", {})
            
            # Discover Kubernetes resources if client available
            if self.k8s_client:
                # Get cluster credentials and connect
                try:
                    kubeconfig_data = await self.aks_client.get_cluster_credentials(cluster_name, resource_group)
                    
                    # Create new K8s client instance for this cluster
                    k8s_cluster_client = KubernetesClient(
                        config_dict={"cluster_name": cluster_name},
                        kubeconfig_data=kubeconfig_data
                    )
                    await k8s_cluster_client.connect()
                    
                    k8s_resources = await k8s_cluster_client.discover_all_resources()
                    cluster_data["kubernetes_resources"] = k8s_resources
                    cluster_data["pod_count"] = len(k8s_resources.get("pods", []))
                    
                    await k8s_cluster_client.disconnect()
                    
                except Exception as e:
                    self.logger.warning(f"Could not discover Kubernetes resources for {cluster_name}: {e}")
                    cluster_data["kubernetes_resources"] = {"error": str(e)}
            
            # Calculate cost allocation for this cluster
            cluster_data["cost_allocation"] = await self._calculate_cluster_cost_allocation(
                cluster_name, node_pools
            )
            
            cluster_data["discovery_status"] = "completed"
            
        except Exception as e:
            self.logger.error(f"Error processing cluster {cluster_name}: {e}")
            cluster_data["discovery_status"] = "failed"
            cluster_data["error"] = str(e)
        
        return cluster_data
    
    async def _calculate_cluster_cost_allocation(self, cluster_name: str, 
                                               node_pools: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate cost allocation for cluster based on node pools."""
        cost_allocation = {
            "cluster_name": cluster_name,
            "total_cost": 0.0,
            "compute_cost": 0.0,
            "storage_cost": 0.0,
            "network_cost": 0.0,
            "currency": "USD",
            "node_pool_breakdown": []
        }
        
        try:
            for pool in node_pools:
                # Calculate cost for this node pool based on VM size and count
                pool_cost = self._calculate_node_pool_cost(pool)
                cost_allocation["total_cost"] += pool_cost["total_cost"]
                cost_allocation["compute_cost"] += pool_cost["compute_cost"]
                cost_allocation["storage_cost"] += pool_cost["storage_cost"]
                cost_allocation["network_cost"] += pool_cost["network_cost"]
                
                cost_allocation["node_pool_breakdown"].append({
                    "node_pool_name": pool["name"],
                    "vm_size": pool["vm_size"],
                    "node_count": pool["count"],
                    "cost_breakdown": pool_cost
                })
        
        except Exception as e:
            self.logger.error(f"Error calculating cluster cost allocation: {e}")
            cost_allocation["error"] = str(e)
        
        return cost_allocation
    
    def _calculate_node_pool_cost(self, pool: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate estimated cost for a node pool."""
        vm_size = pool.get("vm_size", "Standard_D2s_v3")
        node_count = pool.get("count", 1)
        
        # Azure VM pricing estimates (would integrate with Azure Pricing API in production)
        vm_pricing = {
            "Standard_D2s_v3": {"hourly": 0.096, "monthly": 70.0},
            "Standard_D4s_v3": {"hourly": 0.192, "monthly": 140.0},
            "Standard_D8s_v3": {"hourly": 0.384, "monthly": 280.0},
            "Standard_D16s_v3": {"hourly": 0.768, "monthly": 560.0},
            "Standard_B2s": {"hourly": 0.041, "monthly": 30.0},
            "Standard_B4ms": {"hourly": 0.166, "monthly": 120.0},
            "Standard_E2s_v3": {"hourly": 0.134, "monthly": 98.0},
            "Standard_E4s_v3": {"hourly": 0.268, "monthly": 196.0},
            "Standard_F2s_v2": {"hourly": 0.085, "monthly": 62.0},
            "Standard_F4s_v2": {"hourly": 0.169, "monthly": 124.0}
        }
        
        base_pricing = vm_pricing.get(vm_size, {"hourly": 0.096, "monthly": 70.0})
        monthly_vm_cost = base_pricing["monthly"] * node_count
        
        # Apply spot pricing discount if applicable
        if pool.get("scale_set_priority") == "Spot":
            monthly_vm_cost *= 0.2  # 80% discount for spot instances
        
        # Calculate storage cost (OS disk)
        os_disk_size = pool.get("os_disk_size_gb", 128)
        monthly_storage_cost = os_disk_size * 0.045 * node_count  # $0.045 per GB/month for standard SSD
        
        # Estimate network cost (5% of compute cost)
        monthly_network_cost = monthly_vm_cost * 0.05
        
        total_monthly_cost = monthly_vm_cost + monthly_storage_cost + monthly_network_cost
        
        # Convert to daily cost for the analysis period
        daily_cost = total_monthly_cost / 30
        period_cost = daily_cost * self.cost_analysis_days
        
        return {
            "total_cost": round(period_cost, 2),
            "compute_cost": round((monthly_vm_cost / 30) * self.cost_analysis_days, 2),
            "storage_cost": round((monthly_storage_cost / 30) * self.cost_analysis_days, 2),
            "network_cost": round((monthly_network_cost / 30) * self.cost_analysis_days, 2),
            "currency": "USD",
            "period_days": self.cost_analysis_days,
            "pricing_model": "spot" if pool.get("scale_set_priority") == "Spot" else "regular"
        }