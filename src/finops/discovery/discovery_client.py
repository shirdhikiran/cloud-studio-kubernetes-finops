# src/finops/discovery/discovery_client.py
"""Phase 1 Discovery - Pure data collection without analysis."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import structlog

from finops.clients.azure.aks_client import AKSClient
from finops.clients.azure.cost_client import CostClient
from finops.clients.azure.monitor_client import MonitorClient
from finops.clients.kubernetes.k8s_client import KubernetesClient
from finops.core.base_client import BaseClient
from finops.core.exceptions import DiscoveryException

logger = structlog.get_logger(__name__)


class DiscoveryClient(BaseClient):
    """Phase 1 Discovery - Pure data collection client."""
    
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
        """Phase 1: Pure data discovery without analysis."""
        if not self._connected:
            raise DiscoveryException("DiscoveryClient", "Client not connected")
            
        self.logger.info(f"Starting Phase 1 discovery for: {self.resource_group}")
        
        # Pure discovery data structure
        discovery_data = {
            "discovery_metadata": {
                "phase": "1_discovery",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "resource_group": self.resource_group,
                "subscription_id": self.subscription_id,
                "cost_analysis_days": self.cost_analysis_days,
                "metrics_hours": self.metrics_hours,
                "discovery_version": "1.0.0"
            },
            "raw_cost_data": {},
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
            # # Collect raw cost data
            # discovery_data["raw_cost_data"] = await self._collect_raw_cost_data()
            
            # Discover clusters
            clusters = await self.aks_client.discover_clusters(self.resource_group)
            discovery_data["summary"]["total_clusters"] = len(clusters)
            
            # Process each cluster - pure data collection only
            for cluster in clusters:
                cluster_data = await self._discover_cluster_raw_data(cluster)
                discovery_data["clusters"].append(cluster_data)
                
                # Update summary counts
                discovery_data["summary"]["total_nodes"] += cluster_data.get("node_count", 0)
                discovery_data["summary"]["total_pods"] += cluster_data.get("pod_count", 0)
            
            # Set total cost from raw cost data
            raw_costs = discovery_data["raw_cost_data"]
            discovery_data["summary"]["total_cost"] = raw_costs.get("total_cost", 0.0)
            
            self.logger.info(
                f"Phase 1 discovery completed",
                clusters=len(clusters),
                total_cost=discovery_data["summary"]["total_cost"]
            )
            
            return discovery_data
            
        except Exception as e:
            self.logger.error(f"Phase 1 discovery failed: {e}")
            raise DiscoveryException("DiscoveryClient", f"Discovery failed: {e}")
    
    async def _discover_cluster_raw_data(self, cluster: Dict[str, Any]) -> Dict[str, Any]:
        """Discover raw cluster data without any processing or analysis."""
        cluster_name = cluster["name"]
        resource_group = cluster["resource_group"]
        cluster_id = cluster["id"]
        
        self.logger.info(f"Collecting raw data for cluster: {cluster_name}")
        
        cluster_data = {
            "cluster_info": cluster,
            "node_pools": [],
            "kubernetes_resources": {},
            "raw_metrics": {},
            "detailed_costs": {},
            "node_count": 0,
            "pod_count": 0,
            "collection_status": "in_progress"
        }
        
        try:
            # Collect node pools data
            node_pools = await self.aks_client.discover_node_pools(cluster_name, resource_group)
            cluster_data["node_pools"] = node_pools if node_pools is not None else []
            cluster_data["node_count"] = sum(pool.get("count", 0) for pool in cluster_data["node_pools"] if isinstance(pool, dict))
            
            # Collect raw metrics data
            try:
                raw_metrics = await self.monitor_client.collect_cluster_metrics(
                    cluster_id, cluster_name, self.metrics_hours
                )
                cluster_data["raw_metrics"] = raw_metrics if raw_metrics else {}
                
                self.logger.info(
                    f"Raw metrics collected for {cluster_name}",
                    metrics_collected=raw_metrics.get('collection_metadata', {}).get('metrics_collected', 0),
                    queries_successful=raw_metrics.get('collection_metadata', {}).get('queries_successful', 0)
                )
                
            except Exception as e:
                self.logger.error(f"Raw metrics collection failed for {cluster_name}: {e}")
                cluster_data["raw_metrics"] = {"error": str(e)}
            
            # Collect detailed cost data for this cluster
            try:
                detailed_costs = await self.cost_client.get_detailed_resource_costs(
                    resource_group, self.cost_analysis_days
                )
                cluster_data["detailed_costs"] = detailed_costs if detailed_costs else {}
                
            except Exception as e:
                self.logger.error(f"Detailed cost collection failed for {cluster_name}: {e}")
                cluster_data["detailed_costs"] = {"error": str(e)}
            
            # Collect Kubernetes resources data
            if self.k8s_client:
                try:
                    kubeconfig_data = await self.aks_client.get_cluster_credentials(cluster_name, resource_group)
                    
                    if kubeconfig_data:
                        k8s_cluster_client = KubernetesClient(
                            config_dict={"cluster_name": cluster_name},
                            kubeconfig_data=kubeconfig_data
                        )
                        await k8s_cluster_client.connect()
                        
                        k8s_resources = await k8s_cluster_client.discover_all_resources()
                        cluster_data["kubernetes_resources"] = k8s_resources if k8s_resources is not None else {}
                        cluster_data["pod_count"] = len(k8s_resources.get("pods", [])) if isinstance(k8s_resources, dict) else 0
                        
                        await k8s_cluster_client.disconnect()
                    else:
                        cluster_data["kubernetes_resources"] = {"error": "Failed to get cluster credentials"}
                        
                except Exception as e:
                    self.logger.warning(f"Kubernetes resource discovery failed for {cluster_name}: {e}")
                    cluster_data["kubernetes_resources"] = {"error": str(e)}
            
            cluster_data["collection_status"] = "completed"
            
        except Exception as e:
            self.logger.error(f"Error collecting data for cluster {cluster_name}: {e}")
            cluster_data["collection_status"] = "failed"
            cluster_data["error"] = str(e)
        
        return cluster_data
    
    async def _collect_raw_cost_data(self) -> Dict[str, Any]:
        """Collect raw cost data without any processing."""
        default_cost_structure = {
            "total_cost": 0.0,
            "currency": "USD",
            "analysis_period_days": self.cost_analysis_days,
            "collection_status": "failed",
            "error_message": None,
            "raw_data": {}
        }
        
        try:
            self.logger.info(f"Collecting raw cost data for: {self.resource_group}")
            
            rg_costs = await self.cost_client.get_resource_group_costs(
                self.resource_group, self.cost_analysis_days
            )
            
            if rg_costs and isinstance(rg_costs, dict):
                return {
                    "total_cost": rg_costs.get("total_cost", 0.0),
                    "currency": rg_costs.get("currency", "USD"),
                    "analysis_period_days": self.cost_analysis_days,
                    "collection_status": "success",
                    "raw_data": rg_costs
                }
            else:
                return default_cost_structure
                
        except Exception as e:
            self.logger.error(f"Failed to collect raw cost data: {e}")
            default_cost_structure["error_message"] = str(e)
            return default_cost_structure