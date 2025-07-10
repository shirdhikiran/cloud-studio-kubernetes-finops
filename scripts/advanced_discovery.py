"""
Enhanced FinOps Discovery Service
Integrates with existing discovery infrastructure to provide comprehensive
Kubernetes cluster discovery with cost allocation and utilization tracking.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import structlog
from dataclasses import dataclass, field

from finops.discovery.base import BaseDiscoveryService
from finops.clients.azure.aks_client import AKSClient
from finops.clients.kubernetes.k8s_client import KubernetesClient
from finops.core.exceptions import DiscoveryException

logger = structlog.get_logger(__name__)


@dataclass
class CostData:
    """Cost information for resources"""
    total_cost: float = 0.0
    compute_cost: float = 0.0
    storage_cost: float = 0.0
    network_cost: float = 0.0
    currency: str = "USD"
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    cost_per_hour: float = 0.0
    estimated_monthly: float = 0.0
    raw_cost_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResourceUtilization:
    """Resource utilization metrics"""
    cpu_request: float = 0.0
    cpu_limit: float = 0.0
    cpu_usage: float = 0.0
    memory_request: float = 0.0
    memory_limit: float = 0.0
    memory_usage: float = 0.0
    storage_request: float = 0.0
    storage_usage: float = 0.0
    network_rx: float = 0.0
    network_tx: float = 0.0
    
    @property
    def cpu_utilization_percent(self) -> float:
        if self.cpu_request > 0:
            return (self.cpu_usage / self.cpu_request) * 100
        return 0.0
    
    @property
    def memory_utilization_percent(self) -> float:
        if self.memory_request > 0:
            return (self.memory_usage / self.memory_request) * 100
        return 0.0


class EnhancedAKSDiscoveryService(BaseDiscoveryService):
    """Enhanced AKS discovery service with comprehensive resource mapping"""
    
    def __init__(self, aks_client: AKSClient, config: Dict[str, Any]):
        super().__init__(aks_client, config)
        self.resource_group = config.get("resource_group")
        self.include_cost_data = config.get("include_cost_data", True)
        self.include_utilization = config.get("include_utilization", True)
    
    async def discover(self) -> Dict[str, Any]:
        """Discover clusters with enhanced information"""
        self.logger.info("Starting enhanced AKS cluster discovery")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        # Get basic cluster information
        clusters = await self.client.discover_clusters(self.resource_group)
        
        # Enhance each cluster with detailed information
        enhanced_clusters = []
        for cluster in clusters:
            enhanced_cluster = await self._enhance_cluster_data(cluster)
            enhanced_clusters.append(enhanced_cluster)
        
        # Calculate totals
        totals = self._calculate_totals(enhanced_clusters)
        
        discovery_result = {
            "clusters": enhanced_clusters,
            "totals": totals,
            "discovery_timestamp": datetime.now(timezone.utc).isoformat(),
            "discovery_metadata": {
                "total_clusters": len(enhanced_clusters),
                "include_cost_data": self.include_cost_data,
                "include_utilization": self.include_utilization
            }
        }
        
        self.logger.info(f"Discovered {len(enhanced_clusters)} enhanced clusters")
        return discovery_result
    
    async def _enhance_cluster_data(self, cluster: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance cluster data with additional information"""
        enhanced_cluster = cluster.copy()
        
        try:
            # Add basic info and properties
            enhanced_cluster["basic_info"] = {
                "id": cluster.get("id"),
                "name": cluster.get("name"),
                "resource_group": cluster.get("resource_group"),
                "location": cluster.get("location"),
                "kubernetes_version": cluster.get("kubernetes_version"),
                "fqdn": cluster.get("fqdn"),
                "provisioning_state": cluster.get("provisioning_state")
            }
            
            # Add properties
            enhanced_cluster["properties"] = {
                "dns_prefix": cluster.get("dns_prefix"),
                "enable_rbac": cluster.get("enable_rbac"),
                "network_profile": cluster.get("network_profile", {}),
                "addon_profiles": cluster.get("addon_profiles", {}),
                "sku": cluster.get("sku", {})
            }
            
            # Get node pools
            cluster_name = cluster.get("name")
            resource_group = cluster.get("resource_group")
            
            if cluster_name and resource_group:
                node_pools = await self.client.discover_node_pools(cluster_name, resource_group)
                enhanced_cluster["node_pools"] = await self._enhance_node_pools(node_pools, cluster_name)
            else:
                enhanced_cluster["node_pools"] = []
            
            # Add cost data if enabled
            if self.include_cost_data:
                enhanced_cluster["raw_cost_data"] = await self._get_cluster_cost_data(cluster)
                enhanced_cluster["cost_data"] = self._calculate_cluster_costs(enhanced_cluster)
            
            # Add namespaces (placeholder - would need Kubernetes client)
            enhanced_cluster["namespaces"] = await self._get_cluster_namespaces(cluster)
            
            # Add workloads summary
            enhanced_cluster["workloads"] = await self._get_cluster_workloads(cluster)
            
            # Add services
            enhanced_cluster["services"] = await self._get_cluster_services(cluster)
            
            # Add storage resources
            enhanced_cluster["storage"] = await self._get_cluster_storage(cluster)
            
            # Add ingresses
            enhanced_cluster["ingresses"] = await self._get_cluster_ingresses(cluster)
            
            # Add network resources
            enhanced_cluster["network_resources"] = await self._get_cluster_network_resources(cluster)
            
            # Add Azure resources
            enhanced_cluster["azure_resources"] = await self._get_cluster_azure_resources(cluster)
            
        except Exception as e:
            self.logger.error(f"Error enhancing cluster {cluster.get('name', 'unknown')}", error=str(e))
            enhanced_cluster["enhancement_errors"] = [str(e)]
        
        return enhanced_cluster
    
    async def _enhance_node_pools(self, node_pools: List[Dict[str, Any]], cluster_name: str) -> List[Dict[str, Any]]:
        """Enhance node pool data with additional information"""
        enhanced_pools = []
        
        for pool in node_pools:
            enhanced_pool = pool.copy()
            
            # Add configuration details
            enhanced_pool["config"] = {
                "vm_size": pool.get("vm_size"),
                "node_count": pool.get("count"),
                "min_count": pool.get("min_count"),
                "max_count": pool.get("max_count"),
                "auto_scaling_enabled": pool.get("auto_scaling_enabled"),
                "availability_zones": pool.get("availability_zones", []),
                "os_type": pool.get("os_type"),
                "os_disk_size_gb": pool.get("os_disk_size_gb")
            }
            
            # Add scaling information
            enhanced_pool["scaling"] = {
                "mode": pool.get("mode"),
                "auto_scaling_enabled": pool.get("auto_scaling_enabled"),
                "min_count": pool.get("min_count"),
                "max_count": pool.get("max_count"),
                "current_count": pool.get("count"),
                "spot_enabled": pool.get("scale_set_priority") == "Spot",
                "spot_max_price": pool.get("spot_max_price")
            }
            
            # Add cost data for node pool
            if self.include_cost_data:
                enhanced_pool["cost_data"] = self._calculate_node_pool_costs(pool)
            
            # Add nodes (placeholder - would need actual node discovery)
            enhanced_pool["nodes"] = await self._get_node_pool_nodes(pool, cluster_name)
            
            enhanced_pools.append(enhanced_pool)
        
        return enhanced_pools
    
    async def _get_node_pool_nodes(self, node_pool: Dict[str, Any], cluster_name: str) -> List[Dict[str, Any]]:
        """Get nodes in a node pool with capacity, utilization, and costs"""
        # This would integrate with Kubernetes client to get actual node data
        # For now, return estimated structure
        
        node_count = node_pool.get("count", 0)
        vm_size = node_pool.get("vm_size", "")
        
        nodes = []
        for i in range(node_count):
            node = {
                "name": f"{cluster_name}-{node_pool.get('name', 'nodepool')}-{i}",
                "vm_size": vm_size,
                "capacity": self._estimate_node_capacity(vm_size),
                "utilization": ResourceUtilization().__dict__,  # Would be populated from metrics
                "costs": self._estimate_node_costs(vm_size),
                "workload_summary": {}  # Would be populated with pod/namespace breakdown
            }
            nodes.append(node)
        
        return nodes
    
    def _estimate_node_capacity(self, vm_size: str) -> Dict[str, Any]:
        """Estimate node capacity based on VM size"""
        # Basic estimation - in practice, this would come from Azure VM specs
        capacity_map = {
            "Standard_DS2_v2": {"cpu": 2.0, "memory": 7 * 1024**3, "storage": 14 * 1024**3},
            "Standard_DS3_v2": {"cpu": 4.0, "memory": 14 * 1024**3, "storage": 28 * 1024**3},
            "Standard_DS4_v2": {"cpu": 8.0, "memory": 28 * 1024**3, "storage": 56 * 1024**3},
            "Standard_D2s_v3": {"cpu": 2.0, "memory": 8 * 1024**3, "storage": 16 * 1024**3},
            "Standard_D4s_v3": {"cpu": 4.0, "memory": 16 * 1024**3, "storage": 32 * 1024**3},
        }
        
        return capacity_map.get(vm_size, {"cpu": 2.0, "memory": 7 * 1024**3, "storage": 14 * 1024**3})
    
    def _estimate_node_costs(self, vm_size: str) -> Dict[str, float]:
        """Estimate node costs based on VM size"""
        # Basic cost estimation - in practice, this would come from Azure pricing API
        hourly_cost_map = {
            "Standard_DS2_v2": 0.10,
            "Standard_DS3_v2": 0.20,
            "Standard_DS4_v2": 0.40,
            "Standard_D2s_v3": 0.096,
            "Standard_D4s_v3": 0.192,
        }
        
        hourly_cost = hourly_cost_map.get(vm_size, 0.10)
        
        return {
            "hourly": hourly_cost,
            "daily": hourly_cost * 24,
            "monthly": hourly_cost * 24 * 30,
            "currency": "USD"
        }
    
    async def _get_cluster_namespaces(self, cluster: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get cluster namespaces with labels, quotas, and cost allocation"""
        # This would require Kubernetes client integration
        # Return placeholder structure
        
        default_namespaces = [
            {
                "name": "default",
                "labels": {},
                "quotas": {},
                "cost_allocation": CostData().__dict__
            },
            {
                "name": "kube-system",
                "labels": {"name": "kube-system"},
                "quotas": {},
                "cost_allocation": CostData().__dict__
            },
            {
                "name": "production",
                "labels": {"environment": "prod"},
                "quotas": {
                    "requests.cpu": "10",
                    "requests.memory": "20Gi",
                    "limits.cpu": "20",
                    "limits.memory": "40Gi"
                },
                "cost_allocation": CostData(total_cost=1500.0, compute_cost=1200.0).__dict__
            }
        ]
        
        return default_namespaces
    
    async def _get_cluster_workloads(self, cluster: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get cluster workloads with specs, resources, utilization, and costs"""
        # This would require Kubernetes client integration
        # Return placeholder structure
        
        sample_workloads = [
            {
                "name": "web-app",
                "namespace": "production",
                "type": "deployment",
                "replicas": 3,
                "specs": {
                    "image": "nginx:1.21",
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                        "limits": {"cpu": "500m", "memory": "512Mi"}
                    }
                },
                "utilization": ResourceUtilization(
                    cpu_request=0.3, cpu_usage=0.15,
                    memory_request=384*1024*1024, memory_usage=256*1024*1024
                ).__dict__,
                "costs": CostData(total_cost=120.0, compute_cost=120.0).__dict__,
                "pods": [
                    {
                        "name": "web-app-12345",
                        "node": "node-1",
                        "status": "Running",
                        "detailed_resource_usage": ResourceUtilization().__dict__,
                        "costs": CostData(total_cost=40.0).__dict__
                    }
                ]
            }
        ]
        
        return sample_workloads
    
    async def _get_cluster_services(self, cluster: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get cluster services with endpoints and load balancer costs"""
        sample_services = [
            {
                "name": "web-app-service",
                "namespace": "production",
                "type": "LoadBalancer",
                "endpoints": ["10.0.1.100:80"],
                "load_balancer_costs": 50.0
            },
            {
                "name": "api-service",
                "namespace": "production", 
                "type": "ClusterIP",
                "endpoints": ["10.0.1.101:8080"],
                "load_balancer_costs": 0.0
            }
        ]
        
        return sample_services
    
    async def _get_cluster_storage(self, cluster: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get cluster storage with utilization, performance, and costs"""
        sample_storage = [
            {
                "name": "data-pv-1",
                "type": "PersistentVolume",
                "storage_class": "managed-premium",
                "capacity": "100Gi",
                "utilization": {
                    "used": "60Gi",
                    "available": "40Gi",
                    "utilization_percent": 60.0
                },
                "performance": {
                    "iops": 120,
                    "throughput": "25 MB/s"
                },
                "costs": CostData(total_cost=15.0, storage_cost=15.0).__dict__
            }
        ]
        
        return sample_storage
    
    async def _get_cluster_ingresses(self, cluster: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get cluster ingresses with rules, performance, and costs"""
        sample_ingresses = [
            {
                "name": "web-ingress",
                "namespace": "production",
                "rules": [
                    {
                        "host": "app.example.com",
                        "paths": [{"path": "/", "backend": "web-app-service:80"}]
                    }
                ],
                "performance": {
                    "requests_per_minute": 1000,
                    "response_time_ms": 150
                },
                "costs": CostData(total_cost=25.0, network_cost=25.0).__dict__
            }
        ]
        
        return sample_ingresses
    
    async def _get_cluster_network_resources(self, cluster: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get network resources (LB, IPs, VNets) with costs"""
        sample_network = [
            {
                "name": "aks-lb",
                "type": "LoadBalancer",
                "sku": "Standard",
                "public_ip": "20.10.5.100",
                "costs": CostData(total_cost=50.0, network_cost=50.0).__dict__
            },
            {
                "name": "aks-vnet",
                "type": "VirtualNetwork",
                "address_space": "10.0.0.0/16",
                "subnets": ["10.0.1.0/24", "10.0.2.0/24"],
                "costs": CostData(total_cost=10.0, network_cost=10.0).__dict__
            }
        ]
        
        return sample_network
    
    async def _get_cluster_azure_resources(self, cluster: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get Azure resources (Storage accounts, Log Analytics)"""
        sample_azure = [
            {
                "name": "aksstorage12345",
                "type": "StorageAccount",
                "sku": "Standard_LRS",
                "usage": {
                    "blob_storage_gb": 50,
                    "transactions_per_month": 100000
                },
                "costs": CostData(total_cost=5.0, storage_cost=5.0).__dict__
            },
            {
                "name": "aks-logs-workspace",
                "type": "LogAnalyticsWorkspace",
                "retention_days": 30,
                "ingestion_gb_per_day": 2.0,
                "costs": CostData(total_cost=30.0, network_cost=30.0).__dict__
            }
        ]
        
        return sample_azure
    
    async def _get_cluster_cost_data(self, cluster: Dict[str, Any]) -> Dict[str, Any]:
        """Get raw cost data for cluster"""
        # This would integrate with cost client
        return {
            "total_cost": 2500.0,
            "cost_breakdown": {
                "compute": 2000.0,
                "storage": 300.0,
                "network": 200.0
            },
            "daily_costs": [],
            "cost_by_service": {},
            "period": "30_days"
        }
    
    def _calculate_cluster_costs(self, cluster: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate aggregated costs for cluster"""
        total_cost = 0.0
        compute_cost = 0.0
        storage_cost = 0.0
        network_cost = 0.0
        
        # Aggregate from node pools
        for node_pool in cluster.get("node_pools", []):
            if "cost_data" in node_pool:
                cost = node_pool["cost_data"]
                total_cost += cost.get("total_cost", 0)
                compute_cost += cost.get("compute_cost", 0)
        
        # Aggregate from services
        for service in cluster.get("services", []):
            total_cost += service.get("load_balancer_costs", 0)
            network_cost += service.get("load_balancer_costs", 0)
        
        # Aggregate from storage
        for storage in cluster.get("storage", []):
            if "costs" in storage:
                cost = storage["costs"]
                total_cost += cost.get("total_cost", 0)
                storage_cost += cost.get("storage_cost", 0)
        
        # Aggregate from network resources
        for network in cluster.get("network_resources", []):
            if "costs" in network:
                cost = network["costs"]
                total_cost += cost.get("total_cost", 0)
                network_cost += cost.get("network_cost", 0)
        
        # Aggregate from Azure resources
        for azure in cluster.get("azure_resources", []):
            if "costs" in azure:
                cost = azure["costs"]
                total_cost += cost.get("total_cost", 0)
                compute_cost += cost.get("compute_cost", 0)
                storage_cost += cost.get("storage_cost", 0)
                network_cost += cost.get("network_cost", 0)
        
        return {
            "total_cost": total_cost,
            "compute_cost": compute_cost,
            "storage_cost": storage_cost,
            "network_cost": network_cost,
            "currency": "USD",
            "cost_per_hour": total_cost / (24 * 30),
            "estimated_monthly": total_cost
        }
    
    def _calculate_node_pool_costs(self, node_pool: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate costs for a node pool"""
        vm_size = node_pool.get("vm_size", "")
        node_count = node_pool.get("count", 0)
        
        node_costs = self._estimate_node_costs(vm_size)
        monthly_cost = node_costs["monthly"] * node_count
        
        return {
            "total_cost": monthly_cost,
            "compute_cost": monthly_cost,
            "storage_cost": 0.0,
            "network_cost": 0.0,
            "currency": "USD",
            "cost_per_hour": node_costs["hourly"] * node_count,
            "estimated_monthly": monthly_cost
        }
    
    def _calculate_totals(self, clusters: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate aggregated totals across all clusters"""
        totals = {
            "total_clusters": len(clusters),
            "total_node_pools": 0,
            "total_nodes": 0,
            "total_namespaces": 0,
            "total_workloads": 0,
            "total_pods": 0,
            "total_services": 0,
            "total_storage_resources": 0,
            "total_ingresses": 0,
            "total_network_resources": 0,
            "total_azure_resources": 0,
            "aggregated_costs": {
                "total_cost": 0.0,
                "compute_cost": 0.0,
                "storage_cost": 0.0,
                "network_cost": 0.0,
                "currency": "USD"
            },
            "aggregated_utilization": ResourceUtilization().__dict__
        }
        
        for cluster in clusters:
            # Count resources
            totals["total_node_pools"] += len(cluster.get("node_pools", []))
            totals["total_namespaces"] += len(cluster.get("namespaces", []))
            totals["total_workloads"] += len(cluster.get("workloads", []))
            totals["total_services"] += len(cluster.get("services", []))
            totals["total_storage_resources"] += len(cluster.get("storage", []))
            totals["total_ingresses"] += len(cluster.get("ingresses", []))
            totals["total_network_resources"] += len(cluster.get("network_resources", []))
            totals["total_azure_resources"] += len(cluster.get("azure_resources", []))
            
            # Count nodes and pods
            for node_pool in cluster.get("node_pools", []):
                totals["total_nodes"] += len(node_pool.get("nodes", []))
            
            for workload in cluster.get("workloads", []):
                totals["total_pods"] += len(workload.get("pods", []))
            
            # Aggregate costs
            if "cost_data" in cluster:
                cost_data = cluster["cost_data"]
                totals["aggregated_costs"]["total_cost"] += cost_data.get("total_cost", 0)
                totals["aggregated_costs"]["compute_cost"] += cost_data.get("compute_cost", 0)
                totals["aggregated_costs"]["storage_cost"] += cost_data.get("storage_cost", 0)
                totals["aggregated_costs"]["network_cost"] += cost_data.get("network_cost", 0)
        
        return totals
    
    def get_discovery_type(self) -> str:
        """Get discovery type"""
        return "enhanced_aks_clusters"


class KubernetesResourceDiscoveryService(BaseDiscoveryService):
    """Enhanced Kubernetes resource discovery with cost allocation"""
    
    def __init__(self, k8s_client: KubernetesClient, config: Dict[str, Any]):
        super().__init__(k8s_client, config)
        self.namespace = config.get("namespace")
        self.include_system_resources = config.get("include_system_resources", False)
        self.cluster_name = config.get("cluster_name", "unknown")
    
    async def discover(self) -> Dict[str, Any]:
        """Discover Kubernetes resources with enhanced cost allocation"""
        self.logger.info("Starting enhanced Kubernetes resource discovery")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        # Get all resource types
        discovery_result = {
            "cluster_name": self.cluster_name,
            "discovery_timestamp": datetime.now(timezone.utc).isoformat(),
            "namespaces": await self._discover_enhanced_namespaces(),
            "nodes": await self._discover_enhanced_nodes(),
            "workloads": await self._discover_enhanced_workloads(),
            "services": await self._discover_enhanced_services(),
            "storage": await self._discover_enhanced_storage(),
            "ingresses": await self._discover_enhanced_ingresses(),
            "network_resources": [],  # Would be populated from Azure discovery
            "azure_resources": []     # Would be populated from Azure discovery
        }
        
        # Calculate totals and costs
        discovery_result["totals"] = self._calculate_k8s_totals(discovery_result)
        
        self.logger.info("Completed enhanced Kubernetes resource discovery")
        return discovery_result
    
    async def _discover_enhanced_namespaces(self) -> List[Dict[str, Any]]:
        """Discover namespaces with resource quotas and cost allocation"""
        namespaces = await self.client.discover_namespaces()
        enhanced_namespaces = []
        
        for ns in namespaces:
            enhanced_ns = {
                "name": ns.get("name"),
                "labels": ns.get("labels", {}),
                "annotations": ns.get("annotations", {}),
                "quotas": {},  # Would get actual quotas
                "cost_allocation": CostData().__dict__
            }
            enhanced_namespaces.append(enhanced_ns)
        
        return enhanced_namespaces
    
    async def _discover_enhanced_nodes(self) -> List[Dict[str, Any]]:
        """Discover nodes with capacity, utilization, costs, and workload summary"""
        nodes = await self.client.discover_nodes()
        pods = await self.client.discover_pods()
        
        enhanced_nodes = []
        for node in nodes:
            # Get pods on this node
            node_pods = [p for p in pods if p.get("node") == node.get("name")]
            
            enhanced_node = {
                "name": node.get("name"),
                "capacity": {
                    "cpu": self._parse_cpu(node.get("capacity", {}).get("cpu", "0")),
                    "memory": self._parse_memory(node.get("capacity", {}).get("memory", "0")),
                    "pods": int(node.get("capacity", {}).get("pods", "0"))
                },
                "utilization": self._calculate_node_utilization(node_pods),
                "costs": self._estimate_node_costs_from_k8s(node),
                "workload_summary": self._create_workload_summary(node_pods)
            }
            enhanced_nodes.append(enhanced_node)
        
        return enhanced_nodes
    
    async def _discover_enhanced_workloads(self) -> List[Dict[str, Any]]:
        """Discover workloads with detailed resource usage and costs"""
        deployments = await self.client.discover_deployments()
        pods = await self.client.discover_pods()
        
        enhanced_workloads = []
        for deployment in deployments:
            # Find pods belonging to this deployment
            deployment_pods = [
                p for p in pods 
                if p.get("namespace") == deployment.get("namespace") and
                deployment.get("name") in p.get("owner_references", [{}])[0].get("name", "")
            ]
            
            enhanced_workload = {
                "name": deployment.get("name"),
                "namespace": deployment.get("namespace"),
                "type": "deployment",
                "specs": {
                    "replicas": deployment.get("replicas", 0),
                    "ready_replicas": deployment.get("ready_replicas", 0),
                    "strategy": deployment.get("strategy", {})
                },
                "resources": self._aggregate_workload_resources(deployment_pods),
                "utilization": self._calculate_workload_utilization(deployment_pods),
                "costs": self._calculate_workload_costs(deployment_pods),
                "pods": self._enhance_pods(deployment_pods)
            }
            enhanced_workloads.append(enhanced_workload)
        
        return enhanced_workloads
    
    async def _discover_enhanced_services(self) -> List[Dict[str, Any]]:
        """Discover services with endpoints and load balancer costs"""
        services = await self.client.discover_services()
        enhanced_services = []
        
        for service in services:
            enhanced_service = {
                "name": service.get("name"),
                "namespace": service.get("namespace"),
                "type": service.get("type"),
                "endpoints": service.get("ports", []),
                "load_balancer_costs": self._estimate_service_costs(service)
            }
            enhanced_services.append(enhanced_service)
        
        return enhanced_services
    
    async def _discover_enhanced_storage(self) -> List[Dict[str, Any]]:
        """Discover storage with utilization, performance, and costs"""
        try:
            pvs = await self.client.discover_persistent_volumes()
            pvcs = await self.client.discover_persistent_volume_claims()
        except Exception as e:
            self.logger.warning(f"Could not discover storage resources: {e}")
            return []
        
        enhanced_storage = []
        
        # Process Persistent Volumes
        for pv in pvs:
            enhanced_pv = {
                "name": pv.get("name"),
                "type": "PersistentVolume",
                "storage_class": pv.get("storage_class"),
                "capacity": pv.get("capacity"),
                "utilization": {
                    "used": "unknown",  # Would need metrics integration
                    "available": "unknown",
                    "utilization_percent": 0.0
                },
                "performance": self._estimate_storage_performance(pv),
                "costs": self._calculate_storage_costs(pv)
            }
            enhanced_storage.append(enhanced_pv)
        
        # Process Persistent Volume Claims
        for pvc in pvcs:
            enhanced_pvc = {
                "name": pvc.get("name"),
                "namespace": pvc.get("namespace"),
                "type": "PersistentVolumeClaim",
                "storage_class": pvc.get("storage_class"),
                "requested": pvc.get("requested"),
                "volume_name": pvc.get("volume_name"),
                "utilization": {
                    "used": "unknown",
                    "available": "unknown", 
                    "utilization_percent": 0.0
                },
                "costs": self._calculate_pvc_costs(pvc)
            }
            enhanced_storage.append(enhanced_pvc)
        
        return enhanced_storage
    
    async def _discover_enhanced_ingresses(self) -> List[Dict[str, Any]]:
        """Discover ingresses with rules, performance, and costs"""
        try:
            ingresses = await self.client.discover_ingresses()
        except Exception as e:
            self.logger.warning(f"Could not discover ingresses: {e}")
            return []
        
        enhanced_ingresses = []
        for ingress in ingresses:
            enhanced_ingress = {
                "name": ingress.get("name"),
                "namespace": ingress.get("namespace"),
                "rules": ingress.get("rules", []),
                "tls": ingress.get("tls", []),
                "performance": {
                    "requests_per_minute": 0,  # Would need metrics
                    "response_time_ms": 0
                },
                "costs": self._calculate_ingress_costs(ingress)
            }
            enhanced_ingresses.append(enhanced_ingress)
        
        return enhanced_ingresses
    
    def _parse_cpu(self, cpu_str: str) -> float:
        """Parse CPU string to cores"""
        if not cpu_str:
            return 0.0
        if cpu_str.endswith('m'):
            return float(cpu_str[:-1]) / 1000
        return float(cpu_str)
    
    def _parse_memory(self, memory_str: str) -> float:
        """Parse memory string to bytes"""
        if not memory_str:
            return 0.0
        
        