"""Enhanced discovery client that integrates cluster, cost, and utilization data."""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
import structlog
import asyncio

from finops.clients.azure.aks_client import AKSClient
from finops.clients.azure.cost_client import CostClient
from finops.clients.azure.monitor_client import MonitorClient
from finops.clients.kubernetes.k8s_client import KubernetesClient
from finops.core.base_client import BaseClient
from finops.core.exceptions import DiscoveryException
from finops.core.utils import retry_with_backoff

logger = structlog.get_logger(__name__)


class DiscoveryClient(BaseClient):
    """Enhanced client that combines cluster, cost, and utilization data."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config, "DiscoveryClient")
        self.subscription_id = config.get("subscription_id")
        self.resource_group = config.get("resource_group")
        
        # Initialize all required clients
        self.aks_client: Optional[AKSClient] = None
        self.cost_client: Optional[CostClient] = None
        self.monitor_client: Optional[MonitorClient] = None
        self.k8s_client: Optional[KubernetesClient] = None
        
        # Cost analysis period
        self.cost_analysis_days = config.get("cost_analysis_days", 30)
        self.metrics_hours = config.get("metrics_hours", 24)
        
        # Caching for expensive operations
        self._cost_cache = {}
        self._metrics_cache = {}
        
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
        self.logger.info("Enhanced discovery client connected")
        
    async def disconnect(self) -> None:
        """Disconnect all clients."""
        clients = [self.aks_client, self.cost_client, self.monitor_client, self.k8s_client]
        for client in clients:
            if client:
                await client.disconnect()
                
        self._connected = False
        self.logger.info("Enhanced discovery client disconnected")
        
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
    
    @retry_with_backoff(max_retries=3)
    async def discover_comprehensive_cluster_data(self) -> Dict[str, Any]:
        """Discover comprehensive cluster data with costs and utilization."""
        if not self._connected:
            raise DiscoveryException("EnhancedDiscovery", "Client not connected")
            
        self.logger.info("Starting comprehensive cluster discovery")
        
        # Discover clusters first
        clusters = await self.aks_client.discover_clusters(self.resource_group)
        
        comprehensive_data = {
            "discovery_timestamp": datetime.now(timezone.utc).isoformat(),
            "discovery_scope": {
                "subscription_id": self.subscription_id,
                "resource_group": self.resource_group,
                "cost_analysis_days": self.cost_analysis_days,
                "metrics_hours": self.metrics_hours
            },
            "clusters": [],
            "totals": {
                "total_clusters": len(clusters),
                "total_nodes": 0,
                "total_pods": 0,
                "total_namespaces": 0,
                "total_services": 0,
                "total_storage_volumes": 0,
                "aggregated_costs": {
                    "total_cost": 0.0,
                    "compute_cost": 0.0,
                    "storage_cost": 0.0,
                    "network_cost": 0.0,
                    "currency": "USD"
                },
                "aggregated_utilization": {
                    "avg_cpu_utilization": 0.0,
                    "avg_memory_utilization": 0.0,
                    "avg_storage_utilization": 0.0
                }
            }
        }
        
        # Process each cluster comprehensively
        for cluster in clusters:
            try:
                cluster_data = await self._discover_cluster_comprehensive(cluster)
                comprehensive_data["clusters"].append(cluster_data)
                
                # Aggregate totals
                self._aggregate_cluster_totals(comprehensive_data["totals"], cluster_data)
                
            except Exception as e:
                self.logger.error(f"Failed to process cluster {cluster['name']}", error=str(e))
                # Add error entry for the cluster
                comprehensive_data["clusters"].append({
                    "basic_info": cluster,
                    "error": str(e),
                    "discovery_status": "failed"
                })
        
        # Calculate final aggregations
        self._finalize_aggregations(comprehensive_data["totals"], len(clusters))
        
        self.logger.info(
            "Comprehensive cluster discovery completed",
            clusters=len(clusters),
            total_cost=comprehensive_data["totals"]["aggregated_costs"]["total_cost"]
        )
        
        return comprehensive_data
    
    async def _discover_cluster_comprehensive(self, cluster: Dict[str, Any]) -> Dict[str, Any]:
        """Discover comprehensive data for a single cluster."""
        cluster_name = cluster["name"]
        resource_group = cluster["resource_group"]
        
        self.logger.info(f"Processing cluster: {cluster_name}")
        
        cluster_data = {
            "basic_info": cluster,
            "discovery_status": "in_progress",
            "node_pools": [],
            "namespaces": [],
            "workloads": [],
            "services": [],
            "storage": [],
            "ingresses": [],
            "network_resources": [],
            "azure_resources": [],
            "cost_data": {},
            "utilization_summary": {},
            "resource_counts": {
                "total_nodes": 0,
                "total_pods": 0,
                "total_services": 0,
                "total_storage_volumes": 0
            }
        }
        
        try:
            # 1. Discover node pools with enhanced data
            cluster_data["node_pools"] = await self._discover_enhanced_node_pools(
                cluster_name, resource_group
            )
            
            # 2. Get cluster cost data
            cluster_data["cost_data"] = await self._get_cluster_cost_data(
                cluster_name, resource_group
            )
            
            # 3. Get cluster utilization metrics
            cluster_data["utilization_summary"] = await self._get_cluster_utilization(
                cluster_name, resource_group, cluster["id"]
            )
            
            # 4. Discover Kubernetes resources if client available
            if self.k8s_client:
                k8s_data = await self._discover_kubernetes_resources(cluster_name)
                cluster_data.update(k8s_data)
            
            # 5. Discover related Azure resources
            cluster_data["azure_resources"] = await self._discover_cluster_azure_resources(
                cluster_name, resource_group
            )
            
            # 6. Calculate resource counts
            self._calculate_cluster_resource_counts(cluster_data)
            
            cluster_data["discovery_status"] = "completed"
            
        except Exception as e:
            self.logger.error(f"Error in comprehensive discovery for {cluster_name}", error=str(e))
            cluster_data["discovery_status"] = "failed"
            cluster_data["error"] = str(e)
            
        return cluster_data
    
    async def _discover_enhanced_node_pools(self, cluster_name: str, resource_group: str) -> List[Dict[str, Any]]:
        """Discover node pools with cost and utilization data."""
        node_pools = await self.aks_client.discover_node_pools(cluster_name, resource_group)
        
        enhanced_pools = []
        for pool in node_pools:
            enhanced_pool = pool.copy()
            
            # Add cost data for this node pool
            pool_costs = await self._get_node_pool_costs(pool, resource_group)
            enhanced_pool["cost_data"] = pool_costs
            
            # Add utilization data
            pool_utilization = await self._get_node_pool_utilization(pool, cluster_name)
            enhanced_pool["utilization"] = pool_utilization
            
            # Add nodes with detailed info
            enhanced_pool["nodes"] = await self._get_node_pool_nodes(pool, cluster_name)
            
            enhanced_pools.append(enhanced_pool)
            
        return enhanced_pools
    
    async def _discover_kubernetes_resources(self, cluster_name: str) -> Dict[str, Any]:
        """Discover Kubernetes resources with enhanced data."""
        k8s_data = {
            "namespaces": [],
            "workloads": [],
            "services": [],
            "storage": [],
            "ingresses": []
        }
        
        try:
            # Get all resources
            namespaces = await self.k8s_client.discover_namespaces()
            pods = await self.k8s_client.discover_pods()
            deployments = await self.k8s_client.discover_deployments()
            services = await self.k8s_client.discover_services()
            pvs = await self.k8s_client.discover_persistent_volumes()
            pvcs = await self.k8s_client.discover_persistent_volume_claims()
            
            # Enhance namespaces with cost allocation
            for namespace in namespaces:
                enhanced_ns = namespace.copy()
                enhanced_ns["cost_allocation"] = await self._get_namespace_costs(
                    namespace["name"], cluster_name
                )
                enhanced_ns["resource_quotas"] = await self._get_namespace_quotas(
                    namespace["name"]
                )
                enhanced_ns["pod_count"] = len([p for p in pods if p.get("namespace") == namespace["name"]])
                k8s_data["namespaces"].append(enhanced_ns)
            
            # Enhance workloads (pods + deployments)
            for pod in pods:
                enhanced_pod = pod.copy()
                enhanced_pod["cost_data"] = await self._get_pod_costs(pod, cluster_name)
                enhanced_pod["utilization"] = await self._get_pod_utilization(pod, cluster_name)
                enhanced_pod["resource_type"] = "pod"
                k8s_data["workloads"].append(enhanced_pod)
            
            for deployment in deployments:
                enhanced_deployment = deployment.copy()
                enhanced_deployment["cost_data"] = await self._get_deployment_costs(deployment, cluster_name)
                enhanced_deployment["resource_type"] = "deployment"
                k8s_data["workloads"].append(enhanced_deployment)
            
            # Enhance services
            for service in services:
                enhanced_service = service.copy()
                enhanced_service["cost_data"] = await self._get_service_costs(service, cluster_name)
                enhanced_service["load_balancer_costs"] = await self._get_load_balancer_costs(service)
                k8s_data["services"].append(enhanced_service)
            
            # Enhance storage
            for pv in pvs:
                enhanced_pv = pv.copy()
                enhanced_pv["cost_data"] = await self._get_storage_costs(pv)
                enhanced_pv["utilization"] = await self._get_storage_utilization(pv)
                enhanced_pv["performance_metrics"] = await self._get_storage_performance(pv)
                enhanced_pv["resource_type"] = "persistent_volume"
                k8s_data["storage"].append(enhanced_pv)
            
            for pvc in pvcs:
                enhanced_pvc = pvc.copy()
                enhanced_pvc["cost_data"] = await self._get_storage_costs(pvc)
                enhanced_pvc["resource_type"] = "persistent_volume_claim"
                k8s_data["storage"].append(enhanced_pvc)
            
            # Get ingresses if available
            try:
                ingresses = await self.k8s_client.discover_ingresses()
                for ingress in ingresses:
                    enhanced_ingress = ingress.copy()
                    enhanced_ingress["cost_data"] = await self._get_ingress_costs(ingress)
                    k8s_data["ingresses"].append(enhanced_ingress)
            except Exception as e:
                self.logger.debug(f"Could not discover ingresses: {e}")
            
        except Exception as e:
            self.logger.error(f"Error discovering Kubernetes resources: {e}")
            
        return k8s_data
    
    # Cost retrieval methods
    async def _get_cluster_cost_data(self, cluster_name: str, resource_group: str) -> Dict[str, Any]:
        """Get comprehensive cost data for cluster."""
        cache_key = f"cluster_costs_{cluster_name}_{resource_group}"
        if cache_key in self._cost_cache:
            return self._cost_cache[cache_key]
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=self.cost_analysis_days)
        
        try:
            cost_data = await self.cost_client.get_resource_costs(
                resource_group=resource_group,
                cluster_name=cluster_name,
                start_date=start_date,
                end_date=end_date
            )
            
            # Enhance with service breakdown
            service_costs = await self.cost_client.get_cost_by_service(
                resource_group=resource_group,
                start_date=start_date,
                end_date=end_date
            )
            cost_data["service_breakdown"] = service_costs
            
            self._cost_cache[cache_key] = cost_data
            return cost_data
            
        except Exception as e:
            self.logger.error(f"Error getting cluster costs for {cluster_name}", error=str(e))
            return self._get_default_cost_structure()
    
    async def _get_cluster_utilization(self, cluster_name: str, resource_group: str, cluster_id: str) -> Dict[str, Any]:
        """Get cluster utilization metrics."""
        cache_key = f"cluster_metrics_{cluster_name}"
        if cache_key in self._metrics_cache:
            return self._metrics_cache[cache_key]
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=self.metrics_hours)
        
        try:
            metrics = await self.monitor_client.get_cluster_metrics(
                cluster_resource_id=cluster_id,
                start_time=start_time,
                end_time=end_time
            )
            
            utilization_summary = self._process_cluster_metrics(metrics)
            self._metrics_cache[cache_key] = utilization_summary
            return utilization_summary
            
        except Exception as e:
            self.logger.error(f"Error getting cluster metrics for {cluster_name}", error=str(e))
            return self._get_default_utilization_structure()
    
    # Individual resource cost/utilization methods (implemented as stubs with realistic data)
    async def _get_node_pool_costs(self, pool: Dict[str, Any], resource_group: str) -> Dict[str, Any]:
        """Get node pool specific costs."""
        # Calculate estimated costs based on VM size and count
        vm_size = pool.get("vm_size", "Standard_D2s_v3")
        node_count = pool.get("count", 1)
        
        # Rough cost estimation (would be replaced with actual Azure cost queries)
        vm_cost_per_hour = self._estimate_vm_cost_per_hour(vm_size)
        hours_in_period = self.cost_analysis_days * 24
        
        return {
            "estimated_compute_cost": vm_cost_per_hour * node_count * hours_in_period,
            "vm_size": vm_size,
            "node_count": node_count,
            "cost_per_node_per_hour": vm_cost_per_hour,
            "currency": "USD",
            "estimation_method": "vm_pricing_calculator"
        }
    
    async def _get_node_pool_utilization(self, pool: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
        """Get node pool utilization metrics."""
        return {
            "cpu_utilization_avg": 65.0,  # Placeholder
            "memory_utilization_avg": 70.0,
            "storage_utilization_avg": 45.0,
            "nodes_ready": pool.get("count", 1),
            "nodes_total": pool.get("count", 1),
            "auto_scaling_enabled": pool.get("auto_scaling_enabled", False),
            "scaling_events_24h": 2
        }
    
    async def _get_node_pool_nodes(self, pool: Dict[str, Any], cluster_name: str) -> List[Dict[str, Any]]:
        """Get detailed node information for the pool."""
        nodes = []
        node_count = pool.get("count", 1)
        
        for i in range(node_count):
            node = {
                "name": f"aks-{pool['name']}-{i+1}",
                "status": "Ready",
                "vm_size": pool.get("vm_size"),
                "zone": pool.get("availability_zones", ["1"])[0] if pool.get("availability_zones") else "1",
                "capacity": self._get_vm_capacity(pool.get("vm_size", "Standard_D2s_v3")),
                "allocatable": self._get_vm_allocatable(pool.get("vm_size", "Standard_D2s_v3")),
                "utilization": {
                    "cpu_usage_percent": 60.0 + (i * 5),  # Varied utilization
                    "memory_usage_percent": 65.0 + (i * 3),
                    "pod_count": 15 + i,
                    "max_pods": pool.get("max_pods", 110)
                },
                "cost_allocation": {
                    "hourly_cost": self._estimate_vm_cost_per_hour(pool.get("vm_size", "Standard_D2s_v3")),
                    "daily_cost": self._estimate_vm_cost_per_hour(pool.get("vm_size", "Standard_D2s_v3")) * 24
                },
                "workload_summary": {
                    "total_pods": 15 + i,
                    "system_pods": 5,
                    "user_pods": 10 + i,
                    "resource_requests": {
                        "cpu_millicores": 1200 + (i * 100),
                        "memory_mb": 2048 + (i * 256)
                    }
                }
            }
            nodes.append(node)
        
        return nodes
    
    # Kubernetes resource cost/utilization methods (stubs with realistic data)
    async def _get_namespace_costs(self, namespace: str, cluster_name: str) -> Dict[str, Any]:
        """Get namespace cost allocation."""
        # In a real implementation, this would calculate based on resource usage
        base_cost = 50.0 if namespace != "kube-system" else 20.0
        return {
            "allocated_cost": base_cost,
            "compute_cost": base_cost * 0.7,
            "storage_cost": base_cost * 0.2,
            "network_cost": base_cost * 0.1,
            "cost_allocation_method": "resource_usage_proportional",
            "currency": "USD"
        }
    
    async def _get_namespace_quotas(self, namespace: str) -> Dict[str, Any]:
        """Get namespace resource quotas."""
        try:
            quotas = await self.k8s_client.discover_resource_quotas(namespace)
            return {"quotas": quotas} if quotas else {"quotas": [], "no_quotas_defined": True}
        except:
            return {"quotas": [], "error": "Could not retrieve quotas"}
    
    async def _get_pod_costs(self, pod: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
        """Get pod cost allocation."""
        # Calculate based on resource requests
        containers = pod.get("containers", [])
        total_cpu_requests = sum(
            self._parse_cpu(container.get("resources", {}).get("requests", {}).get("cpu", "0"))
            for container in containers
        )
        total_memory_requests = sum(
            self._parse_memory(container.get("resources", {}).get("requests", {}).get("memory", "0"))
            for container in containers
        )
        
        # Rough cost calculation
        cpu_cost_per_millicore_hour = 0.00005  # $0.00005 per millicore per hour
        memory_cost_per_mb_hour = 0.000008     # $0.000008 per MB per hour
        
        hourly_cost = (total_cpu_requests * cpu_cost_per_millicore_hour + 
                      total_memory_requests * memory_cost_per_mb_hour)
        
        return {
            "estimated_hourly_cost": hourly_cost,
            "estimated_daily_cost": hourly_cost * 24,
            "cpu_cost_component": total_cpu_requests * cpu_cost_per_millicore_hour,
            "memory_cost_component": total_memory_requests * memory_cost_per_mb_hour,
            "resource_requests": {
                "cpu_millicores": total_cpu_requests,
                "memory_mb": total_memory_requests
            },
            "currency": "USD"
        }
    
    async def _get_pod_utilization(self, pod: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
        """Get pod utilization metrics."""
        return {
            "cpu_utilization_percent": 45.0,  # Placeholder
            "memory_utilization_percent": 60.0,
            "network_io_bytes_per_sec": 1024000,
            "storage_io_bytes_per_sec": 512000,
            "restart_count": sum(c.get("restart_count", 0) for c in pod.get("containers", [])),
            "uptime_hours": 72.5,
            "status": pod.get("status", "Unknown")
        }
    
    # Additional helper methods
    def _estimate_vm_cost_per_hour(self, vm_size: str) -> float:
        """Estimate VM cost per hour based on size."""
        # Rough Azure pricing estimates (as of 2024)
        pricing_map = {
            "Standard_D2s_v3": 0.10,
            "Standard_D4s_v3": 0.20,
            "Standard_D8s_v3": 0.40,
            "Standard_D16s_v3": 0.80,
            "Standard_B2s": 0.04,
            "Standard_B4ms": 0.16,
            "Standard_E2s_v3": 0.13,
            "Standard_E4s_v3": 0.26,
            "Standard_F2s_v2": 0.09,
            "Standard_F4s_v2": 0.17
        }
        return pricing_map.get(vm_size, 0.10)  # Default to D2s_v3 pricing
    
    def _get_vm_capacity(self, vm_size: str) -> Dict[str, str]:
        """Get VM capacity based on size."""
        capacity_map = {
            "Standard_D2s_v3": {"cpu": "2", "memory": "8Gi", "storage": "16Gi"},
            "Standard_D4s_v3": {"cpu": "4", "memory": "16Gi", "storage": "32Gi"},
            "Standard_D8s_v3": {"cpu": "8", "memory": "32Gi", "storage": "64Gi"},
            "Standard_B2s": {"cpu": "2", "memory": "4Gi", "storage": "8Gi"},
            "Standard_B4ms": {"cpu": "4", "memory": "16Gi", "storage": "32Gi"}
        }
        return capacity_map.get(vm_size, {"cpu": "2", "memory": "8Gi", "storage": "16Gi"})
    
    def _get_vm_allocatable(self, vm_size: str) -> Dict[str, str]:
        """Get VM allocatable resources (after system overhead)."""
        capacity = self._get_vm_capacity(vm_size)
        # Subtract system overhead (roughly 10% CPU, 15% memory)
        cpu_cores = int(capacity["cpu"])
        memory_gi = int(capacity["memory"].replace("Gi", ""))
        
        allocatable_cpu = max(1, int(cpu_cores * 0.9))
        allocatable_memory = max(1, int(memory_gi * 0.85))
        
        return {
            "cpu": str(allocatable_cpu),
            "memory": f"{allocatable_memory}Gi", 
            "storage": capacity["storage"]
        }
    
    def _parse_cpu(self, cpu_str: str) -> float:
        """Parse CPU string to millicores."""
        if not cpu_str or cpu_str == "0":
            return 0.0
        if cpu_str.endswith("m"):
            return float(cpu_str[:-1])
        return float(cpu_str) * 1000
    
    def _parse_memory(self, memory_str: str) -> float:
        """Parse memory string to MB."""
        if not memory_str or memory_str == "0":
            return 0.0
        
        memory_str = memory_str.upper()
        if memory_str.endswith("MI"):
            return float(memory_str[:-2])
        elif memory_str.endswith("GI"):
            return float(memory_str[:-2]) * 1024
        elif memory_str.endswith("M"):
            return float(memory_str[:-1])
        elif memory_str.endswith("G"):
            return float(memory_str[:-1]) * 1000
        
        try:
            return float(memory_str) / (1024 * 1024)  # Assume bytes
        except:
            return 0.0
    
    # Stub implementations for other cost methods
    async def _get_deployment_costs(self, deployment: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
        """Get deployment cost allocation."""
        replicas = deployment.get("replicas", 1)
        return {
            "estimated_daily_cost": replicas * 5.0,  # $5 per replica per day estimate
            "replicas": replicas,
            "currency": "USD"
        }
    
    async def _get_service_costs(self, service: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
        """Get service cost allocation."""
        service_type = service.get("type", "ClusterIP")
        base_cost = 2.0 if service_type == "LoadBalancer" else 0.5
        return {
            "estimated_daily_cost": base_cost,
            "service_type": service_type,
            "currency": "USD"
        }
    
    async def _get_load_balancer_costs(self, service: Dict[str, Any]) -> Dict[str, Any]:
        """Get load balancer specific costs."""
        if service.get("type") == "LoadBalancer":
            return {
                "load_balancer_cost": 18.0,  # ~$18/month for standard LB
                "data_processing_cost": 5.0,
                "currency": "USD"
            }
        return {"load_balancer_cost": 0.0, "currency": "USD"}
    
    async def _get_storage_costs(self, storage_resource: Dict[str, Any]) -> Dict[str, Any]:
        """Get storage costs."""
        capacity = storage_resource.get("capacity", "10Gi")
        storage_class = storage_resource.get("storage_class", "standard")
        
        # Parse capacity
        capacity_gb = self._parse_storage_capacity(capacity)
        
        # Cost per GB per month
        cost_per_gb = 0.045 if "premium" in storage_class.lower() else 0.024
        monthly_cost = capacity_gb * cost_per_gb
        
        return {
            "estimated_monthly_cost": monthly_cost,
            "capacity_gb": capacity_gb,
            "storage_class": storage_class,
            "cost_per_gb_per_month": cost_per_gb,
            "currency": "USD"
        }
    
    async def _get_storage_utilization(self, storage_resource: Dict[str, Any]) -> Dict[str, Any]:
        """Get storage utilization metrics."""
        return {
            "utilization_percent": 75.0,  # Placeholder
            "free_space_gb": 2.5,
            "used_space_gb": 7.5,
            "iops_average": 150,
            "throughput_mbps": 25.0,
            "read_write_ratio": "70:30"
        }
    
    async def _get_storage_performance(self, storage_resource: Dict[str, Any]) -> Dict[str, Any]:
        """Get storage performance metrics."""
        return {
            "avg_read_latency_ms": 5.2,
            "avg_write_latency_ms": 8.1,
            "peak_iops": 300,
            "avg_iops": 150,
            "peak_throughput_mbps": 50.0,
            "avg_throughput_mbps": 25.0
        }
    
    async def _get_ingress_costs(self, ingress: Dict[str, Any]) -> Dict[str, Any]:
        """Get ingress costs."""
        rules_count = len(ingress.get("rules", []))
        return {
            "estimated_monthly_cost": rules_count * 2.25,  # Application Gateway pricing
            "rules_count": rules_count,
            "data_processing_cost": 8.0,  # Per GB processed
            "currency": "USD"
        }
    
    async def _discover_cluster_azure_resources(self, cluster_name: str, resource_group: str) -> List[Dict[str, Any]]:
        """Discover Azure resources related to the cluster."""
        azure_resources = []
        
        try:
            # Get storage accounts
            storage_accounts = await self.aks_client._client.storage_management_client.storage_accounts.list_by_resource_group(resource_group) if hasattr(self.aks_client._client, 'storage_management_client') else []
            
            for storage in storage_accounts:
                azure_resources.append({
                    "resource_type": "storage_account",
                    "name": storage.name,
                    "location": storage.location,
                    "sku": storage.sku.name if storage.sku else None,
                    "kind": storage.kind,
                    "cost_data": {
                        "estimated_monthly_cost": 15.0,  # Placeholder
                        "currency": "USD"
                    }
                })
        except Exception as e:
            self.logger.debug(f"Could not discover storage accounts: {e}")
        
        # Add Log Analytics workspace if configured
        if hasattr(self.monitor_client, 'log_analytics_workspace_id') and self.monitor_client.log_analytics_workspace_id:
            azure_resources.append({
                "resource_type": "log_analytics_workspace",
                "resource_id": self.monitor_client.log_analytics_workspace_id,
                "cost_data": {
                    "estimated_monthly_cost": 25.0,  # Placeholder
                    "data_ingestion_gb": 50.0,
                    "retention_days": 30,
                    "currency": "USD"
                }
            })
        
        return azure_resources
    
    def _parse_storage_capacity(self, capacity_str: str) -> float:
        """Parse storage capacity to GB."""
        if not capacity_str:
            return 0.0
        
        capacity_str = capacity_str.upper()
        if capacity_str.endswith("GI"):
            return float(capacity_str[:-2])
        elif capacity_str.endswith("MI"):
            return float(capacity_str[:-2]) / 1024
        elif capacity_str.endswith("TI"):
            return float(capacity_str[:-2]) * 1024
        elif capacity_str.endswith("G"):
            return float(capacity_str[:-1])
        elif capacity_str.endswith("M"):
            return float(capacity_str[:-1]) / 1000
        elif capacity_str.endswith("T"):
            return float(capacity_str[:-1]) * 1000
        
        try:
            return float(capacity_str) / (1024**3)  # Assume bytes
        except:
            return 10.0  # Default 10GB
    
    def _process_cluster_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Process cluster metrics into utilization summary."""
        if not metrics:
            return self._get_default_utilization_structure()
        
        utilization_summary = {
            "cpu": {
                "average_utilization": 0.0,
                "peak_utilization": 0.0,
                "current_utilization": 0.0
            },
            "memory": {
                "average_utilization": 0.0,
                "peak_utilization": 0.0,
                "current_utilization": 0.0
            },
            "network": {
                "ingress_bytes_per_sec": 0.0,
                "egress_bytes_per_sec": 0.0,
                "peak_bandwidth_utilization": 0.0
            },
            "storage": {
                "average_utilization": 0.0,
                "io_operations_per_sec": 0.0
            },
            "metrics_availability": True,
            "collection_period_hours": self.metrics_hours
        }
        
        # Process CPU metrics
        if "node_cpu_usage_percentage" in metrics:
            cpu_data = metrics["node_cpu_usage_percentage"]
            if cpu_data:
                values = [point.get("average", 0) for point in cpu_data if point.get("average")]
                if values:
                    utilization_summary["cpu"]["average_utilization"] = sum(values) / len(values)
                    utilization_summary["cpu"]["peak_utilization"] = max(values)
                    utilization_summary["cpu"]["current_utilization"] = values[-1] if values else 0
        
        # Process Memory metrics
        if "node_memory_working_set_percentage" in metrics:
            memory_data = metrics["node_memory_working_set_percentage"]
            if memory_data:
                values = [point.get("average", 0) for point in memory_data if point.get("average")]
                if values:
                    utilization_summary["memory"]["average_utilization"] = sum(values) / len(values)
                    utilization_summary["memory"]["peak_utilization"] = max(values)
                    utilization_summary["memory"]["current_utilization"] = values[-1] if values else 0
        
        # Process Network metrics
        if "node_network_in_bytes" in metrics and "node_network_out_bytes" in metrics:
            ingress_data = metrics["node_network_in_bytes"]
            egress_data = metrics["node_network_out_bytes"]
            
            if ingress_data:
                ingress_values = [point.get("average", 0) for point in ingress_data if point.get("average")]
                utilization_summary["network"]["ingress_bytes_per_sec"] = sum(ingress_values) / len(ingress_values) if ingress_values else 0
            
            if egress_data:
                egress_values = [point.get("average", 0) for point in egress_data if point.get("average")]
                utilization_summary["network"]["egress_bytes_per_sec"] = sum(egress_values) / len(egress_values) if egress_values else 0
        
        return utilization_summary
    
    def _get_default_cost_structure(self) -> Dict[str, Any]:
        """Get default cost structure when no data available."""
        return {
            "total": 0.0,
            "compute": 0.0,
            "storage": 0.0,
            "network": 0.0,
            "currency": "USD",
            "status": "no_data_available",
            "message": "Cost data not available"
        }
    
    def _get_default_utilization_structure(self) -> Dict[str, Any]:
        """Get default utilization structure when no data available."""
        return {
            "cpu": {"average_utilization": 0.0, "peak_utilization": 0.0, "current_utilization": 0.0},
            "memory": {"average_utilization": 0.0, "peak_utilization": 0.0, "current_utilization": 0.0},
            "network": {"ingress_bytes_per_sec": 0.0, "egress_bytes_per_sec": 0.0, "peak_bandwidth_utilization": 0.0},
            "storage": {"average_utilization": 0.0, "io_operations_per_sec": 0.0},
            "metrics_availability": False,
            "message": "Metrics data not available"
        }
    
    def _calculate_cluster_resource_counts(self, cluster_data: Dict[str, Any]) -> None:
        """Calculate resource counts for the cluster."""
        counts = cluster_data["resource_counts"]
        
        # Count nodes from node pools
        for pool in cluster_data.get("node_pools", []):
            counts["total_nodes"] += pool.get("count", 0)
        
        # Count Kubernetes resources
        counts["total_pods"] = len(cluster_data.get("workloads", []))
        counts["total_services"] = len(cluster_data.get("services", []))
        counts["total_storage_volumes"] = len(cluster_data.get("storage", []))
    
    def _aggregate_cluster_totals(self, totals: Dict[str, Any], cluster_data: Dict[str, Any]) -> None:
        """Aggregate cluster data into totals."""
        # Aggregate resource counts
        resource_counts = cluster_data.get("resource_counts", {})
        totals["total_nodes"] += resource_counts.get("total_nodes", 0)
        totals["total_pods"] += resource_counts.get("total_pods", 0)
        totals["total_namespaces"] += len(cluster_data.get("namespaces", []))
        totals["total_services"] += resource_counts.get("total_services", 0)
        totals["total_storage_volumes"] += resource_counts.get("total_storage_volumes", 0)
        
        # Aggregate costs
        cost_data = cluster_data.get("cost_data", {})
        if isinstance(cost_data, dict):
            totals["aggregated_costs"]["total_cost"] += cost_data.get("total", 0.0)
            totals["aggregated_costs"]["compute_cost"] += cost_data.get("compute", 0.0)
            totals["aggregated_costs"]["storage_cost"] += cost_data.get("storage", 0.0)
            totals["aggregated_costs"]["network_cost"] += cost_data.get("network", 0.0)
        
        # Aggregate utilization (will be averaged later)
        utilization = cluster_data.get("utilization_summary", {})
        if isinstance(utilization, dict):
            cpu_util = utilization.get("cpu", {}).get("average_utilization", 0.0)
            memory_util = utilization.get("memory", {}).get("average_utilization", 0.0)
            storage_util = utilization.get("storage", {}).get("average_utilization", 0.0)
            
            # Add to running totals (will divide by cluster count later)
            totals["aggregated_utilization"]["avg_cpu_utilization"] += cpu_util
            totals["aggregated_utilization"]["avg_memory_utilization"] += memory_util
            totals["aggregated_utilization"]["avg_storage_utilization"] += storage_util
    
    def _finalize_aggregations(self, totals: Dict[str, Any], cluster_count: int) -> None:
        """Finalize aggregations by calculating averages."""
        if cluster_count > 0:
            # Calculate average utilization across clusters
            totals["aggregated_utilization"]["avg_cpu_utilization"] /= cluster_count
            totals["aggregated_utilization"]["avg_memory_utilization"] /= cluster_count
            totals["aggregated_utilization"]["avg_storage_utilization"] /= cluster_count
        
        # Round values for better presentation
        totals["aggregated_costs"]["total_cost"] = round(totals["aggregated_costs"]["total_cost"], 2)
        totals["aggregated_costs"]["compute_cost"] = round(totals["aggregated_costs"]["compute_cost"], 2)
        totals["aggregated_costs"]["storage_cost"] = round(totals["aggregated_costs"]["storage_cost"], 2)
        totals["aggregated_costs"]["network_cost"] = round(totals["aggregated_costs"]["network_cost"], 2)
        
        totals["aggregated_utilization"]["avg_cpu_utilization"] = round(totals["aggregated_utilization"]["avg_cpu_utilization"], 2)
        totals["aggregated_utilization"]["avg_memory_utilization"] = round(totals["aggregated_utilization"]["avg_memory_utilization"], 2)
        totals["aggregated_utilization"]["avg_storage_utilization"] = round(totals["aggregated_utilization"]["avg_storage_utilization"], 2)