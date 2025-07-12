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
    """Enhanced discovery client with proper cost isolation and raw metrics."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config, "DiscoveryClient")
        self.subscription_id = config.get("subscription_id")
        self.resource_group = config.get("resource_group")
        
        # Initialize all required clients
        self.aks_client: Optional[AKSClient] = None
        self.cost_client: Optional[CostClient] = None
        self.monitor_client: Optional[MonitorClient] = None
        self.k8s_client: Optional[KubernetesClient] = None
        
        # Configuration
        self.cost_analysis_days = config.get("cost_analysis_days", 30)
        self.metrics_hours = config.get("metrics_hours", 24)
        self.include_detailed_metrics = config.get("include_detailed_metrics", True)
        self.include_cost_allocation = config.get("include_cost_allocation", True)
        
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
        """Discover comprehensive cluster data with isolated costs and raw utilization."""
        if not self._connected:
            raise DiscoveryException("EnhancedDiscovery", "Client not connected")
            
        self.logger.info("Starting comprehensive cluster discovery with enhanced cost and metrics")
        
        # Get all subscription costs by resource group first for context
        subscription_cost_breakdown = await self._get_subscription_cost_breakdown()
        
        # Discover clusters
        clusters = await self.aks_client.discover_clusters(self.resource_group)
        
        comprehensive_data = {
            "discovery_timestamp": datetime.now(timezone.utc).isoformat(),
            "discovery_scope": {
                "subscription_id": self.subscription_id,
                "resource_group": self.resource_group,
                "cost_analysis_days": self.cost_analysis_days,
                "metrics_hours": self.metrics_hours,
                "cost_isolation_method": "resource_group_scope",
                "metrics_collection_method": "raw_log_analytics_azure_monitor"
            },
            "subscription_cost_context": subscription_cost_breakdown,
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
                    "monitoring_cost": 0.0,
                    "other_cost": 0.0,
                    "currency": "USD",
                    "cost_validation": {
                        "resource_group_isolated": True,
                        "cross_contamination_detected": False
                    }
                },
                "aggregated_utilization": {
                    "cluster_cpu_average": 0.0,
                    "cluster_memory_average": 0.0,
                    "cluster_storage_average": 0.0,
                    "cluster_network_throughput_mbps": 0.0,
                    "resource_pressure_indicators": {
                        "cpu_pressure": False,
                        "memory_pressure": False,
                        "storage_pressure": False,
                        "network_pressure": False
                    }
                }
            }
        }
        
        # Process each cluster comprehensively
        for cluster in clusters:
            try:
                cluster_data = await self._discover_cluster_comprehensive_enhanced(cluster)
                comprehensive_data["clusters"].append(cluster_data)
                
                # # Aggregate totals with validation
                # self._aggregate_cluster_totals_enhanced(comprehensive_data["totals"], cluster_data)
                
            except Exception as e:
                self.logger.error(f"Failed to process cluster {cluster['name']}", error=str(e))
                # Add error entry for the cluster
                comprehensive_data["clusters"].append({
                    "basic_info": cluster,
                    "error": str(e),
                    "discovery_status": "failed",
                    "cost_data": {"total": 0.0, "error": "discovery_failed"},
                    "utilization_summary": {"error": "discovery_failed"}
                })
        
        # # Finalize aggregations with enhanced validation
        # self._finalize_aggregations_enhanced(comprehensive_data["totals"], len(clusters))
        
        self.logger.info(
            "Enhanced comprehensive cluster discovery completed",
            clusters=len(clusters),
            total_cost=comprehensive_data["totals"]["aggregated_costs"]["total_cost"],
            cost_isolation_validated=comprehensive_data["totals"]["aggregated_costs"]["cost_validation"]["resource_group_isolated"]
        )
        
        return comprehensive_data
    
    async def _get_subscription_cost_breakdown(self) -> Dict[str, Any]:
        """Get subscription-wide cost breakdown for context."""
        try:
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=self.cost_analysis_days)
            
            breakdown = await self.cost_client.get_subscription_costs_by_resource_group(
                start_date=start_date,
                end_date=end_date
            )
            
            return {
                "subscription_breakdown": breakdown,
                "target_resource_group_ranking": self._get_resource_group_ranking(breakdown),
                "cost_context": {
                    "analysis_period_days": self.cost_analysis_days,
                    "total_subscription_cost": breakdown.get("total_subscription_cost", 0.0),
                    "target_rg_percentage": self._calculate_target_rg_percentage(breakdown)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get subscription cost breakdown: {e}")
            # Return fallback structure
            return {
                "subscription_breakdown": {
                    "total_subscription_cost": 0.0,
                    "by_resource_group": {},
                    "resource_group_ranking": [],
                    "error": str(e)
                },
                "target_resource_group_ranking": {
                    "target_resource_group": self.resource_group,
                    "position_in_subscription": None,
                    "total_resource_groups": 0,
                    "top_3_resource_groups": []
                },
                "cost_context": {
                    "analysis_period_days": self.cost_analysis_days,
                    "total_subscription_cost": 0.0,
                    "target_rg_percentage": 0.0,
                    "error": "subscription_breakdown_failed"
                }
            }
    
    async def _discover_cluster_comprehensive_enhanced(self, cluster: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced comprehensive discovery for a single cluster."""
        cluster_name = cluster["name"]
        resource_group = cluster["resource_group"]
        cluster_id = cluster["id"]
        
        self.logger.info(f"Processing cluster with enhanced discovery: {cluster_name}")
        
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
            "raw_metrics": {},
            "resource_counts": {
                "total_nodes": 0,
                "total_pods": 0,
                "total_services": 0,
                "total_storage_volumes": 0
            },
            "cost_validation": {
                "isolation_method": "resource_group_scope",
                "cross_contamination_check": True
            }
        }
        
        try:
            # 1. Get isolated resource group costs
            cluster_data["cost_data"] = await self._get_isolated_cluster_costs(
                cluster_name, resource_group
            )
            
            # 2. Get detailed resource-level costs within the resource group
            cluster_data["detailed_costs"] = await self._get_detailed_resource_costs(
                resource_group
            )
            
            # 3. Get raw metrics with maximum detail
            cluster_data["raw_metrics"] = await self._get_cluster_raw_metrics(
                cluster_id, cluster_name
            )
            
            # 4. Generate utilization summary from raw metrics
            cluster_data["utilization_summary"] = self._process_raw_metrics_to_utilization(
                cluster_data["raw_metrics"]
            )
            
            # 5. Discover enhanced node pools
            cluster_data["node_pools"] = await self._discover_enhanced_node_pools(
                cluster_name, resource_group, cluster_data["raw_metrics"]
            )
            
            # 6. Discover Kubernetes resources if client available
            if self.k8s_client:
                k8s_data = await self._discover_kubernetes_resources_enhanced(
                    cluster_name, cluster_data["raw_metrics"], cluster_data["cost_data"]
                )
                cluster_data.update(k8s_data)
            
            # 7. Discover related Azure resources
            cluster_data["azure_resources"] = await self._discover_cluster_azure_resources(
                cluster_name, resource_group
            )
            
            # 8. Calculate resource counts
            self._calculate_cluster_resource_counts(cluster_data)
            
            # 9. Validate cost isolation
            cluster_data["cost_validation"] = self._validate_cost_isolation(cluster_data["cost_data"])
            
            cluster_data["discovery_status"] = "completed"
            
        except Exception as e:
            self.logger.error(f"Error in enhanced comprehensive discovery for {cluster_name}", error=str(e))
            cluster_data["discovery_status"] = "failed"
            cluster_data["error"] = str(e)
            
        return cluster_data
    
    async def _get_isolated_cluster_costs(self, cluster_name: str, resource_group: str) -> Dict[str, Any]:
        """Get completely isolated costs for the resource group."""
        cache_key = f"isolated_costs_{resource_group}_{self.cost_analysis_days}"
        if cache_key in self._cost_cache:
            return self._cost_cache[cache_key]
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=self.cost_analysis_days)
        
        try:
            # Use the enhanced cost client for complete isolation
            cost_data = await self.cost_client.get_resource_group_costs_isolated(
                resource_group=resource_group,
                start_date=start_date,
                end_date=end_date
            )
            
            # Add cluster-specific context
            cost_data["cluster_context"] = {
                "cluster_name": cluster_name,
                "resource_group": resource_group,
                "isolation_validated": cost_data.get("resource_group_validation", {}).get("isolation_validated", False)
            }
            
            self._cost_cache[cache_key] = cost_data
            return cost_data
            
        except Exception as e:
            self.logger.error(f"Error getting isolated costs for {cluster_name}", error=str(e))
            return self._get_default_cost_structure_enhanced(cluster_name, resource_group, str(e))
    
    async def _get_detailed_resource_costs(self, resource_group: str) -> Dict[str, Any]:
        """Get detailed per-resource costs within the resource group."""
        try:
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=self.cost_analysis_days)
            
            detailed_costs = await self.cost_client.get_detailed_resource_costs(
                resource_group=resource_group,
                start_date=start_date,
                end_date=end_date
            )
            
            return detailed_costs
            
        except Exception as e:
            self.logger.error(f"Error getting detailed resource costs for {resource_group}", error=str(e))
            return {"error": str(e), "resources": {}}
    
    async def _get_cluster_raw_metrics(self, cluster_id: str, cluster_name: str) -> Dict[str, Any]:
        """Get comprehensive raw metrics for the cluster."""
        cache_key = f"raw_metrics_{cluster_name}_{self.metrics_hours}"
        if cache_key in self._metrics_cache:
            return self._metrics_cache[cache_key]
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=self.metrics_hours)
        
        try:
            raw_metrics = await self.monitor_client.get_cluster_raw_metrics(
                cluster_resource_id=cluster_id,
                cluster_name=cluster_name,
                start_time=start_time,
                end_time=end_time,
                granularity_minutes=5
            )
            
            self._metrics_cache[cache_key] = raw_metrics
            return raw_metrics
            
        except Exception as e:
            self.logger.error(f"Error getting raw metrics for {cluster_name}", error=str(e))
            return self._get_default_metrics_structure_enhanced(cluster_name, str(e))
    
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
    
    def _process_raw_metrics_to_utilization(self, raw_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw metrics into comprehensive utilization summary."""
        if not raw_metrics or "error" in raw_metrics:
            return {"error": "raw_metrics_unavailable", "metrics_availability": False}
        
        utilization_summary = raw_metrics.get("utilization_summary", {})
        derived_metrics = raw_metrics.get("derived_metrics", {})
        
        # Enhanced utilization processing
        enhanced_utilization = {
            "metrics_availability": True,
            "data_sources": raw_metrics.get("collection_metadata", {}).get("data_sources", []),
            "collection_period": raw_metrics.get("collection_period", {}),
            "total_data_points": raw_metrics.get("collection_metadata", {}).get("total_data_points", 0),
            
            # Node-level utilization
            "node_utilization": {
                "cpu_utilization_by_node": derived_metrics.get("cpu_utilization_percentages", {}),
                "memory_utilization_by_node": derived_metrics.get("memory_utilization_percentages", {}),
                "resource_efficiency_by_node": derived_metrics.get("resource_efficiency_scores", {}),
                "cluster_averages": utilization_summary.get("cluster_wide_utilization", {})
            },
            
            # Resource pressure indicators
            "resource_pressure": utilization_summary.get("resource_pressure_indicators", {}),
            
            # Efficiency assessment
            "efficiency_assessment": utilization_summary.get("efficiency_assessment", {}),
            
            # Raw data summaries
            "raw_data_summary": {
                "node_metrics_count": len(raw_metrics.get("node_raw_metrics", {}).get("nodes", {})),
                "pod_metrics_count": len(raw_metrics.get("pod_raw_metrics", {}).get("pods", {})),
                "container_metrics_count": len(raw_metrics.get("container_raw_metrics", {}).get("containers", {})),
                "storage_devices_count": len(raw_metrics.get("storage_raw_metrics", {}).get("storage_devices", {})),
                "network_interfaces_count": len(raw_metrics.get("network_raw_metrics", {}).get("network_interfaces", {}))
            }
        }
        
        return enhanced_utilization
    
    async def _discover_enhanced_node_pools(self, cluster_name: str, resource_group: str, 
                                          raw_metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover node pools with enhanced cost and utilization data."""
        node_pools = await self.aks_client.discover_node_pools(cluster_name, resource_group)
        
        enhanced_pools = []
        for pool in node_pools:
            enhanced_pool = pool.copy()
            
            # Add cost allocation based on actual Azure pricing
            enhanced_pool["cost_allocation"] = await self._calculate_node_pool_cost_allocation(
                pool, resource_group
            )
            
            # Add utilization data from raw metrics
            enhanced_pool["utilization_metrics"] = self._extract_node_pool_utilization(
                pool, raw_metrics
            )
            
            # Add efficiency analysis
            enhanced_pool["efficiency_analysis"] = self._analyze_node_pool_efficiency(
                enhanced_pool["utilization_metrics"], enhanced_pool["cost_allocation"]
            )
            
            # Add detailed node information with metrics
            enhanced_pool["nodes"] = await self._get_enhanced_node_details(
                pool, raw_metrics
            )
            
            enhanced_pools.append(enhanced_pool)
            
        return enhanced_pools
    
    async def _get_enhanced_node_details(self, pool: Dict[str, Any], raw_metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch enhanced details for individual nodes in a node pool, including metrics and cost allocation."""
        pool_name = pool.get("name", "")
        resource_group = pool.get("resource_group", self.resource_group)
        cluster_name = pool.get("cluster_name", "unknown")

        self.logger.info(f"Fetching enhanced node details for pool {pool_name} in cluster {cluster_name}")
        
        try:
            # Get node metrics from raw_metrics (provided by _get_cluster_raw_metrics)
            node_metrics = raw_metrics.get("node_raw_metrics", {}).get("nodes", {})
            derived_metrics = raw_metrics.get("derived_metrics", {})

            # Get node pool's VM instances (list nodes in the pool)
            nodes = []
            for node_name, node_data in node_metrics.items():
                if pool_name in node_name or f"aks-{pool_name}" in node_name:
                    # Calculate node-specific cost allocation
                    node_cost_allocation = await self._calculate_node_cost_allocation(
                        node_data, pool, raw_metrics
                    )

                    # Extract node-specific utilization metrics
                    node_utilization = {
                        "cpu_utilization": derived_metrics.get("cpu_utilization_percentages", {}).get(node_name, 0.0),
                        "memory_utilization": derived_metrics.get("memory_utilization_percentages", {}).get(node_name, 0.0),
                        "storage_utilization": derived_metrics.get("storage_utilization_percentages", {}).get(node_name, 0.0),
                        "network_utilization": derived_metrics.get("network_utilization_percentages", {}).get(node_name, 0.0),
                        "raw_data_points": len(node_data.get("data_points", [])),
                    }

                    # Build node details
                    node_details = {
                        "node_name": node_name,
                        "pool_name": pool_name,
                        "resource_group": resource_group,
                        "cost_allocation": node_cost_allocation,
                        "utilization_metrics": node_utilization,
                        "efficiency_analysis": self._analyze_node_efficiency(
                            node_utilization, node_cost_allocation
                        ),
                        "node_properties": node_data.get("properties", {}),
                        "status": "collected"
                    }
                    nodes.append(node_details)

            # If no nodes found, fetch node list from AKS client as fallback
            if not nodes:
                self.logger.warning(f"No metrics found for nodes in pool {pool_name}, fetching node list from AKS")
                try:
                    aks_nodes = await self.aks_client.discover_nodes(cluster_name, resource_group)
                    for node in aks_nodes:
                        if node.get("node_pool") == pool_name:
                            node_details = {
                                "node_name": node.get("name", "unknown"),
                                "pool_name": pool_name,
                                "resource_group": resource_group,
                                "cost_allocation": {"total_cost": 0.0, "error": "no_metrics_available"},
                                "utilization_metrics": {"error": "no_metrics_available"},
                                "efficiency_analysis": {"error": "no_metrics_available"},
                                "node_properties": node,
                                "status": "no_metrics"
                            }
                            nodes.append(node_details)
                except Exception as e:
                    self.logger.error(f"Failed to fetch node list for pool {pool_name}: {e}")
                    nodes.append({
                        "pool_name": pool_name,
                        "resource_group": resource_group,
                        "error": f"Failed to fetch node list: {e}",
                        "status": "failed"
                    })

            self.logger.info(f"Collected details for {len(nodes)} nodes in pool {pool_name}")
            return nodes

        except Exception as e:
            self.logger.error(f"Error fetching enhanced node details for pool {pool_name}: {e}")
            return [{
                "pool_name": pool_name,
                "resource_group": resource_group,
                "error": str(e),
                "status": "failed"
            }]
    
    async def _calculate_node_cost_allocation(self, node_data: Dict[str, Any], pool: Dict[str, Any], 
                                         raw_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate cost allocation for a single node based on its resource usage."""
        node_name = node_data.get("name", "unknown")
        pool_cost_allocation = pool.get("cost_allocation", {})
        total_pool_cost = pool_cost_allocation.get("total_cost", 0.0)
        
        try:
            # Get node utilization metrics
            derived_metrics = raw_metrics.get("derived_metrics", {})
            cpu_util = derived_metrics.get("cpu_utilization_percentages", {}).get(node_name, 0.0)
            memory_util = derived_metrics.get("memory_utilization_percentages", {}).get(node_name, 0.0)
            
            # Estimate node cost based on proportional utilization
            total_nodes = pool.get("count", 1)
            if total_nodes > 0 and (cpu_util > 0 or memory_util > 0):
                # Simple proportional allocation based on CPU and memory usage
                utilization_ratio = (cpu_util + memory_util) / 200  # Normalize to 100% for CPU + memory
                node_cost = total_pool_cost * utilization_ratio / total_nodes
            else:
                node_cost = total_pool_cost / total_nodes if total_nodes > 0 else 0.0

            return {
                "total_cost": node_cost,
                "compute_cost": node_cost * 0.8,  # Assume 80% is compute
                "storage_cost": node_cost * 0.15,  # Assume 15% is storage
                "network_cost": node_cost * 0.05,  # Assume 5% is network
                "currency": pool_cost_allocation.get("currency", "USD"),
                "allocation_method": "proportional_by_utilization",
                "resource_basis": {
                    "cpu_utilization_percent": cpu_util,
                    "memory_utilization_percent": memory_util,
                    "node_count_in_pool": total_nodes
                }
            }
        except Exception as e:
            self.logger.error(f"Error calculating cost allocation for node {node_name}: {e}")
            return {
                "total_cost": 0.0,
                "currency": "USD",
                "error": str(e),
                "allocation_method": "failed"
            }

    def _analyze_node_efficiency(self, utilization_metrics: Dict[str, Any], 
                                cost_allocation: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze efficiency of a single node based on utilization and cost."""
        efficiency_analysis = {
            "cost_efficiency_score": 0.0,
            "utilization_efficiency_score": 0.0,
            "overall_efficiency_score": 0.0,
            "recommendations": [],
            "potential_savings": 0.0
        }
        
        try:
            avg_cpu = utilization_metrics.get("cpu_utilization", 0.0)
            avg_memory = utilization_metrics.get("memory_utilization", 0.0)
            total_cost = cost_allocation.get("total_cost", 0.0)
            
            # Calculate utilization efficiency (target 70% CPU, 80% memory)
            cpu_efficiency = min(avg_cpu / 70, 2 - avg_cpu / 70) * 100 if avg_cpu <= 140 else 0
            memory_efficiency = min(avg_memory / 80, 2 - avg_memory / 80) * 100 if avg_memory <= 160 else 0
            
            efficiency_analysis["utilization_efficiency_score"] = (cpu_efficiency + memory_efficiency) / 2
            
            # Calculate cost efficiency
            if avg_cpu > 0 and avg_memory > 0:
                efficiency_analysis["cost_efficiency_score"] = ((avg_cpu + avg_memory) / 2) * (100 / total_cost) if total_cost > 0 else 0
            
            efficiency_analysis["overall_efficiency_score"] = (
                efficiency_analysis["utilization_efficiency_score"] + 
                efficiency_analysis["cost_efficiency_score"]
            ) / 2
            
            # Generate recommendations
            if avg_cpu < 30:
                efficiency_analysis["recommendations"].append("Node underutilized: consider moving workloads or downsizing")
                efficiency_analysis["potential_savings"] = total_cost * 0.3
            elif avg_cpu > 80:
                efficiency_analysis["recommendations"].append("Node overutilized: consider adding nodes or larger instances")
            
            if avg_memory < 40:
                efficiency_analysis["recommendations"].append("Low memory utilization: consider memory-optimized instances")
            
            return efficiency_analysis
        except Exception as e:
            self.logger.error(f"Error analyzing node efficiency: {e}")
            return {"error": str(e), "status": "failed"}
        
    async def _discover_kubernetes_resources_enhanced(self, cluster_name: str, 
                                                    raw_metrics: Dict[str, Any],
                                                    cost_data: Dict[str, Any]) -> Dict[str, Any]:
        """Discover Kubernetes resources with enhanced cost allocation and metrics."""
        k8s_data = {
            "namespaces": [],
            "workloads": [],
            "services": [],
            "storage": [],
            "ingresses": []
        }
        
        try:
            # Get all Kubernetes resources
            namespaces = await self.k8s_client.discover_namespaces()
            pods = await self.k8s_client.discover_pods()
            deployments = await self.k8s_client.discover_deployments()
            services = await self.k8s_client.discover_services()
            pvs = await self.k8s_client.discover_persistent_volumes()
            pvcs = await self.k8s_client.discover_persistent_volume_claims()
            
            # Enhanced namespace processing with cost allocation
            total_rg_cost = cost_data.get("total", 0.0)
            for namespace in namespaces:
                enhanced_ns = namespace.copy()
                
                # Calculate namespace cost allocation based on resource usage
                enhanced_ns["cost_allocation"] = self._calculate_namespace_cost_allocation(
                    namespace, pods, deployments, total_rg_cost, raw_metrics
                )
                
                # Add resource quotas and limits
                enhanced_ns["resource_quotas"] = await self._get_namespace_quotas(namespace["name"])
                enhanced_ns["limit_ranges"] = await self._get_namespace_limit_ranges(namespace["name"])
                
                # Calculate namespace metrics from pod metrics
                enhanced_ns["utilization_metrics"] = self._calculate_namespace_utilization(
                    namespace, raw_metrics
                )
                
                # Count resources in namespace
                ns_pods = [p for p in pods if p.get("namespace") == namespace["name"]]
                ns_deployments = [d for d in deployments if d.get("namespace") == namespace["name"]]
                enhanced_ns["resource_counts"] = {
                    "pod_count": len(ns_pods),
                    "deployment_count": len(ns_deployments),
                    "total_containers": sum(len(p.get("containers", [])) for p in ns_pods)
                }
                
                k8s_data["namespaces"].append(enhanced_ns)
            
            # Enhanced workload processing
            for pod in pods:
                enhanced_pod = pod.copy()
                enhanced_pod["cost_allocation"] = self._calculate_pod_cost_allocation(
                    pod, total_rg_cost, raw_metrics
                )
                enhanced_pod["utilization_metrics"] = self._extract_pod_utilization_from_raw(
                    pod, raw_metrics
                )
                enhanced_pod["resource_type"] = "pod"
                k8s_data["workloads"].append(enhanced_pod)
            
            for deployment in deployments:
                enhanced_deployment = deployment.copy()
                enhanced_deployment["cost_allocation"] = self._calculate_deployment_cost_allocation(
                    deployment, pods, total_rg_cost
                )
                enhanced_deployment["resource_type"] = "deployment"
                k8s_data["workloads"].append(enhanced_deployment)
            
            # Enhanced service processing
            for service in services:
                enhanced_service = service.copy()
                enhanced_service["cost_allocation"] = self._calculate_service_cost_allocation(
                    service, cost_data
                )
                enhanced_service["load_balancer_analysis"] = await self._analyze_load_balancer_costs(
                    service, cost_data
                )
                k8s_data["services"].append(enhanced_service)
            
            # Enhanced storage processing
            for pv in pvs:
                enhanced_pv = pv.copy()
                enhanced_pv["cost_allocation"] = self._calculate_storage_cost_allocation(
                    pv, cost_data
                )
                enhanced_pv["utilization_metrics"] = self._extract_storage_utilization_from_raw(
                    pv, raw_metrics
                )
                enhanced_pv["performance_analysis"] = self._analyze_storage_performance(
                    pv, raw_metrics
                )
                enhanced_pv["resource_type"] = "persistent_volume"
                k8s_data["storage"].append(enhanced_pv)
            
            for pvc in pvcs:
                enhanced_pvc = pvc.copy()
                enhanced_pvc["cost_allocation"] = self._calculate_storage_cost_allocation(
                    pvc, cost_data
                )
                enhanced_pvc["resource_type"] = "persistent_volume_claim"
                k8s_data["storage"].append(enhanced_pvc)
            
            # Get ingresses if available
            try:
                ingresses = await self.k8s_client.discover_ingresses()
                for ingress in ingresses:
                    enhanced_ingress = ingress.copy()
                    enhanced_ingress["cost_allocation"] = self._calculate_ingress_cost_allocation(
                        ingress, cost_data
                    )
                    k8s_data["ingresses"].append(enhanced_ingress)
            except Exception as e:
                self.logger.debug(f"Could not discover ingresses: {e}")
            
        except Exception as e:
            self.logger.error(f"Error discovering enhanced Kubernetes resources: {e}")
            
        return k8s_data
    
    # Enhanced cost allocation methods
    async def _calculate_node_pool_cost_allocation(self, pool: Dict[str, Any], 
                                                 resource_group: str) -> Dict[str, Any]:
        """Calculate accurate cost allocation for node pool based on Azure pricing."""
        vm_size = pool.get("vm_size", "Standard_D2s_v3")
        node_count = pool.get("count", 1)
        
        # Get actual Azure pricing (this would integrate with Azure Pricing API in production)
        vm_hourly_rate = self._get_azure_vm_pricing(vm_size, pool.get("spot_max_price"))
        hours_in_period = self.cost_analysis_days * 24
        
        compute_cost = vm_hourly_rate * node_count * hours_in_period
        
        # Add disk costs
        os_disk_size = pool.get("os_disk_size_gb", 128)
        disk_type = pool.get("os_disk_type", "Managed")
        disk_cost = self._calculate_disk_cost(os_disk_size, disk_type, hours_in_period)
        
        return {
            "total_cost": compute_cost + disk_cost,
            "compute_cost": compute_cost,
            "storage_cost": disk_cost,
            "vm_hourly_rate": vm_hourly_rate,
            "pricing_model": "spot" if pool.get("scale_set_priority") == "Spot" else "regular",
            "cost_breakdown": {
                "vm_cost_per_hour": vm_hourly_rate,
                "total_vm_hours": node_count * hours_in_period,
                "disk_cost_per_gb_month": self._get_disk_pricing(disk_type),
                "total_disk_gb": os_disk_size * node_count
            },
            "currency": "USD"
        }
    
    
    
    def _extract_node_pool_utilization(self, pool: Dict[str, Any], 
                                     raw_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Extract utilization metrics for a specific node pool from raw metrics."""
        pool_name = pool.get("name", "")
        
        # Extract node metrics that belong to this pool
        node_metrics = raw_metrics.get("node_raw_metrics", {}).get("nodes", {})
        derived_metrics = raw_metrics.get("derived_metrics", {})
        
        pool_utilization = {
            "cpu_utilization": {},
            "memory_utilization": {},
            "storage_utilization": {},
            "network_utilization": {},
            "pool_summary": {
                "average_cpu_percent": 0.0,
                "average_memory_percent": 0.0,
                "peak_cpu_percent": 0.0,
                "peak_memory_percent": 0.0,
                "nodes_in_pool": 0
            }
        }
        
        # Match nodes to this pool (nodes typically have pool name in their computer name)
        pool_nodes = {}
        for node_name, node_data in node_metrics.items():
            if pool_name in node_name or f"aks-{pool_name}" in node_name:
                pool_nodes[node_name] = node_data
                pool_utilization["pool_summary"]["nodes_in_pool"] += 1
        
        # Calculate pool-level metrics
        if pool_nodes:
            cpu_utilizations = []
            memory_utilizations = []
            
            for node_name, node_data in pool_nodes.items():
                # Get CPU utilization
                cpu_util = derived_metrics.get("cpu_utilization_percentages", {}).get(node_name, 0)
                memory_util = derived_metrics.get("memory_utilization_percentages", {}).get(node_name, 0)
                
                if cpu_util > 0:
                    cpu_utilizations.append(cpu_util)
                if memory_util > 0:
                    memory_utilizations.append(memory_util)
                
                pool_utilization["cpu_utilization"][node_name] = cpu_util
                pool_utilization["memory_utilization"][node_name] = memory_util
            
            # Calculate averages and peaks
            if cpu_utilizations:
                pool_utilization["pool_summary"]["average_cpu_percent"] = sum(cpu_utilizations) / len(cpu_utilizations)
                pool_utilization["pool_summary"]["peak_cpu_percent"] = max(cpu_utilizations)
            
            if memory_utilizations:
                pool_utilization["pool_summary"]["average_memory_percent"] = sum(memory_utilizations) / len(memory_utilizations)
                pool_utilization["pool_summary"]["peak_memory_percent"] = max(memory_utilizations)
        
        return pool_utilization
    
    def _analyze_node_pool_efficiency(self, utilization_metrics: Dict[str, Any], 
                                    cost_allocation: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze node pool efficiency based on utilization and cost."""
        efficiency_analysis = {
            "cost_efficiency_score": 0.0,
            "utilization_efficiency_score": 0.0,
            "overall_efficiency_score": 0.0,
            "recommendations": [],
            "potential_savings": 0.0
        }
        
        avg_cpu = utilization_metrics.get("pool_summary", {}).get("average_cpu_percent", 0)
        avg_memory = utilization_metrics.get("pool_summary", {}).get("average_memory_percent", 0)
        total_cost = cost_allocation.get("total_cost", 0)
        
        # Calculate utilization efficiency (target 70% CPU, 80% memory)
        cpu_efficiency = min(avg_cpu / 70, 2 - avg_cpu / 70) * 100 if avg_cpu <= 140 else 0
        memory_efficiency = min(avg_memory / 80, 2 - avg_memory / 80) * 100 if avg_memory <= 160 else 0
        
        efficiency_analysis["utilization_efficiency_score"] = (cpu_efficiency + memory_efficiency) / 2
        
        # Calculate cost efficiency (cost per utilized resource)
        if avg_cpu > 0 and avg_memory > 0:
            efficiency_analysis["cost_efficiency_score"] = ((avg_cpu + avg_memory) / 2) * (100 / total_cost) if total_cost > 0 else 0
        
        efficiency_analysis["overall_efficiency_score"] = (
            efficiency_analysis["utilization_efficiency_score"] + 
            efficiency_analysis["cost_efficiency_score"]
        ) / 2
        
        # Generate recommendations
        if avg_cpu < 30:
            efficiency_analysis["recommendations"].append("Consider reducing node count or using smaller VM sizes")
            efficiency_analysis["potential_savings"] = total_cost * 0.3
        elif avg_cpu > 80:
            efficiency_analysis["recommendations"].append("Consider increasing node count or using larger VM sizes")
        
        if avg_memory < 40:
            efficiency_analysis["recommendations"].append("Consider using memory-optimized instances")
        
        return efficiency_analysis
    
    def _calculate_namespace_cost_allocation(self, namespace: Dict[str, Any], pods: List[Dict[str, Any]], 
                                           deployments: List[Dict[str, Any]], total_cost: float,
                                           raw_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate accurate cost allocation for namespace based on resource usage."""
        ns_name = namespace.get("name", "")
        
        # Get pods in this namespace
        ns_pods = [p for p in pods if p.get("namespace") == ns_name]
        
        # Calculate resource requests/usage for cost allocation
        total_cpu_requests = 0.0
        total_memory_requests = 0.0
        
        for pod in ns_pods:
            for container in pod.get("containers", []):
                resources = container.get("resources", {}).get("requests", {})
                cpu_req = self._parse_cpu_string(resources.get("cpu", "0"))
                memory_req = self._parse_memory_string(resources.get("memory", "0"))
                
                total_cpu_requests += cpu_req
                total_memory_requests += memory_req
        
        # Calculate cost allocation percentage based on resource usage
        # This is a simplified model - in production, this would be more sophisticated
        if len(pods) > 0:  # Avoid division by zero
            cluster_total_pods = len(pods)
            namespace_pod_ratio = len(ns_pods) / cluster_total_pods
            allocated_cost = total_cost * namespace_pod_ratio
        else:
            allocated_cost = 0.0
        
        return {
            "allocated_cost": allocated_cost,
            "allocation_method": "proportional_by_pod_count_and_requests",
            "resource_basis": {
                "pod_count": len(ns_pods),
                "total_cpu_requests_millicores": total_cpu_requests,
                "total_memory_requests_mb": total_memory_requests,
                "cost_allocation_percentage": (allocated_cost / total_cost * 100) if total_cost > 0 else 0
            },
            "currency": "USD"
        }
    
    async def _get_namespace_quotas(self, namespace: str) -> Dict[str, Any]:
        """Get namespace resource quotas."""
        try:
            quotas = await self.k8s_client.discover_resource_quotas(namespace)
            return {"quotas": quotas} if quotas else {"quotas": [], "no_quotas_defined": True}
        except:
            return {"quotas": [], "error": "Could not retrieve quotas"}
        
    # Helper methods for pricing and resource parsing
    def _get_azure_vm_pricing(self, vm_size: str, spot_max_price: Optional[float] = None) -> float:
        """Get Azure VM pricing per hour (would integrate with Azure Pricing API)."""
        # Updated pricing map based on 2024 Azure pricing
        regular_pricing = {
            "Standard_D2s_v3": 0.096,
            "Standard_D4s_v3": 0.192,
            "Standard_D8s_v3": 0.384,
            "Standard_D16s_v3": 0.768,
            "Standard_B2s": 0.041,
            "Standard_B4ms": 0.166,
            "Standard_E2s_v3": 0.134,
            "Standard_E4s_v3": 0.268,
            "Standard_F2s_v2": 0.085,
            "Standard_F4s_v2": 0.169,
            "Standard_DS2_v2": 0.094,
            "Standard_DS3_v2": 0.188
        }
        
        base_price = regular_pricing.get(vm_size, 0.096)  # Default to D2s_v3
        
        # Apply spot pricing discount if applicable
        if spot_max_price is not None:
            return min(base_price * 0.2, spot_max_price)  # Spot is typically 80% cheaper
        
        return base_price
    
    def _calculate_disk_cost(self, size_gb: int, disk_type: str, hours: float) -> float:
        """Calculate disk cost based on size and type."""
        # Azure managed disk pricing (monthly rates converted to hourly)
        pricing_per_gb_month = {
            "Standard_LRS": 0.045,
            "Standard_SSD": 0.10,
            "Premium_SSD": 0.135,
            "Managed": 0.045  # Default to Standard
        }
        
        monthly_rate = pricing_per_gb_month.get(disk_type, 0.045)
        hourly_rate = monthly_rate / (30 * 24)  # Convert monthly to hourly
        
        return size_gb * hourly_rate * hours
    
    def _get_disk_pricing(self, disk_type: str) -> float:
        """Get disk pricing per GB per month."""
        pricing_map = {
            "Standard_LRS": 0.045,
            "Standard_SSD": 0.10,
            "Premium_SSD": 0.135,
            "Managed": 0.045
        }
        return pricing_map.get(disk_type, 0.045)
    
    def _parse_cpu_string(self, cpu_str: str) -> float:
        """Parse CPU string to millicores."""
        if not cpu_str or cpu_str == "0":
            return 0.0
        if cpu_str.endswith("m"):
            return float(cpu_str[:-1])
        return float(cpu_str) * 1000
    
    def _parse_memory_string(self, memory_str: str) -> float:
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
    
    # Default structures for error cases
    def _get_default_cost_structure_enhanced(self, cluster_name: str, resource_group: str, error: str) -> Dict[str, Any]:
        """Enhanced default cost structure with error context."""
        return {
            "total": 0.0,
            "compute": 0.0,
            "storage": 0.0,
            "network": 0.0,
            "monitoring": 0.0,
            "other": 0.0,
            "currency": "USD",
            "status": "error",
            "error": error,
            "cluster_context": {
                "cluster_name": cluster_name,
                "resource_group": resource_group,
                "isolation_validated": False
            },
            "resource_group_validation": {
                "target_resource_group": resource_group,
                "other_resource_groups_found": [],
                "isolation_validated": False
            }
        }
    
    def _get_default_metrics_structure_enhanced(self, cluster_name: str, error: str) -> Dict[str, Any]:
        """Enhanced default metrics structure with error context."""
        return {
            "cluster_name": cluster_name,
            "error": error,
            "collection_metadata": {
                "metrics_collected": [],
                "metrics_failed": ["all"],
                "data_sources": [],
                "total_data_points": 0
            },
            "utilization_summary": {
                "error": error,
                "metrics_availability": False
            }
        }
    
    # Additional helper methods for completeness
    def _get_resource_group_ranking(self, subscription_breakdown: Dict[str, Any]) -> Dict[str, Any]:
        """Get ranking of resource groups by cost."""
        ranking = subscription_breakdown.get("resource_group_ranking", [])
        target_rg = self.resource_group
        
        target_position = None
        for i, (rg_name, cost) in enumerate(ranking):
            if rg_name == target_rg:
                target_position = i + 1
                break
        
        return {
            "target_resource_group": target_rg,
            "position_in_subscription": target_position,
            "total_resource_groups": len(ranking),
            "top_3_resource_groups": ranking[:3] if len(ranking) >= 3 else ranking
        }
    
    def _calculate_target_rg_percentage(self, subscription_breakdown: Dict[str, Any]) -> float:
        """Calculate what percentage of subscription cost the target RG represents."""
        total_cost = subscription_breakdown.get("total_subscription_cost", 0.0)
        by_rg = subscription_breakdown.get("by_resource_group", {})
        target_cost = by_rg.get(self.resource_group, {}).get("total_cost", 0.0)
        
        return (target_cost / total_cost * 100) if total_cost > 0 else 0.0
    
    def _validate_cost_isolation(self, cost_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that cost isolation was successful."""
        validation = cost_data.get("resource_group_validation", {})
        
        return {
            "isolation_method_used": "resource_group_scope",
            "target_resource_group": validation.get("target_resource_group", self.resource_group),
            "isolation_validated": validation.get("isolation_validated", False),
            "cross_contamination_detected": not validation.get("isolation_validated", True),
            "other_resource_groups_found": validation.get("other_resource_groups_found", []),
            "validation_status": "passed" if validation.get("isolation_validated", False) else "failed"
        }
    
    