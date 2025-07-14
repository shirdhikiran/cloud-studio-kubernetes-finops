"""Enhanced discovery client with comprehensive 3-source metrics - no duplicates."""

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
    """Enhanced discovery client with 3-source comprehensive metrics - no duplicates."""
    
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
        """Discover comprehensive data with enhanced 3-source metrics integration."""
        if not self._connected:
            raise DiscoveryException("DiscoveryClient", "Client not connected")
            
        self.logger.info(f"Starting comprehensive discovery with enhanced metrics for: {self.resource_group}")
        
        discovery_data = {
            "discovery_metadata": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "resource_group": self.resource_group,
                "subscription_id": self.subscription_id,
                "cost_analysis_days": self.cost_analysis_days,
                "metrics_hours": self.metrics_hours,
                "metrics_enhancement": "3-source_integration"
            },
            "resource_group_costs": {},
            "clusters": [],
            "summary": {
                "total_clusters": 0,
                "total_nodes": 0,
                "total_pods": 0,
                "total_cost": 0.0,
                "currency": "USD",
                "metrics_sources_available": 0,
                "comprehensive_metrics_enabled": True
            }
        }
        
        try:
            # Get resource group costs (unchanged - no duplicates here)
            discovery_data["resource_group_costs"] = await self._get_resource_group_costs_safe()
            
            # Discover clusters
            clusters = await self.aks_client.discover_clusters(self.resource_group)
            discovery_data["summary"]["total_clusters"] = len(clusters)
            
            # Process each cluster with enhanced metrics
            for cluster in clusters:
                cluster_data = await self._discover_cluster_with_enhanced_metrics(cluster)
                discovery_data["clusters"].append(cluster_data)
                
                # Update summary (no changes needed here)
                discovery_data["summary"]["total_nodes"] += cluster_data.get("node_count", 0)
                discovery_data["summary"]["total_pods"] += cluster_data.get("pod_count", 0)
            
            # Calculate total cost from resource group costs (unchanged)
            rg_costs = discovery_data["resource_group_costs"]
            discovery_data["summary"]["total_cost"] = rg_costs.get("cost_summary", {}).get("total_cost", 0.0)
            
            # Count available metrics sources
            if discovery_data["clusters"]:
                sample_cluster = discovery_data["clusters"][0]
                enhanced_metrics = sample_cluster.get("enhanced_metrics", {})
                successful_sources = enhanced_metrics.get("collection_metadata", {}).get("successful_sources", 0)
                discovery_data["summary"]["metrics_sources_available"] = successful_sources
            
            self.logger.info(
                f"Enhanced comprehensive discovery completed",
                clusters=len(clusters),
                total_cost=discovery_data["summary"]["total_cost"],
                metrics_sources=discovery_data["summary"]["metrics_sources_available"]
            )
            
            return discovery_data
            
        except Exception as e:
            self.logger.error(f"Enhanced comprehensive discovery failed: {e}")
            raise DiscoveryException("DiscoveryClient", f"Discovery failed: {e}")
    
    async def _discover_cluster_with_enhanced_metrics(self, cluster: Dict[str, Any]) -> Dict[str, Any]:
        """Discover cluster data with enhanced 3-source metrics integration."""
        cluster_name = cluster["name"]
        resource_group = cluster["resource_group"]
        cluster_id = cluster["id"]
        
        self.logger.info(f"Processing cluster with enhanced metrics: {cluster_name}")
        
        cluster_data = {
            "cluster_info": cluster,
            "node_pools": [],
            "kubernetes_resources": {},
            "enhanced_metrics": {},  # NEW: Enhanced 3-source metrics
            "utilization_summary": {},  # ENHANCED: Based on new metrics structure
            "node_count": 0,
            "pod_count": 0,
            "discovery_status": "in_progress"
        }
        
        try:
            # Discover node pools (unchanged)
            node_pools = await self.aks_client.discover_node_pools(cluster_name, resource_group)
            cluster_data["node_pools"] = node_pools if node_pools is not None else []
            cluster_data["node_count"] = sum(pool.get("count", 0) for pool in cluster_data["node_pools"] if isinstance(pool, dict))
            
            # NEW: Get enhanced metrics using the new MonitorClient method
            self.logger.info(f"Collecting enhanced 3-source metrics for: {cluster_name}")
            
            try:
                enhanced_metrics = await self.monitor_client.get_enhanced_cluster_metrics(
                    cluster_id, cluster_name, self.metrics_hours
                )
                
                if enhanced_metrics:
                    cluster_data["enhanced_metrics"] = enhanced_metrics
                    self.logger.info(f"Enhanced metrics collected successfully for {cluster_name}")
                    
                    # Log metrics source summary
                    metadata = enhanced_metrics.get("collection_metadata", {})
                    self.logger.info(
                        f"Metrics sources for {cluster_name}",
                        total_data_points=metadata.get("total_data_points", 0),
                        successful_sources=metadata.get("successful_sources", 0),
                        health_score=metadata.get("health_score", 0.0)
                    )
                else:
                    self.logger.warning(f"No enhanced metrics collected for {cluster_name}")
                    cluster_data["enhanced_metrics"] = self._create_empty_enhanced_metrics_structure(cluster_name)
                    
            except Exception as e:
                self.logger.error(f"Enhanced metrics collection failed for {cluster_name}: {e}")
                cluster_data["enhanced_metrics"] = self._create_empty_enhanced_metrics_structure(cluster_name)
            
            # ENHANCED: Build utilization summary from new metrics structure
            cluster_data["utilization_summary"] = self._build_enhanced_utilization_summary(
                cluster_data["enhanced_metrics"], 
                cluster_data["node_pools"]
            )
            
            # Discover Kubernetes resources with enhanced integration
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
                        
                        # ENHANCED: Integrate Kubernetes API data with enhanced metrics
                        self._integrate_kubernetes_data_with_enhanced_metrics(
                            cluster_data["enhanced_metrics"], 
                            k8s_resources
                        )
                        
                        await k8s_cluster_client.disconnect()
                    else:
                        cluster_data["kubernetes_resources"] = {"error": "Failed to get cluster credentials"}
                        
                except Exception as e:
                    self.logger.warning(f"Kubernetes resource discovery failed for {cluster_name}: {e}")
                    cluster_data["kubernetes_resources"] = {"error": str(e)}
            
            cluster_data["discovery_status"] = "completed"
            
        except Exception as e:
            self.logger.error(f"Error processing cluster {cluster_name}: {e}")
            cluster_data["discovery_status"] = "failed"
            cluster_data["error"] = str(e)
        
        return cluster_data
    
    def _integrate_kubernetes_data_with_enhanced_metrics(self, enhanced_metrics: Dict[str, Any], 
                                                       k8s_resources: Dict[str, Any]) -> None:
        """Integrate Kubernetes API data with enhanced metrics (Source 3: Kubernetes API)."""
        if not k8s_resources or not isinstance(enhanced_metrics, dict):
            return
        
        # Update the kubernetes_api data source status
        k8s_source = enhanced_metrics.get('data_sources', {}).get('kubernetes_api', {})
        k8s_source['status'] = 'success'
        k8s_source['resources_discovered'] = list(k8s_resources.keys())
        
        # Add Kubernetes API data to combined_analysis for cross-validation
        combined_analysis = enhanced_metrics.get('combined_analysis', {})
        
        # Cross-validate pod counts between Container Insights and Kubernetes API
        workload_distribution = enhanced_metrics.get('workload_distribution', {})
        k8s_pods = k8s_resources.get('pods', [])
        
        if workload_distribution and k8s_pods:
            # Container Insights pod count
            ci_total_pods = 0
            if isinstance(workload_distribution, list):
                ci_total_pods = sum(ns.get('TotalPods', 0) for ns in workload_distribution)
            elif isinstance(workload_distribution, dict):
                ci_total_pods = workload_distribution.get('TotalPods', 0)
            
            # Kubernetes API pod count
            k8s_total_pods = len(k8s_pods)
            
            # Add cross-validation data
            if 'data_validation' not in combined_analysis:
                combined_analysis['data_validation'] = {}
            
            combined_analysis['data_validation']['pod_count_validation'] = {
                'container_insights_pods': ci_total_pods,
                'kubernetes_api_pods': k8s_total_pods,
                'variance_percentage': abs(ci_total_pods - k8s_total_pods) / max(ci_total_pods, k8s_total_pods, 1) * 100,
                'data_consistency': 'good' if abs(ci_total_pods - k8s_total_pods) <= 2 else 'warning'
            }
        
        # Add current cluster state from Kubernetes API
        cluster_state = {
            'current_namespace_count': len(k8s_resources.get('namespaces', [])),
            'current_deployment_count': len(k8s_resources.get('deployments', [])),
            'current_service_count': len(k8s_resources.get('services', [])),
            'current_pv_count': len(k8s_resources.get('persistent_volumes', [])),
            'current_ingress_count': len(k8s_resources.get('ingresses', []))
        }
        
        combined_analysis['current_cluster_state'] = cluster_state
        
        # Update metadata
        metadata = enhanced_metrics.get('collection_metadata', {})
        metadata['successful_sources'] = metadata.get('successful_sources', 0) + 1
        
        self.logger.debug("Integrated Kubernetes API data with enhanced metrics")
    
    def _build_enhanced_utilization_summary(self, enhanced_metrics: Dict[str, Any], 
                                          node_pools: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build utilization summary from enhanced 3-source metrics structure (NO DUPLICATES)."""
        if not isinstance(enhanced_metrics, dict):
            enhanced_metrics = {}
        if not isinstance(node_pools, list):
            node_pools = []
        
        # Initialize utilization summary with clear source attribution
        utilization_summary = {
            # ===== CLUSTER OVERVIEW (from Container Insights) =====
            "cluster_overview": {
                "source": "container_insights",
                "node_count": 0,
                "ready_nodes": 0,
                "total_vcores": 0,
                "total_memory_gb": 0,
                "node_pools_count": len(node_pools),
                "health_percentage": 0.0
            },
            
            # ===== RESOURCE UTILIZATION (from Azure Monitor) =====
            "resource_utilization": {
                "source": "azure_monitor",
                "cpu": {
                    "latest_percentage": 0.0,
                    "average_percentage": 0.0,
                    "peak_percentage": 0.0,
                    "status": "unknown"
                },
                "memory": {
                    "latest_percentage": 0.0,
                    "average_percentage": 0.0,
                    "peak_percentage": 0.0,
                    "status": "unknown"
                },
                "disk": {
                    "latest_percentage": 0.0,
                    "average_percentage": 0.0,
                    "peak_percentage": 0.0,
                    "status": "unknown"
                },
                "network": {
                    "inbound_bytes_per_sec": 0.0,
                    "outbound_bytes_per_sec": 0.0,
                    "total_throughput": 0.0
                }
            },
            
            # ===== CONTROL PLANE HEALTH (from Azure Monitor) =====
            "control_plane_health": {
                "source": "azure_monitor",
                "apiserver": {
                    "cpu_usage": 0.0,
                    "memory_usage": 0.0,
                    "status": "unknown"
                },
                "etcd": {
                    "cpu_usage": 0.0,
                    "memory_usage": 0.0,
                    "status": "unknown"
                },
                "overall_status": "unknown"
            },
            
            # ===== WORKLOAD DISTRIBUTION (from Container Insights) =====
            "workload_distribution": {
                "source": "container_insights",
                "total_pods": 0,
                "running_pods": 0,
                "pending_pods": 0,
                "failed_pods": 0,
                "succeeded_pods": 0,
                "running_percentage": 0.0,
                "namespace_breakdown": []
            },
            
            # ===== COMBINED ANALYSIS (from all sources) =====
            "combined_analysis": {
                "source": "cross_validation",
                "health_score": 0.0,
                "data_consistency_score": 0.0,
                "recommendations": [],
                "data_source_status": {}
            },
            
            # ===== NODE POOL BREAKDOWN (calculated) =====
            "node_pool_breakdown": [],
            
            # ===== METRICS COLLECTION STATUS =====
            "metrics_collection_status": {
                "total_data_points": 0,
                "successful_sources": 0,
                "available_metrics": []
            }
        }
        
        # ===== POPULATE FROM ENHANCED METRICS =====
        
        # 1. Cluster Overview (from Container Insights) - FIXED STRUCTURE
        cluster_overview = enhanced_metrics.get('cluster_overview', {})

        self.logger.info(f"Processing cluster overview: type={type(cluster_overview)}, value={cluster_overview}")

        if cluster_overview and isinstance(cluster_overview, dict):
            # Check if it's an empty dict
            if not cluster_overview or cluster_overview.get('TotalNodes', 0) == 0:
                self.logger.warning("Cluster overview is empty or has no nodes")
                utilization_summary["cluster_overview"].update({
                    "node_count": 0,
                    "ready_nodes": 0,
                    "not_ready_nodes": 0,
                    "health_percentage": 0.0,
                    "status": "no_data_available"
                })
            else:
                # Handle valid cluster overview structure
                total_nodes = self._safe_int_conversion(cluster_overview.get('TotalNodes', 0))
                ready_nodes = self._safe_int_conversion(cluster_overview.get('ReadyNodes', 0))
                not_ready_nodes = self._safe_int_conversion(cluster_overview.get('NotReadyNodes', 0))
                health_percentage = self._safe_float_conversion(cluster_overview.get('HealthPercentage', 0.0))
                
                utilization_summary["cluster_overview"].update({
                    "node_count": total_nodes,
                    "ready_nodes": ready_nodes,
                    "not_ready_nodes": not_ready_nodes,
                    "health_percentage": health_percentage,
                    "status": "success"
                })
                self.logger.info(f"Cluster overview processed successfully: {total_nodes} nodes, {health_percentage:.1f}% healthy")
        else:
            self.logger.warning(f"Cluster overview data invalid: type={type(cluster_overview)}, value={cluster_overview}")
            utilization_summary["cluster_overview"].update({
                "node_count": 0,
                "ready_nodes": 0,
                "not_ready_nodes": 0,
                "health_percentage": 0.0,
                "status": "collection_failed"
            })
        
        # 2. Resource Utilization (from Azure Monitor node performance) - ALL METRICS
        node_performance = enhanced_metrics.get('node_performance', {})
        if node_performance:
            self.logger.info(f"Processing node performance metrics: {list(node_performance.keys())}")
            
            # CPU utilization (multiple CPU metrics)
            cpu_percentage = node_performance.get('node_cpu_usage_percentage', {})
            cpu_millicores = node_performance.get('node_cpu_usage_millicores', {})
            
            if cpu_percentage or cpu_millicores:
                # Prefer percentage, fallback to millicores
                primary_cpu = cpu_percentage if cpu_percentage else cpu_millicores
                cpu_latest = self._safe_float_conversion(primary_cpu.get('latest_value', 0.0))
                cpu_avg = self._safe_float_conversion(primary_cpu.get('average_value', 0.0))
                cpu_max = self._safe_float_conversion(primary_cpu.get('max_value', 0.0))
                
                utilization_summary["resource_utilization"]["cpu"].update({
                    "latest_percentage": cpu_latest,
                    "average_percentage": cpu_avg,
                    "peak_percentage": cpu_max,
                    "status": self._get_resource_status(cpu_avg, [60, 80]),
                    "millicores_available": self._safe_float_conversion(cpu_millicores.get('latest_value', 0.0)) if cpu_millicores else None,
                    "data_points": primary_cpu.get('data_points', 0)
                })
                self.logger.info(f"CPU metrics processed: avg={cpu_avg}%, latest={cpu_latest}%")
            
            # Memory utilization (multiple memory metrics)
            memory_working_set_pct = node_performance.get('node_memory_working_set_percentage', {})
            memory_working_set_bytes = node_performance.get('node_memory_working_set_bytes', {})
            memory_rss_pct = node_performance.get('node_memory_rss_percentage', {})
            memory_rss_bytes = node_performance.get('node_memory_rss_bytes', {})
            
            # Prefer working set metrics, fallback to RSS
            primary_memory_pct = memory_working_set_pct if memory_working_set_pct else memory_rss_pct
            primary_memory_bytes = memory_working_set_bytes if memory_working_set_bytes else memory_rss_bytes
            
            if primary_memory_pct:
                memory_latest = self._safe_float_conversion(primary_memory_pct.get('latest_value', 0.0))
                memory_avg = self._safe_float_conversion(primary_memory_pct.get('average_value', 0.0))
                memory_max = self._safe_float_conversion(primary_memory_pct.get('max_value', 0.0))
                
                utilization_summary["resource_utilization"]["memory"].update({
                    "latest_percentage": memory_latest,
                    "average_percentage": memory_avg,
                    "peak_percentage": memory_max,
                    "status": self._get_resource_status(memory_avg, [70, 85]),
                    "working_set_bytes": self._safe_float_conversion(primary_memory_bytes.get('latest_value', 0.0)) if primary_memory_bytes else None,
                    "rss_bytes": self._safe_float_conversion(memory_rss_bytes.get('latest_value', 0.0)) if memory_rss_bytes else None,
                    "data_points": primary_memory_pct.get('data_points', 0)
                })
                self.logger.info(f"Memory metrics processed: avg={memory_avg}%, latest={memory_latest}%")
            
            # Disk utilization
            disk_percentage = node_performance.get('node_disk_usage_percentage', {})
            disk_bytes = node_performance.get('node_disk_usage_bytes', {})
            
            if disk_percentage:
                disk_latest = self._safe_float_conversion(disk_percentage.get('latest_value', 0.0))
                disk_avg = self._safe_float_conversion(disk_percentage.get('average_value', 0.0))
                disk_max = self._safe_float_conversion(disk_percentage.get('max_value', 0.0))
                
                utilization_summary["resource_utilization"]["disk"].update({
                    "latest_percentage": disk_latest,
                    "average_percentage": disk_avg,
                    "peak_percentage": disk_max,
                    "status": self._get_resource_status(disk_avg, [75, 90]),
                    "usage_bytes": self._safe_float_conversion(disk_bytes.get('latest_value', 0.0)) if disk_bytes else None,
                    "data_points": disk_percentage.get('data_points', 0)
                })
            
            # Network utilization
            network_in = node_performance.get('node_network_in_bytes', {})
            network_out = node_performance.get('node_network_out_bytes', {})
            
            if network_in or network_out:
                network_in_value = self._safe_float_conversion(network_in.get('latest_value', 0.0)) if network_in else 0.0
                network_out_value = self._safe_float_conversion(network_out.get('latest_value', 0.0)) if network_out else 0.0
                
                utilization_summary["resource_utilization"]["network"].update({
                    "inbound_bytes_per_sec": network_in_value / 300 if network_in_value else 0.0,  # 5-min average
                    "outbound_bytes_per_sec": network_out_value / 300 if network_out_value else 0.0,
                    "total_throughput": (network_in_value + network_out_value) / 300 if (network_in_value or network_out_value) else 0.0,
                    "inbound_bytes_total": network_in_value,
                    "outbound_bytes_total": network_out_value
                })
            
            # Node Resource Allocation (from resource_allocation sub-section)
            resource_allocation = node_performance.get('resource_allocation', {})
            if resource_allocation:
                allocatable_cpu = resource_allocation.get('kube_node_status_allocatable_cpu_cores', {})
                allocatable_memory = resource_allocation.get('kube_node_status_allocatable_memory_bytes', {})
                node_condition = resource_allocation.get('kube_node_status_condition', {})
                
                utilization_summary["resource_utilization"]["node_allocation"] = {
                    "allocatable_cpu_cores": self._safe_float_conversion(allocatable_cpu.get('latest_value', 0.0)) if allocatable_cpu else 0.0,
                    "allocatable_memory_bytes": self._safe_float_conversion(allocatable_memory.get('latest_value', 0.0)) if allocatable_memory else 0.0,
                    "node_condition_status": self._safe_float_conversion(node_condition.get('latest_value', 0.0)) if node_condition else 0.0
                }
        else:
            self.logger.warning("No node performance metrics available")
        
        # 3. Control Plane Health (from Azure Monitor) - ALL CONTROL PLANE METRICS
        control_plane = enhanced_metrics.get('control_plane_health', {})
        if control_plane:
            self.logger.info(f"Processing control plane metrics: {list(control_plane.keys())}")
            
            # API Server metrics
            apiserver_cpu = control_plane.get('apiserver_cpu_usage_percentage', {})
            apiserver_memory = control_plane.get('apiserver_memory_usage_percentage', {})
            apiserver_requests = control_plane.get('apiserver_current_inflight_requests', {})
            
            if apiserver_cpu or apiserver_memory or apiserver_requests:
                utilization_summary["control_plane_health"]["apiserver"].update({
                    "cpu_usage": self._safe_float_conversion(apiserver_cpu.get('latest_value', 0.0)) if apiserver_cpu else 0.0,
                    "memory_usage": self._safe_float_conversion(apiserver_memory.get('latest_value', 0.0)) if apiserver_memory else 0.0,
                    "inflight_requests": self._safe_float_conversion(apiserver_requests.get('latest_value', 0.0)) if apiserver_requests else 0.0,
                    "status": self._get_resource_status(
                        max(
                            self._safe_float_conversion(apiserver_cpu.get('latest_value', 0.0)) if apiserver_cpu else 0.0,
                            self._safe_float_conversion(apiserver_memory.get('latest_value', 0.0)) if apiserver_memory else 0.0
                        ), 
                        [50, 80]
                    )
                })
            
            # etcd metrics
            etcd_cpu = control_plane.get('etcd_cpu_usage_percentage', {})
            etcd_memory = control_plane.get('etcd_memory_usage_percentage', {})
            etcd_db = control_plane.get('etcd_database_usage_percentage', {})
            
            if etcd_cpu or etcd_memory or etcd_db:
                utilization_summary["control_plane_health"]["etcd"].update({
                    "cpu_usage": self._safe_float_conversion(etcd_cpu.get('latest_value', 0.0)) if etcd_cpu else 0.0,
                    "memory_usage": self._safe_float_conversion(etcd_memory.get('latest_value', 0.0)) if etcd_memory else 0.0,
                    "database_usage": self._safe_float_conversion(etcd_db.get('latest_value', 0.0)) if etcd_db else 0.0,
                    "status": self._get_resource_status(
                        max(
                            self._safe_float_conversion(etcd_cpu.get('latest_value', 0.0)) if etcd_cpu else 0.0,
                            self._safe_float_conversion(etcd_memory.get('latest_value', 0.0)) if etcd_memory else 0.0,
                            self._safe_float_conversion(etcd_db.get('latest_value', 0.0)) if etcd_db else 0.0
                        ), 
                        [50, 80]
                    )
                })
            
            # Cluster Autoscaler metrics (from autoscaler_metrics sub-section)
            autoscaler_metrics = control_plane.get('autoscaler_metrics', {})
            if autoscaler_metrics:
                safe_to_autoscale = autoscaler_metrics.get('cluster_autoscaler_cluster_safe_to_autoscale', {})
                scale_down_cooldown = autoscaler_metrics.get('cluster_autoscaler_scale_down_in_cooldown', {})
                unneeded_nodes = autoscaler_metrics.get('cluster_autoscaler_unneeded_nodes_count', {})
                unschedulable_pods = autoscaler_metrics.get('cluster_autoscaler_unschedulable_pods_count', {})
                
                utilization_summary["control_plane_health"]["autoscaler"] = {
                    "safe_to_autoscale": self._safe_float_conversion(safe_to_autoscale.get('latest_value', 0.0)) > 0 if safe_to_autoscale else False,
                    "scale_down_in_cooldown": self._safe_float_conversion(scale_down_cooldown.get('latest_value', 0.0)) > 0 if scale_down_cooldown else False,
                    "unneeded_nodes_count": self._safe_float_conversion(unneeded_nodes.get('latest_value', 0.0)) if unneeded_nodes else 0.0,
                    "unschedulable_pods_count": self._safe_float_conversion(unschedulable_pods.get('latest_value', 0.0)) if unschedulable_pods else 0.0,
                    "status": "healthy" if (self._safe_float_conversion(safe_to_autoscale.get('latest_value', 0.0)) > 0 if safe_to_autoscale else False) else "warning"
                }
            
            # Overall control plane status
            api_status = utilization_summary["control_plane_health"]["apiserver"]["status"]
            etcd_status = utilization_summary["control_plane_health"]["etcd"]["status"]
            
            if api_status == "critical" or etcd_status == "critical":
                overall_status = "critical"
            elif api_status == "warning" or etcd_status == "warning":
                overall_status = "warning"
            elif api_status == "healthy" and etcd_status == "healthy":
                overall_status = "healthy"
            else:
                overall_status = "unknown"
                
            utilization_summary["control_plane_health"]["overall_status"] = overall_status
        else:
            self.logger.warning("No control plane metrics available")
        
        # 4. Workload Distribution (from Container Insights + Azure Pod Metrics) - FIXED
        workload_distribution = enhanced_metrics.get('workload_distribution', {})
        azure_pod_metrics = enhanced_metrics.get('azure_pod_metrics', {})

        self.logger.info(f"Processing workload distribution: type={type(workload_distribution)}, value={workload_distribution}")

        if workload_distribution and isinstance(workload_distribution, dict):
            # Check if it's an empty dict
            if not workload_distribution or workload_distribution.get('TotalPods', 0) == 0:
                self.logger.warning("Workload distribution is empty or has no pods")
                utilization_summary["workload_distribution"].update({
                    "total_pods": 0,
                    "running_pods": 0,
                    "pending_pods": 0,
                    "failed_pods": 0,
                    "succeeded_pods": 0,
                    "running_percentage": 0.0,
                    "source": "no_data",
                    "status": "no_data_available"
                })
            else:
                # Handle valid workload distribution structure
                total_pods = self._safe_int_conversion(workload_distribution.get('TotalPods', 0))
                running_pods = self._safe_int_conversion(workload_distribution.get('RunningPods', 0))
                pending_pods = self._safe_int_conversion(workload_distribution.get('PendingPods', 0))
                failed_pods = self._safe_int_conversion(workload_distribution.get('FailedPods', 0))
                succeeded_pods = self._safe_int_conversion(workload_distribution.get('SucceededPods', 0))
                
                running_percentage = (running_pods / total_pods * 100) if total_pods > 0 else 0.0
                
                utilization_summary["workload_distribution"].update({
                    "total_pods": total_pods,
                    "running_pods": running_pods,
                    "pending_pods": pending_pods,
                    "failed_pods": failed_pods,
                    "succeeded_pods": succeeded_pods,
                    "running_percentage": running_percentage,
                    "source": workload_distribution.get('source', 'container_insights'),
                    "status": "success"
                })
                
                # Handle namespace breakdown if available
                namespace_breakdown = workload_distribution.get('namespace_breakdown', [])
                if namespace_breakdown and isinstance(namespace_breakdown, list) and len(namespace_breakdown) > 0:
                    utilization_summary["workload_distribution"]["namespace_breakdown"] = [
                        {
                            "namespace": ns.get('Namespace', 'unknown'),
                            "total_pods": self._safe_int_conversion(ns.get('TotalPods', 0)),
                            "running_pods": self._safe_int_conversion(ns.get('RunningPods', 0)),
                            "pending_pods": self._safe_int_conversion(ns.get('PendingPods', 0)),
                            "failed_pods": self._safe_int_conversion(ns.get('FailedPods', 0)),
                            "succeeded_pods": self._safe_int_conversion(ns.get('SucceededPods', 0))
                        }
                        for ns in namespace_breakdown
                    ]
                    self.logger.info(f"Namespace breakdown processed: {len(namespace_breakdown)} namespaces")
                
                self.logger.info(f"Workload distribution processed successfully: {total_pods} total pods, {running_percentage:.1f}% running")
        else:
            self.logger.warning(f"Workload distribution data invalid: type={type(workload_distribution)}, value={workload_distribution}")
            utilization_summary["workload_distribution"].update({
                "total_pods": 0,
                "running_pods": 0,
                "pending_pods": 0,
                "failed_pods": 0,
                "succeeded_pods": 0,
                "running_percentage": 0.0,
                "source": "invalid_data",
                "status": "collection_failed"
            })
        
        # Add Azure Pod Metrics (from Azure Monitor)
        if azure_pod_metrics:
            pod_ready_metric = azure_pod_metrics.get('kube_pod_status_ready', {})
            pod_phase_metric = azure_pod_metrics.get('kube_pod_status_phase', {})
            
            utilization_summary["workload_distribution"]["azure_pod_metrics"] = {
                "ready_pods_count": self._safe_float_conversion(pod_ready_metric.get('latest_value', 0.0)) if pod_ready_metric else 0.0,
                "pod_phase_status": self._safe_float_conversion(pod_phase_metric.get('latest_value', 0.0)) if pod_phase_metric else 0.0,
                "ready_pods_average": self._safe_float_conversion(pod_ready_metric.get('average_value', 0.0)) if pod_ready_metric else 0.0,
                "data_points_ready": pod_ready_metric.get('data_points', 0) if pod_ready_metric else 0,
                "data_points_phase": pod_phase_metric.get('data_points', 0) if pod_phase_metric else 0
            }
            self.logger.info(f"Azure pod metrics processed: {utilization_summary['workload_distribution']['azure_pod_metrics']}")
        
        # 5. Combined Analysis (from enhanced metrics) - FIXED
        combined_analysis = enhanced_metrics.get('combined_analysis', {})
        if combined_analysis:
            self.logger.info(f"Processing combined analysis: {list(combined_analysis.keys())}")
            
            utilization_summary["combined_analysis"].update({
                "health_score": self._safe_float_conversion(combined_analysis.get('cluster_health_score', 0.0)),
                "data_consistency_score": self._safe_float_conversion(combined_analysis.get('data_consistency_score', 0.0)),
                "recommendations": combined_analysis.get('recommendations', [])
            })
            
            # Data source status
            data_validation = combined_analysis.get('data_source_validation', {})
            if data_validation:
                utilization_summary["combined_analysis"]["data_source_status"] = {
                    "azure_monitor_available": data_validation.get('azure_monitor_available', False),
                    "container_insights_available": data_validation.get('container_insights_available', False),
                    "azure_metrics_collected": data_validation.get('azure_metrics_collected', 0),
                    "azure_data_points": data_validation.get('azure_data_points', 0),
                    "ci_data_points": data_validation.get('ci_data_points', 0),
                    "primary_source": data_validation.get('primary_source', 'unknown'),
                    "data_coverage_percentage": self._safe_float_conversion(data_validation.get('data_coverage_percentage', 0.0))
                }
            
            # Add validation data if available
            other_validation = combined_analysis.get('data_validation', {})
            if other_validation:
                utilization_summary["combined_analysis"]["cross_validation"] = other_validation
                
            self.logger.info(f"Combined analysis processed: health_score={utilization_summary['combined_analysis']['health_score']}")
        else:
            self.logger.warning("No combined analysis available")
        
        # 6. Node Pool Breakdown (calculated from node_pools)
        for pool in node_pools:
            if not isinstance(pool, dict):
                continue
                
            vm_size = pool.get("vm_size", "")
            count = self._safe_int_conversion(pool.get("count", 0))
            vcores, memory_gb = self._estimate_vm_resources(vm_size)
            
            utilization_summary["cluster_overview"]["total_vcores"] += vcores * count
            utilization_summary["cluster_overview"]["total_memory_gb"] += memory_gb * count
            
            utilization_summary["node_pool_breakdown"].append({
                "name": pool.get("name", "unknown"),
                "vm_size": vm_size,
                "node_count": count,
                "auto_scaling_enabled": pool.get("auto_scaling_enabled", False),
                "total_vcores": vcores * count,
                "total_memory_gb": memory_gb * count,
                "mode": pool.get("mode", "User"),
                "provisioning_state": pool.get("provisioning_state", "Unknown")
            })
        
        # 7. Metrics Collection Status (comprehensive for ALL metrics)
        metadata = enhanced_metrics.get('collection_metadata', {})
        utilization_summary["metrics_collection_status"].update({
            "total_data_points": self._safe_int_conversion(metadata.get('total_data_points', 0)),
            "successful_sources": self._safe_int_conversion(metadata.get('successful_sources', 0)),
            "available_metrics": self._get_available_metrics_list(enhanced_metrics)
        })
        
        # Add detailed metrics breakdown
        azure_source = enhanced_metrics.get('data_sources', {}).get('azure_monitor', {})
        ci_source = enhanced_metrics.get('data_sources', {}).get('container_insights', {})
        k8s_source = enhanced_metrics.get('data_sources', {}).get('kubernetes_api', {})
        
        utilization_summary["metrics_collection_status"]["detailed_breakdown"] = {
            "azure_monitor": {
                "status": azure_source.get('status', 'unknown'),
                "metrics_collected": len(azure_source.get('metrics_collected', [])),
                "data_points": self._safe_int_conversion(azure_source.get('data_points', 0)),
                "metric_categories": {
                    "node_performance": len([m for m in azure_source.get('metrics_collected', []) if m.startswith('node_')]),
                    "control_plane": len([m for m in azure_source.get('metrics_collected', []) if m.startswith(('apiserver_', 'etcd_'))]),
                    "autoscaler": len([m for m in azure_source.get('metrics_collected', []) if 'autoscaler' in m]),
                    "node_resources": len([m for m in azure_source.get('metrics_collected', []) if m.startswith('kube_node_')]),
                    "pod_metrics": len([m for m in azure_source.get('metrics_collected', []) if m.startswith('kube_pod_')])
                },
                "collection_errors": azure_source.get('collection_errors', [])
            },
            "container_insights": {
                "status": ci_source.get('status', 'unknown'),
                "queries_executed": len(ci_source.get('queries_executed', [])),
                "data_points": self._safe_int_conversion(ci_source.get('data_points', 0)),
                "collection_errors": ci_source.get('collection_errors', [])
            },
            "kubernetes_api": {
                "status": k8s_source.get('status', 'unknown'),
                "resources_discovered": len(k8s_source.get('resources_discovered', []))
            }
        }
        
        self.logger.info(f"Utilization summary completed: Azure Monitor: {utilization_summary['metrics_collection_status']['detailed_breakdown']['azure_monitor']['metrics_collected']} metrics, Container Insights: {utilization_summary['metrics_collection_status']['detailed_breakdown']['container_insights']['data_points']} data points")
        
        return utilization_summary
    
    # ===== HELPER METHODS (Complete implementations) =====
    
    def _safe_float_conversion(self, value) -> float:
        """Safely convert value to float."""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            self.logger.debug(f"Failed to convert {value} to float, returning 0.0")
            return 0.0
    
    def _safe_int_conversion(self, value) -> int:
        """Safely convert value to int."""
        if value is None:
            return 0
        try:
            return int(float(value))  # Convert through float first to handle string decimals
        except (ValueError, TypeError):
            self.logger.debug(f"Failed to convert {value} to int, returning 0")
            return 0
    
    def _get_resource_status(self, value: float, thresholds: List[float]) -> str:
        """Get resource status based on value and thresholds."""
        if value == 0.0:
            return "unknown"
        elif value < thresholds[0]:
            return "healthy"
        elif value < thresholds[1]:
            return "warning"
        else:
            return "critical"
    
    def _get_available_metrics_list(self, enhanced_metrics: Dict[str, Any]) -> List[str]:
        """Get list of available metrics from enhanced metrics structure."""
        available_metrics = []
        
        # Azure Monitor metrics
        node_performance = enhanced_metrics.get('node_performance', {})
        control_plane = enhanced_metrics.get('control_plane_health', {})
        
        available_metrics.extend(node_performance.keys())
        available_metrics.extend(control_plane.keys())
        
        # Container Insights metrics
        if enhanced_metrics.get('cluster_overview'):
            available_metrics.append('cluster_overview')
        if enhanced_metrics.get('workload_distribution'):
            available_metrics.append('workload_distribution')
        
        # Combined analysis
        if enhanced_metrics.get('combined_analysis'):
            available_metrics.append('combined_analysis')
        
        return available_metrics
    
    def _create_empty_enhanced_metrics_structure(self, cluster_name: str) -> Dict[str, Any]:
        """Create empty enhanced metrics structure when collection fails."""
        return {
            'cluster_name': cluster_name,
            'collection_period': {
                'start_time': (datetime.now(timezone.utc) - timedelta(hours=self.metrics_hours)).isoformat(),
                'end_time': datetime.now(timezone.utc).isoformat(),
                'hours': self.metrics_hours
            },
            'node_performance': {},
            'cluster_overview': {},
            'workload_distribution': {},
            'control_plane_health': {},
            'combined_analysis': {
                'cluster_health_score': 0.0,
                'data_consistency_score': 0.0,
                'recommendations': ['Enhanced metrics collection failed - check Azure Monitor configuration']
            },
            'data_sources': {
                'azure_monitor': {
                    'status': 'failed',
                    'metrics_collected': [],
                    'data_points': 0,
                    'collection_errors': ['Failed to collect Azure Monitor metrics']
                },
                'container_insights': {
                    'status': 'failed',
                    'queries_executed': [],
                    'data_points': 0,
                    'collection_errors': ['Failed to collect Container Insights metrics']
                },
                'kubernetes_api': {
                    'status': 'not_attempted',
                    'resources_discovered': [],
                    'collection_errors': []
                }
            },
            'collection_metadata': {
                'total_data_points': 0,
                'successful_sources': 0,
                'failed_sources': 3,
                'data_freshness_score': 0.0,
                'health_score': 0.0
            }
        }
    
    def _estimate_vm_resources(self, vm_size: str) -> tuple:
        """Estimate vCores and memory for VM size."""
        if not isinstance(vm_size, str):
            vm_size = ""
            
        # Azure VM size mappings (simplified)
        vm_specs = {
            "Standard_B2s": (2, 4),
            "Standard_B4ms": (4, 16),
            "Standard_D2s_v3": (2, 8),
            "Standard_D4s_v3": (4, 16),
            "Standard_D8s_v3": (8, 32),
            "Standard_D16s_v3": (16, 64),
            "Standard_E2s_v3": (2, 16),
            "Standard_E4s_v3": (4, 32),
            "Standard_E8s_v3": (8, 64),
            "Standard_F2s_v2": (2, 4),
            "Standard_F4s_v2": (4, 8),
            "Standard_F8s_v2": (8, 16)
        }
        
        return vm_specs.get(vm_size, (2, 8))  # Default to 2 cores, 8GB
    
    # ===== SIMPLIFIED COST COLLECTION (fixed) =====
    
    async def _get_resource_group_costs_safe(self) -> Dict[str, Any]:
        """Get resource group costs with simplified error handling."""
        # Default cost structure to return if everything fails
        default_cost_structure = {
            "cost_summary": {
                "total_cost": 0.0,
                "currency": "USD",
                "analysis_period_days": self.cost_analysis_days
            },
            "overall_costs": {},
            "detailed_resource_costs": {},
            "top_cost_resources": [],
            "collection_status": "failed",
            "error_message": None
        }
        
        try:
            self.logger.info(f"Attempting to get resource group costs for: {self.resource_group}")
            
            # Try to get overall resource group costs
            rg_costs = None
            try:
                rg_costs = await self.cost_client.get_resource_group_costs(
                    self.resource_group, self.cost_analysis_days
                )
                self.logger.info(f"Resource group costs result type: {type(rg_costs)}")
                
                if rg_costs is None:
                    self.logger.warning("get_resource_group_costs returned None")
                    rg_costs = {}
                elif not isinstance(rg_costs, dict):
                    self.logger.warning(f"get_resource_group_costs returned unexpected type: {type(rg_costs)}")
                    rg_costs = {}
                    
            except Exception as e:
                self.logger.error(f"Failed to get resource group costs: {e}")
                rg_costs = {}
            
            # Build cost structure - simplified without detailed resource costs
            enhanced_costs = {
                "overall_costs": rg_costs if isinstance(rg_costs, dict) else {},
                "collection_status": "success" if rg_costs else "failed"
            }
            
            # Only extract cost summary for easy access
            if isinstance(rg_costs, dict):
                enhanced_costs["cost_summary"] = {
                    "total_cost": rg_costs.get("total_cost", 0.0) if rg_costs.get("total_cost") is not None else 0.0,
                    "currency": rg_costs.get("currency", "USD"),
                    "analysis_period_days": self.cost_analysis_days
                }
                enhanced_costs["collection_status"] = "success"
            else:
                enhanced_costs["cost_summary"] = default_cost_structure["cost_summary"]
            
            # Add empty placeholders for compatibility
            enhanced_costs["detailed_resource_costs"] = {}
            enhanced_costs["top_cost_resources"] = []
            
            self.logger.info(f"Cost collection completed with status: {enhanced_costs['collection_status']}")
            return enhanced_costs
            
        except Exception as e:
            self.logger.error(f"Failed to get resource group costs: {e}")
            default_cost_structure["error_message"] = str(e)
            return default_cost_structure