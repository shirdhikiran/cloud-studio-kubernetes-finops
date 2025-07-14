# src/finops/discovery/discovery_client.py
"""Fixed discovery client with no compilation errors."""

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
    """Fixed discovery client with enhanced comprehensive metrics."""
    
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
        """Discover comprehensive data with enhanced metrics integration."""
        if not self._connected:
            raise DiscoveryException("DiscoveryClient", "Client not connected")
            
        self.logger.info(f"Starting comprehensive discovery for: {self.resource_group}")
        
        discovery_data = {
            "discovery_metadata": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "resource_group": self.resource_group,
                "subscription_id": self.subscription_id,
                "cost_analysis_days": self.cost_analysis_days,
                "metrics_hours": self.metrics_hours,
                "discovery_version": "2.0.0"
            },
            "resource_group_costs": {},
            "clusters": [],
            "summary": {
                "total_clusters": 0,
                "total_nodes": 0,
                "total_pods": 0,
                "total_cost": 0.0,
                "currency": "USD",
                "metrics_collection_status": "pending"
            }
        }
        
        try:
            # Get resource group costs
            discovery_data["resource_group_costs"] = await self._get_resource_group_costs_safe()
            
            # Discover clusters
            clusters = await self.aks_client.discover_clusters(self.resource_group)
            discovery_data["summary"]["total_clusters"] = len(clusters)
            
            # Process each cluster with enhanced metrics
            for cluster in clusters:
                cluster_data = await self._discover_cluster_with_enhanced_metrics(cluster)
                discovery_data["clusters"].append(cluster_data)
                
                # Update summary
                discovery_data["summary"]["total_nodes"] += cluster_data.get("node_count", 0)
                discovery_data["summary"]["total_pods"] += cluster_data.get("pod_count", 0)
            
            # Calculate total cost from resource group costs
            rg_costs = discovery_data["resource_group_costs"]
            discovery_data["summary"]["total_cost"] = rg_costs.get("cost_summary", {}).get("total_cost", 0.0)
            
            # Update metrics collection status
            if discovery_data["clusters"]:
                successful_metrics = sum(
                    1 for cluster in discovery_data["clusters"] 
                    if cluster.get("comprehensive_metrics", {}).get("collection_summary", {}).get("successful_sources", 0) > 0
                )
                discovery_data["summary"]["metrics_collection_status"] = (
                    "success" if successful_metrics > 0 else "failed"
                )
            
            self.logger.info(
                f"Comprehensive discovery completed",
                clusters=len(clusters),
                total_cost=discovery_data["summary"]["total_cost"],
                metrics_status=discovery_data["summary"]["metrics_collection_status"]
            )
            
            return discovery_data
            
        except Exception as e:
            self.logger.error(f"Comprehensive discovery failed: {e}")
            raise DiscoveryException("DiscoveryClient", f"Discovery failed: {e}")
    
    async def _discover_cluster_with_enhanced_metrics(self, cluster: Dict[str, Any]) -> Dict[str, Any]:
        """Discover cluster data with enhanced metrics integration."""
        cluster_name = cluster["name"]
        resource_group = cluster["resource_group"]
        cluster_id = cluster["id"]
        
        self.logger.info(f"Processing cluster with enhanced metrics: {cluster_name}")
        
        cluster_data = {
            "cluster_info": cluster,
            "node_pools": [],
            "kubernetes_resources": {},
            "comprehensive_metrics": {},  # NEW: All metrics from both sources
            "utilization_summary": {},    # UPDATED: Enhanced utilization summary
            "node_count": 0,
            "pod_count": 0,
            "discovery_status": "in_progress"
        }
        
        try:
            # Discover node pools
            node_pools = await self.aks_client.discover_node_pools(cluster_name, resource_group)
            cluster_data["node_pools"] = node_pools if node_pools is not None else []
            cluster_data["node_count"] = sum(pool.get("count", 0) for pool in cluster_data["node_pools"] if isinstance(pool, dict))
            
            # NEW: Get comprehensive metrics using the updated MonitorClient
            self.logger.info(f"Collecting comprehensive metrics for: {cluster_name}")
            
            try:
                comprehensive_metrics = await self.monitor_client.get_enhanced_cluster_metrics(
                    cluster_id, cluster_name, self.metrics_hours
                )
                
                if comprehensive_metrics:
                    cluster_data["comprehensive_metrics"] = comprehensive_metrics
                    
                    # Log metrics collection summary
                    collection_summary = comprehensive_metrics.get("collection_summary", {})
                    self.logger.info(
                        f"Comprehensive metrics collected for {cluster_name}",
                        successful_sources=collection_summary.get("successful_sources", 0),
                        total_metrics=collection_summary.get("total_metrics_collected", 0),
                        total_data_points=collection_summary.get("total_data_points", 0)
                    )
                    
                    # Create enhanced utilization summary
                    cluster_data["utilization_summary"] = self._create_enhanced_utilization_summary(
                        comprehensive_metrics, cluster_data["node_pools"]
                    )
                else:
                    self.logger.warning(f"No comprehensive metrics collected for {cluster_name}")
                    cluster_data["comprehensive_metrics"] = self._create_empty_comprehensive_metrics(cluster_name)
                    
            except Exception as e:
                self.logger.error(f"Comprehensive metrics collection failed for {cluster_name}: {e}")
                cluster_data["comprehensive_metrics"] = self._create_empty_comprehensive_metrics(cluster_name)
            
            # Discover Kubernetes resources
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
                        
                        # Integrate Kubernetes data with comprehensive metrics
                        self._integrate_kubernetes_data_with_comprehensive_metrics(
                            cluster_data["comprehensive_metrics"], 
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
    
    def _integrate_kubernetes_data_with_comprehensive_metrics(self, comprehensive_metrics: Dict[str, Any], 
                                                           k8s_resources: Dict[str, Any]) -> None:
        """Integrate Kubernetes API data with comprehensive metrics."""
        if not k8s_resources or not isinstance(comprehensive_metrics, dict):
            return
        
        # Add Kubernetes data to Container Insights source
        metrics_by_source = comprehensive_metrics.get('metrics_by_source', {})
        ci_source = metrics_by_source.get('container_insights', {})
        
        if ci_source:
            # Add Kubernetes API data section
            ci_source['kubernetes_api_data'] = {
                'source_description': 'Direct Kubernetes API integration',
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'resource_counts': {
                    'namespaces': len(k8s_resources.get('namespaces', [])),
                    'nodes': len(k8s_resources.get('nodes', [])),
                    'pods': len(k8s_resources.get('pods', [])),
                    'deployments': len(k8s_resources.get('deployments', [])),
                    'services': len(k8s_resources.get('services', [])),
                    'persistent_volumes': len(k8s_resources.get('persistent_volumes', [])),
                    'ingresses': len(k8s_resources.get('ingresses', []))
                },
                'detailed_resources': k8s_resources
            }
            
            # Update collection metadata
            ci_metadata = ci_source.get('collection_metadata', {})
            ci_metadata['kubernetes_api_integrated'] = True
            ci_metadata['total_data_points'] = ci_metadata.get('total_data_points', 0) + len(k8s_resources)
        
        # Add cross-validation data to combined analysis
        combined_analysis = comprehensive_metrics.get('combined_analysis', {})
        cross_validation = combined_analysis.get('cross_validation', {})
        
        # Cross-validate pod counts between different sources
        k8s_pod_count = len(k8s_resources.get('pods', []))
        ci_pod_data = ci_source.get('insights_data', {}).get('pod_count_status', {}).get('data', {})
        
        if k8s_pod_count > 0 and ci_pod_data:
            ci_pod_count = ci_pod_data.get('TotalPods', 0)
            
            cross_validation['pod_count_validation'] = {
                'kubernetes_api_pod_count': k8s_pod_count,
                'container_insights_pod_count': ci_pod_count,
                'variance': abs(k8s_pod_count - ci_pod_count),
                'variance_percentage': (abs(k8s_pod_count - ci_pod_count) / max(k8s_pod_count, ci_pod_count, 1)) * 100,
                'consistency_status': 'good' if abs(k8s_pod_count - ci_pod_count) <= 2 else 'warning'
            }
        
        combined_analysis['cross_validation'] = cross_validation
        
        self.logger.debug("Integrated Kubernetes API data with comprehensive metrics")
    
    def _create_enhanced_utilization_summary(self, comprehensive_metrics: Dict[str, Any], 
                                           node_pools: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create enhanced utilization summary from comprehensive metrics."""
        # Extract data from new comprehensive metrics structure
        metrics_by_source = comprehensive_metrics.get('metrics_by_source', {})
        azure_source = metrics_by_source.get('azure_monitor', {})
        ci_source = metrics_by_source.get('container_insights', {})
        
        # Enhanced structure with source attribution
        enhanced_summary = {
            "data_sources": {
                "azure_monitor": {
                    "status": azure_source.get('status', 'unknown'),
                    "metrics_collected": azure_source.get('collection_metadata', {}).get('successful_metrics', 0),
                    "total_data_points": azure_source.get('collection_metadata', {}).get('total_data_points', 0)
                },
                "container_insights": {
                    "status": ci_source.get('status', 'unknown'),
                    "queries_successful": ci_source.get('collection_metadata', {}).get('successful_queries', 0),
                    "total_data_points": ci_source.get('collection_metadata', {}).get('total_data_points', 0)
                }
            },
            "cluster_overview": self._extract_cluster_overview_from_sources(azure_source, ci_source),
            "resource_utilization": self._extract_resource_utilization_from_azure(azure_source),
            "workload_distribution": self._extract_workload_distribution_from_ci(ci_source),
            "node_pool_breakdown": self._create_node_pool_breakdown(node_pools),
            "combined_insights": self._create_combined_insights(comprehensive_metrics)
        }
        
        return enhanced_summary
    
    def _extract_cluster_overview_from_sources(self, azure_source: Dict[str, Any], 
                                             ci_source: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster overview from both sources."""
        overview = {
            "source": "container_insights_primary",
            "node_count": 0,
            "ready_nodes": 0,
            "health_percentage": 0.0,
            "status": "unknown"
        }
        
        # Primary: Container Insights node data
        if ci_source.get('status') == 'success':
            ci_data = ci_source.get('insights_data', {})
            node_data = ci_data.get('node_count_status', {}).get('data', {})
            
            if node_data and isinstance(node_data, dict):
                overview.update({
                    "node_count": self._safe_int_conversion(node_data.get('TotalNodes', 0)),
                    "ready_nodes": self._safe_int_conversion(node_data.get('ReadyNodes', 0)),
                    "health_percentage": self._safe_float_conversion(node_data.get('HealthPercentage', 0.0)),
                    "status": "success"
                })
        
        # Fallback: Azure Monitor node metrics count
        if overview["node_count"] == 0 and azure_source.get('status') == 'success':
            azure_metrics = azure_source.get('metrics', {})
            node_metrics = [name for name in azure_metrics.keys() if 'node_' in name]
            
            if node_metrics:
                overview.update({
                    "source": "azure_monitor_fallback",
                    "node_count": len(node_metrics),  # Rough estimate
                    "ready_nodes": len(node_metrics),
                    "health_percentage": 100.0,
                    "status": "estimated"
                })
        
        return overview
    
    def _extract_resource_utilization_from_azure(self, azure_source: Dict[str, Any]) -> Dict[str, Any]:
        """Extract resource utilization from Azure Monitor."""
        utilization = {
            "source": "azure_monitor",
            "cpu": {"latest_percentage": 0.0, "average_percentage": 0.0, "status": "unknown"},
            "memory": {"latest_percentage": 0.0, "average_percentage": 0.0, "status": "unknown"},
            "disk": {"latest_percentage": 0.0, "average_percentage": 0.0, "status": "unknown"},
            "network": {"inbound_bytes_per_sec": 0.0, "outbound_bytes_per_sec": 0.0},
            "status": "unknown"
        }
        
        if azure_source.get('status') == 'success':
            azure_metrics = azure_source.get('metrics', {})
            
            # CPU utilization
            cpu_metric = azure_metrics.get('node_cpu_usage_percentage', {})
            if cpu_metric:
                aggregated = cpu_metric.get('aggregated_values', {})
                utilization["cpu"] = {
                    "latest_percentage": self._safe_float_conversion(aggregated.get('latest_average', 0.0)),
                    "average_percentage": self._safe_float_conversion(aggregated.get('overall_average', 0.0)),
                    "status": self._get_resource_status(
                        self._safe_float_conversion(aggregated.get('overall_average', 0.0)), [60, 80]
                    )
                }
            
            # Memory utilization
            memory_metric = azure_metrics.get('node_memory_working_set_percentage', {})
            if memory_metric:
                aggregated = memory_metric.get('aggregated_values', {})
                utilization["memory"] = {
                    "latest_percentage": self._safe_float_conversion(aggregated.get('latest_average', 0.0)),
                    "average_percentage": self._safe_float_conversion(aggregated.get('overall_average', 0.0)),
                    "status": self._get_resource_status(
                        self._safe_float_conversion(aggregated.get('overall_average', 0.0)), [70, 85]
                    )
                }
            
            # Disk utilization
            disk_metric = azure_metrics.get('node_disk_usage_percentage', {})
            if disk_metric:
                aggregated = disk_metric.get('aggregated_values', {})
                utilization["disk"] = {
                    "latest_percentage": self._safe_float_conversion(aggregated.get('latest_average', 0.0)),
                    "average_percentage": self._safe_float_conversion(aggregated.get('overall_average', 0.0)),
                    "status": self._get_resource_status(
                        self._safe_float_conversion(aggregated.get('overall_average', 0.0)), [75, 90]
                    )
                }
            
            # Network utilization
            network_in = azure_metrics.get('node_network_in_bytes', {})
            network_out = azure_metrics.get('node_network_out_bytes', {})
            
            if network_in or network_out:
                utilization["network"] = {
                    "inbound_bytes_per_sec": self._safe_float_conversion(
                        network_in.get('aggregated_values', {}).get('latest_average', 0.0)
                    ) / 300 if network_in else 0.0,  # 5-min average
                    "outbound_bytes_per_sec": self._safe_float_conversion(
                        network_out.get('aggregated_values', {}).get('latest_average', 0.0)
                    ) / 300 if network_out else 0.0
                }
            
            utilization["status"] = "success"
        
        return utilization
    
    def _extract_workload_distribution_from_ci(self, ci_source: Dict[str, Any]) -> Dict[str, Any]:
        """Extract workload distribution from Container Insights."""
        workload = {
            "source": "container_insights",
            "total_pods": 0,
            "running_pods": 0,
            "pending_pods": 0,
            "failed_pods": 0,
            "running_percentage": 0.0,
            "namespace_breakdown": [],
            "status": "unknown"
        }
        
        if ci_source.get('status') == 'success':
            ci_data = ci_source.get('insights_data', {})
            
            # Pod status data
            pod_data = ci_data.get('pod_count_status', {}).get('data', {})
            if pod_data and isinstance(pod_data, dict):
                workload.update({
                    "total_pods": self._safe_int_conversion(pod_data.get('TotalPods', 0)),
                    "running_pods": self._safe_int_conversion(pod_data.get('RunningPods', 0)),
                    "pending_pods": self._safe_int_conversion(pod_data.get('PendingPods', 0)),
                    "failed_pods": self._safe_int_conversion(pod_data.get('FailedPods', 0)),
                    "running_percentage": self._safe_float_conversion(pod_data.get('RunningPercentage', 0.0)),
                    "status": "success"
                })
            
            # Namespace breakdown
            namespace_data = ci_data.get('namespace_breakdown', {}).get('data', [])
            if namespace_data and isinstance(namespace_data, list):
                workload["namespace_breakdown"] = [
                    {
                        "namespace": ns.get('Namespace', 'unknown'),
                        "total_pods": self._safe_int_conversion(ns.get('TotalPods', 0)),
                        "running_pods": self._safe_int_conversion(ns.get('RunningPods', 0))
                    }
                    for ns in namespace_data[:10]  # Top 10 namespaces
                ]
        
        return workload
    
    def _create_node_pool_breakdown(self, node_pools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create node pool breakdown with capacity calculations."""
        breakdown = []
        
        for pool in node_pools:
            if isinstance(pool, dict):
                vm_size = pool.get("vm_size", "")
                count = self._safe_int_conversion(pool.get("count", 0))
                vcores, memory_gb = self._estimate_vm_resources(vm_size)
                
                breakdown.append({
                    "name": pool.get("name", "unknown"),
                    "vm_size": vm_size,
                    "node_count": count,
                    "auto_scaling_enabled": pool.get("auto_scaling_enabled", False),
                    "total_vcores": vcores * count,
                    "total_memory_gb": memory_gb * count,
                    "mode": pool.get("mode", "User"),
                    "provisioning_state": pool.get("provisioning_state", "Unknown")
                })
        
        return breakdown
    
    def _create_combined_insights(self, comprehensive_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Create combined insights from comprehensive metrics."""
        combined_analysis = comprehensive_metrics.get('combined_analysis', {})
        collection_summary = comprehensive_metrics.get('collection_summary', {})
        
        return {
            "health_score": self._safe_float_conversion(combined_analysis.get('health_score', 0.0)),
            "data_consistency_score": self._safe_float_conversion(combined_analysis.get('data_consistency_score', 0.0)),
            "recommendations": combined_analysis.get('recommendations', []),
            "successful_sources": self._safe_int_conversion(collection_summary.get('successful_sources', 0)),
            "total_metrics_collected": self._safe_int_conversion(collection_summary.get('total_metrics_collected', 0)),
            "total_data_points": self._safe_int_conversion(collection_summary.get('total_data_points', 0)),
            "cross_validation": combined_analysis.get('cross_validation', {})
        }
    
    def _create_empty_comprehensive_metrics(self, cluster_name: str) -> Dict[str, Any]:
        """Create empty comprehensive metrics structure when collection fails."""
        return {
            'cluster_name': cluster_name,
            'collection_period': {
                'start_time': (datetime.now(timezone.utc) - timedelta(hours=self.metrics_hours)).isoformat(),
                'end_time': datetime.now(timezone.utc).isoformat(),
                'hours': self.metrics_hours
            },
            'metrics_by_source': {
                'azure_monitor': {
                    'source_name': 'Azure Monitor',
                    'source_type': 'metrics_api',
                    'status': 'failed',
                    'metrics': {},
                    'collection_metadata': {
                        'total_metrics_attempted': 25,
                        'successful_metrics': 0,
                        'failed_metrics': 25,
                        'total_data_points': 0,
                        'collection_errors': ['Collection failed - check configuration']
                    }
                },
                'container_insights': {
                    'source_name': 'Container Insights',
                    'source_type': 'log_analytics',
                    'status': 'failed',
                    'insights_data': {},
                    'collection_metadata': {
                        'queries_executed': 0,
                        'successful_queries': 0,
                        'failed_queries': 0,
                        'total_data_points': 0,
                        'collection_errors': ['Collection failed - check Log Analytics configuration']
                    }
                }
            },
            'combined_analysis': {
                'health_score': 0.0,
                'data_consistency_score': 0.0,
                'recommendations': ['Metrics collection failed - check Azure Monitor and Container Insights configuration'],
                'cross_validation': {}
            },
            'collection_summary': {
                'total_sources': 2,
                'successful_sources': 0,
                'total_metrics_collected': 0,
                'total_data_points': 0,
                'collection_duration_seconds': 0.0
            }
        }
    
    async def _get_resource_group_costs_safe(self) -> Dict[str, Any]:
        """Get resource group costs with error handling."""
        default_cost_structure = {
            "cost_summary": {
                "total_cost": 0.0,
                "currency": "USD",
                "analysis_period_days": self.cost_analysis_days
            },
            "overall_costs": {},
            "collection_status": "failed",
            "error_message": None
        }
        
        try:
            self.logger.info(f"Getting resource group costs for: {self.resource_group}")
            
            rg_costs = await self.cost_client.get_resource_group_costs(
                self.resource_group, self.cost_analysis_days
            )
            
            if rg_costs and isinstance(rg_costs, dict):
                enhanced_costs = {
                    "cost_summary": {
                        "total_cost": rg_costs.get("total_cost", 0.0),
                        "currency": rg_costs.get("currency", "USD"),
                        "analysis_period_days": self.cost_analysis_days
                    },
                    "overall_costs": rg_costs,
                    "collection_status": "success"
                }
                return enhanced_costs
            else:
                return default_cost_structure
                
        except Exception as e:
            self.logger.error(f"Failed to get resource group costs: {e}")
            default_cost_structure["error_message"] = str(e)
            return default_cost_structure
    
    def _safe_float_conversion(self, value) -> float:
        """Safely convert value to float."""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def _safe_int_conversion(self, value) -> int:
        """Safely convert value to int."""
        if value is None:
            return 0
        try:
            return int(float(value))
        except (ValueError, TypeError):
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
    
    def _estimate_vm_resources(self, vm_size: str) -> tuple:
        """Estimate vCores and memory for VM size."""
        if not isinstance(vm_size, str):
            vm_size = ""
            
        # Azure VM size mappings
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