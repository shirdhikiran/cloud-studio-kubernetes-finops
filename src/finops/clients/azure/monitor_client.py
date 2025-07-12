# src/finops/clients/azure/monitor_client.py
"""Fixed Azure Monitor client with proper error handling and debugging."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
import structlog
import asyncio
from azure.mgmt.monitor import MonitorManagementClient
from azure.monitor.query import MetricsQueryClient, LogsQueryClient, MetricAggregationType
from azure.core.exceptions import AzureError, ResourceNotFoundError, ClientAuthenticationError

from finops.core.base_client import BaseClient
from finops.core.exceptions import ClientConnectionException, MetricsException
from finops.core.utils import retry_with_backoff

logger = structlog.get_logger(__name__)


class MonitorClient(BaseClient):
    """Fixed Azure Monitor client for metrics collection."""
    
    def __init__(self, credential, subscription_id: str, config: Dict[str, Any]):
        super().__init__(config, "MonitorClient")
        self.credential = credential
        self.subscription_id = subscription_id
        self.log_analytics_workspace_id = config.get("log_analytics_workspace_id")
        
        self._monitor_client = None
        self._metrics_client = None
        self._logs_client = None
    
    async def connect(self) -> None:
        """Connect to Azure Monitor services."""
        try:
            self._monitor_client = MonitorManagementClient(
                credential=self.credential,
                subscription_id=self.subscription_id
            )
            
            self._metrics_client = MetricsQueryClient(credential=self.credential)
            
            if self.log_analytics_workspace_id:
                self._logs_client = LogsQueryClient(credential=self.credential)
                self.logger.info(f"Log Analytics client initialized with workspace: {self.log_analytics_workspace_id}")
            else:
                self.logger.warning("Log Analytics workspace ID not provided - Container Insights will not be available")
            
            self._connected = True
            self.logger.info("Azure Monitor clients connected successfully")
            
        except ClientAuthenticationError as e:
            raise ClientConnectionException("AzureMonitor", f"Authentication failed: {e}")
        except Exception as e:
            raise ClientConnectionException("AzureMonitor", f"Connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from Azure Monitor services."""
        clients_to_close = [self._monitor_client, self._metrics_client, self._logs_client]
        
        for client in clients_to_close:
            if client and hasattr(client, 'close'):
                try:
                    client.close()
                except Exception as e:
                    self.logger.warning(f"Error closing client: {e}")
        
        self._connected = False
        self.logger.info("Azure Monitor clients disconnected")
    
    async def health_check(self) -> bool:
        """Check Azure Monitor client health."""
        try:
            if not self._connected or not self._metrics_client:
                return False
            
            # Simple test to verify connectivity
            test_resource_id = f"/subscriptions/{self.subscription_id}"
            try:
                # Try to list metric definitions (this should work even if resource doesn't have metrics)
                list(self._monitor_client.metric_definitions.list(test_resource_id))
            except ResourceNotFoundError:
                pass  # Expected for subscription level
            except ClientAuthenticationError:
                return False
            
            return True
            
        except Exception as e:
            self.logger.warning("Azure Monitor health check failed", error=str(e))
            return False
    
    @retry_with_backoff(max_retries=3)
    async def get_enhanced_cluster_metrics(self, cluster_resource_id: str, cluster_name: str, 
                                         hours: int = 24) -> Dict[str, Any]:
        """Get enhanced metrics for cluster with better error handling and debugging."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours)
        
        self.logger.info(f"Collecting enhanced metrics for cluster: {cluster_name}")
        self.logger.info(f"Time range: {start_time} to {end_time}")
        self.logger.info(f"Cluster resource ID: {cluster_resource_id}")
        
        metrics_data = {
            'cluster_name': cluster_name,
            'cluster_resource_id': cluster_resource_id,
            'collection_period': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'hours': hours
            },
            'cluster_metrics': {},
            'node_metrics': {},
            'container_insights_metrics': {},
            'performance_counters': {},
            'available_metrics': [],
            'utilization_summary': {},
            'health_status': {
                'overall_status': 'healthy',
                'issues_detected': [],
                'recommendations': []
            },
            'collection_metadata': {
                'metrics_collected': [],
                'metrics_failed': [],
                'total_data_points': 0,
                'data_sources': ['azure_monitor'],
                'collection_errors': []
            }
        }
        
        try:
            # 0. First discover what metrics are available
            self.logger.info("Discovering available metrics for cluster...")
            available_metrics = await self._discover_available_metrics(cluster_resource_id)
            metrics_data['available_metrics'] = available_metrics
            self.logger.info(f"Available metrics: {available_metrics}")
            
            # 1. Collect cluster-level metrics from Azure Monitor
            self.logger.info("Attempting to collect cluster-level metrics...")
            cluster_metrics = await self._collect_cluster_metrics_safe(
                cluster_resource_id, start_time, end_time
            )
            metrics_data['cluster_metrics'] = cluster_metrics
            
            # Count data points from cluster metrics
            cluster_data_points = 0
            for metric_name, metric_data in cluster_metrics.items():
                if isinstance(metric_data, list):
                    cluster_data_points += len(metric_data)
            
            self.logger.info(f"Cluster metrics collected: {len(cluster_metrics)} metrics, {cluster_data_points} data points")
            metrics_data['collection_metadata']['total_data_points'] += cluster_data_points
            
            if cluster_metrics:
                metrics_data['collection_metadata']['metrics_collected'].extend(cluster_metrics.keys())
            
            # 2. Collect Container Insights metrics if Log Analytics is available
            if self._logs_client and self.log_analytics_workspace_id:
                self.logger.info("Attempting to collect Container Insights metrics...")
                try:
                    container_insights = await self._collect_container_insights_safe(
                        cluster_name, start_time, end_time
                    )
                    metrics_data['container_insights_metrics'] = container_insights
                    metrics_data['collection_metadata']['data_sources'].append('container_insights')
                    
                    # Count container insights data points
                    ci_data_points = container_insights.get('data_points', 0)
                    metrics_data['collection_metadata']['total_data_points'] += ci_data_points
                    self.logger.info(f"Container Insights collected: {ci_data_points} data points")
                    
                except Exception as e:
                    self.logger.error(f"Container Insights collection failed: {e}")
                    metrics_data['collection_metadata']['collection_errors'].append(f"Container Insights: {str(e)}")
            else:
                self.logger.warning("Container Insights not available - Log Analytics not configured")
                metrics_data['collection_metadata']['collection_errors'].append("Log Analytics workspace not configured")
            
            # 3. Generate utilization summary
            metrics_data['utilization_summary'] = self._generate_utilization_summary(metrics_data)
            
            # 4. Analyze health status
            metrics_data['health_status'] = self._analyze_health_status(metrics_data)
            
            total_data_points = metrics_data['collection_metadata']['total_data_points']
            self.logger.info(f"Enhanced metrics collection completed for {cluster_name}: {total_data_points} total data points")
            
            if total_data_points == 0:
                self.logger.warning(f"No metrics data collected for {cluster_name} - check configuration and permissions")
                metrics_data['collection_metadata']['collection_errors'].append("No actual metrics data available")
            
            return metrics_data
            
        except Exception as e:
            self.logger.error(f"Failed to collect enhanced metrics for {cluster_name}: {e}")
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return self._get_default_metrics_structure(cluster_name, str(e))
    
    async def _discover_available_metrics(self, cluster_resource_id: str) -> List[str]:
        """Discover what metrics are actually available for this cluster."""
        try:
            self.logger.debug(f"Discovering available metrics for: {cluster_resource_id}")
            
            metric_definitions = self._monitor_client.metric_definitions.list(cluster_resource_id)
            available_metrics = []
            
            for metric_def in metric_definitions:
                available_metrics.append(metric_def.name.value)
                self.logger.debug(f"Available metric: {metric_def.name.value}")
            
            self.logger.info(f"Found {len(available_metrics)} available metrics")
            return available_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to discover available metrics: {e}")
            return []
    
    async def _collect_cluster_metrics_safe(self, cluster_resource_id: str, 
                                          start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Collect ALL available AKS metrics for comprehensive discovery."""
        cluster_metrics = {}
        
        # ALL available AKS metrics from your cluster - organized by category
        all_metrics = {
            # API Server Metrics
            "apiserver_cpu_usage_percentage",
            "apiserver_memory_usage_percentage", 
            "apiserver_current_inflight_requests",
            
            # Cluster Autoscaler Metrics
            "cluster_autoscaler_cluster_safe_to_autoscale",
            "cluster_autoscaler_scale_down_in_cooldown",
            "cluster_autoscaler_unneeded_nodes_count",
            "cluster_autoscaler_unschedulable_pods_count",
            
            # etcd Metrics
            "etcd_cpu_usage_percentage",
            "etcd_database_usage_percentage",
            "etcd_memory_usage_percentage",
            
            # Node Resource Metrics
            "kube_node_status_allocatable_cpu_cores",
            "kube_node_status_allocatable_memory_bytes",
            "kube_node_status_condition",
            
            # Pod Metrics
            "kube_pod_status_ready",
            "kube_pod_status_phase",
            
            # Node Performance Metrics
            "node_cpu_usage_millicores",
            "node_cpu_usage_percentage", 
            "node_memory_rss_bytes",
            "node_memory_rss_percentage",
            "node_memory_working_set_bytes",
            "node_memory_working_set_percentage",
            "node_disk_usage_bytes",
            "node_disk_usage_percentage",
            "node_network_in_bytes",
            "node_network_out_bytes"
        }
        
        granularity = timedelta(minutes=5)
        successful_metrics = 0
        failed_metrics = 0
        
        self.logger.info(f"Collecting {len(all_metrics)} comprehensive AKS metrics...")
        
        for metric_name in all_metrics:
            try:
                self.logger.debug(f"Collecting metric: {metric_name}")
                metric_data = await self._get_single_metric_safe(
                    cluster_resource_id, metric_name, start_time, end_time, granularity
                )
                
                if metric_data:
                    cluster_metrics[metric_name] = metric_data
                    successful_metrics += 1
                    self.logger.debug(f"✓ Collected {len(metric_data)} data points for {metric_name}")
                else:
                    failed_metrics += 1
                    self.logger.debug(f"✗ No data for metric: {metric_name}")
                    
            except Exception as e:
                failed_metrics += 1
                self.logger.warning(f"✗ Failed to collect metric {metric_name}: {e}")
                continue
        
        self.logger.info(f"Metrics collection complete: {successful_metrics} successful, {failed_metrics} failed")
        return cluster_metrics
    
    async def _get_single_metric_safe(self, resource_id: str, metric_name: str,
                                    start_time: datetime, end_time: datetime,
                                    granularity: timedelta) -> List[Dict[str, Any]]:
        """Safely get data for a single metric with FIXED unit handling."""
        try:
            self.logger.debug(f"Querying metric {metric_name} for resource {resource_id}")
            
            response = self._metrics_client.query_resource(
                resource_uri=resource_id,
                metric_names=[metric_name],
                timespan=(start_time, end_time),
                granularity=granularity,
                aggregations=[MetricAggregationType.AVERAGE, MetricAggregationType.MAXIMUM]
            )
            
            metric_data = []
            for metric in response.metrics:
                self.logger.debug(f"Processing metric: {metric.name}")
                for timeseries in metric.timeseries:
                    self.logger.debug(f"Processing timeseries with {len(timeseries.data)} data points")
                    for data_point in timeseries.data:
                        if data_point.average is not None or data_point.maximum is not None:
                            # FIXED: Proper unit handling
                            unit_value = None
                            try:
                                if hasattr(metric, 'unit') and metric.unit:
                                    if hasattr(metric.unit, 'value'):
                                        unit_value = metric.unit.value
                                    else:
                                        unit_value = str(metric.unit)
                            except Exception as unit_error:
                                self.logger.debug(f"Could not extract unit for {metric_name}: {unit_error}")
                                unit_value = None
                            
                            metric_data.append({
                                'timestamp': data_point.timestamp.isoformat() if data_point.timestamp else None,
                                'average': float(data_point.average) if data_point.average is not None else None,
                                'maximum': float(data_point.maximum) if data_point.maximum is not None else None,
                                'metric_name': metric_name,
                                'unit': unit_value
                            })
            
            self.logger.debug(f"Extracted {len(metric_data)} valid data points for {metric_name}")
            return metric_data
            
        except ResourceNotFoundError as e:
            self.logger.warning(f"Resource not found for metric {metric_name}: {e}")
            return []
        except ClientAuthenticationError as e:
            self.logger.error(f"Authentication failed for metric {metric_name}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error getting metric {metric_name}: {e}")
            return []
    
    async def _collect_container_insights_safe(self, cluster_name: str, 
                                             start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Safely collect Container Insights metrics from Log Analytics with FIXED queries."""
        container_insights = {
            'pod_metrics': {},
            'container_metrics': {},
            'node_metrics': {},
            'data_points': 0,
            'collection_errors': []
        }
        
        if not self._logs_client or not self.log_analytics_workspace_id:
            container_insights['collection_errors'].append("Log Analytics not configured")
            return container_insights
        
        hours = int((end_time - start_time).total_seconds() / 3600)
        
        try:
            # FIXED: Query for basic cluster information using correct table structure
            cluster_query = f"""
            KubePodInventory
            | where TimeGenerated >= ago({hours}h)
            | where ClusterName == '{cluster_name}'
            | summarize 
                TotalPods = dcount(Name),
                RunningPods = dcountif(Name, PodStatus == "Running"),
                PendingPods = dcountif(Name, PodStatus == "Pending"),
                FailedPods = dcountif(Name, PodStatus == "Failed")
            """
            
            self.logger.debug(f"Executing Log Analytics query for cluster: {cluster_name}")
            response = self._logs_client.query_workspace(
                workspace_id=self.log_analytics_workspace_id,
                query=cluster_query,
                timespan=(start_time, end_time)
            )
            
            if response and hasattr(response, 'tables') and response.tables:
                table = response.tables[0]
                if table.rows:
                    row = table.rows[0]
                    container_insights['pod_metrics'] = {
                        'total_pods': row[0] if len(row) > 0 else 0,
                        'running_pods': row[1] if len(row) > 1 else 0,
                        'pending_pods': row[2] if len(row) > 2 else 0,
                        'failed_pods': row[3] if len(row) > 3 else 0
                    }
                    container_insights['data_points'] += 4
                    self.logger.info(f"Collected pod metrics: {container_insights['pod_metrics']}")
            
            # FIXED: Query for performance data using correct table structure
            perf_query = f"""
            Perf
            | where TimeGenerated >= ago({hours}h)
            | where ObjectName == "K8SNode"
            | where CounterName in ("cpuUsageNanoCores", "memoryWorkingSetBytes")
            | summarize 
                AvgValue = avg(CounterValue),
                MaxValue = max(CounterValue)
            by CounterName
            """
            
            perf_response = self._logs_client.query_workspace(
                workspace_id=self.log_analytics_workspace_id,
                query=perf_query,
                timespan=(start_time, end_time)
            )
            
            if perf_response and hasattr(perf_response, 'tables') and perf_response.tables:
                table = perf_response.tables[0]
                performance_data = {}
                for row in table.rows:
                    if len(row) >= 3:
                        counter_name = row[0]
                        performance_data[counter_name] = {
                            'avg_value': row[1],
                            'max_value': row[2]
                        }
                        container_insights['data_points'] += 2
                
                container_insights['performance_counters'] = performance_data
                self.logger.info(f"Collected performance counters: {list(performance_data.keys())}")
            
            # Additional query: Get node information
            node_query = f"""
            KubeNodeInventory
            | where TimeGenerated >= ago({hours}h)
            | where ClusterName == '{cluster_name}'
            | summarize 
                TotalNodes = dcount(Computer),
                ReadyNodes = dcountif(Computer, Status == "Ready")
            """
            
            node_response = self._logs_client.query_workspace(
                workspace_id=self.log_analytics_workspace_id,
                query=node_query,
                timespan=(start_time, end_time)
            )
            
            if node_response and hasattr(node_response, 'tables') and node_response.tables:
                table = node_response.tables[0]
                if table.rows:
                    row = table.rows[0]
                    container_insights['node_metrics'] = {
                        'total_nodes': row[0] if len(row) > 0 else 0,
                        'ready_nodes': row[1] if len(row) > 1 else 0
                    }
                    container_insights['data_points'] += 2
                    self.logger.info(f"Collected node metrics: {container_insights['node_metrics']}")
        
        except Exception as e:
            self.logger.error(f"Failed to query Container Insights: {e}")
            container_insights['collection_errors'].append(str(e))
        
        return container_insights
    
    def _generate_utilization_summary(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive utilization summary from ALL collected metrics."""
        utilization_summary = {
            "cluster_wide_utilization": {
                "avg_cpu_utilization": 0.0,
                "avg_memory_utilization": 0.0,
                "peak_cpu_utilization": 0.0,
                "peak_memory_utilization": 0.0,
                "avg_disk_utilization": 0.0,
                "peak_disk_utilization": 0.0,
                "network_in_bytes_per_sec": 0.0,
                "network_out_bytes_per_sec": 0.0
            },
            "control_plane_health": {
                "apiserver_cpu_usage": 0.0,
                "apiserver_memory_usage": 0.0,
                "apiserver_inflight_requests": 0.0,
                "etcd_cpu_usage": 0.0,
                "etcd_memory_usage": 0.0,
                "etcd_database_usage": 0.0
            },
            "cluster_autoscaler_status": {
                "safe_to_autoscale": False,
                "scale_down_in_cooldown": False,
                "unneeded_nodes_count": 0.0,
                "unschedulable_pods_count": 0.0
            },
            "resource_pressure_indicators": {
                "cpu_pressure": False,
                "memory_pressure": False,
                "disk_pressure": False,
                "network_pressure": False
            },
            "node_resource_allocation": {
                "allocatable_cpu_cores": 0.0,
                "allocatable_memory_bytes": 0.0,
                "node_readiness_status": "unknown"
            },
            "workload_distribution": {
                "total_pods": 0,
                "running_pods": 0,
                "pending_pods": 0,
                "failed_pods": 0,
                "ready_pods": 0.0
            }
        }
        
        # Process cluster metrics
        cluster_metrics = metrics_data.get('cluster_metrics', {})
        
        # Extract node performance metrics
        self._extract_node_performance_metrics(cluster_metrics, utilization_summary)
        
        # Extract control plane metrics
        self._extract_control_plane_metrics(cluster_metrics, utilization_summary)
        
        # Extract autoscaler metrics
        self._extract_autoscaler_metrics(cluster_metrics, utilization_summary)
        
        # Extract resource allocation metrics
        self._extract_resource_allocation_metrics(cluster_metrics, utilization_summary)
        
        # Extract pod metrics
        self._extract_pod_metrics(cluster_metrics, utilization_summary)
        
        # Process Container Insights data
        container_insights = metrics_data.get('container_insights_metrics', {})
        pod_metrics = container_insights.get('pod_metrics', {})
        
        if pod_metrics:
            utilization_summary["workload_distribution"].update(pod_metrics)
        
        # Calculate resource pressure indicators
        self._calculate_resource_pressure(utilization_summary)
        
        return utilization_summary
    
    def _extract_node_performance_metrics(self, cluster_metrics: Dict[str, Any], 
                                         utilization_summary: Dict[str, Any]) -> None:
        """Extract node performance metrics from cluster metrics."""
        # CPU Metrics
        cpu_percentage = self._get_latest_metric_value(cluster_metrics, "node_cpu_usage_percentage")
        cpu_millicores = self._get_latest_metric_value(cluster_metrics, "node_cpu_usage_millicores")
        
        if cpu_percentage is not None:
            utilization_summary["cluster_wide_utilization"]["avg_cpu_utilization"] = cpu_percentage
            utilization_summary["cluster_wide_utilization"]["peak_cpu_utilization"] = self._get_max_metric_value(cluster_metrics, "node_cpu_usage_percentage")
        
        # Memory Metrics
        memory_percentage = self._get_latest_metric_value(cluster_metrics, "node_memory_working_set_percentage")
        memory_rss_percentage = self._get_latest_metric_value(cluster_metrics, "node_memory_rss_percentage")
        
        if memory_percentage is not None:
            utilization_summary["cluster_wide_utilization"]["avg_memory_utilization"] = memory_percentage
            utilization_summary["cluster_wide_utilization"]["peak_memory_utilization"] = self._get_max_metric_value(cluster_metrics, "node_memory_working_set_percentage")
        
        # Disk Metrics
        disk_percentage = self._get_latest_metric_value(cluster_metrics, "node_disk_usage_percentage")
        if disk_percentage is not None:
            utilization_summary["cluster_wide_utilization"]["avg_disk_utilization"] = disk_percentage
            utilization_summary["cluster_wide_utilization"]["peak_disk_utilization"] = self._get_max_metric_value(cluster_metrics, "node_disk_usage_percentage")
        
        # Network Metrics
        network_in = self._get_latest_metric_value(cluster_metrics, "node_network_in_bytes")
        network_out = self._get_latest_metric_value(cluster_metrics, "node_network_out_bytes")
        
        if network_in is not None:
            utilization_summary["cluster_wide_utilization"]["network_in_bytes_per_sec"] = network_in / 300  # 5-min average
        if network_out is not None:
            utilization_summary["cluster_wide_utilization"]["network_out_bytes_per_sec"] = network_out / 300
    
    def _extract_control_plane_metrics(self, cluster_metrics: Dict[str, Any], 
                                      utilization_summary: Dict[str, Any]) -> None:
        """Extract control plane health metrics."""
        # API Server Metrics
        api_cpu = self._get_latest_metric_value(cluster_metrics, "apiserver_cpu_usage_percentage")
        api_memory = self._get_latest_metric_value(cluster_metrics, "apiserver_memory_usage_percentage")
        api_requests = self._get_latest_metric_value(cluster_metrics, "apiserver_current_inflight_requests")
        
        if api_cpu is not None:
            utilization_summary["control_plane_health"]["apiserver_cpu_usage"] = api_cpu
        if api_memory is not None:
            utilization_summary["control_plane_health"]["apiserver_memory_usage"] = api_memory
        if api_requests is not None:
            utilization_summary["control_plane_health"]["apiserver_inflight_requests"] = api_requests
        
        # etcd Metrics
        etcd_cpu = self._get_latest_metric_value(cluster_metrics, "etcd_cpu_usage_percentage")
        etcd_memory = self._get_latest_metric_value(cluster_metrics, "etcd_memory_usage_percentage")
        etcd_db = self._get_latest_metric_value(cluster_metrics, "etcd_database_usage_percentage")
        
        if etcd_cpu is not None:
            utilization_summary["control_plane_health"]["etcd_cpu_usage"] = etcd_cpu
        if etcd_memory is not None:
            utilization_summary["control_plane_health"]["etcd_memory_usage"] = etcd_memory
        if etcd_db is not None:
            utilization_summary["control_plane_health"]["etcd_database_usage"] = etcd_db
    
    def _extract_autoscaler_metrics(self, cluster_metrics: Dict[str, Any], 
                                   utilization_summary: Dict[str, Any]) -> None:
        """Extract cluster autoscaler metrics."""
        safe_to_autoscale = self._get_latest_metric_value(cluster_metrics, "cluster_autoscaler_cluster_safe_to_autoscale")
        scale_down_cooldown = self._get_latest_metric_value(cluster_metrics, "cluster_autoscaler_scale_down_in_cooldown")
        unneeded_nodes = self._get_latest_metric_value(cluster_metrics, "cluster_autoscaler_unneeded_nodes_count")
        unschedulable_pods = self._get_latest_metric_value(cluster_metrics, "cluster_autoscaler_unschedulable_pods_count")
        
        if safe_to_autoscale is not None:
            utilization_summary["cluster_autoscaler_status"]["safe_to_autoscale"] = safe_to_autoscale > 0
        if scale_down_cooldown is not None:
            utilization_summary["cluster_autoscaler_status"]["scale_down_in_cooldown"] = scale_down_cooldown > 0
        if unneeded_nodes is not None:
            utilization_summary["cluster_autoscaler_status"]["unneeded_nodes_count"] = unneeded_nodes
        if unschedulable_pods is not None:
            utilization_summary["cluster_autoscaler_status"]["unschedulable_pods_count"] = unschedulable_pods
    
    def _extract_resource_allocation_metrics(self, cluster_metrics: Dict[str, Any], 
                                            utilization_summary: Dict[str, Any]) -> None:
        """Extract node resource allocation metrics."""
        allocatable_cpu = self._get_latest_metric_value(cluster_metrics, "kube_node_status_allocatable_cpu_cores")
        allocatable_memory = self._get_latest_metric_value(cluster_metrics, "kube_node_status_allocatable_memory_bytes")
        
        if allocatable_cpu is not None:
            utilization_summary["node_resource_allocation"]["allocatable_cpu_cores"] = allocatable_cpu
        if allocatable_memory is not None:
            utilization_summary["node_resource_allocation"]["allocatable_memory_bytes"] = allocatable_memory
    
    def _extract_pod_metrics(self, cluster_metrics: Dict[str, Any], 
                            utilization_summary: Dict[str, Any]) -> None:
        """Extract pod status metrics."""
        ready_pods = self._get_latest_metric_value(cluster_metrics, "kube_pod_status_ready")
        
        if ready_pods is not None:
            utilization_summary["workload_distribution"]["ready_pods"] = ready_pods
    
    def _calculate_resource_pressure(self, utilization_summary: Dict[str, Any]) -> None:
        """Calculate resource pressure indicators."""
        cluster_util = utilization_summary["cluster_wide_utilization"]
        
        # CPU pressure
        cpu_util = cluster_util.get("avg_cpu_utilization", 0)
        utilization_summary["resource_pressure_indicators"]["cpu_pressure"] = cpu_util > 80
        
        # Memory pressure
        memory_util = cluster_util.get("avg_memory_utilization", 0)
        utilization_summary["resource_pressure_indicators"]["memory_pressure"] = memory_util > 80
        
        # Disk pressure
        disk_util = cluster_util.get("avg_disk_utilization", 0)
        utilization_summary["resource_pressure_indicators"]["disk_pressure"] = disk_util > 85
        
        # Network pressure (simplified check)
        network_in = cluster_util.get("network_in_bytes_per_sec", 0)
        network_out = cluster_util.get("network_out_bytes_per_sec", 0)
        total_network = network_in + network_out
        utilization_summary["resource_pressure_indicators"]["network_pressure"] = total_network > 100000000  # 100MB/s
    
    def _get_latest_metric_value(self, cluster_metrics: Dict[str, Any], metric_name: str) -> Optional[float]:
        """Get the latest value for a metric."""
        metric_data = cluster_metrics.get(metric_name, [])
        if metric_data and isinstance(metric_data, list):
            latest_point = metric_data[-1]  # Last data point
            return latest_point.get('average') or latest_point.get('maximum')
        return None
    
    def _get_max_metric_value(self, cluster_metrics: Dict[str, Any], metric_name: str) -> Optional[float]:
        """Get the maximum value for a metric."""
        metric_data = cluster_metrics.get(metric_name, [])
        if metric_data and isinstance(metric_data, list):
            max_values = [point.get('maximum', 0) or point.get('average', 0) for point in metric_data]
            return max(max_values) if max_values else None
        return None
    
    def _analyze_health_status(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze health status from metrics data."""
        health_status = {
            'overall_status': 'healthy',
            'issues_detected': [],
            'recommendations': []
        }
        
        total_data_points = metrics_data.get('collection_metadata', {}).get('total_data_points', 0)
        collection_errors = metrics_data.get('collection_metadata', {}).get('collection_errors', [])
        
        if total_data_points == 0:
            health_status['overall_status'] = 'unknown'
            health_status['issues_detected'].append('No metrics data available')
            health_status['recommendations'].extend([
                'Verify Azure Monitor permissions',
                'Check Log Analytics workspace configuration',
                'Ensure Container Insights is enabled'
            ])
        
        if collection_errors:
            health_status['issues_detected'].extend(collection_errors)
        
        return health_status
    
    def _get_default_metrics_structure(self, cluster_name: str, error: str) -> Dict[str, Any]:
        """Return default metrics structure for error cases."""
        return {
            'cluster_name': cluster_name,
            'error': error,
            'collection_metadata': {
                'metrics_collected': [],
                'metrics_failed': ["all"],
                'total_data_points': 0,
                'data_sources': [],
                'collection_errors': [error]
            },
            'utilization_summary': {
                "cluster_wide_utilization": {
                    "avg_cpu_utilization": 0.0,
                    "avg_memory_utilization": 0.0
                },
                "resource_pressure_indicators": {
                    "cpu_pressure": False,
                    "memory_pressure": False
                }
            },
            'health_status': {
                'overall_status': 'unknown',
                'issues_detected': [error],
                'recommendations': ["Check Azure Monitor configuration and permissions"]
            }
        }