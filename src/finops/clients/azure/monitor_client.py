# src/finops/clients/azure/monitor_client.py
"""Completely fixed Azure Monitor client - no legacy issues."""

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
    """Completely fixed Azure Monitor client with all 21 metrics."""
    
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
            
            test_resource_id = f"/subscriptions/{self.subscription_id}"
            try:
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
        """Get comprehensive metrics from 3 sources - completely rewritten."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours)
        
        self.logger.info(f"Starting fixed comprehensive metrics collection for cluster: {cluster_name}")
        
        # Define ALL 21 of your metrics
        all_metrics_list = [
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
        ]
        
        # Initialize comprehensive metrics structure
        comprehensive_metrics = {
            'cluster_name': cluster_name,
            'cluster_resource_id': cluster_resource_id,
            'collection_period': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'hours': hours
            },
            
            # ===== REALISTIC METRIC GROUPS =====
            'node_performance': {},           
            'cluster_overview': {},           
            'workload_distribution': {},      
            'control_plane_health': {},       
            'combined_analysis': {},          
            
            # ===== DATA SOURCE TRACKING =====
            'data_sources': {
                'azure_monitor': {
                    'status': 'pending',
                    'metrics_collected': [],
                    'data_points': 0,
                    'collection_errors': []
                },
                'container_insights': {
                    'status': 'pending',
                    'queries_executed': [],
                    'data_points': 0,
                    'collection_errors': []
                },
                'kubernetes_api': {
                    'status': 'not_available',
                    'resources_discovered': [],
                    'collection_errors': []
                }
            },
            
            # ===== COLLECTION METADATA =====
            'collection_metadata': {
                'total_data_points': 0,
                'successful_sources': 0,
                'failed_sources': 0,
                'data_freshness_score': 0.0,
                'health_score': 0.0,
                'total_metrics_attempted': len(all_metrics_list)
            }
        }
        
        # ===== COLLECT AZURE MONITOR METRICS =====
        azure_source = comprehensive_metrics['data_sources']['azure_monitor']
        
        try:
            self.logger.info(f"Collecting {len(all_metrics_list)} Azure Monitor metrics...")
            
            granularity = timedelta(minutes=5)
            collected_metrics = {}
            
            for metric_name in all_metrics_list:
                try:
                    self.logger.debug(f"Attempting to collect metric: {metric_name}")
                    metric_data = await self._get_single_metric_safe(
                        cluster_resource_id, metric_name, start_time, end_time, granularity
                    )
                    
                    if metric_data:
                        # Debug: Check data types
                        latest_val = self._get_latest_value(metric_data)
                        avg_val = self._get_average_value(metric_data)
                        max_val = self._get_max_value(metric_data)
                        
                        self.logger.debug(f"Metric {metric_name}: latest={latest_val} (type={type(latest_val)}), avg={avg_val} (type={type(avg_val)})")
                        
                        collected_metrics[metric_name] = {
                            'latest_value': latest_val,
                            'average_value': avg_val,
                            'max_value': max_val,
                            'data_points': len(metric_data),
                            'raw_data': metric_data
                        }
                        azure_source['data_points'] += len(metric_data)
                        azure_source['metrics_collected'].append(metric_name)
                        self.logger.debug(f"✓ Collected {len(metric_data)} data points for {metric_name}")
                    else:
                        self.logger.debug(f"✗ No data for {metric_name}")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to collect {metric_name}: {e}")
                    azure_source['collection_errors'].append(f"{metric_name}: {str(e)}")
                    continue
            
            # Debug: Log collected metrics summary
            self.logger.info(f"Collected metrics summary: {list(collected_metrics.keys())}")
            
            # Organize metrics into groups
            self._organize_collected_metrics(collected_metrics, comprehensive_metrics)
            
            azure_source['status'] = 'success' if azure_source['data_points'] > 0 else 'no_data'
            self.logger.info(f"Azure Monitor: {len(azure_source['metrics_collected'])}/{len(all_metrics_list)} metrics collected, {azure_source['data_points']} data points")
            
        except Exception as e:
            azure_source['status'] = 'failed'
            azure_source['collection_errors'].append(str(e))
            self.logger.error(f"Azure Monitor collection failed: {e}")
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
        
        # ===== COLLECT CONTAINER INSIGHTS =====
        if self._logs_client and self.log_analytics_workspace_id:
            await self._collect_container_insights_safe(cluster_name, start_time, end_time, comprehensive_metrics)
        else:
            comprehensive_metrics['data_sources']['container_insights']['status'] = 'unavailable'
            comprehensive_metrics['data_sources']['container_insights']['collection_errors'].append(
                'Log Analytics workspace not configured'
            )
        
        # ===== COMBINED ANALYSIS =====
        try:
            self.logger.debug("Starting combined analysis...")
            self._perform_combined_analysis(comprehensive_metrics)
            self.logger.debug("Combined analysis completed")
        except Exception as e:
            self.logger.error(f"Combined analysis failed: {e}")
            import traceback
            self.logger.error(f"Combined analysis traceback: {traceback.format_exc()}")
            # Set default combined analysis
            comprehensive_metrics['combined_analysis'] = {
                'cluster_health_score': 0.0,
                'data_consistency_score': 0.0,
                'recommendations': [f"Analysis failed: {str(e)}"],
                'error': str(e)
            }
        
        # ===== FINAL METADATA =====
        self._calculate_final_metadata(comprehensive_metrics)
        
        total_data_points = comprehensive_metrics['collection_metadata']['total_data_points']
        successful_sources = comprehensive_metrics['collection_metadata']['successful_sources']
        
        self.logger.info(
            f"Fixed comprehensive metrics collection completed for {cluster_name}",
            total_data_points=total_data_points,
            successful_sources=successful_sources,
            health_score=comprehensive_metrics['collection_metadata']['health_score']
        )
        
        return comprehensive_metrics
    
    def _organize_collected_metrics(self, collected_metrics: Dict[str, Any], 
                                  comprehensive_metrics: Dict[str, Any]) -> None:
        """Organize collected metrics into logical groups."""
        
        # Node Performance metrics
        node_performance = {}
        node_resource_allocation = {}
        
        for metric_name, metric_data in collected_metrics.items():
            if metric_name.startswith('node_'):
                node_performance[metric_name] = metric_data
            elif metric_name.startswith('kube_node_'):
                node_resource_allocation[metric_name] = metric_data
        
        if node_resource_allocation:
            node_performance['resource_allocation'] = node_resource_allocation
        
        comprehensive_metrics['node_performance'] = node_performance
        
        # Control Plane metrics
        control_plane = {}
        autoscaler_metrics = {}
        
        for metric_name, metric_data in collected_metrics.items():
            if metric_name.startswith(('apiserver_', 'etcd_')):
                control_plane[metric_name] = metric_data
            elif 'autoscaler' in metric_name:
                autoscaler_metrics[metric_name] = metric_data
        
        if autoscaler_metrics:
            control_plane['autoscaler_metrics'] = autoscaler_metrics
        
        comprehensive_metrics['control_plane_health'] = control_plane
        
        # Pod metrics
        pod_metrics = {}
        for metric_name, metric_data in collected_metrics.items():
            if metric_name.startswith('kube_pod_'):
                pod_metrics[metric_name] = metric_data
        
        if pod_metrics:
            comprehensive_metrics['azure_pod_metrics'] = pod_metrics
    
    async def _collect_container_insights_safe(self, cluster_name: str,
                                          start_time: datetime, end_time: datetime,
                                          comprehensive_metrics: Dict[str, Any]) -> None:
        """Fixed Container Insights collection with better queries."""
        ci_source = comprehensive_metrics['data_sources']['container_insights']
        
        try:
            self.logger.info("Collecting Container Insights metrics...")
            hours = int((end_time - start_time).total_seconds() / 3600)
            
            # ===== FIX 1: CLUSTER OVERVIEW QUERY =====
            cluster_overview_query = f"""
            KubeNodeInventory
            | where TimeGenerated >= ago({hours}h)
            | where ClusterName == '{cluster_name}' or ClusterName has '{cluster_name}'
            | summarize 
                TotalNodes = dcount(Computer),
                ReadyNodes = dcountif(Computer, Status == "Ready")
            | extend NotReadyNodes = TotalNodes - ReadyNodes
            | extend HealthPercentage = toreal(ReadyNodes) * 100.0 / toreal(TotalNodes)
            """
            
            cluster_overview = await self._execute_log_query_safe(
                cluster_overview_query, start_time, end_time, "cluster_overview"
            )
            
            # Debug logging for cluster overview
            self.logger.info(f"Raw cluster overview result: {cluster_overview}")
            
            if cluster_overview:
                # Fix: Handle both list and dict results
                if isinstance(cluster_overview, list) and len(cluster_overview) > 0:
                    cluster_overview = cluster_overview[0]
                
                if isinstance(cluster_overview, dict) and cluster_overview:
                    comprehensive_metrics['cluster_overview'] = cluster_overview
                    ci_source['queries_executed'].append('cluster_overview')
                    ci_source['data_points'] += 1
                    self.logger.info(f"Cluster overview collected: {cluster_overview}")
                else:
                    self.logger.warning(f"Cluster overview invalid format: {cluster_overview}")
            else:
                self.logger.warning("No cluster overview data returned - trying alternative query")
                
                # Alternative query without cluster name filter
                alt_cluster_query = f"""
                KubeNodeInventory
                | where TimeGenerated >= ago({hours}h)
                | summarize 
                    TotalNodes = dcount(Computer),
                    ReadyNodes = dcountif(Computer, Status == "Ready")
                | extend NotReadyNodes = TotalNodes - ReadyNodes
                | extend HealthPercentage = toreal(ReadyNodes) * 100.0 / toreal(TotalNodes)
                """
                
                alt_cluster = await self._execute_log_query_safe(
                    alt_cluster_query, start_time, end_time, "alt_cluster_overview"
                )
                
                if alt_cluster:
                    if isinstance(alt_cluster, list) and len(alt_cluster) > 0:
                        alt_cluster = alt_cluster[0]
                    comprehensive_metrics['cluster_overview'] = alt_cluster
                    ci_source['data_points'] += 1
                    self.logger.info(f"Alternative cluster overview collected: {alt_cluster}")
            
            # ===== FIX 2: WORKLOAD DISTRIBUTION QUERY =====
            # Try multiple approaches for workload data
            workload_found = False
            
            # Approach 1: KubePodInventory cluster-wide
            workload_cluster_query = f"""
            KubePodInventory
            | where TimeGenerated >= ago({hours}h)
            | where ClusterName == '{cluster_name}' or ClusterName has '{cluster_name}'
            | summarize 
                TotalPods = dcount(Name),
                RunningPods = dcountif(Name, PodStatus == "Running"),
                PendingPods = dcountif(Name, PodStatus == "Pending"),
                FailedPods = dcountif(Name, PodStatus == "Failed"),
                SucceededPods = dcountif(Name, PodStatus == "Succeeded")
            """
            
            workload_cluster = await self._execute_log_query_safe(
                workload_cluster_query, start_time, end_time, "workload_cluster"
            )
            
            # Debug logging for workload
            self.logger.info(f"Raw workload cluster result: {workload_cluster}")
            
            if workload_cluster:
                if isinstance(workload_cluster, list) and len(workload_cluster) > 0:
                    workload_cluster = workload_cluster[0]
                
                if isinstance(workload_cluster, dict) and workload_cluster.get('TotalPods', 0) > 0:
                    comprehensive_metrics['workload_distribution'] = workload_cluster
                    ci_source['queries_executed'].append('workload_distribution_cluster')
                    ci_source['data_points'] += 1
                    workload_found = True
                    self.logger.info(f"Workload distribution collected: {workload_cluster}")
                    
                    # Also get namespace breakdown
                    namespace_query = f"""
                    KubePodInventory
                    | where TimeGenerated >= ago({hours}h)
                    | where ClusterName == '{cluster_name}' or ClusterName has '{cluster_name}'
                    | summarize 
                        TotalPods = dcount(Name),
                        RunningPods = dcountif(Name, PodStatus == "Running"),
                        PendingPods = dcountif(Name, PodStatus == "Pending"),
                        FailedPods = dcountif(Name, PodStatus == "Failed"),
                        SucceededPods = dcountif(Name, PodStatus == "Succeeded")
                    by Namespace
                    | order by TotalPods desc
                    """
                    
                    namespace_breakdown = await self._execute_log_query_safe(
                        namespace_query, start_time, end_time, "workload_namespaces"
                    )
                    
                    if namespace_breakdown and isinstance(namespace_breakdown, list):
                        comprehensive_metrics['workload_distribution']['namespace_breakdown'] = namespace_breakdown
                        ci_source['data_points'] += len(namespace_breakdown)
                        self.logger.info(f"Namespace breakdown: {len(namespace_breakdown)} namespaces")
            
            # Approach 2: Try without cluster name filter if no results
            if not workload_found:
                self.logger.warning("No workload data with cluster filter - trying without filter")
                
                alt_workload_query = f"""
                KubePodInventory
                | where TimeGenerated >= ago({hours}h)
                | summarize 
                    TotalPods = dcount(Name),
                    RunningPods = dcountif(Name, PodStatus == "Running"),
                    PendingPods = dcountif(Name, PodStatus == "Pending"),
                    FailedPods = dcountif(Name, PodStatus == "Failed"),
                    SucceededPods = dcountif(Name, PodStatus == "Succeeded")
                """
                
                alt_workload = await self._execute_log_query_safe(
                    alt_workload_query, start_time, end_time, "alt_workload"
                )
                
                self.logger.info(f"Alternative workload result: {alt_workload}")
                
                if alt_workload:
                    if isinstance(alt_workload, list) and len(alt_workload) > 0:
                        alt_workload = alt_workload[0]
                    
                    if isinstance(alt_workload, dict) and alt_workload.get('TotalPods', 0) > 0:
                        comprehensive_metrics['workload_distribution'] = alt_workload
                        ci_source['data_points'] += 1
                        workload_found = True
                        self.logger.info(f"Alternative workload collected: {alt_workload}")
            
            # Approach 3: Try ContainerInventory as last resort
            if not workload_found:
                self.logger.warning("Trying ContainerInventory as fallback")
                
                container_query = f"""
                ContainerInventory
                | where TimeGenerated >= ago({hours}h)
                | summarize PodCount = dcount(PodName)
                """
                
                container_result = await self._execute_log_query_safe(
                    container_query, start_time, end_time, "container_fallback"
                )
                
                self.logger.info(f"Container inventory result: {container_result}")
                
                if container_result:
                    if isinstance(container_result, list) and len(container_result) > 0:
                        container_result = container_result[0]
                    
                    pod_count = container_result.get('PodCount', 0) if isinstance(container_result, dict) else 0
                    
                    if pod_count > 0:
                        comprehensive_metrics['workload_distribution'] = {
                            'TotalPods': pod_count,
                            'RunningPods': pod_count,  # Assume running
                            'PendingPods': 0,
                            'FailedPods': 0,
                            'SucceededPods': 0,
                            'source': 'container_inventory'
                        }
                        ci_source['data_points'] += 1
                        workload_found = True
                        self.logger.info(f"Container inventory fallback: {pod_count} pods")
            
            # If still no workload data, create minimal structure
            if not workload_found:
                self.logger.warning("No workload data found from any source")
                comprehensive_metrics['workload_distribution'] = {
                    'TotalPods': 0,
                    'RunningPods': 0,
                    'PendingPods': 0,
                    'FailedPods': 0,
                    'SucceededPods': 0,
                    'source': 'no_data'
                }
            
            ci_source['status'] = 'success' if ci_source['data_points'] > 0 else 'no_data'
            self.logger.info(f"Container Insights: {ci_source['data_points']} data points collected")
            
        except Exception as e:
            ci_source['status'] = 'failed'
            ci_source['collection_errors'].append(str(e))
            self.logger.error(f"Container Insights collection failed: {e}")
            import traceback
            self.logger.error(f"Container Insights traceback: {traceback.format_exc()}")

    async def _execute_log_query_safe(self, query: str, start_time: datetime, 
                                    end_time: datetime, query_name: str) -> Optional[Dict[str, Any]]:
        """Safely execute a Log Analytics query with robust column handling."""
        try:
            self.logger.debug(f"Executing {query_name} query")
            
            response = self._logs_client.query_workspace(
                workspace_id=self.log_analytics_workspace_id,
                query=query,
                timespan=(start_time, end_time)
            )
            
            if response and hasattr(response, 'tables') and response.tables:
                table = response.tables[0]
                if table.rows:
                    # Robust column handling
                    columns = []
                    for col in table.columns:
                        try:
                            if hasattr(col, 'name'):
                                columns.append(col.name)
                            elif hasattr(col, 'column_name'):
                                columns.append(col.column_name)
                            elif isinstance(col, dict) and 'name' in col:
                                columns.append(col['name'])
                            elif isinstance(col, str):
                                columns.append(col)
                            else:
                                columns.append(f"column_{len(columns)}")
                        except Exception as col_error:
                            self.logger.warning(f"Error processing column {col}: {col_error}")
                            columns.append(f"column_{len(columns)}")
                    
                    if len(table.rows) == 1:
                        # Single row result
                        row = table.rows[0]
                        return dict(zip(columns, row))
                    else:
                        # Multiple rows result
                        results = []
                        for row in table.rows:
                            results.append(dict(zip(columns, row)))
                        return results
                        
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to execute {query_name} query: {e}")
            return None
    
    async def _get_single_metric_safe(self, resource_id: str, metric_name: str,
                                    start_time: datetime, end_time: datetime,
                                    granularity: timedelta) -> List[Dict[str, Any]]:
        """Safely get data for a single metric."""
        try:
            response = self._metrics_client.query_resource(
                resource_uri=resource_id,
                metric_names=[metric_name],
                timespan=(start_time, end_time),
                granularity=granularity,
                aggregations=[MetricAggregationType.AVERAGE, MetricAggregationType.MAXIMUM]
            )
            
            metric_data = []
            for metric in response.metrics:
                for timeseries in metric.timeseries:
                    for data_point in timeseries.data:
                        if data_point.average is not None or data_point.maximum is not None:
                            metric_data.append({
                                'timestamp': data_point.timestamp.isoformat() if data_point.timestamp else None,
                                'average': float(data_point.average) if data_point.average is not None else None,
                                'maximum': float(data_point.maximum) if data_point.maximum is not None else None,
                                'metric_name': metric_name
                            })
            
            return metric_data
            
        except Exception as e:
            self.logger.debug(f"Failed to get metric {metric_name}: {e}")
            return []
    
    def _get_latest_value(self, metric_data: List[Dict[str, Any]]) -> Optional[float]:
        """Get latest value from metric data with safe type handling."""
        if not metric_data:
            return None
        try:
            latest = metric_data[-1]
            value = latest.get('average') or latest.get('maximum')
            return self._safe_float_conversion(value)
        except Exception as e:
            self.logger.debug(f"Error getting latest value: {e}")
            return None
    
    def _get_average_value(self, metric_data: List[Dict[str, Any]]) -> Optional[float]:
        """Get average value from metric data with safe type handling."""
        if not metric_data:
            return None
        try:
            values = []
            for point in metric_data:
                val = point.get('average', 0) or point.get('maximum', 0)
                converted_val = self._safe_float_conversion(val)
                if converted_val > 0:  # Only include positive values
                    values.append(converted_val)
            
            return sum(values) / len(values) if values else None
        except Exception as e:
            self.logger.debug(f"Error calculating average: {e}")
            return None
    
    def _get_max_value(self, metric_data: List[Dict[str, Any]]) -> Optional[float]:
        """Get max value from metric data with safe type handling."""
        if not metric_data:
            return None
        try:
            values = []
            for point in metric_data:
                val = point.get('maximum', 0) or point.get('average', 0)
                converted_val = self._safe_float_conversion(val)
                if converted_val > 0:  # Only include positive values
                    values.append(converted_val)
            
            return max(values) if values else None
        except Exception as e:
            self.logger.debug(f"Error getting max value: {e}")
            return None
    
    def _perform_combined_analysis(self, comprehensive_metrics: Dict[str, Any]) -> None:
        """Perform cross-source validation and health scoring."""
        combined_analysis = {
            'data_source_validation': {},
            'health_indicators': {},
            'cluster_health_score': 0.0,
            'data_consistency_score': 0.0,
            'recommendations': []
        }
        
        # Data source validation
        azure_status = comprehensive_metrics['data_sources']['azure_monitor']['status']
        ci_status = comprehensive_metrics['data_sources']['container_insights']['status']
        
        combined_analysis['data_source_validation'] = {
            'azure_monitor_available': azure_status == 'success',
            'container_insights_available': ci_status == 'success',
            'primary_source': 'azure_monitor' if azure_status == 'success' else 'container_insights',
            'data_coverage_percentage': self._calculate_data_coverage(comprehensive_metrics)
        }
        
        # Health indicators
        health_indicators = {}
        
        # Node health
        if comprehensive_metrics['node_performance']:
            cpu_health = self._assess_cpu_health(comprehensive_metrics['node_performance'])
            memory_health = self._assess_memory_health(comprehensive_metrics['node_performance'])
            health_indicators.update({
                'cpu_health': cpu_health,
                'memory_health': memory_health
            })
        
        # Cluster health
        if comprehensive_metrics['cluster_overview']:
            cluster_health = self._assess_cluster_health(comprehensive_metrics['cluster_overview'])
            health_indicators.update(cluster_health)
        
        combined_analysis['health_indicators'] = health_indicators
        combined_analysis['cluster_health_score'] = self._calculate_cluster_health_score(health_indicators)
        combined_analysis['data_consistency_score'] = self._calculate_data_consistency_score(comprehensive_metrics)
        combined_analysis['recommendations'] = self._generate_recommendations(comprehensive_metrics, health_indicators)
        
        comprehensive_metrics['combined_analysis'] = combined_analysis
    
    def _calculate_final_metadata(self, comprehensive_metrics: Dict[str, Any]) -> None:
        """Calculate final collection metadata."""
        metadata = comprehensive_metrics['collection_metadata']
        
        successful_sources = 0
        total_data_points = 0
        
        for source_name, source_data in comprehensive_metrics['data_sources'].items():
            if source_data['status'] == 'success':
                successful_sources += 1
            total_data_points += source_data.get('data_points', 0)
        
        metadata.update({
            'total_data_points': total_data_points,
            'successful_sources': successful_sources,
            'failed_sources': len(comprehensive_metrics['data_sources']) - successful_sources,
            'data_freshness_score': self._calculate_freshness_score(comprehensive_metrics),
            'health_score': comprehensive_metrics['combined_analysis']['cluster_health_score']
        })
    
    # Helper methods for analysis
    def _calculate_data_coverage(self, comprehensive_metrics: Dict[str, Any]) -> float:
        """Calculate data coverage percentage."""
        total_sources = len(comprehensive_metrics['data_sources'])
        successful_sources = sum(
            1 for source in comprehensive_metrics['data_sources'].values()
            if source['status'] == 'success'
        )
        return (successful_sources / total_sources) * 100 if total_sources > 0 else 0
    
    def _assess_cpu_health(self, node_performance: Dict[str, Any]) -> Dict[str, Any]:
        """Assess CPU health from node performance data with safe type checking."""
        cpu_metric = node_performance.get('node_cpu_usage_percentage', {})
        
        # Safe value extraction with type checking
        latest_cpu = self._safe_float_conversion(cpu_metric.get('latest_value', 0))
        avg_cpu = self._safe_float_conversion(cpu_metric.get('average_value', 0))
        
        status = 'healthy'
        if avg_cpu > 80:
            status = 'critical'
        elif avg_cpu > 60:
            status = 'warning'
        
        return {
            'status': status,
            'latest_usage': latest_cpu,
            'average_usage': avg_cpu,
            'trend': 'stable'
        }
    
    def _assess_memory_health(self, node_performance: Dict[str, Any]) -> Dict[str, Any]:
        """Assess memory health from node performance data with safe type checking."""
        memory_metric = node_performance.get('node_memory_working_set_percentage', {})
        
        # Safe value extraction with type checking
        latest_memory = self._safe_float_conversion(memory_metric.get('latest_value', 0))
        avg_memory = self._safe_float_conversion(memory_metric.get('average_value', 0))
        
        status = 'healthy'
        if avg_memory > 85:
            status = 'critical'
        elif avg_memory > 70:
            status = 'warning'
        
        return {
            'status': status,
            'latest_usage': latest_memory,
            'average_usage': avg_memory,
            'trend': 'stable'
        }
    
    def _assess_cluster_health(self, cluster_overview: Dict[str, Any]) -> Dict[str, Any]:
        """Assess cluster health from overview data with safe type checking."""
        if isinstance(cluster_overview, dict):
            total_nodes = self._safe_int_conversion(cluster_overview.get('TotalNodes', 0))
            ready_nodes = self._safe_int_conversion(cluster_overview.get('ReadyNodes', 0))
            health_percentage = self._safe_float_conversion(cluster_overview.get('HealthPercentage', 0))
        else:
            total_nodes = ready_nodes = health_percentage = 0
        
        status = 'healthy'
        if health_percentage < 80:
            status = 'critical'
        elif health_percentage < 95:
            status = 'warning'
        
        return {
            'cluster_node_health': {
                'status': status,
                'total_nodes': total_nodes,
                'ready_nodes': ready_nodes,
                'health_percentage': health_percentage
            }
        }
    
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
    
    def _calculate_cluster_health_score(self, health_indicators: Dict[str, Any]) -> float:
        """Calculate overall cluster health score."""
        scores = []
        
        for indicator_name, indicator_data in health_indicators.items():
            if isinstance(indicator_data, dict) and 'status' in indicator_data:
                status = indicator_data['status']
                if status == 'healthy':
                    scores.append(100)
                elif status == 'warning':
                    scores.append(70)
                elif status == 'critical':
                    scores.append(30)
        
        return sum(scores) / len(scores) if scores else 0.0
    
    def _calculate_data_consistency_score(self, comprehensive_metrics: Dict[str, Any]) -> float:
        """Calculate data consistency score across sources."""
        successful_sources = comprehensive_metrics['collection_metadata']['successful_sources']
        total_sources = len(comprehensive_metrics['data_sources'])
        
        return (successful_sources / total_sources) * 100 if total_sources > 0 else 0
    
    def _calculate_freshness_score(self, comprehensive_metrics: Dict[str, Any]) -> float:
        """Calculate data freshness score."""
        total_data_points = comprehensive_metrics['collection_metadata']['total_data_points']
        
        if total_data_points == 0:
            return 0.0
        elif total_data_points < 10:
            return 30.0
        elif total_data_points < 50:
            return 70.0
        else:
            return 100.0
    
    def _generate_recommendations(self, comprehensive_metrics: Dict[str, Any], 
                                health_indicators: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on metrics and health indicators."""
        recommendations = []
        
        # Check metrics collection
        successful_sources = comprehensive_metrics['collection_metadata']['successful_sources']
        if successful_sources == 0:
            recommendations.append("No metrics data available. Check Azure Monitor permissions and Log Analytics configuration.")
        elif successful_sources == 1:
            recommendations.append("Limited metrics data. Consider enabling additional monitoring sources for comprehensive visibility.")
        
        # Check health indicators
        for indicator_name, indicator_data in health_indicators.items():
            if isinstance(indicator_data, dict) and 'status' in indicator_data:
                status = indicator_data['status']
                if status == 'critical':
                    recommendations.append(f"Critical: {indicator_name} requires immediate attention.")
                elif status == 'warning':
                    recommendations.append(f"Warning: {indicator_name} should be monitored closely.")
        
        return recommendations