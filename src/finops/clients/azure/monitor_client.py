# src/finops/clients/azure/monitor_client.py
"""Complete Azure Monitor client with all methods implemented."""

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
    """Complete Azure Monitor client with comprehensive metrics collection."""
    
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
        """Get comprehensive metrics from all sources with proper JSON structure."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours)
        
        self.logger.info(f"Starting comprehensive metrics collection for cluster: {cluster_name}")
        
        # Define ALL metrics we want to collect
        all_metrics_list = [
            "apiserver_cpu_usage_percentage",
            "apiserver_memory_usage_percentage", 
            "apiserver_current_inflight_requests",
            "cluster_autoscaler_cluster_safe_to_autoscale",
            "cluster_autoscaler_scale_down_in_cooldown",
            "cluster_autoscaler_unneeded_nodes_count",
            "cluster_autoscaler_unschedulable_pods_count",
            "etcd_cpu_usage_percentage",
            "etcd_database_usage_percentage",
            "etcd_memory_usage_percentage",
            "kube_node_status_allocatable_cpu_cores",
            "kube_node_status_allocatable_memory_bytes",
            "kube_node_status_condition",
            "kube_pod_status_ready",
            "kube_pod_status_phase",
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
            'metrics_by_source': {
                'azure_monitor': {
                    'source_name': 'Azure Monitor',
                    'source_type': 'metrics_api',
                    'status': 'pending',
                    'metrics': {},
                    'collection_metadata': {
                        'total_metrics_attempted': len(all_metrics_list),
                        'successful_metrics': 0,
                        'failed_metrics': 0,
                        'total_data_points': 0,
                        'collection_errors': []
                    }
                },
                'container_insights': {
                    'source_name': 'Container Insights',
                    'source_type': 'log_analytics',
                    'status': 'pending',
                    'insights_data': {},
                    'collection_metadata': {
                        'queries_executed': 0,
                        'successful_queries': 0,
                        'failed_queries': 0,
                        'total_data_points': 0,
                        'collection_errors': []
                    }
                }
            },
            'combined_analysis': {
                'health_score': 0.0,
                'data_consistency_score': 0.0,
                'recommendations': [],
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
        
        collection_start = datetime.now(timezone.utc)
        
        # Collect Azure Monitor metrics
        await self._collect_azure_monitor_metrics(
            cluster_resource_id, all_metrics_list, start_time, end_time, comprehensive_metrics
        )
        
        # Collect Container Insights data
        if self._logs_client and self.log_analytics_workspace_id:
            await self._collect_container_insights_data(
                cluster_name, start_time, end_time, comprehensive_metrics
            )
        else:
            ci_source = comprehensive_metrics['metrics_by_source']['container_insights']
            ci_source['status'] = 'unavailable'
            ci_source['collection_metadata']['collection_errors'].append(
                'Log Analytics workspace not configured'
            )
        
        # Perform combined analysis
        self._perform_cross_source_analysis(comprehensive_metrics)
        
        # Calculate final summary
        collection_end = datetime.now(timezone.utc)
        collection_duration = (collection_end - collection_start).total_seconds()
        
        summary = comprehensive_metrics['collection_summary']
        azure_source = comprehensive_metrics['metrics_by_source']['azure_monitor']
        ci_source = comprehensive_metrics['metrics_by_source']['container_insights']
        
        summary.update({
            'successful_sources': sum(1 for source in [azure_source, ci_source] if source['status'] == 'success'),
            'total_metrics_collected': (
                azure_source['collection_metadata']['successful_metrics'] +
                ci_source['collection_metadata']['successful_queries']
            ),
            'total_data_points': (
                azure_source['collection_metadata']['total_data_points'] +
                ci_source['collection_metadata']['total_data_points']
            ),
            'collection_duration_seconds': collection_duration
        })
        
        self.logger.info(
            f"Comprehensive metrics collection completed for {cluster_name}",
            successful_sources=summary['successful_sources'],
            total_metrics=summary['total_metrics_collected'],
            total_data_points=summary['total_data_points'],
            duration_seconds=collection_duration
        )
        
        return comprehensive_metrics
    
    async def _collect_azure_monitor_metrics(self, cluster_resource_id: str, all_metrics_list: List[str],
                                           start_time: datetime, end_time: datetime,
                                           comprehensive_metrics: Dict[str, Any]) -> None:
        """Collect all Azure Monitor metrics with detailed tracking."""
        azure_source = comprehensive_metrics['metrics_by_source']['azure_monitor']
        metadata = azure_source['collection_metadata']
        
        try:
            self.logger.info(f"Collecting {len(all_metrics_list)} Azure Monitor metrics...")
            
            granularity = timedelta(minutes=5)
            
            for metric_name in all_metrics_list:
                try:
                    self.logger.debug(f"Collecting metric: {metric_name}")
                    metric_data = await self._get_single_metric_with_details(
                        cluster_resource_id, metric_name, start_time, end_time, granularity
                    )
                    
                    if metric_data and metric_data.get('raw_data'):
                        azure_source['metrics'][metric_name] = metric_data
                        metadata['successful_metrics'] += 1
                        metadata['total_data_points'] += len(metric_data['raw_data'])
                        
                        self.logger.debug(f"✓ Collected {len(metric_data['raw_data'])} data points for {metric_name}")
                    else:
                        metadata['failed_metrics'] += 1
                        metadata['collection_errors'].append(f"{metric_name}: No data available")
                        self.logger.debug(f"✗ No data for {metric_name}")
                        
                except Exception as e:
                    metadata['failed_metrics'] += 1
                    metadata['collection_errors'].append(f"{metric_name}: {str(e)}")
                    self.logger.warning(f"Failed to collect {metric_name}: {e}")
                    continue
            
            azure_source['status'] = 'success' if metadata['successful_metrics'] > 0 else 'no_data'
            
            self.logger.info(
                f"Azure Monitor collection completed: {metadata['successful_metrics']}/{metadata['total_metrics_attempted']} metrics, "
                f"{metadata['total_data_points']} data points"
            )
            
        except Exception as e:
            azure_source['status'] = 'failed'
            metadata['collection_errors'].append(f"Collection failed: {str(e)}")
            self.logger.error(f"Azure Monitor collection failed: {e}")
    
    async def _collect_container_insights_data(self, cluster_name: str,
                                             start_time: datetime, end_time: datetime,
                                             comprehensive_metrics: Dict[str, Any]) -> None:
        """Collect Container Insights data with comprehensive queries."""
        ci_source = comprehensive_metrics['metrics_by_source']['container_insights']
        metadata = ci_source['collection_metadata']
        
        try:
            self.logger.info("Collecting Container Insights data...")
            hours = int((end_time - start_time).total_seconds() / 3600)
            
            # Define all Container Insights queries
            insights_queries = {
                'node_cpu_utilization': {
                    'description': 'Node CPU Utilization %',
                    'query': f"""
                    Perf
                    | where TimeGenerated >= ago({hours}h)
                    | where ObjectName == "K8SNode"
                    | where CounterName == "cpuUsageNanoCores"
                    | summarize 
                        AvgCPU = avg(CounterValue),
                        MaxCPU = max(CounterValue),
                        MinCPU = min(CounterValue),
                        DataPoints = count()
                    by Computer, bin(TimeGenerated, 5m)
                    | summarize 
                        OverallAvgCPU = avg(AvgCPU),
                        OverallMaxCPU = max(MaxCPU),
                        TotalDataPoints = sum(DataPoints)
                    """
                },
                'node_memory_utilization': {
                    'description': 'Node Memory Utilization %',
                    'query': f"""
                    Perf
                    | where TimeGenerated >= ago({hours}h)
                    | where ObjectName == "K8SNode"
                    | where CounterName == "memoryWorkingSetBytes"
                    | summarize 
                        AvgMemory = avg(CounterValue),
                        MaxMemory = max(CounterValue),
                        MinMemory = min(CounterValue),
                        DataPoints = count()
                    by Computer, bin(TimeGenerated, 5m)
                    | summarize 
                        OverallAvgMemory = avg(AvgMemory),
                        OverallMaxMemory = max(MaxMemory),
                        TotalDataPoints = sum(DataPoints)
                    """
                },
                'node_count_status': {
                    'description': 'Node Count and Status',
                    'query': f"""
                    KubeNodeInventory
                    | where TimeGenerated >= ago({hours}h)
                    | summarize 
                        TotalNodes = dcount(Computer),
                        ReadyNodes = dcountif(Computer, Status == "Ready"),
                        NotReadyNodes = dcountif(Computer, Status != "Ready")
                    | extend HealthPercentage = case(TotalNodes > 0, toreal(ReadyNodes) * 100.0 / toreal(TotalNodes), 0.0)
                    """
                },
                'pod_count_status': {
                    'description': 'Pod Count and Status',
                    'query': f"""
                    KubePodInventory
                    | where TimeGenerated >= ago({hours}h)
                    | summarize 
                        TotalPods = dcount(Name),
                        RunningPods = dcountif(Name, PodStatus == "Running"),
                        PendingPods = dcountif(Name, PodStatus == "Pending"),
                        FailedPods = dcountif(Name, PodStatus == "Failed"),
                        SucceededPods = dcountif(Name, PodStatus == "Succeeded")
                    | extend RunningPercentage = case(TotalPods > 0, toreal(RunningPods) * 100.0 / toreal(TotalPods), 0.0)
                    """
                },
                'namespace_breakdown': {
                    'description': 'Pod distribution by namespace',
                    'query': f"""
                    KubePodInventory
                    | where TimeGenerated >= ago({hours}h)
                    | summarize 
                        TotalPods = dcount(Name),
                        RunningPods = dcountif(Name, PodStatus == "Running"),
                        PendingPods = dcountif(Name, PodStatus == "Pending"),
                        FailedPods = dcountif(Name, PodStatus == "Failed"),
                        SucceededPods = dcountif(Name, PodStatus == "Succeeded")
                    by Namespace
                    | order by TotalPods desc
                    """
                },
                'container_performance': {
                    'description': 'Container CPU and Memory performance',
                    'query': f"""
                    Perf
                    | where TimeGenerated >= ago({hours}h)
                    | where ObjectName == "K8SContainer"
                    | where CounterName in ("cpuUsageNanoCores", "memoryWorkingSetBytes")
                    | summarize 
                        AvgValue = avg(CounterValue),
                        MaxValue = max(CounterValue),
                        DataPoints = count()
                    by CounterName, Computer, InstanceName
                    | summarize 
                        OverallAvgValue = avg(AvgValue),
                        OverallMaxValue = max(MaxValue),
                        TotalDataPoints = sum(DataPoints)
                    by CounterName
                    """
                },
                'node_capacity': {
                    'description': 'Node capacity and allocatable resources',
                    'query': f"""
                    KubeNodeInventory
                    | where TimeGenerated >= ago({hours}h)
                    | summarize arg_max(TimeGenerated, *) by Computer
                    | summarize 
                        TotalCpuCores = sum(AllocatableCpuCores),
                        TotalMemoryBytes = sum(AllocatableMemoryBytes),
                        AvgCpuCores = avg(AllocatableCpuCores),
                        AvgMemoryBytes = avg(AllocatableMemoryBytes),
                        NodeCount = count()
                    """
                },
                'container_restarts': {
                    'description': 'Container restart information',
                    'query': f"""
                    KubePodInventory
                    | where TimeGenerated >= ago({hours}h)
                    | summarize 
                        TotalRestarts = sum(RestartCount),
                        AvgRestarts = avg(RestartCount),
                        MaxRestarts = max(RestartCount),
                        PodsWithRestarts = dcountif(Name, RestartCount > 0)
                    """
                }
            }
            
            # Execute all queries
            for query_name, query_info in insights_queries.items():
                try:
                    self.logger.debug(f"Executing Container Insights query: {query_name}")
                    
                    result = await self._execute_log_query_safe(
                        query_info['query'], start_time, end_time, query_name
                    )
                    
                    if result:
                        ci_source['insights_data'][query_name] = {
                            'description': query_info['description'],
                            'data': result,
                            'query_executed': query_info['query'],
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        }
                        
                        metadata['successful_queries'] += 1
                        
                        # Count data points
                        if isinstance(result, list):
                            metadata['total_data_points'] += len(result)
                        elif isinstance(result, dict):
                            metadata['total_data_points'] += 1
                        
                        self.logger.debug(f"✓ Container Insights query '{query_name}' successful")
                    else:
                        metadata['failed_queries'] += 1
                        metadata['collection_errors'].append(f"{query_name}: No data returned")
                        self.logger.warning(f"✗ Container Insights query '{query_name}' returned no data")
                    
                    metadata['queries_executed'] += 1
                    
                except Exception as e:
                    metadata['failed_queries'] += 1
                    metadata['queries_executed'] += 1
                    metadata['collection_errors'].append(f"{query_name}: {str(e)}")
                    self.logger.error(f"Container Insights query '{query_name}' failed: {e}")
            
            ci_source['status'] = 'success' if metadata['successful_queries'] > 0 else 'no_data'
            
            self.logger.info(
                f"Container Insights collection completed: {metadata['successful_queries']}/{metadata['queries_executed']} queries successful, "
                f"{metadata['total_data_points']} data points"
            )
            
        except Exception as e:
            ci_source['status'] = 'failed'
            metadata['collection_errors'].append(f"Collection failed: {str(e)}")
            self.logger.error(f"Container Insights collection failed: {e}")
    
    async def _get_single_metric_with_details(self, resource_id: str, metric_name: str,
                                            start_time: datetime, end_time: datetime,
                                            granularity: timedelta) -> Optional[Dict[str, Any]]:
        """Get detailed metric data with metadata."""
        try:
            response = self._metrics_client.query_resource(
                resource_uri=resource_id,
                metric_names=[metric_name],
                timespan=(start_time, end_time),
                granularity=granularity,
                aggregations=[MetricAggregationType.AVERAGE, MetricAggregationType.MAXIMUM, MetricAggregationType.MINIMUM]
            )
            
            raw_data = []
            for metric in response.metrics:
                for timeseries in metric.timeseries:
                    for data_point in timeseries.data:
                        if any([data_point.average, data_point.maximum, data_point.minimum]):
                            raw_data.append({
                                'timestamp': data_point.timestamp.isoformat() if data_point.timestamp else None,
                                'average': float(data_point.average) if data_point.average is not None else None,
                                'maximum': float(data_point.maximum) if data_point.maximum is not None else None,
                                'minimum': float(data_point.minimum) if data_point.minimum is not None else None,
                                'metric_name': metric_name
                            })
            
            if raw_data:
                # Calculate aggregated values
                valid_averages = [point['average'] for point in raw_data if point['average'] is not None]
                valid_maximums = [point['maximum'] for point in raw_data if point['maximum'] is not None]
                valid_minimums = [point['minimum'] for point in raw_data if point['minimum'] is not None]
                
                return {
                    'metric_name': metric_name,
                    'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                    'data_points_count': len(raw_data),
                    'aggregated_values': {
                        'latest_average': valid_averages[-1] if valid_averages else None,
                        'latest_maximum': valid_maximums[-1] if valid_maximums else None,
                        'latest_minimum': valid_minimums[-1] if valid_minimums else None,
                        'overall_average': sum(valid_averages) / len(valid_averages) if valid_averages else None,
                        'overall_maximum': max(valid_maximums) if valid_maximums else None,
                        'overall_minimum': min(valid_minimums) if valid_minimums else None
                    },
                    'raw_data': raw_data,
                    'time_range': {
                        'start_time': start_time.isoformat(),
                        'end_time': end_time.isoformat(),
                        'granularity_minutes': granularity.total_seconds() / 60
                    }
                }
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Failed to get metric {metric_name}: {e}")
            return None
    
    async def _execute_log_query_safe(self, query: str, start_time: datetime, 
                                    end_time: datetime, query_name: str) -> Optional[Any]:
        """Execute Log Analytics query safely."""
        try:
            self.logger.debug(f"Executing {query_name} query")
            
            response = self._logs_client.query_workspace(
                workspace_id=self.log_analytics_workspace_id,
                query=query,
                timespan=(start_time, end_time)
            )
            
            if not response or not hasattr(response, 'tables') or not response.tables:
                return None
                
            table = response.tables[0]
            if not table.rows:
                return None
            
            # Get column names
            columns = []
            if hasattr(table, 'columns') and table.columns:
                for col in table.columns:
                    if hasattr(col, 'name'):
                        columns.append(col.name)
                    elif hasattr(col, 'column_name'):
                        columns.append(col.column_name)
                    else:
                        columns.append(f"column_{len(columns)}")
            else:
                columns = [f"column_{i}" for i in range(len(table.rows[0]))]
            
            if len(table.rows) == 1:
                return dict(zip(columns, table.rows[0]))
            else:
                return [dict(zip(columns, row)) for row in table.rows]
                
        except Exception as e:
            self.logger.error(f"Failed to execute {query_name} query: {e}")
            return None
    
    def _perform_cross_source_analysis(self, comprehensive_metrics: Dict[str, Any]) -> None:
        """Perform analysis across different data sources."""
        azure_source = comprehensive_metrics['metrics_by_source']['azure_monitor']
        ci_source = comprehensive_metrics['metrics_by_source']['container_insights']
        analysis = comprehensive_metrics['combined_analysis']
        
        # Calculate health score
        health_factors = []
        
        # Azure Monitor health contribution
        if azure_source['status'] == 'success':
            azure_health = (azure_source['collection_metadata']['successful_metrics'] / 
                          azure_source['collection_metadata']['total_metrics_attempted']) * 100
            health_factors.append(azure_health)
        
        # Container Insights health contribution
        if ci_source['status'] == 'success':
            ci_health = 100.0 if ci_source['collection_metadata']['successful_queries'] > 5 else 50.0
            health_factors.append(ci_health)
        
        analysis['health_score'] = sum(health_factors) / len(health_factors) if health_factors else 0.0
        
        # Data consistency score
        successful_sources = sum(1 for source in [azure_source, ci_source] if source['status'] == 'success')
        analysis['data_consistency_score'] = (successful_sources / 2) * 100
        
        # Generate recommendations
        recommendations = []
        if azure_source['status'] != 'success':
            recommendations.append("Azure Monitor metrics collection failed - check permissions and cluster configuration")
        if ci_source['status'] != 'success':
            recommendations.append("Container Insights data collection failed - verify Log Analytics workspace configuration")
        if analysis['health_score'] < 50:
            recommendations.append("Low metrics collection success rate - review monitoring configuration")
        
        analysis['recommendations'] = recommendations
        
        # Cross-validation between sources
        cross_validation = {}
        
        # Compare node counts if available from both sources
        azure_node_metrics = [name for name in azure_source.get('metrics', {}).keys() if 'node_' in name]
        ci_node_data = ci_source.get('insights_data', {}).get('node_count_status', {})
        
        if azure_node_metrics and ci_node_data:
            cross_validation['node_data_availability'] = {
                'azure_monitor_node_metrics': len(azure_node_metrics),
                'container_insights_node_data': bool(ci_node_data.get('data')),
                'consistency_check': 'available_from_both_sources'
            }
        
        analysis['cross_validation'] = cross_validation
    
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