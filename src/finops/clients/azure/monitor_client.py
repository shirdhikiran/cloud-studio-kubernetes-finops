"""Complete Azure Monitor client implementation for comprehensive metrics collection."""

from typing import Dict, Any, List, Optional, Union
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
    """Complete Azure Monitor client for comprehensive metrics collection."""
    
    def __init__(self, credential, subscription_id: str, config: Dict[str, Any]):
        super().__init__(config, "MonitorClient")
        self.credential = credential
        self.subscription_id = subscription_id
        self.log_analytics_workspace_id = config.get("log_analytics_workspace_id")
        
        self._monitor_client = None
        self._metrics_client = None
        self._logs_client = None
        
        # Azure Monitor metric definitions for AKS
        self.aks_cluster_metrics = [
            "node_cpu_usage_percentage",
            "node_memory_working_set_percentage", 
            "node_disk_usage_percentage",
            "node_network_in_bytes",
            "node_network_out_bytes",
            "kube_pod_status_ready",
            "kube_pod_status_phase",
            "cluster_autoscaler_cluster_safe_to_autoscale",
            "cluster_autoscaler_scale_down_in_cooldown",
            "cluster_autoscaler_unneeded_nodes_count"
        ]
        
        self.node_specific_metrics = [
            "node_cpu_usage_percentage",
            "node_memory_working_set_percentage",
            "node_disk_usage_percentage", 
            "node_network_in_bytes",
            "node_network_out_bytes",
            "node_filesystem_usage_bytes",
            "node_filesystem_avail_bytes"
        ]
        
        # Storage metrics
        self.storage_metrics = [
            "UsedCapacity",
            "Transactions", 
            "Ingress",
            "Egress",
            "SuccessServerLatency",
            "SuccessE2ELatency",
            "Availability"
        ]
        
        # Network metrics
        self.network_metrics = [
            "ByteCount",
            "PacketCount", 
            "SynCount",
            "VipAvailability",
            "DipAvailability"
        ]
        
        # Kusto queries for Log Analytics
        self.kusto_queries = {
            'node_inventory': '''
                KubeNodeInventory
                | where TimeGenerated >= ago({hours}h)
                | where ClusterName == '{cluster_name}'
                | distinct Computer, Status, KubeletVersion, KubeProxyVersion, CreationTimeStamp
                | order by Computer asc
            ''',
            
            'pod_metrics_by_node': '''
                KubePodInventory 
                | where TimeGenerated >= ago({hours}h)
                | where ClusterName == '{cluster_name}'
                | where Computer == '{node_name}'
                | join kind=inner (
                    Perf
                    | where ObjectName == 'K8SContainer'
                    | where CounterName in ('cpuUsageNanoCores', 'memoryWorkingSetBytes')
                    | where TimeGenerated >= ago({hours}h)
                    | summarize 
                        AvgCPU = avg(CounterValue),
                        MaxCPU = max(CounterValue),
                        AvgMemory = avg(CounterValue),
                        MaxMemory = max(CounterValue)
                    by InstanceName, CounterName, bin(TimeGenerated, 5m)
                ) on $left.PodUid == $right.InstanceName
                | project TimeGenerated, PodName = Name, Namespace, CounterName, AvgCPU, MaxCPU, AvgMemory, MaxMemory
                | order by TimeGenerated desc
            ''',
            
            'node_performance': '''
                Perf
                | where TimeGenerated >= ago({hours}h)
                | where ObjectName == 'K8SNode'
                | where Computer == '{node_name}'
                | where CounterName in (
                    'cpuUsageNanoCores', 'memoryWorkingSetBytes', 'cpuCapacityNanoCores', 
                    'memoryCapacityBytes', 'fsUsedBytes', 'fsAvailBytes'
                )
                | summarize 
                    AvgValue = avg(CounterValue),
                    MaxValue = max(CounterValue),
                    MinValue = min(CounterValue),
                    Count = count()
                by CounterName, bin(TimeGenerated, {granularity_minutes}m)
                | order by TimeGenerated desc
            ''',
            
            'pod_resource_requests_limits': '''
                KubePodInventory
                | where TimeGenerated >= ago({hours}h) 
                | where ClusterName == '{cluster_name}'
                | join kind=inner (
                    KubeServices
                    | project ServiceName, ServiceNamespace = Namespace, PodLabel = SelectorLabels
                ) on $left.Namespace == $right.ServiceNamespace
                | project 
                    PodName = Name,
                    Namespace,
                    Computer,
                    PodStatus,
                    PodCreationTimeStamp,
                    ContainerName,
                    ContainerID,
                    PodRestartCount
                | limit 1000
            ''',
            
            'cluster_events': '''
                KubeEvents
                | where TimeGenerated >= ago({hours}h)
                | where ClusterName == '{cluster_name}'
                | where Type == 'Warning' or Type == 'Error'
                | summarize 
                    EventCount = count(),
                    LastSeen = max(TimeGenerated),
                    FirstSeen = min(TimeGenerated)
                by ObjectKind, Reason, Message
                | order by EventCount desc
            ''',
            
            'storage_usage': '''
                InsightsMetrics
                | where TimeGenerated >= ago({hours}h)
                | where Namespace == 'container.azm.ms/disk'
                | where Name in ('used_bytes', 'free_bytes', 'used_percent')
                | extend Tags = todynamic(Tags)
                | extend ClusterName = tostring(Tags['container.azm.ms/clusterId'])
                | where ClusterName contains '{cluster_name}'
                | summarize 
                    AvgValue = avg(Val),
                    MaxValue = max(Val),
                    MinValue = min(Val)
                by Name, bin(TimeGenerated, {granularity_minutes}m)
                | order by TimeGenerated desc
            ''',
            
            'network_usage': '''
                InsightsMetrics
                | where TimeGenerated >= ago({hours}h)
                | where Namespace == 'container.azm.ms/network'
                | where Name in ('rx_bytes', 'tx_bytes', 'rx_dropped', 'tx_dropped')
                | extend Tags = todynamic(Tags)
                | extend ClusterName = tostring(Tags['container.azm.ms/clusterId'])
                | where ClusterName contains '{cluster_name}'
                | summarize 
                    AvgValue = avg(Val),
                    MaxValue = max(Val),
                    TotalValue = sum(Val)
                by Name, bin(TimeGenerated, {granularity_minutes}m)
                | order by TimeGenerated desc
            '''
        }
    
    async def connect(self) -> None:
        """Connect to Azure Monitor services."""
        try:
            self._monitor_client = MonitorManagementClient(
                credential=self.credential,
                subscription_id=self.subscription_id
            )
            
            self._metrics_client = MetricsQueryClient(
                credential=self.credential
            )
            
            if self.log_analytics_workspace_id:
                self._logs_client = LogsQueryClient(
                    credential=self.credential
                )
                self.logger.info("Log Analytics client initialized successfully")
            else:
                self.logger.warning("Log Analytics workspace ID not provided - pod-level metrics will be limited")
            
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
            if client:
                try:
                    if hasattr(client, 'close'):
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
            
            # Try to list metric definitions as a health check
            test_resource_id = f"/subscriptions/{self.subscription_id}/resourceGroups/test/providers/Microsoft.Compute/virtualMachines/test"
            try:
                # This will fail for non-existent resource but validates API connectivity
                list(self._monitor_client.metric_definitions.list(test_resource_id))
            except ResourceNotFoundError:
                # Expected for non-existent resource - indicates API is accessible
                pass
            except ClientAuthenticationError:
                return False
            
            return True
            
        except Exception as e:
            self.logger.warning("Azure Monitor health check failed", error=str(e))
            return False
    
    @retry_with_backoff(max_retries=3)
    async def get_cluster_metrics(self,
                                cluster_resource_id: str,
                                start_time: Optional[datetime] = None,
                                end_time: Optional[datetime] = None,
                                granularity: timedelta = timedelta(minutes=5)) -> Dict[str, Any]:
        """Get comprehensive cluster-level metrics."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        self.logger.info(f"Collecting cluster metrics from {start_time} to {end_time}")
        
        cluster_metrics = {
            'cpu_metrics': {},
            'memory_metrics': {},
            'disk_metrics': {},
            'network_metrics': {},
            'pod_metrics': {},
            'autoscaler_metrics': {},
            'collection_metadata': {
                'resource_id': cluster_resource_id,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'granularity_minutes': granularity.total_seconds() / 60,
                'metrics_collected': [],
                'metrics_failed': []
            }
        }
        
        # Collect all cluster metrics in parallel
        metric_tasks = []
        for metric_name in self.aks_cluster_metrics:
            task = self._get_single_metric_data(cluster_resource_id, metric_name, start_time, end_time, granularity)
            metric_tasks.append((metric_name, task))
        
        # Execute all metric collection tasks
        metric_results = await asyncio.gather(*[task for _, task in metric_tasks], return_exceptions=True)
        
        # Process results
        for i, (metric_name, result) in enumerate(zip([name for name, _ in metric_tasks], metric_results)):
            if isinstance(result, Exception):
                self.logger.warning(f"Failed to collect metric {metric_name}: {result}")
                cluster_metrics['collection_metadata']['metrics_failed'].append(metric_name)
            elif result:
                # Categorize metrics
                if 'cpu' in metric_name:
                    cluster_metrics['cpu_metrics'][metric_name] = result
                elif 'memory' in metric_name:
                    cluster_metrics['memory_metrics'][metric_name] = result
                elif 'disk' in metric_name:
                    cluster_metrics['disk_metrics'][metric_name] = result
                elif 'network' in metric_name:
                    cluster_metrics['network_metrics'][metric_name] = result
                elif 'pod' in metric_name:
                    cluster_metrics['pod_metrics'][metric_name] = result
                elif 'autoscaler' in metric_name:
                    cluster_metrics['autoscaler_metrics'][metric_name] = result
                
                cluster_metrics['collection_metadata']['metrics_collected'].append(metric_name)
        
        # Add summary statistics
        cluster_metrics['summary'] = self._calculate_cluster_summary(cluster_metrics)
        
        return cluster_metrics
    
    @retry_with_backoff(max_retries=3)
    async def get_individual_node_metrics(self,
                                        cluster_name: str,
                                        start_time: Optional[datetime] = None,
                                        end_time: Optional[datetime] = None,
                                        granularity_minutes: int = 5) -> Dict[str, Any]:
        """Get individual node metrics without aggregation."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        self.logger.info(f"Collecting individual node metrics for cluster: {cluster_name}")
        
        node_metrics = {
            'nodes': {},
            'collection_metadata': {
                'cluster_name': cluster_name,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'granularity_minutes': granularity_minutes,
                'nodes_discovered': 0,
                'nodes_with_data': 0,
                'collection_method': 'log_analytics',
                'fallback_used': False
            }
        }
        
        try:
            # Check if Log Analytics is available
            if not self._logs_client or not self.log_analytics_workspace_id:
                self.logger.warning("Log Analytics not available, using fallback method")
                return await self._get_node_metrics_fallback(cluster_name, start_time, end_time, granularity_minutes)
            
            # First, get list of nodes
            hours = int((end_time - start_time).total_seconds() / 3600)
            node_inventory_query = self.kusto_queries['node_inventory'].format(
                hours=hours,
                cluster_name=cluster_name
            )
            
            node_inventory_response = await self._execute_kusto_query(
                node_inventory_query, start_time, end_time
            )
            
            if not node_inventory_response:
                self.logger.warning("No node inventory data available, using fallback")
                return await self._get_node_metrics_fallback(cluster_name, start_time, end_time, granularity_minutes)
            
            # Parse node inventory
            nodes = self._parse_node_inventory(node_inventory_response)
            node_metrics['collection_metadata']['nodes_discovered'] = len(nodes)
            
            if not nodes:
                self.logger.warning("No nodes found in inventory, using fallback")
                return await self._get_node_metrics_fallback(cluster_name, start_time, end_time, granularity_minutes)
            
            # Collect metrics for each node individually
            for node_info in nodes:
                node_name = node_info.get('computer', '')
                if not node_name:
                    continue
                    
                self.logger.debug(f"Collecting metrics for node: {node_name}")
                
                try:
                    # Get node performance metrics
                    node_perf_data = await self._get_node_performance_metrics(
                        cluster_name, node_name, start_time, end_time, granularity_minutes
                    )
                    
                    # Get pod metrics for this node
                    node_pod_data = await self._get_node_pod_metrics(
                        cluster_name, node_name, start_time, end_time
                    )
                    
                    # Combine node data
                    node_metrics['nodes'][node_name] = {
                        'node_info': node_info,
                        'performance_metrics': node_perf_data,
                        'pod_metrics': node_pod_data,
                        'resource_utilization': self._calculate_node_utilization(node_perf_data),
                        'efficiency_scores': self._calculate_node_efficiency(node_perf_data),
                        'collection_status': 'success'
                    }
                    
                    node_metrics['collection_metadata']['nodes_with_data'] += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to collect metrics for node {node_name}: {e}")
                    node_metrics['nodes'][node_name] = {
                        'node_info': node_info,
                        'error': str(e),
                        'collection_status': 'failed'
                    }
            
            return node_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to collect individual node metrics: {e}")
            self.logger.info("Attempting fallback method")
            return await self._get_node_metrics_fallback(cluster_name, start_time, end_time, granularity_minutes)
    
    async def _get_node_metrics_fallback(self,
                                       cluster_name: str,
                                       start_time: datetime,
                                       end_time: datetime,
                                       granularity_minutes: int) -> Dict[str, Any]:
        """Fallback method for node metrics when Log Analytics is not available."""
        self.logger.info("Using fallback method for node metrics collection")
        
        return {
            'nodes': {},
            'collection_metadata': {
                'cluster_name': cluster_name,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'granularity_minutes': granularity_minutes,
                'nodes_discovered': 0,
                'nodes_with_data': 0,
                'collection_method': 'fallback',
                'fallback_used': True,
                'fallback_reason': 'log_analytics_unavailable'
            },
            'fallback_info': {
                'message': 'Individual node metrics require Log Analytics workspace configuration',
                'requirements': [
                    'Configure Log Analytics workspace ID',
                    'Enable Azure Monitor for Containers addon',
                    'Ensure proper RBAC permissions for Log Analytics'
                ],
                'alternative_methods': [
                    'Use Azure Monitor cluster-level metrics',
                    'Configure Prometheus for detailed node metrics',
                    'Use kubectl top nodes for basic utilization'
                ]
            }
        }
    
    @retry_with_backoff(max_retries=3)
    async def get_storage_metrics(self,
                                cluster_resource_id: str,
                                start_time: Optional[datetime] = None,
                                end_time: Optional[datetime] = None,
                                granularity: timedelta = timedelta(minutes=5)) -> Dict[str, Any]:
        """Get comprehensive storage metrics."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        storage_metrics = {
            'cluster_storage_metrics': {},
            'persistent_volume_metrics': {},
            'storage_class_analysis': {},
            'collection_metadata': {
                'resource_id': cluster_resource_id,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
        }
        
        try:
            # Get cluster storage metrics from Log Analytics
            if self._logs_client:
                cluster_name = cluster_resource_id.split('/')[-1]
                hours = int((end_time - start_time).total_seconds() / 3600)
                granularity_minutes = int(granularity.total_seconds() / 60)
                
                storage_query = self.kusto_queries['storage_usage'].format(
                    hours=hours,
                    cluster_name=cluster_name,
                    granularity_minutes=granularity_minutes
                )
                
                storage_response = await self._execute_kusto_query(storage_query, start_time, end_time)
                if storage_response:
                    storage_metrics['cluster_storage_metrics'] = self._parse_storage_metrics(storage_response)
            
            # Get storage account metrics if available
            storage_account_metrics = await self._get_related_storage_account_metrics(
                cluster_resource_id, start_time, end_time, granularity
            )
            if storage_account_metrics:
                storage_metrics['storage_account_metrics'] = storage_account_metrics
            
            return storage_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to collect storage metrics: {e}")
            return storage_metrics
    
    @retry_with_backoff(max_retries=3)
    async def get_network_metrics(self,
                                cluster_resource_id: str,
                                start_time: Optional[datetime] = None,
                                end_time: Optional[datetime] = None,
                                granularity: timedelta = timedelta(minutes=5)) -> Dict[str, Any]:
        """Get comprehensive network metrics."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        network_metrics = {
            'cluster_network_metrics': {},
            'ingress_metrics': {},
            'egress_metrics': {},
            'load_balancer_metrics': {},
            'collection_metadata': {
                'resource_id': cluster_resource_id,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
        }
        
        try:
            # Get cluster network metrics from Log Analytics
            if self._logs_client:
                cluster_name = cluster_resource_id.split('/')[-1]
                hours = int((end_time - start_time).total_seconds() / 3600)
                granularity_minutes = int(granularity.total_seconds() / 60)
                
                network_query = self.kusto_queries['network_usage'].format(
                    hours=hours,
                    cluster_name=cluster_name,
                    granularity_minutes=granularity_minutes
                )
                
                network_response = await self._execute_kusto_query(network_query, start_time, end_time)
                if network_response:
                    parsed_network = self._parse_network_metrics(network_response)
                    network_metrics['cluster_network_metrics'] = parsed_network
            
            # Get load balancer metrics
            lb_metrics = await self._get_load_balancer_metrics(
                cluster_resource_id, start_time, end_time, granularity
            )
            if lb_metrics:
                network_metrics['load_balancer_metrics'] = lb_metrics
            
            return network_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to collect network metrics: {e}")
            return network_metrics
    
    @retry_with_backoff(max_retries=3)
    async def get_pod_metrics_detailed(self,
                                     cluster_name: str,
                                     start_time: Optional[datetime] = None,
                                     end_time: Optional[datetime] = None,
                                     namespace_filter: Optional[str] = None) -> Dict[str, Any]:
        """Get detailed pod metrics with resource consumption analysis."""
        if not self._logs_client or not self.log_analytics_workspace_id:
            self.logger.warning("Log Analytics not configured")
            return {}
        
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        pod_metrics = {
            'pod_summary': {},
            'resource_requests_limits': {},
            'top_cpu_consumers': [],
            'top_memory_consumers': [],
            'pod_restart_analysis': [],
            'namespace_analysis': {},
            'collection_metadata': {
                'cluster_name': cluster_name,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'namespace_filter': namespace_filter
            }
        }
        
        try:
            hours = int((end_time - start_time).total_seconds() / 3600)
            
            # Get pod resource requests and limits
            requests_limits_query = self.kusto_queries['pod_resource_requests_limits'].format(
                hours=hours,
                cluster_name=cluster_name
            )
            
            requests_response = await self._execute_kusto_query(requests_limits_query, start_time, end_time)
            if requests_response:
                pod_metrics['resource_requests_limits'] = self._parse_pod_requests_limits(requests_response)
            
            # Get top CPU consumers
            cpu_consumers = await self._get_top_resource_consuming_pods(
                cluster_name, start_time, end_time, 'cpu', 20
            )
            pod_metrics['top_cpu_consumers'] = cpu_consumers
            
            # Get top memory consumers
            memory_consumers = await self._get_top_resource_consuming_pods(
                cluster_name, start_time, end_time, 'memory', 20
            )
            pod_metrics['top_memory_consumers'] = memory_consumers
            
            # Analyze by namespace
            namespace_analysis = self._analyze_by_namespace(cpu_consumers, memory_consumers)
            pod_metrics['namespace_analysis'] = namespace_analysis
            
            return pod_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to get detailed pod metrics: {e}")
            return pod_metrics
    
    @retry_with_backoff(max_retries=3)
    async def get_cluster_health_metrics(self,
                                       cluster_resource_id: str,
                                       start_time: Optional[datetime] = None,
                                       end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Get comprehensive cluster health metrics."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        health_metrics = {
            'cluster_events': {},
            'node_status': {},
            'pod_health': {},
            'resource_quotas': {},
            'collection_metadata': {
                'resource_id': cluster_resource_id,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
        }
        
        try:
            if self._logs_client:
                cluster_name = cluster_resource_id.split('/')[-1]
                hours = int((end_time - start_time).total_seconds() / 3600)
                
                # Get cluster events
                events_query = self.kusto_queries['cluster_events'].format(
                    hours=hours,
                    cluster_name=cluster_name
                )
                
                events_response = await self._execute_kusto_query(events_query, start_time, end_time)
                if events_response:
                    health_metrics['cluster_events'] = self._parse_cluster_events(events_response)
            
            return health_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to get cluster health metrics: {e}")
            return health_metrics
    
    # Helper methods for metric processing
    async def _get_single_metric_data(self,
                                    resource_id: str,
                                    metric_name: str,
                                    start_time: datetime,
                                    end_time: datetime,
                                    granularity: timedelta) -> List[Dict[str, Any]]:
        """Get data for a single metric."""
        try:
            if not self._metrics_client:
                return []
            
            response = self._metrics_client.query_resource(
                resource_uri=resource_id,
                metric_names=[metric_name],
                timespan=(start_time, end_time),
                granularity=granularity,
                aggregations=[
                    MetricAggregationType.AVERAGE, 
                    MetricAggregationType.MAXIMUM, 
                    MetricAggregationType.MINIMUM,
                    MetricAggregationType.TOTAL,
                    MetricAggregationType.COUNT
                ]
            )
            
            metric_data = []
            for metric in response.metrics:
                for timeseries in metric.timeseries:
                    for data_point in timeseries.data:
                        metric_data.append({
                            'timestamp': data_point.timestamp.isoformat() if data_point.timestamp else None,
                            'average': float(data_point.average) if data_point.average is not None else None,
                            'maximum': float(data_point.maximum) if data_point.maximum is not None else None,
                            'minimum': float(data_point.minimum) if data_point.minimum is not None else None,
                            'total': float(data_point.total) if data_point.total is not None else None,
                            'count': int(data_point.count) if data_point.count is not None else None,
                            'metric_name': metric_name,
                            'unit': metric.unit.value if metric.unit else None,
                            'resource_id': resource_id
                        })
            
            return metric_data
            
        except Exception as e:
            self.logger.debug(f"Error getting metric {metric_name}: {e}")
            return []
    
    async def _get_node_performance_metrics(self,
                                          cluster_name: str,
                                          node_name: str,
                                          start_time: datetime,
                                          end_time: datetime,
                                          granularity_minutes: int) -> Dict[str, Any]:
        """Get performance metrics for a specific node."""
        try:
            hours = int((end_time - start_time).total_seconds() / 3600)
            
            node_perf_query = self.kusto_queries['node_performance'].format(
                hours=hours,
                node_name=node_name,
                granularity_minutes=granularity_minutes
            )
            
            response = await self._execute_kusto_query(node_perf_query, start_time, end_time)
            
            if response:
                return self._parse_node_performance_data(response)
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Failed to get node performance metrics for {node_name}: {e}")
            return {}
    
    async def _get_node_pod_metrics(self,
                                  cluster_name: str,
                                  node_name: str,
                                  start_time: datetime,
                                  end_time: datetime) -> Dict[str, Any]:
        """Get pod metrics for a specific node."""
        try:
            hours = int((end_time - start_time).total_seconds() / 3600)
            
            pod_metrics_query = self.kusto_queries['pod_metrics_by_node'].format(
                hours=hours,
                cluster_name=cluster_name,
                node_name=node_name
            )
            
            response = await self._execute_kusto_query(pod_metrics_query, start_time, end_time)
            
            if response:
                return self._parse_pod_metrics_data(response)
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Failed to get pod metrics for node {node_name}: {e}")
            return {}
    
    async def _get_top_resource_consuming_pods(self,
                                             cluster_name: str,
                                             start_time: datetime,
                                             end_time: datetime,
                                             resource_type: str,
                                             limit: int = 20) -> List[Dict[str, Any]]:
        """Get top resource-consuming pods."""
        try:
            hours = int((end_time - start_time).total_seconds() / 3600)
            
            if resource_type == 'cpu':
                counter_name = 'cpuUsageNanoCores'
                resource_field = 'AvgCPU'
            else:
                counter_name = 'memoryWorkingSetBytes'
                resource_field = 'AvgMemory'
            
            query = f'''
                KubePodInventory
                | where TimeGenerated >= ago({hours}h)
                | where ClusterName == '{cluster_name}'
                | join kind=inner (
                    Perf
                    | where ObjectName == 'K8SContainer'
                    | where CounterName == '{counter_name}'
                    | where TimeGenerated >= ago({hours}h)
                    | summarize {resource_field} = avg(CounterValue) by InstanceName
                ) on $left.PodUid == $right.InstanceName
                | top {limit} by {resource_field} desc
                | project PodName = Name, Namespace, {resource_field}, Node = Computer, PodStatus, ContainerName
            '''
            
            response = await self._execute_kusto_query(query, start_time, end_time)
            
            if response:
                return self._parse_top_consuming_pods(response, resource_type)
            
            return []
            
        except Exception as e:
            self.logger.error(f"Failed to get top {resource_type} consuming pods: {e}")
            return []
    
    async def _get_related_storage_account_metrics(self,
                                                 cluster_resource_id: str,
                                                 start_time: datetime,
                                                 end_time: datetime,
                                                 granularity: timedelta) -> Dict[str, Any]:
        """Get metrics for storage accounts related to the cluster."""
        try:
            # Extract resource group from cluster resource ID
            resource_group = cluster_resource_id.split('/')[4]
            
            # Build storage account resource IDs (this is a simplified approach)
            # In practice, you'd need to discover storage accounts used by the cluster
            storage_accounts = await self._discover_cluster_storage_accounts(resource_group)
            
            storage_metrics = {}
            
            for storage_account in storage_accounts:
                storage_resource_id = f"/subscriptions/{self.subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{storage_account}"
                
                account_metrics = {}
                for metric_name in self.storage_metrics:
                    metric_data = await self._get_single_metric_data(
                        storage_resource_id, metric_name, start_time, end_time, granularity
                    )
                    if metric_data:
                        account_metrics[metric_name] = metric_data
                
                if account_metrics:
                    storage_metrics[storage_account] = account_metrics
            
            return storage_metrics
            
        except Exception as e:
            self.logger.debug(f"Failed to get storage account metrics: {e}")
            return {}
    
    async def _get_load_balancer_metrics(self,
                                       cluster_resource_id: str,
                                       start_time: datetime,
                                       end_time: datetime,
                                       granularity: timedelta) -> Dict[str, Any]:
        """Get load balancer metrics for the cluster."""
        try:
            # Extract resource group from cluster resource ID
            resource_group = cluster_resource_id.split('/')[4]
            
            # Discover load balancers used by the cluster
            load_balancers = await self._discover_cluster_load_balancers(resource_group)
            
            lb_metrics = {}
            
            for lb_name in load_balancers:
                lb_resource_id = f"/subscriptions/{self.subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Network/loadBalancers/{lb_name}"
                
                lb_data = {}
                for metric_name in self.network_metrics:
                    metric_data = await self._get_single_metric_data(
                        lb_resource_id, metric_name, start_time, end_time, granularity
                    )
                    if metric_data:
                        lb_data[metric_name] = metric_data
                
                if lb_data:
                    lb_metrics[lb_name] = lb_data
            
            return lb_metrics
            
        except Exception as e:
            self.logger.debug(f"Failed to get load balancer metrics: {e}")
            return {}
    
    async def _execute_kusto_query(self, query: str, start_time: datetime, end_time: datetime) -> Any:
        """Execute a Kusto query against Log Analytics."""
        try:
            if not self._logs_client or not self.log_analytics_workspace_id:
                return None
            
            response = self._logs_client.query_workspace(
                workspace_id=self.log_analytics_workspace_id,
                query=query,
                timespan=(start_time, end_time)
            )
            
            return response
            
        except Exception as e:
            self.logger.error(f"Failed to execute Kusto query: {e}")
            self.logger.debug(f"Query: {query}")
            return None
    
    async def _discover_cluster_storage_accounts(self, resource_group: str) -> List[str]:
        """Discover storage accounts used by the cluster."""
        try:
            # This is a simplified implementation
            # In practice, you'd query the cluster's storage classes and persistent volumes
            # to identify the actual storage accounts being used
            
            # For now, return common storage account naming patterns
            return []  # Would contain actual storage account names
            
        except Exception as e:
            self.logger.debug(f"Failed to discover storage accounts: {e}")
            return []
    
    async def _discover_cluster_load_balancers(self, resource_group: str) -> List[str]:
        """Discover load balancers used by the cluster."""
        try:
            # This is a simplified implementation
            # In practice, you'd query the cluster's services to identify load balancers
            
            # For now, return common load balancer naming patterns
            return []  # Would contain actual load balancer names
            
        except Exception as e:
            self.logger.debug(f"Failed to discover load balancers: {e}")
            return []
    
    # Data parsing methods
    def _parse_node_inventory(self, response) -> List[Dict[str, Any]]:
        """Parse node inventory response."""
        nodes = []
        
        if response and hasattr(response, 'tables') and response.tables and len(response.tables) > 0:
            table = response.tables[0]
            
            # Safely extract column names
            columns = []
            if hasattr(table, 'columns') and table.columns:
                for col in table.columns:
                    if hasattr(col, 'name'):
                        columns.append(col.name)
                    elif isinstance(col, str):
                        columns.append(col)
                    else:
                        columns.append(str(col))
            
            # Process rows
            if hasattr(table, 'rows') and table.rows:
                for row in table.rows:
                    row_dict = {}
                    row_data = row if isinstance(row, (list, tuple)) else [row]
                    
                    for idx, value in enumerate(row_data):
                        if idx < len(columns):
                            row_dict[columns[idx]] = value
                    
                    node_info = {
                        'computer': row_dict.get('Computer', ''),
                        'status': row_dict.get('Status', 'Unknown'),
                        'kubelet_version': row_dict.get('KubeletVersion', ''),
                        'kube_proxy_version': row_dict.get('KubeProxyVersion', ''),
                        'creation_timestamp': row_dict.get('CreationTimeStamp', '')
                    }
                    
                    # Only add if we have at least the computer name
                    if node_info['computer']:
                        nodes.append(node_info)
        
        return nodes
    
    def _parse_node_performance_data(self, response) -> Dict[str, Any]:
        """Parse node performance data."""
        performance_data = {
            'cpu_usage': [],
            'memory_usage': [],
            'cpu_capacity': [],
            'memory_capacity': [],
            'filesystem_usage': [],
            'filesystem_available': []
        }
        
        if response and hasattr(response, 'tables') and response.tables and len(response.tables) > 0:
            table = response.tables[0]
            
            # Safely extract column names
            columns = []
            if hasattr(table, 'columns') and table.columns:
                for col in table.columns:
                    if hasattr(col, 'name'):
                        columns.append(col.name)
                    elif isinstance(col, str):
                        columns.append(col)
                    else:
                        columns.append(str(col))
            
            # Process rows
            if hasattr(table, 'rows') and table.rows:
                for row in table.rows:
                    row_dict = {}
                    row_data = row if isinstance(row, (list, tuple)) else [row]
                    
                    for idx, value in enumerate(row_data):
                        if idx < len(columns):
                            row_dict[columns[idx]] = value
                    
                    counter_name = row_dict.get('CounterName', '')
                    data_point = {
                        'timestamp': row_dict.get('TimeGenerated'),
                        'average': float(row_dict.get('AvgValue', 0)) if row_dict.get('AvgValue') is not None else 0,
                        'maximum': float(row_dict.get('MaxValue', 0)) if row_dict.get('MaxValue') is not None else 0,
                        'minimum': float(row_dict.get('MinValue', 0)) if row_dict.get('MinValue') is not None else 0,
                        'count': int(row_dict.get('Count', 0)) if row_dict.get('Count') is not None else 0
                    }
                    
                    if 'cpuUsageNanoCores' in counter_name:
                        performance_data['cpu_usage'].append(data_point)
                    elif 'memoryWorkingSetBytes' in counter_name:
                        performance_data['memory_usage'].append(data_point)
                    elif 'cpuCapacityNanoCores' in counter_name:
                        performance_data['cpu_capacity'].append(data_point)
                    elif 'memoryCapacityBytes' in counter_name:
                        performance_data['memory_capacity'].append(data_point)
                    elif 'fsUsedBytes' in counter_name:
                        performance_data['filesystem_usage'].append(data_point)
                    elif 'fsAvailBytes' in counter_name:
                        performance_data['filesystem_available'].append(data_point)
        
        return performance_data
    
    def _parse_pod_metrics_data(self, response) -> Dict[str, Any]:
        """Parse pod metrics data for a node."""
        pod_data = {
            'pods': [],
            'summary': {
                'total_pods': 0,
                'total_cpu_usage': 0,
                'total_memory_usage': 0,
                'avg_cpu_per_pod': 0,
                'avg_memory_per_pod': 0
            }
        }
        
        if response and hasattr(response, 'tables') and response.tables and len(response.tables) > 0:
            table = response.tables[0]
            
            # Safely extract column names
            columns = []
            if hasattr(table, 'columns') and table.columns:
                for col in table.columns:
                    if hasattr(col, 'name'):
                        columns.append(col.name)
                    elif isinstance(col, str):
                        columns.append(col)
                    else:
                        columns.append(str(col))
            
            pods_seen = set()
            total_cpu = 0
            total_memory = 0
            
            # Process rows
            if hasattr(table, 'rows') and table.rows:
                for row in table.rows:
                    row_dict = {}
                    row_data = row if isinstance(row, (list, tuple)) else [row]
                    
                    for idx, value in enumerate(row_data):
                        if idx < len(columns):
                            row_dict[columns[idx]] = value
                    
                    pod_name = row_dict.get('PodName', '')
                    namespace = row_dict.get('Namespace', '')
                    pod_key = f"{namespace}/{pod_name}"
                    
                    if pod_key not in pods_seen and pod_name:
                        pods_seen.add(pod_key)
                        
                        # Safely convert numeric values
                        cpu_usage = 0
                        memory_usage = 0
                        
                        try:
                            cpu_val = row_dict.get('AvgCPU')
                            if cpu_val is not None:
                                cpu_usage = float(cpu_val)
                        except (ValueError, TypeError):
                            cpu_usage = 0
                        
                        try:
                            memory_val = row_dict.get('AvgMemory')
                            if memory_val is not None:
                                memory_usage = float(memory_val)
                        except (ValueError, TypeError):
                            memory_usage = 0
                        
                        pod_info = {
                            'pod_name': pod_name,
                            'namespace': namespace,
                            'cpu_usage_nanocores': cpu_usage,
                            'cpu_usage_millicores': cpu_usage / 1000000 if cpu_usage else 0,
                            'memory_usage_bytes': memory_usage,
                            'memory_usage_mb': memory_usage / (1024 * 1024) if memory_usage else 0,
                            'timestamp': row_dict.get('TimeGenerated')
                        }
                        
                        pod_data['pods'].append(pod_info)
                        total_cpu += cpu_usage
                        total_memory += memory_usage
            
            # Calculate summary
            pod_count = len(pods_seen)
            pod_data['summary'] = {
                'total_pods': pod_count,
                'total_cpu_usage': total_cpu,
                'total_memory_usage': total_memory,
                'avg_cpu_per_pod': total_cpu / pod_count if pod_count > 0 else 0,
                'avg_memory_per_pod': total_memory / pod_count if pod_count > 0 else 0
            }
        
        return pod_data
    
    def _parse_top_consuming_pods(self, response, resource_type: str) -> List[Dict[str, Any]]:
        """Parse top consuming pods response."""
        pods = []
        
        if response and hasattr(response, 'tables') and response.tables and len(response.tables) > 0:
            table = response.tables[0]
            
            # Safely extract column names
            columns = []
            if hasattr(table, 'columns') and table.columns:
                for col in table.columns:
                    if hasattr(col, 'name'):
                        columns.append(col.name)
                    elif isinstance(col, str):
                        columns.append(col)
                    else:
                        columns.append(str(col))
            
            # Process rows
            if hasattr(table, 'rows') and table.rows:
                for row in table.rows:
                    row_dict = {}
                    row_data = row if isinstance(row, (list, tuple)) else [row]
                    
                    for idx, value in enumerate(row_data):
                        if idx < len(columns):
                            row_dict[columns[idx]] = value
                    
                    pod_info = {
                        'pod_name': row_dict.get('PodName', ''),
                        'namespace': row_dict.get('Namespace', ''),
                        'node': row_dict.get('Node', ''),
                        'pod_status': row_dict.get('PodStatus', ''),
                        'container_name': row_dict.get('ContainerName', ''),
                        'resource_type': resource_type
                    }
                    
                    if resource_type == 'cpu':
                        try:
                            cpu_value = float(row_dict.get('AvgCPU', 0)) if row_dict.get('AvgCPU') is not None else 0
                            pod_info['avg_cpu_nanocores'] = cpu_value
                            pod_info['avg_cpu_millicores'] = cpu_value / 1000000 if cpu_value else 0
                        except (ValueError, TypeError):
                            pod_info['avg_cpu_nanocores'] = 0
                            pod_info['avg_cpu_millicores'] = 0
                    else:
                        try:
                            memory_value = float(row_dict.get('AvgMemory', 0)) if row_dict.get('AvgMemory') is not None else 0
                            pod_info['avg_memory_bytes'] = memory_value
                            pod_info['avg_memory_mb'] = memory_value / (1024 * 1024) if memory_value else 0
                        except (ValueError, TypeError):
                            pod_info['avg_memory_bytes'] = 0
                            pod_info['avg_memory_mb'] = 0
                    
                    # Only add if we have meaningful data
                    if pod_info['pod_name']:
                        pods.append(pod_info)
        
        return pods
    
    def _parse_storage_metrics(self, response) -> Dict[str, Any]:
        """Parse storage metrics response."""
        storage_data = {
            'disk_usage': [],
            'disk_available': [],
            'disk_utilization_percent': [],
            'summary': {
                'avg_usage_percent': 0,
                'max_usage_percent': 0,
                'total_used_bytes': 0,
                'total_available_bytes': 0
            }
        }
        
        if response and hasattr(response, 'tables') and response.tables and len(response.tables) > 0:
            table = response.tables[0]
            
            # Safely extract column names
            columns = []
            if hasattr(table, 'columns') and table.columns:
                for col in table.columns:
                    if hasattr(col, 'name'):
                        columns.append(col.name)
                    elif isinstance(col, str):
                        columns.append(col)
                    else:
                        columns.append(str(col))
            
            usage_values = []
            
            # Process rows
            if hasattr(table, 'rows') and table.rows:
                for row in table.rows:
                    row_dict = {}
                    row_data = row if isinstance(row, (list, tuple)) else [row]
                    
                    for idx, value in enumerate(row_data):
                        if idx < len(columns):
                            row_dict[columns[idx]] = value
                    
                    metric_name = row_dict.get('Name', '')
                    
                    # Safely convert numeric values
                    try:
                        avg_val = float(row_dict.get('AvgValue', 0)) if row_dict.get('AvgValue') is not None else 0
                        max_val = float(row_dict.get('MaxValue', 0)) if row_dict.get('MaxValue') is not None else 0
                        min_val = float(row_dict.get('MinValue', 0)) if row_dict.get('MinValue') is not None else 0
                    except (ValueError, TypeError):
                        avg_val = max_val = min_val = 0
                    
                    data_point = {
                        'timestamp': row_dict.get('TimeGenerated'),
                        'average': avg_val,
                        'maximum': max_val,
                        'minimum': min_val
                    }
                    
                    if 'used_bytes' in metric_name:
                        storage_data['disk_usage'].append(data_point)
                    elif 'free_bytes' in metric_name:
                        storage_data['disk_available'].append(data_point)
                    elif 'used_percent' in metric_name:
                        storage_data['disk_utilization_percent'].append(data_point)
                        usage_values.append(avg_val)
            
            # Calculate summary
            if usage_values:
                storage_data['summary']['avg_usage_percent'] = sum(usage_values) / len(usage_values)
                storage_data['summary']['max_usage_percent'] = max(usage_values)
        
        return storage_data
    
    def _parse_network_metrics(self, response) -> Dict[str, Any]:
        """Parse network metrics response."""
        network_data = {
            'rx_bytes': [],
            'tx_bytes': [],
            'rx_dropped': [],
            'tx_dropped': [],
            'summary': {
                'total_rx_bytes': 0,
                'total_tx_bytes': 0,
                'total_dropped_packets': 0,
                'avg_throughput_mbps': 0
            }
        }
        
        if response and hasattr(response, 'tables') and response.tables and len(response.tables) > 0:
            table = response.tables[0]
            
            # Safely extract column names
            columns = []
            if hasattr(table, 'columns') and table.columns:
                for col in table.columns:
                    if hasattr(col, 'name'):
                        columns.append(col.name)
                    elif isinstance(col, str):
                        columns.append(col)
                    else:
                        columns.append(str(col))
            
            total_rx = 0
            total_tx = 0
            total_dropped = 0
            
            # Process rows
            if hasattr(table, 'rows') and table.rows:
                for row in table.rows:
                    row_dict = {}
                    row_data = row if isinstance(row, (list, tuple)) else [row]
                    
                    for idx, value in enumerate(row_data):
                        if idx < len(columns):
                            row_dict[columns[idx]] = value
                    
                    metric_name = row_dict.get('Name', '')
                    
                    # Safely convert numeric values
                    try:
                        avg_val = float(row_dict.get('AvgValue', 0)) if row_dict.get('AvgValue') is not None else 0
                        max_val = float(row_dict.get('MaxValue', 0)) if row_dict.get('MaxValue') is not None else 0
                        total_val = float(row_dict.get('TotalValue', 0)) if row_dict.get('TotalValue') is not None else 0
                    except (ValueError, TypeError):
                        avg_val = max_val = total_val = 0
                    
                    data_point = {
                        'timestamp': row_dict.get('TimeGenerated'),
                        'average': avg_val,
                        'maximum': max_val,
                        'total': total_val
                    }
                    
                    if 'rx_bytes' in metric_name:
                        network_data['rx_bytes'].append(data_point)
                        total_rx += total_val
                    elif 'tx_bytes' in metric_name:
                        network_data['tx_bytes'].append(data_point)
                        total_tx += total_val
                    elif 'dropped' in metric_name:
                        if 'rx_dropped' in metric_name:
                            network_data['rx_dropped'].append(data_point)
                        else:
                            network_data['tx_dropped'].append(data_point)
                        total_dropped += total_val
            
            # Calculate summary
            network_data['summary'] = {
                'total_rx_bytes': total_rx,
                'total_tx_bytes': total_tx,
                'total_dropped_packets': total_dropped,
                'avg_throughput_mbps': (total_rx + total_tx) / (1024 * 1024) / 3600  # Rough estimate
            }
        
        
        
        return network_data
    
    def _parse_pod_requests_limits(self, response) -> Dict[str, Any]:
        """Parse pod resource requests and limits."""
        requests_limits = {
            'pods': [],
            'summary': {
                'total_pods': 0,
                'pods_with_requests': 0,
                'pods_with_limits': 0,
                'pods_without_requests': 0
            }
        }
        
        if response and hasattr(response, 'tables') and response.tables and len(response.tables) > 0:
            table = response.tables[0]
            
            # Safely extract column names
            columns = []
            if hasattr(table, 'columns') and table.columns:
                for col in table.columns:
                    if hasattr(col, 'name'):
                        columns.append(col.name)
                    elif isinstance(col, str):
                        columns.append(col)
                    else:
                        columns.append(str(col))
            
            # Process rows
            if hasattr(table, 'rows') and table.rows:
                for row in table.rows:
                    row_dict = {}
                    row_data = row if isinstance(row, (list, tuple)) else [row]
                    
                    for idx, value in enumerate(row_data):
                        if idx < len(columns):
                            row_dict[columns[idx]] = value
                    
                    # Safely convert restart count
                    restart_count = 0
                    try:
                        restart_val = row_dict.get('PodRestartCount')
                        if restart_val is not None:
                            restart_count = int(restart_val)
                    except (ValueError, TypeError):
                        restart_count = 0
                    
                    pod_info = {
                        'pod_name': row_dict.get('PodName', ''),
                        'namespace': row_dict.get('Namespace', ''),
                        'node': row_dict.get('Computer', ''),
                        'pod_status': row_dict.get('PodStatus', ''),
                        'creation_timestamp': row_dict.get('PodCreationTimeStamp', ''),
                        'container_name': row_dict.get('ContainerName', ''),
                        'container_id': row_dict.get('ContainerID', ''),
                        'restart_count': restart_count
                    }
                    
                    # Only add if we have meaningful data
                    if pod_info['pod_name']:
                        requests_limits['pods'].append(pod_info)
            
            # Calculate summary
            requests_limits['summary']['total_pods'] = len(requests_limits['pods'])
        
        return requests_limits
    
    def _parse_cluster_events(self, response) -> Dict[str, Any]:
        """Parse cluster events response."""
        events_data = {
            'warning_events': [],
            'error_events': [],
            'summary': {
                'total_warnings': 0,
                'total_errors': 0,
                'most_frequent_issues': []
            }
        }
        
        if response and hasattr(response, 'tables') and response.tables and len(response.tables) > 0:
            table = response.tables[0]
            
            # Safely extract column names
            columns = []
            if hasattr(table, 'columns') and table.columns:
                for col in table.columns:
                    if hasattr(col, 'name'):
                        columns.append(col.name)
                    elif isinstance(col, str):
                        columns.append(col)
                    else:
                        columns.append(str(col))
            
            warnings = 0
            errors = 0
            
            # Process rows
            if hasattr(table, 'rows') and table.rows:
                for row in table.rows:
                    row_dict = {}
                    row_data = row if isinstance(row, (list, tuple)) else [row]
                    
                    for idx, value in enumerate(row_data):
                        if idx < len(columns):
                            row_dict[columns[idx]] = value
                    
                    # Safely convert event count
                    event_count = 0
                    try:
                        count_val = row_dict.get('EventCount')
                        if count_val is not None:
                            event_count = int(count_val)
                    except (ValueError, TypeError):
                        event_count = 0
                    
                    event_info = {
                        'object_kind': row_dict.get('ObjectKind', ''),
                        'reason': row_dict.get('Reason', ''),
                        'message': row_dict.get('Message', ''),
                        'event_count': event_count,
                        'first_seen': row_dict.get('FirstSeen', ''),
                        'last_seen': row_dict.get('LastSeen', '')
                    }
                    
                    # Categorize by severity based on reason
                    reason_lower = event_info['reason'].lower()
                    if any(keyword in reason_lower for keyword in ['failed', 'error', 'kill']):
                        events_data['error_events'].append(event_info)
                        errors += event_count
                    else:
                        events_data['warning_events'].append(event_info)
                        warnings += event_count
            
            # Sort by event count to get most frequent issues
            all_events = events_data['warning_events'] + events_data['error_events']
            most_frequent = sorted(all_events, key=lambda x: x['event_count'], reverse=True)[:5]
            
            events_data['summary'] = {
                'total_warnings': warnings,
                'total_errors': errors,
                'most_frequent_issues': most_frequent
            }
        
        return events_data
    
    def _calculate_cluster_summary(self, cluster_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate cluster-level summary statistics."""
        summary = {
            'cpu_summary': {},
            'memory_summary': {},
            'network_summary': {},
            'overall_health': 'unknown'
        }
        
        # CPU summary
        cpu_metrics = cluster_metrics.get('cpu_metrics', {})
        if cpu_metrics:
            all_cpu_values = []
            for metric_data in cpu_metrics.values():
                if isinstance(metric_data, list):
                    values = [point.get('average', 0) for point in metric_data if point.get('average') is not None]
                    all_cpu_values.extend(values)
            
            if all_cpu_values:
                summary['cpu_summary'] = {
                    'average_utilization': sum(all_cpu_values) / len(all_cpu_values),
                    'peak_utilization': max(all_cpu_values),
                    'min_utilization': min(all_cpu_values),
                    'data_points': len(all_cpu_values)
                }
        
        # Memory summary
        memory_metrics = cluster_metrics.get('memory_metrics', {})
        if memory_metrics:
            all_memory_values = []
            for metric_data in memory_metrics.values():
                if isinstance(metric_data, list):
                    values = [point.get('average', 0) for point in metric_data if point.get('average') is not None]
                    all_memory_values.extend(values)
            
            if all_memory_values:
                summary['memory_summary'] = {
                    'average_utilization': sum(all_memory_values) / len(all_memory_values),
                    'peak_utilization': max(all_memory_values),
                    'min_utilization': min(all_memory_values),
                    'data_points': len(all_memory_values)
                }
        
        # Overall health assessment
        cpu_avg = summary.get('cpu_summary', {}).get('average_utilization', 0)
        memory_avg = summary.get('memory_summary', {}).get('average_utilization', 0)
        
        if cpu_avg > 80 or memory_avg > 85:
            summary['overall_health'] = 'critical'
        elif cpu_avg > 60 or memory_avg > 70:
            summary['overall_health'] = 'warning'
        elif cpu_avg > 0 and memory_avg > 0:
            summary['overall_health'] = 'healthy'
        
        return summary
    
    def _calculate_node_utilization(self, performance_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate node resource utilization."""
        utilization = {
            'cpu_utilization_percent': 0,
            'memory_utilization_percent': 0,
            'filesystem_utilization_percent': 0,
            'status': 'unknown'
        }
        
        # Calculate CPU utilization
        cpu_usage = performance_data.get('cpu_usage', [])
        cpu_capacity = performance_data.get('cpu_capacity', [])
        
        if cpu_usage and cpu_capacity:
            latest_usage = cpu_usage[0]['average'] if cpu_usage else 0
            latest_capacity = cpu_capacity[0]['average'] if cpu_capacity else 1
            
            if latest_capacity > 0:
                utilization['cpu_utilization_percent'] = (latest_usage / latest_capacity) * 100
        
        # Calculate memory utilization
        memory_usage = performance_data.get('memory_usage', [])
        memory_capacity = performance_data.get('memory_capacity', [])
        
        if memory_usage and memory_capacity:
            latest_usage = memory_usage[0]['average'] if memory_usage else 0
            latest_capacity = memory_capacity[0]['average'] if memory_capacity else 1
            
            if latest_capacity > 0:
                utilization['memory_utilization_percent'] = (latest_usage / latest_capacity) * 100
        
        # Calculate filesystem utilization
        fs_usage = performance_data.get('filesystem_usage', [])
        fs_available = performance_data.get('filesystem_available', [])
        
        if fs_usage and fs_available:
            latest_usage = fs_usage[0]['average'] if fs_usage else 0
            latest_available = fs_available[0]['average'] if fs_available else 0
            total_capacity = latest_usage + latest_available
            
            if total_capacity > 0:
                utilization['filesystem_utilization_percent'] = (latest_usage / total_capacity) * 100
        
        # Determine status
        cpu_util = utilization['cpu_utilization_percent']
        memory_util = utilization['memory_utilization_percent']
        
        if cpu_util > 90 or memory_util > 95:
            utilization['status'] = 'critical'
        elif cpu_util > 75 or memory_util > 85:
            utilization['status'] = 'warning'
        elif cpu_util > 0 and memory_util > 0:
            utilization['status'] = 'healthy'
        
        return utilization
    
    def _calculate_node_efficiency(self, performance_data: Dict[str, Any]) -> Dict[str, float]:
        """Calculate node efficiency scores."""
        efficiency = {
            'cpu_efficiency': 0.0,
            'memory_efficiency': 0.0,
            'overall_efficiency': 0.0
        }
        
        # CPU efficiency (target ~70% utilization)
        cpu_usage = performance_data.get('cpu_usage', [])
        if cpu_usage:
            avg_cpu = sum(point['average'] for point in cpu_usage) / len(cpu_usage)
            cpu_capacity = performance_data.get('cpu_capacity', [])
            if cpu_capacity:
                avg_capacity = sum(point['average'] for point in cpu_capacity) / len(cpu_capacity)
                if avg_capacity > 0:
                    cpu_util_percent = (avg_cpu / avg_capacity) * 100
                    # Efficiency peaks at 70% utilization
                    if cpu_util_percent <= 70:
                        efficiency['cpu_efficiency'] = cpu_util_percent / 70 * 100
                    else:
                        efficiency['cpu_efficiency'] = max(0, 100 - (cpu_util_percent - 70))
        
        # Memory efficiency (target ~80% utilization)
        memory_usage = performance_data.get('memory_usage', [])
        if memory_usage:
            avg_memory = sum(point['average'] for point in memory_usage) / len(memory_usage)
            memory_capacity = performance_data.get('memory_capacity', [])
            if memory_capacity:
                avg_capacity = sum(point['average'] for point in memory_capacity) / len(memory_capacity)
                if avg_capacity > 0:
                    memory_util_percent = (avg_memory / avg_capacity) * 100
                    # Efficiency peaks at 80% utilization
                    if memory_util_percent <= 80:
                        efficiency['memory_efficiency'] = memory_util_percent / 80 * 100
                    else:
                        efficiency['memory_efficiency'] = max(0, 100 - (memory_util_percent - 80))
        
        # Overall efficiency
        efficiency['overall_efficiency'] = (efficiency['cpu_efficiency'] + efficiency['memory_efficiency']) / 2
        
        return efficiency
    
    def _analyze_by_namespace(self, cpu_consumers: List[Dict], memory_consumers: List[Dict]) -> Dict[str, Any]:
        """Analyze resource consumption by namespace."""
        namespace_analysis = {}
        
        # Analyze CPU consumers by namespace
        for pod in cpu_consumers:
            namespace = pod.get('namespace', 'default')
            if namespace not in namespace_analysis:
                namespace_analysis[namespace] = {
                    'cpu_consuming_pods': 0,
                    'memory_consuming_pods': 0,
                    'total_cpu_usage': 0,
                    'total_memory_usage': 0
                }
            
            namespace_analysis[namespace]['cpu_consuming_pods'] += 1
            namespace_analysis[namespace]['total_cpu_usage'] += pod.get('avg_cpu_millicores', 0)
        
        # Analyze memory consumers by namespace
        for pod in memory_consumers:
            namespace = pod.get('namespace', 'default')
            if namespace not in namespace_analysis:
                namespace_analysis[namespace] = {
                    'cpu_consuming_pods': 0,
                    'memory_consuming_pods': 0,
                    'total_cpu_usage': 0,
                    'total_memory_usage': 0
                }
            
            namespace_analysis[namespace]['memory_consuming_pods'] += 1
            namespace_analysis[namespace]['total_memory_usage'] += pod.get('avg_memory_mb', 0)
        
        return namespace_analysis