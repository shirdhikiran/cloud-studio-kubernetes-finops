"""Enhanced Azure Monitor client with raw metrics collection for accurate utilization data."""

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
    """Enhanced Azure Monitor client for raw metrics collection."""
    
    def __init__(self, credential, subscription_id: str, config: Dict[str, Any]):
        super().__init__(config, "MonitorClient")
        self.credential = credential
        self.subscription_id = subscription_id
        self.log_analytics_workspace_id = config.get("log_analytics_workspace_id")
        
        self._monitor_client = None
        self._metrics_client = None
        self._logs_client = None
        
        # Raw metric names for different Azure services
        self.aks_raw_metrics = [
            "node_cpu_usage_millicores",
            "node_cpu_allocatable_millicores", 
            "node_memory_working_set_bytes",
            "node_memory_allocatable_bytes",
            "node_network_in_bytes_per_sec",
            "node_network_out_bytes_per_sec",
            "node_disk_usage_bytes",
            "node_disk_io_time_seconds_total",
            "kube_pod_container_resource_requests_cpu_cores",
            "kube_pod_container_resource_requests_memory_bytes",
            "kube_pod_container_resource_limits_cpu_cores", 
            "kube_pod_container_resource_limits_memory_bytes"
        ]
        
        # VM-level metrics for AKS nodes
        self.vm_raw_metrics = [
            "Percentage CPU",
            "Available Memory Bytes",
            "Network In Total",
            "Network Out Total", 
            "Disk Read Bytes/sec",
            "Disk Write Bytes/sec",
            "Disk IOPS Consumed Percentage",
            "OS Disk Bandwidth Consumed Percentage"
        ]
        
        # Storage metrics
        self.storage_raw_metrics = [
            "UsedCapacity",
            "Transactions",
            "Availability",
            "SuccessServerLatency",
            "SuccessE2ELatency",
            "Ingress",
            "Egress"
        ]
        
        # Network metrics  
        self.network_raw_metrics = [
            "ByteCount",
            "PacketCount",
            "VipAvailability",
            "DipAvailability",
            "AllocatedSnatPorts",
            "UsedSnatPorts"
        ]
        
        # Raw Kusto queries for Log Analytics
        self.raw_kusto_queries = {
            'node_raw_metrics': '''
                Perf
                | where TimeGenerated >= ago({hours}h)
                | where ObjectName == "K8SNode" 
                | where Computer startswith "aks-"
                | where CounterName in (
                    "cpuUsageNanoCores", "cpuCapacityNanoCores", "cpuAllocatableNanoCores",
                    "memoryWorkingSetBytes", "memoryCapacityBytes", "memoryAllocatableBytes",
                    "networkRxBytes", "networkTxBytes",
                    "fsUsedBytes", "fsCapacityBytes", "fsAvailBytes",
                    "fsInodesFree", "fsInodesUsed", "fsInodesTotal"
                )
                | project TimeGenerated, Computer, CounterName, CounterValue, InstanceName
                | order by TimeGenerated desc
            ''',
            
            'pod_raw_metrics': '''
                Perf
                | where TimeGenerated >= ago({hours}h)
                | where ObjectName == "K8SContainer"
                | where CounterName in (
                    "cpuUsageNanoCores", "cpuRequestNanoCores", "cpuLimitNanoCores",
                    "memoryWorkingSetBytes", "memoryRequestBytes", "memoryLimitBytes",
                    "networkRxBytes", "networkTxBytes"
                )
                | join kind=inner (
                    KubePodInventory
                    | where TimeGenerated >= ago({hours}h)
                    | project PodUid, Name, Namespace, Computer, PodStatus, ServiceName
                ) on $left.InstanceName == $right.PodUid
                | project TimeGenerated, PodName=Name, Namespace, Computer, CounterName, CounterValue, PodStatus, ServiceName
                | order by TimeGenerated desc
            ''',
            
            'container_raw_metrics': '''
                InsightsMetrics
                | where TimeGenerated >= ago({hours}h)
                | where Namespace == "container.azm.ms/cpu" or Namespace == "container.azm.ms/memory"
                | extend Tags = todynamic(Tags)
                | extend ContainerName = tostring(Tags["container.azm.ms/containerId"])
                | extend PodName = tostring(Tags["container.azm.ms/podName"])
                | extend Namespace_k8s = tostring(Tags["container.azm.ms/podNamespace"])
                | project TimeGenerated, ContainerName, PodName, Namespace_k8s, Name, Val
                | order by TimeGenerated desc
            ''',
            
            'cluster_raw_utilization': '''
                KubeNodeInventory
                | where TimeGenerated >= ago({hours}h)
                | join kind=inner (
                    Perf
                    | where TimeGenerated >= ago({hours}h)
                    | where ObjectName == "K8SNode"
                    | where CounterName in ("cpuUsageNanoCores", "cpuCapacityNanoCores", "memoryWorkingSetBytes", "memoryCapacityBytes")
                ) on Computer
                | summarize 
                    AvgCpuUsage = avg(CounterValue),
                    MaxCpuUsage = max(CounterValue),
                    MinCpuUsage = min(CounterValue),
                    LatestCpuUsage = arg_max(TimeGenerated, CounterValue)
                by Computer, CounterName, bin(TimeGenerated, {granularity_minutes}m)
                | order by TimeGenerated desc
            ''',
            
            'resource_requests_limits_raw': '''
                KubePodInventory
                | where TimeGenerated >= ago({hours}h)
                | join kind=leftouter (
                    Perf
                    | where TimeGenerated >= ago({hours}h)
                    | where ObjectName == "K8SContainer"
                    | where CounterName in ("cpuRequestNanoCores", "cpuLimitNanoCores", "memoryRequestBytes", "memoryLimitBytes")
                    | summarize arg_max(TimeGenerated, CounterValue) by InstanceName, CounterName
                ) on $left.PodUid == $right.InstanceName
                | project 
                    PodName = Name,
                    Namespace,
                    Computer,
                    CounterName,
                    RequestedValue = CounterValue,
                    PodStatus,
                    ContainerName,
                    PodCreationTimeStamp
                | where isnotnull(RequestedValue)
            ''',
            
            'storage_raw_metrics': '''
                InsightsMetrics
                | where TimeGenerated >= ago({hours}h)
                | where Namespace == "container.azm.ms/disk"
                | where Name in ("used_bytes", "free_bytes", "capacity_bytes", "used_percent", "inodes_used", "inodes_free")
                | extend Tags = todynamic(Tags)
                | extend Device = tostring(Tags["container.azm.ms/device"])
                | extend ClusterName = tostring(Tags["container.azm.ms/clusterId"])
                | project TimeGenerated, ClusterName, Computer, Device, Name, Val
                | order by TimeGenerated desc
            ''',
            
            'network_raw_metrics': '''
                InsightsMetrics
                | where TimeGenerated >= ago({hours}h)
                | where Namespace == "container.azm.ms/network"
                | where Name in ("rx_bytes", "tx_bytes", "rx_packets", "tx_packets", "rx_errors", "tx_errors", "rx_dropped", "tx_dropped")
                | extend Tags = todynamic(Tags)
                | extend Interface = tostring(Tags["container.azm.ms/interface"])
                | extend ClusterName = tostring(Tags["container.azm.ms/clusterId"])
                | project TimeGenerated, ClusterName, Computer, Interface, Name, Val
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
                self.logger.warning("Log Analytics workspace ID not provided - detailed metrics will be limited")
            
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
            
            # Test with a simple metric query
            test_resource_id = f"/subscriptions/{self.subscription_id}"
            try:
                # This should work for any subscription
                list(self._monitor_client.metric_definitions.list(test_resource_id))
            except ResourceNotFoundError:
                # Expected for the test - indicates API is accessible
                pass
            except ClientAuthenticationError:
                return False
            
            return True
            
        except Exception as e:
            self.logger.warning("Azure Monitor health check failed", error=str(e))
            return False
    
    @retry_with_backoff(max_retries=3)
    async def get_cluster_raw_metrics(self,
                                    cluster_resource_id: str,
                                    cluster_name: str,
                                    start_time: Optional[datetime] = None,
                                    end_time: Optional[datetime] = None,
                                    granularity_minutes: int = 5) -> Dict[str, Any]:
        """Get comprehensive raw metrics for cluster with maximum detail."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(hours=24)
        
        self.logger.info(f"Collecting raw metrics for cluster: {cluster_name}")
        
        raw_metrics = {
            'cluster_name': cluster_name,
            'cluster_resource_id': cluster_resource_id,
            'collection_period': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'granularity_minutes': granularity_minutes
            },
            'node_raw_metrics': {},
            'pod_raw_metrics': {},
            'container_raw_metrics': {},
            'storage_raw_metrics': {},
            'network_raw_metrics': {},
            'cluster_level_metrics': {},
            'resource_utilization_raw': {},
            'collection_metadata': {
                'metrics_collected': [],
                'metrics_failed': [],
                'data_sources': [],
                'total_data_points': 0
            }
        }
        
        try:
            # Collect from multiple sources in parallel
            collection_tasks = []
            
            # 1. Log Analytics raw metrics (most detailed)
            if self._logs_client and self.log_analytics_workspace_id:
                collection_tasks.extend([
                    self._collect_node_raw_metrics(cluster_name, start_time, end_time, granularity_minutes),
                    self._collect_pod_raw_metrics(cluster_name, start_time, end_time, granularity_minutes),
                    self._collect_container_raw_metrics(cluster_name, start_time, end_time, granularity_minutes),
                    self._collect_storage_raw_metrics(cluster_name, start_time, end_time, granularity_minutes),
                    self._collect_network_raw_metrics(cluster_name, start_time, end_time, granularity_minutes)
                ])
                raw_metrics['collection_metadata']['data_sources'].append('log_analytics')
            
            # 2. Azure Monitor VM metrics (for node-level data)
            collection_tasks.append(
                self._collect_vm_level_metrics(cluster_resource_id, start_time, end_time, granularity_minutes)
            )
            raw_metrics['collection_metadata']['data_sources'].append('azure_monitor_vms')
            
            # 3. Azure Monitor cluster metrics
            collection_tasks.append(
                self._collect_cluster_level_metrics(cluster_resource_id, start_time, end_time, granularity_minutes)
            )
            raw_metrics['collection_metadata']['data_sources'].append('azure_monitor_cluster')
            
            # Execute all collection tasks
            results = await asyncio.gather(*collection_tasks, return_exceptions=True)
            
            # Process results
            result_keys = ['node_raw_metrics', 'pod_raw_metrics', 'container_raw_metrics', 
                          'storage_raw_metrics', 'network_raw_metrics', 'vm_level_metrics', 
                          'cluster_level_metrics']
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.warning(f"Failed to collect {result_keys[i % len(result_keys)]}: {result}")
                    raw_metrics['collection_metadata']['metrics_failed'].append(result_keys[i % len(result_keys)])
                else:
                    if i < len(result_keys):
                        raw_metrics[result_keys[i]] = result
                        raw_metrics['collection_metadata']['metrics_collected'].append(result_keys[i])
                        if isinstance(result, dict) and 'data_points' in result:
                            raw_metrics['collection_metadata']['total_data_points'] += result.get('data_points', 0)
            
            # Calculate derived metrics from raw data
            raw_metrics['derived_metrics'] = self._calculate_derived_metrics(raw_metrics)
            
            # Generate utilization summary from raw data
            raw_metrics['utilization_summary'] = self._generate_utilization_summary(raw_metrics)
            
            self.logger.info(
                "Raw metrics collection completed",
                cluster=cluster_name,
                data_points=raw_metrics['collection_metadata']['total_data_points'],
                sources=raw_metrics['collection_metadata']['data_sources']
            )
            
            return raw_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to collect raw metrics for {cluster_name}: {e}")
            raise MetricsException(f"Raw metrics collection failed: {e}")
    
    async def _collect_node_raw_metrics(self, cluster_name: str, start_time: datetime, 
                                       end_time: datetime, granularity_minutes: int) -> Dict[str, Any]:
        """Collect raw node-level metrics from Log Analytics."""
        hours = int((end_time - start_time).total_seconds() / 3600)
        
        query = self.raw_kusto_queries['node_raw_metrics'].format(
            hours=hours
        )
        
        try:
            response = await self._execute_kusto_query(query, start_time, end_time)
            return self._process_node_raw_metrics(response)
        except Exception as e:
            self.logger.error(f"Failed to collect node raw metrics: {e}")
            return {'error': str(e), 'data_points': 0}
    
    async def _collect_pod_raw_metrics(self, cluster_name: str, start_time: datetime,
                                      end_time: datetime, granularity_minutes: int) -> Dict[str, Any]:
        """Collect raw pod-level metrics from Log Analytics."""
        hours = int((end_time - start_time).total_seconds() / 3600)
        
        query = self.raw_kusto_queries['pod_raw_metrics'].format(
            hours=hours
        )
        
        try:
            response = await self._execute_kusto_query(query, start_time, end_time)
            return self._process_pod_raw_metrics(response)
        except Exception as e:
            self.logger.error(f"Failed to collect pod raw metrics: {e}")
            return {'error': str(e), 'data_points': 0}
    
    async def _collect_container_raw_metrics(self, cluster_name: str, start_time: datetime,
                                           end_time: datetime, granularity_minutes: int) -> Dict[str, Any]:
        """Collect raw container-level metrics from Container Insights."""
        hours = int((end_time - start_time).total_seconds() / 3600)
        
        query = self.raw_kusto_queries['container_raw_metrics'].format(
            hours=hours
        )
        
        try:
            response = await self._execute_kusto_query(query, start_time, end_time)
            return self._process_container_raw_metrics(response)
        except Exception as e:
            self.logger.error(f"Failed to collect container raw metrics: {e}")
            return {'error': str(e), 'data_points': 0}
    
    async def _collect_storage_raw_metrics(self, cluster_name: str, start_time: datetime,
                                         end_time: datetime, granularity_minutes: int) -> Dict[str, Any]:
        """Collect raw storage metrics from Container Insights."""
        hours = int((end_time - start_time).total_seconds() / 3600)
        
        query = self.raw_kusto_queries['storage_raw_metrics'].format(
            hours=hours
        )
        
        try:
            response = await self._execute_kusto_query(query, start_time, end_time)
            return self._process_storage_raw_metrics(response)
        except Exception as e:
            self.logger.error(f"Failed to collect storage raw metrics: {e}")
            return {'error': str(e), 'data_points': 0}
    
    async def _collect_network_raw_metrics(self, cluster_name: str, start_time: datetime,
                                         end_time: datetime, granularity_minutes: int) -> Dict[str, Any]:
        """Collect raw network metrics from Container Insights."""
        hours = int((end_time - start_time).total_seconds() / 3600)
        
        query = self.raw_kusto_queries['network_raw_metrics'].format(
            hours=hours
        )
        
        try:
            response = await self._execute_kusto_query(query, start_time, end_time)
            return self._process_network_raw_metrics(response)
        except Exception as e:
            self.logger.error(f"Failed to collect network raw metrics: {e}")
            return {'error': str(e), 'data_points': 0}
    
    async def _collect_vm_level_metrics(self, cluster_resource_id: str, start_time: datetime,
                                       end_time: datetime, granularity_minutes: int) -> Dict[str, Any]:
        """Collect VM-level metrics for AKS nodes."""
        try:
            # Get node resource group from cluster resource ID
            cluster_parts = cluster_resource_id.split('/')
            cluster_name = cluster_parts[-1]
            resource_group = cluster_parts[4]
            
            # Node resource group follows pattern: MC_{resource_group}_{cluster_name}_{location}
            # We'll need to discover the actual node resource group
            vm_metrics = await self._get_aks_node_vm_metrics(resource_group, cluster_name, start_time, end_time)
            return vm_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to collect VM level metrics: {e}")
            return {'error': str(e), 'data_points': 0}
    
    async def _collect_cluster_level_metrics(self, cluster_resource_id: str, start_time: datetime,
                                           end_time: datetime, granularity_minutes: int) -> Dict[str, Any]:
        """Collect cluster-level metrics from Azure Monitor."""
        try:
            granularity = timedelta(minutes=granularity_minutes)
            
            cluster_metrics = {}
            for metric_name in self.aks_raw_metrics:
                try:
                    metric_data = await self._get_single_metric_raw(
                        cluster_resource_id, metric_name, start_time, end_time, granularity
                    )
                    if metric_data:
                        cluster_metrics[metric_name] = metric_data
                except Exception as e:
                    self.logger.debug(f"Metric {metric_name} not available: {e}")
                    continue
            
            return {
                'metrics': cluster_metrics,
                'data_points': sum(len(data) for data in cluster_metrics.values())
            }
            
        except Exception as e:
            self.logger.error(f"Failed to collect cluster level metrics: {e}")
            return {'error': str(e), 'data_points': 0}
    
    async def _get_single_metric_raw(self, resource_id: str, metric_name: str,
                                   start_time: datetime, end_time: datetime,
                                   granularity: timedelta) -> List[Dict[str, Any]]:
        """Get raw data for a single metric with all aggregation types."""
        try:
            response = self._metrics_client.query_resource(
                resource_uri=resource_id,
                metric_names=[metric_name],
                timespan=(start_time, end_time),
                granularity=granularity,
                aggregations=[
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
                            'resource_id': resource_id,
                            'timeseries_metadata': {
                                'metadata': timeseries.metadata_values if hasattr(timeseries, 'metadata_values') else {}
                            }
                        })
            
            return metric_data
            
        except Exception as e:
            self.logger.debug(f"Error getting metric {metric_name}: {e}")
            return []
    
    async def _get_aks_node_vm_metrics(self, resource_group: str, cluster_name: str,
                                     start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get VM-level metrics for AKS nodes."""
        try:
            # AKS creates VMs in a managed resource group with pattern MC_{rg}_{cluster}_{location}
            # We need to discover this or get it from the cluster properties
            
            vm_metrics = {
                'node_vm_metrics': {},
                'aggregated_vm_metrics': {},
                'data_points': 0
            }
            
            # For now, return structure indicating VM metrics need node resource group discovery
            vm_metrics['note'] = "VM metrics require node resource group discovery from AKS cluster properties"
            vm_metrics['required_info'] = {
                'node_resource_group': f"MC_{resource_group}_{cluster_name}_<location>",
                'vm_scale_set_name': f"aks-nodepool1-*",
                'required_metrics': self.vm_raw_metrics
            }
            
            return vm_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to get AKS node VM metrics: {e}")
            return {'error': str(e), 'data_points': 0}
    
    async def _execute_kusto_query(self, query: str, start_time: datetime, end_time: datetime) -> Any:
        """Execute a Kusto query against Log Analytics."""
        try:
            if not self._logs_client or not self.log_analytics_workspace_id:
                return None
            
            self.logger.debug(f"Executing Kusto query: {query[:100]}...")
            
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
    
    def _process_node_raw_metrics(self, response) -> Dict[str, Any]:
        """Process raw node metrics response."""
        node_metrics = {
            'nodes': {},
            'aggregated_metrics': {},
            'data_points': 0,
            'metric_types': set()
        }
        
        if not response or not hasattr(response, 'tables') or not response.tables:
            return node_metrics
        
        table = response.tables[0]
        columns = self._extract_columns(table)
        
        if not hasattr(table, 'rows') or not table.rows:
            return node_metrics
        
        for row in table.rows:
            try:
                row_data = self._extract_row_data(row, columns)
                
                timestamp = row_data.get('TimeGenerated')
                computer = row_data.get('Computer', 'Unknown')
                counter_name = row_data.get('CounterName', 'Unknown')
                counter_value = self._safe_float(row_data.get('CounterValue'))
                instance_name = row_data.get('InstanceName', '')
                
                if computer not in node_metrics['nodes']:
                    node_metrics['nodes'][computer] = {
                        'computer_name': computer,
                        'metrics': {},
                        'latest_timestamp': None
                    }
                
                node_data = node_metrics['nodes'][computer]
                
                if counter_name not in node_data['metrics']:
                    node_data['metrics'][counter_name] = {
                        'values': [],
                        'latest_value': None,
                        'min_value': float('inf'),
                        'max_value': float('-inf'),
                        'avg_value': 0.0,
                        'unit': self._get_metric_unit(counter_name)
                    }
                
                metric_data = node_data['metrics'][counter_name]
                metric_data['values'].append({
                    'timestamp': timestamp,
                    'value': counter_value,
                    'instance': instance_name
                })
                
                # Update statistics
                if counter_value is not None:
                    metric_data['latest_value'] = counter_value
                    metric_data['min_value'] = min(metric_data['min_value'], counter_value)
                    metric_data['max_value'] = max(metric_data['max_value'], counter_value)
                    
                    # Update running average
                    values = [v['value'] for v in metric_data['values'] if v['value'] is not None]
                    metric_data['avg_value'] = sum(values) / len(values) if values else 0.0
                
                node_data['latest_timestamp'] = timestamp
                node_metrics['metric_types'].add(counter_name)
                node_metrics['data_points'] += 1
                
            except Exception as e:
                self.logger.warning(f"Error processing node metric row: {e}")
                continue
        
        # Convert sets to lists
        node_metrics['metric_types'] = list(node_metrics['metric_types'])
        
        # Calculate aggregated metrics across all nodes
        node_metrics['aggregated_metrics'] = self._calculate_node_aggregations(node_metrics['nodes'])
        
        return node_metrics
    
    def _process_pod_raw_metrics(self, response) -> Dict[str, Any]:
        """Process raw pod metrics response."""
        pod_metrics = {
            'pods': {},
            'namespace_aggregations': {},
            'data_points': 0,
            'metric_types': set()
        }
        
        if not response or not hasattr(response, 'tables') or not response.tables:
            return pod_metrics
        
        table = response.tables[0]
        columns = self._extract_columns(table)
        
        if not hasattr(table, 'rows') or not table.rows:
            return pod_metrics
        
        for row in table.rows:
            try:
                row_data = self._extract_row_data(row, columns)
                
                timestamp = row_data.get('TimeGenerated')
                pod_name = row_data.get('PodName', 'Unknown')
                namespace = row_data.get('Namespace', 'Unknown')
                computer = row_data.get('Computer', 'Unknown')
                counter_name = row_data.get('CounterName', 'Unknown')
                counter_value = self._safe_float(row_data.get('CounterValue'))
                pod_status = row_data.get('PodStatus', 'Unknown')
                service_name = row_data.get('ServiceName', '')
                
                pod_key = f"{namespace}/{pod_name}"
                
                if pod_key not in pod_metrics['pods']:
                    pod_metrics['pods'][pod_key] = {
                        'pod_name': pod_name,
                        'namespace': namespace,
                        'computer': computer,
                        'pod_status': pod_status,
                        'service_name': service_name,
                        'metrics': {},
                        'latest_timestamp': None
                    }
                
                pod_data = pod_metrics['pods'][pod_key]
                
                if counter_name not in pod_data['metrics']:
                    pod_data['metrics'][counter_name] = {
                        'values': [],
                        'latest_value': None,
                        'min_value': float('inf'),
                        'max_value': float('-inf'),
                        'avg_value': 0.0,
                        'unit': self._get_metric_unit(counter_name)
                    }
                
                metric_data = pod_data['metrics'][counter_name]
                metric_data['values'].append({
                    'timestamp': timestamp,
                    'value': counter_value
                })
                
                # Update statistics
                if counter_value is not None:
                    metric_data['latest_value'] = counter_value
                    metric_data['min_value'] = min(metric_data['min_value'], counter_value)
                    metric_data['max_value'] = max(metric_data['max_value'], counter_value)
                    
                    values = [v['value'] for v in metric_data['values'] if v['value'] is not None]
                    metric_data['avg_value'] = sum(values) / len(values) if values else 0.0
                
                pod_data['latest_timestamp'] = timestamp
                pod_metrics['metric_types'].add(counter_name)
                pod_metrics['data_points'] += 1
                
            except Exception as e:
                self.logger.warning(f"Error processing pod metric row: {e}")
                continue
        
        # Convert sets to lists
        pod_metrics['metric_types'] = list(pod_metrics['metric_types'])
        
        # Calculate namespace aggregations
        pod_metrics['namespace_aggregations'] = self._calculate_namespace_aggregations(pod_metrics['pods'])
        
        return pod_metrics
    
    def _process_container_raw_metrics(self, response) -> Dict[str, Any]:
        """Process raw container metrics response."""
        container_metrics = {
            'containers': {},
            'pod_aggregations': {},
            'data_points': 0,
            'metric_types': set()
        }
        
        if not response or not hasattr(response, 'tables') or not response.tables:
            return container_metrics
        
        table = response.tables[0]
        columns = self._extract_columns(table)
        
        if not hasattr(table, 'rows') or not table.rows:
            return container_metrics
        
        for row in table.rows:
            try:
                row_data = self._extract_row_data(row, columns)
                
                timestamp = row_data.get('TimeGenerated')
                container_name = row_data.get('ContainerName', 'Unknown')
                pod_name = row_data.get('PodName', 'Unknown')
                namespace = row_data.get('Namespace_k8s', 'Unknown')
                metric_name = row_data.get('Name', 'Unknown')
                value = self._safe_float(row_data.get('Val'))
                unit = "count"  # Default unit since Unit column may not exist
                
                container_key = f"{namespace}/{pod_name}/{container_name}"
                
                if container_key not in container_metrics['containers']:
                    container_metrics['containers'][container_key] = {
                        'container_name': container_name,
                        'pod_name': pod_name,
                        'namespace': namespace,
                        'metrics': {},
                        'latest_timestamp': None
                    }
                
                container_data = container_metrics['containers'][container_key]
                
                if metric_name not in container_data['metrics']:
                    container_data['metrics'][metric_name] = {
                        'values': [],
                        'latest_value': None,
                        'min_value': float('inf'),
                        'max_value': float('-inf'),
                        'avg_value': 0.0,
                        'unit': unit
                    }
                
                metric_data = container_data['metrics'][metric_name]
                metric_data['values'].append({
                    'timestamp': timestamp,
                    'value': value
                })
                
                # Update statistics
                if value is not None:
                    metric_data['latest_value'] = value
                    metric_data['min_value'] = min(metric_data['min_value'], value)
                    metric_data['max_value'] = max(metric_data['max_value'], value)
                    
                    values = [v['value'] for v in metric_data['values'] if v['value'] is not None]
                    metric_data['avg_value'] = sum(values) / len(values) if values else 0.0
                
                container_data['latest_timestamp'] = timestamp
                container_metrics['metric_types'].add(metric_name)
                container_metrics['data_points'] += 1
                
            except Exception as e:
                self.logger.warning(f"Error processing container metric row: {e}")
                continue
        
        # Convert sets to lists
        container_metrics['metric_types'] = list(container_metrics['metric_types'])
        
        # Calculate pod-level aggregations
        container_metrics['pod_aggregations'] = self._calculate_pod_aggregations(container_metrics['containers'])
        
        return container_metrics
    
    def _process_storage_raw_metrics(self, response) -> Dict[str, Any]:
        """Process raw storage metrics response."""
        storage_metrics = {
            'storage_devices': {},
            'cluster_storage_summary': {},
            'data_points': 0,
            'metric_types': set()
        }
        
        if not response or not hasattr(response, 'tables') or not response.tables:
            return storage_metrics
        
        table = response.tables[0]
        columns = self._extract_columns(table)
        
        if not hasattr(table, 'rows') or not table.rows:
            return storage_metrics
        
        for row in table.rows:
            try:
                row_data = self._extract_row_data(row, columns)
                
                timestamp = row_data.get('TimeGenerated')
                cluster_name = row_data.get('ClusterName', 'Unknown')
                computer = row_data.get('Computer', 'Unknown')
                device = row_data.get('Device', 'Unknown')
                metric_name = row_data.get('Name', 'Unknown')
                value = self._safe_float(row_data.get('Val'))
                unit = self._get_storage_metric_unit(metric_name)  # Determine unit from metric name
                
                device_key = f"{computer}/{device}"
                
                if device_key not in storage_metrics['storage_devices']:
                    storage_metrics['storage_devices'][device_key] = {
                        'computer': computer,
                        'device': device,
                        'cluster_name': cluster_name,
                        'metrics': {},
                        'latest_timestamp': None
                    }
                
                device_data = storage_metrics['storage_devices'][device_key]
                
                if metric_name not in device_data['metrics']:
                    device_data['metrics'][metric_name] = {
                        'values': [],
                        'latest_value': None,
                        'min_value': float('inf'),
                        'max_value': float('-inf'),
                        'avg_value': 0.0,
                        'unit': unit
                    }
                
                metric_data = device_data['metrics'][metric_name]
                metric_data['values'].append({
                    'timestamp': timestamp,
                    'value': value
                })
                
                # Update statistics
                if value is not None:
                    metric_data['latest_value'] = value
                    metric_data['min_value'] = min(metric_data['min_value'], value)
                    metric_data['max_value'] = max(metric_data['max_value'], value)
                    
                    values = [v['value'] for v in metric_data['values'] if v['value'] is not None]
                    metric_data['avg_value'] = sum(values) / len(values) if values else 0.0
                
                device_data['latest_timestamp'] = timestamp
                storage_metrics['metric_types'].add(metric_name)
                storage_metrics['data_points'] += 1
                
            except Exception as e:
                self.logger.warning(f"Error processing storage metric row: {e}")
                continue
        
        # Convert sets to lists
        storage_metrics['metric_types'] = list(storage_metrics['metric_types'])
        
        # Calculate cluster storage summary
        storage_metrics['cluster_storage_summary'] = self._calculate_storage_summary(storage_metrics['storage_devices'])
        
        return storage_metrics
    
    def _process_network_raw_metrics(self, response) -> Dict[str, Any]:
        """Process raw network metrics response."""
        network_metrics = {
            'network_interfaces': {},
            'cluster_network_summary': {},
            'data_points': 0,
            'metric_types': set()
        }
        
        if not response or not hasattr(response, 'tables') or not response.tables:
            return network_metrics
        
        table = response.tables[0]
        columns = self._extract_columns(table)
        
        if not hasattr(table, 'rows') or not table.rows:
            return network_metrics
        
        for row in table.rows:
            try:
                row_data = self._extract_row_data(row, columns)
                
                timestamp = row_data.get('TimeGenerated')
                cluster_name = row_data.get('ClusterName', 'Unknown')
                computer = row_data.get('Computer', 'Unknown')
                interface = row_data.get('Interface', 'Unknown')
                metric_name = row_data.get('Name', 'Unknown')
                value = self._safe_float(row_data.get('Val'))
                unit = self._get_network_metric_unit(metric_name)  # Determine unit from metric name
                
                interface_key = f"{computer}/{interface}"
                
                if interface_key not in network_metrics['network_interfaces']:
                    network_metrics['network_interfaces'][interface_key] = {
                        'computer': computer,
                        'interface': interface,
                        'cluster_name': cluster_name,
                        'metrics': {},
                        'latest_timestamp': None
                    }
                
                interface_data = network_metrics['network_interfaces'][interface_key]
                
                if metric_name not in interface_data['metrics']:
                    interface_data['metrics'][metric_name] = {
                        'values': [],
                        'latest_value': None,
                        'min_value': float('inf'),
                        'max_value': float('-inf'),
                        'avg_value': 0.0,
                        'unit': unit
                    }
                
                metric_data = interface_data['metrics'][metric_name]
                metric_data['values'].append({
                    'timestamp': timestamp,
                    'value': value
                })
                
                # Update statistics
                if value is not None:
                    metric_data['latest_value'] = value
                    metric_data['min_value'] = min(metric_data['min_value'], value)
                    metric_data['max_value'] = max(metric_data['max_value'], value)
                    
                    values = [v['value'] for v in metric_data['values'] if v['value'] is not None]
                    metric_data['avg_value'] = sum(values) / len(values) if values else 0.0
                
                interface_data['latest_timestamp'] = timestamp
                network_metrics['metric_types'].add(metric_name)
                network_metrics['data_points'] += 1
                
            except Exception as e:
                self.logger.warning(f"Error processing network metric row: {e}")
                continue
        
        # Convert sets to lists
        network_metrics['metric_types'] = list(network_metrics['metric_types'])
        
        # Calculate cluster network summary
        network_metrics['cluster_network_summary'] = self._calculate_network_summary(network_metrics['network_interfaces'])
        
        return network_metrics
    
    # Helper methods
    def _extract_columns(self, table) -> List[str]:
        """Extract column names from table."""
        columns = []
        if hasattr(table, 'columns') and table.columns:
            for col in table.columns:
                if hasattr(col, 'name'):
                    columns.append(col.name)
                elif isinstance(col, str):
                    columns.append(col)
                else:
                    columns.append(str(col))
        return columns
    
    def _extract_row_data(self, row, columns: List[str]) -> Dict[str, Any]:
        """Extract row data into dictionary."""
        row_dict = {}
        row_data = row if isinstance(row, (list, tuple)) else [row]
        
        for idx, value in enumerate(row_data):
            if idx < len(columns):
                row_dict[columns[idx]] = value
        
        return row_dict
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float."""
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None
    
    def _get_metric_unit(self, counter_name: str) -> str:
        """Get appropriate unit for metric."""
        counter_lower = counter_name.lower()
        if 'nanocores' in counter_lower:
            return 'nanocores'
        elif 'bytes' in counter_lower:
            return 'bytes'
        elif 'percent' in counter_lower:
            return 'percent'
        elif 'seconds' in counter_lower:
            return 'seconds'
        else:
            return 'count'
    
    def _get_storage_metric_unit(self, metric_name: str) -> str:
        """Get appropriate unit for storage metrics."""
        metric_lower = metric_name.lower()
        if 'bytes' in metric_lower:
            return 'bytes'
        elif 'percent' in metric_lower:
            return 'percent'
        elif 'inodes' in metric_lower:
            return 'count'
        else:
            return 'count'
    
    def _get_network_metric_unit(self, metric_name: str) -> str:
        """Get appropriate unit for network metrics."""
        metric_lower = metric_name.lower()
        if 'bytes' in metric_lower:
            return 'bytes'
        elif 'packets' in metric_lower:
            return 'packets'
        elif 'errors' in metric_lower or 'dropped' in metric_lower:
            return 'count'
        else:
            return 'count'
    
    def _calculate_node_aggregations(self, nodes: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate aggregated metrics across all nodes."""
        aggregations = {
            'total_nodes': len(nodes),
            'metrics_summary': {},
            'cluster_totals': {}
        }
        
        # Aggregate metrics across all nodes
        all_metrics = set()
        for node_data in nodes.values():
            all_metrics.update(node_data['metrics'].keys())
        
        for metric_name in all_metrics:
            metric_values = []
            for node_data in nodes.values():
                if metric_name in node_data['metrics'] and node_data['metrics'][metric_name]['latest_value'] is not None:
                    metric_values.append(node_data['metrics'][metric_name]['latest_value'])
            
            if metric_values:
                aggregations['metrics_summary'][metric_name] = {
                    'cluster_average': sum(metric_values) / len(metric_values),
                    'cluster_total': sum(metric_values),
                    'cluster_max': max(metric_values),
                    'cluster_min': min(metric_values),
                    'node_count': len(metric_values)
                }
        
        return aggregations
    
    def _calculate_namespace_aggregations(self, pods: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate aggregated metrics by namespace."""
        namespace_aggs = {}
        
        for pod_data in pods.values():
            namespace = pod_data['namespace']
            if namespace not in namespace_aggs:
                namespace_aggs[namespace] = {
                    'pod_count': 0,
                    'metrics_summary': {}
                }
            
            namespace_aggs[namespace]['pod_count'] += 1
            
            # Aggregate metrics for this namespace
            for metric_name, metric_data in pod_data['metrics'].items():
                if metric_name not in namespace_aggs[namespace]['metrics_summary']:
                    namespace_aggs[namespace]['metrics_summary'][metric_name] = {
                        'values': [],
                        'total': 0.0,
                        'average': 0.0,
                        'max': float('-inf'),
                        'min': float('inf')
                    }
                
                if metric_data['latest_value'] is not None:
                    ns_metric = namespace_aggs[namespace]['metrics_summary'][metric_name]
                    ns_metric['values'].append(metric_data['latest_value'])
                    ns_metric['total'] += metric_data['latest_value']
                    ns_metric['max'] = max(ns_metric['max'], metric_data['latest_value'])
                    ns_metric['min'] = min(ns_metric['min'], metric_data['latest_value'])
        
        # Calculate averages
        for namespace_data in namespace_aggs.values():
            for metric_data in namespace_data['metrics_summary'].values():
                if metric_data['values']:
                    metric_data['average'] = metric_data['total'] / len(metric_data['values'])
        
        return namespace_aggs
    
    def _calculate_pod_aggregations(self, containers: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate pod-level aggregations from container metrics."""
        pod_aggs = {}
        
        for container_data in containers.values():
            pod_key = f"{container_data['namespace']}/{container_data['pod_name']}"
            if pod_key not in pod_aggs:
                pod_aggs[pod_key] = {
                    'namespace': container_data['namespace'],
                    'pod_name': container_data['pod_name'],
                    'container_count': 0,
                    'metrics_summary': {}
                }
            
            pod_aggs[pod_key]['container_count'] += 1
            
            # Aggregate container metrics to pod level
            for metric_name, metric_data in container_data['metrics'].items():
                if metric_name not in pod_aggs[pod_key]['metrics_summary']:
                    pod_aggs[pod_key]['metrics_summary'][metric_name] = {
                        'total': 0.0,
                        'average': 0.0,
                        'max': float('-inf'),
                        'min': float('inf'),
                        'container_count': 0
                    }
                
                if metric_data['latest_value'] is not None:
                    pod_metric = pod_aggs[pod_key]['metrics_summary'][metric_name]
                    pod_metric['total'] += metric_data['latest_value']
                    pod_metric['max'] = max(pod_metric['max'], metric_data['latest_value'])
                    pod_metric['min'] = min(pod_metric['min'], metric_data['latest_value'])
                    pod_metric['container_count'] += 1
        
        # Calculate averages
        for pod_data in pod_aggs.values():
            for metric_data in pod_data['metrics_summary'].values():
                if metric_data['container_count'] > 0:
                    metric_data['average'] = metric_data['total'] / metric_data['container_count']
        
        return pod_aggs
    
    def _calculate_storage_summary(self, storage_devices: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate cluster storage summary."""
        summary = {
            'total_devices': len(storage_devices),
            'total_capacity_bytes': 0.0,
            'total_used_bytes': 0.0,
            'total_free_bytes': 0.0,
            'average_utilization_percent': 0.0,
            'device_utilizations': []
        }
        
        utilization_values = []
        
        for device_data in storage_devices.values():
            metrics = device_data['metrics']
            
            # Get latest values
            used_bytes = metrics.get('used_bytes', {}).get('latest_value', 0) or 0
            free_bytes = metrics.get('free_bytes', {}).get('latest_value', 0) or 0
            capacity_bytes = metrics.get('capacity_bytes', {}).get('latest_value', 0) or 0
            used_percent = metrics.get('used_percent', {}).get('latest_value', 0) or 0
            
            summary['total_used_bytes'] += used_bytes
            summary['total_free_bytes'] += free_bytes
            summary['total_capacity_bytes'] += capacity_bytes
            
            if used_percent > 0:
                utilization_values.append(used_percent)
                summary['device_utilizations'].append({
                    'device': device_data['device'],
                    'computer': device_data['computer'],
                    'utilization_percent': used_percent,
                    'used_bytes': used_bytes,
                    'capacity_bytes': capacity_bytes
                })
        
        if utilization_values:
            summary['average_utilization_percent'] = sum(utilization_values) / len(utilization_values)
        
        return summary
    
    def _calculate_derived_metrics(self, raw_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived metrics from raw data."""
        derived = {
            'cpu_utilization_percentages': {},
            'memory_utilization_percentages': {},
            'storage_utilization_percentages': {},
            'network_throughput_mbps': {},
            'resource_efficiency_scores': {}
        }
        
        # Calculate CPU utilization percentages from raw nanocores
        node_metrics = raw_metrics.get('node_raw_metrics', {}).get('nodes', {})
        for node_name, node_data in node_metrics.items():
            metrics = node_data.get('metrics', {})
            
            cpu_usage = metrics.get('cpuUsageNanoCores', {}).get('latest_value')
            cpu_capacity = metrics.get('cpuCapacityNanoCores', {}).get('latest_value')
            memory_usage = metrics.get('memoryWorkingSetBytes', {}).get('latest_value')
            memory_capacity = metrics.get('memoryCapacityBytes', {}).get('latest_value')
            
            if cpu_usage is not None and cpu_capacity is not None and cpu_capacity > 0:
                derived['cpu_utilization_percentages'][node_name] = (cpu_usage / cpu_capacity) * 100
            
            if memory_usage is not None and memory_capacity is not None and memory_capacity > 0:
                derived['memory_utilization_percentages'][node_name] = (memory_usage / memory_capacity) * 100
            
            # Calculate efficiency score (balanced CPU/Memory utilization)
            cpu_util = derived['cpu_utilization_percentages'].get(node_name, 0)
            mem_util = derived['memory_utilization_percentages'].get(node_name, 0)
            
            if cpu_util > 0 and mem_util > 0:
                # Efficiency peaks around 70% utilization
                cpu_efficiency = min(cpu_util / 70, 2 - cpu_util / 70) * 100 if cpu_util <= 140 else 0
                mem_efficiency = min(mem_util / 80, 2 - mem_util / 80) * 100 if mem_util <= 160 else 0
                derived['resource_efficiency_scores'][node_name] = (cpu_efficiency + mem_efficiency) / 2
        
        # Calculate storage utilization from storage metrics
        storage_metrics = raw_metrics.get('storage_raw_metrics', {}).get('storage_devices', {})
        for device_key, device_data in storage_metrics.items():
            metrics = device_data.get('metrics', {})
            used_percent = metrics.get('used_percent', {}).get('latest_value')
            if used_percent is not None:
                derived['storage_utilization_percentages'][device_key] = used_percent
        
        # Calculate network throughput in Mbps
        network_metrics = raw_metrics.get('network_raw_metrics', {}).get('network_interfaces', {})
        for interface_key, interface_data in network_metrics.items():
            metrics = interface_data.get('metrics', {})
            rx_bytes = metrics.get('rx_bytes', {}).get('latest_value', 0) or 0
            tx_bytes = metrics.get('tx_bytes', {}).get('latest_value', 0) or 0
            total_bytes_per_sec = rx_bytes + tx_bytes
            derived['network_throughput_mbps'][interface_key] = (total_bytes_per_sec * 8) / (1024 * 1024)  # Convert to Mbps
        
        return derived
    
    def _calculate_network_summary(self, network_interfaces: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate cluster network summary."""
        summary = {
            'total_interfaces': len(network_interfaces),
            'total_rx_bytes_per_sec': 0.0,
            'total_tx_bytes_per_sec': 0.0,
            'total_rx_packets_per_sec': 0.0,
            'total_tx_packets_per_sec': 0.0,
            'total_errors': 0.0,
            'interface_summary': []
        }
        
        for interface_data in network_interfaces.values():
            metrics = interface_data['metrics']
            
            # Get latest values
            rx_bytes = metrics.get('rx_bytes', {}).get('latest_value', 0) or 0
            tx_bytes = metrics.get('tx_bytes', {}).get('latest_value', 0) or 0
            rx_packets = metrics.get('rx_packets', {}).get('latest_value', 0) or 0
            tx_packets = metrics.get('tx_packets', {}).get('latest_value', 0) or 0
            rx_errors = metrics.get('rx_errors', {}).get('latest_value', 0) or 0
            tx_errors = metrics.get('tx_errors', {}).get('latest_value', 0) or 0
            
            summary['total_rx_bytes_per_sec'] += rx_bytes
            summary['total_tx_bytes_per_sec'] += tx_bytes
            summary['total_rx_packets_per_sec'] += rx_packets
            summary['total_tx_packets_per_sec'] += tx_packets
            summary['total_errors'] += (rx_errors + tx_errors)
            
            summary['interface_summary'].append({
                'interface': interface_data['interface'],
                'computer': interface_data['computer'],
                'rx_bytes_per_sec': rx_bytes,
                'tx_bytes_per_sec': tx_bytes,
                'total_bytes_per_sec': rx_bytes + tx_bytes,
                'error_rate': rx_errors + tx_errors
            })
        
        return summary
    
    def _generate_utilization_summary(self, raw_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a utilization summary from raw metrics for the cluster."""
        self.logger.info("Generating utilization summary from raw metrics")
        
        try:
            utilization_summary = {
                "cluster_wide_utilization": {
                    "cpu_average_percent": 0.0,
                    "memory_average_percent": 0.0,
                    "storage_average_percent": 0.0,
                    "network_throughput_mbps": 0.0
                },
                "resource_pressure_indicators": {
                    "cpu_pressure": False,
                    "memory_pressure": False,
                    "storage_pressure": False,
                    "network_pressure": False
                },
                "efficiency_assessment": {
                    "cpu_efficiency_score": 0.0,
                    "memory_efficiency_score": 0.0,
                    "overall_efficiency_score": 0.0
                }
            }

            # Extract node metrics
            node_metrics = raw_metrics.get("node_raw_metrics", {}).get("nodes", {})
            derived_metrics = raw_metrics.get("derived_metrics", {})

            # Calculate cluster-wide averages
            cpu_utilizations = []
            memory_utilizations = []
            storage_utilizations = []
            network_throughputs = []

            for node_name, node_data in node_metrics.items():
                cpu_util = derived_metrics.get("cpu_utilization_percentages", {}).get(node_name, 0.0)
                memory_util = derived_metrics.get("memory_utilization_percentages", {}).get(node_name, 0.0)
                storage_util = derived_metrics.get("storage_utilization_percentages", {}).get(node_name, 0.0)
                network_util = derived_metrics.get("network_utilization_mbps", {}).get(node_name, 0.0)

                if cpu_util > 0:
                    cpu_utilizations.append(cpu_util)
                if memory_util > 0:
                    memory_utilizations.append(memory_util)
                if storage_util > 0:
                    storage_utilizations.append(storage_util)
                if network_util > 0:
                    network_throughputs.append(network_util)

            # Compute averages
            if cpu_utilizations:
                utilization_summary["cluster_wide_utilization"]["cpu_average_percent"] = sum(cpu_utilizations) / len(cpu_utilizations)
            if memory_utilizations:
                utilization_summary["cluster_wide_utilization"]["memory_average_percent"] = sum(memory_utilizations) / len(memory_utilizations)
            if storage_utilizations:
                utilization_summary["cluster_wide_utilization"]["storage_average_percent"] = sum(storage_utilizations) / len(storage_utilizations)
            if network_throughputs:
                utilization_summary["cluster_wide_utilization"]["network_throughput_mbps"] = sum(network_throughputs) / len(network_throughputs)

            # Determine resource pressure (e.g., threshold > 80% indicates pressure)
            utilization_summary["resource_pressure_indicators"]["cpu_pressure"] = (
                utilization_summary["cluster_wide_utilization"]["cpu_average_percent"] > 80
            )
            utilization_summary["resource_pressure_indicators"]["memory_pressure"] = (
                utilization_summary["cluster_wide_utilization"]["memory_average_percent"] > 80
            )
            utilization_summary["resource_pressure_indicators"]["storage_pressure"] = (
                utilization_summary["cluster_wide_utilization"]["storage_average_percent"] > 80
            )
            utilization_summary["resource_pressure_indicators"]["network_pressure"] = (
                utilization_summary["cluster_wide_utilization"]["network_throughput_mbps"] > 1000  # Example threshold
            )

            # Calculate efficiency scores (target 70% CPU, 80% memory)
            cpu_avg = utilization_summary["cluster_wide_utilization"]["cpu_average_percent"]
            memory_avg = utilization_summary["cluster_wide_utilization"]["memory_average_percent"]
            
            cpu_efficiency = min(cpu_avg / 70, 2 - cpu_avg / 70) * 100 if cpu_avg <= 140 else 0
            memory_efficiency = min(memory_avg / 80, 2 - memory_avg / 80) * 100 if memory_avg <= 160 else 0
            
            utilization_summary["efficiency_assessment"]["cpu_efficiency_score"] = cpu_efficiency
            utilization_summary["efficiency_assessment"]["memory_efficiency_score"] = memory_efficiency
            utilization_summary["efficiency_assessment"]["overall_efficiency_score"] = (
                cpu_efficiency + memory_efficiency
            ) / 2

            self.logger.info("Utilization summary generated successfully")
            return utilization_summary

        except Exception as e:
            self.logger.error(f"Failed to generate utilization summary: {e}")
            return {
                "error": str(e),
                "metrics_availability": False,
                "cluster_wide_utilization": {},
                "resource_pressure_indicators": {},
                "efficiency_assessment": {}
            }
    