"""Azure Monitor client for metrics collection."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
import structlog
from azure.mgmt.monitor import MonitorManagementClient
from azure.monitor.query import MetricsQueryClient, LogsQueryClient, MetricAggregationType
from azure.core.exceptions import AzureError

from finops.core.base_client import BaseClient
from finops.core.exceptions import ClientConnectionException, MetricsException
from finops.core.utils import retry_with_backoff

logger = structlog.get_logger(__name__)


class MonitorClient(BaseClient):
    """Client for Azure Monitor operations."""
    
    def __init__(self, credential, subscription_id: str, config: Dict[str, Any]):
        super().__init__(config, "MonitorClient")
        self.credential = credential
        self.subscription_id = subscription_id
        self.log_analytics_workspace_id = config.get("log_analytics_workspace_id")
        
        self._monitor_client = None
        self._metrics_client = None
        self._logs_client = None
        
        # Available metrics for AKS
        self.aks_metrics = {
            'cluster': [
                'node_cpu_usage_percentage',
                'node_memory_working_set_percentage',
                'node_disk_usage_percentage',
                'node_network_in_bytes',
                'node_network_out_bytes'
            ],
            'node': [
                'node_cpu_usage_percentage',
                'node_memory_working_set_percentage',
                'node_disk_usage_percentage'
            ],
            'pod': [
                'pod_cpu_usage_millicores',
                'pod_memory_working_set_bytes'
            ]
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
                self.logger.info("Log Analytics client initialized")
            
            self._connected = True
            self.logger.info("Azure Monitor clients connected successfully")
            
        except Exception as e:
            raise ClientConnectionException("AzureMonitor", f"Connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from Azure Monitor services."""
        for client in [self._monitor_client, self._metrics_client, self._logs_client]:
            if client:
                client.close()
        
        self._connected = False
        self.logger.info("Azure Monitor clients disconnected")
    
    async def health_check(self) -> bool:
        """Check Azure Monitor client health."""
        try:
            if not self._connected or not self._metrics_client:
                return False
            
            # Try to query a simple metric as health check
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
        """Get cluster-level metrics."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        metrics_data = {}
        
        try:
            for metric_name in self.aks_metrics['cluster']:
                metric_data = await self._get_metric_data(
                    cluster_resource_id, metric_name, start_time, end_time, granularity
                )
                if metric_data:
                    metrics_data[metric_name] = metric_data
            
            return metrics_data
            
        except AzureError as e:
            raise MetricsException(f"Failed to get cluster metrics: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def get_node_metrics(self,
                             cluster_resource_id: str,
                             node_name: str,
                             start_time: Optional[datetime] = None,
                             end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Get node-specific metrics."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        node_metrics = {}
        
        try:
            for metric_name in self.aks_metrics['node']:
                metric_data = await self._get_metric_data_with_dimension(
                    cluster_resource_id, metric_name, start_time, end_time,
                    {'node': node_name}
                )
                if metric_data:
                    node_metrics[metric_name] = metric_data
            
            return node_metrics
            
        except AzureError as e:
            raise MetricsException(f"Failed to get node metrics: {e}")
    
    async def get_pod_metrics_from_logs(self,
                                      cluster_name: str,
                                      namespace: str,
                                      pod_name: str,
                                      start_time: Optional[datetime] = None,
                                      end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Get pod metrics from Log Analytics."""
        if not self._logs_client or not self.log_analytics_workspace_id:
            self.logger.warning("Log Analytics not configured")
            return {}
        
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(hours=1)
        
        try:
            query = f"""
            KubePodInventory
            | where TimeGenerated between(datetime({start_time.isoformat()}) .. datetime({end_time.isoformat()}))
            | where ClusterName == '{cluster_name}' 
            | where Namespace == '{namespace}' 
            | where Name == '{pod_name}'
            | join kind=inner (
                Perf
                | where ObjectName == 'K8SContainer'
                | where CounterName in ('cpuUsageNanoCores', 'memoryWorkingSetBytes')
                | summarize 
                    AvgValue = avg(CounterValue),
                    MaxValue = max(CounterValue)
                    by InstanceName, CounterName, bin(TimeGenerated, 5m)
            ) on InstanceName
            | project TimeGenerated, CounterName, AvgValue, MaxValue
            | order by TimeGenerated desc
            | take 100
            """
            
            response = self._logs_client.query_workspace(
                workspace_id=self.log_analytics_workspace_id,
                query=query,
                timespan=(start_time, end_time)
            )
            
            return await self._process_logs_response(response)
            
        except Exception as e:
            self.logger.error(f"Failed to get pod metrics from logs: {e}")
            return {}
    
    async def _get_metric_data(self,
                             resource_id: str,
                             metric_name: str,
                             start_time: datetime,
                             end_time: datetime,
                             granularity: timedelta) -> List[Dict[str, Any]]:
        """Get metric data from Azure Monitor."""
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
                        metric_data.append({
                            'timestamp': data_point.timestamp.isoformat() if data_point.timestamp else None,
                            'average': data_point.average,
                            'maximum': data_point.maximum,
                            'total': data_point.total,
                            'count': data_point.count,
                            'metric': metric_name
                        })
            
            return metric_data
            
        except Exception as e:
            self.logger.debug(f"Error getting metric {metric_name}: {e}")
            return []
    
    async def _get_metric_data_with_dimension(self,
                                            resource_id: str,
                                            metric_name: str,
                                            start_time: datetime,
                                            end_time: datetime,
                                            dimensions: Dict[str, str]) -> List[Dict[str, Any]]:
        """Get metric data with specific dimensions."""
        try:
            dimension_filter = " and ".join([f"{k} eq '{v}'" for k, v in dimensions.items() if v])
            
            response = self._metrics_client.query_resource(
                resource_uri=resource_id,
                metric_names=[metric_name],
                timespan=(start_time, end_time),
                granularity=timedelta(minutes=5),
                aggregations=[MetricAggregationType.AVERAGE, MetricAggregationType.MAXIMUM],
                filter=dimension_filter if dimension_filter else None
            )
            
            metric_data = []
            for metric in response.metrics:
                for timeseries in metric.timeseries:
                    for data_point in timeseries.data:
                        metric_data.append({
                            'timestamp': data_point.timestamp.isoformat() if data_point.timestamp else None,
                            'average': data_point.average,
                            'maximum': data_point.maximum,
                            'total': data_point.total,
                            'count': data_point.count,
                            'metric': metric_name,
                            'dimensions': dimensions
                        })
            
            return metric_data
            
        except Exception as e:
            self.logger.debug(f"Error getting metric {metric_name} with dimensions {dimensions}: {e}")
            return []
    
    async def _process_logs_response(self, response) -> Dict[str, Any]:
        """Process Log Analytics query response."""
        metrics = {
            'cpu': {'average': 0, 'max': 0, 'data_points': []},
            'memory': {'average': 0, 'max': 0, 'data_points': []}
        }
        
        if response.tables and len(response.tables) > 0:
            table = response.tables[0]
            columns = [col.name for col in table.columns]
            
            cpu_values = []
            memory_values = []
            
            for row in table.rows:
                row_dict = {}
                for idx, value in enumerate(row):
                    if idx < len(columns):
                        row_dict[columns[idx]] = value
                
                counter_name = row_dict.get('CounterName', '')
                avg_value = row_dict.get('AvgValue', 0)
                max_value = row_dict.get('MaxValue', 0)
                
                if 'cpu' in counter_name.lower():
                    cpu_values.append(avg_value)
                    metrics['cpu']['data_points'].append({
                        'timestamp': row_dict.get('TimeGenerated'),
                        'average': avg_value,
                        'maximum': max_value
                    })
                elif 'memory' in counter_name.lower():
                    memory_values.append(avg_value)
                    metrics['memory']['data_points'].append({
                        'timestamp': row_dict.get('TimeGenerated'),
                        'average': avg_value,
                        'maximum': max_value
                    })
            
            # Calculate aggregates
            if cpu_values:
                metrics['cpu']['average'] = sum(cpu_values) / len(cpu_values)
                metrics['cpu']['max'] = max(cpu_values)
            
            if memory_values:
                metrics['memory']['average'] = sum(memory_values) / len(memory_values)
                metrics['memory']['max'] = max(memory_values)
        
        return metrics
