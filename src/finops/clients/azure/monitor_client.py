"""Azure Monitor client with enhanced metrics collection."""

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
    """Enhanced Azure Monitor client for metrics collection."""
    
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
                self.logger.info("Log Analytics client initialized")
            else:
                self.logger.warning("Log Analytics workspace ID not provided")
            
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
                pass  # Expected for test
            except ClientAuthenticationError:
                return False
            
            return True
            
        except Exception as e:
            self.logger.warning("Azure Monitor health check failed", error=str(e))
            return False
    
    @retry_with_backoff(max_retries=3)
    async def get_cluster_metrics(self, cluster_resource_id: str, cluster_name: str, 
                                 hours: int = 24) -> Dict[str, Any]:
        """Get comprehensive metrics for cluster."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours)
        
        self.logger.info(f"Collecting metrics for cluster: {cluster_name}")
        
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
            'utilization_summary': {},
            'collection_metadata': {
                'metrics_collected': [],
                'metrics_failed': [],
                'total_data_points': 0,
                'data_sources': ['azure_monitor']
            }
        }
        
        try:
            # Collect cluster-level metrics
            cluster_metrics = await self._collect_cluster_level_metrics(
                cluster_resource_id, start_time, end_time
            )
            metrics_data['cluster_metrics'] = cluster_metrics
            metrics_data['collection_metadata']['metrics_collected'].extend(cluster_metrics.keys())
            
            # Collect Log Analytics metrics if available
            if self._logs_client and self.log_analytics_workspace_id:
                log_metrics = await self._collect_log_analytics_metrics(
                    cluster_name, start_time, end_time
                )
                metrics_data['node_metrics'] = log_metrics
                metrics_data['collection_metadata']['data_sources'].append('log_analytics')
            
            # Generate utilization summary
            metrics_data['utilization_summary'] = self._generate_utilization_summary(metrics_data)
            
            # Calculate total data points
            metrics_data['collection_metadata']['total_data_points'] = self._count_data_points(metrics_data)
            
            self.logger.info(
                f"Metrics collection completed for {cluster_name}",
                data_points=metrics_data['collection_metadata']['total_data_points']
            )
            
            return metrics_data
            
        except Exception as e:
            self.logger.error(f"Failed to collect metrics for {cluster_name}: {e}")
            return self._get_default_metrics_structure(cluster_name, str(e))
    
    async def _collect_cluster_level_metrics(self, cluster_resource_id: str, 
                                           start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Collect cluster-level metrics from Azure Monitor."""
        cluster_metrics = {}
        
        # Standard AKS metrics
        metric_names = [
            "node_cpu_usage_millicores",
            "node_memory_working_set_bytes", 
            "node_network_in_bytes",
            "node_network_out_bytes",
            "kube_pod_status_phase",
            "cluster_autoscaler_cluster_safe_to_autoscale"
        ]
        
        granularity = timedelta(minutes=5)
        
        for metric_name in metric_names:
            try:
                metric_data = await self._get_single_metric(
                    cluster_resource_id, metric_name, start_time, end_time, granularity
                )
                if metric_data:
                    cluster_metrics[metric_name] = metric_data
                    
            except Exception as e:
                self.logger.debug(f"Metric {metric_name} not available: {e}")
                continue
        
        return cluster_metrics
    
    async def _get_single_metric(self, resource_id: str, metric_name: str,
                                start_time: datetime, end_time: datetime,
                                granularity: timedelta) -> List[Dict[str, Any]]:
        """Get data for a single metric."""
        try:
            response = self._metrics_client.query_resource(
                resource_uri=resource_id,
                metric_names=[metric_name],
                timespan=(start_time, end_time),
                granularity=granularity,
                aggregations=[
                    MetricAggregationType.AVERAGE,
                    MetricAggregationType.MAXIMUM,
                    MetricAggregationType.MINIMUM
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
                            'metric_name': metric_name,
                            'unit': metric.unit.value if metric.unit else None
                        })
            
            return metric_data
            
        except Exception as e:
            self.logger.debug(f"Error getting metric {metric_name}: {e}")
            return []
    
    async def _collect_log_analytics_metrics(self, cluster_name: str, 
                                           start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Collect metrics from Log Analytics."""
        node_metrics = {
            'nodes': {},
            'aggregated_metrics': {},
            'data_points': 0
        }
        
        if not self._logs_client or not self.log_analytics_workspace_id:
            return node_metrics
        
        hours = int((end_time - start_time).total_seconds() / 3600)
        
        # Query for node performance metrics
        query = f"""
        Perf
        | where TimeGenerated >= ago({hours}h)
        | where ObjectName == "K8SNode"
        | where CounterName in ("cpuUsageNanoCores", "memoryWorkingSetBytes", "networkRxBytes", "networkTxBytes")
        | project TimeGenerated, Computer, CounterName, CounterValue
        | order by TimeGenerated desc
        """
        
        try:
            response = self._logs_client.query_workspace(
                workspace_id=self.log_analytics_workspace_id,
                query=query,
                timespan=(start_time, end_time)
            )
            
            if response and hasattr(response, 'tables') and response.tables:
                node_metrics = self._process_log_analytics_response(response)
            
        except Exception as e:
            self.logger.error(f"Failed to query Log Analytics: {e}")
        
        return node_metrics
    
    def _process_log_analytics_response(self, response) -> Dict[str, Any]:
        """Process Log Analytics response into structured format."""
        node_metrics = {
            'nodes': {},
            'aggregated_metrics': {},
            'data_points': 0
        }
        
        if not response.tables:
            return node_metrics
        
        table = response.tables[0]
        
        for row in table.rows:
            try:
                timestamp = row[0]
                computer = row[1] if len(row) > 1 else "Unknown"
                counter_name = row[2] if len(row) > 2 else "Unknown"
                counter_value = float(row[3]) if len(row) > 3 and row[3] is not None else 0.0
                
                if computer not in node_metrics['nodes']:
                    node_metrics['nodes'][computer] = {
                        'computer_name': computer,
                        'metrics': {},
                        'latest_timestamp': None
                    }
                
                if counter_name not in node_metrics['nodes'][computer]['metrics']:
                    node_metrics['nodes'][computer]['metrics'][counter_name] = {
                        'values': [],
                        'latest_value': None,
                        'avg_value': 0.0
                    }
                
                node_metrics['nodes'][computer]['metrics'][counter_name]['values'].append({
                    'timestamp': timestamp,
                    'value': counter_value
                })
                node_metrics['nodes'][computer]['metrics'][counter_name]['latest_value'] = counter_value
                node_metrics['nodes'][computer]['latest_timestamp'] = timestamp
                node_metrics['data_points'] += 1
                
            except (ValueError, TypeError, IndexError) as e:
                self.logger.warning(f"Error processing log row: {e}")
                continue
        
        # Calculate averages
        for node_data in node_metrics['nodes'].values():
            for metric_data in node_data['metrics'].values():
                values = [v['value'] for v in metric_data['values'] if v['value'] is not None]
                metric_data['avg_value'] = sum(values) / len(values) if values else 0.0
        
        return node_metrics
    
    def _generate_utilization_summary(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate utilization summary from collected metrics."""
        utilization_summary = {
            "cluster_wide_utilization": {
                "avg_cpu_utilization": 0.0,
                "avg_memory_utilization": 0.0,
                "avg_network_utilization": 0.0
            },
            "resource_pressure_indicators": {
                "cpu_pressure": False,
                "memory_pressure": False,
                "network_pressure": False
            },
            "efficiency_assessment": {
                "overall_efficiency_score": 0.0,
                "utilization_rating": "unknown"
            },
            "metrics_availability": True
        }
        
        try:
            # Calculate averages from node metrics
            nodes = metrics_data.get('node_metrics', {}).get('nodes', {})
            
            if nodes:
                cpu_utilizations = []
                memory_utilizations = []
                
                for node_data in nodes.values():
                    metrics = node_data.get('metrics', {})
                    
                    # CPU utilization (convert nanocores to percentage)
                    cpu_metric = metrics.get('cpuUsageNanoCores', {})
                    if cpu_metric.get('avg_value'):
                        # Assuming a standard node has ~2 cores (2,000,000,000 nanocores)
                        cpu_percent = (cpu_metric['avg_value'] / 2000000000) * 100
                        cpu_utilizations.append(min(cpu_percent, 100))
                    
                    # Memory utilization (convert bytes to percentage)
                    memory_metric = metrics.get('memoryWorkingSetBytes', {})
                    if memory_metric.get('avg_value'):
                        # Assuming a standard node has ~8GB (8,589,934,592 bytes)
                        memory_percent = (memory_metric['avg_value'] / 8589934592) * 100
                        memory_utilizations.append(min(memory_percent, 100))
                
                # Calculate cluster averages
                if cpu_utilizations:
                    utilization_summary["cluster_wide_utilization"]["avg_cpu_utilization"] = sum(cpu_utilizations) / len(cpu_utilizations)
                
                if memory_utilizations:
                    utilization_summary["cluster_wide_utilization"]["avg_memory_utilization"] = sum(memory_utilizations) / len(memory_utilizations)
                
                # Determine pressure indicators
                avg_cpu = utilization_summary["cluster_wide_utilization"]["avg_cpu_utilization"]
                avg_memory = utilization_summary["cluster_wide_utilization"]["avg_memory_utilization"]
                
                utilization_summary["resource_pressure_indicators"]["cpu_pressure"] = avg_cpu > 80
                utilization_summary["resource_pressure_indicators"]["memory_pressure"] = avg_memory > 80
                
                # Calculate efficiency score
                efficiency_score = (avg_cpu + avg_memory) / 2
                utilization_summary["efficiency_assessment"]["overall_efficiency_score"] = efficiency_score
                
                if efficiency_score > 70:
                    utilization_summary["efficiency_assessment"]["utilization_rating"] = "excellent"
                elif efficiency_score > 50:
                    utilization_summary["efficiency_assessment"]["utilization_rating"] = "good"
                elif efficiency_score > 30:
                    utilization_summary["efficiency_assessment"]["utilization_rating"] = "fair"
                else:
                    utilization_summary["efficiency_assessment"]["utilization_rating"] = "poor"
        
        except Exception as e:
            self.logger.error(f"Error generating utilization summary: {e}")
            utilization_summary["metrics_availability"] = False
            utilization_summary["error"] = str(e)
        
        return utilization_summary
    
    def _count_data_points(self, metrics_data: Dict[str, Any]) -> int:
        """Count total data points collected."""
        total_points = 0
        
        # Count cluster metrics
        for metric_data in metrics_data.get('cluster_metrics', {}).values():
            if isinstance(metric_data, list):
                total_points += len(metric_data)
        
        # Count node metrics
        node_metrics = metrics_data.get('node_metrics', {})
        total_points += node_metrics.get('data_points', 0)
        
        return total_points
    
    def _get_default_metrics_structure(self, cluster_name: str, error: str) -> Dict[str, Any]:
        """Return default metrics structure for error cases."""
        return {
            'cluster_name': cluster_name,
            'error': error,
            'collection_metadata': {
                'metrics_collected': [],
                'metrics_failed': ["all"],
                'total_data_points': 0,
                'data_sources': []
            },
            'utilization_summary': {
                "metrics_availability": False,
                "error": error
            }
        }