# src/finops/clients/azure/monitor_client.py
"""Phase 1 Discovery - Azure Monitor client for raw data collection only."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
import structlog
from azure.mgmt.monitor import MonitorManagementClient
from azure.monitor.query import MetricsQueryClient, LogsQueryClient, MetricAggregationType
from azure.core.exceptions import AzureError, ResourceNotFoundError, ClientAuthenticationError
import asyncio
import re
from finops.core.base_client import BaseClient
from finops.core.exceptions import ClientConnectionException, MetricsException
from finops.core.utils import retry_with_backoff

logger = structlog.get_logger(__name__)


class MonitorClient(BaseClient):
    """Phase 1 Discovery - Azure Monitor client for raw metrics collection."""
    
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
    async def collect_cluster_metrics(self, cluster_resource_id: str, cluster_name: str, 
                                    hours: int = 24) -> Dict[str, Any]:
        """Phase 1: Collect raw metrics data only - no analysis."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours)
        
        self.logger.info(f"Starting raw metrics collection for cluster: {cluster_name}")
        
        # Raw metrics collection structure
        raw_metrics = {
            'cluster_name': cluster_name,
            'cluster_resource_id': cluster_resource_id,
            'collection_timestamp': datetime.now(timezone.utc).isoformat(),
            'collection_period': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'hours': hours
            },
            'azure_monitor_metrics': {},
            'container_insights_data': {},
            'collection_metadata': {
                'metrics_attempted': 0,
                'metrics_collected': 0,
                'queries_attempted': 0,
                'queries_successful': 0,
                'collection_errors': []
            }
        }
        
        # Collect Azure Monitor metrics
        await self._collect_azure_monitor_raw_metrics(
            cluster_resource_id, start_time, end_time, raw_metrics
        )
        
        # Collect Container Insights raw data
        if self._logs_client and self.log_analytics_workspace_id:
            await self._collect_container_insights_raw_data(
                cluster_name, start_time, end_time, raw_metrics
            )
        else:
            raw_metrics['collection_metadata']['collection_errors'].append(
                'Log Analytics workspace not configured'
            )
        
        self.logger.info(
            f"Raw metrics collection completed for {cluster_name}",
            metrics_collected=raw_metrics['collection_metadata']['metrics_collected'],
            queries_successful=raw_metrics['collection_metadata']['queries_successful']
        )
        
        return raw_metrics
    
    async def _collect_container_insights_raw_data(self, cluster_name: str,
                                             start_time: datetime, end_time: datetime,
                                             raw_metrics: Dict[str, Any]) -> None:
        """Complete Container Insights collection with 30 comprehensive queries"""
        
        metadata = raw_metrics['collection_metadata']
        
        # Time ranges for different query types
        time_ranges = {
            'performance': 6,
            'utilization': 24,
            'inventory': 2,
            'events': 12,
            'storage': 6,
            'network': 6,
            'control_plane': 6,
            'discovery': 24
        }
        
        # Complete collection strategy - 30 queries
        collection_strategy = {
            # HIGH PRIORITY - Core FinOps (6 queries)
            'high_priority': {
                'aks_node_cpu_performance': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['performance']}h)
                    | where ObjectName == "K8SNode" and CounterName == "cpuUsageNanoCores"
                    | extend NodeName = Computer
                    | summarize 
                        avg_cpu_usage_nanocores = avg(todouble(CounterValue)),
                        max_cpu_usage_nanocores = max(todouble(CounterValue)),
                        p95_cpu_usage = percentile(todouble(CounterValue), 95),
                        sample_count = count()
                        by NodeName, bin(TimeGenerated, 5m)
                    | extend cpu_usage_millicores = avg_cpu_usage_nanocores / 1000000
                    | order by TimeGenerated desc, NodeName | take 500
                """,
                
                'aks_node_memory_performance': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['performance']}h)
                    | where ObjectName == "K8SNode"
                    | where CounterName in ("memoryUsageBytes", "memoryCapacityBytes")
                    | extend NodeName = Computer
                    | summarize 
                        memory_usage_bytes = avgif(todouble(CounterValue), CounterName == "memoryUsageBytes"),
                        memory_capacity_bytes = avgif(todouble(CounterValue), CounterName == "memoryCapacityBytes"),
                        max_memory_usage = maxif(todouble(CounterValue), CounterName == "memoryUsageBytes")
                        by NodeName, bin(TimeGenerated, 5m)
                    | extend 
                        memory_usage_percentage = case(memory_capacity_bytes > 0, (memory_usage_bytes / memory_capacity_bytes) * 100, 0.0),
                        memory_usage_gb = memory_usage_bytes / 1024 / 1024 / 1024
                    | order by TimeGenerated desc, NodeName | take 500
                """,
                
                'aks_node_network_performance': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['network']}h)
                    | where ObjectName == "Network Interface"
                    | where CounterName in ("Bytes Received/sec", "Bytes Sent/sec")
                    | extend NodeName = Computer
                    | summarize 
                        network_in_bytes_sec = avgif(todouble(CounterValue), CounterName == "Bytes Received/sec"),
                        network_out_bytes_sec = avgif(todouble(CounterValue), CounterName == "Bytes Sent/sec")
                        by NodeName, bin(TimeGenerated, 5m)
                    | extend 
                        total_network_bytes_sec = network_in_bytes_sec + network_out_bytes_sec,
                        network_in_mbps = network_in_bytes_sec * 8 / 1024 / 1024
                    | order by TimeGenerated desc, NodeName | take 300
                """,
                
                'pod_cpu_usage': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['utilization']}h)
                    | where ObjectName == "K8SContainer"
                    | where CounterName in ("cpuUsageNanoCores", "cpuRequestNanoCores", "cpuLimitNanoCores")
                    | extend PodName = replace_regex(InstanceName, @"^([^/]+)/.*", @"\\1")
                    | extend ContainerName = replace_regex(InstanceName, @"^[^/]+/(.*)$", @"\\1")
                    | summarize 
                        cpu_usage_nanocores = avgif(todouble(CounterValue), CounterName == "cpuUsageNanoCores"),
                        cpu_request_nanocores = avgif(todouble(CounterValue), CounterName == "cpuRequestNanoCores"),
                        cpu_limit_nanocores = avgif(todouble(CounterValue), CounterName == "cpuLimitNanoCores")
                        by PodName, ContainerName, bin(TimeGenerated, 10m)
                    | extend 
                        cpu_usage_millicores = cpu_usage_nanocores / 1000000,
                        cpu_efficiency = case(cpu_request_nanocores > 0, (cpu_usage_nanocores / cpu_request_nanocores) * 100, 0.0)
                    | order by TimeGenerated desc, PodName | take 1000
                """,
                
                'pod_memory_usage': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['utilization']}h)
                    | where ObjectName == "K8SContainer"
                    | where CounterName in ("memoryUsageBytes", "memoryRequestBytes", "memoryLimitBytes")
                    | extend PodName = replace_regex(InstanceName, @"^([^/]+)/.*", @"\\1")
                    | extend ContainerName = replace_regex(InstanceName, @"^[^/]+/(.*)$", @"\\1")
                    | summarize 
                        memory_usage_bytes = avgif(todouble(CounterValue), CounterName == "memoryUsageBytes"),
                        memory_request_bytes = avgif(todouble(CounterValue), CounterName == "memoryRequestBytes"),
                        memory_limit_bytes = avgif(todouble(CounterValue), CounterName == "memoryLimitBytes")
                        by PodName, ContainerName, bin(TimeGenerated, 10m)
                    | extend 
                        memory_usage_mb = memory_usage_bytes / 1024 / 1024,
                        memory_efficiency = case(memory_request_bytes > 0, (memory_usage_bytes / memory_request_bytes) * 100, 0.0)
                    | order by TimeGenerated desc, PodName | take 1000
                """,
                
                'pod_usage_with_limits': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['utilization']}h)
                    | where ObjectName == "K8SContainer"
                    | where CounterName in ("cpuUsageNanoCores", "memoryUsageBytes")
                    | extend PodName = replace_regex(InstanceName, @"^([^/]+)/.*", @"\\1")
                    | summarize 
                        avg_cpu_usage = avgif(todouble(CounterValue), CounterName == "cpuUsageNanoCores"),
                        avg_memory_usage = avgif(todouble(CounterValue), CounterName == "memoryUsageBytes")
                        by PodName, bin(TimeGenerated, 15m)
                    | extend 
                        cpu_usage_millicores = avg_cpu_usage / 1000000,
                        memory_usage_mb = avg_memory_usage / 1024 / 1024
                    | order by TimeGenerated desc, PodName | take 800
                """
            },
            
            # MEDIUM PRIORITY - Container & Resource Details (9 queries)
            'medium_priority': {
                'container_cpu_detailed': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['performance']}h)
                    | where ObjectName == "K8SContainer"
                    | where CounterName in ("cpuUsageNanoCores", "cpuThrottledNanoCores")
                    | extend PodName = replace_regex(InstanceName, @"^([^/]+)/.*", @"\\1")
                    | extend ContainerName = replace_regex(InstanceName, @"^[^/]+/(.*)$", @"\\1")
                    | summarize 
                        cpu_usage_rate = avgif(todouble(CounterValue), CounterName == "cpuUsageNanoCores"),
                        cpu_throttled_rate = avgif(todouble(CounterValue), CounterName == "cpuThrottledNanoCores")
                        by ContainerName, PodName, bin(TimeGenerated, 5m)
                    | extend cpu_throttled_percentage = case(cpu_usage_rate > 0, (cpu_throttled_rate / cpu_usage_rate) * 100, 0.0)
                    | order by TimeGenerated desc, PodName | take 600
                """,
                
                'container_memory_detailed': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['performance']}h)
                    | where ObjectName == "K8SContainer"
                    | where CounterName in ("memoryUsageBytes", "memoryRssBytes", "memoryWorkingSetBytes")
                    | extend PodName = replace_regex(InstanceName, @"^([^/]+)/.*", @"\\1")
                    | extend ContainerName = replace_regex(InstanceName, @"^[^/]+/(.*)$", @"\\1")
                    | summarize 
                        memory_usage = avgif(todouble(CounterValue), CounterName == "memoryUsageBytes"),
                        memory_rss = avgif(todouble(CounterValue), CounterName == "memoryRssBytes"),
                        memory_working_set = avgif(todouble(CounterValue), CounterName == "memoryWorkingSetBytes")
                        by ContainerName, PodName, bin(TimeGenerated, 5m)
                    | extend memory_usage_mb = memory_usage / 1024 / 1024
                    | order by TimeGenerated desc, PodName | take 600
                """,
                
                'container_utilization_insights': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['performance']}h)
                    | where ObjectName == "K8SContainer"
                    | where CounterName in ("cpuUsageNanoCores", "memoryUsageBytes")
                    | extend PodName = replace_regex(InstanceName, @"^([^/]+)/.*", @"\\1")
                    | extend ContainerName = replace_regex(InstanceName, @"^[^/]+/(.*)$", @"\\1")
                    | extend ResourceType = case(CounterName == "cpuUsageNanoCores", "cpu", "memory")
                    | extend MetricValue = todouble(CounterValue)
                    | summarize 
                        avg_usage = avg(MetricValue),
                        max_usage = max(MetricValue),
                        stddev_usage = stdev(MetricValue)
                        by ContainerName, PodName, ResourceType, bin(TimeGenerated, 30m)
                    | extend utilization_variability = case(avg_usage > 0, (stddev_usage / avg_usage) * 100, 0.0)
                    | order by TimeGenerated desc, ContainerName | take 800
                """,
                
                'pod_resource_requests_limits': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['inventory']}h)
                    | where ObjectName == "K8SContainer"
                    | where CounterName in ("cpuRequestNanoCores", "cpuLimitNanoCores", "memoryRequestBytes", "memoryLimitBytes")
                    | extend PodName = replace_regex(InstanceName, @"^([^/]+)/.*", @"\\1")
                    | summarize 
                        cpu_request = avgif(todouble(CounterValue), CounterName == "cpuRequestNanoCores"),
                        cpu_limit = avgif(todouble(CounterValue), CounterName == "cpuLimitNanoCores"),
                        memory_request = avgif(todouble(CounterValue), CounterName == "memoryRequestBytes"),
                        memory_limit = avgif(todouble(CounterValue), CounterName == "memoryLimitBytes")
                        by PodName, bin(TimeGenerated, 30m)
                    | extend
                        cpu_request_millicores = cpu_request / 1000000,
                        memory_request_mb = memory_request / 1024 / 1024
                    | order by TimeGenerated desc, PodName | take 600
                """,
                
                'resource_quota_limits': f"""
                    KubePodInventory
                    | where TimeGenerated >= ago({time_ranges['inventory']}h)
                    | summarize 
                        total_pods = dcount(Name),
                        running_pods = dcountif(Name, PodStatus == "Running"),
                        pending_pods = dcountif(Name, PodStatus == "Pending"),
                        failed_pods = dcountif(Name, PodStatus == "Failed")
                        by Namespace, bin(TimeGenerated, 1h)
                    | extend pod_success_rate = case(total_pods > 0, (running_pods * 100.0) / total_pods, 0.0)
                    | order by TimeGenerated desc, Namespace | take 300
                """,
                
                'node_capacity_vs_allocation': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['inventory']}h)
                    | where ObjectName == "K8SNode"
                    | where CounterName in ("cpuCapacityNanoCores", "memoryCapacityBytes")
                    | extend NodeName = Computer
                    | summarize 
                        cpu_capacity = avgif(todouble(CounterValue), CounterName == "cpuCapacityNanoCores"),
                        memory_capacity = avgif(todouble(CounterValue), CounterName == "memoryCapacityBytes")
                        by NodeName, bin(TimeGenerated, 30m)
                    | extend
                        cpu_capacity_cores = cpu_capacity / 1000000000,
                        memory_capacity_gb = memory_capacity / 1024 / 1024 / 1024
                    | order by TimeGenerated desc, NodeName | take 200
                """,
                
                'namespace_resource_attribution': f"""
                    KubePodInventory
                    | where TimeGenerated >= ago(24h)
                    | where isnotempty(Name) and isnotempty(Namespace) and column_ifexists("Node", "") != ""
                    | extend Node = column_ifexists("Node", "")
                    | summarize 
                        total_pods = dcount(Name),
                        running_pods = dcountif(Name, PodStatus == "Running"),
                        unique_controllers = dcount(ControllerName),
                        nodes_used = dcount(Node)
                        by Namespace, bin(TimeGenerated, 1h)
                    | extend namespace_efficiency = case(total_pods > 0, (running_pods * 100.0) / total_pods, 0.0)
                    | order by TimeGenerated desc, Namespace | take 400



                    
                """,
                
                'namespace_summary': f"""
                    KubePodInventory
                    | where TimeGenerated >= ago({time_ranges['discovery']}h)
                    | summarize 
                        pod_count = count(),
                        unique_pods = dcount(Name),
                        deployment_count = dcountif(ControllerName, ControllerKind == "ReplicaSet"),
                        daemonset_count = dcountif(ControllerName, ControllerKind == "DaemonSet")
                        by Namespace
                    | extend workload_complexity = deployment_count + daemonset_count
                    | order by workload_complexity desc | take 100
                """,
                
                'workload_attribution': f"""
                    KubePodInventory
                    | where TimeGenerated >= ago(24h)
                    | where isnotempty(ControllerName) and isnotempty(ControllerKind)
                    | extend WorkloadType = ControllerKind, WorkloadName = ControllerName
                    | extend RestartCount = toint(column_ifexists("RestartCount", 0))
                    | summarize 
                        pod_count = count(),
                        running_pods = countif(PodStatus == "Running"),
                        avg_restart_count = avg(RestartCount)
                        by WorkloadType, WorkloadName, bin(TimeGenerated, 2h)
                    | extend workload_health = case(pod_count > 0, (running_pods * 100.0) / pod_count, 0.0)
                    | order by TimeGenerated desc, WorkloadType | take 300



                """
            },
            
            # LOW PRIORITY - Events (4 queries)
            'low_priority': {
                'kubernetes_scheduling_events': f"""
                    KubeEvents
                    | where TimeGenerated >= ago({time_ranges['events']}h)
                    | where Reason in ("FailedScheduling", "Scheduled", "Preempted")
                    | summarize 
                        event_count = count(),
                        affected_objects = dcount(Name)
                        by Type, Reason, ObjectKind, Namespace, bin(TimeGenerated, 1h)
                    | extend impact_score = event_count * affected_objects
                    | order by impact_score desc | take 200
                """,
                
                'kubernetes_resource_events': f"""
                    KubeEvents
                    | where TimeGenerated >= ago({time_ranges['events']}h)
                    | where Reason in ("Created", "Started", "Killing", "Failed")
                    | summarize 
                        event_count = count(),
                        latest_event = max(TimeGenerated)
                        by Reason, ObjectKind, Namespace, bin(TimeGenerated, 30m)
                    | order by latest_event desc | take 300
                """,
                
                'kubernetes_warning_events': f"""
                    KubeEvents
                    | where TimeGenerated >= ago({time_ranges['events']}h)
                    | where Type == "Warning"
                    | summarize 
                        warning_count = count(),
                        affected_resources = dcount(Name)
                        by Reason, ObjectKind, Namespace, bin(TimeGenerated, 1h)
                    | extend warning_severity = case(warning_count <= 5, "low", warning_count <= 20, "medium", "high")
                    | order by warning_count desc | take 150
                """,
                
                'pod_restart_events': f"""
                    KubeEvents
                    | where TimeGenerated >= ago({time_ranges['events']}h)
                    | where Reason in ("Killing", "Started", "BackOff")
                    | extend EventCategory = case(Reason == "Killing", "termination", Reason == "Started", "startup", "failure")
                    | summarize 
                        restart_events = countif(EventCategory == "termination"),
                        failure_events = countif(EventCategory == "failure")
                        by Name, Namespace, bin(TimeGenerated, 1h)
                    | extend stability_score = case(failure_events == 0, 100, failure_events <= 3, 80, 50)
                    | order by restart_events desc | take 250
                """
            },
            
            # ADDITIONAL MONITORING - New Queries (9 queries)
            'additional_monitoring': {
                'volume_usage': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['storage']}h)
                    | where ObjectName == "LogicalDisk"
                    | where CounterName in ("% Free Space", "Free Megabytes")
                    | extend NodeName = Computer, DiskInstance = InstanceName
                    | summarize 
                        free_space_percentage = avgif(todouble(CounterValue), CounterName == "% Free Space"),
                        free_megabytes = avgif(todouble(CounterValue), CounterName == "Free Megabytes")
                        by NodeName, DiskInstance, bin(TimeGenerated, 10m)
                    | extend disk_pressure_level = case(free_space_percentage <= 15, "high", free_space_percentage <= 30, "medium", "normal")
                    | order by TimeGenerated desc, NodeName | take 400
                """,
                
                'persistent_volume_claim_metrics': f"""
                    InsightsMetrics
                    | where TimeGenerated >= ago(6h)
                    | where Name in ("used_bytes", "capacity_bytes")
                    | where typeof(Tags) == 'dynamic' and Tags has "volume"
                    | extend PVName = tostring(Tags["volume"])
                    | summarize 
                        used_bytes = avgif(todouble(Val), Name == "used_bytes"),
                        capacity_bytes = avgif(todouble(Val), Name == "capacity_bytes")
                        by PVName, bin(TimeGenerated, 10m)
                    | extend used_percentage = case(capacity_bytes > 0, (used_bytes / capacity_bytes) * 100, 0.0)
                    | order by TimeGenerated desc, PVName | take 300
                """,
                
                'pod_network_traffic': f"""
                    InsightsMetrics
                    | where TimeGenerated >= ago(6h)
                    | where Name in ("rx_bytes", "tx_bytes")
                    | where typeof(Tags) == "dynamic" and Tags has "pod"
                    | extend PodName = tostring(Tags["pod"])
                    | summarize 
                        rx_bytes = avgif(todouble(Val), Name == "rx_bytes"),
                        tx_bytes = avgif(todouble(Val), Name == "tx_bytes")
                        by PodName, bin(TimeGenerated, 5m)
                    | extend total_bytes = rx_bytes + tx_bytes
                    | order by TimeGenerated desc, PodName | take 500
                """,
                
                'pod_network_connectivity': f"""
                    InsightsMetrics
                    | where TimeGenerated >= ago(6h)
                    | where Name in ("rx_packets", "rx_dropped")
                    | where typeof(Tags) == "dynamic" and Tags has "pod"
                    | extend PodName = tostring(Tags["pod"])
                    | summarize 
                        rx_packets = avgif(todouble(Val), Name == "rx_packets"),
                        rx_dropped = avgif(todouble(Val), Name == "rx_dropped")
                        by PodName, bin(TimeGenerated, 5m)
                    | extend packet_loss_rate = case(rx_packets > 0, (rx_dropped / rx_packets) * 100, 0.0)
                    | order by TimeGenerated desc, PodName | take 400
                """,
                
                'kube_apiserver_metrics': f"""
                    InsightsMetrics
                    | where TimeGenerated >= ago({time_ranges['control_plane']}h)
                    | where Namespace == "kube-system"
                    | where Name contains "apiserver_request"
                    | summarize 
                        avg_request_duration = avg(todouble(Val)),
                        total_requests = sum(todouble(Val))
                        by bin(TimeGenerated, 5m)
                    | extend latency_ms = avg_request_duration * 1000
                    | order by TimeGenerated desc | take 200
                """,
                
                'etcd_health_metrics': f"""
                    InsightsMetrics
                    | where TimeGenerated >= ago({time_ranges['control_plane']}h)
                    | where Namespace == "kube-system"
                    | where Name contains "etcd"
                    | summarize 
                        avg_commit_duration = avg(todouble(Val)),
                        sample_count = count()
                        by bin(TimeGenerated, 5m)
                    | extend commit_latency_ms = avg_commit_duration * 1000
                    | order by TimeGenerated desc | take 200
                """,
                
                'node_disk_filesystem_usage': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['performance']}h)
                    | where ObjectName == "LogicalDisk"
                    | where CounterName in ("Disk Reads/sec", "Disk Writes/sec", "% Disk Time")
                    | extend NodeName = Computer
                    | summarize 
                        disk_reads_sec = avgif(todouble(CounterValue), CounterName == "Disk Reads/sec"),
                        disk_writes_sec = avgif(todouble(CounterValue), CounterName == "Disk Writes/sec"),
                        disk_time_percentage = avgif(todouble(CounterValue), CounterName == "% Disk Time")
                        by NodeName, bin(TimeGenerated, 5m)
                    | extend total_iops = disk_reads_sec + disk_writes_sec
                    | order by TimeGenerated desc, NodeName | take 300
                """,
                
                'container_oom_throttling': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['performance']}h)
                    | where ObjectName == "K8SContainer"
                    | where CounterName in ("cpuThrottledNanoCores", "memoryUsageBytes", "memoryLimitBytes")
                    | extend PodName = replace_regex(InstanceName, @"^([^/]+)/.*", @"\\1")
                    | summarize 
                        cpu_throttled = avgif(todouble(CounterValue), CounterName == "cpuThrottledNanoCores"),
                        memory_usage = avgif(todouble(CounterValue), CounterName == "memoryUsageBytes"),
                        memory_limit = avgif(todouble(CounterValue), CounterName == "memoryLimitBytes")
                        by PodName, bin(TimeGenerated, 10m)
                    | extend 
                        is_throttled = cpu_throttled > 0,
                        memory_pressure = case(memory_limit > 0, (memory_usage / memory_limit) * 100, 0.0)
                    | order by TimeGenerated desc, PodName | take 400
                """,
                
                'cluster_capacity_summary': f"""
                    Perf
                    | where TimeGenerated >= ago({time_ranges['inventory']}h)
                    | where ObjectName == "K8SNode"
                    | where CounterName in ("cpuCapacityNanoCores", "memoryCapacityBytes", "cpuUsageNanoCores", "memoryUsageBytes")
                    | summarize 
                        total_cpu_capacity = sumif(todouble(CounterValue), CounterName == "cpuCapacityNanoCores"),
                        total_memory_capacity = sumif(todouble(CounterValue), CounterName == "memoryCapacityBytes"),
                        total_cpu_usage = sumif(todouble(CounterValue), CounterName == "cpuUsageNanoCores"),
                        total_memory_usage = sumif(todouble(CounterValue), CounterName == "memoryUsageBytes"),
                        node_count = dcount(Computer)
                        by bin(TimeGenerated, 30m)
                    | extend 
                        cpu_utilization_percentage = case(total_cpu_capacity > 0, (total_cpu_usage / total_cpu_capacity) * 100, 0.0),
                        memory_utilization_percentage = case(total_memory_capacity > 0, (total_memory_usage / total_memory_capacity) * 100, 0.0)
                    | order by TimeGenerated desc | take 100
                """
            }
        }
        
        # Flatten all queries for counting
        all_queries = {}
        for priority_group in collection_strategy.values():
            all_queries.update(priority_group)
        
        metadata['queries_attempted'] = len(all_queries)
        metadata['time_ranges_used'] = time_ranges
        
        try:
            self.logger.info(f"Starting complete Container Insights collection for {cluster_name}")
            self.logger.info(f"Total queries: {len(all_queries)} (30 comprehensive queries)")
            
            # Collect in priority order
            for priority_level, queries in collection_strategy.items():
                self.logger.debug(f"Collecting {priority_level}: {list(queries.keys())}")
                
                for query_name, query in queries.items():
                    success = await self._collect_table_with_enhanced_handling(
                        query_name, query, start_time, end_time, raw_metrics, priority_level, timeout=60
                    )
                    
                    if not success and priority_level == 'high_priority':
                        self.logger.warning(f"High priority query {query_name} failed")
            
            # Add collection quality indicators
            self._add_comprehensive_collection_quality_indicators(raw_metrics, cluster_name)
            
            # Log final summary
            successful_queries = list(raw_metrics.get('container_insights_data', {}).keys())
            self.logger.info(f"Complete collection finished: {metadata['queries_successful']}/{metadata['queries_attempted']} successful")
            
        except Exception as e:
            metadata['collection_errors'].append(f"Collection failed: {str(e)}")
            self.logger.error(f"Collection failed: {e}")


    async def _collect_table_with_enhanced_handling(self, table_name: str, query: str,
                                                start_time: datetime, end_time: datetime,
                                                raw_metrics: Dict[str, Any], priority_level: str,
                                                timeout: int = 60) -> bool:
        """Enhanced table collection with column-row mapping."""
        
        metadata = raw_metrics['collection_metadata']
        
        try:
            response = self._logs_client.query_workspace(
                workspace_id=self.log_analytics_workspace_id,
                query=query,
                timespan=(start_time, end_time)
            )
            
            if response and response.tables and len(response.tables) > 0:
                table = response.tables[0]
                
                if table.rows and len(table.rows) > 0:
                    columns = list(table.columns) if table.columns else [f"column_{i}" for i in range(len(table.rows[0]))]
                    
                    # Create structured data points
                    data_points = []
                    for row in table.rows:
                        data_point = dict(zip(columns, list(row)))
                        data_points.append(data_point)
                    
                    raw_data = {
                        'table_name': table_name,
                        'description': self._get_table_description(table_name),
                        'priority_level': priority_level,
                        'finops_category': self._get_finops_category(table_name),
                        'columns': columns,
                        'data_points': data_points,
                        'row_count': len(table.rows),
                        'column_count': len(columns),
                        'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                        'query_executed': query,
                        'time_range_hours': self._extract_time_range_from_query(query)
                    }
                    
                    raw_metrics['container_insights_data'][table_name] = raw_data
                    metadata['queries_successful'] += 1
                    
                    self.logger.debug(f"✓ {table_name}: {len(data_points)} data points ({priority_level})")
                    return True
                else:
                    metadata['collection_errors'].append(f"{table_name}: No data returned")
                    return False
            else:
                metadata['collection_errors'].append(f"{table_name}: No response")
                return False
                
        except Exception as e:
            metadata['collection_errors'].append(f"{table_name}: {str(e)}")
            self.logger.warning(f"✗ {table_name}: {e} ({priority_level})")
            return False


    async def _collect_azure_monitor_raw_metrics(self, cluster_resource_id: str,
                                            start_time: datetime, end_time: datetime,
                                            raw_metrics: Dict[str, Any]) -> None:
        """Collect raw Azure Monitor metrics."""
        
        metadata = raw_metrics['collection_metadata']
        
        metrics_to_collect = [
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
        
        metadata['metrics_attempted'] = len(metrics_to_collect)
        
        try:
            granularity = timedelta(minutes=5)
            
            for metric_name in metrics_to_collect:
                try:
                    response = self._metrics_client.query_resource(
                        resource_uri=cluster_resource_id,
                        metric_names=[metric_name],
                        timespan=(start_time, end_time),
                        granularity=granularity,
                        aggregations=[MetricAggregationType.AVERAGE, MetricAggregationType.MAXIMUM, MetricAggregationType.MINIMUM]
                    )
                    
                    if response and response.metrics:
                        metric = response.metrics[0]
                        if metric.timeseries:
                            raw_data_points = []
                            for timeseries in metric.timeseries:
                                for data_point in timeseries.data:
                                    raw_data_points.append({
                                        'timestamp': data_point.timestamp.isoformat(),
                                        'average': data_point.average,
                                        'maximum': data_point.maximum,
                                        'minimum': data_point.minimum
                                    })
                            
                            raw_metrics['azure_monitor_metrics'][metric_name] = {
                                'metric_name': metric_name,
                                'data_points': raw_data_points,
                                'collection_timestamp': datetime.now(timezone.utc).isoformat()
                            }
                            metadata['metrics_collected'] += 1
                            self.logger.debug(f"✓ Collected {len(raw_data_points)} data points for {metric_name}")
                        else:
                            metadata['collection_errors'].append(f"{metric_name}: No data available")
                            
                except Exception as e:
                    metadata['collection_errors'].append(f"{metric_name}: {str(e)}")
                    continue
            
            self.logger.info(f"Azure Monitor: {metadata['metrics_collected']}/{metadata['metrics_attempted']} metrics")
            
        except Exception as e:
            metadata['collection_errors'].append(f"Azure Monitor collection failed: {str(e)}")
            self.logger.error(f"Azure Monitor collection failed: {e}")


    # Supporting methods
    def _extract_time_range_from_query(self, query: str) -> int:
        """Extract time range hours from KQL query."""
        match = re.search(r'ago\((\d+)h\)', query)
        return int(match.group(1)) if match else 24


    def _get_table_description(self, table_name: str) -> str:
        """Get descriptions for all 30 monitoring tables."""
        descriptions = {
            # Original 21 FinOps queries
            'aks_node_cpu_performance': 'AKS node CPU performance with efficiency analysis',
            'aks_node_memory_performance': 'AKS node memory performance with utilization analysis',
            'aks_node_network_performance': 'AKS node network performance and bandwidth utilization',
            'pod_cpu_usage': 'Pod-level CPU usage with request/limit comparison',
            'pod_memory_usage': 'Pod-level memory usage with efficiency scoring',
            'pod_usage_with_limits': 'Pod resource usage analysis',
            'container_cpu_detailed': 'Container CPU analysis with throttling detection',
            'container_memory_detailed': 'Container memory analysis with working set metrics',
            'container_utilization_insights': 'Container utilization patterns analysis',
            'pod_resource_requests_limits': 'Pod resource governance analysis',
            'resource_quota_limits': 'Resource quotas and namespace limits',
            'node_capacity_vs_allocation': 'Node capacity vs allocation efficiency',
            'namespace_resource_attribution': 'Namespace resource attribution',
            'namespace_summary': 'Namespace workload analysis',
            'workload_attribution': 'Workload attribution with health metrics',
            'kubernetes_scheduling_events': 'Kubernetes scheduling events analysis',
            'kubernetes_resource_events': 'Kubernetes resource lifecycle events',
            'kubernetes_warning_events': 'Kubernetes warning events analysis',
            'pod_restart_events': 'Pod restart events with stability scoring',
            
            # Additional 9 monitoring queries
            'volume_usage': 'Storage volume usage and disk pressure analysis',
            'persistent_volume_claim_metrics': 'PVC utilization with storage pressure indicators',
            'pod_network_traffic': 'Pod-level network traffic analysis',
            'pod_network_connectivity': 'Pod network connectivity health analysis',
            'kube_apiserver_metrics': 'Kubernetes API server performance monitoring',
            'etcd_health_metrics': 'etcd health metrics for control plane monitoring',
            'node_disk_filesystem_usage': 'Node disk I/O and filesystem utilization',
            'container_oom_throttling': 'Container OOM events and CPU throttling detection',
            'cluster_capacity_summary': 'Cluster-wide capacity vs utilization summary'
        }
        return descriptions.get(table_name, f'Monitoring table: {table_name}')


    def _get_finops_category(self, table_name: str) -> str:
        """Categorize tables by monitoring focus area."""
        categories = {
            # Original FinOps categories
            'aks_node_cpu_performance': 'infrastructure_performance',
            'aks_node_memory_performance': 'infrastructure_performance',
            'aks_node_network_performance': 'infrastructure_performance',
            'pod_cpu_usage': 'resource_utilization',
            'pod_memory_usage': 'resource_utilization', 
            'pod_usage_with_limits': 'resource_utilization',
            'container_cpu_detailed': 'container_insights',
            'container_memory_detailed': 'container_insights',
            'container_utilization_insights': 'container_insights',
            'pod_resource_requests_limits': 'resource_governance',
            'resource_quota_limits': 'resource_governance',
            'node_capacity_vs_allocation': 'resource_governance',
            'namespace_resource_attribution': 'namespace_attribution',
            'namespace_summary': 'namespace_attribution',
            'workload_attribution': 'namespace_attribution',
            'kubernetes_scheduling_events': 'operational_events',
            'kubernetes_resource_events': 'operational_events',
            'kubernetes_warning_events': 'operational_events',
            'pod_restart_events': 'operational_events',
            
            # Additional monitoring categories
            'volume_usage': 'storage_monitoring',
            'persistent_volume_claim_metrics': 'storage_monitoring',
            'pod_network_traffic': 'network_monitoring',
            'pod_network_connectivity': 'network_monitoring',
            'kube_apiserver_metrics': 'control_plane_monitoring',
            'etcd_health_metrics': 'control_plane_monitoring',
            'node_disk_filesystem_usage': 'infrastructure_performance',
            'container_oom_throttling': 'resource_optimization',
            'cluster_capacity_summary': 'capacity_planning'
        }
        return categories.get(table_name, 'general_monitoring')


    def _add_comprehensive_collection_quality_indicators(self, raw_metrics: Dict[str, Any], cluster_name: str) -> None:
        """Add comprehensive collection quality indicators for all 30 queries."""
        metadata = raw_metrics['collection_metadata']
        container_insights_data = raw_metrics.get('container_insights_data', {})
        
        # Calculate total data points
        total_data_points = sum(
            table_data.get('row_count', 0) 
            for table_data in container_insights_data.values()
        )
        
        # Calculate success percentage
        success_percentage = (metadata.get('queries_successful', 0) / metadata.get('queries_attempted', 1)) * 100
        
        # Determine data richness
        if total_data_points >= 4000:
            data_richness = "excellent"
        elif total_data_points >= 2000:
            data_richness = "good"  
        elif total_data_points >= 1000:
            data_richness = "adequate"
        elif total_data_points >= 300:
            data_richness = "minimal"
        else:
            data_richness = "insufficient"
        
        # Query organization by category (30 total)
        query_categories = {
            'infrastructure_performance': ['aks_node_cpu_performance', 'aks_node_memory_performance', 'aks_node_network_performance', 'node_disk_filesystem_usage'],
            'resource_utilization': ['pod_cpu_usage', 'pod_memory_usage', 'pod_usage_with_limits'],
            'container_insights': ['container_cpu_detailed', 'container_memory_detailed', 'container_utilization_insights'],
            'resource_governance': ['pod_resource_requests_limits', 'resource_quota_limits', 'node_capacity_vs_allocation'],
            'namespace_attribution': ['namespace_resource_attribution', 'namespace_summary', 'workload_attribution'],
            'operational_events': ['kubernetes_scheduling_events', 'kubernetes_resource_events', 'kubernetes_warning_events', 'pod_restart_events'],
            'storage_monitoring': ['volume_usage', 'persistent_volume_claim_metrics'],
            'network_monitoring': ['pod_network_traffic', 'pod_network_connectivity'],
            'control_plane_monitoring': ['kube_apiserver_metrics', 'etcd_health_metrics'],
            'resource_optimization': ['container_oom_throttling'],
            'capacity_planning': ['cluster_capacity_summary']
        }
        
        # Assess each category
        category_assessment = {}
        all_queries = []
        for category, queries in query_categories.items():
            all_queries.extend(queries)
            available = [q for q in queries if q in container_insights_data]
            completion_rate = len(available) / len(queries) * 100
            
            category_assessment[category] = {
                'available_queries': available,
                'missing_queries': [q for q in queries if q not in container_insights_data],
                'completion_rate': round(completion_rate, 1),
                'category_health': 'healthy' if completion_rate >= 75 else 'partial' if completion_rate >= 50 else 'limited'
            }
        
        # Calculate overall monitoring readiness
        category_rates = [data['completion_rate'] for data in category_assessment.values()]
        monitoring_readiness_score = sum(category_rates) / len(category_rates)
        
        # Enhanced metadata
        metadata['data_quality'] = {
            'total_data_points_collected': total_data_points,
            'successful_query_percentage': round(success_percentage, 1),
            'data_richness': data_richness,
            'monitoring_readiness_score': round(monitoring_readiness_score, 1),
            'category_assessment': category_assessment,
            'ready_for_production_monitoring': monitoring_readiness_score >= 70 and total_data_points >= 1500,
            'total_queries_coverage': f"{len([q for q in all_queries if q in container_insights_data])}/30"
        }
        
        # Priority breakdown
        priority_breakdown = {}
        for table_name, table_data in container_insights_data.items():
            priority = table_data.get('priority_level', 'unknown')
            if priority not in priority_breakdown:
                priority_breakdown[priority] = {'count': 0, 'data_points': 0}
            priority_breakdown[priority]['count'] += 1
            priority_breakdown[priority]['data_points'] += table_data.get('row_count', 0)
        
        metadata['priority_breakdown'] = priority_breakdown
        
        # Collection efficiency
        metadata['collection_efficiency'] = {
            'queries_attempted': metadata.get('queries_attempted', 0),
            'queries_successful': metadata.get('queries_successful', 0),
            'average_data_points_per_query': round(total_data_points / max(metadata.get('queries_successful', 1), 1), 1),
            'comprehensive_coverage_achieved': len([q for q in all_queries if q in container_insights_data]) >= 20
        }
        
        # Monitoring capabilities
        metadata['monitoring_capabilities'] = {
            'finops_ready': category_assessment['resource_utilization']['completion_rate'] >= 70,
            'infrastructure_monitoring_ready': category_assessment['infrastructure_performance']['completion_rate'] >= 75,
            'application_monitoring_ready': category_assessment['container_insights']['completion_rate'] >= 70,
            'platform_monitoring_ready': category_assessment['control_plane_monitoring']['completion_rate'] >= 50,
            'storage_monitoring_ready': category_assessment['storage_monitoring']['completion_rate'] >= 50,
            'network_monitoring_ready': category_assessment['network_monitoring']['completion_rate'] >= 50,
            'enterprise_ready': monitoring_readiness_score >= 80
        }
        
        # Cluster health indicators
        metadata['cluster_health_indicators'] = {
            'cluster_name': cluster_name,
            'container_insights_enabled': len(container_insights_data) > 0,
            'comprehensive_monitoring_ready': monitoring_readiness_score >= 75,
            'production_observability_ready': monitoring_readiness_score >= 80 and total_data_points >= 2000
        }