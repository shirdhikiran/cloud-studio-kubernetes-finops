# src/finops/clients/azure/monitor_client.py
"""Phase 1 Discovery - Azure Monitor client for raw data collection only."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
import structlog
from azure.mgmt.monitor import MonitorManagementClient
from azure.monitor.query import MetricsQueryClient, LogsQueryClient, MetricAggregationType
from azure.core.exceptions import AzureError, ResourceNotFoundError, ClientAuthenticationError

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
    
    async def _collect_azure_monitor_raw_metrics(self, cluster_resource_id: str,
                                               start_time: datetime, end_time: datetime,
                                               raw_metrics: Dict[str, Any]) -> None:
        """Collect raw Azure Monitor metrics without processing."""
        metadata = raw_metrics['collection_metadata']
        
        # Standard AKS metrics to collect
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
                    self.logger.debug(f"Collecting raw metric: {metric_name}")
                    
                    response = self._metrics_client.query_resource(
                        resource_uri=cluster_resource_id,
                        metric_names=[metric_name],
                        timespan=(start_time, end_time),
                        granularity=granularity,
                        aggregations=[MetricAggregationType.AVERAGE, MetricAggregationType.MAXIMUM, MetricAggregationType.MINIMUM]
                    )
                    
                    # Store raw data points
                    raw_data_points = []
                    for metric in response.metrics:
                        for timeseries in metric.timeseries:
                            for data_point in timeseries.data:
                                raw_data_points.append({
                                    'timestamp': data_point.timestamp.isoformat() if data_point.timestamp else None,
                                    'average': float(data_point.average) if data_point.average is not None else None,
                                    'maximum': float(data_point.maximum) if data_point.maximum is not None else None,
                                    'minimum': float(data_point.minimum) if data_point.minimum is not None else None
                                })
                    
                    if raw_data_points:
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
                    self.logger.warning(f"Failed to collect {metric_name}: {e}")
                    continue
            
            self.logger.info(f"Azure Monitor collection: {metadata['metrics_collected']}/{metadata['metrics_attempted']} metrics")
            
        except Exception as e:
            metadata['collection_errors'].append(f"Azure Monitor collection failed: {str(e)}")
            self.logger.error(f"Azure Monitor collection failed: {e}")
    
    
    async def _collect_container_insights_raw_data(self, cluster_name: str,
                                             start_time: datetime, end_time: datetime,
                                             raw_metrics: Dict[str, Any]) -> None:
        """Optimized Container Insights collection with time ranges and quality indicators."""
        metadata = raw_metrics['collection_metadata']
        
        # Optimized time ranges by data type
        time_ranges = {
            'performance': 6,    # 6 hours for performance trends
            'inventory': 2,      # 2 hours for current state
            'events': 12,        # 12 hours for recent issues
            'logs': 1,           # 1 hour for log samples
            'discovery': 24      # 24 hours for discovery
        }
        
        # Prioritized collection with optimized time ranges
        collection_strategy = {
            'high_priority': {
                'perf': f"Perf | where TimeGenerated >= ago({time_ranges['performance']}h) | order by TimeGenerated desc | take 500",
                'insights_metrics': f"InsightsMetrics | where TimeGenerated >= ago({time_ranges['performance']}h) | order by TimeGenerated desc | take 400", 
                'kube_pod_inventory': f"KubePodInventory | where TimeGenerated >= ago({time_ranges['inventory']}h) | order by TimeGenerated desc | take 300",
                'kube_node_inventory': f"KubeNodeInventory | where TimeGenerated >= ago({time_ranges['inventory']}h) | order by TimeGenerated desc | take 100",
                'heartbeat': f"Heartbeat | where TimeGenerated >= ago({time_ranges['performance']}h) | order by TimeGenerated desc | take 150",
                'available_tables': "search * | summarize count() by $table | order by count_ desc | take 30"
            },
            'medium_priority': {
                'kube_events': f"KubeEvents | where TimeGenerated >= ago({time_ranges['events']}h) | order by TimeGenerated desc | take 150",
                'container_inventory': f"ContainerInventory | where TimeGenerated >= ago({time_ranges['inventory']}h) | order by TimeGenerated desc | take 200",
                'kube_services': f"KubeServices | where TimeGenerated >= ago({time_ranges['inventory']}h) | order by TimeGenerated desc | take 100",
                'kube_pv_inventory': f"KubePVInventory | where TimeGenerated >= ago({time_ranges['inventory']}h) | order by TimeGenerated desc | take 100"
            },
            'low_priority': {
                'container_log': f"ContainerLog | where TimeGenerated >= ago({time_ranges['logs']}h) | order by TimeGenerated desc | take 50",
                'container_log_v2': f"ContainerLogV2 | where TimeGenerated >= ago({time_ranges['logs']}h) | order by TimeGenerated desc | take 50",
                'syslog': f"Syslog | where TimeGenerated >= ago({time_ranges['logs']}h) | order by TimeGenerated desc | take 30",
                'azure_diagnostics': f"AzureDiagnostics | where TimeGenerated >= ago({time_ranges['performance']}h) | order by TimeGenerated desc | take 100",
                'container_image_inventory': f"ContainerImageInventory | where TimeGenerated >= ago({time_ranges['inventory']}h) | order by TimeGenerated desc | take 150",
                'container_node_inventory': f"ContainerNodeInventory | where TimeGenerated >= ago({time_ranges['inventory']}h) | order by TimeGenerated desc | take 100"
            }
        }
        
        # Flatten all tables for total count
        all_tables = {}
        for priority_tables in collection_strategy.values():
            all_tables.update(priority_tables)
        
        metadata['queries_attempted'] = len(all_tables)
        metadata['time_ranges_used'] = time_ranges
        
        try:
            self.logger.info(f"Starting optimized Container Insights collection for {cluster_name}")
            self.logger.info(f"Time ranges: Performance={time_ranges['performance']}h, Inventory={time_ranges['inventory']}h, Events={time_ranges['events']}h, Logs={time_ranges['logs']}h")
            
            # Collect in priority order with timeout
            for priority_level, tables in collection_strategy.items():
                self.logger.debug(f"Collecting {priority_level} tables: {list(tables.keys())}")
                
                for table_name, query in tables.items():
                    success = await self._collect_table_with_enhanced_handling(
                        table_name, query, start_time, end_time, raw_metrics, priority_level, timeout=45
                    )
                    
                    if not success and priority_level == 'high_priority':
                        self.logger.warning(f"High priority table {table_name} failed - may impact Phase 2 analytics")
            
            # Add collection quality indicators
            self._add_collection_quality_indicators(raw_metrics, cluster_name)
            
            # Log final summary
            successful_tables = list(raw_metrics.get('container_insights_data', {}).keys())
            self.logger.info(f"Container Insights collection completed for {cluster_name}")
            self.logger.info(f"Results: {metadata['queries_successful']}/{metadata['queries_attempted']} tables successful")
            self.logger.info(f"Successful tables: {successful_tables}")
            self.logger.info(f"Data quality: {metadata.get('data_quality', {}).get('data_richness', 'unknown')}")
            
        except Exception as e:
            metadata['collection_errors'].append(f"Container Insights collection failed: {str(e)}")
            self.logger.error(f"Container Insights collection failed: {e}")

    async def _collect_table_with_enhanced_handling(self, table_name: str, query: str,
                                                start_time: datetime, end_time: datetime,
                                                raw_metrics: Dict[str, Any], priority_level: str,
                                                timeout: int = 45) -> bool:
        """Enhanced table collection with proper column handling and timeout."""
        metadata = raw_metrics['collection_metadata']
        
        try:
            self.logger.debug(f"Collecting {priority_level} table: {table_name}")
            
            # Execute query with timeout handling
            response = self._logs_client.query_workspace(
                workspace_id=self.log_analytics_workspace_id,
                query=query,
                timespan=(start_time, end_time)
            )
            
            if response and hasattr(response, 'tables') and response.tables:
                table = response.tables[0]
                
                if table.rows:
                    # FIXED: Properly extract column names
                    columns = []
                    if hasattr(table, 'columns') and table.columns:
                        for col in table.columns:
                            # Try multiple ways to get column name
                            if hasattr(col, 'name') and col.name:
                                columns.append(col.name)
                            elif hasattr(col, 'column_name') and col.column_name:
                                columns.append(col.column_name)
                            elif hasattr(col, 'ColumnName') and col.ColumnName:
                                columns.append(col.ColumnName)
                            else:
                                # Last resort - use generic name with index
                                columns.append(f"column_{len(columns)}")
                    else:
                        # If no column metadata, create generic names
                        columns = [f"column_{i}" for i in range(len(table.rows[0]))]
                    
                    # Enhanced data structure with better metadata
                    raw_data = {
                        'table_name': table_name,
                        'description': self._get_table_description(table_name),
                        'priority_level': priority_level,
                        'columns': columns,
                        'rows': [list(row) for row in table.rows],
                        'row_count': len(table.rows),
                        'column_count': len(columns),
                        'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                        'query_executed': query,
                        'time_range_hours': self._extract_time_range_from_query(query),
                        'data_sample': {
                            'first_row': list(table.rows[0]) if table.rows else None,
                            'last_row': list(table.rows[-1]) if table.rows else None
                        }
                    }
                    
                    raw_metrics['container_insights_data'][table_name] = raw_data
                    metadata['queries_successful'] += 1
                    
                    self.logger.debug(f"✓ {table_name}: {len(table.rows)} rows, {len(columns)} columns ({priority_level})")
                    return True
                else:
                    metadata['collection_errors'].append(f"{table_name}: No data returned")
                    self.logger.debug(f"✗ {table_name}: No data ({priority_level})")
                    return False
            else:
                metadata['collection_errors'].append(f"{table_name}: No response")
                return False
                
        except Exception as e:
            error_msg = str(e)
            if "BadArgumentError" in error_msg or "not found" in error_msg.lower():
                metadata['collection_errors'].append(f"{table_name}: Table not available")
                self.logger.debug(f"✗ {table_name}: Table not available ({priority_level})")
            elif "authorization" in error_msg.lower():
                metadata['collection_errors'].append(f"{table_name}: Access denied")
                self.logger.debug(f"✗ {table_name}: Access denied ({priority_level})")
            elif "timeout" in error_msg.lower():
                metadata['collection_errors'].append(f"{table_name}: Query timeout")
                self.logger.warning(f"✗ {table_name}: Query timeout ({priority_level})")
            else:
                metadata['collection_errors'].append(f"{table_name}: {error_msg}")
                self.logger.warning(f"✗ {table_name}: {error_msg} ({priority_level})")
            
            return False

    def _add_collection_quality_indicators(self, raw_metrics: Dict[str, Any], cluster_name: str) -> None:
        """Add collection quality indicators and health status."""
        metadata = raw_metrics['collection_metadata']
        container_insights_data = raw_metrics.get('container_insights_data', {})
        
        # Calculate total rows collected
        total_rows = sum(
            table_data.get('row_count', 0) 
            for table_data in container_insights_data.values()
        )
        
        # Calculate success percentage
        success_percentage = (metadata.get('queries_successful', 0) / metadata.get('queries_attempted', 1)) * 100
        
        # Determine data richness
        if total_rows >= 1000:
            data_richness = "high"
        elif total_rows >= 200:
            data_richness = "medium"
        elif total_rows >= 50:
            data_richness = "low"
        else:
            data_richness = "minimal"
        
        # Container Insights health indicators
        key_tables = ['perf', 'insights_metrics', 'kube_pod_inventory', 'kube_node_inventory']
        available_key_tables = [table for table in key_tables if table in container_insights_data]
        
        container_insights_health = "healthy" if len(available_key_tables) >= 3 else \
                                "partial" if len(available_key_tables) >= 2 else \
                                "poor" if len(available_key_tables) >= 1 else "unavailable"
        
        # Detailed breakdown by priority
        priority_breakdown = {}
        for table_name, table_data in container_insights_data.items():
            priority = table_data.get('priority_level', 'unknown')
            if priority not in priority_breakdown:
                priority_breakdown[priority] = {'count': 0, 'total_rows': 0}
            priority_breakdown[priority]['count'] += 1
            priority_breakdown[priority]['total_rows'] += table_data.get('row_count', 0)
        
        # Add comprehensive quality metadata
        metadata['data_quality'] = {
            'total_rows_collected': total_rows,
            'successful_table_percentage': round(success_percentage, 1),
            'data_richness': data_richness,
            'container_insights_health': container_insights_health,
            'key_tables_available': available_key_tables,
            'priority_breakdown': priority_breakdown,
            'collection_efficiency': {
                'tables_attempted': metadata.get('queries_attempted', 0),
                'tables_successful': metadata.get('queries_successful', 0),
                'tables_failed': len(metadata.get('collection_errors', [])),
                'average_rows_per_table': round(total_rows / max(metadata.get('queries_successful', 1), 1), 1)
            }
        }
        
        # Cluster-specific health indicators
        metadata['cluster_health_indicators'] = {
            'cluster_name': cluster_name,
            'has_performance_data': 'perf' in container_insights_data or 'insights_metrics' in container_insights_data,
            'has_inventory_data': 'kube_pod_inventory' in container_insights_data or 'kube_node_inventory' in container_insights_data,
            'has_operational_data': 'kube_events' in container_insights_data or 'heartbeat' in container_insights_data,
            'container_insights_enabled': container_insights_health in ['healthy', 'partial'],
            'ready_for_phase2_analytics': container_insights_health == 'healthy' and data_richness in ['high', 'medium']
        }

    def _extract_time_range_from_query(self, query: str) -> int:
        """Extract time range hours from query."""
        import re
        match = re.search(r'ago\((\d+)h\)', query)
        return int(match.group(1)) if match else 24

    def _get_table_description(self, table_name: str) -> str:
        """Enhanced table descriptions."""
        descriptions = {
            'perf': 'Performance metrics (CPU, memory, disk, network) for nodes and containers',
            'insights_metrics': 'Standardized performance metrics used for dashboards and alerts',
            'kube_pod_inventory': 'Pod-level inventory: names, namespaces, container count, resource requests',
            'kube_node_inventory': 'Node-level inventory: OS type, status, role, version, capacity',
            'kube_services': 'Kubernetes service objects and metadata',
            'kube_events': 'Kubernetes events: scheduling errors, evictions, warnings, info',
            'kube_pv_inventory': 'Persistent volume inventory and usage statistics',
            'container_inventory': 'Container details: images, state, creation times, configuration',
            'container_log': 'Container stdout/stderr logs sample',
            'container_log_v2': 'Enhanced structured container logs',
            'container_node_inventory': 'Node hardware and software configuration details',
            'container_image_inventory': 'Container image metadata and usage statistics',
            'heartbeat': 'Agent health and connectivity signals',
            'syslog': 'System logs from Linux nodes',
            'azure_diagnostics': 'Azure platform diagnostic logs',
            'available_tables': 'Discovery of all available tables and data volume',
            'recent_activity': 'Recent activity summary across all Container Insights tables'
        }
        return descriptions.get(table_name, f'Container Insights table: {table_name}')
    
    
    
    @retry_with_backoff(max_retries=3)
    async def get_available_metrics(self, resource_id: str) -> List[str]:
        """Get list of available metrics for a resource."""
        if not self._connected:
            raise MetricsException("Monitor client not connected")
        
        try:
            metric_definitions = self._monitor_client.metric_definitions.list(resource_id)
            return [metric.name.value for metric in metric_definitions]
        except Exception as e:
            self.logger.error(f"Failed to get available metrics: {e}")
            return []