"""Complete MetricsCollectionService implementation for comprehensive FinOps analysis."""

from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta, timezone
import structlog
import asyncio

from finops.discovery.base import BaseDiscoveryService
from finops.clients.azure.monitor_client import MonitorClient

logger = structlog.get_logger(__name__)


class MetricsCollectionService(BaseDiscoveryService):
    """Complete service for collecting comprehensive metrics from Azure Monitor for FinOps analysis."""
    
    def __init__(self, monitor_client: MonitorClient, config: Dict[str, Any]):
        super().__init__(monitor_client, config)
        self.cluster_resource_id = config.get("cluster_resource_id")
        self.resource_group = config.get("resource_group")
        self.cluster_name = config.get("cluster_name")
        self.subscription_id = config.get("subscription_id")
        
        # Time range configuration
        self.metrics_time_range_hours = config.get("metrics_time_range_hours", 24)
        self.granularity_minutes = config.get("granularity_minutes", 5)
        self.max_data_points = config.get("max_data_points", 1000)
        
        # Collection flags
        self.include_cluster_metrics = config.get("include_cluster_metrics", True)
        self.include_individual_node_metrics = config.get("include_individual_node_metrics", True)
        self.include_pod_metrics = config.get("include_pod_metrics", False)  # Requires Log Analytics
        self.include_storage_metrics = config.get("include_storage_metrics", True)
        self.include_network_metrics = config.get("include_network_metrics", True)
        self.include_health_metrics = config.get("include_health_metrics", True)
        
        # Performance settings
        self.max_concurrent_requests = config.get("max_concurrent_requests", 5)
        self.request_timeout_seconds = config.get("request_timeout_seconds", 30)
        self.max_retries = config.get("max_retries", 3)
        
        # Build cluster resource ID if not provided
        if not self.cluster_resource_id and all([self.subscription_id, self.resource_group, self.cluster_name]):
            self.cluster_resource_id = (
                f"/subscriptions/{self.subscription_id}"
                f"/resourceGroups/{self.resource_group}"
                f"/providers/Microsoft.ContainerService/managedClusters/{self.cluster_name}"
            )
        
        # Namespace filtering
        self.namespace_filter = config.get("namespace_filter")
        self.exclude_system_namespaces = config.get("exclude_system_namespaces", True)
        self.system_namespaces = config.get("system_namespaces", [
            "kube-system", "kube-public", "kube-node-lease", "azure-system"
        ])
    
    async def discover(self) -> List[Dict[str, Any]]:
        """Collect comprehensive metrics data for FinOps analysis."""
        self.logger.info("Starting comprehensive metrics collection for FinOps analysis")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        if not self.cluster_resource_id:
            self.logger.warning("No cluster resource ID available, using fallback collection")
            return await self._collect_fallback_metrics()
        
        metrics_data = []
        collection_start_time = datetime.now(timezone.utc)
        
        try:
            # Calculate time range
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=self.metrics_time_range_hours)
            granularity = timedelta(minutes=self.granularity_minutes)
            
            self.logger.info(
                f"Collecting metrics from {start_time} to {end_time} "
                f"with {self.granularity_minutes}min granularity for cluster: {self.cluster_name}"
            )
            
            # Create collection tasks based on configuration
            collection_tasks = []
            
            if self.include_cluster_metrics:
                collection_tasks.append(
                    self._collect_cluster_level_metrics(start_time, end_time, granularity)
                )
            
            if self.include_individual_node_metrics:
                collection_tasks.append(
                    self._collect_individual_node_metrics(start_time, end_time)
                )
            
            if self.include_storage_metrics:
                collection_tasks.append(
                    self._collect_storage_metrics(start_time, end_time, granularity)
                )
            
            if self.include_network_metrics:
                collection_tasks.append(
                    self._collect_network_metrics(start_time, end_time, granularity)
                )
            
            if self.include_pod_metrics:
                collection_tasks.append(
                    self._collect_pod_metrics(start_time, end_time)
                )
            
            if self.include_health_metrics:
                collection_tasks.append(
                    self._collect_health_metrics(start_time, end_time)
                )
            
            # Execute all collection tasks with concurrency control
            self.logger.info(f"Executing {len(collection_tasks)} collection tasks")
            
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            
            async def run_with_semaphore(task):
                async with semaphore:
                    return await task
            
            # Execute tasks with timeout
            results = await asyncio.gather(
                *[run_with_semaphore(task) for task in collection_tasks],
                return_exceptions=True
            )
            
            # Process results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.error(f"Collection task {i} failed", error=str(result))
                    metrics_data.append({
                        'type': f'collection_task_{i}_error',
                        'error': str(result),
                        'status': 'failed'
                    })
                elif result:
                    if isinstance(result, list):
                        metrics_data.extend(result)
                    else:
                        metrics_data.append(result)
            
            # Add collection summary
            collection_end_time = datetime.now(timezone.utc)
            collection_duration = (collection_end_time - collection_start_time).total_seconds()
            
            summary_metrics = {
                'type': 'metrics_collection_summary',
                'cluster_name': self.cluster_name,
                'data': {
                    'collection_start_time': collection_start_time.isoformat(),
                    'collection_end_time': collection_end_time.isoformat(),
                    'collection_duration_seconds': collection_duration,
                    'total_metric_categories': len(metrics_data),
                    'successful_collections': len([m for m in metrics_data if not m.get('error')]),
                    'failed_collections': len([m for m in metrics_data if m.get('error')]),
                    'time_range_hours': self.metrics_time_range_hours,
                    'granularity_minutes': self.granularity_minutes,
                    'cluster_resource_id': self.cluster_resource_id,
                    'configuration': {
                        'include_cluster_metrics': self.include_cluster_metrics,
                        'include_individual_node_metrics': self.include_individual_node_metrics,
                        'include_pod_metrics': self.include_pod_metrics,
                        'include_storage_metrics': self.include_storage_metrics,
                        'include_network_metrics': self.include_network_metrics,
                        'include_health_metrics': self.include_health_metrics
                    }
                }
            }
            
            metrics_data.append(summary_metrics)
            
        except Exception as e:
            self.logger.error("Failed to collect metrics", error=str(e))
            # Add error information but don't fail completely
            metrics_data.append({
                'type': 'metrics_collection_error',
                'error': str(e),
                'cluster_name': self.cluster_name,
                'status': 'failed'
            })
        
        self.logger.info(f"Completed metrics collection - {len(metrics_data)} categories collected")
        return metrics_data
    
    async def _collect_cluster_level_metrics(self, start_time: datetime, end_time: datetime, 
                                           granularity: timedelta) -> Dict[str, Any]:
        """Collect comprehensive cluster-level metrics."""
        self.logger.info("Collecting cluster-level metrics")
        
        try:
            cluster_metrics = await self.client.get_cluster_metrics(
                cluster_resource_id=self.cluster_resource_id,
                start_time=start_time,
                end_time=end_time,
                granularity=granularity
            )
            
            # Add FinOps analysis to cluster metrics
            finops_analysis = self._analyze_cluster_metrics_for_finops(cluster_metrics)
            
            return {
                'type': 'cluster_metrics',
                'cluster_name': self.cluster_name,
                'resource_id': self.cluster_resource_id,
                'data': cluster_metrics,
                'finops_analysis': finops_analysis,
                'collection_metadata': {
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),
                    'granularity_minutes': self.granularity_minutes,
                    'metrics_collected': cluster_metrics.get('collection_metadata', {}).get('metrics_collected', []),
                    'metrics_failed': cluster_metrics.get('collection_metadata', {}).get('metrics_failed', [])
                }
            }
            
        except Exception as e:
            self.logger.error("Failed to collect cluster metrics", error=str(e))
            return {
                'type': 'cluster_metrics',
                'status': 'failed',
                'error': str(e),
                'cluster_name': self.cluster_name
            }
    
    async def _collect_individual_node_metrics(self, start_time: datetime, 
                                             end_time: datetime) -> Dict[str, Any]:
        """Collect individual node metrics without aggregation."""
        self.logger.info("Collecting individual node metrics")
        
        try:
            node_metrics = await self.client.get_individual_node_metrics(
                cluster_name=self.cluster_name,
                start_time=start_time,
                end_time=end_time,
                granularity_minutes=self.granularity_minutes
            )
            
            # Add FinOps analysis for each node
            finops_analysis = self._analyze_node_metrics_for_finops(node_metrics)
            
            return {
                'type': 'individual_node_metrics',
                'cluster_name': self.cluster_name,
                'data': node_metrics,
                'finops_analysis': finops_analysis,
                'collection_metadata': {
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),
                    'nodes_discovered': node_metrics.get('collection_metadata', {}).get('nodes_discovered', 0),
                    'nodes_with_data': node_metrics.get('collection_metadata', {}).get('nodes_with_data', 0)
                }
            }
            
        except Exception as e:
            self.logger.error("Failed to collect individual node metrics", error=str(e))
            return {
                'type': 'individual_node_metrics',
                'status': 'failed',
                'error': str(e),
                'cluster_name': self.cluster_name
            }
    
    async def _collect_storage_metrics(self, start_time: datetime, end_time: datetime,
                                     granularity: timedelta) -> Dict[str, Any]:
        """Collect comprehensive storage metrics."""
        self.logger.info("Collecting storage metrics")
        
        try:
            storage_metrics = await self.client.get_storage_metrics(
                cluster_resource_id=self.cluster_resource_id,
                start_time=start_time,
                end_time=end_time,
                granularity=granularity
            )
            
            # Add FinOps analysis for storage
            finops_analysis = self._analyze_storage_metrics_for_finops(storage_metrics)
            
            return {
                'type': 'storage_metrics',
                'cluster_name': self.cluster_name,
                'resource_id': self.cluster_resource_id,
                'data': storage_metrics,
                'finops_analysis': finops_analysis,
                'collection_metadata': {
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat()
                }
            }
            
        except Exception as e:
            self.logger.error("Failed to collect storage metrics", error=str(e))
            return {
                'type': 'storage_metrics',
                'status': 'failed',
                'error': str(e),
                'cluster_name': self.cluster_name
            }
    
    async def _collect_network_metrics(self, start_time: datetime, end_time: datetime,
                                     granularity: timedelta) -> Dict[str, Any]:
        """Collect comprehensive network metrics."""
        self.logger.info("Collecting network metrics")
        
        try:
            network_metrics = await self.client.get_network_metrics(
                cluster_resource_id=self.cluster_resource_id,
                start_time=start_time,
                end_time=end_time,
                granularity=granularity
            )
            
            # Add FinOps analysis for network
            finops_analysis = self._analyze_network_metrics_for_finops(network_metrics)
            
            return {
                'type': 'network_metrics',
                'cluster_name': self.cluster_name,
                'resource_id': self.cluster_resource_id,
                'data': network_metrics,
                'finops_analysis': finops_analysis,
                'collection_metadata': {
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat()
                }
            }
            
        except Exception as e:
            self.logger.error("Failed to collect network metrics", error=str(e))
            return {
                'type': 'network_metrics',
                'status': 'failed',
                'error': str(e),
                'cluster_name': self.cluster_name
            }
    
    async def _collect_pod_metrics(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Collect detailed pod metrics."""
        self.logger.info("Collecting detailed pod metrics")
        
        try:
            pod_metrics = await self.client.get_pod_metrics_detailed(
                cluster_name=self.cluster_name,
                start_time=start_time,
                end_time=end_time,
                namespace_filter=self.namespace_filter
            )
            
            # Filter out system namespaces if configured
            if self.exclude_system_namespaces:
                pod_metrics = self._filter_system_namespaces(pod_metrics)
            
            # Add FinOps analysis for pods
            finops_analysis = self._analyze_pod_metrics_for_finops(pod_metrics)
            
            return {
                'type': 'pod_metrics_detailed',
                'cluster_name': self.cluster_name,
                'data': pod_metrics,
                'finops_analysis': finops_analysis,
                'collection_metadata': {
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),
                    'namespace_filter': self.namespace_filter,
                    'exclude_system_namespaces': self.exclude_system_namespaces
                }
            }
            
        except Exception as e:
            self.logger.error("Failed to collect pod metrics", error=str(e))
            return {
                'type': 'pod_metrics_detailed',
                'status': 'failed',
                'error': str(e),
                'cluster_name': self.cluster_name
            }
    
    async def _collect_health_metrics(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Collect cluster health metrics."""
        self.logger.info("Collecting cluster health metrics")
        
        try:
            health_metrics = await self.client.get_cluster_health_metrics(
                cluster_resource_id=self.cluster_resource_id,
                start_time=start_time,
                end_time=end_time
            )
            
            # Add FinOps analysis for health
            finops_analysis = self._analyze_health_metrics_for_finops(health_metrics)
            
            return {
                'type': 'cluster_health_metrics',
                'cluster_name': self.cluster_name,
                'resource_id': self.cluster_resource_id,
                'data': health_metrics,
                'finops_analysis': finops_analysis,
                'collection_metadata': {
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat()
                }
            }
            
        except Exception as e:
            self.logger.error("Failed to collect health metrics", error=str(e))
            return {
                'type': 'cluster_health_metrics',
                'status': 'failed',
                'error': str(e),
                'cluster_name': self.cluster_name
            }
    
    # FinOps Analysis Methods
    def _analyze_cluster_metrics_for_finops(self, cluster_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cluster metrics from a FinOps perspective."""
        analysis = {
            'resource_efficiency': {},
            'cost_optimization_opportunities': [],
            'performance_insights': {},
            'recommendations': []
        }
        
        # Analyze CPU efficiency
        cpu_metrics = cluster_metrics.get('cpu_metrics', {})
        if cpu_metrics:
            cpu_analysis = self._analyze_cpu_efficiency(cpu_metrics)
            analysis['resource_efficiency']['cpu'] = cpu_analysis
            
            # Generate CPU-based recommendations
            if cpu_analysis.get('average_utilization', 0) < 30:
                analysis['recommendations'].append({
                    'type': 'cpu_rightsizing',
                    'priority': 'high',
                    'title': 'CPU Over-provisioning Detected',
                    'description': f"Average CPU utilization is {cpu_analysis.get('average_utilization', 0):.1f}%",
                    'estimated_savings': '20-30%',
                    'action': 'Consider reducing CPU requests/limits'
                })
        
        # Analyze memory efficiency
        memory_metrics = cluster_metrics.get('memory_metrics', {})
        if memory_metrics:
            memory_analysis = self._analyze_memory_efficiency(memory_metrics)
            analysis['resource_efficiency']['memory'] = memory_analysis
            
            # Generate memory-based recommendations
            if memory_analysis.get('average_utilization', 0) < 40:
                analysis['recommendations'].append({
                    'type': 'memory_rightsizing',
                    'priority': 'high',
                    'title': 'Memory Over-provisioning Detected',
                    'description': f"Average memory utilization is {memory_analysis.get('average_utilization', 0):.1f}%",
                    'estimated_savings': '15-25%',
                    'action': 'Consider reducing memory requests/limits'
                })
        
        # Analyze autoscaler metrics
        autoscaler_metrics = cluster_metrics.get('autoscaler_metrics', {})
        if autoscaler_metrics:
            analysis['performance_insights']['autoscaler'] = self._analyze_autoscaler_efficiency(autoscaler_metrics)
        
        return analysis
    
    def _analyze_node_metrics_for_finops(self, node_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze individual node metrics from a FinOps perspective."""
        analysis = {
            'node_efficiency_scores': {},
            'underutilized_nodes': [],
            'overutilized_nodes': [],
            'rightsizing_opportunities': [],
            'cost_optimization_insights': {}
        }
        
        nodes_data = node_metrics.get('nodes', {})
        total_nodes = len(nodes_data)
        efficient_nodes = 0
        
        for node_name, node_data in nodes_data.items():
            if node_data.get('error'):
                continue
            
            efficiency_scores = node_data.get('efficiency_scores', {})
            utilization = node_data.get('resource_utilization', {})
            
            # Store individual node efficiency
            analysis['node_efficiency_scores'][node_name] = efficiency_scores
            
            overall_efficiency = efficiency_scores.get('overall_efficiency', 0)
            cpu_util = utilization.get('cpu_utilization_percent', 0)
            memory_util = utilization.get('memory_utilization_percent', 0)
            
            # Categorize nodes
            if overall_efficiency > 70:
                efficient_nodes += 1
            
            if cpu_util < 20 and memory_util < 30:
                analysis['underutilized_nodes'].append({
                    'node_name': node_name,
                    'cpu_utilization': cpu_util,
                    'memory_utilization': memory_util,
                    'efficiency_score': overall_efficiency,
                    'recommendation': 'Consider consolidating workloads or downsizing'
                })
            
            if cpu_util > 90 or memory_util > 95:
                analysis['overutilized_nodes'].append({
                    'node_name': node_name,
                    'cpu_utilization': cpu_util,
                    'memory_utilization': memory_util,
                    'efficiency_score': overall_efficiency,
                    'recommendation': 'Consider scaling up or load balancing'
                })
            
            # Identify rightsizing opportunities
            if 20 < cpu_util < 50 and 30 < memory_util < 60:
                analysis['rightsizing_opportunities'].append({
                    'node_name': node_name,
                    'current_utilization': {
                        'cpu': cpu_util,
                        'memory': memory_util
                    },
                    'optimization_type': 'downsize',
                    'estimated_savings': '15-25%'
                })
        
        # Calculate overall insights
        analysis['cost_optimization_insights'] = {
            'total_nodes': total_nodes,
            'efficient_nodes': efficient_nodes,
            'efficiency_percentage': (efficient_nodes / total_nodes * 100) if total_nodes > 0 else 0,
            'underutilized_count': len(analysis['underutilized_nodes']),
            'overutilized_count': len(analysis['overutilized_nodes']),
            'rightsizing_candidates': len(analysis['rightsizing_opportunities'])
        }
        
        return analysis
    
    def _analyze_storage_metrics_for_finops(self, storage_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze storage metrics from a FinOps perspective."""
        analysis = {
            'storage_efficiency': {},
            'cost_optimization_opportunities': [],
            'capacity_insights': {},
            'recommendations': []
        }
        
        cluster_storage = storage_metrics.get('cluster_storage_metrics', {})
        if cluster_storage:
            # Analyze disk usage patterns
            disk_usage = cluster_storage.get('disk_usage', [])
            disk_available = cluster_storage.get('disk_available', [])
            disk_utilization = cluster_storage.get('disk_utilization_percent', [])
            
            if disk_utilization:
                avg_utilization = sum(point['average'] for point in disk_utilization) / len(disk_utilization)
                max_utilization = max(point['maximum'] for point in disk_utilization)
                
                analysis['storage_efficiency'] = {
                    'average_utilization': avg_utilization,
                    'peak_utilization': max_utilization,
                    'efficiency_score': min(100, avg_utilization / 80 * 100)  # Target 80% utilization
                }
                
                # Generate storage recommendations
                if avg_utilization < 30:
                    analysis['recommendations'].append({
                        'type': 'storage_rightsizing',
                        'priority': 'medium',
                        'title': 'Storage Over-provisioning Detected',
                        'description': f"Average storage utilization is {avg_utilization:.1f}%",
                        'estimated_savings': '10-20%',
                        'action': 'Consider reducing storage allocations or using different storage classes'
                    })
                
                if max_utilization > 90:
                    analysis['recommendations'].append({
                        'type': 'storage_capacity',
                        'priority': 'high',
                        'title': 'Storage Capacity Warning',
                        'description': f"Peak storage utilization is {max_utilization:.1f}%",
                        'action': 'Monitor storage usage and plan for capacity expansion'
                    })
        
        return analysis
    
    def _analyze_network_metrics_for_finops(self, network_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze network metrics from a FinOps perspective."""
        analysis = {
            'bandwidth_efficiency': {},
            'cost_optimization_opportunities': [],
            'traffic_insights': {},
            'recommendations': []
        }
        
        cluster_network = network_metrics.get('cluster_network_metrics', {})
        if cluster_network:
            # Analyze network traffic patterns
            rx_bytes = cluster_network.get('rx_bytes', [])
            tx_bytes = cluster_network.get('tx_bytes', [])
            
            if rx_bytes or tx_bytes:
                total_rx = sum(point['total'] for point in rx_bytes) if rx_bytes else 0
                total_tx = sum(point['total'] for point in tx_bytes) if tx_bytes else 0
                total_traffic = total_rx + total_tx
                
                analysis['traffic_insights'] = {
                    'total_ingress_bytes': total_rx,
                    'total_egress_bytes': total_tx,
                    'total_traffic_bytes': total_traffic,
                    'ingress_percentage': (total_rx / total_traffic * 100) if total_traffic > 0 else 0,
                    'egress_percentage': (total_tx / total_traffic * 100) if total_traffic > 0 else 0
                }
                
                # Generate network cost optimization recommendations
                if total_tx > total_rx * 2:  # High egress traffic
                    analysis['recommendations'].append({
                        'type': 'network_optimization',
                        'priority': 'medium',
                        'title': 'High Egress Traffic Detected',
                        'description': f"Egress traffic is {total_tx / (1024**3):.2f} GB",
                        'estimated_savings': '5-15%',
                        'action': 'Consider CDN or data transfer optimization strategies'
                    })
        
        return analysis
    
    def _analyze_pod_metrics_for_finops(self, pod_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze pod metrics from a FinOps perspective."""
        analysis = {
            'resource_consumption_analysis': {},
            'rightsizing_candidates': [],
            'namespace_efficiency': {},
            'cost_allocation_insights': {},
            'recommendations': []
        }
        
        # Analyze top CPU consumers
        top_cpu = pod_metrics.get('top_cpu_consumers', [])
        if top_cpu:
            high_cpu_pods = [pod for pod in top_cpu if pod.get('avg_cpu_millicores', 0) > 1000]
            analysis['rightsizing_candidates'].extend([
                {
                    'pod_name': pod['pod_name'],
                    'namespace': pod['namespace'],
                    'node': pod['node'],
                    'resource_type': 'cpu',
                    'current_usage': f"{pod.get('avg_cpu_millicores', 0)}m",
                    'optimization_type': 'review_limits',
                    'priority': 'high' if pod.get('avg_cpu_millicores', 0) > 2000 else 'medium'
                }
                for pod in high_cpu_pods
            ])
        
        # Analyze top memory consumers
        top_memory = pod_metrics.get('top_memory_consumers', [])
        if top_memory:
            high_memory_pods = [pod for pod in top_memory if pod.get('avg_memory_mb', 0) > 1024]
            analysis['rightsizing_candidates'].extend([
                {
                    'pod_name': pod['pod_name'],
                    'namespace': pod['namespace'],
                    'node': pod['node'],
                    'resource_type': 'memory',
                    'current_usage': f"{pod.get('avg_memory_mb', 0):.1f}MB",
                    'optimization_type': 'review_limits',
                    'priority': 'high' if pod.get('avg_memory_mb', 0) > 2048 else 'medium'
                }
                for pod in high_memory_pods
            ])
        
        # Analyze by namespace
        namespace_analysis = pod_metrics.get('namespace_analysis', {})
        if namespace_analysis:
            for namespace, data in namespace_analysis.items():
                if data['total_cpu_usage'] > 5000 or data['total_memory_usage'] > 10240:  # High consumption
                    analysis['namespace_efficiency'][namespace] = {
                        'cpu_usage': data['total_cpu_usage'],
                        'memory_usage': data['total_memory_usage'],
                        'efficiency_category': 'high_consumption',
                        'recommendation': 'Review resource allocation and consider optimization'
                    }
        
        # Generate recommendations
        if len(analysis['rightsizing_candidates']) > 0:
            analysis['recommendations'].append({
                'type': 'pod_rightsizing',
                'priority': 'high',
                'title': f'{len(analysis["rightsizing_candidates"])} Pods Need Rightsizing',
                'description': 'High resource-consuming pods identified',
                'estimated_savings': '10-25%',
                'action': 'Review and optimize resource requests/limits for identified pods'
            })
        
        return analysis
    
    def _analyze_health_metrics_for_finops(self, health_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze health metrics from a FinOps perspective."""
        analysis = {
            'operational_efficiency': {},
            'cost_impact_events': [],
            'stability_insights': {},
            'recommendations': []
        }
        
        cluster_events = health_metrics.get('cluster_events', {})
        if cluster_events:
            error_events = cluster_events.get('error_events', [])
            warning_events = cluster_events.get('warning_events', [])
            
            # Analyze cost-impacting events
            cost_impact_keywords = ['failed', 'error', 'outofmemory', 'evicted', 'schedulingfailed']
            
            for event in error_events + warning_events:
                event_reason = event.get('reason', '').lower()
                if any(keyword in event_reason for keyword in cost_impact_keywords):
                    analysis['cost_impact_events'].append({
                        'reason': event.get('reason'),
                        'message': event.get('message'),
                        'event_count': event.get('event_count', 0),
                        'object_kind': event.get('object_kind'),
                        'cost_impact': 'high' if 'failed' in event_reason or 'error' in event_reason else 'medium'
                    })
            
            # Calculate operational efficiency
            total_events = len(error_events) + len(warning_events)
            error_ratio = len(error_events) / total_events if total_events > 0 else 0
            
            analysis['operational_efficiency'] = {
                'total_events': total_events,
                'error_events': len(error_events),
                'warning_events': len(warning_events),
                'error_ratio': error_ratio,
                'health_score': max(0, 100 - (error_ratio * 100))
            }
            
            # Generate health-based recommendations
            if error_ratio > 0.3:  # More than 30% error events
                analysis['recommendations'].append({
                    'type': 'operational_stability',
                    'priority': 'high',
                    'title': 'High Error Rate Detected',
                    'description': f'{error_ratio:.1%} of cluster events are errors',
                    'cost_impact': 'Resource waste due to failed workloads',
                    'action': 'Investigate and resolve recurring error patterns'
                })
        
        return analysis
    
    # Utility methods
    def _analyze_cpu_efficiency(self, cpu_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze CPU efficiency patterns."""
        analysis = {
            'average_utilization': 0.0,
            'peak_utilization': 0.0,
            'utilization_trend': 'stable',
            'efficiency_score': 0.0,
            'waste_percentage': 0.0
        }
        
        all_values = []
        for metric_data in cpu_metrics.values():
            if isinstance(metric_data, list):
                values = [point.get('average', 0) for point in metric_data if point.get('average') is not None]
                all_values.extend(values)
        
        if all_values:
            avg_util = sum(all_values) / len(all_values)
            max_util = max(all_values)
            min_util = min(all_values)
            
            analysis['average_utilization'] = avg_util
            analysis['peak_utilization'] = max_util
            analysis['waste_percentage'] = max(0, 100 - avg_util)
            
            # Calculate efficiency score (target ~70% utilization)
            if avg_util <= 70:
                analysis['efficiency_score'] = avg_util / 70 * 100
            else:
                analysis['efficiency_score'] = max(0, 100 - (avg_util - 70))
            
            # Trend analysis
            if len(all_values) > 1:
                first_half = all_values[:len(all_values)//2]
                second_half = all_values[len(all_values)//2:]
                
                avg_first = sum(first_half) / len(first_half)
                avg_second = sum(second_half) / len(second_half)
                
                if avg_second > avg_first * 1.1:
                    analysis['utilization_trend'] = 'increasing'
                elif avg_second < avg_first * 0.9:
                    analysis['utilization_trend'] = 'decreasing'
        
        return analysis
    
    def _analyze_memory_efficiency(self, memory_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze memory efficiency patterns."""
        analysis = {
            'average_utilization': 0.0,
            'peak_utilization': 0.0,
            'efficiency_score': 0.0,
            'memory_pressure_events': 0
        }
        
        all_values = []
        for metric_data in memory_metrics.values():
            if isinstance(metric_data, list):
                values = [point.get('average', 0) for point in metric_data if point.get('average') is not None]
                all_values.extend(values)
        
        if all_values:
            avg_util = sum(all_values) / len(all_values)
            max_util = max(all_values)
            
            analysis['average_utilization'] = avg_util
            analysis['peak_utilization'] = max_util
            
            # Count memory pressure events (>85% utilization)
            analysis['memory_pressure_events'] = len([v for v in all_values if v > 85])
            
            # Calculate efficiency score (target ~80% utilization)
            if avg_util <= 80:
                analysis['efficiency_score'] = avg_util / 80 * 100
            else:
                analysis['efficiency_score'] = max(0, 100 - (avg_util - 80))
        
        return analysis
    
    def _analyze_autoscaler_efficiency(self, autoscaler_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cluster autoscaler efficiency."""
        analysis = {
            'autoscaler_status': 'unknown',
            'scaling_events': 0,
            'unneeded_nodes': 0,
            'efficiency_insights': {}
        }
        
        # Analyze cluster autoscaler metrics
        safe_to_scale = autoscaler_metrics.get('cluster_autoscaler_cluster_safe_to_autoscale', [])
        scale_down_cooldown = autoscaler_metrics.get('cluster_autoscaler_scale_down_in_cooldown', [])
        unneeded_nodes = autoscaler_metrics.get('cluster_autoscaler_unneeded_nodes_count', [])
        
        if safe_to_scale:
            latest_safe = safe_to_scale[-1].get('average', 0) if safe_to_scale else 0
            analysis['autoscaler_status'] = 'healthy' if latest_safe > 0.5 else 'issues_detected'
        
        if unneeded_nodes:
            latest_unneeded = unneeded_nodes[-1].get('average', 0) if unneeded_nodes else 0
            analysis['unneeded_nodes'] = latest_unneeded
            
            if latest_unneeded > 0:
                analysis['efficiency_insights']['cost_waste'] = f"{latest_unneeded} unneeded nodes detected"
        
        return analysis
    
    def _filter_system_namespaces(self, pod_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Filter out system namespaces from pod metrics."""
        filtered_metrics = pod_metrics.copy()
        
        # Filter top CPU consumers
        top_cpu = pod_metrics.get('top_cpu_consumers', [])
        filtered_metrics['top_cpu_consumers'] = [
            pod for pod in top_cpu 
            if pod.get('namespace', '') not in self.system_namespaces
        ]
        
        # Filter top memory consumers
        top_memory = pod_metrics.get('top_memory_consumers', [])
        filtered_metrics['top_memory_consumers'] = [
            pod for pod in top_memory 
            if pod.get('namespace', '') not in self.system_namespaces
        ]
        
        # Filter namespace analysis
        namespace_analysis = pod_metrics.get('namespace_analysis', {})
        filtered_metrics['namespace_analysis'] = {
            ns: data for ns, data in namespace_analysis.items()
            if ns not in self.system_namespaces
        }
        
        return filtered_metrics
    
    async def _collect_fallback_metrics(self) -> List[Dict[str, Any]]:
        """Fallback method when cluster resource ID is not available."""
        self.logger.info("Using fallback metrics collection")
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=self.metrics_time_range_hours)
        
        return [{
            'type': 'metrics_collection_fallback',
            'cluster_name': self.cluster_name or 'unknown',
            'data': {
                'status': 'fallback_mode',
                'reason': 'cluster_resource_id_not_available',
                'collection_time': end_time.isoformat(),
                'time_range_hours': self.metrics_time_range_hours,
                'configuration': {
                    'subscription_id': self.subscription_id,
                    'resource_group': self.resource_group,
                    'cluster_name': self.cluster_name
                }
            },
            'recommendations': [
                'Ensure cluster resource ID is provided in configuration',
                'Verify Azure Monitor permissions for the cluster',
                'Check if Azure Monitor addon is enabled on the AKS cluster',
                'Confirm Log Analytics workspace is configured for pod-level metrics'
            ],
            'next_steps': [
                'Configure cluster_resource_id in metrics collection config',
                'Enable Azure Monitor for Containers addon',
                'Set up Log Analytics workspace integration',
                'Verify service principal has Monitoring Reader role'
            ]
        }]
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "comprehensive_metrics_collection"
    
    def get_collection_summary(self) -> Dict[str, Any]:
        """Get a summary of the collection configuration."""
        return {
            'service_type': 'MetricsCollectionService',
            'cluster_name': self.cluster_name,
            'cluster_resource_id': self.cluster_resource_id,
            'time_range_hours': self.metrics_time_range_hours,
            'granularity_minutes': self.granularity_minutes,
            'collection_categories': {
                'cluster_metrics': self.include_cluster_metrics,
                'individual_node_metrics': self.include_individual_node_metrics,
                'pod_metrics': self.include_pod_metrics,
                'storage_metrics': self.include_storage_metrics,
                'network_metrics': self.include_network_metrics,
                'health_metrics': self.include_health_metrics
            },
            'performance_settings': {
                'max_concurrent_requests': self.max_concurrent_requests,
                'request_timeout_seconds': self.request_timeout_seconds,
                'max_retries': self.max_retries,
                'max_data_points': self.max_data_points
            },
            'filtering': {
                'namespace_filter': self.namespace_filter,
                'exclude_system_namespaces': self.exclude_system_namespaces,
                'system_namespaces': self.system_namespaces
            }
        }
    
    async def validate_configuration(self) -> Dict[str, Any]:
        """Validate the metrics collection configuration."""
        validation = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'recommendations': []
        }
        
        # Check required configuration
        if not self.cluster_resource_id:
            validation['errors'].append("cluster_resource_id is required for comprehensive metrics collection")
            validation['is_valid'] = False
        
        if not self.cluster_name:
            validation['warnings'].append("cluster_name not provided - using fallback identification")
        
        # Check client connectivity
        if not self.client.is_connected:
            try:
                await self.client.connect()
            except Exception as e:
                validation['errors'].append(f"Failed to connect to Azure Monitor: {e}")
                validation['is_valid'] = False
        
        # Validate time range
        if self.metrics_time_range_hours > 720:  # 30 days
            validation['warnings'].append("Very large time range may impact performance and cost")
        
        if self.granularity_minutes < 1:
            validation['errors'].append("Granularity must be at least 1 minute")
            validation['is_valid'] = False
        
        # Check Log Analytics for pod metrics
        if self.include_pod_metrics and not self.client.log_analytics_workspace_id:
            validation['warnings'].append("Pod metrics require Log Analytics workspace configuration")
            validation['recommendations'].append("Configure log_analytics_workspace_id for detailed pod metrics")
        
        # Performance recommendations
        if self.max_concurrent_requests > 10:
            validation['warnings'].append("High concurrency may hit Azure Monitor rate limits")
        
        return validation
    
    def get_estimated_api_calls(self) -> Dict[str, Any]:
        """Estimate the number of API calls this collection will make."""
        estimate = {
            'total_estimated_calls': 0,
            'breakdown': {},
            'factors': {
                'time_range_hours': self.metrics_time_range_hours,
                'granularity_minutes': self.granularity_minutes,
                'collection_categories': []
            }
        }
        
        # Base metrics per category
        if self.include_cluster_metrics:
            cluster_calls = len(self.client.aks_cluster_metrics) if hasattr(self.client, 'aks_cluster_metrics') else 10
            estimate['breakdown']['cluster_metrics'] = cluster_calls
            estimate['total_estimated_calls'] += cluster_calls
            estimate['factors']['collection_categories'].append('cluster_metrics')
        
        if self.include_individual_node_metrics:
            # Estimate based on typical node count (3-10 nodes)
            node_calls = 5 * 3  # Assume 3 nodes average
            estimate['breakdown']['individual_node_metrics'] = node_calls
            estimate['total_estimated_calls'] += node_calls
            estimate['factors']['collection_categories'].append('individual_node_metrics')
        
        if self.include_storage_metrics:
            storage_calls = 3  # Typical storage metrics
            estimate['breakdown']['storage_metrics'] = storage_calls
            estimate['total_estimated_calls'] += storage_calls
            estimate['factors']['collection_categories'].append('storage_metrics')
        
        if self.include_network_metrics:
            network_calls = 3  # Typical network metrics
            estimate['breakdown']['network_metrics'] = network_calls
            estimate['total_estimated_calls'] += network_calls
            estimate['factors']['collection_categories'].append('network_metrics')
        
        if self.include_pod_metrics:
            pod_calls = 5  # Log Analytics queries
            estimate['breakdown']['pod_metrics'] = pod_calls
            estimate['total_estimated_calls'] += pod_calls
            estimate['factors']['collection_categories'].append('pod_metrics')
        
        if self.include_health_metrics:
            health_calls = 2  # Health queries
            estimate['breakdown']['health_metrics'] = health_calls
            estimate['total_estimated_calls'] += health_calls
            estimate['factors']['collection_categories'].append('health_metrics')
        
        return estimate