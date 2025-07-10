"""Enhanced Metrics Collection Service for FinOps discovery phase."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.azure.monitor_client import MonitorClient

logger = structlog.get_logger(__name__)


class MetricsCollectionService(BaseDiscoveryService):
    """Enhanced service for collecting comprehensive metrics for FinOps discovery."""
    
    def __init__(self, monitor_client: MonitorClient, config: Dict[str, Any]):
        super().__init__(monitor_client, config)
        self.cluster_resource_id = config.get("cluster_resource_id")
        self.cluster_name = config.get("cluster_name", "unknown")
        self.resource_group = config.get("resource_group", "unknown")
        self.metrics_time_range_hours = config.get("metrics_time_range_hours", 24)
        self.include_node_metrics = config.get("include_node_metrics", True)
        self.include_pod_metrics = config.get("include_pod_metrics", True)
        self.include_cost_metrics = config.get("include_cost_metrics", True)
        self.granularity_minutes = config.get("granularity_minutes", 5)
    
    async def discover(self) -> Dict[str, Any]:
        """Discover comprehensive metrics for FinOps analysis."""
        self.logger.info("Starting enhanced metrics collection for FinOps discovery")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        if not self.cluster_resource_id:
            self.logger.warning("No cluster resource ID provided, returning empty metrics")
            return self._get_empty_metrics_structure()
        
        # Main metrics discovery structure
        metrics_discovery = {
            'discovery_metadata': {
                'discovery_type': 'metrics_collection',
                'cluster_resource_id': self.cluster_resource_id,
                'cluster_name': self.cluster_name,
                'resource_group': self.resource_group,
                'discovery_timestamp': datetime.now(timezone.utc).isoformat(),
                'collection_parameters': {
                    'time_range_hours': self.metrics_time_range_hours,
                    'granularity_minutes': self.granularity_minutes,
                    'include_node_metrics': self.include_node_metrics,
                    'include_pod_metrics': self.include_pod_metrics,
                    'include_cost_metrics': self.include_cost_metrics
                },
                'discovery_status': 'in_progress',
                'data_sources': [],
                'collection_errors': []
            },
            'cluster_overview': {},
            'resource_utilization': {},
            'capacity_planning': {},
            'cost_insights': {},
            'efficiency_metrics': {},
            'optimization_opportunities': [],
            'finops_summary': {},
            'recommendations': []
        }
        
        try:
            # Collect cluster-level metrics
            cluster_metrics = await self._collect_cluster_metrics()
            if cluster_metrics:
                metrics_discovery['cluster_overview'] = cluster_metrics
                metrics_discovery['discovery_metadata']['data_sources'].append('azure_monitor_cluster')
            
            # Process resource utilization metrics
            await self._process_resource_utilization(cluster_metrics, metrics_discovery)
            
            # Analyze capacity planning metrics
            await self._analyze_capacity_planning(cluster_metrics, metrics_discovery)
            
            # Generate cost insights
            if self.include_cost_metrics:
                await self._generate_cost_insights(cluster_metrics, metrics_discovery)
            
            # Calculate efficiency metrics
            await self._calculate_efficiency_metrics(cluster_metrics, metrics_discovery)
            
            # Generate optimization opportunities
            await self._generate_optimization_opportunities(metrics_discovery)
            
            # Create FinOps summary
            await self._create_finops_summary(metrics_discovery)
            
            # Generate actionable recommendations
            await self._generate_recommendations(metrics_discovery)
            
            metrics_discovery['discovery_metadata']['discovery_status'] = 'completed'
            
            self.logger.info(
                "Enhanced metrics collection completed",
                cluster=self.cluster_name,
                data_sources=len(metrics_discovery['discovery_metadata']['data_sources']),
                optimization_opportunities=len(metrics_discovery['optimization_opportunities']),
                recommendations=len(metrics_discovery['recommendations'])
            )
            
        except Exception as e:
            self.logger.error("Failed to collect enhanced metrics", error=str(e))
            metrics_discovery['discovery_metadata']['discovery_status'] = 'failed'
            metrics_discovery['discovery_metadata']['collection_errors'].append(str(e))
            raise
        
        return metrics_discovery
    
    async def _collect_cluster_metrics(self) -> Dict[str, Any]:
        """Collect comprehensive cluster metrics."""
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=self.metrics_time_range_hours)
            granularity = timedelta(minutes=self.granularity_minutes)
            
            cluster_metrics = await self.client.get_cluster_metrics(
                cluster_resource_id=self.cluster_resource_id,
                start_time=start_time,
                end_time=end_time,
                granularity=granularity
            )
            
            # Also collect Log Analytics insights if available
            if hasattr(self.client, 'get_finops_log_analytics_insights'):
                try:
                    log_analytics_insights = await self.client.get_finops_log_analytics_insights(
                        cluster_name=self.cluster_name,
                        start_time=start_time,
                        end_time=end_time
                    )
                    
                    if log_analytics_insights:
                        cluster_metrics['log_analytics_insights'] = log_analytics_insights
                        self.logger.info("Successfully collected Log Analytics insights")
                    
                except Exception as e:
                    self.logger.warning("Failed to collect Log Analytics insights", error=str(e))
            
            return cluster_metrics
            
        except Exception as e:
            self.logger.error("Failed to collect cluster metrics", error=str(e))
            return {}
    
    async def _process_resource_utilization(self, 
                                          cluster_metrics: Dict[str, Any], 
                                          metrics_discovery: Dict[str, Any]) -> None:
        """Process and structure resource utilization data for FinOps insights."""
        resource_utilization = {
            'cpu_utilization': {
                'current_state': 'unknown',
                'utilization_pattern': 'unknown',
                'efficiency_score': 0.0,
                'waste_indicators': [],
                'optimization_potential': 'unknown'
            },
            'memory_utilization': {
                'current_state': 'unknown',
                'utilization_pattern': 'unknown',
                'efficiency_score': 0.0,
                'waste_indicators': [],
                'optimization_potential': 'unknown'
            },
            'storage_utilization': {
                'current_state': 'unknown',
                'utilization_pattern': 'unknown',
                'efficiency_score': 0.0,
                'waste_indicators': [],
                'optimization_potential': 'unknown'
            },
            'network_utilization': {
                'current_state': 'unknown',
                'traffic_pattern': 'unknown',
                'bandwidth_efficiency': 0.0,
                'cost_implications': []
            },
            'overall_utilization': {
                'efficiency_rating': 'unknown',
                'resource_balance': 'unknown',
                'bottlenecks_detected': [],
                'rightsizing_opportunities': []
            }
        }
        
        cluster_data = cluster_metrics.get('cluster_metrics', {})
        
        # Process CPU utilization
        cpu_metric = cluster_data.get('node_cpu_usage_percentage')
        if cpu_metric:
            await self._analyze_cpu_utilization(cpu_metric, resource_utilization['cpu_utilization'])
        
        # Process Memory utilization
        memory_metric = cluster_data.get('node_memory_working_set_percentage')
        if memory_metric:
            await self._analyze_memory_utilization(memory_metric, resource_utilization['memory_utilization'])
        
        # Process Storage utilization
        disk_metric = cluster_data.get('node_disk_usage_percentage')
        if disk_metric:
            await self._analyze_storage_utilization(disk_metric, resource_utilization['storage_utilization'])
        
        # Process Network utilization
        network_in = cluster_data.get('node_network_in_bytes')
        network_out = cluster_data.get('node_network_out_bytes')
        if network_in and network_out:
            await self._analyze_network_utilization(network_in, network_out, resource_utilization['network_utilization'])
        
        # Calculate overall utilization summary
        await self._calculate_overall_utilization(resource_utilization)
        
        metrics_discovery['resource_utilization'] = resource_utilization
    
    async def _analyze_cpu_utilization(self, cpu_metric: Dict[str, Any], cpu_analysis: Dict[str, Any]) -> None:
        """Analyze CPU utilization patterns for FinOps insights."""
        stats = cpu_metric.get('statistics', {})
        avg_cpu = stats.get('average', 0)
        max_cpu = stats.get('maximum', 0)
        trend = stats.get('trend', 'stable')
        
        # Determine current state
        if avg_cpu < 20:
            cpu_analysis['current_state'] = 'severely_underutilized'
            cpu_analysis['efficiency_score'] = 25.0
            cpu_analysis['waste_indicators'].extend([
                'Very low CPU utilization indicates significant waste',
                'Potential for major cost savings through rightsizing'
            ])
            cpu_analysis['optimization_potential'] = 'very_high'
        elif avg_cpu < 40:
            cpu_analysis['current_state'] = 'underutilized'
            cpu_analysis['efficiency_score'] = 50.0
            cpu_analysis['waste_indicators'].append('CPU resources are underutilized')
            cpu_analysis['optimization_potential'] = 'high'
        elif avg_cpu < 70:
            cpu_analysis['current_state'] = 'moderately_utilized'
            cpu_analysis['efficiency_score'] = 75.0
            cpu_analysis['optimization_potential'] = 'medium'
        elif avg_cpu < 85:
            cpu_analysis['current_state'] = 'well_utilized'
            cpu_analysis['efficiency_score'] = 90.0
            cpu_analysis['optimization_potential'] = 'low'
        else:
            cpu_analysis['current_state'] = 'highly_utilized'
            cpu_analysis['efficiency_score'] = 95.0
            if max_cpu > 95:
                cpu_analysis['waste_indicators'].append('Potential CPU bottleneck - consider scaling up')
            cpu_analysis['optimization_potential'] = 'low'
        
        # Analyze utilization pattern
        if trend == 'increasing':
            cpu_analysis['utilization_pattern'] = 'growing_demand'
        elif trend == 'decreasing':
            cpu_analysis['utilization_pattern'] = 'declining_demand'
        else:
            cpu_analysis['utilization_pattern'] = 'stable_demand'
    
    async def _analyze_memory_utilization(self, memory_metric: Dict[str, Any], memory_analysis: Dict[str, Any]) -> None:
        """Analyze memory utilization patterns for FinOps insights."""
        stats = memory_metric.get('statistics', {})
        avg_memory = stats.get('average', 0)
        max_memory = stats.get('maximum', 0)
        trend = stats.get('trend', 'stable')
        
        # Determine current state
        if avg_memory < 25:
            memory_analysis['current_state'] = 'severely_underutilized'
            memory_analysis['efficiency_score'] = 30.0
            memory_analysis['waste_indicators'].extend([
                'Very low memory utilization indicates oversized instances',
                'Significant cost reduction possible through rightsizing'
            ])
            memory_analysis['optimization_potential'] = 'very_high'
        elif avg_memory < 50:
            memory_analysis['current_state'] = 'underutilized'
            memory_analysis['efficiency_score'] = 60.0
            memory_analysis['waste_indicators'].append('Memory allocation exceeds actual usage')
            memory_analysis['optimization_potential'] = 'high'
        elif avg_memory < 75:
            memory_analysis['current_state'] = 'moderately_utilized'
            memory_analysis['efficiency_score'] = 80.0
            memory_analysis['optimization_potential'] = 'medium'
        elif avg_memory < 90:
            memory_analysis['current_state'] = 'well_utilized'
            memory_analysis['efficiency_score'] = 95.0
            memory_analysis['optimization_potential'] = 'low'
        else:
            memory_analysis['current_state'] = 'highly_utilized'
            memory_analysis['efficiency_score'] = 90.0  # Slightly lower due to pressure risk
            if max_memory > 95:
                memory_analysis['waste_indicators'].append('Memory pressure detected - risk of performance issues')
            memory_analysis['optimization_potential'] = 'medium'  # May need scaling up
        
        # Analyze utilization pattern
        if trend == 'increasing':
            memory_analysis['utilization_pattern'] = 'growing_usage'
        elif trend == 'decreasing':
            memory_analysis['utilization_pattern'] = 'declining_usage'
        else:
            memory_analysis['utilization_pattern'] = 'stable_usage'
    
    async def _analyze_storage_utilization(self, disk_metric: Dict[str, Any], storage_analysis: Dict[str, Any]) -> None:
        """Analyze storage utilization patterns for FinOps insights."""
        stats = disk_metric.get('statistics', {})
        avg_disk = stats.get('average', 0)
        max_disk = stats.get('maximum', 0)
        trend = stats.get('trend', 'stable')
        
        # Determine current state
        if avg_disk < 25:
            storage_analysis['current_state'] = 'underutilized'
            storage_analysis['efficiency_score'] = 40.0
            storage_analysis['waste_indicators'].append('Significant unused storage capacity')
            storage_analysis['optimization_potential'] = 'high'
        elif avg_disk < 60:
            storage_analysis['current_state'] = 'moderately_utilized'
            storage_analysis['efficiency_score'] = 70.0
            storage_analysis['optimization_potential'] = 'medium'
        elif avg_disk < 85:
            storage_analysis['current_state'] = 'well_utilized'
            storage_analysis['efficiency_score'] = 90.0
            storage_analysis['optimization_potential'] = 'low'
        else:
            storage_analysis['current_state'] = 'highly_utilized'
            storage_analysis['efficiency_score'] = 85.0
            if max_disk > 95:
                storage_analysis['waste_indicators'].append('Storage capacity nearly exhausted - expansion needed')
            storage_analysis['optimization_potential'] = 'high'  # Need to expand or optimize
        
        # Analyze utilization pattern
        if trend == 'increasing':
            storage_analysis['utilization_pattern'] = 'growing_storage_needs'
        elif trend == 'decreasing':
            storage_analysis['utilization_pattern'] = 'storage_cleanup_opportunity'
        else:
            storage_analysis['utilization_pattern'] = 'stable_storage_usage'
    
    async def _analyze_network_utilization(self, 
                                         network_in: Dict[str, Any], 
                                         network_out: Dict[str, Any], 
                                         network_analysis: Dict[str, Any]) -> None:
        """Analyze network utilization patterns for FinOps insights."""
        in_stats = network_in.get('statistics', {})
        out_stats = network_out.get('statistics', {})
        
        avg_in = in_stats.get('average', 0)
        avg_out = out_stats.get('average', 0)
        
        # Convert bytes to GB for analysis
        total_gb_per_hour = (avg_in + avg_out) / (1024**3)
        daily_gb = total_gb_per_hour * 24
        
        # Determine traffic state
        if daily_gb < 10:
            network_analysis['current_state'] = 'low_traffic'
            network_analysis['bandwidth_efficiency'] = 60.0
        elif daily_gb < 100:
            network_analysis['current_state'] = 'moderate_traffic'
            network_analysis['bandwidth_efficiency'] = 80.0
        elif daily_gb < 1000:
            network_analysis['current_state'] = 'high_traffic'
            network_analysis['bandwidth_efficiency'] = 90.0
        else:
            network_analysis['current_state'] = 'very_high_traffic'
            network_analysis['bandwidth_efficiency'] = 85.0
            network_analysis['cost_implications'].append('High data transfer costs - consider data locality optimization')
        
        # Analyze traffic pattern
        in_trend = in_stats.get('trend', 'stable')
        out_trend = out_stats.get('trend', 'stable')
        
        if in_trend == 'increasing' or out_trend == 'increasing':
            network_analysis['traffic_pattern'] = 'growing_traffic'
            network_analysis['cost_implications'].append('Growing network usage - monitor egress costs')
        elif in_trend == 'decreasing' and out_trend == 'decreasing':
            network_analysis['traffic_pattern'] = 'declining_traffic'
        else:
            network_analysis['traffic_pattern'] = 'stable_traffic'
    
    async def _calculate_overall_utilization(self, resource_utilization: Dict[str, Any]) -> None:
        """Calculate overall utilization metrics and identify bottlenecks."""
        overall = resource_utilization['overall_utilization']
        
        # Calculate average efficiency across resources
        cpu_score = resource_utilization['cpu_utilization']['efficiency_score']
        memory_score = resource_utilization['memory_utilization']['efficiency_score']
        storage_score = resource_utilization['storage_utilization']['efficiency_score']
        
        avg_efficiency = (cpu_score + memory_score + storage_score) / 3
        
        # Determine overall efficiency rating
        if avg_efficiency >= 85:
            overall['efficiency_rating'] = 'excellent'
        elif avg_efficiency >= 70:
            overall['efficiency_rating'] = 'good'
        elif avg_efficiency >= 50:
            overall['efficiency_rating'] = 'fair'
        else:
            overall['efficiency_rating'] = 'poor'
        
        # Analyze resource balance
        scores = [cpu_score, memory_score, storage_score]
        score_variance = max(scores) - min(scores)
        
        if score_variance < 15:
            overall['resource_balance'] = 'well_balanced'
        elif score_variance < 30:
            overall['resource_balance'] = 'moderately_balanced'
        else:
            overall['resource_balance'] = 'imbalanced'
            overall['bottlenecks_detected'].append('Significant variance in resource utilization detected')
        
        # Identify specific bottlenecks
        if cpu_score > 90 and (memory_score < 70 or storage_score < 70):
            overall['bottlenecks_detected'].append('CPU constrained - consider CPU-optimized instances')
        
        if memory_score > 90 and (cpu_score < 70 or storage_score < 70):
            overall['bottlenecks_detected'].append('Memory constrained - consider memory-optimized instances')
        
        # Generate rightsizing opportunities
        if cpu_score < 50 and memory_score < 50:
            overall['rightsizing_opportunities'].append('Significant over-provisioning - consider smaller instance sizes')
        elif cpu_score < 50:
            overall['rightsizing_opportunities'].append('CPU over-provisioned - consider CPU-optimized or smaller instances')
        elif memory_score < 50:
            overall['rightsizing_opportunities'].append('Memory over-provisioned - consider compute-optimized instances')
    
    async def _analyze_capacity_planning(self, 
                                       cluster_metrics: Dict[str, Any], 
                                       metrics_discovery: Dict[str, Any]) -> None:
        """Analyze capacity planning metrics and trends."""
        capacity_planning = {
            'current_capacity': {
                'cpu_capacity_status': 'unknown',
                'memory_capacity_status': 'unknown',
                'storage_capacity_status': 'unknown',
                'overall_capacity_health': 'unknown'
            },
            'growth_trends': {
                'cpu_trend': 'stable',
                'memory_trend': 'stable',
                'storage_trend': 'stable',
                'projected_capacity_needs': {}
            },
            'scaling_recommendations': {
                'immediate_actions': [],
                'short_term_planning': [],
                'long_term_planning': []
            }
        }
        
        cluster_data = cluster_metrics.get('cluster_metrics', {})
        
        # Analyze current capacity status
        cpu_metric = cluster_data.get('node_cpu_usage_percentage', {})
        memory_metric = cluster_data.get('node_memory_working_set_percentage', {})
        storage_metric = cluster_data.get('node_disk_usage_percentage', {})
        
        # CPU capacity analysis
        cpu_stats = cpu_metric.get('statistics', {})
        avg_cpu = cpu_stats.get('average', 0)
        max_cpu = cpu_stats.get('maximum', 0)
        
        if max_cpu > 90:
            capacity_planning['current_capacity']['cpu_capacity_status'] = 'critical'
            capacity_planning['scaling_recommendations']['immediate_actions'].append(
                'CPU capacity critical - immediate scaling required'
            )
        elif avg_cpu > 75:
            capacity_planning['current_capacity']['cpu_capacity_status'] = 'high'
            capacity_planning['scaling_recommendations']['short_term_planning'].append(
                'Plan CPU capacity expansion within 30 days'
            )
        elif avg_cpu > 50:
            capacity_planning['current_capacity']['cpu_capacity_status'] = 'moderate'
        else:
            capacity_planning['current_capacity']['cpu_capacity_status'] = 'low'
            capacity_planning['scaling_recommendations']['long_term_planning'].append(
                'Consider CPU capacity optimization for cost savings'
            )
        
        # Memory capacity analysis
        memory_stats = memory_metric.get('statistics', {})
        avg_memory = memory_stats.get('average', 0)
        max_memory = memory_stats.get('maximum', 0)
        
        if max_memory > 95:
            capacity_planning['current_capacity']['memory_capacity_status'] = 'critical'
            capacity_planning['scaling_recommendations']['immediate_actions'].append(
                'Memory capacity critical - immediate scaling required'
            )
        elif avg_memory > 80:
            capacity_planning['current_capacity']['memory_capacity_status'] = 'high'
            capacity_planning['scaling_recommendations']['short_term_planning'].append(
                'Plan memory capacity expansion within 30 days'
            )
        elif avg_memory > 60:
            capacity_planning['current_capacity']['memory_capacity_status'] = 'moderate'
        else:
            capacity_planning['current_capacity']['memory_capacity_status'] = 'low'
        
        # Storage capacity analysis
        storage_stats = storage_metric.get('statistics', {})
        avg_storage = storage_stats.get('average', 0)
        max_storage = storage_stats.get('maximum', 0)
        
        if max_storage > 90:
            capacity_planning['current_capacity']['storage_capacity_status'] = 'critical'
            capacity_planning['scaling_recommendations']['immediate_actions'].append(
                'Storage capacity critical - immediate expansion required'
            )
        elif avg_storage > 75:
            capacity_planning['current_capacity']['storage_capacity_status'] = 'high'
            capacity_planning['scaling_recommendations']['short_term_planning'].append(
                'Plan storage expansion within 60 days'
            )
        elif avg_storage > 50:
            capacity_planning['current_capacity']['storage_capacity_status'] = 'moderate'
        else:
            capacity_planning['current_capacity']['storage_capacity_status'] = 'low'
        
        # Overall capacity health
        capacity_statuses = [
            capacity_planning['current_capacity']['cpu_capacity_status'],
            capacity_planning['current_capacity']['memory_capacity_status'],
            capacity_planning['current_capacity']['storage_capacity_status']
        ]
        
        if 'critical' in capacity_statuses:
            capacity_planning['current_capacity']['overall_capacity_health'] = 'critical'
        elif 'high' in capacity_statuses:
            capacity_planning['current_capacity']['overall_capacity_health'] = 'high'
        elif 'moderate' in capacity_statuses:
            capacity_planning['current_capacity']['overall_capacity_health'] = 'moderate'
        else:
            capacity_planning['current_capacity']['overall_capacity_health'] = 'healthy'
        
        # Analyze growth trends
        capacity_planning['growth_trends']['cpu_trend'] = cpu_stats.get('trend', 'stable')
        capacity_planning['growth_trends']['memory_trend'] = memory_stats.get('trend', 'stable')
        capacity_planning['growth_trends']['storage_trend'] = storage_stats.get('trend', 'stable')
        
        metrics_discovery['capacity_planning'] = capacity_planning
    
    async def _generate_cost_insights(self, 
                                    cluster_metrics: Dict[str, Any], 
                                    metrics_discovery: Dict[str, Any]) -> None:
        """Generate cost insights based on utilization metrics."""
        cost_insights = {
            'current_cost_efficiency': {
                'overall_score': 0.0,
                'cpu_cost_efficiency': 0.0,
                'memory_cost_efficiency': 0.0,
                'storage_cost_efficiency': 0.0,
                'network_cost_efficiency': 0.0
            },
            'waste_analysis': {
                'estimated_waste_percentage': 0.0,
                'primary_waste_sources': [],
                'potential_monthly_savings': 'unknown'
            },
            'optimization_impact': {
                'immediate_savings_potential': 'unknown',
                'long_term_savings_potential': 'unknown',
                'optimization_complexity': 'unknown'
            },
            'cost_recommendations': []
        }
        
        resource_util = metrics_discovery.get('resource_utilization', {})
        
        # Calculate cost efficiency scores
        cpu_efficiency = resource_util.get('cpu_utilization', {}).get('efficiency_score', 0)
        memory_efficiency = resource_util.get('memory_utilization', {}).get('efficiency_score', 0)
        storage_efficiency = resource_util.get('storage_utilization', {}).get('efficiency_score', 0)
        network_efficiency = resource_util.get('network_utilization', {}).get('bandwidth_efficiency', 0)
        
        cost_insights['current_cost_efficiency']['cpu_cost_efficiency'] = cpu_efficiency
        cost_insights['current_cost_efficiency']['memory_cost_efficiency'] = memory_efficiency
        cost_insights['current_cost_efficiency']['storage_cost_efficiency'] = storage_efficiency
        cost_insights['current_cost_efficiency']['network_cost_efficiency'] = network_efficiency
        
        overall_efficiency = (cpu_efficiency + memory_efficiency + storage_efficiency + network_efficiency) / 4
        cost_insights['current_cost_efficiency']['overall_score'] = overall_efficiency
        
        # Analyze waste
        waste_percentage = 100 - overall_efficiency
        cost_insights['waste_analysis']['estimated_waste_percentage'] = waste_percentage
        
        if cpu_efficiency < 60:
            cost_insights['waste_analysis']['primary_waste_sources'].append('CPU over-provisioning')
        if memory_efficiency < 60:
            cost_insights['waste_analysis']['primary_waste_sources'].append('Memory over-provisioning')
        if storage_efficiency < 60:
            cost_insights['waste_analysis']['primary_waste_sources'].append('Storage over-provisioning')
        
        # Estimate savings potential
        if waste_percentage > 40:
            cost_insights['waste_analysis']['potential_monthly_savings'] = 'high (>30% of current costs)'
            cost_insights['optimization_impact']['immediate_savings_potential'] = 'very_high'
            cost_insights['optimization_impact']['optimization_complexity'] = 'medium'
        elif waste_percentage > 25:
            cost_insights['waste_analysis']['potential_monthly_savings'] = 'medium (15-30% of current costs)'
            cost_insights['optimization_impact']['immediate_savings_potential'] = 'high'
            cost_insights['optimization_impact']['optimization_complexity'] = 'low'
        elif waste_percentage > 15:
            cost_insights['waste_analysis']['potential_monthly_savings'] = 'low (5-15% of current costs)'
            cost_insights['optimization_impact']['immediate_savings_potential'] = 'medium'
            cost_insights['optimization_impact']['optimization_complexity'] = 'low'
        else:
            cost_insights['waste_analysis']['potential_monthly_savings'] = 'minimal (<5% of current costs)'
            cost_insights['optimization_impact']['immediate_savings_potential'] = 'low'
            cost_insights['optimization_impact']['optimization_complexity'] = 'low'
        
        cost_insights['optimization_impact']['long_term_savings_potential'] = 'high' if waste_percentage > 20 else 'medium'
        
        # Generate cost recommendations
        if cpu_efficiency < 50:
            cost_insights['cost_recommendations'].append({
                'type': 'rightsizing',
                'priority': 'high',
                'action': 'Reduce CPU allocation or use smaller instance types',
                'estimated_savings': 'Up to 40% on compute costs'
            })
        
        if memory_efficiency < 50:
            cost_insights['cost_recommendations'].append({
                'type': 'rightsizing',
                'priority': 'high',
                'action': 'Reduce memory allocation or use memory-optimized instances',
                'estimated_savings': 'Up to 35% on compute costs'
            })
        
        if storage_efficiency < 60:
            cost_insights['cost_recommendations'].append({
                'type': 'storage_optimization',
                'priority': 'medium',
                'action': 'Implement storage cleanup policies or use different storage tiers',
                'estimated_savings': 'Up to 25% on storage costs'
            })
        
        metrics_discovery['cost_insights'] = cost_insights
    
    async def _calculate_efficiency_metrics(self, 
                                          cluster_metrics: Dict[str, Any], 
                                          metrics_discovery: Dict[str, Any]) -> None:
        """Calculate comprehensive efficiency metrics for FinOps analysis."""
        efficiency_metrics = {
            'resource_efficiency': {
                'cpu_efficiency_rating': 'unknown',
                'memory_efficiency_rating': 'unknown',
                'storage_efficiency_rating': 'unknown',
                'network_efficiency_rating': 'unknown',
                'overall_efficiency_grade': 'unknown'
            },
            'operational_efficiency': {
                'workload_density': 'unknown',
                'scaling_efficiency': 'unknown',
                'resource_balance': 'unknown'
            },
            'sustainability_metrics': {
                'green_computing_score': 0.0,
                'energy_waste_indicators': []
            }
        }
        
        resource_util = metrics_discovery.get('resource_utilization', {})
        
        # Calculate resource efficiency ratings
        cpu_score = resource_util.get('cpu_utilization', {}).get('efficiency_score', 0)
        memory_score = resource_util.get('memory_utilization', {}).get('efficiency_score', 0)
        storage_score = resource_util.get('storage_utilization', {}).get('efficiency_score', 0)
        network_score = resource_util.get('network_utilization', {}).get('bandwidth_efficiency', 0)
        
        efficiency_metrics['resource_efficiency']['cpu_efficiency_rating'] = self._get_efficiency_grade(cpu_score)
        efficiency_metrics['resource_efficiency']['memory_efficiency_rating'] = self._get_efficiency_grade(memory_score)
        efficiency_metrics['resource_efficiency']['storage_efficiency_rating'] = self._get_efficiency_grade(storage_score)
        efficiency_metrics['resource_efficiency']['network_efficiency_rating'] = self._get_efficiency_grade(network_score)
        
        # Calculate overall efficiency grade
        overall_score = (cpu_score + memory_score + storage_score + network_score) / 4
        efficiency_metrics['resource_efficiency']['overall_efficiency_grade'] = self._get_efficiency_grade(overall_score)
        
        # Calculate operational efficiency
        overall_util = resource_util.get('overall_utilization', {})
        efficiency_metrics['operational_efficiency']['resource_balance'] = overall_util.get('resource_balance', 'unknown')
        efficiency_metrics['operational_efficiency']['workload_density'] = 'medium'  # Placeholder
        efficiency_metrics['operational_efficiency']['scaling_efficiency'] = 'good'  # Placeholder
        
        # Calculate sustainability metrics
        green_score = (cpu_score + memory_score) / 2
        efficiency_metrics['sustainability_metrics']['green_computing_score'] = green_score
        
        if green_score < 70:
            efficiency_metrics['sustainability_metrics']['energy_waste_indicators'].append('Inefficient resource utilization increases carbon footprint')
        
        metrics_discovery['efficiency_metrics'] = efficiency_metrics
    
    def _get_efficiency_grade(self, score: float) -> str:
        """Convert efficiency score to letter grade."""
        if score >= 90:
            return 'A'
        elif score >= 80:
            return 'B'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'
    
    async def _generate_optimization_opportunities(self, metrics_discovery: Dict[str, Any]) -> None:
        """Generate comprehensive optimization opportunities."""
        opportunities = []
        
        resource_util = metrics_discovery.get('resource_utilization', {})
        cost_insights = metrics_discovery.get('cost_insights', {})
        capacity_planning = metrics_discovery.get('capacity_planning', {})
        
        # CPU optimization opportunities
        cpu_util = resource_util.get('cpu_utilization', {})
        if cpu_util.get('efficiency_score', 0) < 60:
            opportunities.append({
                'type': 'rightsizing',
                'category': 'cpu_optimization',
                'priority': 'high',
                'title': 'CPU Over-Provisioning Detected',
                'description': 'CPU utilization is significantly below optimal levels',
                'impact': {
                    'cost_savings': 'high',
                    'effort': 'medium',
                    'risk': 'low'
                },
                'recommended_actions': [
                    'Analyze workload patterns to determine optimal CPU allocation',
                    'Consider downsizing to smaller instance types',
                    'Implement CPU-based auto-scaling'
                ],
                'estimated_savings': '20-40% reduction in compute costs',
                'timeline': '2-4 weeks'
            })
        
        # Memory optimization opportunities
        memory_util = resource_util.get('memory_utilization', {})
        if memory_util.get('efficiency_score', 0) < 60:
            opportunities.append({
                'type': 'rightsizing',
                'category': 'memory_optimization',
                'priority': 'high',
                'title': 'Memory Over-Provisioning Detected',
                'description': 'Memory allocation exceeds actual usage requirements',
                'impact': {
                    'cost_savings': 'high',
                    'effort': 'medium',
                    'risk': 'medium'
                },
                'recommended_actions': [
                    'Review application memory requirements',
                    'Implement memory limits and requests',
                    'Consider memory-optimized instance types'
                ],
                'estimated_savings': '15-35% reduction in compute costs',
                'timeline': '3-6 weeks'
            })
        
        # Storage optimization opportunities
        storage_util = resource_util.get('storage_utilization', {})
        if storage_util.get('efficiency_score', 0) < 70:
            opportunities.append({
                'type': 'storage_optimization',
                'category': 'storage_efficiency',
                'priority': 'medium',
                'title': 'Storage Optimization Opportunity',
                'description': 'Storage resources are not being utilized efficiently',
                'impact': {
                    'cost_savings': 'medium',
                    'effort': 'low',
                    'risk': 'low'
                },
                'recommended_actions': [
                    'Implement storage cleanup policies',
                    'Review storage class assignments',
                    'Consider storage tiering strategies'
                ],
                'estimated_savings': '10-25% reduction in storage costs',
                'timeline': '1-3 weeks'
            })
        
        # Capacity planning opportunities
        capacity_health = capacity_planning.get('current_capacity', {}).get('overall_capacity_health', 'unknown')
        if capacity_health == 'critical':
            opportunities.append({
                'type': 'scaling',
                'category': 'capacity_management',
                'priority': 'critical',
                'title': 'Immediate Capacity Expansion Required',
                'description': 'Current capacity is at critical levels and requires immediate attention',
                'impact': {
                    'cost_savings': 'none',
                    'effort': 'high',
                    'risk': 'high'
                },
                'recommended_actions': [
                    'Implement immediate capacity expansion',
                    'Set up auto-scaling to prevent future capacity issues',
                    'Review capacity planning processes'
                ],
                'estimated_savings': 'Prevents potential downtime costs',
                'timeline': 'Immediate'
            })
        
        # Cost efficiency opportunities
        overall_efficiency = cost_insights.get('current_cost_efficiency', {}).get('overall_score', 0)
        if overall_efficiency < 70:
            opportunities.append({
                'type': 'cost_optimization',
                'category': 'financial_efficiency',
                'priority': 'high',
                'title': 'Comprehensive Cost Optimization',
                'description': 'Multiple areas identified for cost optimization',
                'impact': {
                    'cost_savings': 'very_high',
                    'effort': 'high',
                    'risk': 'medium'
                },
                'recommended_actions': [
                    'Implement comprehensive rightsizing strategy',
                    'Review and optimize resource allocation policies',
                    'Consider reserved instance purchases for stable workloads',
                    'Implement cost monitoring and alerting'
                ],
                'estimated_savings': 'Up to 50% reduction in total costs',
                'timeline': '6-12 weeks'
            })
        
        metrics_discovery['optimization_opportunities'] = opportunities
    
    async def _create_finops_summary(self, metrics_discovery: Dict[str, Any]) -> None:
        """Create comprehensive FinOps summary."""
        summary = {
            'overall_assessment': {
                'efficiency_grade': 'unknown',
                'cost_optimization_potential': 'unknown',
                'operational_health': 'unknown',
                'sustainability_score': 0.0
            },
            'key_findings': {
                'primary_waste_sources': [],
                'optimization_priorities': [],
                'immediate_actions_required': [],
                'long_term_improvements': []
            },
            'financial_impact': {
                'potential_monthly_savings': 'unknown',
                'roi_timeline': 'unknown',
                'investment_required': 'unknown'
            },
            'risk_assessment': {
                'capacity_risks': [],
                'performance_risks': [],
                'cost_risks': [],
                'mitigation_strategies': []
            }
        }
        
        # Calculate overall assessment
        efficiency_metrics = metrics_discovery.get('efficiency_metrics', {})
        cost_insights = metrics_discovery.get('cost_insights', {})
        capacity_planning = metrics_discovery.get('capacity_planning', {})
        
        overall_grade = efficiency_metrics.get('resource_efficiency', {}).get('overall_efficiency_grade', 'unknown')
        summary['overall_assessment']['efficiency_grade'] = overall_grade
        
        waste_percentage = cost_insights.get('waste_analysis', {}).get('estimated_waste_percentage', 0)
        if waste_percentage > 40:
            summary['overall_assessment']['cost_optimization_potential'] = 'very_high'
        elif waste_percentage > 25:
            summary['overall_assessment']['cost_optimization_potential'] = 'high'
        elif waste_percentage > 15:
            summary['overall_assessment']['cost_optimization_potential'] = 'medium'
        else:
            summary['overall_assessment']['cost_optimization_potential'] = 'low'
        
        capacity_health = capacity_planning.get('current_capacity', {}).get('overall_capacity_health', 'unknown')
        summary['overall_assessment']['operational_health'] = capacity_health
        
        green_score = efficiency_metrics.get('sustainability_metrics', {}).get('green_computing_score', 0)
        summary['overall_assessment']['sustainability_score'] = green_score
        
        # Compile key findings
        waste_sources = cost_insights.get('waste_analysis', {}).get('primary_waste_sources', [])
        summary['key_findings']['primary_waste_sources'] = waste_sources
        
        optimization_priorities = cost_insights.get('cost_recommendations', [])
        summary['key_findings']['optimization_priorities'] = [rec['action'] for rec in optimization_priorities[:3]]
        
        immediate_actions = capacity_planning.get('scaling_recommendations', {}).get('immediate_actions', [])
        summary['key_findings']['immediate_actions_required'] = immediate_actions
        
        long_term_actions = capacity_planning.get('scaling_recommendations', {}).get('long_term_planning', [])
        summary['key_findings']['long_term_improvements'] = long_term_actions
        
        # Calculate financial impact
        potential_savings = cost_insights.get('waste_analysis', {}).get('potential_monthly_savings', 'unknown')
        summary['financial_impact']['potential_monthly_savings'] = potential_savings
        
        savings_potential = cost_insights.get('optimization_impact', {}).get('immediate_savings_potential', 'unknown')
        if savings_potential == 'very_high':
            summary['financial_impact']['roi_timeline'] = '1-3 months'
            summary['financial_impact']['investment_required'] = 'low'
        elif savings_potential == 'high':
            summary['financial_impact']['roi_timeline'] = '3-6 months'
            summary['financial_impact']['investment_required'] = 'medium'
        else:
            summary['financial_impact']['roi_timeline'] = '6-12 months'
            summary['financial_impact']['investment_required'] = 'medium'
        
        # Assess risks
        if capacity_health == 'critical':
            summary['risk_assessment']['capacity_risks'].append('Immediate capacity expansion required')
            summary['risk_assessment']['performance_risks'].append('Risk of performance degradation or outages')
        
        if waste_percentage > 30:
            summary['risk_assessment']['cost_risks'].append('Significant budget waste - immediate action recommended')
        
        # Add mitigation strategies
        if summary['risk_assessment']['capacity_risks']:
            summary['risk_assessment']['mitigation_strategies'].append('Implement auto-scaling and capacity monitoring')
        
        if summary['risk_assessment']['cost_risks']:
            summary['risk_assessment']['mitigation_strategies'].append('Implement cost governance and optimization policies')
        
        metrics_discovery['finops_summary'] = summary
    
    async def _generate_recommendations(self, metrics_discovery: Dict[str, Any]) -> None:
        """Generate actionable recommendations based on all collected data."""
        recommendations = []
        
        finops_summary = metrics_discovery.get('finops_summary', {})
        optimization_opportunities = metrics_discovery.get('optimization_opportunities', [])
        
        # High priority recommendations based on summary
        overall_assessment = finops_summary.get('overall_assessment', {})
        cost_potential = overall_assessment.get('cost_optimization_potential', 'unknown')
        
        if cost_potential in ['very_high', 'high']:
            recommendations.append({
                'id': 'cost_optimization_priority',
                'category': 'cost_management',
                'priority': 'high',
                'title': 'Implement Immediate Cost Optimization Strategy',
                'description': 'Significant cost savings opportunity identified across multiple resource types',
                'business_impact': {
                    'cost_savings': 'high',
                    'operational_efficiency': 'high',
                    'risk_mitigation': 'medium'
                },
                'implementation': {
                    'complexity': 'medium',
                    'timeline': '4-8 weeks',
                    'resources_required': 'DevOps team, Finance team',
                    'dependencies': ['Resource utilization analysis', 'Workload assessment']
                },
                'action_items': [
                    'Conduct detailed rightsizing analysis for top resource consumers',
                    'Implement resource governance policies',
                    'Set up cost monitoring and alerting',
                    'Create optimization roadmap with quarterly targets'
                ],
                'success_metrics': [
                    'Reduce overall cloud costs by 20-40%',
                    'Improve resource utilization efficiency to >75%',
                    'Implement automated cost controls'
                ]
            })
        
        # Capacity planning recommendations
        operational_health = overall_assessment.get('operational_health', 'unknown')
        if operational_health == 'critical':
            recommendations.append({
                'id': 'capacity_expansion_urgent',
                'category': 'capacity_management',
                'priority': 'critical',
                'title': 'Urgent Capacity Expansion Required',
                'description': 'Current capacity levels are at critical thresholds requiring immediate action',
                'business_impact': {
                    'cost_savings': 'none',
                    'operational_efficiency': 'critical',
                    'risk_mitigation': 'high'
                },
                'implementation': {
                    'complexity': 'low',
                    'timeline': '1-2 weeks',
                    'resources_required': 'Infrastructure team',
                    'dependencies': ['Budget approval', 'Capacity planning review']
                },
                'action_items': [
                    'Immediately scale up critical resource bottlenecks',
                    'Implement automated scaling policies',
                    'Review and update capacity planning processes',
                    'Set up proactive capacity monitoring'
                ],
                'success_metrics': [
                    'Eliminate capacity-related performance issues',
                    'Implement predictive capacity planning',
                    'Reduce capacity-related incidents by 90%'
                ]
            })
        
        # Efficiency improvement recommendations
        efficiency_grade = overall_assessment.get('efficiency_grade', 'unknown')
        if efficiency_grade in ['D', 'F']:
            recommendations.append({
                'id': 'efficiency_improvement_program',
                'category': 'operational_efficiency',
                'priority': 'high',
                'title': 'Comprehensive Efficiency Improvement Program',
                'description': 'Resource efficiency is below acceptable levels requiring systematic improvement',
                'business_impact': {
                    'cost_savings': 'high',
                    'operational_efficiency': 'very_high',
                    'risk_mitigation': 'high'
                },
                'implementation': {
                    'complexity': 'high',
                    'timeline': '8-12 weeks',
                    'resources_required': 'DevOps team, Platform engineering, Application teams',
                    'dependencies': ['Application profiling', 'Workload analysis', 'Performance testing']
                },
                'action_items': [
                    'Conduct comprehensive workload analysis',
                    'Implement resource request and limit standards',
                    'Deploy efficiency monitoring and alerting',
                    'Create efficiency improvement training program'
                ],
                'success_metrics': [
                    'Achieve efficiency grade of B or better',
                    'Improve resource utilization to >70%',
                    'Establish efficiency governance processes'
                ]
            })
        
        # Add opportunity-specific recommendations
        for opportunity in optimization_opportunities[:3]:  # Top 3 opportunities
            if opportunity.get('priority') in ['critical', 'high']:
                recommendations.append({
                    'id': f"opportunity_{opportunity.get('category', 'general')}",
                    'category': opportunity.get('category', 'optimization'),
                    'priority': opportunity.get('priority', 'medium'),
                    'title': opportunity.get('title', 'Optimization Opportunity'),
                    'description': opportunity.get('description', ''),
                    'business_impact': opportunity.get('impact', {}),
                    'implementation': {
                        'complexity': opportunity.get('impact', {}).get('effort', 'medium'),
                        'timeline': opportunity.get('timeline', 'TBD'),
                        'estimated_savings': opportunity.get('estimated_savings', 'TBD')
                    },
                    'action_items': opportunity.get('recommended_actions', []),
                    'success_metrics': [f"Achieve {opportunity.get('estimated_savings', 'estimated savings')}"]
                })
        
        metrics_discovery['recommendations'] = recommendations
    
    def _get_empty_metrics_structure(self) -> Dict[str, Any]:
        """Return empty metrics structure when cluster resource ID is not provided."""
        return {
            'discovery_metadata': {
                'discovery_type': 'metrics_collection',
                'discovery_timestamp': datetime.now(timezone.utc).isoformat(),
                'discovery_status': 'skipped',
                'collection_errors': ['No cluster resource ID provided']
            },
            'cluster_overview': {},
            'resource_utilization': {},
            'capacity_planning': {},
            'cost_insights': {},
            'efficiency_metrics': {},
            'optimization_opportunities': [],
            'finops_summary': {
                'overall_assessment': {
                    'efficiency_grade': 'unknown',
                    'cost_optimization_potential': 'unknown',
                    'operational_health': 'unknown',
                    'sustainability_score': 0.0
                }
            },
            'recommendations': [{
                'id': 'enable_metrics_collection',
                'category': 'setup',
                'priority': 'high',
                'title': 'Enable Metrics Collection',
                'description': 'Configure cluster resource ID to enable comprehensive metrics collection',
                'action_items': [
                    'Provide cluster resource ID in configuration',
                    'Ensure Azure Monitor is enabled for the cluster',
                    'Verify Log Analytics workspace configuration'
                ]
            }]
        }
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "metrics_collection"