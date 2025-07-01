"""Simplified cost discovery service that works with existing infrastructure."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
import structlog

from finops.discovery.base import BaseDiscoveryService

logger = structlog.get_logger(__name__)


class CostDiscoveryService(BaseDiscoveryService):
    """Simplified cost discovery service that doesn't assume specific client methods."""
    
    def __init__(self, client, config: Dict[str, Any]):
        super().__init__(client, config)
        self.subscription_id = config.get("subscription_id")
        self.resource_group = config.get("resource_group")
        self.cluster_name = config.get("cluster_name")
        self.time_range = timedelta(days=config.get("cost_analysis_days", 30))
    
    async def discover(self) -> Dict[str, Any]:
        """Discover basic cost data for the cluster."""
        self.logger.info("Starting simplified cost discovery")
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - self.time_range
        
        cost_data = {
            'discovery_timestamp': datetime.now(timezone.utc).isoformat(),
            'analysis_period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'days': self.time_range.days
            },
            'cluster_costs': await self._get_basic_cluster_costs(start_date, end_date),
            'cost_estimates': self._generate_cost_estimates(),
            'optimization_opportunities': self._generate_basic_optimization_opportunities(),
            'discovery_status': 'completed',
            'limitations': [
                'Cost data requires Azure Cost Management API integration',
                'Using estimated costs based on resource discovery',
                'Actual costs may vary based on Azure pricing and usage'
            ]
        }
        
        # Calculate summary metrics
        cost_data['summary'] = self._calculate_basic_cost_summary(cost_data)
        
        self.logger.info(
            f"Completed simplified cost discovery",
            estimated_monthly_cost=cost_data['summary'].get('estimated_monthly_cost', 0),
            analysis_days=self.time_range.days
        )
        
        return cost_data
    
    async def _get_basic_cluster_costs(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Generate basic cluster cost structure with estimates."""
        try:
            # Check if client has cost methods available
            has_cost_methods = (
                hasattr(self.client, 'get_resource_costs') or
                hasattr(self.client, 'query_costs') or
                hasattr(self.client, 'get_cost_data')
            )
            
            if has_cost_methods:
                # Try to get actual cost data if client supports it
                try:
                    # Try different possible method signatures
                    if hasattr(self.client, 'get_resource_costs'):
                        cost_response = await self._try_get_resource_costs(start_date, end_date)
                        if cost_response:
                            return cost_response
                    elif hasattr(self.client, 'query_costs'):
                        cost_response = await self.client.query_costs(
                            subscription_id=self.subscription_id,
                            resource_group=self.resource_group,
                            start_date=start_date,
                            end_date=end_date
                        )
                        if cost_response:
                            return self._normalize_cost_response(cost_response)
                except Exception as e:
                    self.logger.warning(f"Failed to get actual cost data: {e}")
            
            # Generate estimated costs based on typical Azure pricing
            return self._generate_estimated_costs(start_date, end_date)
            
        except Exception as e:
            self.logger.error("Failed to get cluster costs", error=str(e))
            return self._generate_default_cost_structure()
    
    async def _try_get_resource_costs(self, start_date: datetime, end_date: datetime) -> Optional[Dict[str, Any]]:
        """Try different method signatures for getting resource costs."""
        try:
            # Try with different parameter combinations
            method_variants = [
                # Variant 1: Basic parameters
                {
                    'resource_group': self.resource_group,
                    'start_date': start_date,
                    'end_date': end_date
                },
                # Variant 2: With subscription
                {
                    'subscription_id': self.subscription_id,
                    'resource_group': self.resource_group,
                    'start_date': start_date,
                    'end_date': end_date
                },
                # Variant 3: With cluster name
                {
                    'resource_group': self.resource_group,
                    'cluster_name': self.cluster_name,
                    'start_date': start_date,
                    'end_date': end_date
                }
            ]
            
            for params in method_variants:
                try:
                    cost_response = await self.client.get_resource_costs(**params)
                    if cost_response:
                        return cost_response
                except TypeError:
                    # Method signature doesn't match, try next variant
                    continue
                except Exception as e:
                    self.logger.debug(f"Cost method variant failed: {e}")
                    continue
            
            return None
            
        except Exception as e:
            self.logger.debug(f"All cost method variants failed: {e}")
            return None
    
    def _normalize_cost_response(self, cost_response: Any) -> Dict[str, Any]:
        """Normalize cost response to standard format."""
        if isinstance(cost_response, dict):
            return cost_response
        
        # Try to extract cost data from response object
        normalized = {
            'total': 0.0,
            'compute': 0.0,
            'storage': 0.0,
            'network': 0.0,
            'other': 0.0,
            'currency': 'USD',
            'daily_breakdown': [],
            'by_service': {},
            'source': 'client_response'
        }
        
        # Extract total cost if available
        if hasattr(cost_response, 'total_cost'):
            normalized['total'] = float(cost_response.total_cost)
        elif hasattr(cost_response, 'cost'):
            normalized['total'] = float(cost_response.cost)
        
        return normalized
    
    def _generate_estimated_costs(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Generate estimated costs based on typical Azure pricing."""
        days = (end_date - start_date).days or 1
        
        # Basic estimates based on typical AKS pricing (these are rough estimates)
        estimated_costs = {
            'total': 0.0,
            'compute': 0.0,
            'storage': 0.0,
            'network': 0.0,
            'monitoring': 0.0,
            'other': 0.0,
            'currency': 'USD',
            'daily_breakdown': [],
            'by_service': {
                'Virtual Machines': {'cost': 0.0, 'estimated': True},
                'Storage': {'cost': 0.0, 'estimated': True},
                'Networking': {'cost': 0.0, 'estimated': True},
                'Azure Monitor': {'cost': 0.0, 'estimated': True}
            },
            'estimation_method': 'based_on_typical_azure_pricing',
            'limitations': [
                'These are rough estimates based on typical Azure pricing',
                'Actual costs depend on specific VM sizes, storage types, and usage patterns',
                'Enable Azure Cost Management API for accurate cost data'
            ]
        }
        
        # Estimate daily costs (very basic estimates)
        base_daily_compute = 50.0  # Rough estimate for small cluster
        base_daily_storage = 10.0  # Rough estimate for storage
        base_daily_network = 5.0   # Rough estimate for networking
        base_daily_monitoring = 2.0  # Rough estimate for monitoring
        
        for i in range(days):
            day_date = start_date + timedelta(days=i)
            daily_cost = {
                'date': day_date.strftime('%Y-%m-%d'),
                'cost': base_daily_compute + base_daily_storage + base_daily_network + base_daily_monitoring,
                'compute': base_daily_compute,
                'storage': base_daily_storage,
                'network': base_daily_network,
                'monitoring': base_daily_monitoring,
                'estimated': True
            }
            estimated_costs['daily_breakdown'].append(daily_cost)
        
        # Calculate totals
        estimated_costs['compute'] = base_daily_compute * days
        estimated_costs['storage'] = base_daily_storage * days
        estimated_costs['network'] = base_daily_network * days
        estimated_costs['monitoring'] = base_daily_monitoring * days
        estimated_costs['total'] = (
            estimated_costs['compute'] + 
            estimated_costs['storage'] + 
            estimated_costs['network'] + 
            estimated_costs['monitoring']
        )
        
        # Update service costs
        estimated_costs['by_service']['Virtual Machines']['cost'] = estimated_costs['compute']
        estimated_costs['by_service']['Storage']['cost'] = estimated_costs['storage']
        estimated_costs['by_service']['Networking']['cost'] = estimated_costs['network']
        estimated_costs['by_service']['Azure Monitor']['cost'] = estimated_costs['monitoring']
        
        return estimated_costs
    
    def _generate_default_cost_structure(self) -> Dict[str, Any]:
        """Generate default cost structure when no data is available."""
        return {
            'total': 0.0,
            'compute': 0.0,
            'storage': 0.0,
            'network': 0.0,
            'monitoring': 0.0,
            'other': 0.0,
            'currency': 'USD',
            'daily_breakdown': [],
            'by_service': {},
            'status': 'no_data_available',
            'message': 'Cost data not available - requires Azure Cost Management API integration'
        }
    
    def _generate_cost_estimates(self) -> Dict[str, Any]:
        """Generate cost estimates and projections."""
        return {
            'monthly_projection': {
                'estimated_cost': 2000.0,  # Rough estimate
                'confidence': 'low',
                'method': 'industry_averages',
                'note': 'Based on typical small-to-medium AKS cluster costs'
            },
            'cost_drivers': [
                {
                    'category': 'Compute',
                    'description': 'Virtual machine instances for cluster nodes',
                    'estimated_percentage': 70,
                    'optimization_potential': 'high'
                },
                {
                    'category': 'Storage',
                    'description': 'Persistent volumes and managed disks',
                    'estimated_percentage': 20,
                    'optimization_potential': 'medium'
                },
                {
                    'category': 'Networking',
                    'description': 'Load balancers, public IPs, and data transfer',
                    'estimated_percentage': 8,
                    'optimization_potential': 'low'
                },
                {
                    'category': 'Monitoring',
                    'description': 'Azure Monitor, Log Analytics, and metrics',
                    'estimated_percentage': 2,
                    'optimization_potential': 'low'
                }
            ],
            'factors_affecting_cost': [
                'Number and size of cluster nodes',
                'Storage type and size requirements',
                'Network traffic and load balancer usage',
                'Monitoring and logging retention policies',
                'Auto-scaling configuration',
                'Resource utilization efficiency'
            ]
        }
    
    def _generate_basic_optimization_opportunities(self) -> List[Dict[str, Any]]:
        """Generate basic optimization opportunities."""
        return [
            {
                'type': 'right_sizing',
                'title': 'Review Node Pool Sizing',
                'description': 'Analyze if current VM sizes match actual resource usage',
                'potential_savings': 'Up to 30%',
                'effort': 'medium',
                'risk': 'low',
                'recommendation': 'Monitor CPU and memory usage to identify over-provisioned nodes'
            },
            {
                'type': 'auto_scaling',
                'title': 'Enable Cluster Auto-scaling',
                'description': 'Automatically scale nodes based on workload demand',
                'potential_savings': 'Up to 20%',
                'effort': 'low',
                'risk': 'low',
                'recommendation': 'Configure horizontal pod autoscaler and cluster autoscaler'
            },
            {
                'type': 'spot_instances',
                'title': 'Use Spot Virtual Machines',
                'description': 'Use Azure Spot VMs for non-critical workloads',
                'potential_savings': 'Up to 70%',
                'effort': 'medium',
                'risk': 'medium',
                'recommendation': 'Identify fault-tolerant workloads suitable for spot instances'
            },
            {
                'type': 'storage_optimization',
                'title': 'Optimize Storage Classes',
                'description': 'Review storage types and implement lifecycle policies',
                'potential_savings': 'Up to 25%',
                'effort': 'low',
                'risk': 'low',
                'recommendation': 'Use Standard SSD instead of Premium SSD where appropriate'
            },
            {
                'type': 'resource_limits',
                'title': 'Set Resource Requests and Limits',
                'description': 'Properly configure CPU and memory requests/limits for pods',
                'potential_savings': 'Up to 15%',
                'effort': 'high',
                'risk': 'medium',
                'recommendation': 'Analyze actual usage and set appropriate resource constraints'
            }
        ]
    
    def _calculate_basic_cost_summary(self, cost_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate basic cost summary metrics."""
        cluster_costs = cost_data.get('cluster_costs', {})
        
        total_cost = cluster_costs.get('total', 0.0)
        days = self.time_range.days or 1
        
        summary = {
            'total_cost': round(total_cost, 2),
            'cost_per_day': round(total_cost / days, 2),
            'estimated_monthly_cost': round((total_cost / days) * 30, 2),
            'currency': cluster_costs.get('currency', 'USD'),
            'analysis_period_days': days,
            'cost_breakdown': {
                'compute_percentage': round((cluster_costs.get('compute', 0) / total_cost * 100), 2) if total_cost > 0 else 0,
                'storage_percentage': round((cluster_costs.get('storage', 0) / total_cost * 100), 2) if total_cost > 0 else 0,
                'network_percentage': round((cluster_costs.get('network', 0) / total_cost * 100), 2) if total_cost > 0 else 0,
                'monitoring_percentage': round((cluster_costs.get('monitoring', 0) / total_cost * 100), 2) if total_cost > 0 else 0
            },
            'optimization_potential': {
                'total_opportunities': len(cost_data.get('optimization_opportunities', [])),
                'estimated_savings_range': '15-40%',
                'priority_actions': [
                    'Enable auto-scaling',
                    'Review node sizes',
                    'Set resource limits'
                ]
            },
            'data_quality': {
                'source': cluster_costs.get('estimation_method', 'unknown'),
                'confidence': 'low' if 'estimated' in str(cluster_costs) else 'medium',
                'limitations': cluster_costs.get('limitations', [])
            }
        }
        
        return summary
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "cost_analysis"
    
    async def discover_with_resource_context(self, resource_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced discovery that uses resource data for better cost estimates."""
        cost_data = await self.discover()
        
        if resource_data:
            # Enhance cost estimates with actual resource data
            enhanced_estimates = self._enhance_cost_estimates_with_resources(resource_data)
            cost_data['enhanced_estimates'] = enhanced_estimates
            
            # Add resource-based optimization opportunities
            resource_opportunities = self._generate_resource_based_opportunities(resource_data)
            cost_data['optimization_opportunities'].extend(resource_opportunities)
        
        return cost_data
    
    def _enhance_cost_estimates_with_resources(self, resource_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance cost estimates using actual resource discovery data."""
        nodes = resource_data.get('nodes', [])
        pods = resource_data.get('pods', [])
        pvs = resource_data.get('persistent_volumes', [])
        
        enhanced = {
            'node_based_estimates': {
                'total_nodes': len(nodes),
                'estimated_compute_cost_per_month': len(nodes) * 100,  # Rough estimate
                'node_types': {}
            },
            'storage_based_estimates': {
                'total_pvs': len(pvs),
                'estimated_storage_cost_per_month': len(pvs) * 20,  # Rough estimate
                'storage_classes': {}
            },
            'workload_based_estimates': {
                'total_pods': len(pods),
                'pods_per_node': len(pods) / len(nodes) if nodes else 0,
                'resource_efficiency': 'medium'  # Would need actual metrics
            }
        }
        
        # Analyze node types
        node_types = {}
        for node in nodes:
            instance_type = node.get('instance_type', 'Unknown')
            if instance_type not in node_types:
                node_types[instance_type] = 0
            node_types[instance_type] += 1
        
        enhanced['node_based_estimates']['node_types'] = node_types
        
        # Analyze storage classes
        storage_classes = {}
        for pv in pvs:
            storage_class = pv.get('storage_class', 'Unknown')
            if storage_class not in storage_classes:
                storage_classes[storage_class] = {'count': 0, 'total_capacity': 0}
            storage_classes[storage_class]['count'] += 1
            # Would need to parse capacity from storage size
        
        enhanced['storage_based_estimates']['storage_classes'] = storage_classes
        
        return enhanced
    
    def _generate_resource_based_opportunities(self, resource_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate optimization opportunities based on actual resource data."""
        opportunities = []
        
        pods = resource_data.get('pods', [])
        deployments = resource_data.get('deployments', [])
        
        # Check for pods without resource limits
        pods_without_limits = [
            pod for pod in pods 
            if not any(
                container.get('resources', {}).get('limits') 
                for container in pod.get('containers', [])
            )
        ]
        
        if pods_without_limits:
            opportunities.append({
                'type': 'resource_governance',
                'title': f'{len(pods_without_limits)} Pods Without Resource Limits',
                'description': 'Pods without resource limits can cause resource contention',
                'potential_savings': 'Up to 20%',
                'effort': 'medium',
                'risk': 'low',
                'recommendation': 'Set CPU and memory limits for all production pods',
                'affected_resources': len(pods_without_limits)
            })
        
        # Check for over-replicated deployments
        over_replicated = [
            dep for dep in deployments 
            if dep.get('replicas', 0) > 5 and dep.get('ready_replicas', 0) == dep.get('replicas', 0)
        ]
        
        if over_replicated:
            opportunities.append({
                'type': 'scaling_optimization',
                'title': f'{len(over_replicated)} Potentially Over-Replicated Deployments',
                'description': 'Deployments with high replica counts may be over-provisioned',
                'potential_savings': 'Up to 30%',
                'effort': 'medium',
                'risk': 'medium',
                'recommendation': 'Review if high replica counts are necessary for these deployments',
                'affected_resources': len(over_replicated)
            })
        
        return opportunities