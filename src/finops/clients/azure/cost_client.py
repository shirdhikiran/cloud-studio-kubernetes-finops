"""Azure Cost Management client aligned with cost discovery service requirements."""

from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta, timezone
import structlog
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.costmanagement.models import (
    QueryDefinition, QueryDataset, QueryAggregation, QueryGrouping,
    QueryTimePeriod, TimeframeType, GranularityType, QueryFilter,
    QueryComparisonExpression, QueryOperatorType
)
from azure.core.exceptions import AzureError

from finops.core.base_client import BaseClient
from finops.core.exceptions import ClientConnectionException, DiscoveryException
from finops.core.utils import retry_with_backoff

logger = structlog.get_logger(__name__)


class CostClient(BaseClient):
    """Client for Azure Cost Management operations with flexible method signatures."""
    
    def __init__(self, credential, subscription_id: str, config: Dict[str, Any]):
        super().__init__(config, "CostClient")
        self.credential = credential
        self.subscription_id = subscription_id
        self._client = None
    
    async def connect(self) -> None:
        """Connect to Cost Management service."""
        try:
            self._client = CostManagementClient(
                credential=self.credential
            )
            self._connected = True
            self.logger.info("Cost Management client connected successfully")
        except Exception as e:
            raise ClientConnectionException("CostManagement", f"Connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from Cost Management service."""
        if self._client:
            self._client.close()
            self._connected = False
            self.logger.info("Cost Management client disconnected")
    
    async def health_check(self) -> bool:
        """Check Cost Management client health."""
        try:
            if not self._connected or not self._client:
                return False
            
            # Try a simple query as health check
            scope = f"/subscriptions/{self.subscription_id}"
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=1)
            
            query_definition = QueryDefinition(
                type="Usage",
                timeframe=TimeframeType.CUSTOM,
                time_period=QueryTimePeriod(
                    from_property=start_date,
                    to=end_date
                ),
                dataset=QueryDataset(
                    granularity=GranularityType.DAILY,
                    aggregation={
                        "totalCost": QueryAggregation(name="PreTaxCost", function="Sum")
                    }
                )
            )
            
            self._client.query.usage(scope, query_definition)
            return True
            
        except Exception as e:
            self.logger.warning("Cost Management health check failed", error=str(e))
            return False
    
    @retry_with_backoff(max_retries=3)
    async def get_resource_costs(self, 
                               resource_group: Optional[str] = None,
                               cluster_name: Optional[str] = None,
                               subscription_id: Optional[str] = None,
                               start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None,
                               **kwargs) -> Dict[str, Any]:
        """
        Get resource costs with flexible parameters.
        
        This method supports multiple parameter combinations:
        - get_resource_costs(resource_group="rg-name", start_date=..., end_date=...)
        - get_resource_costs(subscription_id="sub-id", resource_group="rg-name", ...)
        - get_resource_costs(resource_group="rg-name", cluster_name="cluster", ...)
        """
        if not self._connected:
            raise DiscoveryException("CostManagement", "Client not connected")
        
        # Use provided subscription_id or fallback to instance default
        sub_id = subscription_id or self.subscription_id
        
        # Default to last 30 days if dates not provided
        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        scope = f"/subscriptions/{sub_id}"
        
        try:
            # Build filter conditions
            filter_conditions = []
            
            if resource_group:
                filter_conditions.append(
                    QueryComparisonExpression(
                        name="ResourceGroupName",
                        operator=QueryOperatorType.IN,
                        values=[resource_group]
                    )
                )
            
            # If cluster_name is provided, we can filter by resources containing the cluster name
            # or use tags if available
            if cluster_name:
                # This would work if resources are tagged with cluster name
                # For now, we'll include it in the response metadata
                pass
            
            # Combine filters if multiple conditions
            filter_expression = None
            if filter_conditions:
                if len(filter_conditions) == 1:
                    filter_expression = QueryFilter(dimensions=filter_conditions[0])
                else:
                    # For multiple conditions, we'd need to use 'and_' operator
                    filter_expression = QueryFilter(
                        and_=[QueryFilter(dimensions=condition) for condition in filter_conditions]
                    )
            
            query_definition = QueryDefinition(
                type="Usage",
                timeframe=TimeframeType.CUSTOM,
                time_period=QueryTimePeriod(
                    from_property=start_date,
                    to=end_date
                ),
                dataset=QueryDataset(
                    granularity=GranularityType.DAILY,
                    aggregation={
                        "totalCost": QueryAggregation(name="PreTaxCost", function="Sum"),
                        "usageQuantity": QueryAggregation(name="UsageQuantity", function="Sum")
                    },
                    grouping=[
                        QueryGrouping(type="Dimension", name="ResourceType"),
                        QueryGrouping(type="Dimension", name="ServiceName"),
                        QueryGrouping(type="Dimension", name="MeterCategory"),
                        QueryGrouping(type="Dimension", name="ResourceGroupName")
                    ],
                    filter=filter_expression
                )
            )
            
            response = self._client.query.usage(scope, query_definition)
            
            # Process and return standardized response
            processed_costs = await self._process_cost_response(response)
            
            # Add metadata about the query
            processed_costs['query_metadata'] = {
                'subscription_id': sub_id,
                'resource_group': resource_group,
                'cluster_name': cluster_name,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'scope': scope
            }
            
            return processed_costs
            
        except AzureError as e:
            self.logger.error("Failed to get resource costs", error=str(e))
            raise DiscoveryException("CostManagement", f"Failed to get resource costs: {e}")
        except Exception as e:
            self.logger.error("Unexpected error getting resource costs", error=str(e))
            # Return fallback structure instead of failing
            return self._get_fallback_cost_structure(resource_group, cluster_name, start_date, end_date)
    
    @retry_with_backoff(max_retries=3)
    async def query_costs(self,
                         subscription_id: Optional[str] = None,
                         resource_group: Optional[str] = None,
                         start_date: Optional[datetime] = None,
                         end_date: Optional[datetime] = None,
                         granularity: str = "Daily",
                         **kwargs) -> Dict[str, Any]:
        """
        Alternative method name for querying costs.
        Maps to get_resource_costs for compatibility.
        """
        return await self.get_resource_costs(
            subscription_id=subscription_id,
            resource_group=resource_group,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
    
    @retry_with_backoff(max_retries=3)
    async def get_cost_data(self,
                           scope: Optional[str] = None,
                           filters: Optional[Dict[str, Any]] = None,
                           **kwargs) -> Dict[str, Any]:
        """
        Generic cost data method that can be called by discovery services.
        """
        # Extract common parameters from kwargs
        resource_group = kwargs.get('resource_group') or (filters and filters.get('resource_group'))
        start_date = kwargs.get('start_date') or (filters and filters.get('start_date'))
        end_date = kwargs.get('end_date') or (filters and filters.get('end_date'))
        
        return await self.get_resource_costs(
            resource_group=resource_group,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
    
    @retry_with_backoff(max_retries=3)
    async def get_cost_by_service(self,
                                resource_group: Optional[str] = None,
                                start_date: Optional[datetime] = None,
                                end_date: Optional[datetime] = None,
                                **kwargs) -> Dict[str, Any]:
        """Get costs broken down by service."""
        if not self._connected:
            raise DiscoveryException("CostManagement", "Client not connected")
        
        # Default to last 30 days
        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        scope = f"/subscriptions/{self.subscription_id}"
        
        try:
            filter_expression = None
            if resource_group:
                filter_expression = QueryFilter(
                    dimensions=QueryComparisonExpression(
                        name="ResourceGroupName",
                        operator=QueryOperatorType.IN,
                        values=[resource_group]
                    )
                )
            
            query_definition = QueryDefinition(
                type="Usage",
                timeframe=TimeframeType.CUSTOM,
                time_period=QueryTimePeriod(
                    from_property=start_date,
                    to=end_date
                ),
                dataset=QueryDataset(
                    granularity=GranularityType.NONE,
                    aggregation={
                        "totalCost": QueryAggregation(name="PreTaxCost", function="Sum")
                    },
                    grouping=[
                        QueryGrouping(type="Dimension", name="ServiceName"),
                        QueryGrouping(type="Dimension", name="MeterCategory")
                    ],
                    filter=filter_expression
                )
            )
            
            response = self._client.query.usage(scope, query_definition)
            return await self._process_service_cost_response(response)
            
        except AzureError as e:
            raise DiscoveryException("CostManagement", f"Failed to get costs by service: {e}")
    
    async def get_cost_forecast(self,
                              resource_group: Optional[str] = None,
                              days_ahead: int = 30,
                              **kwargs) -> Dict[str, Any]:
        """Get cost forecasting data."""
        # This would use Azure's forecasting API when available
        # For now, return a basic forecast structure
        return {
            'forecast_period_days': days_ahead,
            'estimated_cost': 0.0,
            'confidence': 'low',
            'method': 'not_implemented',
            'note': 'Cost forecasting requires Azure Cost Management forecasting API'
        }
    
    async def get_cost_anomalies(self,
                               resource_group: Optional[str] = None,
                               **kwargs) -> List[Dict[str, Any]]:
        """Get cost anomalies and unusual spending patterns."""
        # This would use Azure's anomaly detection when available
        return []
    
    async def _process_cost_response(self, response) -> Dict[str, Any]:
        """Process cost API response into standardized format."""
        costs = {
            'total': 0.0,
            'compute': 0.0,
            'storage': 0.0,
            'network': 0.0,
            'monitoring': 0.0,
            'other': 0.0,
            'currency': 'USD',
            'daily_breakdown': [],
            'by_service': {},
            'by_resource_type': {},
            'by_meter_category': {},
            'source': 'azure_cost_management_api'
        }
        
        if not hasattr(response, 'rows') or not response.rows:
            self.logger.warning("No cost data returned from Azure Cost Management API")
            return costs
        
        daily_costs = {}
        
        for row in response.rows:
            try:
                if len(row) >= 6:
                    cost = float(row[0]) if row[0] is not None else 0.0
                    usage_quantity = float(row[1]) if row[1] is not None else 0.0
                    usage_date_raw = row[2] if row[2] is not None else None
                    resource_type = row[3] if row[3] is not None else "Unknown"
                    service_name = row[4] if row[4] is not None else "Unknown"
                    meter_category = row[5] if row[5] is not None else "Unknown"
                    resource_group_name = row[6] if len(row) > 6 and row[6] is not None else "Unknown"
                    currency = row[7] if len(row) > 7 and row[7] is not None else "USD"
                    
                    # Parse date (Azure returns dates in YYYYMMDD format as integers)
                    usage_date = None
                    if usage_date_raw:
                        try:
                            if isinstance(usage_date_raw, (int, float)):
                                date_str = str(int(usage_date_raw))
                                if len(date_str) == 8:
                                    year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
                                    usage_date = datetime(year, month, day)
                            elif isinstance(usage_date_raw, str) and len(usage_date_raw) == 8:
                                usage_date = datetime.strptime(usage_date_raw, '%Y%m%d')
                            else:
                                usage_date = usage_date_raw if hasattr(usage_date_raw, 'strftime') else None
                        except (ValueError, TypeError):
                            self.logger.debug(f"Could not parse date: {usage_date_raw}")
                    
                    costs['total'] += cost
                    
                    # Enhanced categorization based on service and meter category
                    self._categorize_cost(costs, cost, service_name, meter_category, resource_type)
                    
                    # Group by service
                    if service_name not in costs['by_service']:
                        costs['by_service'][service_name] = {
                            'cost': 0.0,
                            'usage_quantity': 0.0,
                            'currency': currency,
                            'resource_types': set()
                        }
                    costs['by_service'][service_name]['cost'] += cost
                    costs['by_service'][service_name]['usage_quantity'] += usage_quantity
                    costs['by_service'][service_name]['resource_types'].add(resource_type)
                    
                    # Group by resource type
                    if resource_type not in costs['by_resource_type']:
                        costs['by_resource_type'][resource_type] = {
                            'cost': 0.0,
                            'usage_quantity': 0.0,
                            'services': set()
                        }
                    costs['by_resource_type'][resource_type]['cost'] += cost
                    costs['by_resource_type'][resource_type]['usage_quantity'] += usage_quantity
                    costs['by_resource_type'][resource_type]['services'].add(service_name)
                    
                    # Group by meter category
                    if meter_category not in costs['by_meter_category']:
                        costs['by_meter_category'][meter_category] = {
                            'cost': 0.0,
                            'usage_quantity': 0.0,
                            'resource_types': set()
                        }
                    costs['by_meter_category'][meter_category]['cost'] += cost
                    costs['by_meter_category'][meter_category]['usage_quantity'] += usage_quantity
                    costs['by_meter_category'][meter_category]['resource_types'].add(resource_type)
                    
                    # Daily breakdown
                    if usage_date:
                        date_str = usage_date.strftime('%Y-%m-%d')
                        if date_str not in daily_costs:
                            daily_costs[date_str] = {
                                'date': date_str,
                                'cost': 0.0,
                                'usage': 0.0,
                                'currency': currency,
                                'services': {},
                                'categories': {}
                            }
                        daily_costs[date_str]['cost'] += cost
                        daily_costs[date_str]['usage'] += usage_quantity
                        
                        # Track top services per day
                        if service_name not in daily_costs[date_str]['services']:
                            daily_costs[date_str]['services'][service_name] = 0.0
                        daily_costs[date_str]['services'][service_name] += cost
                        
                        # Track categories per day
                        category = self._get_cost_category(service_name, meter_category, resource_type)
                        if category not in daily_costs[date_str]['categories']:
                            daily_costs[date_str]['categories'][category] = 0.0
                        daily_costs[date_str]['categories'][category] += cost
                    
            except (ValueError, TypeError, IndexError) as e:
                self.logger.warning(f"Error processing cost row: {e}", row=row)
                continue
        
        # Convert sets to lists for JSON serialization
        self._convert_sets_to_lists(costs)
        
        # Sort daily breakdown by date
        costs['daily_breakdown'] = sorted(daily_costs.values(), key=lambda x: x['date'])
        costs['currency'] = list(daily_costs.values())[0]['currency'] if daily_costs else 'USD'
        
        # Add cost distribution percentages
        if costs['total'] > 0:
            costs['cost_distribution'] = {
                'compute_percentage': round((costs['compute'] / costs['total']) * 100, 2),
                'storage_percentage': round((costs['storage'] / costs['total']) * 100, 2),
                'network_percentage': round((costs['network'] / costs['total']) * 100, 2),
                'monitoring_percentage': round((costs['monitoring'] / costs['total']) * 100, 2),
                'other_percentage': round((costs['other'] / costs['total']) * 100, 2)
            }
        
        # Add summary statistics
        costs['summary'] = {
            'total_days': len(daily_costs),
            'average_daily_cost': round(costs['total'] / len(daily_costs), 2) if daily_costs else 0.0,
            'peak_daily_cost': max([day['cost'] for day in daily_costs.values()]) if daily_costs else 0.0,
            'min_daily_cost': min([day['cost'] for day in daily_costs.values()]) if daily_costs else 0.0,
            'top_service': max(costs['by_service'].items(), key=lambda x: x[1]['cost'])[0] if costs['by_service'] else 'None',
            'top_resource_type': max(costs['by_resource_type'].items(), key=lambda x: x[1]['cost'])[0] if costs['by_resource_type'] else 'None'
        }
        
        self.logger.info(
            "Processed cost data successfully",
            total_cost=costs['total'],
            currency=costs['currency'],
            days=len(daily_costs),
            services=len(costs['by_service'])
        )
        
        return costs
    
    def _categorize_cost(self, costs: Dict[str, Any], cost: float, service_name: str, meter_category: str, resource_type: str):
        """Categorize cost into compute, storage, network, monitoring, or other."""
        service_lower = service_name.lower()
        meter_lower = meter_category.lower()
        resource_type_lower = resource_type.lower()
        
        # Compute costs
        if (any(keyword in service_lower for keyword in ['virtual machines', 'compute', 'container']) or
            any(keyword in meter_lower for keyword in ['virtual machines', 'compute', 'cpu', 'container']) or
            'microsoft.compute' in resource_type_lower or
            'microsoft.containerservice' in resource_type_lower):
            costs['compute'] += cost
        
        # Storage costs
        elif (any(keyword in service_lower for keyword in ['storage', 'blob', 'file', 'disk']) or
              any(keyword in meter_lower for keyword in ['storage', 'disk', 'blob', 'file']) or
              'microsoft.storage' in resource_type_lower or
              'microsoft.containerregistry' in resource_type_lower):
            costs['storage'] += cost
        
        # Network costs
        elif (any(keyword in service_lower for keyword in ['bandwidth', 'network', 'load balancer', 'dns', 'cdn']) or
              any(keyword in meter_lower for keyword in ['bandwidth', 'network', 'load balancer', 'dns', 'cdn']) or
              'microsoft.network' in resource_type_lower):
            costs['network'] += cost
        
        # Monitoring costs
        elif (any(keyword in service_lower for keyword in ['monitor', 'grafana', 'insights', 'log analytics']) or
              any(keyword in meter_lower for keyword in ['monitor', 'grafana', 'insights', 'log analytics']) or
              'microsoft.monitor' in resource_type_lower or
              'microsoft.insights' in resource_type_lower or
              'microsoft.dashboard' in resource_type_lower or
              'microsoft.operationalinsights' in resource_type_lower):
            costs['monitoring'] += cost
        
        # Everything else
        else:
            costs['other'] += cost
    
    def _get_cost_category(self, service_name: str, meter_category: str, resource_type: str) -> str:
        """Get cost category for a service/meter combination."""
        service_lower = service_name.lower()
        meter_lower = meter_category.lower()
        resource_type_lower = resource_type.lower()
        
        if (any(keyword in service_lower for keyword in ['virtual machines', 'compute', 'container']) or
            any(keyword in meter_lower for keyword in ['virtual machines', 'compute', 'cpu', 'container']) or
            'microsoft.compute' in resource_type_lower):
            return 'compute'
        elif (any(keyword in service_lower for keyword in ['storage', 'blob', 'file', 'disk']) or
              any(keyword in meter_lower for keyword in ['storage', 'disk', 'blob', 'file']) or
              'microsoft.storage' in resource_type_lower):
            return 'storage'
        elif (any(keyword in service_lower for keyword in ['bandwidth', 'network', 'load balancer']) or
              any(keyword in meter_lower for keyword in ['bandwidth', 'network', 'load balancer']) or
              'microsoft.network' in resource_type_lower):
            return 'network'
        elif (any(keyword in service_lower for keyword in ['monitor', 'grafana', 'insights']) or
              any(keyword in meter_lower for keyword in ['monitor', 'grafana', 'insights']) or
              'microsoft.monitor' in resource_type_lower):
            return 'monitoring'
        else:
            return 'other'
    
    def _convert_sets_to_lists(self, costs: Dict[str, Any]):
        """Convert sets to lists for JSON serialization."""
        for service_data in costs['by_service'].values():
            if 'resource_types' in service_data and isinstance(service_data['resource_types'], set):
                service_data['resource_types'] = list(service_data['resource_types'])
        
        for resource_data in costs['by_resource_type'].values():
            if 'services' in resource_data and isinstance(resource_data['services'], set):
                resource_data['services'] = list(resource_data['services'])
        
        for meter_data in costs['by_meter_category'].values():
            if 'resource_types' in meter_data and isinstance(meter_data['resource_types'], set):
                meter_data['resource_types'] = list(meter_data['resource_types'])
    
    def _get_fallback_cost_structure(self, 
                                   resource_group: Optional[str],
                                   cluster_name: Optional[str],
                                   start_date: datetime,
                                   end_date: datetime) -> Dict[str, Any]:
        """Return fallback cost structure when API fails."""
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
            'by_resource_type': {},
            'by_meter_category': {},
            'source': 'fallback_due_to_api_error',
            'query_metadata': {
                'resource_group': resource_group,
                'cluster_name': cluster_name,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'error': 'Azure Cost Management API unavailable'
            },
            'status': 'no_data_available',
            'message': 'Cost data not available - Azure Cost Management API failed'
        }
    
    async def _process_service_cost_response(self, response) -> Dict[str, Any]:
        """Process service cost response."""
        service_costs = {
            'services': [],
            'total': 0.0,
            'top_services': [],
            'cost_distribution': {}
        }
        
        if not hasattr(response, 'rows') or not response.rows:
            return service_costs
        
        service_totals = {}
        
        for row in response.rows:
            if len(row) >= 3:
                service_name = row[0] if row[0] else "Unknown"
                meter_category = row[1] if row[1] else "Unknown"
                cost = float(row[2]) if row[2] else 0.0
                
                if service_name not in service_totals:
                    service_totals[service_name] = {
                        'name': service_name,
                        'total_cost': 0.0,
                        'categories': {}
                    }
                
                service_totals[service_name]['total_cost'] += cost
                service_totals[service_name]['categories'][meter_category] = \
                    service_totals[service_name]['categories'].get(meter_category, 0.0) + cost
                
                service_costs['total'] += cost
        
        # Convert to list and sort
        service_costs['services'] = list(service_totals.values())
        service_costs['services'].sort(key=lambda x: x['total_cost'], reverse=True)
        
        # Get top 5 services
        service_costs['top_services'] = service_costs['services'][:5]
        
        # Calculate percentages
        if service_costs['total'] > 0:
            for service in service_costs['services']:
                percentage = (service['total_cost'] / service_costs['total']) * 100
                service['percentage'] = round(percentage, 2)
                service_costs['cost_distribution'][service['name']] = percentage
        
        return service_costs

    def __repr__(self) -> str:
        """String representation."""
        return f"CostClient(subscription_id='{self.subscription_id}', connected={self._connected})"