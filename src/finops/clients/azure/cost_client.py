"""Enhanced Azure Cost Management client with proper resource group cost isolation."""

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
    """Enhanced client for Azure Cost Management with proper resource group isolation."""
    
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
    async def get_resource_group_costs_isolated(self, 
                                              resource_group: str,
                                              start_date: Optional[datetime] = None,
                                              end_date: Optional[datetime] = None,
                                              granularity: GranularityType = GranularityType.DAILY) -> Dict[str, Any]:
        """
        Get completely isolated costs for a specific resource group only.
        This ensures no cross-contamination from other resource groups.
        """
        if not self._connected:
            raise DiscoveryException("CostManagement", "Client not connected")
        
        # Default to last 30 days if dates not provided
        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        # Use resource group scope for complete isolation
        scope = f"/subscriptions/{self.subscription_id}/resourceGroups/{resource_group}"
        
        try:
            # Query with strict resource group filtering
            query_definition = QueryDefinition(
                type="Usage",
                timeframe=TimeframeType.CUSTOM,
                time_period=QueryTimePeriod(
                    from_property=start_date,
                    to=end_date
                ),
                dataset=QueryDataset(
                    granularity=granularity,
                    aggregation={
                        "totalCost": QueryAggregation(name="PreTaxCost", function="Sum"),
                        "usageQuantity": QueryAggregation(name="UsageQuantity", function="Sum")
                    },
                    grouping=[
                        QueryGrouping(type="Dimension", name="ResourceType"),
                        QueryGrouping(type="Dimension", name="ServiceName"),
                        QueryGrouping(type="Dimension", name="MeterCategory"),
                        QueryGrouping(type="Dimension", name="ResourceGroupName"),
                        QueryGrouping(type="Dimension", name="ResourceLocation"),
                        QueryGrouping(type="Dimension", name="ConsumedService"),
                        QueryGrouping(type="Dimension", name="ResourceId")
                    ]
                )
            )
            
            self.logger.info(f"Querying costs for resource group: {resource_group}", 
                           start_date=start_date.isoformat(), 
                           end_date=end_date.isoformat())
            
            response = self._client.query.usage(scope, query_definition)
            
            # Process with enhanced isolation
            processed_costs = await self._process_isolated_cost_response(response, resource_group)
            
            # Add metadata about the query
            processed_costs['query_metadata'] = {
                'subscription_id': self.subscription_id,
                'resource_group': resource_group,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'scope': scope,
                'granularity': granularity.value if hasattr(granularity, 'value') else str(granularity),
                'isolation_method': 'resource_group_scope'
            }
            
            return processed_costs
            
        except AzureError as e:
            self.logger.error("Failed to get isolated resource group costs", 
                            resource_group=resource_group, error=str(e))
            raise DiscoveryException("CostManagement", f"Failed to get costs for {resource_group}: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def get_subscription_costs_by_resource_group(self,
                                                     start_date: Optional[datetime] = None,
                                                     end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get all costs in subscription broken down by resource group."""
        if not self._connected:
            raise DiscoveryException("CostManagement", "Client not connected")
        
        # Default to last 30 days if dates not provided
        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        scope = f"/subscriptions/{self.subscription_id}"
        
        try:
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
                        "totalCost": QueryAggregation(name="PreTaxCost", function="Sum"),
                        "usageQuantity": QueryAggregation(name="UsageQuantity", function="Sum")
                    },
                    grouping=[
                        QueryGrouping(type="Dimension", name="ResourceGroupName"),
                        QueryGrouping(type="Dimension", name="ServiceName"),
                        QueryGrouping(type="Dimension", name="ResourceType"),
                        QueryGrouping(type="Dimension", name="MeterCategory")
                    ]
                )
            )
            
            response = self._client.query.usage(scope, query_definition)
            return await self._process_subscription_cost_breakdown(response)
            
        except AzureError as e:
            self.logger.error("Failed to get subscription costs by resource group", error=str(e))
            raise DiscoveryException("CostManagement", f"Failed to get subscription cost breakdown: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def get_detailed_resource_costs(self,
                                        resource_group: str,
                                        start_date: Optional[datetime] = None,
                                        end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get detailed per-resource costs within a resource group."""
        if not self._connected:
            raise DiscoveryException("CostManagement", "Client not connected")
        
        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        scope = f"/subscriptions/{self.subscription_id}/resourceGroups/{resource_group}"
        
        try:
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
                        QueryGrouping(type="Dimension", name="ResourceId"),
                        QueryGrouping(type="Dimension", name="ResourceType"),
                        QueryGrouping(type="Dimension", name="ServiceName"),
                        QueryGrouping(type="Dimension", name="MeterCategory")
                    ]
                )
            )
            
            response = self._client.query.usage(scope, query_definition)
            return await self._process_detailed_resource_costs(response, resource_group)
            
        except AzureError as e:
            self.logger.error("Failed to get detailed resource costs", 
                            resource_group=resource_group, error=str(e))
            raise DiscoveryException("CostManagement", f"Failed to get detailed costs for {resource_group}: {e}")
    
    async def _process_isolated_cost_response(self, response, resource_group: str) -> Dict[str, Any]:
        """Process cost API response with strict isolation validation."""
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
            'by_resource_id': {},
            'resource_group_validation': {
                'target_resource_group': resource_group,
                'other_resource_groups_found': set(),
                'isolation_validated': True
            },
            'source': 'azure_cost_management_api_isolated'
        }
        
        if not hasattr(response, 'rows') or not response.rows:
            self.logger.warning("No cost data returned from Azure Cost Management API")
            return costs
        
        daily_costs = {}
        resource_validation = costs['resource_group_validation']
        
        for row in response.rows:
            try:
                if len(row) >= 7:  # Updated for correct number of columns
                    cost = float(row[0]) if row[0] is not None else 0.0
                    usage_quantity = float(row[1]) if row[1] is not None else 0.0
                    usage_date_raw = row[2] if row[2] is not None else None
                    resource_type = row[3] if row[3] is not None else "Unknown"
                    service_name = row[4] if row[4] is not None else "Unknown"
                    meter_category = row[5] if row[5] is not None else "Unknown"
                    returned_rg = row[6] if row[6] is not None else "Unknown"
                    location = row[7] if len(row) > 7 and row[7] is not None else "Unknown"
                    consumed_service = row[8] if len(row) > 8 and row[8] is not None else "Unknown"
                    resource_id = row[9] if len(row) > 9 and row[9] is not None else "Unknown"
                    
                    # Validate resource group isolation
                    if returned_rg != resource_group and returned_rg != "Unknown":
                        resource_validation['other_resource_groups_found'].add(returned_rg)
                        resource_validation['isolation_validated'] = False
                        self.logger.warning(f"Found costs from different resource group: {returned_rg}")
                        continue  # Skip this row as it's not from our target RG
                    
                    # Parse date
                    usage_date = self._parse_usage_date(usage_date_raw)
                    
                    costs['total'] += cost
                    
                    # Enhanced categorization
                    self._categorize_cost_enhanced(costs, cost, service_name, meter_category, 
                                                 resource_type, consumed_service)
                    
                    # Group by service with more details
                    if service_name not in costs['by_service']:
                        costs['by_service'][service_name] = {
                            'cost': 0.0,
                            'usage_quantity': 0.0,
                            'currency': 'USD',
                            'resource_types': set(),
                            'locations': set(),
                            'meter_categories': set()
                        }
                    service_data = costs['by_service'][service_name]
                    service_data['cost'] += cost
                    service_data['usage_quantity'] += usage_quantity
                    service_data['resource_types'].add(resource_type)
                    service_data['locations'].add(location)
                    service_data['meter_categories'].add(meter_category)
                    
                    # Group by resource type
                    if resource_type not in costs['by_resource_type']:
                        costs['by_resource_type'][resource_type] = {
                            'cost': 0.0,
                            'usage_quantity': 0.0,
                            'services': set(),
                            'meter_categories': set(),
                            'resource_count': set()
                        }
                    rt_data = costs['by_resource_type'][resource_type]
                    rt_data['cost'] += cost
                    rt_data['usage_quantity'] += usage_quantity
                    rt_data['services'].add(service_name)
                    rt_data['meter_categories'].add(meter_category)
                    if resource_id != "Unknown":
                        rt_data['resource_count'].add(resource_id)
                    
                    # Group by meter category
                    if meter_category not in costs['by_meter_category']:
                        costs['by_meter_category'][meter_category] = {
                            'cost': 0.0,
                            'usage_quantity': 0.0,
                            'resource_types': set(),
                            'services': set(),
                            'consumed_services': set()
                        }
                    mc_data = costs['by_meter_category'][meter_category]
                    mc_data['cost'] += cost
                    mc_data['usage_quantity'] += usage_quantity
                    mc_data['resource_types'].add(resource_type)
                    mc_data['services'].add(service_name)
                    mc_data['consumed_services'].add(consumed_service)
                    
                    # Group by resource ID for detailed breakdown
                    if resource_id != "Unknown":
                        if resource_id not in costs['by_resource_id']:
                            costs['by_resource_id'][resource_id] = {
                                'cost': 0.0,
                                'usage_quantity': 0.0,
                                'resource_type': resource_type,
                                'service_name': service_name,
                                'location': location,
                                'meter_details': {}
                            }
                        rid_data = costs['by_resource_id'][resource_id]
                        rid_data['cost'] += cost
                        rid_data['usage_quantity'] += usage_quantity
                        
                        # Track meter-level details
                        meter_key = f"{meter_category}:{consumed_service}"
                        if meter_key not in rid_data['meter_details']:
                            rid_data['meter_details'][meter_key] = {
                                'cost': 0.0,
                                'usage_quantity': 0.0,
                                'consumed_service': consumed_service
                            }
                        rid_data['meter_details'][meter_key]['cost'] += cost
                        rid_data['meter_details'][meter_key]['usage_quantity'] += usage_quantity
                    
                    # Daily breakdown
                    if usage_date:
                        date_str = usage_date.strftime('%Y-%m-%d')
                        if date_str not in daily_costs:
                            daily_costs[date_str] = {
                                'date': date_str,
                                'cost': 0.0,
                                'usage': 0.0,
                                'currency': 'USD',
                                'services': {},
                                'categories': {},
                                'resource_types': {}
                            }
                        day_data = daily_costs[date_str]
                        day_data['cost'] += cost
                        day_data['usage'] += usage_quantity
                        
                        # Track daily breakdowns
                        if service_name not in day_data['services']:
                            day_data['services'][service_name] = 0.0
                        day_data['services'][service_name] += cost
                        
                        category = self._get_cost_category_enhanced(service_name, meter_category, 
                                                                 resource_type, service_name)
                        if category not in day_data['categories']:
                            day_data['categories'][category] = 0.0
                        day_data['categories'][category] += cost
                        
                        if resource_type not in day_data['resource_types']:
                            day_data['resource_types'][resource_type] = 0.0
                        day_data['resource_types'][resource_type] += cost
                    
            except (ValueError, TypeError, IndexError) as e:
                self.logger.warning(f"Error processing cost row: {e}", row=row)
                continue
        
        # Convert sets to lists for JSON serialization
        self._convert_sets_to_lists_enhanced(costs)
        
        # Finalize resource group validation
        resource_validation['other_resource_groups_found'] = list(resource_validation['other_resource_groups_found'])
        
        # Sort daily breakdown by date
        costs['daily_breakdown'] = sorted(daily_costs.values(), key=lambda x: x['date'])
        
        # Add enhanced summary statistics
        costs['summary'] = self._calculate_enhanced_summary(costs, daily_costs)
        
        self.logger.info(
            "Processed isolated cost data successfully",
            resource_group=resource_group,
            total_cost=costs['total'],
            currency=costs['currency'],
            days=len(daily_costs),
            services=len(costs['by_service']),
            resources=len(costs['by_resource_id']),
            isolation_validated=resource_validation['isolation_validated']
        )
        
        return costs
    
    async def _process_subscription_cost_breakdown(self, response) -> Dict[str, Any]:
        """Process subscription-wide cost breakdown by resource group."""
        breakdown = {
            'total_subscription_cost': 0.0,
            'by_resource_group': {},
            'resource_group_ranking': [],
            'currency': 'USD',
            'summary': {
                'resource_group_count': 0,
                'top_resource_group': None,
                'cost_distribution': {}
            }
        }
        
        if not hasattr(response, 'rows') or not response.rows:
            return breakdown
        
        for row in response.rows:
            try:
                if len(row) >= 6:
                    cost = float(row[0]) if row[0] is not None else 0.0
                    usage_quantity = float(row[1]) if row[1] is not None else 0.0
                    resource_group = row[2] if row[2] is not None else "Unknown"
                    service_name = row[3] if row[3] is not None else "Unknown"
                    resource_type = row[4] if len(row) > 4 and row[4] is not None else "Unknown"
                    meter_category = row[5] if len(row) > 5 and row[5] is not None else "Unknown"
                    
                    breakdown['total_subscription_cost'] += cost
                    
                    if resource_group not in breakdown['by_resource_group']:
                        breakdown['by_resource_group'][resource_group] = {
                            'total_cost': 0.0,
                            'by_service': {},
                            'by_resource_type': {},
                            'by_meter_category': {}
                        }
                    
                    rg_data = breakdown['by_resource_group'][resource_group]
                    rg_data['total_cost'] += cost
                    
                    # Service breakdown within RG
                    if service_name not in rg_data['by_service']:
                        rg_data['by_service'][service_name] = 0.0
                    rg_data['by_service'][service_name] += cost
                    
                    # Resource type breakdown within RG
                    if resource_type not in rg_data['by_resource_type']:
                        rg_data['by_resource_type'][resource_type] = 0.0
                    rg_data['by_resource_type'][resource_type] += cost
                    
                    # Meter category breakdown within RG
                    if meter_category not in rg_data['by_meter_category']:
                        rg_data['by_meter_category'][meter_category] = 0.0
                    rg_data['by_meter_category'][meter_category] += cost
                    
            except (ValueError, TypeError, IndexError) as e:
                self.logger.warning(f"Error processing subscription cost row: {e}", row=row)
                continue
        
        # Calculate rankings and summaries
        rg_costs = [(rg, data['total_cost']) for rg, data in breakdown['by_resource_group'].items()]
        breakdown['resource_group_ranking'] = sorted(rg_costs, key=lambda x: x[1], reverse=True)
        
        breakdown['summary']['resource_group_count'] = len(breakdown['by_resource_group'])
        if breakdown['resource_group_ranking']:
            breakdown['summary']['top_resource_group'] = breakdown['resource_group_ranking'][0][0]
        
        # Calculate cost distribution percentages
        if breakdown['total_subscription_cost'] > 0:
            for rg, cost in breakdown['resource_group_ranking']:
                percentage = (cost / breakdown['total_subscription_cost']) * 100
                breakdown['summary']['cost_distribution'][rg] = round(percentage, 2)
        
        return breakdown
    
    async def _process_detailed_resource_costs(self, response, resource_group: str) -> Dict[str, Any]:
        """Process detailed per-resource cost response."""
        detailed_costs = {
            'resource_group': resource_group,
            'resources': {},
            'summary': {
                'total_resources': 0,
                'total_cost': 0.0,
                'avg_cost_per_resource': 0.0,
                'top_cost_resources': []
            },
            'currency': 'USD'
        }
        
        if not hasattr(response, 'rows') or not response.rows:
            return detailed_costs
        
        for row in response.rows:
            try:
                if len(row) >= 6:
                    cost = float(row[0]) if row[0] is not None else 0.0
                    usage_quantity = float(row[1]) if row[1] is not None else 0.0
                    usage_date_raw = row[2] if row[2] is not None else None
                    resource_id = row[3] if row[3] is not None else "Unknown"
                    resource_type = row[4] if row[4] is not None else "Unknown"
                    service_name = row[5] if row[5] is not None else "Unknown"
                    meter_category = row[6] if len(row) > 6 and row[6] is not None else "Unknown"
                    
                    usage_date = self._parse_usage_date(usage_date_raw)
                    
                    if resource_id not in detailed_costs['resources']:
                        detailed_costs['resources'][resource_id] = {
                            'resource_id': resource_id,
                            'resource_type': resource_type,
                            'service_name': service_name,
                            'total_cost': 0.0,
                            'total_usage': 0.0,
                            'daily_costs': {},
                            'meter_breakdown': {},
                            'cost_category': self._get_cost_category_enhanced(
                                service_name, meter_category, resource_type, service_name
                            )
                        }
                    
                    resource_data = detailed_costs['resources'][resource_id]
                    resource_data['total_cost'] += cost
                    resource_data['total_usage'] += usage_quantity
                    
                    # Daily breakdown for this resource
                    if usage_date:
                        date_str = usage_date.strftime('%Y-%m-%d')
                        if date_str not in resource_data['daily_costs']:
                            resource_data['daily_costs'][date_str] = 0.0
                        resource_data['daily_costs'][date_str] += cost
                    
                    # Meter breakdown for this resource
                    meter_key = f"{meter_category}:{service_name}"
                    if meter_key not in resource_data['meter_breakdown']:
                        resource_data['meter_breakdown'][meter_key] = {
                            'cost': 0.0,
                            'usage': 0.0,
                            'service': service_name,
                            'unit': 'unknown'
                        }
                    meter_data = resource_data['meter_breakdown'][meter_key]
                    meter_data['cost'] += cost
                    meter_data['usage'] += usage_quantity
                    
                    detailed_costs['summary']['total_cost'] += cost
                    
            except (ValueError, TypeError, IndexError) as e:
                self.logger.warning(f"Error processing detailed cost row: {e}", row=row)
                continue
        
        # Calculate summary statistics
        detailed_costs['summary']['total_resources'] = len(detailed_costs['resources'])
        if detailed_costs['summary']['total_resources'] > 0:
            detailed_costs['summary']['avg_cost_per_resource'] = (
                detailed_costs['summary']['total_cost'] / detailed_costs['summary']['total_resources']
            )
        
        # Get top cost resources
        resource_costs = [
            (rid, data['total_cost']) for rid, data in detailed_costs['resources'].items()
        ]
        detailed_costs['summary']['top_cost_resources'] = sorted(
            resource_costs, key=lambda x: x[1], reverse=True
        )[:10]
        
        return detailed_costs
    
    def _parse_usage_date(self, usage_date_raw) -> Optional[datetime]:
        """Parse usage date from various formats."""
        if not usage_date_raw:
            return None
        
        try:
            if isinstance(usage_date_raw, (int, float)):
                date_str = str(int(usage_date_raw))
                if len(date_str) == 8:
                    year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
                    return datetime(year, month, day)
            elif isinstance(usage_date_raw, str) and len(usage_date_raw) == 8:
                return datetime.strptime(usage_date_raw, '%Y%m%d')
            else:
                return usage_date_raw if hasattr(usage_date_raw, 'strftime') else None
        except (ValueError, TypeError):
            return None
    
    def _categorize_cost_enhanced(self, costs: Dict[str, Any], cost: float, service_name: str, 
                                meter_category: str, resource_type: str, consumed_service: str):
        """Enhanced cost categorization with more precise rules."""
        service_lower = service_name.lower()
        meter_lower = meter_category.lower()
        resource_type_lower = resource_type.lower()
        consumed_lower = consumed_service.lower()
        
        # Compute costs - more precise detection
        if (any(keyword in service_lower for keyword in ['virtual machines', 'compute', 'container service']) or
            any(keyword in meter_lower for keyword in ['virtual machines', 'compute', 'cpu', 'container']) or
            any(keyword in resource_type_lower for keyword in ['microsoft.compute', 'microsoft.containerservice']) or
            any(keyword in consumed_lower for keyword in ['compute', 'virtual machines', 'container'])):
            costs['compute'] += cost
        
        # Storage costs - enhanced detection
        elif (any(keyword in service_lower for keyword in ['storage', 'blob', 'file', 'disk', 'managed disks']) or
              any(keyword in meter_lower for keyword in ['storage', 'disk', 'blob', 'file', 'data stored']) or
              any(keyword in resource_type_lower for keyword in ['microsoft.storage', 'microsoft.containerregistry']) or
              any(keyword in consumed_lower for keyword in ['storage', 'disk'])):
            costs['storage'] += cost
        
        # Network costs - enhanced detection
        elif (any(keyword in service_lower for keyword in ['bandwidth', 'network', 'load balancer', 'dns', 'cdn', 'application gateway']) or
              any(keyword in meter_lower for keyword in ['bandwidth', 'network', 'load balancer', 'dns', 'cdn', 'data transfer']) or
              any(keyword in resource_type_lower for keyword in ['microsoft.network']) or
              any(keyword in consumed_lower for keyword in ['bandwidth', 'network', 'load balancer'])):
            costs['network'] += cost
        
        # Monitoring costs - enhanced detection
        elif (any(keyword in service_lower for keyword in ['monitor', 'grafana', 'insights', 'log analytics', 'application insights']) or
              any(keyword in meter_lower for keyword in ['monitor', 'grafana', 'insights', 'log analytics', 'application insights']) or
              any(keyword in resource_type_lower for keyword in ['microsoft.monitor', 'microsoft.insights', 'microsoft.dashboard', 'microsoft.operationalinsights']) or
              any(keyword in consumed_lower for keyword in ['monitoring', 'insights', 'log analytics'])):
            costs['monitoring'] += cost
        
        # Everything else
        else:
            costs['other'] += cost
    
    def _get_cost_category_enhanced(self, service_name: str, meter_category: str, 
                                  resource_type: str, consumed_service: str) -> str:
        """Enhanced cost category detection."""
        service_lower = service_name.lower()
        meter_lower = meter_category.lower()
        resource_type_lower = resource_type.lower()
        consumed_lower = consumed_service.lower()
        
        if (any(keyword in service_lower for keyword in ['virtual machines', 'compute', 'container service']) or
            any(keyword in meter_lower for keyword in ['virtual machines', 'compute', 'cpu', 'container']) or
            'microsoft.compute' in resource_type_lower or 'microsoft.containerservice' in resource_type_lower):
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
    
    def _convert_sets_to_lists_enhanced(self, costs: Dict[str, Any]):
        """Convert sets to lists for JSON serialization."""
        for service_data in costs['by_service'].values():
            for key in ['resource_types', 'locations', 'meter_categories']:
                if key in service_data and isinstance(service_data[key], set):
                    service_data[key] = list(service_data[key])
        
        for resource_data in costs['by_resource_type'].values():
            for key in ['services', 'meter_categories', 'resource_count']:
                if key in resource_data and isinstance(resource_data[key], set):
                    resource_data[key] = list(resource_data[key]) if key != 'resource_count' else len(resource_data[key])
        
        for meter_data in costs['by_meter_category'].values():
            for key in ['resource_types', 'services', 'consumed_services']:
                if key in meter_data and isinstance(meter_data[key], set):
                    meter_data[key] = list(meter_data[key])
    
    def _calculate_enhanced_summary(self, costs: Dict[str, Any], daily_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate enhanced summary statistics."""
        summary = {
            'total_days': len(daily_costs),
            'average_daily_cost': round(costs['total'] / len(daily_costs), 2) if daily_costs else 0.0,
            'peak_daily_cost': max([day['cost'] for day in daily_costs.values()]) if daily_costs else 0.0,
            'min_daily_cost': min([day['cost'] for day in daily_costs.values()]) if daily_costs else 0.0,
            'cost_trend': self._calculate_cost_trend(daily_costs),
            'service_count': len(costs['by_service']),
            'resource_type_count': len(costs['by_resource_type']),
            'resource_count': len(costs['by_resource_id']),
            'top_service': max(costs['by_service'].items(), key=lambda x: x[1]['cost'])[0] if costs['by_service'] else 'None',
            'top_resource_type': max(costs['by_resource_type'].items(), key=lambda x: x[1]['cost'])[0] if costs['by_resource_type'] else 'None',
            'cost_distribution_percentage': {
                'compute': round((costs['compute'] / costs['total']) * 100, 2) if costs['total'] > 0 else 0,
                'storage': round((costs['storage'] / costs['total']) * 100, 2) if costs['total'] > 0 else 0,
                'network': round((costs['network'] / costs['total']) * 100, 2) if costs['total'] > 0 else 0,
                'monitoring': round((costs['monitoring'] / costs['total']) * 100, 2) if costs['total'] > 0 else 0,
                'other': round((costs['other'] / costs['total']) * 100, 2) if costs['total'] > 0 else 0
            }
        }
        
        return summary
    
    def _calculate_cost_trend(self, daily_costs: Dict[str, Any]) -> str:
        """Calculate cost trend over the period."""
        if len(daily_costs) < 3:
            return 'insufficient_data'
        
        sorted_days = sorted(daily_costs.values(), key=lambda x: x['date'])
        first_third = sorted_days[:len(sorted_days)//3]
        last_third = sorted_days[-len(sorted_days)//3:]
        
        first_avg = sum(day['cost'] for day in first_third) / len(first_third)
        last_avg = sum(day['cost'] for day in last_third) / len(last_third)
        
        change_percent = ((last_avg - first_avg) / first_avg * 100) if first_avg > 0 else 0
        
        if change_percent > 10:
            return 'increasing'
        elif change_percent < -10:
            return 'decreasing'
        else:
            return 'stable'