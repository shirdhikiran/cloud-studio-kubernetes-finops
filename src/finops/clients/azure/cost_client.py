# src/finops/clients/azure/cost_client.py
"""Azure Cost Management client with enhanced functionality."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
import structlog
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.costmanagement.models import (
    QueryDefinition, QueryDataset, QueryAggregation, QueryGrouping,
    QueryTimePeriod, TimeframeType, GranularityType
)
from azure.core.exceptions import AzureError

from finops.core.base_client import BaseClient
from finops.core.exceptions import ClientConnectionException, DiscoveryException
from finops.core.utils import retry_with_backoff

logger = structlog.get_logger(__name__)


class CostClient(BaseClient):
    """Enhanced Azure Cost Management client."""
    
    def __init__(self, credential, subscription_id: str, config: Dict[str, Any]):
        super().__init__(config, "CostClient")
        self.credential = credential
        self.subscription_id = subscription_id
        self._client = None
    
    async def connect(self) -> None:
        """Connect to Cost Management service."""
        try:
            self._client = CostManagementClient(credential=self.credential)
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
            # Simple health check with minimal query
            scope = f"/subscriptions/{self.subscription_id}"
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=1)
            
            query_definition = QueryDefinition(
                type="Usage",
                timeframe=TimeframeType.CUSTOM,
                time_period=QueryTimePeriod(from_property=start_date, to=end_date),
                dataset=QueryDataset(
                    granularity=GranularityType.DAILY,
                    aggregation={"totalCost": QueryAggregation(name="PreTaxCost", function="Sum")}
                )
            )
            self._client.query.usage(scope, query_definition)
            return True
        except Exception as e:
            self.logger.warning("Cost Management health check failed", error=str(e))
            return False
    
    @retry_with_backoff(max_retries=3)
    async def get_resource_group_costs(self, resource_group: str, days: int = 30) -> Dict[str, Any]:
        """Get costs for specific resource group with detailed breakdown."""
        if not self._connected:
            raise DiscoveryException("CostManagement", "Client not connected")
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
        scope = f"/subscriptions/{self.subscription_id}/resourceGroups/{resource_group}"
        
        try:
            query_definition = QueryDefinition(
                type="Usage",
                timeframe=TimeframeType.CUSTOM,
                time_period=QueryTimePeriod(from_property=start_date, to=end_date),
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
                        QueryGrouping(type="Dimension", name="ResourceId")
                    ]
                )
            )
            
            self.logger.info(f"Querying costs for resource group: {resource_group}")
            response = self._client.query.usage(scope, query_definition)
            
            return self._process_cost_response(response, resource_group, start_date, end_date)
            
        except AzureError as e:
            self.logger.error(f"Failed to get costs for {resource_group}", error=str(e))
            raise DiscoveryException("CostManagement", f"Failed to get costs: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def get_detailed_resource_costs(self, resource_group: str, days: int = 30) -> Dict[str, Any]:
        """Get detailed per-resource costs."""
        if not self._connected:
            raise DiscoveryException("CostManagement", "Client not connected")
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
        scope = f"/subscriptions/{self.subscription_id}/resourceGroups/{resource_group}"
        
        try:
            query_definition = QueryDefinition(
                type="Usage",
                timeframe=TimeframeType.CUSTOM,
                time_period=QueryTimePeriod(from_property=start_date, to=end_date),
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
            return self._process_detailed_resource_costs(response, resource_group)
            
        except AzureError as e:
            self.logger.error(f"Failed to get detailed costs for {resource_group}", error=str(e))
            return {"error": str(e), "resources": {}}
    
    def _process_cost_response(self, response, resource_group: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Process cost API response into structured format with daily breakdown."""
        
        costs = {
            'resource_group': resource_group,
            'period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'days': (end_date - start_date).days
            },
            'total_cost': 0.0,
            'by_service': {},
            'by_resource_type': {},
            'by_meter_category': {},
            'by_resource_id': {},
            'daily_breakdown': [],
            'currency': 'USD'
        }
        
        if not hasattr(response, 'rows') or not response.rows:
            self.logger.warning("No cost data returned")
            return costs
        
        # Track daily costs for aggregation with resource details
        daily_costs = {}
        
        for row in response.rows:
            try:
                if len(row) >= 6:
                    cost = float(row[0]) if row[0] is not None else 0.0
                    usage_quantity = float(row[1]) if row[1] is not None else 0.0
                    usage_date_raw = row[2] if len(row) > 2 and row[2] is not None else None
                    resource_type = row[3] if len(row) > 3 and row[3] is not None else "Unknown"
                    service_name = row[4] if len(row) > 4 and row[4] is not None else "Unknown"
                    meter_category = row[5] if len(row) > 5 and row[5] is not None else "Unknown"
                    resource_id = row[6] if len(row) > 6 and row[6] is not None else "Unknown"
                    
                    costs['total_cost'] += cost
                    
                    # Process daily breakdown with resource details
                    if usage_date_raw:
                        parsed_date = self._parse_usage_date(usage_date_raw)
                        if parsed_date:
                            date_key = parsed_date.strftime('%Y-%m-%d')
                            
                            if date_key not in daily_costs:
                                daily_costs[date_key] = {
                                    'date': date_key,
                                    'cost': 0.0,
                                    'usage_quantity': 0.0,
                                    'currency': 'USD',
                                    'resources': {},
                                    'services': {},
                                    'resource_types': {},
                                    'meter_categories': {}
                                }
                            
                            # Aggregate daily totals
                            daily_costs[date_key]['cost'] += cost
                            daily_costs[date_key]['usage_quantity'] += usage_quantity
                            
                            # Resource-level breakdown for the day
                            if resource_id != "Unknown":
                                if resource_id not in daily_costs[date_key]['resources']:
                                    daily_costs[date_key]['resources'][resource_id] = {
                                        'resource_id': resource_id,
                                        'resource_type': resource_type,
                                        'service_name': service_name,
                                        'meter_category': meter_category,
                                        'cost': 0.0,
                                        'usage_quantity': 0.0,
                                        'currency': 'USD'
                                    }
                                daily_costs[date_key]['resources'][resource_id]['cost'] += cost
                                daily_costs[date_key]['resources'][resource_id]['usage_quantity'] += usage_quantity
                            
                            # Service-level breakdown for the day
                            if service_name not in daily_costs[date_key]['services']:
                                daily_costs[date_key]['services'][service_name] = {
                                    'cost': 0.0,
                                    'usage_quantity': 0.0,
                                    'currency': 'USD'
                                }
                            daily_costs[date_key]['services'][service_name]['cost'] += cost
                            daily_costs[date_key]['services'][service_name]['usage_quantity'] += usage_quantity
                            
                            # Resource type breakdown for the day
                            if resource_type not in daily_costs[date_key]['resource_types']:
                                daily_costs[date_key]['resource_types'][resource_type] = {
                                    'cost': 0.0,
                                    'usage_quantity': 0.0,
                                    'currency': 'USD'
                                }
                            daily_costs[date_key]['resource_types'][resource_type]['cost'] += cost
                            daily_costs[date_key]['resource_types'][resource_type]['usage_quantity'] += usage_quantity
                            
                            # Meter category breakdown for the day
                            if meter_category not in daily_costs[date_key]['meter_categories']:
                                daily_costs[date_key]['meter_categories'][meter_category] = {
                                    'cost': 0.0,
                                    'usage_quantity': 0.0,
                                    'currency': 'USD'
                                }
                            daily_costs[date_key]['meter_categories'][meter_category]['cost'] += cost
                            daily_costs[date_key]['meter_categories'][meter_category]['usage_quantity'] += usage_quantity
                    
                    # Group by service
                    if service_name not in costs['by_service']:
                        costs['by_service'][service_name] = {
                            'cost': 0.0, 'usage_quantity': 0.0, 'currency': 'USD'
                        }
                    costs['by_service'][service_name]['cost'] += cost
                    costs['by_service'][service_name]['usage_quantity'] += usage_quantity
                    
                    # Group by resource type
                    if resource_type not in costs['by_resource_type']:
                        costs['by_resource_type'][resource_type] = {
                            'cost': 0.0, 'usage_quantity': 0.0, 'currency': 'USD'
                        }
                    costs['by_resource_type'][resource_type]['cost'] += cost
                    costs['by_resource_type'][resource_type]['usage_quantity'] += usage_quantity
                    
                    # Group by meter category
                    if meter_category not in costs['by_meter_category']:
                        costs['by_meter_category'][meter_category] = {
                            'cost': 0.0, 'usage_quantity': 0.0, 'currency': 'USD'
                        }
                    costs['by_meter_category'][meter_category]['cost'] += cost
                    costs['by_meter_category'][meter_category]['usage_quantity'] += usage_quantity
                    
                    # Group by resource ID
                    if resource_id != "Unknown" and resource_id not in costs['by_resource_id']:
                        costs['by_resource_id'][resource_id] = {
                            'cost': 0.0, 'usage_quantity': 0.0, 'resource_type': resource_type,
                            'service_name': service_name, 'currency': 'USD'
                        }
                    if resource_id != "Unknown":
                        costs['by_resource_id'][resource_id]['cost'] += cost
                        costs['by_resource_id'][resource_id]['usage_quantity'] += usage_quantity
                    
            except (ValueError, TypeError, IndexError) as e:
                self.logger.warning(f"Error processing cost row: {e}")
                continue
        
        # Convert daily_costs dict to sorted list for daily_breakdown
        costs['daily_breakdown'] = sorted(daily_costs.values(), key=lambda x: x['date'])
        
        # Add summary statistics for daily breakdown
        if costs['daily_breakdown']:
            costs['daily_summary'] = {
                'total_days': len(costs['daily_breakdown']),
                'avg_daily_cost': costs['total_cost'] / len(costs['daily_breakdown']),
                'max_daily_cost': max(day['cost'] for day in costs['daily_breakdown']),
                'min_daily_cost': min(day['cost'] for day in costs['daily_breakdown']),
                'cost_volatility': self._calculate_cost_volatility(costs['daily_breakdown'])
            }
        
        self.logger.info(
            f"Processed cost data for {resource_group}",
            total_cost=costs['total_cost'],
            services=len(costs['by_service']),
            resources=len(costs['by_resource_id']),
            daily_entries=len(costs['daily_breakdown'])
        )
        
        return costs

    def _calculate_cost_volatility(self, daily_breakdown: List[Dict[str, Any]]) -> float:
        """Calculate cost volatility (standard deviation of daily costs)."""
        if len(daily_breakdown) < 2:
            return 0.0
        
        costs = [day['cost'] for day in daily_breakdown]
        mean_cost = sum(costs) / len(costs)
        variance = sum((cost - mean_cost) ** 2 for cost in costs) / len(costs)
        return variance ** 0.5

    def get_daily_cost_drivers(self, daily_breakdown: List[Dict[str, Any]], top_n: int = 5) -> Dict[str, Any]:
        """Get top cost drivers for each day from daily breakdown."""
        daily_drivers = {}
        
        for day in daily_breakdown:
            date = day['date']
            daily_drivers[date] = {
                'date': date,
                'total_cost': day['cost'],
                'top_resources': [],
                'top_services': [],
                'top_resource_types': []
            }
            
            # Top resources by cost
            if 'resources' in day:
                sorted_resources = sorted(
                    day['resources'].items(),
                    key=lambda x: x[1]['cost'],
                    reverse=True
                )[:top_n]
                
                daily_drivers[date]['top_resources'] = [
                    {
                        'resource_id': res_id,
                        'cost': res_data['cost'],
                        'percentage': (res_data['cost'] / day['cost']) * 100 if day['cost'] > 0 else 0,
                        'resource_type': res_data['resource_type'],
                        'service_name': res_data['service_name']
                    }
                    for res_id, res_data in sorted_resources
                ]
            
            # Top services by cost
            if 'services' in day:
                sorted_services = sorted(
                    day['services'].items(),
                    key=lambda x: x[1]['cost'],
                    reverse=True
                )[:top_n]
                
                daily_drivers[date]['top_services'] = [
                    {
                        'service_name': svc_name,
                        'cost': svc_data['cost'],
                        'percentage': (svc_data['cost'] / day['cost']) * 100 if day['cost'] > 0 else 0
                    }
                    for svc_name, svc_data in sorted_services
                ]
            
            # Top resource types by cost
            if 'resource_types' in day:
                sorted_types = sorted(
                    day['resource_types'].items(),
                    key=lambda x: x[1]['cost'],
                    reverse=True
                )[:top_n]
                
                daily_drivers[date]['top_resource_types'] = [
                    {
                        'resource_type': type_name,
                        'cost': type_data['cost'],
                        'percentage': (type_data['cost'] / day['cost']) * 100 if day['cost'] > 0 else 0
                    }
                    for type_name, type_data in sorted_types
                ]
        
        return daily_drivers

    def _parse_usage_date(self, usage_date_raw) -> Optional[datetime]:
        """Parse usage date from various formats."""
        if not usage_date_raw:
            return None
        
        try:
            # Handle integer/float date formats (e.g., 20250716)
            if isinstance(usage_date_raw, (int, float)):
                date_str = str(int(usage_date_raw))
                if len(date_str) == 8:
                    year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
                    return datetime(year, month, day)
            
            # Handle string date formats
            elif isinstance(usage_date_raw, str):
                # Try YYYYMMDD format
                if len(usage_date_raw) == 8 and usage_date_raw.isdigit():
                    return datetime.strptime(usage_date_raw, '%Y%m%d')
                # Try ISO format
                elif 'T' in usage_date_raw:
                    return datetime.fromisoformat(usage_date_raw.replace('Z', '+00:00'))
                # Try date-only format
                elif '-' in usage_date_raw:
                    return datetime.strptime(usage_date_raw.split('T')[0], '%Y-%m-%d')
            
            # Handle datetime objects
            elif hasattr(usage_date_raw, 'strftime'):
                return usage_date_raw
            
            return None
            
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Failed to parse usage date: {usage_date_raw}, error: {e}")
            return None