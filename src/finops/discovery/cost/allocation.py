"""Cost allocation discovery service."""

from typing import Dict, Any, List
from datetime import datetime, timedelta, timezone
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.azure.cost_client import CostClient

logger = structlog.get_logger(__name__)


class CostAllocationService(BaseDiscoveryService):
    """Service for discovering cost allocation data."""
    
    def __init__(self, cost_client: CostClient, config: Dict[str, Any]):
        super().__init__(cost_client, config)
        self.resource_group = config.get("resource_group")
        self.allocation_dimensions = config.get("allocation_dimensions", ["environment", "project", "team"])
    
    async def discover(self) -> List[Dict[str, Any]]:
        """Discover cost allocation data."""
        self.logger.info("Starting cost allocation discovery")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=30)
        
        allocation_data = []
        
        try:
            # For now, we'll create a placeholder structure
            # In a real implementation, this would query tag-based cost allocation
            allocation_summary = {
                'total_allocated_cost': 0.0,
                'unallocated_cost': 0.0,
                'allocation_by_dimension': {},
                'allocation_coverage': 0.0,
                'period_start': start_date.isoformat(),
                'period_end': end_date.isoformat()
            }
            
            for dimension in self.allocation_dimensions:
                allocation_summary['allocation_by_dimension'][dimension] = {
                    'allocated_cost': 0.0,
                    'breakdown': {},
                    'coverage_percentage': 0.0
                }
            
            allocation_data.append({
                'type': 'cost_allocation_summary',
                'resource_group': self.resource_group,
                'data': allocation_summary
            })
            
        except Exception as e:
            self.logger.error("Failed to discover cost allocation", error=str(e))
            raise
        
        self.logger.info("Completed cost allocation discovery")
        return allocation_data
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "cost_allocation"