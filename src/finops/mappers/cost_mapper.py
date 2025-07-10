"""Cost data mapping utilities."""

from typing import Dict, Any, List
from finops.models.discovery_models import CostData
import structlog

logger = structlog.get_logger(__name__)


class CostDataMapper:
    """Maps cost data from various sources to standardized format."""
    
    def map_azure_cost_data(self, azure_cost: Dict[str, Any]) -> CostData:
        """Map Azure cost management data to CostData model."""
        return CostData(
            total=azure_cost.get('total', 0.0),
            compute=azure_cost.get('compute', 0.0),
            storage=azure_cost.get('storage', 0.0),
            network=azure_cost.get('network', 0.0),
            monitoring=azure_cost.get('monitoring', 0.0),
            other=azure_cost.get('other', 0.0),
            currency=azure_cost.get('currency', 'USD'),
            daily_breakdown=azure_cost.get('daily_breakdown', []),
            by_service=azure_cost.get('by_service', {}),
            cost_trend=self._determine_trend(azure_cost.get('daily_breakdown', []))
        )
    
    def _determine_trend(self, daily_breakdown: List[Dict[str, Any]]) -> str:
        """Determine cost trend from daily data."""
        if len(daily_breakdown) < 2:
            return "stable"
        
        first_cost = daily_breakdown[0].get('cost', 0)
        last_cost = daily_breakdown[-1].get('cost', 0)
        
        if last_cost > first_cost * 1.1:
            return "increasing"
        elif last_cost < first_cost * 0.9:
            return "decreasing"
        else:
            return "stable"