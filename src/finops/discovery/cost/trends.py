"""Cost trend discovery service."""

from typing import Dict, Any, List
from datetime import datetime, timedelta
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.azure.cost_client import CostClient

logger = structlog.get_logger(__name__)


class CostTrendService(BaseDiscoveryService):
    """Service for discovering cost trends and patterns."""
    
    def __init__(self, cost_client: CostClient, config: Dict[str, Any]):
        super().__init__(cost_client, config)
        self.resource_group = config.get("resource_group")
        self.trend_period_days = config.get("trend_period_days", 90)
    
    # async def discover(self) -> List[Dict[str, Any]]:
    #     """Discover cost trends."""
    #     self.logger.info("Starting cost trend discovery")
        
    #     if not self.client.is_connected:
    #         await self.client.connect()
        
    #     end_date = datetime.now(timezone.utc)
    #     start_date = end_date