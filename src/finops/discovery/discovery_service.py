# src/finops/discovery/discovery_service.py
"""Fixed discovery service for data collection only."""

from typing import Dict, Any, List
import structlog
from datetime import datetime, timezone

from finops.discovery.base import BaseDiscoveryService
from finops.discovery.discovery_client import DiscoveryClient

logger = structlog.get_logger(__name__)


class DiscoveryService(BaseDiscoveryService):
    """Discovery service for data collection only."""
    
    def __init__(self, discovery_client: DiscoveryClient, config: Dict[str, Any]):
        super().__init__(discovery_client, config)
        
    async def discover(self) -> Dict[str, Any]:
        """Perform comprehensive discovery - data collection only."""
        self.logger.info("Starting discovery service")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        try:
            # Get comprehensive discovery data
            discovery_data = await self.client.discover_comprehensive_data()
            
            self.logger.info(
                "Discovery service completed",
                clusters=len(discovery_data.get("clusters", [])),
                total_cost=discovery_data.get("summary", {}).get("total_cost", 0.0)
            )
            
            return discovery_data
            
        except Exception as e:
            self.logger.error("Discovery service failed", error=str(e))
            raise
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "comprehensive_discovery"