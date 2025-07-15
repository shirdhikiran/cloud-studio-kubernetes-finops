# src/finops/discovery/discovery_service.py
"""Phase 1 Discovery - Pure data collection service."""

from typing import Dict, Any
import structlog
from datetime import datetime, timezone

from finops.discovery.base import BaseDiscoveryService
from finops.discovery.discovery_client import DiscoveryClient

logger = structlog.get_logger(__name__)


class DiscoveryService(BaseDiscoveryService):
    """Phase 1 Discovery - Pure data collection service."""
    
    def __init__(self, discovery_client: DiscoveryClient, config: Dict[str, Any]):
        super().__init__(discovery_client, config)
        
    async def discover(self) -> Dict[str, Any]:
        """Phase 1: Pure data discovery without analysis."""
        self.logger.info("Starting Phase 1 discovery service")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        try:
            # Get raw discovery data
            discovery_data = await self.client.discover_comprehensive_data()
            
            self.logger.info(
                "Phase 1 discovery service completed",
                clusters=len(discovery_data.get("clusters", [])),
                total_cost=discovery_data.get("summary", {}).get("total_cost", 0.0)
            )
            
            return discovery_data
            
        except Exception as e:
            self.logger.error("Phase 1 discovery service failed", error=str(e))
            raise
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "phase1_data_discovery"