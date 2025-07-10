
from typing import Any, Dict, List

import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.discovery.orchestrator import DiscoveryOrchestrator
from finops.mappers.discovery_mapper import DiscoveryDataMapper
from finops.models.discovery_models import DiscoveryInfo

logger = structlog.get_logger(__name__)

class EnhancedDiscoveryOrchestrator:
    """Enhanced discovery orchestrator that outputs structured data models."""
    
    
    def __init__(self, services: List[BaseDiscoveryService]):
        self.services = services
        self.orchestrator = DiscoveryOrchestrator(services)
        self.mapper = DiscoveryDataMapper()
        self.logger = logger.bind(component="enhanced_orchestrator")
        
    
    async def run_structured_discovery(self) -> DiscoveryInfo:
        """Run discovery and return structured data models."""
        try:
            # Run standard discovery
            raw_results = await self.orchestrator.run_parallel_discovery()
            
            # Map to structured models
            structured_results = self.mapper.map_discovery_results(raw_results)
            
            self.logger.info(
                "Structured discovery completed",
                clusters=len(structured_results.clusters),
                total_resources=structured_results.totals.total_resources,
                total_cost=structured_results.totals.total_cost.total
            )
            
            return structured_results
            
        except Exception as e:
            self.logger.error("Structured discovery failed", error=str(e))
            raise
    
    async def run_discovery_with_analysis(self) -> Dict[str, Any]:
        """Run discovery and include analysis insights."""
        discovery_info = await self.run_structured_discovery()
        
        # Generate analysis insights
        analysis_results = {
            'discovery_info': discovery_info,
            'cost_summary': discovery_info.get_cost_summary(),
            'utilization_summary': discovery_info.get_utilization_summary(),
            'efficiency_insights': discovery_info.get_efficiency_insights(),
            'export_data': discovery_info.export_to_dict()
        }
        
        return analysis_results

