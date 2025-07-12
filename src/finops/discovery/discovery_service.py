# src/finops/discovery/discovery_service.py
"""Complete discovery service for orchestrating comprehensive discovery."""

from typing import Dict, Any, List
import structlog
from datetime import datetime, timezone

from finops.discovery.base import BaseDiscoveryService
from finops.discovery.discovery_client import DiscoveryClient

logger = structlog.get_logger(__name__)


class DiscoveryService(BaseDiscoveryService):
    """Complete service for comprehensive discovery orchestration."""
    
    def __init__(self, discovery_client: DiscoveryClient, config: Dict[str, Any]):
        super().__init__(discovery_client, config)
        self.include_detailed_metrics = config.get("include_detailed_metrics", True)
        self.include_cost_allocation = config.get("include_cost_allocation", True)
        
    async def discover(self) -> Dict[str, Any]:
        """Perform comprehensive discovery."""
        self.logger.info("Starting comprehensive discovery service")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        try:
            # Get comprehensive discovery data
            discovery_data = await self.client.discover_comprehensive_data()
            
            # Add service-level enhancements
            enhanced_data = self._enhance_discovery_data(discovery_data)
            
            self.logger.info(
                "Discovery service completed",
                clusters=len(enhanced_data.get("clusters", [])),
                total_cost=enhanced_data.get("summary", {}).get("total_cost", 0.0)
            )
            
            return enhanced_data
            
        except Exception as e:
            self.logger.error("Discovery service failed", error=str(e))
            raise
    
    def _enhance_discovery_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance discovery data with additional insights."""
        enhanced_data = data.copy()
        
        # Add efficiency analysis
        enhanced_data["efficiency_analysis"] = self._calculate_efficiency_analysis(data)
        
        # Add cost optimization opportunities
        enhanced_data["optimization_opportunities"] = self._identify_optimization_opportunities(data)
        
        # Add resource utilization insights
        enhanced_data["utilization_insights"] = self._generate_utilization_insights(data)
        
        return enhanced_data
    
    def _calculate_efficiency_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate overall efficiency analysis."""
        clusters = data.get("clusters", [])
        
        if not clusters:
            return {"error": "No clusters to analyze"}
        
        total_utilization = 0.0
        utilization_count = 0
        underutilized_clusters = []
        overutilized_clusters = []
        
        for cluster in clusters:
            utilization_summary = cluster.get("utilization_summary", {})
            cluster_utilization = utilization_summary.get("cluster_wide_utilization", {})
            
            avg_cpu = cluster_utilization.get("avg_cpu_utilization", 0.0)
            avg_memory = cluster_utilization.get("avg_memory_utilization", 0.0)
            
            if avg_cpu > 0 or avg_memory > 0:
                cluster_avg = (avg_cpu + avg_memory) / 2
                total_utilization += cluster_avg
                utilization_count += 1
                
                if cluster_avg < 30:
                    underutilized_clusters.append({
                        "cluster_name": cluster["cluster_info"]["name"],
                        "avg_utilization": cluster_avg,
                        "potential_savings": cluster.get("cost_allocation", {}).get("total_cost", 0) * 0.3
                    })
                elif cluster_avg > 80:
                    overutilized_clusters.append({
                        "cluster_name": cluster["cluster_info"]["name"],
                        "avg_utilization": cluster_avg,
                        "scaling_recommended": True
                    })
        
        overall_efficiency = total_utilization / utilization_count if utilization_count > 0 else 0.0
        
        return {
            "overall_efficiency_score": round(overall_efficiency, 2),
            "efficiency_rating": self._get_efficiency_rating(overall_efficiency),
            "underutilized_clusters": underutilized_clusters,
            "overutilized_clusters": overutilized_clusters,
            "total_potential_savings": sum(c.get("potential_savings", 0) for c in underutilized_clusters)
        }
    
    def _identify_optimization_opportunities(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify optimization opportunities."""
        opportunities = []
        clusters = data.get("clusters", [])
        
        for cluster in clusters:
            cluster_name = cluster["cluster_info"]["name"]
            node_pools = cluster.get("node_pools", [])
            
            # Check for non-autoscaling node pools
            non_autoscaling_pools = [p for p in node_pools if not p.get("auto_scaling_enabled")]
            if non_autoscaling_pools:
                opportunities.append({
                    "type": "autoscaling",
                    "cluster": cluster_name,
                    "description": f"Enable autoscaling for {len(non_autoscaling_pools)} node pools",
                    "impact": "medium",
                    "effort": "low"
                })
            
            # Check for spot instance opportunities
            regular_pools = [p for p in node_pools if p.get("scale_set_priority") != "Spot"]
            if regular_pools:
                opportunities.append({
                    "type": "spot_instances",
                    "cluster": cluster_name,
                    "description": f"Consider spot instances for {len(regular_pools)} regular node pools",
                    "impact": "high",
                    "effort": "medium",
                    "potential_savings": "60-80%"
                })
            
            # Check for oversized instances
            large_instances = [p for p in node_pools if "D8" in p.get("vm_size", "") or "D16" in p.get("vm_size", "")]
            utilization = cluster.get("utilization_summary", {}).get("cluster_wide_utilization", {})
            avg_cpu = utilization.get("avg_cpu_utilization", 0.0)
            
            if large_instances and avg_cpu < 50:
                opportunities.append({
                    "type": "rightsizing",
                    "cluster": cluster_name,
                    "description": "Consider smaller instance types due to low CPU utilization",
                    "impact": "high",
                    "effort": "medium"
                })
        
        return opportunities
    
    def _generate_utilization_insights(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate utilization insights across all clusters."""
        clusters = data.get("clusters", [])
        
        cpu_utilizations = []
        memory_utilizations = []
        clusters_with_pressure = []
        
        for cluster in clusters:
            utilization_summary = cluster.get("utilization_summary", {})
            cluster_utilization = utilization_summary.get("cluster_wide_utilization", {})
            pressure_indicators = utilization_summary.get("resource_pressure_indicators", {})
            
            cpu_util = cluster_utilization.get("avg_cpu_utilization", 0.0)
            memory_util = cluster_utilization.get("avg_memory_utilization", 0.0)
            
            if cpu_util > 0:
                cpu_utilizations.append(cpu_util)
            if memory_util > 0:
                memory_utilizations.append(memory_util)
            
            if (pressure_indicators.get("cpu_pressure") or 
                pressure_indicators.get("memory_pressure")):
                clusters_with_pressure.append({
                    "cluster_name": cluster["cluster_info"]["name"],
                    "cpu_pressure": pressure_indicators.get("cpu_pressure", False),
                    "memory_pressure": pressure_indicators.get("memory_pressure", False)
                })
        
        return {
            "average_cpu_utilization": round(sum(cpu_utilizations) / len(cpu_utilizations), 2) if cpu_utilizations else 0.0,
            "average_memory_utilization": round(sum(memory_utilizations) / len(memory_utilizations), 2) if memory_utilizations else 0.0,
            "peak_cpu_utilization": max(cpu_utilizations) if cpu_utilizations else 0.0,
            "peak_memory_utilization": max(memory_utilizations) if memory_utilizations else 0.0,
            "clusters_with_resource_pressure": clusters_with_pressure,
            "utilization_distribution": {
                "low_utilization_clusters": len([u for u in cpu_utilizations if u < 30]),
                "medium_utilization_clusters": len([u for u in cpu_utilizations if 30 <= u <= 70]),
                "high_utilization_clusters": len([u for u in cpu_utilizations if u > 70])
            }
        }
    
    def _get_efficiency_rating(self, efficiency_score: float) -> str:
        """Get efficiency rating based on score."""
        if efficiency_score >= 70:
            return "excellent"
        elif efficiency_score >= 50:
            return "good"
        elif efficiency_score >= 30:
            return "fair"
        else:
            return "poor"
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "comprehensive_discovery"