"""Comprehensive discovery service for Phase 1 complete discovery."""

from typing import Dict, Any, List
import structlog
from datetime import datetime, timezone

from finops.discovery.base import BaseDiscoveryService
from finops.discovery.discovery_client import DiscoveryClient

logger = structlog.get_logger(__name__)


class DiscoveryService(BaseDiscoveryService):
    """
    Comprehensive discovery service that provides complete Phase 1 discovery data.
    
    This service integrates all discovery capabilities to provide:
    - Cluster information with costs and utilization
    - Node pools with detailed node information
    - Kubernetes resources with cost allocation
    - Storage with utilization and performance metrics
    - Network resources with load balancer costs
    - Azure resources related to clusters
    """
    
    def __init__(self, enhanced_client: DiscoveryClient, config: Dict[str, Any]):
        super().__init__(enhanced_client, config)
        self.include_detailed_metrics = config.get("include_detailed_metrics", True)
        self.include_cost_allocation = config.get("include_cost_allocation", True)
        self.include_recommendations = config.get("include_recommendations", False)
        
    async def discover(self) -> Dict[str, Any]:
        """
        Perform comprehensive discovery returning complete Phase 1 data structure.
        
        Returns:
            Dict containing the complete discovery structure:
            {
                "discovery_info": {...},
                "clusters": [...],
                "totals": {...}
            }
        """
        self.logger.info("Starting comprehensive discovery for Phase 1")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        try:
            # Get comprehensive cluster data
            discovery_data = await self.client.discover_comprehensive_cluster_data()
            
            # Enhance with additional analysis if requested
            if self.include_recommendations:
                discovery_data = await self._add_discovery_insights(discovery_data)
            
            # Add discovery metadata
            discovery_data["discovery_metadata"] = {
                "discovery_phase": "Phase 1 - Complete Discovery",
                "service_version": "1.0.0",
                "data_quality": self._assess_data_quality(discovery_data),
                "discovery_completeness": self._calculate_completeness(discovery_data),
                "next_recommended_actions": self._get_next_actions(discovery_data)
            }
            
            self.logger.info(
                "Comprehensive discovery completed",
                clusters=discovery_data["totals"]["total_clusters"],
                total_cost=discovery_data["totals"]["aggregated_costs"]["total_cost"],
                total_resources=discovery_data["totals"]["total_nodes"] + discovery_data["totals"]["total_pods"]
            )
            
            return discovery_data
            
        except Exception as e:
            self.logger.error("Comprehensive discovery failed", error=str(e))
            raise
    
    async def _add_discovery_insights(self, discovery_data: Dict[str, Any]) -> Dict[str, Any]:
        """Add insights and preliminary recommendations to discovery data."""
        insights = {
            "cost_insights": self._analyze_cost_patterns(discovery_data),
            "utilization_insights": self._analyze_utilization_patterns(discovery_data),
            "resource_insights": self._analyze_resource_patterns(discovery_data),
            "optimization_opportunities": self._identify_optimization_opportunities(discovery_data)
        }
        
        discovery_data["discovery_insights"] = insights
        return discovery_data
    
    def _analyze_cost_patterns(self, discovery_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cost patterns across clusters."""
        total_cost = discovery_data["totals"]["aggregated_costs"]["total_cost"]
        compute_cost = discovery_data["totals"]["aggregated_costs"]["compute_cost"]
        storage_cost = discovery_data["totals"]["aggregated_costs"]["storage_cost"]
        network_cost = discovery_data["totals"]["aggregated_costs"]["network_cost"]
        
        cost_insights = {
            "cost_distribution": {
                "compute_percentage": round((compute_cost / total_cost * 100), 2) if total_cost > 0 else 0,
                "storage_percentage": round((storage_cost / total_cost * 100), 2) if total_cost > 0 else 0,
                "network_percentage": round((network_cost / total_cost * 100), 2) if total_cost > 0 else 0
            },
            "cost_per_cluster": round(total_cost / len(discovery_data["clusters"]), 2) if discovery_data["clusters"] else 0,
            "cost_per_node": round(total_cost / discovery_data["totals"]["total_nodes"], 2) if discovery_data["totals"]["total_nodes"] > 0 else 0,
            "cost_trends": {
                "dominant_cost_driver": self._get_dominant_cost_driver(compute_cost, storage_cost, network_cost),
                "cost_efficiency_rating": self._calculate_cost_efficiency(discovery_data)
            }
        }
        
        return cost_insights
    
    def _analyze_utilization_patterns(self, discovery_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze utilization patterns across clusters."""
        utilization = discovery_data["totals"]["aggregated_utilization"]
        
        utilization_insights = {
            "overall_efficiency": {
                "cpu_efficiency": self._rate_utilization(utilization["avg_cpu_utilization"]),
                "memory_efficiency": self._rate_utilization(utilization["avg_memory_utilization"]),
                "storage_efficiency": self._rate_utilization(utilization["avg_storage_utilization"])
            },
            "resource_balance": self._assess_resource_balance(utilization),
            "scaling_opportunities": self._identify_scaling_opportunities(discovery_data),
            "waste_indicators": self._identify_waste_indicators(discovery_data)
        }
        
        return utilization_insights
    
    def _analyze_resource_patterns(self, discovery_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze resource allocation patterns."""
        clusters = discovery_data["clusters"]
        totals = discovery_data["totals"]
        
        resource_insights = {
            "cluster_distribution": {
                "avg_nodes_per_cluster": round(totals["total_nodes"] / len(clusters), 2) if clusters else 0,
                "avg_pods_per_cluster": round(totals["total_pods"] / len(clusters), 2) if clusters else 0,
                "avg_pods_per_node": round(totals["total_pods"] / totals["total_nodes"], 2) if totals["total_nodes"] > 0 else 0
            },
            "resource_consolidation_opportunities": self._identify_consolidation_opportunities(clusters),
            "governance_gaps": self._identify_governance_gaps(clusters),
            "scalability_assessment": self._assess_scalability(clusters)
        }
        
        return resource_insights
    
    def _identify_optimization_opportunities(self, discovery_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify immediate optimization opportunities."""
        opportunities = []
        
        # Cost optimization opportunities
        if discovery_data["totals"]["aggregated_costs"]["compute_cost"] > discovery_data["totals"]["aggregated_costs"]["total_cost"] * 0.8:
            opportunities.append({
                "type": "cost_optimization",
                "category": "compute_rightsizing",
                "title": "High Compute Cost Ratio Detected",
                "description": "Compute costs represent >80% of total costs, indicating potential rightsizing opportunities",
                "potential_impact": "15-30% cost reduction",
                "effort": "medium",
                "priority": "high"
            })
        
        # Utilization optimization opportunities
        avg_cpu = discovery_data["totals"]["aggregated_utilization"]["avg_cpu_utilization"]
        if avg_cpu < 40:
            opportunities.append({
                "type": "utilization_optimization",
                "category": "cpu_rightsizing",
                "title": "Low CPU Utilization Detected",
                "description": f"Average CPU utilization is {avg_cpu}%, indicating over-provisioning",
                "potential_impact": "20-40% cost reduction",
                "effort": "medium", 
                "priority": "high"
            })
        
        # Storage optimization opportunities
        storage_volumes = discovery_data["totals"]["total_storage_volumes"]
        if storage_volumes > discovery_data["totals"]["total_pods"] * 0.8:
            opportunities.append({
                "type": "storage_optimization",
                "category": "storage_cleanup",
                "title": "High Storage Volume Ratio",
                "description": "Large number of storage volumes relative to pods may indicate unused volumes",
                "potential_impact": "10-25% storage cost reduction",
                "effort": "low",
                "priority": "medium"
            })
        
        # Governance opportunities
        clusters_without_quotas = sum(1 for cluster in discovery_data["clusters"] 
                                    if not any(ns.get("resource_quotas", {}).get("quotas") 
                                             for ns in cluster.get("namespaces", [])))
        if clusters_without_quotas > 0:
            opportunities.append({
                "type": "governance",
                "category": "resource_quotas",
                "title": "Missing Resource Quotas",
                "description": f"{clusters_without_quotas} clusters lack resource quotas for cost control",
                "potential_impact": "Prevent cost overruns",
                "effort": "low",
                "priority": "medium"
            })
        
        return opportunities
    
    def _get_dominant_cost_driver(self, compute_cost: float, storage_cost: float, network_cost: float) -> str:
        """Identify the dominant cost driver."""
        costs = {"compute": compute_cost, "storage": storage_cost, "network": network_cost}
        return max(costs, key=costs.get)
    
    def _calculate_cost_efficiency(self, discovery_data: Dict[str, Any]) -> str:
        """Calculate overall cost efficiency rating."""
        # Simple heuristic based on utilization and cost distribution
        avg_utilization = (
            discovery_data["totals"]["aggregated_utilization"]["avg_cpu_utilization"] +
            discovery_data["totals"]["aggregated_utilization"]["avg_memory_utilization"]
        ) / 2
        
        if avg_utilization > 70:
            return "high"
        elif avg_utilization > 40:
            return "medium"
        else:
            return "low"
    
    def _rate_utilization(self, utilization: float) -> str:
        """Rate utilization efficiency."""
        if utilization > 75:
            return "high"
        elif utilization > 50:
            return "good"
        elif utilization > 25:
            return "fair"
        else:
            return "poor"
    
    def _assess_resource_balance(self, utilization: Dict[str, Any]) -> str:
        """Assess balance between CPU and memory utilization."""
        cpu_util = utilization["avg_cpu_utilization"]
        memory_util = utilization["avg_memory_utilization"]
        
        diff = abs(cpu_util - memory_util)
        if diff < 10:
            return "well_balanced"
        elif diff < 25:
            return "moderately_unbalanced"
        else:
            return "poorly_balanced"
    
    def _identify_scaling_opportunities(self, discovery_data: Dict[str, Any]) -> List[str]:
        """Identify scaling opportunities."""
        opportunities = []
        
        for cluster in discovery_data["clusters"]:
            for pool in cluster.get("node_pools", []):
                if not pool.get("auto_scaling_enabled"):
                    opportunities.append(f"Enable auto-scaling for node pool {pool['name']} in cluster {cluster['basic_info']['name']}")
        
        return opportunities[:5]  # Limit to top 5
    
    def _identify_waste_indicators(self, discovery_data: Dict[str, Any]) -> List[str]:
        """Identify waste indicators."""
        indicators = []
        
        # Check for clusters with very low utilization
        for cluster in discovery_data["clusters"]:
            utilization = cluster.get("utilization_summary", {})
            cpu_util = utilization.get("cpu", {}).get("average_utilization", 0)
            if cpu_util < 20:
                indicators.append(f"Cluster {cluster['basic_info']['name']} has very low CPU utilization ({cpu_util}%)")
        
        return indicators[:3]  # Limit to top 3
    
    def _identify_consolidation_opportunities(self, clusters: List[Dict[str, Any]]) -> List[str]:
        """Identify cluster consolidation opportunities."""
        opportunities = []
        
        small_clusters = [c for c in clusters if c.get("resource_counts", {}).get("total_nodes", 0) < 3]
        if len(small_clusters) > 1:
            opportunities.append(f"Consider consolidating {len(small_clusters)} small clusters to reduce overhead")
        
        return opportunities
    
    def _identify_governance_gaps(self, clusters: List[Dict[str, Any]]) -> List[str]:
        """Identify governance gaps."""
        gaps = []
        
        clusters_without_monitoring = sum(1 for c in clusters 
                                        if not c.get("utilization_summary", {}).get("metrics_availability"))
        if clusters_without_monitoring > 0:
            gaps.append(f"{clusters_without_monitoring} clusters lack proper monitoring setup")
        
        return gaps
    
    def _assess_scalability(self, clusters: List[Dict[str, Any]]) -> str:
        """Assess overall scalability."""
        auto_scaling_enabled = sum(1 for cluster in clusters
                                 for pool in cluster.get("node_pools", [])
                                 if pool.get("auto_scaling_enabled"))
        
        total_pools = sum(len(cluster.get("node_pools", [])) for cluster in clusters)
        
        if total_pools == 0:
            return "unknown"
        
        auto_scaling_ratio = auto_scaling_enabled / total_pools
        
        if auto_scaling_ratio > 0.8:
            return "excellent"
        elif auto_scaling_ratio > 0.5:
            return "good"
        elif auto_scaling_ratio > 0.2:
            return "fair"
        else:
            return "poor"
    
    def _assess_data_quality(self, discovery_data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess the quality of discovered data."""
        total_clusters = len(discovery_data["clusters"])
        successful_clusters = sum(1 for c in discovery_data["clusters"] 
                                if c.get("discovery_status") == "completed")
        
        clusters_with_costs = sum(1 for c in discovery_data["clusters"]
                                if c.get("cost_data", {}).get("total", 0) > 0)
        
        clusters_with_metrics = sum(1 for c in discovery_data["clusters"]
                                  if c.get("utilization_summary", {}).get("metrics_availability"))
        
        return {
            "overall_quality": "high" if successful_clusters == total_clusters else "medium",
            "discovery_success_rate": round((successful_clusters / total_clusters * 100), 2) if total_clusters > 0 else 0,
            "cost_data_coverage": round((clusters_with_costs / total_clusters * 100), 2) if total_clusters > 0 else 0,
            "metrics_data_coverage": round((clusters_with_metrics / total_clusters * 100), 2) if total_clusters > 0 else 0,
            "data_freshness": "current",
            "estimated_accuracy": "85-95%"
        }
    
    def _calculate_completeness(self, discovery_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate discovery completeness."""
        total_clusters = len(discovery_data["clusters"])
        
        # Check different aspects of completeness
        infrastructure_complete = sum(1 for c in discovery_data["clusters"]
                                    if c.get("node_pools")) / total_clusters if total_clusters > 0 else 0
        
        cost_complete = sum(1 for c in discovery_data["clusters"]
                          if c.get("cost_data")) / total_clusters if total_clusters > 0 else 0
        
        k8s_complete = sum(1 for c in discovery_data["clusters"]
                         if c.get("namespaces")) / total_clusters if total_clusters > 0 else 0
        
        overall_completeness = (infrastructure_complete + cost_complete + k8s_complete) / 3
        
        return {
            "overall_completeness_percent": round(overall_completeness * 100, 2),
            "infrastructure_completeness": round(infrastructure_complete * 100, 2),
            "cost_data_completeness": round(cost_complete * 100, 2),
            "kubernetes_data_completeness": round(k8s_complete * 100, 2),
            "completeness_rating": "excellent" if overall_completeness > 0.9 else 
                                 "good" if overall_completeness > 0.7 else "needs_improvement"
        }
    
    def _get_next_actions(self, discovery_data: Dict[str, Any]) -> List[str]:
        """Get recommended next actions based on discovery results."""
        actions = []
        
        # Based on data quality
        data_quality = discovery_data.get("discovery_metadata", {}).get("data_quality", {})
        if data_quality.get("metrics_data_coverage", 0) < 80:
            actions.append("Set up Azure Monitor integration for detailed utilization metrics")
        
        # Based on optimization opportunities
        insights = discovery_data.get("discovery_insights", {})
        opportunities = insights.get("optimization_opportunities", [])
        high_priority_ops = [op for op in opportunities if op.get("priority") == "high"]
        
        if high_priority_ops:
            actions.append(f"Address {len(high_priority_ops)} high-priority optimization opportunities")
        
        # Based on governance gaps
        governance_gaps = insights.get("resource_insights", {}).get("governance_gaps", [])
        if governance_gaps:
            actions.append("Implement resource governance policies and quotas")
        
        # Always recommend moving to analytics phase
        actions.append("Proceed to Phase 2: Analytics for detailed cost and utilization analysis")
        
        return actions[:5]  # Limit to top 5 actions
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "comprehensive_discovery_phase1"    