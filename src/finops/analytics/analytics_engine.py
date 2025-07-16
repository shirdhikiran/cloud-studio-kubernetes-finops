# src/finops/analytics/analytics_engine.py
"""
Phase 2 Analytics Engine - Comprehensive FinOps Analytics
Processes Phase 1 discovery data to generate actionable insights
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import structlog
import json
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import statistics
import numpy as np
from collections import defaultdict

from finops.analytics.utilization_analytics import UtilizationAnalytics
from finops.core.exceptions import AnalyticsException
from finops.analytics.cost_analytics import CostAnalytics

from finops.analytics.waste_analytics import WasteAnalytics
from finops.analytics.rightsizing_analytics import RightsizingAnalytics
from finops.analytics.benchmarking_analytics import BenchmarkingAnalytics

logger = structlog.get_logger(__name__)


class AnalyticsSeverity(str, Enum):
    """Analytics severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class AnalyticsCategory(str, Enum):
    """Analytics categories"""
    COST_OPTIMIZATION = "cost_optimization"
    RESOURCE_EFFICIENCY = "resource_efficiency"
    WASTE_REDUCTION = "waste_reduction"
    CAPACITY_PLANNING = "capacity_planning"
    GOVERNANCE = "governance"
    SECURITY = "security"
    COMPLIANCE = "compliance"


@dataclass
class AnalyticsInsight:
    """Individual analytics insight"""
    id: str
    title: str
    description: str
    category: AnalyticsCategory
    severity: AnalyticsSeverity
    impact_score: float  # 0-100
    confidence: float   # 0-100
    potential_savings: float  # USD
    affected_resources: List[str]
    recommendations: List[str]
    metrics: Dict[str, Any]
    created_at: datetime


@dataclass
class AnalyticsReport:
    """Complete analytics report"""
    report_id: str
    cluster_name: str
    analysis_timestamp: datetime
    analysis_period: Dict[str, str]
    
    # Executive Summary
    executive_summary: Dict[str, Any]
    
    # Key Metrics
    key_metrics: Dict[str, Any]
    
    # Insights by Category
    insights: List[AnalyticsInsight]
    
    # Detailed Analysis
    cost_analysis: Dict[str, Any]
    utilization_analysis: Dict[str, Any]
    waste_analysis: Dict[str, Any]
    rightsizing_analysis: Dict[str, Any]
    forecasting_analysis: Dict[str, Any]
    benchmarking_analysis: Dict[str, Any]
    
    # Action Items
    priority_actions: List[Dict[str, Any]]
    
    # Metadata
    metadata: Dict[str, Any]


class AnalyticsEngine:
    """
    Phase 2 Analytics Engine
    Transforms Phase 1 discovery data into actionable FinOps insights
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(engine="analytics")
        
        # Initialize analytics components
        self.cost_analytics = CostAnalytics(config)
        self.utilization_analytics = UtilizationAnalytics(config)
        self.waste_analytics = WasteAnalytics(config)
        self.rightsizing_analytics = RightsizingAnalytics(config)
        self.benchmarking_analytics = BenchmarkingAnalytics(config)
        
        # Analytics thresholds
        self.thresholds = {
            'waste_cpu_threshold': config.get('waste_cpu_threshold', 10.0),  # %
            'waste_memory_threshold': config.get('waste_memory_threshold', 10.0),  # %
            'overprovisioning_threshold': config.get('overprovisioning_threshold', 50.0),  # %
            'cost_anomaly_threshold': config.get('cost_anomaly_threshold', 20.0),  # %
            'efficiency_target': config.get('efficiency_target', 70.0),  # %
            'high_cost_threshold': config.get('high_cost_threshold', 1000.0),  # USD
        }
        
    async def analyze_discovery_data(self, discovery_data: Dict[str, Any]) -> AnalyticsReport:
        """
        Main analytics entry point - Process Phase 1 discovery data
        """
        self.logger.info("Starting Phase 2 Analytics processing")
        
        try:
            # Extract metadata
            discovery_metadata = discovery_data.get("discovery_metadata", {})
            clusters = discovery_data.get("clusters", [])
            
            if not clusters:
                raise AnalyticsException("No clusters found in discovery data")
            
            # Process each cluster
            cluster_reports = []
            for cluster_data in clusters:
                cluster_report = await self._analyze_cluster(cluster_data, discovery_metadata)
                cluster_reports.append(cluster_report)
            
            # Generate multi-cluster report if multiple clusters
            if len(cluster_reports) == 1:
                final_report = cluster_reports[0]
            else:
                final_report = await self._generate_multi_cluster_report(cluster_reports, discovery_metadata)
            
            # Add analytics metadata
            final_report.metadata.update({
                "analytics_version": "2.0.0",
                "analysis_engine": "comprehensive_finops",
                "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                "clusters_analyzed": len(clusters),
                "total_insights_generated": len(final_report.insights)
            })
            
            self.logger.info(
                "Phase 2 Analytics completed",
                clusters_analyzed=len(clusters),
                insights_generated=len(final_report.insights),
                total_potential_savings=final_report.executive_summary.get("total_potential_savings", 0)
            )
            
            return final_report
            
        except Exception as e:
            self.logger.error(f"Analytics processing failed: {e}")
            raise AnalyticsException(f"Analytics processing failed: {e}")
    
    async def _analyze_cluster(self, cluster_data: Dict[str, Any], 
                             discovery_metadata: Dict[str, Any]) -> AnalyticsReport:
        """Analyze individual cluster data"""
        
        cluster_info = cluster_data.get("cluster_info", {})
        cluster_name = cluster_info.get("name", "unknown-cluster")
        
        self.logger.info(f"Analyzing cluster: {cluster_name}")
        
        # Initialize report
        report = AnalyticsReport(
            report_id=f"analytics_{cluster_name}_{int(datetime.now().timestamp())}",
            cluster_name=cluster_name,
            analysis_timestamp=datetime.now(timezone.utc),
            analysis_period={
                "start_date": discovery_metadata.get("timestamp", ""),
                "analysis_scope": f"{discovery_metadata.get('cost_analysis_days', 30)} days"
            },
            executive_summary={},
            key_metrics={},
            insights=[],
            cost_analysis={},
            utilization_analysis={},
            waste_analysis={},
            rightsizing_analysis={},
            forecasting_analysis={},
            benchmarking_analysis={},
            priority_actions=[],
            metadata={}
        )
        
        # Run all analytics modules
        try:
            # 1. Cost Analytics
            report.cost_analysis = await self.cost_analytics.analyze_cluster_costs(cluster_data)
            
            # 2. Utilization Analytics  
            report.utilization_analysis = await self.utilization_analytics.analyze_cluster_utilization(cluster_data)
            
            # 3. Waste Analytics
            report.waste_analysis = await self.waste_analytics.analyze_cluster_waste(cluster_data)
            
            # 4. Right-sizing Analytics
            report.rightsizing_analysis = await self.rightsizing_analytics.analyze_cluster_rightsizing(cluster_data)
            
                    
            # 6. Benchmarking Analytics
            report.benchmarking_analysis = await self.benchmarking_analytics.analyze_cluster_benchmarking(cluster_data)
            
            # Generate insights from all analytics
            report.insights = await self._generate_insights(report)
            
            # Generate executive summary
            report.executive_summary = await self._generate_executive_summary(report)
            
            # Generate key metrics
            report.key_metrics = await self._generate_key_metrics(report)
            
            # Generate priority actions
            report.priority_actions = await self._generate_priority_actions(report)
            
        except Exception as e:
            self.logger.error(f"Error analyzing cluster {cluster_name}: {e}")
            # Add error to metadata but continue
            report.metadata["analysis_errors"] = str(e)
        
        return report
    
    async def _generate_insights(self, report: AnalyticsReport) -> List[AnalyticsInsight]:
        """Generate actionable insights from all analytics"""
        
        insights = []
        insight_id = 0
        
        # Cost insights
        cost_analysis = report.cost_analysis
        if cost_analysis.get("total_monthly_cost", 0) > self.thresholds["high_cost_threshold"]:
            insights.append(AnalyticsInsight(
                id=f"COST_{insight_id:03d}",
                title="High Monthly Spend Detected",
                description=f"Cluster monthly cost of ${cost_analysis.get('total_monthly_cost', 0):.2f} exceeds threshold",
                category=AnalyticsCategory.COST_OPTIMIZATION,
                severity=AnalyticsSeverity.HIGH,
                impact_score=85.0,
                confidence=95.0,
                potential_savings=cost_analysis.get("optimization_opportunities", {}).get("total_savings", 0),
                affected_resources=[report.cluster_name],
                recommendations=cost_analysis.get("recommendations", []),
                metrics={"monthly_cost": cost_analysis.get("total_monthly_cost", 0)},
                created_at=datetime.now(timezone.utc)
            ))
            insight_id += 1
        
        # Utilization insights
        utilization = report.utilization_analysis
        avg_cpu_util = utilization.get("cluster_utilization", {}).get("average_cpu_utilization", 0)
        if avg_cpu_util < self.thresholds["efficiency_target"]:
            severity = AnalyticsSeverity.CRITICAL if avg_cpu_util < 30 else AnalyticsSeverity.HIGH
            insights.append(AnalyticsInsight(
                id=f"UTIL_{insight_id:03d}",
                title="Low CPU Utilization",
                description=f"Average CPU utilization of {avg_cpu_util:.1f}% is below efficiency target",
                category=AnalyticsCategory.RESOURCE_EFFICIENCY,
                severity=severity,
                impact_score=min(90.0, (self.thresholds["efficiency_target"] - avg_cpu_util) * 2),
                confidence=90.0,
                potential_savings=utilization.get("rightsizing_savings", 0),
                affected_resources=utilization.get("underutilized_nodes", []),
                recommendations=utilization.get("recommendations", []),
                metrics={"cpu_utilization": avg_cpu_util},
                created_at=datetime.now(timezone.utc)
            ))
            insight_id += 1
        
        # Waste insights
        waste_analysis = report.waste_analysis
        total_waste_cost = waste_analysis.get("total_waste_cost", 0)
        if total_waste_cost > 0:
            insights.append(AnalyticsInsight(
                id=f"WASTE_{insight_id:03d}",
                title="Resource Waste Identified",
                description=f"${total_waste_cost:.2f} monthly waste from idle and overprovisioned resources",
                category=AnalyticsCategory.WASTE_REDUCTION,
                severity=AnalyticsSeverity.HIGH if total_waste_cost > 500 else AnalyticsSeverity.MEDIUM,
                impact_score=min(95.0, total_waste_cost / 10),
                confidence=85.0,
                potential_savings=total_waste_cost,
                affected_resources=waste_analysis.get("wasteful_resources", []),
                recommendations=waste_analysis.get("recommendations", []),
                metrics={"waste_cost": total_waste_cost},
                created_at=datetime.now(timezone.utc)
            ))
            insight_id += 1
        
        # Right-sizing insights
        rightsizing = report.rightsizing_analysis
        rightsizing_opportunities = rightsizing.get("optimization_opportunities", [])
        if rightsizing_opportunities:
            total_rightsizing_savings = sum(
                opp.get("potential_savings", 0) for opp in rightsizing_opportunities
            )
            insights.append(AnalyticsInsight(
                id=f"SIZE_{insight_id:03d}",
                title="Right-sizing Opportunities",
                description=f"{len(rightsizing_opportunities)} resources can be right-sized for ${total_rightsizing_savings:.2f} savings",
                category=AnalyticsCategory.RESOURCE_EFFICIENCY,
                severity=AnalyticsSeverity.MEDIUM,
                impact_score=min(80.0, total_rightsizing_savings / 20),
                confidence=80.0,
                potential_savings=total_rightsizing_savings,
                affected_resources=[opp.get("resource_id", "") for opp in rightsizing_opportunities],
                recommendations=rightsizing.get("recommendations", []),
                metrics={"rightsizing_opportunities": len(rightsizing_opportunities)},
                created_at=datetime.now(timezone.utc)
            ))
            insight_id += 1
        
        # Forecasting insights
        forecasting = report.forecasting_analysis
        cost_trend = forecasting.get("cost_trend", {}).get("trend_direction", "stable")
        if cost_trend == "increasing":
            insights.append(AnalyticsInsight(
                id=f"TREND_{insight_id:03d}",
                title="Increasing Cost Trend",
                description="Cost trend is increasing - proactive optimization needed",
                category=AnalyticsCategory.CAPACITY_PLANNING,
                severity=AnalyticsSeverity.MEDIUM,
                impact_score=60.0,
                confidence=75.0,
                potential_savings=forecasting.get("optimization_impact", 0),
                affected_resources=[report.cluster_name],
                recommendations=forecasting.get("recommendations", []),
                metrics={"trend_direction": cost_trend},
                created_at=datetime.now(timezone.utc)
            ))
            insight_id += 1
        
        # Sort insights by impact score
        insights.sort(key=lambda x: x.impact_score, reverse=True)
        
        return insights
    
    async def _generate_executive_summary(self, report: AnalyticsReport) -> Dict[str, Any]:
        """Generate executive summary"""
        
        # Calculate totals
        total_monthly_cost = report.cost_analysis.get("total_monthly_cost", 0)
        total_potential_savings = sum(insight.potential_savings for insight in report.insights)
        savings_percentage = (total_potential_savings / total_monthly_cost * 100) if total_monthly_cost > 0 else 0
        
        # Count insights by severity
        insight_counts = defaultdict(int)
        for insight in report.insights:
            insight_counts[insight.severity.value] += 1
        
        # Efficiency scores
        cpu_efficiency = report.utilization_analysis.get("cluster_utilization", {}).get("average_cpu_utilization", 0)
        memory_efficiency = report.utilization_analysis.get("cluster_utilization", {}).get("average_memory_utilization", 0)
        overall_efficiency = (cpu_efficiency + memory_efficiency) / 2
        
        # Top opportunities
        top_opportunities = []
        if report.insights:
            top_opportunities = [
                {
                    "title": insight.title,
                    "potential_savings": insight.potential_savings,
                    "category": insight.category.value
                }
                for insight in report.insights[:5]  # Top 5
            ]
        
        return {
            "cluster_name": report.cluster_name,
            "analysis_date": report.analysis_timestamp.strftime("%Y-%m-%d"),
            "total_monthly_cost": total_monthly_cost,
            "total_potential_savings": total_potential_savings,
            "savings_percentage": round(savings_percentage, 1),
            "efficiency_score": round(overall_efficiency, 1),
            "insights_summary": {
                "total_insights": len(report.insights),
                "critical_insights": insight_counts.get("critical", 0),
                "high_priority_insights": insight_counts.get("high", 0),
                "medium_priority_insights": insight_counts.get("medium", 0),
                "low_priority_insights": insight_counts.get("low", 0)
            },
            "top_opportunities": top_opportunities,
            "health_status": self._calculate_cluster_health(report),
            "recommended_actions": len(report.priority_actions)
        }
    
    async def _generate_key_metrics(self, report: AnalyticsReport) -> Dict[str, Any]:
        """Generate key metrics dashboard"""
        
        return {
            "cost_metrics": {
                "total_monthly_cost": report.cost_analysis.get("total_monthly_cost", 0),
                "cost_per_pod": report.cost_analysis.get("cost_per_pod", 0),
                "cost_per_cpu_hour": report.cost_analysis.get("cost_per_cpu_hour", 0),
                "cost_trend": report.forecasting_analysis.get("cost_trend", {}).get("trend_direction", "stable"),
                "cost_variance": report.cost_analysis.get("cost_variance", 0)
            },
            "utilization_metrics": {
                "cpu_utilization": report.utilization_analysis.get("cluster_utilization", {}).get("average_cpu_utilization", 0),
                "memory_utilization": report.utilization_analysis.get("cluster_utilization", {}).get("average_memory_utilization", 0),
                "storage_utilization": report.utilization_analysis.get("cluster_utilization", {}).get("average_storage_utilization", 0),
                "network_utilization": report.utilization_analysis.get("cluster_utilization", {}).get("average_network_utilization", 0)
            },
            "efficiency_metrics": {
                "resource_efficiency_score": report.utilization_analysis.get("efficiency_score", 0),
                "cost_efficiency_score": report.cost_analysis.get("efficiency_score", 0),
                "waste_percentage": report.waste_analysis.get("waste_percentage", 0),
                "rightsizing_opportunities": len(report.rightsizing_analysis.get("optimization_opportunities", []))
            },
            "capacity_metrics": {
                "total_nodes": report.utilization_analysis.get("node_count", 0),
                "total_pods": report.utilization_analysis.get("pod_count", 0),
                "cluster_capacity_used": report.utilization_analysis.get("capacity_utilization", 0),
                "scaling_recommendations": len(report.rightsizing_analysis.get("scaling_recommendations", []))
            },
            "savings_metrics": {
                "total_potential_savings": sum(insight.potential_savings for insight in report.insights),
                "waste_savings": report.waste_analysis.get("total_waste_cost", 0),
                "rightsizing_savings": sum(
                    opp.get("potential_savings", 0) 
                    for opp in report.rightsizing_analysis.get("optimization_opportunities", [])
                ),
                "quick_wins": len([i for i in report.insights if i.confidence > 85 and i.potential_savings > 100])
            }
        }
    
    async def _generate_priority_actions(self, report: AnalyticsReport) -> List[Dict[str, Any]]:
        """Generate prioritized action items"""
        
        actions = []
        
        # High-impact, high-confidence insights become priority actions
        for insight in report.insights:
            if insight.impact_score >= 70 and insight.confidence >= 80:
                actions.append({
                    "id": f"ACTION_{insight.id}",
                    "title": f"Address: {insight.title}",
                    "description": insight.description,
                    "priority": "high" if insight.severity in [AnalyticsSeverity.CRITICAL, AnalyticsSeverity.HIGH] else "medium",
                    "estimated_savings": insight.potential_savings,
                    "effort_level": self._estimate_effort_level(insight),
                    "timeline": self._estimate_timeline(insight),
                    "affected_resources": insight.affected_resources,
                    "recommendations": insight.recommendations,
                    "category": insight.category.value,
                    "roi_score": insight.potential_savings / max(1, self._estimate_effort_level(insight))
                })
        
        # Sort by ROI score
        actions.sort(key=lambda x: x["roi_score"], reverse=True)
        
        return actions[:10]  # Top 10 actions
    
    def _calculate_cluster_health(self, report: AnalyticsReport) -> str:
        """Calculate overall cluster health"""
        
        # Factors for health calculation
        efficiency_score = (
            report.utilization_analysis.get("cluster_utilization", {}).get("average_cpu_utilization", 0) +
            report.utilization_analysis.get("cluster_utilization", {}).get("average_memory_utilization", 0)
        ) / 2
        
        critical_issues = len([i for i in report.insights if i.severity == AnalyticsSeverity.CRITICAL])
        high_issues = len([i for i in report.insights if i.severity == AnalyticsSeverity.HIGH])
        
        waste_percentage = report.waste_analysis.get("waste_percentage", 0)
        
        # Health calculation
        health_score = 100
        health_score -= critical_issues * 20  # Critical issues heavily penalize
        health_score -= high_issues * 10      # High issues moderately penalize
        health_score -= max(0, (50 - efficiency_score))  # Low efficiency penalizes
        health_score -= waste_percentage       # Waste penalizes
        
        health_score = max(0, min(100, health_score))
        
        if health_score >= 80:
            return "excellent"
        elif health_score >= 60:
            return "good"
        elif health_score >= 40:
            return "fair"
        else:
            return "poor"
    
    def _estimate_effort_level(self, insight: AnalyticsInsight) -> int:
        """Estimate effort level (1-5 scale)"""
        
        effort_map = {
            AnalyticsCategory.WASTE_REDUCTION: 2,  # Usually easy
            AnalyticsCategory.RESOURCE_EFFICIENCY: 3,  # Moderate
            AnalyticsCategory.COST_OPTIMIZATION: 3,  # Moderate
            AnalyticsCategory.CAPACITY_PLANNING: 4,  # Complex
            AnalyticsCategory.GOVERNANCE: 5,  # Most complex
        }
        
        return effort_map.get(insight.category, 3)
    
    def _estimate_timeline(self, insight: AnalyticsInsight) -> str:
        """Estimate implementation timeline"""
        
        effort = self._estimate_effort_level(insight)
        
        if effort <= 2:
            return "1-2 weeks"
        elif effort <= 3:
            return "2-4 weeks"
        elif effort <= 4:
            return "1-2 months"
        else:
            return "2-3 months"
    
    async def _generate_multi_cluster_report(self, cluster_reports: List[AnalyticsReport], 
                                           discovery_metadata: Dict[str, Any]) -> AnalyticsReport:
        """Generate consolidated multi-cluster report"""
        
        # This would aggregate multiple cluster reports
        # For now, return the first cluster report
        # TODO: Implement proper multi-cluster aggregation
        return cluster_reports[0]
    
    def export_report_to_dict(self, report: AnalyticsReport) -> Dict[str, Any]:
        """Export analytics report to dictionary"""
        
        return {
            "analytics_report": {
                "report_metadata": {
                    "report_id": report.report_id,
                    "cluster_name": report.cluster_name,
                    "analysis_timestamp": report.analysis_timestamp.isoformat(),
                    "analysis_period": report.analysis_period,
                    "analytics_version": "2.0.0"
                },
                "executive_summary": report.executive_summary,
                "key_metrics": report.key_metrics,
                "insights": [
                    {
                        "id": insight.id,
                        "title": insight.title,
                        "description": insight.description,
                        "category": insight.category.value,
                        "severity": insight.severity.value,
                        "impact_score": insight.impact_score,
                        "confidence": insight.confidence,
                        "potential_savings": insight.potential_savings,
                        "affected_resources": insight.affected_resources,
                        "recommendations": insight.recommendations,
                        "metrics": insight.metrics,
                        "created_at": insight.created_at.isoformat()
                    }
                    for insight in report.insights
                ],
                "detailed_analysis": {
                    "cost_analysis": report.cost_analysis,
                    "utilization_analysis": report.utilization_analysis,
                    "waste_analysis": report.waste_analysis,
                    "rightsizing_analysis": report.rightsizing_analysis,
                    "forecasting_analysis": report.forecasting_analysis,
                    "benchmarking_analysis": report.benchmarking_analysis
                },
                "priority_actions": report.priority_actions,
                "metadata": report.metadata
            }
        }