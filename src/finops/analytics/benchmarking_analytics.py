# src/finops/analytics/benchmarking_analytics.py
"""
Enhanced Phase 2 Benchmarking Analytics
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
import structlog

logger = structlog.get_logger(__name__)


class BenchmarkingAnalytics:
    """Enhanced Benchmarking Analytics for Phase 2"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(analytics="benchmarking")
        
        # Industry benchmarks
        self.industry_benchmarks = {
            "cost_per_pod_monthly": 150.0,  # USD
            "cost_per_cpu_hour": 0.05,      # USD
            "cost_per_gb_memory_hour": 0.01, # USD
            "cpu_utilization_target": 65.0,  # %
            "memory_utilization_target": 70.0, # %
            "waste_percentage_threshold": 15.0, # %
            "efficiency_score_target": 75.0,   # %
            "cost_optimization_potential": 20.0 # %
        }
    
    async def analyze_cluster_benchmarks(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced benchmarking analysis"""
        
        cluster_name = cluster_data.get("cluster_info", {}).get("name", "unknown")

        
        analysis = {
            "cluster_name": cluster_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "performance_scores": {},
            "industry_comparison": {},
            "peer_comparison": {},
            "optimization_potential": {},
            "maturity_assessment": {},
            "recommendations": []
        }
        
        try:
            node_rg_costs = cluster_data.get("node_resource_group_costs", {})
            
            if "error" not in node_rg_costs:
                # 1. Performance scoring
                analysis["performance_scores"] = await self._calculate_performance_scores(
                    cluster_data, node_rg_costs
                )
                
                # 2. Industry comparison
                analysis["industry_comparison"] = await self._compare_with_industry(
                    cluster_data, node_rg_costs
                )
                
                # 3. Optimization potential
                analysis["optimization_potential"] = await self._assess_optimization_potential(
                    cluster_data, node_rg_costs
                )
                
                # 4. Maturity assessment
                analysis["maturity_assessment"] = await self._assess_finops_maturity(
                    cluster_data, node_rg_costs
                )
                
                # 5. Generate recommendations
                analysis["recommendations"] = await self._generate_benchmark_recommendations(
                    analysis
                )
                
                self.logger.info(f"Benchmarking analysis completed for {cluster_name}")
            else:
                self.logger.warning(f"Skipping benchmarking for {cluster_name} - no cost data")
                analysis["error"] = "No valid cost data available"
                
        except Exception as e:
            self.logger.error(f"Benchmarking analysis failed for {cluster_name}: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    async def _calculate_performance_scores(self, cluster_data: Dict[str, Any], 
                                          node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate performance scores against benchmarks"""
        
        scores = {
            "cost_efficiency_score": 0.0,
            "resource_efficiency_score": 0.0,
            "operational_efficiency_score": 0.0,
            "overall_score": 0.0,
            "scoring_details": {}
        }
        
        total_cost = node_rg_costs.get("total_cost", 0.0)
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        pods = k8s_resources.get("pods", [])
        
        # Cost efficiency scoring
        cost_scores = []
        
        # Cost per pod
        if pods and total_cost > 0:
            cost_per_pod = total_cost / len(pods)
            benchmark_cost_per_pod = self.industry_benchmarks["cost_per_pod_monthly"]
            
            if cost_per_pod <= benchmark_cost_per_pod * 0.8:  # 20% better
                pod_score = 100.0
            elif cost_per_pod <= benchmark_cost_per_pod:
                pod_score = 80.0
            elif cost_per_pod <= benchmark_cost_per_pod * 1.2:  # 20% worse
                pod_score = 60.0
            else:
                pod_score = 40.0
            
            cost_scores.append(pod_score)
            scores["scoring_details"]["cost_per_pod"] = {
                "current": cost_per_pod,
                "benchmark": benchmark_cost_per_pod,
                "score": pod_score,
                "performance": "excellent" if pod_score >= 90 else "good" if pod_score >= 70 else "needs_improvement"
            }
        
        # Resource efficiency scoring
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        resource_scores = []
        
        # CPU utilization scoring
        if "cluster_cpu_utilization" in container_insights:
            cpu_data = container_insights["cluster_cpu_utilization"]
            data_points = cpu_data.get("data_points", [])
            
            if data_points:
                cpu_values = [point.get("cpu_utilization_percentage", 0) for point in data_points]
                avg_cpu_util = sum(cpu_values) / len(cpu_values)
                target_cpu_util = self.industry_benchmarks["cpu_utilization_target"]
                
                # Score based on how close to target
                if abs(avg_cpu_util - target_cpu_util) <= 5:  # Within 5%
                    cpu_score = 100.0
                elif abs(avg_cpu_util - target_cpu_util) <= 10:  # Within 10%
                    cpu_score = 80.0
                elif abs(avg_cpu_util - target_cpu_util) <= 20:  # Within 20%
                    cpu_score = 60.0
                else:
                    cpu_score = 40.0
                
                resource_scores.append(cpu_score)
                scores["scoring_details"]["cpu_utilization"] = {
                    "current": avg_cpu_util,
                    "benchmark": target_cpu_util,
                    "score": cpu_score,
                    "performance": "excellent" if cpu_score >= 90 else "good" if cpu_score >= 70 else "needs_improvement"
                }
        
        # Memory utilization scoring
        if "cluster_memory_utilization" in container_insights:
            memory_data = container_insights["cluster_memory_utilization"]
            data_points = memory_data.get("data_points", [])
            
            if data_points:
                memory_values = [point.get("memory_utilization_percentage", 0) for point in data_points]
                avg_memory_util = sum(memory_values) / len(memory_values)
                target_memory_util = self.industry_benchmarks["memory_utilization_target"]
                
                # Score based on how close to target
                if abs(avg_memory_util - target_memory_util) <= 5:  # Within 5%
                    memory_score = 100.0
                elif abs(avg_memory_util - target_memory_util) <= 10:  # Within 10%
                    memory_score = 80.0
                elif abs(avg_memory_util - target_memory_util) <= 20:  # Within 20%
                    memory_score = 60.0
                else:
                    memory_score = 40.0
                
                resource_scores.append(memory_score)
                scores["scoring_details"]["memory_utilization"] = {
                    "current": avg_memory_util,
                    "benchmark": target_memory_util,
                    "score": memory_score,
                    "performance": "excellent" if memory_score >= 90 else "good" if memory_score >= 70 else "needs_improvement"
                }
        
        # Operational efficiency scoring
        operational_scores = []
        
        # Node pool configuration
        node_pools = cluster_data.get("node_pools", [])
        if node_pools:
            # Check for auto-scaling
            auto_scaling_enabled = any(pool.get("auto_scaling_enabled", False) for pool in node_pools)
            auto_scaling_score = 100.0 if auto_scaling_enabled else 50.0
            operational_scores.append(auto_scaling_score)
            
            scores["scoring_details"]["auto_scaling"] = {
                "enabled": auto_scaling_enabled,
                "score": auto_scaling_score,
                "performance": "excellent" if auto_scaling_score >= 90 else "needs_improvement"
            }
        
        # Calculate final scores
        if cost_scores:
            scores["cost_efficiency_score"] = sum(cost_scores) / len(cost_scores)
        
        if resource_scores:
            scores["resource_efficiency_score"] = sum(resource_scores) / len(resource_scores)
        
        if operational_scores:
            scores["operational_efficiency_score"] = sum(operational_scores) / len(operational_scores)
        
        # Overall score
        all_scores = [
            scores["cost_efficiency_score"],
            scores["resource_efficiency_score"],
            scores["operational_efficiency_score"]
        ]
        
        valid_scores = [score for score in all_scores if score > 0]
        if valid_scores:
            scores["overall_score"] = sum(valid_scores) / len(valid_scores)
        
        return scores
    
    async def _compare_with_industry(self, cluster_data: Dict[str, Any], 
                                   node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Compare with industry benchmarks"""
        
        comparison = {
            "cost_metrics": {},
            "efficiency_metrics": {},
            "performance_summary": {
                "better_than_industry": [],
                "meets_industry_standard": [],
                "below_industry_standard": []
            }
        }
        
        total_cost = node_rg_costs.get("total_cost", 0.0)
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        pods = k8s_resources.get("pods", [])
        
        # Cost per pod comparison
        if pods and total_cost > 0:
            cost_per_pod = total_cost / len(pods)
            benchmark = self.industry_benchmarks["cost_per_pod_monthly"]
            
            comparison["cost_metrics"]["cost_per_pod"] = {
                "current": cost_per_pod,
                "industry_benchmark": benchmark,
                "difference": cost_per_pod - benchmark,
                "percentage_diff": ((cost_per_pod - benchmark) / benchmark) * 100,
                "performance": "better" if cost_per_pod < benchmark else "worse"
            }
            
            if cost_per_pod <= benchmark * 0.9:  # 10% better
                comparison["performance_summary"]["better_than_industry"].append("cost_per_pod")
            elif cost_per_pod <= benchmark * 1.1:  # Within 10%
                comparison["performance_summary"]["meets_industry_standard"].append("cost_per_pod")
            else:
                comparison["performance_summary"]["below_industry_standard"].append("cost_per_pod")
        
        # Resource utilization comparison
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # CPU utilization
        if "cluster_cpu_utilization" in container_insights:
            cpu_data = container_insights["cluster_cpu_utilization"]
            data_points = cpu_data.get("data_points", [])
            
            if data_points:
                cpu_values = [point.get("cpu_utilization_percentage", 0) for point in data_points]
                avg_cpu_util = sum(cpu_values) / len(cpu_values)
                benchmark = self.industry_benchmarks["cpu_utilization_target"]
                
                comparison["efficiency_metrics"]["cpu_utilization"] = {
                    "current": avg_cpu_util,
                    "industry_benchmark": benchmark,
                    "difference": avg_cpu_util - benchmark,
                    "performance": "better" if abs(avg_cpu_util - benchmark) <= 5 else "needs_improvement"
                }
                
                if abs(avg_cpu_util - benchmark) <= 5:  # Within 5%
                    comparison["performance_summary"]["meets_industry_standard"].append("cpu_utilization")
                elif avg_cpu_util < benchmark - 5:  # Under-utilized
                    comparison["performance_summary"]["below_industry_standard"].append("cpu_utilization")
                else:  # Over-utilized
                    comparison["performance_summary"]["below_industry_standard"].append("cpu_utilization")
        
        # Memory utilization
        if "cluster_memory_utilization" in container_insights:
            memory_data = container_insights["cluster_memory_utilization"]
            data_points = memory_data.get("data_points", [])
            
            if data_points:
                memory_values = [point.get("memory_utilization_percentage", 0) for point in data_points]
                avg_memory_util = sum(memory_values) / len(memory_values)
                benchmark = self.industry_benchmarks["memory_utilization_target"]
                
                comparison["efficiency_metrics"]["memory_utilization"] = {
                    "current": avg_memory_util,
                    "industry_benchmark": benchmark,
                    "difference": avg_memory_util - benchmark,
                    "performance": "better" if abs(avg_memory_util - benchmark) <= 5 else "needs_improvement"
                }
                
                if abs(avg_memory_util - benchmark) <= 5:  # Within 5%
                    comparison["performance_summary"]["meets_industry_standard"].append("memory_utilization")
                elif avg_memory_util < benchmark - 5:  # Under-utilized
                    comparison["performance_summary"]["below_industry_standard"].append("memory_utilization")
                else:  # Over-utilized
                    comparison["performance_summary"]["below_industry_standard"].append("memory_utilization")
        
        return comparison
    
    async def _assess_optimization_potential(self, cluster_data: Dict[str, Any], 
                                           node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Assess optimization potential"""
        
        potential = {
            "cost_optimization_potential": 0.0,
            "resource_optimization_potential": 0.0,
            "operational_optimization_potential": 0.0,
            "overall_optimization_potential": 0.0,
            "priority_areas": [],
            "quick_wins": [],
            "strategic_initiatives": []
        }
        
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        # Cost optimization potential
        by_service = node_rg_costs.get("by_service", {})
        
        cost_optimization_score = 0.0
        
        # Check for spot instance potential
        vm_cost = by_service.get("Virtual Machines", {}).get("cost", 0.0)
        if vm_cost > 0:
            spot_potential = vm_cost * 0.7  # 70% potential savings
            cost_optimization_score += (spot_potential / total_cost) * 100
            
            potential["quick_wins"].append({
                "opportunity": "Implement spot instances",
                "potential_savings": spot_potential,
                "effort": "low",
                "timeline": "immediate"
            })
        
        # Check for storage optimization
        storage_services = ["Storage", "Managed Disks"]
        storage_cost = sum(by_service.get(service, {}).get("cost", 0.0) for service in storage_services)
        if storage_cost > 0:
            storage_potential = storage_cost * 0.3  # 30% potential savings
            cost_optimization_score += (storage_potential / total_cost) * 100
            
            potential["quick_wins"].append({
                "opportunity": "Optimize storage tiers",
                "potential_savings": storage_potential,
                "effort": "medium",
                "timeline": "short-term"
            })
        
        potential["cost_optimization_potential"] = min(cost_optimization_score, 100.0)
        
        # Resource optimization potential
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        resource_optimization_score = 0.0
        
        # CPU optimization
        if "cluster_cpu_utilization" in container_insights:
            cpu_data = container_insights["cluster_cpu_utilization"]
            data_points = cpu_data.get("data_points", [])
            
            if data_points:
                cpu_values = [point.get("cpu_utilization_percentage", 0) for point in data_points]
                avg_cpu_util = sum(cpu_values) / len(cpu_values)
                target_cpu_util = self.industry_benchmarks["cpu_utilization_target"]
                
                if avg_cpu_util < target_cpu_util - 10:  # More than 10% below target
                    resource_optimization_score += 30.0
                    potential["priority_areas"].append("cpu_utilization")
                elif avg_cpu_util > target_cpu_util + 10:  # More than 10% above target
                    resource_optimization_score += 20.0
                    potential["strategic_initiatives"].append({
                        "initiative": "Scale up CPU capacity",
                        "rationale": "High CPU utilization detected",
                        "effort": "high",
                        "timeline": "medium-term"
                    })
        
        # Memory optimization
        if "cluster_memory_utilization" in container_insights:
            memory_data = container_insights["cluster_memory_utilization"]
            data_points = memory_data.get("data_points", [])
            
            if data_points:
                memory_values = [point.get("memory_utilization_percentage", 0) for point in data_points]
                avg_memory_util = sum(memory_values) / len(memory_values)
                target_memory_util = self.industry_benchmarks["memory_utilization_target"]
                
                if avg_memory_util < target_memory_util - 10:  # More than 10% below target
                    resource_optimization_score += 30.0
                    potential["priority_areas"].append("memory_utilization")
                elif avg_memory_util > target_memory_util + 10:  # More than 10% above target
                    resource_optimization_score += 20.0
                    potential["strategic_initiatives"].append({
                        "initiative": "Scale up memory capacity",
                        "rationale": "High memory utilization detected",
                        "effort": "high",
                        "timeline": "medium-term"
                    })
        
        potential["resource_optimization_potential"] = min(resource_optimization_score, 100.0)
        
        # Operational optimization potential
        node_pools = cluster_data.get("node_pools", [])
        operational_optimization_score = 0.0
        
        # Check for auto-scaling
        auto_scaling_enabled = any(pool.get("auto_scaling_enabled", False) for pool in node_pools)
        if not auto_scaling_enabled:
            operational_optimization_score += 40.0
            potential["priority_areas"].append("auto_scaling")
            potential["quick_wins"].append({
                "opportunity": "Enable auto-scaling",
                "effort": "low",
                "timeline": "immediate"
            })
        
        # Check for monitoring and alerting
        # This would need more detailed analysis
        operational_optimization_score += 30.0  # Assume room for improvement
        potential["strategic_initiatives"].append({
            "initiative": "Implement comprehensive monitoring",
            "rationale": "Improve observability and automated responses",
            "effort": "medium",
            "timeline": "medium-term"
        })
        
        potential["operational_optimization_potential"] = min(operational_optimization_score, 100.0)
        
        # Overall optimization potential
        potential["overall_optimization_potential"] = (
            potential["cost_optimization_potential"] +
            potential["resource_optimization_potential"] +
            potential["operational_optimization_potential"]
        ) / 3
        
        return potential
    
    async def _assess_finops_maturity(self, cluster_data: Dict[str, Any], 
                                    node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Assess FinOps maturity level"""
        
        maturity = {
            "overall_maturity_level": "crawl",
            "maturity_score": 0.0,
            "dimension_scores": {
                "cost_visibility": 0.0,
                "cost_allocation": 0.0,
                "cost_optimization": 0.0,
                "governance": 0.0,
                "automation": 0.0
            },
            "strengths": [],
            "improvement_areas": [],
            "next_steps": []
        }
        
        # Cost visibility assessment
        visibility_score = 0.0
        
        # Check if cost data is available
        if "error" not in node_rg_costs:
            visibility_score += 30.0
            maturity["strengths"].append("Cost data collection implemented")
        
        # Check for cost breakdown
        if node_rg_costs.get("by_service") and node_rg_costs.get("by_resource_type"):
            visibility_score += 40.0
            maturity["strengths"].append("Cost breakdown by service and resource type")
        
        # Check for daily cost tracking
        if node_rg_costs.get("daily_breakdown"):
            visibility_score += 30.0
            maturity["strengths"].append("Daily cost tracking available")
        
        maturity["dimension_scores"]["cost_visibility"] = visibility_score
        
        # Cost allocation assessment
        allocation_score = 0.0
        
        # Check for resource tagging
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        namespaces = k8s_resources.get("namespaces", [])
        
        if namespaces:
            # Check if namespaces have labels for cost allocation
            labeled_namespaces = [ns for ns in namespaces if ns.get("labels", {})]
            if labeled_namespaces:
                allocation_score += 40.0
                maturity["strengths"].append("Namespace labeling for cost allocation")
        
        # Check for workload cost attribution
        deployments = k8s_resources.get("deployments", [])
        if deployments:
            allocation_score += 30.0
            maturity["strengths"].append("Workload-level cost attribution")
        
        # Check for node pool cost allocation
        node_pools = cluster_data.get("node_pools", [])
        if node_pools:
            allocation_score += 30.0
            maturity["strengths"].append("Node pool cost allocation")
        
        maturity["dimension_scores"]["cost_allocation"] = allocation_score
        
        # Cost optimization assessment
        optimization_score = 0.0
        
        # Check for spot instance usage
        spot_usage = any(pool.get("scale_set_priority") == "Spot" for pool in node_pools)
        if spot_usage:
            optimization_score += 30.0
            maturity["strengths"].append("Spot instance usage implemented")
        else:
            maturity["improvement_areas"].append("Implement spot instances")
        
        # Check for auto-scaling
        auto_scaling = any(pool.get("auto_scaling_enabled", False) for pool in node_pools)
        if auto_scaling:
            optimization_score += 30.0
            maturity["strengths"].append("Auto-scaling enabled")
        else:
            maturity["improvement_areas"].append("Enable auto-scaling")
        
        # Check for resource requests/limits
        pods = k8s_resources.get("pods", [])
        pods_with_requests = [pod for pod in pods if pod.get("resource_requests", {})]
        if pods_with_requests:
            optimization_score += 40.0
            maturity["strengths"].append("Resource requests/limits configured")
        else:
            maturity["improvement_areas"].append("Configure resource requests/limits")
        
        maturity["dimension_scores"]["cost_optimization"] = optimization_score
        
        # Governance assessment
        governance_score = 0.0
        
        # Check for resource quotas
        resource_quotas = []
        for ns in namespaces:
            if ns.get("resource_quotas"):
                resource_quotas.extend(ns["resource_quotas"])
        
        if resource_quotas:
            governance_score += 40.0
            maturity["strengths"].append("Resource quotas implemented")
        else:
            maturity["improvement_areas"].append("Implement resource quotas")
        
        # Check for limit ranges
        limit_ranges = []
        for ns in namespaces:
            if ns.get("limit_ranges"):
                limit_ranges.extend(ns["limit_ranges"])
        
        if limit_ranges:
            governance_score += 30.0
            maturity["strengths"].append("Limit ranges configured")
        else:
            maturity["improvement_areas"].append("Configure limit ranges")
        
        # Check for policy enforcement
        governance_score += 30.0  # Assume some basic policies
        
        maturity["dimension_scores"]["governance"] = governance_score
        
        # Automation assessment
        automation_score = 0.0
        
        # Check for auto-scaling (already checked)
        if auto_scaling:
            automation_score += 40.0
        
        # Check for monitoring
        raw_metrics = cluster_data.get("raw_metrics", {})
        if raw_metrics.get("container_insights_data"):
            automation_score += 30.0
            maturity["strengths"].append("Monitoring and metrics collection")
        
        # Assume some level of automation
        automation_score += 30.0
        
        maturity["dimension_scores"]["automation"] = automation_score
        
        # Calculate overall maturity
        all_scores = list(maturity["dimension_scores"].values())
        maturity["maturity_score"] = sum(all_scores) / len(all_scores)
        
        # Determine maturity level
        if maturity["maturity_score"] >= 80:
            maturity["overall_maturity_level"] = "run"
        elif maturity["maturity_score"] >= 60:
            maturity["overall_maturity_level"] = "walk"
        else:
            maturity["overall_maturity_level"] = "crawl"
        
        # Generate next steps
        if maturity["overall_maturity_level"] == "crawl":
            maturity["next_steps"] = [
                "Implement comprehensive cost visibility",
                "Set up basic cost allocation and tagging",
                "Enable auto-scaling and basic optimization"
            ]
        elif maturity["overall_maturity_level"] == "walk":
            maturity["next_steps"] = [
                "Implement advanced cost optimization",
                "Set up automated governance policies",
                "Enable predictive scaling and forecasting"
            ]
        else:
            maturity["next_steps"] = [
                "Implement ML-driven optimization",
                "Set up advanced FinOps automation",
                "Enable continuous optimization loops"
            ]
        
        return maturity
    
    async def _generate_benchmark_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate benchmarking recommendations"""
        
        recommendations = []
        
        performance_scores = analysis.get("performance_scores", {})
        industry_comparison = analysis.get("industry_comparison", {})
        optimization_potential = analysis.get("optimization_potential", {})
        maturity_assessment = analysis.get("maturity_assessment", {})
        
        # Performance-based recommendations
        overall_score = performance_scores.get("overall_score", 0.0)
        
        if overall_score < 60:
            recommendations.append("PRIORITY: Overall performance below industry standards - implement immediate optimization")
        elif overall_score < 80:
            recommendations.append("FOCUS: Performance approaching industry standards - continue optimization efforts")
        else:
            recommendations.append("MAINTAIN: Performance exceeds industry standards - focus on continuous improvement")
        
        # Industry comparison recommendations
        performance_summary = industry_comparison.get("performance_summary", {})
        
        below_standard = performance_summary.get("below_industry_standard", [])
        if below_standard:
            recommendations.append(f"Address underperforming areas: {', '.join(below_standard)}")
        
        # Optimization potential recommendations
        overall_potential = optimization_potential.get("overall_optimization_potential", 0.0)
        
        if overall_potential > 60:
            recommendations.append(f"HIGH POTENTIAL: {overall_potential:.1f}% optimization potential identified")
        elif overall_potential > 30:
            recommendations.append(f"MODERATE POTENTIAL: {overall_potential:.1f}% optimization potential identified")
        
        # Quick wins
        quick_wins = optimization_potential.get("quick_wins", [])
        if quick_wins:
            recommendations.append(f"Implement {len(quick_wins)} quick wins for immediate impact")
        
        # Maturity-based recommendations
        maturity_level = maturity_assessment.get("overall_maturity_level", "crawl")
        
        if maturity_level == "crawl":
            recommendations.append("FOUNDATION: Focus on building basic FinOps capabilities")
        elif maturity_level == "walk":
            recommendations.append("ADVANCEMENT: Enhance existing capabilities with automation")
        else:
            recommendations.append("OPTIMIZATION: Implement advanced FinOps practices")
        
        # Improvement areas
        improvement_areas = maturity_assessment.get("improvement_areas", [])
        if improvement_areas:
            recommendations.append(f"Address improvement areas: {', '.join(improvement_areas[:3])}")
        
        return recommendations