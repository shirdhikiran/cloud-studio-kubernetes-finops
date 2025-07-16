# src/finops/analytics/rightsizing_analytics.py
"""
Right-sizing Analytics Module - Intelligent resource sizing recommendations
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import structlog
import statistics
from collections import defaultdict
import math

logger = structlog.get_logger(__name__)


class RightsizingAnalytics:
    """Advanced right-sizing analytics for optimal resource allocation"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(module="rightsizing_analytics")
        
        # Right-sizing parameters
        self.parameters = {
            'cpu_target_utilization': config.get('cpu_target_utilization', 70.0),  # %
            'memory_target_utilization': config.get('memory_target_utilization', 70.0),  # %
            'safety_buffer': config.get('safety_buffer', 20.0),  # % buffer above usage
            'min_cpu_millicores': config.get('min_cpu_millicores', 100),
            'min_memory_mb': config.get('min_memory_mb', 128),
            'confidence_threshold': config.get('confidence_threshold', 7),  # days of data
        }
    
    async def analyze_cluster_rightsizing(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive right-sizing analysis for a cluster"""
        
        cluster_info = cluster_data.get("cluster_info", {})
        cluster_name = cluster_info.get("name", "unknown")
        
        self.logger.info(f"Starting right-sizing analysis for cluster: {cluster_name}")
        
        analysis = {
            "cluster_name": cluster_name,
            "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
            "pod_rightsizing": {},
            "node_rightsizing": {},
            "workload_rightsizing": {},
            "optimization_opportunities": [],
            "scaling_recommendations": [],
            "resource_recommendations": {},
            "cost_impact": {},
            "implementation_plan": {},
            "confidence_scores": {}
        }
        
        try:
            # 1. Pod-level right-sizing analysis
            analysis["pod_rightsizing"] = await self._analyze_pod_rightsizing(cluster_data)
            
            # 2. Node-level right-sizing analysis
            analysis["node_rightsizing"] = await self._analyze_node_rightsizing(cluster_data)
            
            # 3. Workload-level right-sizing analysis
            analysis["workload_rightsizing"] = await self._analyze_workload_rightsizing(cluster_data)
            
            # 4. Generate optimization opportunities
            analysis["optimization_opportunities"] = await self._generate_optimization_opportunities(analysis)
            
            # 5. Generate scaling recommendations
            analysis["scaling_recommendations"] = await self._generate_scaling_recommendations(analysis)
            
            # 6. Create resource recommendations
            analysis["resource_recommendations"] = await self._create_resource_recommendations(analysis)
            
            # 7. Calculate cost impact
            analysis["cost_impact"] = await self._calculate_rightsizing_cost_impact(analysis)
            
            # 8. Create implementation plan
            analysis["implementation_plan"] = await self._create_implementation_plan(analysis)
            
            # 9. Calculate confidence scores
            analysis["confidence_scores"] = await self._calculate_confidence_scores(analysis, cluster_data)
            
            self.logger.info(
                f"Right-sizing analysis completed for {cluster_name}",
                optimization_opportunities=len(analysis["optimization_opportunities"]),
                potential_savings=analysis["cost_impact"].get("total_potential_savings", 0)
            )
            
        except Exception as e:
            self.logger.error(f"Right-sizing analysis failed for {cluster_name}: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    async def _analyze_pod_rightsizing(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze pod-level right-sizing opportunities"""
        
        pod_analysis = {
            "pod_recommendations": [],
            "resource_efficiency": {},
            "right_sizing_potential": {},
            "cpu_recommendations": {},
            "memory_recommendations": {}
        }
        
        # Extract pod utilization data
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # Collect pod usage patterns
        pod_usage_data = defaultdict(lambda: {
            "cpu_usage": [],
            "memory_usage": [],
            "cpu_requests": 0,
            "memory_requests": 0,
            "cpu_limits": 0,
            "memory_limits": 0
        })
        
        # Gather CPU usage data
        if "pod_cpu_usage" in container_insights:
            cpu_data = container_insights["pod_cpu_usage"]
            for point in cpu_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                cpu_usage = self.safe_float(point.get("cpu_usage_millicores", 0))
                if cpu_usage > 0:
                    pod_usage_data[pod_name]["cpu_usage"].append(cpu_usage)
        
        # Gather memory usage data
        if "pod_memory_usage" in container_insights:
            memory_data = container_insights["pod_memory_usage"]
            for point in memory_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                memory_usage = self.safe_float(point.get("memory_usage_mb", 0))
                if memory_usage > 0:
                    pod_usage_data[pod_name]["memory_usage"].append(memory_usage)
        
        # Gather resource requests and limits
        if "pod_resource_requests_limits" in container_insights:
            request_data = container_insights["pod_resource_requests_limits"]
            for point in request_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                if pod_name in pod_usage_data:
                    pod_usage_data[pod_name]["cpu_requests"] = self.safe_float(point.get("cpu_request_millicores", 0))
                    pod_usage_data[pod_name]["memory_requests"] = self.safe_float(point.get("memory_request_mb", 0))
                    pod_usage_data[pod_name]["cpu_limits"] = self.safe_float(point.get("cpu_limit", 0))
                    pod_usage_data[pod_name]["memory_limits"] = self.safe_float(point.get("memory_limit", 0))
        
        # Analyze each pod for right-sizing opportunities
        for pod_name, usage_data in pod_usage_data.items():
            cpu_usage = usage_data["cpu_usage"]
            memory_usage = usage_data["memory_usage"]
            
            if len(cpu_usage) >= 3 and len(memory_usage) >= 3:  # Minimum data points
                recommendation = self._generate_pod_recommendation(
                    pod_name, usage_data, cpu_usage, memory_usage
                )
                
                if recommendation:
                    pod_analysis["pod_recommendations"].append(recommendation)
        
        # Calculate overall efficiency
        if pod_analysis["pod_recommendations"]:
            total_pods = len(pod_usage_data)
            pods_needing_rightsizing = len(pod_analysis["pod_recommendations"])
            
            pod_analysis["resource_efficiency"] = {
                "total_pods_analyzed": total_pods,
                "pods_needing_rightsizing": pods_needing_rightsizing,
                "rightsizing_percentage": (pods_needing_rightsizing / total_pods) * 100 if total_pods > 0 else 0,
                "avg_cpu_over_provisioning": self._calculate_avg_overprovisioning(pod_analysis["pod_recommendations"], "cpu"),
                "avg_memory_over_provisioning": self._calculate_avg_overprovisioning(pod_analysis["pod_recommendations"], "memory")
            }
        
        return pod_analysis
    
    async def _analyze_node_rightsizing(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze node-level right-sizing opportunities"""
        
        node_analysis = {
            "node_recommendations": [],
            "node_pool_optimization": [],
            "cluster_capacity_optimization": {},
            "node_efficiency_scores": {}
        }
        
        # Get node pool information
        node_pools = cluster_data.get("node_pools", [])
        
        # Extract node utilization data
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # Collect node utilization patterns
        node_utilization = defaultdict(lambda: {"cpu": [], "memory": []})
        
        if "aks_node_cpu_performance" in container_insights:
            cpu_data = container_insights["aks_node_cpu_performance"]
            for point in cpu_data.get("data_points", []):
                node_name = point.get("NodeName", "unknown")
                cpu_percent = self.safe_float(point.get("cpu_usage_millicores", 0)) / 20  # Convert to percentage
                node_utilization[node_name]["cpu"].append(min(100, cpu_percent))
        
        if "aks_node_memory_performance" in container_insights:
            memory_data = container_insights["aks_node_memory_performance"]
            for point in memory_data.get("data_points", []):
                node_name = point.get("NodeName", "unknown")
                memory_percent = self.safe_float(point.get("memory_usage_percentage", 0))
                node_utilization[node_name]["memory"].append(min(100, memory_percent))
        
        # Analyze each node pool
        for pool in node_pools:
            pool_name = pool.get("name", "unknown")
            vm_size = pool.get("vm_size", "")
            node_count = pool.get("count", 0)
            auto_scaling = pool.get("auto_scaling_enabled", False)
            min_count = pool.get("min_count", 0)
            max_count = pool.get("max_count", 0)
            
            # Calculate pool utilization
            pool_nodes = [name for name in node_utilization.keys() if pool_name.lower() in name.lower()]
            
            if pool_nodes:
                pool_cpu_usage = []
                pool_memory_usage = []
                
                for node_name in pool_nodes:
                    pool_cpu_usage.extend(node_utilization[node_name]["cpu"])
                    pool_memory_usage.extend(node_utilization[node_name]["memory"])
                
                if pool_cpu_usage and pool_memory_usage:
                    avg_cpu = statistics.mean(pool_cpu_usage)
                    avg_memory = statistics.mean(pool_memory_usage)
                    peak_cpu = max(pool_cpu_usage)
                    peak_memory = max(pool_memory_usage)
                    
                    # Generate node pool recommendation
                    recommendation = self._generate_node_pool_recommendation(
                        pool, avg_cpu, avg_memory, peak_cpu, peak_memory
                    )
                    
                    if recommendation:
                        node_analysis["node_pool_optimization"].append(recommendation)
        
        return node_analysis
    
    async def _analyze_workload_rightsizing(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze workload-level right-sizing opportunities"""
        
        workload_analysis = {
            "deployment_recommendations": [],
            "workload_scaling_analysis": {},
            "resource_allocation_optimization": {}
        }
        
        # Get Kubernetes workloads
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        deployments = k8s_resources.get("deployments", [])
        
        # Analyze each deployment
        for deployment in deployments:
            deployment_name = deployment.get("name", "unknown")
            namespace = deployment.get("namespace", "unknown")
            replicas = deployment.get("replicas", 1)
            ready_replicas = deployment.get("ready_replicas", 0)
            
            # Analyze replica efficiency
            if replicas > 0:
                replica_efficiency = (ready_replicas / replicas) * 100
                
                # Check if deployment is over or under-scaled
                recommendation = self._analyze_deployment_scaling(
                    deployment_name, namespace, replicas, ready_replicas, replica_efficiency
                )
                
                if recommendation:
                    workload_analysis["deployment_recommendations"].append(recommendation)
        
        return workload_analysis
    
    async def _generate_optimization_opportunities(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate comprehensive optimization opportunities"""
        
        opportunities = []
        
        # Pod-level opportunities
        pod_analysis = analysis.get("pod_rightsizing", {})
        pod_recommendations = pod_analysis.get("pod_recommendations", [])
        
        for rec in pod_recommendations:
            if rec.get("potential_savings", 0) > 10:  # More than $10/month savings
                opportunities.append({
                    "type": "pod_rightsizing",
                    "resource_id": rec["pod_name"],
                    "current_allocation": {
                        "cpu_request": rec["current_cpu_request"],
                        "memory_request": rec["current_memory_request"]
                    },
                    "recommended_allocation": {
                        "cpu_request": rec["recommended_cpu_request"],
                        "memory_request": rec["recommended_memory_request"]
                    },
                    "potential_savings": rec["potential_savings"],
                    "confidence": rec["confidence"],
                    "implementation_effort": "low",
                    "risk_level": "low"
                })
        
        # Node pool opportunities
        node_analysis = analysis.get("node_rightsizing", {})
        node_recommendations = node_analysis.get("node_pool_optimization", [])
        
        for rec in node_recommendations:
            if rec.get("action") in ["downsize", "optimize_vm_size"]:
                opportunities.append({
                    "type": "node_pool_optimization",
                    "resource_id": rec["pool_name"],
                    "current_allocation": {
                        "vm_size": rec["current_vm_size"],
                        "node_count": rec["current_node_count"]
                    },
                    "recommended_allocation": {
                        "vm_size": rec.get("recommended_vm_size", rec["current_vm_size"]),
                        "node_count": rec.get("recommended_node_count", rec["current_node_count"])
                    },
                    "potential_savings": rec.get("potential_savings", 0),
                    "confidence": rec.get("confidence", 70),
                    "implementation_effort": "medium",
                    "risk_level": "medium"
                })
        
        # Sort by potential savings
        opportunities.sort(key=lambda x: x["potential_savings"], reverse=True)
        
        return opportunities
    
    async def _generate_scaling_recommendations(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate scaling recommendations for auto-scalers"""
        
        scaling_recommendations = []
        
        # HPA recommendations
        pod_analysis = analysis.get("pod_rightsizing", {})
        
        # VPA recommendations based on pod analysis
        for pod_rec in pod_analysis.get("pod_recommendations", []):
            if pod_rec.get("action") == "resize":
                scaling_recommendations.append({
                    "type": "VPA",
                    "target": pod_rec["pod_name"],
                    "current_requests": {
                        "cpu": f"{pod_rec['current_cpu_request']}m",
                        "memory": f"{pod_rec['current_memory_request']}Mi"
                    },
                    "recommended_requests": {
                        "cpu": f"{pod_rec['recommended_cpu_request']}m",
                        "memory": f"{pod_rec['recommended_memory_request']}Mi"
                    },
                    "expected_savings": pod_rec.get("potential_savings", 0),
                    "confidence": pod_rec.get("confidence", 70)
                })
        
        # Cluster Autoscaler recommendations
        node_analysis = analysis.get("node_rightsizing", {})
        for node_rec in node_analysis.get("node_pool_optimization", []):
            if node_rec.get("action") == "enable_autoscaling":
                scaling_recommendations.append({
                    "type": "Cluster_Autoscaler",
                    "target": node_rec["pool_name"],
                    "recommendation": "Enable auto-scaling",
                    "suggested_min": node_rec.get("suggested_min_nodes", 1),
                    "suggested_max": node_rec.get("suggested_max_nodes", 10),
                    "current_count": node_rec["current_node_count"],
                    "rationale": node_rec.get("rationale", "")
                })
        
        return scaling_recommendations
    
    async def _create_resource_recommendations(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Create detailed resource recommendations"""
        
        recommendations = {
            "immediate_actions": [],
            "planned_actions": [],
            "long_term_actions": [],
            "resource_policies": []
        }
        
        opportunities = analysis.get("optimization_opportunities", [])
        
        for opp in opportunities:
            action = {
                "resource_id": opp["resource_id"],
                "type": opp["type"],
                "current": opp["current_allocation"],
                "recommended": opp["recommended_allocation"],
                "savings": opp["potential_savings"],
                "effort": opp["implementation_effort"],
                "risk": opp["risk_level"]
            }
            
            # Categorize by implementation timeline
            if opp["implementation_effort"] == "low" and opp["risk_level"] == "low":
                recommendations["immediate_actions"].append(action)
            elif opp["implementation_effort"] == "medium":
                recommendations["planned_actions"].append(action)
            else:
                recommendations["long_term_actions"].append(action)
        
        # Generate resource policies
        pod_analysis = analysis.get("pod_rightsizing", {})
        efficiency = pod_analysis.get("resource_efficiency", {})
        
        if efficiency.get("avg_cpu_over_provisioning", 0) > 50:
            recommendations["resource_policies"].append({
                "type": "ResourceQuota",
                "recommendation": "Implement CPU resource quotas to prevent over-provisioning",
                "rationale": f"Average CPU over-provisioning is {efficiency['avg_cpu_over_provisioning']:.1f}%"
            })
        
        if efficiency.get("avg_memory_over_provisioning", 0) > 50:
            recommendations["resource_policies"].append({
                "type": "LimitRange",
                "recommendation": "Implement memory limit ranges for better resource governance",
                "rationale": f"Average memory over-provisioning is {efficiency['avg_memory_over_provisioning']:.1f}%"
            })
        
        return recommendations
    
    async def _calculate_rightsizing_cost_impact(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate cost impact of right-sizing recommendations"""
        
        cost_impact = {
            "total_potential_savings": 0.0,
            "savings_by_category": {},
            "implementation_costs": {},
            "roi_analysis": {},
            "payback_period": {}
        }
        
        # Calculate savings from opportunities
        opportunities = analysis.get("optimization_opportunities", [])
        
        savings_by_category = defaultdict(float)
        for opp in opportunities:
            category = opp["type"]
            savings = opp["potential_savings"]
            savings_by_category[category] += savings
        
        cost_impact["savings_by_category"] = dict(savings_by_category)
        cost_impact["total_potential_savings"] = sum(savings_by_category.values())
        
        # Estimate implementation costs
        implementation_costs = {
            "pod_rightsizing": len([o for o in opportunities if o["type"] == "pod_rightsizing"]) * 2,  # $2 per pod change
            "node_pool_optimization": len([o for o in opportunities if o["type"] == "node_pool_optimization"]) * 50,  # $50 per node pool change
            "total": 0
        }
        implementation_costs["total"] = sum(implementation_costs.values()) - implementation_costs["total"]
        cost_impact["implementation_costs"] = implementation_costs
        
        # ROI analysis
        annual_savings = cost_impact["total_potential_savings"] * 12
        total_implementation_cost = implementation_costs["total"]
        
        if total_implementation_cost > 0:
            roi_percentage = ((annual_savings - total_implementation_cost) / total_implementation_cost) * 100
            payback_months = total_implementation_cost / max(cost_impact["total_potential_savings"], 1)
        else:
            roi_percentage = float('inf')
            payback_months = 0
        
        cost_impact["roi_analysis"] = {
            "annual_savings": annual_savings,
            "implementation_cost": total_implementation_cost,
            "roi_percentage": roi_percentage,
            "net_annual_benefit": annual_savings - total_implementation_cost
        }
        
        cost_impact["payback_period"] = {
            "months": payback_months,
            "description": f"Payback in {payback_months:.1f} months" if payback_months < 12 else "Payback over 1 year"
        }
        
        return cost_impact
    
    async def _create_implementation_plan(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Create phased implementation plan"""
        
        plan = {
            "phase_1_immediate": [],
            "phase_2_planned": [],
            "phase_3_long_term": [],
            "prerequisites": [],
            "success_metrics": []
        }
        
        recommendations = analysis.get("resource_recommendations", {})
        
        # Phase 1: Immediate actions (low risk, high impact)
        immediate_actions = recommendations.get("immediate_actions", [])
        for action in immediate_actions[:10]:  # Limit to top 10
            plan["phase_1_immediate"].append({
                "action": f"Right-size {action['resource_id']}",
                "timeline": "1-2 weeks",
                "expected_savings": action["savings"],
                "risk": action["risk"],
                "steps": [
                    "Analyze current usage patterns",
                    "Update resource requests/limits",
                    "Monitor for 48 hours",
                    "Validate performance"
                ]
            })
        
        # Phase 2: Planned actions
        planned_actions = recommendations.get("planned_actions", [])
        for action in planned_actions[:5]:  # Limit to top 5
            plan["phase_2_planned"].append({
                "action": f"Optimize {action['resource_id']}",
                "timeline": "2-4 weeks",
                "expected_savings": action["savings"],
                "risk": action["risk"],
                "steps": [
                    "Detailed capacity planning",
                    "Create rollback plan",
                    "Implement changes during maintenance window",
                    "Monitor and adjust"
                ]
            })
        
        # Prerequisites
        plan["prerequisites"] = [
            "Establish baseline metrics collection",
            "Set up monitoring and alerting",
            "Create rollback procedures",
            "Get stakeholder approval for changes"
        ]
        
        # Success metrics
        plan["success_metrics"] = [
            "Achieve target utilization rates (70% CPU, 70% memory)",
            "Reduce resource waste by 30%",
            "Maintain application performance SLAs",
            "Realize projected cost savings within 3 months"
        ]
        
        return plan
    
    async def _calculate_confidence_scores(self, analysis: Dict[str, Any], cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate confidence scores for recommendations"""
        
        confidence = {
            "overall_confidence": 0.0,
            "data_quality_score": 0.0,
            "recommendation_confidence": {},
            "risk_assessment": {}
        }
        
        # Assess data quality
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        data_sources = ["pod_cpu_usage", "pod_memory_usage", "pod_resource_requests_limits"]
        available_sources = sum(1 for source in data_sources if source in container_insights)
        data_quality = (available_sources / len(data_sources)) * 100
        
        # Assess data completeness
        total_data_points = 0
        for source in data_sources:
            if source in container_insights:
                data_points = len(container_insights[source].get("data_points", []))
                total_data_points += data_points
        
        completeness_score = min(100, (total_data_points / 100) * 100)  # Normalize to 100
        
        confidence["data_quality_score"] = (data_quality + completeness_score) / 2
        
        # Calculate recommendation confidence
        opportunities = analysis.get("optimization_opportunities", [])
        
        for opp in opportunities:
            resource_id = opp["resource_id"]
            opp_confidence = opp.get("confidence", 50)
            
            # Adjust confidence based on data quality
            adjusted_confidence = opp_confidence * (confidence["data_quality_score"] / 100)
            
            confidence["recommendation_confidence"][resource_id] = {
                "base_confidence": opp_confidence,
                "adjusted_confidence": adjusted_confidence,
                "risk_level": opp["risk_level"]
            }
        
        # Overall confidence
        if confidence["recommendation_confidence"]:
            avg_confidence = statistics.mean([
                rec["adjusted_confidence"] 
                for rec in confidence["recommendation_confidence"].values()
            ])
            confidence["overall_confidence"] = avg_confidence
        
        return confidence
    
    # Helper methods
    def _generate_pod_recommendation(self, pod_name: str, usage_data: Dict[str, Any], 
                                   cpu_usage: List[float], memory_usage: List[float]) -> Optional[Dict[str, Any]]:
        """Generate right-sizing recommendation for a pod"""
        
        # Calculate usage statistics
        avg_cpu = statistics.mean(cpu_usage)
        p95_cpu = self._percentile(cpu_usage, 95)
        avg_memory = statistics.mean(memory_usage)
        p95_memory = self._percentile(memory_usage, 95)
        
        current_cpu_request = usage_data["cpu_requests"]
        current_memory_request = usage_data["memory_requests"]
        
        if current_cpu_request == 0 or current_memory_request == 0:
            return None  # Skip pods without resource requests
        
        # Calculate recommended values with safety buffer
        safety_factor = 1 + (self.parameters["safety_buffer"] / 100)
        
        recommended_cpu = max(
            self.parameters["min_cpu_millicores"],
            int(p95_cpu * safety_factor)
        )
        recommended_memory = max(
            self.parameters["min_memory_mb"],
            int(p95_memory * safety_factor)
        )
        
        # Determine if resizing is beneficial
        cpu_savings_percent = ((current_cpu_request - recommended_cpu) / current_cpu_request) * 100
        memory_savings_percent = ((current_memory_request - recommended_memory) / current_memory_request) * 100
        
        # Only recommend if significant savings (>15%) or if severely over-provisioned (>200%)
        should_resize = (
            (cpu_savings_percent > 15 or memory_savings_percent > 15) or
            (current_cpu_request > recommended_cpu * 2 or current_memory_request > recommended_memory * 2)
        )
        
        if not should_resize:
            return None
        
        # Calculate potential savings
        cpu_cost_savings = (current_cpu_request - recommended_cpu) / 1000 * 20  # $20 per core/month
        memory_cost_savings = (current_memory_request - recommended_memory) / 1024 * 5  # $5 per GB/month
        total_savings = cpu_cost_savings + memory_cost_savings
        
        # Calculate confidence based on data consistency
        cpu_cv = statistics.stdev(cpu_usage) / avg_cpu if avg_cpu > 0 else 1
        memory_cv = statistics.stdev(memory_usage) / avg_memory if avg_memory > 0 else 1
        confidence = max(0, min(100, 100 - (cpu_cv + memory_cv) * 50))
        
        return {
            "pod_name": pod_name,
            "action": "resize",
            "current_cpu_request": current_cpu_request,
            "current_memory_request": current_memory_request,
            "recommended_cpu_request": recommended_cpu,
            "recommended_memory_request": recommended_memory,
            "cpu_savings_percent": cpu_savings_percent,
            "memory_savings_percent": memory_savings_percent,
            "potential_savings": total_savings,
            "confidence": confidence,
            "usage_stats": {
                "avg_cpu": avg_cpu,
                "p95_cpu": p95_cpu,
                "avg_memory": avg_memory,
                "p95_memory": p95_memory
            }
        }
    
    def _generate_node_pool_recommendation(self, pool: Dict[str, Any], avg_cpu: float, 
                                         avg_memory: float, peak_cpu: float, peak_memory: float) -> Optional[Dict[str, Any]]:
        """Generate right-sizing recommendation for a node pool"""
        
        pool_name = pool.get("name", "unknown")
        vm_size = pool.get("vm_size", "")
        node_count = pool.get("count", 0)
        auto_scaling = pool.get("auto_scaling_enabled", False)
        
        recommendation = {
            "pool_name": pool_name,
            "current_vm_size": vm_size,
            "current_node_count": node_count,
            "avg_cpu_utilization": avg_cpu,
            "avg_memory_utilization": avg_memory,
            "peak_cpu_utilization": peak_cpu,
            "peak_memory_utilization": peak_memory
        }
        
        # Determine action based on utilization
        if avg_cpu < 30 and avg_memory < 30:
            # Severely underutilized
            if node_count > 1:
                recommendation.update({
                    "action": "downsize",
                    "recommended_node_count": max(1, node_count - 1),
                    "rationale": "Low average utilization suggests over-provisioning",
                    "potential_savings": self._estimate_node_cost() * (node_count - max(1, node_count - 1)),
                    "confidence": 80
                })
            else:
                recommendation.update({
                    "action": "optimize_vm_size",
                    "recommended_vm_size": self._suggest_smaller_vm_size(vm_size),
                    "rationale": "Consider smaller VM size for better efficiency",
                    "potential_savings": self._estimate_vm_size_savings(vm_size),
                    "confidence": 60
                })
        elif peak_cpu > 85 or peak_memory > 85:
            # High peak utilization
            if auto_scaling:
                recommendation.update({
                    "action": "adjust_autoscaling",
                    "suggested_max_nodes": pool.get("max_count", 10) + 2,
                    "rationale": "High peak utilization may require more autoscaling headroom",
                    "confidence": 70
                })
            else:
                recommendation.update({
                    "action": "enable_autoscaling",
                    "suggested_min_nodes": node_count,
                    "suggested_max_nodes": node_count + 3,
                    "rationale": "High utilization variance suggests need for autoscaling",
                    "confidence": 75
                })
        else:
            return None  # No recommendation needed
        
        return recommendation
    
    def _analyze_deployment_scaling(self, name: str, namespace: str, replicas: int, 
                                  ready_replicas: int, efficiency: float) -> Optional[Dict[str, Any]]:
        """Analyze deployment for scaling optimization"""
        
        if efficiency < 80 and replicas > 1:
            # Poor replica efficiency
            return {
                "deployment_name": name,
                "namespace": namespace,
                "current_replicas": replicas,
                "ready_replicas": ready_replicas,
                "efficiency": efficiency,
                "action": "investigate_failures",
                "rationale": "Low replica efficiency indicates potential issues",
                "recommended_action": "Debug failed replicas or reduce replica count"
            }
        elif replicas == 1 and ready_replicas == 1:
            # Single replica deployment
            return {
                "deployment_name": name,
                "namespace": namespace,
                "current_replicas": replicas,
                "action": "consider_scaling",
                "rationale": "Single replica deployment may need HA consideration",
                "recommended_action": "Evaluate if HA is needed and scale accordingly"
            }
        
        return None
    
    def _calculate_avg_overprovisioning(self, recommendations: List[Dict[str, Any]], resource_type: str) -> float:
        """Calculate average over-provisioning percentage"""
        
        if not recommendations:
            return 0.0
        
        savings_key = f"{resource_type}_savings_percent"
        savings = [rec.get(savings_key, 0) for rec in recommendations if savings_key in rec]
        
        return statistics.mean(savings) if savings else 0.0
    
    def _percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile of data"""
        if not data:
            return 0.0
        
        sorted_data = sorted(data)
        index = (percentile / 100) * (len(sorted_data) - 1)
        
        if index.is_integer():
            return sorted_data[int(index)]
        else:
            lower = sorted_data[int(index)]
            upper = sorted_data[int(index) + 1]
            return lower + (upper - lower) * (index - int(index))
    
    def _estimate_node_cost(self) -> float:
        """Estimate monthly cost per node"""
        return 70.0  # Simplified estimate
    
    def _estimate_vm_size_savings(self, current_vm_size: str) -> float:
        """Estimate savings from VM size optimization"""
        # Simplified savings estimation
        return 20.0  # $20/month savings
    
    def _suggest_smaller_vm_size(self, current_vm_size: str) -> str:
        """Suggest smaller VM size"""
        
        vm_size_map = {
            "Standard_D4s_v3": "Standard_D2s_v3",
            "Standard_D8s_v3": "Standard_D4s_v3",
            "Standard_D16s_v3": "Standard_D8s_v3",
            "Standard_F8s_v2": "Standard_F4s_v2",
        }
        
        return vm_size_map.get(current_vm_size, current_vm_size)
    
    @staticmethod
    def safe_float(value, default=0.0):
        try:
            
            if value is None or value == '' or value == 'NaN' or value == "'NaN'":
                return default
            result = float(value)
            if math.isnan(result):
                return default
            return result
        except (ValueError, TypeError):
            return default