# src/finops/analytics/rightsizing_analytics.py
"""
Enhanced Phase 2 Rightsizing Analytics with node_resource_group_costs integration
"""
import traceback
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import structlog

logger = structlog.get_logger(__name__)


class RightsizingAnalytics:
    """Enhanced Rightsizing Analytics for Phase 2"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(analytics="rightsizing")
        
        self.thresholds = {
            'cpu_utilization_target': config.get('cpu_target_utilization', 70.0),
            'memory_utilization_target': config.get('memory_target_utilization', 70.0),
            'over_provisioning_threshold': config.get('overprovisioning_threshold', 200.0),
            'under_utilization_threshold': config.get('low_utilization_threshold', 30.0),
            'min_confidence_threshold': config.get('min_confidence_threshold', 70.0)
        }
    
    async def analyze_cluster_rightsizing(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced rightsizing analysis with cost correlation"""
        
        cluster_name = cluster_data.get("cluster_info", {}).get("name", "unknown")

        
        analysis = {
            "cluster_name": cluster_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "node_pool_rightsizing": {},
            "pod_rightsizing": {},
            "workload_rightsizing": {},
            "optimization_opportunities": [],
            "scaling_recommendations": [],
            "resource_recommendations": {},
            "cost_impact": {},
            "implementation_plan": {},
            "confidence_scores": {}
        }
        
        try:
            node_rg_costs = cluster_data.get("node_resource_group_costs", {})
            raw_metrics = cluster_data.get("raw_metrics", {})
            
            if "error" not in node_rg_costs:
                # 1. Node pool rightsizing analysis
                analysis["node_pool_rightsizing"] = await self._analyze_node_pool_rightsizing(
                    cluster_data, raw_metrics, node_rg_costs
                )
                
                # 2. Pod-level rightsizing analysis
                analysis["pod_rightsizing"] = await self._analyze_pod_rightsizing(
                    cluster_data, raw_metrics, node_rg_costs
                )
                
                # 3. Workload rightsizing analysis
                analysis["workload_rightsizing"] = await self._analyze_workload_rightsizing(
                    cluster_data, raw_metrics, node_rg_costs
                )
                
                # 4. Generate optimization opportunities
                analysis["optimization_opportunities"] = await self._generate_optimization_opportunities(
                    analysis, node_rg_costs
                )
                
                # 5. Generate scaling recommendations
                analysis["scaling_recommendations"] = await self._generate_scaling_recommendations(
                    analysis, node_rg_costs
                )
                
                # 6. Create resource recommendations
                analysis["resource_recommendations"] = await self._create_resource_recommendations(
                    analysis, node_rg_costs
                )
                
                # 7. Calculate cost impact
                analysis["cost_impact"] = await self._calculate_rightsizing_cost_impact(
                    analysis, node_rg_costs
                )
                
                # 8. Create implementation plan
                analysis["implementation_plan"] = await self._create_implementation_plan(
                    analysis, node_rg_costs
                )
                
                # 9. Calculate confidence scores
                analysis["confidence_scores"] = await self._calculate_confidence_scores(analysis)
                
                self.logger.info(f"Rightsizing analysis completed for {cluster_name}")
            else:
                self.logger.warning(f"Skipping rightsizing analysis for {cluster_name} - no cost data")
                analysis["error"] = "No valid cost data available"
                
        except Exception as e:
            import pdb; pdb.set_trace()
            self.logger.error(f"Rightsizing analysis failed for {cluster_name}: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    async def _analyze_node_pool_rightsizing(self, cluster_data: Dict[str, Any], 
                                          raw_metrics: Dict[str, Any], 
                                          node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze node pool rightsizing opportunities"""
        
        node_pool_analysis = {
            "current_configuration": {},
            "recommended_configuration": {},
            "rightsizing_opportunities": [],
            "potential_savings": 0.0
        }
        
        node_pools = cluster_data.get("node_pools", [])
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        # Get utilization data
        container_insights = raw_metrics.get("container_insights_data", {})
        
        cpu_utilization = 0.0
        memory_utilization = 0.0
        
        if "cluster_cpu_utilization" in container_insights:
            cpu_data = container_insights["cluster_cpu_utilization"]
            data_points = cpu_data.get("data_points", [])
            if data_points:
                cpu_values = [point.get("cpu_utilization_percentage", 0) for point in data_points]
                cpu_utilization = sum(cpu_values) / len(cpu_values)
        
        if "cluster_memory_utilization" in container_insights:
            memory_data = container_insights["cluster_memory_utilization"]
            data_points = memory_data.get("data_points", [])
            if data_points:
                memory_values = [point.get("memory_utilization_percentage", 0) for point in data_points]
                memory_utilization = sum(memory_values) / len(memory_values)
        
        # Analyze each node pool
        for pool in node_pools:
            pool_name = pool.get("name", "unknown")
            vm_size = pool.get("vm_size", "")
            current_count = pool.get("count", 0)
            min_count = pool.get("min_count", 1)
            max_count = pool.get("max_count", current_count)
            
            # Get VM specifications
            cpu_cores, memory_gb = self._parse_vm_size(vm_size)
            
            # Calculate pool cost
            pool_cost = total_cost * (current_count / max(sum(p.get("count", 0) for p in node_pools), 1))
            
            node_pool_analysis["current_configuration"][pool_name] = {
                "vm_size": vm_size,
                "current_count": current_count,
                "min_count": min_count,
                "max_count": max_count,
                "cpu_cores_per_node": cpu_cores,
                "memory_gb_per_node": memory_gb,
                "total_cpu_cores": cpu_cores * current_count,
                "total_memory_gb": memory_gb * current_count,
                "estimated_monthly_cost": pool_cost
            }
            
            # Rightsizing recommendations
            rightsizing_opportunity = {}
            
            # Check for underutilization
            if cpu_utilization < self.thresholds['under_utilization_threshold'] and memory_utilization < self.thresholds['under_utilization_threshold']:
                # Recommend scaling down
                recommended_count = max(min_count, int(current_count * 0.7))  # 30% reduction
                cost_savings = pool_cost * (current_count - recommended_count) / current_count
                
                rightsizing_opportunity = {
                    "type": "scale_down",
                    "current_count": current_count,
                    "recommended_count": recommended_count,
                    "reduction": current_count - recommended_count,
                    "cost_savings": cost_savings,
                    "confidence": 85.0,
                    "rationale": f"Low utilization: CPU {cpu_utilization:.1f}%, Memory {memory_utilization:.1f}%"
                }
            
            # Check for right-sizing VM type
            elif cpu_utilization > 80 or memory_utilization > 80:
                # Recommend scaling up or changing VM size
                if current_count < max_count:
                    recommended_count = min(max_count, int(current_count * 1.3))  # 30% increase
                    additional_cost = pool_cost * (recommended_count - current_count) / current_count
                    
                    rightsizing_opportunity = {
                        "type": "scale_up",
                        "current_count": current_count,
                        "recommended_count": recommended_count,
                        "increase": recommended_count - current_count,
                        "additional_cost": additional_cost,
                        "confidence": 75.0,
                        "rationale": f"High utilization: CPU {cpu_utilization:.1f}%, Memory {memory_utilization:.1f}%"
                    }
                else:
                    # Recommend larger VM size
                    recommended_vm_size = self._get_next_vm_size(vm_size)
                    if recommended_vm_size:
                        rightsizing_opportunity = {
                            "type": "vm_size_upgrade",
                            "current_vm_size": vm_size,
                            "recommended_vm_size": recommended_vm_size,
                            "confidence": 70.0,
                            "rationale": f"High utilization and max node count reached"
                        }
            
             # Check for VM size optimization
            else:
                # Check if we can use smaller VM size
                target_cpu_cores = int((cpu_cores * current_count) * (cpu_utilization / 100) / self.thresholds['cpu_utilization_target'] * 100)
                target_memory_gb = int((memory_gb * current_count) * (memory_utilization / 100) / self.thresholds['memory_utilization_target'] * 100)
                
                smaller_vm_size = self._find_optimal_vm_size(target_cpu_cores / current_count, target_memory_gb / current_count)
                if smaller_vm_size and smaller_vm_size != vm_size:
                    cost_savings = pool_cost * 0.2  # Estimate 20% savings
                    rightsizing_opportunity = {
                        "type": "vm_size_optimization",
                        "current_vm_size": vm_size,
                        "recommended_vm_size": smaller_vm_size,
                        "potential_savings": cost_savings,
                        "confidence": 60.0,
                        "rationale": f"Current utilization allows for smaller VM size"
                    }
            
            if rightsizing_opportunity:
                rightsizing_opportunity["pool_name"] = pool_name
                node_pool_analysis["rightsizing_opportunities"].append(rightsizing_opportunity)
                
                # Add to recommended configuration
                node_pool_analysis["recommended_configuration"][pool_name] = rightsizing_opportunity
                
                # Add to potential savings
                node_pool_analysis["potential_savings"] += rightsizing_opportunity.get("cost_savings", 0.0)
        
        return node_pool_analysis
    
    async def _analyze_pod_rightsizing(self, cluster_data: Dict[str, Any], 
                                     raw_metrics: Dict[str, Any], 
                                     node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze pod-level rightsizing opportunities"""
        
        pod_analysis = {
            "total_pods": 0,
            "overprovisioned_pods": [],
            "underprovisioned_pods": [],
            "resource_efficiency": {
                "avg_cpu_utilization": 0.0,
                "avg_memory_utilization": 0.0,
                "avg_cpu_over_provisioning": 0.0,
                "avg_memory_over_provisioning": 0.0
            },
            "rightsizing_recommendations": [],
            "potential_savings": 0.0
        }
        
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        pods = k8s_resources.get("pods", [])
        pod_analysis["total_pods"] = len(pods)
        
        if not pods:
            return pod_analysis
        
        total_cost = node_rg_costs.get("total_cost", 0.0)
        cost_per_pod = total_cost / len(pods) if pods else 0.0
        
        cpu_utilizations = []
        memory_utilizations = []
        cpu_over_provisioning = []
        memory_over_provisioning = []
        
        for pod in pods:
            pod_name = pod.get("name", "unknown")
            namespace = pod.get("namespace", "unknown")
            
            # Get resource requests and limits
            resource_requests = pod.get("resource_requests", {})
            resource_limits = pod.get("resource_limits", {})
            
            cpu_request = resource_requests.get("cpu", 0)
            memory_request = resource_requests.get("memory", 0)
            cpu_limit = resource_limits.get("cpu", 0)
            memory_limit = resource_limits.get("memory", 0)
            
            # Get utilization data (would need more detailed metrics)
            # For now, simulate based on cluster-level utilization
            container_insights = raw_metrics.get("container_insights_data", {})
            
            # Estimate pod utilization based on cluster utilization
            cluster_cpu_util = 50.0  # Default
            cluster_memory_util = 50.0  # Default
            
            if "cluster_cpu_utilization" in container_insights:
                cpu_data = container_insights["cluster_cpu_utilization"]
                data_points = cpu_data.get("data_points", [])
                if data_points:
                    cpu_values = [point.get("cpu_utilization_percentage", 0) for point in data_points]
                    cluster_cpu_util = sum(cpu_values) / len(cpu_values)
            
            if "cluster_memory_utilization" in container_insights:
                memory_data = container_insights["cluster_memory_utilization"]
                data_points = memory_data.get("data_points", [])
                if data_points:
                    memory_values = [point.get("memory_utilization_percentage", 0) for point in data_points]
                    cluster_memory_util = sum(memory_values) / len(memory_values)
            
            # Use cluster utilization as proxy for pod utilization
            pod_cpu_util = cluster_cpu_util
            pod_memory_util = cluster_memory_util
            
            cpu_utilizations.append(pod_cpu_util)
            memory_utilizations.append(pod_memory_util)
            cpu_over_prov = 0
            memory_over_prov = 0
            # Calculate over-provisioning
            if cpu_request > 0:
                cpu_over_prov = max(0, (cpu_request - pod_cpu_util) / cpu_request * 100)
                cpu_over_provisioning.append(cpu_over_prov)
            
            if memory_request > 0:
                memory_over_prov = max(0, (memory_request - pod_memory_util) / memory_request * 100)
                memory_over_provisioning.append(memory_over_prov)
            
            # Identify overprovisioned pods
            if cpu_over_prov > self.thresholds['over_provisioning_threshold'] or memory_over_prov > self.thresholds['over_provisioning_threshold']:
                savings = cost_per_pod * 0.3  # Estimate 30% savings
                
                pod_analysis["overprovisioned_pods"].append({
                    "name": pod_name,
                    "namespace": namespace,
                    "cpu_over_provisioning": cpu_over_prov,
                    "memory_over_provisioning": memory_over_prov,
                    "current_cpu_request": cpu_request,
                    "current_memory_request": memory_request,
                    "recommended_cpu_request": cpu_request * 0.7,  # 30% reduction
                    "recommended_memory_request": memory_request * 0.7,  # 30% reduction
                    "potential_savings": savings
                })
            
            # Identify underprovisioned pods
            elif pod_cpu_util > 85 or pod_memory_util > 85:
                pod_analysis["underprovisioned_pods"].append({
                    "name": pod_name,
                    "namespace": namespace,
                    "cpu_utilization": pod_cpu_util,
                    "memory_utilization": pod_memory_util,
                    "current_cpu_request": cpu_request,
                    "current_memory_request": memory_request,
                    "recommended_cpu_request": cpu_request * 1.3,  # 30% increase
                    "recommended_memory_request": memory_request * 1.3,  # 30% increase
                    "risk": "performance degradation"
                })
        
        # Calculate resource efficiency
        if cpu_utilizations:
            pod_analysis["resource_efficiency"]["avg_cpu_utilization"] = sum(cpu_utilizations) / len(cpu_utilizations)
        
        if memory_utilizations:
            pod_analysis["resource_efficiency"]["avg_memory_utilization"] = sum(memory_utilizations) / len(memory_utilizations)
        
        if cpu_over_provisioning:
            pod_analysis["resource_efficiency"]["avg_cpu_over_provisioning"] = sum(cpu_over_provisioning) / len(cpu_over_provisioning)
        
        if memory_over_provisioning:
            pod_analysis["resource_efficiency"]["avg_memory_over_provisioning"] = sum(memory_over_provisioning) / len(memory_over_provisioning)
        
        # Calculate potential savings
        pod_analysis["potential_savings"] = sum(pod["potential_savings"] for pod in pod_analysis["overprovisioned_pods"])
        
        # Generate rightsizing recommendations
        if pod_analysis["overprovisioned_pods"]:
            pod_analysis["rightsizing_recommendations"].append({
                "type": "reduce_overprovisioning",
                "count": len(pod_analysis["overprovisioned_pods"]),
                "potential_savings": pod_analysis["potential_savings"],
                "recommendation": "Reduce CPU/memory requests for overprovisioned pods"
            })
        
        if pod_analysis["underprovisioned_pods"]:
            pod_analysis["rightsizing_recommendations"].append({
                "type": "increase_underprovisioning",
                "count": len(pod_analysis["underprovisioned_pods"]),
                "recommendation": "Increase CPU/memory requests for underprovisioned pods"
            })
        
        return pod_analysis
    
    async def _analyze_workload_rightsizing(self, cluster_data: Dict[str, Any], 
                                         raw_metrics: Dict[str, Any], 
                                         node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze workload-level rightsizing opportunities"""
        
        workload_analysis = {
            "total_workloads": 0,
            "rightsizing_opportunities": [],
            "scaling_recommendations": [],
            "resource_optimization": {},
            "potential_savings": 0.0
        }
        
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        deployments = k8s_resources.get("deployments", [])
        workload_analysis["total_workloads"] = len(deployments)
        
        if not deployments:
            return workload_analysis
        
        total_cost = node_rg_costs.get("total_cost", 0.0)
        cost_per_workload = total_cost / len(deployments) if deployments else 0.0
        
        for deployment in deployments:
            workload_name = deployment.get("name", "unknown")
            namespace = deployment.get("namespace", "unknown")
            replicas = deployment.get("replicas", 1)
            ready_replicas = deployment.get("ready_replicas", 0)
            
            # Get resource specifications
            resource_requests = deployment.get("resource_requests", {})
            resource_limits = deployment.get("resource_limits", {})
            
            # Analyze replica count
            if ready_replicas < replicas:
                # Some replicas are not ready
                workload_analysis["rightsizing_opportunities"].append({
                    "workload": f"{namespace}/{workload_name}",
                    "type": "replica_optimization",
                    "current_replicas": replicas,
                    "ready_replicas": ready_replicas,
                    "issue": "Some replicas not ready",
                    "recommendation": "Investigate failed replicas or reduce replica count"
                })
            
            # Analyze resource requests vs limits
            cpu_request = resource_requests.get("cpu", 0)
            cpu_limit = resource_limits.get("cpu", 0)
            memory_request = resource_requests.get("memory", 0)
            memory_limit = resource_limits.get("memory", 0)
            
            if cpu_limit > cpu_request * 2:  # Limit is 2x request
                savings = cost_per_workload * 0.15  # 15% savings
                workload_analysis["rightsizing_opportunities"].append({
                    "workload": f"{namespace}/{workload_name}",
                    "type": "resource_limit_optimization",
                    "current_cpu_request": cpu_request,
                    "current_cpu_limit": cpu_limit,
                    "recommended_cpu_limit": cpu_request * 1.5,  # 50% buffer
                    "potential_savings": savings,
                    "recommendation": "Reduce CPU limits closer to requests"
                })
            
            if memory_limit > memory_request * 2:  # Limit is 2x request
                savings = cost_per_workload * 0.15  # 15% savings
                workload_analysis["rightsizing_opportunities"].append({
                    "workload": f"{namespace}/{workload_name}",
                    "type": "resource_limit_optimization",
                    "current_memory_request": memory_request,
                    "current_memory_limit": memory_limit,
                    "recommended_memory_limit": memory_request * 1.5,  # 50% buffer
                    "potential_savings": savings,
                    "recommendation": "Reduce memory limits closer to requests"
                })
        
        # Calculate total potential savings
        workload_analysis["potential_savings"] = sum(
            opp.get("potential_savings", 0.0) for opp in workload_analysis["rightsizing_opportunities"]
        )
        
        return workload_analysis
    
    async def _generate_optimization_opportunities(self, analysis: Dict[str, Any], 
                                                 node_rg_costs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate optimization opportunities"""
        
        opportunities = []
        
        # Node pool opportunities
        node_pool_analysis = analysis.get("node_pool_rightsizing", {})
        node_pool_opportunities = node_pool_analysis.get("rightsizing_opportunities", [])
        
        for opp in node_pool_opportunities:
            opportunities.append({
                "type": "node_pool_optimization",
                "resource_id": opp.get("pool_name", "unknown"),
                "current_state": opp.get("current_vm_size", "unknown"),
                "recommended_state": opp.get("recommended_vm_size", "unknown"),
                "potential_savings": opp.get("cost_savings", 0.0),
                "confidence": opp.get("confidence", 0.0),
                "implementation_effort": "medium",
                "risk": "medium"
            })
        
        # Pod opportunities
        pod_analysis = analysis.get("pod_rightsizing", {})
        overprovisioned_pods = pod_analysis.get("overprovisioned_pods", [])
        
        for pod in overprovisioned_pods:
            opportunities.append({
                "type": "pod_rightsizing",
                "resource_id": f"{pod['namespace']}/{pod['name']}",
                "current_state": {
                    "cpu_request": pod["current_cpu_request"],
                    "memory_request": pod["current_memory_request"]
                },
                "recommended_state": {
                    "cpu_request": pod["recommended_cpu_request"],
                    "memory_request": pod["recommended_memory_request"]
                },
                "potential_savings": pod["potential_savings"],
                "confidence": 75.0,
                "implementation_effort": "low",
                "risk": "low"
            })
        
        # Workload opportunities
        workload_analysis = analysis.get("workload_rightsizing", {})
        workload_opportunities = workload_analysis.get("rightsizing_opportunities", [])
        
        for opp in workload_opportunities:
            opportunities.append({
                "type": "workload_optimization",
                "resource_id": opp.get("workload", "unknown"),
                "optimization_type": opp.get("type", "unknown"),
                "potential_savings": opp.get("potential_savings", 0.0),
                "confidence": 70.0,
                "implementation_effort": "low",
                "risk": "low"
            })
        
        # Sort by potential savings
        opportunities.sort(key=lambda x: x["potential_savings"], reverse=True)
        
        return opportunities
    
    async def _generate_scaling_recommendations(self, analysis: Dict[str, Any], 
                                              node_rg_costs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate scaling recommendations"""
        
        recommendations = []
        
        # Node pool scaling recommendations
        node_pool_analysis = analysis.get("node_pool_rightsizing", {})
        node_pool_opportunities = node_pool_analysis.get("rightsizing_opportunities", [])
        
        for opp in node_pool_opportunities:
            if opp.get("type") in ["scale_up", "scale_down"]:
                recommendations.append({
                    "type": "node_pool_scaling",
                    "pool_name": opp.get("pool_name", "unknown"),
                    "action": opp.get("type"),
                    "current_count": opp.get("current_count", 0),
                    "recommended_count": opp.get("recommended_count", 0),
                    "rationale": opp.get("rationale", ""),
                    "confidence": opp.get("confidence", 0.0),
                    "impact": opp.get("cost_savings", 0.0)
                })
        
        # HPA recommendations
        workload_analysis = analysis.get("workload_rightsizing", {})
        workload_opportunities = workload_analysis.get("rightsizing_opportunities", [])
        
        for opp in workload_opportunities:
            if opp.get("type") == "replica_optimization":
                recommendations.append({
                    "type": "horizontal_pod_autoscaler",
                    "workload": opp.get("workload", "unknown"),
                    "action": "implement_hpa",
                    "current_replicas": opp.get("current_replicas", 0),
                    "recommended_min_replicas": max(1, opp.get("ready_replicas", 0)),
                    "recommended_max_replicas": opp.get("current_replicas", 0),
                    "rationale": "Optimize replica count based on load",
                    "confidence": 60.0
                })
        
        return recommendations
    
    async def _create_resource_recommendations(self, analysis: Dict[str, Any], 
                                             node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Create resource recommendations"""
        
        recommendations = {
            "immediate_actions": [],
            "planned_actions": [],
            "long_term_actions": []
        }
        
        # Extract all opportunities
        opportunities = analysis.get("optimization_opportunities", [])
        
        for opp in opportunities:
            confidence = opp.get("confidence", 0.0)
            savings = opp.get("potential_savings", 0.0)
            risk = opp.get("risk", "medium")
            
            action = {
                "resource_id": opp.get("resource_id", "unknown"),
                "type": opp.get("type", "unknown"),
                "potential_savings": savings,
                "confidence": confidence,
                "risk": risk,
                "implementation_effort": opp.get("implementation_effort", "medium")
            }
            
            # Categorize by urgency and risk
            if confidence > 80 and risk == "low" and savings > 100:
                recommendations["immediate_actions"].append(action)
            elif confidence > 60 and savings > 50:
                recommendations["planned_actions"].append(action)
            else:
                recommendations["long_term_actions"].append(action)
        
        return recommendations
    
    async def _calculate_rightsizing_cost_impact(self, analysis: Dict[str, Any], 
                                               node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate cost impact of rightsizing"""
        
        cost_impact = {
            "total_potential_savings": 0.0,
            "savings_by_category": {},
            "implementation_costs": 0.0,
            "roi_analysis": {},
            "payback_period": 0.0
        }
        
        # Calculate savings by category
        categories = ["node_pool_rightsizing", "pod_rightsizing", "workload_rightsizing"]
        
        for category in categories:
            category_analysis = analysis.get(category, {})
            category_savings = category_analysis.get("potential_savings", 0.0)
            
            cost_impact["savings_by_category"][category] = category_savings
            cost_impact["total_potential_savings"] += category_savings
        
        # Estimate implementation costs
        opportunities = analysis.get("optimization_opportunities", [])
        implementation_cost = len(opportunities) * 50  # $50 per change
        cost_impact["implementation_costs"] = implementation_cost
        
        # ROI analysis
        annual_savings = cost_impact["total_potential_savings"] * 12
        if implementation_cost > 0:
            roi = ((annual_savings - implementation_cost) / implementation_cost) * 100
            payback_months = implementation_cost / max(cost_impact["total_potential_savings"], 1)
        else:
            roi = float('inf')
            payback_months = 0
        
        cost_impact["roi_analysis"] = {
            "annual_savings": annual_savings,
            "implementation_cost": implementation_cost,
            "roi_percentage": roi,
            "net_annual_benefit": annual_savings - implementation_cost
        }
        
        cost_impact["payback_period"] = payback_months
        
        return cost_impact
    
    async def _create_implementation_plan(self, analysis: Dict[str, Any], 
                                        node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Create implementation plan"""
        
        plan = {
            "phase_1_immediate": [],
            "phase_2_planned": [],
            "phase_3_long_term": [],
            "prerequisites": [],
            "success_metrics": []
        }
        
        opportunities = analysis.get("optimization_opportunities", [])
        
        for opp in opportunities:
            confidence = opp.get("confidence", 0.0)
            savings = opp.get("potential_savings", 0.0)
            risk = opp.get("risk", "medium")
            
            phase_item = {
                "resource_id": opp.get("resource_id", "unknown"),
                "action": opp.get("type", "unknown"),
                "potential_savings": savings,
                "confidence": confidence,
                "risk": risk
            }
            
            # Phase 1: High confidence, low risk, high savings
            if confidence > 80 and risk == "low" and savings > 100:
                plan["phase_1_immediate"].append(phase_item)
            # Phase 2: Medium confidence, medium risk
            elif confidence > 60 and savings > 50:
                plan["phase_2_planned"].append(phase_item)
            # Phase 3: Lower confidence or savings
            else:
                plan["phase_3_long_term"].append(phase_item)
        
        # Add prerequisites
        plan["prerequisites"] = [
            "Establish baseline metrics",
            "Set up monitoring and alerting",
            "Define rollback procedures",
            "Get stakeholder approval"
        ]
        
        # Add success metrics
        plan["success_metrics"] = [
            "Cost reduction achieved",
            "Resource utilization improved",
            "Application performance maintained",
            "No service degradation"
        ]
        
        return plan
    
    async def _calculate_confidence_scores(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate confidence scores for recommendations"""
        
        confidence_scores = {
            "overall_confidence": 0.0,
            "data_quality_score": 0.0,
            "recommendation_reliability": 0.0,
            "factors": []
        }
        
        # Factors affecting confidence
        factors = []
        
        # Data availability
        if analysis.get("node_pool_rightsizing", {}).get("rightsizing_opportunities"):
            factors.append({"factor": "node_pool_data_available", "weight": 0.3, "score": 90.0})
        
        if analysis.get("pod_rightsizing", {}).get("total_pods", 0) > 0:
            factors.append({"factor": "pod_data_available", "weight": 0.3, "score": 85.0})
        
        if analysis.get("workload_rightsizing", {}).get("total_workloads", 0) > 0:
            factors.append({"factor": "workload_data_available", "weight": 0.2, "score": 80.0})
        
        # Metrics quality
        opportunities = analysis.get("optimization_opportunities", [])
        if opportunities:
            avg_confidence = sum(opp.get("confidence", 0.0) for opp in opportunities) / len(opportunities)
            factors.append({"factor": "opportunity_confidence", "weight": 0.2, "score": avg_confidence})
        
        # Calculate overall confidence
        if factors:
            weighted_score = sum(factor["weight"] * factor["score"] for factor in factors)
            total_weight = sum(factor["weight"] for factor in factors)
            confidence_scores["overall_confidence"] = weighted_score / total_weight
        
        confidence_scores["factors"] = factors
        
        return confidence_scores
    
    # Helper methods
    def _parse_vm_size(self, vm_size: str) -> tuple:
        """Parse VM size to extract CPU cores and memory GB"""
        size_mappings = {
            "Standard_B2s": (2, 4),
            "Standard_B4ms": (4, 16),
            "Standard_D2s_v3": (2, 8),
            "Standard_D4s_v3": (4, 16),
            "Standard_D8s_v3": (8, 32),
            "Standard_D16s_v3": (16, 64),
            "Standard_DS2_v2": (2, 7),
            "Standard_DS3_v2": (4, 14),
            "Standard_DS4_v2": (8, 28),
            "Standard_DS5_v2": (16, 56),
            "Standard_E2s_v3": (2, 16),
            "Standard_E4s_v3": (4, 32),
            "Standard_E8s_v3": (8, 64),
            "Standard_E16s_v3": (16, 128),
            "Standard_F2s_v2": (2, 4),
            "Standard_F4s_v2": (4, 8),
            "Standard_F8s_v2": (8, 16),
            "Standard_F16s_v2": (16, 32),
        }
        
        return size_mappings.get(vm_size, (2, 4))
    
    def _get_next_vm_size(self, current_vm_size: str) -> str:
        """Get next larger VM size"""
        size_progression = {
            "Standard_D2s_v3": "Standard_D4s_v3",
            "Standard_D4s_v3": "Standard_D8s_v3",
            "Standard_D8s_v3": "Standard_D16s_v3",
            "Standard_B2s": "Standard_B4ms",
            "Standard_E2s_v3": "Standard_E4s_v3",
            "Standard_E4s_v3": "Standard_E8s_v3",
            "Standard_E8s_v3": "Standard_E16s_v3",
            "Standard_F2s_v2": "Standard_F4s_v2",
            "Standard_F4s_v2": "Standard_F8s_v2",
            "Standard_F8s_v2": "Standard_F16s_v2",
        }
        
        return size_progression.get(current_vm_size, current_vm_size)
    
    def _find_optimal_vm_size(self, target_cpu: float, target_memory: float) -> str:
        """Find optimal VM size for given requirements"""
        vm_sizes = {
            "Standard_B2s": (2, 4),
            "Standard_D2s_v3": (2, 8),
            "Standard_D4s_v3": (4, 16),
            "Standard_D8s_v3": (8, 32),
            "Standard_E2s_v3": (2, 16),
            "Standard_E4s_v3": (4, 32),
            "Standard_F2s_v2": (2, 4),
            "Standard_F4s_v2": (4, 8),
        }
        
        for vm_size, (cpu, memory) in vm_sizes.items():
            if cpu >= target_cpu and memory >= target_memory:
                return vm_size
        
        return "Standard_D4s_v3"