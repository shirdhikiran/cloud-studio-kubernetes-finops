# src/finops/analytics/cost_analytics.py
"""
Updated Phase 2 Cost Analytics - Integration with node_resource_group_costs
Focuses on leveraging the latest Phase 1 cost data structure
"""
import traceback
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import structlog

logger = structlog.get_logger(__name__)


class CostAnalytics:
    """
    Updated Cost Analytics Engine for Phase 2
    Now properly integrates with node_resource_group_costs from Phase 1
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(analytics="cost")
        
        # Cost analysis thresholds
        self.thresholds = {
            'high_cost_threshold': config.get('high_cost_threshold', 1000.0),
            'anomaly_threshold': config.get('cost_anomaly_threshold', 20.0),
            'waste_threshold': config.get('waste_threshold', 20.0),
            'efficiency_target': config.get('efficiency_target', 70.0)
        }
    
    async def analyze_cluster_costs(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main cost analysis method - updated to use node_resource_group_costs
        """
        cluster_name = cluster_data.get("cluster_info", {}).get("name", "unknown")

        
        self.logger.info(f"Starting updated cost analysis for {cluster_name}")
        
        analysis = {
            "cluster_name": cluster_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "cost_attribution": {},
            "cost_trends": {},
            "unit_costs": {},
            "cost_optimization": {},
            "anomalies": {},
            "recommendations": [],
            "total_monthly_cost": 0.0,
            "efficiency_score": 0.0
        }
        
        try:
            # Extract node_resource_group_costs (latest Phase 1 data)
            node_rg_costs = cluster_data.get("node_resource_group_costs", {})
            
            if node_rg_costs and "error" not in node_rg_costs:
                # 1. Updated Cost Attribution using node_resource_group_costs
                analysis["cost_attribution"] = await self._analyze_updated_cost_attribution(
                    cluster_data, node_rg_costs
                )
                
                # 2. Enhanced Cost Trends Analysis
                analysis["cost_trends"] = await self._analyze_enhanced_cost_trends(node_rg_costs)
                
                # 3. Updated Unit Cost Analysis
                analysis["unit_costs"] = await self._analyze_updated_unit_costs(
                    cluster_data, node_rg_costs
                )
                
                # 4. Cost Optimization with real cost data
                analysis["cost_optimization"] = await self._identify_enhanced_cost_optimization(
                    cluster_data, node_rg_costs
                )
                
                # 5. Cost Anomaly Detection
                analysis["anomalies"] = await self._detect_enhanced_cost_anomalies(node_rg_costs)
                
                # 6. Generate Updated Recommendations
                analysis["recommendations"] = await self._generate_enhanced_cost_recommendations(analysis)
                
                # 7. Calculate totals and efficiency with real data
                analysis["total_monthly_cost"] = self._calculate_actual_monthly_cost(node_rg_costs)
                analysis["efficiency_score"] = self._calculate_enhanced_cost_efficiency_score(analysis)
                
                self.logger.info(
                    f"Updated cost analysis completed for {cluster_name}",
                    total_cost=analysis["total_monthly_cost"],
                    efficiency_score=analysis["efficiency_score"]
                )
            else:
                self.logger.warning(f"No valid node_resource_group_costs data for {cluster_name}")
                analysis["error"] = "Missing or invalid node_resource_group_costs data"
            
        except Exception as e:
            import pdb; pdb.set_trace()
            self.logger.error(f"Updated cost analysis failed for {cluster_name}: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    async def _analyze_updated_cost_attribution(self, cluster_data: Dict[str, Any], 
                                              node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updated cost attribution using real Azure cost data from node_resource_group_costs
        """
        attribution = {
            "by_resource_type": {},
            "by_service": {},
            "by_resource_id": {},
            "by_meter_category": {},
            "by_node_pool": {},
            "by_namespace": {},
            "by_workload": {},
            "unallocated_costs": 0.0,
            "total_allocated_cost": 0.0
        }
        
        # Use actual cost data from node_resource_group_costs
        total_cost = node_rg_costs.get("total_cost", 0.0)
        attribution["total_allocated_cost"] = total_cost
        
        # Resource type attribution from actual Azure data
        by_resource_type = node_rg_costs.get("by_resource_type", {})
        for resource_type, cost_data in by_resource_type.items():
            attribution["by_resource_type"][resource_type] = {
                "cost": cost_data.get("cost", 0.0),
                "usage_quantity": cost_data.get("usage_quantity", 0.0),
                "percentage": (cost_data.get("cost", 0.0) / max(total_cost, 1)) * 100
            }
        
        # Service attribution from actual Azure data
        by_service = node_rg_costs.get("by_service", {})
        for service_name, cost_data in by_service.items():
            attribution["by_service"][service_name] = {
                "cost": cost_data.get("cost", 0.0),
                "usage_quantity": cost_data.get("usage_quantity", 0.0),
                "percentage": (cost_data.get("cost", 0.0) / max(total_cost, 1)) * 100
            }
        
        # Resource ID attribution from actual Azure data
        by_resource_id = node_rg_costs.get("by_resource_id", {})
        for resource_id, cost_data in by_resource_id.items():
            attribution["by_resource_id"][resource_id] = {
                "cost": cost_data.get("cost", 0.0),
                "resource_type": cost_data.get("resource_type", "unknown"),
                "service_name": cost_data.get("service_name", "unknown"),
                "percentage": (cost_data.get("cost", 0.0) / max(total_cost, 1)) * 100
            }
        
        # Meter category attribution
        by_meter_category = node_rg_costs.get("by_meter_category", {})
        for meter_category, cost_data in by_meter_category.items():
            attribution["by_meter_category"][meter_category] = {
                "cost": cost_data.get("cost", 0.0),
                "usage_quantity": cost_data.get("usage_quantity", 0.0),
                "percentage": (cost_data.get("cost", 0.0) / max(total_cost, 1)) * 100
            }
        
        # Enhanced node pool attribution using actual cost mapping
        await self._map_costs_to_node_pools(cluster_data, node_rg_costs, attribution)
        
        # Enhanced namespace attribution using cost allocation
        await self._map_costs_to_namespaces(cluster_data, attribution)
        
        # Enhanced workload attribution
        await self._map_costs_to_workloads(cluster_data, attribution)
        
        return attribution
    
    async def _analyze_enhanced_cost_trends(self, node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced cost trends analysis using actual daily cost data
        """
        trends = {
            "daily_trends": [],
            "weekly_pattern": {},
            "monthly_pattern": {},
            "cost_velocity": 0.0,
            "trend_direction": "stable",
            "volatility": 0.0,
            "forecasted_monthly_cost": 0.0
        }
        
        # Extract actual daily costs
        daily_breakdown = node_rg_costs.get("daily_breakdown", [])
        
        if daily_breakdown and len(daily_breakdown) > 1:
            # Process daily trends
            trends["daily_trends"] = daily_breakdown
            
            # Calculate cost velocity (rate of change)
            costs = [day.get("cost", 0.0) for day in daily_breakdown]
            if len(costs) >= 2:
                recent_costs = costs[-7:]  # Last 7 days
                older_costs = costs[-14:-7] if len(costs) >= 14 else costs[:-7]
                
                if older_costs:
                    recent_avg = sum(recent_costs) / len(recent_costs)
                    older_avg = sum(older_costs) / len(older_costs)
                    trends["cost_velocity"] = ((recent_avg - older_avg) / older_avg) * 100
                    
                    if trends["cost_velocity"] > 5:
                        trends["trend_direction"] = "increasing"
                    elif trends["cost_velocity"] < -5:
                        trends["trend_direction"] = "decreasing"
                    else:
                        trends["trend_direction"] = "stable"
            
            # Calculate volatility
            if len(costs) > 1:
                mean_cost = sum(costs) / len(costs)
                variance = sum((cost - mean_cost) ** 2 for cost in costs) / len(costs)
                trends["volatility"] = (variance ** 0.5) / mean_cost * 100
            
            # Weekly pattern analysis
            weekly_costs = defaultdict(list)
            for day in daily_breakdown:
                try:
                    date_obj = datetime.fromisoformat(day.get("date", ""))
                    weekday = date_obj.strftime("%A")
                    weekly_costs[weekday].append(day.get("cost", 0.0))
                except:
                    continue
            
            for weekday, costs in weekly_costs.items():
                trends["weekly_pattern"][weekday] = {
                    "average_cost": sum(costs) / len(costs),
                    "min_cost": min(costs),
                    "max_cost": max(costs),
                    "sample_size": len(costs)
                }
            
            # Forecast monthly cost based on recent trends
            if len(costs) >= 7:
                recent_daily_avg = sum(costs[-7:]) / 7
                trends["forecasted_monthly_cost"] = recent_daily_avg * 30
        
        return trends
    
    async def _analyze_updated_unit_costs(self, cluster_data: Dict[str, Any], 
                                        node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updated unit cost analysis using actual cost data
        """
        unit_costs = {
            "cost_per_pod": 0.0,
            "cost_per_namespace": 0.0,
            "cost_per_workload": 0.0,
            "cost_per_node": 0.0,
            "cost_per_cpu_core": 0.0,
            "cost_per_gb_memory": 0.0,
            "cost_per_vcpu_hour": 0.0,
            "cost_per_gb_hour": 0.0,
            "cost_efficiency_ratios": {}
        }
        
        total_cost = node_rg_costs.get("total_cost", 0.0)
        if total_cost == 0:
            return unit_costs
        
        # Kubernetes resource counts
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        
        # Cost per pod
        pods = k8s_resources.get("pods", [])
        if pods:
            unit_costs["cost_per_pod"] = total_cost / len(pods)
        
        # Cost per namespace
        namespaces = k8s_resources.get("namespaces", [])
        if namespaces:
            unit_costs["cost_per_namespace"] = total_cost / len(namespaces)
        
        # Cost per workload
        deployments = k8s_resources.get("deployments", [])
        if deployments:
            unit_costs["cost_per_workload"] = total_cost / len(deployments)
        
        # Node-based unit costs
        node_pools = cluster_data.get("node_pools", [])
        total_nodes = sum(pool.get("count", 0) for pool in node_pools)
        total_cpu_cores = 0
        total_memory_gb = 0
        
        for pool in node_pools:
            vm_size = pool.get("vm_size", "")
            cpu_cores, memory_gb = self._parse_vm_size(vm_size)
            node_count = pool.get("count", 0)
            
            total_cpu_cores += cpu_cores * node_count
            total_memory_gb += memory_gb * node_count
        
        if total_nodes > 0:
            unit_costs["cost_per_node"] = total_cost / total_nodes
        
        if total_cpu_cores > 0:
            unit_costs["cost_per_cpu_core"] = total_cost / total_cpu_cores
            unit_costs["cost_per_vcpu_hour"] = total_cost / total_cpu_cores / 24 / 30
        
        if total_memory_gb > 0:
            unit_costs["cost_per_gb_memory"] = total_cost / total_memory_gb
            unit_costs["cost_per_gb_hour"] = total_cost / total_memory_gb / 24 / 30
        
        # Enhanced cost efficiency ratios
        unit_costs["cost_efficiency_ratios"] = {
            "cost_per_pod_per_day": unit_costs["cost_per_pod"] / 30 if unit_costs["cost_per_pod"] > 0 else 0,
            "cost_per_request_per_hour": unit_costs["cost_per_cpu_core"] / 24 / 30 if unit_costs["cost_per_cpu_core"] > 0 else 0,
            "cost_per_workload_per_day": unit_costs["cost_per_workload"] / 30 if unit_costs["cost_per_workload"] > 0 else 0,
            "memory_cost_ratio": unit_costs["cost_per_gb_memory"] / max(unit_costs["cost_per_cpu_core"], 1) if unit_costs["cost_per_cpu_core"] > 0 else 0
        }
        
        return unit_costs
    
    async def _identify_enhanced_cost_optimization(self, cluster_data: Dict[str, Any], 
                                                 node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced cost optimization using actual cost breakdowns
        """
        optimization = {
            "spot_instance_opportunities": [],
            "reserved_instance_opportunities": [],
            "rightsizing_opportunities": [],
            "storage_optimization": [],
            "network_optimization": [],
            "service_optimization": [],
            "total_potential_savings": 0.0
        }
        
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        # Service-based optimization opportunities
        by_service = node_rg_costs.get("by_service", {})
        
        # VM/Compute optimization
        if "Virtual Machines" in by_service:
            vm_cost = by_service["Virtual Machines"].get("cost", 0.0)
            
            # Spot instance opportunities (typically 60-90% savings)
            node_pools = cluster_data.get("node_pools", [])
            for pool in node_pools:
                if pool.get("scale_set_priority") != "Spot":
                    pool_cost_estimate = vm_cost * (pool.get("count", 0) / max(sum(p.get("count", 0) for p in node_pools), 1))
                    spot_savings = pool_cost_estimate * 0.7  # 70% average savings
                    
                    optimization["spot_instance_opportunities"].append({
                        "node_pool": pool.get("name", "unknown"),
                        "current_cost": pool_cost_estimate,
                        "potential_savings": spot_savings,
                        "vm_size": pool.get("vm_size", ""),
                        "node_count": pool.get("count", 0),
                        "risk_level": "medium"
                    })
        
        # Storage optimization
        storage_services = ["Storage", "Managed Disks", "Storage Accounts"]
        storage_cost = sum(by_service.get(service, {}).get("cost", 0.0) for service in storage_services)
        
        if storage_cost > 0:
            # Premium to Standard disk optimization
            premium_savings = storage_cost * 0.3  # 30% typical savings
            optimization["storage_optimization"].append({
                "opportunity": "Premium to Standard disk migration",
                "current_cost": storage_cost,
                "potential_savings": premium_savings,
                "description": "Migrate non-critical workloads from Premium to Standard storage"
            })
        
        # Network optimization
        network_services = ["Load Balancer", "Application Gateway", "Public IP", "Bandwidth"]
        network_cost = sum(by_service.get(service, {}).get("cost", 0.0) for service in network_services)
        
        if network_cost > 0:
            # Network optimization opportunities
            network_savings = network_cost * 0.15  # 15% typical savings
            optimization["network_optimization"].append({
                "opportunity": "Network resource optimization",
                "current_cost": network_cost,
                "potential_savings": network_savings,
                "description": "Optimize load balancers and reduce unnecessary public IPs"
            })
        
        # Resource type optimization
        by_resource_type = node_rg_costs.get("by_resource_type", {})
        for resource_type, cost_data in by_resource_type.items():
            cost = cost_data.get("cost", 0.0)
            
            if cost > total_cost * 0.1:  # If resource type is >10% of total cost
                if "disk" in resource_type.lower():
                    optimization["storage_optimization"].append({
                        "opportunity": f"{resource_type} optimization",
                        "current_cost": cost,
                        "potential_savings": cost * 0.2,
                        "description": f"Optimize {resource_type} configuration and sizing"
                    })
        
        # Calculate total potential savings
        optimization["total_potential_savings"] = (
            sum(opp["potential_savings"] for opp in optimization["spot_instance_opportunities"]) +
            sum(opp["potential_savings"] for opp in optimization["storage_optimization"]) +
            sum(opp["potential_savings"] for opp in optimization["network_optimization"])
        )
        
        return optimization
    
    async def _detect_enhanced_cost_anomalies(self, node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced cost anomaly detection using actual cost data
        """
        anomalies = {
            "daily_anomalies": [],
            "service_anomalies": [],
            "resource_anomalies": [],
            "trend_anomalies": [],
            "total_anomaly_cost": 0.0
        }
        
        daily_breakdown = node_rg_costs.get("daily_breakdown", [])
        
        if len(daily_breakdown) >= 7:
            costs = [day.get("cost", 0.0) for day in daily_breakdown]
            mean_cost = sum(costs) / len(costs)
            std_dev = (sum((cost - mean_cost) ** 2 for cost in costs) / len(costs)) ** 0.5
            
            threshold = mean_cost + (2 * std_dev)  # 2 standard deviations
            
            for day in daily_breakdown:
                daily_cost = day.get("cost", 0.0)
                if daily_cost > threshold:
                    anomaly_cost = daily_cost - mean_cost
                    anomalies["daily_anomalies"].append({
                        "date": day.get("date", ""),
                        "cost": daily_cost,
                        "expected_cost": mean_cost,
                        "anomaly_amount": anomaly_cost,
                        "severity": "high" if anomaly_cost > mean_cost * 0.5 else "medium"
                    })
                    anomalies["total_anomaly_cost"] += anomaly_cost
        
        # Service cost anomalies
        by_service = node_rg_costs.get("by_service", {})
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        for service_name, cost_data in by_service.items():
            service_cost = cost_data.get("cost", 0.0)
            service_percentage = (service_cost / max(total_cost, 1)) * 100
            
            # Flag services that consume unusual percentage of costs
            if service_percentage > 50:  # More than 50% of total cost
                anomalies["service_anomalies"].append({
                    "service": service_name,
                    "cost": service_cost,
                    "percentage": service_percentage,
                    "severity": "high",
                    "description": f"{service_name} consuming {service_percentage:.1f}% of total cost"
                })
        
        return anomalies
    
    async def _generate_enhanced_cost_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """
        Generate enhanced cost recommendations based on actual cost analysis
        """
        recommendations = []
        
        cost_attribution = analysis.get("cost_attribution", {})
        cost_optimization = analysis.get("cost_optimization", {})
        anomalies = analysis.get("anomalies", {})
        
        # Recommendations based on service costs
        by_service = cost_attribution.get("by_service", {})
        for service_name, service_data in by_service.items():
            percentage = service_data.get("percentage", 0.0)
            
            if percentage > 40:
                recommendations.append(
                    f"Focus optimization on {service_name} (consuming {percentage:.1f}% of total cost)"
                )
        
        # Spot instance recommendations
        spot_opportunities = cost_optimization.get("spot_instance_opportunities", [])
        if spot_opportunities:
            total_spot_savings = sum(opp["potential_savings"] for opp in spot_opportunities)
            recommendations.append(
                f"Implement spot instances for suitable workloads - potential savings: ${total_spot_savings:.2f}/month"
            )
        
        # Storage optimization recommendations
        storage_opportunities = cost_optimization.get("storage_optimization", [])
        if storage_opportunities:
            total_storage_savings = sum(opp["potential_savings"] for opp in storage_opportunities)
            recommendations.append(
                f"Optimize storage configuration - potential savings: ${total_storage_savings:.2f}/month"
            )
        
        # Anomaly-based recommendations
        daily_anomalies = anomalies.get("daily_anomalies", [])
        if daily_anomalies:
            recommendations.append(
                f"Investigate cost spikes detected on {len(daily_anomalies)} days - total anomaly cost: ${anomalies.get('total_anomaly_cost', 0):.2f}"
            )
        
        # Total potential savings recommendation
        total_savings = cost_optimization.get("total_potential_savings", 0.0)
        total_monthly_cost = analysis.get("total_monthly_cost", 0.0)

        if total_savings > 0:
            if total_monthly_cost > 0:
                savings_percent = (total_savings / total_monthly_cost) * 100
            else:
                savings_percent = 0.0  # or float('inf') if you want to indicate undefined % savings

            recommendations.append(
                f"Total identified cost optimization potential: ${total_savings:.2f}/month ({savings_percent:.1f}% savings)"
            )

        
        return recommendations
    
    def _calculate_actual_monthly_cost(self, node_rg_costs: Dict[str, Any]) -> float:
        """Calculate actual monthly cost from node resource group costs"""
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        # # If we have daily breakdown, extrapolate to monthly
        # daily_breakdown = node_rg_costs.get("daily_breakdown", [])
        # if daily_breakdown:
        #     # Calculate average daily cost from recent data
        #     recent_days = daily_breakdown[-7:] if len(daily_breakdown) >= 7 else daily_breakdown
        #     if recent_days:
        #         avg_daily_cost = sum(day.get("cost", 0.0) for day in recent_days) / len(recent_days)
        #         return avg_daily_cost * 30
        
        return total_cost
    
    def _calculate_enhanced_cost_efficiency_score(self, analysis: Dict[str, Any]) -> float:
        """Calculate enhanced cost efficiency score"""
        score = 100.0
        
        # Deduct points for high waste
        cost_optimization = analysis.get("cost_optimization", {})
        total_cost = analysis.get("total_monthly_cost", 1)
        potential_savings = cost_optimization.get("total_potential_savings", 0.0)
        
        if total_cost > 0:
            waste_percentage = (potential_savings / total_cost) * 100
            score -= min(waste_percentage, 50)  # Max 50 points deduction for waste
        
        # Deduct points for anomalies
        anomalies = analysis.get("anomalies", {})
        anomaly_cost = anomalies.get("total_anomaly_cost", 0.0)
        if total_cost > 0:
            anomaly_percentage = (anomaly_cost / total_cost) * 100
            score -= min(anomaly_percentage, 20)  # Max 20 points deduction for anomalies
        
        # Deduct points for poor attribution
        cost_attribution = analysis.get("cost_attribution", {})
        unallocated_costs = cost_attribution.get("unallocated_costs", 0.0)
        if total_cost > 0:
            unallocated_percentage = (unallocated_costs / total_cost) * 100
            score -= min(unallocated_percentage, 30)  # Max 30 points deduction for poor attribution
        
        return max(0.0, score)
    
    async def _map_costs_to_node_pools(self, cluster_data: Dict[str, Any], 
                                     node_rg_costs: Dict[str, Any], 
                                     attribution: Dict[str, Any]):
        """Map Azure costs to specific node pools"""
        node_pools = cluster_data.get("node_pools", [])
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        # Use VM-related costs from Azure data
        by_service = node_rg_costs.get("by_service", {})
        vm_cost = by_service.get("Virtual Machines", {}).get("cost", total_cost)
        
        total_nodes = sum(pool.get("count", 0) for pool in node_pools)
        
        for pool in node_pools:
            pool_name = pool.get("name", "unknown")
            node_count = pool.get("count", 0)
            
            # Allocate cost proportionally by node count
            if total_nodes > 0:
                pool_cost = vm_cost * (node_count / total_nodes)
                attribution["by_node_pool"][pool_name] = {
                    "cost": pool_cost,
                    "node_count": node_count,
                    "vm_size": pool.get("vm_size", ""),
                    "cost_per_node": pool_cost / max(node_count, 1),
                    "percentage": (pool_cost / max(total_cost, 1)) * 100
                }
    
    async def _map_costs_to_namespaces(self, cluster_data: Dict[str, Any], 
                                     attribution: Dict[str, Any]):
        """Map costs to Kubernetes namespaces"""
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        namespaces = k8s_resources.get("namespaces", [])
        
        total_node_pool_cost = sum(
            pool_data["cost"] for pool_data in attribution["by_node_pool"].values()
        )
        
        total_pods = sum(len(ns.get("pods", [])) for ns in namespaces)
        
        for ns in namespaces:
            ns_name = ns.get("name", "unknown")
            pod_count = len(ns.get("pods", []))
            
            # Allocate cost proportionally by pod count
            if total_pods > 0:
                ns_cost = total_node_pool_cost * (pod_count / total_pods)
                attribution["by_namespace"][ns_name] = {
                    "cost": ns_cost,
                    "pod_count": pod_count,
                    "cost_per_pod": ns_cost / max(pod_count, 1),
                    "percentage": (ns_cost / max(total_node_pool_cost, 1)) * 100
                }
    
    async def _map_costs_to_workloads(self, cluster_data: Dict[str, Any], 
                                    attribution: Dict[str, Any]):
        """Map costs to Kubernetes workloads"""
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        deployments = k8s_resources.get("deployments", [])
        
        total_namespace_cost = sum(
            ns_data["cost"] for ns_data in attribution["by_namespace"].values()
        )
        
        for deployment in deployments:
            workload_name = f"{deployment.get('namespace', 'unknown')}/{deployment.get('name', 'unknown')}"
            namespace = deployment.get("namespace", "unknown")
            replicas = deployment.get("replicas", 1)
            
            # Get namespace cost allocation
            ns_cost_data = attribution["by_namespace"].get(namespace, {})
            ns_cost = ns_cost_data.get("cost", 0.0)
            ns_pod_count = ns_cost_data.get("pod_count", 1)
            
            # Allocate cost proportionally by replica count
            workload_cost = ns_cost * (replicas / max(ns_pod_count, 1))
            attribution["by_workload"][workload_name] = {
                "cost": workload_cost,
                "namespace": namespace,
                "replicas": replicas,
                "cost_per_replica": workload_cost / max(replicas, 1),
                "percentage": (workload_cost / max(total_namespace_cost, 1)) * 100
            }
    
    def _parse_vm_size(self, vm_size: str) -> tuple:
        """Parse VM size to extract CPU cores and memory GB"""
        # Standard Azure VM size parsing
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
        
        return size_mappings.get(vm_size, (2, 4))  # Default to 2 cores, 4GB
    
    def _estimate_vm_cost(self, vm_size: str, node_count: int) -> float:
        """Estimate VM cost based on size and count"""
        # Rough cost estimates (USD/hour) - should be updated with actual pricing
        cost_per_hour = {
            "Standard_B2s": 0.0416,
            "Standard_B4ms": 0.166,
            "Standard_D2s_v3": 0.096,
            "Standard_D4s_v3": 0.192,
            "Standard_D8s_v3": 0.384,
            "Standard_D16s_v3": 0.768,
            "Standard_DS2_v2": 0.135,
            "Standard_DS3_v2": 0.27,
            "Standard_DS4_v2": 0.54,
            "Standard_DS5_v2": 1.08,
            "Standard_E2s_v3": 0.126,
            "Standard_E4s_v3": 0.252,
            "Standard_E8s_v3": 0.504,
            "Standard_E16s_v3": 1.008,
            "Standard_F2s_v2": 0.085,
            "Standard_F4s_v2": 0.169,
            "Standard_F8s_v2": 0.338,
            "Standard_F16s_v2": 0.676,
        }
        
        hourly_cost = cost_per_hour.get(vm_size, 0.096)  # Default to D2s_v3 pricing
        return hourly_cost * node_count * 24 * 30  # Monthly cost


# Additional analytics classes for Phase 2

class UtilizationAnalytics:
    """Updated Utilization Analytics for Phase 2"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(analytics="utilization")
        
        self.thresholds = {
            'low_utilization_threshold': config.get('low_utilization_threshold', 30.0),
            'high_utilization_threshold': config.get('high_utilization_threshold', 80.0),
            'cpu_target_utilization': config.get('cpu_target_utilization', 70.0),
            'memory_target_utilization': config.get('memory_target_utilization', 70.0)
        }
    
    async def analyze_cluster_utilization(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced utilization analysis integrating with node_resource_group_costs
        """
        cluster_name = cluster_data.get("cluster_info", {}).get("name", "unknown")

        
        analysis = {
            "cluster_name": cluster_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "cluster_utilization": {},
            "node_utilization": {},
            "pod_utilization": {},
            "capacity_analysis": {},
            "efficiency_metrics": {},
            "recommendations": []
        }
        
        try:
            # Get raw metrics and cost data
            raw_metrics = cluster_data.get("raw_metrics", {})
            node_rg_costs = cluster_data.get("node_resource_group_costs", {})
            
            # 1. Cluster-level utilization analysis
            analysis["cluster_utilization"] = await self._analyze_cluster_utilization_metrics(
                cluster_data, raw_metrics
            )
            
            # 2. Node-level utilization analysis
            analysis["node_utilization"] = await self._analyze_node_utilization_metrics(
                cluster_data, raw_metrics
            )
            
            # 3. Pod-level utilization analysis
            analysis["pod_utilization"] = await self._analyze_pod_utilization_metrics(
                cluster_data, raw_metrics
            )
            
            # 4. Capacity analysis with cost correlation
            analysis["capacity_analysis"] = await self._analyze_capacity_with_costs(
                cluster_data, raw_metrics, node_rg_costs
            )
            
            # 5. Efficiency metrics calculation
            analysis["efficiency_metrics"] = await self._calculate_efficiency_metrics(
                analysis, node_rg_costs
            )
            
            # 6. Generate utilization recommendations
            analysis["recommendations"] = await self._generate_utilization_recommendations(
                analysis, node_rg_costs
            )
            
            self.logger.info(f"Utilization analysis completed for {cluster_name}")
            
        except Exception as e:
            self.logger.error(f"Utilization analysis failed for {cluster_name}: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    async def _analyze_cluster_utilization_metrics(self, cluster_data: Dict[str, Any], 
                                                 raw_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cluster-level utilization metrics"""
        cluster_util = {
            "cpu_utilization": {"current": 0.0, "average": 0.0, "peak": 0.0},
            "memory_utilization": {"current": 0.0, "average": 0.0, "peak": 0.0},
            "node_utilization": {"active_nodes": 0, "total_nodes": 0},
            "pod_utilization": {"running_pods": 0, "total_capacity": 0},
            "resource_efficiency": 0.0
        }
        
        # Extract Container Insights data
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # CPU utilization
        if "cluster_cpu_utilization" in container_insights:
            cpu_data = container_insights["cluster_cpu_utilization"]
            data_points = cpu_data.get("data_points", [])
            
            if data_points:
                cpu_values = [point.get("cpu_utilization_percentage", 0) for point in data_points]
                cluster_util["cpu_utilization"] = {
                    "current": cpu_values[-1] if cpu_values else 0.0,
                    "average": sum(cpu_values) / len(cpu_values),
                    "peak": max(cpu_values),
                    "trend": "increasing" if len(cpu_values) > 1 and cpu_values[-1] > cpu_values[0] else "stable"
                }
        
        # Memory utilization
        if "cluster_memory_utilization" in container_insights:
            memory_data = container_insights["cluster_memory_utilization"]
            data_points = memory_data.get("data_points", [])
            
            if data_points:
                memory_values = [point.get("memory_utilization_percentage", 0) for point in data_points]
                cluster_util["memory_utilization"] = {
                    "current": memory_values[-1] if memory_values else 0.0,
                    "average": sum(memory_values) / len(memory_values),
                    "peak": max(memory_values),
                    "trend": "increasing" if len(memory_values) > 1 and memory_values[-1] > memory_values[0] else "stable"
                }
        
        # Node utilization
        node_pools = cluster_data.get("node_pools", [])
        total_nodes = sum(pool.get("count", 0) for pool in node_pools)
        cluster_util["node_utilization"]["total_nodes"] = total_nodes
        cluster_util["node_utilization"]["active_nodes"] = total_nodes  # Assume all nodes are active
        
        # Pod utilization
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        pods = k8s_resources.get("pods", [])
        running_pods = len([pod for pod in pods if pod.get("status", {}).get("phase") == "Running"])
        cluster_util["pod_utilization"]["running_pods"] = running_pods
        cluster_util["pod_utilization"]["total_capacity"] = total_nodes * 110  # Assume 110 pods per node max
        
        # Calculate resource efficiency
        cpu_eff = cluster_util["cpu_utilization"]["average"] / 100
        memory_eff = cluster_util["memory_utilization"]["average"] / 100
        cluster_util["resource_efficiency"] = ((cpu_eff + memory_eff) / 2) * 100
        
        return cluster_util
    
    async def _analyze_capacity_with_costs(self, cluster_data: Dict[str, Any], 
                                         raw_metrics: Dict[str, Any], 
                                         node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze capacity with cost correlation"""
        capacity_analysis = {
            "current_capacity": {},
            "utilization_efficiency": {},
            "cost_per_utilized_resource": {},
            "headroom_analysis": {},
            "scaling_recommendations": []
        }
        
        # Current capacity from node pools
        node_pools = cluster_data.get("node_pools", [])
        total_cpu_cores = 0
        total_memory_gb = 0
        
        for pool in node_pools:
            vm_size = pool.get("vm_size", "")
            node_count = pool.get("count", 0)
            
            # Parse VM size to get resources
            cpu_cores, memory_gb = self._parse_vm_size(vm_size)
            
            total_cpu_cores += cpu_cores * node_count
            total_memory_gb += memory_gb * node_count
        
        capacity_analysis["current_capacity"] = {
            "total_cpu_cores": total_cpu_cores,
            "total_memory_gb": total_memory_gb,
            "total_nodes": sum(pool.get("count", 0) for pool in node_pools)
        }
        
        # Cost efficiency metrics
        total_cost = node_rg_costs.get("total_cost", 0.0)
        if total_cost > 0:
            capacity_analysis["cost_per_utilized_resource"] = {
                "cost_per_cpu_core": total_cost / max(total_cpu_cores, 1),
                "cost_per_gb_memory": total_cost / max(total_memory_gb, 1),
                "cost_per_node": total_cost / max(len(node_pools), 1)
            }
        
        # Headroom analysis
        container_insights = raw_metrics.get("container_insights_data", {})
        if "cluster_capacity_summary" in container_insights:
            capacity_data = container_insights["cluster_capacity_summary"]
            latest_point = capacity_data.get("data_points", [{}])[-1] if capacity_data.get("data_points") else {}
            
            cpu_utilization = latest_point.get("cpu_utilization_percentage", 0)
            memory_utilization = latest_point.get("memory_utilization_percentage", 0)
            
            capacity_analysis["headroom_analysis"] = {
                "cpu_headroom_percentage": 100 - cpu_utilization,
                "memory_headroom_percentage": 100 - memory_utilization,
                "cpu_headroom_cores": total_cpu_cores * (100 - cpu_utilization) / 100,
                "memory_headroom_gb": total_memory_gb * (100 - memory_utilization) / 100,
                "wasted_cost": total_cost * (100 - max(cpu_utilization, memory_utilization)) / 100
            }
        
        return capacity_analysis
    
    def _parse_vm_size(self, vm_size: str) -> tuple:
        """Parse VM size to extract CPU cores and memory GB"""
        # Reuse the same method from CostAnalytics
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
        
        return size_mappings.get(vm_size, (2, 4))  # Default to 2 cores, 4GB