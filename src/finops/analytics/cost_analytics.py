# src/finops/analytics/cost_analytics.py
"""
Cost Analytics Module - Advanced cost analysis and attribution
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import structlog
import statistics
from collections import defaultdict
import numpy as np

logger = structlog.get_logger(__name__)


class CostAnalytics:
    """Advanced cost analytics for FinOps insights"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(module="cost_analytics")
        
        # Cost analysis thresholds
        self.thresholds = {
            'high_cost_pod_threshold': config.get('high_cost_pod_threshold', 100.0),  # USD/month
            'cost_variance_threshold': config.get('cost_variance_threshold', 15.0),   # %
            'unit_cost_anomaly_threshold': config.get('unit_cost_anomaly_threshold', 25.0),  # %
        }
    
    async def analyze_cluster_costs(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive cost analysis for a cluster"""
        
        cluster_info = cluster_data.get("cluster_info", {})
        cluster_name = cluster_info.get("name", "unknown")
        
        self.logger.info(f"Starting cost analysis for cluster: {cluster_name}")
        
        # Extract cost data
        detailed_costs = cluster_data.get("detailed_costs", {})
        # raw_cost_data = cluster_data.get("raw_cost_data", {})
        
        analysis = {
            "cluster_name": cluster_name,
            "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
            "cost_attribution": {},
            "cost_trends": {},
            "unit_costs": {},
            "cost_optimization": {},
            "cost_forecasting": {},
            "anomalies": [],
            "recommendations": [],
            "total_monthly_cost": 0.0,
            "efficiency_score": 0.0
        }
        
        try:
            # 1. Cost Attribution Analysis
            analysis["cost_attribution"] = await self._analyze_cost_attribution(
                cluster_data, detailed_costs
            )
            
            # 2. Cost Trends Analysis
            analysis["cost_trends"] = await self._analyze_cost_trends(detailed_costs)
            
            # 3. Unit Cost Analysis
            analysis["unit_costs"] = await self._analyze_unit_costs(cluster_data)
            
            # 4. Cost Optimization Opportunities
            analysis["cost_optimization"] = await self._identify_cost_optimization(cluster_data)
            
            # 5. Cost Anomaly Detection
            analysis["anomalies"] = await self._detect_cost_anomalies(detailed_costs)
            
            # 6. Generate Recommendations
            analysis["recommendations"] = await self._generate_cost_recommendations(analysis)
            
            # 7. Calculate totals and efficiency
            analysis["total_monthly_cost"] = self._calculate_total_monthly_cost(detailed_costs)
            analysis["efficiency_score"] = self._calculate_cost_efficiency_score(analysis)
            
            self.logger.info(
                f"Cost analysis completed for {cluster_name}",
                total_cost=analysis["total_monthly_cost"],
                efficiency_score=analysis["efficiency_score"]
            )
            
        except Exception as e:
            self.logger.error(f"Cost analysis failed for {cluster_name}: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    async def _analyze_cost_attribution(self, cluster_data: Dict[str, Any], 
                                      detailed_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cost attribution by various dimensions"""
        
        attribution = {
            "by_resource_type": {},
            "by_service": {},
            "by_node_pool": {},
            "by_namespace": {},
            "by_workload": {},
            "unallocated_costs": 0.0
        }
        
        # Resource-level costs from detailed_costs
        resources = detailed_costs.get("resources", {})
        total_resource_cost = 0.0
        
        for resource_id, resource_data in resources.items():
            cost = resource_data.get("total_cost", 0.0)
            total_resource_cost += cost
            
            # Attribution by resource type
            resource_type = resource_data.get("resource_type", "unknown")
            if resource_type not in attribution["by_resource_type"]:
                attribution["by_resource_type"][resource_type] = {"cost": 0.0, "count": 0}
            attribution["by_resource_type"][resource_type]["cost"] += cost
            attribution["by_resource_type"][resource_type]["count"] += 1
            
            # Attribution by service
            service_name = resource_data.get("service_name", "unknown")
            if service_name not in attribution["by_service"]:
                attribution["by_service"][service_name] = {"cost": 0.0, "count": 0}
            attribution["by_service"][service_name]["cost"] += cost
            attribution["by_service"][service_name]["count"] += 1
        
        # Node pool attribution
        node_pools = cluster_data.get("node_pools", [])
        for pool in node_pools:
            pool_name = pool.get("name", "unknown")
            # Estimate pool cost based on VM size and count
            vm_cost = self._estimate_vm_cost(pool.get("vm_size", ""), pool.get("count", 0))
            attribution["by_node_pool"][pool_name] = {
                "cost": vm_cost,
                "vm_size": pool.get("vm_size", ""),
                "node_count": pool.get("count", 0),
                "cost_per_node": vm_cost / max(1, pool.get("count", 1))
            }
        
        # Kubernetes resource attribution
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        
        # Namespace attribution
        namespaces = k8s_resources.get("namespaces", [])
        for ns in namespaces:
            ns_name = ns.get("name", "unknown")
            # Estimate namespace cost based on pod count and resource requests
            ns_cost = self._estimate_namespace_cost(ns, attribution["by_node_pool"])
            attribution["by_namespace"][ns_name] = {
                "cost": ns_cost,
                "pod_count": len(ns.get("pods", [])),
                "cost_per_pod": ns_cost / max(1, len(ns.get("pods", [])))
            }
        
        # Workload attribution
        deployments = k8s_resources.get("deployments", [])
        for deployment in deployments:
            workload_name = f"{deployment.get('namespace', 'unknown')}/{deployment.get('name', 'unknown')}"
            replica_count = deployment.get("replicas", 1)
            # Estimate workload cost
            workload_cost = self._estimate_workload_cost(deployment, attribution["by_namespace"])
            attribution["by_workload"][workload_name] = {
                "cost": workload_cost,
                "replicas": replica_count,
                "namespace": deployment.get("namespace", "unknown"),
                "cost_per_replica": workload_cost / max(1, replica_count)
            }
        
        return attribution
    
    async def _analyze_cost_trends(self, detailed_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cost trends and patterns"""
        
        trends = {
            "daily_trend": [],
            "weekly_pattern": {},
            "cost_velocity": 0.0,
            "trend_direction": "stable",
            "volatility": 0.0
        }
        
        # Extract daily costs from raw data
        raw_data = detailed_costs.get("raw_data", {})
        daily_breakdown = raw_data.get("daily_breakdown", [])
        
        if daily_breakdown:
            costs = [day.get("cost", 0.0) for day in daily_breakdown]
            dates = [day.get("date", "") for day in daily_breakdown]
            
            # Calculate daily trend
            trends["daily_trend"] = [
                {"date": date, "cost": cost} 
                for date, cost in zip(dates, costs)
            ]
            
            # Calculate cost velocity (rate of change)
            if len(costs) >= 2:
                recent_avg = statistics.mean(costs[-7:]) if len(costs) >= 7 else statistics.mean(costs[-3:])
                older_avg = statistics.mean(costs[:7]) if len(costs) >= 14 else statistics.mean(costs[:len(costs)//2])
                
                if older_avg > 0:
                    trends["cost_velocity"] = ((recent_avg - older_avg) / older_avg) * 100
                
                # Determine trend direction
                if trends["cost_velocity"] > 5:
                    trends["trend_direction"] = "increasing"
                elif trends["cost_velocity"] < -5:
                    trends["trend_direction"] = "decreasing"
                else:
                    trends["trend_direction"] = "stable"
            
            # Calculate volatility
            if len(costs) > 1:
                trends["volatility"] = statistics.stdev(costs) / statistics.mean(costs) * 100
            
            # Weekly pattern analysis
            for i, day_data in enumerate(daily_breakdown):
                day_of_week = (i % 7)  # Simplified day mapping
                day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                day_name = day_names[day_of_week]
                
                if day_name not in trends["weekly_pattern"]:
                    trends["weekly_pattern"][day_name] = []
                trends["weekly_pattern"][day_name].append(day_data.get("cost", 0.0))
            
            # Average weekly pattern
            for day, costs_list in trends["weekly_pattern"].items():
                trends["weekly_pattern"][day] = {
                    "average_cost": statistics.mean(costs_list),
                    "cost_variance": statistics.stdev(costs_list) if len(costs_list) > 1 else 0.0
                }
        
        return trends
    
    async def _analyze_unit_costs(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate various unit cost metrics"""
        
        unit_costs = {
            "cost_per_pod": 0.0,
            "cost_per_cpu_core": 0.0,
            "cost_per_gb_memory": 0.0,
            "cost_per_node": 0.0,
            "cost_per_namespace": 0.0,
            "cost_per_workload": 0.0,
            "cost_efficiency_ratios": {}
        }
        
        # Get total cost
        detailed_costs = cluster_data.get("detailed_costs", {})
        total_cost = detailed_costs.get("summary", {}).get("total_cost", 0.0)
        
        # Get Kubernetes metrics
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        
        # Calculate unit costs
        pod_count = len(k8s_resources.get("pods", []))
        if pod_count > 0:
            unit_costs["cost_per_pod"] = total_cost / pod_count
        
        namespace_count = len(k8s_resources.get("namespaces", []))
        if namespace_count > 0:
            unit_costs["cost_per_namespace"] = total_cost / namespace_count
        
        deployment_count = len(k8s_resources.get("deployments", []))
        if deployment_count > 0:
            unit_costs["cost_per_workload"] = total_cost / deployment_count
        
        # Calculate resource-based unit costs
        node_pools = cluster_data.get("node_pools", [])
        total_nodes = sum(pool.get("count", 0) for pool in node_pools)
        total_cpu_cores = 0
        total_memory_gb = 0
        
        for pool in node_pools:
            # Estimate resources per node based on VM size
            vm_size = pool.get("vm_size", "")
            cpu_cores, memory_gb = self._parse_vm_size(vm_size)
            node_count = pool.get("count", 0)
            
            total_cpu_cores += cpu_cores * node_count
            total_memory_gb += memory_gb * node_count
        
        if total_nodes > 0:
            unit_costs["cost_per_node"] = total_cost / total_nodes
        
        if total_cpu_cores > 0:
            unit_costs["cost_per_cpu_core"] = total_cost / total_cpu_cores
        
        if total_memory_gb > 0:
            unit_costs["cost_per_gb_memory"] = total_cost / total_memory_gb
        
        # Cost efficiency ratios
        unit_costs["cost_efficiency_ratios"] = {
            "cost_per_pod_per_day": unit_costs["cost_per_pod"] / 30,  # Assuming monthly cost
            "cost_per_cpu_hour": unit_costs["cost_per_cpu_core"] / 24 / 30,
            "cost_per_gb_memory_hour": unit_costs["cost_per_gb_memory"] / 24 / 30
        }
        
        return unit_costs
    
    async def _identify_cost_optimization(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Identify cost optimization opportunities"""
        
        optimization = {
            "spot_instance_opportunities": [],
            "reserved_instance_opportunities": [],
            "rightsizing_opportunities": [],
            "storage_optimization": [],
            "network_optimization": [],
            "total_savings": 0.0
        }
        
        # Spot instance opportunities
        node_pools = cluster_data.get("node_pools", [])
        for pool in node_pools:
            if pool.get("scale_set_priority") != "Spot":
                # Calculate potential spot savings (typically 60-90% discount)
                current_cost = self._estimate_vm_cost(pool.get("vm_size", ""), pool.get("count", 0))
                spot_savings = current_cost * 0.7  # 70% savings estimate
                
                optimization["spot_instance_opportunities"].append({
                    "node_pool": pool.get("name", ""),
                    "current_cost": current_cost,
                    "potential_savings": spot_savings,
                    "recommendation": f"Convert to spot instances for {pool.get('name', '')} node pool",
                    "risk_level": "medium",
                    "suitability_score": self._calculate_spot_suitability(pool)
                })
        
        # Reserved instance opportunities (for stable workloads)
        for pool in node_pools:
            if pool.get("auto_scaling_enabled", False) and pool.get("min_count", 0) > 0:
                # Reserved instances for minimum capacity
                min_cost = self._estimate_vm_cost(pool.get("vm_size", ""), pool.get("min_count", 0))
                reserved_savings = min_cost * 0.3  # 30% savings for 1-year reserved
                
                optimization["reserved_instance_opportunities"].append({
                    "node_pool": pool.get("name", ""),
                    "minimum_nodes": pool.get("min_count", 0),
                    "current_cost": min_cost,
                    "potential_savings": reserved_savings,
                    "recommendation": f"Use reserved instances for base capacity in {pool.get('name', '')}",
                    "commitment_period": "1-year"
                })
        
        # Storage optimization
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        pvs = k8s_resources.get("persistent_volumes", [])
        
        for pv in pvs:
            # Check for premium storage that could be standard
            storage_class = pv.get("storage_class", "")
            if "premium" in storage_class.lower():
                capacity = self._parse_storage_capacity(pv.get("capacity", "0"))
                if capacity > 0:
                    # Estimate 50% savings by moving to standard storage
                    premium_cost = capacity * 0.15  # $0.15/GB/month for premium
                    standard_cost = capacity * 0.05  # $0.05/GB/month for standard
                    savings = premium_cost - standard_cost
                    
                    optimization["storage_optimization"].append({
                        "pv_name": pv.get("name", ""),
                        "current_storage_class": storage_class,
                        "capacity_gb": capacity,
                        "current_cost": premium_cost,
                        "optimized_cost": standard_cost,
                        "potential_savings": savings,
                        "recommendation": "Consider moving to standard storage if IOPS requirements allow"
                    })
        
        # Calculate total savings
        optimization["total_savings"] = (
            sum(opp["potential_savings"] for opp in optimization["spot_instance_opportunities"]) +
            sum(opp["potential_savings"] for opp in optimization["reserved_instance_opportunities"]) +
            sum(opp["potential_savings"] for opp in optimization["storage_optimization"])
        )
        
        return optimization
    
    async def _detect_cost_anomalies(self, detailed_costs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect cost anomalies and unusual patterns"""
        
        anomalies = []
        
        # Daily cost anomalies
        raw_data = detailed_costs.get("raw_data", {})
        daily_breakdown = raw_data.get("daily_breakdown", [])
        
        if len(daily_breakdown) >= 7:
            costs = [day.get("cost", 0.0) for day in daily_breakdown]
            mean_cost = statistics.mean(costs)
            std_cost = statistics.stdev(costs) if len(costs) > 1 else 0
            
            for i, day in enumerate(daily_breakdown):
                daily_cost = day.get("cost", 0.0)
                if std_cost > 0:
                    z_score = abs((daily_cost - mean_cost) / std_cost)
                    
                    # Anomaly if cost is more than 2 standard deviations away
                    if z_score > 2:
                        anomalies.append({
                            "type": "daily_cost_anomaly",
                            "date": day.get("date", ""),
                            "actual_cost": daily_cost,
                            "expected_cost": mean_cost,
                            "deviation_percentage": ((daily_cost - mean_cost) / mean_cost) * 100,
                            "severity": "high" if z_score > 3 else "medium",
                            "description": f"Cost spike detected on {day.get('date', '')}: ${daily_cost:.2f} vs expected ${mean_cost:.2f}"
                        })
        
        # Resource cost anomalies
        resources = detailed_costs.get("resources", {})
        if resources:
            resource_costs = [res.get("total_cost", 0.0) for res in resources.values()]
            if resource_costs:
                mean_resource_cost = statistics.mean(resource_costs)
                
                for resource_id, resource_data in resources.items():
                    cost = resource_data.get("total_cost", 0.0)
                    if cost > mean_resource_cost * 3:  # More than 3x average
                        anomalies.append({
                            "type": "high_cost_resource",
                            "resource_id": resource_id,
                            "resource_type": resource_data.get("resource_type", "unknown"),
                            "cost": cost,
                            "average_cost": mean_resource_cost,
                            "cost_multiple": cost / max(mean_resource_cost, 1),
                            "severity": "high",
                            "description": f"Resource {resource_id} has unusually high cost: ${cost:.2f}"
                        })
        
        return anomalies
    
    async def _generate_cost_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate cost optimization recommendations"""
        
        recommendations = []
        
        # Based on optimization opportunities
        optimization = analysis.get("cost_optimization", {})
        
        spot_opps = optimization.get("spot_instance_opportunities", [])
        if spot_opps:
            total_spot_savings = sum(opp["potential_savings"] for opp in spot_opps)
            recommendations.append(
                f"Consider spot instances for suitable workloads to save up to ${total_spot_savings:.2f}/month"
            )
        
        reserved_opps = optimization.get("reserved_instance_opportunities", [])
        if reserved_opps:
            total_reserved_savings = sum(opp["potential_savings"] for opp in reserved_opps)
            recommendations.append(
                f"Use reserved instances for base capacity to save ${total_reserved_savings:.2f}/month"
            )
        
        storage_opps = optimization.get("storage_optimization", [])
        if storage_opps:
            total_storage_savings = sum(opp["potential_savings"] for opp in storage_opps)
            recommendations.append(
                f"Optimize storage classes to save ${total_storage_savings:.2f}/month"
            )
        
        # Based on cost trends
        trends = analysis.get("cost_trends", {})
        if trends.get("trend_direction") == "increasing":
            recommendations.append(
                "Cost trend is increasing - implement proactive cost controls and monitoring"
            )
        
        if trends.get("volatility", 0) > 20:
            recommendations.append(
                "High cost volatility detected - investigate workload patterns and implement predictable scaling"
            )
        
        # Based on anomalies
        anomalies = analysis.get("anomalies", [])
        high_anomalies = [a for a in anomalies if a.get("severity") == "high"]
        if high_anomalies:
            recommendations.append(
                f"Investigate {len(high_anomalies)} high-severity cost anomalies"
            )
        
        return recommendations
    
    def _calculate_total_monthly_cost(self, detailed_costs: Dict[str, Any]) -> float:
        """Calculate total monthly cost"""
        return detailed_costs.get("summary", {}).get("total_cost", 0.0)
    
    def _calculate_cost_efficiency_score(self, analysis: Dict[str, Any]) -> float:
        """Calculate cost efficiency score (0-100)"""
        
        score = 100.0
        
        # Penalize for high waste
        optimization = analysis.get("cost_optimization", {})
        total_savings = optimization.get("total_savings", 0.0)
        total_cost = analysis.get("total_monthly_cost", 1.0)
        
        waste_percentage = (total_savings / total_cost) * 100 if total_cost > 0 else 0
        score -= min(50, waste_percentage)  # Max 50 point penalty for waste
        
        # Penalize for anomalies
        anomalies = analysis.get("anomalies", [])
        high_anomalies = len([a for a in anomalies if a.get("severity") == "high"])
        score -= high_anomalies * 5  # 5 points per high anomaly
        
        # Penalize for high volatility
        trends = analysis.get("cost_trends", {})
        volatility = trends.get("volatility", 0)
        if volatility > 15:
            score -= min(20, (volatility - 15) * 2)  # Penalty for high volatility
        
        return max(0, min(100, score))
    
    # Helper methods
    def _estimate_vm_cost(self, vm_size: str, count: int) -> float:
        """Estimate VM cost based on size and count"""
        
        # Simplified cost estimates (USD/month per VM)
        vm_costs = {
            "Standard_D2s_v3": 70.0,
            "Standard_D4s_v3": 140.0,
            "Standard_D8s_v3": 280.0,
            "Standard_D16s_v3": 560.0,
            "Standard_B2s": 30.0,
            "Standard_B4ms": 120.0,
            "Standard_F4s_v2": 150.0,
            "Standard_F8s_v2": 300.0,
        }
        
        base_cost = vm_costs.get(vm_size, 100.0)  # Default $100/month
        return base_cost * count
    
    def _parse_vm_size(self, vm_size: str) -> Tuple[int, int]:
        """Parse VM size to get CPU cores and memory GB"""
        
        vm_specs = {
            "Standard_D2s_v3": (2, 8),
            "Standard_D4s_v3": (4, 16),
            "Standard_D8s_v3": (8, 32),
            "Standard_D16s_v3": (16, 64),
            "Standard_B2s": (2, 4),
            "Standard_B4ms": (4, 16),
            "Standard_F4s_v2": (4, 8),
            "Standard_F8s_v2": (8, 16),
        }
        
        return vm_specs.get(vm_size, (2, 8))  # Default 2 cores, 8GB
    
    def _parse_storage_capacity(self, capacity_str: str) -> float:
        """Parse storage capacity string to GB"""
        
        if not capacity_str:
            return 0.0
        
        # Remove units and convert to GB
        capacity_str = capacity_str.upper().replace("I", "")
        
        if capacity_str.endswith("GB") or capacity_str.endswith("G"):
            return float(capacity_str.replace("GB", "").replace("G", ""))
        elif capacity_str.endswith("TB") or capacity_str.endswith("T"):
            return float(capacity_str.replace("TB", "").replace("T", "")) * 1024
        else:
            # Assume GB
            try:
                return float(capacity_str)
            except:
                return 0.0
    
    def _estimate_namespace_cost(self, namespace: Dict[str, Any], 
                                node_pool_costs: Dict[str, Any]) -> float:
        """Estimate namespace cost based on resource usage"""
        
        # Simplified estimation based on pod count
        pod_count = len(namespace.get("pods", []))
        total_cluster_cost = sum(pool["cost"] for pool in node_pool_costs.values())
        
        # Very rough estimation - divide total cost by total pods
        # In practice, this would use actual resource requests/limits
        return total_cluster_cost * 0.1 * pod_count  # 10% per pod assumption
    
    def _estimate_workload_cost(self, workload: Dict[str, Any], 
                               namespace_costs: Dict[str, Any]) -> float:
        """Estimate workload cost"""
        
        namespace = workload.get("namespace", "unknown")
        replicas = workload.get("replicas", 1)
        
        # Estimate based on namespace cost and replica proportion
        namespace_cost = namespace_costs.get(namespace, {}).get("cost", 0.0)
        return namespace_cost * 0.2 * replicas  # 20% per replica assumption
    
    def _calculate_spot_suitability(self, node_pool: Dict[str, Any]) -> float:
        """Calculate suitability score for spot instances (0-100)"""
        
        score = 50.0  # Base score
        
        # Higher score for auto-scaling enabled pools
        if node_pool.get("auto_scaling_enabled", False):
            score += 30.0
        
        # Higher score for user pools vs system pools
        if node_pool.get("mode", "") == "User":
            score += 20.0
        
        # Lower score for single-node pools
        if node_pool.get("count", 0) == 1:
            score -= 40.0
        
        return max(0, min(100, score))