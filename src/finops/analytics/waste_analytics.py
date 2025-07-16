# src/finops/analytics/waste_analytics.py
"""
Updated Phase 2 Waste Analytics - Enhanced with node_resource_group_costs
Focus on identifying and quantifying waste using actual cost data
"""
import traceback
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import structlog
import math

logger = structlog.get_logger(__name__)


class WasteAnalytics:
    """
    Enhanced Waste Analytics for Phase 2
    Identifies and quantifies waste using actual cost data from node_resource_group_costs
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(analytics="waste")
        
        # Waste detection thresholds
        self.thresholds = {
            'cpu_waste_threshold': config.get('cpu_waste_threshold', 10.0),  # % utilization
            'memory_waste_threshold': config.get('memory_waste_threshold', 10.0),  # % utilization
            'idle_threshold': config.get('idle_threshold', 5.0),  # % utilization
            'overprovisioning_threshold': config.get('overprovisioning_threshold', 200.0),  # % over request
            'zombie_threshold_days': config.get('zombie_threshold_days', 7),  # days
            'cost_waste_threshold': config.get('cost_waste_threshold', 100.0),  # USD per month
        }
    
    async def analyze_cluster_waste(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced waste analysis using actual cost data
        """
        cluster_name = cluster_data.get("cluster_info", {}).get("name", "unknown")
        
        self.logger.info(f"Starting enhanced waste analysis for {cluster_name}")
        
        analysis = {
            "cluster_name": cluster_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "resource_waste": {},
            "cost_waste": {},
            "zombie_resources": {},
            "overprovisioning_analysis": {},
            "idle_resources": {},
            "storage_waste": {},
            "network_waste": {},
            "total_waste_cost": 0.0,
            "waste_percentage": 0.0,
            "recommendations": []
        }
        
        try:
            # Get cost and metrics data
            node_rg_costs = cluster_data.get("node_resource_group_costs", {})
            raw_metrics = cluster_data.get("raw_metrics", {})
            
            if node_rg_costs and "error" not in node_rg_costs:
                # 1. Resource waste analysis with cost correlation
                analysis["resource_waste"] = await self._analyze_resource_waste_with_costs(
                    cluster_data, raw_metrics, node_rg_costs
                )
                
                # 2. Cost-based waste analysis
                analysis["cost_waste"] = await self._analyze_cost_waste(
                    cluster_data, node_rg_costs
                )
                
                # 3. Zombie resource detection
                analysis["zombie_resources"] = await self._detect_zombie_resources(
                    cluster_data, node_rg_costs
                )
                
                # 4. Overprovisioning analysis
                analysis["overprovisioning_analysis"] = await self._analyze_overprovisioning(
                    cluster_data, raw_metrics, node_rg_costs
                )
                
                # 5. Idle resource analysis
                analysis["idle_resources"] = await self._analyze_idle_resources(
                    cluster_data, raw_metrics, node_rg_costs
                )
                
                # 6. Storage waste analysis
                analysis["storage_waste"] = await self._analyze_storage_waste(
                    cluster_data, node_rg_costs
                )
                
                # 7. Network waste analysis
                analysis["network_waste"] = await self._analyze_network_waste(
                    cluster_data, node_rg_costs
                )
                
                # 8. Calculate total waste metrics
                analysis["total_waste_cost"] = self._calculate_total_waste_cost(analysis)
                analysis["waste_percentage"] = self._calculate_waste_percentage(
                    analysis, node_rg_costs
                )
                
                # 9. Generate waste reduction recommendations
                analysis["recommendations"] = await self._generate_waste_recommendations(
                    analysis, node_rg_costs
                )
                
                self.logger.info(
                    f"Enhanced waste analysis completed for {cluster_name}",
                    total_waste_cost=analysis["total_waste_cost"],
                    waste_percentage=analysis["waste_percentage"]
                )
            else:
                self.logger.warning(f"No valid cost data for waste analysis in {cluster_name}")
                analysis["error"] = "Missing or invalid cost data"
                
        except Exception as e:
            
            self.logger.error(f"Enhanced waste analysis failed for {cluster_name}: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    async def _analyze_resource_waste_with_costs(self, cluster_data: Dict[str, Any], 
                                               raw_metrics: Dict[str, Any], 
                                               node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze resource waste with cost correlation"""
        
        resource_waste = {
            "cpu_waste": {"percentage": 0.0, "cost": 0.0, "wasted_cores": 0.0},
            "memory_waste": {"percentage": 0.0, "cost": 0.0, "wasted_gb": 0.0},
            "node_waste": {"underutilized_nodes": [], "total_waste_cost": 0.0},
            "resource_efficiency": 0.0
        }
        
        # Get cluster utilization data
        container_insights = raw_metrics.get("container_insights_data", {})
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        # Calculate VM costs for resource correlation
        by_service = node_rg_costs.get("by_service", {})
        vm_cost = by_service.get("Virtual Machines", {}).get("cost", total_cost * 0.7)  # Assume 70% is VM cost
        
        # CPU waste analysis
        if "cluster_cpu_utilization" in container_insights:
            cpu_data = container_insights["cluster_cpu_utilization"]
            data_points = cpu_data.get("data_points", [])
            
            if data_points:
                cpu_values = [point.get("cpu_utilization_percentage", 0) for point in data_points]
                avg_cpu_utilization = sum(cpu_values) / len(cpu_values)
                
                if avg_cpu_utilization < self.thresholds['cpu_waste_threshold']:
                    cpu_waste_percentage = 100 - avg_cpu_utilization
                    cpu_waste_cost = vm_cost * (cpu_waste_percentage / 100)
                    
                    # Calculate wasted cores
                    node_pools = cluster_data.get("node_pools", [])
                    total_cores = 0
                    for pool in node_pools:
                        vm_size = pool.get("vm_size", "")
                        cores, _ = self._parse_vm_size(vm_size)
                        total_cores += cores * pool.get("count", 0)
                    
                    wasted_cores = total_cores * (cpu_waste_percentage / 100)
                    
                    resource_waste["cpu_waste"] = {
                        "percentage": cpu_waste_percentage,
                        "cost": cpu_waste_cost,
                        "wasted_cores": wasted_cores,
                        "utilization": avg_cpu_utilization
                    }
        
        # Memory waste analysis
        if "cluster_memory_utilization" in container_insights:
            memory_data = container_insights["cluster_memory_utilization"]
            data_points = memory_data.get("data_points", [])
            
            if data_points:
                memory_values = [point.get("memory_utilization_percentage", 0) for point in data_points]
                avg_memory_utilization = sum(memory_values) / len(memory_values)
                
                if avg_memory_utilization < self.thresholds['memory_waste_threshold']:
                    memory_waste_percentage = 100 - avg_memory_utilization
                    memory_waste_cost = vm_cost * (memory_waste_percentage / 100)
                    
                    # Calculate wasted memory
                    node_pools = cluster_data.get("node_pools", [])
                    total_memory = 0
                    for pool in node_pools:
                        vm_size = pool.get("vm_size", "")
                        _, memory_gb = self._parse_vm_size(vm_size)
                        total_memory += memory_gb * pool.get("count", 0)
                    
                    wasted_memory = total_memory * (memory_waste_percentage / 100)
                    
                    resource_waste["memory_waste"] = {
                        "percentage": memory_waste_percentage,
                        "cost": memory_waste_cost,
                        "wasted_gb": wasted_memory,
                        "utilization": avg_memory_utilization
                    }
        
        # Node-level waste analysis
        if "node_metrics" in raw_metrics:
            node_metrics = raw_metrics["node_metrics"]
            node_pools = cluster_data.get("node_pools", [])
            
            for pool in node_pools:
                pool_name = pool.get("name", "")
                node_count = pool.get("count", 0)
                vm_size = pool.get("vm_size", "")
                
                # Estimate pool cost
                pool_cost = vm_cost * (node_count / max(sum(p.get("count", 0) for p in node_pools), 1))
                
                # Check if pool is underutilized (this would need more detailed node metrics)
                # For now, we'll use cluster-level metrics as proxy
                cluster_cpu_avg = resource_waste["cpu_waste"].get("utilization", 50)
                cluster_memory_avg = resource_waste["memory_waste"].get("utilization", 50)
                
                if cluster_cpu_avg < 30 and cluster_memory_avg < 30:  # Both CPU and memory underutilized
                    resource_waste["node_waste"]["underutilized_nodes"].append({
                        "pool_name": pool_name,
                        "node_count": node_count,
                        "vm_size": vm_size,
                        "estimated_waste_cost": pool_cost * 0.5,  # Assume 50% waste
                        "recommended_action": "Scale down or right-size"
                    })
        
        # Calculate total node waste cost
        resource_waste["node_waste"]["total_waste_cost"] = sum(
            node["estimated_waste_cost"] for node in resource_waste["node_waste"]["underutilized_nodes"]
        )
        
        # Calculate overall resource efficiency
        cpu_efficiency = resource_waste["cpu_waste"].get("utilization", 100)
        memory_efficiency = resource_waste["memory_waste"].get("utilization", 100)
        resource_waste["resource_efficiency"] = (cpu_efficiency + memory_efficiency) / 2
        
        return resource_waste
    
    async def _analyze_cost_waste(self, cluster_data: Dict[str, Any], 
                                node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cost waste patterns"""
        
        cost_waste = {
            "service_waste": {},
            "daily_cost_spikes": [],
            "unused_resources": [],
            "optimization_opportunities": [],
            "total_cost_waste": 0.0
        }
        
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        # Service-level waste analysis
        by_service = node_rg_costs.get("by_service", {})
        
        for service_name, service_data in by_service.items():
            service_cost = service_data.get("cost", 0.0)
            service_percentage = (service_cost / max(total_cost, 1)) * 100
            
            # Identify services with disproportionate costs
            if service_percentage > 10:  # Services consuming >10% of total cost
                waste_indicators = []
                
                # Check for specific waste patterns
                if service_name == "Storage":
                    waste_indicators.append("Potential unused storage volumes")
                elif service_name == "Load Balancer":
                    waste_indicators.append("Potential unused load balancers")
                elif service_name == "Public IP":
                    waste_indicators.append("Potential unused public IPs")
                
                if waste_indicators:
                    cost_waste["service_waste"][service_name] = {
                        "cost": service_cost,
                        "percentage": service_percentage,
                        "waste_indicators": waste_indicators,
                        "potential_savings": service_cost * 0.2  # Assume 20% waste
                    }
        
        # Daily cost spike analysis
        daily_breakdown = node_rg_costs.get("daily_breakdown", [])
        if len(daily_breakdown) > 7:
            costs = [day.get("cost", 0.0) for day in daily_breakdown]
            mean_cost = sum(costs) / len(costs)
            
            for day in daily_breakdown:
                daily_cost = day.get("cost", 0.0)
                if daily_cost > mean_cost * 1.3:  # 30% above average
                    cost_waste["daily_cost_spikes"].append({
                        "date": day.get("date", ""),
                        "cost": daily_cost,
                        "excess_cost": daily_cost - mean_cost,
                        "percentage_above_average": ((daily_cost - mean_cost) / mean_cost) * 100
                    })
        
        # Resource ID level waste
        by_resource_id = node_rg_costs.get("by_resource_id", {})
        
        for resource_id, resource_data in by_resource_id.items():
            resource_cost = resource_data.get("cost", 0.0)
            resource_type = resource_data.get("resource_type", "")
            
            # Flag resources with high cost but potentially low utilization
            if resource_cost > total_cost * 0.05:  # Resource consuming >5% of total cost
                cost_waste["unused_resources"].append({
                    "resource_id": resource_id,
                    "resource_type": resource_type,
                    "cost": resource_cost,
                    "percentage": (resource_cost / total_cost) * 100,
                    "investigation_needed": True
                })
        
        # Calculate total cost waste
        service_waste_total = sum(
            waste_data["potential_savings"] for waste_data in cost_waste["service_waste"].values()
        )
        spike_waste_total = sum(
            spike["excess_cost"] for spike in cost_waste["daily_cost_spikes"]
        )
        
        cost_waste["total_cost_waste"] = service_waste_total + spike_waste_total
        
        return cost_waste
    
    async def _detect_zombie_resources(self, cluster_data: Dict[str, Any], 
                                     node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Detect zombie resources (resources that exist but are unused)"""
        
        zombie_resources = {
            "zombie_pods": [],
            "zombie_volumes": [],
            "zombie_services": [],
            "zombie_cost_impact": 0.0,
            "cleanup_recommendations": []
        }
        
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        # Zombie pod detection
        pods = k8s_resources.get("pods", [])
        
        for pod in pods:
            if not isinstance(pod, dict):
                logger.warning(f"Skipping invalid pod object (not a dict): {pod}")
                continue

            pod_status = pod.get("status", {})

            

            
            # Check for failed or completed pods that are still consuming resources
            if pod_status in ["Failed", "Succeeded"]:
                
                # Estimate cost impact based on resource requests
                resource_requests = pod.get("resource_requests", {})
                cpu_request = resource_requests.get("cpu", 0)
                memory_request = resource_requests.get("memory", 0)
                
                # Rough cost estimation
                estimated_cost = (cpu_request * 0.05 + memory_request * 0.01) * 24 * 30  # Monthly cost
                
                zombie_resources["zombie_pods"].append({
                    "name": pod.get("name", "unknown"),
                    "namespace": pod.get("namespace", "unknown"),
                    "phase": phase,
                    "estimated_monthly_cost": estimated_cost,
                    "age_days": self._calculate_resource_age(pod.get("created", "")),
                    "recommendation": "Delete completed/failed pod"
                })
        
        # Zombie volume detection
        volumes = k8s_resources.get("persistent_volumes", [])
        for volume in volumes:
            volume_status = volume.get("status", {})
            phase = volume_status.get("phase", "")
            
            # Check for available volumes that are not bound
            if phase == "Available":
                # Estimate storage cost
                capacity = volume.get("capacity", {}).get("storage", "0Gi")
                size_gb = self._parse_storage_size(capacity)
                estimated_cost = size_gb * 0.1 * 30  # $0.1 per GB per month
                
                zombie_resources["zombie_volumes"].append({
                    "name": volume.get("name", "unknown"),
                    "capacity": capacity,
                    "storage_class": volume.get("storage_class", "unknown"),
                    "estimated_monthly_cost": estimated_cost,
                    "age_days": self._calculate_resource_age(volume.get("created", "")),
                    "recommendation": "Delete unbound persistent volume"
                })
        
        # Zombie service detection (services without endpoints)
        services = k8s_resources.get("services", [])
        for service in services:
            # Check if service has no endpoints (no backing pods)
            if not service.get("endpoints", []):
                service_type = service.get("type", "ClusterIP")
                
                # Estimate cost based on service type
                estimated_cost = 0.0
                if service_type == "LoadBalancer":
                    estimated_cost = 20.0  # $20/month for load balancer
                elif service_type == "NodePort":
                    estimated_cost = 5.0   # $5/month for nodeport overhead
                
                if estimated_cost > 0:
                    zombie_resources["zombie_services"].append({
                        "name": service.get("name", "unknown"),
                        "namespace": service.get("namespace", "unknown"),
                        "type": service_type,
                        "estimated_monthly_cost": estimated_cost,
                        "age_days": self._calculate_resource_age(service.get("created", "")),
                        "recommendation": "Delete service without endpoints"
                    })
        
        # Calculate total zombie cost impact
        zombie_resources["zombie_cost_impact"] = (
            sum(pod["estimated_monthly_cost"] for pod in zombie_resources["zombie_pods"]) +
            sum(vol["estimated_monthly_cost"] for vol in zombie_resources["zombie_volumes"]) +
            sum(svc["estimated_monthly_cost"] for svc in zombie_resources["zombie_services"])
        )
        
        # Generate cleanup recommendations
        if zombie_resources["zombie_pods"]:
            zombie_resources["cleanup_recommendations"].append(
                f"Clean up {len(zombie_resources['zombie_pods'])} zombie pods - potential savings: ${sum(pod['estimated_monthly_cost'] for pod in zombie_resources['zombie_pods']):.2f}/month"
            )
        
        if zombie_resources["zombie_volumes"]:
            zombie_resources["cleanup_recommendations"].append(
                f"Clean up {len(zombie_resources['zombie_volumes'])} unbound volumes - potential savings: ${sum(vol['estimated_monthly_cost'] for vol in zombie_resources['zombie_volumes']):.2f}/month"
            )
        
        if zombie_resources["zombie_services"]:
            zombie_resources["cleanup_recommendations"].append(
                f"Clean up {len(zombie_resources['zombie_services'])} unused services - potential savings: ${sum(svc['estimated_monthly_cost'] for svc in zombie_resources['zombie_services']):.2f}/month"
            )
        
        return zombie_resources
    
    async def _analyze_overprovisioning(self, cluster_data: Dict[str, Any], 
                                      raw_metrics: Dict[str, Any], 
                                      node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze overprovisioning waste"""
        
        overprovisioning = {
            "cpu_overprovisioning": {"percentage": 0.0, "cost": 0.0},
            "memory_overprovisioning": {"percentage": 0.0, "cost": 0.0},
            "pod_overprovisioning": [],
            "node_pool_overprovisioning": [],
            "total_overprovisioning_cost": 0.0
        }
        
        # Get utilization data
        container_insights = raw_metrics.get("container_insights_data", {})
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        # Calculate overprovisioning at cluster level
        if "cluster_cpu_utilization" in container_insights:
            cpu_data = container_insights["cluster_cpu_utilization"]
            data_points = cpu_data.get("data_points", [])
            
            if data_points:
                cpu_values = [point.get("cpu_utilization_percentage", 0) for point in data_points]
                avg_cpu_utilization = sum(cpu_values) / len(cpu_values)
                
                if avg_cpu_utilization < 50:  # Less than 50% utilization indicates overprovisioning
                    overprovisioning_percentage = 100 - avg_cpu_utilization
                    overprovisioning_cost = total_cost * (overprovisioning_percentage / 200)  # Conservative estimate
                    
                    overprovisioning["cpu_overprovisioning"] = {
                        "percentage": overprovisioning_percentage,
                        "cost": overprovisioning_cost,
                        "current_utilization": avg_cpu_utilization,
                        "target_utilization": 70.0
                    }
        
        # Memory overprovisioning analysis
        if "cluster_memory_utilization" in container_insights:
            memory_data = container_insights["cluster_memory_utilization"]
            data_points = memory_data.get("data_points", [])
            
            if data_points:
                memory_values = [point.get("memory_utilization_percentage", 0) for point in data_points]
                avg_memory_utilization = sum(memory_values) / len(memory_values)
                
                if avg_memory_utilization < 50:  # Less than 50% utilization indicates overprovisioning
                    overprovisioning_percentage = 100 - avg_memory_utilization
                    overprovisioning_cost = total_cost * (overprovisioning_percentage / 200)  # Conservative estimate
                    
                    overprovisioning["memory_overprovisioning"] = {
                        "percentage": overprovisioning_percentage,
                        "cost": overprovisioning_cost,
                        "current_utilization": avg_memory_utilization,
                        "target_utilization": 70.0
                    }
        
        # Pod-level overprovisioning (would need more detailed metrics)
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        pods = k8s_resources.get("pods", [])
        
        for pod in pods:
            resource_requests = pod.get("resource_requests", {})
            resource_limits = pod.get("resource_limits", {})
            
            # Check if limits are significantly higher than requests
            cpu_request = resource_requests.get("cpu", 0)
            cpu_limit = resource_limits.get("cpu", 0)
            memory_request = resource_requests.get("memory", 0)
            memory_limit = resource_limits.get("memory", 0)
            
            if cpu_limit > cpu_request * 2:  # Limit is 2x request
                overprovisioning["pod_overprovisioning"].append({
                    "pod_name": pod.get("name", "unknown"),
                    "namespace": pod.get("namespace", "unknown"),
                    "cpu_request": cpu_request,
                    "cpu_limit": cpu_limit,
                    "overprovisioning_ratio": cpu_limit / max(cpu_request, 1),
                    "resource_type": "cpu",
                    "recommendation": "Reduce CPU limits closer to requests"
                })
            
            if memory_limit > memory_request * 2:  # Limit is 2x request
                overprovisioning["pod_overprovisioning"].append({
                    "pod_name": pod.get("name", "unknown"),
                    "namespace": pod.get("namespace", "unknown"),
                    "memory_request": memory_request,
                    "memory_limit": memory_limit,
                    "overprovisioning_ratio": memory_limit / max(memory_request, 1),
                    "resource_type": "memory",
                    "recommendation": "Reduce memory limits closer to requests"
                })
        
        # Node pool overprovisioning
        node_pools = cluster_data.get("node_pools", [])
        for pool in node_pools:
            # Check if node pool has too many nodes for current workload
            node_count = pool.get("count", 0)
            max_count = pool.get("max_count", node_count)
            
            # If current count is close to max but utilization is low, it's overprovisioned
            if node_count >= max_count * 0.8:  # 80% of max capacity
                cpu_util = overprovisioning["cpu_overprovisioning"].get("current_utilization", 50)
                memory_util = overprovisioning["memory_overprovisioning"].get("current_utilization", 50)
                
                if cpu_util < 40 and memory_util < 40:
                    # Estimate cost of overprovisioned nodes
                    vm_cost = self._estimate_vm_cost(pool.get("vm_size", ""), 1)
                    overprovisioned_nodes = max(1, int(node_count * 0.3))  # Assume 30% overprovisioning
                    overprovisioning_cost = vm_cost * overprovisioned_nodes
                    
                    overprovisioning["node_pool_overprovisioning"].append({
                        "pool_name": pool.get("name", "unknown"),
                        "current_nodes": node_count,
                        "recommended_nodes": node_count - overprovisioned_nodes,
                        "overprovisioned_nodes": overprovisioned_nodes,
                        "monthly_waste_cost": overprovisioning_cost,
                        "vm_size": pool.get("vm_size", ""),
                        "recommendation": f"Scale down by {overprovisioned_nodes} nodes"
                    })
        
        # Calculate total overprovisioning cost
        overprovisioning["total_overprovisioning_cost"] = (
            overprovisioning["cpu_overprovisioning"].get("cost", 0.0) +
            overprovisioning["memory_overprovisioning"].get("cost", 0.0) +
            sum(pool["monthly_waste_cost"] for pool in overprovisioning["node_pool_overprovisioning"])
        )
        
        return overprovisioning
    
    async def _analyze_idle_resources(self, cluster_data: Dict[str, Any], 
                                    raw_metrics: Dict[str, Any], 
                                    node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze idle resources"""
        
        idle_resources = {
            "idle_nodes": [],
            "idle_volumes": [],
            "idle_services": [],
            "idle_cost_impact": 0.0,
            "recommendations": []
        }
        
        total_cost = node_rg_costs.get("total_cost", 0.0)
        
        # Idle node detection (based on very low utilization)
        container_insights = raw_metrics.get("container_insights_data", {})
        
        if "cluster_cpu_utilization" in container_insights:
            cpu_data = container_insights["cluster_cpu_utilization"]
            data_points = cpu_data.get("data_points", [])
            
            if data_points:
                cpu_values = [point.get("cpu_utilization_percentage", 0) for point in data_points]
                avg_cpu_utilization = sum(cpu_values) / len(cpu_values)
                
                if avg_cpu_utilization < self.thresholds['idle_threshold']:
                    # Estimate idle cost
                    idle_cost = total_cost * (self.thresholds['idle_threshold'] - avg_cpu_utilization) / 100
                    
                    idle_resources["idle_nodes"].append({
                        "issue": "Cluster CPU utilization below idle threshold",
                        "current_utilization": avg_cpu_utilization,
                        "threshold": self.thresholds['idle_threshold'],
                        "estimated_idle_cost": idle_cost,
                        "recommendation": "Consider scaling down or consolidating workloads"
                    })
        
        # Idle volume detection
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        volumes = k8s_resources.get("persistent_volumes", [])
        
        for volume in volumes:
            volume_status = volume.get("status", {})
            phase = volume_status.get("phase", "")
            
            # Volumes that are bound but not actively used
            if phase == "Bound":
                # Check if volume has been idle (would need more detailed metrics)
                age_days = self._calculate_resource_age(volume.get("created", ""))
                
                if age_days > 30:  # Volume older than 30 days, potentially idle
                    capacity = volume.get("capacity", {}).get("storage", "0Gi")
                    size_gb = self._parse_storage_size(capacity)
                    estimated_cost = size_gb * 0.1 * 30  # $0.1 per GB per month
                    
                    idle_resources["idle_volumes"].append({
                        "name": volume.get("name", "unknown"),
                        "capacity": capacity,
                        "age_days": age_days,
                        "estimated_monthly_cost": estimated_cost,
                        "recommendation": "Investigate volume usage patterns"
                    })
        
        # Calculate total idle cost impact
        idle_resources["idle_cost_impact"] = (
            sum(node["estimated_idle_cost"] for node in idle_resources["idle_nodes"]) +
            sum(vol["estimated_monthly_cost"] for vol in idle_resources["idle_volumes"])
        )
        
        # Generate recommendations
        if idle_resources["idle_nodes"]:
            idle_resources["recommendations"].append(
                "Implement auto-scaling to handle idle node capacity"
            )
        
        if idle_resources["idle_volumes"]:
            idle_resources["recommendations"].append(
                f"Review {len(idle_resources['idle_volumes'])} potentially idle volumes"
            )
        
        return idle_resources
    
    async def _analyze_storage_waste(self, cluster_data: Dict[str, Any], 
                                   node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze storage waste"""
        
        storage_waste = {
            "unused_disks": [],
            "oversized_volumes": [],
            "premium_storage_opportunities": [],
            "storage_waste_cost": 0.0
        }
        
        # Get storage costs from Azure cost data
        by_service = node_rg_costs.get("by_service", {})
        storage_services = ["Storage", "Managed Disks", "Storage Accounts"]
        total_storage_cost = sum(by_service.get(service, {}).get("cost", 0.0) for service in storage_services)
        
        # Analyze storage by resource type
        by_resource_type = node_rg_costs.get("by_resource_type", {})
        
        for resource_type, cost_data in by_resource_type.items():
            if "disk" in resource_type.lower() or "storage" in resource_type.lower():
                resource_cost = cost_data.get("cost", 0.0)
                
                # Check if storage cost is disproportionate
                if resource_cost > total_storage_cost * 0.3:  # >30% of storage cost
                    storage_waste["premium_storage_opportunities"].append({
                        "resource_type": resource_type,
                        "current_cost": resource_cost,
                        "potential_savings": resource_cost * 0.3,  # 30% savings from optimization
                        "recommendation": f"Review {resource_type} configuration and consider Standard tier"
                    })
        
        # Kubernetes storage analysis
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        volumes = k8s_resources.get("persistent_volumes", [])
        
        for volume in volumes:
            capacity = volume.get("capacity", {}).get("storage", "0Gi")
            size_gb = self._parse_storage_size(capacity)
            storage_class = volume.get("storage_class", "")
            
            # Check for oversized volumes (>100GB)
            if size_gb > 100:
                estimated_cost = size_gb * 0.1 * 30  # $0.1 per GB per month
                
                storage_waste["oversized_volumes"].append({
                    "name": volume.get("name", "unknown"),
                    "capacity": capacity,
                    "size_gb": size_gb,
                    "storage_class": storage_class,
                    "estimated_monthly_cost": estimated_cost,
                    "recommendation": "Review if full capacity is needed"
                })
        
        # Calculate total storage waste
        storage_waste["storage_waste_cost"] = (
            sum(opp["potential_savings"] for opp in storage_waste["premium_storage_opportunities"]) +
            sum(vol["estimated_monthly_cost"] * 0.2 for vol in storage_waste["oversized_volumes"])  # 20% waste assumption
        )
        
        return storage_waste
    
    async def _analyze_network_waste(self, cluster_data: Dict[str, Any], 
                                   node_rg_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze network waste"""
        
        network_waste = {
            "unused_load_balancers": [],
            "unused_public_ips": [],
            "network_waste_cost": 0.0
        }
        
        # Get network costs from Azure cost data
        by_service = node_rg_costs.get("by_service", {})
        network_services = ["Load Balancer", "Public IP", "Application Gateway", "Bandwidth"]
        
        for service_name in network_services:
            service_data = by_service.get(service_name, {})
            service_cost = service_data.get("cost", 0.0)
            
            if service_cost > 0:
                if service_name == "Load Balancer":
                    # Check if load balancer is actually needed
                    k8s_resources = cluster_data.get("kubernetes_resources", {})
                    services = k8s_resources.get("services", [])
                    lb_services = [svc for svc in services if svc.get("type") == "LoadBalancer"]
                    
                    if len(lb_services) < service_cost / 20:  # Less than 1 LB service per $20
                        network_waste["unused_load_balancers"].append({
                            "service": service_name,
                            "cost": service_cost,
                            "lb_services_count": len(lb_services),
                            "potential_savings": service_cost * 0.5,  # 50% savings assumption
                            "recommendation": "Review load balancer usage"
                        })
                
                elif service_name == "Public IP":
                    # Similar analysis for public IPs
                    network_waste["unused_public_ips"].append({
                        "service": service_name,
                        "cost": service_cost,
                        "potential_savings": service_cost * 0.3,  # 30% savings assumption
                        "recommendation": "Review public IP assignments"
                    })
        
        # Calculate total network waste
        network_waste["network_waste_cost"] = (
            sum(lb["potential_savings"] for lb in network_waste["unused_load_balancers"]) +
            sum(ip["potential_savings"] for ip in network_waste["unused_public_ips"])
        )
        
        return network_waste
    
    def _calculate_total_waste_cost(self, analysis: Dict[str, Any]) -> float:
        """Calculate total waste cost across all categories"""
        
        total_waste = 0.0
        
        # Resource waste
        resource_waste = analysis.get("resource_waste", {})
        total_waste += resource_waste.get("cpu_waste", {}).get("cost", 0.0)
        total_waste += resource_waste.get("memory_waste", {}).get("cost", 0.0)
        total_waste += resource_waste.get("node_waste", {}).get("total_waste_cost", 0.0)
        
        # Cost waste
        cost_waste = analysis.get("cost_waste", {})
        total_waste += cost_waste.get("total_cost_waste", 0.0)
        
        # Zombie resources
        zombie_resources = analysis.get("zombie_resources", {})
        total_waste += zombie_resources.get("zombie_cost_impact", 0.0)
        
        # Overprovisioning
        overprovisioning = analysis.get("overprovisioning_analysis", {})
        total_waste += overprovisioning.get("total_overprovisioning_cost", 0.0)
        
        # Idle resources
        idle_resources = analysis.get("idle_resources", {})
        total_waste += idle_resources.get("idle_cost_impact", 0.0)
        
        # Storage waste
        storage_waste = analysis.get("storage_waste", {})
        total_waste += storage_waste.get("storage_waste_cost", 0.0)
        
        # Network waste
        network_waste = analysis.get("network_waste", {})
        total_waste += network_waste.get("network_waste_cost", 0.0)
        
        return total_waste
    
    def _calculate_waste_percentage(self, analysis: Dict[str, Any], 
                                  node_rg_costs: Dict[str, Any]) -> float:
        """Calculate waste percentage of total cost"""
        
        total_cost = node_rg_costs.get("total_cost", 0.0)
        total_waste = analysis.get("total_waste_cost", 0.0)
        
        if total_cost > 0:
            return (total_waste / total_cost) * 100
        
        return 0.0
    
    async def _generate_waste_recommendations(self, analysis: Dict[str, Any], 
                                            node_rg_costs: Dict[str, Any]) -> List[str]:
        """Generate waste reduction recommendations"""
        
        recommendations = []
        waste_percentage = analysis.get("waste_percentage", 0.0)
        total_waste_cost = analysis.get("total_waste_cost", 0.0)
        
        # High-level recommendations
        if waste_percentage > 30:
            recommendations.append(
                f"CRITICAL: {waste_percentage:.1f}% waste detected (${total_waste_cost:.2f}/month) - immediate action required"
            )
        elif waste_percentage > 20:
            recommendations.append(
                f"HIGH: {waste_percentage:.1f}% waste detected (${total_waste_cost:.2f}/month) - optimization recommended"
            )
        elif waste_percentage > 10:
            recommendations.append(
                f"MEDIUM: {waste_percentage:.1f}% waste detected (${total_waste_cost:.2f}/month) - consider optimization"
            )
        
        # Specific recommendations based on analysis
        resource_waste = analysis.get("resource_waste", {})
        if resource_waste.get("cpu_waste", {}).get("cost", 0.0) > 100:
            recommendations.append(
                f"Optimize CPU utilization - current waste: ${resource_waste['cpu_waste']['cost']:.2f}/month"
            )
        
        if resource_waste.get("memory_waste", {}).get("cost", 0.0) > 100:
            recommendations.append(
                f"Optimize memory utilization - current waste: ${resource_waste['memory_waste']['cost']:.2f}/month"
            )
        
        # Zombie resource recommendations
        zombie_resources = analysis.get("zombie_resources", {})
        if zombie_resources.get("zombie_cost_impact", 0.0) > 50:
            recommendations.append(
                f"Clean up zombie resources - potential savings: ${zombie_resources['zombie_cost_impact']:.2f}/month"
            )
        
        # Overprovisioning recommendations
        overprovisioning = analysis.get("overprovisioning_analysis", {})
        if overprovisioning.get("total_overprovisioning_cost", 0.0) > 100:
            recommendations.append(
                f"Address overprovisioning - potential savings: ${overprovisioning['total_overprovisioning_cost']:.2f}/month"
            )
        
        # Storage optimization recommendations
        storage_waste = analysis.get("storage_waste", {})
        if storage_waste.get("storage_waste_cost", 0.0) > 50:
            recommendations.append(
                f"Optimize storage configuration - potential savings: ${storage_waste['storage_waste_cost']:.2f}/month"
            )
        
        # Network optimization recommendations
        network_waste = analysis.get("network_waste", {})
        if network_waste.get("network_waste_cost", 0.0) > 20:
            recommendations.append(
                f"Optimize network resources - potential savings: ${network_waste['network_waste_cost']:.2f}/month"
            )
        
        return recommendations
    
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
    
    def _estimate_vm_cost(self, vm_size: str, node_count: int) -> float:
        """Estimate VM cost based on size and count"""
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
        
        hourly_cost = cost_per_hour.get(vm_size, 0.096)
        return hourly_cost * node_count * 24 * 30
    
    def _calculate_resource_age(self, created_timestamp: str) -> int:
        """Calculate resource age in days"""
        if not created_timestamp:
            return 0
        
        try:
            created_date = datetime.fromisoformat(created_timestamp.replace('Z', '+00:00'))
            age = datetime.now() - created_date.replace(tzinfo=None)
            return age.days
        except:
            return 0
    
    def _parse_storage_size(self, capacity: str) -> float:
        """Parse storage capacity string to GB"""
        if not capacity:
            return 0.0
        
        try:
            if capacity.endswith('Gi'):
                return float(capacity[:-2])
            elif capacity.endswith('Mi'):
                return float(capacity[:-2]) / 1024
            elif capacity.endswith('Ti'):
                return float(capacity[:-2]) * 1024
            else:
                return float(capacity)
        except:
            return 0.0

    
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