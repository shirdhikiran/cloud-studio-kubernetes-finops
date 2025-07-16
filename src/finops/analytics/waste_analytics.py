# src/finops/analytics/waste_analytics.py
"""
Waste Analytics Module - Identify and quantify resource waste
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import structlog
import statistics
from collections import defaultdict
import math
import traceback

logger = structlog.get_logger(__name__)


class WasteAnalytics:
    """Advanced waste analytics for identifying and quantifying resource waste"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(module="waste_analytics")
        
        # Waste detection thresholds
        self.thresholds = {
            'idle_cpu_threshold': config.get('idle_cpu_threshold', 5.0),  # %
            'idle_memory_threshold': config.get('idle_memory_threshold', 10.0),  # %
            'low_utilization_threshold': config.get('low_utilization_threshold', 20.0),  # %
            'zombie_pod_age_hours': config.get('zombie_pod_age_hours', 72),  # hours
            'failed_pod_threshold': config.get('failed_pod_threshold', 5),  # count
            'overprovisioning_ratio': config.get('overprovisioning_ratio', 3.0),  # times
            'unused_storage_threshold': config.get('unused_storage_threshold', 30),  # days
        }
    
    async def analyze_cluster_waste(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive waste analysis for a cluster"""
        
        cluster_info = cluster_data.get("cluster_info", {})
        cluster_name = cluster_info.get("name", "unknown")
        
        self.logger.info(f"Starting waste analysis for cluster: {cluster_name}")
        
        analysis = {
            "cluster_name": cluster_name,
            "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
            "idle_resources": {},
            "zombie_workloads": {},
            "overprovisioned_resources": {},
            "failed_resources": {},
            "unused_storage": {},
            "network_waste": {},
            "cost_waste": {},
            "waste_summary": {},
            "recommendations": [],
            "total_waste_cost": 0.0,
            "waste_percentage": 0.0
        }
        
        try:
            # 1. Identify idle resources
            analysis["idle_resources"] = await self._identify_idle_resources(cluster_data)
            
            # 2. Detect zombie workloads
            analysis["zombie_workloads"] = await self._detect_zombie_workloads(cluster_data)
            
            # 3. Find overprovisioned resources
            analysis["overprovisioned_resources"] = await self._find_overprovisioned_resources(cluster_data)
            
            # 4. Identify failed/problematic resources
            analysis["failed_resources"] = await self._identify_failed_resources(cluster_data)
            
            # 5. Detect unused storage
            analysis["unused_storage"] = await self._detect_unused_storage(cluster_data)
            
            # 6. Analyze network waste
            analysis["network_waste"] = await self._analyze_network_waste(cluster_data)
            
            # 7. Quantify cost waste
            analysis["cost_waste"] = await self._quantify_cost_waste(cluster_data, analysis)
            
            # 8. Generate waste summary
            analysis["waste_summary"] = await self._generate_waste_summary(analysis)
            
            # 9. Generate recommendations
            analysis["recommendations"] = await self._generate_waste_recommendations(analysis)
            
            # 10. Calculate totals
            analysis["total_waste_cost"] = self._calculate_total_waste_cost(analysis)
            analysis["waste_percentage"] = self._calculate_waste_percentage(analysis, cluster_data)
            
            self.logger.info(
                f"Waste analysis completed for {cluster_name}",
                total_waste_cost=analysis["total_waste_cost"],
                waste_percentage=analysis["waste_percentage"]
            )
            
        except Exception as e:
            import pdb; pdb.set_trace()
            self.logger.error(f"Waste analysis failed for {cluster_name}: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    async def _identify_idle_resources(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Identify idle CPU, memory, and storage resources"""
        
        idle_resources = {
            "idle_nodes": [],
            "idle_pods": [],
            "idle_cpu_capacity": 0.0,
            "idle_memory_capacity": 0.0,
            "idle_cost_impact": 0.0
        }
        
        # Extract metrics from Container Insights
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # Analyze node-level idleness
        node_utilization = defaultdict(lambda: {"cpu": [], "memory": []})
        
        if "aks_node_cpu_performance" in container_insights:
            cpu_data = container_insights["aks_node_cpu_performance"]
            for point in cpu_data.get("data_points", []):
                node_name = point.get("NodeName", "unknown")
                cpu_usage = self.safe_float(point.get("cpu_usage_millicores", 0))
                cpu_percent = (cpu_usage / 2000) * 100  # Assuming 2-core baseline
                node_utilization[node_name]["cpu"].append(min(100, cpu_percent))
        
        if "aks_node_memory_performance" in container_insights:
            memory_data = container_insights["aks_node_memory_performance"]
            for point in memory_data.get("data_points", []):
                node_name = point.get("NodeName", "unknown")
                memory_percent = self.safe_float(point.get("memory_usage_percentage", 0))
                node_utilization[node_name]["memory"].append(min(100, memory_percent))
        
        # Identify idle nodes
        for node_name, utilization in node_utilization.items():
            cpu_usage = utilization["cpu"]
            memory_usage = utilization["memory"]
            
            if cpu_usage and memory_usage:
                avg_cpu = statistics.mean(cpu_usage)
                avg_memory = statistics.mean(memory_usage)
                
                # Node is idle if both CPU and memory are below threshold
                if (avg_cpu < self.thresholds["idle_cpu_threshold"] and 
                    avg_memory < self.thresholds["idle_memory_threshold"]):
                    
                    idle_resources["idle_nodes"].append({
                        "node_name": node_name,
                        "avg_cpu_utilization": avg_cpu,
                        "avg_memory_utilization": avg_memory,
                        "idle_duration_hours": len(cpu_usage) * 0.5,  # Assuming 30-min intervals
                        "estimated_cost_waste": self._estimate_node_cost_waste(node_name, avg_cpu, avg_memory)
                    })
        
        # Analyze pod-level idleness
        pod_utilization = defaultdict(lambda: {"cpu": [], "memory": []})
        
        if "pod_cpu_usage" in container_insights:
            pod_cpu_data = container_insights["pod_cpu_usage"]
            for point in pod_cpu_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                cpu_usage = self.safe_float(point.get("cpu_usage_millicores", 0))
                pod_utilization[pod_name]["cpu"].append(cpu_usage)
        
        if "pod_memory_usage" in container_insights:
            pod_memory_data = container_insights["pod_memory_usage"]
            for point in pod_memory_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                memory_usage = self.safe_float(point.get("memory_usage_mb", 0))
                pod_utilization[pod_name]["memory"].append(memory_usage)
        
        # Identify idle pods
        for pod_name, utilization in pod_utilization.items():
            cpu_usage = utilization["cpu"]
            memory_usage = utilization["memory"]
            
            if cpu_usage and memory_usage:
                avg_cpu = statistics.mean(cpu_usage)
                avg_memory = statistics.mean(memory_usage)
                
                # Pod is idle if resource usage is very low
                if avg_cpu < 50 and avg_memory < 100:  # 50 millicores, 100MB
                    idle_resources["idle_pods"].append({
                        "pod_name": pod_name,
                        "avg_cpu_usage_millicores": avg_cpu,
                        "avg_memory_usage_mb": avg_memory,
                        "idle_duration_hours": len(cpu_usage) * 0.25,  # Assuming 15-min intervals
                        "estimated_cost_waste": self._estimate_pod_cost_waste(avg_cpu, avg_memory)
                    })
        
        # Calculate idle capacity
        total_idle_nodes = len(idle_resources["idle_nodes"])
        if total_idle_nodes > 0:
            idle_resources["idle_cpu_capacity"] = total_idle_nodes * 2.0  # 2 cores per node estimate
            idle_resources["idle_memory_capacity"] = total_idle_nodes * 8.0  # 8GB per node estimate
        
        # Calculate cost impact
        idle_resources["idle_cost_impact"] = (
            sum(node["estimated_cost_waste"] for node in idle_resources["idle_nodes"]) +
            sum(pod["estimated_cost_waste"] for pod in idle_resources["idle_pods"])
        )
        
        return idle_resources
    
    async def _detect_zombie_workloads(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect zombie workloads (long-running, failed, or stuck resources)"""
        
        zombie_workloads = {
            "zombie_pods": [],
            "stuck_deployments": [],
            "failed_jobs": [],
            "long_running_pods": [],
            "zombie_cost_impact": 0.0
        }
        
        # Get Kubernetes resources
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        pods = k8s_resources.get("pods", [])
        deployments = k8s_resources.get("deployments", [])
        
        current_time = datetime.now(timezone.utc)
        
        # Analyze pods for zombie behavior
        for pod in pods:
            pod_name = pod.get("name", "unknown")
            status = pod.get("status", "Unknown")
            created_str = pod.get("created")
            restart_count = sum(
                container.get("restart_count", 0) 
                for container in pod.get("containers", [])
            )
            
            if created_str:
                try:
                    created_time = datetime.fromisoformat(created_str.replace('Z', '+00:00'))
                    age_hours = (current_time - created_time).total_seconds() / 3600
                    
                    # Zombie criteria
                    is_failed = status in ["Failed", "Error", "CrashLoopBackOff"]
                    is_long_running = age_hours > self.thresholds["zombie_pod_age_hours"]
                    has_many_restarts = restart_count > self.thresholds["failed_pod_threshold"]
                    
                    if is_failed:
                        zombie_workloads["zombie_pods"].append({
                            "pod_name": pod_name,
                            "namespace": pod.get("namespace", "unknown"),
                            "status": status,
                            "age_hours": age_hours,
                            "restart_count": restart_count,
                            "issue": "Failed status",
                            "estimated_waste_cost": self._estimate_zombie_cost(age_hours)
                        })
                    elif is_long_running and has_many_restarts:
                        zombie_workloads["long_running_pods"].append({
                            "pod_name": pod_name,
                            "namespace": pod.get("namespace", "unknown"),
                            "status": status,
                            "age_hours": age_hours,
                            "restart_count": restart_count,
                            "issue": "Long-running with frequent restarts",
                            "estimated_waste_cost": self._estimate_zombie_cost(age_hours)
                        })
                        
                except ValueError:
                    # Skip pods with invalid timestamps
                    continue
        
        # Analyze deployments for stuck status
        for deployment in deployments:
            deployment_name = deployment.get("name", "unknown")
            replicas = deployment.get("replicas", 0)
            ready_replicas = deployment.get("ready_replicas", 0)
            available_replicas = deployment.get("available_replicas", 0)
            
            # Deployment is stuck if replicas don't match ready/available
            if replicas > 0 and (ready_replicas < replicas or available_replicas < replicas):
                unhealthy_ratio = 1 - (min(ready_replicas, available_replicas) / replicas)
                
                if unhealthy_ratio > 0.5:  # More than 50% unhealthy
                    zombie_workloads["stuck_deployments"].append({
                        "deployment_name": deployment_name,
                        "namespace": deployment.get("namespace", "unknown"),
                        "replicas": replicas,
                        "ready_replicas": ready_replicas,
                        "available_replicas": available_replicas,
                        "unhealthy_ratio": unhealthy_ratio,
                        "issue": "Deployment stuck with unhealthy replicas",
                        "estimated_waste_cost": self._estimate_deployment_waste_cost(replicas - ready_replicas)
                    })
        
        # Calculate total zombie cost impact
        zombie_workloads["zombie_cost_impact"] = (
            sum(pod["estimated_waste_cost"] for pod in zombie_workloads["zombie_pods"]) +
            sum(pod["estimated_waste_cost"] for pod in zombie_workloads["long_running_pods"]) +
            sum(dep["estimated_waste_cost"] for dep in zombie_workloads["stuck_deployments"])
        )
        
        return zombie_workloads
    
    async def _find_overprovisioned_resources(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Find overprovisioned resources (requests >> actual usage)"""
        
        overprovisioned = {
            "overprovisioned_pods": [],
            "overprovisioned_nodes": [],
            "resource_waste": {},
            "overprovisioning_cost": 0.0
        }
        
        # Extract resource request and usage data
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # Analyze pod resource requests vs usage
        pod_efficiency_data = {}
        
        if "pod_resource_requests_limits" in container_insights:
            request_data = container_insights["pod_resource_requests_limits"]
            for point in request_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                pod_efficiency_data[pod_name] = {
                    "cpu_request": self.safe_float(point.get("cpu_request_millicores", 0)),
                    "memory_request": self.safe_float(point.get("memory_request_mb", 0)),
                    "cpu_usage": 0,
                    "memory_usage": 0
                }
        
        # Add actual usage data
        if "pod_cpu_usage" in container_insights:
            cpu_data = container_insights["pod_cpu_usage"]
            for point in cpu_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                if pod_name in pod_efficiency_data:
                    cpu_usage = self.safe_float(point.get("cpu_usage_millicores", 0))
                    if pod_efficiency_data[pod_name]["cpu_usage"] == 0:
                        pod_efficiency_data[pod_name]["cpu_usage"] = cpu_usage
                    else:
                        # Average with existing data
                        pod_efficiency_data[pod_name]["cpu_usage"] = (
                            pod_efficiency_data[pod_name]["cpu_usage"] + cpu_usage
                        ) / 2
        
        if "pod_memory_usage" in container_insights:
            memory_data = container_insights["pod_memory_usage"]
            for point in memory_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                if pod_name in pod_efficiency_data:
                    memory_usage = self.safe_float(point.get("memory_usage_mb", 0))
                    if pod_efficiency_data[pod_name]["memory_usage"] == 0:
                        pod_efficiency_data[pod_name]["memory_usage"] = memory_usage
                    else:
                        # Average with existing data
                        pod_efficiency_data[pod_name]["memory_usage"] = (
                            pod_efficiency_data[pod_name]["memory_usage"] + memory_usage
                        ) / 2
        
        # Identify overprovisioned pods
        total_wasted_cpu = 0.0
        total_wasted_memory = 0.0
        
        for pod_name, data in pod_efficiency_data.items():
            cpu_request = data["cpu_request"]
            memory_request = data["memory_request"]
            cpu_usage = data["cpu_usage"]
            memory_usage = data["memory_usage"]
            
            if cpu_request > 0 and memory_request > 0:
                cpu_efficiency = (cpu_usage / cpu_request) * 100 if cpu_request > 0 else 0
                memory_efficiency = (memory_usage / memory_request) * 100 if memory_request > 0 else 0
                
                # Overprovisioned if efficiency is very low
                if (cpu_efficiency < 25 or memory_efficiency < 25) and (cpu_request > 100 or memory_request > 512):
                    wasted_cpu = max(0, cpu_request - cpu_usage)
                    wasted_memory = max(0, memory_request - memory_usage)
                    
                    total_wasted_cpu += wasted_cpu
                    total_wasted_memory += wasted_memory
                    
                    overprovisioned["overprovisioned_pods"].append({
                        "pod_name": pod_name,
                        "cpu_request_millicores": cpu_request,
                        "cpu_usage_millicores": cpu_usage,
                        "memory_request_mb": memory_request,
                        "memory_usage_mb": memory_usage,
                        "cpu_efficiency": cpu_efficiency,
                        "memory_efficiency": memory_efficiency,
                        "wasted_cpu_millicores": wasted_cpu,
                        "wasted_memory_mb": wasted_memory,
                        "estimated_waste_cost": self._estimate_overprovisioning_cost(wasted_cpu, wasted_memory)
                    })
        
        # Resource waste summary
        overprovisioned["resource_waste"] = {
            "total_wasted_cpu_cores": total_wasted_cpu / 1000,
            "total_wasted_memory_gb": total_wasted_memory / 1024,
            "wasted_cpu_percentage": self._calculate_waste_percentage(total_wasted_cpu, "cpu"),
            "wasted_memory_percentage": self._calculate_waste_percentage(total_wasted_memory, "memory")
        }
        
        # Calculate overprovisioning cost
        overprovisioned["overprovisioning_cost"] = sum(
            pod["estimated_waste_cost"] for pod in overprovisioned["overprovisioned_pods"]
        )
        
        return overprovisioned
    
    async def _identify_failed_resources(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Identify failed or problematic resources consuming costs"""
        
        failed_resources = {
            "failed_pods": [],
            "error_events": [],
            "unhealthy_services": [],
            "failed_storage": [],
            "failure_cost_impact": 0.0
        }
        
        # Get Kubernetes resources
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        pods = k8s_resources.get("pods", [])
        services = k8s_resources.get("services", [])
        events = k8s_resources.get("events", [])
        pvs = k8s_resources.get("persistent_volumes", [])
        
        # Analyze failed pods
        for pod in pods:
            status = pod.get("status", "Unknown")
            restart_count = sum(
                container.get("restart_count", 0) 
                for container in pod.get("containers", [])
            )
            
            if status in ["Failed", "Error", "CrashLoopBackOff"] or restart_count > 10:
                failed_resources["failed_pods"].append({
                    "pod_name": pod.get("name", "unknown"),
                    "namespace": pod.get("namespace", "unknown"),
                    "status": status,
                    "restart_count": restart_count,
                    "node": pod.get("node", "unknown"),
                    "failure_reason": self._determine_failure_reason(pod),
                    "estimated_cost_impact": self._estimate_failed_pod_cost(pod)
                })
        
        # Analyze error events
        for event in events[:50]:  # Recent events only
            event_type = event.get("type", "")
            reason = event.get("reason", "")
            
            if event_type == "Warning" and reason in ["FailedScheduling", "FailedMount", "Unhealthy"]:
                failed_resources["error_events"].append({
                    "event_type": event_type,
                    "reason": reason,
                    "message": event.get("message", ""),
                    "object_kind": event.get("involved_object", {}).get("kind", ""),
                    "object_name": event.get("involved_object", {}).get("name", ""),
                    "namespace": event.get("namespace", "unknown"),
                    "count": event.get("count", 1)
                })
        
        # Analyze services without endpoints
        for service in services:
            endpoints = service.get("endpoints", [])
            service_type = service.get("type", "")
            
            if service_type == "LoadBalancer" and not endpoints:
                failed_resources["unhealthy_services"].append({
                    "service_name": service.get("name", "unknown"),
                    "namespace": service.get("namespace", "unknown"),
                    "type": service_type,
                    "issue": "LoadBalancer with no endpoints",
                    "estimated_cost_impact": 50.0  # Estimated monthly cost for unused LB
                })
        
        # Analyze failed storage
        for pv in pvs:
            status = pv.get("status", "Unknown")
            
            if status in ["Failed", "Released"]:
                capacity = self._parse_storage_capacity(pv.get("capacity", "0"))
                failed_resources["failed_storage"].append({
                    "pv_name": pv.get("name", "unknown"),
                    "status": status,
                    "capacity_gb": capacity,
                    "storage_class": pv.get("storage_class", "unknown"),
                    "issue": f"PV in {status} state",
                    "estimated_cost_impact": capacity * 0.1  # $0.1/GB/month estimate
                })
        
        # Calculate total failure cost impact
        failed_resources["failure_cost_impact"] = (
            sum(pod["estimated_cost_impact"] for pod in failed_resources["failed_pods"]) +
            sum(svc["estimated_cost_impact"] for svc in failed_resources["unhealthy_services"]) +
            sum(pv["estimated_cost_impact"] for pv in failed_resources["failed_storage"])
        )
        
        return failed_resources
    
    async def _detect_unused_storage(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect unused or underutilized storage resources"""
        
        unused_storage = {
            "unused_volumes": [],
            "underutilized_volumes": [],
            "orphaned_volumes": [],
            "storage_waste_cost": 0.0
        }
        
        # Get storage resources
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        pvs = k8s_resources.get("persistent_volumes", [])
        pvcs = k8s_resources.get("persistent_volume_claims", [])
        
        # Extract storage utilization data
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        volume_utilization = {}
        if "persistent_volume_claim_metrics" in container_insights:
            pvc_data = container_insights["persistent_volume_claim_metrics"]
            for point in pvc_data.get("data_points", []):
                pv_name = point.get("PVName", "unknown")
                used_percentage = self.safe_float(point.get("used_percentage", 0))
                volume_utilization[pv_name] = used_percentage
        
        # Analyze persistent volumes
        for pv in pvs:
            pv_name = pv.get("name", "unknown")
            status = pv.get("status", "Unknown")
            capacity = self._parse_storage_capacity(pv.get("capacity", "0"))
            claim_ref = pv.get("claim")
            
            # Check if volume is unused
            if status == "Available" and not claim_ref:
                unused_storage["unused_volumes"].append({
                    "pv_name": pv_name,
                    "capacity_gb": capacity,
                    "storage_class": pv.get("storage_class", "unknown"),
                    "status": status,
                    "issue": "Volume available but not claimed",
                    "estimated_waste_cost": capacity * 0.1  # $0.1/GB/month
                })
            
            # Check utilization if volume is bound
            elif status == "Bound" and pv_name in volume_utilization:
                utilization = volume_utilization[pv_name]
                
                if utilization < 20:  # Less than 20% utilized
                    unused_storage["underutilized_volumes"].append({
                        "pv_name": pv_name,
                        "capacity_gb": capacity,
                        "utilization_percentage": utilization,
                        "storage_class": pv.get("storage_class", "unknown"),
                        "issue": f"Volume only {utilization:.1f}% utilized",
                        "estimated_waste_cost": capacity * (100 - utilization) / 100 * 0.1
                    })
        
        # Find orphaned PVCs (PVCs without corresponding pods)
        pvc_to_pod_mapping = self._map_pvcs_to_pods(k8s_resources)
        
        for pvc in pvcs:
            pvc_name = pvc.get("name", "unknown")
            namespace = pvc.get("namespace", "unknown")
            pvc_key = f"{namespace}/{pvc_name}"
            
            if pvc_key not in pvc_to_pod_mapping:
                capacity = self._parse_storage_capacity(pvc.get("capacity", "0"))
                unused_storage["orphaned_volumes"].append({
                    "pvc_name": pvc_name,
                    "namespace": namespace,
                    "capacity_gb": capacity,
                    "status": pvc.get("status", "Unknown"),
                    "issue": "PVC not used by any pod",
                    "estimated_waste_cost": capacity * 0.1
                })
        
        # Calculate total storage waste cost
        unused_storage["storage_waste_cost"] = (
            sum(vol["estimated_waste_cost"] for vol in unused_storage["unused_volumes"]) +
            sum(vol["estimated_waste_cost"] for vol in unused_storage["underutilized_volumes"]) +
            sum(vol["estimated_waste_cost"] for vol in unused_storage["orphaned_volumes"])
        )
        
        return unused_storage
    
    async def _analyze_network_waste(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze network resource waste"""
        
        network_waste = {
            "unused_load_balancers": [],
            "unused_public_ips": [],
            "low_traffic_services": [],
            "network_waste_cost": 0.0
        }
        
        # Get network resources
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        services = k8s_resources.get("services", [])
        
        # Analyze load balancer services
        for service in services:
            service_type = service.get("type", "")
            service_name = service.get("name", "unknown")
            namespace = service.get("namespace", "unknown")
            endpoints = service.get("endpoints", [])
            
            if service_type == "LoadBalancer":
                # Check if load balancer has endpoints
                if not endpoints:
                    network_waste["unused_load_balancers"].append({
                        "service_name": service_name,
                        "namespace": namespace,
                        "issue": "LoadBalancer service without endpoints",
                        "estimated_waste_cost": 25.0  # ~$25/month for Azure LB
                    })
                
                # Check traffic metrics if available
                traffic_metrics = service.get("traffic_metrics", {})
                if traffic_metrics:
                    requests_per_day = traffic_metrics.get("requests_per_day", 0)
                    if requests_per_day < 100:  # Very low traffic
                        network_waste["low_traffic_services"].append({
                            "service_name": service_name,
                            "namespace": namespace,
                            "requests_per_day": requests_per_day,
                            "issue": "LoadBalancer with very low traffic",
                            "estimated_waste_cost": 20.0  # Partial waste
                        })
        
        # Calculate network waste cost
        network_waste["network_waste_cost"] = (
            sum(lb["estimated_waste_cost"] for lb in network_waste["unused_load_balancers"]) +
            sum(svc["estimated_waste_cost"] for svc in network_waste["low_traffic_services"])
        )
        
        return network_waste
    
    async def _quantify_cost_waste(self, cluster_data: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Quantify the cost impact of identified waste"""
        
        cost_waste = {
            "waste_by_category": {},
            "waste_by_resource_type": {},
            "monthly_waste_cost": 0.0,
            "annual_waste_cost": 0.0,
            "cost_optimization_potential": 0.0
        }
        
        # Aggregate waste costs by category
        categories = {
            "idle_resources": analysis.get("idle_resources", {}).get("idle_cost_impact", 0.0),
            "zombie_workloads": analysis.get("zombie_workloads", {}).get("zombie_cost_impact", 0.0),
            "overprovisioned_resources": analysis.get("overprovisioned_resources", {}).get("overprovisioning_cost", 0.0),
            "failed_resources": analysis.get("failed_resources", {}).get("failure_cost_impact", 0.0),
            "unused_storage": analysis.get("unused_storage", {}).get("storage_waste_cost", 0.0),
            "network_waste": analysis.get("network_waste", {}).get("network_waste_cost", 0.0)
        }
        
        cost_waste["waste_by_category"] = categories
        cost_waste["monthly_waste_cost"] = sum(categories.values())
        cost_waste["annual_waste_cost"] = cost_waste["monthly_waste_cost"] * 12
        
        # Waste by resource type
        cost_waste["waste_by_resource_type"] = {
            "compute_waste": categories["idle_resources"] + categories["zombie_workloads"] + categories["overprovisioned_resources"],
            "storage_waste": categories["unused_storage"],
            "network_waste": categories["network_waste"],
            "management_waste": categories["failed_resources"]
        }
        
        # Calculate optimization potential (immediate vs long-term)
        immediate_savings = categories["idle_resources"] + categories["unused_storage"] + categories["network_waste"]
        medium_term_savings = categories["overprovisioned_resources"] + categories["zombie_workloads"]
        long_term_savings = categories["failed_resources"]
        
        cost_waste["cost_optimization_potential"] = {
            "immediate_savings": immediate_savings,  # Can be fixed within days
            "medium_term_savings": medium_term_savings,  # Requires planning (weeks)
            "long_term_savings": long_term_savings,  # Requires investigation (months)
            "total_potential": immediate_savings + medium_term_savings + long_term_savings
        }
        
        return cost_waste
    
    async def _generate_waste_summary(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive waste summary"""
        
        summary = {
            "total_waste_issues": 0,
            "critical_waste_issues": 0,
            "waste_distribution": {},
            "top_waste_sources": [],
            "quick_wins": [],
            "waste_trends": {}
        }
        
        # Count waste issues
        idle_resources = analysis.get("idle_resources", {})
        zombie_workloads = analysis.get("zombie_workloads", {})
        overprovisioned = analysis.get("overprovisioned_resources", {})
        failed_resources = analysis.get("failed_resources", {})
        unused_storage = analysis.get("unused_storage", {})
        network_waste = analysis.get("network_waste", {})
        
        issue_counts = {
            "idle_nodes": len(idle_resources.get("idle_nodes", [])),
            "idle_pods": len(idle_resources.get("idle_pods", [])),
            "zombie_pods": len(zombie_workloads.get("zombie_pods", [])),
            "overprovisioned_pods": len(overprovisioned.get("overprovisioned_pods", [])),
            "failed_pods": len(failed_resources.get("failed_pods", [])),
            "unused_volumes": len(unused_storage.get("unused_volumes", [])),
            "unused_load_balancers": len(network_waste.get("unused_load_balancers", []))
        }
        
        summary["total_waste_issues"] = sum(issue_counts.values())
        summary["waste_distribution"] = issue_counts
        
        # Identify critical issues (high cost impact)
        cost_waste = analysis.get("cost_waste", {})
        categories = cost_waste.get("waste_by_category", {})
        
        for category, cost in categories.items():
            if cost > 100:  # More than $100/month waste
                summary["critical_waste_issues"] += 1
        
        # Top waste sources
        top_sources = []
        
        # Add top idle nodes
        for node in idle_resources.get("idle_nodes", [])[:3]:
            top_sources.append({
                "source": f"Idle node: {node['node_name']}",
                "type": "idle_resource",
                "cost_impact": node["estimated_cost_waste"],
                "severity": "high" if node["estimated_cost_waste"] > 50 else "medium"
            })
        
        # Add top overprovisioned pods
        for pod in sorted(overprovisioned.get("overprovisioned_pods", []), 
                         key=lambda x: x["estimated_waste_cost"], reverse=True)[:3]:
            top_sources.append({
                "source": f"Overprovisioned pod: {pod['pod_name']}",
                "type": "overprovisioned",
                "cost_impact": pod["estimated_waste_cost"],
                "severity": "medium"
            })
        
        summary["top_waste_sources"] = sorted(top_sources, key=lambda x: x["cost_impact"], reverse=True)[:5]
        
        # Quick wins (easy to fix, high impact)
        quick_wins = []
        
        # Unused load balancers are quick wins
        for lb in network_waste.get("unused_load_balancers", []):
            quick_wins.append({
                "action": f"Remove unused LoadBalancer: {lb['service_name']}",
                "effort": "low",
                "impact": "medium",
                "savings": lb["estimated_waste_cost"]
            })
        
        # Unused storage is a quick win
        for vol in unused_storage.get("unused_volumes", []):
            quick_wins.append({
                "action": f"Delete unused volume: {vol['pv_name']}",
                "effort": "low",
                "impact": "low",
                "savings": vol["estimated_waste_cost"]
            })
        
        summary["quick_wins"] = sorted(quick_wins, key=lambda x: x["savings"], reverse=True)[:5]
        
        return summary
    
    async def _generate_waste_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate waste reduction recommendations"""
        
        recommendations = []
        
        # Based on idle resources
        idle_resources = analysis.get("idle_resources", {})
        idle_nodes = idle_resources.get("idle_nodes", [])
        idle_pods = idle_resources.get("idle_pods", [])
        
        if len(idle_nodes) > 0:
            recommendations.append(
                f"Scale down or terminate {len(idle_nodes)} idle nodes to save ~${sum(n['estimated_cost_waste'] for n in idle_nodes):.0f}/month"
            )
        
        if len(idle_pods) > 0:
            recommendations.append(
                f"Investigate {len(idle_pods)} idle pods - consider reducing replicas or resource requests"
            )
        
        # Based on zombie workloads
        zombie_workloads = analysis.get("zombie_workloads", {})
        zombie_pods = zombie_workloads.get("zombie_pods", [])
        stuck_deployments = zombie_workloads.get("stuck_deployments", [])
        
        if len(zombie_pods) > 0:
            recommendations.append(
                f"Clean up {len(zombie_pods)} failed/zombie pods to reclaim resources"
            )
        
        if len(stuck_deployments) > 0:
            recommendations.append(
                f"Fix {len(stuck_deployments)} stuck deployments to improve resource efficiency"
            )
        
        # Based on overprovisioning
        overprovisioned = analysis.get("overprovisioned_resources", {})
        overprovisioned_pods = overprovisioned.get("overprovisioned_pods", [])
        
        if len(overprovisioned_pods) > 0:
            total_savings = sum(pod["estimated_waste_cost"] for pod in overprovisioned_pods)
            recommendations.append(
                f"Right-size {len(overprovisioned_pods)} overprovisioned pods to save ~${total_savings:.0f}/month"
            )
        
        # Based on storage waste
        unused_storage = analysis.get("unused_storage", {})
        unused_volumes = unused_storage.get("unused_volumes", [])
        underutilized_volumes = unused_storage.get("underutilized_volumes", [])
        
        if len(unused_volumes) > 0:
            recommendations.append(
                f"Delete {len(unused_volumes)} unused storage volumes"
            )
        
        if len(underutilized_volumes) > 0:
            recommendations.append(
                f"Consider resizing {len(underutilized_volumes)} underutilized storage volumes"
            )
        
        # Based on network waste
        network_waste = analysis.get("network_waste", {})
        unused_lbs = network_waste.get("unused_load_balancers", [])
        
        if len(unused_lbs) > 0:
            lb_savings = sum(lb["estimated_waste_cost"] for lb in unused_lbs)
            recommendations.append(
                f"Remove {len(unused_lbs)} unused LoadBalancers to save ~${lb_savings:.0f}/month"
            )
        
        # Based on overall waste level
        cost_waste = analysis.get("cost_waste", {})
        monthly_waste = cost_waste.get("monthly_waste_cost", 0.0)
        
        if monthly_waste > 500:
            recommendations.append(
                "Implement automated waste detection and cleanup policies"
            )
        
        if monthly_waste > 200:
            recommendations.append(
                "Establish regular resource optimization reviews"
            )
        
        return recommendations
    
    # Helper methods
    def _estimate_node_cost_waste(self, node_name: str, cpu_util: float, memory_util: float) -> float:
        """Estimate cost waste for an idle node"""
        # Simplified estimation - assume $70/month per node
        base_cost = 70.0
        utilization_factor = max(cpu_util, memory_util) / 100
        waste_factor = 1 - utilization_factor
        return base_cost * waste_factor
    
    def _estimate_pod_cost_waste(self, cpu_usage: float, memory_usage: float) -> float:
        """Estimate cost waste for an idle pod"""
        # Very rough estimation based on resource consumption
        cpu_cost = (cpu_usage / 1000) * 20  # $20 per core per month
        memory_cost = (memory_usage / 1024) * 5  # $5 per GB per month
        return cpu_cost + memory_cost
    
    def _estimate_zombie_cost(self, age_hours: float) -> float:
        """Estimate cost impact of zombie workload"""
        # Assume $0.05 per hour for a typical pod
        return age_hours * 0.05
    
    def _estimate_deployment_waste_cost(self, unhealthy_replicas: int) -> float:
        """Estimate cost waste from unhealthy deployment replicas"""
        # Assume $20/month per replica
        return unhealthy_replicas * 20.0
    
    def _estimate_overprovisioning_cost(self, wasted_cpu: float, wasted_memory: float) -> float:
        """Estimate cost of overprovisioned resources"""
        cpu_cost = (wasted_cpu / 1000) * 20  # $20 per core per month
        memory_cost = (wasted_memory / 1024) * 5  # $5 per GB per month
        return cpu_cost + memory_cost
    
    def _estimate_failed_pod_cost(self, pod: Dict[str, Any]) -> float:
        """Estimate cost impact of failed pod"""
        # Simplified estimation based on pod age and resource allocation
        return 10.0  # $10/month average for failed pod
    
    def _parse_storage_capacity(self, capacity_str: str) -> float:
        """Parse storage capacity string to GB"""
        if not capacity_str:
            return 0.0
        
        capacity_str = capacity_str.upper().replace("I", "")
        
        try:
            if capacity_str.endswith("GB") or capacity_str.endswith("G"):
                return float(capacity_str.replace("GB", "").replace("G", ""))
            elif capacity_str.endswith("TB") or capacity_str.endswith("T"):
                return float(capacity_str.replace("TB", "").replace("T", "")) * 1024
            else:
                return float(capacity_str)
        except ValueError:
            return 0.0
    
    def _determine_failure_reason(self, pod: Dict[str, Any]) -> str:
        """Determine primary failure reason for a pod"""
        status = pod.get("status", "Unknown")
        containers = pod.get("containers", [])
        
        if status == "CrashLoopBackOff":
            return "Application crashing repeatedly"
        elif status == "Failed":
            return "Pod execution failed"
        elif any(c.get("restart_count", 0) > 5 for c in containers):
            return "High restart count indicates instability"
        else:
            return "Unknown failure reason"
    
    def _map_pvcs_to_pods(self, k8s_resources: Dict[str, Any]) -> Dict[str, List[str]]:
        """Map PVCs to pods that use them"""
        pvc_mapping = {}
        pods = k8s_resources.get("pods", [])
        
        for pod in pods:
            pod_name = pod.get("name", "unknown")
            namespace = pod.get("namespace", "unknown")
            containers = pod.get("containers", [])
            
            for container in containers:
                volume_mounts = container.get("volume_mounts", [])
                for mount in volume_mounts:
                    pvc_name = mount.get("pvc_name")  # This would need proper extraction
                    if pvc_name:
                        pvc_key = f"{namespace}/{pvc_name}"
                        if pvc_key not in pvc_mapping:
                            pvc_mapping[pvc_key] = []
                        pvc_mapping[pvc_key].append(pod_name)
        
        return pvc_mapping
    
    def _calculate_waste_percentage(self, wasted_amount: float, resource_type: str) -> float:
        """Calculate waste percentage for a resource type"""
        # This would need cluster capacity data for accurate calculation
        # Simplified estimation
        if resource_type == "cpu":
            # Assume cluster has 100 cores total
            return (wasted_amount / 100000) * 100  # millicores to percentage
        elif resource_type == "memory":
            # Assume cluster has 400GB total
            return (wasted_amount / 400000) * 100  # MB to percentage
        else:
            return 0.0
    
    def _calculate_total_waste_cost(self, analysis: Dict[str, Any]) -> float:
        """Calculate total waste cost across all categories"""
        if isinstance(analysis, dict):
            cost_waste = analysis.get("cost_waste", {})
            if isinstance(cost_waste, dict):
                return cost_waste.get("monthly_waste_cost", 0.0)
        return 0.0

    
    def _calculate_waste_percentage(self, analysis: Dict[str, Any], cluster_data: Any) -> float:
        """Calculate waste as percentage of total cluster cost"""
        total_waste = self._calculate_total_waste_cost(analysis)

        if not isinstance(cluster_data, dict):
            return 0.0  # or log a warning if needed

        detailed_costs = cluster_data.get("detailed_costs", {})
        if not isinstance(detailed_costs, dict):
            detailed_costs = {}

        total_cluster_cost = detailed_costs.get("summary", {}).get("total_cost", 1.0)

        return (total_waste / total_cluster_cost) * 100 if total_cluster_cost > 0 else 0.0

    
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