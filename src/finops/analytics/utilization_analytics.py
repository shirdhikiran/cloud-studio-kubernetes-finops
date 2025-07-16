# src/finops/analytics/utilization_analytics.py
"""
Utilization Analytics Module - Resource utilization analysis and efficiency scoring
"""
import traceback
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import structlog
import statistics
from collections import defaultdict
import numpy as np
import math

logger = structlog.get_logger(__name__)


class UtilizationAnalytics:
    """Advanced utilization analytics for resource efficiency insights"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(module="utilization_analytics")
        
        # Utilization thresholds
        self.thresholds = {
            'low_utilization_threshold': config.get('low_utilization_threshold', 30.0),  # %
            'high_utilization_threshold': config.get('high_utilization_threshold', 80.0),  # %
            'efficiency_target': config.get('efficiency_target', 70.0),  # %
            'waste_threshold': config.get('waste_threshold', 10.0),  # %
        }
    
    async def analyze_cluster_utilization(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive utilization analysis for a cluster"""
        
        cluster_info = cluster_data.get("cluster_info", {})
        cluster_name = cluster_info.get("name", "unknown")
        
        self.logger.info(f"Starting utilization analysis for cluster: {cluster_name}")
        
        analysis = {
            "cluster_name": cluster_name,
            "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
            "cluster_utilization": {},
            "node_utilization": {},
            "pod_utilization": {},
            "namespace_utilization": {},
            "resource_patterns": {},
            "efficiency_metrics": {},
            "capacity_analysis": {},
            "recommendations": [],
            "efficiency_score": 0.0
        }
        
        try:
            # 1. Cluster-level utilization
            analysis["cluster_utilization"] = await self._analyze_cluster_level_utilization(cluster_data)
            
            # 2. Node-level utilization
            analysis["node_utilization"] = await self._analyze_node_utilization(cluster_data)
            
            # 3. Pod-level utilization
            analysis["pod_utilization"] = await self._analyze_pod_utilization(cluster_data)
            
            # 4. Namespace-level utilization
            analysis["namespace_utilization"] = await self._analyze_namespace_utilization(cluster_data)
            
            # 5. Resource usage patterns
            analysis["resource_patterns"] = await self._analyze_resource_patterns(cluster_data)
            
            # 6. Efficiency metrics
            analysis["efficiency_metrics"] = await self._calculate_efficiency_metrics(cluster_data)
            
            # 7. Capacity analysis
            analysis["capacity_analysis"] = await self._analyze_capacity_utilization(cluster_data)
            
            # 8. Generate recommendations
            analysis["recommendations"] = await self._generate_utilization_recommendations(analysis)
            
            # 9. Calculate efficiency score
            analysis["efficiency_score"] = self._calculate_utilization_efficiency_score(analysis)
            
            # Add summary counts
            analysis["node_count"] = len(cluster_data.get("node_pools", []))
            k8s_resources = cluster_data.get("kubernetes_resources", {})
            analysis["pod_count"] = len(k8s_resources.get("pods", []))
            analysis["namespace_count"] = len(k8s_resources.get("namespaces", []))
            
            self.logger.info(
                f"Utilization analysis completed for {cluster_name}",
                efficiency_score=analysis["efficiency_score"],
                avg_cpu_utilization=analysis["cluster_utilization"].get("average_cpu_utilization", 0),
                avg_memory_utilization=analysis["cluster_utilization"].get("average_memory_utilization", 0)
            )
            
        except Exception as e:
           
            self.logger.error(f"Utilization analysis failed for {cluster_name}: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    async def _analyze_cluster_level_utilization(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cluster-level resource utilization"""
        
        utilization = {
            "average_cpu_utilization": 0.0,
            "average_memory_utilization": 0.0,
            "average_storage_utilization": 0.0,
            "average_network_utilization": 0.0,
            "peak_cpu_utilization": 0.0,
            "peak_memory_utilization": 0.0,
            "utilization_trend": "stable",
            "resource_distribution": {}
        }
        
        # Extract metrics from Container Insights data
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # CPU utilization from node performance data
        cpu_utilization_data = []
        if "aks_node_cpu_performance" in container_insights:
            cpu_data = container_insights["aks_node_cpu_performance"]
            data_points = cpu_data.get("data_points", [])
            
            for point in data_points:
                cpu_usage = self.safe_float(point.get("cpu_usage_millicores", 0))
                if cpu_usage > 0:
                    # Convert millicores to percentage (assuming 2-core nodes as baseline)
                    cpu_percent = (cpu_usage / 2000) * 100  # 2000 millicores = 2 cores
                    cpu_utilization_data.append(min(100, cpu_percent))
        
        if cpu_utilization_data:
            utilization["average_cpu_utilization"] = statistics.mean(cpu_utilization_data)
            utilization["peak_cpu_utilization"] = max(cpu_utilization_data)
        
        # Memory utilization from node performance data
        memory_utilization_data = []
        if "aks_node_memory_performance" in container_insights:
            memory_data = container_insights["aks_node_memory_performance"]
            data_points = memory_data.get("data_points", [])
            
            for point in data_points:
                
                memory_usage_percent = self.safe_float(point.get("memory_usage_percentage", 0))
                
                if memory_usage_percent > 0:
                    memory_utilization_data.append(min(100, memory_usage_percent))
        
        if memory_utilization_data:
            utilization["average_memory_utilization"] = statistics.mean(memory_utilization_data)
            utilization["peak_memory_utilization"] = max(memory_utilization_data)
        
        # Network utilization from node network data
        network_utilization_data = []
        if "aks_node_network_performance" in container_insights:
            network_data = container_insights["aks_node_network_performance"]
            data_points = network_data.get("data_points", [])
            
            for point in data_points:
                network_mbps = self.safe_float(point.get("network_in_mbps", 0))
                if network_mbps > 0:
                    # Assume 1Gbps network capacity
                    network_percent = (network_mbps / 1000) * 100
                    network_utilization_data.append(min(100, network_percent))
        
        if network_utilization_data:
            utilization["average_network_utilization"] = statistics.mean(network_utilization_data)
        
        # Storage utilization from volume data
        storage_utilization_data = []
        if "volume_usage" in container_insights:
            volume_data = container_insights["volume_usage"]
            data_points = volume_data.get("data_points", [])
            
            for point in data_points:
                free_space_percent = self.safe_float(point.get("free_space_percentage", 100))
                if free_space_percent < 100:
                    used_percent = 100 - free_space_percent
                    storage_utilization_data.append(used_percent)
        
        if storage_utilization_data:
            utilization["average_storage_utilization"] = statistics.mean(storage_utilization_data)
        
        # Determine utilization trend
        if len(cpu_utilization_data) >= 10:
            recent_cpu = statistics.mean(cpu_utilization_data[-5:])
            older_cpu = statistics.mean(cpu_utilization_data[:5])
            
            if recent_cpu > older_cpu + 10:
                utilization["utilization_trend"] = "increasing"
            elif recent_cpu < older_cpu - 10:
                utilization["utilization_trend"] = "decreasing"
            else:
                utilization["utilization_trend"] = "stable"
        
        # Resource distribution
        utilization["resource_distribution"] = {
            "cpu_distribution": self._calculate_distribution(cpu_utilization_data),
            "memory_distribution": self._calculate_distribution(memory_utilization_data),
            "balanced_utilization": self._calculate_balance_score(utilization)
        }
        
        return utilization
    
    async def _analyze_node_utilization(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze node-level utilization patterns"""
        
        node_analysis = {
            "node_utilization_summary": {},
            "underutilized_nodes": [],
            "overutilized_nodes": [],
            "node_efficiency_scores": {},
            "node_recommendations": []
        }
        
        # Extract node data from Container Insights
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # Group utilization data by node
        node_cpu_data = defaultdict(list)
        node_memory_data = defaultdict(list)
        
        if "aks_node_cpu_performance" in container_insights:
            cpu_data = container_insights["aks_node_cpu_performance"]
            for point in cpu_data.get("data_points", []):
                node_name = point.get("NodeName", "unknown")
                cpu_usage = self.safe_float(point.get("cpu_usage_millicores", 0))
                if cpu_usage > 0:
                    cpu_percent = (cpu_usage / 2000) * 100  # Assuming 2-core baseline
                    node_cpu_data[node_name].append(min(100, cpu_percent))
        
        if "aks_node_memory_performance" in container_insights:
            memory_data = container_insights["aks_node_memory_performance"]
            for point in memory_data.get("data_points", []):
                node_name = point.get("NodeName", "unknown")
                memory_percent = self.safe_float(point.get("memory_usage_percentage", 0))
                if memory_percent > 0:
                    node_memory_data[node_name].append(min(100, memory_percent))
        
        # Analyze each node
        all_nodes = set(node_cpu_data.keys()) | set(node_memory_data.keys())
        
        for node_name in all_nodes:
            cpu_utilization = node_cpu_data.get(node_name, [])
            memory_utilization = node_memory_data.get(node_name, [])
            
            avg_cpu = statistics.mean(cpu_utilization) if cpu_utilization else 0
            avg_memory = statistics.mean(memory_utilization) if memory_utilization else 0
            
            node_analysis["node_utilization_summary"][node_name] = {
                "average_cpu_utilization": avg_cpu,
                "average_memory_utilization": avg_memory,
                "peak_cpu_utilization": max(cpu_utilization) if cpu_utilization else 0,
                "peak_memory_utilization": max(memory_utilization) if memory_utilization else 0,
                "cpu_variance": statistics.stdev(cpu_utilization) if len(cpu_utilization) > 1 else 0,
                "memory_variance": statistics.stdev(memory_utilization) if len(memory_utilization) > 1 else 0,
                "data_points": len(cpu_utilization) + len(memory_utilization)
            }
            
            # Calculate efficiency score for this node
            efficiency_score = self._calculate_node_efficiency_score(avg_cpu, avg_memory)
            node_analysis["node_efficiency_scores"][node_name] = efficiency_score
            
            # Categorize nodes
            if avg_cpu < self.thresholds["low_utilization_threshold"] and avg_memory < self.thresholds["low_utilization_threshold"]:
                node_analysis["underutilized_nodes"].append({
                    "node_name": node_name,
                    "cpu_utilization": avg_cpu,
                    "memory_utilization": avg_memory,
                    "efficiency_score": efficiency_score,
                    "potential_action": "Consider scaling down or consolidating workloads"
                })
            elif avg_cpu > self.thresholds["high_utilization_threshold"] or avg_memory > self.thresholds["high_utilization_threshold"]:
                node_analysis["overutilized_nodes"].append({
                    "node_name": node_name,
                    "cpu_utilization": avg_cpu,
                    "memory_utilization": avg_memory,
                    "efficiency_score": efficiency_score,
                    "potential_action": "Consider scaling up or redistributing workloads"
                })
        
        return node_analysis
    
    async def _analyze_pod_utilization(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze pod-level utilization and efficiency"""
        
        pod_analysis = {
            "pod_utilization_summary": {},
            "inefficient_pods": [],
            "high_efficiency_pods": [],
            "resource_request_analysis": {},
            "pod_recommendations": []
        }
        
        # Extract pod data from Container Insights
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # Group pod utilization data
        pod_cpu_data = defaultdict(list)
        pod_memory_data = defaultdict(list)
        pod_request_data = {}
        
        if "pod_cpu_usage" in container_insights:
            cpu_data = container_insights["pod_cpu_usage"]
            for point in cpu_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                cpu_usage = self.safe_float(point.get("cpu_usage_millicores", 0))
                cpu_efficiency = self.safe_float(point.get("cpu_efficiency", 0))
                
                if cpu_usage > 0:
                    pod_cpu_data[pod_name].append({
                        "usage_millicores": cpu_usage,
                        "efficiency_percent": cpu_efficiency
                    })
        
        if "pod_memory_usage" in container_insights:
            memory_data = container_insights["pod_memory_usage"]
            for point in memory_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                memory_usage = self.safe_float(point.get("memory_usage_mb", 0))
                memory_efficiency = self.safe_float(point.get("memory_efficiency", 0))
                
                if memory_usage > 0:
                    pod_memory_data[pod_name].append({
                        "usage_mb": memory_usage,
                        "efficiency_percent": memory_efficiency
                    })
        
        # Analyze resource requests vs limits
        if "pod_resource_requests_limits" in container_insights:
            request_data = container_insights["pod_resource_requests_limits"]
            for point in request_data.get("data_points", []):
                pod_name = point.get("PodName", "unknown")
                pod_request_data[pod_name] = {
                    "cpu_request_millicores": self.safe_float(point.get("cpu_request_millicores", 0)),
                    "memory_request_mb": self.safe_float(point.get("memory_request_mb", 0)),
                    "cpu_limit_millicores": self.safe_float(point.get("cpu_limit", 0)),
                    "memory_limit_mb": self.safe_float(point.get("memory_limit", 0))
                }
        
        # Analyze each pod
        all_pods = set(pod_cpu_data.keys()) | set(pod_memory_data.keys())
        
        for pod_name in all_pods:
            cpu_data = pod_cpu_data.get(pod_name, [])
            memory_data = pod_memory_data.get(pod_name, [])
            
            if cpu_data:
                avg_cpu_usage = statistics.mean([p["usage_millicores"] for p in cpu_data])
                avg_cpu_efficiency = statistics.mean([p["efficiency_percent"] for p in cpu_data])
            else:
                avg_cpu_usage = 0
                avg_cpu_efficiency = 0
            
            if memory_data:
                avg_memory_usage = statistics.mean([p["usage_mb"] for p in memory_data])
                avg_memory_efficiency = statistics.mean([p["efficiency_percent"] for p in memory_data])
            else:
                avg_memory_usage = 0
                avg_memory_efficiency = 0
            
            # Get request/limit data
            requests = pod_request_data.get(pod_name, {})
            
            pod_summary = {
                "pod_name": pod_name,
                "cpu_usage_millicores": avg_cpu_usage,
                "memory_usage_mb": avg_memory_usage,
                "cpu_efficiency_percent": avg_cpu_efficiency,
                "memory_efficiency_percent": avg_memory_efficiency,
                "cpu_request_millicores": requests.get("cpu_request_millicores", 0),
                "memory_request_mb": requests.get("memory_request_mb", 0),
                "overall_efficiency": (avg_cpu_efficiency + avg_memory_efficiency) / 2
            }
            
            pod_analysis["pod_utilization_summary"][pod_name] = pod_summary
            
            # Categorize pods
            if pod_summary["overall_efficiency"] < 30:  # Low efficiency
                pod_analysis["inefficient_pods"].append({
                    **pod_summary,
                    "issue": "Low resource efficiency",
                    "recommendation": "Consider reducing resource requests or investigating usage patterns"
                })
            elif pod_summary["overall_efficiency"] > 80:  # High efficiency
                pod_analysis["high_efficiency_pods"].append({
                    **pod_summary,
                    "status": "Highly efficient resource usage"
                })
        
        # Resource request analysis
        pod_analysis["resource_request_analysis"] = self._analyze_resource_requests(pod_analysis["pod_utilization_summary"])
        
        return pod_analysis
    
    async def _analyze_namespace_utilization(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze namespace-level resource utilization"""
        
        namespace_analysis = {
            "namespace_utilization_summary": {},
            "resource_intensive_namespaces": [],
            "underutilized_namespaces": [],
            "namespace_efficiency_scores": {}
        }
        
        # Extract namespace data from Container Insights
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # Group data by namespace
        namespace_data = defaultdict(lambda: {"pods": 0, "cpu_usage": [], "memory_usage": []})
        
        if "namespace_resource_attribution" in container_insights:
            ns_data = container_insights["namespace_resource_attribution"]
            for point in ns_data.get("data_points", []):
                namespace = point.get("Namespace", "unknown")
                total_pods = self.safe_float(point.get("total_pods", 0))
                running_pods = self.safe_float(point.get("running_pods", 0))
                efficiency = self.safe_float(point.get("namespace_efficiency", 0))
                
                namespace_data[namespace]["pods"] = max(namespace_data[namespace]["pods"], total_pods)
                namespace_data[namespace]["running_pods"] = max(namespace_data[namespace].get("running_pods", 0), running_pods)
                namespace_data[namespace]["efficiency"] = efficiency
        
        # Analyze each namespace
        for namespace, data in namespace_data.items():
            pod_count = data["pods"]
            running_pods = data.get("running_pods", 0)
            efficiency = data.get("efficiency", 0)
            
            namespace_summary = {
                "namespace": namespace,
                "total_pods": pod_count,
                "running_pods": running_pods,
                "pod_success_rate": efficiency,
                "resource_density": self._calculate_namespace_density(namespace, pod_count),
                "efficiency_score": self._calculate_namespace_efficiency_score(efficiency, pod_count)
            }
            
            namespace_analysis["namespace_utilization_summary"][namespace] = namespace_summary
            namespace_analysis["namespace_efficiency_scores"][namespace] = namespace_summary["efficiency_score"]
            
            # Categorize namespaces
            if pod_count > 20 and efficiency > 90:  # High pod count with high efficiency
                namespace_analysis["resource_intensive_namespaces"].append({
                    **namespace_summary,
                    "status": "High resource usage with good efficiency"
                })
            elif pod_count > 5 and efficiency < 70:  # Moderate pod count with low efficiency
                namespace_analysis["underutilized_namespaces"].append({
                    **namespace_summary,
                    "issue": "Low efficiency or pod failures",
                    "recommendation": "Investigate pod failures and resource allocation"
                })
        
        return namespace_analysis
    
    async def _analyze_resource_patterns(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze resource usage patterns and trends"""
        
        patterns = {
            "usage_patterns": {},
            "scaling_patterns": {},
            "resource_correlation": {},
            "peak_usage_analysis": {},
            "trend_analysis": {}
        }
        
        # Extract time-series data
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # Usage patterns over time
        if "cluster_capacity_summary" in container_insights:
            capacity_data = container_insights["cluster_capacity_summary"]
            data_points = capacity_data.get("data_points", [])
            
            cpu_utilization_over_time = []
            memory_utilization_over_time = []
            
            for point in data_points:
                cpu_util = self.safe_float(point.get("cpu_utilization_percentage", 0))
                memory_util = self.safe_float(point.get("memory_utilization_percentage", 0))
                
                if cpu_util > 0:
                    cpu_utilization_over_time.append(cpu_util)
                if memory_util > 0:
                    memory_utilization_over_time.append(memory_util)
            
            patterns["usage_patterns"] = {
                "cpu_pattern": {
                    "average": statistics.mean(cpu_utilization_over_time) if cpu_utilization_over_time else 0,
                    "variance": statistics.stdev(cpu_utilization_over_time) if len(cpu_utilization_over_time) > 1 else 0,
                    "trend": self._calculate_trend(cpu_utilization_over_time)
                },
                "memory_pattern": {
                    "average": statistics.mean(memory_utilization_over_time) if memory_utilization_over_time else 0,
                    "variance": statistics.stdev(memory_utilization_over_time) if len(memory_utilization_over_time) > 1 else 0,
                    "trend": self._calculate_trend(memory_utilization_over_time)
                }
            }
        
        # Resource correlation analysis
        if "usage_patterns" in patterns:
            cpu_data = patterns["usage_patterns"]["cpu_pattern"]
            memory_data = patterns["usage_patterns"]["memory_pattern"]
            
            patterns["resource_correlation"] = {
                "cpu_memory_correlation": self._calculate_correlation(cpu_data, memory_data),
                "resource_balance": self._calculate_resource_balance(cpu_data, memory_data)
            }
        
        return patterns
    
    async def _calculate_efficiency_metrics(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate comprehensive efficiency metrics"""
        
        metrics = {
            "overall_efficiency": 0.0,
            "resource_efficiency": {},
            "waste_indicators": {},
            "optimization_potential": {},
            "efficiency_trends": {}
        }
        
        # Extract utilization data
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # Calculate resource efficiency
        cpu_utilization = []
        memory_utilization = []
        
        if "cluster_capacity_summary" in container_insights:
            capacity_data = container_insights["cluster_capacity_summary"]
            for point in capacity_data.get("data_points", []):
                cpu_util = self.safe_float(point.get("cpu_utilization_percentage", 0))
                memory_util = self.safe_float(point.get("memory_utilization_percentage", 0))
                
                if cpu_util > 0:
                    cpu_utilization.append(cpu_util)
                if memory_util > 0:
                    memory_utilization.append(memory_util)
        
        avg_cpu = statistics.mean(cpu_utilization) if cpu_utilization else 0
        avg_memory = statistics.mean(memory_utilization) if memory_utilization else 0
        
        metrics["resource_efficiency"] = {
            "cpu_efficiency": avg_cpu,
            "memory_efficiency": avg_memory,
            "balanced_efficiency": (avg_cpu + avg_memory) / 2,
            "efficiency_consistency": self._calculate_efficiency_consistency(cpu_utilization, memory_utilization)
        }
        
        # Waste indicators
        metrics["waste_indicators"] = {
            "low_utilization_percentage": self._calculate_waste_percentage(cpu_utilization, memory_utilization),
            "overprovisioning_indicator": self._calculate_overprovisioning_indicator(cluster_data),
            "idle_capacity_percentage": max(0, 100 - avg_cpu, 100 - avg_memory)
        }
        
        # Overall efficiency score
        metrics["overall_efficiency"] = self._calculate_overall_efficiency_score(metrics)
        
        return metrics
    
    async def _analyze_capacity_utilization(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze capacity utilization and planning"""
        
        capacity_analysis = {
            "current_capacity": {},
            "capacity_utilization": {},
            "capacity_trends": {},
            "scaling_recommendations": [],
            "capacity_planning": {}
        }
        
        # Extract capacity data
        node_pools = cluster_data.get("node_pools", [])
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        
        # Calculate current capacity
        total_nodes = sum(pool.get("count", 0) for pool in node_pools)
        total_pods = len(k8s_resources.get("pods", []))
        
        capacity_analysis["current_capacity"] = {
            "total_nodes": total_nodes,
            "total_pods": total_pods,
            "pods_per_node": total_pods / max(1, total_nodes),
            "node_utilization_ratio": total_pods / max(1, total_nodes * 110)  # Assuming 110 pods per node max
        }
        
        # Capacity utilization from metrics
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        if "cluster_capacity_summary" in container_insights:
            capacity_data = container_insights["cluster_capacity_summary"]
            latest_point = capacity_data.get("data_points", [{}])[-1] if capacity_data.get("data_points") else {}
            
            capacity_analysis["capacity_utilization"] = {
                "cpu_capacity_used": self.safe_float(latest_point.get("cpu_utilization_percentage", 0)),
                "memory_capacity_used": self.safe_float(latest_point.get("memory_utilization_percentage", 0)),
                "nodes_active": self.safe_float(latest_point.get("node_count", 0)),
                "headroom_cpu": 100 - self.safe_float(latest_point.get("cpu_utilization_percentage", 0)),
                "headroom_memory": 100 - self.safe_float(latest_point.get("memory_utilization_percentage", 0))
            }
        
        return capacity_analysis
    
    async def _generate_utilization_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate utilization optimization recommendations"""
        
        recommendations = []
        
        # Based on cluster utilization
        cluster_util = analysis.get("cluster_utilization", {})
        avg_cpu = cluster_util.get("average_cpu_utilization", 0)
        avg_memory = cluster_util.get("average_memory_utilization", 0)
        
        if avg_cpu < self.thresholds["low_utilization_threshold"]:
            recommendations.append(
                f"CPU utilization is low ({avg_cpu:.1f}%) - consider scaling down or consolidating workloads"
            )
        
        if avg_memory < self.thresholds["low_utilization_threshold"]:
            recommendations.append(
                f"Memory utilization is low ({avg_memory:.1f}%) - consider optimizing memory requests"
            )
        
        if avg_cpu > self.thresholds["high_utilization_threshold"]:
            recommendations.append(
                f"CPU utilization is high ({avg_cpu:.1f}%) - consider scaling up or load balancing"
            )
        
        if avg_memory > self.thresholds["high_utilization_threshold"]:
            recommendations.append(
                f"Memory utilization is high ({avg_memory:.1f}%) - consider increasing memory capacity"
            )
        
        # Based on node analysis
        node_analysis = analysis.get("node_utilization", {})
        underutilized_nodes = node_analysis.get("underutilized_nodes", [])
        overutilized_nodes = node_analysis.get("overutilized_nodes", [])
        
        if len(underutilized_nodes) > 0:
            recommendations.append(
                f"{len(underutilized_nodes)} nodes are underutilized - consider consolidation or scaling down"
            )
        
        if len(overutilized_nodes) > 0:
            recommendations.append(
                f"{len(overutilized_nodes)} nodes are overutilized - consider scaling up or load redistribution"
            )
        
        # Based on pod analysis
        pod_analysis = analysis.get("pod_utilization", {})
        inefficient_pods = pod_analysis.get("inefficient_pods", [])
        
        if len(inefficient_pods) > 0:
            recommendations.append(
                f"{len(inefficient_pods)} pods have low efficiency - review resource requests and limits"
            )
        
        # Based on efficiency metrics
        efficiency_metrics = analysis.get("efficiency_metrics", {})
        overall_efficiency = efficiency_metrics.get("overall_efficiency", 0)
        
        if overall_efficiency < self.thresholds["efficiency_target"]:
            recommendations.append(
                f"Overall efficiency is below target ({overall_efficiency:.1f}%) - implement comprehensive optimization"
            )
        
        return recommendations
    
    # Helper methods
    def _calculate_distribution(self, data: List[float]) -> Dict[str, Any]:
        """Calculate distribution statistics"""
        
        if not data:
            return {"quartiles": [], "outliers": 0, "distribution_type": "unknown"}
        
        q1 = np.percentile(data, 25)
        q2 = np.percentile(data, 50)
        q3 = np.percentile(data, 75)
        
        # Simple outlier detection
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = len([x for x in data if x < lower_bound or x > upper_bound])
        
        return {
            "quartiles": [q1, q2, q3],
            "outliers": outliers,
            "distribution_type": "normal" if outliers < len(data) * 0.1 else "skewed"
        }
    
    def _calculate_balance_score(self, utilization: Dict[str, Any]) -> float:
        """Calculate resource balance score"""
        
        cpu_util = utilization.get("average_cpu_utilization", 0)
        memory_util = utilization.get("average_memory_utilization", 0)
        
        if cpu_util == 0 and memory_util == 0:
            return 0.0
        
        # Perfect balance is when CPU and memory utilization are equal
        difference = abs(cpu_util - memory_util)
        max_util = max(cpu_util, memory_util)
        
        if max_util == 0:
            return 100.0
        
        balance_score = max(0, 100 - (difference / max_util) * 100)
        return balance_score
    
    def _calculate_node_efficiency_score(self, cpu_util: float, memory_util: float) -> float:
        """Calculate efficiency score for a node"""
        
        # Ideal utilization is around 70%
        target = self.thresholds["efficiency_target"]
        
        cpu_score = max(0, 100 - abs(cpu_util - target))
        memory_score = max(0, 100 - abs(memory_util - target))
        
        return (cpu_score + memory_score) / 2
    
    def _calculate_trend(self, data: List[float]) -> str:
        """Calculate trend direction"""
        
        if len(data) < 2:
            return "stable"
        
        # Simple trend calculation
        first_half = statistics.mean(data[:len(data)//2])
        second_half = statistics.mean(data[len(data)//2:])
        
        if second_half > first_half * 1.1:
            return "increasing"
        elif second_half < first_half * 0.9:
            return "decreasing"
        else:
            return "stable"
    
    def _calculate_correlation(self, cpu_data: Dict[str, Any], memory_data: Dict[str, Any]) -> float:
        """Calculate correlation between CPU and memory usage"""
        
        # Simplified correlation - in practice would use actual time series data
        cpu_avg = cpu_data.get("average", 0)
        memory_avg = memory_data.get("average", 0)
        
        if cpu_avg == 0 or memory_avg == 0:
            return 0.0
        
        # Simple correlation indicator
        ratio = min(cpu_avg, memory_avg) / max(cpu_avg, memory_avg)
        return ratio * 100  # Convert to percentage
    
    def _calculate_resource_balance(self, cpu_data: Dict[str, Any], memory_data: Dict[str, Any]) -> str:
        """Determine resource balance status"""
        
        cpu_avg = cpu_data.get("average", 0)
        memory_avg = memory_data.get("average", 0)
        
        if abs(cpu_avg - memory_avg) < 10:
            return "balanced"
        elif cpu_avg > memory_avg + 10:
            return "cpu_heavy"
        else:
            return "memory_heavy"
    
    def _analyze_resource_requests(self, pod_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze resource requests vs actual usage"""
        
        request_analysis = {
            "over_requested_pods": [],
            "under_requested_pods": [],
            "optimal_pods": [],
            "request_efficiency": 0.0
        }
        
        total_efficiency = 0.0
        pod_count = 0
        
        for pod_name, pod_data in pod_summary.items():
            cpu_request = pod_data.get("cpu_request_millicores", 0)
            cpu_usage = pod_data.get("cpu_usage_millicores", 0)
            memory_request = pod_data.get("memory_request_mb", 0)
            memory_usage = pod_data.get("memory_usage_mb", 0)
            
            if cpu_request > 0 and memory_request > 0:
                cpu_efficiency = (cpu_usage / cpu_request) * 100
                memory_efficiency = (memory_usage / memory_request) * 100
                overall_efficiency = (cpu_efficiency + memory_efficiency) / 2
                
                total_efficiency += overall_efficiency
                pod_count += 1
                
                if overall_efficiency < 30:
                    request_analysis["over_requested_pods"].append({
                        "pod_name": pod_name,
                        "efficiency": overall_efficiency,
                        "cpu_efficiency": cpu_efficiency,
                        "memory_efficiency": memory_efficiency
                    })
                elif overall_efficiency > 90:
                    request_analysis["under_requested_pods"].append({
                        "pod_name": pod_name,
                        "efficiency": overall_efficiency,
                        "cpu_efficiency": cpu_efficiency,
                        "memory_efficiency": memory_efficiency
                    })
                else:
                    request_analysis["optimal_pods"].append({
                        "pod_name": pod_name,
                        "efficiency": overall_efficiency
                    })
        
        if pod_count > 0:
            request_analysis["request_efficiency"] = total_efficiency / pod_count
        
        return request_analysis
    
    def _calculate_efficiency_consistency(self, cpu_data: List[float], memory_data: List[float]) -> float:
        """Calculate consistency of efficiency over time"""
        
        if not cpu_data or not memory_data:
            return 0.0
        
        # Calculate coefficient of variation for both CPU and memory
        cpu_cv = statistics.stdev(cpu_data) / statistics.mean(cpu_data) if statistics.mean(cpu_data) > 0 else 0
        memory_cv = statistics.stdev(memory_data) / statistics.mean(memory_data) if statistics.mean(memory_data) > 0 else 0
        
        # Lower coefficient of variation means higher consistency
        avg_cv = (cpu_cv + memory_cv) / 2
        consistency_score = max(0, 100 - (avg_cv * 100))
        
        return consistency_score
    
    def _calculate_waste_percentage(self, cpu_data: List[float], memory_data: List[float]) -> float:
        """Calculate percentage of time resources are wasted"""
        
        if not cpu_data or not memory_data:
            return 0.0
        
        waste_threshold = self.thresholds["waste_threshold"]
        
        cpu_waste_count = len([x for x in cpu_data if x < waste_threshold])
        memory_waste_count = len([x for x in memory_data if x < waste_threshold])
        
        total_measurements = len(cpu_data) + len(memory_data)
        waste_measurements = cpu_waste_count + memory_waste_count
        
        return (waste_measurements / total_measurements) * 100 if total_measurements > 0 else 0.0
    
    def _calculate_overprovisioning_indicator(self, cluster_data: Dict[str, Any]) -> float:
        """Calculate overprovisioning indicator"""
        
        # Extract pod resource requests vs usage
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        overprovisioning_score = 0.0
        
        if "pod_resource_requests_limits" in container_insights:
            request_data = container_insights["pod_resource_requests_limits"]
            data_points = request_data.get("data_points", [])
            
            total_pods = len(data_points)
            overprovisioned_pods = 0
            
            for point in data_points:
                cpu_request = self.safe_float(point.get("cpu_request_millicores", 0))
                memory_request = self.safe_float(point.get("memory_request_mb", 0))
                
                # If requests are very high compared to typical usage patterns
                # This is a simplified check - in practice would compare with actual usage
                if cpu_request > 2000 or memory_request > 8192:  # High resource requests
                    overprovisioned_pods += 1
            
            if total_pods > 0:
                overprovisioning_score = (overprovisioned_pods / total_pods) * 100
        
        return overprovisioning_score
    
    def _calculate_overall_efficiency_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall efficiency score"""
        
        resource_efficiency = metrics.get("resource_efficiency", {})
        waste_indicators = metrics.get("waste_indicators", {})
        
        # Base efficiency from resource utilization
        balanced_efficiency = resource_efficiency.get("balanced_efficiency", 0)
        
        # Penalties
        waste_penalty = waste_indicators.get("low_utilization_percentage", 0) * 0.5
        overprovisioning_penalty = waste_indicators.get("overprovisioning_indicator", 0) * 0.3
        
        # Bonus for consistency
        consistency_bonus = resource_efficiency.get("efficiency_consistency", 0) * 0.2
        
        overall_score = balanced_efficiency - waste_penalty - overprovisioning_penalty + consistency_bonus
        return max(0, min(100, overall_score))
    
    def _calculate_namespace_density(self, namespace: str, pod_count: int) -> str:
        """Calculate namespace resource density"""
        
        if pod_count == 0:
            return "empty"
        elif pod_count <= 5:
            return "low"
        elif pod_count <= 20:
            return "medium"
        else:
            return "high"
    
    def _calculate_namespace_efficiency_score(self, pod_success_rate: float, pod_count: int) -> float:
        """Calculate namespace efficiency score"""
        
        # Base score from pod success rate
        base_score = pod_success_rate
        
        # Bonus for managing many pods efficiently
        if pod_count > 10 and pod_success_rate > 90:
            base_score += 10
        
        # Penalty for few pods with low success rate
        if pod_count <= 3 and pod_success_rate < 80:
            base_score -= 20
        
        return max(0, min(100, base_score))
    
    def _calculate_utilization_efficiency_score(self, analysis: Dict[str, Any]) -> float:
        """Calculate overall utilization efficiency score"""
        
        # Extract key metrics
        cluster_util = analysis.get("cluster_utilization", {})
        efficiency_metrics = analysis.get("efficiency_metrics", {})
        node_analysis = analysis.get("node_utilization", {})
        
        # Base score from cluster utilization
        cpu_util = cluster_util.get("average_cpu_utilization", 0)
        memory_util = cluster_util.get("average_memory_utilization", 0)
        target = self.thresholds["efficiency_target"]
        
        cpu_score = max(0, 100 - abs(cpu_util - target))
        memory_score = max(0, 100 - abs(memory_util - target))
        base_score = (cpu_score + memory_score) / 2
        
        # Adjust for efficiency metrics
        overall_efficiency = efficiency_metrics.get("overall_efficiency", 0)
        efficiency_weight = 0.4
        
        # Adjust for node distribution
        underutilized_nodes = len(node_analysis.get("underutilized_nodes", []))
        overutilized_nodes = len(node_analysis.get("overutilized_nodes", []))
        total_nodes = len(node_analysis.get("node_efficiency_scores", {}))
        
        if total_nodes > 0:
            node_imbalance_penalty = ((underutilized_nodes + overutilized_nodes) / total_nodes) * 20
        else:
            node_imbalance_penalty = 0
        
        # Calculate final score
        final_score = (base_score * (1 - efficiency_weight)) + (overall_efficiency * efficiency_weight) - node_imbalance_penalty
        
        return max(0, min(100, final_score))
    
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