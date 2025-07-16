# src/finops/analytics/benchmarking_analytics.py
"""
Benchmarking Analytics Module - Compare performance against industry standards
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import structlog
import statistics

logger = structlog.get_logger(__name__)


class BenchmarkingAnalytics:
    """Advanced benchmarking analytics for industry comparison"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(module="benchmarking_analytics")
        
        # Industry benchmarks (these would typically come from external data sources)
        self.industry_benchmarks = {
            'cost_per_pod_per_month': {'p50': 15.0, 'p75': 25.0, 'p90': 40.0},
            'cost_per_cpu_core_per_month': {'p50': 35.0, 'p75': 50.0, 'p90': 75.0},
            'cost_per_gb_memory_per_month': {'p50': 8.0, 'p75': 12.0, 'p90': 18.0},
            'cpu_utilization_percentage': {'p50': 45.0, 'p75': 65.0, 'p90': 80.0},
            'memory_utilization_percentage': {'p50': 50.0, 'p75': 70.0, 'p90': 85.0},
            'waste_percentage': {'p10': 5.0, 'p25': 15.0, 'p50': 25.0},
            'efficiency_score': {'p25': 60.0, 'p50': 75.0, 'p75': 85.0},
            'pods_per_node': {'p50': 30.0, 'p75': 50.0, 'p90': 70.0},
            'container_density': {'p50': 15.0, 'p75': 25.0, 'p90': 35.0}
        }
        
        # Best practices thresholds
        self.best_practices = {
            'target_cpu_utilization': 70.0,
            'target_memory_utilization': 75.0,
            'max_acceptable_waste': 20.0,
            'min_efficiency_score': 70.0,
            'optimal_pods_per_node': 40.0
        }
    
    async def analyze_cluster_benchmarking(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive benchmarking analysis for a cluster"""
        
        cluster_info = cluster_data.get("cluster_info", {})
        cluster_name = cluster_info.get("name", "unknown")
        
        self.logger.info(f"Starting benchmarking analysis for cluster: {cluster_name}")
        
        analysis = {
            "cluster_name": cluster_name,
            "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
            "cost_benchmarks": {},
            "utilization_benchmarks": {},
            "efficiency_benchmarks": {},
            "density_benchmarks": {},
            "performance_benchmarks": {},
            "industry_comparison": {},
            "best_practices_assessment": {},
            "benchmarking_score": 0.0,
            "improvement_opportunities": []
        }
        
        try:
            # 1. Cost benchmarking
            analysis["cost_benchmarks"] = await self._benchmark_costs(cluster_data)
            
            # 2. Utilization benchmarking
            analysis["utilization_benchmarks"] = await self._benchmark_utilization(cluster_data)
            
            # 3. Efficiency benchmarking
            analysis["efficiency_benchmarks"] = await self._benchmark_efficiency(cluster_data)
            
            # 4. Density benchmarking
            analysis["density_benchmarks"] = await self._benchmark_density(cluster_data)
            
            # 5. Performance benchmarking
            analysis["performance_benchmarks"] = await self._benchmark_performance(cluster_data)
            
            # 6. Industry comparison
            analysis["industry_comparison"] = await self._compare_to_industry(analysis)
            
            # 7. Best practices assessment
            analysis["best_practices_assessment"] = await self._assess_best_practices(analysis)
            
            # 8. Calculate overall benchmarking score
            analysis["benchmarking_score"] = self._calculate_benchmarking_score(analysis)
            
            # 9. Identify improvement opportunities
            analysis["improvement_opportunities"] = await self._identify_improvement_opportunities(analysis)
            
            self.logger.info(
                f"Benchmarking analysis completed for {cluster_name}",
                benchmarking_score=analysis["benchmarking_score"],
                improvement_opportunities=len(analysis["improvement_opportunities"])
            )
            
        except Exception as e:
            self.logger.error(f"Benchmarking analysis failed for {cluster_name}: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    async def _benchmark_costs(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Benchmark cost metrics against industry standards"""
        
        cost_benchmarks = {
            "cost_per_pod": {},
            "cost_per_cpu_core": {},
            "cost_per_gb_memory": {},
            "total_cost_efficiency": {},
            "cost_percentile_ranking": {}
        }
        
        # Extract cost data
        detailed_costs = cluster_data.get("detailed_costs", {})
        total_cost = detailed_costs.get("summary", {}).get("total_cost", 0.0)
        
        # Extract cluster metrics
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        total_pods = len(k8s_resources.get("pods", []))
        
        # Calculate cluster capacity
        node_pools = cluster_data.get("node_pools", [])
        total_cpu_cores = 0
        total_memory_gb = 0
        
        for pool in node_pools:
            vm_size = pool.get("vm_size", "")
            node_count = pool.get("count", 0)
            cpu_cores, memory_gb = self._parse_vm_specs(vm_size)
            total_cpu_cores += cpu_cores * node_count
            total_memory_gb += memory_gb * node_count
        
        # Calculate cost metrics
        if total_pods > 0:
            cost_per_pod = total_cost / total_pods
            cost_benchmarks["cost_per_pod"] = self._compare_to_benchmark(
                cost_per_pod, "cost_per_pod_per_month"
            )
        
        if total_cpu_cores > 0:
            cost_per_cpu_core = total_cost / total_cpu_cores
            cost_benchmarks["cost_per_cpu_core"] = self._compare_to_benchmark(
                cost_per_cpu_core, "cost_per_cpu_core_per_month"
            )
        
        if total_memory_gb > 0:
            cost_per_gb_memory = total_cost / total_memory_gb
            cost_benchmarks["cost_per_gb_memory"] = self._compare_to_benchmark(
                cost_per_gb_memory, "cost_per_gb_memory_per_month"
            )
        
        return cost_benchmarks
    
    async def _benchmark_utilization(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Benchmark utilization metrics"""
        
        utilization_benchmarks = {
            "cpu_utilization": {},
            "memory_utilization": {},
            "storage_utilization": {},
            "overall_utilization_score": 0.0
        }
        
        # Extract utilization data from metrics
        raw_metrics = cluster_data.get("raw_metrics", {})
        container_insights = raw_metrics.get("container_insights_data", {})
        
        # CPU utilization benchmarking
        if "cluster_capacity_summary" in container_insights:
            capacity_data = container_insights["cluster_capacity_summary"]
            cpu_utilization_data = []
            memory_utilization_data = []
            
            for point in capacity_data.get("data_points", []):
                cpu_util = point.get("cpu_utilization_percentage", 0)
                memory_util = point.get("memory_utilization_percentage", 0)
                
                if cpu_util > 0:
                    cpu_utilization_data.append(cpu_util)
                if memory_util > 0:
                    memory_utilization_data.append(memory_util)
            
            if cpu_utilization_data:
                avg_cpu_util = statistics.mean(cpu_utilization_data)
                utilization_benchmarks["cpu_utilization"] = self._compare_to_benchmark(
                    avg_cpu_util, "cpu_utilization_percentage"
                )
            
            if memory_utilization_data:
                avg_memory_util = statistics.mean(memory_utilization_data)
                utilization_benchmarks["memory_utilization"] = self._compare_to_benchmark(
                    avg_memory_util, "memory_utilization_percentage"
                )
        
        # Calculate overall utilization score
        cpu_score = utilization_benchmarks.get("cpu_utilization", {}).get("performance_score", 0)
        memory_score = utilization_benchmarks.get("memory_utilization", {}).get("performance_score", 0)
        
        if cpu_score > 0 and memory_score > 0:
            utilization_benchmarks["overall_utilization_score"] = (cpu_score + memory_score) / 2
        
        return utilization_benchmarks
    
    async def _benchmark_efficiency(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Benchmark efficiency metrics"""
        
        efficiency_benchmarks = {
            "resource_efficiency": {},
            "waste_percentage": {},
            "cost_efficiency": {},
            "overall_efficiency_score": 0.0
        }
        
        # Calculate waste percentage
        # This would typically come from waste analytics
        estimated_waste_percentage = 20.0  # Placeholder
        
        efficiency_benchmarks["waste_percentage"] = self._compare_to_benchmark(
            estimated_waste_percentage, "waste_percentage", lower_is_better=True
        )
        
        # Calculate efficiency score
        # This would typically come from utilization analytics
        estimated_efficiency_score = 65.0  # Placeholder
        
        efficiency_benchmarks["resource_efficiency"] = self._compare_to_benchmark(
            estimated_efficiency_score, "efficiency_score"
        )
        
        return efficiency_benchmarks
    
    async def _benchmark_density(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Benchmark density metrics"""
        
        density_benchmarks = {
            "pods_per_node": {},
            "container_density": {},
            "resource_density": {},
            "density_optimization_score": 0.0
        }
        
        # Calculate pods per node
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        total_pods = len(k8s_resources.get("pods", []))
        
        node_pools = cluster_data.get("node_pools", [])
        total_nodes = sum(pool.get("count", 0) for pool in node_pools)
        
        if total_nodes > 0:
            pods_per_node = total_pods / total_nodes
            density_benchmarks["pods_per_node"] = self._compare_to_benchmark(
                pods_per_node, "pods_per_node"
            )
        
        # Calculate container density
        total_containers = 0
        pods = k8s_resources.get("pods", [])
        for pod in pods:
            total_containers += len(pod.get("containers", []))
        
        if total_nodes > 0:
            containers_per_node = total_containers / total_nodes
            density_benchmarks["container_density"] = self._compare_to_benchmark(
                containers_per_node, "container_density"
            )
        
        return density_benchmarks
    
    async def _benchmark_performance(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Benchmark performance metrics"""
        
        performance_benchmarks = {
            "pod_health_score": 0.0,
            "deployment_success_rate": 0.0,
            "resource_allocation_efficiency": 0.0,
            "overall_performance_score": 0.0
        }
        
        # Calculate pod health score
        k8s_resources = cluster_data.get("kubernetes_resources", {})
        pods = k8s_resources.get("pods", [])
        
        if pods:
            healthy_pods = len([pod for pod in pods if pod.get("status") == "Running"])
            pod_health_score = (healthy_pods / len(pods)) * 100
            performance_benchmarks["pod_health_score"] = pod_health_score
        
        # Calculate deployment success rate
        deployments = k8s_resources.get("deployments", [])
        
        if deployments:
            successful_deployments = 0
            for deployment in deployments:
                replicas = deployment.get("replicas", 0)
                ready_replicas = deployment.get("ready_replicas", 0)
                if replicas > 0 and ready_replicas == replicas:
                    successful_deployments += 1
            
            deployment_success_rate = (successful_deployments / len(deployments)) * 100
            performance_benchmarks["deployment_success_rate"] = deployment_success_rate
        
        # Calculate overall performance score
        scores = [
            performance_benchmarks["pod_health_score"],
            performance_benchmarks["deployment_success_rate"]
        ]
        
        if scores:
            performance_benchmarks["overall_performance_score"] = statistics.mean([s for s in scores if s > 0])
        
        return performance_benchmarks
    
    async def _compare_to_industry(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Compare cluster metrics to industry standards"""
        
        industry_comparison = {
            "overall_industry_ranking": "",
            "strong_areas": [],
            "improvement_areas": [],
            "industry_position": {},
            "peer_comparison": {}
        }
        
        # Collect all benchmark scores
        benchmark_scores = []
        
        # Cost benchmarks
        cost_benchmarks = analysis.get("cost_benchmarks", {})
        for metric, benchmark in cost_benchmarks.items():
            if isinstance(benchmark, dict) and "performance_score" in benchmark:
                benchmark_scores.append(benchmark["performance_score"])
        
        # Utilization benchmarks
        utilization_benchmarks = analysis.get("utilization_benchmarks", {})
        overall_util_score = utilization_benchmarks.get("overall_utilization_score", 0)
        if overall_util_score > 0:
            benchmark_scores.append(overall_util_score)
        
        # Efficiency benchmarks
        efficiency_benchmarks = analysis.get("efficiency_benchmarks", {})
        for metric, benchmark in efficiency_benchmarks.items():
            if isinstance(benchmark, dict) and "performance_score" in benchmark:
                benchmark_scores.append(benchmark["performance_score"])
        
        # Calculate overall industry ranking
        if benchmark_scores:
            avg_score = statistics.mean(benchmark_scores)
            
            if avg_score >= 85:
                industry_comparison["overall_industry_ranking"] = "Top 10%"
            elif avg_score >= 75:
                industry_comparison["overall_industry_ranking"] = "Top 25%"
            elif avg_score >= 50:
                industry_comparison["overall_industry_ranking"] = "Average"
            elif avg_score >= 25:
                industry_comparison["overall_industry_ranking"] = "Below Average"
            else:
                industry_comparison["overall_industry_ranking"] = "Bottom 25%"
        
        # Identify strong areas and improvement areas
        for category, benchmarks in [
            ("cost", cost_benchmarks),
            ("utilization", utilization_benchmarks),
            ("efficiency", efficiency_benchmarks)
        ]:
            category_scores = []
            for metric, benchmark in benchmarks.items():
                if isinstance(benchmark, dict) and "performance_score" in benchmark:
                    category_scores.append(benchmark["performance_score"])
            
            if category_scores:
                avg_category_score = statistics.mean(category_scores)
                if avg_category_score >= 75:
                    industry_comparison["strong_areas"].append({
                        "area": category,
                        "score": avg_category_score,
                        "description": f"Performing well in {category} metrics"
                    })
                elif avg_category_score < 50:
                    industry_comparison["improvement_areas"].append({
                        "area": category,
                        "score": avg_category_score,
                        "description": f"Below average performance in {category} metrics"
                    })
        
        return industry_comparison
    
    async def _assess_best_practices(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Assess adherence to best practices"""
        
        best_practices_assessment = {
            "overall_best_practices_score": 0.0,
            "best_practices_compliance": {},
            "recommendations": [],
            "compliance_gaps": []
        }
        
        compliance_scores = []
        
        # CPU utilization best practice
        utilization_benchmarks = analysis.get("utilization_benchmarks", {})
        cpu_benchmark = utilization_benchmarks.get("cpu_utilization", {})
        cpu_value = cpu_benchmark.get("actual_value", 0)
        
        target_cpu = self.best_practices["target_cpu_utilization"]
        cpu_compliance = max(0, 100 - abs(cpu_value - target_cpu))
        compliance_scores.append(cpu_compliance)
        
        best_practices_assessment["best_practices_compliance"]["cpu_utilization"] = {
            "score": cpu_compliance,
            "target": target_cpu,
            "actual": cpu_value,
            "compliant": abs(cpu_value - target_cpu) <= 10
        }
        
        # Memory utilization best practice
        memory_benchmark = utilization_benchmarks.get("memory_utilization", {})
        memory_value = memory_benchmark.get("actual_value", 0)
        
        target_memory = self.best_practices["target_memory_utilization"]
        memory_compliance = max(0, 100 - abs(memory_value - target_memory))
        compliance_scores.append(memory_compliance)
        
        best_practices_assessment["best_practices_compliance"]["memory_utilization"] = {
            "score": memory_compliance,
            "target": target_memory,
            "actual": memory_value,
            "compliant": abs(memory_value - target_memory) <= 10
        }
        
        # Efficiency best practice
        efficiency_benchmarks = analysis.get("efficiency_benchmarks", {})
        efficiency_benchmark = efficiency_benchmarks.get("resource_efficiency", {})
        efficiency_value = efficiency_benchmark.get("actual_value", 0)
        
        min_efficiency = self.best_practices["min_efficiency_score"]
        efficiency_compliance = min(100, (efficiency_value / min_efficiency) * 100)
        compliance_scores.append(efficiency_compliance)
        
        best_practices_assessment["best_practices_compliance"]["efficiency_score"] = {
            "score": efficiency_compliance,
            "target": min_efficiency,
            "actual": efficiency_value,
            "compliant": efficiency_value >= min_efficiency
        }
        
        # Calculate overall best practices score
        if compliance_scores:
            best_practices_assessment["overall_best_practices_score"] = statistics.mean(compliance_scores)
        
        # Generate recommendations
        for practice, compliance in best_practices_assessment["best_practices_compliance"].items():
            if not compliance["compliant"]:
                best_practices_assessment["recommendations"].append(
                    f"Improve {practice}: target {compliance['target']}, current {compliance['actual']:.1f}"
                )
        
        return best_practices_assessment
    
    async def _identify_improvement_opportunities(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify specific improvement opportunities"""
        
        opportunities = []
        
        # Cost improvement opportunities
        cost_benchmarks = analysis.get("cost_benchmarks", {})
        for metric, benchmark in cost_benchmarks.items():
            if isinstance(benchmark, dict):
                performance_score = benchmark.get("performance_score", 0)
                if performance_score < 50:  # Below average
                    opportunities.append({
                        "area": "cost_optimization",
                        "metric": metric,
                        "current_score": performance_score,
                        "improvement_potential": "high",
                        "recommendation": f"Optimize {metric} - currently below industry average",
                        "priority": "high" if performance_score < 25 else "medium"
                    })
        
        # Utilization improvement opportunities
        utilization_benchmarks = analysis.get("utilization_benchmarks", {})
        overall_util_score = utilization_benchmarks.get("overall_utilization_score", 0)
        
        if overall_util_score < 60:
            opportunities.append({
                "area": "utilization_optimization",
                "metric": "overall_utilization",
                "current_score": overall_util_score,
                "improvement_potential": "high",
                "recommendation": "Improve resource utilization through right-sizing and optimization",
                "priority": "high"
            })
        
        # Efficiency improvement opportunities
        efficiency_benchmarks = analysis.get("efficiency_benchmarks", {})
        for metric, benchmark in efficiency_benchmarks.items():
            if isinstance(benchmark, dict):
                performance_score = benchmark.get("performance_score", 0)
                if performance_score < 60:
                    opportunities.append({
                        "area": "efficiency_optimization",
                        "metric": metric,
                        "current_score": performance_score,
                        "improvement_potential": "medium",
                        "recommendation": f"Improve {metric} through waste reduction and optimization",
                        "priority": "medium"
                    })
        
        # Sort by priority and score
        priority_order = {"high": 3, "medium": 2, "low": 1}
        opportunities.sort(key=lambda x: (priority_order.get(x["priority"], 0), -x["current_score"]), reverse=True)
        
        return opportunities[:10]  # Top 10 opportunities
    
    def _calculate_benchmarking_score(self, analysis: Dict[str, Any]) -> float:
        """Calculate overall benchmarking score"""
        
        scores = []
        
        # Cost benchmarking scores
        cost_benchmarks = analysis.get("cost_benchmarks", {})
        for benchmark in cost_benchmarks.values():
            if isinstance(benchmark, dict) and "performance_score" in benchmark:
                scores.append(benchmark["performance_score"])
        
        # Utilization benchmarking scores
        utilization_benchmarks = analysis.get("utilization_benchmarks", {})
        util_score = utilization_benchmarks.get("overall_utilization_score", 0)
        if util_score > 0:
            scores.append(util_score)
        
        # Efficiency benchmarking scores
        efficiency_benchmarks = analysis.get("efficiency_benchmarks", {})
        for benchmark in efficiency_benchmarks.values():
            if isinstance(benchmark, dict) and "performance_score" in benchmark:
                scores.append(benchmark["performance_score"])
        
        # Performance benchmarking scores
        performance_benchmarks = analysis.get("performance_benchmarks", {})
        perf_score = performance_benchmarks.get("overall_performance_score", 0)
        if perf_score > 0:
            scores.append(perf_score)
        
        # Calculate weighted average
        if scores:
            return statistics.mean(scores)
        
        return 0.0
    
    def _compare_to_benchmark(self, actual_value: float, benchmark_key: str, 
                             lower_is_better: bool = False) -> Dict[str, Any]:
        """Compare actual value to industry benchmark"""
        
        benchmark = self.industry_benchmarks.get(benchmark_key, {})
        
        if not benchmark:
            return {
                "actual_value": actual_value,
                "benchmark_not_available": True,
                "performance_score": 50  # Neutral score
            }
        
        p50 = benchmark.get("p50", 0)
        p75 = benchmark.get("p75", 0)
        p90 = benchmark.get("p90", 0)
        
        # Calculate performance score
        if lower_is_better:
            # For metrics where lower is better (like waste percentage)
            if actual_value <= benchmark.get("p10", 0):
                performance_score = 95
                percentile_ranking = "Top 10%"
            elif actual_value <= benchmark.get("p25", p50 * 0.5):
                performance_score = 85
                percentile_ranking = "Top 25%"
            elif actual_value <= p50:
                performance_score = 70
                percentile_ranking = "Above Average"
            elif actual_value <= p75:
                performance_score = 50
                percentile_ranking = "Average"
            else:
                performance_score = 25
                percentile_ranking = "Below Average"
        else:
            # For metrics where higher is better
            if actual_value >= p90:
                performance_score = 95
                percentile_ranking = "Top 10%"
            elif actual_value >= p75:
                performance_score = 85
                percentile_ranking = "Top 25%"
            elif actual_value >= p50:
                performance_score = 70
                percentile_ranking = "Above Average"
            elif actual_value >= benchmark.get("p25", p50 * 0.75):
                performance_score = 50
                percentile_ranking = "Average"
            else:
                performance_score = 25
                percentile_ranking = "Below Average"
        
        return {
            "actual_value": actual_value,
            "industry_p50": p50,
            "industry_p75": p75,
            "industry_p90": p90,
            "performance_score": performance_score,
            "percentile_ranking": percentile_ranking,
            "variance_from_median": ((actual_value - p50) / p50) * 100 if p50 > 0 else 0
        }
    
    def _parse_vm_specs(self, vm_size: str) -> Tuple[int, int]:
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