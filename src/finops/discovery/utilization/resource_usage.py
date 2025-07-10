# """Resource usage discovery service."""

# from typing import Dict, Any, List
# import structlog

# from finops.discovery.base import BaseDiscoveryService
# from finops.clients.kubernetes.k8s_client import KubernetesClient

# logger = structlog.get_logger(__name__)


# class ResourceUsageService(BaseDiscoveryService):
#     """Service for discovering resource usage patterns."""
    
#     def __init__(self, k8s_client: KubernetesClient, config: Dict[str, Any]):
#         super().__init__(k8s_client, config)
#         self.namespace = config.get("namespace")
#         self.include_system_resources = config.get("include_system_resources", False)
    
#     async def discover(self) -> List[Dict[str, Any]]:
#         """Discover resource usage patterns."""
#         self.logger.info("Starting resource usage discovery")
        
#         if not self.client.is_connected:
#             await self.client.connect()
        
#         usage_data = []
        
#         try:
#             # Get all pods to analyze resource requests and limits
#             pods = await self.client.discover_pods(self.namespace)
            
#             # Analyze resource usage patterns
#             resource_analysis = self._analyze_resource_patterns(pods)
            
#             usage_data.append({
#                 'type': 'resource_usage_analysis',
#                 'namespace': self.namespace or 'all',
#                 'data': resource_analysis,
#                 'total_pods_analyzed': len(pods)
#             })
            
#             # Get deployments for workload analysis
#             deployments = await self.client.discover_deployments(self.namespace)
            
#             workload_analysis = self._analyze_workload_patterns(deployments)
            
#             usage_data.append({
#                 'type': 'workload_analysis',
#                 'namespace': self.namespace or 'all',
#                 'data': workload_analysis,
#                 'total_deployments_analyzed': len(deployments)
#             })
            
#         except Exception as e:
#             self.logger.error("Failed to discover resource usage", error=str(e))
#             raise
        
#         self.logger.info(f"Completed resource usage analysis for {len(usage_data)} categories")
#         return usage_data
    
#     def _analyze_resource_patterns(self, pods: List[Dict[str, Any]]) -> Dict[str, Any]:
#         """Analyze resource request and limit patterns."""
#         analysis = {
#             'total_pods': len(pods),
#             'pods_with_requests': 0,
#             'pods_with_limits': 0,
#             'pods_without_requests': 0,
#             'resource_efficiency': {
#                 'over_requested': 0,
#                 'under_requested': 0,
#                 'well_sized': 0
#             },
#             'resource_totals': {
#                 'cpu_requests_millicores': 0.0,
#                 'memory_requests_mb': 0.0,
#                 'cpu_limits_millicores': 0.0,
#                 'memory_limits_mb': 0.0
#             },
#             'namespace_breakdown': {},
#             'system_vs_user_pods': {
#                 'system_pods': 0,
#                 'user_pods': 0
#             }
#         }
        
#         for pod in pods:
#             namespace = pod.get('namespace', 'unknown')
            
#             # Initialize namespace tracking
#             if namespace not in analysis['namespace_breakdown']:
#                 analysis['namespace_breakdown'][namespace] = {
#                     'pod_count': 0,
#                     'cpu_requests': 0.0,
#                     'memory_requests': 0.0
#                 }
            
#             analysis['namespace_breakdown'][namespace]['pod_count'] += 1
            
#             # Check if system or user pod
#             if self._is_system_pod(pod):
#                 analysis['system_vs_user_pods']['system_pods'] += 1
#             else:
#                 analysis['system_vs_user_pods']['user_pods'] += 1
            
#             # Analyze containers in pod
#             containers = pod.get('containers', [])
#             pod_has_requests = False
#             pod_has_limits = False
            
#             for container in containers:
#                 resources = container.get('resources', {})
#                 requests = resources.get('requests', {})
#                 limits = resources.get('limits', {})
                
#                 if requests:
#                     pod_has_requests = True
#                     # Parse CPU and memory requests
#                     cpu_request = self._parse_cpu_resource(requests.get('cpu', '0'))
#                     memory_request = self._parse_memory_resource(requests.get('memory', '0'))
                    
#                     analysis['resource_totals']['cpu_requests_millicores'] += cpu_request
#                     analysis['resource_totals']['memory_requests_mb'] += memory_request
                    
#                     analysis['namespace_breakdown'][namespace]['cpu_requests'] += cpu_request
#                     analysis['namespace_breakdown'][namespace]['memory_requests'] += memory_request
                
#                 if limits:
#                     pod_has_limits = True
#                     cpu_limit = self._parse_cpu_resource(limits.get('cpu', '0'))
#                     memory_limit = self._parse_memory_resource(limits.get('memory', '0'))
                    
#                     analysis['resource_totals']['cpu_limits_millicores'] += cpu_limit
#                     analysis['resource_totals']['memory_limits_mb'] += memory_limit
            
#             if pod_has_requests:
#                 analysis['pods_with_requests'] += 1
#             else:
#                 analysis['pods_without_requests'] += 1
            
#             if pod_has_limits:
#                 analysis['pods_with_limits'] += 1
        
#         # Calculate efficiency ratios
#         total_pods = analysis['total_pods']
#         if total_pods > 0:
#             analysis['resource_coverage'] = {
#                 'requests_coverage': (analysis['pods_with_requests'] / total_pods) * 100,
#                 'limits_coverage': (analysis['pods_with_limits'] / total_pods) * 100,
#                 'no_requests_percentage': (analysis['pods_without_requests'] / total_pods) * 100
#             }
        
#         return analysis
    
#     def _analyze_workload_patterns(self, deployments: List[Dict[str, Any]]) -> Dict[str, Any]:
#         """Analyze workload scaling and resource patterns."""
#         analysis = {
#             'total_deployments': len(deployments),
#             'scaling_patterns': {
#                 'single_replica': 0,
#                 'small_scale': 0,  # 2-5 replicas
#                 'medium_scale': 0,  # 6-20 replicas
#                 'large_scale': 0   # 20+ replicas
#             },
#             'availability_patterns': {
#                 'fully_available': 0,
#                 'partially_available': 0,
#                 'unavailable': 0
#             },
#             'resource_distribution': {
#                 'by_namespace': {},
#                 'by_strategy': {}
#             }
#         }
        
#         for deployment in deployments:
#             replicas = deployment.get('replicas', 0)
#             ready_replicas = deployment.get('ready_replicas', 0)
#             namespace = deployment.get('namespace', 'unknown')
#             strategy = deployment.get('strategy', {}).get('type', 'unknown')
            
#             # Analyze scaling patterns
#             if replicas == 1:
#                 analysis['scaling_patterns']['single_replica'] += 1
#             elif 2 <= replicas <= 5:
#                 analysis['scaling_patterns']['small_scale'] += 1
#             elif 6 <= replicas <= 20:
#                 analysis['scaling_patterns']['medium_scale'] += 1
#             else:
#                 analysis['scaling_patterns']['large_scale'] += 1
            
#             # Analyze availability
#             if ready_replicas == replicas and replicas > 0:
#                 analysis['availability_patterns']['fully_available'] += 1
#             elif ready_replicas > 0:
#                 analysis['availability_patterns']['partially_available'] += 1
#             else:
#                 analysis['availability_patterns']['unavailable'] += 1
            
#             # Track by namespace
#             if namespace not in analysis['resource_distribution']['by_namespace']:
#                 analysis['resource_distribution']['by_namespace'][namespace] = {
#                     'deployment_count': 0,
#                     'total_replicas': 0
#                 }
            
#             analysis['resource_distribution']['by_namespace'][namespace]['deployment_count'] += 1
#             analysis['resource_distribution']['by_namespace'][namespace]['total_replicas'] += replicas
            
#             # Track by strategy
#             if strategy not in analysis['resource_distribution']['by_strategy']:
#                 analysis['resource_distribution']['by_strategy'][strategy] = 0
#             analysis['resource_distribution']['by_strategy'][strategy] += 1
        
#         return analysis
    
#     def _is_system_pod(self, pod: Dict[str, Any]) -> bool:
#         """Check if pod is a system pod."""
#         namespace = pod.get('namespace', '')
#         system_namespaces = {
#             'kube-system', 'kube-public', 'kube-node-lease',
#             'azure-system', 'gatekeeper-system'
#         }
#         return namespace in system_namespaces
    
#     def _parse_cpu_resource(self, cpu_str: str) -> float:
#         """Parse CPU resource string to millicores."""
#         if not cpu_str or cpu_str == '0':
#             return 0.0
        
#         cpu_str = cpu_str.strip()
#         if cpu_str.endswith('m'):
#             return float(cpu_str[:-1])
#         else:
#             return float(cpu_str) * 1000
    
#     def _parse_memory_resource(self, memory_str: str) -> float:
#         """Parse memory resource string to MB."""
#         if not memory_str or memory_str == '0':
#             return 0.0
        
#         memory_str = memory_str.strip().upper()
        
#         # Convert to MB
#         if memory_str.endswith('MI'):
#             return float(memory_str[:-2])
#         elif memory_str.endswith('GI'):
#             return float(memory_str[:-2]) * 1024
#         elif memory_str.endswith('KI'):
#             return float(memory_str[:-2]) / 1024
#         elif memory_str.endswith('M'):
#             return float(memory_str[:-1])
#         elif memory_str.endswith('G'):
#             return float(memory_str[:-1]) * 1000
#         elif memory_str.endswith('K'):
#             return float(memory_str[:-1]) / 1000
#         else:
#             # Assume bytes
#             try:
#                 return float(memory_str) / (1024 * 1024)
#             except ValueError:
#                 return 0.0
    
#     def get_discovery_type(self) -> str:
#         """Get discovery type."""
#         return "resource_usage"