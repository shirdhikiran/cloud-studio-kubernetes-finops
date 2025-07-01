"""Simplified and robust Kubernetes resource discovery service."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.kubernetes.k8s_client import KubernetesClient

logger = structlog.get_logger(__name__)


class ResourceDiscoveryService(BaseDiscoveryService):
    """Simplified and robust discovery service for Kubernetes resources."""
    
    def __init__(self, k8s_client: KubernetesClient, config: Dict[str, Any]):
        super().__init__(k8s_client, config)
        self.namespace = config.get("namespace")
        self.include_system_resources = config.get("include_system_resources", False)
        self.include_metrics = config.get("include_metrics", False)
        
        # Get cluster name from client or config
        self.cluster_name = (
            getattr(k8s_client, 'cluster_name', None) or 
            config.get("cluster_name", "unknown")
        )
        self.resource_group = config.get("resource_group", "unknown")
    
    async def discover(self) -> Dict[str, Any]:
        """Discover Kubernetes resources with error handling."""
        self.logger.info("Starting Kubernetes resource discovery")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        # Initialize the discovery result structure
        discovery_result = {
            # 'cluster_name': self.cluster_name,
            # 'resource_group': self.resource_group,
            'discovery_timestamp': datetime.now(timezone.utc).isoformat(),
            'discovery_status': 'in_progress',
            'errors': [],
            'warnings': []
        }
        
        # Discover resources with error handling
        resource_discoveries = [
            ('namespaces', self._discover_namespaces),
            ('nodes', self._discover_nodes),
            ('pods', self._discover_pods),
            ('deployments', self._discover_deployments),
            ('services', self._discover_services)
        ]
        
        # Extended discoveries (optional, may not be supported by all clients)
        extended_discoveries = [
            ('statefulsets', self._discover_statefulsets),
            ('daemonsets', self._discover_daemonsets),
            ('replicasets', self._discover_replicasets),
            ('ingresses', self._discover_ingresses),
            ('persistent_volumes', self._discover_persistent_volumes),
            ('persistent_volume_claims', self._discover_persistent_volume_claims),
            ('config_maps', self._discover_config_maps),
            ('secrets', self._discover_secrets),
            ('jobs', self._discover_jobs),
            ('cronjobs', self._discover_cronjobs),
            ('resource_quotas', self._discover_resource_quotas),
            ('limit_ranges', self._discover_limit_ranges)
        ]
        
        # Discover core resources
        for resource_name, discovery_func in resource_discoveries:
            try:
                discovery_result[resource_name] = await discovery_func()
                self.logger.debug(f"Successfully discovered {resource_name}", count=len(discovery_result[resource_name]))
            except Exception as e:
                self.logger.error(f"Failed to discover {resource_name}", error=str(e))
                discovery_result[resource_name] = []
                discovery_result['errors'].append({
                    'resource_type': resource_name,
                    'error': str(e),
                    'severity': 'high'
                })
        
        # Discover extended resources (with warnings instead of errors)
        for resource_name, discovery_func in extended_discoveries:
            try:
                discovery_result[resource_name] = await discovery_func()
                self.logger.debug(f"Successfully discovered {resource_name}", count=len(discovery_result[resource_name]))
            except AttributeError as e:
                # Client doesn't support this resource type
                self.logger.debug(f"Client doesn't support {resource_name} discovery")
                discovery_result[resource_name] = []
                discovery_result['warnings'].append({
                    'resource_type': resource_name,
                    'warning': f"Client doesn't support {resource_name} discovery",
                    'severity': 'low'
                })
            except Exception as e:
                self.logger.warning(f"Failed to discover {resource_name}", error=str(e))
                discovery_result[resource_name] = []
                discovery_result['warnings'].append({
                    'resource_type': resource_name,
                    'warning': str(e),
                    'severity': 'medium'
                })
        
        # Calculate resource utilization
        discovery_result['resource_utilization'] = self._calculate_resource_utilization(discovery_result)
        
        # Add summary statistics
        discovery_result['summary'] = self._calculate_discovery_summary(discovery_result)
        
        # Update discovery status
        if len(discovery_result['errors']) == 0:
            discovery_result['discovery_status'] = 'completed'
        elif len([r for r in discovery_result.values() if isinstance(r, list) and len(r) > 0]) > 0:
            discovery_result['discovery_status'] = 'partial'
        else:
            discovery_result['discovery_status'] = 'failed'
        
        self.logger.info(
            "Completed Kubernetes resource discovery",
            status=discovery_result['discovery_status'],
            total_resources=discovery_result['summary']['total_resources'],
            errors=len(discovery_result['errors']),
            warnings=len(discovery_result['warnings'])
        )
        
        return discovery_result
    
    async def _discover_namespaces(self) -> List[Dict[str, Any]]:
        """Discover namespaces."""
        namespaces_raw = await self.client.discover_namespaces()
        namespaces = []
        
        for ns in namespaces_raw:
            ns_name = self._safe_extract_value(ns, 'name', str(ns))
            
            # Skip system namespaces if not requested
            if not self.include_system_resources and self._is_system_namespace(ns_name):
                continue
            
            namespace_data = {
                'name': ns_name,
                'status': self._safe_extract_value(ns, 'status', 'Unknown'),
                'created': self._safe_extract_value(ns, 'creation_timestamp'),
                'labels': self._safe_extract_value(ns, 'labels', {}),
                'annotations': self._safe_extract_value(ns, 'annotations', {}),
                'resource_type': 'namespace'
            }
            namespaces.append(namespace_data)
        
        return namespaces
    
    async def _discover_nodes(self) -> List[Dict[str, Any]]:
        """Discover nodes."""
        nodes_raw = await self.client.discover_nodes()
        nodes = []
        
        for node in nodes_raw:
            node_data = {
                'name': self._safe_extract_value(node, 'name', str(node)),
                'status': self._determine_node_status(node),
                'roles': self._determine_node_roles(node),
                'version': self._safe_extract_value(node, 'version', 'Unknown'),
                'instance_type': self._extract_label(node, 'node.kubernetes.io/instance-type', 'Unknown'),
                'zone': self._extract_label(node, 'topology.kubernetes.io/zone', 'Unknown'),
                'capacity': self._safe_extract_value(node, 'capacity', {}),
                'allocatable': self._safe_extract_value(node, 'allocatable', {}),
                'created': self._safe_extract_value(node, 'creation_timestamp'),
                'labels': self._safe_extract_value(node, 'labels', {}),
                'resource_type': 'node'
            }
            nodes.append(node_data)
        
        return nodes
    
    async def _discover_pods(self) -> List[Dict[str, Any]]:
        """Discover pods."""
        pods_raw = await self.client.discover_pods(self.namespace)
        pods = []
        
        for pod in pods_raw:
            pod_namespace = self._safe_extract_value(pod, 'namespace', '')
            
            # Skip system pods if not requested
            if not self.include_system_resources and self._is_system_namespace(pod_namespace):
                continue
            
            pod_data = {
                'name': self._safe_extract_value(pod, 'name', str(pod)),
                'namespace': pod_namespace,
                'status': self._safe_extract_value(pod, 'status', 'Unknown'),
                'node': self._safe_extract_value(pod, 'node_name'),
                'ip': self._safe_extract_value(pod, 'pod_ip'),
                'created': self._safe_extract_value(pod, 'creation_timestamp'),
                'labels': self._safe_extract_value(pod, 'labels', {}),
                'annotations': self._safe_extract_value(pod, 'annotations', {}),
                'containers': self._extract_container_info(pod),
                'resource_requests': self._calculate_pod_resource_requests(pod),
                'resource_limits': self._calculate_pod_resource_limits(pod),
                'resource_type': 'pod'
            }
            pods.append(pod_data)
        
        return pods
    
    async def _discover_deployments(self) -> List[Dict[str, Any]]:
        """Discover deployments."""
        deployments_raw = await self.client.discover_deployments(self.namespace)
        deployments = []
        
        for deployment in deployments_raw:
            deployment_namespace = self._safe_extract_value(deployment, 'namespace', '')
            
            # Skip system deployments if not requested
            if not self.include_system_resources and self._is_system_namespace(deployment_namespace):
                continue
            
            deployment_data = {
                'name': self._safe_extract_value(deployment, 'name', str(deployment)),
                'namespace': deployment_namespace,
                'replicas': self._safe_extract_value(deployment, 'replicas', 0),
                'ready_replicas': self._safe_extract_value(deployment, 'ready_replicas', 0),
                'available_replicas': self._safe_extract_value(deployment, 'available_replicas', 0),
                'created': self._safe_extract_value(deployment, 'creation_timestamp'),
                'labels': self._safe_extract_value(deployment, 'labels', {}),
                'annotations': self._safe_extract_value(deployment, 'annotations', {}),
                'resource_type': 'deployment'
            }
            deployments.append(deployment_data)
        
        return deployments
    
    async def _discover_services(self) -> List[Dict[str, Any]]:
        """Discover services."""
        services_raw = await self.client.discover_services(self.namespace)
        services = []
        
        for service in services_raw:
            service_namespace = self._safe_extract_value(service, 'namespace', '')
            
            # Skip system services if not requested
            if not self.include_system_resources and self._is_system_namespace(service_namespace):
                continue
            
            service_data = {
                'name': self._safe_extract_value(service, 'name', str(service)),
                'namespace': service_namespace,
                'type': self._safe_extract_value(service, 'type', 'Unknown'),
                'cluster_ip': self._safe_extract_value(service, 'cluster_ip'),
                'ports': self._safe_extract_value(service, 'ports', []),
                'selector': self._safe_extract_value(service, 'selector', {}),
                'created': self._safe_extract_value(service, 'creation_timestamp'),
                'labels': self._safe_extract_value(service, 'labels', {}),
                'annotations': self._safe_extract_value(service, 'annotations', {}),
                'resource_type': 'service'
            }
            services.append(service_data)
        
        return services
    
    # Extended discovery methods (may not be supported by all clients)
    async def _discover_statefulsets(self) -> List[Dict[str, Any]]:
        """Discover statefulsets."""
        if not hasattr(self.client, 'discover_statefulsets'):
            raise AttributeError("Client does not support statefulsets discovery")
        
        statefulsets_raw = await self.client.discover_statefulsets(self.namespace)
        return self._process_workload_resources(statefulsets_raw, 'statefulset')
    
    async def _discover_daemonsets(self) -> List[Dict[str, Any]]:
        """Discover daemonsets."""
        if not hasattr(self.client, 'discover_daemonsets'):
            raise AttributeError("Client does not support daemonsets discovery")
        
        daemonsets_raw = await self.client.discover_daemonsets(self.namespace)
        return self._process_workload_resources(daemonsets_raw, 'daemonset')
    
    async def _discover_replicasets(self) -> List[Dict[str, Any]]:
        """Discover replicasets."""
        if not hasattr(self.client, 'discover_replicasets'):
            raise AttributeError("Client does not support replicasets discovery")
        
        replicasets_raw = await self.client.discover_replicasets(self.namespace)
        return self._process_workload_resources(replicasets_raw, 'replicaset')
    
    async def _discover_ingresses(self) -> List[Dict[str, Any]]:
        """Discover ingresses."""
        if not hasattr(self.client, 'discover_ingresses'):
            raise AttributeError("Client does not support ingresses discovery")
        
        ingresses_raw = await self.client.discover_ingresses(self.namespace)
        return self._process_network_resources(ingresses_raw, 'ingress')
    
    async def _discover_persistent_volumes(self) -> List[Dict[str, Any]]:
        """Discover persistent volumes."""
        if not hasattr(self.client, 'discover_persistent_volumes'):
            raise AttributeError("Client does not support persistent volumes discovery")
        
        pvs_raw = await self.client.discover_persistent_volumes()
        return self._process_storage_resources(pvs_raw, 'persistent_volume')
    
    async def _discover_persistent_volume_claims(self) -> List[Dict[str, Any]]:
        """Discover persistent volume claims."""
        if not hasattr(self.client, 'discover_persistent_volume_claims'):
            raise AttributeError("Client does not support persistent volume claims discovery")
        
        pvcs_raw = await self.client.discover_persistent_volume_claims(self.namespace)
        return self._process_storage_resources(pvcs_raw, 'persistent_volume_claim')
    
    async def _discover_config_maps(self) -> List[Dict[str, Any]]:
        """Discover config maps."""
        if not hasattr(self.client, 'discover_config_maps'):
            raise AttributeError("Client does not support config maps discovery")
        
        cms_raw = await self.client.discover_config_maps(self.namespace)
        return self._process_config_resources(cms_raw, 'config_map')
    
    async def _discover_secrets(self) -> List[Dict[str, Any]]:
        """Discover secrets."""
        if not hasattr(self.client, 'discover_secrets'):
            raise AttributeError("Client does not support secrets discovery")
        
        secrets_raw = await self.client.discover_secrets(self.namespace)
        return self._process_config_resources(secrets_raw, 'secret')
    
    async def _discover_jobs(self) -> List[Dict[str, Any]]:
        """Discover jobs."""
        if not hasattr(self.client, 'discover_jobs'):
            raise AttributeError("Client does not support jobs discovery")
        
        jobs_raw = await self.client.discover_jobs(self.namespace)
        return self._process_workload_resources(jobs_raw, 'job')
    
    async def _discover_cronjobs(self) -> List[Dict[str, Any]]:
        """Discover cronjobs."""
        if not hasattr(self.client, 'discover_cronjobs'):
            raise AttributeError("Client does not support cronjobs discovery")
        
        cronjobs_raw = await self.client.discover_cronjobs(self.namespace)
        return self._process_workload_resources(cronjobs_raw, 'cronjob')
    
    async def _discover_resource_quotas(self) -> List[Dict[str, Any]]:
        """Discover resource quotas."""
        if not hasattr(self.client, 'discover_resource_quotas'):
            raise AttributeError("Client does not support resource quotas discovery")
        
        quotas_raw = await self.client.discover_resource_quotas(self.namespace)
        return self._process_policy_resources(quotas_raw, 'resource_quota')
    
    async def _discover_limit_ranges(self) -> List[Dict[str, Any]]:
        """Discover limit ranges."""
        if not hasattr(self.client, 'discover_limit_ranges'):
            raise AttributeError("Client does not support limit ranges discovery")
        
        ranges_raw = await self.client.discover_limit_ranges(self.namespace)
        return self._process_policy_resources(ranges_raw, 'limit_range')
    
    # Helper methods for processing different resource types
    def _process_workload_resources(self, resources: List, resource_type: str) -> List[Dict[str, Any]]:
        """Process workload resources (deployments, statefulsets, etc.)."""
        processed = []
        
        for resource in resources:
            resource_namespace = self._safe_extract_value(resource, 'namespace', '')
            
            if not self.include_system_resources and self._is_system_namespace(resource_namespace):
                continue
            
            resource_data = {
                'name': self._safe_extract_value(resource, 'name', str(resource)),
                'namespace': resource_namespace,
                'replicas': self._safe_extract_value(resource, 'replicas', 0),
                'ready_replicas': self._safe_extract_value(resource, 'ready_replicas', 0),
                'created': self._safe_extract_value(resource, 'creation_timestamp'),
                'labels': self._safe_extract_value(resource, 'labels', {}),
                'annotations': self._safe_extract_value(resource, 'annotations', {}),
                'resource_type': resource_type
            }
            
            # Add resource-specific fields
            if resource_type == 'job':
                resource_data['status'] = {
                    'active': self._safe_extract_value(resource, 'active', 0),
                    'succeeded': self._safe_extract_value(resource, 'succeeded', 0),
                    'failed': self._safe_extract_value(resource, 'failed', 0)
                }
            elif resource_type == 'cronjob':
                resource_data['schedule'] = self._safe_extract_value(resource, 'schedule', '')
                resource_data['suspend'] = self._safe_extract_value(resource, 'suspend', False)
            
            processed.append(resource_data)
        
        return processed
    
    def _process_network_resources(self, resources: List, resource_type: str) -> List[Dict[str, Any]]:
        """Process network resources (services, ingresses, etc.)."""
        processed = []
        
        for resource in resources:
            resource_namespace = self._safe_extract_value(resource, 'namespace', '')
            
            if not self.include_system_resources and self._is_system_namespace(resource_namespace):
                continue
            
            resource_data = {
                'name': self._safe_extract_value(resource, 'name', str(resource)),
                'namespace': resource_namespace,
                'created': self._safe_extract_value(resource, 'creation_timestamp'),
                'labels': self._safe_extract_value(resource, 'labels', {}),
                'annotations': self._safe_extract_value(resource, 'annotations', {}),
                'resource_type': resource_type
            }
            
            # Add resource-specific fields
            if resource_type == 'ingress':
                resource_data['rules'] = self._safe_extract_value(resource, 'rules', [])
                resource_data['tls'] = self._safe_extract_value(resource, 'tls', [])
            
            processed.append(resource_data)
        
        return processed
    
    def _process_storage_resources(self, resources: List, resource_type: str) -> List[Dict[str, Any]]:
        """Process storage resources (PVs, PVCs, etc.)."""
        processed = []
        
        for resource in resources:
            resource_namespace = self._safe_extract_value(resource, 'namespace', '')
            
            # PVs don't have namespaces, but PVCs do
            if resource_namespace and not self.include_system_resources and self._is_system_namespace(resource_namespace):
                continue
            
            resource_data = {
                'name': self._safe_extract_value(resource, 'name', str(resource)),
                'namespace': resource_namespace,
                'capacity': self._safe_extract_value(resource, 'capacity', ''),
                'status': self._safe_extract_value(resource, 'status', 'Unknown'),
                'storage_class': self._safe_extract_value(resource, 'storage_class', ''),
                'created': self._safe_extract_value(resource, 'creation_timestamp'),
                'labels': self._safe_extract_value(resource, 'labels', {}),
                'annotations': self._safe_extract_value(resource, 'annotations', {}),
                'resource_type': resource_type
            }
            
            # Add resource-specific fields
            if resource_type == 'persistent_volume':
                resource_data['reclaim_policy'] = self._safe_extract_value(resource, 'reclaim_policy', '')
                resource_data['access_modes'] = self._safe_extract_value(resource, 'access_modes', [])
            elif resource_type == 'persistent_volume_claim':
                resource_data['volume_name'] = self._safe_extract_value(resource, 'volume_name', '')
                resource_data['requested'] = self._safe_extract_value(resource, 'requested', '')
            
            processed.append(resource_data)
        
        return processed
    
    def _process_config_resources(self, resources: List, resource_type: str) -> List[Dict[str, Any]]:
        """Process configuration resources (ConfigMaps, Secrets, etc.)."""
        processed = []
        
        for resource in resources:
            resource_namespace = self._safe_extract_value(resource, 'namespace', '')
            
            if not self.include_system_resources and self._is_system_namespace(resource_namespace):
                continue
            
            data = self._safe_extract_value(resource, 'data', {})
            
            resource_data = {
                'name': self._safe_extract_value(resource, 'name', str(resource)),
                'namespace': resource_namespace,
                'data_keys': list(data.keys()) if isinstance(data, dict) else [],
                'data_count': len(data) if isinstance(data, dict) else 0,
                'created': self._safe_extract_value(resource, 'creation_timestamp'),
                'labels': self._safe_extract_value(resource, 'labels', {}),
                'annotations': self._safe_extract_value(resource, 'annotations', {}),
                'resource_type': resource_type
            }
            
            # Add resource-specific fields
            if resource_type == 'secret':
                resource_data['type'] = self._safe_extract_value(resource, 'type', 'Opaque')
            
            processed.append(resource_data)
        
        return processed
    
    def _process_policy_resources(self, resources: List, resource_type: str) -> List[Dict[str, Any]]:
        """Process policy resources (ResourceQuotas, LimitRanges, etc.)."""
        processed = []
        
        for resource in resources:
            resource_namespace = self._safe_extract_value(resource, 'namespace', '')
            
            if not self.include_system_resources and self._is_system_namespace(resource_namespace):
                continue
            
            resource_data = {
                'name': self._safe_extract_value(resource, 'name', str(resource)),
                'namespace': resource_namespace,
                'created': self._safe_extract_value(resource, 'creation_timestamp'),
                'labels': self._safe_extract_value(resource, 'labels', {}),
                'annotations': self._safe_extract_value(resource, 'annotations', {}),
                'resource_type': resource_type
            }
            
            # Add resource-specific fields
            if resource_type == 'resource_quota':
                resource_data['hard'] = self._safe_extract_value(resource, 'hard', {})
                resource_data['used'] = self._safe_extract_value(resource, 'used', {})
            elif resource_type == 'limit_range':
                resource_data['limits'] = self._safe_extract_value(resource, 'limits', [])
            
            processed.append(resource_data)
        
        return processed
    
    # Utility methods
    def _safe_extract_value(self, obj, key: str, default=None):
        """Safely extract value from dict or object."""
        if obj is None:
            return default
        
        if isinstance(obj, dict):
            return obj.get(key, default)
        elif hasattr(obj, key):
            value = getattr(obj, key, default)
            return value if value is not None else default
        else:
            return default
    
    def _extract_label(self, obj, label_key: str, default='Unknown') -> str:
        """Extract a specific label value."""
        labels = self._safe_extract_value(obj, 'labels', {})
        if isinstance(labels, dict):
            return labels.get(label_key, default)
        return default
    
    def _determine_node_status(self, node) -> str:
        """Determine node status."""
        conditions = self._safe_extract_value(node, 'conditions', [])
        if isinstance(conditions, list):
            for condition in conditions:
                if isinstance(condition, dict) and condition.get('type') == 'Ready':
                    return 'Ready' if condition.get('status') == 'True' else 'NotReady'
        return 'Unknown'
    
    def _determine_node_roles(self, node) -> List[str]:
        """Determine node roles from labels."""
        labels = self._safe_extract_value(node, 'labels', {})
        roles = []
        
        if isinstance(labels, dict):
            for label_key in labels:
                if label_key.startswith('node-role.kubernetes.io/'):
                    role = label_key.split('/')[-1]
                    roles.append(role)
        
        return roles if roles else ['worker']
    
    def _extract_container_info(self, pod) -> List[Dict[str, Any]]:
        """Extract container information from pod."""
        containers = self._safe_extract_value(pod, 'containers', [])
        container_info = []
        
        if isinstance(containers, list):
            for container in containers:
                info = {
                    'name': self._safe_extract_value(container, 'name', ''),
                    'image': self._safe_extract_value(container, 'image', ''),
                    'ready': self._safe_extract_value(container, 'ready', False),
                    'restart_count': self._safe_extract_value(container, 'restart_count', 0)
                }
                container_info.append(info)
        
        return container_info
    
    def _calculate_pod_resource_requests(self, pod) -> Dict[str, float]:
        """Calculate total resource requests for a pod."""
        containers = self._safe_extract_value(pod, 'containers', [])
        total_requests = {'cpu': 0.0, 'memory': 0.0}
        
        if isinstance(containers, list):
            for container in containers:
                resources = self._safe_extract_value(container, 'resources', {})
                requests = resources.get('requests', {}) if isinstance(resources, dict) else {}
                
                if isinstance(requests, dict):
                    total_requests['cpu'] += self._parse_cpu(requests.get('cpu', '0'))
                    total_requests['memory'] += self._parse_memory(requests.get('memory', '0'))
        
        return total_requests
    
    def _calculate_pod_resource_limits(self, pod) -> Dict[str, float]:
        """Calculate total resource limits for a pod."""
        containers = self._safe_extract_value(pod, 'containers', [])
        total_limits = {'cpu': 0.0, 'memory': 0.0}
        
        if isinstance(containers, list):
            for container in containers:
                resources = self._safe_extract_value(container, 'resources', {})
                limits = resources.get('limits', {}) if isinstance(resources, dict) else {}
                
                if isinstance(limits, dict):
                    total_limits['cpu'] += self._parse_cpu(limits.get('cpu', '0'))
                    total_limits['memory'] += self._parse_memory(limits.get('memory', '0'))
        
        return total_limits
    
    def _parse_cpu(self, cpu_str: str) -> float:
        """Parse CPU value to millicores."""
        if not cpu_str or not isinstance(cpu_str, str):
            return 0.0
        try:
            if cpu_str.endswith('m'):
                return float(cpu_str[:-1])
            return float(cpu_str) * 1000
        except (ValueError, TypeError):
            return 0.0
    
    def _parse_memory(self, memory_str: str) -> float:
        """Parse memory value to bytes."""
        if not memory_str or not isinstance(memory_str, str):
            return 0.0
        try:
            units = {'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3, 'K': 1000, 'M': 1000**2, 'G': 1000**3}
            for unit, multiplier in units.items():
                if memory_str.endswith(unit):
                    return float(memory_str[:-len(unit)]) * multiplier
            return float(memory_str)
        except (ValueError, TypeError):
            return 0.0
    
    def _is_system_namespace(self, namespace: str) -> bool:
        """Check if namespace is a system namespace."""
        if not namespace:
            return False
        
        system_namespaces = {
            'kube-system', 'kube-public', 'kube-node-lease', 
            'azure-system', 'gatekeeper-system'
        }
        return namespace in system_namespaces
    
    def _calculate_resource_utilization(self, discovery_result: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate resource utilization across the cluster."""
        utilization = {
            'nodes': {
                'total': len(discovery_result.get('nodes', [])),
                'ready': sum(1 for n in discovery_result.get('nodes', []) if n.get('status') == 'Ready'),
                'not_ready': sum(1 for n in discovery_result.get('nodes', []) if n.get('status') != 'Ready')
            },
            'pods': {
                'total': len(discovery_result.get('pods', [])),
                'running': sum(1 for p in discovery_result.get('pods', []) if p.get('status') == 'Running'),
                'pending': sum(1 for p in discovery_result.get('pods', []) if p.get('status') == 'Pending'),
                'failed': sum(1 for p in discovery_result.get('pods', []) if p.get('status') == 'Failed'),
                'by_namespace': {}
            },
            'deployments': {
                'total': len(discovery_result.get('deployments', [])),
                'available': sum(1 for d in discovery_result.get('deployments', []) 
                               if d.get('ready_replicas', 0) == d.get('replicas', 0) and d.get('replicas', 0) > 0),
                'degraded': sum(1 for d in discovery_result.get('deployments', []) 
                              if 0 < d.get('ready_replicas', 0) < d.get('replicas', 0)),
                'unavailable': sum(1 for d in discovery_result.get('deployments', []) 
                                 if d.get('ready_replicas', 0) == 0 and d.get('replicas', 0) > 0)
            },
            'resources': {
                'cpu': {'requested': 0.0, 'limits': 0.0, 'allocatable': 0.0},
                'memory': {'requested': 0.0, 'limits': 0.0, 'allocatable': 0.0}
            }
        }
        
        # Calculate pods by namespace
        for pod in discovery_result.get('pods', []):
            ns = pod.get('namespace', 'default')
            if ns not in utilization['pods']['by_namespace']:
                utilization['pods']['by_namespace'][ns] = {'total': 0, 'running': 0, 'pending': 0, 'failed': 0}
            utilization['pods']['by_namespace'][ns]['total'] += 1
            
            status = pod.get('status', 'Unknown')
            if status == 'Running':
                utilization['pods']['by_namespace'][ns]['running'] += 1
            elif status == 'Pending':
                utilization['pods']['by_namespace'][ns]['pending'] += 1
            elif status == 'Failed':
                utilization['pods']['by_namespace'][ns]['failed'] += 1
            
            # Accumulate resource requests and limits
            requests = pod.get('resource_requests', {})
            limits = pod.get('resource_limits', {})
            
            utilization['resources']['cpu']['requested'] += requests.get('cpu', 0)
            utilization['resources']['cpu']['limits'] += limits.get('cpu', 0)
            utilization['resources']['memory']['requested'] += requests.get('memory', 0)
            utilization['resources']['memory']['limits'] += limits.get('memory', 0)
        
        # Calculate allocatable resources from nodes
        for node in discovery_result.get('nodes', []):
            if node.get('status') == 'Ready':
                allocatable = node.get('allocatable', {})
                if isinstance(allocatable, dict):
                    utilization['resources']['cpu']['allocatable'] += self._parse_cpu(allocatable.get('cpu', '0'))
                    utilization['resources']['memory']['allocatable'] += self._parse_memory(allocatable.get('memory', '0'))
        
        # Calculate utilization percentages
        if utilization['resources']['cpu']['allocatable'] > 0:
            utilization['resources']['cpu']['utilization_percent'] = round(
                (utilization['resources']['cpu']['requested'] / utilization['resources']['cpu']['allocatable']) * 100, 2
            )
        
        if utilization['resources']['memory']['allocatable'] > 0:
            utilization['resources']['memory']['utilization_percent'] = round(
                (utilization['resources']['memory']['requested'] / utilization['resources']['memory']['allocatable']) * 100, 2
            )
        
        return utilization
    
    def _calculate_discovery_summary(self, discovery_result: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate summary statistics for the discovery."""
        summary = {
            'total_resources': 0,
            'resources_by_type': {},
            'namespaces_discovered': len(discovery_result.get('namespaces', [])),
            'nodes_discovered': len(discovery_result.get('nodes', [])),
            'workloads_discovered': 0,
            'network_resources_discovered': 0,
            'storage_resources_discovered': 0,
            'config_resources_discovered': 0
        }
        
        # Count resources by type
        resource_types = [
            'namespaces', 'nodes', 'pods', 'deployments', 'services',
            'statefulsets', 'daemonsets', 'replicasets', 'ingresses',
            'persistent_volumes', 'persistent_volume_claims',
            'config_maps', 'secrets', 'jobs', 'cronjobs',
            'resource_quotas', 'limit_ranges'
        ]
        
        for resource_type in resource_types:
            count = len(discovery_result.get(resource_type, []))
            summary['resources_by_type'][resource_type] = count
            summary['total_resources'] += count
            
            # Categorize resources
            if resource_type in ['pods', 'deployments', 'statefulsets', 'daemonsets', 'replicasets', 'jobs', 'cronjobs']:
                summary['workloads_discovered'] += count
            elif resource_type in ['services', 'ingresses']:
                summary['network_resources_discovered'] += count
            elif resource_type in ['persistent_volumes', 'persistent_volume_claims']:
                summary['storage_resources_discovered'] += count
            elif resource_type in ['config_maps', 'secrets', 'resource_quotas', 'limit_ranges']:
                summary['config_resources_discovered'] += count
        
        return summary
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "kubernetes_resources"