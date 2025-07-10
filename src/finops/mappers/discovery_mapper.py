"""
Discovery Service Integration with Data Models
Integration layer to convert discovery service outputs to structured data models
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.discovery.orchestrator import DiscoveryOrchestrator
from finops.discovery.infrastructure.aks_discovery import AKSDiscoveryService
from finops.discovery.infrastructure.node_pool_discovery import NodePoolDiscoveryService
from finops.discovery.cost.cost_discovery import CostDiscoveryService
from finops.discovery.kubernetes.resource_discovery import ResourceDiscoveryService

# Import the data models
from finops.models.discovery_models import (
    DiscoveryInfo, ClusterInfo, NodePoolInfo, NodeInfo, NamespaceInfo,
    WorkloadInfo, PodInfo, ServiceInfo, StorageInfo, IngressInfo,
    NetworkResourceInfo, AzureResourceInfo, ContainerInfo, CostData,
    ResourceMetrics, ResourceRequests, DiscoveryTotals, ResourceStatus,
    NodePoolMode, ServiceType, VolumeType
)

logger = structlog.get_logger(__name__)


class DiscoveryDataMapper:
    """Maps raw discovery results to structured data models."""
    
    def __init__(self):
        self.logger = logger.bind(component="discovery_mapper")
    
    def map_discovery_results(self, raw_results: Dict[str, Any]) -> DiscoveryInfo:
        """Map raw discovery results to DiscoveryInfo model."""
        discovery = DiscoveryInfo()
        
        try:
            # Extract discovery data
            discovery_data = raw_results.get('discovery_data', {})
            
            # Map AKS clusters
            clusters_data = discovery_data.get('aks_clusters', {})
            if clusters_data.get('status') == 'success':
                for cluster_raw in clusters_data.get('data', []):
                    cluster = self._map_cluster_info(cluster_raw)
                    
                    # Enrich cluster with additional data
                    self._enrich_cluster_with_node_pools(cluster, discovery_data)
                    self._enrich_cluster_with_kubernetes_resources(cluster, discovery_data)
                    self._enrich_cluster_with_cost_data(cluster, discovery_data)
                    self._enrich_cluster_with_network_resources(cluster, discovery_data)
                    self._enrich_cluster_with_azure_resources(cluster, discovery_data)
                    
                    discovery.add_cluster(cluster)
            
            # Update totals metadata
            self._update_discovery_metadata(discovery, raw_results)
            
        except Exception as e:
            self.logger.error("Failed to map discovery results", error=str(e))
            raise
        
        return discovery
    
    def _map_cluster_info(self, cluster_raw: Dict[str, Any]) -> ClusterInfo:
        """Map raw cluster data to ClusterInfo model."""
        return ClusterInfo(
            id=cluster_raw.get('id', ''),
            name=cluster_raw.get('name', ''),
            resource_group=cluster_raw.get('resource_group', ''),
            location=cluster_raw.get('location', ''),
            kubernetes_version=cluster_raw.get('kubernetes_version', 'Unknown'),
            provisioning_state=cluster_raw.get('provisioning_state', 'Unknown'),
            fqdn=cluster_raw.get('fqdn'),
            node_resource_group=cluster_raw.get('node_resource_group'),
            dns_prefix=cluster_raw.get('dns_prefix'),
            enable_rbac=cluster_raw.get('enable_rbac', True),
            network_profile=cluster_raw.get('network_profile', {}),
            addon_profiles=cluster_raw.get('addon_profiles', {}),
            sku=cluster_raw.get('sku', {}),
            created_time=self._parse_datetime(cluster_raw.get('created_time')),
            last_modified=self._parse_datetime(cluster_raw.get('last_modified')),
            tags=cluster_raw.get('tags', {})
        )
    
    def _enrich_cluster_with_node_pools(self, cluster: ClusterInfo, discovery_data: Dict[str, Any]):
        """Enrich cluster with node pool data."""
        node_pools_data = discovery_data.get('node_pools', {})
        if node_pools_data.get('status') == 'success':
            for pool_raw in node_pools_data.get('data', []):
                if pool_raw.get('cluster_name') == cluster.name:
                    node_pool = self._map_node_pool_info(pool_raw)
                    cluster.node_pools.append(node_pool)
    
    def _map_node_pool_info(self, pool_raw: Dict[str, Any]) -> NodePoolInfo:
        """Map raw node pool data to NodePoolInfo model."""
        return NodePoolInfo(
            name=pool_raw.get('name', ''),
            cluster_name=pool_raw.get('cluster_name', ''),
            vm_size=pool_raw.get('vm_size', ''),
            os_type=pool_raw.get('os_type', 'Linux'),
            os_disk_size_gb=pool_raw.get('os_disk_size_gb', 0),
            count=pool_raw.get('count', 0),
            min_count=pool_raw.get('min_count'),
            max_count=pool_raw.get('max_count'),
            auto_scaling_enabled=pool_raw.get('auto_scaling_enabled', False),
            node_taints=pool_raw.get('node_taints', []),
            node_labels=pool_raw.get('node_labels', {}),
            availability_zones=pool_raw.get('availability_zones', []),
            mode=NodePoolMode(pool_raw.get('mode', 'User')),
            orchestrator_version=pool_raw.get('orchestrator_version', 'Unknown'),
            provisioning_state=pool_raw.get('provisioning_state', 'Unknown'),
            power_state=pool_raw.get('power_state', 'Unknown'),
            max_pods=pool_raw.get('max_pods', 110),
            os_disk_type=pool_raw.get('os_disk_type', 'Managed'),
            scale_set_priority=pool_raw.get('scale_set_priority'),
            spot_max_price=pool_raw.get('spot_max_price'),
            upgrade_settings=pool_raw.get('upgrade_settings', {}),
            tags=pool_raw.get('tags', {})
        )
    
    def _enrich_cluster_with_kubernetes_resources(self, cluster: ClusterInfo, discovery_data: Dict[str, Any]):
        """Enrich cluster with Kubernetes resources."""
        k8s_resources = discovery_data.get('kubernetes_resources', {})
        if k8s_resources.get('status') == 'success':
            k8s_data = k8s_resources.get('data', {})
            
            # Map namespaces
            namespaces_raw = k8s_data.get('namespaces', [])
            for ns_raw in namespaces_raw:
                namespace = self._map_namespace_info(ns_raw)
                cluster.namespaces.append(namespace)
            
            # Map nodes
            nodes_raw = k8s_data.get('nodes', [])
            for node_raw in nodes_raw:
                node = self._map_node_info(node_raw)
                cluster.nodes.append(node)
                
                # Associate node with node pool
                for node_pool in cluster.node_pools:
                    if self._node_belongs_to_pool(node, node_pool):
                        node_pool.nodes.append(node)
                        break
            
            # Map workloads
            workloads_raw = k8s_data.get('deployments', [])
            for workload_raw in workloads_raw:
                workload = self._map_workload_info(workload_raw, 'Deployment')
                cluster.workloads.append(workload)
                
                # Associate workload with namespace
                for namespace in cluster.namespaces:
                    if namespace.name == workload.namespace:
                        namespace.workloads.append(workload)
                        break
            
            # Map pods
            pods_raw = k8s_data.get('pods', [])
            for pod_raw in pods_raw:
                pod = self._map_pod_info(pod_raw)
                
                # Associate pod with namespace
                for namespace in cluster.namespaces:
                    if namespace.name == pod.namespace:
                        namespace.pods.append(pod)
                        break
                
                # Associate pod with node
                for node in cluster.nodes:
                    if node.name == pod.node:
                        node.pods.append(pod)
                        break
                
                # Associate pod with workload
                for workload in cluster.workloads:
                    if self._pod_belongs_to_workload(pod, workload):
                        workload.pods.append(pod)
                        break
            
            # Map services
            services_raw = k8s_data.get('services', [])
            for service_raw in services_raw:
                service = self._map_service_info(service_raw)
                cluster.services.append(service)
                
                # Associate service with namespace
                for namespace in cluster.namespaces:
                    if namespace.name == service.namespace:
                        namespace.services.append(service)
                        break
            
            # Map storage
            storage_raw = k8s_data.get('persistent_volumes', [])
            for pv_raw in storage_raw:
                storage = self._map_storage_info(pv_raw)
                cluster.storage.append(storage)
            
            # Map ingresses
            ingresses_raw = k8s_data.get('ingresses', [])
            for ingress_raw in ingresses_raw:
                ingress = self._map_ingress_info(ingress_raw)
                cluster.ingresses.append(ingress)
                
                # Associate ingress with namespace
                for namespace in cluster.namespaces:
                    if namespace.name == ingress.namespace:
                        namespace.ingresses.append(ingress)
                        break
    
    def _map_namespace_info(self, ns_raw: Dict[str, Any]) -> NamespaceInfo:
        """Map raw namespace data to NamespaceInfo model."""
        return NamespaceInfo(
            name=ns_raw.get('name', ''),
            status=ResourceStatus(ns_raw.get('status', 'Unknown').lower()),
            created=self._parse_datetime(ns_raw.get('created')),
            labels=ns_raw.get('labels', {}),
            annotations=ns_raw.get('annotations', {})
        )
    
    def _map_node_info(self, node_raw: Dict[str, Any]) -> NodeInfo:
        """Map raw node data to NodeInfo model."""
        return NodeInfo(
            name=node_raw.get('name', ''),
            status=ResourceStatus.ACTIVE if node_raw.get('status') == 'Ready' else ResourceStatus.INACTIVE,
            roles=node_raw.get('roles', []),
            version=node_raw.get('version', 'Unknown'),
            os=node_raw.get('os', 'Unknown'),
            kernel=node_raw.get('kernel', 'Unknown'),
            container_runtime=node_raw.get('container_runtime', 'Unknown'),
            instance_type=node_raw.get('instance_type', 'Unknown'),
            zone=node_raw.get('zone', 'Unknown'),
            capacity=node_raw.get('capacity', {}),
            allocatable=node_raw.get('allocatable', {}),
            conditions=node_raw.get('conditions', []),
            created=self._parse_datetime(node_raw.get('created')),
            labels=node_raw.get('labels', {}),
            annotations=node_raw.get('annotations', {}),
            taints=node_raw.get('taints', [])
        )
    
    def _map_workload_info(self, workload_raw: Dict[str, Any], kind: str) -> WorkloadInfo:
        """Map raw workload data to WorkloadInfo model."""
        return WorkloadInfo(
            name=workload_raw.get('name', ''),
            namespace=workload_raw.get('namespace', ''),
            kind=kind,
            replicas=workload_raw.get('replicas', 0),
            ready_replicas=workload_raw.get('ready_replicas', 0),
            available_replicas=workload_raw.get('available_replicas', 0),
            updated_replicas=workload_raw.get('updated_replicas', 0),
            created=self._parse_datetime(workload_raw.get('created')),
            labels=workload_raw.get('labels', {}),
            annotations=workload_raw.get('annotations', {}),
            selector=workload_raw.get('selector', {}),
            strategy=workload_raw.get('strategy', {}),
            conditions=workload_raw.get('conditions', [])
        )
    
    def _map_pod_info(self, pod_raw: Dict[str, Any]) -> PodInfo:
        """Map raw pod data to PodInfo model."""
        # Map containers
        containers = []
        for container_raw in pod_raw.get('containers', []):
            container = ContainerInfo(
                name=container_raw.get('name', ''),
                image=container_raw.get('image', ''),
                ready=container_raw.get('ready', False),
                restart_count=container_raw.get('restart_count', 0),
                resources=self._map_resource_requests(container_raw.get('resources', {})),
                status=container_raw.get('status', 'Unknown')
            )
            containers.append(container)
        
        return PodInfo(
            name=pod_raw.get('name', ''),
            namespace=pod_raw.get('namespace', ''),
            status=ResourceStatus(pod_raw.get('status', 'Unknown').lower()),
            node=pod_raw.get('node'),
            ip=pod_raw.get('ip'),
            host_ip=pod_raw.get('host_ip'),
            created=self._parse_datetime(pod_raw.get('created')),
            started=self._parse_datetime(pod_raw.get('started')),
            labels=pod_raw.get('labels', {}),
            annotations=pod_raw.get('annotations', {}),
            containers=containers,
            resource_requests=self._map_resource_requests(pod_raw.get('resource_requests', {})),
            resource_limits=self._map_resource_requests(pod_raw.get('resource_limits', {})),
            owner_references=pod_raw.get('owner_references', []),
            conditions=pod_raw.get('conditions', []),
            qos_class=pod_raw.get('qos_class', 'BestEffort'),
            phase=pod_raw.get('phase', 'Unknown')
        )
    
    def _map_service_info(self, service_raw: Dict[str, Any]) -> ServiceInfo:
        """Map raw service data to ServiceInfo model."""
        return ServiceInfo(
            name=service_raw.get('name', ''),
            namespace=service_raw.get('namespace', ''),
            type=ServiceType(service_raw.get('type', 'ClusterIP')),
            cluster_ip=service_raw.get('cluster_ip'),
            external_ips=service_raw.get('external_ips', []),
            ports=service_raw.get('ports', []),
            selector=service_raw.get('selector', {}),
            created=self._parse_datetime(service_raw.get('created')),
            labels=service_raw.get('labels', {}),
            annotations=service_raw.get('annotations', {}),
            load_balancer_ingress=service_raw.get('load_balancer_ingress', [])
        )
    
    def _map_storage_info(self, storage_raw: Dict[str, Any]) -> StorageInfo:
        """Map raw storage data to StorageInfo model."""
        return StorageInfo(
            name=storage_raw.get('name', ''),
            namespace=storage_raw.get('namespace'),
            type=VolumeType.PERSISTENT_VOLUME,
            capacity=storage_raw.get('capacity', '0'),
            status=ResourceStatus(storage_raw.get('status', 'Unknown').lower()),
            storage_class=storage_raw.get('storage_class'),
            access_modes=storage_raw.get('access_modes', []),
            reclaim_policy=storage_raw.get('reclaim_policy'),
            mount_options=storage_raw.get('mount_options', []),
            created=self._parse_datetime(storage_raw.get('created')),
            labels=storage_raw.get('labels', {}),
            annotations=storage_raw.get('annotations', {}),
            claim_ref=storage_raw.get('claim_ref'),
            source=storage_raw.get('source', {})
        )
    
    def _map_ingress_info(self, ingress_raw: Dict[str, Any]) -> IngressInfo:
        """Map raw ingress data to IngressInfo model."""
        return IngressInfo(
            name=ingress_raw.get('name', ''),
            namespace=ingress_raw.get('namespace', ''),
            ingress_class=ingress_raw.get('ingress_class_name'),
            rules=ingress_raw.get('rules', []),
            tls=ingress_raw.get('tls', []),
            load_balancer_ingress=ingress_raw.get('load_balancer_ingress', []),
            created=self._parse_datetime(ingress_raw.get('created')),
            labels=ingress_raw.get('labels', {}),
            annotations=ingress_raw.get('annotations', {}),
            backend_services=self._extract_backend_services(ingress_raw.get('rules', []))
        )
    
    def _map_resource_requests(self, resources_raw: Dict[str, Any]) -> ResourceRequests:
        """Map raw resource data to ResourceRequests model."""
        return ResourceRequests(
            cpu_request=self._parse_cpu_value(resources_raw.get('cpu', '0')),
            memory_request=self._parse_memory_value(resources_raw.get('memory', '0')),
            storage_request=self._parse_memory_value(resources_raw.get('storage', '0')),
            cpu_limit=self._parse_cpu_value(resources_raw.get('cpu_limit', '0')),
            memory_limit=self._parse_memory_value(resources_raw.get('memory_limit', '0')),
            storage_limit=self._parse_memory_value(resources_raw.get('storage_limit', '0'))
        )
    
    def _enrich_cluster_with_cost_data(self, cluster: ClusterInfo, discovery_data: Dict[str, Any]):
        """Enrich cluster with cost data."""
        cost_data = discovery_data.get('cost_analysis', {})
        if cost_data.get('status') == 'success':
            cost_raw = cost_data.get('data', {})
            
            # Map cluster costs
            cluster_costs = cost_raw.get('cluster_costs', {})
            cluster.raw_cost_data = self._map_cost_data(cluster_costs)
            
            # Distribute costs to node pools (simplified allocation)
            self._allocate_costs_to_node_pools(cluster, cluster.raw_cost_data)
            
            # Allocate costs to namespaces based on resource usage
            self._allocate_costs_to_namespaces(cluster, cluster.raw_cost_data)
    
    def _map_cost_data(self, cost_raw: Dict[str, Any]) -> CostData:
        """Map raw cost data to CostData model."""
        return CostData(
            total=cost_raw.get('total', 0.0),
            compute=cost_raw.get('compute', 0.0),
            storage=cost_raw.get('storage', 0.0),
            network=cost_raw.get('network', 0.0),
            monitoring=cost_raw.get('monitoring', 0.0),
            other=cost_raw.get('other', 0.0),
            currency=cost_raw.get('currency', 'USD'),
            daily_breakdown=cost_raw.get('daily_breakdown', []),
            by_service=cost_raw.get('by_service', {}),
            cost_trend=self._determine_cost_trend(cost_raw.get('daily_breakdown', []))
        )
    
    def _enrich_cluster_with_network_resources(self, cluster: ClusterInfo, discovery_data: Dict[str, Any]):
        """Enrich cluster with network resources."""
        network_data = discovery_data.get('network_resources', {})
        if network_data.get('status') == 'success':
            for net_raw in network_data.get('data', []):
                network_resource = NetworkResourceInfo(
                    name=net_raw.get('name', ''),
                    resource_group=net_raw.get('resource_group', ''),
                    type=net_raw.get('resource_type', ''),
                    location=net_raw.get('location', ''),
                    sku=net_raw.get('sku'),
                    ip_address=net_raw.get('ip_address'),
                    allocation_method=net_raw.get('allocation_method'),
                    tags=net_raw.get('tags', {}),
                    configuration=net_raw.get('configuration', {})
                )
                cluster.network_resources.append(network_resource)
    
    def _enrich_cluster_with_azure_resources(self, cluster: ClusterInfo, discovery_data: Dict[str, Any]):
        """Enrich cluster with Azure resources."""
        storage_data = discovery_data.get('storage_resources', {})
        if storage_data.get('status') == 'success':
            for storage_raw in storage_data.get('data', []):
                azure_resource = AzureResourceInfo(
                    name=storage_raw.get('name', ''),
                    resource_group=storage_raw.get('resource_group', ''),
                    type=storage_raw.get('resource_type', ''),
                    location=storage_raw.get('location', ''),
                    sku=storage_raw.get('sku'),
                    tags=storage_raw.get('tags', {}),
                    properties=storage_raw
                )
                cluster.azure_resources.append(azure_resource)
    
    def _allocate_costs_to_node_pools(self, cluster: ClusterInfo, total_cost: CostData):
        """Allocate costs to node pools based on node count and VM size."""
        if not cluster.node_pools:
            return
        
        # Simple allocation based on node count
        total_nodes = sum(pool.count for pool in cluster.node_pools)
        if total_nodes == 0:
            return
        
        for node_pool in cluster.node_pools:
            allocation_ratio = node_pool.count / total_nodes
            node_pool.cost_data = CostData(
                total=total_cost.total * allocation_ratio,
                compute=total_cost.compute * allocation_ratio,
                storage=total_cost.storage * allocation_ratio,
                network=total_cost.network * allocation_ratio,
                monitoring=total_cost.monitoring * allocation_ratio,
                other=total_cost.other * allocation_ratio,
                currency=total_cost.currency,
                allocation_percentage=allocation_ratio * 100
            )
    
    def _allocate_costs_to_namespaces(self, cluster: ClusterInfo, total_cost: CostData):
        """Allocate costs to namespaces based on resource usage."""
        if not cluster.namespaces:
            return
        
        # Calculate total resource requests across all namespaces
        total_cpu_requests = sum(
            sum(pod.resource_requests.cpu_request for pod in ns.pods)
            for ns in cluster.namespaces
        )
        total_memory_requests = sum(
            sum(pod.resource_requests.memory_request for pod in ns.pods)
            for ns in cluster.namespaces
        )
        
        if total_cpu_requests == 0 and total_memory_requests == 0:
            return
        
        for namespace in cluster.namespaces:
            ns_cpu_requests = sum(pod.resource_requests.cpu_request for pod in namespace.pods)
            ns_memory_requests = sum(pod.resource_requests.memory_request for pod in namespace.pods)
            
            # Calculate allocation ratio (weighted average of CPU and memory)
            cpu_ratio = ns_cpu_requests / total_cpu_requests if total_cpu_requests > 0 else 0
            memory_ratio = ns_memory_requests / total_memory_requests if total_memory_requests > 0 else 0
            allocation_ratio = (cpu_ratio + memory_ratio) / 2
            
            namespace.cost_data = CostData(
                total=total_cost.total * allocation_ratio,
                compute=total_cost.compute * allocation_ratio,
                storage=total_cost.storage * allocation_ratio,
                network=total_cost.network * allocation_ratio,
                monitoring=total_cost.monitoring * allocation_ratio,
                other=total_cost.other * allocation_ratio,
                currency=total_cost.currency,
                allocation_percentage=allocation_ratio * 100
            )
    
    def _update_discovery_metadata(self, discovery: DiscoveryInfo, raw_results: Dict[str, Any]):
        """Update discovery metadata from raw results."""
        orchestrator_metadata = raw_results.get('orchestrator_metadata', {})
        summary = raw_results.get('summary', {})
        
        discovery.totals.discovery_timestamp = self._parse_datetime(
            orchestrator_metadata.get('start_time')
        ) or datetime.now(timezone.utc)
        
        discovery.totals.discovery_duration_seconds = orchestrator_metadata.get('duration_seconds', 0.0)
        discovery.totals.successful_discoveries = orchestrator_metadata.get('successful_services', 0)
        discovery.totals.failed_discoveries = orchestrator_metadata.get('failed_services', 0)
        discovery.totals.errors = summary.get('errors', [])
        
        # Update discovery metadata
        discovery.metadata = {
            'orchestrator_metadata': orchestrator_metadata,
            'summary': summary,
            'discovery_types': summary.get('discovery_types', [])
        }
    
    # Helper methods
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        if not dt_str:
            return None
        
        try:
            # Try ISO format first
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except:
            try:
                # Try standard format
                return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S.%f')
            except:
                return None
    
    def _parse_cpu_value(self, cpu_str: str) -> float:
        """Parse CPU value to millicores."""
        if not cpu_str or cpu_str == '0':
            return 0.0
        
        cpu_str = cpu_str.strip()
        if cpu_str.endswith('m'):
            return float(cpu_str[:-1])
        else:
            return float(cpu_str) * 1000
    
    def _parse_memory_value(self, memory_str: str) -> float:
        """Parse memory value to bytes."""
        if not memory_str or memory_str == '0':
            return 0.0
        
        memory_str = memory_str.strip().upper()
        units = {
            'KI': 1024, 'MI': 1024**2, 'GI': 1024**3, 'TI': 1024**4,
            'K': 1000, 'M': 1000**2, 'G': 1000**3, 'T': 1000**4
        }
        
        for unit, multiplier in units.items():
            if memory_str.endswith(unit):
                return float(memory_str[:-len(unit)]) * multiplier
        
        try:
            return float(memory_str)
        except ValueError:
            return 0.0
    
    def _node_belongs_to_pool(self, node: NodeInfo, node_pool: NodePoolInfo) -> bool:
        """Check if a node belongs to a node pool."""
        # Simple heuristic: check if node name contains pool name
        return node_pool.name in node.name or any(
            label_key.startswith('agentpool') and label_value == node_pool.name
            for label_key, label_value in node.labels.items()
        )
    
    def _pod_belongs_to_workload(self, pod: PodInfo, workload: WorkloadInfo) -> bool:
        """Check if a pod belongs to a workload."""
        # Check owner references
        for owner_ref in pod.owner_references:
            if owner_ref.get('name') == workload.name:
                return True
        
        # Check labels
        for label_key, label_value in pod.labels.items():
            if label_key in workload.selector and workload.selector[label_key] == label_value:
                return True
        
        return False
    
    def _extract_backend_services(self, rules: List[Dict[str, Any]]) -> List[str]:
        """Extract backend services from ingress rules."""
        services = []
        for rule in rules:
            paths = rule.get('paths', [])
            for path in paths:
                backend = path.get('backend', {})
                service_name = backend.get('service_name')
                if service_name:
                    services.append(service_name)
        return services
    
    def _determine_cost_trend(self, daily_breakdown: List[Dict[str, Any]]) -> str:
        """Determine cost trend from daily breakdown."""
        if len(daily_breakdown) < 2:
            return "stable"
        
        # Compare first and last day
        first_day_cost = daily_breakdown[0].get('cost', 0)
        last_day_cost = daily_breakdown[-1].get('cost', 0)
        
        if last_day_cost > first_day_cost * 1.1:
            return "increasing"
        elif last_day_cost < first_day_cost * 0.9:
            return "decreasing"
        else:
            return "stable"

