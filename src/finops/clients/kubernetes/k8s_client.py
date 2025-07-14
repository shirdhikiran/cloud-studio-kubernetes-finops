# src/finops/clients/kubernetes/k8s_client.py
"""Complete Kubernetes client with comprehensive resource discovery."""

from typing import Dict, Any, List, Optional
import yaml
import structlog
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from finops.core.base_client import BaseClient
from finops.core.exceptions import ClientConnectionException, DiscoveryException
from finops.core.utils import retry_with_backoff

logger = structlog.get_logger(__name__)


class KubernetesClient(BaseClient):
    """Complete Kubernetes client for comprehensive resource discovery."""
    
    def __init__(self, 
                 config_dict: Dict[str, Any],
                 kubeconfig_path: Optional[str] = None,
                 context: Optional[str] = None,
                 kubeconfig_data: Optional[bytes] = None,
                 cluster_name: Optional[str] = None):
        super().__init__(config_dict, "KubernetesClient")
        self.kubeconfig_path = kubeconfig_path
        self.context = context
        self.kubeconfig_data = kubeconfig_data
        self.namespace = config_dict.get("namespace", "default")
        self.cluster_name = cluster_name or config_dict.get("cluster_name", "unknown")
        
        # API clients
        self.v1 = None
        self.apps_v1 = None
        self.networking_v1 = None
        self.batch_v1 = None
        self.storage_v1 = None
    
    async def connect(self) -> None:
        """Connect to Kubernetes cluster."""
        try:
            if self.kubeconfig_data:
                # Load from provided kubeconfig data
                kubeconfig_dict = yaml.safe_load(self.kubeconfig_data.decode('utf-8'))
                config.load_kube_config_from_dict(kubeconfig_dict, context=self.context)
                self.logger.info("Loaded kubeconfig from provided data")
            elif self.kubeconfig_path:
                # Load from file path
                config.load_kube_config(config_file=self.kubeconfig_path, context=self.context)
                self.logger.info(f"Loaded kubeconfig from {self.kubeconfig_path}")
            else:
                # Try default kubeconfig location
                config.load_kube_config(context=self.context)
                self.logger.info("Loaded default kubeconfig")
            
            # Initialize API clients
            self.v1 = client.CoreV1Api()
            self.apps_v1 = client.AppsV1Api()
            self.networking_v1 = client.NetworkingV1Api()
            self.batch_v1 = client.BatchV1Api()
            self.storage_v1 = client.StorageV1Api()
            
            self._connected = True
            self.logger.info(f"Kubernetes client connected to cluster: {self.cluster_name}")
            
        except Exception as e:
            raise ClientConnectionException("Kubernetes", f"Connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from Kubernetes cluster."""
        self._connected = False
        self.logger.info("Kubernetes client disconnected")
    
    async def health_check(self) -> bool:
        """Check Kubernetes client health."""
        try:
            if not self._connected or not self.v1:
                return False
            self.v1.get_code()
            return True
        except Exception as e:
            self.logger.warning("Kubernetes health check failed", error=str(e))
            return False
    
    @retry_with_backoff(max_retries=3)
    async def discover_all_resources(self) -> Dict[str, Any]:
        """Discover all Kubernetes resources."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            resources = {
                'namespaces': await self.discover_namespaces(),
                'nodes': await self.discover_nodes(),
                'pods': await self.discover_pods(),
                'deployments': await self.discover_deployments(),
                'services': await self.discover_services(),
                'persistent_volumes': await self.discover_persistent_volumes(),
                'persistent_volume_claims': await self.discover_persistent_volume_claims(),
                'ingresses': await self.discover_ingresses(),
                'config_maps': await self.discover_config_maps(),
                'secrets': await self.discover_secrets(),
                'resource_quotas': await self.discover_resource_quotas(),
                'limit_ranges': await self.discover_limit_ranges(),
                'events': await self.discover_events()
            }
            
            # Calculate summary
            resources['summary'] = {
                'total_namespaces': len(resources['namespaces']),
                'total_nodes': len(resources['nodes']),
                'total_pods': len(resources['pods']),
                'total_deployments': len(resources['deployments']),
                'total_services': len(resources['services']),
                'total_pvs': len(resources['persistent_volumes']),
                'total_pvcs': len(resources['persistent_volume_claims']),
                'total_ingresses': len(resources['ingresses']),
                'total_events': len(resources['events']),
                'cluster_name': self.cluster_name
            }
            
            self.logger.info(
                f"Discovered all Kubernetes resources for {self.cluster_name}",
                **{k: v for k, v in resources['summary'].items() if k.startswith('total_')}
            )
            
            return resources
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover resources: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_namespaces(self) -> List[Dict[str, Any]]:
        """Discover namespaces."""
        try:
            namespaces = []
            for ns in self.v1.list_namespace().items:
                ns_data = {
                    'name': ns.metadata.name,
                    'status': ns.status.phase,
                    'created': ns.metadata.creation_timestamp.isoformat() if ns.metadata.creation_timestamp else None,
                    'labels': ns.metadata.labels or {},
                    'annotations': ns.metadata.annotations or {},
                    'uid': ns.metadata.uid
                }
                namespaces.append(ns_data)
            
            self.logger.info(f"Discovered {len(namespaces)} namespaces")
            return namespaces
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover namespaces: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_nodes(self) -> List[Dict[str, Any]]:
        """Discover cluster nodes."""
        try:
            nodes = []
            for node in self.v1.list_node().items:
                node_data = {
                    'name': node.metadata.name,
                    'status': self._get_node_status(node),
                    'roles': self._get_node_roles(node),
                    'version': node.status.node_info.kubelet_version,
                    'os': node.status.node_info.os_image,
                    'kernel': node.status.node_info.kernel_version,
                    'container_runtime': node.status.node_info.container_runtime_version,
                    'instance_type': node.metadata.labels.get('beta.kubernetes.io/instance-type', 
                                                             node.metadata.labels.get('node.kubernetes.io/instance-type', 'Unknown')),
                    'zone': node.metadata.labels.get('topology.kubernetes.io/zone', 
                                                    node.metadata.labels.get('failure-domain.beta.kubernetes.io/zone', 'Unknown')),
                    'capacity': {
                        'cpu': node.status.capacity.get('cpu'),
                        'memory': node.status.capacity.get('memory'),
                        'pods': node.status.capacity.get('pods'),
                        'storage': node.status.capacity.get('ephemeral-storage')
                    },
                    'allocatable': {
                        'cpu': node.status.allocatable.get('cpu'),
                        'memory': node.status.allocatable.get('memory'),
                        'pods': node.status.allocatable.get('pods'),
                        'storage': node.status.allocatable.get('ephemeral-storage')
                    },
                    'conditions': [{'type': cond.type, 'status': cond.status, 'reason': cond.reason} 
                                  for cond in node.status.conditions],
                    'created': node.metadata.creation_timestamp.isoformat() if node.metadata.creation_timestamp else None,
                    'labels': node.metadata.labels or {},
                    'annotations': node.metadata.annotations or {},
                    'taints': [{'key': taint.key, 'value': taint.value, 'effect': taint.effect} 
                              for taint in (node.spec.taints or [])],
                    'uid': node.metadata.uid
                }
                nodes.append(node_data)
            
            self.logger.info(f"Discovered {len(nodes)} nodes")
            return nodes
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover nodes: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_pods(self) -> List[Dict[str, Any]]:
        """Discover pods - FIXED status detection only."""
        try:
            pods = []
            pod_list = self.v1.list_pod_for_all_namespaces()
            
            for pod in pod_list.items:
                # FIX: Use the correct pod status field
                pod_status = pod.status.phase if pod.status and pod.status.phase else "Unknown"
                
                pod_data = {
                    'name': pod.metadata.name,
                    'namespace': pod.metadata.namespace,
                    'status': pod_status,  # FIXED: This now correctly shows Running/Pending/Failed/Succeeded
                    'node': pod.spec.node_name,
                    'ip': pod.status.pod_ip,
                    'host_ip': pod.status.host_ip,
                    'restart_policy': pod.spec.restart_policy,
                    'service_account': pod.spec.service_account_name,
                    'containers': [
                        {
                            'name': container.name,
                            'image': container.image,
                            'ready': self._is_container_ready(container.name, pod.status.container_statuses),
                            'restart_count': self._get_container_restart_count(container.name, pod.status.container_statuses),
                            'resources': {
                                'requests': container.resources.requests if container.resources else {},
                                'limits': container.resources.limits if container.resources else {}
                            }
                        }
                        for container in (pod.spec.containers or [])
                    ],
                    'conditions': [{'type': cond.type, 'status': cond.status, 'reason': cond.reason} 
                                  for cond in (pod.status.conditions or [])],
                    'created': pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None,
                    'started': pod.status.start_time.isoformat() if pod.status.start_time else None,
                    'labels': pod.metadata.labels or {},
                    'annotations': pod.metadata.annotations or {},
                    'owner_references': [{'kind': ref.kind, 'name': ref.name, 'controller': ref.controller} 
                                       for ref in (pod.metadata.owner_references or [])],
                    'uid': pod.metadata.uid
                }
                pods.append(pod_data)
            
            self.logger.info(f"Discovered {len(pods)} pods")
            return pods
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover pods: {e}")

    
    @retry_with_backoff(max_retries=3)
    async def discover_deployments(self) -> List[Dict[str, Any]]:
        """Discover deployments."""
        try:
            deployments = []
            deployment_list = self.apps_v1.list_deployment_for_all_namespaces()
            
            for deployment in deployment_list.items:
                deployment_data = {
                    'name': deployment.metadata.name,
                    'namespace': deployment.metadata.namespace,
                    'replicas': deployment.spec.replicas,
                    'ready_replicas': deployment.status.ready_replicas or 0,
                    'available_replicas': deployment.status.available_replicas or 0,
                    'updated_replicas': deployment.status.updated_replicas or 0,
                    'strategy': {
                        'type': deployment.spec.strategy.type if deployment.spec.strategy else None
                    },
                    'created': deployment.metadata.creation_timestamp.isoformat() if deployment.metadata.creation_timestamp else None,
                    'labels': deployment.metadata.labels or {},
                    'annotations': deployment.metadata.annotations or {},
                    'selector': deployment.spec.selector.match_labels if deployment.spec.selector else {},
                    'conditions': [{'type': cond.type, 'status': cond.status, 'reason': cond.reason} 
                                  for cond in (deployment.status.conditions or [])],
                    'uid': deployment.metadata.uid
                }
                deployments.append(deployment_data)
            
            self.logger.info(f"Discovered {len(deployments)} deployments")
            return deployments
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover deployments: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_services(self) -> List[Dict[str, Any]]:
        """Discover services."""
        try:
            services = []
            service_list = self.v1.list_service_for_all_namespaces()
            
            for service in service_list.items:
                service_data = {
                    'name': service.metadata.name,
                    'namespace': service.metadata.namespace,
                    'type': service.spec.type,
                    'cluster_ip': service.spec.cluster_ip,
                    'external_ips': service.spec.external_i_ps or [],
                    'ports': [{'name': p.name, 'port': p.port, 'target_port': p.target_port, 
                              'protocol': p.protocol, 'node_port': p.node_port} 
                             for p in (service.spec.ports or [])],
                    'selector': service.spec.selector or {},
                    'created': service.metadata.creation_timestamp.isoformat() if service.metadata.creation_timestamp else None,
                    'labels': service.metadata.labels or {},
                    'annotations': service.metadata.annotations or {},
                    'load_balancer_ingress': [
                        {'ip': ing.ip, 'hostname': ing.hostname}
                        for ing in (service.status.load_balancer.ingress or [])
                    ] if service.status.load_balancer else [],
                    'uid': service.metadata.uid
                }
                services.append(service_data)
            
            self.logger.info(f"Discovered {len(services)} services")
            return services
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover services: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_persistent_volumes(self) -> List[Dict[str, Any]]:
        """Discover persistent volumes."""
        try:
            pvs = []
            pv_list = self.v1.list_persistent_volume()
            
            for pv in pv_list.items:
                pv_data = {
                    'name': pv.metadata.name,
                    'capacity': pv.spec.capacity.get('storage') if pv.spec.capacity else None,
                    'access_modes': pv.spec.access_modes or [],
                    'reclaim_policy': pv.spec.persistent_volume_reclaim_policy,
                    'status': pv.status.phase,
                    'claim': f"{pv.spec.claim_ref.namespace}/{pv.spec.claim_ref.name}" if pv.spec.claim_ref else None,
                    'storage_class': pv.spec.storage_class_name,
                    'volume_mode': pv.spec.volume_mode,
                    'mount_options': pv.spec.mount_options or [],
                    'created': pv.metadata.creation_timestamp.isoformat() if pv.metadata.creation_timestamp else None,
                    'labels': pv.metadata.labels or {},
                    'annotations': pv.metadata.annotations or {},
                    'source': self._get_pv_source(pv),
                    'uid': pv.metadata.uid
                }
                pvs.append(pv_data)
            
            self.logger.info(f"Discovered {len(pvs)} persistent volumes")
            return pvs
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover persistent volumes: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_persistent_volume_claims(self) -> List[Dict[str, Any]]:
        """Discover persistent volume claims."""
        try:
            pvcs = []
            pvc_list = self.v1.list_persistent_volume_claim_for_all_namespaces()
            
            for pvc in pvc_list.items:
                pvc_data = {
                    'name': pvc.metadata.name,
                    'namespace': pvc.metadata.namespace,
                    'status': pvc.status.phase,
                    'volume_name': pvc.spec.volume_name,
                    'storage_class': pvc.spec.storage_class_name,
                    'access_modes': pvc.spec.access_modes or [],
                    'volume_mode': pvc.spec.volume_mode,
                    'capacity': pvc.status.capacity.get('storage') if pvc.status.capacity else None,
                    'requested': pvc.spec.resources.requests.get('storage') if pvc.spec.resources and pvc.spec.resources.requests else None,
                    'created': pvc.metadata.creation_timestamp.isoformat() if pvc.metadata.creation_timestamp else None,
                    'labels': pvc.metadata.labels or {},
                    'annotations': pvc.metadata.annotations or {},
                    'uid': pvc.metadata.uid
                }
                pvcs.append(pvc_data)
            
            self.logger.info(f"Discovered {len(pvcs)} persistent volume claims")
            return pvcs
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover persistent volume claims: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_ingresses(self) -> List[Dict[str, Any]]:
        """Discover ingresses."""
        try:
            ingresses = []
            ingress_list = self.networking_v1.list_ingress_for_all_namespaces()
            
            for ingress in ingress_list.items:
                ingress_data = {
                    'name': ingress.metadata.name,
                    'namespace': ingress.metadata.namespace,
                    'ingress_class_name': getattr(ingress.spec, 'ingress_class_name', None),
                    'rules': [
                        {
                            'host': rule.host,
                            'paths': [
                                {
                                    'path': path.path,
                                    'path_type': path.path_type,
                                    'backend': {
                                        'service_name': path.backend.service.name if path.backend.service else None,
                                        'service_port': path.backend.service.port.number if path.backend.service and path.backend.service.port else None
                                    }
                                }
                                for path in rule.http.paths
                            ] if rule.http else []
                        }
                        for rule in (ingress.spec.rules or [])
                    ],
                    'tls': [
                        {
                            'hosts': tls.hosts,
                            'secret_name': tls.secret_name
                        }
                        for tls in (ingress.spec.tls or [])
                    ],
                    'load_balancer_ingress': [
                        {
                            'ip': ing.ip,
                            'hostname': ing.hostname
                        }
                        for ing in (ingress.status.load_balancer.ingress or [])
                    ] if ingress.status.load_balancer else [],
                    'created': ingress.metadata.creation_timestamp.isoformat() if ingress.metadata.creation_timestamp else None,
                    'labels': ingress.metadata.labels or {},
                    'annotations': ingress.metadata.annotations or {},
                    'uid': ingress.metadata.uid
                }
                ingresses.append(ingress_data)
            
            self.logger.info(f"Discovered {len(ingresses)} ingresses")
            return ingresses
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover ingresses: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_config_maps(self) -> List[Dict[str, Any]]:
        """Discover config maps."""
        try:
            config_maps = []
            cm_list = self.v1.list_config_map_for_all_namespaces()
            
            for cm in cm_list.items:
                cm_data = {
                    'name': cm.metadata.name,
                    'namespace': cm.metadata.namespace,
                    'data_keys': list(cm.data.keys()) if cm.data else [],
                    'binary_data_keys': list(cm.binary_data.keys()) if cm.binary_data else [],
                    'created': cm.metadata.creation_timestamp.isoformat() if cm.metadata.creation_timestamp else None,
                    'labels': cm.metadata.labels or {},
                    'annotations': cm.metadata.annotations or {},
                    'uid': cm.metadata.uid
                }
                config_maps.append(cm_data)
            
            self.logger.info(f"Discovered {len(config_maps)} config maps")
            return config_maps
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover config maps: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_secrets(self) -> List[Dict[str, Any]]:
        """Discover secrets."""
        try:
            secrets = []
            secret_list = self.v1.list_secret_for_all_namespaces()
            
            for secret in secret_list.items:
                secret_data = {
                    'name': secret.metadata.name,
                    'namespace': secret.metadata.namespace,
                    'type': secret.type,
                    'data_keys': list(secret.data.keys()) if secret.data else [],
                    'created': secret.metadata.creation_timestamp.isoformat() if secret.metadata.creation_timestamp else None,
                    'labels': secret.metadata.labels or {},
                    'annotations': secret.metadata.annotations or {},
                    'uid': secret.metadata.uid
                }
                secrets.append(secret_data)
            
            self.logger.info(f"Discovered {len(secrets)} secrets")
            return secrets
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover secrets: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_resource_quotas(self) -> List[Dict[str, Any]]:
        """Discover resource quotas."""
        try:
            quotas = []
            quota_list = self.v1.list_resource_quota_for_all_namespaces()
            
            for quota in quota_list.items:
                quota_data = {
                    'name': quota.metadata.name,
                    'namespace': quota.metadata.namespace,
                    'hard': quota.status.hard or {},
                    'used': quota.status.used or {},
                    'created': quota.metadata.creation_timestamp.isoformat() if quota.metadata.creation_timestamp else None,
                    'labels': quota.metadata.labels or {},
                    'annotations': quota.metadata.annotations or {},
                    'uid': quota.metadata.uid
                }
                quotas.append(quota_data)
            
            self.logger.info(f"Discovered {len(quotas)} resource quotas")
            return quotas
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover resource quotas: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_limit_ranges(self) -> List[Dict[str, Any]]:
        """Discover limit ranges."""
        try:
            limit_ranges = []
            lr_list = self.v1.list_limit_range_for_all_namespaces()
            
            for lr in lr_list.items:
                lr_data = {
                    'name': lr.metadata.name,
                    'namespace': lr.metadata.namespace,
                    'limits': [
                        {
                            'type': limit.type,
                            'default': limit.default or {},
                            'default_request': limit.default_request or {},
                            'max': limit.max or {},
                            'min': limit.min or {}
                        }
                        for limit in lr.spec.limits
                    ],
                    'created': lr.metadata.creation_timestamp.isoformat() if lr.metadata.creation_timestamp else None,
                    'labels': lr.metadata.labels or {},
                    'annotations': lr.metadata.annotations or {},
                    'uid': lr.metadata.uid
                }
                limit_ranges.append(lr_data)
            
            self.logger.info(f"Discovered {len(limit_ranges)} limit ranges")
            return limit_ranges
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover limit ranges: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Discover recent cluster events - NEW method, doesn't affect existing ones."""
        try:
            events = []
            event_list = self.v1.list_event_for_all_namespaces(limit=limit)
            
            for event in event_list.items:
                event_data = {
                    'name': event.metadata.name,
                    'namespace': event.metadata.namespace,
                    'type': event.type,
                    'reason': event.reason,
                    'message': event.message,
                    'source': {
                        'component': event.source.component if event.source else None,
                        'host': event.source.host if event.source else None
                    },
                    'involved_object': {
                        'kind': event.involved_object.kind if event.involved_object else None,
                        'name': event.involved_object.name if event.involved_object else None,
                        'namespace': event.involved_object.namespace if event.involved_object else None
                    },
                    'first_timestamp': event.first_timestamp.isoformat() if event.first_timestamp else None,
                    'last_timestamp': event.last_timestamp.isoformat() if event.last_timestamp else None,
                    'count': event.count or 1
                }
                events.append(event_data)
            
            # Sort by last timestamp (most recent first)
            events.sort(key=lambda x: x['last_timestamp'] or '', reverse=True)
            
            self.logger.info(f"Discovered {len(events)} cluster events")
            return events
            
        except ApiException as e:
            self.logger.warning(f"Failed to discover events: {e}")
            return []
        
    # Helper methods
    def _get_pv_source(self, pv) -> Dict[str, Any]:
        """Determine PV source type and details."""
        if pv.spec.azure_disk:
            return {
                'type': 'azureDisk',
                'disk_name': pv.spec.azure_disk.disk_name,
                'disk_uri': pv.spec.azure_disk.disk_uri
            }
        elif pv.spec.azure_file:
            return {
                'type': 'azureFile',
                'share_name': pv.spec.azure_file.share_name,
                'secret_name': pv.spec.azure_file.secret_name
            }
        elif pv.spec.csi:
            return {
                'type': 'csi',
                'driver': pv.spec.csi.driver,
                'volume_handle': pv.spec.csi.volume_handle
            }
        elif pv.spec.nfs:
            return {
                'type': 'nfs',
                'server': pv.spec.nfs.server,
                'path': pv.spec.nfs.path
            }
        elif pv.spec.host_path:
            return {
                'type': 'hostPath',
                'path': pv.spec.host_path.path
            }
        elif pv.spec.local:
            return {
                'type': 'local',
                'path': pv.spec.local.path
            }
        else:
            return {'type': 'unknown'}
    
    def _get_node_status(self, node) -> str:
        """Determine node status from conditions."""
        for condition in node.status.conditions:
            if condition.type == 'Ready':
                return 'Ready' if condition.status == 'True' else 'NotReady'
        return 'Unknown'
    
    def _get_node_roles(self, node) -> List[str]:
        """Extract node roles from labels."""
        roles = []
        for label_key in node.metadata.labels:
            if label_key.startswith('node-role.kubernetes.io/'):
                role = label_key.split('/')[-1]
                roles.append(role)
        return roles if roles else ['worker']
    
    def _is_container_ready(self, container_name: str, container_statuses) -> bool:
        """Check if container is ready."""
        if not container_statuses:
            return False
        
        for status in container_statuses:
            if status.name == container_name:
                return status.ready
        return False
    
    def _get_container_restart_count(self, container_name: str, container_statuses) -> int:
        """Get container restart count."""
        if not container_statuses:
            return 0
        
        for status in container_statuses:
            if status.name == container_name:
                return status.restart_count
        return 0