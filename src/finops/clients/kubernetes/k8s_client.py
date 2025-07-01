"""Enhanced Kubernetes client for comprehensive cluster resource discovery."""

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
    """Enhanced client for comprehensive Kubernetes operations."""
    
    def __init__(self, 
                 config: Dict[str, Any],
                 kubeconfig_path: Optional[str] = None,
                 context: Optional[str] = None,
                 kubeconfig_data: Optional[bytes] = None,
                 cluster_name: Optional[str] = None):
        super().__init__(config, "KubernetesClient")
        self.kubeconfig_path = kubeconfig_path
        self.context = context
        self.kubeconfig_data = kubeconfig_data
        self.namespace = config.get("namespace", "default")
        self.cluster_name = cluster_name or config.get("cluster_name", "unknown")
        
        # API clients
        self.v1 = None
        self.apps_v1 = None
        self.networking_v1 = None
        self.batch_v1 = None
        self.batch_v1beta1 = None
        self.storage_v1 = None
        self.rbac_v1 = None
        
        # API capabilities tracking
        self.api_capabilities = {
            'cronjobs_v1': False,
            'cronjobs_v1beta1': False,
            'networking_v1': False,
            'storage_v1': False
        }
    
    async def connect(self) -> None:
        """Connect to Kubernetes cluster."""
        try:
            
            if self.kubeconfig_data:
                # Load from provided kubeconfig data (e.g., from AKS)
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
            
            # Initialize API clients with proper version checking
            self.v1 = client.CoreV1Api()
            self.apps_v1 = client.AppsV1Api()
            
            # Try to initialize networking v1 (preferred)
            try:
                self.networking_v1 = client.NetworkingV1Api()
                self.api_capabilities['networking_v1'] = True
                self.logger.debug("NetworkingV1Api initialized successfully")
            except AttributeError:
                # Fallback to extensions/v1beta1 for older clusters
                try:
                    self.networking_v1 = client.ExtensionsV1beta1Api()
                    self.logger.info("Using ExtensionsV1beta1Api for networking resources")
                except AttributeError:
                    self.networking_v1 = None
                    self.logger.warning("No networking API available")
            
            # Initialize batch APIs
            self.batch_v1 = client.BatchV1Api()
            
            # Check CronJob API availability
            try:
                # Test if BatchV1 supports CronJobs (Kubernetes 1.21+)
                self.batch_v1.list_cron_job_for_all_namespaces(limit=1)
                self.api_capabilities['cronjobs_v1'] = True
                self.logger.debug("CronJobs available in BatchV1Api")
            except (AttributeError, ApiException):
                try:
                    # Try BatchV1beta1 for older clusters
                    self.batch_v1beta1 = client.BatchV1beta1Api()
                    self.batch_v1beta1.list_cron_job_for_all_namespaces(limit=1)
                    self.api_capabilities['cronjobs_v1beta1'] = True
                    self.logger.debug("CronJobs available in BatchV1beta1Api")
                except (AttributeError, ApiException):
                    self.batch_v1beta1 = None
                    self.logger.warning("CronJobs not available in any API version")
            
            # Initialize storage and RBAC APIs
            try:
                self.storage_v1 = client.StorageV1Api()
                self.api_capabilities['storage_v1'] = True
            except AttributeError:
                self.storage_v1 = None
                self.logger.warning("StorageV1Api not available")
            
            try:
                self.rbac_v1 = client.RbacAuthorizationV1Api()
            except AttributeError:
                self.rbac_v1 = None
                self.logger.warning("RbacAuthorizationV1Api not available")
            
            self._connected = True
            self.logger.info(f"Kubernetes client connected successfully to cluster: {self.cluster_name}")
            
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
            
            # Try to get cluster version as health check
            self.v1.get_code()
            return True
            
        except Exception as e:
            self.logger.warning("Kubernetes health check failed", error=str(e))
            return False
    
    @property
    def cluster_info(self) -> Dict[str, Any]:
        """Get cluster information."""
        return {
            'cluster_name': self.cluster_name,
            'context': self.context,
            'connected': self._connected
        }
    
    # Core resource discovery methods
    @retry_with_backoff(max_retries=3)
    async def discover_namespaces(self) -> List[Dict[str, Any]]:
        """Discover namespaces."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            namespaces = []
            
            for ns in self.v1.list_namespace().items:
                ns_data = {
                    'name': ns.metadata.name,
                    'status': ns.status.phase,
                    'created': ns.metadata.creation_timestamp,
                    'labels': ns.metadata.labels or {},
                    'annotations': ns.metadata.annotations or {}
                }
                namespaces.append(ns_data)
            
            self.logger.info(f"Discovered {len(namespaces)} namespaces")
            return namespaces
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover namespaces: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_nodes(self) -> List[Dict[str, Any]]:
        """Discover cluster nodes."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            nodes = []
            
            for node in self.v1.list_node().items:
                node_data = await self._extract_node_data(node)
                nodes.append(node_data)
            
            self.logger.info(f"Discovered {len(nodes)} nodes")
            return nodes
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover nodes: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_pods(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover pods."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            pods = []
            
            pod_list = self.v1.list_pod_for_all_namespaces()
            
            for pod in pod_list.items:
                pod_data = await self._extract_pod_data(pod)
                pods.append(pod_data)
            
            self.logger.info(f"Discovered {len(pods)} pods")
            return pods
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover pods: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_deployments(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover deployments."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            deployments = []
            
            deployment_list = self.apps_v1.list_deployment_for_all_namespaces()
            
            for deployment in deployment_list.items:
                deployment_data = await self._extract_deployment_data(deployment)
                deployments.append(deployment_data)
            
            self.logger.info(f"Discovered {len(deployments)} deployments")
            return deployments
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover deployments: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_services(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover services."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            services = []
            
            service_list = self.v1.list_service_for_all_namespaces()
            
            for service in service_list.items:
                service_data = await self._extract_service_data(service)
                services.append(service_data)
            
            self.logger.info(f"Discovered {len(services)} services")
            return services
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover services: {e}")
    
    # Extended workload discovery methods
    @retry_with_backoff(max_retries=3)
    async def discover_statefulsets(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover statefulsets."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            statefulsets = []
            
            ss_list = self.apps_v1.list_stateful_set_for_all_namespaces()
            
            for ss in ss_list.items:
                ss_data = await self._extract_statefulset_data(ss)
                statefulsets.append(ss_data)
            
            self.logger.info(f"Discovered {len(statefulsets)} statefulsets")
            return statefulsets
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover statefulsets: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_daemonsets(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover daemonsets."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            daemonsets = []
            
            ds_list = self.apps_v1.list_daemon_set_for_all_namespaces()
            
            for ds in ds_list.items:
                ds_data = await self._extract_daemonset_data(ds)
                daemonsets.append(ds_data)
            
            self.logger.info(f"Discovered {len(daemonsets)} daemonsets")
            return daemonsets
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover daemonsets: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_replicasets(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover replicasets."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            replicasets = []
            
            rs_list = self.apps_v1.list_replica_set_for_all_namespaces()
            
            for rs in rs_list.items:
                rs_data = await self._extract_replicaset_data(rs)
                replicasets.append(rs_data)
            
            self.logger.info(f"Discovered {len(replicasets)} replicasets")
            return replicasets
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover replicasets: {e}")
    
    # Networking discovery methods
    @retry_with_backoff(max_retries=3)
    async def discover_ingresses(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover ingresses."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            ingresses = []
            
            
            ingress_list = self.networking_v1.list_ingress_for_all_namespaces()
            
            for ingress in ingress_list.items:
                ingress_data = await self._extract_ingress_data(ingress)
                ingresses.append(ingress_data)
            
            self.logger.info(f"Discovered {len(ingresses)} ingresses")
            return ingresses
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover ingresses: {e}")
    
    # Storage discovery methods
    @retry_with_backoff(max_retries=3)
    async def discover_persistent_volumes(self) -> List[Dict[str, Any]]:
        """Discover persistent volumes."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            pvs = []
            
            pv_list = self.v1.list_persistent_volume()
            
            for pv in pv_list.items:
                pv_data = await self._extract_persistent_volume_data(pv)
                pvs.append(pv_data)
            
            self.logger.info(f"Discovered {len(pvs)} persistent volumes")
            return pvs
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover persistent volumes: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_persistent_volume_claims(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover persistent volume claims."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            pvcs = []
            
            pvc_list = self.v1.list_persistent_volume_claim_for_all_namespaces()
            
            for pvc in pvc_list.items:
                pvc_data = await self._extract_persistent_volume_claim_data(pvc)
                pvcs.append(pvc_data)
            
            self.logger.info(f"Discovered {len(pvcs)} persistent volume claims")
            return pvcs
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover persistent volume claims: {e}")
    
    # Configuration discovery methods
    @retry_with_backoff(max_retries=3)
    async def discover_config_maps(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover config maps."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            config_maps = []
            
            cm_list = self.v1.list_config_map_for_all_namespaces()
            
            for cm in cm_list.items:
                cm_data = await self._extract_config_map_data(cm)
                config_maps.append(cm_data)
            
            self.logger.info(f"Discovered {len(config_maps)} config maps")
            return config_maps
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover config maps: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_secrets(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover secrets."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            secrets = []
            
            secret_list = self.v1.list_secret_for_all_namespaces()
            
            for secret in secret_list.items:
                secret_data = await self._extract_secret_data(secret)
                secrets.append(secret_data)
            
            self.logger.info(f"Discovered {len(secrets)} secrets")
            return secrets
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover secrets: {e}")
    
    # Job discovery methods
    @retry_with_backoff(max_retries=3)
    async def discover_jobs(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover jobs."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            jobs = []
            
            job_list = self.batch_v1.list_job_for_all_namespaces()
            
            for job in job_list.items:
                job_data = await self._extract_job_data(job)
                jobs.append(job_data)
            
            self.logger.info(f"Discovered {len(jobs)} jobs")
            return jobs
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover jobs: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_cronjobs(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover cronjobs."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            cronjobs = []
            
            # Try BatchV1 first (Kubernetes 1.21+), fallback to BatchV1beta1
            try:
                cj_list = self.batch_v1.list_cron_job_for_all_namespaces()
                self.logger.debug("Using BatchV1Api for cronjobs")
            except (AttributeError, ApiException) as e:
                if self.batch_v1beta1:
                    cj_list = self.batch_v1beta1.list_cron_job_for_all_namespaces()
                    self.logger.debug("Using BatchV1beta1Api for cronjobs")
                else:
                    raise DiscoveryException("Kubernetes", f"CronJob discovery not supported: {e}")
            
            for cj in cj_list.items:
                cj_data = await self._extract_cronjob_data(cj)
                cronjobs.append(cj_data)
            
            self.logger.info(f"Discovered {len(cronjobs)} cronjobs")
            return cronjobs
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover cronjobs: {e}")
    
    # Policy discovery methods
    @retry_with_backoff(max_retries=3)
    async def discover_resource_quotas(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover resource quotas."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            quotas = []
            
            quota_list = self.v1.list_resource_quota_for_all_namespaces()
            
            for quota in quota_list.items:
                quota_data = await self._extract_resource_quota_data(quota)
                quotas.append(quota_data)
            
            self.logger.info(f"Discovered {len(quotas)} resource quotas")
            return quotas
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover resource quotas: {e}")
    
    @retry_with_backoff(max_retries=3)
    async def discover_limit_ranges(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover limit ranges."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            limit_ranges = []
            
            lr_list = self.v1.list_limit_range_for_all_namespaces()
            
            for lr in lr_list.items:
                lr_data = await self._extract_limit_range_data(lr)
                limit_ranges.append(lr_data)
            
            self.logger.info(f"Discovered {len(limit_ranges)} limit ranges")
            return limit_ranges
            
        except ApiException as e:
            raise DiscoveryException("Kubernetes", f"Failed to discover limit ranges: {e}")
    
    # Data extraction methods
    async def _extract_node_data(self, node) -> Dict[str, Any]:
        """Extract node data from Kubernetes response."""
        return {
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
            'created': node.metadata.creation_timestamp,
            'labels': node.metadata.labels or {},
            'annotations': node.metadata.annotations or {},
            'taints': [{'key': taint.key, 'value': taint.value, 'effect': taint.effect} 
                      for taint in (node.spec.taints or [])]
        }
    
    async def _extract_pod_data(self, pod) -> Dict[str, Any]:
        """Extract pod data from Kubernetes response."""
        return {
            'name': pod.metadata.name,
            'namespace': pod.metadata.namespace,
            'status': pod.status.phase,
            'node': pod.spec.node_name,
            'ip': pod.status.pod_ip,
            'host_ip': pod.status.host_ip,
            'restart_policy': pod.spec.restart_policy,
            'service_account': pod.spec.service_account_name,
            'priority': pod.spec.priority,
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
            'created': pod.metadata.creation_timestamp,
            'started': pod.status.start_time,
            'labels': pod.metadata.labels or {},
            'annotations': pod.metadata.annotations or {},
            'owner_references': [{'kind': ref.kind, 'name': ref.name, 'controller': ref.controller} 
                               for ref in (pod.metadata.owner_references or [])]
        }
    
    async def _extract_deployment_data(self, deployment) -> Dict[str, Any]:
        """Extract deployment data from Kubernetes response."""
        return {
            'name': deployment.metadata.name,
            'namespace': deployment.metadata.namespace,
            'replicas': deployment.spec.replicas,
            'ready_replicas': deployment.status.ready_replicas or 0,
            'available_replicas': deployment.status.available_replicas or 0,
            'updated_replicas': deployment.status.updated_replicas or 0,
            'strategy': {
                'type': deployment.spec.strategy.type if deployment.spec.strategy else None
            },
            'created': deployment.metadata.creation_timestamp,
            'labels': deployment.metadata.labels or {},
            'annotations': deployment.metadata.annotations or {},
            'selector': deployment.spec.selector.match_labels if deployment.spec.selector else {},
            'conditions': [{'type': cond.type, 'status': cond.status, 'reason': cond.reason} 
                          for cond in (deployment.status.conditions or [])]
        }
    
    async def _extract_service_data(self, service) -> Dict[str, Any]:
        """Extract service data from Kubernetes response."""
        return {
            'name': service.metadata.name,
            'namespace': service.metadata.namespace,
            'type': service.spec.type,
            'cluster_ip': service.spec.cluster_ip,
            'external_ips': service.spec.external_i_ps or [],
            'ports': [{'name': p.name, 'port': p.port, 'target_port': p.target_port, 
                      'protocol': p.protocol, 'node_port': p.node_port} 
                     for p in (service.spec.ports or [])],
            'selector': service.spec.selector or {},
            'created': service.metadata.creation_timestamp,
            'labels': service.metadata.labels or {},
            'annotations': service.metadata.annotations or {}
        }
    
    async def _extract_statefulset_data(self, statefulset) -> Dict[str, Any]:
        """Extract statefulset data from Kubernetes response."""
        return {
            'name': statefulset.metadata.name,
            'namespace': statefulset.metadata.namespace,
            'replicas': statefulset.spec.replicas,
            'ready_replicas': statefulset.status.ready_replicas or 0,
            'current_replicas': statefulset.status.current_replicas or 0,
            'updated_replicas': statefulset.status.updated_replicas or 0,
            'service_name': statefulset.spec.service_name,
            'pod_management_policy': statefulset.spec.pod_management_policy,
            'update_strategy': {
                'type': statefulset.spec.update_strategy.type if statefulset.spec.update_strategy else None
            },
            'created': statefulset.metadata.creation_timestamp,
            'labels': statefulset.metadata.labels or {},
            'annotations': statefulset.metadata.annotations or {},
            'volume_claim_templates': len(statefulset.spec.volume_claim_templates or []),
            'current_revision': statefulset.status.current_revision,
            'update_revision': statefulset.status.update_revision
        }
    
    async def _extract_daemonset_data(self, daemonset) -> Dict[str, Any]:
        """Extract daemonset data from Kubernetes response."""
        return {
            'name': daemonset.metadata.name,
            'namespace': daemonset.metadata.namespace,
            'desired_number_scheduled': daemonset.status.desired_number_scheduled or 0,
            'current_number_scheduled': daemonset.status.current_number_scheduled or 0,
            'number_ready': daemonset.status.number_ready or 0,
            'number_available': daemonset.status.number_available or 0,
            'number_unavailable': daemonset.status.number_unavailable or 0,
            'number_misscheduled': daemonset.status.number_misscheduled or 0,
            'updated_number_scheduled': daemonset.status.updated_number_scheduled or 0,
            'created': daemonset.metadata.creation_timestamp,
            'labels': daemonset.metadata.labels or {},
            'annotations': daemonset.metadata.annotations or {},
            'update_strategy': {
                'type': daemonset.spec.update_strategy.type if daemonset.spec.update_strategy else None
            }
        }
    
    async def _extract_replicaset_data(self, replicaset) -> Dict[str, Any]:
        """Extract replicaset data from Kubernetes response."""
        return {
            'name': replicaset.metadata.name,
            'namespace': replicaset.metadata.namespace,
            'replicas': replicaset.spec.replicas,
            'ready_replicas': replicaset.status.ready_replicas or 0,
            'available_replicas': replicaset.status.available_replicas or 0,
            'fully_labeled_replicas': replicaset.status.fully_labeled_replicas or 0,
            'created': replicaset.metadata.creation_timestamp,
            'labels': replicaset.metadata.labels or {},
            'annotations': replicaset.metadata.annotations or {},
            'owner_references': [{'kind': ref.kind, 'name': ref.name, 'controller': ref.controller} 
                               for ref in (replicaset.metadata.owner_references or [])],
            'selector': replicaset.spec.selector.match_labels if replicaset.spec.selector else {}
        }
    
    async def _extract_ingress_data(self, ingress) -> Dict[str, Any]:
        """Extract ingress data from Kubernetes response."""
        return {
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
            'created': ingress.metadata.creation_timestamp,
            'labels': ingress.metadata.labels or {},
            'annotations': ingress.metadata.annotations or {}
        }
    
    async def _extract_persistent_volume_data(self, pv) -> Dict[str, Any]:
        """Extract persistent volume data from Kubernetes response."""
        return {
            'name': pv.metadata.name,
            'capacity': pv.spec.capacity.get('storage') if pv.spec.capacity else None,
            'access_modes': pv.spec.access_modes or [],
            'reclaim_policy': pv.spec.persistent_volume_reclaim_policy,
            'status': pv.status.phase,
            'claim': f"{pv.spec.claim_ref.namespace}/{pv.spec.claim_ref.name}" if pv.spec.claim_ref else None,
            'storage_class': pv.spec.storage_class_name,
            'volume_mode': pv.spec.volume_mode,
            'mount_options': pv.spec.mount_options or [],
            'created': pv.metadata.creation_timestamp,
            'labels': pv.metadata.labels or {},
            'annotations': pv.metadata.annotations or {},
            'source': self._get_pv_source(pv)
        }
    
    async def _extract_persistent_volume_claim_data(self, pvc) -> Dict[str, Any]:
        """Extract persistent volume claim data from Kubernetes response."""
        return {
            'name': pvc.metadata.name,
            'namespace': pvc.metadata.namespace,
            'status': pvc.status.phase,
            'volume_name': pvc.spec.volume_name,
            'storage_class': pvc.spec.storage_class_name,
            'access_modes': pvc.spec.access_modes or [],
            'volume_mode': pvc.spec.volume_mode,
            'capacity': pvc.status.capacity.get('storage') if pvc.status.capacity else None,
            'requested': pvc.spec.resources.requests.get('storage') if pvc.spec.resources and pvc.spec.resources.requests else None,
            'created': pvc.metadata.creation_timestamp,
            'labels': pvc.metadata.labels or {},
            'annotations': pvc.metadata.annotations or {}
        }
    
    async def _extract_config_map_data(self, config_map) -> Dict[str, Any]:
        """Extract config map data from Kubernetes response."""
        return {
            'name': config_map.metadata.name,
            'namespace': config_map.metadata.namespace,
            'data': config_map.data or {},
            'binary_data': config_map.binary_data or {},
            'created': config_map.metadata.creation_timestamp,
            'labels': config_map.metadata.labels or {},
            'annotations': config_map.metadata.annotations or {}
        }
    
    async def _extract_secret_data(self, secret) -> Dict[str, Any]:
        """Extract secret data from Kubernetes response (without exposing sensitive data)."""
        return {
            'name': secret.metadata.name,
            'namespace': secret.metadata.namespace,
            'type': secret.type,
            'data': secret.data or {},  # Keys only, values are base64 encoded
            'created': secret.metadata.creation_timestamp,
            'labels': secret.metadata.labels or {},
            'annotations': secret.metadata.annotations or {}
        }
    
    async def _extract_job_data(self, job) -> Dict[str, Any]:
        """Extract job data from Kubernetes response."""
        return {
            'name': job.metadata.name,
            'namespace': job.metadata.namespace,
            'active': job.status.active or 0,
            'succeeded': job.status.succeeded or 0,
            'failed': job.status.failed or 0,
            'completion_time': job.status.completion_time,
            'start_time': job.status.start_time,
            'parallelism': job.spec.parallelism,
            'completions': job.spec.completions,
            'active_deadline_seconds': job.spec.active_deadline_seconds,
            'backoff_limit': job.spec.backoff_limit,
            'ttl_seconds_after_finished': job.spec.ttl_seconds_after_finished,
            'created': job.metadata.creation_timestamp,
            'labels': job.metadata.labels or {},
            'annotations': job.metadata.annotations or {}
        }
    
    async def _extract_cronjob_data(self, cronjob) -> Dict[str, Any]:
        """Extract cronjob data from Kubernetes response."""
        return {
            'name': cronjob.metadata.name,
            'namespace': cronjob.metadata.namespace,
            'schedule': cronjob.spec.schedule,
            'suspend': cronjob.spec.suspend,
            'active': cronjob.status.active or [],
            'last_schedule_time': cronjob.status.last_schedule_time,
            'last_successful_time': cronjob.status.last_successful_time,
            'starting_deadline_seconds': cronjob.spec.starting_deadline_seconds,
            'concurrency_policy': cronjob.spec.concurrency_policy,
            'successful_jobs_history_limit': cronjob.spec.successful_jobs_history_limit,
            'failed_jobs_history_limit': cronjob.spec.failed_jobs_history_limit,
            'created': cronjob.metadata.creation_timestamp,
            'labels': cronjob.metadata.labels or {},
            'annotations': cronjob.metadata.annotations or {}
        }
    
    async def _extract_resource_quota_data(self, quota) -> Dict[str, Any]:
        """Extract resource quota data from Kubernetes response."""
        return {
            'name': quota.metadata.name,
            'namespace': quota.metadata.namespace,
            'hard': quota.status.hard or {},
            'used': quota.status.used or {},
            'created': quota.metadata.creation_timestamp,
            'labels': quota.metadata.labels or {},
            'annotations': quota.metadata.annotations or {}
        }
    
    async def _extract_limit_range_data(self, limit_range) -> Dict[str, Any]:
        """Extract limit range data from Kubernetes response."""
        return {
            'name': limit_range.metadata.name,
            'namespace': limit_range.metadata.namespace,
            'limits': [
                {
                    'type': limit.type,
                    'default': limit.default or {},
                    'default_request': limit.default_request or {},
                    'max': limit.max or {},
                    'min': limit.min or {}
                }
                for limit in limit_range.spec.limits
            ],
            'created': limit_range.metadata.creation_timestamp,
            'labels': limit_range.metadata.labels or {},
            'annotations': limit_range.metadata.annotations or {}
        }
    
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
    
    # Utility methods for resource analysis
    async def get_cluster_summary(self) -> Dict[str, Any]:
        """Get a summary of cluster resources."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            summary = {
                'cluster_name': self.cluster_name,
                'namespaces': len(await self.discover_namespaces()),
                'nodes': len(await self.discover_nodes()),
                'pods': len(await self.discover_pods()),
                'deployments': len(await self.discover_deployments()),
                'services': len(await self.discover_services()),
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            # Try to get extended resources
            try:
                summary['statefulsets'] = len(await self.discover_statefulsets())
                summary['daemonsets'] = len(await self.discover_daemonsets())
                summary['jobs'] = len(await self.discover_jobs())
                summary['cronjobs'] = len(await self.discover_cronjobs())
                summary['persistent_volumes'] = len(await self.discover_persistent_volumes())
                summary['config_maps'] = len(await self.discover_config_maps())
                summary['secrets'] = len(await self.discover_secrets())
            except Exception as e:
                self.logger.debug(f"Could not get extended resource counts: {e}")
            
            return summary
            
        except Exception as e:
            raise DiscoveryException("Kubernetes", f"Failed to get cluster summary: {e}")
    
    async def get_namespace_resources(self, namespace: str) -> Dict[str, Any]:
        """Get all resources in a specific namespace."""
        if not self._connected:
            raise DiscoveryException("Kubernetes", "Client not connected")
        
        try:
            resources = {
                'namespace': namespace,
                'pods': await self.discover_pods(namespace),
                'deployments': await self.discover_deployments(namespace),
                'services': await self.discover_services(namespace),
                'config_maps': await self.discover_config_maps(namespace),
                'secrets': await self.discover_secrets(namespace),
                'persistent_volume_claims': await self.discover_persistent_volume_claims(namespace),
                'resource_quotas': await self.discover_resource_quotas(namespace),
                'limit_ranges': await self.discover_limit_ranges(namespace)
            }
            
            # Try to get extended resources
            try:
                resources['statefulsets'] = await self.discover_statefulsets(namespace)
                resources['daemonsets'] = await self.discover_daemonsets(namespace)
                resources['replicasets'] = await self.discover_replicasets(namespace)
                resources['jobs'] = await self.discover_jobs(namespace)
                resources['cronjobs'] = await self.discover_cronjobs(namespace)
                resources['ingresses'] = await self.discover_ingresses(namespace)
            except Exception as e:
                self.logger.debug(f"Could not get extended namespace resources: {e}")
            
            return resources
            
        except Exception as e:
            raise DiscoveryException("Kubernetes", f"Failed to get namespace resources: {e}")
        
