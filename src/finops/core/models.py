"""
FinOps Discovery Data Models
Comprehensive data models for Kubernetes FinOps discovery information
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum
import uuid


class ResourceStatus(str, Enum):
    """Resource status enumeration."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    FAILED = "failed"
    UNKNOWN = "unknown"


class NodePoolMode(str, Enum):
    """Node pool mode enumeration."""
    SYSTEM = "System"
    USER = "User"


class ServiceType(str, Enum):
    """Service type enumeration."""
    CLUSTER_IP = "ClusterIP"
    NODE_PORT = "NodePort"
    LOAD_BALANCER = "LoadBalancer"
    EXTERNAL_NAME = "ExternalName"


class VolumeType(str, Enum):
    """Volume type enumeration."""
    PERSISTENT_VOLUME = "PersistentVolume"
    PERSISTENT_VOLUME_CLAIM = "PersistentVolumeClaim"
    CONFIGMAP = "ConfigMap"
    SECRET = "Secret"
    EMPTYDIR = "EmptyDir"


class BaseFinOpsModel(BaseModel):
    """Base model with common fields."""
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }


class CostData(BaseModel):
    """Cost data model."""
    
    total: float = Field(0.0, ge=0, description="Total cost")
    compute: float = Field(0.0, ge=0, description="Compute cost")
    storage: float = Field(0.0, ge=0, description="Storage cost")
    network: float = Field(0.0, ge=0, description="Network cost")
    monitoring: float = Field(0.0, ge=0, description="Monitoring cost")
    other: float = Field(0.0, ge=0, description="Other costs")
    currency: str = Field("USD", description="Currency")
    by_service: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    cost_trend: str = Field("stable", description="Cost trend: increasing, decreasing, stable")
    allocation_percentage: float = Field(0.0, ge=0, le=100, description="Percentage of total cluster cost")


class ResourceMetrics(BaseModel):
    """Resource metrics model."""
    
    cpu_usage: float = Field(0.0, ge=0, description="CPU usage percentage")
    memory_usage: float = Field(0.0, ge=0, description="Memory usage percentage")
    storage_usage: float = Field(0.0, ge=0, description="Storage usage percentage")
    network_in: float = Field(0.0, ge=0, description="Network in bytes")
    network_out: float = Field(0.0, ge=0, description="Network out bytes")
    peak_cpu: float = Field(0.0, ge=0, description="Peak CPU usage")
    peak_memory: float = Field(0.0, ge=0, description="Peak memory usage")
    avg_cpu: float = Field(0.0, ge=0, description="Average CPU usage")
    avg_memory: float = Field(0.0, ge=0, description="Average memory usage")
    collection_timestamp: datetime = Field(default_factory=datetime.utcnow)


class ResourceRequests(BaseModel):
    """Resource requests and limits model."""
    
    cpu_request: float = Field(0.0, ge=0, description="CPU request in millicores")
    memory_request: float = Field(0.0, ge=0, description="Memory request in bytes")
    storage_request: float = Field(0.0, ge=0, description="Storage request in bytes")
    cpu_limit: float = Field(0.0, ge=0, description="CPU limit in millicores")
    memory_limit: float = Field(0.0, ge=0, description="Memory limit in bytes")
    storage_limit: float = Field(0.0, ge=0, description="Storage limit in bytes")
    
    @property
    def cpu_utilization_ratio(self) -> float:
        """Calculate CPU utilization ratio."""
        return (self.cpu_request / self.cpu_limit) * 100 if self.cpu_limit > 0 else 0
    
    @property
    def memory_utilization_ratio(self) -> float:
        """Calculate memory utilization ratio."""
        return (self.memory_request / self.memory_limit) * 100 if self.memory_limit > 0 else 0


class ContainerInfo(BaseModel):
    """Container information model."""
    
    name: str
    image: str
    ready: bool = False
    restart_count: int = Field(0, ge=0)
    resources: ResourceRequests = Field(default_factory=ResourceRequests)
    status: str = "Unknown"
    last_state: Optional[str] = None
    environment_variables: Dict[str, str] = Field(default_factory=dict)
    volume_mounts: List[Dict[str, Any]] = Field(default_factory=list)


class PodInfo(BaseModel):
    """Pod information model."""
    
    name: str
    namespace: str
    status: ResourceStatus = ResourceStatus.UNKNOWN
    node: Optional[str] = None
    ip: Optional[str] = None
    host_ip: Optional[str] = None
    created: Optional[datetime] = None
    started: Optional[datetime] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    annotations: Dict[str, str] = Field(default_factory=dict)
    containers: List[ContainerInfo] = Field(default_factory=list)
    init_containers: List[ContainerInfo] = Field(default_factory=list)
    resource_requests: ResourceRequests = Field(default_factory=ResourceRequests)
    resource_limits: ResourceRequests = Field(default_factory=ResourceRequests)
    utilization: ResourceMetrics = Field(default_factory=ResourceMetrics)
    cost_data: CostData = Field(default_factory=CostData)
    owner_references: List[Dict[str, Any]] = Field(default_factory=list)
    conditions: List[Dict[str, Any]] = Field(default_factory=list)
    qos_class: str = "BestEffort"
    phase: str = "Unknown"
    
    @property
    def age_days(self) -> float:
        """Calculate pod age in days."""
        if self.created:
            return (datetime.utcnow() - self.created).days
        return 0


class WorkloadInfo(BaseModel):
    """Workload information model."""
    
    name: str
    namespace: str
    kind: str  # Deployment, StatefulSet, DaemonSet, etc.
    replicas: int = Field(0, ge=0)
    ready_replicas: int = Field(0, ge=0)
    available_replicas: int = Field(0, ge=0)
    updated_replicas: int = Field(0, ge=0)
    created: Optional[datetime] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    annotations: Dict[str, str] = Field(default_factory=dict)
    selector: Dict[str, str] = Field(default_factory=dict)
    strategy: Dict[str, Any] = Field(default_factory=dict)
    pods: List[PodInfo] = Field(default_factory=list)
    resource_requests: ResourceRequests = Field(default_factory=ResourceRequests)
    resource_limits: ResourceRequests = Field(default_factory=ResourceRequests)
    utilization: ResourceMetrics = Field(default_factory=ResourceMetrics)
    cost_data: CostData = Field(default_factory=CostData)
    conditions: List[Dict[str, Any]] = Field(default_factory=list)
    scaling_history: List[Dict[str, Any]] = Field(default_factory=list)
    
    @property
    def health_status(self) -> str:
        """Calculate workload health status."""
        if self.ready_replicas == self.replicas and self.replicas > 0:
            return "healthy"
        elif self.ready_replicas > 0:
            return "degraded"
        else:
            return "unhealthy"
    
    @property
    def efficiency_score(self) -> float:
        """Calculate efficiency score based on utilization."""
        if self.utilization.cpu_usage > 0 and self.utilization.memory_usage > 0:
            return (self.utilization.cpu_usage + self.utilization.memory_usage) / 2
        return 0


class ServiceInfo(BaseModel):
    """Service information model."""
    
    name: str
    namespace: str
    type: ServiceType = ServiceType.CLUSTER_IP
    cluster_ip: Optional[str] = None
    external_ips: List[str] = Field(default_factory=list)
    ports: List[Dict[str, Any]] = Field(default_factory=list)
    selector: Dict[str, str] = Field(default_factory=dict)
    created: Optional[datetime] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    annotations: Dict[str, str] = Field(default_factory=dict)
    endpoints: List[Dict[str, Any]] = Field(default_factory=list)
    load_balancer_ingress: List[Dict[str, Any]] = Field(default_factory=list)
    cost_data: CostData = Field(default_factory=CostData)
    traffic_metrics: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def is_load_balancer(self) -> bool:
        """Check if service is a load balancer."""
        return self.type == ServiceType.LOAD_BALANCER


class StorageInfo(BaseModel):
    """Storage information model."""
    
    name: str
    namespace: Optional[str] = None
    type: VolumeType = VolumeType.PERSISTENT_VOLUME
    capacity: str = "0"
    used: str = "0"
    available: str = "0"
    status: ResourceStatus = ResourceStatus.UNKNOWN
    storage_class: Optional[str] = None
    access_modes: List[str] = Field(default_factory=list)
    reclaim_policy: Optional[str] = None
    mount_options: List[str] = Field(default_factory=list)
    created: Optional[datetime] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    annotations: Dict[str, str] = Field(default_factory=dict)
    claim_ref: Optional[str] = None
    source: Dict[str, Any] = Field(default_factory=dict)
    utilization: ResourceMetrics = Field(default_factory=ResourceMetrics)
    cost_data: CostData = Field(default_factory=CostData)
    backup_info: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def utilization_percentage(self) -> float:
        """Calculate storage utilization percentage."""
        try:
            capacity_bytes = self._parse_storage_size(self.capacity)
            used_bytes = self._parse_storage_size(self.used)
            if capacity_bytes > 0:
                return (used_bytes / capacity_bytes) * 100
        except:
            pass
        return 0
    
    def _parse_storage_size(self, size_str: str) -> int:
        """Parse storage size string to bytes."""
        if not size_str or size_str == "0":
            return 0
        
        units = {
            'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3, 'Ti': 1024**4,
            'K': 1000, 'M': 1000**2, 'G': 1000**3, 'T': 1000**4
        }
        
        for unit, multiplier in units.items():
            if size_str.endswith(unit):
                return int(float(size_str[:-len(unit)]) * multiplier)
        
        return int(size_str)


class IngressInfo(BaseModel):
    """Ingress information model."""
    
    name: str
    namespace: str
    ingress_class: Optional[str] = None
    rules: List[Dict[str, Any]] = Field(default_factory=list)
    tls: List[Dict[str, Any]] = Field(default_factory=list)
    load_balancer_ingress: List[Dict[str, Any]] = Field(default_factory=list)
    created: Optional[datetime] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    annotations: Dict[str, str] = Field(default_factory=dict)
    backend_services: List[str] = Field(default_factory=list)
    cost_data: CostData = Field(default_factory=CostData)
    traffic_metrics: Dict[str, Any] = Field(default_factory=dict)
    ssl_cert_info: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def has_tls(self) -> bool:
        """Check if ingress has TLS configuration."""
        return len(self.tls) > 0


class NetworkResourceInfo(BaseModel):
    """Network resource information model."""
    
    name: str
    resource_group: str
    type: str  # LoadBalancer, PublicIP, VirtualNetwork, etc.
    location: str
    sku: Optional[str] = None
    ip_address: Optional[str] = None
    allocation_method: Optional[str] = None
    dns_settings: Dict[str, Any] = Field(default_factory=dict)
    created: Optional[datetime] = None
    tags: Dict[str, str] = Field(default_factory=dict)
    associated_resources: List[str] = Field(default_factory=list)
    configuration: Dict[str, Any] = Field(default_factory=dict)
    cost_data: CostData = Field(default_factory=CostData)
    utilization: ResourceMetrics = Field(default_factory=ResourceMetrics)
    
    @property
    def is_public_ip(self) -> bool:
        """Check if resource is a public IP."""
        return self.type.lower() == "publicip"


class AzureResourceInfo(BaseModel):
    """Azure resource information model."""
    
    name: str
    resource_group: str
    type: str  # StorageAccount, LogAnalytics, etc.
    location: str
    sku: Optional[str] = None
    tier: Optional[str] = None
    kind: Optional[str] = None
    created: Optional[datetime] = None
    tags: Dict[str, str] = Field(default_factory=dict)
    properties: Dict[str, Any] = Field(default_factory=dict)
    cost_data: CostData = Field(default_factory=CostData)
    utilization: ResourceMetrics = Field(default_factory=ResourceMetrics)
    compliance_status: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def is_storage_account(self) -> bool:
        """Check if resource is a storage account."""
        return "storage" in self.type.lower()


class NamespaceInfo(BaseModel):
    """Namespace information model."""
    
    name: str
    status: ResourceStatus = ResourceStatus.ACTIVE
    created: Optional[datetime] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    annotations: Dict[str, str] = Field(default_factory=dict)
    resource_quotas: List[Dict[str, Any]] = Field(default_factory=list)
    limit_ranges: List[Dict[str, Any]] = Field(default_factory=list)
    workloads: List[WorkloadInfo] = Field(default_factory=list)
    pods: List[PodInfo] = Field(default_factory=list)
    services: List[ServiceInfo] = Field(default_factory=list)
    storage: List[StorageInfo] = Field(default_factory=list)
    ingresses: List[IngressInfo] = Field(default_factory=list)
    resource_requests: ResourceRequests = Field(default_factory=ResourceRequests)
    resource_limits: ResourceRequests = Field(default_factory=ResourceRequests)
    utilization: ResourceMetrics = Field(default_factory=ResourceMetrics)
    cost_data: CostData = Field(default_factory=CostData)
    
    @property
    def workload_count(self) -> int:
        """Get total workload count."""
        return len(self.workloads)
    
    @property
    def pod_count(self) -> int:
        """Get total pod count."""
        return len(self.pods)
    
    @property
    def is_system_namespace(self) -> bool:
        """Check if namespace is a system namespace."""
        system_namespaces = {
            'kube-system', 'kube-public', 'kube-node-lease',
            'azure-system', 'gatekeeper-system', 'default'
        }
        return self.name in system_namespaces


class NodeInfo(BaseModel):
    """Node information model."""
    
    name: str
    status: ResourceStatus = ResourceStatus.UNKNOWN
    roles: List[str] = Field(default_factory=list)
    version: str = "Unknown"
    os: str = "Unknown"
    kernel: str = "Unknown"
    container_runtime: str = "Unknown"
    instance_type: str = "Unknown"
    zone: str = "Unknown"
    capacity: Dict[str, str] = Field(default_factory=dict)
    allocatable: Dict[str, str] = Field(default_factory=dict)
    conditions: List[Dict[str, Any]] = Field(default_factory=list)
    created: Optional[datetime] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    annotations: Dict[str, str] = Field(default_factory=dict)
    taints: List[Dict[str, Any]] = Field(default_factory=list)
    pods: List[PodInfo] = Field(default_factory=list)
    utilization: ResourceMetrics = Field(default_factory=ResourceMetrics)
    cost_data: CostData = Field(default_factory=CostData)
    workload_summary: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def is_ready(self) -> bool:
        """Check if node is ready."""
        return self.status == ResourceStatus.ACTIVE
    
    @property
    def pod_count(self) -> int:
        """Get pod count on this node."""
        return len(self.pods)
    
    @property
    def cpu_capacity_millicores(self) -> int:
        """Get CPU capacity in millicores."""
        cpu_str = self.capacity.get('cpu', '0')
        if cpu_str.endswith('m'):
            return int(cpu_str[:-1])
        return int(float(cpu_str)) * 1000
    
    @property
    def memory_capacity_bytes(self) -> int:
        """Get memory capacity in bytes."""
        memory_str = self.capacity.get('memory', '0')
        return self._parse_memory_size(memory_str)
    
    def _parse_memory_size(self, memory_str: str) -> int:
        """Parse memory size string to bytes."""
        if not memory_str or memory_str == "0":
            return 0
        
        units = {
            'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3, 'Ti': 1024**4,
            'K': 1000, 'M': 1000**2, 'G': 1000**3, 'T': 1000**4
        }
        
        for unit, multiplier in units.items():
            if memory_str.endswith(unit):
                return int(float(memory_str[:-len(unit)]) * multiplier)
        
        return int(memory_str)


class NodePoolInfo(BaseModel):
    """Node pool information model."""
    
    name: str
    cluster_name: str
    vm_size: str
    os_type: str = "Linux"
    os_disk_size_gb: int = Field(0, ge=0)
    count: int = Field(0, ge=0)
    min_count: Optional[int] = None
    max_count: Optional[int] = None
    auto_scaling_enabled: bool = False
    node_taints: List[str] = Field(default_factory=list)
    node_labels: Dict[str, str] = Field(default_factory=dict)
    availability_zones: List[str] = Field(default_factory=list)
    mode: NodePoolMode = NodePoolMode.USER
    orchestrator_version: str = "Unknown"
    provisioning_state: str = "Unknown"
    power_state: str = "Unknown"
    max_pods: int = Field(110, ge=0)
    os_disk_type: str = "Managed"
    scale_set_priority: Optional[str] = None
    spot_max_price: Optional[float] = None
    upgrade_settings: Dict[str, Any] = Field(default_factory=dict)
    created: Optional[datetime] = None
    tags: Dict[str, str] = Field(default_factory=dict)
    nodes: List[NodeInfo] = Field(default_factory=list)
    utilization: ResourceMetrics = Field(default_factory=ResourceMetrics)
    cost_data: CostData = Field(default_factory=CostData)
    scaling_history: List[Dict[str, Any]] = Field(default_factory=list)
    
    @property
    def is_spot_instance(self) -> bool:
        """Check if node pool uses spot instances."""
        return self.scale_set_priority == "Spot"
    
    @property
    def node_count(self) -> int:
        """Get actual node count."""
        return len(self.nodes)
    
    @property
    def total_capacity(self) -> Dict[str, int]:
        """Calculate total capacity across all nodes."""
        total_cpu = 0
        total_memory = 0
        
        for node in self.nodes:
            total_cpu += node.cpu_capacity_millicores
            total_memory += node.memory_capacity_bytes
        
        return {
            'cpu_millicores': total_cpu,
            'memory_bytes': total_memory
        }


class ClusterInfo(BaseModel):
    """Cluster information model."""
    
    id: str
    name: str
    resource_group: str
    location: str
    kubernetes_version: str = "Unknown"
    provisioning_state: str = "Unknown"
    fqdn: Optional[str] = None
    node_resource_group: Optional[str] = None
    dns_prefix: Optional[str] = None
    enable_rbac: bool = True
    network_profile: Dict[str, Any] = Field(default_factory=dict)
    addon_profiles: Dict[str, Any] = Field(default_factory=dict)
    sku: Dict[str, Any] = Field(default_factory=dict)
    created_time: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    tags: Dict[str, str] = Field(default_factory=dict)
    
    # Kubernetes resources
    node_pools: List[NodePoolInfo] = Field(default_factory=list)
    nodes: List[NodeInfo] = Field(default_factory=list)
    namespaces: List[NamespaceInfo] = Field(default_factory=list)
    workloads: List[WorkloadInfo] = Field(default_factory=list)
    services: List[ServiceInfo] = Field(default_factory=list)
    storage: List[StorageInfo] = Field(default_factory=list)
    ingresses: List[IngressInfo] = Field(default_factory=list)
    
    # Azure resources
    network_resources: List[NetworkResourceInfo] = Field(default_factory=list)
    azure_resources: List[AzureResourceInfo] = Field(default_factory=list)
    
    # Aggregated data
    properties: Dict[str, Any] = Field(default_factory=dict)
    raw_cost_data: CostData = Field(default_factory=CostData)
    utilization: ResourceMetrics = Field(default_factory=ResourceMetrics)
    
    @property
    def total_nodes(self) -> int:
        """Get total node count."""
        return len(self.nodes)
    
    @property
    def total_pods(self) -> int:
        """Get total pod count."""
        return sum(len(ns.pods) for ns in self.namespaces)
    
    @property
    def total_workloads(self) -> int:
        """Get total workload count."""
        return sum(len(ns.workloads) for ns in self.namespaces)
    
    @property
    def cluster_health(self) -> str:
        """Calculate cluster health status."""
        ready_nodes = sum(1 for node in self.nodes if node.is_ready)
        total_nodes = len(self.nodes)
        
        if total_nodes == 0:
            return "unknown"
        
        health_ratio = ready_nodes / total_nodes
        if health_ratio >= 0.9:
            return "healthy"
        elif health_ratio >= 0.7:
            return "warning"
        else:
            return "critical"


class DiscoveryTotals(BaseModel):
    """Discovery totals and aggregated information."""
    
    total_clusters: int = 0
    total_node_pools: int = 0
    total_nodes: int = 0
    total_namespaces: int = 0
    total_workloads: int = 0
    total_pods: int = 0
    total_services: int = 0
    total_storage_volumes: int = 0
    total_ingresses: int = 0
    total_network_resources: int = 0
    total_azure_resources: int = 0
    
    # Aggregated costs
    total_cost: CostData = Field(default_factory=CostData)
    cost_by_cluster: Dict[str, CostData] = Field(default_factory=dict)
    cost_by_namespace: Dict[str, CostData] = Field(default_factory=dict)
    cost_by_resource_type: Dict[str, CostData] = Field(default_factory=dict)
    
    # Aggregated utilization
    overall_utilization: ResourceMetrics = Field(default_factory=ResourceMetrics)
    utilization_by_cluster: Dict[str, ResourceMetrics] = Field(default_factory=dict)
    
    # Summary statistics
    discovery_timestamp: datetime = Field(default_factory=datetime.utcnow)
    discovery_duration_seconds: float = 0.0
    successful_discoveries: int = 0
    failed_discoveries: int = 0
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    
    @property
    def total_resources(self) -> int:
        """Calculate total resource count."""
        return (
            self.total_clusters + self.total_node_pools + self.total_nodes +
            self.total_namespaces + self.total_workloads + self.total_pods +
            self.total_services + self.total_storage_volumes + self.total_ingresses +
            self.total_network_resources + self.total_azure_resources
        )
    
    @property
    def discovery_success_rate(self) -> float:
        """Calculate discovery success rate."""
        total_attempts = self.successful_discoveries + self.failed_discoveries
        if total_attempts > 0:
            return (self.successful_discoveries / total_attempts) * 100
        return 0.0


class DiscoveryInfo(BaseModel):
    """Main discovery information model."""
    
    discovery_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    discovery_timestamp: datetime = Field(default_factory=datetime.utcnow)
    discovery_version: str = "1.0.0"
    
    # Main cluster information
    clusters: List[ClusterInfo] = Field(default_factory=list)
    
    # Aggregated totals
    totals: DiscoveryTotals = Field(default_factory=DiscoveryTotals)
    
    # Metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    def add_cluster(self, cluster: ClusterInfo) -> None:
        """Add a cluster to the discovery info."""
        self.clusters.append(cluster)
        self._update_totals()
    
    def _update_totals(self) -> None:
        """Update totals based on current cluster data."""
        self.totals.total_clusters = len(self.clusters)
        self.totals.total_node_pools = sum(len(cluster.node_pools) for cluster in self.clusters)
        self.totals.total_nodes = sum(len(cluster.nodes) for cluster in self.clusters)
        self.totals.total_namespaces = sum(len(cluster.namespaces) for cluster in self.clusters)
        self.totals.total_workloads = sum(cluster.total_workloads for cluster in self.clusters)
        self.totals.total_pods = sum(cluster.total_pods for cluster in self.clusters)
        self.totals.total_services = sum(len(cluster.services) for cluster in self.clusters)
        self.totals.total_storage_volumes = sum(len(cluster.storage) for cluster in self.clusters)
        self.totals.total_ingresses = sum(len(cluster.ingresses) for cluster in self.clusters)
        self.totals.total_network_resources = sum(len(cluster.network_resources) for cluster in self.clusters)
        self.totals.total_azure_resources = sum(len(cluster.azure_resources) for cluster in self.clusters)
        
        # Aggregate costs
        total_cost = 0.0
        for cluster in self.clusters:
            total_cost += cluster.raw_cost_data.total
            self.totals.cost_by_cluster[cluster.name] = cluster.raw_cost_data
        
        self.totals.total_cost.total = total_cost
    
    def get_cluster_by_name(self, name: str) -> Optional[ClusterInfo]:
        """Get cluster by name."""
        for cluster in self.clusters:
            if cluster.name == name:
                return cluster
        return None
    
    def get_cost_summary(self) -> Dict[str, Any]:
        """Get cost summary."""
        return {
            'total_cost': self.totals.total_cost.total,
            'currency': self.totals.total_cost.currency,
            'cost_by_cluster': {
                name: cost.total for name, cost in self.totals.cost_by_cluster.items()
            },
            'cost_breakdown': {
                'compute': self.totals.total_cost.compute,
                'storage': self.totals.total_cost.storage,
                'network': self.totals.total_cost.network,
                'monitoring': self.totals.total_cost.monitoring,
                'other': self.totals.total_cost.other
            }
        }
    
    def get_utilization_summary(self) -> Dict[str, Any]:
        """Get utilization summary."""
        return {
            'overall_cpu_usage': self.totals.overall_utilization.cpu_usage,
            'overall_memory_usage': self.totals.overall_utilization.memory_usage,
            'overall_storage_usage': self.totals.overall_utilization.storage_usage,
            'utilization_by_cluster': {
                name: {
                    'cpu_usage': util.cpu_usage,
                    'memory_usage': util.memory_usage,
                    'storage_usage': util.storage_usage
                }
                for name, util in self.totals.utilization_by_cluster.items()
            }
        }
    
    def get_efficiency_insights(self) -> Dict[str, Any]:
        """Get efficiency insights."""
        insights = {
            'clusters_analyzed': len(self.clusters),
            'underutilized_clusters': [],
            'overutilized_clusters': [],
            'cost_optimization_opportunities': [],
            'resource_optimization_opportunities': []
        }
        
        for cluster in self.clusters:
            cpu_usage = cluster.utilization.cpu_usage
            memory_usage = cluster.utilization.memory_usage
            
            if cpu_usage < 30 and memory_usage < 30:
                insights['underutilized_clusters'].append({
                    'name': cluster.name,
                    'cpu_usage': cpu_usage,
                    'memory_usage': memory_usage,
                    'potential_savings': cluster.raw_cost_data.total * 0.3
                })
            elif cpu_usage > 80 or memory_usage > 80:
                insights['overutilized_clusters'].append({
                    'name': cluster.name,
                    'cpu_usage': cpu_usage,
                    'memory_usage': memory_usage,
                    'scaling_needed': True
                })
            
            # Check for cost optimization opportunities
            if cluster.raw_cost_data.total > 1000:  # High cost threshold
                insights['cost_optimization_opportunities'].append({
                    'cluster': cluster.name,
                    'current_cost': cluster.raw_cost_data.total,
                    'recommendations': self._generate_cost_recommendations(cluster)
                })
        
        return insights
    
    def _generate_cost_recommendations(self, cluster: ClusterInfo) -> List[str]:
        """Generate cost optimization recommendations for a cluster."""
        recommendations = []
        
        # Check for spot instance opportunities
        spot_eligible_pools = [
            pool for pool in cluster.node_pools
            if not pool.is_spot_instance and pool.mode == NodePoolMode.USER
        ]
        if spot_eligible_pools:
            recommendations.append("Consider using spot instances for non-critical workloads")
        
        # Check for over-provisioned node pools
        underutilized_pools = [
            pool for pool in cluster.node_pools
            if pool.utilization.cpu_usage < 40 and pool.utilization.memory_usage < 40
        ]
        if underutilized_pools:
            recommendations.append("Scale down or consolidate underutilized node pools")
        
        # Check for auto-scaling opportunities
        non_autoscaling_pools = [
            pool for pool in cluster.node_pools
            if not pool.auto_scaling_enabled and pool.mode == NodePoolMode.USER
        ]
        if non_autoscaling_pools:
            recommendations.append("Enable auto-scaling for user node pools")
        
        # Check for storage optimization
        high_storage_cost = cluster.raw_cost_data.storage > (cluster.raw_cost_data.total * 0.3)
        if high_storage_cost:
            recommendations.append("Review storage classes and implement lifecycle policies")
        
        return recommendations
    
    def export_to_dict(self) -> Dict[str, Any]:
        """Export discovery info to dictionary format."""
        return {
            "discovery_info": {
                "discovery_id": self.discovery_id,
                "discovery_timestamp": self.discovery_timestamp.isoformat(),
                "discovery_version": self.discovery_version,
                "clusters": [
                    {
                        "basic_info": {
                            "id": cluster.id,
                            "name": cluster.name,
                            "resource_group": cluster.resource_group,
                            "location": cluster.location,
                            "kubernetes_version": cluster.kubernetes_version,
                            "provisioning_state": cluster.provisioning_state,
                            "fqdn": cluster.fqdn,
                            "node_resource_group": cluster.node_resource_group,
                            "dns_prefix": cluster.dns_prefix,
                            "enable_rbac": cluster.enable_rbac,
                            "created_time": cluster.created_time.isoformat() if cluster.created_time else None,
                            "last_modified": cluster.last_modified.isoformat() if cluster.last_modified else None,
                            "tags": cluster.tags
                        },
                        "properties": cluster.properties,
                        "raw_cost_data": cluster.raw_cost_data.dict(),
                        "node_pools": [
                            {
                                "config": {
                                    "name": pool.name,
                                    "vm_size": pool.vm_size,
                                    "os_type": pool.os_type,
                                    "os_disk_size_gb": pool.os_disk_size_gb,
                                    "count": pool.count,
                                    "min_count": pool.min_count,
                                    "max_count": pool.max_count,
                                    "auto_scaling_enabled": pool.auto_scaling_enabled,
                                    "mode": pool.mode,
                                    "orchestrator_version": pool.orchestrator_version,
                                    "provisioning_state": pool.provisioning_state,
                                    "power_state": pool.power_state,
                                    "max_pods": pool.max_pods,
                                    "os_disk_type": pool.os_disk_type,
                                    "scale_set_priority": pool.scale_set_priority,
                                    "spot_max_price": pool.spot_max_price,
                                    "node_taints": pool.node_taints,
                                    "node_labels": pool.node_labels,
                                    "availability_zones": pool.availability_zones,
                                    "upgrade_settings": pool.upgrade_settings,
                                    "tags": pool.tags
                                },
                                "scaling": {
                                    "current_count": pool.count,
                                    "min_count": pool.min_count,
                                    "max_count": pool.max_count,
                                    "auto_scaling_enabled": pool.auto_scaling_enabled,
                                    "scaling_history": pool.scaling_history
                                },
                                "cost_data": pool.cost_data.dict(),
                                "nodes": [
                                    {
                                        "name": node.name,
                                        "status": node.status,
                                        "roles": node.roles,
                                        "version": node.version,
                                        "os": node.os,
                                        "kernel": node.kernel,
                                        "container_runtime": node.container_runtime,
                                        "instance_type": node.instance_type,
                                        "zone": node.zone,
                                        "capacity": node.capacity,
                                        "allocatable": node.allocatable,
                                        "conditions": node.conditions,
                                        "created": node.created.isoformat() if node.created else None,
                                        "labels": node.labels,
                                        "annotations": node.annotations,
                                        "taints": node.taints,
                                        "utilization": node.utilization.dict(),
                                        "costs": node.cost_data.dict(),
                                        "workload_summary": node.workload_summary,
                                        "pod_count": node.pod_count
                                    }
                                    for node in pool.nodes
                                ]
                            }
                            for pool in cluster.node_pools
                        ],
                        "namespaces": [
                            {
                                "name": ns.name,
                                "status": ns.status,
                                "created": ns.created.isoformat() if ns.created else None,
                                "labels": ns.labels,
                                "annotations": ns.annotations,
                                "resource_quotas": ns.resource_quotas,
                                "limit_ranges": ns.limit_ranges,
                                "cost_allocation": ns.cost_data.dict(),
                                "workload_count": ns.workload_count,
                                "pod_count": ns.pod_count,
                                "is_system": ns.is_system_namespace
                            }
                            for ns in cluster.namespaces
                        ],
                        "workloads": [
                            {
                                "name": workload.name,
                                "namespace": workload.namespace,
                                "kind": workload.kind,
                                "replicas": workload.replicas,
                                "ready_replicas": workload.ready_replicas,
                                "available_replicas": workload.available_replicas,
                                "updated_replicas": workload.updated_replicas,
                                "created": workload.created.isoformat() if workload.created else None,
                                "labels": workload.labels,
                                "annotations": workload.annotations,
                                "selector": workload.selector,
                                "strategy": workload.strategy,
                                "specs": {
                                    "resource_requests": workload.resource_requests.dict(),
                                    "resource_limits": workload.resource_limits.dict()
                                },
                                "resources": workload.resource_requests.dict(),
                                "utilization": workload.utilization.dict(),
                                "costs": workload.cost_data.dict(),
                                "health_status": workload.health_status,
                                "efficiency_score": workload.efficiency_score,
                                "conditions": workload.conditions,
                                "scaling_history": workload.scaling_history,
                                "pods": [
                                    {
                                        "name": pod.name,
                                        "namespace": pod.namespace,
                                        "status": pod.status,
                                        "node": pod.node,
                                        "ip": pod.ip,
                                        "host_ip": pod.host_ip,
                                        "created": pod.created.isoformat() if pod.created else None,
                                        "started": pod.started.isoformat() if pod.started else None,
                                        "labels": pod.labels,
                                        "annotations": pod.annotations,
                                        "containers": [
                                            {
                                                "name": container.name,
                                                "image": container.image,
                                                "ready": container.ready,
                                                "restart_count": container.restart_count,
                                                "resources": container.resources.dict(),
                                                "status": container.status,
                                                "last_state": container.last_state,
                                                "environment_variables": container.environment_variables,
                                                "volume_mounts": container.volume_mounts
                                            }
                                            for container in pod.containers
                                        ],
                                        "init_containers": [
                                            {
                                                "name": container.name,
                                                "image": container.image,
                                                "ready": container.ready,
                                                "restart_count": container.restart_count,
                                                "resources": container.resources.dict(),
                                                "status": container.status
                                            }
                                            for container in pod.init_containers
                                        ],
                                        "resource_requests": pod.resource_requests.dict(),
                                        "resource_limits": pod.resource_limits.dict(),
                                        "utilization": pod.utilization.dict(),
                                        "costs": pod.cost_data.dict(),
                                        "owner_references": pod.owner_references,
                                        "conditions": pod.conditions,
                                        "qos_class": pod.qos_class,
                                        "phase": pod.phase,
                                        "age_days": pod.age_days
                                    }
                                    for pod in workload.pods
                                ]
                            }
                            for workload in cluster.workloads
                        ],
                        "services": [
                            {
                                "name": svc.name,
                                "namespace": svc.namespace,
                                "type": svc.type,
                                "cluster_ip": svc.cluster_ip,
                                "external_ips": svc.external_ips,
                                "ports": svc.ports,
                                "selector": svc.selector,
                                "created": svc.created.isoformat() if svc.created else None,
                                "labels": svc.labels,
                                "annotations": svc.annotations,
                                "endpoints": svc.endpoints,
                                "load_balancer_ingress": svc.load_balancer_ingress,
                                "costs": svc.cost_data.dict(),
                                "traffic_metrics": svc.traffic_metrics,
                                "is_load_balancer": svc.is_load_balancer
                            }
                            for svc in cluster.services
                        ],
                        "storage": [
                            {
                                "name": storage.name,
                                "namespace": storage.namespace,
                                "type": storage.type,
                                "capacity": storage.capacity,
                                "used": storage.used,
                                "available": storage.available,
                                "status": storage.status,
                                "storage_class": storage.storage_class,
                                "access_modes": storage.access_modes,
                                "reclaim_policy": storage.reclaim_policy,
                                "mount_options": storage.mount_options,
                                "created": storage.created.isoformat() if storage.created else None,
                                "labels": storage.labels,
                                "annotations": storage.annotations,
                                "claim_ref": storage.claim_ref,
                                "source": storage.source,
                                "utilization": storage.utilization.dict(),
                                "costs": storage.cost_data.dict(),
                                "backup_info": storage.backup_info,
                                "utilization_percentage": storage.utilization_percentage
                            }
                            for storage in cluster.storage
                        ],
                        "ingresses": [
                            {
                                "name": ing.name,
                                "namespace": ing.namespace,
                                "ingress_class": ing.ingress_class,
                                "rules": ing.rules,
                                "tls": ing.tls,
                                "load_balancer_ingress": ing.load_balancer_ingress,
                                "created": ing.created.isoformat() if ing.created else None,
                                "labels": ing.labels,
                                "annotations": ing.annotations,
                                "backend_services": ing.backend_services,
                                "costs": ing.cost_data.dict(),
                                "traffic_metrics": ing.traffic_metrics,
                                "ssl_cert_info": ing.ssl_cert_info,
                                "has_tls": ing.has_tls
                            }
                            for ing in cluster.ingresses
                        ],
                        "network_resources": [
                            {
                                "name": net.name,
                                "resource_group": net.resource_group,
                                "type": net.type,
                                "location": net.location,
                                "sku": net.sku,
                                "ip_address": net.ip_address,
                                "allocation_method": net.allocation_method,
                                "dns_settings": net.dns_settings,
                                "created": net.created.isoformat() if net.created else None,
                                "tags": net.tags,
                                "associated_resources": net.associated_resources,
                                "configuration": net.configuration,
                                "costs": net.cost_data.dict(),
                                "utilization": net.utilization.dict(),
                                "is_public_ip": net.is_public_ip
                            }
                            for net in cluster.network_resources
                        ],
                        "azure_resources": [
                            {
                                "name": azure.name,
                                "resource_group": azure.resource_group,
                                "type": azure.type,
                                "location": azure.location,
                                "sku": azure.sku,
                                "tier": azure.tier,
                                "kind": azure.kind,
                                "created": azure.created.isoformat() if azure.created else None,
                                "tags": azure.tags,
                                "properties": azure.properties,
                                "costs": azure.cost_data.dict(),
                                "utilization": azure.utilization.dict(),
                                "compliance_status": azure.compliance_status,
                                "is_storage_account": azure.is_storage_account
                            }
                            for azure in cluster.azure_resources
                        ],
                        "cluster_summary": {
                            "total_nodes": cluster.total_nodes,
                            "total_pods": cluster.total_pods,
                            "total_workloads": cluster.total_workloads,
                            "cluster_health": cluster.cluster_health,
                            "network_profile": cluster.network_profile,
                            "addon_profiles": cluster.addon_profiles,
                            "sku": cluster.sku,
                            "overall_utilization": cluster.utilization.dict()
                        }
                    }
                    for cluster in self.clusters
                ],
                "totals": {
                    "aggregated_costs": self.totals.total_cost.dict(),
                    "counts": {
                        "total_clusters": self.totals.total_clusters,
                        "total_node_pools": self.totals.total_node_pools,
                        "total_nodes": self.totals.total_nodes,
                        "total_namespaces": self.totals.total_namespaces,
                        "total_workloads": self.totals.total_workloads,
                        "total_pods": self.totals.total_pods,
                        "total_services": self.totals.total_services,
                        "total_storage_volumes": self.totals.total_storage_volumes,
                        "total_ingresses": self.totals.total_ingresses,
                        "total_network_resources": self.totals.total_network_resources,
                        "total_azure_resources": self.totals.total_azure_resources,
                        "total_resources": self.totals.total_resources
                    },
                    "utilization": self.totals.overall_utilization.dict(),
                    "cost_by_cluster": {
                        name: cost.dict() for name, cost in self.totals.cost_by_cluster.items()
                    },
                    "cost_by_namespace": {
                        name: cost.dict() for name, cost in self.totals.cost_by_namespace.items()
                    },
                    "cost_by_resource_type": {
                        name: cost.dict() for name, cost in self.totals.cost_by_resource_type.items()
                    },
                    "utilization_by_cluster": {
                        name: util.dict() for name, util in self.totals.utilization_by_cluster.items()
                    },
                    "discovery_metadata": {
                        "discovery_timestamp": self.totals.discovery_timestamp.isoformat(),
                        "discovery_duration_seconds": self.totals.discovery_duration_seconds,
                        "successful_discoveries": self.totals.successful_discoveries,
                        "failed_discoveries": self.totals.failed_discoveries,
                        "discovery_success_rate": self.totals.discovery_success_rate,
                        "warnings": self.totals.warnings,
                        "errors": self.totals.errors
                    }
                }
            }
        }


# Example usage and utility functions
def create_sample_discovery_info() -> DiscoveryInfo:
    """Create a sample discovery info for testing."""
    discovery = DiscoveryInfo()
    
    # Create sample cluster
    cluster = ClusterInfo(
        id="/subscriptions/123/resourceGroups/test-rg/providers/Microsoft.ContainerService/managedClusters/test-cluster",
        name="test-cluster",
        resource_group="test-rg",
        location="eastus",
        kubernetes_version="1.25.2",
        provisioning_state="Succeeded",
        fqdn="test-cluster.hcp.eastus.azmk8s.io",
        dns_prefix="test-cluster",
        enable_rbac=True
    )
    
    # Add sample node pool
    node_pool = NodePoolInfo(
        name="nodepool1",
        cluster_name="test-cluster",
        vm_size="Standard_D2s_v3",
        count=3,
        min_count=1,
        max_count=5,
        auto_scaling_enabled=True,
        mode=NodePoolMode.SYSTEM
    )
    
    # Add sample nodes
    for i in range(3):
        node = NodeInfo(
            name=f"node-{i}",
            status=ResourceStatus.ACTIVE,
            roles=["worker"],
            version="1.25.2",
            instance_type="Standard_D2s_v3",
            zone=f"eastus-{i+1}",
            capacity={"cpu": "2", "memory": "7Gi"},
            allocatable={"cpu": "1900m", "memory": "6Gi"}
        )
        node_pool.nodes.append(node)
    
    cluster.node_pools.append(node_pool)
    
    # Add sample namespace
    namespace = NamespaceInfo(
        name="default",
        status=ResourceStatus.ACTIVE,
        labels={"name": "default"}
    )
    
    # Add sample workload
    workload = WorkloadInfo(
        name="nginx-deployment",
        namespace="default",
        kind="Deployment",
        replicas=3,
        ready_replicas=3,
        available_replicas=3
    )
    
    # Add sample pods
    for i in range(3):
        pod = PodInfo(
            name=f"nginx-deployment-{i}",
            namespace="default",
            status=ResourceStatus.ACTIVE,
            node=f"node-{i % 3}",
            ip=f"10.244.{i}.{i+1}"
        )
        workload.pods.append(pod)
    
    namespace.workloads.append(workload)
    cluster.namespaces.append(namespace)
    
    # Add sample service
    service = ServiceInfo(
        name="nginx-service",
        namespace="default",
        type=ServiceType.LOAD_BALANCER,
        cluster_ip="10.0.0.100",
        ports=[{"port": 80, "target_port": 80, "protocol": "TCP"}]
    )
    cluster.services.append(service)
    
    discovery.add_cluster(cluster)
    return discovery


def validate_discovery_info(discovery: DiscoveryInfo) -> List[str]:
    """Validate discovery info and return list of validation errors."""
    errors = []
    
    if not discovery.clusters:
        errors.append("No clusters found in discovery")
    
    for cluster in discovery.clusters:
        if not cluster.name:
            errors.append(f"Cluster {cluster.id} has no name")
        
        if not cluster.node_pools:
            errors.append(f"Cluster {cluster.name} has no node pools")
        
        for node_pool in cluster.node_pools:
            if node_pool.count != len(node_pool.nodes):
                errors.append(f"Node pool {node_pool.name} count mismatch: {node_pool.count} vs {len(node_pool.nodes)}")
        
        for namespace in cluster.namespaces:
            if namespace.workload_count != len(namespace.workloads):
                errors.append(f"Namespace {namespace.name} workload count mismatch")
    
    return errors


def generate_cost_report(discovery: DiscoveryInfo) -> Dict[str, Any]:
    """Generate a cost report from discovery info."""
    report = {
        "total_cost": discovery.totals.total_cost.total,
        "currency": discovery.totals.total_cost.currency,
        "clusters": [],
        "cost_breakdown": discovery.get_cost_summary(),
        "optimization_opportunities": []
    }
    
    for cluster in discovery.clusters:
        cluster_report = {
            "name": cluster.name,
            "cost": cluster.raw_cost_data.total,
            "node_pools": [],
            "namespaces": []
        }
        
        for node_pool in cluster.node_pools:
            cluster_report["node_pools"].append({
                "name": node_pool.name,
                "cost": node_pool.cost_data.total,
                "nodes": node_pool.count,
                "vm_size": node_pool.vm_size,
                "auto_scaling": node_pool.auto_scaling_enabled
            })
        
        for namespace in cluster.namespaces:
            cluster_report["namespaces"].append({
                "name": namespace.name,
                "cost": namespace.cost_data.total,
                "workloads": namespace.workload_count,
                "pods": namespace.pod_count
            })
        
        report["clusters"].append(cluster_report)
    
    return report


# Export all models
__all__ = [
    "DiscoveryInfo",
    "ClusterInfo",
    "NodePoolInfo",
    "NodeInfo",
    "NamespaceInfo",
    "WorkloadInfo",
    "PodInfo",
    "ServiceInfo",
    "StorageInfo",
    "IngressInfo",
    "NetworkResourceInfo",
    "AzureResourceInfo",
    "ContainerInfo",
    "CostData",
    "ResourceMetrics",
    "ResourceRequests",
    "DiscoveryTotals",
    "ResourceStatus",
    "NodePoolMode",
    "ServiceType",
    "VolumeType",
    "create_sample_discovery_info",
    "validate_discovery_info",
    "generate_cost_report"
]