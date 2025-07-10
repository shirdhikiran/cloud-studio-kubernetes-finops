"""Resource data mapping utilities."""

from typing import Dict, Any, List
from finops.models.discovery_models import ResourceMetrics, ResourceRequests
import structlog

logger = structlog.get_logger(__name__)


class ResourceDataMapper:
    """Maps resource data from various sources to standardized format."""
    
    def map_kubernetes_resources(self, k8s_resources: Dict[str, Any]) -> ResourceMetrics:
        """Map Kubernetes resource data to ResourceMetrics model."""
        return ResourceMetrics(
            cpu_usage=k8s_resources.get('cpu_usage', 0.0),
            memory_usage=k8s_resources.get('memory_usage', 0.0),
            storage_usage=k8s_resources.get('storage_usage', 0.0),
            network_in=k8s_resources.get('network_in', 0.0),
            network_out=k8s_resources.get('network_out', 0.0),
            peak_cpu=k8s_resources.get('peak_cpu', 0.0),
            peak_memory=k8s_resources.get('peak_memory', 0.0),
            avg_cpu=k8s_resources.get('avg_cpu', 0.0),
            avg_memory=k8s_resources.get('avg_memory', 0.0)
        )
    
    def map_resource_requests(self, resource_data: Dict[str, Any]) -> ResourceRequests:
        """Map resource requests/limits data."""
        return ResourceRequests(
            cpu_request=self._parse_cpu(resource_data.get('cpu_request', '0')),
            memory_request=self._parse_memory(resource_data.get('memory_request', '0')),
            storage_request=self._parse_memory(resource_data.get('storage_request', '0')),
            cpu_limit=self._parse_cpu(resource_data.get('cpu_limit', '0')),
            memory_limit=self._parse_memory(resource_data.get('memory_limit', '0')),
            storage_limit=self._parse_memory(resource_data.get('storage_limit', '0'))
        )
    
    def _parse_cpu(self, cpu_str: str) -> float:
        """Parse CPU string to millicores."""
        if not cpu_str or cpu_str == '0':
            return 0.0
        
        if cpu_str.endswith('m'):
            return float(cpu_str[:-1])
        return float(cpu_str) * 1000
    
    def _parse_memory(self, memory_str: str) -> float:
        """Parse memory string to bytes."""
        if not memory_str or memory_str == '0':
            return 0.0
        
        units = {
            'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3,
            'K': 1000, 'M': 1000**2, 'G': 1000**3
        }
        
        for unit, multiplier in units.items():
            if memory_str.endswith(unit):
                return float(memory_str[:-len(unit)]) * multiplier
        
        return float(memory_str)