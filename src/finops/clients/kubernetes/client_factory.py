# src/finops/clients/kubernetes/client_factory.py
"""Fixed Kubernetes client factory."""

from typing import Dict, Any, Optional
import structlog

from finops.core.exceptions import ConfigurationException
from .k8s_client import KubernetesClient

logger = structlog.get_logger(__name__)


class KubernetesClientFactory:
    """Factory for creating Kubernetes clients."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.kubeconfig_path = config.get("kubeconfig_path")
        self.context = config.get("context")
        self.namespace = config.get("namespace", "default")
        
        self.logger = logger.bind(factory="kubernetes")
    
    def create_client(self, kubeconfig_data: Optional[bytes] = None) -> KubernetesClient:
        """Create Kubernetes client with correct parameters."""
        return KubernetesClient(
            config_dict=self.config,  # Use config_dict parameter name
            kubeconfig_path=self.kubeconfig_path,
            context=self.context,
            kubeconfig_data=kubeconfig_data
        )
    
    def create_client_from_cluster_credentials(self, kubeconfig_data: bytes) -> KubernetesClient:
        """Create client from AKS cluster credentials."""
        return self.create_client(kubeconfig_data=kubeconfig_data)