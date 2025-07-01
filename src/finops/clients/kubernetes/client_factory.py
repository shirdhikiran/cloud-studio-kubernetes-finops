"""Kubernetes client factory."""

from typing import Dict, Any, Optional
import structlog
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from finops.core.exceptions import ClientConnectionException, ConfigurationException
from .k8s_client import KubernetesClient

logger = structlog.get_logger(__name__)


class KubernetesClientFactory:
    """Factory for creating Kubernetes clients."""
    
    def __init__(self, config_dict: Dict[str, Any]):
        self.config = config_dict
        self.kubeconfig_path = config_dict.get("kubeconfig_path")
        self.context = config_dict.get("context")
        self.namespace = config_dict.get("namespace", "default")
        
        self.logger = logger.bind(factory="kubernetes")
    
    def create_client(self, kubeconfig_data: Optional[bytes] = None) -> KubernetesClient:
        """Create Kubernetes client."""
        return KubernetesClient(
            config=self.config,
            kubeconfig_path=self.kubeconfig_path,
            context=self.context,
            kubeconfig_data=kubeconfig_data
        )
    
    def create_client_from_cluster_credentials(self, kubeconfig_data: bytes) -> KubernetesClient:
        """Create client from AKS cluster credentials."""
        return self.create_client(kubeconfig_data=kubeconfig_data)