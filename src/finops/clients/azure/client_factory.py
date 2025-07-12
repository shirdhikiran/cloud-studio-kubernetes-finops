# src/finops/clients/azure/client_factory.py
"""Fixed Azure client factory for creating and managing Azure service clients."""

from typing import Dict, Any, Optional
import structlog
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.core.exceptions import ClientAuthenticationError

from finops.core.exceptions import ClientConnectionException, ConfigurationException
from .aks_client import AKSClient
from .cost_client import CostClient
from .monitor_client import MonitorClient

logger = structlog.get_logger(__name__)


class AzureClientFactory:
    """Factory for creating Azure service clients."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.subscription_id = config.get("subscription_id")
        self.tenant_id = config.get("tenant_id")
        self.client_id = config.get("client_id")
        self.client_secret = config.get("client_secret")
        
        if not self.subscription_id:
            raise ConfigurationException("Azure subscription_id is required")
        
        self._credential = None
        self._clients = {}
        self.logger = logger.bind(factory="azure")
    
    def _get_credential(self):
        """Get Azure credential based on configuration."""
        if self._credential:
            return self._credential
        
        try:
            if self.client_id and self.client_secret and self.tenant_id:
                # Use service principal authentication
                self._credential = ClientSecretCredential(
                    tenant_id=self.tenant_id,
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )
                self.logger.info("Using service principal authentication")
            else:
                # Use default credential chain (managed identity, CLI, etc.)
                self._credential = DefaultAzureCredential()
                self.logger.info("Using default credential chain")
            
            return self._credential
            
        except Exception as e:
            raise ClientConnectionException("Azure", f"Failed to create credential: {e}")
    
    def create_aks_client(self) -> AKSClient:
        """Create AKS client."""
        credential = self._get_credential()
        return AKSClient(
            credential=credential,
            subscription_id=self.subscription_id,
            config=self.config
        )
    
    def create_cost_client(self) -> CostClient:
        """Create Cost Management client."""
        credential = self._get_credential()
        return CostClient(
            credential=credential,
            subscription_id=self.subscription_id,
            config=self.config
        )
    
    def create_monitor_client(self) -> MonitorClient:
        """Create Azure Monitor client."""
        credential = self._get_credential()
        return MonitorClient(
            credential=credential,
            subscription_id=self.subscription_id,
            config=self.config
        )
    
    async def create_all_clients(self) -> Dict[str, Any]:
        """Create all Azure clients."""
        try:
            # Create all clients
            aks_client = self.create_aks_client()
            cost_client = self.create_cost_client()
            monitor_client = self.create_monitor_client()
            
            clients = {
                "aks": aks_client,
                "cost": cost_client,
                "monitor": monitor_client
            }
            
            # Connect all clients
            for name, client in clients.items():
                try:
                    await client.connect()
                    self.logger.info(f"Connected {name} client successfully")
                except Exception as e:
                    self.logger.error(f"Failed to connect {name} client", error=str(e))
                    raise ClientConnectionException(name, str(e))
            
            # Store clients for cleanup
            self._clients = clients
            return clients
            
        except Exception as e:
            self.logger.error("Failed to create all clients", error=str(e))
            raise ClientConnectionException("AzureFactory", f"Failed to create clients: {e}")
    
    async def disconnect_all(self) -> None:
        """Disconnect all clients (for cleanup)."""
        for name, client in self._clients.items():
            try:
                await client.disconnect()
                self.logger.info(f"Disconnected {name} client successfully")
            except Exception as e:
                self.logger.warning(f"Error disconnecting {name} client: {e}")
        
        self._clients = {}
        self.logger.info("Azure client factory cleanup completed")