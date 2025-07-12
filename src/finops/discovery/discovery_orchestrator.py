# src/finops/discovery/discovery_orchestrator.py
"""Fixed discovery orchestrator for coordinating the entire discovery process."""

import asyncio
from typing import Dict, Any, Optional
import structlog
from datetime import datetime, timezone

from finops.discovery.discovery_service import DiscoveryService
from finops.discovery.discovery_client import DiscoveryClient
from finops.clients.azure.client_factory import AzureClientFactory
from finops.clients.kubernetes.client_factory import KubernetesClientFactory
from finops.core.exceptions import DiscoveryException

logger = structlog.get_logger(__name__)


class DiscoveryOrchestrator:
    """
    Enhanced orchestrator for Phase 1 comprehensive discovery.
    
    This orchestrator coordinates the comprehensive discovery process,
    managing all required clients and producing the complete Phase 1 output.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.azure_config = config.get("azure", {})
        self.k8s_config = config.get("kubernetes", {})
        self.discovery_config = config.get("discovery", {})
        
        # Client factories
        self.azure_factory: Optional[AzureClientFactory] = None
        self.k8s_factory: Optional[KubernetesClientFactory] = None
        
        # Enhanced client
        self.enhanced_client: Optional[DiscoveryClient] = None
        
        # Discovery service
        self.discovery_service: Optional[DiscoveryService] = None
        
        self.logger = logger.bind(orchestrator="enhanced_discovery")
        
    async def initialize(self) -> None:
        """Initialize all clients and services."""
        self.logger.info("Initializing Enhanced Discovery Orchestrator")
        import pdb; pdb.set_trace()
        try:
            # Initialize Azure clients
            self.azure_factory = AzureClientFactory(self.azure_config)
            azure_clients = await self.azure_factory.create_all_clients()
            
            # Initialize Kubernetes client if configured
            k8s_client = None
            if self.k8s_config.get("kubeconfig_path"):
                self.k8s_factory = KubernetesClientFactory(self.k8s_config)
                k8s_client = self.k8s_factory.create_client()
                await k8s_client.connect()
            
            # Initialize enhanced discovery client
            self.enhanced_client = DiscoveryClient(self.azure_config)
            self.enhanced_client.set_clients(
                aks_client=azure_clients["aks"],
                cost_client=azure_clients["cost"], 
                monitor_client=azure_clients["monitor"],
                k8s_client=k8s_client
            )
            await self.enhanced_client.connect()
            
            # Initialize comprehensive discovery service
            self.discovery_service = DiscoveryService(
                self.enhanced_client,
                self.discovery_config
            )
            
            self.logger.info("Enhanced Discovery Orchestrator initialized successfully")
            
        except Exception as e:
            self.logger.error("Failed to initialize Enhanced Discovery Orchestrator", error=str(e))
            raise DiscoveryException("Orchestrator", f"Initialization failed: {e}")
    
    async def run_discovery(self) -> Dict[str, Any]:
        """Run the complete discovery process."""
        if not self.discovery_service:
            raise DiscoveryException("Orchestrator", "Orchestrator not initialized")
        
        start_time = datetime.now(timezone.utc)
        self.logger.info("Starting comprehensive discovery")
        
        try:
            # Run discovery
            discovery_result = await self.discovery_service.discover_with_metadata()
            
            if discovery_result["status"] != "success":
                raise DiscoveryException("Discovery", f"Discovery failed: {discovery_result.get('error')}")
            
            discovery_data = discovery_result["data"]
            
            # Add orchestrator metadata
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            
            final_result = {
                "discovery_data": discovery_data,
                "orchestrator_metadata": {
                    "orchestrator_version": "1.0.0",
                    "discovery_start_time": start_time.isoformat(),
                    "discovery_end_time": end_time.isoformat(),
                    "discovery_duration_seconds": duration,
                    "resource_group": self.azure_config.get("resource_group"),
                    "subscription_id": self.azure_config.get("subscription_id")
                }
            }
            
            self.logger.info(
                "Comprehensive discovery completed successfully",
                duration_seconds=duration,
                clusters_discovered=discovery_data.get("summary", {}).get("total_clusters", 0),
                total_cost=discovery_data.get("summary", {}).get("total_cost", 0)
            )
            
            return final_result
            
        except Exception as e:
            self.logger.error("Discovery failed", error=str(e))
            raise DiscoveryException("Discovery", f"Discovery failed: {e}")
    
    async def cleanup(self) -> None:
        """Cleanup all clients and resources."""
        self.logger.info("Cleaning up Discovery Orchestrator")
        
        try:
            if self.discovery_client:
                await self.discovery_client.disconnect()
            
            if self.azure_factory:
                await self.azure_factory.disconnect_all()
                
        except Exception as e:
            self.logger.warning("Error during cleanup", error=str(e))
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()