"""Enhanced discovery orchestrator specifically for Phase 1 comprehensive discovery."""

import asyncio
from typing import Dict, Any, List, Optional
import structlog
from datetime import datetime, timezone

from finops.discovery.base import BaseDiscoveryService
from finops.discovery.discovery_service import DiscoveryService
from finops.discovery.discovery_client import DiscoveryClient
from finops.clients.azure.client_factory import AzureClientFactory
from finops.clients.kubernetes.client_factory import KubernetesClientFactory
from finops.core.exceptions import DiscoveryException
from finops.data.storage.file_storage import FileStorage

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
    
    async def run_comprehensive_discovery(self) -> Dict[str, Any]:
        """
        Run the complete Phase 1 discovery process.
        
        Returns:
            Complete discovery data in the specified format
        """
        if not self.discovery_service:
            raise DiscoveryException("Orchestrator", "Orchestrator not initialized")
        
        start_time = datetime.now(timezone.utc)
        self.logger.info("Starting comprehensive Phase 1 discovery")
        
        try:
            # Run comprehensive discovery
            discovery_result = await self.discovery_service.discover_with_metadata()
            
            if discovery_result["status"] != "success":
                raise DiscoveryException("Discovery", f"Discovery failed: {discovery_result.get('error')}")
            
            discovery_data = discovery_result["data"]
            
           
            # Create final orchestrator response
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            
            final_result = {
                "discovery_data": discovery_data
            }
            
            self.logger.info(
                "Comprehensive Phase 1 discovery completed successfully",
                duration_seconds=duration,
                clusters_discovered=discovery_data.get("totals", {}).get("total_clusters", 0),
                total_estimated_cost=discovery_data.get("totals", {}).get("aggregated_costs", {}).get("total_cost", 0)
            )
            
            return final_result
            
        except Exception as e:
            self.logger.error("Comprehensive discovery failed", error=str(e))
            raise DiscoveryException("Discovery", f"Comprehensive discovery failed: {e}")
    
    async def save_discovery_results(self, discovery_data: Dict[str, Any], storage: FileStorage) -> None:
        """Save discovery results to storage with proper organization."""
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        
        try:
            # Save complete discovery data
            await storage.save("phase1_discovery", f"comprehensive_{timestamp}", discovery_data)
            await storage.save("phase1_discovery", "latest_comprehensive", discovery_data)
            
            # Save summary report separately for quick access
            summary_report = discovery_data.get("summary_report", {})
            await storage.save("phase1_discovery", f"summary_report_{timestamp}", summary_report)
            await storage.save("phase1_discovery", "latest_summary", summary_report)
            
            # Save just the discovery data (without orchestrator metadata) for phase 2 input
            pure_discovery_data = discovery_data.get("discovery_data", {})
            await storage.save("phase1_discovery", "discovery_data_for_phase2", pure_discovery_data)
            
            # Save cluster-specific data for individual analysis
            clusters_data = pure_discovery_data.get("clusters", [])
            for i, cluster in enumerate(clusters_data):
                cluster_name = cluster.get("basic_info", {}).get("name", f"cluster_{i}")
                await storage.save("phase1_discovery", f"cluster_{cluster_name}_{timestamp}", cluster)
            
            self.logger.info(
                "Discovery results saved successfully",
                clusters_saved=len(clusters_data),
                storage_base=str(storage.base_path)
            )
            
        except Exception as e:
            self.logger.error("Failed to save discovery results", error=str(e))
            raise
    
    async def validate_discovery_output(self, discovery_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the discovery output format and completeness."""
        validation_result = {
            "format_valid": True,
            "completeness_score": 0,
            "validation_errors": [],
            "validation_warnings": [],
            "recommendations": []
        }
        
        try:
            # Check required top-level structure
            required_sections = ["discovery_data", "summary_report", "orchestrator_metadata"]
            for section in required_sections:
                if section not in discovery_data:
                    validation_result["validation_errors"].append(f"Missing required section: {section}")
                    validation_result["format_valid"] = False
            
            # Validate discovery data structure
            discovery_section = discovery_data.get("discovery_data", {})
            if discovery_section:
                # Check required discovery data fields
                required_discovery_fields = ["discovery_timestamp", "discovery_scope", "clusters", "totals"]
                for field in required_discovery_fields:
                    if field not in discovery_section:
                        validation_result["validation_errors"].append(f"Missing discovery data field: {field}")
                
                # Validate clusters data
                clusters = discovery_section.get("clusters", [])
                if not clusters:
                    validation_result["validation_warnings"].append("No clusters discovered")
                else:
                    for i, cluster in enumerate(clusters):
                        cluster_validation = self._validate_cluster_data(cluster, i)
                        validation_result["validation_warnings"].extend(cluster_validation)
                
                # Calculate completeness score
                validation_result["completeness_score"] = self._calculate_completeness_score(discovery_section)
            
            # Generate recommendations based on validation
            validation_result["recommendations"] = self._generate_validation_recommendations(validation_result)
            
        except Exception as e:
            validation_result["validation_errors"].append(f"Validation process failed: {str(e)}")
            validation_result["format_valid"] = False
        
        return validation_result
    
    def _validate_cluster_data(self, cluster: Dict[str, Any], cluster_index: int) -> List[str]:
        """Validate individual cluster data."""
        warnings = []
        
        # Check basic info
        if "basic_info" not in cluster:
            warnings.append(f"Cluster {cluster_index}: Missing basic_info")
        
        # Check discovery status
        if cluster.get("discovery_status") != "completed":
            warnings.append(f"Cluster {cluster_index}: Discovery not completed - status: {cluster.get('discovery_status')}")
        
        # Check for cost data
        if not cluster.get("cost_data"):
            warnings.append(f"Cluster {cluster_index}: Missing cost data")
        
        # Check for utilization data
        if not cluster.get("utilization_summary"):
            warnings.append(f"Cluster {cluster_index}: Missing utilization data")
        
        # Check for node pools
        if not cluster.get("node_pools"):
            warnings.append(f"Cluster {cluster_index}: Missing node pools data")
        
        return warnings
    
    def _calculate_completeness_score(self, discovery_data: Dict[str, Any]) -> float:
        """Calculate completeness score (0-100)."""
        score = 0
        max_score = 100
        
        # Basic discovery (20 points)
        if discovery_data.get("clusters"):
            score += 20
        
        # Cost data completeness (25 points)
        clusters_with_costs = sum(1 for c in discovery_data.get("clusters", [])
                                if c.get("cost_data", {}).get("total", 0) > 0)
        total_clusters = len(discovery_data.get("clusters", []))
        if total_clusters > 0:
            score += (clusters_with_costs / total_clusters) * 25
        
        # Utilization data completeness (25 points)
        clusters_with_utilization = sum(1 for c in discovery_data.get("clusters", [])
                                      if c.get("utilization_summary", {}).get("metrics_availability"))
        if total_clusters > 0:
            score += (clusters_with_utilization / total_clusters) * 25
        
        # Kubernetes data completeness (15 points)
        clusters_with_k8s_data = sum(1 for c in discovery_data.get("clusters", [])
                                   if c.get("namespaces"))
        if total_clusters > 0:
            score += (clusters_with_k8s_data / total_clusters) * 15
        
        # Azure resources data (10 points)
        clusters_with_azure_resources = sum(1 for c in discovery_data.get("clusters", [])
                                          if c.get("azure_resources"))
        if total_clusters > 0:
            score += (clusters_with_azure_resources / total_clusters) * 10
        
        # Totals aggregation (5 points)
        if discovery_data.get("totals", {}).get("total_clusters", 0) == total_clusters:
            score += 5
        
        return round(score, 2)
    
    def _generate_validation_recommendations(self, validation_result: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        if not validation_result["format_valid"]:
            recommendations.append("Fix format validation errors before proceeding to Phase 2")
        
        if validation_result["completeness_score"] < 70:
            recommendations.append("Improve data collection completeness before proceeding")
        
        if validation_result["validation_warnings"]:
            recommendations.append("Review and address validation warnings for better data quality")
        
        if validation_result["completeness_score"] >= 80:
            recommendations.append("Data quality is sufficient to proceed to Phase 2: Analytics")
        
        return recommendations
    
    async def cleanup(self) -> None:
        """Cleanup all clients and resources."""
        self.logger.info("Cleaning up Enhanced Discovery Orchestrator")
        
        try:
            if self.enhanced_client:
                await self.enhanced_client.disconnect()
            
            if self.k8s_factory and hasattr(self, 'k8s_client'):
                # Cleanup K8s client if created separately
                pass
                
        except Exception as e:
            self.logger.warning("Error during cleanup", error=str(e))
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()