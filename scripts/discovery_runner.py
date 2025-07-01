"""Discovery runner script for scheduled execution."""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from finops.config.settings import Settings
from finops.core.utils import setup_logging
from finops.discovery.orchestrator import DiscoveryOrchestrator
from finops.clients.azure.client_factory import AzureClientFactory
from finops.data.storage.file_storage import FileStorage
import structlog

logger = structlog.get_logger(__name__)


async def main():
    """Main discovery runner function."""
    try:
        # Load configuration
        settings = Settings.create_from_env()
        
        # Setup logging
        setup_logging(log_level=settings.log_level.value)
        
        logger.info("Starting scheduled discovery run")
        
        # Initialize Azure clients
        azure_config = settings.azure.model_dump()
        azure_factory = AzureClientFactory(azure_config)
        azure_clients = await azure_factory.create_all_clients()

        
        
        # Initialize storage
        storage = FileStorage(settings.storage.base_path)
        
        # Create discovery services
        from finops.discovery.infrastructure.aks_discovery import AKSDiscoveryService
        from finops.discovery.infrastructure.node_pool_discovery import NodePoolDiscoveryService
        from finops.discovery.infrastructure.network_discovery import NetworkDiscoveryService
        from finops.discovery.infrastructure.storage_discovery import StorageDiscoveryService
        from finops.discovery.cost.cost_discovery import CostDiscoveryService
        
        services = [
            AKSDiscoveryService(azure_clients['aks'], azure_config),
            NodePoolDiscoveryService(azure_clients['aks'], azure_config),
            NetworkDiscoveryService(azure_clients['resource'], azure_config),
            StorageDiscoveryService(azure_clients['resource'], azure_config),
            CostDiscoveryService(azure_clients['cost'], azure_config),
        ]
        
        # Add Kubernetes discovery if kubeconfig is available
        
        if settings.kubernetes.kubeconfig_path and Path(settings.kubernetes.kubeconfig_path).exists():
            from finops.clients.kubernetes.client_factory import KubernetesClientFactory
            from finops.discovery.kubernetes.resource_discovery import ResourceDiscoveryService
            
            k8s_factory = KubernetesClientFactory(settings.kubernetes.model_dump())
            k8s_client = k8s_factory.create_client()
            
            k8s_service = ResourceDiscoveryService(k8s_client, settings.kubernetes.model_dump())
            services.append(k8s_service)
        
        # Run discovery
        orchestrator = DiscoveryOrchestrator(
            services=services,
            max_concurrency=settings.discovery.parallel_workers,
            timeout_seconds=settings.discovery.timeout_seconds
        )
        
        results = await orchestrator.run_parallel_discovery()
        
        # Save results with timestamp
        from datetime import datetime
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        
        await storage.save("discovery_results", f"run_{timestamp}", results)
        await storage.save("discovery_results", "latest", results)
        
        # Log summary
        summary = results.get('summary', {})
        logger.info(
            "Discovery run completed",
            total_resources=summary.get('total_resources', 0),
            successful_discoveries=summary.get('successful_discoveries', 0),
            failed_discoveries=summary.get('failed_discoveries', 0),
            duration=results.get('orchestrator_metadata', {}).get('duration_seconds', 0)
        )
        
        # Cleanup old discovery results (keep last 10)
        await cleanup_old_results(storage)
        
    except Exception as e:
        logger.error("Discovery run failed", error=str(e))
        sys.exit(1)
    
    finally:
        # Cleanup clients
        if 'azure_clients' in locals():
            for client in azure_clients.values():
                try:
                    await client.disconnect()
                except:
                    pass


async def cleanup_old_results(storage: FileStorage):
    """Clean up old discovery results."""
    try:
        collection_path = storage.base_path / "discovery_results"
        if not collection_path.exists():
            return
        
        # Get all result files (excluding 'latest')
        result_files = [
            f for f in collection_path.glob("run_*.json")
        ]
        
        # Sort by modification time (newest first)
        result_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        # Keep only the 10 most recent
        files_to_delete = result_files[10:]
        
        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                logger.debug(f"Cleaned up old result file: {file_path.name}")
            except Exception as e:
                logger.warning(f"Failed to delete {file_path.name}", error=str(e))
        
        if files_to_delete:
            logger.info(f"Cleaned up {len(files_to_delete)} old discovery result files")
            
    except Exception as e:
        logger.error("Failed to cleanup old results", error=str(e))


if __name__ == "__main__":
    asyncio.run(main())