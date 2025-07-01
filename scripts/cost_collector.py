"""Cost collection script for detailed cost analysis."""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.finops.config.settings import Settings
from src.finops.core.utils import setup_logging
from src.finops.clients.azure.client_factory import AzureClientFactory
from src.finops.data.storage.file_storage import FileStorage
import structlog

logger = structlog.get_logger(__name__)


async def main():
    """Main cost collection function."""
    try:
        settings = Settings.create_from_env()
        setup_logging(log_level=settings.log_level.value)
        
        logger.info("Starting cost collection")
        
        # Initialize Azure clients
        azure_config = settings.azure.model_dump()
        azure_factory = AzureClientFactory(azure_config)
        cost_client = azure_factory.create_cost_client()
        await cost_client.connect()
        
        # Initialize storage
        storage = FileStorage(settings.storage.base_path)
        
        # Collect costs for different time periods
        cost_data = {}
        
        # Last 30 days
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=30)
        
        logger.info("Collecting 30-day cost data")
        cost_30d = await cost_client.get_resource_costs(
            resource_group=settings.azure.resource_group,
            start_date=start_date,
            end_date=end_date
        )
        cost_data['30_days'] = cost_30d
        
        # Last 7 days
        start_date = end_date - timedelta(days=7)
        logger.info("Collecting 7-day cost data")
        cost_7d = await cost_client.get_resource_costs(
            resource_group=settings.azure.resource_group,
            start_date=start_date,
            end_date=end_date
        )
        cost_data['7_days'] = cost_7d
        
        # Service breakdown
        start_date = end_date - timedelta(days=30)
        logger.info("Collecting service cost breakdown")
        service_costs = await cost_client.get_cost_by_service(
            resource_group=settings.azure.resource_group,
            start_date=start_date,
            end_date=end_date
        )
        cost_data['service_breakdown'] = service_costs
        
        # Save results
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        await storage.save("cost_analysis", f"collection_{timestamp}", {
            'collection_timestamp': datetime.now(timezone.utc).isoformat(),
            'collection_period': '30_days',
            'data': cost_data
        })
        
        await storage.save("cost_analysis", "latest", {
            'collection_timestamp': datetime.now(timezone.utc).isoformat(),
            'collection_period': '30_days',
            'data': cost_data
        })
        
        # Generate summary
        total_30d = cost_data['30_days'].get('total', 0)
        total_7d = cost_data['7_days'].get('total', 0)
        
        logger.info(
            "Cost collection completed",
            total_30d_cost=total_30d,
            total_7d_cost=total_7d,
            daily_average=total_30d / 30 if total_30d > 0 else 0,
            currency=cost_data['30_days'].get('currency', 'USD')
        )
        
    except Exception as e:
        logger.error("Cost collection failed", error=str(e))
        sys.exit(1)
    
    finally:
        if 'cost_client' in locals():
            await cost_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())