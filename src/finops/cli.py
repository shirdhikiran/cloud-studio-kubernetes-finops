# src/finops/cli.py
"""Phase 1 Discovery CLI - Pure data collection."""

import asyncio
import click
import json
from pathlib import Path
import structlog

from finops.config.settings import Settings
from finops.core.utils import setup_logging
from finops.discovery.discovery_orchestrator import DiscoveryOrchestrator

logger = structlog.get_logger(__name__)


@click.command()
@click.option('--output', '-o', default='./data/phase1_discovery_results.json', help='Output JSON file path')
@click.option('--cost-days', default=30, help='Number of days for cost analysis (default: 30)')
@click.option('--metrics-hours', default=24, help='Number of hours for metrics collection (default: 24)')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.option('--debug', is_flag=True, help='Enable debug logging')
def discover(output, cost_days, metrics_hours, verbose, debug):
    """
    Phase 1 Discovery: Collect raw data from AKS clusters and Azure resources.
    
    This command performs pure data discovery without analysis or processing:
    - Discovers AKS clusters and their configurations
    - Collects raw cost data from Azure Cost Management
    - Gathers raw metrics from Azure Monitor and Container Insights
    - Inventories Kubernetes resources via API
    
    Configure your .env file with:
        AZURE_SUBSCRIPTION_ID=your-subscription-id
        AZURE_TENANT_ID=your-tenant-id
        AZURE_CLIENT_ID=your-client-id
        AZURE_CLIENT_SECRET=your-client-secret
        AZURE_RESOURCE_GROUP=your-resource-group
        AZURE_LOG_ANALYTICS_WORKSPACE_ID=your-workspace-id
    
    Example:
        python -m finops.cli --output phase1_data.json --cost-days 30 --metrics-hours 24
    """
    
    async def run_phase1_discovery():
        try:
            # Setup logging
            log_level = "DEBUG" if debug else "INFO"
            setup_logging(log_level=log_level)
            
            # Load settings
            settings = Settings.create_from_env()
            if debug:
                settings.debug = True
                settings.log_level = "DEBUG"
            
            # Check if resource group is configured
            if not settings.azure.resource_group:
                click.echo("❌ Error: AZURE_RESOURCE_GROUP not configured in .env file")
                click.echo("Please set AZURE_RESOURCE_GROUP in your .env file")
                return 1
            
            resource_group = settings.azure.resource_group
            
            if verbose:
                click.echo("🔍 Phase 1: Discovery - Raw Data Collection")
                click.echo(f"📍 Resource Group: {resource_group}")
                click.echo(f"📅 Cost Analysis Period: {cost_days} days")
                click.echo(f"📊 Metrics Collection: {metrics_hours} hours")
                click.echo("📋 Data Sources:")
                click.echo("   • Azure Resource Manager (AKS clusters)")
                click.echo("   • Azure Cost Management (billing data)")
                click.echo("   • Azure Monitor (metrics)")
                click.echo("   • Container Insights (log analytics)")
                click.echo("   • Kubernetes API (resources)")
            
            # Prepare configuration
            config = {
                "azure": settings.azure.model_dump(),
                "kubernetes": settings.kubernetes.model_dump(),
                "discovery": {
                    **settings.discovery.model_dump(),
                    "cost_analysis_days": cost_days,
                    "metrics_hours": metrics_hours
                }
            }
            
            # Run Phase 1 discovery
            async with DiscoveryOrchestrator(config) as orchestrator:
                if verbose:
                    click.echo("🚀 Starting Phase 1 data collection...")
                
                discovery_results = await orchestrator.run_discovery()
                
                # Save raw results
                output_path = Path(output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w') as f:
                    json.dump(discovery_results, f, indent=2, default=str)
                
                # Print Phase 1 summary
                discovery_data = discovery_results.get("discovery_data", {})
                summary = discovery_data.get("summary", {})
                
                click.echo("✅ Phase 1 Discovery completed successfully!")
                click.echo(f"📁 Raw data saved to: {output_path}")
                click.echo(f"📈 Data Collection Summary:")
                click.echo(f"   🏗️  Clusters discovered: {summary.get('total_clusters', 0)}")
                click.echo(f"   🖥️  Nodes inventoried: {summary.get('total_nodes', 0)}")
                click.echo(f"   📦 Pods cataloged: {summary.get('total_pods', 0)}")
                click.echo(f"   💰 Raw cost data: ${summary.get('total_cost', 0.0):.2f}")
                
                # Show metrics collection status
                clusters = discovery_data.get("clusters", [])
                if clusters:
                    metrics_collected = 0
                    for cluster in clusters:
                        raw_metrics = cluster.get("raw_metrics", {})
                        if raw_metrics and raw_metrics.get("collection_metadata", {}).get("metrics_collected", 0) > 0:
                            metrics_collected += 1
                    
                    click.echo(f"   📊 Clusters with metrics: {metrics_collected}/{len(clusters)}")
                
                click.echo("🔄 Next Phase: Run analytics to process this raw data")
                
                return 0
                
        except Exception as e:
            click.echo(f"❌ Phase 1 Discovery failed: {e}")
            if debug:
                import traceback
                click.echo(traceback.format_exc())
            return 1
    
    exit_code = asyncio.run(run_phase1_discovery())
    exit(exit_code)


if __name__ == '__main__':
    discover()