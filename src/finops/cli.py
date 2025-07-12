# src/finops/cli.py
"""Simple CLI for discovery only."""

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
@click.option('--output', '-o', default='./discovery_results.json', help='Output JSON file path')
@click.option('--cost-days', default=30, help='Number of days for cost analysis (default: 30)')
@click.option('--metrics-hours', default=24, help='Number of hours for metrics collection (default: 24)')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.option('--debug', is_flag=True, help='Enable debug logging')
def discover(output, cost_days, metrics_hours, verbose, debug):
    """
    Discover AKS clusters and resources in the configured Azure resource group.
    
    This command discovers all AKS clusters in the resource group specified in your
    Azure configuration (.env file), collects cost data, utilization metrics, and 
    Kubernetes resources.
    
    Configure your .env file with:
        AZURE_SUBSCRIPTION_ID=your-subscription-id
        AZURE_TENANT_ID=your-tenant-id
        AZURE_CLIENT_ID=your-client-id
        AZURE_CLIENT_SECRET=your-client-secret
        AZURE_RESOURCE_GROUP=your-resource-group
        AZURE_LOG_ANALYTICS_WORKSPACE_ID=your-workspace-id
    
    Example:
        python -m finops.cli --output results.json
    """
    
    async def run_discovery():
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
                click.echo(f"🚀 Starting discovery for resource group: {resource_group}")
                click.echo(f"📅 Cost analysis period: {cost_days} days")
                click.echo(f"📊 Metrics collection: {metrics_hours} hours")
            
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
            
            # Run discovery
            async with DiscoveryOrchestrator(config) as orchestrator:
                if verbose:
                    click.echo("🔍 Running comprehensive discovery...")
                
                discovery_results = await orchestrator.run_discovery()
                
                # Save results
                output_path = Path(output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w') as f:
                    json.dump(discovery_results, f, indent=2, default=str)
                
                # Print summary
                discovery_data = discovery_results.get("discovery_data", {})
                summary = discovery_data.get("summary", {})
                
                click.echo("✅ Discovery completed successfully!")
                click.echo(f"📁 Results saved to: {output_path}")
                click.echo(f"🏗️  Clusters: {summary.get('total_clusters', 0)}")
                click.echo(f"🖥️  Nodes: {summary.get('total_nodes', 0)}")
                click.echo(f"📦 Pods: {summary.get('total_pods', 0)}")
                click.echo(f"💰 Total Cost: ${summary.get('total_cost', 0.0):.2f}")
                
                return 0
                
        except Exception as e:
            click.echo(f"❌ Discovery failed: {e}")
            if debug:
                import traceback
                click.echo(traceback.format_exc())
            return 1
    
    exit_code = asyncio.run(run_discovery())
    exit(exit_code)


if __name__ == '__main__':
    discover()
    