"""Enhanced CLI for Phase 1 comprehensive discovery."""

import asyncio
import click
import json
from pathlib import Path
from typing import Optional
import structlog
from datetime import datetime, timezone

from finops.config.settings import Settings
from finops.core.utils import setup_logging
from finops.discovery.discovery_orchestrator import DiscoveryOrchestrator
from finops.data.storage.file_storage import FileStorage


logger = structlog.get_logger(__name__)


@click.group()
@click.option('--config', '-c', help='Configuration file path')
@click.option('--environment', '-e', default='development', help='Environment (development, staging, production)')
@click.option('--debug', is_flag=True, help='Enable debug mode')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.pass_context
def cli(ctx, config, environment, debug, verbose):
    """Kubernetes FinOps Platform - Phase 1 Discovery CLI."""
    ctx.ensure_object(dict)
    
    # Load settings
    settings = Settings.create_from_env()
    if debug:
        settings.debug = True
        settings.log_level = "DEBUG"
    
    # Setup logging
    log_level = "DEBUG" if verbose or debug else settings.log_level.value
    setup_logging(log_level=log_level)
    
    ctx.obj['settings'] = settings
    ctx.obj['environment'] = environment
    ctx.obj['verbose'] = verbose


@cli.command()
@click.option('--output', '-o', default='./phase1_discovery_results.json', help='Output file path')
@click.option('--cost-analysis-days', default=30, help='Number of days for cost analysis')
@click.option('--metrics-hours', default=24, help='Number of hours for metrics collection')
@click.option('--validate-output', is_flag=True, help='Validate output format and completeness')
@click.pass_context
def discover_comprehensive(ctx, output, cost_analysis_days, metrics_hours, validate_output):
    """
    Run comprehensive Phase 1 discovery.
    
    This command performs complete discovery including:
    - Infrastructure discovery (clusters, nodes, node pools)
    - Cost discovery with allocation
    - Utilization metrics collection
    - Kubernetes resources discovery
    - Azure resources mapping
    - Preliminary optimization opportunities
    """
    settings = ctx.obj['settings']
    verbose = ctx.obj.get('verbose', False)
    
    async def run_comprehensive_discovery():
        logger.info("Starting comprehensive Phase 1 discovery")
        
        if verbose:
            click.echo("🚀 Initializing comprehensive discovery...")
        
        try:
            # Prepare enhanced configuration
            enhanced_config = {
                "azure": settings.azure.model_dump(),
                "kubernetes": settings.kubernetes.model_dump(),
                "discovery": {
                    **settings.discovery.model_dump(),
                    "cost_analysis_days": cost_analysis_days,
                    "metrics_hours": metrics_hours,
                    "include_detailed_metrics": True,
                    "include_cost_allocation": True
                }
            }
            
            # Initialize storage
            storage = FileStorage(settings.storage.base_path)
            
            # Run comprehensive discovery
            async with DiscoveryOrchestrator(enhanced_config) as orchestrator:
                if verbose:
                    click.echo("🔍 Running comprehensive discovery...")
                
                discovery_results = await orchestrator.run_comprehensive_discovery()
                
                if verbose:
                    click.echo("💾 Saving discovery results...")
                
                # Save results to storage
                await orchestrator.save_discovery_results(discovery_results, storage)
                
                # Validate output if requested
                validation_results = None
                if validate_output:
                    if verbose:
                        click.echo("✅ Validating discovery output...")
                    validation_results = await orchestrator.validate_discovery_output(discovery_results)
                
                # Save to specified output file
                output_path = Path(output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                final_output = discovery_results.copy()
                if validation_results:
                    final_output["validation_results"] = validation_results
                
                with open(output_path, 'w') as f:
                    json.dump(final_output, f, indent=2, default=str)
                
                
                
                if validation_results and not validation_results["format_valid"]:
                    click.echo("⚠️  Validation errors found. Please review before proceeding to Phase 2.")
                    return 1
                
                return 0
                
        except Exception as e:
            logger.error("Comprehensive discovery failed", error=str(e))
            click.echo(f"❌ Discovery failed: {e}")
            return 1
    
    exit_code = asyncio.run(run_comprehensive_discovery())
    ctx.exit(exit_code)




if __name__ == '__main__':
    cli()