"""Command line interface for FinOps platform."""

import asyncio
import click
from pathlib import Path
from typing import Optional
import structlog
from datetime import datetime, timedelta, timezone

from finops.config.settings import Settings
from finops.core.utils import setup_logging
from finops.discovery.cost.cost_discovery import CostDiscoveryService
from finops.discovery.enhanced_orchestrator import EnhancedDiscoveryOrchestrator
from finops.discovery.infrastructure.aks_discovery import AKSDiscoveryService
from finops.discovery.infrastructure.node_pool_discovery import NodePoolDiscoveryService
from finops.discovery.orchestrator import DiscoveryOrchestrator
from finops.clients.azure.client_factory import AzureClientFactory
from finops.clients.kubernetes.client_factory import KubernetesClientFactory
from finops.data.storage.file_storage import FileStorage
from finops.data.repositories.cluster_repository import ClusterRepository

logger = structlog.get_logger(__name__)


@click.group()
@click.option('--config', '-c', help='Configuration file path')
@click.option('--environment', '-e', default='development', help='Environment (development, staging, production)')
@click.option('--debug', is_flag=True, help='Enable debug mode')
@click.pass_context
def cli(ctx, config, environment, debug):
    """Kubernetes FinOps Platform CLI."""
    ctx.ensure_object(dict)
    
    # Load settings
    settings = Settings.create_from_env()
    if debug:
        settings.debug = True
        settings.log_level = "DEBUG"
    
    # Setup logging
    setup_logging(log_level=settings.log_level.value)
    
    ctx.obj['settings'] = settings
    ctx.obj['environment'] = environment


@cli.command()
@click.option('--resource-group', '-rg', help='Azure resource group to discover')
@click.option('--cluster-name', '-c', help='Specific cluster name to discover')
@click.option('--output', '-o', default='./discovery_results.json', help='Output file path')
@click.option('--parallel', is_flag=True, default=True, help='Run discovery in parallel')
@click.option('--include-metrics', is_flag=True, help='Include metrics collection')
@click.pass_context
def discover(ctx, resource_group, cluster_name, output, parallel, include_metrics):
    """Run discovery operations."""
    settings = ctx.obj['settings']
    
    async def run_discovery():
        logger.info("Starting FinOps discovery")
        
        try:
            # Initialize Azure clients
            azure_config = settings.azure.model_dump()
            if resource_group:
                azure_config['resource_group'] = resource_group

            if cluster_name:
                azure_config['cluster_name'] = cluster_name
            
            azure_factory = AzureClientFactory(azure_config)
            azure_clients = await azure_factory.create_all_clients()
            
            # Initialize storage
            storage = FileStorage(settings.storage.base_path)
            
            # Create discovery services
            
            # from finops.discovery.infrastructure.aks_discovery import AKSDiscoveryService
            # from finops.discovery.infrastructure.node_pool_discovery import NodePoolDiscoveryService
            # from finops.discovery.infrastructure.network_discovery import NetworkDiscoveryService
            # from finops.discovery.infrastructure.storage_discovery import StorageDiscoveryService
            # from finops.discovery.cost.cost_discovery import CostDiscoveryService
            # from finops.discovery.utilization.metrics_collector1 import MetricsCollectionService
            
            services = []

            services = [
                AKSDiscoveryService(azure_clients['aks'], azure_config),
                NodePoolDiscoveryService(azure_clients['aks'], azure_config),
                CostDiscoveryService(azure_clients['cost'], azure_config),
            ]
            
            # # AKS discovery
            # aks_service = AKSDiscoveryService(azure_clients['aks'], azure_config)
            # services.append(aks_service)
            
            # # Node pool discovery
            # node_pool_config = azure_config.copy()
            # if cluster_name:
            #     node_pool_config['cluster_name'] = cluster_name
            
            # node_pool_service = NodePoolDiscoveryService(azure_clients['aks'], node_pool_config)
            # services.append(node_pool_service)

            # network_service = NetworkDiscoveryService(azure_clients['resource'], azure_config)
            # services.append(network_service)
            # storage_service = StorageDiscoveryService(azure_clients['resource'], azure_config)
            # services.append(storage_service)
            
            # # Cost discovery
            # cost_service = CostDiscoveryService(azure_clients['cost'], azure_config)
            # services.append(cost_service)
            
            # Metrics discovery (if requested)
            # if include_metrics:
            #     from finops.discovery.utilization.metrics_collector1 import MetricsCollectionService
                
            #     metrics_config = azure_config.copy()
            #     if cluster_name and resource_group:
            #         metrics_config['cluster_resource_id'] = f"/subscriptions/{settings.azure.subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.ContainerService/managedClusters/{cluster_name}"
                
            #     metrics_service = MetricsCollectionService(azure_clients['monitor'], metrics_config)
            #     services.append(metrics_service)
            
            # Run discovery
            # orchestrator = DiscoveryOrchestrator(
            #     services=services,
            #     max_concurrency=settings.discovery.parallel_workers,
            #     timeout_seconds=settings.discovery.timeout_seconds
            # )

            enhanced_orchestrator = EnhancedDiscoveryOrchestrator(services)
        
        
            
            # if parallel:
            #     results = await orchestrator.run_parallel_discovery()
            # else:
            #     results = await orchestrator.run_sequential_discovery()
            results = await enhanced_orchestrator.run_discovery_with_analysis()
            
            # Save results
            await storage.save("discovery_results", "latest", results)
            
            # Also save to specified output file
            if output:
                import json
                output_path = Path(output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
            
            # Print summary
            summary = results.get('summary', {})
            click.echo(f"\n✅ Discovery completed successfully!")
            click.echo(f"📊 Total resources discovered: {summary.get('total_resources', 0)}")
            click.echo(f"✅ Successful discoveries: {summary.get('successful_discoveries', 0)}")
            click.echo(f"❌ Failed discoveries: {summary.get('failed_discoveries', 0)}")
            click.echo(f"📁 Results saved to: {output}")
            
            if summary.get('errors'):
                click.echo(f"\n⚠️  Errors encountered:")
                for error in summary['errors']:
                    click.echo(f"   - {error}")
            
        except Exception as e:
            logger.error("Discovery failed", error=str(e))
            click.echo(f"❌ Discovery failed: {e}")
            raise click.ClickException(str(e))
        
        finally:
            # Cleanup
            for client in azure_clients.values():
                try:
                    await client.disconnect()
                except:
                    pass
    
    asyncio.run(run_discovery())


@cli.command()
@click.option('--input', '-i', default='./discovery_results.json', help='Discovery results file')
@click.option('--output', '-o', default='./analysis_results.json', help='Analysis output file')
@click.option('--cost-analysis', is_flag=True, help='Perform cost analysis')
@click.option('--utilization-analysis', is_flag=True, help='Perform utilization analysis')
@click.pass_context
def analyze(ctx, input, output, cost_analysis, utilization_analysis):
    """Run analysis on discovery results."""
    settings = ctx.obj['settings']
    
    click.echo("🔍 Starting analysis...")
    
    try:
        # Load discovery results
        import json
        with open(input, 'r') as f:
            discovery_data = json.load(f)
        
        analysis_results = {
            'input_file': input,
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'analyses': {}
        }
        
        # Basic summary analysis
        discovery_summary = discovery_data.get('summary', {})
        analysis_results['analyses']['summary'] = {
            'total_resources': discovery_summary.get('total_resources', 0),
            'successful_discoveries': discovery_summary.get('successful_discoveries', 0),
            'failed_discoveries': discovery_summary.get('failed_discoveries', 0),
            'discovery_types': discovery_summary.get('discovery_types', [])
        }
        
        # Cost analysis
        if cost_analysis:
            cost_data = discovery_data.get('discovery_data', {}).get('cost_data', {})
            if cost_data and cost_data.get('status') == 'success':
                cost_analysis_result = analyze_cost_data(cost_data.get('data', []))
                analysis_results['analyses']['cost_analysis'] = cost_analysis_result
            else:
                click.echo("⚠️  No cost data available for analysis")
        
        # Utilization analysis
        if utilization_analysis:
            resource_usage_data = discovery_data.get('discovery_data', {}).get('resource_usage', {})
            if resource_usage_data and resource_usage_data.get('status') == 'success':
                utilization_analysis_result = analyze_utilization_data(resource_usage_data.get('data', []))
                analysis_results['analyses']['utilization_analysis'] = utilization_analysis_result
            else:
                click.echo("⚠️  No utilization data available for analysis")
        
        # Save analysis results
        with open(output, 'w') as f:
            json.dump(analysis_results, f, indent=2, default=str)
        
        click.echo(f"✅ Analysis completed! Results saved to: {output}")
        
    except FileNotFoundError:
        click.echo(f"❌ Discovery results file not found: {input}")
        raise click.ClickException(f"File not found: {input}")
    except Exception as e:
        logger.error("Analysis failed", error=str(e))
        click.echo(f"❌ Analysis failed: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--host', default='0.0.0.0', help='API host')
@click.option('--port', default=8000, help='API port')
@click.option('--reload', is_flag=True, help='Enable auto-reload')
@click.pass_context
def serve(ctx, host, port, reload):
    """Start the FinOps API server."""
    settings = ctx.obj['settings']
    
    try:
        import uvicorn
        from finops.api.main import create_app
        
        app = create_app(settings)
        
        click.echo(f"🚀 Starting FinOps API server...")
        click.echo(f"📡 Server will be available at http://{host}:{port}")
        click.echo(f"📚 API documentation at http://{host}:{port}/docs")
        
        uvicorn.run(
            app,
            host=host,
            port=port,
            reload=reload,
            log_level=settings.log_level.value.lower()
        )
        
    except ImportError:
        click.echo("❌ FastAPI dependencies not installed. Install with: pip install 'kubernetes-finops[api]'")
        raise click.ClickException("Missing API dependencies")
    except Exception as e:
        logger.error("Failed to start API server", error=str(e))
        click.echo(f"❌ Failed to start server: {e}")
        raise click.ClickException(str(e))


def analyze_cost_data(cost_data: list) -> dict:
    """Analyze cost data for insights."""
    analysis = {
        'total_cost_categories': len(cost_data),
        'cost_insights': [],
        'recommendations': []
    }
    
    for cost_category in cost_data:
        if cost_category.get('type') == 'resource_costs':
            data = cost_category.get('data', {})
            total_cost = data.get('total', 0)
            
            analysis['cost_insights'].append({
                'category': 'resource_costs',
                'total_cost': total_cost,
                'currency': data.get('currency', 'USD'),
                'cost_breakdown': {
                    'compute': data.get('compute', 0),
                    'storage': data.get('storage', 0),
                    'network': data.get('network', 0),
                    'other': data.get('other', 0)
                }
            })
            
            # Generate recommendations
            if data.get('compute', 0) > total_cost * 0.7:
                analysis['recommendations'].append(
                    "High compute costs detected - consider rightsizing VMs or using spot instances"
                )
    
    return analysis


def analyze_utilization_data(utilization_data: list) -> dict:
    """Analyze utilization data for insights."""
    analysis = {
        'total_analysis_categories': len(utilization_data),
        'utilization_insights': [],
        'recommendations': []
    }
    
    for util_category in utilization_data:
        if util_category.get('type') == 'resource_usage_analysis':
            data = util_category.get('data', {})
            
            analysis['utilization_insights'].append({
                'category': 'resource_usage',
                'total_pods': data.get('total_pods', 0),
                'pods_with_requests': data.get('pods_with_requests', 0),
                'pods_without_requests': data.get('pods_without_requests', 0),
                'resource_coverage': data.get('resource_coverage', {})
            })
            
            # Generate recommendations
            coverage = data.get('resource_coverage', {})
            requests_coverage = coverage.get('requests_coverage', 0)
            
            if requests_coverage < 80:
                analysis['recommendations'].append(
                    f"Low resource request coverage ({requests_coverage:.1f}%) - add resource requests to improve scheduling"
                )
    
    return analysis


if __name__ == '__main__':
    cli()